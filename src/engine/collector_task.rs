use std::panic::AssertUnwindSafe;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::{FutureExt, StreamExt, TryStreamExt, stream};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::signals::Signals;
use super::{Fatal, Shared};
use crate::delivery::Filter;
use crate::error::panic_message;
use crate::obs;
use crate::{Collector, CollectorOpts, Envelope, ExitReason, Item, OnFull, SourceId, SystemEvent};

pub(crate) enum Msg<E> {
    Event(Envelope<E>),
    System(SystemEvent),
}

pub(crate) struct Sink<E> {
    pub(crate) filter: Option<Filter<E>>,
    pub(crate) kind: SinkKind<E>,
}

pub(crate) enum SinkKind<E> {
    Queue(QueueSink<E>),
    Broadcast {
        tx: broadcast::Sender<Envelope<E>>,
        wakers: Vec<Arc<Signals>>,
    },
    Watch {
        tx: watch::Sender<Option<Envelope<E>>>,
        wakers: Vec<Arc<Signals>>,
    },
}

pub(crate) struct QueueSink<E> {
    pub(crate) tx: mpsc::Sender<Msg<E>>,
    pub(crate) lossless: bool,
    pub(crate) on_full: OnFull,
    pub(crate) strategy: Arc<str>,
    pub(crate) signals: Arc<Signals>,
    pub(crate) slot: usize,
    pub(crate) high: usize,
    pub(crate) low: usize,
    pub(crate) lagging: bool,
    pub(crate) depth: obs::Gauge,
    pub(crate) wait: obs::Histogram,
    pub(crate) dropped: obs::Counter,
}

pub(crate) struct StrategyLink<E> {
    pub(crate) inline: Option<mpsc::Sender<Msg<E>>>,
    pub(crate) side: mpsc::UnboundedSender<SystemEvent>,
}

pub(crate) struct CollectorRuntime<E> {
    pub(crate) id: SourceId,
    pub(crate) index: usize,
    pub(crate) sinks: Vec<Sink<E>>,
    pub(crate) links: Vec<StrategyLink<E>>,
    pub(crate) shared: Arc<Shared>,
    pub(crate) fatal: Fatal,
}

enum Halt {
    Stopped,
    Aborted,
}

enum Ended {
    Clean,
    Failed(String),
}

impl<E: Send + Sync + 'static> QueueSink<E> {
    async fn push(&mut self, env: Envelope<E>, rt: &CollectorShared<'_>) -> Result<(), Halt> {
        let msg = match self.tx.try_send(Msg::Event(env)) {
            Ok(()) => {
                self.after_send();
                return Ok(());
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
            Err(mpsc::error::TrySendError::Full(msg)) => msg,
        };
        if !self.lossless {
            self.dropped.increment(1);
            self.signals.slots[self.slot]
                .dropped
                .fetch_add(1, Ordering::AcqRel);
            self.signals.wake();
            return Ok(());
        }
        self.raise_lagging(self.tx.max_capacity());
        let started = Instant::now();
        let stop = rt.shared.collectors_stop.cancelled();
        let result = match self.on_full {
            OnFull::Wait => tokio::select! {
                biased;
                () = stop => return Err(Halt::Stopped),
                r = self.tx.send(msg) => r,
            },
            OnFull::Abort { after } => tokio::select! {
                biased;
                () = stop => return Err(Halt::Stopped),
                r = tokio::time::timeout(after, self.tx.send(msg)) => match r {
                    Ok(r) => r,
                    Err(_) => {
                        let _ = rt.fatal.send(ExitReason::ChannelAborted {
                            source: rt.id.to_string(),
                            strategy: self.strategy.to_string(),
                            waited: after,
                        });
                        return Err(Halt::Aborted);
                    }
                },
            },
        };
        self.wait.record(started.elapsed().as_secs_f64());
        if result.is_ok() {
            self.after_send();
        }
        Ok(())
    }

    fn after_send(&mut self) {
        let depth = self.tx.max_capacity() - self.tx.capacity();
        self.depth.set(depth as f64);
        if self.lagging {
            if depth <= self.low {
                self.lagging = false;
            }
        } else if depth >= self.high {
            self.raise_lagging(depth);
        }
    }

    fn raise_lagging(&mut self, depth: usize) {
        if !self.lagging {
            self.lagging = true;
            self.signals.slots[self.slot]
                .lagging
                .store(depth, Ordering::Release);
            self.signals.wake();
        }
    }
}

impl<E: Send + Sync + 'static> Sink<E> {
    fn accepts(&self, env: &Envelope<E>) -> bool {
        self.filter.as_ref().is_none_or(|filter| filter(&env.event))
    }

    async fn deliver(&mut self, env: &Envelope<E>, rt: &CollectorShared<'_>) -> Result<(), Halt> {
        match &mut self.kind {
            SinkKind::Queue(queue) => return queue.push(env.clone(), rt).await,
            SinkKind::Broadcast { tx, wakers } => {
                let _ = tx.send(env.clone());
                wakers.iter().for_each(|w| w.wake());
            }
            SinkKind::Watch { tx, wakers } => {
                tx.send_replace(Some(env.clone()));
                wakers.iter().for_each(|w| w.wake());
            }
        }
        Ok(())
    }
}

impl<E> StrategyLink<E> {
    async fn send(&self, event: &SystemEvent, stop: &CancellationToken) -> Result<(), Halt> {
        match &self.inline {
            Some(tx) => tokio::select! {
                biased;
                () = stop.cancelled() => return Err(Halt::Stopped),
                _ = tx.send(Msg::System(event.clone())) => {}
            },
            None => {
                let _ = self.side.send(event.clone());
            }
        }
        Ok(())
    }
}

struct CollectorShared<'a> {
    id: &'a SourceId,
    shared: &'a Shared,
    fatal: &'a Fatal,
}

impl<E: Send + Sync + 'static> CollectorRuntime<E> {
    async fn dispatch(&mut self, env: Envelope<E>) -> Result<(), Halt> {
        let rt = CollectorShared {
            id: &self.id,
            shared: &self.shared,
            fatal: &self.fatal,
        };
        stream::iter(self.sinks.iter_mut().filter(|sink| sink.accepts(&env)))
            .map(Ok)
            .try_for_each(|sink| sink.deliver(&env, &rt))
            .await
    }

    async fn system(&self, event: SystemEvent) -> Result<(), Halt> {
        let stop = &self.shared.collectors_stop;
        stream::iter(&self.links)
            .map(Ok)
            .try_for_each(|link| link.send(&event, stop))
            .await
    }

    fn state(&self) -> &super::CollectorState {
        &self.shared.collectors[self.index]
    }
}

pub(crate) async fn run_collector<E, C>(
    collector: Arc<C>,
    opts: CollectorOpts,
    mut rt: CollectorRuntime<E>,
) where
    E: Send + Sync + 'static,
    C: Collector<E>,
{
    let span = tracing::info_span!("collector", name = %rt.id);
    async move {
        let label = [("source", rt.id.as_str())];
        let events = obs::counter(obs::EVENTS, &label);
        let reconnects = obs::counter(obs::RECONNECTS, &label);
        let gaps_filled =
            obs::counter(obs::GAPS, &[("source", rt.id.as_str()), ("filled", "true")]);
        let gaps_unfilled = obs::counter(
            obs::GAPS,
            &[("source", rt.id.as_str()), ("filled", "false")],
        );
        let mut cursor: Option<C::Cursor> = None;
        let mut backoff = opts.backoff.start();
        let mut seq = 0u64;
        let stop = rt.shared.collectors_stop.clone();

        loop {
            let connect = AssertUnwindSafe(collector.connect(cursor.clone())).catch_unwind();
            let connected = tokio::select! {
                biased;
                () = stop.cancelled() => return,
                r = connect => r,
            };
            let error = match connected {
                Err(panic) => format!("connect panicked: {}", panic_message(&*panic)),
                Ok(Err(err)) => err.to_string(),
                Ok(Ok(stream)) => {
                    tracing::info!(resume = ?cursor, "connected");
                    let connected_at = Instant::now();
                    rt.state().connected.store(true, Ordering::Release);
                    if rt
                        .system(SystemEvent::CollectorConnected {
                            source: rt.id.clone(),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    let mut stream = pin!(stream.into_stream());
                    let ended = loop {
                        let next = tokio::select! {
                            biased;
                            () = stop.cancelled() => return,
                            n = AssertUnwindSafe(stream.next()).catch_unwind() => n,
                        };
                        let item = match next {
                            Err(panic) => {
                                break Ended::Failed(format!(
                                    "stream panicked: {}",
                                    panic_message(&*panic)
                                ));
                            }
                            Ok(None) => break Ended::Clean,
                            Ok(Some(Err(err))) => break Ended::Failed(err.to_string()),
                            Ok(Some(Ok(item))) => item,
                        };
                        let halted = match item {
                            Item::Event {
                                event,
                                cursor: next_cursor,
                            } => {
                                seq += 1;
                                let env = Envelope {
                                    source: rt.id.clone(),
                                    seq,
                                    received_at: Instant::now(),
                                    event: Arc::new(event),
                                };
                                events.increment(1);
                                rt.state()
                                    .last_event
                                    .store(rt.shared.millis_since_start(), Ordering::Release);
                                let r = rt.dispatch(env).await;
                                if let Some(c) = next_cursor {
                                    cursor = Some(c);
                                }
                                r
                            }
                            Item::GapFilled { from, to } => {
                                gaps_filled.increment(1);
                                tracing::info!(from, to, "gap filled");
                                rt.system(SystemEvent::GapFilled {
                                    source: rt.id.clone(),
                                    from,
                                    to,
                                })
                                .await
                            }
                            Item::GapUnfilled { from, to } => {
                                gaps_unfilled.increment(1);
                                tracing::warn!(from, to, "gap could not be filled");
                                rt.system(SystemEvent::GapUnfilled {
                                    source: rt.id.clone(),
                                    from: Some(from),
                                    to: Some(to),
                                })
                                .await
                            }
                        };
                        if halted.is_err() {
                            return;
                        }
                    };
                    rt.state().connected.store(false, Ordering::Release);
                    if connected_at.elapsed() >= opts.backoff.healthy_after() {
                        backoff.reset();
                    }
                    let error = match ended {
                        Ended::Clean if collector.is_exhausted() => {
                            tracing::info!("source exhausted");
                            rt.state().done.store(true, Ordering::Release);
                            return;
                        }
                        Ended::Clean => "stream ended".to_owned(),
                        Ended::Failed(error) => error,
                    };
                    if rt
                        .system(SystemEvent::CollectorDisconnected {
                            source: rt.id.clone(),
                            error: error.clone(),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    error
                }
            };

            reconnects.increment(1);
            let Some(delay) = backoff.next_delay() else {
                tracing::error!(%error, attempts = backoff.attempts(), "giving up");
                let _ = rt.fatal.send(ExitReason::CollectorFailed {
                    collector: rt.id.to_string(),
                    error,
                });
                return;
            };
            tracing::warn!(%error, ?delay, "reconnecting");
            tokio::select! {
                biased;
                () = stop.cancelled() => return,
                () = tokio::time::sleep(delay) => {}
            }
        }
    }
    .instrument(span)
    .await
}
