use std::sync::Arc;

use itertools::{Either, Itertools};
use tokio::sync::{broadcast, mpsc, watch};

use super::builder::StrategyRegistration;
use super::collector_task::{Msg, QueueSink, Sink, SinkKind, StrategyLink};
use super::router::Router;
use super::signals::Signals;
use super::strategy_task::{BroadcastRx, StrategyRuntime};
use super::{Fatal, Shared};
use crate::delivery::Subscription;
use crate::notify::NotifierHandle;
use crate::obs;
use crate::{Delivery, Envelope, SourceId};

type SharedBroadcast<E> = (broadcast::Sender<Envelope<E>>, Vec<Arc<Signals>>);
type SharedWatch<E> = (watch::Sender<Option<Envelope<E>>>, Vec<Arc<Signals>>);

pub(super) struct CollectorWiring<E> {
    sinks: Vec<Sink<E>>,
    links: Vec<StrategyLink<E>>,
    broadcast_capacity: usize,
    broadcast: Option<SharedBroadcast<E>>,
    watch: Option<SharedWatch<E>>,
}

struct Target<'a, E> {
    name: &'a Arc<str>,
    signals: &'a Arc<Signals>,
    lossless: Option<&'a mpsc::Sender<Msg<E>>>,
    drop_newest: Option<&'a mpsc::Sender<Msg<E>>>,
    lagging_threshold: f64,
}

enum SideRx<E> {
    Broadcast(BroadcastRx<E>),
    Watch(watch::Receiver<Option<Envelope<E>>>),
}

impl<E> CollectorWiring<E> {
    pub(super) fn finish(mut self) -> (Vec<Sink<E>>, Vec<StrategyLink<E>>) {
        if let Some((tx, wakers)) = self.broadcast {
            self.sinks.push(Sink {
                filter: None,
                kind: SinkKind::Broadcast { tx, wakers },
            });
        }
        if let Some((tx, wakers)) = self.watch {
            self.sinks.push(Sink {
                filter: None,
                kind: SinkKind::Watch { tx, wakers },
            });
        }
        (self.sinks, self.links)
    }

    fn subscribe(&mut self, sub: &Subscription<E>, target: &Target<'_, E>) -> Option<SideRx<E>> {
        let slot = target.signals.slot_of(&sub.source);
        let Subscription {
            source,
            delivery,
            filter,
            on_full,
        } = sub;
        let name = target.name;
        let labels = [("source", source.as_str()), ("strategy", &**name)];
        let dropped = || {
            obs::counter(
                obs::DROPPED,
                &[
                    ("source", source.as_str()),
                    ("strategy", &**name),
                    ("delivery", delivery.label()),
                ],
            )
        };
        match *delivery {
            Delivery::Lossless { .. } | Delivery::DropNewest { .. } => {
                let lossless_kind = matches!(delivery, Delivery::Lossless { .. });
                let channel = if lossless_kind {
                    target.lossless
                } else {
                    target.drop_newest
                };
                let tx = channel.expect("channel exists for this delivery").clone();
                let capacity = tx.max_capacity();
                let high = ((capacity as f64 * target.lagging_threshold).ceil() as usize)
                    .clamp(1, capacity);
                let queue = QueueSink {
                    tx,
                    lossless: lossless_kind,
                    on_full: *on_full,
                    strategy: Arc::clone(name),
                    signals: Arc::clone(target.signals),
                    slot,
                    high,
                    low: high / 2,
                    lagging: false,
                    depth: obs::gauge(obs::CHANNEL_DEPTH, &labels),
                    wait: obs::histogram(obs::DISPATCH_WAIT, &labels),
                    dropped: dropped(),
                };
                self.sinks.push(Sink {
                    filter: filter.clone(),
                    kind: SinkKind::Queue(queue),
                });
                None
            }
            Delivery::DropOldest { capacity } => {
                let rx = match filter {
                    None => {
                        let cap = self.broadcast_capacity;
                        let (tx, wakers) = self
                            .broadcast
                            .get_or_insert_with(|| (broadcast::channel(cap).0, Vec::new()));
                        wakers.push(Arc::clone(target.signals));
                        tx.subscribe()
                    }
                    Some(filter) => {
                        let (tx, rx) = broadcast::channel(capacity);
                        let kind = SinkKind::Broadcast {
                            tx,
                            wakers: vec![Arc::clone(target.signals)],
                        };
                        self.sinks.push(Sink {
                            filter: Some(filter.clone()),
                            kind,
                        });
                        rx
                    }
                };
                Some(SideRx::Broadcast(BroadcastRx {
                    slot,
                    rx,
                    dropped: dropped(),
                }))
            }
            Delivery::Latest => {
                let rx = match filter {
                    None => {
                        let (tx, wakers) = self
                            .watch
                            .get_or_insert_with(|| (watch::channel(None).0, Vec::new()));
                        wakers.push(Arc::clone(target.signals));
                        tx.subscribe()
                    }
                    Some(filter) => {
                        let (tx, rx) = watch::channel(None);
                        let kind = SinkKind::Watch {
                            tx,
                            wakers: vec![Arc::clone(target.signals)],
                        };
                        self.sinks.push(Sink {
                            filter: Some(filter.clone()),
                            kind,
                        });
                        rx
                    }
                };
                Some(SideRx::Watch(rx))
            }
        }
    }
}

pub(super) struct Wiring<E> {
    pub(super) collectors: Vec<CollectorWiring<E>>,
    lagging_threshold: f64,
}

impl<E: Send + Sync + 'static> Wiring<E> {
    pub(super) fn new<A>(
        collectors: &[SourceId],
        strategies: &[StrategyRegistration<E, A>],
        lagging_threshold: f64,
    ) -> Self {
        let collectors = collectors
            .iter()
            .map(|id| {
                let broadcast_capacity = strategies
                    .iter()
                    .flat_map(|reg| &reg.subs.subs)
                    .filter(|sub| &sub.source == id && sub.filter.is_none())
                    .filter_map(|sub| match sub.delivery {
                        Delivery::DropOldest { capacity } => Some(capacity),
                        _ => None,
                    })
                    .max()
                    .unwrap_or(1);
                CollectorWiring {
                    sinks: Vec::new(),
                    links: Vec::new(),
                    broadcast_capacity,
                    broadcast: None,
                    watch: None,
                }
            })
            .collect();
        Self {
            collectors,
            lagging_threshold,
        }
    }

    pub(super) fn strategy<A: Send + Sync + 'static>(
        &mut self,
        index: usize,
        reg: &StrategyRegistration<E, A>,
        shared: &Arc<Shared>,
        fatal: &Fatal,
        router: &Arc<Router<A>>,
        notifier: &NotifierHandle,
    ) -> StrategyRuntime<E, A> {
        let name: Arc<str> = reg.entry.name().into();
        let subs = &reg.subs.subs;
        let sources: Vec<SourceId> = subs.iter().map(|sub| sub.source.clone()).unique().collect();
        let (signals, wake) = Signals::new(&sources);
        let signals = Arc::new(signals);
        let max_cap = |pred: fn(&Delivery) -> bool| {
            subs.iter()
                .filter(|s| pred(&s.delivery))
                .filter_map(|s| s.delivery.capacity())
                .max()
        };
        let lossless = max_cap(|d| matches!(d, Delivery::Lossless { .. })).map(mpsc::channel);
        let drop_newest = max_cap(|d| matches!(d, Delivery::DropNewest { .. })).map(mpsc::channel);
        let (system_tx, system_rx) = mpsc::unbounded_channel();
        let collector_of = |source: &SourceId| {
            shared
                .collectors
                .iter()
                .position(|c| &c.name == source)
                .expect("validated")
        };

        let target = Target {
            name: &name,
            signals: &signals,
            lossless: lossless.as_ref().map(|(tx, _)| tx),
            drop_newest: drop_newest.as_ref().map(|(tx, _)| tx),
            lagging_threshold: self.lagging_threshold,
        };
        let (broadcasts, watches) = subs
            .iter()
            .filter_map(|sub| self.collectors[collector_of(&sub.source)].subscribe(sub, &target))
            .partition_map(|rx| match rx {
                SideRx::Broadcast(rx) => Either::Left(rx),
                SideRx::Watch(rx) => Either::Right(rx),
            });

        sources.iter().for_each(|source| {
            let has_lossless = subs
                .iter()
                .any(|s| &s.source == source && matches!(s.delivery, Delivery::Lossless { .. }));
            let inline = target.lossless.filter(|_| has_lossless).cloned();
            self.collectors[collector_of(source)]
                .links
                .push(StrategyLink {
                    inline,
                    side: system_tx.clone(),
                });
        });

        StrategyRuntime {
            index,
            name,
            lossless: lossless.map(|(_, rx)| rx),
            drop_newest: drop_newest.map(|(_, rx)| rx),
            broadcasts,
            watches,
            system: system_rx,
            wake,
            signals,
            on_error: reg.subs.on_error,
            on_panic: reg.subs.on_panic,
            max_tasks: reg.subs.max_tasks,
            router: Arc::clone(router),
            notifier: notifier.clone(),
            shared: Arc::clone(shared),
            fatal: fatal.clone(),
            rotation: 0,
        }
    }
}
