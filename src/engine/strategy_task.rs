use std::collections::VecDeque;
use std::future::Future;
use std::iter;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use futures::FutureExt;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_util::sync::WaitForCancellationFutureOwned;
use tracing::Instrument;

use super::collector_task::Msg;
use super::router::Router;
use super::signals::Signals;
use super::{Fatal, Shared};
use crate::error::panic_message;
use crate::notify::NotifierHandle;
use crate::obs;
use crate::strategy::TaskSpawner;
use crate::{
    ActionSender, BoxError, Envelope, ExitReason, OnError, OnPanic, Strategy, StrategyCtx,
    SystemEvent,
};

const SIDE_BUDGET: usize = 64;

pub(crate) struct BroadcastRx<E> {
    pub(crate) slot: usize,
    pub(crate) rx: broadcast::Receiver<Envelope<E>>,
    pub(crate) dropped: obs::Counter,
}

pub(crate) struct StrategyRuntime<E, A> {
    pub(crate) index: usize,
    pub(crate) name: Arc<str>,
    pub(crate) lossless: Option<mpsc::Receiver<Msg<E>>>,
    pub(crate) drop_newest: Option<mpsc::Receiver<Msg<E>>>,
    pub(crate) broadcasts: Vec<BroadcastRx<E>>,
    pub(crate) watches: Vec<watch::Receiver<Option<Envelope<E>>>>,
    pub(crate) system: mpsc::UnboundedReceiver<SystemEvent>,
    pub(crate) wake: mpsc::Receiver<()>,
    pub(crate) signals: Arc<Signals>,
    pub(crate) on_error: OnError,
    pub(crate) on_panic: OnPanic,
    pub(crate) max_tasks: usize,
    pub(crate) router: Arc<Router<A>>,
    pub(crate) notifier: NotifierHandle,
    pub(crate) shared: Arc<Shared>,
    pub(crate) fatal: Fatal,
    pub(crate) rotation: usize,
}

enum Next<E> {
    Msg(Msg<E>),
    Wake,
    Stop,
}

enum Failure {
    Error(String),
    Panic(String),
}

struct Loop<E, A, S, F> {
    rt: StrategyRuntime<E, A>,
    handler: Handler<E, A, S>,
    factory: Option<F>,
    restarts: VecDeque<Instant>,
    restart_counter: obs::Counter,
}

struct Handler<E, A, S> {
    strategy: S,
    ctx: StrategyCtx<A>,
    on_error: OnError,
    handle_time: obs::Histogram,
    _event: PhantomData<fn(&E)>,
}

pub(crate) async fn run_strategy<E, A, S, F>(
    first: S,
    factory: Option<F>,
    rt: StrategyRuntime<E, A>,
    ready: oneshot::Sender<bool>,
) where
    E: Send + Sync + 'static,
    A: Send + Sync + 'static,
    S: Strategy<E, A>,
    F: FnMut() -> S + Send + 'static,
{
    let span = tracing::info_span!("strategy", name = %rt.name);
    let ctx = rt.new_ctx();
    let label = [("strategy", &*rt.name)];
    let handler = Handler {
        strategy: first,
        ctx,
        on_error: rt.on_error,
        handle_time: obs::histogram(obs::EVENT_HANDLE, &label),
        _event: PhantomData,
    };
    let mut this = Loop {
        restart_counter: obs::counter(obs::RESTARTS, &label),
        rt,
        handler,
        factory,
        restarts: VecDeque::new(),
    };
    this.run(ready).instrument(span).await;
}

impl<E, A> StrategyRuntime<E, A>
where
    E: Send + Sync + 'static,
    A: Send + Sync + 'static,
{
    fn new_ctx(&self) -> StrategyCtx<A> {
        StrategyCtx {
            actions: ActionSender::new(Arc::clone(&self.router)),
            tasks: TaskSpawner {
                strategy: Arc::clone(&self.name),
                tracker: self.shared.tracker.clone(),
                token: self.shared.tasks_stop.child_token(),
                active: Arc::clone(&self.shared.strategies[self.index].tasks_active),
                limit: self.max_tasks,
                gauge: obs::gauge(obs::TASKS_ACTIVE, &[("strategy", &*self.name)]),
            },
            shutdown: self.shared.shutdown.clone(),
            notifier: self.notifier.clone(),
        }
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stop: Pin<&mut WaitForCancellationFutureOwned>,
    ) -> Poll<Next<E>> {
        if stop.poll(cx).is_ready() {
            return Poll::Ready(Next::Stop);
        }
        let start = self.rotation;
        self.rotation = (start + 1) % 4;
        (start..start + 4)
            .find_map(|branch| match branch % 4 {
                0 => match self.system.poll_recv(cx) {
                    Poll::Ready(Some(ev)) => Some(Next::Msg(Msg::System(ev))),
                    _ => None,
                },
                1 => poll_queue(&mut self.lossless, cx).map(Next::Msg),
                2 => poll_queue(&mut self.drop_newest, cx).map(Next::Msg),
                _ => match self.wake.poll_recv(cx) {
                    Poll::Ready(Some(())) => Some(Next::Wake),
                    _ => None,
                },
            })
            .map_or(Poll::Pending, Poll::Ready)
    }

    fn try_next_queued(&mut self) -> Option<Msg<E>> {
        self.system.try_recv().ok().map(Msg::System).or_else(|| {
            [&mut self.lossless, &mut self.drop_newest]
                .into_iter()
                .flatten()
                .find_map(|rx| rx.try_recv().ok())
        })
    }

    fn side(&mut self) -> impl Iterator<Item = Msg<E>> {
        let signals = &*self.signals;
        let broadcasts = self
            .broadcasts
            .iter_mut()
            .flat_map(move |b| iter::from_fn(move || b.try_recv(signals)).take(SIDE_BUDGET));
        let latest = self
            .watches
            .iter_mut()
            .filter_map(|w| match w.has_changed() {
                Ok(true) => w.borrow_and_update().clone(),
                _ => None,
            })
            .map(Msg::Event);
        signals
            .take()
            .map(Msg::System)
            .chain(broadcasts)
            .chain(latest)
    }

    fn is_empty(&self) -> bool {
        self.system.is_empty()
            && self.lossless.as_ref().is_none_or(|rx| rx.is_empty())
            && self.drop_newest.as_ref().is_none_or(|rx| rx.is_empty())
            && self.broadcasts.iter().all(|b| b.rx.is_empty())
            && self
                .watches
                .iter()
                .all(|w| !w.has_changed().unwrap_or(false))
            && self.wake.is_empty()
            && !self.signals.pending()
    }

    fn set_idle(&self, idle: bool) {
        self.shared.strategies[self.index]
            .idle
            .store(idle, Ordering::Release);
    }
}

impl<E> BroadcastRx<E> {
    fn try_recv(&mut self, signals: &Signals) -> Option<Msg<E>> {
        match self.rx.try_recv() {
            Ok(env) => Some(Msg::Event(env)),
            Err(broadcast::error::TryRecvError::Lagged(count)) => {
                self.dropped.increment(count);
                let source = signals.slots[self.slot].source.clone();
                Some(Msg::System(SystemEvent::Dropped { source, count }))
            }
            Err(_) => None,
        }
    }
}

fn poll_queue<E>(
    queue: &mut Option<mpsc::Receiver<Msg<E>>>,
    cx: &mut Context<'_>,
) -> Option<Msg<E>> {
    let rx = queue.as_mut()?;
    match rx.poll_recv(cx) {
        Poll::Ready(Some(msg)) => Some(msg),
        Poll::Ready(None) => {
            *queue = None;
            None
        }
        Poll::Pending => None,
    }
}

async fn guarded(
    fut: impl Future<Output = Result<(), BoxError>>,
) -> Result<Result<(), BoxError>, String> {
    AssertUnwindSafe(fut)
        .catch_unwind()
        .await
        .map_err(|panic| panic_message(&*panic))
}

impl<E, A, S> Handler<E, A, S>
where
    E: Send + Sync + 'static,
    A: Send + Sync + 'static,
    S: Strategy<E, A>,
{
    async fn init(&mut self) -> Result<Result<(), BoxError>, String> {
        guarded(self.strategy.init(&mut self.ctx)).await
    }

    async fn dispatch(&mut self, msg: Msg<E>) -> Result<(), Failure> {
        match msg {
            Msg::Event(env) => self.on_event(&env).await,
            Msg::System(event) => self.on_system(&event).await,
        }
    }

    async fn on_event(&mut self, env: &Envelope<E>) -> Result<(), Failure> {
        let started = Instant::now();
        let span = tracing::debug_span!("event", source = %env.source, seq = env.seq);
        let result = guarded(self.strategy.on_event(env, &mut self.ctx))
            .instrument(span)
            .await;
        self.handle_time.record(started.elapsed().as_secs_f64());
        self.classify(result)
    }

    async fn on_system(&mut self, event: &SystemEvent) -> Result<(), Failure> {
        tracing::debug!(?event, "system event");
        let result = guarded(self.strategy.on_system(event, &mut self.ctx)).await;
        self.classify(result)
    }

    fn classify(&self, result: Result<Result<(), BoxError>, String>) -> Result<(), Failure> {
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => match self.on_error {
                OnError::LogAndContinue => {
                    tracing::warn!(error = %err, "handler failed");
                    Ok(())
                }
                OnError::Abort => Err(Failure::Error(err.to_string())),
            },
            Err(message) => Err(Failure::Panic(message)),
        }
    }
}

impl<E, A, S, F> Loop<E, A, S, F>
where
    E: Send + Sync + 'static,
    A: Send + Sync + 'static,
    S: Strategy<E, A>,
    F: FnMut() -> S + Send + 'static,
{
    async fn run(&mut self, ready: oneshot::Sender<bool>) {
        let init = self.handler.init().await;
        let init_failure = match init {
            Ok(Ok(())) => None,
            Ok(Err(err)) => Some(ExitReason::StrategyFailed {
                strategy: self.rt.name.to_string(),
                error: err.to_string(),
            }),
            Err(message) => Some(ExitReason::StrategyPanicked {
                strategy: self.rt.name.to_string(),
                message,
            }),
        };
        if let Some(reason) = init_failure {
            tracing::error!(%reason, "init failed");
            let _ = self.rt.fatal.send(reason);
            let _ = ready.send(false);
            return;
        }
        let _ = ready.send(true);

        let mut stop = Box::pin(self.rt.shared.strategies_stop.clone().cancelled_owned());
        loop {
            self.rt.set_idle(self.rt.is_empty());
            let next = std::future::poll_fn(|cx| self.rt.poll_next(cx, stop.as_mut())).await;
            self.rt.set_idle(false);
            let result = match next {
                Next::Stop => break,
                Next::Msg(msg) => self.handler.dispatch(msg).await,
                Next::Wake => self.drain_side().await,
            };
            if let Err(failure) = result
                && !self.recover(failure).await
            {
                return;
            }
        }
        self.shutdown().await;
    }

    async fn shutdown(&mut self) {
        let deadline = self.rt.shared.deadline();
        let drained = tokio::time::timeout_at(deadline, async {
            loop {
                let result = match self.rt.try_next_queued() {
                    Some(msg) => self.handler.dispatch(msg).await,
                    None if !self.rt.is_empty() => {
                        let _ = self.rt.wake.try_recv();
                        self.drain_side().await
                    }
                    None => return true,
                };
                if let Err(failure) = result
                    && !self.recover(failure).await
                {
                    return false;
                }
            }
        })
        .await;
        match drained {
            Ok(false) => return,
            Ok(true) => {}
            Err(_) => tracing::warn!("shutdown timeout reached before the channels were drained"),
        }
        let _ = tokio::time::timeout_at(
            deadline,
            self.handler.on_system(&SystemEvent::ShutdownRequested),
        )
        .await;
        self.rt.set_idle(true);
    }

    async fn drain_side(&mut self) -> Result<(), Failure> {
        for msg in self.rt.side() {
            self.handler.dispatch(msg).await?;
        }
        if self.rt.broadcasts.iter().any(|b| !b.rx.is_empty()) {
            self.rt.signals.wake();
        }
        Ok(())
    }

    async fn announce_restart(&mut self, reason: &str) -> Result<(), Failure> {
        let restarted = SystemEvent::Restarted {
            reason: reason.to_owned(),
        };
        let gaps = self
            .rt
            .signals
            .slots
            .iter()
            .map(|slot| SystemEvent::GapUnfilled {
                source: slot.source.clone(),
                from: None,
                to: None,
            });
        for event in iter::once(restarted).chain(gaps) {
            self.handler.on_system(&event).await?;
        }
        Ok(())
    }

    /// Returns `false` if the strategy loop has to end.
    async fn recover(&mut self, failure: Failure) -> bool {
        let name = self.rt.name.to_string();
        let mut reason = match failure {
            Failure::Error(error) => {
                tracing::error!(%error, "handler failed, aborting");
                let _ = self.rt.fatal.send(ExitReason::StrategyFailed {
                    strategy: name,
                    error,
                });
                return false;
            }
            Failure::Panic(message) => message,
        };
        let (max, within) = match (self.rt.on_panic, self.factory.is_some()) {
            (OnPanic::Restart { max, within }, true) => (max, within),
            _ => {
                tracing::error!(panic = %reason, "strategy panicked, aborting");
                let _ = self.rt.fatal.send(ExitReason::StrategyPanicked {
                    strategy: name,
                    message: reason,
                });
                return false;
            }
        };
        loop {
            let now = Instant::now();
            self.restarts.retain(|t| now.duration_since(*t) < within);
            if self.restarts.len() >= max as usize {
                tracing::error!(panic = %reason, max, ?within, "restart limit exceeded");
                let message = format!("{reason} (more than {max} restarts within {within:?})");
                let _ = self.rt.fatal.send(ExitReason::StrategyPanicked {
                    strategy: name,
                    message,
                });
                return false;
            }
            self.restarts.push_back(now);
            self.restart_counter.increment(1);
            tracing::warn!(panic = %reason, "strategy panicked, rebuilding");
            self.handler.ctx.tasks.token.cancel();
            self.handler.ctx = self.rt.new_ctx();
            self.handler.strategy = (self.factory.as_mut().expect("checked above"))();
            match self.handler.init().await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    let _ = self.rt.fatal.send(ExitReason::StrategyFailed {
                        strategy: name,
                        error: err.to_string(),
                    });
                    return false;
                }
                Err(message) => {
                    reason = message;
                    continue;
                }
            }
            match self.announce_restart(&reason).await {
                Ok(()) => {
                    // the panic may have interrupted a drain and left side work without a wake
                    self.rt.signals.wake();
                    return true;
                }
                Err(Failure::Error(error)) => {
                    let _ = self.rt.fatal.send(ExitReason::StrategyFailed {
                        strategy: name,
                        error,
                    });
                    return false;
                }
                Err(Failure::Panic(message)) => reason = message,
            }
        }
    }
}
