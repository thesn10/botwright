use std::collections::VecDeque;
use std::future::Future;
use std::iter;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use futures::{FutureExt, Stream, StreamExt};
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
}

/// Every channel a strategy reads, as one stream that ends when the engine stops strategies.
struct Inbox<E> {
    index: usize,
    lossless: Option<mpsc::Receiver<Msg<E>>>,
    drop_newest: Option<mpsc::Receiver<Msg<E>>>,
    broadcasts: Vec<BroadcastRx<E>>,
    watches: Vec<watch::Receiver<Option<Envelope<E>>>>,
    system: mpsc::UnboundedReceiver<SystemEvent>,
    wake: mpsc::Receiver<()>,
    signals: Arc<Signals>,
    shared: Arc<Shared>,
    stop: Pin<Box<WaitForCancellationFutureOwned>>,
    rotation: usize,
    side: Option<Side>,
}

/// Position in the side drain: signal slots, then broadcasts (with budget), then watches.
#[derive(Clone, Copy)]
enum Side {
    Dropped(usize),
    Lagging(usize),
    Broadcast(usize, usize),
    Watch(usize),
}

struct Env<A> {
    index: usize,
    name: Arc<str>,
    on_panic: OnPanic,
    max_tasks: usize,
    router: Arc<Router<A>>,
    notifier: NotifierHandle,
    shared: Arc<Shared>,
    fatal: Fatal,
    signals: Arc<Signals>,
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
    env: Env<A>,
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
    let on_error = rt.on_error;
    let (inbox, env) = rt.split();
    let label = [("strategy", &*env.name)];
    let handler = Handler {
        strategy: first,
        ctx: env.new_ctx(),
        on_error,
        handle_time: obs::histogram(obs::EVENT_HANDLE, &label),
        _event: PhantomData,
    };
    let mut this = Loop {
        restart_counter: obs::counter(obs::RESTARTS, &label),
        env,
        handler,
        factory,
        restarts: VecDeque::new(),
    };
    this.run(inbox, ready).instrument(span).await;
}

impl<E, A> StrategyRuntime<E, A> {
    fn split(self) -> (Inbox<E>, Env<A>) {
        let inbox = Inbox {
            index: self.index,
            lossless: self.lossless,
            drop_newest: self.drop_newest,
            broadcasts: self.broadcasts,
            watches: self.watches,
            system: self.system,
            wake: self.wake,
            signals: Arc::clone(&self.signals),
            shared: Arc::clone(&self.shared),
            stop: Box::pin(self.shared.strategies_stop.clone().cancelled_owned()),
            rotation: 0,
            side: None,
        };
        let env = Env {
            index: self.index,
            name: self.name,
            on_panic: self.on_panic,
            max_tasks: self.max_tasks,
            router: self.router,
            notifier: self.notifier,
            shared: self.shared,
            fatal: self.fatal,
            signals: self.signals,
        };
        (inbox, env)
    }
}

impl<A: Send + Sync + 'static> Env<A> {
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
}

impl<E> Stream for Inbox<E> {
    type Item = Msg<E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Msg<E>>> {
        let this = self.get_mut();
        loop {
            if let Some(msg) = this.next_side() {
                this.set_idle(false);
                return Poll::Ready(Some(msg));
            }
            match this.poll_queues(cx) {
                Poll::Pending => {
                    this.set_idle(this.is_empty());
                    return Poll::Pending;
                }
                Poll::Ready(Next::Stop) => return Poll::Ready(None),
                Poll::Ready(Next::Msg(msg)) => {
                    this.set_idle(false);
                    return Poll::Ready(Some(msg));
                }
                Poll::Ready(Next::Wake) => this.side = Some(Side::Dropped(0)),
            }
        }
    }
}

impl<E> Inbox<E> {
    fn poll_queues(&mut self, cx: &mut Context<'_>) -> Poll<Next<E>> {
        if self.stop.as_mut().poll(cx).is_ready() {
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

    /// What is still queued once strategies are stopped, without waiting for more.
    fn remaining(&mut self) -> impl Iterator<Item = Msg<E>> + '_ {
        iter::from_fn(|| {
            loop {
                if let Some(msg) = self.next_side().or_else(|| self.try_next_queued()) {
                    return Some(msg);
                }
                if self.is_empty() {
                    return None;
                }
                let _ = self.wake.try_recv();
                self.side = Some(Side::Dropped(0));
            }
        })
    }

    fn try_next_queued(&mut self) -> Option<Msg<E>> {
        self.system.try_recv().ok().map(Msg::System).or_else(|| {
            [&mut self.lossless, &mut self.drop_newest]
                .into_iter()
                .flatten()
                .find_map(|rx| rx.try_recv().ok())
        })
    }

    fn next_side(&mut self) -> Option<Msg<E>> {
        while let Some(step) = self.side {
            let (msg, next) = self.side_step(step);
            self.side = next;
            if msg.is_some() {
                return msg;
            }
        }
        None
    }

    fn side_step(&mut self, step: Side) -> (Option<Msg<E>>, Option<Side>) {
        match step {
            Side::Dropped(slot) if slot < self.signals.slots.len() => (
                self.signals.take_dropped(slot).map(Msg::System),
                Some(Side::Lagging(slot)),
            ),
            Side::Dropped(_) => (None, Some(Side::Broadcast(0, SIDE_BUDGET))),
            Side::Lagging(slot) => (
                self.signals.take_lagging(slot).map(Msg::System),
                Some(Side::Dropped(slot + 1)),
            ),
            Side::Broadcast(index, budget) if index < self.broadcasts.len() => {
                let msg = (budget > 0)
                    .then(|| self.broadcasts[index].try_recv(&self.signals))
                    .flatten();
                let next = match msg {
                    Some(_) => Side::Broadcast(index, budget - 1),
                    None => Side::Broadcast(index + 1, SIDE_BUDGET),
                };
                (msg, Some(next))
            }
            Side::Broadcast(..) => (None, Some(Side::Watch(0))),
            Side::Watch(index) if index < self.watches.len() => {
                (self.latest(index), Some(Side::Watch(index + 1)))
            }
            Side::Watch(_) => {
                if self.broadcasts.iter().any(|b| !b.rx.is_empty()) {
                    self.signals.wake();
                }
                (None, None)
            }
        }
    }

    fn latest(&mut self, index: usize) -> Option<Msg<E>> {
        let watch = &mut self.watches[index];
        match watch.has_changed() {
            Ok(true) => watch.borrow_and_update().clone().map(Msg::Event),
            _ => None,
        }
    }

    fn is_empty(&self) -> bool {
        self.side.is_none()
            && self.system.is_empty()
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
    async fn run(&mut self, mut inbox: Inbox<E>, ready: oneshot::Sender<bool>) {
        let init = self.handler.init().await;
        let init_failure = match init {
            Ok(Ok(())) => None,
            Ok(Err(err)) => Some(ExitReason::StrategyFailed {
                strategy: self.env.name.to_string(),
                error: err.to_string(),
            }),
            Err(message) => Some(ExitReason::StrategyPanicked {
                strategy: self.env.name.to_string(),
                message,
            }),
        };
        if let Some(reason) = init_failure {
            tracing::error!(%reason, "init failed");
            let _ = self.env.fatal.send(reason);
            let _ = ready.send(false);
            return;
        }
        let _ = ready.send(true);

        while let Some(msg) = inbox.next().await {
            if !self.handle(msg).await {
                return;
            }
        }
        self.shutdown(&mut inbox).await;
    }

    async fn shutdown(&mut self, inbox: &mut Inbox<E>) {
        let deadline = self.env.shared.deadline();
        let drained = tokio::time::timeout_at(deadline, async {
            for msg in inbox.remaining() {
                if !self.handle(msg).await {
                    return false;
                }
            }
            true
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
        inbox.set_idle(true);
    }

    /// Returns `false` if the strategy loop has to end.
    async fn handle(&mut self, msg: Msg<E>) -> bool {
        match self.handler.dispatch(msg).await {
            Ok(()) => true,
            Err(failure) => self.recover(failure).await,
        }
    }

    async fn announce_restart(&mut self, reason: &str) -> Result<(), Failure> {
        let restarted = SystemEvent::Restarted {
            reason: reason.to_owned(),
        };
        let gaps = self
            .env
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
        let name = self.env.name.to_string();
        let mut reason = match failure {
            Failure::Error(error) => {
                tracing::error!(%error, "handler failed, aborting");
                let _ = self.env.fatal.send(ExitReason::StrategyFailed {
                    strategy: name,
                    error,
                });
                return false;
            }
            Failure::Panic(message) => message,
        };
        let (max, within) = match (self.env.on_panic, self.factory.is_some()) {
            (OnPanic::Restart { max, within }, true) => (max, within),
            _ => {
                tracing::error!(panic = %reason, "strategy panicked, aborting");
                let _ = self.env.fatal.send(ExitReason::StrategyPanicked {
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
                let _ = self.env.fatal.send(ExitReason::StrategyPanicked {
                    strategy: name,
                    message,
                });
                return false;
            }
            self.restarts.push_back(now);
            self.restart_counter.increment(1);
            tracing::warn!(panic = %reason, "strategy panicked, rebuilding");
            self.handler.ctx.tasks.token.cancel();
            self.handler.ctx = self.env.new_ctx();
            self.handler.strategy = (self.factory.as_mut().expect("checked above"))();
            match self.handler.init().await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    let _ = self.env.fatal.send(ExitReason::StrategyFailed {
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
                Ok(()) => return true,
                Err(Failure::Error(error)) => {
                    let _ = self.env.fatal.send(ExitReason::StrategyFailed {
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
