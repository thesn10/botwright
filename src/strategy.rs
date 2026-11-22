use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::FutureExt;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::Instrument;

use crate::engine::router::Router;
use crate::error::panic_message;
use crate::notify::NotifierHandle;
use crate::obs;
use crate::{BoxError, Envelope, SpawnError, SubmitError, SystemEvent};

/// Holds state, processes events one at a time and emits actions.
pub trait Strategy<E, A>: Send + 'static {
    /// Unique name.
    fn name(&self) -> &str;

    /// Runs before the collectors start, and again after a factory restart.
    fn init(
        &mut self,
        _ctx: &mut StrategyCtx<A>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
    }

    /// Handles one event. Long work belongs in [`StrategyCtx::spawn`].
    fn on_event(
        &mut self,
        event: &Envelope<E>,
        ctx: &mut StrategyCtx<A>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send;

    /// Handles an engine notification.
    fn on_system(
        &mut self,
        _event: &SystemEvent,
        _ctx: &mut StrategyCtx<A>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
    }
}

/// What happens when `on_event` or `on_system` returns an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum OnError {
    /// Log, count and continue with the next event.
    #[default]
    LogAndContinue,
    /// End the engine with [`ExitReason::StrategyFailed`](crate::ExitReason::StrategyFailed).
    Abort,
}

/// What happens when a strategy panics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum OnPanic {
    /// End the engine with [`ExitReason::StrategyPanicked`](crate::ExitReason::StrategyPanicked).
    #[default]
    Abort,
    /// Discard the strategy, build a new one with the factory and run `init` again.
    /// More than `max` restarts within `within` end the engine.
    /// Only valid with [`EngineBuilder::strategy_with_factory`](crate::EngineBuilder::strategy_with_factory).
    Restart {
        /// Restarts allowed within the window.
        max: u32,
        /// Sliding window.
        within: Duration,
    },
}

/// Hands actions to the executors. Cheap to clone; usable from opportunity tasks.
pub struct ActionSender<A> {
    router: Arc<Router<A>>,
}

impl<A> Clone for ActionSender<A> {
    fn clone(&self) -> Self {
        Self {
            router: Arc::clone(&self.router),
        }
    }
}

impl<A: Send + Sync + 'static> ActionSender<A> {
    pub(crate) fn new(router: Arc<Router<A>>) -> Self {
        Self { router }
    }

    /// Sends the action to every executor that handles it, waiting while a queue is full.
    pub async fn submit(&self, action: A) -> Result<(), SubmitError> {
        self.router.submit(Arc::new(action)).await
    }

    /// Like [`submit`](Self::submit), but fails instead of waiting. On `Full` no executor got the action.
    pub fn try_submit(&self, action: A) -> Result<(), SubmitError> {
        self.router.try_submit(Arc::new(action))
    }
}

pub(crate) struct TaskSpawner {
    pub(crate) strategy: Arc<str>,
    pub(crate) tracker: TaskTracker,
    pub(crate) token: CancellationToken,
    pub(crate) active: Arc<AtomicUsize>,
    pub(crate) limit: usize,
    pub(crate) gauge: obs::Gauge,
}

/// Context passed to every strategy call.
pub struct StrategyCtx<A> {
    pub(crate) actions: ActionSender<A>,
    pub(crate) tasks: TaskSpawner,
    pub(crate) shutdown: CancellationToken,
    pub(crate) notifier: NotifierHandle,
}

impl<A: Send + Sync + 'static> StrategyCtx<A> {
    /// Name of the strategy.
    pub fn name(&self) -> &str {
        &self.tasks.strategy
    }

    /// See [`ActionSender::submit`].
    pub async fn submit(&self, action: A) -> Result<(), SubmitError> {
        self.actions.submit(action).await
    }

    /// See [`ActionSender::try_submit`].
    pub fn try_submit(&self, action: A) -> Result<(), SubmitError> {
        self.actions.try_submit(action)
    }

    /// A sender for actions, e.g. to move into a task the strategy manages itself.
    pub fn actions(&self) -> ActionSender<A> {
        self.actions.clone()
    }

    /// Starts an opportunity task managed by the engine.
    ///
    /// The task is cancelled at shutdown and when the strategy is restarted. Errors and panics
    /// are logged.
    pub fn spawn<F, Fut>(&self, name: &str, task: F) -> Result<(), SpawnError>
    where
        F: FnOnce(ActionSender<A>) -> Fut,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        let spawner = &self.tasks;
        if spawner.token.is_cancelled() || spawner.tracker.is_closed() {
            return Err(SpawnError::ShuttingDown);
        }
        if spawner.active.fetch_add(1, Ordering::AcqRel) >= spawner.limit {
            spawner.active.fetch_sub(1, Ordering::AcqRel);
            return Err(SpawnError::LimitReached(spawner.limit));
        }
        spawner.gauge.increment(1.0);
        let guard = ActiveGuard {
            active: Arc::clone(&spawner.active),
            gauge: spawner.gauge.clone(),
        };
        let token = spawner.token.clone();
        let fut = task(self.actions.clone());
        let span = tracing::debug_span!("task", strategy = %spawner.strategy, task = name);
        spawner.tracker.spawn(
            async move {
                let _guard = guard;
                tokio::select! {
                    biased;
                    () = token.cancelled() => tracing::debug!("task cancelled"),
                    result = std::panic::AssertUnwindSafe(fut).catch_unwind() => match result {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => tracing::warn!(error = %err, "task failed"),
                        Err(panic) => tracing::error!(panic = %panic_message(&*panic), "task panicked"),
                    },
                }
            }
            .instrument(span),
        );
        Ok(())
    }

    /// Number of running opportunity tasks of this strategy.
    pub fn active_tasks(&self) -> usize {
        self.tasks.active.load(Ordering::Acquire)
    }

    /// Cancelled when the engine starts shutting down.
    pub fn cancellation(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Non-blocking notifications.
    pub fn notifier(&self) -> &NotifierHandle {
        &self.notifier
    }

    /// Current time of the tokio clock, so tests with paused time see virtual time.
    pub fn now(&self) -> Instant {
        Instant::now()
    }
}

struct ActiveGuard {
    active: Arc<AtomicUsize>,
    gauge: obs::Gauge,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        self.gauge.decrement(1.0);
    }
}
