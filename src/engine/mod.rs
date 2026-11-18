mod builder;
pub(crate) mod collector_task;
mod entry;
pub(crate) mod executor_task;
mod handle;
pub(crate) mod router;
mod run;
pub(crate) mod signals;
pub(crate) mod strategy_task;
mod wiring;

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub use self::builder::EngineBuilder;
pub use self::handle::{CollectorHealth, EngineHandle};
pub use self::run::Engine;
use crate::{ExitReason, SourceId};

pub(crate) type Fatal = mpsc::UnboundedSender<ExitReason>;

pub(crate) struct CollectorState {
    pub(crate) name: SourceId,
    pub(crate) connected: AtomicBool,
    pub(crate) done: AtomicBool,
    pub(crate) last_event: AtomicU64,
    exhausted: Box<dyn Fn() -> bool + Send + Sync>,
}

pub(crate) struct StrategyState {
    pub(crate) idle: AtomicBool,
    pub(crate) tasks_active: Arc<AtomicUsize>,
}

pub(crate) struct ExecutorState {
    pub(crate) pending: AtomicUsize,
}

pub(crate) struct Shared {
    started: Instant,
    pub(crate) collectors: Vec<CollectorState>,
    pub(crate) strategies: Vec<StrategyState>,
    pub(crate) executors: Vec<ExecutorState>,
    pub(crate) request: CancellationToken,
    pub(crate) shutdown: CancellationToken,
    pub(crate) collectors_stop: CancellationToken,
    pub(crate) strategies_stop: CancellationToken,
    pub(crate) tasks_stop: CancellationToken,
    pub(crate) executors_stop: CancellationToken,
    pub(crate) tracker: TaskTracker,
    deadline: OnceLock<Instant>,
}

impl Shared {
    pub(crate) fn millis_since_start(&self) -> u64 {
        self.started.elapsed().as_millis() as u64 + 1
    }

    pub(crate) fn deadline(&self) -> Instant {
        self.deadline.get().copied().unwrap_or_else(Instant::now)
    }

    fn is_idle(&self) -> bool {
        self.collectors
            .iter()
            .all(|c| c.done.load(Ordering::Acquire) || (c.exhausted)())
            && self.strategies.iter().all(|s| {
                s.idle.load(Ordering::Acquire) && s.tasks_active.load(Ordering::Acquire) == 0
            })
            && self
                .executors
                .iter()
                .all(|x| x.pending.load(Ordering::Acquire) == 0)
    }
}
