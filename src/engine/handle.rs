use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::Shared;

/// Health of one collector.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CollectorHealth {
    /// Collector name.
    pub name: String,
    /// Whether its stream is currently established.
    pub connected: bool,
    /// Whether the collector is exhausted and stopped.
    pub finished: bool,
    /// Time since its last event, `None` before the first event.
    pub last_event_age: Option<Duration>,
}

/// Controls a running engine. Cheap to clone.
#[derive(Clone)]
pub struct EngineHandle {
    shared: Arc<Shared>,
}

impl EngineHandle {
    pub(super) fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }

    /// Starts an orderly shutdown; [`Engine::run`](crate::Engine::run) then returns
    /// [`ExitReason::Shutdown`](crate::ExitReason::Shutdown).
    pub fn shutdown(&self) {
        self.shared.request.cancel();
    }

    /// Whether shutdown has started.
    pub fn is_shutting_down(&self) -> bool {
        self.shared.shutdown.is_cancelled()
    }

    /// True when every collector is exhausted, no strategy has queued events or running tasks
    /// and no action is queued or executing.
    pub fn is_idle(&self) -> bool {
        self.shared.is_idle()
    }

    /// Time since the engine was built.
    pub fn uptime(&self) -> Duration {
        self.shared.started.elapsed()
    }

    /// Connection state and event age of every collector.
    pub fn health(&self) -> Vec<CollectorHealth> {
        let now = self.shared.millis_since_start();
        self.shared
            .collectors
            .iter()
            .map(|c| {
                let last = c.last_event.load(Ordering::Acquire);
                CollectorHealth {
                    name: c.name.to_string(),
                    connected: c.connected.load(Ordering::Acquire),
                    finished: c.done.load(Ordering::Acquire),
                    last_event_age: (last > 0)
                        .then(|| Duration::from_millis(now.saturating_sub(last))),
                }
            })
            .collect()
    }
}
