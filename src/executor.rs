use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::BoxError;

/// Executes actions.
pub trait Executor<A>: Send + Sync + 'static {
    /// Unique name.
    fn name(&self) -> &str;

    /// Whether this executor wants the action. The router sends each action to every executor
    /// returning `true`.
    fn handles(&self, _action: &A) -> bool {
        true
    }

    /// Executes one action. Up to `concurrency` calls run at the same time.
    fn execute(&self, action: Arc<A>) -> impl Future<Output = Result<(), BoxError>> + Send;
}

/// Per-executor settings.
#[derive(Debug, Clone)]
pub struct ExecutorOpts {
    pub(crate) concurrency: usize,
    pub(crate) timeout: Option<Duration>,
    pub(crate) queue_capacity: usize,
}

impl Default for ExecutorOpts {
    /// Concurrency 1, no timeout, queue of 1024 actions.
    fn default() -> Self {
        Self {
            concurrency: 1,
            timeout: None,
            queue_capacity: 1024,
        }
    }
}

impl ExecutorOpts {
    /// Maximum number of concurrent `execute` calls, at least 1.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Cancels an `execute` call after this duration.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Capacity of the action queue, at least 1.
    pub fn queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity.max(1);
        self
    }
}
