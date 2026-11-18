use std::fmt::Debug;
use std::sync::Arc;

use crate::{BoxError, Executor};

/// Prints every action with `Debug` to stdout.
#[derive(Debug, Clone)]
pub struct PrintExecutor {
    name: String,
}

impl PrintExecutor {
    /// Creates a print executor.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for PrintExecutor {
    fn default() -> Self {
        Self::new("print")
    }
}

impl<A: Debug + Send + Sync + 'static> Executor<A> for PrintExecutor {
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(&self, action: Arc<A>) -> Result<(), BoxError> {
        println!("[{}] {action:?}", self.name);
        Ok(())
    }
}

/// Accepts and discards every action, e.g. for shadow mode.
#[derive(Debug, Clone)]
pub struct NoopExecutor {
    name: String,
}

impl NoopExecutor {
    /// Creates a no-op executor.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for NoopExecutor {
    fn default() -> Self {
        Self::new("noop")
    }
}

impl<A: Send + Sync + 'static> Executor<A> for NoopExecutor {
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(&self, _action: Arc<A>) -> Result<(), BoxError> {
        Ok(())
    }
}
