use std::fmt;
use std::time::Duration;

/// Error type for user code: strategies, executors and notifier sinks.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Error that ends a collector connection and triggers a reconnect.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CollectorError {
    /// Network or transport failure.
    #[error("transport error: {0}")]
    Transport(#[source] BoxError),
    /// The remote side answered with something unusable.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// The stream ended.
    #[error("stream ended")]
    Ended,
    /// Any other failure.
    #[error("{0}")]
    Other(#[source] BoxError),
}

impl CollectorError {
    /// Wraps a transport error.
    pub fn transport(err: impl Into<BoxError>) -> Self {
        Self::Transport(err.into())
    }

    /// Creates a protocol error from a message.
    pub fn protocol(msg: impl Into<String>) -> Self {
        Self::Protocol(msg.into())
    }

    /// Wraps any other error.
    pub fn other(err: impl Into<BoxError>) -> Self {
        Self::Other(err.into())
    }
}

/// Failure to hand an action to the executors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SubmitError {
    /// No executor returned `true` from `handles`.
    #[error("no executor handles the action")]
    Unrouted,
    /// The queue of an executor is full (only from `try_submit`).
    #[error("queue of executor `{0}` is full")]
    Full(String),
    /// The executor no longer accepts actions because the engine is shutting down.
    #[error("executor `{0}` is stopped")]
    Closed(String),
}

/// Failure to start an opportunity task.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SpawnError {
    /// The strategy already runs its maximum number of tasks.
    #[error("task limit of {0} reached")]
    LimitReached(usize),
    /// The engine is shutting down.
    #[error("engine is shutting down")]
    ShuttingDown,
}

/// Invalid engine configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BuildError {
    /// Two components of the same kind share a name.
    #[error("duplicate {kind} name `{name}`")]
    DuplicateName {
        /// `collector`, `strategy` or `executor`.
        kind: &'static str,
        /// The duplicated name.
        name: String,
    },
    /// A strategy subscribes to a collector that is not registered.
    #[error("strategy `{strategy}` subscribes to unknown collector `{source_name}`")]
    UnknownSource {
        /// Subscribing strategy.
        strategy: String,
        /// Requested collector.
        source_name: String,
    },
    /// `OnPanic::Restart` needs `strategy_with_factory`.
    #[error("strategy `{0}` uses OnPanic::Restart without a factory")]
    RestartWithoutFactory(String),
    /// A channel capacity of zero was configured.
    #[error("strategy `{strategy}`: capacity for `{source_name}` must be greater than zero")]
    ZeroCapacity {
        /// Subscribing strategy.
        strategy: String,
        /// Affected collector.
        source_name: String,
    },
    /// `filter` or `on_full` was called before any `from`.
    #[error("strategy `{0}`: `{1}` needs a preceding `from`")]
    MisplacedOption(String, &'static str),
}

/// Why [`Engine::run`](crate::Engine::run) returned.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExitReason {
    /// Orderly shutdown by signal or [`EngineHandle::shutdown`](crate::EngineHandle::shutdown).
    Shutdown,
    /// A collector exhausted its backoff retries.
    CollectorFailed {
        /// Collector name.
        collector: String,
        /// Last error.
        error: String,
    },
    /// A strategy failed in `init` or, with `OnError::Abort`, in `on_event`/`on_system`.
    StrategyFailed {
        /// Strategy name.
        strategy: String,
        /// The error.
        error: String,
    },
    /// A strategy panicked under `OnPanic::Abort` or exceeded its restart budget.
    StrategyPanicked {
        /// Strategy name.
        strategy: String,
        /// Panic message.
        message: String,
    },
    /// A collector waited longer than `OnFull::Abort { after }` for a full channel.
    ChannelAborted {
        /// Collector name.
        source: String,
        /// Strategy whose channel was full.
        strategy: String,
        /// How long the collector waited.
        waited: Duration,
    },
}

impl ExitReason {
    /// Whether this is an orderly shutdown.
    pub fn is_shutdown(&self) -> bool {
        matches!(self, Self::Shutdown)
    }
}

impl fmt::Display for ExitReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shutdown => f.write_str("shutdown"),
            Self::CollectorFailed { collector, error } => {
                write!(f, "collector `{collector}` failed: {error}")
            }
            Self::StrategyFailed { strategy, error } => {
                write!(f, "strategy `{strategy}` failed: {error}")
            }
            Self::StrategyPanicked { strategy, message } => {
                write!(f, "strategy `{strategy}` panicked: {message}")
            }
            Self::ChannelAborted {
                source,
                strategy,
                waited,
            } => write!(
                f,
                "collector `{source}` waited {waited:?} for the full channel of strategy `{strategy}`"
            ),
        }
    }
}

pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}
