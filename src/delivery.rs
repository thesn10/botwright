use std::sync::Arc;
use std::time::Duration;

use crate::{BuildError, OnError, OnPanic, SourceId};

/// How events of one subscription reach the strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// Never drops; a full channel makes the collector wait.
    Lossless {
        /// Channel capacity; the strategy's lossless channel uses the largest value.
        capacity: usize,
    },
    /// Drops the new event when the channel is full and reports `Dropped`.
    DropNewest {
        /// Channel capacity; the strategy's drop-newest channel uses the largest value.
        capacity: usize,
    },
    /// Drops the oldest event when the channel is full and reports `Dropped`.
    DropOldest {
        /// Ring buffer capacity, shared by all unfiltered drop-oldest subscribers of the collector.
        capacity: usize,
    },
    /// Only the most recent event is kept.
    Latest,
}

impl Default for Delivery {
    /// `Lossless { capacity: 1024 }`.
    fn default() -> Self {
        Self::Lossless { capacity: 1024 }
    }
}

impl Delivery {
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Lossless { .. } => "lossless",
            Self::DropNewest { .. } => "drop_newest",
            Self::DropOldest { .. } => "drop_oldest",
            Self::Latest => "latest",
        }
    }

    pub(crate) fn capacity(&self) -> Option<usize> {
        match *self {
            Self::Lossless { capacity }
            | Self::DropNewest { capacity }
            | Self::DropOldest { capacity } => Some(capacity),
            Self::Latest => None,
        }
    }
}

/// Behaviour of a `Lossless` subscription whose channel stays full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnFull {
    /// Wait as long as needed.
    #[default]
    Wait,
    /// End the engine with [`ExitReason::ChannelAborted`](crate::ExitReason::ChannelAborted)
    /// once the collector waited longer than `after`.
    Abort {
        /// Maximum wait.
        after: Duration,
    },
}

pub(crate) type Filter<E> = Arc<dyn Fn(&E) -> bool + Send + Sync>;

pub(crate) struct Subscription<E> {
    pub(crate) source: SourceId,
    pub(crate) delivery: Delivery,
    pub(crate) filter: Option<Filter<E>>,
    pub(crate) on_full: OnFull,
}

/// Subscriptions and options of one strategy, configured in the closure passed to
/// [`EngineBuilder::strategy`](crate::EngineBuilder::strategy).
pub struct Subscriptions<E> {
    pub(crate) subs: Vec<Subscription<E>>,
    pub(crate) on_error: OnError,
    pub(crate) on_panic: OnPanic,
    pub(crate) max_tasks: usize,
    pub(crate) misuse: Option<&'static str>,
}

impl<E> Subscriptions<E> {
    pub(crate) fn new() -> Self {
        Self {
            subs: Vec::new(),
            on_error: OnError::default(),
            on_panic: OnPanic::default(),
            max_tasks: 1024,
            misuse: None,
        }
    }

    /// Subscribes to a collector.
    pub fn from(mut self, source: impl Into<SourceId>, delivery: Delivery) -> Self {
        self.subs.push(Subscription {
            source: source.into(),
            delivery,
            filter: None,
            on_full: OnFull::Wait,
        });
        self
    }

    /// Filters the preceding subscription. Runs in the collector task before sending.
    pub fn filter(mut self, filter: impl Fn(&E) -> bool + Send + Sync + 'static) -> Self {
        match self.subs.last_mut() {
            Some(sub) => sub.filter = Some(Arc::new(filter)),
            None => self.misuse = Some("filter"),
        }
        self
    }

    /// Full-channel behaviour of the preceding subscription; only effective for `Lossless`.
    pub fn on_full(mut self, on_full: OnFull) -> Self {
        match self.subs.last_mut() {
            Some(sub) => sub.on_full = on_full,
            None => self.misuse = Some("on_full"),
        }
        self
    }

    /// Error policy of the strategy, default [`OnError::LogAndContinue`].
    pub fn on_error(mut self, on_error: OnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// Panic policy of the strategy, default [`OnPanic::Abort`].
    pub fn on_panic(mut self, on_panic: OnPanic) -> Self {
        self.on_panic = on_panic;
        self
    }

    /// Maximum number of concurrent opportunity tasks, default 1024.
    pub fn max_tasks(mut self, max: usize) -> Self {
        self.max_tasks = max;
        self
    }

    pub(crate) fn validate(&self, strategy: &str) -> Result<(), BuildError> {
        if let Some(option) = self.misuse {
            return Err(BuildError::MisplacedOption(strategy.to_owned(), option));
        }
        for sub in &self.subs {
            if sub.delivery.capacity() == Some(0) {
                return Err(BuildError::ZeroCapacity {
                    strategy: strategy.to_owned(),
                    source_name: sub.source.to_string(),
                });
            }
        }
        Ok(())
    }
}
