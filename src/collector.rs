use std::future::Future;

use futures::TryStream;

use crate::{Backoff, CollectorError, Item};

/// A source of events.
///
/// The engine calls [`connect`](Collector::connect) in a reconnect loop. An `Err` in the stream or
/// the end of the stream ends the connection; the engine reconnects with backoff and passes the
/// last cursor it saw. Errors of single elements that do not affect the connection must be
/// handled inside the collector.
pub trait Collector<E>: Send + Sync + 'static {
    /// Resume position, e.g. block number and log index.
    type Cursor: Clone + Send + Sync + std::fmt::Debug + 'static;

    /// Unique name; becomes the [`SourceId`](crate::SourceId) of all events.
    fn name(&self) -> &str;

    /// Opens a stream starting after `resume`, backfilling anything missed since then.
    fn connect(
        &self,
        resume: Option<Self::Cursor>,
    ) -> impl Future<
        Output = Result<
            impl TryStream<Ok = Item<E, Self::Cursor>, Error = CollectorError> + Send + 'static,
            CollectorError,
        >,
    > + Send;

    /// Whether the source will produce nothing further on its own.
    ///
    /// When the stream of an exhausted collector ends, the engine stops the collector instead of
    /// reconnecting. The test harness treats exhausted collectors as idle.
    fn is_exhausted(&self) -> bool {
        false
    }
}

/// Per-collector settings.
#[derive(Debug, Clone, Default)]
pub struct CollectorOpts {
    pub(crate) backoff: Backoff,
}

impl CollectorOpts {
    /// Reconnect policy.
    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }
}
