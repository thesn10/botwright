use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{TryStream, stream};
use tokio::sync::{Mutex, mpsc};

use crate::{Collector, CollectorError, Item};

/// Feeds events from outside the engine, e.g. executor results back to strategies.
pub struct ChannelCollector<E> {
    name: String,
    rx: Arc<Mutex<mpsc::Receiver<E>>>,
    queued: Arc<AtomicUsize>,
}

/// Sending half of a [`ChannelCollector`]. Cheap to clone.
pub struct EventSender<E> {
    tx: mpsc::Sender<E>,
    queued: Arc<AtomicUsize>,
}

impl<E> Clone for EventSender<E> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            queued: Arc::clone(&self.queued),
        }
    }
}

impl<E> Debug for EventSender<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventSender")
            .field("queued", &self.queued.load(Ordering::Relaxed))
            .finish()
    }
}

/// The collector side of a [`ChannelCollector`] is gone.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("channel collector is closed")]
pub struct ChannelClosed;

/// Failure of [`EventSender::try_send`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TrySendError {
    /// The queue is full.
    #[error("channel collector queue is full")]
    Full,
    /// The collector side is gone.
    #[error("channel collector is closed")]
    Closed,
}

impl<E: Send + 'static> EventSender<E> {
    /// Queues an event, waiting while the queue is full.
    pub async fn send(&self, event: E) -> Result<(), ChannelClosed> {
        self.queued.fetch_add(1, Ordering::AcqRel);
        self.tx.send(event).await.map_err(|_| {
            self.queued.fetch_sub(1, Ordering::AcqRel);
            ChannelClosed
        })
    }

    /// Queues an event without waiting.
    pub fn try_send(&self, event: E) -> Result<(), TrySendError> {
        self.queued.fetch_add(1, Ordering::AcqRel);
        self.tx.try_send(event).map_err(|err| {
            self.queued.fetch_sub(1, Ordering::AcqRel);
            match err {
                mpsc::error::TrySendError::Full(_) => TrySendError::Full,
                mpsc::error::TrySendError::Closed(_) => TrySendError::Closed,
            }
        })
    }
}

impl<E: Send + 'static> ChannelCollector<E> {
    /// Creates the collector and its sender with a queue of `capacity` events.
    pub fn new(name: impl Into<String>, capacity: usize) -> (Self, EventSender<E>) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let queued = Arc::new(AtomicUsize::new(0));
        let sender = EventSender {
            tx,
            queued: Arc::clone(&queued),
        };
        (
            Self {
                name: name.into(),
                rx: Arc::new(Mutex::new(rx)),
                queued,
            },
            sender,
        )
    }
}

impl<E: Send + Sync + 'static> Collector<E> for ChannelCollector<E> {
    type Cursor = ();

    fn name(&self) -> &str {
        &self.name
    }

    async fn connect(
        &self,
        _resume: Option<()>,
    ) -> Result<
        impl TryStream<Ok = Item<E, ()>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        let rx = Arc::clone(&self.rx).lock_owned().await;
        let queued = Arc::clone(&self.queued);
        Ok(stream::unfold(
            (rx, queued),
            |(mut rx, queued)| async move {
                let event = rx.recv().await?;
                queued.fetch_sub(1, Ordering::AcqRel);
                Some((Ok(Item::event(event)), (rx, queued)))
            },
        ))
    }

    /// Exhausted while nothing is queued; the stream only ends once every sender is dropped.
    fn is_exhausted(&self) -> bool {
        self.queued.load(Ordering::Acquire) == 0
    }
}
