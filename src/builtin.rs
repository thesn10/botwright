use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::{TryStream, TryStreamExt, stream};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Instant, MissedTickBehavior};

use crate::{BoxError, Collector, CollectorError, Executor, Item};

/// Event of an [`IntervalCollector`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tick {
    /// Counter starting at 1.
    pub n: u64,
    /// Scheduled time of this tick.
    pub at: Instant,
}

/// Emits a [`Tick`] at a fixed period, e.g. to drive a reconciler.
///
/// Implements `Collector<E>` for every `E: From<Tick>`. Missed ticks are delayed, not burst.
#[derive(Debug, Clone)]
pub struct IntervalCollector {
    name: String,
    period: Duration,
}

impl IntervalCollector {
    /// Creates a collector ticking every `period`; the first tick fires after one period.
    pub fn new(name: impl Into<String>, period: Duration) -> Self {
        Self {
            name: name.into(),
            period,
        }
    }
}

impl<E> Collector<E> for IntervalCollector
where
    E: From<Tick> + Send + Sync + 'static,
{
    type Cursor = u64;

    fn name(&self) -> &str {
        &self.name
    }

    async fn connect(
        &self,
        resume: Option<u64>,
    ) -> Result<
        impl TryStream<Ok = Item<E, u64>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        let period = self.period;
        let mut interval = tokio::time::interval_at(Instant::now() + period, period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let start = resume.unwrap_or(0);
        Ok(stream::unfold(
            (interval, start),
            |(mut interval, n)| async move {
                let at = interval.tick().await;
                let n = n + 1;
                Some((Ok(Item::new(E::from(Tick { n, at }), n)), (interval, n)))
            },
        ))
    }
}

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

/// Converts the events of another collector, keeping cursors and gap notices.
pub struct MapCollector<C, F, E1> {
    inner: C,
    map: Arc<F>,
    _event: PhantomData<fn(E1)>,
}

impl<C, F, E1> MapCollector<C, F, E1> {
    /// Wraps `inner`; the name stays the same.
    pub fn new(inner: C, map: F) -> Self {
        Self {
            inner,
            map: Arc::new(map),
            _event: PhantomData,
        }
    }
}

impl<C, F, E1, E2> Collector<E2> for MapCollector<C, F, E1>
where
    C: Collector<E1>,
    F: Fn(E1) -> E2 + Send + Sync + 'static,
    E1: Send + 'static,
    E2: Send + Sync + 'static,
{
    type Cursor = C::Cursor;

    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn connect(
        &self,
        resume: Option<C::Cursor>,
    ) -> Result<
        impl TryStream<Ok = Item<E2, C::Cursor>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        let stream = self.inner.connect(resume).await?;
        let map = Arc::clone(&self.map);
        Ok(stream.map_ok(move |item| item.map_event(|e| map(e))))
    }

    fn is_exhausted(&self) -> bool {
        self.inner.is_exhausted()
    }
}

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
