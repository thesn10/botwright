use std::time::Duration;

use futures::{TryStream, stream};
use tokio::time::{Instant, MissedTickBehavior};

use crate::{Collector, CollectorError, Item};

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
