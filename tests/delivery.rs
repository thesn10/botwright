mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use botwright::test_util::{Harness, Pace, ReplayCollector};
use botwright::{
    BoxError, Collector, CollectorError, CollectorOpts, Delivery, Engine, EngineBuilder, Envelope,
    ExitReason, Item, OnFull, Strategy, StrategyCtx, SystemEvent,
};
use common::{Log, Recorder, is_sorted_strictly};
use futures::{TryStream, stream};
use tokio::time::Instant;

#[derive(Clone)]
struct Burst {
    name: &'static str,
    n: u64,
    spacing: Option<Duration>,
    last: Arc<AtomicU64>,
    done_at: Arc<Mutex<Option<Instant>>>,
}

impl Burst {
    fn new(name: &'static str, n: u64) -> Self {
        Self {
            name,
            n,
            spacing: None,
            last: Arc::default(),
            done_at: Arc::default(),
        }
    }

    fn spaced(mut self, spacing: Duration) -> Self {
        self.spacing = Some(spacing);
        self
    }

    fn done_at(&self) -> Option<Instant> {
        *self.done_at.lock().unwrap()
    }
}

impl Collector<u64> for Burst {
    type Cursor = u64;

    fn name(&self) -> &str {
        self.name
    }

    async fn connect(
        &self,
        resume: Option<u64>,
    ) -> Result<
        impl TryStream<Ok = Item<u64, u64>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        let this = self.clone();
        Ok(stream::unfold(resume.unwrap_or(0), move |i| {
            let this = this.clone();
            async move {
                match this.spacing {
                    Some(d) => tokio::time::sleep(d).await,
                    None => tokio::task::yield_now().await,
                }
                if i >= this.n {
                    this.done_at
                        .lock()
                        .unwrap()
                        .get_or_insert_with(Instant::now);
                    return None;
                }
                let v = i + 1;
                this.last.store(v, Ordering::SeqCst);
                Some((Ok(Item::new(v, v)), v))
            }
        }))
    }

    fn is_exhausted(&self) -> bool {
        self.last.load(Ordering::SeqCst) >= self.n
    }
}

fn builder() -> EngineBuilder<u64, u64> {
    Engine::builder()
}

#[test]
fn lossless_delivers_100k_events_in_order_to_a_slow_strategy() {
    const N: u64 = 100_000;
    let log = Log::default();
    let slow = SlowYield { log: log.clone() };
    let report = Harness::new(
        builder()
            .collector(
                ReplayCollector::from_events("src", 1..=N),
                CollectorOpts::default(),
            )
            .strategy(slow, |s| s.from("src", Delivery::Lossless { capacity: 64 })),
    )
    .run();
    assert!(report.completed, "{:?}", report.exit);
    let values = log.values("src");
    assert_eq!(values.len() as u64, N);
    assert!(values.iter().copied().eq(1..=N));
    assert!(log.seqs("src").iter().copied().eq(1..=N));
}

struct SlowYield {
    log: Log,
}

impl Strategy<u64, u64> for SlowYield {
    fn name(&self) -> &str {
        "slow"
    }

    async fn on_event(
        &mut self,
        e: &Envelope<u64>,
        _: &mut StrategyCtx<u64>,
    ) -> Result<(), BoxError> {
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        self.log.push(common::Seen::Event {
            source: e.source.to_string(),
            seq: e.seq,
            value: *e.event,
        });
        Ok(())
    }
}

struct LatestChecker {
    last: Arc<AtomicU64>,
    seen: Arc<Mutex<Vec<(u64, u64)>>>,
}

impl Strategy<u64, u64> for LatestChecker {
    fn name(&self) -> &str {
        "latest"
    }

    async fn on_event(
        &mut self,
        e: &Envelope<u64>,
        _: &mut StrategyCtx<u64>,
    ) -> Result<(), BoxError> {
        self.seen
            .lock()
            .unwrap()
            .push((*e.event, self.last.load(Ordering::SeqCst)));
        tokio::time::sleep(Duration::from_millis(1)).await;
        Ok(())
    }
}

#[test]
fn latest_always_sees_the_newest_value() {
    let burst = Burst::new("src", 5_000).spaced(Duration::from_micros(100));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let checker = LatestChecker {
        last: Arc::clone(&burst.last),
        seen: Arc::clone(&seen),
    };
    let report = Harness::new(
        builder()
            .collector(burst, CollectorOpts::default())
            .strategy(checker, |s| s.from("src", Delivery::Latest)),
    )
    .run();
    assert!(report.completed);
    let seen = seen.lock().unwrap().clone();
    assert!(seen.len() > 100, "{}", seen.len());
    assert!(
        seen.iter().all(|(got, newest)| got == newest),
        "saw a stale value: {seen:?}"
    );
    let values: Vec<u64> = seen.iter().map(|(v, _)| *v).collect();
    assert!(is_sorted_strictly(&values));
    assert_eq!(*values.last().unwrap(), 5_000);
}

#[test]
fn full_channel_reports_lagging() {
    let log = Log::default();
    let report = Harness::new(
        builder()
            .collector(
                ReplayCollector::from_events("src", 1..=100u64),
                CollectorOpts::default(),
            )
            .strategy(
                Recorder::new("slow", &log).delay(Duration::from_millis(10)),
                |s| s.from("src", Delivery::Lossless { capacity: 10 }),
            ),
    )
    .run();
    assert!(report.completed);
    assert!(log.values("src").iter().copied().eq(1..=100));
    let lagging: Vec<_> = log
        .systems()
        .into_iter()
        .filter_map(|e| match e {
            SystemEvent::Lagging { source, depth } => Some((source, depth)),
            _ => None,
        })
        .collect();
    assert!(!lagging.is_empty());
    assert!(
        lagging
            .iter()
            .all(|(source, depth)| source == "src" && *depth >= 8)
    );
}

#[test]
fn on_full_abort_ends_the_engine_after_the_wait() {
    let log = Log::default();
    let report = Harness::new(
        builder()
            .collector(
                ReplayCollector::from_events("src", 1..=100u64),
                CollectorOpts::default(),
            )
            .strategy(
                Recorder::new("stuck", &log).delay(Duration::from_secs(3600)),
                |s| {
                    s.from("src", Delivery::Lossless { capacity: 4 })
                        .on_full(OnFull::Abort {
                            after: Duration::from_millis(100),
                        })
                },
            )
            .shutdown_timeout(Duration::from_secs(1)),
    )
    .run();
    assert!(!report.completed);
    assert_eq!(
        report.exit,
        ExitReason::ChannelAborted {
            source: "src".into(),
            strategy: "stuck".into(),
            waited: Duration::from_millis(100)
        }
    );
    assert!(report.elapsed >= Duration::from_millis(100));
    assert!(
        report.elapsed <= Duration::from_millis(1110),
        "{:?}",
        report.elapsed
    );
}

fn dropping_does_not_slow_others(delivery: Delivery) {
    const N: u64 = 1_000;
    let burst = Burst::new("src", N);
    let slow = Log::default();
    let fast = Log::default();
    let fast_done = Arc::new(Mutex::new(None));
    let started = Instant::now();
    let report = Harness::new(
        builder()
            .collector(burst.clone(), CollectorOpts::default())
            .strategy(
                Recorder::new("slow", &slow).delay(Duration::from_millis(1)),
                |s| s.from("src", delivery),
            )
            .strategy(
                FastTimed {
                    log: fast.clone(),
                    n: N,
                    done: Arc::clone(&fast_done),
                },
                |s| {
                    s.from(
                        "src",
                        Delivery::Lossless {
                            capacity: N as usize,
                        },
                    )
                },
            ),
    )
    .run();
    assert!(report.completed);

    let collector_time = burst.done_at().expect("collector finished") - started;
    let fast_time = fast_done.lock().unwrap().expect("fast strategy finished") - started;
    assert!(
        collector_time < Duration::from_millis(1),
        "collector was delayed: {collector_time:?}"
    );
    assert!(
        fast_time < Duration::from_millis(1),
        "fast strategy was delayed: {fast_time:?}"
    );
    assert!(fast.values("src").iter().copied().eq(1..=N));

    let received = slow.values("src");
    assert!(is_sorted_strictly(&received));
    assert!(received.len() < N as usize, "nothing was dropped");
    assert_eq!(received.len() as u64 + slow.dropped_total(), N);
}

struct FastTimed {
    log: Log,
    n: u64,
    done: Arc<Mutex<Option<Instant>>>,
}

impl Strategy<u64, u64> for FastTimed {
    fn name(&self) -> &str {
        "fast"
    }

    async fn on_event(
        &mut self,
        e: &Envelope<u64>,
        ctx: &mut StrategyCtx<u64>,
    ) -> Result<(), BoxError> {
        self.log.push(common::Seen::Event {
            source: e.source.to_string(),
            seq: e.seq,
            value: *e.event,
        });
        if *e.event == self.n {
            *self.done.lock().unwrap() = Some(ctx.now());
        }
        Ok(())
    }
}

#[test]
fn drop_oldest_is_isolated_and_counted() {
    dropping_does_not_slow_others(Delivery::DropOldest { capacity: 16 });
}

#[test]
fn drop_newest_is_isolated_and_counted() {
    dropping_does_not_slow_others(Delivery::DropNewest { capacity: 16 });
}

struct SpawnsSlowTasks {
    latencies: Arc<Mutex<Vec<Duration>>>,
}

impl Strategy<u64, u64> for SpawnsSlowTasks {
    fn name(&self) -> &str {
        "spawner"
    }

    async fn on_event(
        &mut self,
        e: &Envelope<u64>,
        ctx: &mut StrategyCtx<u64>,
    ) -> Result<(), BoxError> {
        self.latencies
            .lock()
            .unwrap()
            .push(ctx.now() - e.received_at);
        let value = *e.event;
        ctx.spawn("opportunity", move |actions| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            actions.submit(value).await?;
            Ok(())
        })?;
        Ok(())
    }
}

#[test]
fn opportunity_tasks_do_not_block_the_strategy() {
    let latencies = Arc::new(Mutex::new(Vec::new()));
    let events = (0..100u64).map(|i| (Duration::from_millis(10 * i), i));
    let report = Harness::new(
        builder()
            .collector(
                ReplayCollector::from_timed("src", events).pace(Pace::Original),
                CollectorOpts::default(),
            )
            .strategy(
                SpawnsSlowTasks {
                    latencies: Arc::clone(&latencies),
                },
                |s| s.from("src", Delivery::default()),
            ),
    )
    .run();
    assert!(report.completed);
    let latencies = latencies.lock().unwrap();
    assert_eq!(latencies.len(), 100);
    assert!(
        latencies.iter().all(|l| *l < Duration::from_millis(1)),
        "{latencies:?}"
    );
    let mut actions: Vec<u64> = report.actions.iter().map(|a| **a).collect();
    actions.sort_unstable();
    assert!(actions.into_iter().eq(0..100));
    assert!(
        report.elapsed < Duration::from_millis(1300),
        "{:?}",
        report.elapsed
    );
}

#[test]
fn filter_runs_per_subscription() {
    let even = Log::default();
    let all = Log::default();
    let report = Harness::new(
        builder()
            .collector(
                ReplayCollector::from_events("src", 1..=20u64),
                CollectorOpts::default(),
            )
            .strategy(Recorder::new("even", &even), |s| {
                s.from("src", Delivery::DropOldest { capacity: 64 })
                    .filter(|v| v % 2 == 0)
            })
            .strategy(Recorder::new("all", &all), |s| {
                s.from("src", Delivery::DropOldest { capacity: 64 })
            }),
    )
    .run();
    assert!(report.completed);
    assert_eq!(
        even.values("src"),
        (1..=10).map(|v| v * 2).collect::<Vec<_>>()
    );
    assert_eq!(
        even.seqs("src"),
        (1..=10).map(|v| v * 2).collect::<Vec<_>>()
    );
    assert!(all.values("src").iter().copied().eq(1..=20));
}

#[test]
fn unknown_source_is_a_build_error() {
    let log = Log::default();
    let err = builder()
        .strategy(Recorder::new("s", &log), |s| {
            s.from("missing", Delivery::Latest)
        })
        .build()
        .err()
        .unwrap();
    assert!(matches!(err, botwright::BuildError::UnknownSource { .. }));
}
