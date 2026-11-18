mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use botwright::test_util::{Harness, ReplayCollector};
use botwright::{
    BoxError, CollectorOpts, Delivery, Engine, Envelope, Executor, ExecutorOpts, Strategy,
    StrategyCtx, SubmitError,
};
use common::{Log, Recorder};

#[derive(Default, Clone)]
struct Gauge {
    current: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
    done: Arc<AtomicUsize>,
}

struct Sleepy {
    gauge: Gauge,
    work: Duration,
}

impl Executor<u64> for Sleepy {
    fn name(&self) -> &str {
        "sleepy"
    }

    async fn execute(&self, _: Arc<u64>) -> Result<(), BoxError> {
        let now = self.gauge.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.gauge.max.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(self.work).await;
        self.gauge.current.fetch_sub(1, Ordering::SeqCst);
        self.gauge.done.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn concurrency_limit_is_respected() {
    let gauge = Gauge::default();
    let log = Log::default();
    let report = Harness::new(
        Engine::<u64, u64>::builder()
            .collector(
                ReplayCollector::from_events("src", 1..=20u64),
                CollectorOpts::default(),
            )
            .strategy(Recorder::new("s", &log).submitting(), |s| {
                s.from("src", Delivery::default())
            })
            .executor(
                Sleepy {
                    gauge: gauge.clone(),
                    work: Duration::from_millis(100),
                },
                ExecutorOpts::default().concurrency(3),
            ),
    )
    .run();
    assert!(report.completed);
    assert_eq!(gauge.max.load(Ordering::SeqCst), 3);
    assert_eq!(gauge.done.load(Ordering::SeqCst), 20);
    assert!(
        report.elapsed >= Duration::from_millis(700),
        "{:?}",
        report.elapsed
    );
}

struct Cancelled(Arc<AtomicBool>);

impl Drop for Cancelled {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct Hangs {
    dropped: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
}

impl Executor<u64> for Hangs {
    fn name(&self) -> &str {
        "hangs"
    }

    async fn execute(&self, _: Arc<u64>) -> Result<(), BoxError> {
        let _guard = Cancelled(Arc::clone(&self.dropped));
        tokio::time::sleep(Duration::from_secs(10)).await;
        self.finished.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn timeout_cancels_the_execution() {
    let dropped = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let log = Log::default();
    let report = Harness::new(
        Engine::<u64, u64>::builder()
            .collector(
                ReplayCollector::from_events("src", [1u64]),
                CollectorOpts::default(),
            )
            .strategy(Recorder::new("s", &log).submitting(), |s| {
                s.from("src", Delivery::default())
            })
            .executor(
                Hangs {
                    dropped: Arc::clone(&dropped),
                    finished: Arc::clone(&finished),
                },
                ExecutorOpts::default().timeout(Duration::from_millis(50)),
            ),
    )
    .run();
    assert!(report.completed);
    assert!(dropped.load(Ordering::SeqCst));
    assert!(!finished.load(Ordering::SeqCst));
    assert!(
        report.elapsed < Duration::from_millis(100),
        "{:?}",
        report.elapsed
    );
}

#[derive(Debug, Clone, PartialEq)]
enum Action {
    Swap(u64),
    Alert(u64),
    Nobody,
}

struct Typed {
    name: &'static str,
    wants: fn(&Action) -> bool,
    got: Arc<Mutex<Vec<Action>>>,
}

impl Executor<Action> for Typed {
    fn name(&self) -> &str {
        self.name
    }

    fn handles(&self, action: &Action) -> bool {
        (self.wants)(action)
    }

    async fn execute(&self, action: Arc<Action>) -> Result<(), BoxError> {
        self.got.lock().unwrap().push((*action).clone());
        Ok(())
    }
}

struct Router {
    unrouted: Arc<Mutex<Vec<SubmitError>>>,
}

impl Strategy<u64, Action> for Router {
    fn name(&self) -> &str {
        "router"
    }

    async fn on_event(
        &mut self,
        e: &Envelope<u64>,
        ctx: &mut StrategyCtx<Action>,
    ) -> Result<(), BoxError> {
        let v = *e.event;
        let action = match v % 3 {
            0 => Action::Swap(v),
            1 => Action::Alert(v),
            _ => Action::Nobody,
        };
        let result = if v.is_multiple_of(2) {
            ctx.submit(action).await
        } else {
            ctx.try_submit(action)
        };
        if let Err(err) = result {
            self.unrouted.lock().unwrap().push(err);
        }
        Ok(())
    }
}

#[test]
fn handles_routes_actions() {
    let swaps = Arc::new(Mutex::new(Vec::new()));
    let alerts = Arc::new(Mutex::new(Vec::new()));
    let unrouted = Arc::new(Mutex::new(Vec::new()));
    let engine = Engine::<u64, Action>::builder()
        .collector(
            ReplayCollector::from_events("src", 1..=9u64),
            CollectorOpts::default(),
        )
        .strategy(
            Router {
                unrouted: Arc::clone(&unrouted),
            },
            |s| s.from("src", Delivery::default()),
        )
        .executor(
            Typed {
                name: "swap",
                wants: |a| matches!(a, Action::Swap(_)),
                got: Arc::clone(&swaps),
            },
            ExecutorOpts::default(),
        )
        .executor(
            Typed {
                name: "alert",
                wants: |a| matches!(a, Action::Alert(_)),
                got: Arc::clone(&alerts),
            },
            ExecutorOpts::default(),
        );
    let report = Harness::new(engine).run();
    assert!(report.completed);
    assert_eq!(
        *swaps.lock().unwrap(),
        [Action::Swap(3), Action::Swap(6), Action::Swap(9)]
    );
    assert_eq!(
        *alerts.lock().unwrap(),
        [Action::Alert(1), Action::Alert(4), Action::Alert(7)]
    );
    assert_eq!(
        report.actions.len(),
        9,
        "the harness capture receives every action"
    );
}

#[test]
fn unrouted_actions_are_reported() {
    let unrouted = Arc::new(Mutex::new(Vec::new()));
    let engine = Engine::<u64, Action>::builder()
        .collector(
            ReplayCollector::from_events("src", [2u64, 5]),
            CollectorOpts::default(),
        )
        .strategy(
            Router {
                unrouted: Arc::clone(&unrouted),
            },
            |s| s.from("src", Delivery::default()),
        )
        .executor(
            Typed {
                name: "swap",
                wants: |a| matches!(a, Action::Swap(_)),
                got: Arc::default(),
            },
            ExecutorOpts::default(),
        )
        .build()
        .unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    rt.block_on(async {
        let handle = engine.handle();
        let run = tokio::spawn(engine.run());
        while !handle.is_idle() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        handle.shutdown();
        run.await.unwrap();
    });
    assert_eq!(
        *unrouted.lock().unwrap(),
        [SubmitError::Unrouted, SubmitError::Unrouted]
    );
}

struct Blocked;

impl Executor<u64> for Blocked {
    fn name(&self) -> &str {
        "blocked"
    }

    async fn execute(&self, _: Arc<u64>) -> Result<(), BoxError> {
        std::future::pending::<()>().await;
        Ok(())
    }
}

struct TrySubmitter {
    results: Arc<Mutex<Vec<Result<(), SubmitError>>>>,
}

impl Strategy<u64, u64> for TrySubmitter {
    fn name(&self) -> &str {
        "try"
    }

    async fn on_event(
        &mut self,
        e: &Envelope<u64>,
        ctx: &mut StrategyCtx<u64>,
    ) -> Result<(), BoxError> {
        self.results.lock().unwrap().push(ctx.try_submit(*e.event));
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn try_submit_fails_when_the_queue_is_full() {
    let results = Arc::new(Mutex::new(Vec::new()));
    let engine = Engine::<u64, u64>::builder()
        .collector(
            ReplayCollector::from_events("src", 1..=5u64),
            CollectorOpts::default(),
        )
        .strategy(
            TrySubmitter {
                results: Arc::clone(&results),
            },
            |s| s.from("src", Delivery::default()),
        )
        .executor(Blocked, ExecutorOpts::default().queue_capacity(2))
        .shutdown_timeout(Duration::from_millis(10))
        .build()
        .unwrap();
    let handle = engine.handle();
    let run = tokio::spawn(engine.run());
    tokio::time::sleep(Duration::from_millis(10)).await;
    handle.shutdown();
    run.await.unwrap();
    let results = results.lock().unwrap();
    assert_eq!(results.len(), 5);
    let accepted = results.iter().take_while(|r| r.is_ok()).count();
    assert!((2..=3).contains(&accepted), "{results:?}");
    assert!(
        results[accepted..]
            .iter()
            .all(|r| *r == Err(SubmitError::Full("blocked".into())))
    );
}
