mod common;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use botwright::test_util::{Harness, ReplayCollector};
use botwright::{
    Backoff, BoxError, Collector, CollectorError, CollectorOpts, Delivery, Engine, Envelope,
    Executor, ExecutorOpts, ExitReason, Item, OnPanic, SourceId, Strategy, StrategyCtx,
    SystemEvent,
};
use common::{Log, Recorder};
use futures::{TryStream, stream};
use tokio::time::Instant;

enum Step {
    Event(u64),
    Fail,
}

type Resume = (Option<u64>, Instant);

/// Each connection plays the next script; afterwards the collector is exhausted.
#[derive(Clone)]
struct Scripted {
    scripts: Arc<Mutex<VecDeque<Vec<Step>>>>,
    resumes: Arc<Mutex<Vec<Resume>>>,
}

impl Collector<u64> for Scripted {
    type Cursor = u64;

    fn name(&self) -> &str {
        "scripted"
    }

    async fn connect(
        &self,
        resume: Option<u64>,
    ) -> Result<
        impl TryStream<Ok = Item<u64, u64>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        self.resumes.lock().unwrap().push((resume, Instant::now()));
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        Ok(stream::iter(script.into_iter().map(|step| match step {
            Step::Event(v) => Ok(Item::new(v, v)),
            Step::Fail => Err(CollectorError::protocol("boom")),
        })))
    }

    fn is_exhausted(&self) -> bool {
        self.scripts.lock().unwrap().is_empty()
    }
}

#[test]
fn reconnects_with_backoff_and_last_cursor() {
    let scripted = Scripted {
        scripts: Arc::new(Mutex::new(VecDeque::from([
            vec![Step::Event(1), Step::Event(2), Step::Event(3), Step::Fail],
            vec![Step::Event(4), Step::Event(5)],
            vec![Step::Event(6)],
        ]))),
        resumes: Arc::default(),
    };
    let log = Log::default();
    let report = Harness::new(
        Engine::<u64, u64>::builder()
            .collector(
                scripted.clone(),
                CollectorOpts::default().backoff(Backoff::constant(Duration::from_millis(100))),
            )
            .strategy(Recorder::new("s", &log), |s| {
                s.from("scripted", Delivery::default())
            }),
    )
    .run();
    assert!(report.completed);
    assert_eq!(log.values("scripted"), [1, 2, 3, 4, 5, 6]);

    let resumes = scripted.resumes.lock().unwrap().clone();
    let cursors: Vec<_> = resumes.iter().map(|(c, _)| *c).collect();
    assert_eq!(cursors, [None, Some(3), Some(5)]);
    assert!(
        resumes
            .windows(2)
            .all(|pair| pair[1].1 - pair[0].1 >= Duration::from_millis(100)),
        "{resumes:?}"
    );

    let systems = log.systems();
    let connected = systems
        .iter()
        .filter(|e| matches!(e, SystemEvent::CollectorConnected { .. }))
        .count();
    let disconnected: Vec<_> = systems
        .iter()
        .filter_map(|e| match e {
            SystemEvent::CollectorDisconnected { error, .. } => Some(error.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(connected, 3);
    assert_eq!(disconnected.len(), 2);
    assert!(disconnected[0].contains("boom"));
    assert!(disconnected[1].contains("ended"));
}

struct AlwaysFails;

impl Collector<u64> for AlwaysFails {
    type Cursor = ();

    fn name(&self) -> &str {
        "broken"
    }

    async fn connect(
        &self,
        _: Option<()>,
    ) -> Result<
        impl TryStream<Ok = Item<u64, ()>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        Err::<stream::Empty<Result<Item<u64, ()>, CollectorError>>, _>(CollectorError::protocol(
            "refused",
        ))
    }
}

#[test]
fn collector_fails_after_max_retries() {
    let log = Log::default();
    let report = Harness::new(
        Engine::<u64, u64>::builder()
            .collector(
                AlwaysFails,
                CollectorOpts::default()
                    .backoff(Backoff::constant(Duration::from_millis(10)).max_retries(3)),
            )
            .strategy(Recorder::new("s", &log), |s| {
                s.from("broken", Delivery::Latest)
            }),
    )
    .run();
    match report.exit {
        ExitReason::CollectorFailed { collector, error } => {
            assert_eq!(collector, "broken");
            assert!(error.contains("refused"));
        }
        other => panic!("unexpected exit {other:?}"),
    }
}

struct Panicky {
    panic_on: u64,
    inits: Arc<AtomicU32>,
    log: Log,
}

impl Strategy<u64, u64> for Panicky {
    fn name(&self) -> &str {
        "panicky"
    }

    async fn init(&mut self, _: &mut StrategyCtx<u64>) -> Result<(), BoxError> {
        self.inits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn on_event(
        &mut self,
        e: &Envelope<u64>,
        _: &mut StrategyCtx<u64>,
    ) -> Result<(), BoxError> {
        self.log.push(common::Seen::Event {
            source: e.source.to_string(),
            seq: e.seq,
            value: *e.event,
        });
        if e.event.is_multiple_of(self.panic_on) {
            panic!("bad event {}", e.event);
        }
        Ok(())
    }

    async fn on_system(
        &mut self,
        e: &SystemEvent,
        _: &mut StrategyCtx<u64>,
    ) -> Result<(), BoxError> {
        self.log.push(common::Seen::System(e.clone()));
        Ok(())
    }
}

#[test]
fn panic_with_abort_ends_the_engine() {
    let log = Log::default();
    let inits = Arc::new(AtomicU32::new(0));
    let report = Harness::new(
        Engine::<u64, u64>::builder()
            .collector(
                ReplayCollector::from_events("src", 1..=10u64),
                CollectorOpts::default(),
            )
            .strategy(
                Panicky {
                    panic_on: 3,
                    inits,
                    log: log.clone(),
                },
                |s| s.from("src", Delivery::default()),
            ),
    )
    .run();
    assert_eq!(
        report.exit,
        ExitReason::StrategyPanicked {
            strategy: "panicky".into(),
            message: "bad event 3".into()
        }
    );
}

#[test]
fn panic_with_factory_rebuilds_until_the_budget_is_exhausted() {
    let log = Log::default();
    let inits = Arc::new(AtomicU32::new(0));
    let (l, i) = (log.clone(), Arc::clone(&inits));
    let report = Harness::new(
        Engine::<u64, u64>::builder()
            .collector(
                ReplayCollector::from_events("src", 1..=20u64),
                CollectorOpts::default(),
            )
            .strategy_with_factory(
                move || Panicky {
                    panic_on: 5,
                    inits: Arc::clone(&i),
                    log: l.clone(),
                },
                |s| {
                    s.from("src", Delivery::default())
                        .on_panic(OnPanic::Restart {
                            max: 2,
                            within: Duration::from_secs(60),
                        })
                },
            ),
    )
    .run();

    assert_eq!(
        inits.load(Ordering::SeqCst),
        3,
        "initial init plus two restarts"
    );
    match &report.exit {
        ExitReason::StrategyPanicked { strategy, message } => {
            assert_eq!(strategy, "panicky");
            assert!(message.starts_with("bad event 15"), "{message}");
        }
        other => panic!("unexpected exit {other:?}"),
    }
    let systems = log.systems();
    let restarted: Vec<_> = systems
        .iter()
        .filter_map(|e| match e {
            SystemEvent::Restarted { reason } => Some(reason.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(restarted, ["bad event 5", "bad event 10"]);
    let gaps = systems
        .iter()
        .filter(|e| matches!(e, SystemEvent::GapUnfilled { source, from: None, to: None } if source == "src"))
        .count();
    assert_eq!(gaps, 2);
    assert_eq!(log.values("src"), (1..=15).collect::<Vec<_>>());
}

#[test]
fn restart_without_factory_is_rejected() {
    let log = Log::default();
    let err = Engine::<u64, u64>::builder()
        .collector(
            ReplayCollector::from_events("src", [1u64]),
            CollectorOpts::default(),
        )
        .strategy(Recorder::new("s", &log), |s| {
            s.from("src", Delivery::default())
                .on_panic(OnPanic::Restart {
                    max: 1,
                    within: Duration::from_secs(1),
                })
        })
        .build()
        .err()
        .unwrap();
    assert_eq!(
        err,
        botwright::BuildError::RestartWithoutFactory("s".into())
    );
}

struct PanickingExecutor;

impl Executor<u64> for PanickingExecutor {
    fn name(&self) -> &str {
        "panicking"
    }

    async fn execute(&self, action: Arc<u64>) -> Result<(), BoxError> {
        if *action == 2 {
            panic!("executor panic");
        }
        Ok(())
    }
}

#[test]
fn executor_panic_does_not_end_the_engine() {
    let log = Log::default();
    let report = Harness::new(
        Engine::<u64, u64>::builder()
            .collector(
                ReplayCollector::from_events("src", 1..=5u64),
                CollectorOpts::default(),
            )
            .strategy(Recorder::new("s", &log).submitting(), |s| {
                s.from("src", Delivery::default())
            })
            .executor(PanickingExecutor, ExecutorOpts::default()),
    )
    .run();
    assert!(report.completed);
    assert_eq!(report.exit, ExitReason::Shutdown);
    assert_eq!(report.actions.len(), 5);
}

struct Guard(Arc<AtomicBool>);

impl Drop for Guard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct ShutdownProbe {
    log: Log,
    cancelled: Vec<Arc<AtomicBool>>,
    shared: Arc<Mutex<Vec<Arc<AtomicBool>>>>,
}

impl Strategy<u64, u64> for ShutdownProbe {
    fn name(&self) -> &str {
        "probe"
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
        let flag = Arc::new(AtomicBool::new(false));
        self.cancelled.push(Arc::clone(&flag));
        self.shared.lock().unwrap().push(Arc::clone(&flag));
        ctx.spawn("forever", move |_| async move {
            let _guard = Guard(flag);
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(())
        })?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    }

    async fn on_system(
        &mut self,
        e: &SystemEvent,
        _: &mut StrategyCtx<u64>,
    ) -> Result<(), BoxError> {
        self.log.push(common::Seen::System(e.clone()));
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_drains_channels_cancels_tasks_and_respects_the_timeout() {
    let log = Log::default();
    let flags = Arc::new(Mutex::new(Vec::new()));
    let engine = Engine::<u64, u64>::builder()
        .collector(
            ReplayCollector::from_events("src", 1..=50u64),
            CollectorOpts::default(),
        )
        .strategy(
            ShutdownProbe {
                log: log.clone(),
                cancelled: Vec::new(),
                shared: Arc::clone(&flags),
            },
            |s| s.from("src", Delivery::Lossless { capacity: 100 }),
        )
        .shutdown_timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let handle = engine.handle();
    let run = tokio::spawn(engine.run());
    tokio::time::sleep(Duration::from_millis(5)).await;
    let started = Instant::now();
    handle.shutdown();
    let exit = run.await.unwrap();
    let took = started.elapsed();

    assert_eq!(exit, ExitReason::Shutdown);
    assert!(took <= Duration::from_secs(5), "{took:?}");
    assert_eq!(
        log.values("src"),
        (1..=50).collect::<Vec<_>>(),
        "channel was not drained"
    );
    assert_eq!(log.systems().last(), Some(&SystemEvent::ShutdownRequested));
    let flags = flags.lock().unwrap();
    assert_eq!(flags.len(), 50);
    assert!(
        flags.iter().all(|f| f.load(Ordering::SeqCst)),
        "tasks were not cancelled"
    );
}

struct Stuck;

impl Strategy<u64, u64> for Stuck {
    fn name(&self) -> &str {
        "stuck"
    }

    async fn on_event(
        &mut self,
        _: &Envelope<u64>,
        _: &mut StrategyCtx<u64>,
    ) -> Result<(), BoxError> {
        std::future::pending::<()>().await;
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_ends_within_the_timeout_even_if_a_strategy_hangs() {
    let engine = Engine::<u64, u64>::builder()
        .collector(
            ReplayCollector::from_events("src", 1..=5u64),
            CollectorOpts::default(),
        )
        .strategy(Stuck, |s| s.from("src", Delivery::default()))
        .shutdown_timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let handle = engine.handle();
    let run = tokio::spawn(engine.run());
    tokio::time::sleep(Duration::from_millis(5)).await;
    let started = Instant::now();
    handle.shutdown();
    assert_eq!(run.await.unwrap(), ExitReason::Shutdown);
    let took = started.elapsed();
    assert!(
        took >= Duration::from_secs(2) && took < Duration::from_millis(2100),
        "{took:?}"
    );
}

struct FailingInit;

impl Strategy<u64, u64> for FailingInit {
    fn name(&self) -> &str {
        "failing"
    }

    async fn init(&mut self, _: &mut StrategyCtx<u64>) -> Result<(), BoxError> {
        Err("no state".into())
    }

    async fn on_event(
        &mut self,
        _: &Envelope<u64>,
        _: &mut StrategyCtx<u64>,
    ) -> Result<(), BoxError> {
        Ok(())
    }
}

#[test]
fn init_runs_before_collectors_and_its_failure_ends_the_engine() {
    let replay = ReplayCollector::from_events("src", 1..=5u64);
    let report = Harness::new(
        Engine::<u64, u64>::builder()
            .collector(replay, CollectorOpts::default())
            .strategy(FailingInit, |s| s.from("src", Delivery::default())),
    )
    .run();
    assert_eq!(
        report.exit,
        ExitReason::StrategyFailed {
            strategy: "failing".into(),
            error: "no state".into()
        }
    );
}

#[test]
fn source_id_compares_with_str() {
    assert_eq!(SourceId::from("a"), "a");
}
