use std::path::PathBuf;
use std::time::Duration;

use botwright::test_util::{
    Harness, HarnessReport, Pace, Record, RecordingCollector, ReplayCollector,
};
use botwright::{BoxError, CollectorOpts, Delivery, Engine, Envelope, Strategy, StrategyCtx};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Event {
    Price { pair: String, price: u64 },
    Block(u64),
}

#[derive(Debug, Clone, PartialEq)]
enum Action {
    Bid {
        pair: String,
        price: u64,
        block: u64,
    },
    Reconcile(u64),
}

#[derive(Default)]
struct Bidder {
    block: u64,
}

impl Strategy<Event, Action> for Bidder {
    fn name(&self) -> &str {
        "bidder"
    }

    async fn on_event(
        &mut self,
        e: &Envelope<Event>,
        ctx: &mut StrategyCtx<Action>,
    ) -> Result<(), BoxError> {
        match &*e.event {
            Event::Block(n) => {
                self.block = *n;
                ctx.submit(Action::Reconcile(*n)).await?;
            }
            Event::Price { pair, price } if price % 3 != 0 => {
                let (pair, price, block) = (pair.clone(), *price, self.block);
                let think = Duration::from_millis(price % 7 * 5);
                ctx.spawn("bid", move |actions| async move {
                    tokio::time::sleep(think).await;
                    actions.submit(Action::Bid { pair, price, block }).await?;
                    Ok(())
                })?;
            }
            Event::Price { .. } => {}
        }
        Ok(())
    }
}

fn recording_file(tag: &str) -> PathBuf {
    let path =
        std::env::temp_dir().join(format!("botwright-replay-{}-{tag}.jsonl", std::process::id()));
    let lines: String = (0..300u64)
        .map(|i| {
            let event = if i % 10 == 0 {
                Event::Block(i / 10)
            } else {
                Event::Price {
                    pair: ["ETH/USDC", "BTC/USDC"][(i % 2) as usize].into(),
                    price: 1000 + i * 7,
                }
            };
            serde_json::to_string(&Record {
                at_ms: i * 3,
                event,
            })
            .unwrap()
                + "\n"
        })
        .collect();
    std::fs::write(&path, lines).unwrap();
    path
}

fn run(path: &PathBuf) -> HarnessReport<Action> {
    let prices = ReplayCollector::<Event>::from_jsonl("prices", path)
        .unwrap()
        .pace(Pace::Original);
    let fast = ReplayCollector::<Event>::from_jsonl("fast", path).unwrap();
    Harness::new(
        Engine::<Event, Action>::builder()
            .collector(prices, CollectorOpts::default())
            .collector(fast, CollectorOpts::default())
            .strategy(Bidder::default(), |s| {
                s.from("prices", Delivery::Lossless { capacity: 8 })
                    .from("fast", Delivery::DropOldest { capacity: 16 })
                    .filter(|e| matches!(e, Event::Block(_)))
            }),
    )
    .run()
}

#[test]
fn same_recording_yields_the_same_actions() {
    let path = recording_file("deterministic");
    let first = run(&path);
    let second = run(&path);
    std::fs::remove_file(&path).ok();
    assert!(first.completed && second.completed);
    assert!(first.actions.len() > 100);
    assert_eq!(first.actions, second.actions);
    assert_eq!(first.elapsed, second.elapsed);
}

#[test]
fn recording_writes_replayable_jsonl() {
    let path = std::env::temp_dir().join(format!("botwright-record-{}.jsonl", std::process::id()));
    let events = vec![
        Event::Block(1),
        Event::Price {
            pair: "ETH/USDC".into(),
            price: 5,
        },
    ];
    let recorder =
        RecordingCollector::new(ReplayCollector::from_events("live", events.clone()), &path)
            .unwrap();
    let report = Harness::new(
        Engine::<Event, Action>::builder()
            .collector(recorder, CollectorOpts::default())
            .strategy(Bidder::default(), |s| s.from("live", Delivery::default())),
    )
    .run();
    assert!(report.completed);
    let replayed = ReplayCollector::<Event>::from_jsonl("again", &path).unwrap();
    std::fs::remove_file(&path).ok();
    assert_eq!(replayed.len(), 2);
}

#[test]
fn stream_jsonl_matches_in_memory_replay() {
    let path = recording_file("stream");
    let in_memory = Harness::new(
        Engine::<Event, Action>::builder()
            .collector(
                ReplayCollector::<Event>::from_jsonl("prices", &path)
                    .unwrap()
                    .pace(Pace::Original),
                CollectorOpts::default(),
            )
            .strategy(Bidder::default(), |s| {
                s.from("prices", Delivery::Lossless { capacity: 16 })
            }),
    )
    .run();

    let streamed = Harness::new(
        Engine::<Event, Action>::builder()
            .collector(
                ReplayCollector::<Event>::stream_jsonl("prices", &path).pace(Pace::Original),
                CollectorOpts::default(),
            )
            .strategy(Bidder::default(), |s| {
                s.from("prices", Delivery::Lossless { capacity: 16 })
            }),
    )
    .run();
    std::fs::remove_file(&path).ok();

    assert!(in_memory.completed && streamed.completed);
    assert_eq!(in_memory.actions, streamed.actions);
    assert_eq!(in_memory.elapsed, streamed.elapsed);
}

#[test]
fn pace_capped_clamps_large_gaps_without_burning_virtual_time() {
    let records = vec![
        Record {
            at_ms: 0,
            event: Event::Block(1),
        },
        Record {
            at_ms: 100,
            event: Event::Block(2),
        },
        Record {
            // 10-hour gap should be clamped to 1s.
            at_ms: 100 + 10 * 3_600 * 1_000,
            event: Event::Block(3),
        },
    ];
    let collector = ReplayCollector::from_records("blocks", records).pace(Pace::Capped {
        max_gap: Duration::from_secs(1),
    });

    let report = Harness::new(
        Engine::<Event, Action>::builder()
            .collector(collector, CollectorOpts::default())
            .strategy(Bidder::default(), |s| s.from("blocks", Delivery::default())),
    )
    .run();

    assert!(report.completed);
    assert_eq!(report.actions.len(), 3);
    assert!(report.elapsed >= Duration::from_millis(1_100));
    assert!(report.elapsed < Duration::from_secs(5));
}

#[tokio::test]
async fn from_stream_supports_cursor_resume() {
    use botwright::{Collector, Item};
    use futures::TryStreamExt;

    let collector = ReplayCollector::from_stream("stream", |start| {
        Ok((start..5usize).map(|i| {
            Ok(Record {
                at_ms: (i as u64) * 10,
                event: Event::Block(i as u64),
            })
        }))
    });
    assert!(collector.is_streaming());
    assert!(!collector.is_exhausted());

    let items: Vec<Item<Event, usize>> = collector
        .connect(Some(2))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert!(collector.is_exhausted());
    let blocks: Vec<u64> = items
        .into_iter()
        .filter_map(|item| match item {
            Item::Event {
                event: Event::Block(n),
                ..
            } => Some(n),
            _ => None,
        })
        .collect();
    assert_eq!(blocks, vec![2, 3, 4]);
}
