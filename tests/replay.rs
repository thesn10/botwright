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

fn recording_file() -> PathBuf {
    let path = std::env::temp_dir().join(format!("botwright-replay-{}.jsonl", std::process::id()));
    let mut lines = String::new();
    for i in 0..300u64 {
        let event = if i % 10 == 0 {
            Event::Block(i / 10)
        } else {
            Event::Price {
                pair: ["ETH/USDC", "BTC/USDC"][(i % 2) as usize].into(),
                price: 1000 + i * 7,
            }
        };
        lines.push_str(
            &serde_json::to_string(&Record {
                at_ms: i * 3,
                event,
            })
            .unwrap(),
        );
        lines.push('\n');
    }
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
    let path = recording_file();
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
