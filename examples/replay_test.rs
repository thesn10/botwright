//! Replays recorded events through a strategy on a paused clock and checks the actions, the way a
//! regression test would. The recording is written first so the example is self-contained.
//!
//! ```sh
//! cargo run --example replay_test --features test-util
//! ```

use std::time::Duration;

use botwright::test_util::{Harness, Record, ReplayCollector};
use botwright::{BoxError, CollectorOpts, Delivery, Engine, Envelope, Strategy, StrategyCtx};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Price {
    pair: String,
    price: f64,
}

#[derive(Debug, PartialEq)]
enum Action {
    Buy { pair: String, price: f64 },
}

#[derive(Default)]
struct DipBuyer {
    last: Option<f64>,
}

impl Strategy<Price, Action> for DipBuyer {
    fn name(&self) -> &str {
        "dip-buyer"
    }

    async fn on_event(
        &mut self,
        event: &Envelope<Price>,
        ctx: &mut StrategyCtx<Action>,
    ) -> Result<(), BoxError> {
        let price = event.event.price;
        if let Some(last) = self.last.replace(price)
            && price < last * 0.95
        {
            let pair = event.event.pair.clone();
            ctx.spawn("confirm-dip", move |actions| async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                actions.submit(Action::Buy { pair, price }).await?;
                Ok(())
            })?;
        }
        Ok(())
    }
}

fn write_recording(path: &std::path::Path) -> std::io::Result<()> {
    let prices = [100.0, 101.0, 94.0, 95.0, 96.0, 90.0, 91.0];
    let lines = prices
        .into_iter()
        .enumerate()
        .map(|(i, price)| {
            let record = Record {
                at_ms: i as u64 * 12_000,
                event: Price {
                    pair: "ETH/USDC".into(),
                    price,
                },
            };
            serde_json::to_string(&record).map(|json| json + "\n")
        })
        .collect::<Result<String, _>>()?;
    std::fs::write(path, lines)
}

fn main() -> Result<(), BoxError> {
    let path = std::env::temp_dir().join("botwright-replay-example.jsonl");
    write_recording(&path)?;

    let run = || -> Result<Vec<String>, BoxError> {
        let replay = ReplayCollector::<Price>::from_jsonl("prices", &path)?;
        let builder = Engine::<Price, Action>::builder()
            .collector(replay, CollectorOpts::default())
            .strategy(DipBuyer::default(), |sub| {
                sub.from("prices", Delivery::default())
            });
        let report = Harness::new(builder).run();
        assert!(report.completed, "{}", report.exit);
        println!("replayed in {:?} of virtual time", report.elapsed);
        Ok(report.actions.iter().map(|a| format!("{a:?}")).collect())
    };

    let first = run()?;
    let second = run()?;
    assert_eq!(first, second, "replay must be deterministic");
    assert_eq!(first.len(), 2);
    first.iter().for_each(|action| println!("{action}"));
    Ok(())
}
