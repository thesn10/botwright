//! Watches ERC-20 `Transfer` logs of one token and prints an alert for large transfers instead of
//! acting on them: logs → strategy → `PrintExecutor`.
//!
//! ```sh
//! RPC_WS=wss://… TOKEN=0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 MIN_AMOUNT=1000000000000 \
//!     cargo run --example logs_dry_run
//! ```

use botwright::eth::{Address, Filter, LogCollector, LogEvent, U256, sol, sol_types::SolEvent};
use botwright::{
    BoxError, CollectorOpts, Delivery, Engine, Envelope, ExecutorOpts, PrintExecutor, Strategy,
    StrategyCtx,
};

sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
}

#[derive(Debug)]
enum Event {
    Log(LogEvent),
}

impl From<LogEvent> for Event {
    fn from(event: LogEvent) -> Self {
        Self::Log(event)
    }
}

#[expect(dead_code, reason = "only read through Debug by PrintExecutor")]
#[derive(Debug)]
enum Action {
    LargeTransfer {
        from: Address,
        to: Address,
        value: U256,
        block: Option<u64>,
    },
    Reverted {
        from: Address,
        to: Address,
        value: U256,
    },
}

struct Watcher {
    min_amount: U256,
}

impl Strategy<Event, Action> for Watcher {
    fn name(&self) -> &str {
        "large-transfers"
    }

    async fn on_event(
        &mut self,
        event: &Envelope<Event>,
        ctx: &mut StrategyCtx<Action>,
    ) -> Result<(), BoxError> {
        let Event::Log(log_event) = &*event.event;
        let (log, removed) = match log_event {
            LogEvent::Added(log) => (log, false),
            LogEvent::Removed(log) => (log, true),
        };
        let Ok(transfer) = Transfer::decode_log(&log.inner) else {
            return Ok(());
        };
        if transfer.value < self.min_amount {
            return Ok(());
        }
        let (from, to, value) = (transfer.from, transfer.to, transfer.value);
        let action = if removed {
            Action::Reverted { from, to, value }
        } else {
            Action::LargeTransfer {
                from,
                to,
                value,
                block: log.block_number,
            }
        };
        ctx.submit(action).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let url = std::env::var("RPC_WS").unwrap_or_else(|_| "ws://127.0.0.1:8545".into());
    let token: Address = std::env::var("TOKEN")?.parse()?;
    let min_amount: U256 = std::env::var("MIN_AMOUNT")
        .unwrap_or_else(|_| "0".into())
        .parse()?;

    let filter = Filter::new()
        .address(token)
        .event_signature(Transfer::SIGNATURE_HASH);
    let engine = Engine::<Event, Action>::builder()
        .collector(
            LogCollector::ws("transfers", url, filter),
            CollectorOpts::default(),
        )
        .strategy(Watcher { min_amount }, |sub| {
            sub.from("transfers", Delivery::Lossless { capacity: 4096 })
        })
        .executor(PrintExecutor::default(), ExecutorOpts::default())
        .build()?;
    let exit = engine.run().await;
    tracing::info!(%exit, "engine stopped");
    Ok(())
}
