//! Executors report results as events: a `TransactionExecutor` sends a transfer every few seconds
//! and feeds `Sent` and `Included` back into the strategy through a `ChannelCollector`, so the
//! strategy only sends again once the previous transaction is included.
//!
//! ```sh
//! anvil --block-time 2 &
//! RPC_HTTP=http://127.0.0.1:8545 PRIVATE_KEY=<anvil dev key> cargo run --example feedback_loop
//! ```

use std::time::Duration;

use botwright::eth::{
    Address, PrivateKeySigner, TransactionAction, TransactionExecutor, TransactionRequest,
    TxFeedback, U256,
};
use botwright::{
    BoxError, ChannelCollector, CollectorOpts, Delivery, Engine, Envelope, ExecutorOpts,
    IntervalCollector, Strategy, StrategyCtx, Tick,
};

#[derive(Debug)]
enum Event {
    Tick,
    Feedback(TxFeedback),
}

impl From<Tick> for Event {
    fn from(_: Tick) -> Self {
        Self::Tick
    }
}

impl From<TxFeedback> for Event {
    fn from(feedback: TxFeedback) -> Self {
        Self::Feedback(feedback)
    }
}

#[derive(Debug)]
struct Transfer {
    to: Address,
}

impl TransactionAction for Transfer {
    fn transaction(&self) -> Option<TransactionRequest> {
        Some(
            TransactionRequest::default()
                .to(self.to)
                .value(U256::from(1)),
        )
    }
}

struct Pinger {
    to: Address,
    in_flight: bool,
}

impl Strategy<Event, Transfer> for Pinger {
    fn name(&self) -> &str {
        "pinger"
    }

    async fn on_event(
        &mut self,
        event: &Envelope<Event>,
        ctx: &mut StrategyCtx<Transfer>,
    ) -> Result<(), BoxError> {
        match &*event.event {
            Event::Tick if !self.in_flight => {
                self.in_flight = true;
                ctx.submit(Transfer { to: self.to }).await?;
            }
            Event::Tick => tracing::info!("previous transfer still pending"),
            Event::Feedback(TxFeedback::Sent { tx_hash, .. }) => tracing::info!(%tx_hash, "sent"),
            Event::Feedback(TxFeedback::Included { receipt, .. }) => {
                tracing::info!(tx_hash = %receipt.transaction_hash, block = ?receipt.block_number, "included");
                self.in_flight = false;
            }
            Event::Feedback(other) => {
                tracing::warn!(?other, "transfer failed");
                self.in_flight = false;
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let url = std::env::var("RPC_HTTP").unwrap_or_else(|_| "http://127.0.0.1:8545".into());
    let signer: PrivateKeySigner = std::env::var("PRIVATE_KEY")?.parse()?;
    let me = signer.address();

    let (feedback, sender) = ChannelCollector::new("tx-feedback", 256);
    let executor = TransactionExecutor::new("tx", signer, &url)?.feedback(sender);
    let engine = Engine::<Event, Transfer>::builder()
        .collector(
            IntervalCollector::new("clock", Duration::from_secs(3)),
            CollectorOpts::default(),
        )
        .collector(feedback, CollectorOpts::default())
        .strategy(
            Pinger {
                to: me,
                in_flight: false,
            },
            |sub| {
                sub.from("clock", Delivery::Latest)
                    .from("tx-feedback", Delivery::default())
            },
        )
        .executor(
            executor,
            ExecutorOpts::default().timeout(Duration::from_secs(30)),
        )
        .build()?;
    let exit = engine.run().await;
    tracing::info!(%exit, "engine stopped");
    Ok(())
}
