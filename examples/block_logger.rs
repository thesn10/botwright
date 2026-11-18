//! Logs every new block and every reorg of a chain.
//!
//! ```sh
//! anvil --block-time 1 &
//! RPC_WS=ws://127.0.0.1:8545 cargo run --example block_logger
//! ```

use botwright::eth::{BlockCollector, ChainEvent, Header};
use botwright::{
    BoxError, CollectorOpts, Delivery, Engine, Envelope, Strategy, StrategyCtx, SystemEvent,
};

#[derive(Debug)]
enum Event {
    Chain(ChainEvent<Header>),
}

impl From<ChainEvent<Header>> for Event {
    fn from(event: ChainEvent<Header>) -> Self {
        Self::Chain(event)
    }
}

struct BlockLogger;

impl Strategy<Event, ()> for BlockLogger {
    fn name(&self) -> &str {
        "block-logger"
    }

    async fn on_event(
        &mut self,
        event: &Envelope<Event>,
        _ctx: &mut StrategyCtx<()>,
    ) -> Result<(), BoxError> {
        match &*event.event {
            Event::Chain(ChainEvent::Block(header)) => {
                tracing::info!(number = header.number, hash = %header.hash, txs_root = %header.transactions_root, "block");
            }
            Event::Chain(ChainEvent::Reorg {
                common_ancestor,
                dropped,
            }) => {
                tracing::warn!(
                    ancestor = common_ancestor.number,
                    depth = dropped.len(),
                    "reorg"
                );
            }
        }
        Ok(())
    }

    async fn on_system(
        &mut self,
        event: &SystemEvent,
        _ctx: &mut StrategyCtx<()>,
    ) -> Result<(), BoxError> {
        tracing::info!(?event, "system");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let url = std::env::var("RPC_WS").unwrap_or_else(|_| "ws://127.0.0.1:8545".into());
    let engine = Engine::<Event, ()>::builder()
        .collector(BlockCollector::ws("blocks", url), CollectorOpts::default())
        .strategy(BlockLogger, |sub| {
            sub.from("blocks", Delivery::Lossless { capacity: 256 })
        })
        .build()?;
    let exit = engine.run().await;
    tracing::info!(%exit, "engine stopped");
    Ok(())
}
