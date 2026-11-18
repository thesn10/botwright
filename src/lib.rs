//! Botwright is a framework for event-driven bots (MEV, liquidations, keepers, arbitrage,
//! monitoring) built on the collector → strategy → executor pattern.
//!
//! - [`Collector`]s produce events and reconnect with backoff, resuming from their last cursor.
//! - [`Strategy`]s hold state, process events one at a time and emit actions; long work runs in
//!   opportunity tasks started with [`StrategyCtx::spawn`].
//! - [`Executor`]s execute actions with bounded concurrency and an optional timeout.
//!
//! Each subscription chooses its [`Delivery`]: `Lossless` (default), `DropNewest`, `DropOldest`
//! or `Latest`. Dropping is never silent: the strategy receives [`SystemEvent::Dropped`].
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use botwright::{
//!     BoxError, CollectorOpts, Delivery, Engine, Envelope, ExecutorOpts, IntervalCollector, PrintExecutor,
//!     Strategy, StrategyCtx, Tick,
//! };
//!
//! #[derive(Debug)]
//! enum Event { Tick(Tick) }
//! impl From<Tick> for Event {
//!     fn from(t: Tick) -> Self { Event::Tick(t) }
//! }
//!
//! #[derive(Debug)]
//! struct Ping(u64);
//!
//! struct Pinger;
//! impl Strategy<Event, Ping> for Pinger {
//!     fn name(&self) -> &str { "pinger" }
//!     async fn on_event(&mut self, e: &Envelope<Event>, ctx: &mut StrategyCtx<Ping>) -> Result<(), BoxError> {
//!         let Event::Tick(t) = &*e.event;
//!         ctx.submit(Ping(t.n)).await?;
//!         Ok(())
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let engine = Engine::<Event, Ping>::builder()
//!     .collector(IntervalCollector::new("clock", Duration::from_secs(1)), CollectorOpts::default())
//!     .strategy(Pinger, |sub| sub.from("clock", Delivery::Latest))
//!     .executor(PrintExecutor::default(), ExecutorOpts::default())
//!     .build()?;
//! let exit = engine.run().await;
//! # Ok(()) }
//! ```
#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

#[cfg(all(feature = "evm-alloy1", feature = "evm-alloy2"))]
compile_error!(
    "features `evm-alloy1` and `evm-alloy2` are mutually exclusive; use `default-features = false` with `evm-alloy2`"
);

#[cfg(all(
    any(feature = "mempool", feature = "hypersync", feature = "bundles"),
    not(any(feature = "evm-alloy1", feature = "evm-alloy2"))
))]
compile_error!("features `mempool`, `hypersync` and `bundles` need `evm-alloy1` or `evm-alloy2`");

mod backoff;
mod builtin;
mod collector;
mod delivery;
mod engine;
mod error;
mod executor;
mod obs;
mod strategy;
mod types;

pub mod notify;

#[cfg(any(feature = "evm-alloy1", feature = "evm-alloy2"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "evm-alloy1", feature = "evm-alloy2"))))]
pub mod eth;

#[cfg(feature = "http")]
#[cfg_attr(docsrs, doc(cfg(feature = "http")))]
pub mod http;

#[cfg(feature = "test-util")]
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
pub mod test_util;

pub use backoff::Backoff;
pub use builtin::{
    ChannelClosed, ChannelCollector, EventSender, IntervalCollector, MapCollector, NoopExecutor,
    PrintExecutor, Tick, TrySendError,
};
pub use collector::{Collector, CollectorOpts};
pub use delivery::{Delivery, OnFull, Subscriptions};
pub use engine::{CollectorHealth, Engine, EngineBuilder, EngineHandle};
pub use error::{BoxError, BuildError, CollectorError, ExitReason, SpawnError, SubmitError};
pub use executor::{Executor, ExecutorOpts};
pub use strategy::{ActionSender, OnError, OnPanic, Strategy, StrategyCtx};
pub use tokio_util::sync::CancellationToken;
pub use types::{Envelope, Item, SourceId, SystemEvent};
