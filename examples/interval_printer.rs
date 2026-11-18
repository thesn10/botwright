//! Smallest possible bot: a clock, a strategy that turns every fifth tick into an action, and an
//! executor that prints it. Stop with Ctrl-C.
//!
//! ```sh
//! cargo run --example interval_printer
//! ```

use std::time::Duration;

use botwright::{
    BoxError, CollectorOpts, Delivery, Engine, Envelope, ExecutorOpts, IntervalCollector,
    PrintExecutor, Strategy, StrategyCtx, Tick,
};

#[derive(Debug)]
struct Event(Tick);

impl From<Tick> for Event {
    fn from(tick: Tick) -> Self {
        Self(tick)
    }
}

#[expect(dead_code, reason = "only read through Debug by PrintExecutor")]
#[derive(Debug)]
struct Report {
    tick: u64,
}

struct EveryFifth;

impl Strategy<Event, Report> for EveryFifth {
    fn name(&self) -> &str {
        "every-fifth"
    }

    async fn on_event(
        &mut self,
        event: &Envelope<Event>,
        ctx: &mut StrategyCtx<Report>,
    ) -> Result<(), BoxError> {
        let Event(tick) = &*event.event;
        if tick.n % 5 == 0 {
            ctx.submit(Report { tick: tick.n }).await?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let engine = Engine::<Event, Report>::builder()
        .collector(
            IntervalCollector::new("clock", Duration::from_millis(200)),
            CollectorOpts::default(),
        )
        .strategy(EveryFifth, |sub| sub.from("clock", Delivery::Latest))
        .executor(PrintExecutor::default(), ExecutorOpts::default())
        .build()?;
    let exit = engine.run().await;
    println!("engine stopped: {exit}");
    Ok(())
}
