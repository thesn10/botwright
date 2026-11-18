use std::sync::{Arc, Mutex};
use std::time::Duration;

use botwright::{
    BoxError, ChannelCollector, CollectorOpts, Delivery, Engine, Envelope, Strategy, StrategyCtx,
};

struct Measure {
    samples: Arc<Mutex<Vec<Duration>>>,
}

impl Strategy<u64, u64> for Measure {
    fn name(&self) -> &str {
        "measure"
    }

    async fn on_event(
        &mut self,
        e: &Envelope<u64>,
        ctx: &mut StrategyCtx<u64>,
    ) -> Result<(), BoxError> {
        self.samples.lock().unwrap().push(ctx.now() - e.received_at);
        Ok(())
    }
}

fn limit(var: &str, default_us: u64) -> Duration {
    Duration::from_micros(
        std::env::var(var)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default_us),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collector_to_on_event_latency_stays_below_the_limit() {
    let samples = Arc::new(Mutex::new(Vec::new()));
    let (collector, sender) = ChannelCollector::new("feed", 1024);
    let engine = Engine::<u64, u64>::builder()
        .collector(collector, CollectorOpts::default())
        .strategy(
            Measure {
                samples: Arc::clone(&samples),
            },
            |s| s.from("feed", Delivery::default()),
        )
        .build()
        .unwrap();
    let handle = engine.handle();
    let run = tokio::spawn(engine.run());

    for i in 0..5_000 {
        sender.send(i).await.unwrap();
        if i % 10 == 0 {
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
    }
    while samples.lock().unwrap().len() < 5_000 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    handle.shutdown();
    run.await.unwrap();

    let mut samples = samples.lock().unwrap().clone();
    samples.sort_unstable();
    let p50 = samples[samples.len() / 2];
    let p99 = samples[samples.len() * 99 / 100];
    eprintln!("dispatch latency p50 {p50:?}, p99 {p99:?}");
    assert!(
        p50 <= limit("BOTWRIGHT_LATENCY_P50_US", 1_000),
        "p50 {p50:?}"
    );
    assert!(
        p99 <= limit("BOTWRIGHT_LATENCY_P99_US", 20_000),
        "p99 {p99:?}"
    );
}
