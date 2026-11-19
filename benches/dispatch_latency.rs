use std::sync::Arc;
use std::time::{Duration, Instant};

use botwright::{
    BoxError, ChannelCollector, CollectorOpts, Delivery, Engine, Envelope, Strategy, StrategyCtx,
};
use criterion::{Criterion, criterion_group, criterion_main};
use futures::{StreamExt, future, stream};
use tokio::sync::{Mutex, mpsc};

struct Ack {
    tx: mpsc::UnboundedSender<Duration>,
}

impl Strategy<Instant, ()> for Ack {
    fn name(&self) -> &str {
        "ack"
    }

    async fn on_event(
        &mut self,
        e: &Envelope<Instant>,
        _: &mut StrategyCtx<()>,
    ) -> Result<(), BoxError> {
        let _ = self.tx.send(e.event.elapsed());
        Ok(())
    }
}

fn dispatch(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    [
        Delivery::Lossless { capacity: 1024 },
        Delivery::DropOldest { capacity: 1024 },
        Delivery::Latest,
    ]
    .into_iter()
    .for_each(|delivery| bench_delivery(c, &rt, delivery));
}

fn bench_delivery(c: &mut Criterion, rt: &tokio::runtime::Runtime, delivery: Delivery) {
    let (collector, sender) = ChannelCollector::new("feed", 1024);
    let (tx, rx) = mpsc::unbounded_channel();
    let rx = Arc::new(Mutex::new(rx));
    let engine = rt.block_on(async {
        Engine::<Instant, ()>::builder()
            .collector(collector, CollectorOpts::default())
            .strategy(Ack { tx }, |s| s.from("feed", delivery))
            .build()
            .unwrap()
    });
    let handle = engine.handle();
    let run = rt.spawn(engine.run());
    c.bench_function(&format!("send_to_on_event/{delivery:?}"), |b| {
        b.to_async(rt).iter_custom(|iters| {
            let sender = sender.clone();
            let rx = Arc::clone(&rx);
            async move {
                let mut rx = rx.lock().await;
                let latencies = stream::poll_fn(|cx| rx.poll_recv(cx));
                stream::iter(0..iters)
                    .then(|_| sender.send(Instant::now()))
                    .zip(latencies)
                    .fold(Duration::ZERO, |total, (sent, latency)| {
                        sent.unwrap();
                        future::ready(total + latency)
                    })
                    .await
            }
        })
    });
    handle.shutdown();
    rt.block_on(run).unwrap();
}

criterion_group!(benches, dispatch);
criterion_main!(benches);
