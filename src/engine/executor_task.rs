use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::FutureExt;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::Instant;
use tokio_util::task::TaskTracker;
use tracing::Instrument;

use super::Shared;
use crate::error::panic_message;
use crate::obs;
use crate::{Executor, ExecutorOpts};

pub(crate) struct ExecutorRuntime<A> {
    pub(crate) index: usize,
    pub(crate) rx: mpsc::Receiver<Arc<A>>,
    pub(crate) shared: Arc<Shared>,
}

struct Metrics {
    ok: obs::Counter,
    error: obs::Counter,
    timeout: obs::Counter,
    panic: obs::Counter,
    duration: obs::Histogram,
}

pub(crate) async fn run_executor<A, X>(
    executor: Arc<X>,
    opts: ExecutorOpts,
    mut rt: ExecutorRuntime<A>,
) where
    A: Send + Sync + 'static,
    X: Executor<A>,
{
    let name = executor.name().to_owned();
    let span = tracing::info_span!("executor", name = %name);
    async move {
        let result =
            |r: &'static str| obs::counter(obs::ACTIONS, &[("executor", &name), ("result", r)]);
        let metrics = Arc::new(Metrics {
            ok: result("ok"),
            error: result("error"),
            timeout: result("timeout"),
            panic: result("panic"),
            duration: obs::histogram(obs::EXECUTE, &[("executor", &name)]),
        });
        let permits = Arc::new(Semaphore::new(opts.concurrency));
        let running = TaskTracker::new();
        let stop = rt.shared.executors_stop.clone();

        loop {
            let permit = tokio::select! {
                biased;
                () = stop.cancelled() => break,
                p = Arc::clone(&permits).acquire_owned() => p.expect("semaphore is never closed"),
            };
            let action = tokio::select! {
                biased;
                () = stop.cancelled() => break,
                a = rt.rx.recv() => a,
            };
            let Some(action) = action else { break };
            spawn_execution(&executor, action, permit, &opts, &rt, &metrics, &running);
        }

        rt.rx.close();
        let deadline = rt.shared.deadline();
        let drained = tokio::time::timeout_at(deadline, async {
            while let Some(action) = rt.rx.recv().await {
                let permit = Arc::clone(&permits)
                    .acquire_owned()
                    .await
                    .expect("semaphore is never closed");
                spawn_execution(&executor, action, permit, &opts, &rt, &metrics, &running);
            }
            running.close();
            running.wait().await;
        })
        .await;
        if drained.is_err() {
            tracing::warn!("shutdown timeout reached with actions still queued or running");
        }
    }
    .instrument(span)
    .await
}

fn spawn_execution<A, X>(
    executor: &Arc<X>,
    action: Arc<A>,
    permit: tokio::sync::OwnedSemaphorePermit,
    opts: &ExecutorOpts,
    rt: &ExecutorRuntime<A>,
    metrics: &Arc<Metrics>,
    running: &TaskTracker,
) where
    A: Send + Sync + 'static,
    X: Executor<A>,
{
    let executor = Arc::clone(executor);
    let metrics = Arc::clone(metrics);
    let shared = Arc::clone(&rt.shared);
    let index = rt.index;
    let timeout = opts.timeout;
    running.spawn(
        async move {
            let _permit = permit;
            let started = Instant::now();
            let call = AssertUnwindSafe(executor.execute(action)).catch_unwind();
            let outcome = match timeout {
                Some(limit) => tokio::time::timeout(limit, call).await.ok(),
                None => Some(call.await),
            };
            metrics.duration.record(started.elapsed().as_secs_f64());
            match outcome {
                Some(Ok(Ok(()))) => metrics.ok.increment(1),
                Some(Ok(Err(err))) => {
                    metrics.error.increment(1);
                    tracing::warn!(error = %err, "action failed");
                }
                Some(Err(panic)) => {
                    metrics.panic.increment(1);
                    tracing::error!(panic = %panic_message(&*panic), "action panicked");
                }
                None => {
                    metrics.timeout.increment(1);
                    tracing::warn!(?timeout, "action timed out");
                }
            }
            shared.executors[index]
                .pending
                .fetch_sub(1, Ordering::AcqRel);
        }
        .in_current_span(),
    );
}
