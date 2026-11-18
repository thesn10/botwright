use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::{EngineBuilder, EngineHandle, Shared};
use crate::ExitReason;

/// The wired bot: collectors, strategies and executors ready to run.
pub struct Engine<E, A> {
    pub(super) shared: Arc<Shared>,
    pub(super) fatal_rx: mpsc::UnboundedReceiver<ExitReason>,
    pub(super) collector_tasks: Vec<BoxFuture<'static, ()>>,
    pub(super) strategy_tasks: Vec<(BoxFuture<'static, ()>, oneshot::Receiver<bool>)>,
    pub(super) executor_tasks: Vec<BoxFuture<'static, ()>>,
    pub(super) shutdown_timeout: Duration,
    pub(super) handle_signals: bool,
    #[cfg(feature = "http")]
    pub(super) http: Option<crate::http::HttpOpts>,
    pub(super) _types: PhantomData<fn(E) -> A>,
}

impl<E, A> Engine<E, A>
where
    E: Send + Sync + 'static,
    A: Send + Sync + 'static,
{
    /// Starts configuring an engine.
    pub fn builder() -> EngineBuilder<E, A> {
        EngineBuilder::new()
    }

    /// A handle to stop or inspect the engine.
    pub fn handle(&self) -> EngineHandle {
        EngineHandle::new(Arc::clone(&self.shared))
    }

    /// Runs until shutdown or a fatal error.
    ///
    /// Start order: executors, strategies (`init`), collectors. Shutdown order: collectors stop,
    /// strategies drain and get `ShutdownRequested`, opportunity tasks are cancelled, executors
    /// drain. The whole shutdown is bounded by the shutdown timeout.
    pub async fn run(self) -> ExitReason {
        let Engine {
            shared,
            mut fatal_rx,
            collector_tasks,
            strategy_tasks,
            executor_tasks,
            shutdown_timeout,
            handle_signals,
            #[cfg(feature = "http")]
            http,
            _types,
        } = self;

        #[cfg(feature = "http")]
        let http_task = http.map(|opts| {
            tokio::spawn(crate::http::serve(
                opts,
                EngineHandle::new(Arc::clone(&shared)),
            ))
        });

        let executors: Vec<JoinHandle<()>> = executor_tasks.into_iter().map(tokio::spawn).collect();
        let mut strategies = Vec::new();
        let mut ready = Vec::new();
        for (task, ready_rx) in strategy_tasks {
            strategies.push(tokio::spawn(task));
            ready.push(ready_rx);
        }

        let mut init_ok = true;
        for rx in ready {
            if !rx.await.unwrap_or(false) {
                init_ok = false;
                break;
            }
        }

        let mut collectors: Vec<JoinHandle<()>> = Vec::new();
        let reason = if init_ok {
            collectors = collector_tasks.into_iter().map(tokio::spawn).collect();
            tracing::info!("engine running");
            tokio::select! {
                biased;
                r = fatal_rx.recv() => r.unwrap_or(ExitReason::Shutdown),
                () = shared.request.cancelled() => ExitReason::Shutdown,
                () = wait_for_signal(handle_signals) => ExitReason::Shutdown,
            }
        } else {
            drop(collector_tasks);
            fatal_rx.try_recv().unwrap_or(ExitReason::Shutdown)
        };

        tracing::info!(%reason, "shutting down");
        let deadline = Instant::now() + shutdown_timeout;
        let _ = shared.deadline.set(deadline);
        shared.shutdown.cancel();

        shared.collectors_stop.cancel();
        join_until(collectors, deadline, "collector").await;

        shared.strategies_stop.cancel();
        join_until(strategies, deadline, "strategy").await;

        shared.tasks_stop.cancel();
        shared.tracker.close();
        if tokio::time::timeout_at(deadline, shared.tracker.wait())
            .await
            .is_err()
        {
            tracing::warn!("opportunity tasks did not finish before the shutdown timeout");
        }

        shared.executors_stop.cancel();
        join_until(executors, deadline, "executor").await;

        #[cfg(feature = "http")]
        if let Some(task) = http_task {
            task.abort();
        }
        tracing::info!(%reason, "engine stopped");
        reason
    }
}

async fn join_until(handles: Vec<JoinHandle<()>>, deadline: Instant, kind: &'static str) {
    for mut handle in handles {
        if tokio::time::timeout_at(deadline, &mut handle)
            .await
            .is_err()
        {
            tracing::warn!(
                kind,
                "task did not stop before the shutdown timeout, aborting"
            );
            handle.abort();
        }
    }
}

#[cfg_attr(not(feature = "signal"), allow(unused_variables))]
async fn wait_for_signal(enabled: bool) {
    #[cfg(feature = "signal")]
    if enabled {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut term = signal(SignalKind::terminate()).ok();
            let term = async {
                match term.as_mut() {
                    Some(t) => {
                        t.recv().await;
                    }
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                r = tokio::signal::ctrl_c() => if r.is_err() { std::future::pending::<()>().await },
                () = term => {}
            }
            tracing::info!("signal received");
            return;
        }
        #[cfg(not(unix))]
        {
            if tokio::signal::ctrl_c().await.is_err() {
                std::future::pending::<()>().await;
            }
            return;
        }
    }
    std::future::pending().await
}
