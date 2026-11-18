use std::collections::{BTreeMap, HashSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::collector_task::CollectorRuntime;
use super::entry::{
    CollectorEntry, CollectorSlot, ExecutorEntry, ExecutorSlot, StrategyEntry, StrategySlot,
};
use super::executor_task::ExecutorRuntime;
use super::router::{Route, Router};
use super::wiring::Wiring;
use super::{CollectorState, Engine, ExecutorState, Shared, StrategyState};
use crate::notify::NotifierHandle;
use crate::{
    BuildError, Collector, CollectorOpts, Executor, ExecutorOpts, OnPanic, SourceId, Strategy,
    Subscriptions,
};

pub(super) struct StrategyRegistration<E, A> {
    pub(super) entry: Box<dyn StrategyEntry<E, A>>,
    pub(super) subs: Subscriptions<E>,
}

/// Configures an [`Engine`].
pub struct EngineBuilder<E, A> {
    collectors: Vec<Box<dyn CollectorEntry<E>>>,
    strategies: Vec<StrategyRegistration<E, A>>,
    executors: Vec<Box<dyn ExecutorEntry<A>>>,
    shutdown_timeout: Duration,
    lagging_threshold: f64,
    notifier: Option<NotifierHandle>,
    handle_signals: bool,
    #[cfg(feature = "http")]
    http: Option<crate::http::HttpOpts>,
}

impl<E, A> EngineBuilder<E, A>
where
    E: Send + Sync + 'static,
    A: Send + Sync + 'static,
{
    pub(super) fn new() -> Self {
        Self {
            collectors: Vec::new(),
            strategies: Vec::new(),
            executors: Vec::new(),
            shutdown_timeout: Duration::from_secs(10),
            lagging_threshold: 0.8,
            notifier: None,
            handle_signals: cfg!(feature = "signal"),
            #[cfg(feature = "http")]
            http: None,
        }
    }

    /// Registers a collector. Its [`name`](Collector::name) is the source id strategies subscribe to.
    pub fn collector<C: Collector<E>>(mut self, collector: C, opts: CollectorOpts) -> Self {
        self.collectors.push(Box::new(CollectorSlot {
            collector: Arc::new(collector),
            opts,
        }));
        self
    }

    /// Registers a strategy with [`OnPanic::Abort`] semantics.
    pub fn strategy<S: Strategy<E, A>>(
        self,
        strategy: S,
        subscribe: impl FnOnce(Subscriptions<E>) -> Subscriptions<E>,
    ) -> Self {
        self.add_strategy(
            StrategySlot::<S, fn() -> S> {
                strategy,
                factory: None,
            },
            subscribe,
        )
    }

    /// Registers a strategy built by `factory`; allows [`OnPanic::Restart`].
    pub fn strategy_with_factory<S, F>(
        self,
        mut factory: F,
        subscribe: impl FnOnce(Subscriptions<E>) -> Subscriptions<E>,
    ) -> Self
    where
        S: Strategy<E, A>,
        F: FnMut() -> S + Send + 'static,
    {
        let strategy = factory();
        self.add_strategy(
            StrategySlot {
                strategy,
                factory: Some(factory),
            },
            subscribe,
        )
    }

    fn add_strategy<S, F>(
        mut self,
        slot: StrategySlot<S, F>,
        subscribe: impl FnOnce(Subscriptions<E>) -> Subscriptions<E>,
    ) -> Self
    where
        S: Strategy<E, A>,
        F: FnMut() -> S + Send + 'static,
    {
        let subs = subscribe(Subscriptions::new());
        self.strategies.push(StrategyRegistration {
            entry: Box::new(slot),
            subs,
        });
        self
    }

    /// Registers an executor.
    pub fn executor<X: Executor<A>>(mut self, executor: X, opts: ExecutorOpts) -> Self {
        self.executors.push(Box::new(ExecutorSlot {
            executor: Arc::new(executor),
            opts,
        }));
        self
    }

    /// Upper bound for the whole shutdown sequence, default 10 s.
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Fill level (fraction of capacity) at which `Lagging` is reported, default 0.8.
    pub fn lagging_threshold(mut self, fraction: f64) -> Self {
        self.lagging_threshold = fraction.clamp(0.0, 1.0);
        self
    }

    /// Notifier available to strategies via [`StrategyCtx::notifier`](crate::StrategyCtx::notifier);
    /// default [`LogNotifier`](crate::notify::LogNotifier).
    pub fn notifier(mut self, notifier: impl Into<NotifierHandle>) -> Self {
        self.notifier = Some(notifier.into());
        self
    }

    /// Shut down on SIGINT and SIGTERM; default `true` with feature `signal`.
    #[cfg(feature = "signal")]
    pub fn handle_signals(mut self, enabled: bool) -> Self {
        self.handle_signals = enabled;
        self
    }

    /// Serves `/healthz` and `/metrics` while the engine runs.
    #[cfg(feature = "http")]
    pub fn http(mut self, opts: crate::http::HttpOpts) -> Self {
        self.http = Some(opts);
        self
    }

    #[cfg(feature = "test-util")]
    pub(crate) fn disable_signals(mut self) -> Self {
        self.handle_signals = false;
        self
    }

    /// Validates the configuration and wires all channels.
    pub fn build(self) -> Result<Engine<E, A>, BuildError> {
        check_unique("collector", self.collectors.iter().map(|c| c.name()))?;
        check_unique("strategy", self.strategies.iter().map(|s| s.entry.name()))?;
        check_unique("executor", self.executors.iter().map(|x| x.name()))?;

        let collector_index: BTreeMap<String, usize> = self
            .collectors
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name().to_owned(), i))
            .collect();
        for reg in &self.strategies {
            let name = reg.entry.name();
            reg.subs.validate(name)?;
            for sub in &reg.subs.subs {
                if !collector_index.contains_key(sub.source.as_str()) {
                    return Err(BuildError::UnknownSource {
                        strategy: name.to_owned(),
                        source_name: sub.source.to_string(),
                    });
                }
            }
            if matches!(reg.subs.on_panic, OnPanic::Restart { .. }) && !reg.entry.has_factory() {
                return Err(BuildError::RestartWithoutFactory(name.to_owned()));
            }
        }

        let request = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let tasks_stop = CancellationToken::new();
        let shared = Arc::new(Shared {
            started: Instant::now(),
            collectors: self
                .collectors
                .iter()
                .map(|c| CollectorState {
                    name: SourceId::new(c.name()),
                    connected: AtomicBool::new(false),
                    done: AtomicBool::new(false),
                    last_event: AtomicU64::new(0),
                    exhausted: c.exhausted_probe(),
                })
                .collect(),
            strategies: self
                .strategies
                .iter()
                .map(|_| StrategyState {
                    idle: AtomicBool::new(false),
                    tasks_active: Arc::new(AtomicUsize::new(0)),
                })
                .collect(),
            executors: self
                .executors
                .iter()
                .map(|_| ExecutorState {
                    pending: AtomicUsize::new(0),
                })
                .collect(),
            request,
            shutdown,
            collectors_stop: CancellationToken::new(),
            strategies_stop: CancellationToken::new(),
            tasks_stop,
            executors_stop: CancellationToken::new(),
            tracker: TaskTracker::new(),
            deadline: OnceLock::new(),
        });
        let (fatal, fatal_rx) = mpsc::unbounded_channel();
        let notifier = self.notifier.unwrap_or_default();

        let mut routes = Vec::new();
        let mut executor_rx = Vec::new();
        for (index, entry) in self.executors.iter().enumerate() {
            let (tx, rx) = mpsc::channel(entry.queue_capacity());
            routes.push(Route {
                name: entry.name().into(),
                index,
                handles: entry.handles_fn(),
                tx,
            });
            executor_rx.push(rx);
        }
        let router = Arc::new(Router {
            routes,
            shared: Arc::clone(&shared),
        });

        let collector_ids: Vec<SourceId> =
            shared.collectors.iter().map(|c| c.name.clone()).collect();
        let mut wiring = Wiring::new(&collector_ids, &self.strategies, self.lagging_threshold);
        let mut strategy_runtimes = Vec::new();
        for (index, reg) in self.strategies.iter().enumerate() {
            strategy_runtimes
                .push(wiring.strategy(index, reg, &shared, &fatal, &router, &notifier));
        }

        let mut collector_tasks = Vec::new();
        for (index, (entry, wired)) in self
            .collectors
            .into_iter()
            .zip(wiring.collectors)
            .enumerate()
        {
            let (sinks, links) = wired.finish();
            let rt = CollectorRuntime {
                id: SourceId::new(entry.name()),
                index,
                sinks,
                links,
                shared: Arc::clone(&shared),
                fatal: fatal.clone(),
            };
            collector_tasks.push(entry.into_task(rt));
        }

        let mut strategy_tasks = Vec::new();
        for (reg, rt) in self.strategies.into_iter().zip(strategy_runtimes) {
            let (ready_tx, ready_rx) = oneshot::channel();
            strategy_tasks.push((reg.entry.into_task(rt, ready_tx), ready_rx));
        }

        let executor_tasks = self
            .executors
            .into_iter()
            .zip(executor_rx)
            .enumerate()
            .map(|(index, (entry, rx))| {
                entry.into_task(ExecutorRuntime {
                    index,
                    rx,
                    shared: Arc::clone(&shared),
                })
            })
            .collect();

        Ok(Engine {
            shared,
            fatal_rx,
            collector_tasks,
            strategy_tasks,
            executor_tasks,
            shutdown_timeout: self.shutdown_timeout,
            handle_signals: self.handle_signals,
            #[cfg(feature = "http")]
            http: self.http,
            _types: PhantomData,
        })
    }
}

fn check_unique<'a>(
    kind: &'static str,
    names: impl Iterator<Item = &'a str>,
) -> Result<(), BuildError> {
    let mut seen = HashSet::new();
    for name in names {
        if !seen.insert(name) {
            return Err(BuildError::DuplicateName {
                kind,
                name: name.to_owned(),
            });
        }
    }
    Ok(())
}
