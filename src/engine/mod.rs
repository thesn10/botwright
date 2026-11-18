pub(crate) mod collector_task;
pub(crate) mod executor_task;
pub(crate) mod router;
pub(crate) mod signals;
pub(crate) mod strategy_task;

use std::collections::{BTreeMap, HashSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use self::collector_task::{CollectorRuntime, QueueSink, Sink, SinkKind, StrategyLink};
use self::executor_task::ExecutorRuntime;
use self::router::{HandlesFn, Route, Router};
use self::signals::Signals;
use self::strategy_task::{BroadcastRx, StrategyRuntime};
use crate::delivery::Subscription;
use crate::notify::NotifierHandle;
use crate::obs;
use crate::{
    BuildError, Collector, CollectorOpts, Delivery, Envelope, Executor, ExecutorOpts, ExitReason,
    OnPanic, SourceId, Strategy, Subscriptions,
};

pub(crate) type Fatal = mpsc::UnboundedSender<ExitReason>;

pub(crate) struct CollectorState {
    pub(crate) name: SourceId,
    pub(crate) connected: AtomicBool,
    pub(crate) done: AtomicBool,
    pub(crate) last_event: AtomicU64,
    exhausted: Box<dyn Fn() -> bool + Send + Sync>,
}

pub(crate) struct StrategyState {
    pub(crate) idle: AtomicBool,
    pub(crate) tasks_active: Arc<AtomicUsize>,
}

pub(crate) struct ExecutorState {
    pub(crate) pending: AtomicUsize,
}

pub(crate) struct Shared {
    started: Instant,
    pub(crate) collectors: Vec<CollectorState>,
    pub(crate) strategies: Vec<StrategyState>,
    pub(crate) executors: Vec<ExecutorState>,
    pub(crate) request: CancellationToken,
    pub(crate) shutdown: CancellationToken,
    pub(crate) collectors_stop: CancellationToken,
    pub(crate) strategies_stop: CancellationToken,
    pub(crate) tasks_stop: CancellationToken,
    pub(crate) executors_stop: CancellationToken,
    pub(crate) tracker: TaskTracker,
    deadline: OnceLock<Instant>,
}

impl Shared {
    pub(crate) fn millis_since_start(&self) -> u64 {
        self.started.elapsed().as_millis() as u64 + 1
    }

    pub(crate) fn deadline(&self) -> Instant {
        self.deadline.get().copied().unwrap_or_else(Instant::now)
    }

    fn is_idle(&self) -> bool {
        self.collectors
            .iter()
            .all(|c| c.done.load(Ordering::Acquire) || (c.exhausted)())
            && self.strategies.iter().all(|s| {
                s.idle.load(Ordering::Acquire) && s.tasks_active.load(Ordering::Acquire) == 0
            })
            && self
                .executors
                .iter()
                .all(|x| x.pending.load(Ordering::Acquire) == 0)
    }
}

/// Health of one collector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectorHealth {
    /// Collector name.
    pub name: String,
    /// Whether its stream is currently established.
    pub connected: bool,
    /// Whether the collector is exhausted and stopped.
    pub finished: bool,
    /// Time since its last event, `None` before the first event.
    pub last_event_age: Option<Duration>,
}

/// Controls a running engine. Cheap to clone.
#[derive(Clone)]
pub struct EngineHandle {
    shared: Arc<Shared>,
}

impl EngineHandle {
    /// Starts an orderly shutdown; [`Engine::run`] then returns [`ExitReason::Shutdown`].
    pub fn shutdown(&self) {
        self.shared.request.cancel();
    }

    /// Whether shutdown has started.
    pub fn is_shutting_down(&self) -> bool {
        self.shared.shutdown.is_cancelled()
    }

    /// True when every collector is exhausted, no strategy has queued events or running tasks
    /// and no action is queued or executing.
    pub fn is_idle(&self) -> bool {
        self.shared.is_idle()
    }

    /// Time since the engine was built.
    pub fn uptime(&self) -> Duration {
        self.shared.started.elapsed()
    }

    /// Connection state and event age of every collector.
    pub fn health(&self) -> Vec<CollectorHealth> {
        let now = self.shared.millis_since_start();
        self.shared
            .collectors
            .iter()
            .map(|c| {
                let last = c.last_event.load(Ordering::Acquire);
                CollectorHealth {
                    name: c.name.to_string(),
                    connected: c.connected.load(Ordering::Acquire),
                    finished: c.done.load(Ordering::Acquire),
                    last_event_age: (last > 0)
                        .then(|| Duration::from_millis(now.saturating_sub(last))),
                }
            })
            .collect()
    }
}

trait CollectorEntry<E>: Send {
    fn name(&self) -> &str;
    fn exhausted_probe(&self) -> Box<dyn Fn() -> bool + Send + Sync>;
    fn into_task(self: Box<Self>, rt: CollectorRuntime<E>) -> BoxFuture<'static, ()>;
}

struct CollectorSlot<C> {
    collector: Arc<C>,
    opts: CollectorOpts,
}

impl<E, C> CollectorEntry<E> for CollectorSlot<C>
where
    E: Send + Sync + 'static,
    C: Collector<E>,
{
    fn name(&self) -> &str {
        self.collector.name()
    }

    fn exhausted_probe(&self) -> Box<dyn Fn() -> bool + Send + Sync> {
        let collector = Arc::clone(&self.collector);
        Box::new(move || collector.is_exhausted())
    }

    fn into_task(self: Box<Self>, rt: CollectorRuntime<E>) -> BoxFuture<'static, ()> {
        Box::pin(collector_task::run_collector(self.collector, self.opts, rt))
    }
}

trait StrategyEntry<E, A>: Send {
    fn name(&self) -> &str;
    fn has_factory(&self) -> bool;
    fn into_task(
        self: Box<Self>,
        rt: StrategyRuntime<E, A>,
        ready: oneshot::Sender<bool>,
    ) -> BoxFuture<'static, ()>;
}

struct StrategySlot<S, F> {
    strategy: S,
    factory: Option<F>,
}

impl<E, A, S, F> StrategyEntry<E, A> for StrategySlot<S, F>
where
    E: Send + Sync + 'static,
    A: Send + Sync + 'static,
    S: Strategy<E, A>,
    F: FnMut() -> S + Send + 'static,
{
    fn name(&self) -> &str {
        self.strategy.name()
    }

    fn has_factory(&self) -> bool {
        self.factory.is_some()
    }

    fn into_task(
        self: Box<Self>,
        rt: StrategyRuntime<E, A>,
        ready: oneshot::Sender<bool>,
    ) -> BoxFuture<'static, ()> {
        Box::pin(strategy_task::run_strategy(
            self.strategy,
            self.factory,
            rt,
            ready,
        ))
    }
}

trait ExecutorEntry<A>: Send {
    fn name(&self) -> &str;
    fn queue_capacity(&self) -> usize;
    fn handles_fn(&self) -> HandlesFn<A>;
    fn into_task(self: Box<Self>, rt: ExecutorRuntime<A>) -> BoxFuture<'static, ()>;
}

struct ExecutorSlot<X> {
    executor: Arc<X>,
    opts: ExecutorOpts,
}

impl<A, X> ExecutorEntry<A> for ExecutorSlot<X>
where
    A: Send + Sync + 'static,
    X: Executor<A>,
{
    fn name(&self) -> &str {
        self.executor.name()
    }

    fn queue_capacity(&self) -> usize {
        self.opts.queue_capacity
    }

    fn handles_fn(&self) -> HandlesFn<A> {
        let executor = Arc::clone(&self.executor);
        Box::new(move |action| executor.handles(action))
    }

    fn into_task(self: Box<Self>, rt: ExecutorRuntime<A>) -> BoxFuture<'static, ()> {
        Box::pin(executor_task::run_executor(self.executor, self.opts, rt))
    }
}

struct StrategyRegistration<E, A> {
    entry: Box<dyn StrategyEntry<E, A>>,
    subs: Subscriptions<E>,
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
    fn new() -> Self {
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

type SharedBroadcast<E> = (broadcast::Sender<Envelope<E>>, Vec<Arc<Signals>>);
type SharedWatch<E> = (watch::Sender<Option<Envelope<E>>>, Vec<Arc<Signals>>);

struct CollectorWiring<E> {
    sinks: Vec<Sink<E>>,
    links: Vec<StrategyLink<E>>,
    broadcast_capacity: usize,
    broadcast: Option<SharedBroadcast<E>>,
    watch: Option<SharedWatch<E>>,
}

impl<E> CollectorWiring<E> {
    fn finish(mut self) -> (Vec<Sink<E>>, Vec<StrategyLink<E>>) {
        if let Some((tx, wakers)) = self.broadcast {
            self.sinks.push(Sink {
                filter: None,
                kind: SinkKind::Broadcast { tx, wakers },
            });
        }
        if let Some((tx, wakers)) = self.watch {
            self.sinks.push(Sink {
                filter: None,
                kind: SinkKind::Watch { tx, wakers },
            });
        }
        (self.sinks, self.links)
    }
}

struct Wiring<E> {
    collectors: Vec<CollectorWiring<E>>,
    lagging_threshold: f64,
}

impl<E: Send + Sync + 'static> Wiring<E> {
    fn new<A>(
        collectors: &[SourceId],
        strategies: &[StrategyRegistration<E, A>],
        lagging_threshold: f64,
    ) -> Self {
        let collectors = collectors
            .iter()
            .map(|id| {
                let broadcast_capacity = strategies
                    .iter()
                    .flat_map(|reg| &reg.subs.subs)
                    .filter(|sub| &sub.source == id && sub.filter.is_none())
                    .filter_map(|sub| match sub.delivery {
                        Delivery::DropOldest { capacity } => Some(capacity),
                        _ => None,
                    })
                    .max()
                    .unwrap_or(1);
                CollectorWiring {
                    sinks: Vec::new(),
                    links: Vec::new(),
                    broadcast_capacity,
                    broadcast: None,
                    watch: None,
                }
            })
            .collect();
        Self {
            collectors,
            lagging_threshold,
        }
    }

    fn strategy<A: Send + Sync + 'static>(
        &mut self,
        index: usize,
        reg: &StrategyRegistration<E, A>,
        shared: &Arc<Shared>,
        fatal: &Fatal,
        router: &Arc<Router<A>>,
        notifier: &NotifierHandle,
    ) -> StrategyRuntime<E, A> {
        let name: Arc<str> = reg.entry.name().into();
        let subs = &reg.subs.subs;
        let mut sources: Vec<SourceId> = Vec::new();
        for sub in subs {
            if !sources.contains(&sub.source) {
                sources.push(sub.source.clone());
            }
        }
        let (signals, wake) = Signals::new(&sources);
        let signals = Arc::new(signals);
        let max_cap = |pred: fn(&Delivery) -> bool| {
            subs.iter()
                .filter(|s| pred(&s.delivery))
                .filter_map(|s| s.delivery.capacity())
                .max()
        };
        let lossless = max_cap(|d| matches!(d, Delivery::Lossless { .. })).map(mpsc::channel);
        let drop_newest = max_cap(|d| matches!(d, Delivery::DropNewest { .. })).map(mpsc::channel);
        let (system_tx, system_rx) = mpsc::unbounded_channel();
        let mut broadcasts = Vec::new();
        let mut watches = Vec::new();
        let collector_of = |source: &SourceId| {
            shared
                .collectors
                .iter()
                .position(|c| &c.name == source)
                .expect("validated")
        };

        for sub in subs {
            let wiring = &mut self.collectors[collector_of(&sub.source)];
            let slot = signals.slot_of(&sub.source);
            let Subscription {
                source,
                delivery,
                filter,
                on_full,
            } = sub;
            let labels = [("source", source.as_str()), ("strategy", &*name)];
            let dropped = || {
                obs::counter(
                    obs::DROPPED,
                    &[
                        ("source", source.as_str()),
                        ("strategy", &*name),
                        ("delivery", delivery.label()),
                    ],
                )
            };
            match *delivery {
                Delivery::Lossless { .. } | Delivery::DropNewest { .. } => {
                    let lossless_kind = matches!(delivery, Delivery::Lossless { .. });
                    let channel = if lossless_kind {
                        &lossless
                    } else {
                        &drop_newest
                    };
                    let tx = channel
                        .as_ref()
                        .expect("channel exists for this delivery")
                        .0
                        .clone();
                    let capacity = tx.max_capacity();
                    let high = ((capacity as f64 * self.lagging_threshold).ceil() as usize)
                        .clamp(1, capacity);
                    let queue = QueueSink {
                        tx,
                        lossless: lossless_kind,
                        on_full: *on_full,
                        strategy: Arc::clone(&name),
                        signals: Arc::clone(&signals),
                        slot,
                        high,
                        low: high / 2,
                        lagging: false,
                        depth: obs::gauge(obs::CHANNEL_DEPTH, &labels),
                        wait: obs::histogram(obs::DISPATCH_WAIT, &labels),
                        dropped: dropped(),
                    };
                    wiring.sinks.push(Sink {
                        filter: filter.clone(),
                        kind: SinkKind::Queue(queue),
                    });
                }
                Delivery::DropOldest { capacity } => {
                    let rx = match filter {
                        None => {
                            let cap = wiring.broadcast_capacity;
                            let (tx, wakers) = wiring
                                .broadcast
                                .get_or_insert_with(|| (broadcast::channel(cap).0, Vec::new()));
                            wakers.push(Arc::clone(&signals));
                            tx.subscribe()
                        }
                        Some(filter) => {
                            let (tx, rx) = broadcast::channel(capacity);
                            let kind = SinkKind::Broadcast {
                                tx,
                                wakers: vec![Arc::clone(&signals)],
                            };
                            wiring.sinks.push(Sink {
                                filter: Some(filter.clone()),
                                kind,
                            });
                            rx
                        }
                    };
                    broadcasts.push(BroadcastRx {
                        slot,
                        rx,
                        dropped: dropped(),
                    });
                }
                Delivery::Latest => {
                    let rx = match filter {
                        None => {
                            let (tx, wakers) = wiring
                                .watch
                                .get_or_insert_with(|| (watch::channel(None).0, Vec::new()));
                            wakers.push(Arc::clone(&signals));
                            tx.subscribe()
                        }
                        Some(filter) => {
                            let (tx, rx) = watch::channel(None);
                            let kind = SinkKind::Watch {
                                tx,
                                wakers: vec![Arc::clone(&signals)],
                            };
                            wiring.sinks.push(Sink {
                                filter: Some(filter.clone()),
                                kind,
                            });
                            rx
                        }
                    };
                    watches.push(rx);
                }
            }
        }

        for source in &sources {
            let has_lossless = subs
                .iter()
                .any(|s| &s.source == source && matches!(s.delivery, Delivery::Lossless { .. }));
            let inline = if has_lossless {
                lossless.as_ref().map(|(tx, _)| tx.clone())
            } else {
                None
            };
            self.collectors[collector_of(source)]
                .links
                .push(StrategyLink {
                    inline,
                    side: system_tx.clone(),
                });
        }

        StrategyRuntime {
            index,
            name,
            lossless: lossless.map(|(_, rx)| rx),
            drop_newest: drop_newest.map(|(_, rx)| rx),
            broadcasts,
            watches,
            system: system_rx,
            wake,
            signals,
            on_error: reg.subs.on_error,
            on_panic: reg.subs.on_panic,
            max_tasks: reg.subs.max_tasks,
            router: Arc::clone(router),
            notifier: notifier.clone(),
            shared: Arc::clone(shared),
            fatal: fatal.clone(),
            rotation: 0,
        }
    }
}

/// The wired bot: collectors, strategies and executors ready to run.
pub struct Engine<E, A> {
    shared: Arc<Shared>,
    fatal_rx: mpsc::UnboundedReceiver<ExitReason>,
    collector_tasks: Vec<BoxFuture<'static, ()>>,
    strategy_tasks: Vec<(BoxFuture<'static, ()>, oneshot::Receiver<bool>)>,
    executor_tasks: Vec<BoxFuture<'static, ()>>,
    shutdown_timeout: Duration,
    handle_signals: bool,
    #[cfg(feature = "http")]
    http: Option<crate::http::HttpOpts>,
    _types: PhantomData<fn(E) -> A>,
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
        EngineHandle {
            shared: Arc::clone(&self.shared),
        }
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
                EngineHandle {
                    shared: Arc::clone(&shared),
                },
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
