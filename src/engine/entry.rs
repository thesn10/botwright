use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::sync::oneshot;

use super::collector_task::{self, CollectorRuntime};
use super::executor_task::{self, ExecutorRuntime};
use super::router::HandlesFn;
use super::strategy_task::{self, StrategyRuntime};
use crate::{Collector, CollectorOpts, Executor, ExecutorOpts, Strategy};

pub(super) trait CollectorEntry<E>: Send {
    fn name(&self) -> &str;
    fn exhausted_probe(&self) -> Box<dyn Fn() -> bool + Send + Sync>;
    fn into_task(self: Box<Self>, rt: CollectorRuntime<E>) -> BoxFuture<'static, ()>;
}

pub(super) struct CollectorSlot<C> {
    pub(super) collector: Arc<C>,
    pub(super) opts: CollectorOpts,
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

pub(super) trait StrategyEntry<E, A>: Send {
    fn name(&self) -> &str;
    fn has_factory(&self) -> bool;
    fn into_task(
        self: Box<Self>,
        rt: StrategyRuntime<E, A>,
        ready: oneshot::Sender<bool>,
    ) -> BoxFuture<'static, ()>;
}

pub(super) struct StrategySlot<S, F> {
    pub(super) strategy: S,
    pub(super) factory: Option<F>,
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

pub(super) trait ExecutorEntry<A>: Send {
    fn name(&self) -> &str;
    fn queue_capacity(&self) -> usize;
    fn handles_fn(&self) -> HandlesFn<A>;
    fn into_task(self: Box<Self>, rt: ExecutorRuntime<A>) -> BoxFuture<'static, ()>;
}

pub(super) struct ExecutorSlot<X> {
    pub(super) executor: Arc<X>,
    pub(super) opts: ExecutorOpts,
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
