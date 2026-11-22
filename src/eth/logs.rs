//! [`LogCollector`]: logs from a subscription, gaps from a [`LogBackfill`] page by page, and
//! removals for blocks that left the canonical chain.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt, TryStream, TryStreamExt, stream};
use itertools::Itertools;
use tokio::sync::broadcast::error::RecvError;

use super::alloy::consensus::BlockHeader;
use super::alloy::network::primitives::HeaderResponse;
use super::alloy::network::{Ethereum, Network};
use super::alloy::primitives::B256;
use super::alloy::providers::{DynProvider, Provider};
use super::alloy::pubsub::Subscription;
use super::alloy::rpc::types::{Filter, Log};
use super::alloy::transports::{RpcError, TransportErrorKind};
use super::follower::{ChainItem, ChainOpts, Follower, Recent};
use super::{ChainEvent, ProviderSource, transport};
use crate::{Collector, CollectorError, Item};

/// Resume position of a [`LogCollector`]: the last delivered log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogCursor {
    /// Block number.
    pub block: u64,
    /// Log index within the block.
    pub log_index: u64,
}

impl LogCursor {
    fn of(log: &Log) -> Option<Self> {
        Some(Self {
            block: log.block_number?,
            log_index: log.log_index?,
        })
    }

    fn before(self) -> Option<Self> {
        match self.log_index {
            0 => self.block.checked_sub(1).map(|block| Self {
                block,
                log_index: u64::MAX,
            }),
            i => Some(Self {
                block: self.block,
                log_index: i - 1,
            }),
        }
    }
}

/// Event of a [`LogCollector`].
#[derive(Debug, Clone, PartialEq)]
pub enum LogEvent {
    /// A new log.
    Added(Log),
    /// A previously delivered log whose block left the canonical chain; `removed` is set.
    Removed(Log),
}

/// Fetches the logs of a block range after a reconnect.
pub trait LogBackfill<N: Network = Ethereum>: Send + Sync + 'static {
    /// All logs matching `filter` in `from..=to`, ordered by block and log index.
    fn backfill(
        &self,
        provider: &DynProvider<N>,
        filter: &Filter,
        from: u64,
        to: u64,
    ) -> impl Future<Output = Result<Vec<Log>, CollectorError>> + Send;

    /// The logs of `from..=to` as consecutive pages, each ordered by block and log index.
    ///
    /// [`LogCollector`] pulls the next page only after the previous one was delivered, so a large
    /// gap is never held in memory at once. The default is a single page from
    /// [`backfill`](Self::backfill).
    fn pages(
        self: Arc<Self>,
        provider: DynProvider<N>,
        filter: Filter,
        from: u64,
        to: u64,
    ) -> impl Stream<Item = Result<Vec<Log>, CollectorError>> + Send + 'static {
        stream::once(async move { self.backfill(&provider, &filter, from, to).await })
    }
}

/// Backfills with `eth_getLogs` in ranges of at most `max_range` blocks.
///
/// A rate-limited request (HTTP 429 or 503, JSON-RPC code 429 or a rate-limit message) is retried
/// with the same range after a backoff. Any other failure counts as a range the node rejects and
/// halves the range. After a few pages in a row succeed at a reduced range, it doubles again, up to
/// `max_range`.
#[derive(Debug, Clone, Copy)]
pub struct RpcBackfill {
    max_range: u64,
}

impl RpcBackfill {
    /// Ranges of at most `max_range` blocks.
    pub fn new(max_range: u64) -> Self {
        Self {
            max_range: max_range.max(1),
        }
    }
}

impl Default for RpcBackfill {
    /// Ranges of 1000 blocks.
    fn default() -> Self {
        Self::new(1_000)
    }
}

/// Pages in a row that must succeed before a reduced range doubles; growing after every page would
/// send every other request with a size the node just rejected.
const GROW_AFTER: u32 = 4;
/// Backoffs on rate limits for one page before its error is returned.
const MAX_WAITS: u32 = 8;
const FIRST_WAIT: Duration = Duration::from_secs(1);
const MAX_WAIT: Duration = Duration::from_secs(30);

struct RpcPages<N: Network> {
    provider: DynProvider<N>,
    filter: Filter,
    start: u64,
    to: u64,
    range: u64,
    max_range: u64,
    /// Pages in a row that succeeded at `range`.
    streak: u32,
}

/// Why a request failed, which decides whether to wait or to shrink the range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Failure {
    RateLimited,
    Rejected,
}

struct Attempt {
    range: u64,
    end: u64,
    logs: Result<Vec<Log>, (Failure, CollectorError)>,
}

/// What to try after a failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retry {
    Wait(Duration),
    Halve,
}

#[derive(Debug, Clone, Copy)]
struct Plan {
    range: u64,
    waits: u32,
}

impl<N: Network> RpcPages<N> {
    async fn next(self) -> Result<Option<(Vec<Log>, Self)>, CollectorError> {
        if self.start > self.to {
            return Ok(None);
        }

        let attempt = self
            .settle(Plan {
                range: self.range,
                waits: 0,
            })
            .await;

        let logs = attempt.logs.map_err(|(_, err)| err)?;
        let streak = if attempt.range < self.range {
            1
        } else {
            self.streak + 1
        };
        let (range, streak) = grown(attempt.range, streak, self.max_range);
        let rest = Self {
            start: attempt.end + 1,
            range,
            streak,
            ..self
        };
        Ok(Some((logs, rest)))
    }

    /// Fetches one page, waiting out rate limits and halving rejected ranges, until it succeeds or
    /// neither helps any more.
    fn settle(&self, plan: Plan) -> Pin<Box<dyn Future<Output = Attempt> + Send + '_>> {
        Box::pin(async move {
            let attempt = self.fetch(plan.range).await;
            let failure = attempt.logs.as_ref().err().map(|(failure, _)| *failure);
            match failure.and_then(|failure| retry(failure, plan)) {
                None => attempt,
                Some(Retry::Wait(delay)) => {
                    tokio::time::sleep(delay).await;
                    self.settle(Plan {
                        waits: plan.waits + 1,
                        ..plan
                    })
                    .await
                }
                Some(Retry::Halve) => {
                    self.settle(Plan {
                        range: plan.range / 2,
                        ..plan
                    })
                    .await
                }
            }
        })
    }

    fn fetch(&self, range: u64) -> impl Future<Output = Attempt> + Send + use<N> {
        let end = self.to.min(self.start.saturating_add(range - 1));
        let filter = self.filter.clone().from_block(self.start).to_block(end);
        let provider = self.provider.clone();
        async move {
            let logs = provider
                .get_logs(&filter)
                .await
                .map(|logs| {
                    logs.into_iter()
                        .sorted_by_key(|log| (log.block_number, log.log_index))
                        .collect()
                })
                .map_err(|err| {
                    let failure = failure(&err);
                    tracing::debug!(error = %err, range, ?failure, "eth_getLogs failed");
                    (failure, transport(err))
                });
            Attempt { range, end, logs }
        }
    }
}

fn retry(failure: Failure, plan: Plan) -> Option<Retry> {
    match failure {
        Failure::RateLimited if plan.waits < MAX_WAITS => Some(Retry::Wait(
            FIRST_WAIT.saturating_mul(1 << plan.waits).min(MAX_WAIT),
        )),
        Failure::Rejected if plan.range > 1 => Some(Retry::Halve),
        _ => None,
    }
}

/// The range and streak for the next page after a page succeeded at `range`.
fn grown(range: u64, streak: u32, max_range: u64) -> (u64, u32) {
    if range < max_range && streak >= GROW_AFTER {
        (range.saturating_mul(2).min(max_range), 0)
    } else {
        (range, streak)
    }
}

/// alloy's own `is_retry_err` also retries JSON-RPC -32005, which Infura returns for too many logs too.
fn failure(err: &RpcError<TransportErrorKind>) -> Failure {
    let limited = match err {
        RpcError::Transport(kind) => kind.is_retry_err(),
        RpcError::ErrorResp(payload) => payload.code == 429 || rate_limit_message(&payload.message),
        _ => false,
    };
    if limited {
        Failure::RateLimited
    } else {
        Failure::Rejected
    }
}

fn rate_limit_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "rate limit",
        "rate exceeded",
        "too many requests",
        "credits limited",
        "request limit reached",
    ]
    .iter()
    .any(|phrase| message.contains(phrase))
}

impl<N: Network> LogBackfill<N> for RpcBackfill {
    async fn backfill(
        &self,
        provider: &DynProvider<N>,
        filter: &Filter,
        from: u64,
        to: u64,
    ) -> Result<Vec<Log>, CollectorError> {
        Arc::new(*self)
            .pages(provider.clone(), filter.clone(), from, to)
            .try_concat()
            .await
    }

    fn pages(
        self: Arc<Self>,
        provider: DynProvider<N>,
        filter: Filter,
        from: u64,
        to: u64,
    ) -> impl Stream<Item = Result<Vec<Log>, CollectorError>> + Send + 'static {
        let pages = RpcPages {
            provider,
            filter,
            start: from,
            to,
            range: self.max_range,
            max_range: self.max_range,
            streak: 0,
        };
        stream::try_unfold(pages, RpcPages::next)
    }
}

/// Subscribes to logs and, after a reconnect, backfills everything since the last cursor with a
/// [`LogBackfill`], dropping duplicates.
///
/// The backfill arrives page by page; the next page is fetched only after the previous one was
/// delivered. Before [`GapFilled`](Item::GapFilled) it backfills again up to the new head. When the
/// subscription buffer overflows while the strategy is busy, it backfills from the last delivered
/// block the same way.
///
/// Alongside the logs it follows the canonical chain like a [`BlockCollector`](super::BlockCollector).
/// When a block with delivered logs leaves the canonical chain, its logs arrive again as
/// [`LogEvent::Removed`] (newest first) and the logs of the new chain follow as `Added`. Logs the
/// node itself marks `removed: true` are reported the same way, once.
///
/// Implements `Collector<E>` for every `E: From<LogEvent>`.
pub struct LogCollector<N: Network = Ethereum, B = RpcBackfill> {
    name: String,
    source: ProviderSource<N>,
    filter: Filter,
    backfill: Arc<B>,
    start_block: Option<u64>,
    chain: ChainOpts,
    recent: Recent,
    delivered: Arc<Mutex<Delivered>>,
}

impl LogCollector<Ethereum, RpcBackfill> {
    /// Opens a fresh WebSocket connection on every connect; for other networks use
    /// [`from_source`](Self::from_source) with [`ProviderSource::ws`].
    pub fn ws(name: impl Into<String>, url: impl Into<String>, filter: Filter) -> Self {
        Self::from_source(name, ProviderSource::ws(url), filter)
    }
}

impl<N: Network> LogCollector<N, RpcBackfill> {
    /// Uses `provider` for every connection.
    pub fn new(
        name: impl Into<String>,
        provider: impl Provider<N> + 'static,
        filter: Filter,
    ) -> Self {
        Self::from_source(name, ProviderSource::shared(provider), filter)
    }

    /// Uses the given provider source.
    pub fn from_source(name: impl Into<String>, source: ProviderSource<N>, filter: Filter) -> Self {
        Self {
            name: name.into(),
            source,
            filter,
            backfill: Arc::new(RpcBackfill::default()),
            start_block: None,
            chain: ChainOpts::default(),
            recent: Recent::default(),
            delivered: Arc::default(),
        }
    }
}

impl<N: Network, B: LogBackfill<N>> LogCollector<N, B> {
    /// Replaces the backfill strategy.
    pub fn backfill<B2: LogBackfill<N>>(self, backfill: B2) -> LogCollector<N, B2> {
        LogCollector {
            name: self.name,
            source: self.source,
            filter: self.filter,
            backfill: Arc::new(backfill),
            start_block: self.start_block,
            chain: self.chain,
            recent: self.recent,
            delivered: self.delivered,
        }
    }

    /// On the first connect, also deliver historic logs from this block on.
    pub fn start_block(mut self, block: u64) -> Self {
        self.start_block = Some(block);
        self
    }

    /// How many recent blocks are watched for reorgs, default 64.
    pub fn max_reorg_depth(mut self, depth: u64) -> Self {
        self.chain.max_reorg_depth = depth;
        self
    }
}

#[derive(Default)]
struct Delivered {
    blocks: BTreeMap<u64, (B256, Vec<Log>)>,
    newest: u64,
}

impl Delivered {
    fn covers(&self, block: u64, depth: u64) -> bool {
        self.newest > 0 && block.saturating_add(depth) >= self.newest
    }

    fn advance(&mut self, block: u64, depth: u64) {
        self.newest = self.newest.max(block);
        let floor = self.newest.saturating_sub(depth);
        self.blocks = self.blocks.split_off(&floor);
    }
}

type LiveStream = Pin<Box<dyn Stream<Item = Live> + Send>>;
type PageStream = Pin<Box<dyn Stream<Item = Result<Vec<Log>, CollectorError>> + Send>>;
type HeadItems<H> = Pin<Box<dyn Stream<Item = Result<ChainItem<H>, CollectorError>> + Send>>;

enum Live {
    Log(Box<Log>),
    Lagged(u64),
}

struct Gap {
    from: u64,
    to: u64,
    pages: PageStream,
}

struct LogState<N: Network, B> {
    provider: DynProvider<N>,
    live: LiveStream,
    heads: HeadItems<N::HeaderResponse>,
    filter: Filter,
    backfill: Arc<B>,
    depth: u64,
    delivered: Arc<Mutex<Delivered>>,
    gap: Option<Gap>,
    floor: u64,
    repair_from: Option<u64>,
    queued: VecDeque<Item<LogEvent, LogCursor>>,
    last: Option<LogCursor>,
}

impl<N: Network, B: LogBackfill<N>> LogState<N, B> {
    async fn next_item(&mut self) -> Result<Option<Item<LogEvent, LogCursor>>, CollectorError> {
        loop {
            if let Some(item) = self.queued.pop_front() {
                return Ok(Some(item));
            }
            if let Some(gap) = self.gap.as_mut() {
                match gap.pages.try_next().await? {
                    Some(page) => page.into_iter().for_each(|log| self.added(log)),
                    None => self.close_gap().await?,
                }
                continue;
            }
            if let Some(from) = self.repair_from.take() {
                self.repair(from).await?;
                continue;
            }
            tokio::select! {
                biased;
                head = self.heads.next() => match head {
                    Some(item) => self.on_chain(item?).await?,
                    None => return Ok(None),
                },
                live = self.live.next() => match live {
                    Some(Live::Log(log)) if log.removed => self.removed(&log),
                    Some(Live::Log(log)) => self.added(*log),
                    Some(Live::Lagged(missed)) => self.refill(missed).await?,
                    None => return Ok(None),
                },
            }
        }
    }

    fn pages(&self, from: u64, to: u64) -> PageStream {
        Box::pin(Arc::clone(&self.backfill).pages(
            self.provider.clone(),
            self.filter.clone(),
            from,
            to,
        ))
    }

    fn open_gap(&mut self, from: u64, to: u64) {
        let pages = self.pages(from, to);
        self.gap = Some(Gap { from, to, pages });
    }

    async fn close_gap(&mut self) -> Result<(), CollectorError> {
        let Some(mut gap) = self.gap.take() else {
            return Ok(());
        };
        let head = self.provider.get_block_number().await.map_err(transport)?;
        if head > gap.to {
            gap.pages = self.pages(gap.to + 1, head);
            gap.to = head;
            self.gap = Some(gap);
        } else {
            self.queued.push_back(Item::GapFilled {
                from: gap.from,
                to: gap.to,
            });
        }
        Ok(())
    }

    async fn refill(&mut self, missed: u64) -> Result<(), CollectorError> {
        let from = self.last.map_or(self.floor, |last| last.block);
        let head = self.provider.get_block_number().await.map_err(transport)?;
        tracing::info!(missed, from, head, "log subscription lagged, backfilling");
        if head >= from {
            self.open_gap(from, head);
        }
        Ok(())
    }

    async fn on_chain(&mut self, item: ChainItem<N::HeaderResponse>) -> Result<(), CollectorError> {
        match item {
            Item::Event {
                event: ChainEvent::Block(header),
                ..
            } => {
                let (number, hash) = (header.number(), header.hash());
                let stale = {
                    let mut delivered = self.delivered.lock().expect("lock");
                    delivered.advance(number, self.depth);
                    delivered
                        .blocks
                        .get(&number)
                        .is_some_and(|(h, _)| *h != hash)
                };
                if stale {
                    self.remove_block(number, None);
                }
            }
            Item::Event {
                event:
                    ChainEvent::Reorg {
                        common_ancestor,
                        dropped,
                    },
                ..
            } => {
                dropped
                    .iter()
                    .rev()
                    .for_each(|block| self.remove_block(block.number, Some(block.hash)));
                self.schedule_repair(common_ancestor.number + 1);
            }
            Item::GapFilled { .. } | Item::GapUnfilled { .. } => {}
        }
        Ok(())
    }

    async fn repair(&mut self, from: u64) -> Result<(), CollectorError> {
        let head = self.provider.get_block_number().await.map_err(transport)?;
        if head < from {
            return Ok(());
        }
        let logs = RpcBackfill::default()
            .backfill(&self.provider, &self.filter, from, head)
            .await?;
        logs.into_iter().for_each(|log| self.added(log));
        Ok(())
    }

    fn schedule_repair(&mut self, from: u64) {
        self.repair_from = Some(self.repair_from.map_or(from, |pending| pending.min(from)));
    }

    fn remove_block(&mut self, number: u64, hash: Option<B256>) {
        let logs = {
            let mut delivered = self.delivered.lock().expect("lock");
            match delivered.blocks.get(&number) {
                Some((h, _)) if hash.is_none_or(|hash| hash == *h) => {
                    delivered.blocks.remove(&number).map(|(_, logs)| logs)
                }
                _ => None,
            }
        };
        let Some(logs) = logs else { return };
        let rewound = LogCursor {
            block: number,
            log_index: 0,
        }
        .before();
        if self.last.is_some_and(|last| last.block >= number) {
            self.last = rewound;
        }
        let cursor = self.last;
        self.queued.extend(logs.into_iter().rev().map(|mut log| {
            log.removed = true;
            Item::Event {
                event: LogEvent::Removed(log),
                cursor,
            }
        }));
    }

    fn added(&mut self, log: Log) {
        let (Some(position), Some(hash)) = (LogCursor::of(&log), log.block_hash) else {
            tracing::debug!(?log, "skipping log without position");
            return;
        };
        let stale = {
            let delivered = self.delivered.lock().expect("lock");
            if delivered.covers(position.block, self.depth) {
                match delivered.blocks.get(&position.block) {
                    Some((h, logs)) if *h == hash => {
                        if logs.iter().any(|l| l.log_index == log.log_index) {
                            return;
                        }
                        false
                    }
                    Some(_) => true,
                    None => false,
                }
            } else {
                if self.last.is_some_and(|last| position <= last) {
                    return;
                }
                false
            }
        };
        if stale {
            self.remove_from(position.block, None);
        }
        {
            let mut delivered = self.delivered.lock().expect("lock");
            let entry = delivered
                .blocks
                .entry(position.block)
                .or_insert_with(|| (hash, Vec::new()));
            entry.1.push(log.clone());
            delivered.advance(position.block, self.depth);
        }
        self.last = Some(position);
        self.queued
            .push_back(Item::new(LogEvent::Added(log), position));
    }

    fn removed(&mut self, log: &Log) {
        let (Some(number), Some(hash)) = (log.block_number, log.block_hash) else {
            return;
        };
        let known = self
            .delivered
            .lock()
            .expect("lock")
            .blocks
            .get(&number)
            .is_some_and(|(h, logs)| {
                *h == hash && logs.iter().any(|l| l.log_index == log.log_index)
            });
        if known {
            self.remove_from(number, Some(hash));
        }
    }

    fn remove_from(&mut self, number: u64, hash: Option<B256>) {
        let descendants: Vec<u64> = {
            let delivered = self.delivered.lock().expect("lock");
            if !delivered
                .blocks
                .get(&number)
                .is_some_and(|(h, _)| hash.is_none_or(|hash| hash == *h))
            {
                return;
            }
            delivered
                .blocks
                .range(number + 1..)
                .rev()
                .map(|(n, _)| *n)
                .collect()
        };
        descendants
            .into_iter()
            .for_each(|block| self.remove_block(block, None));
        self.remove_block(number, hash);
        self.schedule_repair(number);
    }
}

async fn next_live(mut subscription: Subscription<Log>) -> Option<(Live, Subscription<Log>)> {
    match subscription.recv().await {
        Ok(log) => Some((Live::Log(Box::new(log)), subscription)),
        Err(RecvError::Lagged(missed)) => Some((Live::Lagged(missed), subscription)),
        Err(RecvError::Closed) => None,
    }
}

impl<N, B, E> Collector<E> for LogCollector<N, B>
where
    N: Network,
    B: LogBackfill<N>,
    E: From<LogEvent> + Send + Sync + 'static,
{
    type Cursor = LogCursor;

    fn name(&self) -> &str {
        &self.name
    }

    async fn connect(
        &self,
        resume: Option<LogCursor>,
    ) -> Result<
        impl TryStream<Ok = Item<E, LogCursor>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        let provider = self.source.connect().await?;
        let tip = self.recent.lock().expect("lock").back().copied();
        let follower = Follower::start(
            provider.clone(),
            self.chain.clone(),
            Arc::clone(&self.recent),
            tip,
        )
        .await?;
        let subscription = provider
            .subscribe_logs(&self.filter)
            .await
            .map_err(transport)?;
        let live = stream::unfold(subscription, next_live);
        let head = provider.get_block_number().await.map_err(transport)?;
        let from = resume.map(|c| c.block).or(self.start_block);
        let mut state = LogState {
            provider,
            live: Box::pin(live),
            heads: Box::pin(follower.into_stream()),
            filter: self.filter.clone(),
            backfill: Arc::clone(&self.backfill),
            depth: self.chain.max_reorg_depth,
            delivered: Arc::clone(&self.delivered),
            gap: None,
            floor: from.unwrap_or(head),
            repair_from: None,
            queued: VecDeque::new(),
            last: resume,
        };
        if let Some(from) = from.filter(|from| head >= *from) {
            state.open_gap(from, head);
        }
        Ok(stream::try_unfold(state, |mut state| async move {
            let item = state.next_item().await?;
            Ok(item.map(|item| (item.map_event(E::from), state)))
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::alloy::primitives::{Address, LogData, U64};
    use super::super::alloy::providers::ProviderBuilder;
    use super::super::alloy::transports::mock::Asserter;
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn log(block: u64, hash: u8, value: u8, removed: bool) -> Log {
        Log {
            inner: super::super::alloy::primitives::Log {
                address: Address::ZERO,
                data: LogData::new_unchecked(Vec::new(), vec![value].into()),
            },
            block_hash: Some(B256::repeat_byte(hash)),
            block_number: Some(block),
            log_index: Some(0),
            removed,
            ..Default::default()
        }
    }

    fn state(live: Vec<Log>, node: &Asserter) -> LogState<Ethereum, RpcBackfill> {
        let live = live.into_iter().map(Box::new).map(Live::Log).collect();
        state_with(live, node, RpcBackfill::default())
    }

    fn state_with<B>(live: Vec<Live>, node: &Asserter, backfill: B) -> LogState<Ethereum, B> {
        let provider = ProviderBuilder::new().connect_mocked_client(node.clone());
        LogState {
            provider: DynProvider::new(provider),
            live: Box::pin(stream::iter(live)),
            heads: Box::pin(stream::pending()),
            filter: Filter::new(),
            backfill: Arc::new(backfill),
            depth: 64,
            delivered: Arc::default(),
            gap: None,
            floor: 0,
            repair_from: None,
            queued: VecDeque::new(),
            last: None,
        }
    }

    #[derive(Default)]
    struct Scripted {
        responses: Mutex<VecDeque<Vec<Vec<Log>>>>,
        requests: Mutex<Vec<(u64, u64)>>,
        pulled: Arc<AtomicUsize>,
    }

    impl Scripted {
        fn new(responses: Vec<Vec<Vec<Log>>>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                ..Self::default()
            }
        }
    }

    impl LogBackfill for Scripted {
        async fn backfill(
            &self,
            _: &DynProvider,
            _: &Filter,
            _: u64,
            _: u64,
        ) -> Result<Vec<Log>, CollectorError> {
            unreachable!("the collector pages")
        }

        fn pages(
            self: Arc<Self>,
            _: DynProvider,
            _: Filter,
            from: u64,
            to: u64,
        ) -> impl Stream<Item = Result<Vec<Log>, CollectorError>> + Send + 'static {
            self.requests.lock().unwrap().push((from, to));
            let pages = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_default();
            let pulled = Arc::clone(&self.pulled);
            stream::iter(pages).map(move |page| {
                pulled.fetch_add(1, Ordering::SeqCst);
                Ok(page)
            })
        }
    }

    #[derive(Debug, PartialEq)]
    enum Step {
        Added(u8),
        Removed(u8),
        Filled(u64, u64),
    }

    fn step(item: Item<LogEvent, LogCursor>) -> Step {
        match item {
            Item::Event {
                event: LogEvent::Added(log),
                ..
            } => Step::Added(log.inner.data.data[0]),
            Item::Event {
                event: LogEvent::Removed(log),
                ..
            } => Step::Removed(log.inner.data.data[0]),
            Item::GapFilled { from, to } => Step::Filled(from, to),
            other => panic!("unexpected {other:?}"),
        }
    }

    async fn steps<B: LogBackfill>(state: &mut LogState<Ethereum, B>) -> Vec<Step> {
        stream::unfold(state, |state| async move {
            let item = state.next_item().await.unwrap()?;
            Some((step(item), state))
        })
        .collect()
        .await
    }

    fn canonical(node: &Asserter, head: u64, logs: Vec<Log>) {
        node.push_success(&U64::from(head));
        node.push_success(&logs);
    }

    async fn events(mut state: LogState<Ethereum, RpcBackfill>) -> Vec<(bool, u8)> {
        let mut out = Vec::new();
        while let Some(item) = state.next_item().await.unwrap() {
            match item {
                Item::Event {
                    event: LogEvent::Added(log),
                    ..
                } => out.push((true, log.inner.data.data[0])),
                Item::Event {
                    event: LogEvent::Removed(log),
                    ..
                } => out.push((false, log.inner.data.data[0])),
                other => panic!("unexpected {other:?}"),
            }
        }
        out
    }

    #[tokio::test]
    async fn node_reported_removals_arrive_newest_first() {
        let node = Asserter::new();
        canonical(&node, 5, Vec::new());
        let live = vec![
            log(4, 0xa, 3, false),
            log(5, 0xb, 4, false),
            log(4, 0xa, 3, true),
            log(5, 0xb, 4, true),
        ];
        assert_eq!(
            events(state(live, &node)).await,
            vec![(true, 3), (true, 4), (false, 4), (false, 3)]
        );
        assert!(node.read_q().is_empty());
    }

    #[tokio::test]
    async fn replaced_block_removes_its_descendants_newest_first() {
        let node = Asserter::new();
        canonical(&node, 4, vec![log(4, 0xc, 5, false)]);
        let live = vec![
            log(4, 0xa, 3, false),
            log(5, 0xb, 4, false),
            log(4, 0xc, 5, false),
        ];
        assert_eq!(
            events(state(live, &node)).await,
            vec![(true, 3), (true, 4), (false, 4), (false, 3), (true, 5)]
        );
        assert!(node.read_q().is_empty());
    }

    #[tokio::test]
    async fn new_chain_log_before_the_removal_is_restored() {
        let node = Asserter::new();
        canonical(&node, 6, vec![log(6, 0xc, 5, false)]);
        let live = vec![
            log(4, 0xa, 3, false),
            log(5, 0xb, 4, false),
            log(6, 0xc, 5, false),
            log(4, 0xa, 3, true),
        ];
        assert_eq!(
            events(state(live, &node)).await,
            vec![
                (true, 3),
                (true, 4),
                (true, 5),
                (false, 5),
                (false, 4),
                (false, 3),
                (true, 5)
            ]
        );
        assert!(node.read_q().is_empty());
    }

    #[tokio::test]
    async fn gap_pages_arrive_in_order_and_catch_up_to_the_head() {
        let node = Asserter::new();
        node.push_success(&U64::from(5));
        node.push_success(&U64::from(5));
        let backfill = Scripted::new(vec![
            vec![
                vec![log(1, 0xa, 1, false)],
                vec![log(2, 0xb, 2, false), log(3, 0xc, 3, false)],
            ],
            vec![vec![log(4, 0xd, 4, false)]],
        ]);
        let mut state = state_with(Vec::new(), &node, backfill);
        state.open_gap(1, 3);

        assert_eq!(
            steps(&mut state).await,
            vec![
                Step::Added(1),
                Step::Added(2),
                Step::Added(3),
                Step::Added(4),
                Step::Filled(1, 5)
            ]
        );
        assert_eq!(
            *state.backfill.requests.lock().unwrap(),
            vec![(1, 3), (4, 5)]
        );
        assert!(node.read_q().is_empty());
    }

    #[tokio::test]
    async fn next_page_is_fetched_after_the_previous_one_was_delivered() {
        let node = Asserter::new();
        node.push_success(&U64::from(2));
        let backfill = Scripted::new(vec![vec![
            vec![log(1, 0xa, 1, false)],
            vec![log(2, 0xb, 2, false)],
        ]]);
        let mut state = state_with(Vec::new(), &node, backfill);
        state.open_gap(1, 2);

        let first = state.next_item().await.unwrap();
        assert!(matches!(
            first,
            Some(Item::Event {
                event: LogEvent::Added(_),
                ..
            })
        ));
        assert_eq!(state.backfill.pulled.load(Ordering::SeqCst), 1);
        assert_eq!(
            steps(&mut state).await,
            vec![Step::Added(2), Step::Filled(1, 2)]
        );
        assert_eq!(state.backfill.pulled.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn lagged_subscription_backfills_from_the_last_delivered_block() {
        let node = Asserter::new();
        node.push_success(&U64::from(6));
        node.push_success(&U64::from(6));
        let backfill = Scripted::new(vec![vec![vec![
            log(4, 0xa, 3, false),
            log(5, 0xb, 4, false),
            log(6, 0xc, 5, false),
        ]]]);
        let live = vec![
            Live::Log(Box::new(log(4, 0xa, 3, false))),
            Live::Lagged(7),
            Live::Log(Box::new(log(6, 0xc, 5, false))),
        ];
        let mut state = state_with(live, &node, backfill);

        assert_eq!(
            steps(&mut state).await,
            vec![
                Step::Added(3),
                Step::Added(4),
                Step::Added(5),
                Step::Filled(4, 6)
            ]
        );
        assert_eq!(*state.backfill.requests.lock().unwrap(), vec![(4, 6)]);
        assert!(node.read_q().is_empty());
    }

    #[tokio::test]
    async fn rpc_pages_halve_the_range_after_a_rejected_request() {
        let node = Asserter::new();
        node.push_failure_msg("range too large");
        node.push_success(&vec![log(1, 0xa, 1, false)]);
        node.push_success(&vec![log(4, 0xb, 2, false)]);
        node.push_success(&Vec::<Log>::new());
        let provider = DynProvider::new(ProviderBuilder::new().connect_mocked_client(node.clone()));

        let pages: Vec<Vec<Log>> = Arc::new(RpcBackfill::new(4))
            .pages(provider, Filter::new(), 1, 6)
            .try_collect()
            .await
            .unwrap();

        assert_eq!(pages.iter().map(Vec::len).collect_vec(), vec![1, 1, 0]);
        assert!(node.read_q().is_empty());
    }

    fn rejection(code: i64, message: &str) -> RpcError<TransportErrorKind> {
        let payload = serde_json::json!({ "code": code, "message": message });
        RpcError::ErrorResp(serde_json::from_value(payload).unwrap())
    }

    #[tokio::test(start_paused = true)]
    async fn rpc_pages_wait_out_a_rate_limit_without_shrinking_the_range() {
        let node = Asserter::new();
        let too_many = serde_json::json!({ "code": 429, "message": "Too Many Requests" });
        node.push_failure(serde_json::from_value(too_many).unwrap());
        node.push_success(&vec![log(1, 0xa, 1, false)]);
        node.push_success(&Vec::<Log>::new());
        let provider = DynProvider::new(ProviderBuilder::new().connect_mocked_client(node.clone()));

        let pages: Vec<Vec<Log>> = Arc::new(RpcBackfill::new(4))
            .pages(provider, Filter::new(), 1, 6)
            .try_collect()
            .await
            .unwrap();

        assert_eq!(pages.iter().map(Vec::len).collect_vec(), vec![1, 0]);
        assert!(node.read_q().is_empty());
    }

    #[tokio::test]
    async fn rpc_pages_grow_back_to_max_range_after_a_streak() {
        let node = Asserter::new();
        node.push_failure_msg("range too large");
        // 1..=2, 3..=4, 5..=6, 7..=8 at range 2, then 9..=30 in ranges of 4.
        (0..10).for_each(|_| node.push_success(&Vec::<Log>::new()));
        let provider = DynProvider::new(ProviderBuilder::new().connect_mocked_client(node.clone()));

        let pages: Vec<Vec<Log>> = Arc::new(RpcBackfill::new(4))
            .pages(provider, Filter::new(), 1, 30)
            .try_collect()
            .await
            .unwrap();

        assert_eq!(pages.len(), 10);
        assert!(node.read_q().is_empty());
    }

    #[test]
    fn only_rate_limits_wait_and_too_many_logs_shrink_the_range() {
        let http_429 = RpcError::Transport(TransportErrorKind::HttpError(
            super::super::alloy::transports::HttpError {
                status: 429,
                body: String::new(),
            },
        ));
        assert_eq!(failure(&http_429), Failure::RateLimited);
        assert_eq!(
            failure(&rejection(429, "Too Many Requests")),
            Failure::RateLimited
        );
        assert_eq!(
            failure(&rejection(
                -32016,
                "Your app has exceeded its compute units per second capacity, rate limit"
            )),
            Failure::RateLimited
        );
        assert_eq!(
            failure(&rejection(-32005, "query returned more than 10000 results")),
            Failure::Rejected
        );
        assert_eq!(
            failure(&rejection(-32602, "Log response size exceeded")),
            Failure::Rejected
        );
    }

    #[test]
    fn rate_limit_backoff_doubles_up_to_the_cap_and_gives_up() {
        let plan = |waits| Plan { range: 8, waits };
        assert_eq!(
            retry(Failure::RateLimited, plan(0)),
            Some(Retry::Wait(Duration::from_secs(1)))
        );
        assert_eq!(
            retry(Failure::RateLimited, plan(7)),
            Some(Retry::Wait(MAX_WAIT))
        );
        assert_eq!(retry(Failure::RateLimited, plan(MAX_WAITS)), None);
        assert_eq!(retry(Failure::Rejected, Plan { range: 1, waits: 0 }), None);
    }
}
