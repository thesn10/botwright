use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures::{Stream, StreamExt, TryStream, stream};

use super::alloy::consensus::BlockHeader;
use super::alloy::network::primitives::HeaderResponse;
use super::alloy::network::{Ethereum, Network};
use super::alloy::primitives::B256;
use super::alloy::providers::{DynProvider, Provider};
use super::alloy::rpc::types::{Filter, Log};
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
}

/// Backfills with `eth_getLogs` in ranges of at most `max_range` blocks, halving the range when
/// the node rejects a request.
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

impl<N: Network> LogBackfill<N> for RpcBackfill {
    async fn backfill(
        &self,
        provider: &DynProvider<N>,
        filter: &Filter,
        from: u64,
        to: u64,
    ) -> Result<Vec<Log>, CollectorError> {
        let mut logs = Vec::new();
        let mut range = self.max_range;
        let mut start = from;
        while start <= to {
            let end = to.min(start.saturating_add(range - 1));
            match provider
                .get_logs(&filter.clone().from_block(start).to_block(end))
                .await
            {
                Ok(mut chunk) => {
                    logs.append(&mut chunk);
                    start = end + 1;
                }
                Err(err) if range > 1 => {
                    range /= 2;
                    tracing::debug!(error = %err, range, "eth_getLogs failed, shrinking range");
                }
                Err(err) => return Err(transport(err)),
            }
        }
        logs.sort_by_key(|log| (log.block_number, log.log_index));
        Ok(logs)
    }
}

/// Subscribes to logs and, after a reconnect, backfills everything since the last cursor with a
/// [`LogBackfill`], dropping duplicates.
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

type LogStream = Pin<Box<dyn Stream<Item = Log> + Send>>;
type HeadItems<H> = Pin<Box<dyn Stream<Item = Result<ChainItem<H>, CollectorError>> + Send>>;

struct Gap {
    from: u64,
    to: u64,
}

struct LogState<N: Network, B> {
    provider: DynProvider<N>,
    live: LogStream,
    heads: HeadItems<N::HeaderResponse>,
    filter: Filter,
    backfill: Arc<B>,
    depth: u64,
    delivered: Arc<Mutex<Delivered>>,
    gap: Option<Gap>,
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
            if let Some(Gap { from, to }) = self.gap.take() {
                let logs = self
                    .backfill
                    .backfill(&self.provider, &self.filter, from, to)
                    .await?;
                for log in logs {
                    self.added(log);
                }
                self.queued.push_back(Item::GapFilled { from, to });
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
                log = self.live.next() => match log {
                    Some(log) if log.removed => self.removed(&log),
                    Some(log) => self.added(log),
                    None => return Ok(None),
                },
            }
        }
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
                for block in dropped.iter().rev() {
                    self.remove_block(block.number, Some(block.hash));
                }
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
        for log in logs {
            self.added(log);
        }
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
        for mut log in logs.into_iter().rev() {
            log.removed = true;
            self.queued.push_back(Item::Event {
                event: LogEvent::Removed(log),
                cursor: self.last,
            });
        }
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
        for block in descendants {
            self.remove_block(block, None);
        }
        self.remove_block(number, hash);
        self.schedule_repair(number);
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
        let from = resume.map(|c| c.block).or(self.start_block);
        let gap = match from {
            Some(from) => {
                let head = provider.get_block_number().await.map_err(transport)?;
                (head >= from).then_some(Gap { from, to: head })
            }
            None => None,
        };
        let state = LogState {
            provider,
            live: Box::pin(subscription.into_stream()),
            heads: Box::pin(follower.into_stream()),
            filter: self.filter.clone(),
            backfill: Arc::clone(&self.backfill),
            depth: self.chain.max_reorg_depth,
            delivered: Arc::clone(&self.delivered),
            gap,
            repair_from: None,
            queued: VecDeque::new(),
            last: resume,
        };
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
        let provider = ProviderBuilder::new().connect_mocked_client(node.clone());
        LogState {
            provider: DynProvider::new(provider),
            live: Box::pin(stream::iter(live)),
            heads: Box::pin(stream::pending()),
            filter: Filter::new(),
            backfill: Arc::new(RpcBackfill::default()),
            depth: 64,
            delivered: Arc::default(),
            gap: None,
            repair_from: None,
            queued: VecDeque::new(),
            last: None,
        }
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
}
