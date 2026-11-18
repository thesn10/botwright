use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt, TryStream, TryStreamExt, stream};
use tokio::time::MissedTickBehavior;

use super::alloy::consensus::BlockHeader;
use super::alloy::network::primitives::HeaderResponse;
use super::alloy::network::{BlockResponse, Ethereum, Network};
use super::alloy::primitives::B256;
use super::alloy::providers::{DynProvider, Provider};
use super::alloy::rpc::types::{BlockNumberOrTag, Filter, Log};
use super::{ProviderSource, transport};
use crate::{Collector, CollectorError, Item, obs};

/// Resume position of block-based collectors: number and hash of the last delivered block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockCursor {
    /// Block number.
    pub number: u64,
    /// Block hash.
    pub hash: B256,
}

/// Event of the block-based collectors; `T` is the header, the full block or [`BlockLogs`].
#[derive(Debug, Clone, PartialEq)]
pub enum ChainEvent<T> {
    /// A new canonical block, delivered in chain order.
    Block(T),
    /// Blocks above `common_ancestor` were replaced; the new canonical blocks follow as `Block`.
    Reorg {
        /// Last block both chains share.
        common_ancestor: BlockCursor,
        /// Previously delivered blocks that are no longer canonical, oldest first.
        dropped: Vec<BlockCursor>,
    },
}

/// Header and all matching logs of one block.
#[derive(Debug, Clone, PartialEq)]
pub struct BlockLogs<H> {
    /// Block header.
    pub header: H,
    /// Logs of this block matching the filter, in log index order.
    pub logs: Vec<Log>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Heads {
    Auto,
    Poll,
}

#[derive(Debug, Clone)]
pub(super) struct ChainOpts {
    pub(super) max_reorg_depth: u64,
    max_backfill: u64,
    poll_interval: Duration,
    heads: Heads,
}

impl Default for ChainOpts {
    fn default() -> Self {
        Self {
            max_reorg_depth: 64,
            max_backfill: 1_000,
            poll_interval: Duration::from_secs(1),
            heads: Heads::Auto,
        }
    }
}

pub(super) type Recent = Arc<Mutex<VecDeque<BlockCursor>>>;
pub(super) type ChainItem<H> = Item<ChainEvent<H>, BlockCursor>;

/// Follows the canonical chain via `newHeads` (falling back to polling), backfills missing
/// headers with `eth_getBlockByNumber` and detects reorgs through `parent_hash`.
///
/// Implements `Collector<E>` for every `E: From<ChainEvent<N::HeaderResponse>>`.
pub struct BlockCollector<N: Network = Ethereum> {
    name: String,
    source: ProviderSource<N>,
    opts: ChainOpts,
    recent: Recent,
}

impl<N: Network> fmt::Debug for BlockCollector<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockCollector")
            .field("name", &self.name)
            .field("opts", &self.opts)
            .finish()
    }
}

impl BlockCollector<Ethereum> {
    /// Opens a fresh WebSocket connection on every connect; for other networks use
    /// [`from_source`](Self::from_source) with [`ProviderSource::ws`].
    pub fn ws(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self::from_source(name, ProviderSource::ws(url))
    }
}

impl<N: Network> BlockCollector<N> {
    /// Uses `provider` for every connection.
    pub fn new(name: impl Into<String>, provider: impl Provider<N> + 'static) -> Self {
        Self::from_source(name, ProviderSource::shared(provider))
    }

    /// Uses the given provider source.
    pub fn from_source(name: impl Into<String>, source: ProviderSource<N>) -> Self {
        Self {
            name: name.into(),
            source,
            opts: ChainOpts::default(),
            recent: Recent::default(),
        }
    }

    /// How far back a reorg is followed, default 64 blocks. Deeper reorgs yield `GapUnfilled`.
    pub fn max_reorg_depth(mut self, depth: u64) -> Self {
        self.opts.max_reorg_depth = depth.max(1);
        self
    }

    /// Maximum number of missing blocks fetched after a reconnect, default 1000.
    /// Larger gaps are skipped and reported as `GapUnfilled`.
    pub fn max_backfill(mut self, blocks: u64) -> Self {
        self.opts.max_backfill = blocks;
        self
    }

    /// Poll interval when no subscription is available, default 1 s.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.opts.poll_interval = interval;
        self
    }

    /// Never subscribes; always polls the latest header.
    pub fn polling(mut self) -> Self {
        self.opts.heads = Heads::Poll;
        self
    }

    async fn follow(&self, resume: Option<BlockCursor>) -> Result<Follower<N>, CollectorError> {
        Follower::connect(
            &self.source,
            self.opts.clone(),
            Arc::clone(&self.recent),
            resume,
        )
        .await
    }
}

impl<N, E> Collector<E> for BlockCollector<N>
where
    N: Network,
    E: From<ChainEvent<N::HeaderResponse>> + Send + Sync + 'static,
{
    type Cursor = BlockCursor;

    fn name(&self) -> &str {
        &self.name
    }

    async fn connect(
        &self,
        resume: Option<BlockCursor>,
    ) -> Result<
        impl TryStream<Ok = Item<E, BlockCursor>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        let follower = self.follow(resume).await?;
        Ok(follower
            .into_stream()
            .map_ok(|item| item.map_event(E::from)))
    }
}

/// Like [`BlockCollector`], but delivers each block's logs fetched with `eth_getLogs(blockHash)`:
/// complete per block and in order. Gap and reorg handling are the same.
pub struct LogsInBlockCollector<N: Network = Ethereum> {
    blocks: BlockCollector<N>,
    filter: Filter,
}

impl<N: Network> LogsInBlockCollector<N> {
    /// Fetches the logs matching `filter` (its block range is ignored) for every block.
    pub fn new(blocks: BlockCollector<N>, filter: Filter) -> Self {
        Self { blocks, filter }
    }
}

impl<N, E> Collector<E> for LogsInBlockCollector<N>
where
    N: Network,
    E: From<ChainEvent<BlockLogs<N::HeaderResponse>>> + Send + Sync + 'static,
{
    type Cursor = BlockCursor;

    fn name(&self) -> &str {
        &self.blocks.name
    }

    async fn connect(
        &self,
        resume: Option<BlockCursor>,
    ) -> Result<
        impl TryStream<Ok = Item<E, BlockCursor>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        let follower = self.blocks.follow(resume).await?;
        let provider = follower.provider.clone();
        let filter = self.filter.clone();
        Ok(follower.into_stream().and_then(move |item| {
            let provider = provider.clone();
            let filter = filter.clone();
            async move {
                Ok(match item {
                    Item::Event {
                        event: ChainEvent::Block(header),
                        cursor,
                    } => {
                        let at = filter.at_block_hash(header.hash());
                        let mut logs = provider.get_logs(&at).await.map_err(transport)?;
                        logs.sort_by_key(|l| l.log_index);
                        Item::Event {
                            event: E::from(ChainEvent::Block(BlockLogs { header, logs })),
                            cursor,
                        }
                    }
                    Item::Event {
                        event:
                            ChainEvent::Reorg {
                                common_ancestor,
                                dropped,
                            },
                        cursor,
                    } => Item::Event {
                        event: E::from(ChainEvent::Reorg {
                            common_ancestor,
                            dropped,
                        }),
                        cursor,
                    },
                    Item::GapFilled { from, to } => Item::GapFilled { from, to },
                    Item::GapUnfilled { from, to } => Item::GapUnfilled { from, to },
                })
            }
        }))
    }
}

/// Like [`BlockCollector`], but delivers full blocks with transactions. The block is loaded by
/// hash only after its header arrived, with bounded retries while the node does not serve it yet.
pub struct FullBlockCollector<N: Network = Ethereum> {
    blocks: BlockCollector<N>,
    max_retries: u32,
    retry_delay: Duration,
}

impl<N: Network> FullBlockCollector<N> {
    /// Wraps a block collector; 5 retries with 100 ms delay.
    pub fn new(blocks: BlockCollector<N>) -> Self {
        Self {
            blocks,
            max_retries: 5,
            retry_delay: Duration::from_millis(100),
        }
    }

    /// Retries while the block is not available yet.
    pub fn retries(mut self, max: u32, delay: Duration) -> Self {
        self.max_retries = max;
        self.retry_delay = delay;
        self
    }
}

impl<N, E> Collector<E> for FullBlockCollector<N>
where
    N: Network,
    E: From<ChainEvent<N::BlockResponse>> + Send + Sync + 'static,
{
    type Cursor = BlockCursor;

    fn name(&self) -> &str {
        &self.blocks.name
    }

    async fn connect(
        &self,
        resume: Option<BlockCursor>,
    ) -> Result<
        impl TryStream<Ok = Item<E, BlockCursor>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        let follower = self.blocks.follow(resume).await?;
        let provider = follower.provider.clone();
        let (retries, delay) = (self.max_retries, self.retry_delay);
        Ok(follower.into_stream().and_then(move |item| {
            let provider = provider.clone();
            async move {
                Ok(match item {
                    Item::Event {
                        event: ChainEvent::Block(header),
                        cursor,
                    } => {
                        let block =
                            load_full_block(&provider, header.hash(), retries, delay).await?;
                        Item::Event {
                            event: E::from(ChainEvent::Block(block)),
                            cursor,
                        }
                    }
                    Item::Event {
                        event:
                            ChainEvent::Reorg {
                                common_ancestor,
                                dropped,
                            },
                        cursor,
                    } => Item::Event {
                        event: E::from(ChainEvent::Reorg {
                            common_ancestor,
                            dropped,
                        }),
                        cursor,
                    },
                    Item::GapFilled { from, to } => Item::GapFilled { from, to },
                    Item::GapUnfilled { from, to } => Item::GapUnfilled { from, to },
                })
            }
        }))
    }
}

async fn load_full_block<N: Network>(
    provider: &DynProvider<N>,
    hash: B256,
    retries: u32,
    delay: Duration,
) -> Result<N::BlockResponse, CollectorError> {
    for attempt in 0..=retries {
        if let Some(block) = provider
            .get_block_by_hash(hash)
            .full()
            .await
            .map_err(transport)?
        {
            return Ok(block);
        }
        if attempt < retries {
            tokio::time::sleep(delay).await;
        }
    }
    Err(CollectorError::protocol(format!(
        "block {hash} not available after {retries} retries"
    )))
}

/// [`FullBlockCollector`] for HTTP endpoints without subscriptions: polls the latest block.
pub struct PollFullBlockCollector<N: Network = Ethereum>(FullBlockCollector<N>);

impl<N: Network> PollFullBlockCollector<N> {
    /// Polls `provider` every `interval`.
    pub fn new(
        name: impl Into<String>,
        provider: impl Provider<N> + 'static,
        interval: Duration,
    ) -> Self {
        Self(FullBlockCollector::new(
            BlockCollector::new(name, provider)
                .polling()
                .poll_interval(interval),
        ))
    }

    /// Adjusts the underlying collector, e.g. reorg depth or retries.
    pub fn configure(self, f: impl FnOnce(FullBlockCollector<N>) -> FullBlockCollector<N>) -> Self {
        Self(f(self.0))
    }
}

impl<N, E> Collector<E> for PollFullBlockCollector<N>
where
    N: Network,
    E: From<ChainEvent<N::BlockResponse>> + Send + Sync + 'static,
{
    type Cursor = BlockCursor;

    fn name(&self) -> &str {
        <FullBlockCollector<N> as Collector<E>>::name(&self.0)
    }

    async fn connect(
        &self,
        resume: Option<BlockCursor>,
    ) -> Result<
        impl TryStream<Ok = Item<E, BlockCursor>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        <FullBlockCollector<N> as Collector<E>>::connect(&self.0, resume).await
    }
}

type HeadStream<H> = std::pin::Pin<Box<dyn Stream<Item = H> + Send>>;

pub(super) struct Follower<N: Network> {
    provider: DynProvider<N>,
    heads: Option<HeadStream<N::HeaderResponse>>,
    poll: Option<tokio::time::Interval>,
    recent: Recent,
    opts: ChainOpts,
    pending: VecDeque<ChainItem<N::HeaderResponse>>,
    backfill: Option<(u64, u64, u64)>,
}

fn cursor_of<H: HeaderResponse>(header: &H) -> BlockCursor {
    BlockCursor {
        number: header.number(),
        hash: header.hash(),
    }
}

impl<N: Network> Follower<N> {
    async fn connect(
        source: &ProviderSource<N>,
        opts: ChainOpts,
        recent: Recent,
        resume: Option<BlockCursor>,
    ) -> Result<Self, CollectorError> {
        Self::start(source.connect().await?, opts, recent, resume).await
    }

    pub(super) async fn start(
        provider: DynProvider<N>,
        opts: ChainOpts,
        recent: Recent,
        resume: Option<BlockCursor>,
    ) -> Result<Self, CollectorError> {
        let (heads, poll) = match opts.heads {
            Heads::Auto => match provider.subscribe_blocks().await {
                Ok(sub) => (Some(Box::pin(sub.into_stream()) as HeadStream<_>), None),
                Err(err) => {
                    tracing::info!(error = %err, "no newHeads subscription, polling");
                    (None, Some(poll_interval(opts.poll_interval)))
                }
            },
            Heads::Poll => (None, Some(poll_interval(opts.poll_interval))),
        };

        let mut backfill = None;
        {
            let mut recent = recent.lock().expect("lock");
            match resume {
                None => recent.clear(),
                Some(cursor) => {
                    if recent.back().is_none_or(|tip| *tip != cursor) {
                        recent.clear();
                        recent.push_back(cursor);
                    }
                }
            }
        }
        let mut pending = VecDeque::new();
        if let Some(cursor) = resume {
            let head = provider.get_block_number().await.map_err(transport)?;
            if head > cursor.number {
                let from = cursor.number + 1;
                if head - cursor.number > opts.max_backfill {
                    recent.lock().expect("lock").clear();
                    pending.push_back(Item::GapUnfilled { from, to: head });
                } else {
                    backfill = Some((from, from, head));
                }
            }
        }
        Ok(Self {
            provider,
            heads,
            poll,
            recent,
            opts,
            pending,
            backfill,
        })
    }

    pub(super) fn into_stream(
        self,
    ) -> impl Stream<Item = Result<ChainItem<N::HeaderResponse>, CollectorError>> + Send + 'static
    {
        stream::try_unfold(self, |mut this| async move {
            let item = this.next_item().await?;
            Ok(item.map(|item| (item, this)))
        })
    }

    async fn next_item(&mut self) -> Result<Option<ChainItem<N::HeaderResponse>>, CollectorError> {
        loop {
            if let Some(item) = self.pending.pop_front() {
                return Ok(Some(item));
            }
            if let Some((from, next, to)) = self.backfill {
                let header = header_by_number(&self.provider, next).await?;
                self.accept(header).await?;
                self.backfill = if next < to {
                    Some((from, next + 1, to))
                } else {
                    self.pending.push_back(Item::GapFilled { from, to });
                    None
                };
                continue;
            }
            let header = match (&mut self.heads, &mut self.poll) {
                (Some(heads), _) => match heads.next().await {
                    Some(header) => header,
                    None => return Ok(None),
                },
                (None, Some(poll)) => {
                    poll.tick().await;
                    latest_header(&self.provider).await?
                }
                (None, None) => return Ok(None),
            };
            self.accept(header).await?;
        }
    }

    fn tip(&self) -> Option<BlockCursor> {
        self.recent.lock().expect("lock").back().copied()
    }

    fn known(&self, hash: B256) -> bool {
        self.recent
            .lock()
            .expect("lock")
            .iter()
            .any(|c| c.hash == hash)
    }

    fn append(&mut self, header: N::HeaderResponse) {
        let cursor = cursor_of(&header);
        {
            let mut recent = self.recent.lock().expect("lock");
            recent.push_back(cursor);
            while recent.len() as u64 > self.opts.max_reorg_depth + 1 {
                recent.pop_front();
            }
        }
        self.pending
            .push_back(Item::new(ChainEvent::Block(header), cursor));
    }

    async fn accept(&mut self, header: N::HeaderResponse) -> Result<(), CollectorError> {
        let mut work = VecDeque::from([header]);
        while let Some(header) = work.pop_front() {
            let (number, hash, parent) = (header.number(), header.hash(), header.parent_hash());
            let Some(tip) = self.tip() else {
                self.append(header);
                continue;
            };
            if self.known(hash) {
                continue;
            }
            if number == tip.number + 1 && parent == tip.hash {
                self.append(header);
            } else if number > tip.number + 1 {
                let missing = number - tip.number - 1;
                if missing > self.opts.max_backfill {
                    self.recent.lock().expect("lock").clear();
                    self.pending.push_back(Item::GapUnfilled {
                        from: tip.number + 1,
                        to: number - 1,
                    });
                    self.append(header);
                    continue;
                }
                work.push_front(header);
                for n in (tip.number + 1..number).rev() {
                    let fetched = header_by_number(&self.provider, n).await?;
                    work.push_front(fetched);
                }
            } else {
                self.reorg(header).await?;
            }
        }
        Ok(())
    }

    async fn reorg(&mut self, header: N::HeaderResponse) -> Result<(), CollectorError> {
        let tip_number = header.number();
        let oldest = self
            .recent
            .lock()
            .expect("lock")
            .front()
            .map_or(0, |c| c.number);
        let mut new_chain = vec![header];
        let ancestor = loop {
            let last = new_chain.last().expect("non-empty");
            let (number, hash) = (last.number().saturating_sub(1), last.parent_hash());
            let known = self
                .recent
                .lock()
                .expect("lock")
                .iter()
                .find(|c| c.number == number)
                .copied();
            match known {
                Some(c) if c.hash == hash => break Some(c),
                None if number < oldest => break None,
                _ if new_chain.len() as u64 > self.opts.max_reorg_depth => break None,
                _ => {
                    let parent = header_by_hash(&self.provider, hash).await?;
                    new_chain.push(parent);
                }
            }
        };
        let Some(ancestor) = ancestor else {
            tracing::warn!(oldest, tip_number, "reorg deeper than the tracked history");
            self.recent.lock().expect("lock").clear();
            self.pending.push_back(Item::GapUnfilled {
                from: oldest,
                to: tip_number,
            });
            for header in new_chain.into_iter().rev() {
                self.append(header);
            }
            return Ok(());
        };
        let dropped: Vec<BlockCursor> = {
            let mut recent = self.recent.lock().expect("lock");
            let keep = recent
                .iter()
                .position(|c| *c == ancestor)
                .map_or(0, |i| i + 1);
            recent.drain(keep..).collect()
        };
        tracing::info!(ancestor = ancestor.number, depth = dropped.len(), "reorg");
        obs::counter(obs::REORGS, &[("depth", &dropped.len().to_string())]).increment(1);
        self.pending.push_back(Item::new(
            ChainEvent::Reorg {
                common_ancestor: ancestor,
                dropped,
            },
            ancestor,
        ));
        for header in new_chain.into_iter().rev() {
            self.append(header);
        }
        Ok(())
    }
}

async fn header_by_number<N: Network>(
    provider: &DynProvider<N>,
    number: u64,
) -> Result<N::HeaderResponse, CollectorError> {
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(number))
        .await
        .map_err(transport)?
        .ok_or_else(|| CollectorError::protocol(format!("block {number} not found")))?;
    Ok(block.header().clone())
}

async fn header_by_hash<N: Network>(
    provider: &DynProvider<N>,
    hash: B256,
) -> Result<N::HeaderResponse, CollectorError> {
    let block = provider
        .get_block_by_hash(hash)
        .await
        .map_err(transport)?
        .ok_or_else(|| CollectorError::protocol(format!("block {hash} not found")))?;
    Ok(block.header().clone())
}

async fn latest_header<N: Network>(
    provider: &DynProvider<N>,
) -> Result<N::HeaderResponse, CollectorError> {
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Latest)
        .await
        .map_err(transport)?
        .ok_or_else(|| CollectorError::protocol("latest block not found"))?;
    Ok(block.header().clone())
}

fn poll_interval(period: Duration) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval
}
