use std::fmt;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStream, TryStreamExt, future, stream};

use super::alloy::network::primitives::HeaderResponse;
use super::alloy::network::{Ethereum, Network};
use super::alloy::primitives::B256;
use super::alloy::providers::{DynProvider, Provider};
use super::alloy::rpc::types::{Filter, Log};
use super::follower::{ChainOpts, Follower, Heads, Recent};
use super::{ProviderSource, transport};
use crate::{Collector, CollectorError, Item};

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
    let attempts = stream::iter(0..=retries).then(|attempt| async move {
        if attempt > 0 {
            tokio::time::sleep(delay).await;
        }
        provider
            .get_block_by_hash(hash)
            .full()
            .await
            .map_err(transport)
    });
    pin!(attempts.try_filter_map(future::ok))
        .try_next()
        .await?
        .ok_or_else(|| {
            CollectorError::protocol(format!(
                "block {hash} not available after {retries} retries"
            ))
        })
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
