use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt, stream};
use tokio::time::MissedTickBehavior;

use super::alloy::consensus::BlockHeader;
use super::alloy::network::primitives::HeaderResponse;
use super::alloy::network::{BlockResponse, Network};
use super::alloy::primitives::B256;
use super::alloy::providers::{DynProvider, Provider};
use super::alloy::rpc::types::BlockNumberOrTag;
use super::{BlockCursor, ChainEvent, ProviderSource, transport};
use crate::{CollectorError, Item, obs};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Heads {
    Auto,
    Poll,
}

#[derive(Debug, Clone)]
pub(super) struct ChainOpts {
    pub(super) max_reorg_depth: u64,
    pub(super) max_backfill: u64,
    pub(super) poll_interval: Duration,
    pub(super) heads: Heads,
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

type HeadStream<H> = std::pin::Pin<Box<dyn Stream<Item = H> + Send>>;

pub(super) struct Follower<N: Network> {
    pub(super) provider: DynProvider<N>,
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
    pub(super) async fn connect(
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
