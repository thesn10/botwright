//! [`HyperSyncBackfill`]: log pages from Envio HyperSync for [`LogCollector`](super::LogCollector)
//! gaps, with an `eth_getLogs` tail above the archive height.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use futures::{Stream, StreamExt, TryStreamExt, stream};
use hypersync_client::net_types::{BlockField, LogField, LogFilter, Query};
use hypersync_client::{QueryResponse, StreamConfig, simple_types};
use itertools::Itertools;

use super::alloy::network::Network;
use super::alloy::primitives::{Address, B256, Bytes, LogData, U64};
use super::alloy::providers::DynProvider;
use super::alloy::rpc::types::{Filter, Log};
use super::{LogBackfill, RpcBackfill};
use crate::CollectorError;

/// Backfills logs through Envio HyperSync, which is much faster than `eth_getLogs` for large
/// gaps. Pages are fetched ahead in parallel by [`hypersync_client::Client::stream`]; blocks above
/// HyperSync's archive height are fetched with [`RpcBackfill`]. Logs carry their block timestamp.
#[derive(Clone)]
pub struct HyperSyncBackfill {
    client: Arc<hypersync_client::Client>,
    config: StreamConfig,
    rpc: RpcBackfill,
}

impl fmt::Debug for HyperSyncBackfill {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HyperSyncBackfill")
            .field("url", &self.client.url().as_str())
            .finish()
    }
}

impl HyperSyncBackfill {
    /// Uses a configured HyperSync client.
    pub fn new(client: hypersync_client::Client) -> Self {
        Self {
            client: Arc::new(client),
            config: StreamConfig::default(),
            rpc: RpcBackfill::default(),
        }
    }

    /// Connects to `url` with an Envio API token.
    pub fn from_url(url: impl ToString, api_token: impl ToString) -> Result<Self, CollectorError> {
        let client = hypersync_client::Client::builder()
            .url(url)
            .api_token(api_token)
            .build()
            .map_err(CollectorError::other)?;
        Ok(Self::new(client))
    }

    /// Backfill for the blocks HyperSync has not indexed yet.
    pub fn rpc_fallback(mut self, rpc: RpcBackfill) -> Self {
        self.rpc = rpc;
        self
    }

    /// Concurrency, batch sizes and look-ahead buffer of the HyperSync stream; the default is
    /// [`StreamConfig::default`].
    pub fn stream_config(mut self, config: StreamConfig) -> Self {
        self.config = config;
        self
    }

    async fn open<N: Network>(
        self: Arc<Self>,
        provider: DynProvider<N>,
        filter: Filter,
        from: u64,
        to: u64,
    ) -> Result<impl Stream<Item = Result<Vec<Log>, CollectorError>> + Send + 'static, CollectorError>
    {
        let indexed_end = self
            .client
            .get_height()
            .await
            .map_err(CollectorError::transport)?
            .min(to + 1);
        let receiver = if from < indexed_end {
            let query = query(&filter, from, indexed_end)?;
            let receiver = self.client.stream(query, self.config.clone()).await;
            Some(receiver.map_err(CollectorError::transport)?)
        } else {
            None
        };
        let indexed = stream::unfold(receiver, |receiver| async move {
            let mut receiver = receiver?;
            let response = receiver.recv().await?;
            let page = response.map_err(CollectorError::transport).and_then(page);
            Some((page, Some(receiver)))
        });
        let rest = <RpcBackfill as LogBackfill<N>>::pages(
            Arc::new(self.rpc),
            provider,
            filter,
            from.max(indexed_end),
            to,
        );
        Ok(indexed.chain(rest))
    }
}

fn query(filter: &Filter, from: u64, to_excl: u64) -> Result<Query, CollectorError> {
    Ok(Query::new()
        .from_block(from)
        .to_block_excl(to_excl)
        .where_logs(log_filter(filter)?)
        .select_log_fields(LogField::all())
        .select_block_fields([BlockField::Number, BlockField::Timestamp]))
}

fn page(response: QueryResponse) -> Result<Vec<Log>, CollectorError> {
    let timestamps: HashMap<u64, u64> = response
        .data
        .blocks
        .iter()
        .flatten()
        .filter_map(block_timestamp)
        .collect();
    let logs: Vec<Log> = response
        .data
        .logs
        .into_iter()
        .flatten()
        .map(|log| convert(log, &timestamps))
        .try_collect()?;

    Ok(logs
        .into_iter()
        .sorted_by_key(|log| (log.block_number, log.log_index))
        .collect())
}

fn block_timestamp(block: &simple_types::Block) -> Option<(u64, u64)> {
    let timestamp = U64::try_from_be_slice(block.timestamp.as_ref()?.as_ref())?;
    Some((block.number?, timestamp.to()))
}

fn log_filter(filter: &Filter) -> Result<LogFilter, CollectorError> {
    let mut selection = LogFilter::all();
    if !filter.address.is_empty() {
        selection = selection
            .and_address(filter.address.iter().map(|a| a.0.0))
            .map_err(CollectorError::other)?;
    }
    filter
        .topics
        .iter()
        .enumerate()
        .filter(|(_, topic)| !topic.is_empty())
        .try_fold(selection, |selection, (i, topic)| {
            let values = topic.iter().map(|t| t.0);
            match i {
                0 => selection.and_topic0(values),
                1 => selection.and_topic1(values),
                2 => selection.and_topic2(values),
                _ => selection.and_topic3(values),
            }
            .map_err(CollectorError::other)
        })
}

fn convert(log: simple_types::Log, timestamps: &HashMap<u64, u64>) -> Result<Log, CollectorError> {
    let missing = |field: &str| CollectorError::protocol(format!("hypersync log without {field}"));
    let address = log.address.ok_or_else(|| missing("address"))?;
    let topics = log
        .topics
        .iter()
        .flatten()
        .map(|t| B256::from_slice(t.as_ref()))
        .collect();
    let data = Bytes::copy_from_slice(log.data.as_ref().map_or(&[][..], |d| d.as_ref()));
    let block_number = *log.block_number.ok_or_else(|| missing("block number"))?;
    Ok(Log {
        inner: super::alloy::primitives::Log {
            address: Address::from_slice(address.as_ref()),
            data: LogData::new_unchecked(topics, data),
        },
        block_hash: log.block_hash.map(|h| B256::from_slice(h.as_ref())),
        block_number: Some(block_number),
        block_timestamp: timestamps.get(&block_number).copied(),
        transaction_hash: log.transaction_hash.map(|h| B256::from_slice(h.as_ref())),
        transaction_index: log.transaction_index.map(|i| *i),
        log_index: Some(*log.log_index.ok_or_else(|| missing("log index"))?),
        removed: log.removed.unwrap_or(false),
    })
}

impl<N: Network> LogBackfill<N> for HyperSyncBackfill {
    async fn backfill(
        &self,
        provider: &DynProvider<N>,
        filter: &Filter,
        from: u64,
        to: u64,
    ) -> Result<Vec<Log>, CollectorError> {
        Arc::new(self.clone())
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
        stream::once(self.open(provider, filter, from, to)).try_flatten()
    }
}

#[cfg(test)]
mod tests {
    use hypersync_client::format::{Address as HsAddress, Quantity, UInt};

    use super::*;

    fn hs_log(block: u64, index: u64) -> simple_types::Log {
        simple_types::Log {
            log_index: Some(UInt::from(index)),
            block_number: Some(UInt::from(block)),
            address: Some(HsAddress::from([0u8; 20])),
            ..Default::default()
        }
    }

    fn block(number: u64, timestamp: u64) -> simple_types::Block {
        simple_types::Block {
            number: Some(number),
            timestamp: Some(Quantity::from(timestamp)),
            ..Default::default()
        }
    }

    #[test]
    fn page_is_ordered_and_carries_block_timestamps() {
        let mut response: QueryResponse = QueryResponse {
            archive_height: None,
            next_block: 9,
            total_execution_time: 0,
            data: Default::default(),
            rollback_guard: None,
        };
        response.data.blocks = vec![vec![block(7, 1_700_000_000), block(8, 1_700_000_002)]];
        response.data.logs = vec![vec![hs_log(8, 0), hs_log(7, 3), hs_log(7, 1)]];

        let positions = page(response)
            .unwrap()
            .iter()
            .map(|log| (log.block_number, log.log_index, log.block_timestamp))
            .collect_vec();

        assert_eq!(
            positions,
            vec![
                (Some(7), Some(1), Some(1_700_000_000)),
                (Some(7), Some(3), Some(1_700_000_000)),
                (Some(8), Some(0), Some(1_700_000_002)),
            ]
        );
    }
}
