use std::fmt;
use std::sync::Arc;

use hypersync_client::net_types::{LogField, LogFilter, Query};
use hypersync_client::simple_types;

use super::alloy::network::Network;
use super::alloy::primitives::{Address, B256, Bytes, LogData};
use super::alloy::providers::DynProvider;
use super::alloy::rpc::types::{Filter, Log};
use super::{LogBackfill, RpcBackfill};
use crate::CollectorError;

/// Backfills logs through Envio HyperSync, which is much faster than `eth_getLogs` for large
/// gaps. Blocks above HyperSync's archive height are fetched with [`RpcBackfill`].
#[derive(Clone)]
pub struct HyperSyncBackfill {
    client: Arc<hypersync_client::Client>,
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
}

fn log_filter(filter: &Filter) -> Result<LogFilter, CollectorError> {
    let mut selection = LogFilter::all();
    if !filter.address.is_empty() {
        selection = selection
            .and_address(filter.address.iter().map(|a| a.0.0))
            .map_err(CollectorError::other)?;
    }
    for (i, topic) in filter.topics.iter().enumerate() {
        if topic.is_empty() {
            continue;
        }
        let values = topic.iter().map(|t| t.0);
        selection = match i {
            0 => selection.and_topic0(values),
            1 => selection.and_topic1(values),
            2 => selection.and_topic2(values),
            _ => selection.and_topic3(values),
        }
        .map_err(CollectorError::other)?;
    }
    Ok(selection)
}

fn convert(log: simple_types::Log) -> Result<Log, CollectorError> {
    let missing = |field: &str| CollectorError::protocol(format!("hypersync log without {field}"));
    let address = log.address.ok_or_else(|| missing("address"))?;
    let topics = log
        .topics
        .iter()
        .flatten()
        .map(|t| B256::from_slice(t.as_ref()))
        .collect();
    let data = Bytes::copy_from_slice(log.data.as_ref().map_or(&[][..], |d| d.as_ref()));
    Ok(Log {
        inner: super::alloy::primitives::Log {
            address: Address::from_slice(address.as_ref()),
            data: LogData::new_unchecked(topics, data),
        },
        block_hash: log.block_hash.map(|h| B256::from_slice(h.as_ref())),
        block_number: Some(*log.block_number.ok_or_else(|| missing("block number"))?),
        block_timestamp: None,
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
        let selection = log_filter(filter)?;
        let mut logs = Vec::new();
        let mut start = from;
        while start <= to {
            let query = Query::new()
                .from_block(start)
                .to_block_excl(to + 1)
                .where_logs(selection.clone())
                .select_log_fields(LogField::all());
            let response = self
                .client
                .get(&query)
                .await
                .map_err(CollectorError::transport)?;
            for log in response.data.logs.into_iter().flatten() {
                logs.push(convert(log)?);
            }
            if response.next_block <= start {
                break;
            }
            start = response.next_block;
        }
        if start <= to {
            logs.extend(self.rpc.backfill(provider, filter, start, to).await?);
        }
        logs.sort_by_key(|log| (log.block_number, log.log_index));
        Ok(logs)
    }
}
