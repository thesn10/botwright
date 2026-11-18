use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use futures::{StreamExt, TryStream, future};
use lru::LruCache;

use super::alloy::network::{Ethereum, Network};
use super::alloy::primitives::TxHash;
use super::alloy::providers::Provider;
use super::{ProviderSource, transport};
use crate::{Collector, CollectorError, Item};

/// A transaction seen in the mempool.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingTransaction<T> {
    /// Transaction hash.
    pub hash: TxHash,
    /// The transaction as returned by `eth_getTransactionByHash`.
    pub transaction: T,
}

/// Subscribes to `newPendingTransactions` and loads every transaction with
/// `eth_getTransactionByHash`, up to `max_in_flight` at a time. Transactions that are already gone
/// (`null`) are skipped; an LRU cache drops hashes seen before.
///
/// Implements `Collector<E>` for every `E: From<PendingTransaction<N::TransactionResponse>>`.
/// Worth it on chains with a public mempool, not on sequencer chains.
pub struct MempoolCollector<N: Network = Ethereum> {
    name: String,
    source: ProviderSource<N>,
    max_in_flight: usize,
    seen: Arc<Mutex<LruCache<TxHash, ()>>>,
}

impl<N: Network> fmt::Debug for MempoolCollector<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MempoolCollector")
            .field("name", &self.name)
            .field("source", &self.source)
            .field("max_in_flight", &self.max_in_flight)
            .finish_non_exhaustive()
    }
}

const DEFAULT_SEEN: NonZeroUsize = NonZeroUsize::new(65_536).expect("non-zero");

impl MempoolCollector<Ethereum> {
    /// Opens a fresh WebSocket connection on every connect; for other networks use
    /// [`from_source`](Self::from_source) with [`ProviderSource::ws`].
    pub fn ws(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self::from_source(name, ProviderSource::ws(url))
    }
}

impl<N: Network> MempoolCollector<N> {
    /// Uses `provider` for every connection; it must support subscriptions.
    pub fn new(name: impl Into<String>, provider: impl Provider<N> + 'static) -> Self {
        Self::from_source(name, ProviderSource::shared(provider))
    }

    /// Uses the given provider source.
    pub fn from_source(name: impl Into<String>, source: ProviderSource<N>) -> Self {
        Self {
            name: name.into(),
            source,
            max_in_flight: 256,
            seen: Arc::new(Mutex::new(LruCache::new(DEFAULT_SEEN))),
        }
    }

    /// Concurrent `eth_getTransactionByHash` requests, default 256.
    pub fn max_in_flight(mut self, n: usize) -> Self {
        self.max_in_flight = n.max(1);
        self
    }

    /// Number of hashes remembered for deduplication, default 65536.
    pub fn dedupe_capacity(self, n: NonZeroUsize) -> Self {
        Self {
            seen: Arc::new(Mutex::new(LruCache::new(n))),
            ..self
        }
    }
}

impl<N, E> Collector<E> for MempoolCollector<N>
where
    N: Network,
    E: From<PendingTransaction<N::TransactionResponse>> + Send + Sync + 'static,
{
    type Cursor = ();

    fn name(&self) -> &str {
        &self.name
    }

    async fn connect(
        &self,
        _resume: Option<()>,
    ) -> Result<
        impl TryStream<Ok = Item<E, ()>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        let provider = self.source.connect().await?;
        let hashes = provider
            .subscribe_pending_transactions()
            .await
            .map_err(transport)?
            .into_stream();
        let seen = Arc::clone(&self.seen);
        let fresh = hashes
            .filter(move |hash| future::ready(seen.lock().expect("lock").put(*hash, ()).is_none()));
        let loaded = fresh
            .map(move |hash| {
                let provider = provider.clone();
                async move { (hash, provider.get_transaction_by_hash(hash).await) }
            })
            .buffer_unordered(self.max_in_flight)
            .filter_map(|(hash, result)| {
                future::ready(match result {
                    Ok(Some(transaction)) => Some(Ok(Item::event(E::from(PendingTransaction {
                        hash,
                        transaction,
                    })))),
                    Ok(None) => {
                        tracing::debug!(%hash, "pending transaction not found");
                        None
                    }
                    Err(err) => {
                        tracing::debug!(%hash, error = %err, "loading pending transaction failed");
                        None
                    }
                })
            });
        Ok(loaded)
    }
}
