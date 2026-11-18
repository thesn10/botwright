use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures::future::{BoxFuture, join_all};

use super::alloy::eips::eip2718::Encodable2718;
use super::alloy::network::{Ethereum, EthereumWallet};
use super::alloy::primitives::{Bytes, TxHash, keccak256};
use super::alloy::providers::fillers::{
    CachedNonceManager, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
};
use super::alloy::providers::{
    DynProvider, Identity, Provider, ProviderBuilder, RootProvider, SendableTx,
};
use super::alloy::rpc::types::{TransactionReceipt, TransactionRequest};
use super::alloy::signers::local::PrivateKeySigner;
use crate::{BoxError, EventSender, Executor};

/// Result of a transaction, fed back to strategies through a
/// [`ChannelCollector`](crate::ChannelCollector).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TxFeedback {
    /// Accepted by at least one endpoint.
    Sent {
        /// Executor name.
        executor: String,
        /// Transaction hash.
        tx_hash: TxHash,
        /// Number of endpoints that accepted it.
        accepted_by: usize,
    },
    /// Included in a block.
    Included {
        /// Executor name.
        executor: String,
        /// The receipt.
        receipt: Box<TransactionReceipt>,
    },
    /// No receipt within the receipt timeout.
    NotIncluded {
        /// Executor name.
        executor: String,
        /// Transaction hash.
        tx_hash: TxHash,
    },
    /// Filling, signing or sending failed.
    Failed {
        /// Executor name.
        executor: String,
        /// Hash, if the transaction was signed.
        tx_hash: Option<TxHash>,
        /// The error.
        error: String,
    },
}

type FeedbackFn = Arc<dyn Fn(TxFeedback) -> BoxFuture<'static, ()> + Send + Sync>;

fn feedback_fn<E: From<TxFeedback> + Send + 'static>(sender: EventSender<E>) -> FeedbackFn {
    Arc::new(move |feedback| {
        let sender = sender.clone();
        Box::pin(async move {
            if sender.send(E::from(feedback)).await.is_err() {
                tracing::debug!("feedback channel closed");
            }
        })
    })
}

async fn report(feedback: &Option<FeedbackFn>, event: TxFeedback) {
    if let Some(f) = feedback {
        f(event).await;
    }
}

/// An action that may carry a transaction for [`TransactionExecutor`].
pub trait TransactionAction {
    /// The transaction to fill, sign and send; `None` if this action is not a transaction.
    fn transaction(&self) -> Option<TransactionRequest>;
}

/// An action that may carry signed transaction bytes for [`RawTransactionExecutor`].
pub trait RawTransactionAction {
    /// EIP-2718 encoded signed transaction; `None` if this action is not one.
    fn raw_transaction(&self) -> Option<Bytes>;
}

type SignerProvider = FillProvider<
    JoinFill<
        JoinFill<
            JoinFill<JoinFill<Identity, GasFiller>, NonceFiller<CachedNonceManager>>,
            ChainIdFiller,
        >,
        WalletFiller<EthereumWallet>,
    >,
    RootProvider<Ethereum>,
    Ethereum,
>;

/// Builds, signs and sends transactions. Nonces come from alloy's `CachedNonceManager`, gas and
/// chain id from fillers. The signed bytes can go to several endpoints at once. Hash and later the
/// receipt are reported as [`TxFeedback`].
pub struct TransactionExecutor {
    name: String,
    signer: SignerProvider,
    endpoints: Vec<DynProvider<Ethereum>>,
    feedback: Option<FeedbackFn>,
    receipt_timeout: Duration,
    receipt_poll: Duration,
}

impl fmt::Debug for TransactionExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionExecutor")
            .field("name", &self.name)
            .field("endpoints", &self.endpoints.len())
            .finish()
    }
}

/// Invalid endpoint URL.
#[derive(Debug, thiserror::Error)]
#[error("invalid endpoint url `{0}`")]
pub struct InvalidUrl(String);

fn parse_url(url: &str) -> Result<reqwest_url::Url, InvalidUrl> {
    url.parse().map_err(|_| InvalidUrl(url.to_owned()))
}

mod reqwest_url {
    pub(super) use super::super::alloy::transports::http::reqwest::Url;
}

impl TransactionExecutor {
    /// Signs with `signer` and sends over HTTP to `url`.
    pub fn new(
        name: impl Into<String>,
        signer: PrivateKeySigner,
        url: &str,
    ) -> Result<Self, InvalidUrl> {
        let provider = ProviderBuilder::<Identity, Identity, Ethereum>::default()
            .with_gas_estimation()
            .with_cached_nonce_management()
            .fetch_chain_id()
            .wallet(EthereumWallet::from(signer))
            .connect_http(parse_url(url)?);
        let primary = DynProvider::new(provider.root().clone());
        Ok(Self {
            name: name.into(),
            signer: provider,
            endpoints: vec![primary],
            feedback: None,
            receipt_timeout: Duration::from_secs(60),
            receipt_poll: Duration::from_secs(1),
        })
    }

    /// Also sends the signed transaction to `url`.
    pub fn broadcast_to(mut self, url: &str) -> Result<Self, InvalidUrl> {
        let provider = RootProvider::<Ethereum>::new_http(parse_url(url)?);
        self.endpoints.push(DynProvider::new(provider));
        Ok(self)
    }

    /// Reports [`TxFeedback`] into a [`ChannelCollector`](crate::ChannelCollector).
    pub fn feedback<E: From<TxFeedback> + Send + 'static>(
        mut self,
        sender: EventSender<E>,
    ) -> Self {
        self.feedback = Some(feedback_fn(sender));
        self
    }

    /// How long to wait for a receipt, default 60 s, polled every `poll`, default 1 s.
    pub fn receipt_timeout(mut self, timeout: Duration, poll: Duration) -> Self {
        self.receipt_timeout = timeout;
        self.receipt_poll = poll;
        self
    }
}

async fn broadcast(endpoints: &[DynProvider<Ethereum>], raw: &[u8]) -> (usize, Option<String>) {
    let results = join_all(endpoints.iter().map(|p| p.send_raw_transaction(raw))).await;
    let accepted = results.iter().filter(|r| r.is_ok()).count();
    let error = results
        .into_iter()
        .find_map(|r| r.err())
        .map(|e| e.to_string());
    (accepted, error)
}

fn watch_receipt(
    provider: DynProvider<Ethereum>,
    executor: String,
    tx_hash: TxHash,
    feedback: Option<FeedbackFn>,
    timeout: Duration,
    poll: Duration,
) {
    tokio::spawn(async move {
        let wait = async {
            loop {
                match provider.get_transaction_receipt(tx_hash).await {
                    Ok(Some(receipt)) => return receipt,
                    Ok(None) => {}
                    Err(err) => tracing::debug!(error = %err, %tx_hash, "receipt query failed"),
                }
                tokio::time::sleep(poll).await;
            }
        };
        let event = match tokio::time::timeout(timeout, wait).await {
            Ok(receipt) => TxFeedback::Included {
                executor,
                receipt: Box::new(receipt),
            },
            Err(_) => TxFeedback::NotIncluded { executor, tx_hash },
        };
        report(&feedback, event).await;
    });
}

impl<A> Executor<A> for TransactionExecutor
where
    A: TransactionAction + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn handles(&self, action: &A) -> bool {
        action.transaction().is_some()
    }

    async fn execute(&self, action: Arc<A>) -> Result<(), BoxError> {
        let Some(request) = action.transaction() else {
            return Ok(());
        };
        let envelope = match self.signer.fill(request).await {
            Ok(SendableTx::Envelope(envelope)) => envelope,
            Ok(SendableTx::Builder(_)) => {
                return Err("transaction was filled but not signed".into());
            }
            Err(err) => {
                let error = err.to_string();
                report(
                    &self.feedback,
                    TxFeedback::Failed {
                        executor: self.name.clone(),
                        tx_hash: None,
                        error,
                    },
                )
                .await;
                return Err(err.into());
            }
        };
        let tx_hash = *envelope.tx_hash();
        let raw = envelope.encoded_2718();
        let (accepted_by, error) = broadcast(&self.endpoints, &raw).await;
        if accepted_by == 0 {
            let error = error.unwrap_or_else(|| "no endpoint accepted the transaction".into());
            let event = TxFeedback::Failed {
                executor: self.name.clone(),
                tx_hash: Some(tx_hash),
                error: error.clone(),
            };
            report(&self.feedback, event).await;
            return Err(error.into());
        }
        tracing::info!(%tx_hash, accepted_by, "transaction sent");
        report(
            &self.feedback,
            TxFeedback::Sent {
                executor: self.name.clone(),
                tx_hash,
                accepted_by,
            },
        )
        .await;
        watch_receipt(
            self.endpoints[0].clone(),
            self.name.clone(),
            tx_hash,
            self.feedback.clone(),
            self.receipt_timeout,
            self.receipt_poll,
        );
        Ok(())
    }
}

/// Sends already signed transactions to one or more endpoints.
pub struct RawTransactionExecutor {
    name: String,
    endpoints: Vec<DynProvider<Ethereum>>,
    feedback: Option<FeedbackFn>,
}

impl fmt::Debug for RawTransactionExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawTransactionExecutor")
            .field("name", &self.name)
            .field("endpoints", &self.endpoints.len())
            .finish()
    }
}

impl RawTransactionExecutor {
    /// Sends over HTTP to every URL.
    pub fn new<'a>(
        name: impl Into<String>,
        urls: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, InvalidUrl> {
        let endpoints = urls
            .into_iter()
            .map(|url| {
                Ok(DynProvider::new(RootProvider::<Ethereum>::new_http(
                    parse_url(url)?,
                )))
            })
            .collect::<Result<Vec<_>, InvalidUrl>>()?;
        Ok(Self {
            name: name.into(),
            endpoints,
            feedback: None,
        })
    }

    /// Sends through existing providers.
    pub fn with_providers(name: impl Into<String>, providers: Vec<DynProvider<Ethereum>>) -> Self {
        Self {
            name: name.into(),
            endpoints: providers,
            feedback: None,
        }
    }

    /// Reports [`TxFeedback::Sent`] and [`TxFeedback::Failed`] into a channel collector.
    pub fn feedback<E: From<TxFeedback> + Send + 'static>(
        mut self,
        sender: EventSender<E>,
    ) -> Self {
        self.feedback = Some(feedback_fn(sender));
        self
    }
}

impl<A> Executor<A> for RawTransactionExecutor
where
    A: RawTransactionAction + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn handles(&self, action: &A) -> bool {
        action.raw_transaction().is_some()
    }

    async fn execute(&self, action: Arc<A>) -> Result<(), BoxError> {
        let Some(raw) = action.raw_transaction() else {
            return Ok(());
        };
        let tx_hash = keccak256(&raw);
        let (accepted_by, error) = broadcast(&self.endpoints, &raw).await;
        let executor = self.name.clone();
        if accepted_by == 0 {
            let error = error.unwrap_or_else(|| "no endpoint configured".into());
            report(
                &self.feedback,
                TxFeedback::Failed {
                    executor,
                    tx_hash: Some(tx_hash),
                    error: error.clone(),
                },
            )
            .await;
            return Err(error.into());
        }
        report(
            &self.feedback,
            TxFeedback::Sent {
                executor,
                tx_hash,
                accepted_by,
            },
        )
        .await;
        Ok(())
    }
}
