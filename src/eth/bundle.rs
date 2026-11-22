use std::fmt;
use std::sync::Arc;

use futures::future::join_all;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::alloy::primitives::{B256, Bytes, TxHash, hex, keccak256};
use super::alloy::signers::Signer;
use super::alloy::signers::local::PrivateKeySigner;
use crate::{BoxError, Executor};

/// A Flashbots-style bundle for `eth_sendBundle`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Bundle {
    /// Signed transactions, EIP-2718 encoded, executed in this order.
    pub txs: Vec<Bytes>,
    /// Target block number.
    #[serde(with = "hex_u64")]
    pub block_number: u64,
    /// Earliest block timestamp at which the bundle is valid.
    #[serde(default, skip_serializing_if = "Option::is_none", with = "hex_u64_opt")]
    pub min_timestamp: Option<u64>,
    /// Latest block timestamp at which the bundle is valid.
    #[serde(default, skip_serializing_if = "Option::is_none", with = "hex_u64_opt")]
    pub max_timestamp: Option<u64>,
    /// Transactions that may revert without invalidating the bundle.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reverting_tx_hashes: Vec<TxHash>,
}

impl Bundle {
    /// A bundle of `txs` for `block_number`.
    pub fn new(txs: Vec<Bytes>, block_number: u64) -> Self {
        Self {
            txs,
            block_number,
            min_timestamp: None,
            max_timestamp: None,
            reverting_tx_hashes: Vec::new(),
        }
    }
}

mod hex_u64 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(value: &u64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{value:#x}"))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        let s = String::deserialize(d)?;
        u64::from_str_radix(s.trim_start_matches("0x"), 16).map_err(serde::de::Error::custom)
    }
}

mod hex_u64_opt {
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(value: &Option<u64>, s: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(v) => super::hex_u64::serialize(v, s),
            None => s.serialize_none(),
        }
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
        Option::<String>::deserialize(d)?
            .map(|s| {
                u64::from_str_radix(s.trim_start_matches("0x"), 16)
                    .map_err(serde::de::Error::custom)
            })
            .transpose()
    }
}

/// An action that may carry a bundle for [`BundleExecutor`].
pub trait BundleAction {
    /// The bundle to send; `None` if this action is not a bundle.
    fn bundle(&self) -> Option<Bundle>;
}

/// Sends bundles with `eth_sendBundle` to all configured builders at once, signing every request
/// with the `X-Flashbots-Signature` header.
pub struct BundleExecutor {
    name: String,
    client: reqwest::Client,
    auth: PrivateKeySigner,
    builders: Vec<String>,
}

impl fmt::Debug for BundleExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BundleExecutor")
            .field("name", &self.name)
            .field("auth", &self.auth.address())
            .field("builders", &self.builders)
            .finish()
    }
}

#[derive(Deserialize)]
struct RpcResponse {
    result: Option<serde_json::Value>,
    error: Option<serde_json::Value>,
}

impl BundleExecutor {
    /// `auth` signs the requests only and identifies the searcher; it needs no funds.
    pub fn new(name: impl Into<String>, auth: PrivateKeySigner) -> Self {
        Self {
            name: name.into(),
            client: reqwest::Client::new(),
            auth,
            builders: Vec::new(),
        }
    }

    /// Adds a builder RPC URL.
    pub fn builder(mut self, url: impl Into<String>) -> Self {
        self.builders.push(url.into());
        self
    }

    async fn signature(&self, body: &str) -> Result<String, BoxError> {
        let digest = hex::encode_prefixed(keccak256(body.as_bytes()));
        let signature = self.auth.sign_message(digest.as_bytes()).await?;
        Ok(format!(
            "{}:{}",
            self.auth.address(),
            hex::encode_prefixed(signature.as_bytes())
        ))
    }

    async fn send_to(
        &self,
        url: &str,
        body: &str,
        signature: &str,
    ) -> Result<serde_json::Value, BoxError> {
        let response = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .header("X-Flashbots-Signature", signature)
            .body(body.to_owned())
            .send()
            .await?;
        let status = response.status();
        let rpc: RpcResponse = response.json().await?;
        match (rpc.result, rpc.error) {
            (Some(result), None) if status.is_success() => Ok(result),
            (_, Some(error)) => Err(format!("{url}: {error}").into()),
            _ => Err(format!("{url}: HTTP {status}").into()),
        }
    }
}

impl<A> Executor<A> for BundleExecutor
where
    A: BundleAction + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn handles(&self, action: &A) -> bool {
        action.bundle().is_some()
    }

    async fn execute(&self, action: Arc<A>) -> Result<(), BoxError> {
        let Some(bundle) = action.bundle() else {
            return Ok(());
        };
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_sendBundle",
            "params": [bundle],
        });
        let body = request.to_string();
        let signature = self.signature(&body).await?;
        let results = join_all(
            self.builders
                .iter()
                .map(|url| self.send_to(url, &body, &signature)),
        )
        .await;
        let (accepted, errors): (Vec<_>, Vec<_>) = results
            .into_iter()
            .map(|result| result.map_err(|err| err.to_string()))
            .partition_result();
        accepted.iter().for_each(|result| {
            let bundle_hash = result
                .get("bundleHash")
                .and_then(|h| h.as_str())
                .and_then(|h| h.parse::<B256>().ok());
            tracing::debug!(?bundle_hash, block = bundle.block_number, "bundle accepted");
        });
        let accepted = accepted.len();
        if !errors.is_empty() {
            tracing::warn!(errors = ?errors, accepted, "bundle rejected by some builders");
        }
        if accepted == 0 {
            return Err(format!("no builder accepted the bundle: {}", errors.join("; ")).into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_like_flashbots() {
        let mut bundle = Bundle::new(vec![Bytes::from_static(&[0x02, 0xab])], 0x10);
        bundle.max_timestamp = Some(255);
        let json = serde_json::to_value(&bundle).expect("serialize");
        assert_eq!(
            json,
            serde_json::json!({ "txs": ["0x02ab"], "blockNumber": "0x10", "maxTimestamp": "0xff" })
        );
        let back: Bundle = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back, bundle);
    }

    #[tokio::test]
    async fn signature_header_recovers_to_auth_address() {
        let auth = PrivateKeySigner::random();
        let executor = BundleExecutor::new("bundles", auth.clone());
        let header = executor.signature("{}").await.expect("sign");
        let (address, signature) = header.split_once(':').expect("address:signature");
        assert_eq!(
            address.parse::<super::super::Address>().expect("address"),
            auth.address()
        );
        let signature: super::super::alloy::primitives::Signature =
            signature.parse().expect("signature");
        let digest = hex::encode_prefixed(keccak256(b"{}"));
        assert_eq!(
            signature
                .recover_address_from_msg(digest.as_bytes())
                .expect("recover"),
            auth.address()
        );
    }
}
