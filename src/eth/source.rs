use std::fmt;

use super::alloy::network::{Ethereum, Network};
use super::alloy::providers::{DynProvider, Identity, Provider, ProviderBuilder, WsConnect};
use super::transport;
use crate::CollectorError;

/// Where a collector gets its provider from on every (re)connect.
pub struct ProviderSource<N: Network = Ethereum>(Kind<N>);

enum Kind<N: Network> {
    Shared(DynProvider<N>),
    Ws(String),
}

impl<N: Network> Clone for ProviderSource<N> {
    fn clone(&self) -> Self {
        Self(match &self.0 {
            Kind::Shared(p) => Kind::Shared(p.clone()),
            Kind::Ws(url) => Kind::Ws(url.clone()),
        })
    }
}

impl<N: Network> fmt::Debug for ProviderSource<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Kind::Shared(_) => f.write_str("ProviderSource::Shared"),
            Kind::Ws(url) => f.debug_tuple("ProviderSource::Ws").field(url).finish(),
        }
    }
}

impl<N: Network> ProviderSource<N> {
    /// Reuses one provider for every connection.
    ///
    /// A WebSocket provider with alloy's automatic reconnect silently loses subscription events
    /// during the outage; prefer [`ws`](Self::ws), which opens a fresh connection without
    /// reconnect so the collector notices the break and backfills.
    pub fn shared(provider: impl Provider<N> + 'static) -> Self {
        Self(Kind::Shared(DynProvider::new(provider)))
    }

    /// Opens a new WebSocket connection on every connect, with alloy's own reconnect disabled.
    pub fn ws(url: impl Into<String>) -> Self {
        Self(Kind::Ws(url.into()))
    }

    pub(crate) async fn connect(&self) -> Result<DynProvider<N>, CollectorError> {
        match &self.0 {
            Kind::Shared(p) => Ok(p.clone()),
            Kind::Ws(url) => {
                let provider = ProviderBuilder::<Identity, Identity, N>::default()
                    .connect_ws(WsConnect::new(url.clone()).with_max_retries(0))
                    .await
                    .map_err(transport)?;
                Ok(DynProvider::new(provider))
            }
        }
    }
}
