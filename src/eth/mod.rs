//! EVM collectors and executors on top of alloy.
//!
//! The backend is chosen with feature `evm-alloy1` (alloy 1.x) or `evm-alloy2` (alloy 2.x); code
//! written against this module compiles unchanged with both.

mod blocks;
mod follower;
mod logs;
mod source;
mod tx;

#[cfg(feature = "bundles")]
mod bundle;
#[cfg(feature = "hypersync")]
mod hypersync;
#[cfg(feature = "mempool")]
mod mempool;

#[cfg(feature = "evm-alloy2")]
pub use alloy2 as alloy;

#[cfg(all(feature = "evm-alloy1", not(feature = "evm-alloy2")))]
pub use alloy1 as alloy;

pub use alloy::consensus::BlockHeader;
pub use alloy::network::{
    self, AnyNetwork, BlockResponse, Ethereum, EthereumWallet, Network, TransactionResponse,
    primitives::HeaderResponse,
};
pub use alloy::primitives::{
    self, Address, B256, BlockHash, BlockNumber, Bytes, TxHash, U256, address, b256, bytes,
};
pub use alloy::providers::{self, DynProvider, Provider, ProviderBuilder, RootProvider, WsConnect};
pub use alloy::rpc::types::{
    Block, BlockNumberOrTag, Filter, Header, Log, Transaction, TransactionReceipt,
    TransactionRequest,
};
pub use alloy::signers::{self, Signer, local::PrivateKeySigner};
pub use alloy::{sol, sol_types};

pub use blocks::{
    BlockCollector, BlockCursor, BlockLogs, ChainEvent, FullBlockCollector, LogsInBlockCollector,
    PollFullBlockCollector,
};
#[cfg(feature = "bundles")]
pub use bundle::{Bundle, BundleAction, BundleExecutor};
#[cfg(feature = "hypersync")]
pub use hypersync::HyperSyncBackfill;
#[cfg(feature = "hypersync")]
pub use hypersync_client::{
    StreamConfig as HyperSyncStreamConfig, StreamMetrics as HyperSyncStreamMetrics,
    StreamObserver as HyperSyncStreamObserver, net_types::LogField as HyperSyncLogField,
};
pub use logs::{LogBackfill, LogCollector, LogCursor, LogEvent, RpcBackfill};
#[cfg(feature = "mempool")]
pub use mempool::{MempoolCollector, PendingTransaction};
pub use source::ProviderSource;
pub use tx::{
    RawTransactionAction, RawTransactionExecutor, TransactionAction, TransactionExecutor,
    TxFeedback,
};

use crate::CollectorError;

pub(crate) fn transport(err: impl std::error::Error + Send + Sync + 'static) -> CollectorError {
    CollectorError::transport(err)
}
