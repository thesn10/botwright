# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- `RpcBackfill` separates failures by cause. A rate-limited `eth_getLogs` (HTTP 429/503, JSON-RPC
  code 429 or a rate-limit message) is retried with the same range after an exponential backoff
  (1 s doubling up to 30 s, 8 times) instead of halving the range. Every other failure still halves
  it. After four pages in a row succeed at a reduced range, the range doubles again, up to `max_range`.

## [0.1.3] - 2026-11-21

### Added

- `ReplayCollector::from_stream` and `ReplayCollector::stream_jsonl` stream recorded events lazily
  in O(1) memory instead of loading the entire tape into RAM, preserving cursor resume.
- `Pace::Capped { max_gap }` replays with original inter-event spacing while clamping large idle
  gaps (e.g. multi-hour/multi-day gaps) to `max_gap`.
- `HyperSyncBackfill::sequential` (`concurrency = 1` for rate-limited tiers),
  `HyperSyncBackfill::log_fields` / `minimal_log_fields` for selecting HyperSync log columns, and
  `HyperSyncBackfill::observer` for forwarding `hypersync_client::StreamObserver` metrics (re-exported
  as `HyperSyncStreamConfig`, `HyperSyncLogField`, `HyperSyncStreamObserver`, `HyperSyncStreamMetrics`).

## [0.1.2] - 2026-11-20

### Added

- `LogBackfill::pages` delivers a gap as a stream of pages; the default wraps `backfill` in one page.
  `RpcBackfill` yields one `eth_getLogs` range per page, `HyperSyncBackfill` one HyperSync response.
- `HyperSyncBackfill::stream_config` sets concurrency and look-ahead of the HyperSync stream.

### Changed

- `LogCollector` delivers a backfill page by page with backpressure instead of loading the whole gap
  into memory first, and backfills again up to the new head before it reports `GapFilled`.
- `HyperSyncBackfill` fetches pages ahead in parallel through `hypersync_client::Client::stream`,
  falls back to `eth_getLogs` only above HyperSync's archive height, and fills `block_timestamp`.

### Fixed

- `LogCollector` lost logs when the strategy applied backpressure for longer than the node's
  subscription buffer (16 entries by default) lasted, for example during a large backfill: alloy
  dropped the oldest entries silently. A lagging subscription now triggers a backfill from the last
  delivered block.

## [0.1.1] - 2026-11-19

### Fixed

- After a strategy restart, `Dropped` and `Lagging` notices and `Latest` values left over from the
  interrupted drain are delivered right away instead of waiting for the next drop or update.

## [0.1.0] - 2026-11-18

### Added

- Engine with collectors, strategies and executors, each in its own task.
- Delivery per subscription: `Lossless`, `DropNewest`, `DropOldest`, `Latest`, with `Dropped` and
  `Lagging` system events and `OnFull::Abort`.
- Reconnect with exponential backoff and cursor resume; `GapFilled` / `GapUnfilled`.
- Strategy failure policy: `OnError`, `OnPanic::Abort`, `OnPanic::Restart` via factory.
- Opportunity tasks with a per-strategy limit and cancellation on shutdown.
- Action routing by `Executor::handles`, per-executor queues, concurrency and timeouts.
- Orderly shutdown with a single deadline; SIGINT/SIGTERM handling (`signal`).
- Built-in `IntervalCollector`, `ChannelCollector`, `MapCollector`, `PrintExecutor`, `NoopExecutor`.
- Non-blocking notifications: `LogNotifier`, `QueuedNotifier`, `TelegramNotifier` (`telegram`).
- Metrics through the `metrics` facade (`metrics`); `/healthz` and `/metrics` (`http`).
- Test utilities: `ReplayCollector`, `RecordingCollector`, `CapturingExecutor`, `Harness` (`test-util`).
- EVM module on alloy 1.x (`evm-alloy1`) or 2.x (`evm-alloy2`): `BlockCollector`, `LogCollector`,
  `LogsInBlockCollector`, `FullBlockCollector`, `PollFullBlockCollector`, `MempoolCollector`
  (`mempool`), `HyperSyncBackfill` (`hypersync`), `TransactionExecutor`, `RawTransactionExecutor`,
  `BundleExecutor` (`bundles`).
