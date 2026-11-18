# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
