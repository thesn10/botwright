# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Rust crate (edition 2024, MSRV 1.90) for event-driven bots: collector → strategy → executor (Artemis pattern). Public API, rustdoc and docs are in English (crates.io). `docs/guarantees.md` is a contract: changing behaviour described there needs a test and a CHANGELOG entry.

## Commands

`evm-alloy1` (default) and `evm-alloy2` are mutually exclusive (`compile_error!`), so never use `--all-features`. `mempool`/`hypersync`/`bundles` need one of the two backends.

```sh
E=mempool,hypersync,bundles,telegram,metrics,http,test-util
cargo clippy --all-targets --features $E -- -D warnings                                   # alloy 1
cargo clippy --all-targets --no-default-features --features evm-alloy2,signal,$E -- -D warnings
cargo test --features $E                                                                   # all tests
cargo test --no-default-features --features test-util                                      # core only
cargo test --features test-util --test lifecycle <name>                                    # single test
BOTWRIGHT_REQUIRE_ANVIL=1 cargo test --test anvil                                          # EVM tests, need anvil (skip silently without)
cargo hack clippy --each-feature --exclude-all-features --exclude-features evm-alloy2,default --features evm-alloy1 --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --features $E
cargo +1.90 check --all-targets --features $E                                              # MSRV
```

`tests/latency.rs` reads limits from `BOTWRIGHT_LATENCY_P50_US` / `_P99_US`. `ci/pin-alloy1.sh 1.1.0` pins all alloy 1.x crates to the minimal supported version (CI job).

## Architecture

- **Static dispatch:** traits `Collector<E>`, `Strategy<E, A>`, `Executor<A>` use native async fns (RPITIT, `+ Send`), no `dyn`/`async_trait`. `EngineBuilder` (`src/engine/builder.rs`) captures each concrete type in a generic slot implementing a private entry trait (`entry.rs`: `CollectorEntry`/`StrategyEntry`/`ExecutorEntry::into_task` → `BoxFuture`). `wiring.rs` creates the channels per subscription, `run.rs` holds `Engine::run`, `handle.rs` the `EngineHandle`. Only filters and router `handles` predicates are `dyn` closures.
- **Tasks:** one tokio task per collector, strategy and executor. There is no dispatcher: `collector_task.rs` pushes each `Envelope<E>` (`Arc<E>` + source, seq, received_at) directly into subscriber channels.
- **Delivery per subscription** (`delivery.rs`). `Lossless` uses one mpsc per strategy; `DropNewest` a second mpsc (`try_send`). `DropOldest` uses a broadcast and `Latest` a watch, each shared per collector (own channel if filtered). `Dropped`/`Lagging` (high/low watermark) travel via atomic slots + a coalescing wake mpsc(1) (`signals.rs`). `OnFull::Abort` ends the engine.
- **Strategy loop** (`strategy_task.rs`): a manual `poll_fn` rotates over system/lossless/drop-newest/wake branches for fairness and determinism. It publishes an idle flag for the test harness and runs `catch_unwind` around handlers. `OnPanic::Restart` rebuilds the strategy via its factory and sends `Restarted` + `GapUnfilled`.
- **Router** (`router.rs`): an action goes to every executor whose `handles` is true. `try_submit` reserves all permits first. Executors use a semaphore for concurrency, a timeout and `catch_unwind`.
- **Shutdown** (`Engine::run`): collectors stop → strategies drain and get `ShutdownRequested` → opportunity tasks (`StrategyCtx::spawn`) are cancelled → executors drain. One shared deadline, then abort.
- **Reconnect:** the engine holds the last cursor and calls `connect(resume)` in a backoff loop. `Item` is an enum (`Event` / `GapFilled` / `GapUnfilled`). A collector reporting `is_exhausted()` is stopped instead of reconnected.
- **`test_util::Harness`:** runs the engine on its own thread with a paused current_thread runtime. It adds a capture executor and ends when `EngineHandle::is_idle()` holds twice in a row.
- **`obs.rs`:** metric names, plus no-op stubs when feature `metrics` is off.
- **`eth`:** facade over `alloy1`/`alloy2` (renamed deps, re-exported as `eth::alloy`). Code must compile with both backends.
  - `blocks.rs` holds the block collectors, all built on `follower.rs::Follower`, which follows newHeads (fallback: polling). It backfills headers and detects reorgs via `parent_hash`, sharing the `recent` buffer across reconnects.
  - `LogCollector` (`logs.rs`) combines the logs subscription, a `LogBackfill` and its own `Follower`. A `Delivered` buffer (shared across reconnects) dedupes logs and emits `Removed` for blocks that left the chain, because Anvil never sends `removed: true`.
  - `ProviderSource::ws` opens WebSocket connections with alloy's auto-reconnect disabled (it would silently lose events).
  - `TransactionExecutor` fills and signs via a FillProvider (`CachedNonceManager`), broadcasts the raw tx to all endpoints and reports `TxFeedback` through a `ChannelCollector`.

## Conventions

- `#![deny(missing_docs)]`, `unsafe_code = "forbid"`. No inline comments except a one-line non-obvious "why". Rustdoc stays short.
- Don't name thiserror fields `source` (use `source_name`).
