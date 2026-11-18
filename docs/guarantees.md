# Guarantees

What botwright promises about delivery, ordering, failures and shutdown, and what it does not.

## Tasks

- Every collector, strategy and executor runs as its own tokio task.
- A collector task distributes each event to its subscribers itself. There is no dispatcher task between collector and strategy.
- A strategy processes events strictly one at a time and is the only writer of its state. Long-running work goes into opportunity tasks started with `StrategyCtx::spawn`; the engine bounds their number (`max_tasks`, default 1024), cancels them on shutdown and logs their errors and panics.
- An executor runs up to `concurrency` actions at once (default 1), each with an optional timeout.

## Delivery

Each subscription picks a `Delivery`:

| Delivery | When the channel is full | Channel |
|---|---|---|
| `Lossless { capacity }` (default, 1024) | the collector waits | one mpsc per strategy, shared by all its lossless subscriptions |
| `DropNewest { capacity }` | the new event is dropped | a second mpsc per strategy, filled with `try_send` |
| `DropOldest { capacity }` | the oldest event is dropped | one broadcast per collector, shared by its unfiltered `DropOldest` subscribers |
| `Latest` | only the newest value is kept | one watch per collector, shared by its unfiltered `Latest` subscribers |

- **Dropping is never silent.** The affected strategy receives `SystemEvent::Dropped { source, count }` and `botwright_dropped_total` counts it.
- **Backpressure is visible.** When a lossless or drop-newest channel reaches the lagging threshold (default 80 % of its capacity) the strategy receives `SystemEvent::Lagging { source, depth }`. It is raised again only after the fill level fell to half the threshold. Fill level and waiting time are exported as `botwright_channel_depth` and `botwright_dispatch_wait_seconds`.
- **Backpressure costs.** A slow lossless subscriber holds up the collector and with it all other subscribers of that collector. For high-rate sources such as the mempool, use `DropOldest`.
- **Stuck channels can end the engine.** With `OnFull::Abort { after }` (lossless only) the engine stops with `ExitReason::ChannelAborted` when the collector waited longer than `after`. The default is `OnFull::Wait`.
- **Filters** run in the collector task before sending. A filtered `DropOldest` or `Latest` subscription gets its own broadcast or watch.
- **No copies.** Events are shared as `Arc<E>` in an `Envelope` that also carries source, sequence number and receive time.
- **Actions** go to every executor whose `handles(&action)` returns `true`, each through its own bounded queue. `submit` waits while a queue is full; `try_submit` fails with `SubmitError::Full` without enqueueing anywhere. An action no executor handles fails with `SubmitError::Unrouted`.

## Ordering

- Per collector and subscription, events arrive in the order the collector produced them. With `DropOldest`, `DropNewest` and `Latest` events may be missing in between, but are never reordered.
- All lossless events of one strategy share one channel and therefore arrive in arrival order across sources. Between different channels of a strategy there is no order; use the envelope's `seq` and `received_at`.
- When several channels are ready, the strategy loop takes them in rotation, so no source starves another.
- Replaying the same source gives the same order every time. Run under `test_util::Harness`, the same recording yields exactly the same actions.

## System events

| Event | Meaning |
|---|---|
| `CollectorConnected { source }` | the stream is (again) established |
| `CollectorDisconnected { source, error }` | the stream broke; a reconnect with backoff is running |
| `GapFilled { source, from, to }` | the gap after a break was backfilled completely |
| `GapUnfilled { source, from, to }` | the gap cannot be backfilled; rebuild state |
| `Lagging { source, depth }` | the channel is over the lagging threshold |
| `Dropped { source, count }` | events were dropped (`DropOldest`, `DropNewest`) |
| `Restarted { reason }` | the strategy was rebuilt by its factory after a panic |
| `ShutdownRequested` | the engine is shutting down |

## Collectors and cursors

- `Collector::connect(resume)` receives the last cursor the engine saw. The engine keeps the cursor outside the collector, so it survives every reconnect.
- An `Err` in the stream or the end of the stream ends the connection. The engine reconnects with exponential backoff and jitter (default 100 ms up to 30 s, reset after 60 s of healthy connection). With `max_retries` set, exhausting the retries ends the engine with `ExitReason::CollectorFailed`.
- A panic in a collector is treated like an error.
- A collector reporting `is_exhausted()` is stopped when its stream ends instead of being reconnected.

## EVM collectors

- `BlockCollector` delivers canonical blocks in chain order. Missing headers after a break are fetched up to `max_backfill` (default 1000); beyond that, `GapUnfilled` is reported. Reorgs are detected via `parent_hash` up to `max_reorg_depth` (default 64) and reported as `ChainEvent::Reorg { common_ancestor, dropped }` followed by the new canonical blocks.
- `LogCollector` subscribes to logs and, after a break, backfills from the last delivered log with `eth_getLogs` (or `HyperSyncBackfill`). Every log is delivered exactly once. It also follows the canonical chain: when a block whose logs were delivered leaves the chain, those logs arrive again as `LogEvent::Removed` (newest first), followed by the logs of the new chain as `LogEvent::Added`. Removals the node reports itself (`removed: true`) are deduplicated with this.
- `LogsInBlockCollector` and `FullBlockCollector` inherit gap and reorg handling from `BlockCollector`.
- Use `ProviderSource::ws` (the `ws` constructors) rather than a shared WebSocket provider with alloy's automatic reconnect: the automatic reconnect resubscribes silently, so events from the outage are lost without the collector noticing.

## Failures

| Component | Error | Panic |
|---|---|---|
| Collector | reconnect with backoff, `CollectorDisconnected` | same as an error |
| Strategy `on_event` / `on_system` | `OnError::LogAndContinue` (default) or `OnError::Abort` | `OnPanic` (below) |
| Opportunity task | logged | caught and logged |
| Executor | logged, `botwright_actions_total{result="error"}` | caught and logged, the executor keeps running |

Strategies are treated differently because they alone hold state that does not recover by itself. A strategy that panicked halfway through `on_event` has half-updated state and has lost the triggering event. Botwright never continues with that object:

- `OnPanic::Abort` (default): the engine ends with `ExitReason::StrategyPanicked`.
- `OnPanic::Restart { max, within }` (only with `strategy_with_factory`): the factory builds a new object, `init` runs again, then the strategy receives `Restarted` and `GapUnfilled` for every source. More than `max` restarts within `within` end the engine.

An error from `init` ends the engine with `ExitReason::StrategyFailed`.

## Start and shutdown

Start order: executors, strategies (`init`), collectors.

Shutdown is triggered by `EngineHandle::shutdown`, SIGINT or SIGTERM (feature `signal`), or a fatal error. One deadline (`shutdown_timeout`, default 10 s) covers all steps:

1. Collectors stop.
2. Strategies drain their channels until the deadline, then receive `ShutdownRequested`.
3. Opportunity tasks are cancelled.
4. Executors drain their queues until the deadline.
5. `Engine::run` returns the `ExitReason`.

Tasks still running at the deadline are aborted.

## Notifications

`Notifier::notify` never blocks. `QueuedNotifier` puts notifications into a bounded queue that a worker drains; on overflow the notification is dropped and counted in `botwright_notifications_dropped_total`.

## Not guaranteed

- Exactly-once delivery across process restarts: cursors live in memory.
- Ordering between different channels of one strategy.
- That a sent transaction is included; `TxFeedback::NotIncluded` reports a missing receipt after the receipt timeout. Replacing or cancelling stuck transactions is left to the bot.
- Nonce continuity after a failed send: `CachedNonceManager` has already advanced the nonce, so a transaction that no endpoint accepted leaves a nonce gap until the process restarts.
