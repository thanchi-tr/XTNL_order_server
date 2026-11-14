---
name: live-trading-server
description: Architecture, read-only broker ingestion (StoneX & cTrader ProtoOA), 4-topic Bulkhead pub-sub (Order/Position/Price/Account DTOs), tiered state integrity (Redis > PostgreSQL > Image Backup), fail-closed bootstrap, and level-triggered Oversee risk sweep for XTNL Phase 1. Invoke explicitly when modifying adapters, DTOs, pub-sub queues, persistence, or Oversee rules.
disable-model-invocation: true
---

# 1. Scope & 6-Layer Execution Lockout (Read-Only Phase 1)
This service implements **Phase 1: Read-Only Broker Ingestion, State Normalization, Tiered Persistence, and Level-Triggered Risk Oversight (`Oversee`)**. Live order placement, modification, and cancellation are structurally prohibited:

* **Stack Standard:** Python 3.12+ (`asyncio` + `uvloop`, `frozen=True` dataclasses), Redis 7.4+ with embedded Lua 5.1 (`EVALSHA`), and PostgreSQL 17 (`asyncpg` binary protocol, `pgBackRest` WAL archiving).
* **Execution Lockout Guard (`P2`):**
  1. **PreToolUse Hook:** `.claude/hooks/pre_tool_guard.py` blocks agent edits defining `place_order`, `modify_order`, or `cancel_order`.
  2. **Interface Lock:** `BrokerAdapter` ABC defines zero order-mutation methods.
  3. **Environment Gate:** `BROKER_ENV` must be `sandbox` or `live_readonly`; any other value aborts startup with `ExecutionLockedError`.
  4. **ReadOnlyTransport Proxy:** StoneX REST denies mutation paths (`/newtradeorder`, `/newstoplimitorder`, `/updatetradeorder`, `/updatestoplimitorder`, `/cancel` — deny by **path**, not HTTP verb, because `POST /activeorders` is read-only). cTrader `ProtoOA` enforces a strict `payloadType` allow-list (`accounts` OAuth scope only).
  5. **ActionGate (`P11`):** Pre-wired fail-closed gatekeeper for Phase 2 (`ExecutionGateway`), denying all outbound actions whenever `halt:state` or `redis:boot_id` is missing/mismatched.
  6. **Single-Leader Fencing (`P12`):** Deploy Phase 1 in single-leader mode on London (`eu-west-2` / Equinix LD4 proximity) with lock-free SQL fencing (`WHERE (SELECT epoch FROM engine_epoch) = $1`) on state writes.

# 2. Pluggable Adapters & Causal Data Model (`P1`, `P3`)
Adapters (`StoneXAdapter`, `PepperstoneAdapter`, `MockBrokerAdapter`) are pure translation layers with **zero imports** from `src.db`, `src.cache`, `src.pubsub`, `src.integrity`, or `src.oversee`:

* **Adapter Contract (`BrokerAdapter`):** Implements `fetch_open_orders()`, `fetch_order_status(client_order_id)`, `fetch_positions()`, `fetch_position_status(position_id)`, `fetch_account_snapshot()`, `fetch_cash_flows(since_ms)`, `fetch_instruments()`, `stream_ticks(symbols)`, `ensure_ticks(symbols)`, `stream_order_updates()`, `stream_positions()`, and `stream_account()`.
* **In-Adapter Instrument Map:** `fetch_instruments()` populates a private read-only `self._instruments: dict[str, InstrumentSpec]` during `SM4 SNAPSHOTTING` before streams start, converting broker quantities (`CONTRACTS`, `STAKE`, `CENTI_UNITS`, `LOTS`) into canonical base-currency `units`. Never block the socket stream loop on inline REST enrichment.
* **Canonical Frozen DTOs:**
  * `OrderDTO`: `client_order_id`, `broker_order_id`, `account_id`, `adapter_id`, `symbol`, `side`, `status`, `entry_price`, `stop_loss?`, `take_profit?`, `ordered_units?`, `remaining_units?`, `is_reduce_only`, `linked_position_id?`, `instrument_unknown`, `version`, `checksum`. (No risk field on DTOs; `Oversee` owns all risk math).
  * `PositionDTO` (`SM7`): Tracks post-fill open trades (`position_id`, `units`, `entry_price`, `stop_loss?`, `take_profit?`, `status: OPEN | CLOSED`, `accrued_swap`, `commission`, `version`, `checksum`) so post-fill Stop-Loss removals or moves are continuously audited after entry orders terminate at `FILLED` (`SM1`).
  * `AccountDTO` & `CashFlowDTO`: Versioned `balance` and `realised_pnl_today` plus unversioned live fields (`equity?`, `margin_used`, `free_margin`, `unrealised_pnl`) coalesced into `account_live:{id}`; `CashFlowDTO` deduplicates deposits/withdrawals on `transaction_id`.
  * `PriceDTO`: `adapter_id`, `symbol`, `bid`, `ask`, `spread_pips`, `timestamp_ms`, `seq_id`.
* **Causal Version Ordering:** Every stateful DTO carries `version = (broker_seq, broker_updated_at_ms, observed_seq)` plus orchestrator HLC `seq_id = max(epoch_ms << 20, last + 1)` seeded from PostgreSQL. Newer broker version always wins over slow REST snapshots; identical checksums advance version as a no-op; lower versions are dropped and logged to `event_log`.

# 3. 4-Topic Bulkhead Bus & Non-Blocking DLQ (`P4`, `P5`, `P6`)
`IngestionOrchestrator` is the sole publisher and **never awaits subscribers, disk I/O, or PostgreSQL**:

* **Four Isolated Bounded Queues:** `market.price.dto`, `account.state.dto` (`AccountDTO | CashFlowDTO`), `position.state.dto`, and `orders.internal.dto`, published via `put_nowait()`.
* **Socket Drain & Backpressure Invariant:** Always keep reading broker sockets so heartbeat/pong control frames never stall behind unread ticks. On `QueueFull` or validation error (`schema`, `checksum`, `future_timestamp > receipt + 5s`), append synchronously in memory to `DlqSpooler.offer()`. `DlqSpooler.flush()` batch-inserts into PostgreSQL `events_dlq` (or spills to CRC-framed `dlq_spill.jsonl` via `asyncio.to_thread` if DB is down). If both queue and ring fill, set `backpressure_overflow` to drop price frames at the parser, delete that stream's health key, and trigger `SM4 RESYNCING` (`re-subscribe lost streams + REST reconcile`) once ring `< 50%`.
* **Per-Stream TTL Health Keys:** Maintain `conn:{adapter_id}:{prices | orders | account}` (`PEXPIRE 30s`), refreshed on transport heartbeats only while that subscription is `SUBSCRIBED` (plus a 60 s REST `fetch_account_snapshot()` check when flat).
* **Check-Before-Retry DLQ (`SM2`):** `DlqRecoveryWorker` leases rows only when their adapter is `STREAMING`, queries the broker for authoritative current state (`fetch_order_status`, `fetch_position_status`, `fetch_account_snapshot`, or `fetch_cash_flows`), and re-submits the broker's fresh row via `IngestionOrchestrator`. Quarantine after `5` deterministic failures.

# 4. Tiered State (`Redis > PostgreSQL > Image`) & Fail-Closed `Oversee` (`P7`–`P11`)
* **Write-Through Atomicity (`P6`, `P7`):** Consumers (`OrderStateConsumer`, `PositionStateConsumer`, `AccountImageProjector`) commit state + `outbox_events` + `event_log` in one PostgreSQL transaction first, then call `apply_state.lua` to conditionally `SET` Redis (`incoming version > stored version`), `XADD oversee:feed` (capped at `MAXLEN ~ 1,000,000` and trimmed via `XTRIM MINID`), and update commutative `XOR` `state:digest`. `PriceSubscriber` writes `price:{adapter}:{symbol}` and `clean_mid:{adapter}:{symbol}` via a lightweight Lua script without `oversee:feed` entries.
* **Fail-Closed Redis Bootstrap (`SM8`):** `ActionGate` denies all actions unless `redis:boot_id == run_id:epoch` and `halt:state` matches `MAX(halt_events.id)`. On Redis restart/loss, `IntegrityWorker` (`SM3`, running every `5.0 s` over settled `[last - MAX_TXN_MS, now - 500 ms]` rows without pausing symbols) executes `SM8`: restore `halt:state` $\rightarrow$ create `oversee:feed` consumer group $\rightarrow$ seed prices $\rightarrow$ heal entity keys from PostgreSQL $\rightarrow$ write `redis:boot_id` **last**.
* **Level-Triggered 1 Hz `RiskSweep` (`P9`):** `Oversee` uses **no volatile timers**. A feed observer wakes `RiskSweep` on `oversee:feed` updates, and `RiskSweep` evaluates every account every `1000 ms` against a `READ ONLY REPEATABLE READ` PostgreSQL snapshot + Redis live marks:
  * **Shared Risk Math:** Directional risk floored at `0`, converted to `AccountDTO.currency` via clean cross mid (`spread <= max_clean_spread_pips`, using `last_clean_marks` during `SM10` market breaks with `FLAG stale_mark`).
  * **Rule Plugins:**
    * `MaxPositionRiskRule`: $\le 1.0\% \text{ equity\_mark} \rightarrow \text{PASS}$; $1.0\%\text{–}1.5\% \rightarrow \text{FLAG}$; $> 1.5\% \rightarrow \text{SOFT\_HALT}$ (per-order `ordered_units` and open positions).
    * `MaxAggregateOpenRiskRule`: Sums opening `remaining_units` across working orders + open positions (`confirm_ms: 1000`); $> 2.0\% \rightarrow \text{FLAG}$; $> 3.0\% \rightarrow \text{SOFT\_HALT}$.
    * `MissingStopLossRule`: Tracks durable `effective_stop_missing_since_ms` in PostgreSQL on opening orders and open positions without an effective stop; $\ge 2000\text{ ms} \rightarrow \text{SOFT\_HALT}$.
    * `MaxDailyLossRule`: `(equity_mark - sod_equity - net_transfers_today) / sod_equity < -3.0%` (`confirm_ms: 1000`, `17:00 America/New_York` roll) $\rightarrow \text{HARD\_HALT}$.
    * `MarginUsageRule`: `margin_used / equity_mark > 80%` $\rightarrow \text{FLAG}$; $> 95\% \rightarrow \text{SOFT\_HALT}$.
  * **Hysteresis & Alerting (`SM5`, `SM6`):** States require `confirm_ms` to activate, `5000 ms` below a `10%` margin to release, escalate `SOFT_HALT` to `HARD_HALT` after `900,000 ms` of uncapped active time, and dispatch deduplicated episode alerts via `notification_outbox` (`FLAG` $\rightarrow$ email; `SOFT/HARD` $\rightarrow$ email + mobile push).