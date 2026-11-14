---
paths:
  - "src/integrity/**"
  - "src/db/**"
  - "src/cache/**"
---

# Tiered Integrity (`Redis Cache > DB > Image Backup`), Bootstrap & DLQ Rules

1. **Tier 1 Redis Is an Intentional Duplicate:**
   - **Entity keys** (copies of PostgreSQL current rows): `order:{client_order_id}` (`orders_state`), `position:{position_id}` (`positions_state`), `account:{account_id}` (`account_state`), `instrument:{adapter_id}:{symbol}` (`instruments`). They serve the dashboard and hot-path reads; the RiskSweep reads entity state from PostgreSQL (`oversee-rules.md` §2).
   - **Live keys** (not in PostgreSQL, not versioned, latest value wins, each with its timestamp): `price:{adapter_id}:{symbol}` (latest quote), `clean_mid:{adapter_id}:{symbol}` (latest mid whose spread is within `max_clean_spread_pips`, updated only by such a tick), `account_live:{account_id}` (equity?, margin_used, free_margin, unrealised_pnl), and `conn:{adapter_id}:{prices | orders | account}`.
     - `conn:` keys have `PEXPIRE 30000`. They are refreshed only while the subscription is `SUBSCRIBED`, and deleted during an unreconciled gap.
   - **Control keys:**
     - `halt:state` = (`effective_level`, `halt_event_id`).
     - `redis:boot_id` = `run_id:epoch` (Redis `INFO run_id` + the engine epoch), written last by SM8. `apply_state.lua` compares it with the expected value passed in `ARGV`.
     - `redis:boot_lock` (SM8 single-flight).
     - `state:digest` and the stream `oversee:feed`. There is no timer ZSET: Oversee keeps no timers (`oversee-rules.md` §2).
   - Write order is always **PostgreSQL first, Redis after commit** for entity keys.
   - **Entity writes go only through `apply_state.lua`** (one atomic script):
     - Version compare as in `dto-contracts.md` §2, using fixed-width strings. Same checksum = no-op that only advances the stored version (PostgreSQL's current row was advanced in the same transaction first). Lower version = ignored.
     - `XADD oversee:feed` (with the entity's `account_id`) only when the SET applied and the key is an order, position or account; `instrument:` keys are never fed. A linked child's entry wakes the sweep for the same account, which re-evaluates the parent too, so no parent re-feed is needed.
     - Update `state:digest` commutatively: XOR of `SHA-256(entity_key || version || checksum)`, XOR-ing out the old hash and XOR-ing in the new one. PostgreSQL keeps the same digest in the same transaction. The digest covers entity keys only.
     - Return `NO_BOOT` when `redis:boot_id` is missing, unless the caller holds `redis:boot_lock`. A `NO_BOOT` write is **not applied**: the caller keeps it and signals the integrity worker, and SM8 re-applies it **before** writing `redis:boot_id`. The integrity commit window would also catch it.
   - **Live keys** (`price:`, `clean_mid:`, `account_live:`, `conn:`) are written by a separate light script that touches neither `state:digest` nor the quiescence clock, needs no boot check and writes no feed entries.
   - **Feed writers** (the only two; the feed only wakes the RiskSweep, `oversee-rules.md` §1):
     - `apply_state.lua`: entity changes, including heals and SM8's re-apply.
     - The cash-flow commit: XADDs an `ACCOUNT` entry, whether or not `account:` changed.

     A ruleset reload calls `sweep_now()` directly and writes no feed entry.
   - **Statement scope:** entity keys are written only after commit and only through `apply_state.lua`; live keys use the light script; `halt:state` uses its CAS script.
   - `halt:state` is written only by its own CAS script, which applies only when the incoming `halt_event_id` is greater than the stored one. trip, clear and the bootstrap all use it.
   - **Bounded memory:**
     - `XTRIM oversee:feed MINID` (oldest un-acked) every 10 s, plus `MAXLEN ~ 1000000` on every `XADD`, with a lag alert at 50 % of the cap.
     - `maxmemory-policy noeviction`, with a memory alert before the limit.
   - Redis is never the source of truth; AOF is optional.
2. **Redis Data Loss — SM8 bootstrap (owned by the integrity worker, fail-closed):**
   - **Detection:** `NO_BOOT` from `apply_state.lua`, or the integrity tick sees `redis:boot_id` ≠ `run_id:epoch`. Every leader change also runs SM8, because LEADER_INIT does `FLUSHDB` of the engine's own Redis database first (never `FLUSHALL`; see `operations.md` §4); no Redis from an earlier term is trusted.
   - **Single-flight:** the bootstrap starts only if `SET redis:boot_lock <owner> NX PX <ttl>` succeeds (renewed while running). Every other signal just waits.
   - `ActionGate` denies everything until `redis:boot_id` = `run_id:epoch` again (see `oversee-rules.md` §8). The RiskSweep keeps running from PostgreSQL throughout; only the feed observer waits.
   - **Order (each step retries on failure; the gate stays closed):**
     1. Restore `halt:state` from the latest `halt_events` row through the CAS script. No new row and no alert.
     2. `XGROUP CREATE oversee:feed <group> 0 MKSTREAM`; a `BUSYGROUP` reply counts as success.
     3. Seed `price:{adapter_id}:{symbol}` and `clean_mid:{adapter_id}:{symbol}` from PostgreSQL `last_clean_marks` and `account_live:{id}` from the latest `account_snapshots` sample (marked `sampled`), each with its original timestamp and **only where the key is absent or older** (the script compares timestamps), so a fresher live value is never overwritten. Seeded prices are stale by timestamp, so the sweep caps them (`oversee-rules.md` §5); a sampled live field is UNKNOWN to `MarginUsageRule`. Then wait, bounded, for `conn:{adapter}:prices` to be live; on timeout, continue.
     4. Heal: as lock holder, re-apply every PostgreSQL entity through `apply_state.lua`.
     5. As lock holder, re-apply the queued `NO_BOOT` writes.
     6. Write `redis:boot_id` **last** and release the lock. For an in-place SM8 (no leader change, so no SNAPSHOTTING follows), request `fetch_account_snapshot()` from each STREAMING adapter after READY, so the live fields stop being sampled. Every step retries only within the LEADER_INIT deadline (`operations.md` §4); in place, a failing step pages after 60 s.
3. **Integrity Verification (every `INTEGRITY_CHECK_INTERVAL_SEC`, default 5.0 s, non-blocking):**
   - **Step 1 (Redis vs. DB):**
     - Take rows **committed** in `[last_check − MAX_TXN_MS, now − 500 ms]` (commit time; `MAX_TXN_MS` bounded by `statement_timeout` and `idle_in_transaction_session_timeout`). Compare each row's version and checksum with Redis using pipelined reads.
     - Add a rotating sweep of K keys per tick. Compare the full XOR digest only when quiet (no entity write for 500 ms and no PostgreSQL row newer than 500 ms).
     - A mismatch must persist for **two ticks**. Then:
       - Redis **behind**: heal in place through `apply_state.lua`.
       - Redis **ahead** of PostgreSQL's current row: alert, `DEL` the key, re-apply from PostgreSQL. A version CAS can never correct a key that is ahead.
     - **Never pause a symbol or stop consumers.**
     - Each tick also compares `halt:state.halt_event_id` with `MAX(halt_events.id)`. **Behind**: re-apply through the CAS. **Ahead** (possible only after a restore): alert, `DEL halt:state`, re-apply the latest row. It also compares `redis:boot_id` with `run_id:epoch`; a difference signals SM8.
   - **Step 2 (PostgreSQL integrity):** checksums and `seq_id` **monotonicity** (no regression; the HLC has gaps by design; the check is timeline- and epoch-aware). The engine **never restores PostgreSQL** (see `operations.md` §7). On corruption or regression:
     1. **HALTED:** halt ingestion and fire a critical alert.
     2. **SWITCHING_OVER** (damage confined to the primary): Patroni switchover to the sync standby after it passes the same check, then `patronictl reinit` the bad member.
     2b. **MANUAL_INTERVENTION** (from HALTED when the standby is damaged too, or from SWITCHING_OVER when the switchover fails): an operator runs the Tier 3 restore runbook through Patroni.
     3. **RECONCILING:**
        - resume price ingestion first (sockets drained); the RiskSweep keeps running but caps every result at SOFT until the reconcile commits (`oversee-rules.md` §2);
        - reconcile with the broker: `fetch_open_orders()`, `fetch_positions()`, `fetch_account_snapshot()`, `fetch_cash_flows(since the last committed broker_ts, or the restore target after an operator restore)`, plus watermark re-reads by id (never close on absence);
        - commit the reconcile;
        - then **`FLUSHDB`** (no partial alternative), which also removes `redis:boot_id`, so SM8 is triggered and rebuilds Redis from the restored data.
     4. Resume full ingestion only after SM8 is READY.
4. **Tables (PostgreSQL):**
   - HA / runtime tables: `engine_epoch` (fencing token), `leader_terms`, `last_clean_marks(adapter_id, symbol, bid, ask, ts)`, `cluster_state` (durable SAFE_HOLD, `operations.md` §4), `notification_dedupe` (alert dedupe state, so a new leader doesn't re-send), `outbound_intent` (Phase 2), versioned `rulesets` and `market_sessions`. Large tables are partitioned by `seq_id` range (`operations.md` §11); `partition_archive` is the ledger of verified partition exports, written (fenced) by the leader's lifecycle task after the archive file has been read back, and the detach requires its row.
   - `rulesets` is operator-owned config: the engine only reads it (and writes `ruleset_audit`, fenced). Rows are written only by the audited operator CLI, validated with the engine's own `parse_ruleset` and stored as the complete `ruleset_json`, under a table lock rather than the epoch fence (`operations.md` §12).
   - Every engine state write is fenced in the same statement on `(SELECT epoch FROM engine_epoch) = $1` (single-leader lock-free fencing, `operations.md` §2; no epoch lock), and a 0-row result where a row was expected raises `EpochMismatch`. Named exceptions: the RiskSweep's READ ONLY snapshot, the idempotent spill-drain `INSERT` into `events_dlq` (§5) and the cTrader `token_generation` compare-and-set.
   - `orders_state`, `positions_state` and `account_state` hold each entity's **current row** (version, checksum). Sweep-owned `stop_watch(entity_type, entity_id, effective_stop_missing_since_ms)`: written the first time the RiskSweep sees `needs_stop()` with no effective stop, cleared when an effective stop is seen; it is the durable stop-loss grace deadline. `stream_health(adapter_id, stream, conn_lost_at)`: set for every row by LEADER_INIT step 1 (`COALESCE(conn_lost_at, now())`) and by the orchestrator when a subscription leaves `SUBSCRIBED` or a gap opens; cleared by the orchestrator only once this term has the stream `SUBSCRIBED` and its reconcile committed (the account fence, `oversee-rules.md` §2). Operator-owned `breach_acks` (acknowledgements). `sweep_quarantine` (accounts whose pass failed for 5 min; retried every 10 min, released by a good pass or `release_account()`). `realised P&L` arrives as `CashFlowDTO` kind `TRADE_PNL` (one per closing deal), so `cash_flows` is the full ledger of booked balance moves. Also `outbox_events`, `event_log` (append-only; also records dropped lower versions) and `instruments`.
   - `account_snapshots`: versioned `AccountDTO` history. Live fields are sampled into it at a fixed interval, not on every tick.
   - `cash_flows`: `CashFlowDTO` rows, unique on `transaction_id`. Each commit XADDs an `ACCOUNT` feed entry.
     - `net_transfers_today` = the transfers whose balance effect is **not** already in `sod_equity`: every `DEPOSIT` / `WITHDRAWAL` with `broker_ts` at or after the roll, plus pre-roll `DEPOSIT`s explained only after the roll, up to the `unexplained_increase_at_roll` stored in `sod_marks` (that amount was excluded from `sod_equity`). A pre-roll `WITHDRAWAL` explained after the roll is already in the start-of-day balance and does not count.
     - `FINANCING` / `COMMISSION` / `ADJUSTMENT` are P&L.
     - An adapter with no transfer source contributes 0, and the day is marked `transfers_unverified`.
   - `sod_marks`: one row per account per trading day at the **17:00 America/New_York** roll (DST-aware), written by the RiskSweep's first pass at or after the roll (LEADER_INIT step 4 only catches up a missed row). It stores `sod_equity` with its inputs: balance, position marks, the clean mids **and their timestamps** taken from before the roll, and `unexplained_increase_at_roll`. A mid-session first start computes it at start-up and marks it `partial_session`; so is any row written more than 30 s after the roll. A row is also `partial_session` when an input is missing, when a held symbol's newest clean mid is more than 5 min older than that symbol's last scheduled close or break before the roll (the roll itself if the symbol was open), or when no leader term covered the 5 min before that point (a leaderless period). Scheduled closures (weekends, holidays, early closes) therefore never set it. The trading day that starts at Friday's 17:00 roll covers the weekend; the Sunday open is not a roll. While the day's row is `partial_session`, `MaxDailyLossRule` results are capped at SOFT.
   - `oversee_events`: one row per rule-state transition written by the RiskSweep, unique on (`rule_id`, `subject_type`, `subject_id`, `transition`, `sweep_id`) and partitioned by `sweep_id` range (`sweep_id` is an HLC value, so partitions follow time and the unique key includes the partition key).
   - `adapter_auth_state`: `AUTH_FAILED` per (adapter, credential generation), a cluster-wide fact. The credential generation is the Secrets Manager version id, or `token_generation` for the cTrader token row (a monotonic counter bumped on every save). A leader does not retry that adapter until the generation changes or an operator re-arms it (`operations.md` §16).
   - `halt_breaches`: one row per (`rule_id`, `subject_type`, `subject_id`), written by the RiskSweep (`oversee-rules.md` §2) and, for a reviewed clear, by the operator `clear()` under `HALT_LOCK`: `level` (FLAG, SOFT, HARD), `cap`, `reason`, `pending_since`, `active_since`, `release_pending_since`, `unknown_since`, `uncapped_ms` (the accumulated escalation clock), `resolved_at`, `resolved_by` (sweep, day_end, entity end, reload, operator), `below_soft_since`, `last_sweep_id` and `clear_generation` (compare-and-set). Acknowledgements live in `breach_acks`. The subject is the account for `MarginUsageRule` and `MaxAggregateOpenRiskRule`, (account, trading_day) for `MaxDailyLossRule`, and the order or position for `MaxPositionRiskRule` and `MissingStopLossRule`. FLAG rows never affect the halt level.
   - `halt_events`: append-only (`id`, `level`, `effective_level`, `hard_latch`, `rule_id`, `reason`, `by`, `at_ms`), the truth for the halt level. Rows are inserted only by the RiskSweep's `trip()` and system `clear()` and by the operator `clear()`, all under `pg_advisory_xact_lock(HALT_LOCK)`, and `effective_level` is computed inside that lock.
   - `notification_outbox`: written in the same transaction as the transition / halt row.
5. **Dead-Letter Queue (`events_dlq`):**
   - QueueFull, schema-invalid and `future_timestamp` events arrive through the `DlqSpooler`; consumer failures are written by the consumer. A lower version is never parked.
   - Columns: `dlq_id`, `origin_dlq_id` (lineage), `event_type` (`ORDER` | `POSITION` | `ACCOUNT` | `CASH_FLOW`), `entity_id` (`broker_order_id`, `position_id`, `account_id` or `transaction_id`; for orders it is the broker id that `fetch_order_status()` re-reads, an accepted deviation from `client_order_id`, which on StoneX is the broker `OrderId` anyway), `adapter_id`, `raw_payload`, `payload_hash` (unique), `error_reason`, `retry_count`, `status`, `next_retry_at_ms`. Price ticks are never parked.
   - **Lineage:** a re-submitted row carries its `origin_dlq_id`. If it is parked again with the same (`entity_id`, `error_reason`) and that reason is **deterministic** (schema-invalid, checksum, `future_timestamp`, illegal transition), the original row's `retry_count` is incremented instead of a new row starting at 0, so an event that is invalid every time is quarantined, with an alert, after `MAX_DLQ_RETRIES`. Transient reasons (`QueueFull`, consumer timeout) start a fresh count; they page if one entity has been parked for more than 15 min.
   - **Spill file format:** one file per node (`XTNL_SPILL_PATH`; default `var/dlq_spill.jsonl`, `/var/lib/xtnl/dlq_spill.jsonl` on a deployed node); each record is framed (length + CRC). A torn tail left by a SIGKILL is moved aside with an alert; a spill parse error never fails LEADER_INIT.
   - **Spill drain:** the spooler's `flush()` drains the spill file into `events_dlq` (`INSERT … ON CONFLICT (payload_hash) DO NOTHING`) as soon as PostgreSQL accepts writes again, not only at start-up, and truncates it after commit. Because these inserts are append-only and idempotent, **any node** may drain its own spill file, outside the epoch check, so a fenced leader that restarts as STANDBY still delivers it; the leader's DLQ worker processes the rows. Every node publishes its spill-file size in its health record, and a non-empty spill file on any node for more than 60 s raises an off-node alert naming the node.
   - **Start-up barrier:** any spill file left by a crash is drained at start-up (database only). A row is leased only while its adapter is `STREAMING`. A lease that expires, or an adapter that leaves `STREAMING`, returns the row to `QUEUED` without `retry_count++`.
   - **Check before retry:**
     - Read the broker's row for that id: `fetch_order_status()`, `fetch_position_status()`, `fetch_account_snapshot()`, or, for `CASH_FLOW`, `fetch_cash_flows(since min(last committed, oldest parked cash flow))` matched on `transaction_id`. `[converged]` means that read succeeded and, for `CASH_FLOW`, that the `transaction_id` was found. A cash-flow read that succeeds without the id is not converged: keep the payload and back off; after `MAX_DLQ_RETRIES`, quarantine, alert, and mark the day `transfers_unverified`.
     - Re-submit that **fresh broker row**, versioned at its own read, through the orchestrator. The parked payload is dropped.
     - A converged row whose `error_reason` was an illegal transition goes to `events_quarantine` instead.
     - A lease that expires, or an adapter that leaves `STREAMING`, returns the row to `QUEUED` without `retry_count++` (the BACKOFF path requires the adapter to be `STREAMING`).
     - After `MAX_DLQ_RETRIES = 5` failures, quarantine.
