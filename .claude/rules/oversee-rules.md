---
paths:
  - "src/oversee/**"
---

# Oversee (Queue Observer, Risk Sweep, Halt & Alerts) Rules

Oversee has two parts. The **feed observer** watches the queue of committed changes and wakes the sweep for that account within milliseconds. The **RiskSweep** runs the rule plugins and is the only automatic writer of rule states and the halt level; the one other writer is the operator `clear()` under `HALT_LOCK` (§4). It re-evaluates every account from a consistent snapshot every second, so halt state is level-triggered: it never depends on a particular event, timer or redelivery arriving.

1. **Feed observer (queue observer of committed state):**
   - Runs only on the engine leader (`operations.md`). It consumes the Redis Stream `oversee:feed` with `XREADGROUP`, never a bus topic. A feed entry is appended atomically with the committed state it refers to (`apply_state.lua`, `tier-persistence.md` §1).
   - Feed entity types: `ORDER`, `POSITION`, `ACCOUNT` (entity changes and cash-flow commits). Every entry carries its `account_id`. Prices and live account fields produce no feed entries: the RiskSweep reads them every second.
   - For each entry it calls `RiskSweep.sweep_now(account_id)` (a non-blocking signal), then `XACK`s. It evaluates nothing itself, never creates or resolves a rule state, and never changes the halt level.
   - It never waits on a condition. It consumes nothing while `redis:boot_id` ≠ `run_id:epoch`; entries wait in the stream until SM8 is READY. This costs no oversight, because the RiskSweep keeps running from PostgreSQL meanwhile.
   - It never publishes to the bus and never calls a `BrokerAdapter`. Oversee's only way to ask for broker data is an in-process request to the ingestion orchestrator, which owns the adapters (§6, §4 `refresh_cash_flows`).
2. **RiskSweep (level-triggered; the automatic writer of rule states and the halt level):**
   - One supervised task on the leader. Every `sweep_interval_ms` (1000), and at once for an account signalled by `sweep_now()`, it evaluates every enabled rule for every account and every live entity of that account. Each pass has a monotonic `sweep_id` (an HLC value). A pass over all accounts that takes longer than the interval pages.
   - **Inputs** (read in one **READ ONLY REPEATABLE READ** transaction per account, which writes nothing):
     - PostgreSQL: `orders_state`, `positions_state`, `account_state`, the trading day's `cash_flows`, its `sod_marks` row, `instruments`, `stop_watch`, `stream_health`, `breach_acks`, `last_clean_marks` and the account's `halt_breaches` rows;
     - Redis, read with their timestamps: `price:{adapter_id}:{symbol}` (latest quote), `clean_mid:{adapter_id}:{symbol}` (latest clean mid, kept by the price light script) and `account_live:{account_id}`.
     - **Redis is optional for a pass:** on a Redis error or a missing key the sweep falls back to `last_clean_marks` (stale by timestamp, so capped) and treats live fields as UNKNOWN, instead of failing the pass.

     Entity state never comes from Redis, so the sweep does not depend on SM8 and never pauses for it.
   - **Writes** go in a separate **READ COMMITTED** transaction that follows the epoch pattern (`operations.md` §2), so a paused ex-leader's sweep can't commit: `halt_breaches` (compare-and-set on `last_sweep_id` and `clear_generation`), `stop_watch`, `oversee_events`, `notification_outbox`, `sod_marks`. Inside `HALT_LOCK` it re-reads `MAX(halt_events.id)`, the HARD latch and the latest operator clear before any `trip()` / system `clear()`. The operator `clear()` writes `halt_breaches` only under `HALT_LOCK` and bumps `clear_generation` on every row it touches, so an in-flight sweep write built from an older snapshot fails its compare-and-set and is recomputed; acknowledgements live in `breach_acks`.
   - **Passes are per account.** An account whose snapshot, evaluation or write fails keeps its last committed states. After 10 s without a successful pass it gets a capped SOFT `evaluation_failed` state (written by the next level decision) and a page. After 5 min of consecutive failures it is quarantined (`sweep_quarantine`) with a critical "oversight lapsed for account X" page; a quarantined account is retried every 10 min (half-open) and released by its first good pass or by an audited operator `release_account()`. The halt level is decided after **every** pass, from all accounts' committed states.
     - **Dependency-wide failures are not account faults:** when PostgreSQL is unreachable for the whole pass, no account is marked `evaluation_failed` or counted toward quarantine; one cluster-level alert fires instead, and the gate denies anyway because no pass is fresh.
   - **Day roll:** the first pass at or after the 17:00 America/New_York roll writes each account's new `sod_marks` row (idempotent on (account_id, trading_day)), from the pre-roll `last_clean_marks` (only marks with `ts` before the roll) that the roll transition has just forced, applying the `partial_session` rules (`tier-persistence.md` §4). A row written more than 30 s after the roll is `partial_session` (its balance and positions may already include post-roll trades), with a page. LEADER_INIT step 4 only catches up a missed row. A missing row 60 s after the roll pages, and `MaxDailyLossRule` is UNKNOWN for that account until it exists.
   - **Balance / position skew:** the balance and a position close arrive on different streams and commit separately. While any of the account's positions or orders changed status or units more recently than its latest balance version, for up to the adapter's `balance_skew_ms` (measured per adapter; default 5 s), `MaxDailyLossRule` is capped at SOFT, so a close that is committed before its balance credit (or the reverse) can't confirm a HARD.
   - **Integrity incident:** while SM3 is HALTED, SWITCHING_OVER, MANUAL_INTERVENTION or RECONCILING, every result is capped at SOFT (`FLAG stale_account_image`) until the broker reconcile commits.
   - Each result per (rule, subject) is a **level** (PASS, FLAG, SOFT_HALT, HARD_HALT), a **cap** and a **reason**:
     - the cap is **SOFT** when an input is stale or estimated (a stale mark, a `partial_session` start of day, a stale account image, §5); otherwise **none**;
     - a missing or unknown input (no `InstrumentSpec`, no mid ever seen, a missing or only sampled live field, a rule that raised an error) makes the result **UNKNOWN**.
   - **Rule states** (`halt_breaches`, one row per (rule, subject); FLAG-level states live there too but never affect the halt level). The subject is the **account** for `MarginUsageRule` and `MaxAggregateOpenRiskRule`, **(account, trading_day)** for `MaxDailyLossRule`, and the **order or position** for `MaxPositionRiskRule` and `MissingStopLossRule`:
     - **Confirm:** a level is **active** only once the sweep has seen it continuously for the rule's `confirm_ms` (1000 for the aggregate and daily-loss rules, 0 otherwise): `pending_since` is the first sweep that saw it, and a later sweep that no longer sees it clears `pending_since`. A rise to a higher level is confirmed the same way.
     - **Level and cap:** an active state takes the latest result's level within its cap. A capped result can create, lower or resolve a state only up to SOFT; it never lowers or resolves a HARD state. HARD always needs an uncapped result.
     - **Release (hysteresis):** a state lowers or resolves only after its result has stayed below its level by the rule's release margin (10 % of the band edge) for `release_ms` (5 s), with known inputs and within its cap, so a value sitting on a band edge doesn't flap. It also resolves when its entity is terminal, `CLOSED` or gone from the snapshot, or when a ruleset reload removes or disables its rule.
     - **UNKNOWN** changes nothing for `unknown_grace_ms` (30 s), measured from `unknown_since` (stored in `halt_breaches`, so a restart doesn't restart it). After that it becomes a **capped SOFT** state (`risk_unknown`, `mark_unknown`, `live_unknown` or `evaluation_failed`), which resolves once a known result is below SOFT.
     - **Escalation clock:** each state accumulates `uncapped_ms`, the time it has been active at SOFT or higher with uncapped inputs. Each successful pass adds min(now − the state's previous pass, 2 × `sweep_interval_ms`), so time nobody observed (a failover, a slow pass) is never counted. Capped spells and short lowerings to FLAG **pause** the clock. It resets once the state has stayed below SOFT (FLAG or PASS) for `cool_down_ms` (`below_soft_since`), and when an operator `clear()` reviews it.
     - **Transitions** (activated, raised, lowered, resolved, capped, uncapped) each write one `oversee_events` row keyed (`rule_id`, `subject_type`, `subject_id`, `transition`, `sweep_id`) and queue an alert (§10). Replays, heals and re-feeds produce no transition, so they can't double-alert.
   - **Halt level** (decided after each complete pass and applied through `trip()` / system `clear()` under `HALT_LOCK`, §8):
     - target = HARD if any active state is HARD or the **HARD latch** is set; else SOFT if any active state is SOFT; else NONE;
     - **Escalate:** an active, uncapped SOFT state with `uncapped_ms` ≥ `escalate_after_ms` → `trip(HARD, reason=escalation)`;
     - every HARD trip sets the HARD latch, which only an operator `clear()` releases (§4);
     - capped states never escalate; they re-page every `escalate_after_ms` unless acknowledged;
     - **Cool-down:** SOFT lowers to NONE only when no SOFT-or-higher state is active and now ≥ MAX(every state's `below_soft_since`, the time each dropped below SOFT by lowering or resolving; the time the level last rose) + `cool_down_ms` → `clear(by=system, reason=cool_down)`.
     - A pass that fails for an account changes **nothing** for it: no activation, resolution, escalation or cool-down (and after 10 s it becomes `evaluation_failed`, above).
   - **Deadlines are durable timestamps**, compared by the sweep. `MissingStopLossRule` uses `effective_stop_missing_since_ms` in the sweep-owned table `stop_watch` (never in the orchestrator's rows, so the two writers never contend), written the first time the sweep sees `needs_stop()` with no `effective_stop()` and cleared when a stop is seen. Confirm, release, UNKNOWN grace, escalation and cool-down use `halt_breaches` columns; the account fence uses `stream_health.conn_lost_at`. **There are no Oversee timers**: no `oversee:timers`, no timer scripts and no TimerScanner, so no deadline can be lost, pushed forward or raced.
   - **Stale account image (fence):** the account's image is stale while any stream of its adapter (orders, positions or account) has `stream_health.conn_lost_at` set and older than `ACCOUNT_FENCE_TIMEOUT_MS` (10 000). Then the account gets `FLAG stale_account_image` and all its results are capped at SOFT. `conn_lost_at` lives in PostgreSQL, so it survives restarts:
     - LEADER_INIT step 1 sets it, in the epoch-bump transaction, for **every** (adapter, stream) that has none (`COALESCE(conn_lost_at, now())`), so a takeover always starts fenced and a SIGKILLed predecessor that recorded nothing can't leave a frozen image looking current;
     - the orchestrator sets it when a subscription leaves `SUBSCRIBED` or a gap opens, and clears it only once this term's SM4 has that stream `SUBSCRIBED` **and** its SNAPSHOTTING or RESYNCING reconcile has committed.
3. **Consumer-Group Recovery (feed):**
   - `XAUTOCLAIM` entries idle > 30 s, on start-up and every 30 s.
   - An entry delivered 5 times goes to `events_quarantine` with an alert, then is `XACK`ed. This costs only the wake-up: the next periodic pass still evaluates the account.
   - `XACK` after the wake-up signal is sent.
4. **Operator actions** (authenticated and audited; one signature each):
   - `acknowledge(operator, why, breach_id)`: writes a `breach_acks` row that silences the re-pages of that state until its level, cap or reason changes. It never changes a state or the level. This is the use cases' "acknowledge".
   - `clear(operator, why, reviewed_event_id, force=false)`: releases the HARD latch.
     - Refused if a newer `halt_events` row exists, or if the latest pass still shows an active HARD state (the refusal names it), unless `force` is set and that HARD state's latest results are capped or UNKNOWN (it can't be confirmed either way); a forced clear needs a written reason and **resolves** that HARD state (`resolved_by` = operator). Later capped or UNKNOWN results can recreate it only up to SOFT; only an uncapped result can raise it to HARD again.
     - Otherwise the level becomes the sweep's target (SOFT or NONE), and every unresolved state's `uncapped_ms` (FLAG included) restarts from 0, so a reviewed condition gets a full `escalate_after_ms` again. All of this is written under `HALT_LOCK` with `clear_generation` bumped (§2).
     - The sweep keeps evaluating everything after a clear: a clear never ends oversight of a condition that persists.
   - `refresh_cash_flows(operator, account)`: asks the orchestrator (which owns the adapters) for `fetch_cash_flows(since the roll)`, so a transfer that explains a balance move can be booked without waiting for a resync.
   - `reset_task(operator, task)`: closes a task's circuit (`operations.md` §3).
   - `release_account(operator, account)`: releases an account from `sweep_quarantine`.
5. **Shared Risk Definitions (pure functions; `Decimal` throughout):**
   - **Prices are per (adapter_id, symbol).** Every position and order is marked, and converted into its account currency, with its own adapter's prices and crosses.
   - **`risk(e)`**: directional, floored at 0.
     - BUY: `(entry − stop) × units`. SELL: `(stop − entry) × units`. Then `× fx(pnl_ccy → account currency)` at a clean mid.
     - A stop at or past breakeven counts 0.
     - UNKNOWN for a missing `InstrumentSpec` (`instrument_unknown`), a WORKING order whose stop is on the wrong side, or no mid ever seen.
   - **`effective_stop(e)`**: `e.stop_loss`; else the non-terminal linked reduce-only stop children whose remaining units cover `e`'s units; else `None`.
   - **`reducing_units(order)`**: 0 unless `is_reduce_only`. Otherwise `min(remaining_units, units of the linked entity)`, where the linked entity is the `OPEN` position (same symbol, opposite side) or the still-pending parent entry order. That part carries **0 risk and needs no stop**; any excess is opening.
   - **`opening_units(order)`** = `remaining_units − reducing_units` for a `PENDING`, `WORKING` or `PARTIALLY_FILLED` order, and **0 for a terminal order**.
   - **`needs_stop(e)`** = an `OPEN` position, or a non-terminal order with `opening_units > 0`.
   - **Clean mid:** the last mid whose spread is ≤ `marks.max_clean_spread_pips` (rule config). Last clean mids persist in PostgreSQL `last_clean_marks(adapter_id, symbol, …)`, so a new leader is never blind. Every stale case caps results at SOFT and counts as a `stale_mark` symbol for the gate (§8):
     - OPEN and ticking, but no clean quote for longer than `stale_mark_ms`: mark conservatively at the liquidation side of the latest raw quote (bid for longs, ask for shorts).
     - OPEN but **no quote at all** (clean or raw) for longer than `stale_mark_ms`: the last mid with `FLAG stale_mark`. A frozen feed is never treated as clean.
     - Closed or in its roll break (SM10): the last clean mid before the break, with `FLAG stale_mark`.
     - No mid ever seen: UNKNOWN (`mark_unknown`), never `HARD_HALT`.
   - **`equity_mark`** = balance + Σ `OPEN` positions (P&L at the mark + swap and commission not yet booked to the balance, per the adapter's `swap_booked_to_balance`), converted at clean cross mids, **minus unexplained balance increases**.
   - **Unexplained balance delta** = (balance − the `sod_marks` balance) − Σ `cash_flows` since the roll, where the cash ledger holds **every** booked balance move: transfers (`DEPOSIT` / `WITHDRAWAL`) and P&L kinds (`TRADE_PNL` from each closing deal, `FINANCING`, `COMMISSION`, `ADJUSTMENT`). A realised trade P&L is therefore explained, not excluded. The same formula, at the roll, gives `unexplained_increase_at_roll`. An adapter that can't report `TRADE_PNL` marks the day `pnl_unverified` (FLAG) and its balance moves are treated as P&L.
   - **`sod_equity`** = `equity_mark` from the last clean mids before the 17:00 America/New_York roll (`sod_marks`). **`net_transfers_today`** is defined in `tier-persistence.md` §4 (transfers whose balance effect is not already in `sod_equity`).
   - Every "% of equity" in the rules below means **% of `equity_mark`**. Because the sweep re-runs every rule each second against current prices, price moves alone create and resolve states for every rule.
6. **Rule Plugins (Strategy):**
   - `Rule.evaluate(snapshot, subject) -> Result(level, cap, reason, risk_pct?)`. Rules are pure and built by `RuleRegistry` from config, each with its own `confirm_ms`. The most severe level wins.
   - **`MaxPositionRiskRule`** (1R = 1.0 %): each **non-terminal** order (`ordered_units`, the whole trade, for an order that is not reduce-only; `opening_units` for a reduce-only order) and each `OPEN` position.
     - ≤ 1.0 % → `PASS`; up to 1.5 % → `FLAG`; > 1.5 % → `SOFT_HALT`.
   - **`MaxAggregateOpenRiskRule`** (example 2.0 % / 3.0 %, `confirm_ms` 1000): Σ `risk()` over `opening_units` of `WORKING` / `PARTIALLY_FILLED` orders, plus `OPEN` positions; no netting. > 2.0 % → `FLAG`, > 3.0 % → `SOFT_HALT`. Any UNKNOWN leg makes the result UNKNOWN.
   - **`MissingStopLossRule`**: for every entity where `needs_stop()` holds and `effective_stop()` is `None`, `SOFT_HALT` once now − `effective_stop_missing_since_ms` ≥ `missing_stop_loss.grace_ms` (2000). Oversee never calls the broker.
   - **`MaxDailyLossRule`** (example 3 %, `confirm_ms` 1000): `(equity_mark − sod_equity − net_transfers_today) / sod_equity < −0.03` → `HARD_HALT`. While the day's `sod_marks` row is `partial_session`, or any input is stale, the result is capped at SOFT.
     - Its subject is **(account, trading_day)**: the roll resolves the previous day's state (`resolved_by` = day_end), so a HARD from one day never carries a capped next day.
     - An unexplained balance **decrease** also asks the orchestrator for `fetch_cash_flows(since the roll)` on that account's adapter (at most once a minute per account), so a withdrawal is explained quickly instead of standing as a loss.
     - Broker equity is a cross-check only: `FLAG equity_divergence`.
     - An unexplained balance **decrease** counts as loss. An unexplained **increase** is excluded until explained (`FLAG unexplained_balance_delta`).
   - **`MarginUsageRule`** (example): `margin_used / equity_mark` > 80 % → `FLAG`; > 95 % → `SOFT_HALT`. A missing or only sampled live field makes the result UNKNOWN.
7. **Idempotency and audit:**
   - Rule states are recomputed, not accumulated, so replaying, re-feeding or healing anything can't double-count.
   - `oversee_events` holds one row per transition, unique on (`rule_id`, `subject_type`, `subject_id`, `transition`, `sweep_id`) and partitioned by `sweep_id` range (`tier-persistence.md` §4).
   - A **ruleset reload** (the only reload mechanism) bumps `ruleset_version`, is audited, and calls `sweep_now()` for every account; states of removed or disabled rules resolve with an audit row.
8. **Halts (HaltSwitch, ActionGate):**
   - PostgreSQL `halt_events` is the truth; Redis `halt:state` = (`effective_level`, `halt_event_id`) is written only by its CAS script.
   - **Serialised:** `trip()` and the system `clear()` (called only by the RiskSweep) and the operator `clear()` run under one `pg_advisory_xact_lock(HALT_LOCK)`. The rise check and `effective_level` are computed inside the lock. `trip(level)` writes a `halt_events` row and alerts **only when the level rises**.
   - **Fail-closed ActionGate.** This is the canonical list; other documents refer to it. `allow()` returns False unless **all** of these hold:
     - `redis:boot_id` = `run_id:epoch` (so SM8 is not running);
     - this node's epoch = `engine_epoch` and its lease is valid;
     - `halt:state` is present and its `halt_event_id` = `MAX(halt_events.id)`;
     - the level permits the action (SOFT: risk-reducing only; HARD: nothing);
     - for a **risk-increasing** action, also:
       - the takeover settle window (TTL + `TCP_USER_TIMEOUT` + broker ack timeout) has passed;
       - the account has been reconciled after the latest market open, in the current epoch. When the latest open cannot be determined (calendar not loaded, or no session known for the adapter) the gate denies with `latest_open_unknown`; a 24/7 session reports its start time, so an always-open symbol always has a latest open, and an adapter with no calendar sessions pages instead of leaving the gate silently shut;
       - no stream of its adapter has an open `stream_health` row (§2 fence);
       - **its adapter's downtime catch-up is verified** (`catchup_unverified` otherwise; single-PC mode, `operations.md` §17): this term's newest catch-up of the adapter (start-up, wake from sleep, reconnect, retry) finished **complete**, and nothing interrupted its orders / account path since that catch-up's read. An incomplete one (broker retention, the history's own incomplete flag, no history facility, an id the broker no longer knows, an unversioned row) keeps it denied until a later catch-up of the term is complete; every new term starts unverified;
       - it holds no `stale_mark` symbol (§5), and the latest pass has no UNKNOWN result for it (the 30 s grace applies to states and alerts, never to the gate);
       - **the RiskSweep is current**: its last complete pass over that account finished less than 3 × `sweep_interval_ms` ago.

     The account's adapter (for the `stream_health` and latest-open conditions, and for the NOT NULL `adapter_id` of its `halt_breaches` / `oversee_events` / `sweep_quarantine` rows) is taken from `account_state`, else from the account's positions or orders, else from its existing `halt_breaches` rows (`adapter_of`), so an account known only through its entities, or no longer listed, still resolves to its adapter.

     Any Redis or PostgreSQL error or timeout → False. SM5 starts in `UNKNOWN` and leaves it only once SM8 is READY (`redis:boot_id` = `run_id:epoch`). Leaving `UNKNOWN` only restores the stored level (no row, no alert).
9. **Phase 1 Boundary:** No outbound sender exists. `ActionGate.allow(action)` is the contract any Phase 2 `ExecutionGateway` must call before every send.
10. **Notifications:**
    - Alerts go to `notification_outbox` in the same transaction as the transition or halt row, then to `EmailChannel` / `MobilePushChannel`.
    - Alerts are per **state episode**, keyed (`rule_id`, `subject_type`, `subject_id`, activation id, level): one alert when a state activates or rises, then a re-page every `escalate_after_ms` while it stays active, unless acknowledged. The dedupe state lives in PostgreSQL (`notification_dedupe`), so a new leader doesn't re-send. **Only the level-raising `trip()` alert bypasses dedupe.**
    - Channel credentials come from AWS Secrets Manager with the same periodic refresh as other secrets, and are re-read on any auth error. They are never baked into the environment.
