# Proposed change (S37): `.claude/rules/operations.md`, `tier-persistence.md`, `dto-contracts.md`, `adapter-isolation.md`, `oversee-rules.md` §10 (design §C2 Rules)

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it: the
S37 deploy, metrics and runbook tests (`tests/deploy/test_md_arm_deploy.py`, `tests/ops/test_md_acl.py`) read only
`deploy/`, `docs/runbooks/` and `src/`.

## Why

Design §C2 Rules lists the rule text the market-data layer and the ARM need. Earlier stages proposed their own parts:
S01 / S04 (dto-contracts §2, §3 fields and labels), S03 (adapter-isolation §4 `fetch_price_bars`, §5 price stream
fields; dto-contracts §3 `BrokerBar`), S04 (adapter-isolation §4 `fetch_active_stop_orders`), S05 (adapter-isolation §5
tick-only symbols, operations §10), S06 (operations §15 `BARS`), S07 (oversee-rules §2, §5, §6), S11 (the lockout, the
new `riskmgr.md` / `marketdata.md` and the `CLAUDE.md` line), S22 (operations §4 hung job threads). This file adds what
is left after S36 / S36b built the runtime and S37 the AWS deploy: where `redis-md` sits in the topology and the tiers,
the hard check that stays the engine Redis only, the dashboard freshness rule for market data, notification content and
the FAILED ping with its dead-man backstop, the ARM tables in the lifecycle and migration rules, the ARM's place outside
the bus, the `order_kind` mapping per broker, and the ARM notices next to Oversee's. Nothing here narrows a read-only
rule, `FORBIDDEN_METHOD_NAMES`, the `ReadOnlyTransport` deny-list or the hook's `FORBIDDEN_PATTERNS`.

Each hunk is a unified diff against today's file; `...` stands for the unchanged text of the same line.

## 1. `.claude/rules/operations.md`

```diff
--- a/.claude/rules/operations.md
+++ b/.claude/rules/operations.md
@@ §1 Topology
    - Each node has its own Redis, used only while that node is the engine leader.
+   - **Optional market-data store** (design §A2; only with `XTNL_MD_ENABLED=1`): each engine node also has its own `redis-md` (`deploy/redis/redis-md.conf`, `127.0.0.1:6381`, AOF `everysec`, `maxmemory 1gb` `noeviction`, `xtnl-redis-md.service` with `MemoryMax=2G`, installed but enabled only with the flag), not replicated, separate from the engine Redis. Remote readers reach it only as `md_reader` through the node's mutual-TLS proxy (`${NODE_IP}:6382`, VPN CIDR only). Its ACL file is rendered at every start from the secrets source (`python -m src.ops.md_acl`, `#<sha256>` entries only).
@@ §2 Single writer
    - Only the leader opens broker sessions, refreshes tokens or sends notifications. Every one of these externally visible actions first checks `lease_deadline` synchronously.
+   - `redis-md` writes carry the writer token `md:writer = (cluster_id, epoch revision, node, term nonce)`, set by a new leader after its lease check and its `engine_epoch` confirmation; every write script requires it, so a fenced ex-leader cannot write market data either (design §A7). The ARM's PostgreSQL writes are fenced like every engine write (the ARM pool runs as `xtnl_arm`).
@@ §3 Task supervision
      - Each task stamps its liveness against its own **maximum stamp age** (for example 5 s for the RiskSweep, 30 s for the feed observer, 60 s for the outbox relay).
+     - The optional market-data and ARM tasks (`optional-bootstrap`, `md-writer`, `md-bars`, `md-indicators`, `arm-status`, `arm`, `arm-relay`, `arm-config`, `arm-tracker`) are supervised, **non-critical** tasks started only after LEADER (`EngineSession.start_optional_services()`); their circuits page and feed the ARM's FAILED (design §B14) and never end the term, never hard-exit and never count as a failed hard check.
@@ §4 Node role (SM9), hard checks
      - **Hard checks:** PostgreSQL primary writable, etcd reachable, local Redis writable, chrony synced within 50 ms; for the leader only, the RiskSweep's circuit not open for more than 5 min (§3).
+     - "Local Redis" is the **engine** Redis only. `redis-md` is never a hard check, never a preference and never blocks LEADER_INIT; its loss marks market-data series GAPPED (design §A2).
@@ §4 DRAINING
    - **DRAINING** (...): stop intake, flush the DlqSpooler (...), write this term's `end_reason`, close sockets, release the lease. Redis is not drained, because the next leader never reads it.
+     - The market-data and ARM tasks watch the stop flag and start no new write or transaction; `aclose` closes the ARM pool and the `redis-md` client in parallel with the HTTP client and the term pool inside `drain_aclose_s` (whatever has not closed is dropped). Nothing is flushed to `redis-md`; the DRAINING budget is unchanged.
@@ §5 Phase 2 send contract
    - Deny risk-increasing actions until ... (canonical list: `oversee-rules.md` §8).
+   - The ARM's advice is not an action: a Phase 2 sender that acts on it must treat it as actionable only per design §B10 (advice, not superseded, not PRE, not UNRESOLVED_RANGE, within its maximum age, not under FAILED), keep one non-terminal intent per stop handle with the expected broker version, and still pass `ActionGate.allow`.
@@ §9 Dashboard
    - **Dashboard** (Melbourne, read-only over the VPN): entity views come from PostgreSQL; Redis live values are shown only when the Redis it reads has a `redis:boot_id` epoch equal to the epoch in the latest leader heartbeat, otherwise they are marked stale.
+     - Market data read from a node's `redis-md` is shown only while that node's `md:writer` epoch equals the epoch in the latest leader heartbeat (otherwise `STALE_NODE`), and only as a display "best bar" labelled with its source (BROKER or TICK).
@@ §9 Push dead-man's switch
      - A daily test page proves the path.
+     - With the ARM on, the payload also carries `arm_failed` (open FAILED scopes, global flag) and `arm_overdue_positions` (optional fields; the receiver accepts their absence). The ARM's FAILED ping (design §B14, PR4) is committed with its critical outbox copy in one fenced transaction and also exported as `xtnl_arm_failed_scopes` (off-node `XtnlArmFailed`), so a FAILED ARM pages even when its own channels are down. On a single PC without an off-PC receiver the ping goes out when PostgreSQL returns (accepted).
+     - **Notification content** (Oversee and ARM): ids, codes, counts, levels of orders (entry, stops) and times only; never a tick, bar, quote, spread, indicator value or a value a close can be recomputed from (favour R, favour / risk). The ARM notice body in the outbox holds ids and outcome codes only; favour R is rendered at send time from a hash-checked market-data read and omitted when unavailable.
@@ §9 Metrics
    - **Metrics:** ingest rate per stream, ... certificate expiry.
+     - With the optional services on: the design §C2 market-data and ARM list (`deploy/observability/metrics.md` "Design §C2 metric list"), counts and states only, bounded labels; `redis-md` memory alerts at 70 % (`XtnlRedisMdMemoryHigh`).
@@ §11 Data lifecycle
    - Older partitions are exported and **verified** ... Audit tables (`halt_events`, `oversee_events`, `cash_flows`, `leader_terms`) are kept per compliance and never exported or detached by the engine.
+     - `arm_decisions` and `arm_decision_handles` (partitioned by `bar_end_ms`, months created ahead by the lifecycle task, a DEFAULT partition that stays empty and pages if not) are audit tables too: kept hot, never exported, detached or dropped by the engine.
+     - `redis-md` retention is enforced by md-writer (`RETAIN` for ticks and TICK bars, `RETAIN_BROKER` for every stored BROKER series, 1 year, design §A5), never by PostgreSQL lifecycle; no market data is ever archived.
@@ §12 Change management
    - Schema migrations are expand / contract, ... (accepted deviation, used for 0008's stale exporter comments).
+     - On a partitioned table (`event_log`, `notification_outbox`, `arm_decisions`) a new column is nullable with no default; a new CHECK is added `NOT VALID` and validated by the no-transaction post step of `src/db/migrate.py`; a new index is created `ON ONLY` the parent and built per partition `CONCURRENTLY` by the post step, then attached (migrations 0015-0017, design §C1).
@@ §13 Credentials & secrets
    - Certificate expiry alert at 30 days.
+   - The market-data and ARM secrets (design §C2): `XTNL_MD_REDIS_URL`, `XTNL_MD_REDIS_READER_URL`, `XTNL_MD_HMAC_KEY` (identical on both nodes; `XTNL_MD_HMAC_KEY_PREV` kept during a rotation until `md show` reports the re-hash complete plus one correction window), `XTNL_ARM_CONFIG_DSN` and the `XTNL_ARM_*` channel credentials. AWS: the secrets source; PC: the Windows User environment only, refused in the env file, dropped from the API child.
@@ §16 Security
    - Operator actions (...) are authenticated and audited.
+   - So are the ARM and market-data actions (`arm config accept`, `arm pause` / `resume`, `arm failed clear`, `arm disarm` / `rearm` / `repin`, `arm notify-test`, `md backfill` / `rebuild` / `writer-reset` / `rehash`): `operator_requests` rows answered by the leader as applied / refused / moot; a FAILED clear runs its cause probe first.
```

## 2. `.claude/rules/tier-persistence.md`

```diff
--- a/.claude/rules/tier-persistence.md
+++ b/.claude/rules/tier-persistence.md
@@ §1 Tier 1 Redis Is an Intentional Duplicate
    - Redis is never the source of truth; AOF is optional.
+   - **Market-data tier (`redis-md`, design §A2-§A13):** a separate store that is **not** a duplicate of PostgreSQL: market data derived from the broker (ticks, bars, indicator values), rebuildable from the broker for BROKER series, best-effort for TICK series. PostgreSQL never holds a tick, bar, quote, spread or indicator value (only ids, bucket times, check names, counts and keyed HMAC hashes with their key id); the existing `last_clean_marks` and `sod_marks.clean_mids` stay as they are until the user answers Q15 (design §0); this rule then follows the answer (S38e amendment: Q15 has no default, so nothing here records it as answered). Writes go only through md-writer's scripts under the writer token and the `md:boot` sentinel; AOF `everysec` is on.
@@ §2 Redis Data Loss — SM8
    - **Detection:** `NO_BOOT` from `apply_state.lua`, ...
+   - SM8 covers the engine Redis only. `redis-md` has its own bootstrap (SM12, md-writer, design §A7): `NO_BOOT` or a changed `run_id` rebuilds the registry, leases and rollback sets in place and md-bars backfills; it never closes the gate and never waits for SM8.
@@ §3 Integrity Verification
    - **Step 1 (Redis vs. DB):**
+   - The integrity worker never reads or compares `redis-md` (it is not a copy of PostgreSQL). Market-data integrity is checked by the md layer itself: the zero-trust bar checks on write and read, keyed content hashes and the quarantine (design §A4, §A13).
@@ §4 Tables
    - `notification_outbox`: written in the same transaction as the transition / halt row.
+   - ARM tables (migration 0017, design §C1): `arm_positions`, `arm_decisions` / `arm_decision_handles` (append-only, partitioned by `bar_end_ms`, audit), `arm_correction_audit`, `arm_notices`, `arm_status`, `arm_failures` (FAILED episodes; `cleared_at_ms` set only by the leader's `arm_clear_failed` handler after its probe, under an advisory lock), `arm_controls`, `arm_accepted`, `arm_config_audit`, `arm_config_cache`. Written by the ARM pool as `xtnl_arm` (fenced); ids, codes, counts, order-state levels and keyed hashes only, never a price. ARM notices are `notification_outbox` rows of kind `arm` (0016 columns), relayed by `arm-relay`, never by the Oversee relay.
```

## 3. `.claude/rules/dto-contracts.md`

```diff
--- a/.claude/rules/dto-contracts.md
+++ b/.claude/rules/dto-contracts.md
@@ §6 Single Publisher
 6. **Single Publisher:** Only the ingestion orchestrator publishes to the four topics. Adapters, the DLQ recovery worker, and Oversee never publish directly.
+   - The market-data layer adds no topic: ticks reach it only through the orchestrator's price hook (a non-blocking ring append that can never stop or slow the price path, and whose exceptions are contained), and it never publishes to a bus topic.
@@ §7 Oversee Is Not a Bus Subscriber
 7. **Oversee Is Not a Bus Subscriber:** ...
+   - Nor is the ARM: it reads committed state from PostgreSQL (the arm-tracker's READ ONLY snapshot, `event_log` versions for `S_at_T`) and market data only through the `MarketDataPort` over `redis-md`; it never subscribes to a topic, never reads `oversee:feed` and never writes the engine Redis.
```

## 4. `.claude/rules/adapter-isolation.md`

```diff
--- a/.claude/rules/adapter-isolation.md
+++ b/.claude/rules/adapter-isolation.md
@@ §2 Broker Mapping Rules, StoneX
      - If-done stop / limit children stay **separate** reduce-only `OrderDTO`s with `linked_position_id` = the parent `OrderId` (which may still be a pending entry). Never fold a child stop into the parent's `stop_loss`.
+     - `order_kind` (design PR1): order type Stop (2) → `STOP`, Limit (3) → `LIMIT`, a Trade (1) has none (`None`), from the row's own type field (stream, REST and the stop / limit history's `TypeId`); trade-history rows state no type (`None`). A row without a kind inherits the entity's known kind in the orchestrator (`dto-contracts.md` §2), so a stream item and a REST read of the same child never flip it.
@@ §2 Broker Mapping Rules, Pepperstone cTrader
      - `closingOrder` or an SL/TP order type sets `is_reduce_only`.
+     - `order_kind`: LIMIT → `LIMIT`; STOP, STOP_LIMIT → `STOP`; STOP_LOSS_TAKE_PROFIT → `STOP` when its `stopPrice` is set (the stop-loss leg), else `LIMIT`; MARKET, MARKET_RANGE, unknown or missing → `None`. A position's own `stopLoss` stays the position's stop.
@@ §3 Forbidden Imports
 3. **Forbidden Imports:** Never import `src.db`, `src.cache`, `src.pubsub`, `src.integrity`, or `src.oversee` inside `src/adapters/`. ...
+   - Nor `src.marketdata`, `src.riskmgr` or `src.armconfig`: the market-data layer and the ARM call adapters only through the read-only capabilities of §4 (`fetch_price_bars`, `fetch_active_stop_orders`) via `src/runtime/md_ports.py`, and adapters never know they exist.
```

## 5. `.claude/rules/oversee-rules.md` §10

```diff
--- a/.claude/rules/oversee-rules.md
+++ b/.claude/rules/oversee-rules.md
@@ §10 Notifications
    - Channel credentials come from AWS Secrets Manager with the same periodic refresh as other secrets, and are re-read on any auth error. They are never baked into the environment.
+   - **ARM notices are separate** (design §B8, PR4): their own channels (`XTNL_ARM_*` credentials, `arm_email` / `arm_push`), their own outbox kind `arm` and relay (`arm-relay`, no episode dedupe, no escalation, never `critical` on the push channel), so an ARM channel failure never delays an Oversee alert and the Oversee relay never touches an `arm` row. The one exception is the ARM FAILED ping's critical copy, an ordinary `info` / `critical` outbox row committed with the `arm_failures` row, so the operator hears of a FAILED ARM through the Oversee path too.
+   - Oversee never reads ARM state, and the ARM never changes a rule state, a halt or the gate: its advice is information for a human.
```

## After approval

Apply the five hunks as written (the `...` lines are context only). No code or test changes are needed: the code the
rules describe is already in the tree (S01-S36b) and this stage's deploy assets.
