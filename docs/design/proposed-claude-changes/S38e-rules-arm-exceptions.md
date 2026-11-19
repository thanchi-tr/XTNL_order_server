# Proposed change (S38e): the ARM's unfenced writes and read snapshots as named exceptions (operations §2, §12; tier-persistence §4)

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it:
the S38e tests (`tests/riskmgr/test_s38e.py`, `tests/marketdata/test_s38e_md.py`) read only `src/` and `docs/`.

## Why

Review round 13 (plan stage S38e) found that the build already relies on single-writer exceptions that
`.claude/rules/operations.md` §2 does not name. §2 says "There are three named exceptions" (the RiskSweep's read
snapshot, the spill-drain insert, the cTrader `token_generation` compare-and-set), and every other engine transaction
runs in READ COMMITTED with every state write fenced in the same statement. The ARM code departs from that in these
places (each was reviewed against the design, and none writes order state or market data):

| # | Code | What it does | Why it is safe |
|---|---|---|---|
| 1 | `src/runtime/arm_ports.py::PgTrackerSnapshot` (`EngineDb.snapshot()`) | the arm-tracker's account image: one READ ONLY REPEATABLE READ transaction per pass, no fence | writes nothing; every ARM write goes through a separate fenced READ COMMITTED transaction on the ARM pool (as the RiskSweep's snapshot, `oversee-rules.md` §2) |
| 2 | `src/db/arm_versions.py::PgArmVersionReader` (`EngineDb.snapshot()`) | the `S_at_T` reads and the entry-time read: all statements of one call in one READ ONLY REPEATABLE READ transaction | writes nothing; the snapshot only makes the counting read and the marker reads see one committed state of `event_log` |
| 3 | `src/db/arm_status_store.py::PgArmStatusStore` (the `arm-status` loop, every 5 s) | the per-node `arm_status` heartbeat upsert and this node's market-data record (`md_sentinel`, `md_last_write_ms`, forward-only), unfenced | node telemetry, like `engine_runs` / `adapter_coverage` in §17; keyed by node, so a fenced ex-leader only updates its own row; nothing reads it as state except `md ...` / `arm ...` request freshness and the bootstrap's classification (info only) |
| 4 | `src/ops/service.py` (`arm config accept`, `arm pause` / `resume`, `arm notify-test`) | operator writes of `arm_accepted` and `arm_controls` under a table lock with the `operator_audit` row in the same transaction | operator-owned configuration the engine only reads, exactly as `rulesets` in §12 |

`ArmDecisionReader.decision_memory` (`src/db/arm_store.py::PgArmStateStore.decision_memory`) is two plain reads in
READ COMMITTED and needs no exception. The ARM's own state writes (`arm_positions`, `arm_decisions`, the ARM outbox
rows, `arm_failures`) are fenced in the same statement, as §2 requires.

Until this is approved the code and the rule text disagree in exactly these four places; `src/db/arm_versions.py`'s
docstring now points here instead of calling itself "named exception 1" (the RiskSweep's). Nothing here narrows a
read-only rule, `FORBIDDEN_METHOD_NAMES`, the `ReadOnlyTransport` deny-list or the hook's `FORBIDDEN_PATTERNS`.

## 1. `.claude/rules/operations.md`

```diff
--- a/.claude/rules/operations.md
+++ b/.claude/rules/operations.md
@@ §2 Single writer
-   - ... There are three named exceptions: the RiskSweep's read snapshot, a READ ONLY REPEATABLE READ transaction that writes nothing (every sweep write goes through a separate fenced READ COMMITTED transaction, `oversee-rules.md` §2); the idempotent spill-drain `INSERT … ON CONFLICT DO NOTHING` into `events_dlq`, which any node may run (`tier-persistence.md` §5); and the cTrader `token_generation` compare-and-set (§13).
+   - ... There are three named exceptions: the RiskSweep's read snapshot, a READ ONLY REPEATABLE READ transaction that writes nothing (every sweep write goes through a separate fenced READ COMMITTED transaction, `oversee-rules.md` §2); the idempotent spill-drain `INSERT … ON CONFLICT DO NOTHING` into `events_dlq`, which any node may run (`tier-persistence.md` §5); and the cTrader `token_generation` compare-and-set (§13).
+   - **ARM exceptions** (only with `XTNL_ARM_ENABLED=1` or `XTNL_MD_ENABLED=1`; design §B3, §B5.1): (a) the ARM's read snapshots, READ ONLY REPEATABLE READ transactions that write nothing, like the RiskSweep's: the arm-tracker's account image (`PgTrackerSnapshot`) and the `S_at_T` / entry-time reads (`PgArmVersionReader`); every ARM write goes through a separate fenced READ COMMITTED transaction on the ARM pool; (b) the per-node `arm_status` heartbeat upsert and the node's market-data record in it, unfenced node telemetry like `engine_runs` (§17), keyed by node and forward-only on `md_last_write_ms`.
@@ §12 Change management
    - `rulesets` rows are written only through the audited operator CLI: ... so these writes run under a table lock instead of the epoch fence.
+   - `arm_accepted` and `arm_controls` (the ARM's accepted configuration hash and its pause / drain controls) are operator-owned configuration the engine only reads, written only by the audited operator CLI (`arm config accept`, `arm pause` / `resume`, `arm notify-test`) under a table lock with the `operator_audit` row in the same transaction, like `rulesets`.
```

## 2. `.claude/rules/tier-persistence.md`

```diff
--- a/.claude/rules/tier-persistence.md
+++ b/.claude/rules/tier-persistence.md
@@ §4 Tables
+   - `arm_status` (one row per node): unfenced node telemetry (operations §2 ARM exceptions), never engine state; `arm_accepted` / `arm_controls`: operator-owned, written only by the audited operator CLI (operations §12).
```

## 3. Amendment to `S37-rules-redis-md-and-arm.md` (applied in that file, plan S38e)

The S37 tier-persistence §1 hunk said the existing `last_clean_marks` and `sod_marks.clean_mids` stay as they are
"(Q15 (a), an explicit user override)". Q15 has no answer and no default (design §0): that wording would have made an
unanswered question binding rule text. S38e changed it to "stay as they are until the user answers Q15 (design §0);
this rule then follows the answer". Its §11 hunk also names `RETAIN_BROKER` (the BROKER retention S38e built) next to
`RETAIN`.
