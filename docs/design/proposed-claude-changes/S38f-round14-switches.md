# Proposed change (S38f): the accepted migration level, the S07 stop-rule switch, and the FAILED / error-budget hardening in the rules

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it:
the S38f tests (`tests/db/test_migrate_accepted.py`, `tests/oversee/test_stop_rule_flag.py`,
`tests/db/test_schema_compat.py`, `tests/db/test_arm_hardening_migration.py`, `tests/riskmgr/test_budget_durable.py`)
read only `src/`, `deploy/` and `docs/`. Nothing here narrows a read-only rule, `FORBIDDEN_METHOD_NAMES`, the
`ReadOnlyTransport` deny-list or the hook's `FORBIDDEN_PATTERNS`.

## Why

Review round 14 (plan stage S38f, user: "Yes, fix those 3") found that on the single PC the working tree is the
deployment: `deploy/local/start-xtnl.ps1` runs `python -m src.db.migrate` at every start, so migrations 0015-0019 and
the uncommitted S07 Oversee change would go live at the next restart. The build now makes both opt-in, and hardens the
ARM's FAILED table and error budget. The rule text still describes the old behaviour in three places.

## 1. `.claude/rules/operations.md`

§12 "Change management", second bullet: append

> Every runner applies migrations only up to the local setting `XTNL_MIGRATIONS_ACCEPTED_UPTO` (default 14,
> `src/db/migrate.py`); later ones are held back, named in one line, and the job exits 0, so a start that runs it (the
> single-PC `start-xtnl.ps1` step 6, §17) never puts an unreviewed migration live. On a schema without the 0015 columns
> every pool runs the core statements in their 0014 form and the orchestrator publishes rows in their pre-0015 shape
> (`src/db/schema_compat.py`), so the engine behaves as before (accepted deviation: the single PC runs the operator
> job at its own start, gated by this setting).

## 2. `.claude/rules/oversee-rules.md`

§5, the `effective_stop(e)` bullet: replace with

> - **`effective_stop(e)`**: `e.stop_loss`; else the non-terminal linked reduce-only stop children whose remaining
>   units cover `e`'s units; else `None`. Which children count is a local switch, `XTNL_OVERSEE_STOP_AT_ENTRY`
>   (`src/oversee/stop_rule.py`; plan S38f): unset or `0`, a child counts only strictly on the loss side of `e`'s
>   entry, whatever its kind (the rule before S07); `1`, the PR1 rule (a STOP child at any price except strictly beyond
>   an unfilled entry order's entry, a LIMIT child never, a child of unknown kind only on the loss side, the
>   `stop_kind_unknown` / `stop_kind_inconsistent` caveats, the latter UNKNOWN in `MissingStopLossRule`). The ARM always
>   uses the PR1 rule and may run only with the switch on (go-live gate `oversee_c4`).

## 3. `.claude/rules/riskmgr.md` (the file the S11 proposal creates)

Add to its FAILED / error-budget section:

> - `xtnl_arm` has UPDATE on `arm_failures` only for `counters`, `position_ids`, `last_ping_at_ms`, `ping_count`,
>   `last_probe_at_ms`, `last_probe_result` (0020); the BEFORE UPDATE trigger `arm_failures_guard` refuses any change of
>   an identity column, a shrinking `position_ids`, any change of a cleared episode and a clear by any role but the table
>   owner's. The S31 privilege self-check reads the trigger too.
> - The error budget's counters are durable (`arm_budget_counters`, 0020): written fenced on the ARM pool only on
>   change, restored by the FAILED manager at start; a restart is never itself a clean result. `XTNL_ARM_FAIL_RESTARTS_N`
>   (3) leader terms ended `lost` / `init_failed` within `XTNL_ARM_FAIL_RESTART_WINDOW_S` (1800 s) while positions are
>   armed is ARM-global FAILED `unclean_restarts`.

## 4. `CLAUDE.md`

No change needed.
