"""The ARM's PostgreSQL schema facts (migrations 0017, 0018 and 0020; design §B3, §B8, §B11, §B14, §C1; plans S23,
S38c, S38f).

The constants here are the one statement in code of what 0017, 0018 and 0020 create, so tests (and the S31 start-up
self-check) compare the migrations and the catalog with them:

* `ARM_GRANTS` / `ARM_COLUMN_GRANTS` / `ARM_SEQUENCES`: the complete privilege list of the `xtnl_arm` role (§B3).
  The ARM pool runs as that role (`SET ROLE xtnl_arm`, S31): SELECT, INSERT and UPDATE on the mutable ARM state
  tables (`arm_positions`, `arm_notices`, `arm_status`, `arm_config_cache`; the `row_version` CAS and
  `INSERT ... ON CONFLICT DO NOTHING RETURNING` need SELECT); SELECT and INSERT only on the immutable decision rows
  and the audit tables (`arm_decisions`, `arm_decision_handles`, `arm_correction_audit`, `arm_config_audit`), plus a
  column-level UPDATE of `arm_decisions.notice_id`, the one update those rows need (0018, round 12); SELECT on
  `arm_failures`, INSERT only on its columns other than `cleared_at_ms` / `clear_request_id` (so the role cannot
  insert an episode already cleared) and, since 0020 (S38f), UPDATE only on the ping and probe notes
  (`last_ping_at_ms`, `ping_count`, `last_probe_at_ms`, `last_probe_result`; `ARM_FAILURES_UPDATABLE`: the columns
  its re-ping and probe statements set, so it cannot rewrite an episode's recorded evidence), with the BEFORE
  UPDATE trigger `arm_failures_guard` refusing any identity-column change, a shrinking `position_ids`, any change of a cleared
  episode and a clear by any role but the table owner's (so the role can neither clear nor re-key an episode);
  SELECT, INSERT and UPDATE on `arm_budget_counters` (0020, the durable error-budget counters); INSERT only on
  `notification_outbox`; SELECT only on `arm_accepted`, `arm_controls`, `engine_epoch` (the FENCE subquery),
  `event_log`, `orders_state` and `positions_state`; USAGE on the sequences behind its inserts. Everything else (the
  arm-relay, `operator_requests`, the arm-tracker snapshot, `xtnl_ensure_month_partition`, the `arm_clear_failed`
  clear statement) runs on the engine (owner) pool.
* `privilege_problems(...)`: compares rows read from the catalog (`SQL_ROLE_*`, every relation including partitions:
  a partition can be queried and written directly, so any `xtnl_arm` grant on one is reported as unexpected; the
  role reaches partitions only through their parents) with that list, and (given the `SQL_ARM_FAILURES_GUARD` rows)
  that the `arm_failures_guard` trigger exists and is enabled; empty means equal.
* The CHECK value sets of 0017 as 0018 widens them (`ARM_STATES`, `SCOPE_KINDS`, `PROBE_KINDS`, ...) and
  `REASON_PATTERN`, so the ARM's enums and the migrations cannot drift apart unnoticed; `probe_kind_for(scope_kind, reason)` (over
  `REASON_PROBE_KIND` and `SCOPE_PROBE_KIND`) is the one place an episode's reason code becomes its `probe_kind`.

Read-only module: it holds no write statement (every ARM write lives in the S24 stores, each fenced).
"""
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Final

ARM_ROLE: Final = "xtnl_arm"
DASHBOARD_ROLE: Final = "xtnl_dashboard"

#: The ARM state tables (`xtnl_arm` writes them).
ARM_STATE_TABLES: Final[tuple[str, ...]] = (
    "arm_positions", "arm_decisions", "arm_decision_handles", "arm_correction_audit", "arm_notices", "arm_status",
    "arm_config_cache", "arm_config_audit")
#: The immutable decision rows and the audit tables: SELECT and INSERT only (0018 revoked their UPDATE; §B3, §B10).
ARM_IMMUTABLE_TABLES: Final[tuple[str, ...]] = (
    "arm_decisions", "arm_decision_handles", "arm_correction_audit", "arm_config_audit")
#: Range-partitioned on `bar_end_ms` (ms, shift 0) with a DEFAULT partition; months -1..2 created by 0017.
ARM_PARTITIONED: Final[tuple[str, ...]] = ("arm_decisions", "arm_decision_handles")
#: Month offsets 0017 creates for each partitioned ARM table (previous, current, next two; ST-1).
ARM_PARTITION_MONTHS: Final[tuple[int, ...]] = (-1, 0, 1, 2)

_STATE_PRIVS: Final = frozenset({"SELECT", "INSERT", "UPDATE"})
_APPEND_PRIVS: Final = frozenset({"SELECT", "INSERT"})
#: Table-level privileges of `xtnl_arm` (the complete list; anything else is a problem).
ARM_GRANTS: Final[Mapping[str, frozenset[str]]] = MappingProxyType({
    **{t: (_APPEND_PRIVS if t in ARM_IMMUTABLE_TABLES else _STATE_PRIVS) for t in ARM_STATE_TABLES},
    "arm_failures": frozenset({"SELECT"}),
    "arm_budget_counters": frozenset({"SELECT", "INSERT", "UPDATE"}),     # 0020 (S38f)
    "notification_outbox": frozenset({"INSERT"}),
    "arm_accepted": frozenset({"SELECT"}),
    "arm_controls": frozenset({"SELECT"}),
    "engine_epoch": frozenset({"SELECT"}),
    "event_log": frozenset({"SELECT"}),
    "orders_state": frozenset({"SELECT"}),
    "positions_state": frozenset({"SELECT"}),
})
#: Every `arm_failures` column; the two the ARM role may never set come last.
ARM_FAILURES_COLUMNS: Final[tuple[str, ...]] = (
    "failure_id", "scope_kind", "scope_id", "edge_code", "reason", "probe_kind", "ping_id", "counters", "position_ids",
    "first_fault_at_ms", "entered_at_ms", "epoch", "last_ping_at_ms", "ping_count", "last_probe_at_ms",
    "last_probe_result", "cleared_at_ms", "clear_request_id")
#: Set only by the `arm_clear_failed` clear statement on the engine (owner) pool.
ARM_FAILURES_OWNER_ONLY: Final = frozenset({"cleared_at_ms", "clear_request_id"})
#: An episode's identity (0020, S38f): never updated by any role (`arm_failures_guard`), never granted for UPDATE.
ARM_FAILURES_IDENTITY: Final = frozenset({
    "failure_id", "scope_kind", "scope_id", "edge_code", "reason", "probe_kind", "ping_id", "first_fault_at_ms",
    "entered_at_ms", "epoch"})
#: The `arm_failures` columns `xtnl_arm` may UPDATE (0020): only the ping and probe notes, the columns the re-ping and
#: probe statements of `src/db/arm_failures_store.py` set; `counters` and `position_ids` (the episode's evidence) are
#: written by the INSERT only (R14 review).
ARM_FAILURES_UPDATABLE: Final = frozenset({"last_ping_at_ms", "ping_count", "last_probe_at_ms", "last_probe_result"})
ARM_FAILURES_EVIDENCE: Final = (frozenset(ARM_FAILURES_COLUMNS) - ARM_FAILURES_IDENTITY - ARM_FAILURES_OWNER_ONLY
                                - ARM_FAILURES_UPDATABLE)
"""`counters` and `position_ids`: neither identity nor updatable by `xtnl_arm` (INSERT only)."""
#: The `arm_failures` BEFORE UPDATE trigger of 0020 (S38f).
ARM_FAILURES_GUARD: Final = "arm_failures_guard"
#: Column-level privileges of `xtnl_arm`: (table, column) -> privileges. `arm_failures` INSERT is column-level too,
#: so an INSERT that names `cleared_at_ms` or `clear_request_id` is refused (42501); its UPDATE covers only
#: `ARM_FAILURES_UPDATABLE` (0020).
ARM_COLUMN_GRANTS: Final[Mapping[tuple[str, str], frozenset[str]]] = MappingProxyType({
    **{("arm_failures", c): (frozenset({"INSERT", "UPDATE"}) if c in ARM_FAILURES_UPDATABLE else frozenset({"INSERT"}))
       for c in ARM_FAILURES_COLUMNS if c not in ARM_FAILURES_OWNER_ONLY},
    ("arm_decisions", "notice_id"): frozenset({"UPDATE"}),     # 0018: the only update a decision row takes
})
#: Sequences behind the `xtnl_arm` inserts (USAGE).
ARM_SEQUENCES: Final = frozenset({
    "arm_decisions_decision_id_seq", "arm_correction_audit_audit_id_seq", "arm_notices_notice_id_seq",
    "arm_config_audit_config_audit_id_seq", "notification_outbox_outbox_id_seq"})
#: Dashboard views (outcomes, R values, quality): SELECT for `xtnl_dashboard`, no `xtnl_api` grant.
ARM_DASHBOARD_VIEWS: Final = frozenset({"arm_positions_view", "arm_decisions_view", "arm_failures_view"})

# ---- CHECK value sets of 0017 and 0018---------------------------------------------------------------------------------
RESOURCES: Final = frozenset({"STOP_LEVEL"})
OWNERS: Final = frozenset({"position"})
ARM_STATES: Final = frozenset({"DISCOVERED", "ARMED", "TRACKING", "ADVICE_OUTSTANDING", "STRATEGY_COMPLETE",
                               "SUSPENDED", "FAILED", "CLOSED", "DISARMED"})
#: States a FAILED position may restore (`prior_state`): never FAILED or a terminal state.
PRIOR_STATES: Final = ARM_STATES - {"FAILED", "CLOSED", "DISARMED"}
ATTRIBUTION_SOURCES: Final = frozenset({"SINGLE_EDGE_DEFAULT", "OPERATOR_TAG", "INTENT"})
ENTRY_TIME_SOURCES: Final = frozenset({"STREAM", "BROKER", "ESTIMATED"})
S0_SOURCES: Final = frozenset({"AT_FILL", "FIRST_SEEN", "HISTORY_LAST_LEVEL"})
DECISION_EVENTS: Final = frozenset({"PRE", "POST", "LATE", "REOPEN", "CORRECTED"})
OUTCOMES: Final = frozenset({"ADJUST", "COMPLETE", "PRE_NOT_CONFIRMED", "AT_TARGET_UNCONFIRMED",
                             "LOOSENED_AFTER_COMPLETE", "BROKER_AHEAD", "STRATEGY_REASON", "GUARD_REASON",
                             "MARKET_CLOSED", "PRICE_THROUGH_LEVEL", "NO_STOP", "DATA_UNAVAILABLE", "STRATEGY_ERROR",
                             "WRITE_ERROR", "UNRESOLVED_RANGE", "NO_BROKER_BAR"})
CATEGORIES: Final = frozenset({"ACTIONABLE", "INFO", "HOLD", "FAULT"})
CLOSE_SOURCES: Final = frozenset({"QUOTE", "BROKER"})
STOP_BASES: Final = frozenset({"STREAM", "READ", "PRIOR_VERSION", "AT_PRE"})
HANDLE_KINDS: Final = frozenset({"CHILD_ORDER", "POSITION_FIELD"})
SCOPE_KINDS: Final = frozenset({"SERIES", "ADAPTER", "STRATEGY_VERSION", "ARM", "POSITION"})
"""0017's four plus POSITION (0018: a stuck frontier on one position, round 12)."""
#: The clear probe of the recorded cause (§B14 Clearing, R11-M12): five cause probes, the ADAPTER scope's probe of
#: each of its FAILED series, and the ARM-global trial bootstrap.
PROBE_KINDS: Final = frozenset({"series_reread", "fresh_bar", "indicator_rebuild", "span_write",
                                "strategy_conformance", "adapter_series", "arm_global"})
#: `arm_failures.reason` (and the other lower-case code columns): a snake_case code, never an id with ':' in it.
REASON_PATTERN: Final = r"^[a-z][a-z0-9_]{0,63}$"
#: The clear probe of each recorded cause (§B14 Clearing): reason code -> `probe_kind`.
REASON_PROBE_KIND: Final[Mapping[str, str]] = MappingProxyType({
    "invalid_bar": "series_reread", "source_mismatch": "series_reread", "refetch_failed": "series_reread",
    "unresolved": "fresh_bar",
    "indicator_faulted": "indicator_rebuild",
    "stuck_frontier": "span_write",
    "strategy_version": "strategy_conformance",
})
#: The probe of a scope whatever its reason (ADAPTER: the probe of each of its FAILED series) or, for ARM, of any
#: reason not in `REASON_PROBE_KIND` (the trial bootstrap: `redis-md` down, bootstrap failures, the pool, a circuit).
SCOPE_PROBE_KIND: Final[Mapping[str, str]] = MappingProxyType({"ADAPTER": "adapter_series", "ARM": "arm_global"})


def probe_kind_for(scope_kind: str, reason: str) -> str:
    """The `probe_kind` an episode of `scope_kind` with `reason` is filed with (§B14 Clearing, R11-M12): ADAPTER
    always probes its series; otherwise the cause probe of `reason`, else ARM's trial bootstrap. Raises ValueError
    for an unknown scope, a reason that does not match `REASON_PATTERN`, or a SERIES / STRATEGY_VERSION reason with
    no cause probe (the INSERT would otherwise fail its CHECK when FAILED is recorded)."""
    if scope_kind not in SCOPE_KINDS:
        raise ValueError(f"unknown scope kind {scope_kind!r}")
    if re.fullmatch(REASON_PATTERN, reason) is None:
        raise ValueError(f"reason {reason!r} is not a code")
    if scope_kind == "ADAPTER":
        return SCOPE_PROBE_KIND["ADAPTER"]
    if scope_kind == "POSITION" and reason != "stuck_frontier":
        raise ValueError(f"a POSITION episode has reason stuck_frontier, not {reason!r}")
    probe = REASON_PROBE_KIND.get(reason)
    if probe is not None:
        return probe
    if scope_kind == "ARM":
        return SCOPE_PROBE_KIND["ARM"]
    raise ValueError(f"no clear probe for a {scope_kind} episode with reason {reason!r}")
#: `arm_decisions.notice_id` of a decision not shown (§B8 `suppressed` notice sentinel); real notice ids start at 1.
SUPPRESSED_NOTICE_ID: Final = 0
NOTICE_MAX_PART: Final = 5

# ---- catalog reads (the pg test and the S31 self-check) -------------------------------------------------------------
#: Table / view privileges of role $1 in schema public, partitions included (a grant on a partition is real access).
SQL_ROLE_TABLE_PRIVILEGES: Final = (
    "SELECT c.relname::text, a.privilege_type::text FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a "
    "WHERE a.grantee = to_regrole($1::text) AND c.relnamespace = 'public'::regnamespace "
    "AND c.relkind IN ('r', 'p', 'v', 'm', 'f') ORDER BY 1, 2"
)
#: Column privileges of role $1, partitions included.
SQL_ROLE_COLUMN_PRIVILEGES: Final = (
    "SELECT c.relname::text, att.attname::text, a.privilege_type::text FROM pg_attribute att "
    "JOIN pg_class c ON c.oid = att.attrelid CROSS JOIN LATERAL aclexplode(att.attacl) a "
    "WHERE a.grantee = to_regrole($1::text) AND c.relnamespace = 'public'::regnamespace "
    "AND att.attnum > 0 AND NOT att.attisdropped ORDER BY 1, 2, 3"
)
#: Sequence privileges of role $1.
SQL_ROLE_SEQUENCE_PRIVILEGES: Final = (
    "SELECT c.relname::text, a.privilege_type::text FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a "
    "WHERE a.grantee = to_regrole($1::text) AND c.relnamespace = 'public'::regnamespace AND c.relkind = 'S' "
    "ORDER BY 1, 2"
)
#: The non-internal triggers on `arm_failures`: (tgname, tgenabled); `tgenabled` 'D' is disabled (0020).
SQL_ARM_FAILURES_GUARD: Final = (
    "SELECT t.tgname::text, t.tgenabled::text FROM pg_trigger t WHERE t.tgrelid = to_regclass('arm_failures') "
    "AND NOT t.tgisinternal ORDER BY 1"
)
#: The role's attributes: (rolcanlogin, rolsuper, rolinherit, rolbypassrls, rolcreaterole, rolcreatedb).
SQL_ROLE_ATTRIBUTES: Final = (
    "SELECT rolcanlogin, rolsuper, rolinherit, rolbypassrls, rolcreaterole, rolcreatedb FROM pg_roles "
    "WHERE rolname = $1::text"
)


def _grouped(rows: Iterable[tuple[str, str]]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for name, priv in rows:
        out.setdefault(str(name), set()).add(str(priv).upper())
    return out


def _diff(label: str, have: Mapping[str, set[str]], want: Mapping[str, frozenset[str]]) -> list[str]:
    problems: list[str] = []
    for name in sorted(set(have) | set(want)):
        extra = sorted(have.get(name, set()) - want.get(name, frozenset()))
        missing = sorted(want.get(name, frozenset()) - have.get(name, set()))
        if extra:
            problems.append(f"{label} {name}: unexpected {', '.join(extra)}")
        if missing:
            problems.append(f"{label} {name}: missing {', '.join(missing)}")
    return problems


def guard_problems(triggers: Iterable[tuple[str, str]]) -> list[str]:
    """`arm_failures_guard` missing or disabled (rows of `SQL_ARM_FAILURES_GUARD`); empty when it is enabled."""
    state = {str(name): str(enabled) for name, enabled in triggers}
    if ARM_FAILURES_GUARD not in state:
        return [f"trigger arm_failures.{ARM_FAILURES_GUARD}: missing"]
    if state[ARM_FAILURES_GUARD] == "D":
        return [f"trigger arm_failures.{ARM_FAILURES_GUARD}: disabled"]
    return []


def privilege_problems(tables: Iterable[tuple[str, str]], columns: Iterable[tuple[str, str, str]],
                       sequences: Iterable[tuple[str, str]],
                       triggers: Iterable[tuple[str, str]] | None = None) -> list[str]:
    """Differences between the catalog's privileges of `xtnl_arm` (rows of the three `SQL_ROLE_*` reads) and the
    §B3 list, plus (when `triggers`, the `SQL_ARM_FAILURES_GUARD` rows, are given) a missing or disabled
    `arm_failures_guard` (0020); empty when they are equal. Names and privilege words only, never a value."""
    want_columns = {f"{t}.{c}": privs for (t, c), privs in ARM_COLUMN_GRANTS.items()}
    have_columns = _grouped((f"{t}.{c}", p) for t, c, p in columns)
    return (_diff("table", _grouped(tables), ARM_GRANTS)
            + _diff("column", have_columns, want_columns)
            + _diff("sequence", _grouped(sequences), {s: frozenset({"USAGE"}) for s in ARM_SEQUENCES})
            + ([] if triggers is None else guard_problems(triggers)))


__all__ = ["ARM_COLUMN_GRANTS", "ARM_DASHBOARD_VIEWS", "ARM_FAILURES_COLUMNS", "ARM_FAILURES_GUARD",
           "ARM_FAILURES_EVIDENCE", "ARM_FAILURES_IDENTITY", "ARM_FAILURES_OWNER_ONLY", "ARM_FAILURES_UPDATABLE", "ARM_GRANTS",
           "SQL_ARM_FAILURES_GUARD", "guard_problems",
           "ARM_IMMUTABLE_TABLES",
           "ARM_PARTITIONED", "ARM_PARTITION_MONTHS", "ARM_ROLE", "ARM_SEQUENCES", "ARM_STATES", "ARM_STATE_TABLES",
           "ATTRIBUTION_SOURCES", "CATEGORIES", "CLOSE_SOURCES", "DASHBOARD_ROLE", "DECISION_EVENTS",
           "ENTRY_TIME_SOURCES", "HANDLE_KINDS", "NOTICE_MAX_PART", "OUTCOMES", "OWNERS", "PRIOR_STATES",
           "PROBE_KINDS", "REASON_PATTERN", "REASON_PROBE_KIND", "RESOURCES", "S0_SOURCES", "SCOPE_KINDS",
           "SCOPE_PROBE_KIND", "SQL_ROLE_ATTRIBUTES", "SQL_ROLE_COLUMN_PRIVILEGES",
           "SQL_ROLE_SEQUENCE_PRIVILEGES", "SQL_ROLE_TABLE_PRIVILEGES", "STOP_BASES", "SUPPRESSED_NOTICE_ID",
           "privilege_problems", "probe_kind_for"]
