"""0017_arm.sql (plan S23; design §B3, §B8, §B11-§B14, §C1 0017): idempotent DDL, the ARM tables and their keys, the
partition loop over months -1..2 (ST-1), the CHECK value sets (equal to src/db/arm_schema.py and to the ARM's own
enums), the operator request kind CHECK (equal to REQUEST_KINDS), the event_log index ON ONLY plus its post-step
registration, and the xtnl_arm role with exactly the §B3 grant list (parsed from the GRANT statements here; the
`pg`-marked test in tests/db/test_pg_integration.py reads the same list back from the catalog)."""
from __future__ import annotations

import re

import pytest

from src.db import arm_schema as A
from src.db import lifecycle as LC
from src.db.fence import FENCE
from src.db.migrate import POST_STEP, PartitionIndex, discover
from src.db.operator_requests import ARM_REQUEST_KINDS, CORE_REQUEST_KINDS, REPAIR_REQUEST_KINDS, REQUEST_KINDS

MIG = next(m for m in discover() if m.version == 17)
SQL = MIG.sql
CODE = "\n".join(line.split("--", 1)[0] for line in SQL.splitlines())
MIG18 = next(m for m in discover() if m.version == 18)
CODE18 = "\n".join(line.split("--", 1)[0] for line in MIG18.sql.splitlines())
WIDENED_BY_0018 = {("arm_decisions", "outcome"), ("arm_failures", "scope_kind")}
"""Column CHECKs 0018 replaces (S38c): their value set is the one 0018 adds."""

ARM_TABLES = {"arm_positions", "arm_decisions", "arm_decision_handles", "arm_correction_audit", "arm_notices",
              "arm_status", "arm_failures", "arm_controls", "arm_accepted", "arm_config_audit", "arm_config_cache"}


def body(table: str) -> str:
    m = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\n\)", CODE, re.S)
    assert m is not None, table
    return m.group(1)


def check_values_0018(table: str, column: str) -> set[str]:
    """The value list of the `<table>_<column>_check` constraint 0018 drops and re-adds."""
    m = re.search(rf"ALTER TABLE {table} ADD CONSTRAINT {table}_{column}_check\s+CHECK \({column} IN \(([^)]*)\)\);",
                  CODE18, re.S)
    assert m is not None, (table, column)
    assert f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {table}_{column}_check;" in CODE18
    return set(re.findall(r"'([^']*)'", m.group(1)))


def check_values(table: str, column: str) -> set[str]:
    """The literal list of `column ... CHECK (column IN (...))` in the table's CREATE TABLE body (0017), or the
    list 0018 replaces it with (`WIDENED_BY_0018`)."""
    if (table, column) in WIDENED_BY_0018:
        return check_values_0018(table, column)
    m = re.search(rf"^\s+{column}\s+[^\n]*?CHECK \({column} IN \(([^)]*)\)\)", body(table), re.M | re.S)
    assert m is not None, (table, column)
    return set(re.findall(r"'([^']*)'", m.group(1)))


def table_list(text: str) -> list[str]:
    return [t.strip() for t in text.replace("\n", " ").split(",") if t.strip()]


MIG20 = next(m for m in discover() if m.version == 20)
CODE20 = "\n".join(line.split("--", 1)[0] for line in MIG20.sql.splitlines())


def arm_grants() -> tuple[dict[str, set[str]], dict[tuple[str, str], set[str]], set[str]]:
    """The xtnl_arm grants after 0017, then 0018 (its REVOKE of UPDATE and its column grant), then 0020 (S38f: the
    REVOKE of UPDATE on arm_failures, which PostgreSQL applies to its column grants too, the narrowed column grant
    and arm_budget_counters)."""
    tables, columns, sequences = arm_grants_0017()
    for code in (CODE18, CODE20):
        for privs, names in re.findall(r"REVOKE ([A-Z, ]+) ON TABLE\s+(.*?)\s+FROM xtnl_arm;", code, re.S):
            for t in table_list(names):
                gone = {p.strip() for p in privs.split(",")}
                tables[t] -= gone
                for key in [k for k in columns if k[0] == t]:   # a table REVOKE takes its column privileges too
                    columns[key] -= gone
                    if not columns[key]:
                        del columns[key]
        for priv, cols, table in re.findall(r"GRANT (\w+) \(([^)]*)\) ON TABLE (\w+) TO xtnl_arm;", code, re.S):
            for c in table_list(cols):
                columns.setdefault((table, c), set()).add(priv)
    assert not re.search(r"GRANT [A-Z, ]+ ON TABLE", CODE18) and "GRANT ALL" not in CODE18
    for privs, names in re.findall(r"GRANT ([A-Z, ]+) ON TABLE\s+(.*?)\s+TO xtnl_arm;", CODE20, re.S):
        for t in table_list(names):
            assert t == "arm_budget_counters", t                # 0020's only table grant
            tables.setdefault(t, set()).update(p.strip() for p in privs.split(","))
    assert "GRANT ALL" not in CODE20
    return tables, columns, sequences


def arm_grants_0017() -> tuple[dict[str, set[str]], dict[tuple[str, str], set[str]], set[str]]:
    tables: dict[str, set[str]] = {}
    columns: dict[tuple[str, str], set[str]] = {}
    sequences: set[str] = set()
    for privs, names in re.findall(r"GRANT ([A-Z, ]+) ON TABLE\s+(.*?)\s+TO xtnl_arm;", CODE, re.S):
        for t in table_list(names):
            tables.setdefault(t, set()).update(p.strip() for p in privs.split(","))
    for priv, cols, table in re.findall(r"GRANT (\w+) \(([^)]*)\) ON TABLE (\w+) TO xtnl_arm;", CODE, re.S):
        for c in table_list(cols):
            columns.setdefault((table, c), set()).add(priv)
    for names in re.findall(r"GRANT USAGE ON SEQUENCE\s+(.*?)\s+TO xtnl_arm;", CODE, re.S):
        sequences.update(table_list(names))
    return tables, columns, sequences


# ============================================================================================ shape
def test_migration_is_0017_arm() -> None:
    assert MIG.name == "arm" and SQL.startswith("-- 0017_arm.sql")
    assert "SET client_min_messages = warning;" in CODE


def test_idempotent_ddl() -> None:
    assert not re.search(r"CREATE TABLE (?!IF NOT EXISTS)", CODE)
    assert not re.search(r"CREATE (UNIQUE )?INDEX (?!IF NOT EXISTS)", CODE)
    assert not re.search(r"CREATE VIEW", CODE)                         # always CREATE OR REPLACE VIEW
    assert not re.search(r"ADD COLUMN (?!IF NOT EXISTS)", CODE)
    assert "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xtnl_arm')" in CODE
    assert CODE.count("CREATE ROLE") == 1 and "PASSWORD" not in CODE.upper()
    assert "ON CONFLICT (id) DO NOTHING" in CODE                       # the arm_controls singleton row
    assert "CONCURRENTLY" not in CODE and "VALIDATE CONSTRAINT" not in CODE   # the post step's work
    assert not re.search(r"\bDROP (TABLE|COLUMN|INDEX|VIEW|ROLE)\b", CODE)
    assert not re.search(r"\b(DELETE FROM|UPDATE \w+ SET|TRUNCATE)\b", CODE)


def test_every_arm_table_is_created() -> None:
    assert set(re.findall(r"CREATE TABLE IF NOT EXISTS (arm_\w+) \(", CODE)) == ARM_TABLES
    assert set(A.ARM_STATE_TABLES) | {"arm_failures", "arm_controls", "arm_accepted"} == ARM_TABLES


def test_keys() -> None:
    assert "PRIMARY KEY (adapter_id, position_id, resource, owner)" in body("arm_positions")
    assert "resource                   text     NOT NULL DEFAULT 'STOP_LEVEL' CHECK (resource = 'STOP_LEVEL')" in \
        body("arm_positions")
    assert "CHECK (owner = 'position')" in body("arm_positions")
    dec = body("arm_decisions")
    assert "PRIMARY KEY (decision_id, bar_end_ms)" in dec
    assert re.search(r"UNIQUE \(adapter_id, position_id, resource, owner, bar_spec, event, bar_end_ms,\s+revision\)",
                     dec)
    assert "PRIMARY KEY (decision_id, bar_end_ms, handle_id)" in body("arm_decision_handles")
    assert "UNIQUE (route, bar_spec, event, bar_end_ms, part, item_key)" in body("arm_notices")
    assert "item_key                   text     NOT NULL DEFAULT ''" in body("arm_notices")
    assert "CHECK (part BETWEEN 0 AND 5)" in body("arm_notices")
    assert "CHECK (part = 0 OR event IN ('PRE', 'POST'))" in body("arm_notices")
    assert ("CREATE UNIQUE INDEX IF NOT EXISTS arm_failures_open_idx ON arm_failures (scope_kind, scope_id)\n"
            "    WHERE cleared_at_ms IS NULL;") in CODE
    assert "CHECK (id = 1)" in body("arm_controls") and "CHECK (id = 1)" in body("arm_config_cache")
    assert "node                       text     PRIMARY KEY" in body("arm_status")


def test_decisions_and_handles_are_partitioned_with_a_default_and_months_minus_one_to_two() -> None:
    """ST-1: an explicit loop over month offsets -1..2 (0009's 0..2 loop is not copied as is), shift 0 (ms)."""
    for t in A.ARM_PARTITIONED:
        assert re.search(rf"CREATE TABLE IF NOT EXISTS {t} \(.*?\n\) PARTITION BY RANGE \(bar_end_ms\);", CODE, re.S)
        assert f"CREATE TABLE IF NOT EXISTS {t}_default PARTITION OF {t} DEFAULT;" in CODE
        assert re.search(rf"PERFORM xtnl_ensure_month_partition\('{t}',\s+\(base \+ make_interval\(months => m\)\)"
                         r"::date, 0\);", CODE)
        assert LC.PARTITIONED[t] == 0 and t in LC.AUDIT_TABLES
    loop = re.search(r"FOR m IN (-?\d+)\.\.(-?\d+) LOOP", CODE)
    assert loop is not None
    assert tuple(range(int(loop.group(1)), int(loop.group(2)) + 1)) == A.ARM_PARTITION_MONTHS == (-1, 0, 1, 2)
    assert "base date := date_trunc('month', now() AT TIME ZONE 'UTC')::date;" in CODE


def test_failure_columns_match_the_schema_module() -> None:
    cols = [re.match(r"\s+(\w+)\s", line) for line in body("arm_failures").splitlines()]
    names = tuple(m.group(1) for m in cols if m and m.group(1) not in ("CONSTRAINT", "PRIMARY", "UNIQUE"))
    assert names == A.ARM_FAILURES_COLUMNS
    assert "CHECK ((cleared_at_ms IS NULL) = (clear_request_id IS NULL))" in body("arm_failures")


# ============================================================================================ CHECK value sets
@pytest.mark.parametrize(("table", "column", "values"), [
    ("arm_positions", "state", A.ARM_STATES),
    ("arm_positions", "prior_state", A.PRIOR_STATES),
    ("arm_positions", "attribution_source", A.ATTRIBUTION_SOURCES),
    ("arm_positions", "entry_time_source", A.ENTRY_TIME_SOURCES),
    ("arm_positions", "s0_source", A.S0_SOURCES),
    ("arm_positions", "pending_event", A.DECISION_EVENTS),
    ("arm_decisions", "event", A.DECISION_EVENTS),
    ("arm_decisions", "outcome", A.OUTCOMES),
    ("arm_decisions", "category", A.CATEGORIES),
    ("arm_decisions", "close_source", A.CLOSE_SOURCES),
    ("arm_decisions", "stop_basis", A.STOP_BASES),
    ("arm_decision_handles", "handle_kind", A.HANDLE_KINDS),
    ("arm_failures", "scope_kind", A.SCOPE_KINDS),
    ("arm_failures", "probe_kind", A.PROBE_KINDS),
])
def test_check_values_equal_the_schema_module(table: str, column: str, values: frozenset[str]) -> None:
    assert check_values(table, column) == set(values)


def test_check_values_equal_the_arm_enums() -> None:
    """The migration and the pure ARM core (S19-S22) name the same values."""
    from src.riskmgr.outcomes import Category, Outcome
    from src.riskmgr.sm11 import ArmState
    from src.riskmgr.types import ArmEvent, CloseSource, EntryTimeSource, Resource, S0Source, ScopeKind, StopBasis

    assert A.ARM_STATES == {s.value for s in ArmState}
    assert A.DECISION_EVENTS == {e.value for e in ArmEvent}
    assert A.OUTCOMES == {o.value for o in Outcome}
    assert A.CATEGORIES == {c.value for c in Category}
    assert A.CLOSE_SOURCES == {c.value for c in CloseSource}
    assert A.STOP_BASES == {b.value for b in StopBasis}
    assert A.S0_SOURCES == {s.value for s in S0Source}
    assert A.ENTRY_TIME_SOURCES == {s.value for s in EntryTimeSource}
    assert A.SCOPE_KINDS == {s.value for s in ScopeKind}
    assert A.RESOURCES == {r.value for r in Resource}


def test_reasons_and_probe_kinds_equal_the_migration_and_the_arm_core() -> None:
    """Every reason code the ARM core and the market-data side file passes the arm_failures.reason CHECK and maps to
    a probe_kind the CHECK accepts; together the mappings name exactly the seven probe kinds."""
    from src.marketdata.types import INDICATOR_FAULTED, INVALID_BAR, SOURCE_MISMATCH
    from src.riskmgr.frontier import STUCK_FRONTIER, FrontierFault, StuckCause

    m = re.search(r"^\s+reason\s+text\s+NOT NULL CHECK \(reason ~ '([^']*)'\)", body("arm_failures"), re.M)
    assert m is not None and m.group(1) == A.REASON_PATTERN
    reasons = (STUCK_FRONTIER, INVALID_BAR, SOURCE_MISMATCH, INDICATOR_FAULTED)
    for reason in (*reasons, *A.REASON_PROBE_KIND):
        assert re.fullmatch(A.REASON_PATTERN, reason), reason
    assert set(reasons) <= set(A.REASON_PROBE_KIND)
    assert set(A.REASON_PROBE_KIND.values()) | set(A.SCOPE_PROBE_KIND.values()) == A.PROBE_KINDS
    assert set(A.SCOPE_PROBE_KIND) <= A.SCOPE_KINDS
    assert A.probe_kind_for("ARM", STUCK_FRONTIER) == "span_write"
    assert A.probe_kind_for("ARM", "md_down") == "arm_global"
    assert A.probe_kind_for("ADAPTER", INVALID_BAR) == "adapter_series"
    assert A.probe_kind_for("SERIES", SOURCE_MISMATCH) == "series_reread"
    assert A.probe_kind_for("SERIES", "unresolved") == "fresh_bar"
    assert A.probe_kind_for("SERIES", INDICATOR_FAULTED) == "indicator_rebuild"
    assert A.probe_kind_for("STRATEGY_VERSION", "strategy_version") == "strategy_conformance"
    # the in-memory dedupe key is not a reason: the CHECK would refuse it
    key = FrontierFault(StuckCause.DATA, ("P1",), 0).key
    assert re.fullmatch(A.REASON_PATTERN, key) is None
    for scope, reason in (("SERIES", "md_down"), ("ARM", key), ("NOPE", INVALID_BAR)):
        with pytest.raises(ValueError):
            A.probe_kind_for(scope, reason)


def test_attribution_source_is_the_r11_m18_enumeration() -> None:
    assert check_values("arm_positions", "attribution_source") == {"SINGLE_EDGE_DEFAULT", "OPERATOR_TAG", "INTENT"}
    assert "it carries no edge information" in SQL                    # the opened_by_client_order_id comment


def test_probe_kinds_cover_every_clear_probe_of_b14() -> None:
    """R11-M12: five cause probes, the ADAPTER scope's per-series probe and the ARM-global trial bootstrap."""
    assert A.PROBE_KINDS == {"series_reread", "fresh_bar", "indicator_rebuild", "span_write", "strategy_conformance",
                             "adapter_series", "arm_global"}


def test_keyed_hash_columns_hold_key_id_and_hash() -> None:
    for table, col in (("arm_positions", "last_decided_hash"), ("arm_decisions", "inputs_digest")):
        assert re.search(rf"{col}\s+char\(34\) CHECK \({col} ~ '\^\[0-9a-f\]\{{34\}}\$'\)", body(table)), (table, col)


# ============================================================================================ request kinds
def test_kind_check_equals_request_kinds() -> None:
    m = re.search(r"ADD CONSTRAINT operator_requests_kind_check\s+CHECK \(kind IN \(([^)]*)\)\);", CODE, re.S)
    assert m is not None
    assert set(re.findall(r"'([^']*)'", m.group(1))) == set(REQUEST_KINDS)
    assert "ALTER TABLE operator_requests DROP CONSTRAINT IF EXISTS operator_requests_kind_check;" in CODE
    assert REQUEST_KINDS == CORE_REQUEST_KINDS | ARM_REQUEST_KINDS
    assert ARM_REQUEST_KINDS == {"arm_disarm", "arm_repin", "arm_rearm", "arm_clear_failed", "md_backfill",
                                 "md_rebuild", "md_writer_reset", "md_rehash"}
    assert not ARM_REQUEST_KINDS & CORE_REQUEST_KINDS
    assert REPAIR_REQUEST_KINDS == {"md_rebuild", "md_backfill", "md_writer_reset", "md_rehash", "reset_task"}


def test_request_kind_literal_lists_every_kind() -> None:
    from typing import get_args

    from src.db.operator_requests import RequestKind
    assert set(get_args(RequestKind)) == set(REQUEST_KINDS)


def test_every_arm_kind_is_registrable() -> None:
    from src.runtime.request_handlers import KIND_PATTERN
    assert all(KIND_PATTERN.match(k) for k in ARM_REQUEST_KINDS)


# ============================================================================================ event_log index
def test_event_log_index_on_only_and_registered_for_the_post_step() -> None:
    assert ("CREATE INDEX IF NOT EXISTS event_log_broker_time_idx\n"
            "    ON ONLY event_log (entity_type, entity_key, broker_time_ms);") in CODE
    (item,) = [it for it in POST_STEP if it.version == 17]
    assert item == PartitionIndex(17, "event_log", "event_log_broker_time_idx", "broker_time_idx",
                                  "entity_type, entity_key, broker_time_ms")


# ============================================================================================ xtnl_arm
def test_role_is_nologin_and_granted_to_the_engine_role() -> None:
    assert ("CREATE ROLE xtnl_arm NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;"
            in CODE)
    assert "FOREACH r IN ARRAY ARRAY[current_user::text, 'xtnl_engine'] LOOP" in CODE
    assert "EXECUTE format('GRANT xtnl_arm TO %I', r);" in CODE
    assert "ALTER DEFAULT PRIVILEGES" not in CODE
    assert "GRANT USAGE ON SCHEMA public TO xtnl_arm;" in CODE


def test_grant_statements_equal_the_b3_list() -> None:
    tables, columns, sequences = arm_grants()
    assert tables == {t: set(p) for t, p in A.ARM_GRANTS.items()}
    assert columns == {k: set(p) for k, p in A.ARM_COLUMN_GRANTS.items()}
    assert sequences == set(A.ARM_SEQUENCES)


def test_role_cannot_clear_a_failure_or_rewrite_the_outbox() -> None:
    tables, columns, _ = arm_grants()
    assert tables["arm_failures"] == {"SELECT"}                       # INSERT and UPDATE: column grants only
    assert ("arm_failures", "cleared_at_ms") not in columns and ("arm_failures", "clear_request_id") not in columns
    assert {p for (t, _), ps in columns.items() if t == "arm_failures" for p in ps} == {"INSERT", "UPDATE"}
    assert tables["notification_outbox"] == {"INSERT"}
    assert "operator_requests" not in tables and "operator_audit" not in tables
    for t in ("arm_accepted", "arm_controls", "engine_epoch", "event_log", "orders_state", "positions_state"):
        assert tables[t] == {"SELECT"}, t
    assert not any(p & {"DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "ALL"} for p in tables.values())
    assert not re.search(r"GRANT ALL", CODE)


def test_grants_start_from_a_clean_slate() -> None:
    """A re-run never leaves an earlier, wider grant: every table, view, partition and sequence of the schema is
    revoked from xtnl_arm before the §B3 list is granted (column grants go with their table's REVOKE ALL)."""
    revokes = re.findall(r"REVOKE ALL ON (.*?) FROM xtnl_arm;", CODE, re.S)
    assert revokes == ["ALL TABLES IN SCHEMA public", "ALL SEQUENCES IN SCHEMA public"]
    first_grant = min(m.start() for m in re.finditer(r"GRANT [A-Z, ]+(?:\([^)]*\) )?ON (?:TABLE|SEQUENCE)", CODE))
    assert CODE.index("REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM xtnl_arm;") < first_grant
    assert CODE.index("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM xtnl_arm;") < first_grant


def test_every_sequence_backs_a_granted_insert() -> None:
    for seq in A.ARM_SEQUENCES:
        table, col = re.match(r"^(\w+?)_(decision_id|audit_id|notice_id|config_audit_id|outbox_id)_seq$", seq).groups()  # type: ignore[union-attr]
        assert "INSERT" in A.ARM_GRANTS[table], seq
        if table != "notification_outbox":
            assert re.search(rf"^\s+{col}\s+bigserial", body(table), re.M), seq


# ============================================================================================ privilege compare
def _catalog_rows() -> tuple[list[tuple[str, str]], list[tuple[str, str, str]], list[tuple[str, str]]]:
    tables = [(t, p) for t, privs in A.ARM_GRANTS.items() for p in sorted(privs)]
    columns = [(t, c, p) for (t, c), privs in A.ARM_COLUMN_GRANTS.items() for p in sorted(privs)]
    sequences = [(s, "USAGE") for s in sorted(A.ARM_SEQUENCES)]
    return tables, columns, sequences


def test_privilege_problems_is_empty_for_the_exact_list() -> None:
    assert A.privilege_problems(*_catalog_rows()) == []


def test_privilege_problems_names_extra_and_missing_grants() -> None:
    tables, columns, sequences = _catalog_rows()
    tables = [r for r in tables if r != ("arm_positions", "UPDATE")] + [("notification_outbox", "UPDATE")]
    columns = columns + [("arm_failures", "cleared_at_ms", "UPDATE")]
    assert A.privilege_problems(tables, columns, sequences[1:]) == [
        "table arm_positions: missing UPDATE",
        "table notification_outbox: unexpected UPDATE",
        "column arm_failures.cleared_at_ms: unexpected UPDATE",
        f"sequence {sorted(A.ARM_SEQUENCES)[0]}: missing USAGE",
    ]


def test_privilege_problems_reports_a_grant_on_a_partition() -> None:
    """A partition can be queried and written directly: any xtnl_arm grant on one is unexpected (the catalog reads
    include partitions)."""
    tables, columns, sequences = _catalog_rows()
    tables = tables + [("arm_decisions_p202611", "DELETE"), ("arm_decisions_p202611", "UPDATE")]
    columns = columns + [("arm_decision_handles_default", "target_level", "UPDATE")]
    assert A.privilege_problems(tables, columns, sequences) == [
        "table arm_decisions_p202611: unexpected DELETE, UPDATE",
        "column arm_decision_handles_default.target_level: unexpected UPDATE",
    ]
    for sql in (A.SQL_ROLE_TABLE_PRIVILEGES, A.SQL_ROLE_COLUMN_PRIVILEGES):
        assert "relispartition" not in sql


def test_arm_positions_consistency_checks() -> None:
    pos = body("arm_positions")
    assert "config_doc_hash            char(64) NOT NULL CHECK (config_doc_hash ~ '^[0-9a-f]{64}$')" in pos
    # prior_state is set exactly while FAILED: a stale one cannot survive the restore (§B12, sm11.Transition)
    assert "CONSTRAINT arm_positions_failed_prior_check CHECK ((state = 'FAILED') = (prior_state IS NOT NULL))" in pos


def test_schema_module_holds_no_write_statement() -> None:
    """Every write SQL_* in the modules S23 adds or changes is fenced (pattern of tests/db/test_repository.py);
    arm_schema holds catalog reads only, and the lifecycle's new DEFAULT count is a read."""
    for mod in (A, LC):
        for name in dir(mod):
            sql = getattr(mod, name)
            if name.startswith("SQL_") and isinstance(sql, str) and sql.split()[0] in ("INSERT", "UPDATE", "DELETE"):
                assert FENCE in sql, name
    assert all(getattr(A, n).startswith("SELECT ") for n in dir(A) if n.startswith("SQL_"))
    assert LC.SQL_DEFAULT_ROWS.startswith("SELECT ")
