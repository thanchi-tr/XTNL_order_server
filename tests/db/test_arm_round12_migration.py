"""0018_arm_round12.sql (plan S38c; design §B3, §B10, §B14): the `xtnl_arm` role can no longer UPDATE the immutable
decision rows or the audit tables (only `arm_decisions.notice_id`), the `NO_BROKER_BAR` outcome and the POSITION
scope kind; every ARM store statement still fits the narrowed grants; no market-data column. The `pg` marker test in
tests/db/test_pg_integration.py reads the grants back from the catalog through `privilege_problems` after all
migrations, as the S31 start-up self-check does."""
from __future__ import annotations

import re

from src.db import arm_config_store, arm_failures_store, arm_status_store, arm_store
from src.db import arm_schema as A
from src.db.migrate import discover

MIG = next(m for m in discover() if m.version == 18)
CODE = "\n".join(line.split("--", 1)[0] for line in MIG.sql.splitlines())
STORE_MODULES = (arm_store, arm_failures_store, arm_status_store, arm_config_store)


def test_migration_is_0018() -> None:
    assert MIG.name == "arm_round12" and MIG.sql.startswith("-- 0018_arm_round12.sql")
    assert max(m.version for m in discover()) >= 18            # 0019 (S38d review) adds an index after it


def test_the_immutable_tables_lose_update_and_decisions_keep_only_notice_id() -> None:
    assert set(A.ARM_IMMUTABLE_TABLES) == {"arm_decisions", "arm_decision_handles", "arm_correction_audit",
                                           "arm_config_audit"}
    for t in A.ARM_IMMUTABLE_TABLES:
        assert A.ARM_GRANTS[t] == {"SELECT", "INSERT"}, t
    for t in ("arm_positions", "arm_notices", "arm_status", "arm_config_cache"):
        assert A.ARM_GRANTS[t] == {"SELECT", "INSERT", "UPDATE"}, t
    decision_columns = {c: p for (t, c), p in A.ARM_COLUMN_GRANTS.items() if t == "arm_decisions"}
    assert decision_columns == {"notice_id": {"UPDATE"}}
    m = re.search(r"REVOKE UPDATE ON TABLE\s+(.*?)\s+FROM xtnl_arm;", CODE, re.S)
    assert m is not None
    assert {t.strip() for t in m.group(1).split(",")} == set(A.ARM_IMMUTABLE_TABLES)
    assert "GRANT UPDATE (notice_id) ON TABLE arm_decisions TO xtnl_arm;" in CODE
    assert not re.search(r"GRANT\s+(ALL|DELETE|TRUNCATE)", CODE)


def test_the_privilege_self_check_reports_a_leftover_update_on_an_immutable_table() -> None:
    tables = [(t, p) for t, privs in A.ARM_GRANTS.items() for p in sorted(privs)]
    columns = [(t, c, p) for (t, c), privs in A.ARM_COLUMN_GRANTS.items() for p in sorted(privs)]
    sequences = [(s, "USAGE") for s in sorted(A.ARM_SEQUENCES)]
    assert A.privilege_problems(tables, columns, sequences) == []
    stale = tables + [("arm_decisions", "UPDATE"), ("arm_config_audit", "UPDATE")]
    assert A.privilege_problems(stale, columns, sequences) == [
        "table arm_config_audit: unexpected UPDATE", "table arm_decisions: unexpected UPDATE"]


def _statements() -> list[str]:
    out: list[str] = []
    for mod in STORE_MODULES:
        for name, value in vars(mod).items():
            if name.startswith("SQL") and isinstance(value, str):
                out.append(value)
    return out


def test_every_arm_store_statement_fits_the_narrowed_grants() -> None:
    """The ARM pool never needs UPDATE on an immutable table: the only UPDATE of `arm_decisions` sets `notice_id`
    alone, nothing updates the handles or the audit tables, and no `ON CONFLICT DO UPDATE` targets them."""
    sql = _statements()
    assert sql
    for s in sql:
        flat = " ".join(s.split())
        for t in ("arm_decision_handles", "arm_correction_audit", "arm_config_audit"):
            assert not re.search(rf"\bUPDATE {t}\b", flat), t
            assert not re.search(rf"INSERT INTO {t}\b.*DO UPDATE", flat), t
        assert not re.search(r"INSERT INTO arm_decisions\b.*DO UPDATE", flat)
        m = re.search(r"\bUPDATE arm_decisions\b\s+\w*\s*SET (.*?) (FROM|WHERE)", flat)
        if m is not None:
            assert re.fullmatch(r"notice_id = \$\d+(::\w+)?", m.group(1).strip()), m.group(1)


def test_check_value_sets_widened() -> None:
    assert "NO_BROKER_BAR" in A.OUTCOMES and "POSITION" in A.SCOPE_KINDS
    assert A.probe_kind_for("POSITION", "stuck_frontier") == "span_write"
    for bad in ("md_down", "invalid_bar"):
        try:
            A.probe_kind_for("POSITION", bad)
        except ValueError:
            continue
        raise AssertionError(bad)


def test_0018_adds_no_column_and_no_market_data() -> None:
    assert "ADD COLUMN" not in CODE.upper() and "CREATE TABLE" not in CODE.upper()
    for word in ("bid", "ask", "close", "ohlc", "spread", "indicator", "favour"):
        assert not re.search(rf"\b{word}\b", CODE, re.I), word
