"""0011_dashboard_role.sql: the read-only xtnl_dashboard role (operations.md s9; IMPLEMENTATION_PLAN 6.4)."""
from __future__ import annotations

import re

from src.dashboard.views import ALL_SQL
from src.db.migrate import discover

MIG = next(m for m in discover() if m.version == 11)
SQL = MIG.sql


def _tables_read_by_dashboard() -> set[str]:
    names: set[str] = set()
    for stmt in ALL_SQL:
        names |= set(re.findall(r"\b(?:FROM|JOIN)\s+([a-z_][a-z0-9_]*)", stmt))
    return names


def _granted() -> set[str]:
    m = re.search(r"GRANT SELECT ON TABLE(.*?)TO xtnl_dashboard", SQL, re.S)
    assert m is not None
    return {t.strip() for t in m.group(1).replace("\n", " ").split(",") if t.strip()}


def test_migration_is_0011_dashboard_role() -> None:
    assert MIG.name == "dashboard_role"


def test_select_grants_cover_exactly_what_the_dashboard_reads() -> None:
    """Without these grants every dashboard query fails; a new dashboard view needs a new grant migration."""
    assert _granted() == _tables_read_by_dashboard()


def test_role_can_never_write() -> None:
    code = "\n".join(line.split("--", 1)[0] for line in SQL.splitlines())
    assert not re.search(r"\bGRANT\s+(ALL|INSERT|UPDATE|DELETE|TRUNCATE|REFERENCES|TRIGGER)\b", code, re.I)
    assert "ALTER DEFAULT PRIVILEGES" not in code
    for attr in ("NOSUPERUSER", "NOCREATEROLE", "NOCREATEDB", "NOREPLICATION", "NOBYPASSRLS", "NOLOGIN"):
        assert attr in code
    assert "default_transaction_read_only = on" in code
    assert "statement_timeout = '5s'" in code


def test_role_creation_is_idempotent_and_holds_no_credential() -> None:
    code = "\n".join(line.split("--", 1)[0] for line in SQL.splitlines())
    assert "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xtnl_dashboard')" in code
    assert "PASSWORD" not in code.upper()
    assert code.count("CREATE ROLE") == 1


# ============================================================================================ 0017 ARM views (S23)
ARM_SQL = next(m for m in discover() if m.version == 17).sql
ARM_CODE = "\n".join(line.split("--", 1)[0] for line in ARM_SQL.splitlines())


def _arm_dashboard_grants() -> set[str]:
    grants = [names for names, role in re.findall(r"GRANT SELECT ON TABLE\s+(.*?)\s+TO (\w+);", ARM_CODE, re.S)
              if role == "xtnl_dashboard"]
    assert len(grants) == 1
    return {t.strip() for t in grants[0].replace("\n", " ").split(",") if t.strip()}


def test_0017_grants_the_dashboard_select_on_exactly_the_arm_views() -> None:
    """Design §C1 / plan S23 (R11-m): the ARM dashboard views (outcomes, R values, quality) are readable by
    xtnl_dashboard, SELECT only, and the views are the only ARM objects it is granted."""
    from src.db.arm_schema import ARM_DASHBOARD_VIEWS

    assert _arm_dashboard_grants() == set(ARM_DASHBOARD_VIEWS)
    for view in ARM_DASHBOARD_VIEWS:
        assert f"CREATE OR REPLACE VIEW {view} AS" in ARM_CODE
    assert ARM_CODE.count("TO xtnl_dashboard") == 1
    assert not re.search(r"\bGRANT\s+(ALL|INSERT|UPDATE|DELETE|TRUNCATE|REFERENCES|TRIGGER)\b[^;]*TO xtnl_dashboard",
                         ARM_CODE, re.I)


def test_0017_never_grants_the_api_role() -> None:
    assert "xtnl_api" not in ARM_CODE


def test_arm_views_show_no_levels_or_hashes() -> None:
    """The views carry outcomes, R values and quality: no entry, stop or target level, and no keyed hash."""
    for view in re.findall(r"CREATE OR REPLACE VIEW (\w+) AS(.*?)FROM", ARM_CODE, re.S):
        cols = {c.strip() for c in view[1].replace("SELECT", "").split(",")}
        for banned in ("entry_level", "s0_level", "proposed_stop", "pre_basis", "last_decided_hash", "inputs_digest",
                       "series_inputs", "indicator_inputs", "strategy_params", "strategy_state", "counters"):
            assert banned not in cols, (view[0], banned)
