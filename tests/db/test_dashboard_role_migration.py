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
