"""0014_api_role.sql: the read-only xtnl_api role of the order-server API (least privilege for the one
network-facing process: SELECT on exactly what the Pine Script compile reads, never a write)."""
from __future__ import annotations

import re

from src.db.migrate import discover
from src.reports.lifecycle import SQL_CURRENT_ORDERS, SQL_CURRENT_POSITIONS, SQL_LOG_VERSIONS

MIG = next(m for m in discover() if m.version == 14)
SQL = MIG.sql
CODE = "\n".join(line.split("--", 1)[0] for line in SQL.splitlines())


def _tables_read_by_the_compile() -> set[str]:
    names: set[str] = set()
    for stmt in (SQL_LOG_VERSIONS, SQL_CURRENT_ORDERS, SQL_CURRENT_POSITIONS):
        names |= set(re.findall(r"\b(?:FROM|JOIN)\s+([a-z_][a-z0-9_]*)", stmt))
    return names - {"unnest"}


def _granted() -> set[str]:
    m = re.search(r"GRANT SELECT ON TABLE(.*?)TO xtnl_api", CODE, re.S)
    assert m is not None
    return {t.strip() for t in m.group(1).replace("\n", " ").split(",") if t.strip()}


def test_migration_is_0014_api_role() -> None:
    assert MIG.name == "api_role"


def test_select_grants_cover_exactly_what_the_compile_reads() -> None:
    assert _granted() == _tables_read_by_the_compile() == {"event_log", "orders_state", "positions_state"}


def test_role_can_never_write() -> None:
    assert not re.search(r"\bGRANT\s+(ALL|INSERT|UPDATE|DELETE|TRUNCATE|REFERENCES|TRIGGER)\b", CODE, re.I)
    assert "ALTER DEFAULT PRIVILEGES" not in CODE
    for attr in ("NOSUPERUSER", "NOCREATEROLE", "NOCREATEDB", "NOREPLICATION", "NOBYPASSRLS", "NOLOGIN", "NOINHERIT"):
        assert attr in CODE
    assert "ALTER ROLE xtnl_api SET default_transaction_read_only = on" in CODE
    assert "halt" not in CODE.lower()


def test_role_creation_is_idempotent_and_holds_no_credential() -> None:
    assert "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xtnl_api')" in CODE
    assert "PASSWORD" not in CODE.upper()
    assert CODE.count("CREATE ROLE") == 1
