"""Migration 0007 (operator_audit, operator_requests): shape, append-only guard, grants (no server needed)."""
from __future__ import annotations

import re

from src.db import MIGRATIONS_DIR
from src.db import migrate as M

SQL = (MIGRATIONS_DIR / "0007_operator_audit.sql").read_text(encoding="utf-8")
CODE = re.sub(r"--[^\n]*", "", SQL)


def _body(table: str) -> str:
    m = re.search(rf"CREATE TABLE IF NOT EXISTS {table}\s*\((.*?)\n\);", SQL, re.S)
    assert m, table
    return m.group(1)


def test_shipped_migrations_have_unique_versions_and_include_0007() -> None:
    ms = M.discover(MIGRATIONS_DIR)          # raises MigrationError on a duplicate version
    assert (7, "operator_audit") in [(m.version, m.name) for m in ms]


def test_operator_audit_columns_and_checks() -> None:
    body = _body("operator_audit")
    for col in ("audit_id", "at_ms", "command", "target", "operator", "approver", "reason", "outcome", "detail",
                "epoch", "db_user", "os_user"):
        assert re.search(rf"^\s+{col}\s", body, re.M), col
    assert "lower(approver) <> lower(operator)" in body              # two-person: approver is someone else
    assert "length(btrim(reason)) >= 8" in body
    assert "outcome IN ('applied', 'refused', 'noop')" in body
    assert "DEFAULT session_user" in body                             # the authenticated login


def test_operator_audit_is_append_only() -> None:
    assert "RAISE EXCEPTION 'operator_audit is append-only'" in CODE
    assert re.search(r"BEFORE UPDATE OR DELETE ON operator_audit\s+FOR EACH ROW", CODE)
    assert re.search(r"BEFORE TRUNCATE ON operator_audit\s+FOR EACH STATEMENT", CODE)
    assert "REVOKE UPDATE, DELETE, TRUNCATE ON operator_audit FROM %I" in CODE
    assert "'xtnl_engine'" in CODE
    assert not re.search(r"GRANT[^;]*\b(UPDATE|DELETE)\b[^;]*ON operator_audit", CODE)


def test_requests_and_quarantine_release_columns() -> None:
    body = _body("operator_requests")
    assert "REFERENCES operator_audit (audit_id)" in body and "requested_epoch" in body
    for col in ("released_at_ms", "released_by", "release_audit_id"):
        assert f"ALTER TABLE events_quarantine ADD COLUMN IF NOT EXISTS {col}" in SQL


def test_0007_is_idempotent() -> None:
    assert not re.search(r"CREATE TABLE (?!IF NOT EXISTS)", CODE)
    assert not re.search(r"CREATE (UNIQUE )?INDEX (?!IF NOT EXISTS)", CODE)
    assert not re.search(r"ADD COLUMN (?!IF NOT EXISTS)", CODE)
    assert not re.search(r"CREATE FUNCTION", CODE)
    assert CODE.count("IF NOT EXISTS (SELECT 1 FROM pg_trigger") == 2
