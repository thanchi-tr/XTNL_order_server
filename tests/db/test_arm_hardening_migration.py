"""0020_arm_hardening.sql (plan S38f, round 14 fixes 2 and 3; design §B3, §B14, §C1).

* `xtnl_arm` cannot clear or rewrite a FAILED episode: UPDATE on `arm_failures` only on the non-identity columns, and
  the BEFORE UPDATE trigger `arm_failures_guard` refusing identity changes, a shrinking `position_ids`, any change of a
  cleared episode and a clear by any role but the owner's (42501).
* `arm_budget_counters`: the durable error-budget counters (scope ids, codes, counts, times; never a price).
* Held back like 0015-0019 until `XTNL_MIGRATIONS_ACCEPTED_UPTO` reaches it.
The `pg` marker test runs the refusals on a real server.
"""
from __future__ import annotations

import os
import re
from collections.abc import AsyncIterator
from typing import Any

import pytest

from src.db import arm_budget_store as B
from src.db import arm_failures_store as F
from src.db import arm_schema as A
from src.db.arm_ports import BudgetCounterRow
from src.db.fence import FENCE, EpochMismatch
from src.db.migrate import DEFAULT_ACCEPTED_UPTO, discover

from tests.db.conftest import FakeConn

MIG = next(m for m in discover() if m.version == 20)
CODE = "\n".join(line.split("--", 1)[0] for line in MIG.sql.splitlines())
MARKET_WORDS = re.compile(r"\b(tick|bid|ask|spread|ohlc|open_px|close_px|high|low|quote|indicator|atr|favour|price)\w*",
                          re.I)


def test_0020_is_held_back_by_default() -> None:
    assert MIG.name == "arm_hardening" and MIG.sql.startswith("-- 0020_arm_hardening.sql")
    assert MIG.version > DEFAULT_ACCEPTED_UPTO


def test_update_is_narrowed_to_the_non_identity_columns() -> None:
    assert re.search(r"REVOKE UPDATE ON TABLE\s+arm_failures\s+FROM xtnl_arm;", CODE)
    (cols,) = re.findall(r"GRANT UPDATE \(([^)]*)\) ON TABLE arm_failures TO xtnl_arm;", CODE, re.S)
    granted = {c.strip() for c in cols.replace("\n", " ").split(",")}
    assert granted == set(A.ARM_FAILURES_UPDATABLE)
    assert not granted & (A.ARM_FAILURES_IDENTITY | A.ARM_FAILURES_OWNER_ONLY | A.ARM_FAILURES_EVIDENCE)
    assert A.ARM_FAILURES_EVIDENCE == {"counters", "position_ids"}        # the recorded evidence: INSERT only
    assert A.ARM_FAILURES_IDENTITY >= {"failure_id", "scope_kind", "scope_id", "reason", "probe_kind",
                                       "first_fault_at_ms"}
    updatable = {c for (t, c), p in A.ARM_COLUMN_GRANTS.items() if t == "arm_failures" and "UPDATE" in p}
    assert updatable == set(A.ARM_FAILURES_UPDATABLE)
    insertable = {c for (t, c), p in A.ARM_COLUMN_GRANTS.items() if t == "arm_failures" and "INSERT" in p}
    assert insertable == set(A.ARM_FAILURES_COLUMNS) - A.ARM_FAILURES_OWNER_ONLY
    assert not re.search(r"GRANT\s+(ALL|DELETE|TRUNCATE)", CODE)


def test_the_guard_trigger_covers_every_identity_column_and_the_clear() -> None:
    fn = CODE[CODE.index("CREATE OR REPLACE FUNCTION arm_failures_guard()"):CODE.index("REVOKE ALL ON FUNCTION")]
    for col in A.ARM_FAILURES_IDENTITY:
        assert f"NEW.{col} IS DISTINCT FROM OLD.{col}" in fn, col
    for col in A.ARM_FAILURES_OWNER_ONLY:
        assert f"NEW.{col} IS DISTINCT FROM OLD.{col}" in fn, col
    assert "IF OLD.cleared_at_ms IS NOT NULL THEN" in fn                # a cleared episode is final
    assert "NOT (NEW.position_ids @> OLD.position_ids)" in fn           # grow only
    assert "current_user = 'xtnl_arm' OR NOT pg_has_role(current_user, owner_oid, 'MEMBER')" in fn
    assert fn.count("USING ERRCODE = '42501'") == 4 and "SECURITY DEFINER" not in fn
    assert ("CREATE TRIGGER arm_failures_guard BEFORE UPDATE ON arm_failures\n"
            "    FOR EACH ROW EXECUTE FUNCTION arm_failures_guard();") in CODE
    assert "DROP TRIGGER IF EXISTS arm_failures_guard ON arm_failures;" in CODE


def test_every_arm_pool_update_of_arm_failures_fits_the_narrowed_grant() -> None:
    """The re-ping and probe notes (ARM pool) set only updatable columns; only the clear (engine pool) sets the
    owner-only ones."""
    for name in ("SQL_FAILURE_REPING", "SQL_FAILURE_PROBE"):
        sql = getattr(F, name)
        sets = re.search(r"SET (.*?) WHERE", sql)
        assert sets is not None
        cols = {part.split("=")[0].strip() for part in sets.group(1).split(",")}
        assert cols <= A.ARM_FAILURES_UPDATABLE, name
    clear = re.search(r"SET (.*?) WHERE", F.SQL_FAILURE_CLEAR)
    assert clear is not None
    assert {p.split("=")[0].strip().removeprefix("f.") for p in clear.group(1).split(",")} == A.ARM_FAILURES_OWNER_ONLY


def test_budget_table_holds_codes_counts_and_times_only() -> None:
    body = CODE[CODE.index("CREATE TABLE IF NOT EXISTS arm_budget_counters"):CODE.index("COMMENT ON TABLE "
                                                                                          "arm_budget_counters")]
    cols = re.findall(r"^\s{4}([a-z_]+)\s+(text|bigint)", body, re.M)
    assert [c for c, _ in cols] == ["scope_kind", "scope_id", "counter", "count", "streak_start_ms", "last_event_ms",
                                    "epoch", "updated_at_ms"]
    assert not MARKET_WORDS.search(body)
    assert "PRIMARY KEY (scope_kind, scope_id, counter)" in body and "CHECK (count >= 0)" in body
    assert A.ARM_GRANTS["arm_budget_counters"] == {"SELECT", "INSERT", "UPDATE"}
    assert re.search(r"GRANT SELECT, INSERT, UPDATE ON TABLE\s+arm_budget_counters\s+TO xtnl_arm;", CODE)


def test_privilege_problems_reports_a_missing_or_disabled_guard() -> None:
    tables = [(t, p) for t, privs in A.ARM_GRANTS.items() for p in sorted(privs)]
    columns = [(t, c, p) for (t, c), privs in A.ARM_COLUMN_GRANTS.items() for p in sorted(privs)]
    seqs = [(s, "USAGE") for s in sorted(A.ARM_SEQUENCES)]
    assert A.privilege_problems(tables, columns, seqs, [("arm_failures_guard", "O")]) == []
    assert A.privilege_problems(tables, columns, seqs) == []                 # the guard check is opt-in
    assert A.privilege_problems(tables, columns, seqs, []) == ["trigger arm_failures.arm_failures_guard: missing"]
    assert A.privilege_problems(tables, columns, seqs, [("arm_failures_guard", "D")]) == [
        "trigger arm_failures.arm_failures_guard: disabled"]
    stale = columns + [("arm_failures", "scope_id", "UPDATE")]               # an UPDATE left from 0017
    assert A.privilege_problems(tables, stale, seqs, [("arm_failures_guard", "O")]) == [
        "column arm_failures.scope_id: unexpected UPDATE"]
    assert "pg_trigger" in A.SQL_ARM_FAILURES_GUARD


# ---------------------------------------------------------------------------------------------------- the stores
async def test_budget_store_writes_fenced_in_one_transaction(conn: FakeConn, db: Any) -> None:
    conn.status_for(B.SQL_BUDGET_UPSERT, "INSERT 0 1", "INSERT 0 1")
    store = B.PgArmBudgetStore(db)
    rows = [BudgetCounterRow(scope_kind="SERIES", scope_id="syn:SYN1:15m:bid", counter="unresolved", count=1,
                             streak_start_ms=10, last_event_ms=900_000),
            BudgetCounterRow(scope_kind="ARM", scope_id="arm", counter="md_down_ms", count=0)]
    await store.save(rows, 1_000)
    assert FENCE in B.SQL_BUDGET_UPSERT and "ON CONFLICT (scope_kind, scope_id, counter) DO UPDATE" in B.SQL_BUDGET_UPSERT
    args = conn.args_of(B.SQL_BUDGET_UPSERT)
    assert [a[1:4] for a in args] == [("SERIES", "syn:SYN1:15m:bid", "unresolved"), ("ARM", "arm", "md_down_ms")]
    assert args[0][0] == 7 and args[0][-1] == 1_000                       # $1 the epoch; updated_at_ms
    await store.save([], 1_000)                                           # nothing changed: no transaction
    conn.status_for(B.SQL_BUDGET_UPSERT, "INSERT 0 0")
    with pytest.raises(EpochMismatch):
        await store.save(rows[:1], 2_000)


async def test_budget_store_load_and_restart_history(conn: FakeConn, db: Any) -> None:
    conn.returns_all(B.SQL_BUDGET_LOAD, [("SERIES", "s1", "unresolved", 1, 5, 900_000), ("ARM", "arm",
                                                                                         "unclean_restarts", 0, None,
                                                                                         77)])
    rows = await B.PgArmBudgetStore(db).load()
    assert rows[0] == BudgetCounterRow(scope_kind="SERIES", scope_id="s1", counter="unresolved", count=1,
                                       streak_start_ms=5, last_event_ms=900_000)
    assert rows[1].last_event_ms == 77 and rows[1].streak_start_ms is None
    conn.returns_all(B.SQL_UNCLEAN_ENDS, [(100,), (None,), (200,)])
    assert await B.PgRestartHistory(db.source).unclean_ends_since(50) == [100, 200]
    assert conn.args_of(B.SQL_UNCLEAN_ENDS) == [(50,)]
    assert "end_reason IN ('lost', 'init_failed')" in B.SQL_UNCLEAN_ENDS
    # a term left open by a kill long ago and closed now by its successor is no unclean restart in the window
    assert "started_at >= $1::bigint" in B.SQL_UNCLEAN_ENDS


# ---------------------------------------------------------------------------------------------------- pg marker
DSN = os.environ.get("ORDER_SERVER_TEST_DSN")


@pytest.fixture
async def pg_source() -> AsyncIterator[Any]:
    from src.db.epoch import Epoch, bump_epoch
    from src.db.migrate import migrate
    from src.db.pg import create_asyncpg_source
    assert DSN is not None
    src = await create_asyncpg_source(DSN, application_name="xtnl:5:s38f")
    async with src.acquire() as conn:
        await migrate(conn)
        await conn.execute("TRUNCATE arm_failures, arm_budget_counters")
        try:
            await bump_epoch(conn, Epoch("it-cluster", 5), "it-node", terminate_older=False)
        except Exception:
            pass                                       # an equal or newer epoch from another test is fine
    try:
        yield src
    finally:
        await src.close()


async def _refused(c: Any, sql: str) -> str | None:
    """Run `sql` in a savepoint; the SQLSTATE when PostgreSQL refuses it, else None."""
    from src.db.pg import sqlstate
    await c.execute("SAVEPOINT s38f")
    try:
        await c.execute(sql)
    except Exception as exc:
        await c.execute("ROLLBACK TO SAVEPOINT s38f")
        return sqlstate(exc)
    await c.execute("RELEASE SAVEPOINT s38f")
    return None


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
async def test_pg_xtnl_arm_can_neither_clear_nor_rekey_an_episode(pg_source: Any) -> None:
    async with pg_source.acquire() as conn:
        c: Any = conn
        tables = [(str(r[0]), str(r[1])) for r in await c.fetch(A.SQL_ROLE_TABLE_PRIVILEGES, A.ARM_ROLE)]
        columns = [(str(r[0]), str(r[1]), str(r[2])) for r in await c.fetch(A.SQL_ROLE_COLUMN_PRIVILEGES, A.ARM_ROLE)]
        seqs = [(str(r[0]), str(r[1])) for r in await c.fetch(A.SQL_ROLE_SEQUENCE_PRIVILEGES, A.ARM_ROLE)]
        guards = [(str(r[0]), str(r[1])) for r in await c.fetch(A.SQL_ARM_FAILURES_GUARD)]
        assert A.privilege_problems(tables, columns, seqs, guards) == []
        await c.execute("BEGIN")
        try:
            await c.execute(
                "INSERT INTO arm_failures (failure_id, scope_kind, scope_id, reason, probe_kind, ping_id, "
                "position_ids, first_fault_at_ms, entered_at_ms, epoch) VALUES ('f38', 'SERIES', 's:1', "
                "'invalid_bar', 'series_reread', 'p38', ARRAY['a:1'], 1, 2, 5)")
            await c.execute("SET LOCAL ROLE xtnl_arm")
            for sql in ("UPDATE arm_failures SET scope_id = 's:2' WHERE failure_id = 'f38'",
                        "UPDATE arm_failures SET scope_kind = 'ADAPTER' WHERE failure_id = 'f38'",
                        "UPDATE arm_failures SET probe_kind = 'fresh_bar' WHERE failure_id = 'f38'",
                        "UPDATE arm_failures SET reason = 'unresolved' WHERE failure_id = 'f38'",
                        "UPDATE arm_failures SET cleared_at_ms = 3, clear_request_id = 1 WHERE failure_id = 'f38'"):
                assert await _refused(c, sql) == "42501", sql
            assert await _refused(c, "UPDATE arm_failures SET position_ids = '{}' WHERE failure_id = 'f38'") == "42501"
            assert await _refused(c, "UPDATE arm_failures SET position_ids = ARRAY['a:1', 'b:2'] "
                                     "WHERE failure_id = 'f38'") == "42501"         # evidence: INSERT only
            assert await _refused(c, "UPDATE arm_failures SET counters = '{}'::jsonb "
                                     "WHERE failure_id = 'f38'") == "42501"
            assert await _refused(c, "UPDATE arm_failures SET ping_count = 2, last_ping_at_ms = 3, "
                                     "last_probe_at_ms = 4, last_probe_result = 'passed' "
                                     "WHERE failure_id = 'f38'") is None
            await c.execute(
                "INSERT INTO arm_budget_counters (scope_kind, scope_id, counter, count, epoch, updated_at_ms) "
                "VALUES ('SERIES', 's:1', 'unresolved', 1, 5, 1)")
            assert await _refused(c, "DELETE FROM arm_budget_counters") == "42501"
            await c.execute("RESET ROLE")
            # the owner (the engine pool) clears; nobody re-keys, and a cleared episode is final
            assert await _refused(c, "UPDATE arm_failures SET scope_id = 's:9' WHERE failure_id = 'f38'") == "42501"
            assert await _refused(c, "UPDATE arm_failures SET cleared_at_ms = 3, clear_request_id = 1 "
                                     "WHERE failure_id = 'f38'") is None
            assert await _refused(c, "UPDATE arm_failures SET cleared_at_ms = NULL, clear_request_id = NULL "
                                     "WHERE failure_id = 'f38'") == "42501"
        finally:
            await c.execute("ROLLBACK")
