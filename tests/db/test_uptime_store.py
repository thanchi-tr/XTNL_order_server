"""Migration 0012 stores (src/db/uptime_store.py): engine_runs / adapter_coverage written without the hard epoch
fence (node telemetry; coverage only under the writer's current epoch, never raising), catchup_runs fenced. FakeConn
unit tests, plus a real-PostgreSQL test when ORDER_SERVER_TEST_DSN points at a DISPOSABLE database."""
from __future__ import annotations

import os
import re
import uuid
from collections.abc import AsyncIterator

import pytest

from src.db import MIGRATIONS_DIR
from src.db.epoch import EngineDb, Epoch, bump_epoch
from src.db.fence import FENCE, EpochMismatch
from src.db.migrate import migrate
from src.db.pg import AsyncpgSource, create_asyncpg_source
from src.db.uptime_store import (
    SQL_CATCHUP_INSERT,
    SQL_COVERAGE_GET,
    SQL_COVERAGE_UPSERT,
    SQL_LAST_EVENT_BEFORE,
    SQL_PRIOR_RUNS,
    SQL_RUN_HEARTBEAT,
    SQL_RUN_START,
    SQL_RUN_STOP,
    SQL_RUN_SUSPENDED,
    SQL_STORED_ORDERS,
    CatchupRecord,
    PgCatchupStore,
    PgUptimeStore,
)
from tests.db.conftest import EPOCH, FakeConn, FakeSource

MIGRATION = (MIGRATIONS_DIR / "0012_uptime_catchup.sql").read_text(encoding="utf-8")
RUN = "6f1c1d2e-0000-4000-8000-000000000001"


def rec(**kw: object) -> CatchupRecord:
    base: dict[str, object] = dict(run_id=RUN, adapter_id="stonex", reason="startup", gap_start_ms=1_000,
                                   gap_end_ms=2_000, covered_from_ms=1_000, offline_from_ms=1_900, started_at_ms=2_000,
                                   finished_at_ms=2_500, orders_seen=3, orders_new=1, orders_updated=1,
                                   positions_seen=1, positions_new=0, positions_closed=1, cash_flows_new=2,
                                   anomalies=0, complete=True, notes="")
    base.update(kw)
    return CatchupRecord(**base)  # type: ignore[arg-type]


# ===================================================================== migration text
def test_migration_creates_the_three_tables_idempotently_and_has_no_amount_column() -> None:
    tables = set(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)\s*\(", MIGRATION))
    assert tables == {"engine_runs", "adapter_coverage", "catchup_runs"}
    assert not re.search(r"CREATE TABLE (?!IF NOT EXISTS)", MIGRATION)
    assert not re.search(r"CREATE (UNIQUE )?INDEX (?!IF NOT EXISTS)", MIGRATION)
    body = MIGRATION.split("SET client_min_messages", 1)[1]
    assert not re.search(r"\b(amount|balance|equity|margin|pnl)\b", body, re.I)
    for col in ("run_id", "node_id", "started_at_ms", "last_seen_ms", "stopped_at_ms", "end_reason",
                "suspended_gaps", "last_streaming_ms", "last_streaming_run_id", "gap_start_ms", "gap_end_ms",
                "covered_from_ms", "orders_seen", "orders_new", "orders_updated", "positions_seen", "positions_new",
                "cash_flows_new", "complete", "notes"):
        assert re.search(rf"^\s+{col}\s", body, re.M), col
    assert "'startup', 'resume_after_sleep', 'operator'" in body
    assert "WITHOUT the hard epoch fence" in MIGRATION         # the accepted reading is documented in place


# ===================================================================== SQL shape
def test_run_writes_are_unfenced_and_coverage_carries_the_epoch_as_a_soft_condition() -> None:
    for sql in (SQL_RUN_START, SQL_RUN_HEARTBEAT, SQL_RUN_SUSPENDED, SQL_RUN_STOP):
        assert FENCE not in sql
    assert FENCE in SQL_COVERAGE_UPSERT and "GREATEST" in SQL_COVERAGE_UPSERT       # only moves forward
    assert FENCE in SQL_CATCHUP_INSERT
    assert "stopped_at_ms IS NULL" in SQL_RUN_STOP                                     # a stop is written once


async def test_uptime_store_statements_and_arguments() -> None:
    conn = FakeConn()
    store = PgUptimeStore(FakeSource(conn))
    await store.start_run(RUN, "pc-1", 10)
    conn.status_for(SQL_RUN_HEARTBEAT, "UPDATE 1", "UPDATE 0")
    assert await store.heartbeat(RUN, 20) is True
    assert await store.heartbeat(RUN, 30) is False            # the row is missing: the tracker rewrites it
    await store.note_suspended(RUN, 40)
    await store.stop_run(RUN, 50, "SIGINT")
    assert conn.args_of(SQL_RUN_START) == [(RUN, "pc-1", 10)]
    assert conn.args_of(SQL_RUN_STOP) == [(RUN, 50, "SIGINT")]


async def test_coverage_under_a_stale_epoch_is_skipped_without_raising() -> None:
    conn = FakeConn()
    store = PgUptimeStore(FakeSource(conn))
    conn.status_for(SQL_COVERAGE_UPSERT, "INSERT 0 1")
    assert await store.advance_coverage(EPOCH.revision, "stonex", 100, RUN) is True
    assert await store.advance_coverage(EPOCH.revision - 1, "stonex", 200, RUN) is False   # fenced out: 0 rows
    assert conn.args_of(SQL_COVERAGE_UPSERT)[0] == (EPOCH.revision, "stonex", 100, RUN)


async def test_catchup_store_reads_and_the_fenced_record() -> None:
    conn = FakeConn()
    store = PgCatchupStore(EngineDb(FakeSource(conn), lambda: EPOCH))
    conn.returns(SQL_COVERAGE_GET, ("stonex", 123, RUN))
    cov = await store.coverage("stonex")
    assert cov is not None and (cov.last_streaming_ms, cov.run_id) == (123, RUN)
    conn.returns_all(SQL_STORED_ORDERS, [("O1", "a" * 64 + " ", "FILLED")])
    got = await store.stored_orders("stonex", ["O1", "O2"])
    assert got["O1"].checksum == "a" * 64 and got["O1"].status == "FILLED"
    assert await store.stored_orders("stonex", []) == {}       # no query for nothing
    conn.returns(SQL_CATCHUP_INSERT, (5,))
    assert await store.record(EPOCH.revision, rec()) == 5
    conn.stored_epoch = EPOCH.revision + 1                     # a newer leader: the fence is lost
    with pytest.raises(EpochMismatch):
        await store.record(EPOCH.revision, rec())


# ===================================================================== real PostgreSQL
DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
IT_EPOCH = Epoch("it-cluster", 12)


@pytest.fixture
async def source() -> AsyncIterator[AsyncpgSource]:
    assert DSN is not None
    src = await create_asyncpg_source(DSN, application_name="xtnl:12:it-uptime")
    async with src.acquire() as conn:
        await migrate(conn)
        await conn.execute("TRUNCATE engine_runs, adapter_coverage, catchup_runs, engine_epoch RESTART IDENTITY")
        await bump_epoch(conn, IT_EPOCH, "it-node", terminate_older=False)
    try:
        yield src
    finally:
        await src.close()


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
async def test_pg_uptime_coverage_and_catchup_rows(source: AsyncpgSource) -> None:
    up = PgUptimeStore(source)
    run, prev = str(uuid.uuid4()), str(uuid.uuid4())
    await up.start_run(prev, "pc-1", 1_000)
    await up.heartbeat(prev, 5_000)
    await up.start_run(run, "pc-1", 9_000)
    await up.start_run(run, "pc-1", 9_000)                       # idempotent
    assert await up.heartbeat(run, 10_000) is True
    assert await up.heartbeat(str(uuid.uuid4()), 10_000) is False
    await up.note_suspended(run, 11_000)
    await up.stop_run(run, 12_000, "SIGINT")
    await up.stop_run(run, 13_000, "again")                      # the first stop stays
    p = await up.previous_run("pc-1", run)
    assert p is not None and p.run_id == prev and p.last_seen_ms == 5_000 and p.stopped_at_ms is None
    async with source.acquire() as conn:
        row = await conn.fetchrow("SELECT last_seen_ms, stopped_at_ms, end_reason, suspended_gaps FROM engine_runs "
                                  "WHERE run_id = $1::uuid", run)
    assert tuple(row) == (12_000, 12_000, "SIGINT", 1)

    assert await up.advance_coverage(IT_EPOCH.revision, "stonex", 20_000, run) is True
    assert await up.advance_coverage(IT_EPOCH.revision, "stonex", 15_000, run) is True   # never moves back
    assert await up.advance_coverage(IT_EPOCH.revision - 1, "stonex", 30_000, run) is False
    store = PgCatchupStore(EngineDb(source, lambda: IT_EPOCH))
    cov = await store.coverage("stonex")
    assert cov is not None and cov.last_streaming_ms == 20_000 and cov.run_id == run
    rid = await store.record(IT_EPOCH.revision, rec(run_id=run))
    assert rid >= 1
    with pytest.raises(EpochMismatch):
        await store.record(IT_EPOCH.revision + 5, rec(run_id=run))
    assert await store.stored_orders("stonex", ["nope"]) == {}
    assert await store.known_cash_flows("stonex", ["nope"]) == set()

    # the operator CLI reads the same rows (read-only, no amounts)
    from src.ops.service import OpsService
    async with source.acquire() as conn:
        svc = OpsService(conn)
        u = await svc.uptime_show(limit=10)
        c = await svc.catchup_show(limit=10)
    assert [r["reason"] for r in u.rows] == ["SIGINT", None]
    assert u.sections["adapter coverage"][0]["adapter"] == "stonex"
    assert u.sections["offline gaps"][0]["offline"] == "0m 4s"
    assert c.rows[0]["orders_new"] == 1 and c.rows[0]["complete"] is True


# ===================================================================== migration 0013 (reconnect / retry reasons)
MIGRATION_0013 = (MIGRATIONS_DIR / "0013_catchup_reasons.sql").read_text(encoding="utf-8")


def test_migration_0013_widens_the_reason_check_idempotently() -> None:
    body = MIGRATION_0013.split("SET client_min_messages", 1)[1]
    assert "DROP CONSTRAINT IF EXISTS catchup_runs_reason_check" in body
    assert "'startup', 'resume_after_sleep', 'operator', 'reconnect', 'retry'" in body
    assert not re.search(r"\b(amount|balance|equity|margin|pnl)\b", body, re.I)


def test_every_coordinator_reason_is_allowed_by_the_schema() -> None:
    from typing import get_args

    from src.runtime.catchup import CatchupReason
    allowed = set(re.findall(r"'(\w+)'", MIGRATION_0013.split("CHECK", 1)[1]))
    assert set(get_args(CatchupReason)) == allowed


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
async def test_pg_catchup_runs_accepts_reconnect_and_retry_rows(source: AsyncpgSource) -> None:
    store = PgCatchupStore(EngineDb(source, lambda: IT_EPOCH))
    for reason in ("reconnect", "retry"):
        assert await store.record(IT_EPOCH.revision, rec(reason=reason)) >= 1
    async with source.acquire() as conn:
        await migrate(conn)                                        # re-applying is a no-op
        with pytest.raises(Exception, match="catchup_runs_reason_check"):
            await conn.execute("INSERT INTO catchup_runs (epoch, adapter_id, reason, gap_start_ms, gap_end_ms, "
                               "started_at_ms, finished_at_ms, complete) VALUES (1, 'x', 'bogus', 0, 0, 0, 0, true)")


# ===================================================================== prior activity (a window without coverage)
async def test_prior_activity_is_the_older_of_the_earliest_earlier_run_and_the_last_event_before_the_term() -> None:
    conn = FakeConn()
    store = PgCatchupStore(EngineDb(FakeSource(conn), lambda: EPOCH))
    assert await store.prior_activity_ms("stonex", RUN, 50_000) is None          # nothing recorded
    conn.returns(SQL_PRIOR_RUNS, (40_000,))
    conn.returns(SQL_LAST_EVENT_BEFORE, (30_000 << 20 | 7,))                     # an HLC seq_id
    assert await store.prior_activity_ms("stonex", RUN, 50_000) == 30_000
    conn.returns(SQL_PRIOR_RUNS, (20_000,))
    conn.returns(SQL_LAST_EVENT_BEFORE, (None,))
    assert await store.prior_activity_ms("stonex", None, 50_000) == 20_000
    assert conn.args_of(SQL_PRIOR_RUNS)[-1] == (None, 50_000)
    assert conn.args_of(SQL_LAST_EVENT_BEFORE)[-1] == (50_000 << 20,)


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
async def test_pg_prior_activity_queries(source: AsyncpgSource) -> None:
    """The two reads behind `prior_activity_ms`, inside a rolled-back transaction (event_log is append-only)."""
    cur, old, later = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    async with source.acquire() as conn:
        await conn.execute("BEGIN")
        try:
            await conn.execute(SQL_RUN_START, old, "pc-1", 1_000)
            await conn.execute("UPDATE engine_runs SET last_seen_ms = 4_000, stopped_at_ms = 5_000, "
                               "end_reason = 'SIGINT' WHERE run_id = $1::uuid", old)
            await conn.execute(SQL_RUN_START, cur, "pc-1", 9_000)
            await conn.execute(SQL_RUN_START, later, "pc-2", 99_000)                # started after this term
            row = await conn.fetchrow(SQL_PRIOR_RUNS, cur, 10_000)
            assert row is not None and row[0] == 5_000                              # the stop beats last_seen
            row = await conn.fetchrow(SQL_PRIOR_RUNS, None, 10_000)
            assert row is not None and row[0] == 5_000
            await conn.execute(
                "INSERT INTO event_log (seq_id, entity_type, entity_key, adapter_id, action, payload, epoch_revision) "
                "VALUES ($1::bigint, 'ORDER', 'k', 'stonex', 'APPLIED', '{}'::jsonb, 1)", (7_000 << 20) | 3)
            row = await conn.fetchrow(SQL_LAST_EVENT_BEFORE, 8_000 << 20)
            assert row is not None and int(row[0]) >> 20 == 7_000
        finally:
            await conn.execute("ROLLBACK")
