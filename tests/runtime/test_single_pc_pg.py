"""The single-PC auto-resume against a real PostgreSQL (skipped unless ORDER_SERVER_TEST_DSN points at a DISPOSABLE
database: engine tables are truncated). Checks what a fake cannot: migration 0007's CHECKs accept the audit row, the
resume + audit row commit together under the stored epoch, a newer hold is never cleared by a resume judged for an
older one, and SQL_COUNT_AUTO_RESUMES starts a fresh budget after a person's resume. With ORDER_SERVER_TEST_ENGINE_DSN
(the same database as the engine's login role, e.g. xtnl_engine) the audit insert also runs under that role's grants."""
from __future__ import annotations

import importlib
import os
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import pytest

from src.db.epoch import Epoch, bump_epoch
from src.db.cluster_state import SQL_AUDIT_INSERT
from src.db.migrate import migrate
from src.db.pg import AsyncpgConnection, PgConnection, init_connection
from src.runtime.single_pc import AUDIT_OPERATOR, AUTO_RESUME_ACTOR, PgAutoResumeStore

DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
ENGINE_DSN = os.environ.get("ORDER_SERVER_TEST_ENGINE_DSN")
pytestmark = [pytest.mark.integration, pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")]


class _One:
    def __init__(self, conn: PgConnection) -> None:
        self.conn = conn

    @asynccontextmanager
    async def _acq(self) -> AsyncIterator[PgConnection]:
        yield self.conn

    def acquire(self) -> AbstractAsyncContextManager[PgConnection]:
        return self._acq()


@pytest.fixture
async def conn() -> AsyncIterator[PgConnection]:
    asyncpg = importlib.import_module("asyncpg")
    raw = await asyncpg.connect(DSN, server_settings={"application_name": "xtnl-it-single-pc"})
    await init_connection(raw)
    c = AsyncpgConnection(raw)
    try:
        await migrate(c)
        await c.execute("TRUNCATE engine_epoch RESTART IDENTITY CASCADE")
        await c.execute("UPDATE cluster_state SET safe_hold = false WHERE id = 1")
        await bump_epoch(c, Epoch("it-single-pc", 21), "it-node", terminate_older=False)
        # operator_audit is append-only: a person's resume row as the baseline gives each test a fresh budget
        await _audit(c, "it-baseline-operator")
        yield c
    finally:
        await raw.close()


async def test_auto_resume_and_audit_commit_together(conn: PgConnection) -> None:
    await conn.execute("UPDATE cluster_state SET safe_hold = true, safe_hold_since = 1000, "
                       "safe_hold_reason = 'it: 3 abnormal' WHERE id = 1")
    store = PgAutoResumeStore(_One(conn))
    before = await store.count_auto_resumes(0)
    rec = await store.resume("single-PC auto-resume 1 of 6 after a 300 s cool-down; held: it", {"auto_resume": 1},
                             1000, "it: 3 abnormal")
    assert rec is not None and rec.epoch == 21 and rec.held_reason == "it: 3 abnormal"
    row = await conn.fetchrow("SELECT safe_hold, resumed_by, last_resume_at IS NOT NULL FROM cluster_state WHERE id = 1")
    assert row == (False, AUTO_RESUME_ACTOR, True)
    audit = await conn.fetchrow("SELECT command, outcome, operator, approver, epoch, detail->>'actor' FROM "
                                "operator_audit ORDER BY audit_id DESC LIMIT 1")
    assert audit == ("safe_hold.resume", "applied", AUDIT_OPERATOR, None, 21, AUTO_RESUME_ACTOR)
    assert await store.count_auto_resumes(0) == before + 1
    assert await store.resume("single-PC auto-resume 2 of 6", {}, 1000, "it: 3 abnormal") is None   # nothing held
    assert await store.count_auto_resumes(0) == before + 1


async def test_a_newer_hold_is_not_cleared_by_a_resume_judged_for_an_older_one(conn: PgConnection) -> None:
    await conn.execute("UPDATE cluster_state SET safe_hold = true, safe_hold_since = 2000, "
                       "safe_hold_reason = 'it: 7 epoch changes' WHERE id = 1")
    store = PgAutoResumeStore(_One(conn))
    assert await store.resume("single-PC auto-resume 1 of 6; held: it", {}, 1000, "it: 3 abnormal") is None
    assert await store.resume("single-PC auto-resume 1 of 6; held: it", {}, 2000, "it: other reason") is None
    row = await conn.fetchrow("SELECT safe_hold, safe_hold_since FROM cluster_state WHERE id = 1")
    assert row == (True, 2000)
    assert await store.count_auto_resumes(0) == 0                                   # no audit row either
    rec = await store.resume("single-PC auto-resume 1 of 6; held: it", {}, 2000, "it: 7 epoch changes")
    assert rec is not None and rec.held_since == 2000


async def _audit(conn: PgConnection, operator: str, outcome: str = "applied") -> None:
    approver = None if operator == AUDIT_OPERATOR else "it-approver"
    await conn.fetchrow(SQL_AUDIT_INSERT, "safe_hold.resume", "cluster", operator, approver,
                        "it: budget test row", outcome, "{}", 21, None)


async def test_a_persons_resume_starts_a_fresh_auto_resume_budget(conn: PgConnection) -> None:
    store = PgAutoResumeStore(_One(conn))
    for _ in range(6):
        await _audit(conn, AUDIT_OPERATOR)
    assert await store.count_auto_resumes(0) == 6                  # budget used up
    await _audit(conn, "it-operator", outcome="refused")
    assert await store.count_auto_resumes(0) == 6                  # a refused resume changes nothing
    await _audit(conn, "it-operator")
    assert await store.count_auto_resumes(0) == 0                  # the person's resume: a fresh budget
    await _audit(conn, AUDIT_OPERATOR)
    assert await store.count_auto_resumes(0) == 1
    far = await conn.fetchrow("SELECT max(at_ms) + 1 FROM operator_audit")
    assert far is not None
    assert await store.count_auto_resumes(int(str(far[0]))) == 0   # the window bound still applies


@pytest.mark.skipif(not ENGINE_DSN, reason="ORDER_SERVER_TEST_ENGINE_DSN not set")
async def test_the_engine_role_may_write_and_count_the_audit_row(conn: PgConnection) -> None:
    asyncpg = importlib.import_module("asyncpg")
    await conn.execute("UPDATE cluster_state SET safe_hold = true, safe_hold_since = 3000, "
                       "safe_hold_reason = 'it: engine role' WHERE id = 1")
    raw = await asyncpg.connect(ENGINE_DSN, server_settings={"application_name": "xtnl-it-single-pc-engine"})
    try:
        await init_connection(raw)
        eng = AsyncpgConnection(raw)
        store = PgAutoResumeStore(_One(eng))
        rec = await store.resume("single-PC auto-resume 1 of 6; held: it", {"auto_resume": 1}, 3000,
                                 "it: engine role")
        assert rec is not None and rec.epoch == 21
        assert await store.count_auto_resumes(0) == 1
    finally:
        await raw.close()
