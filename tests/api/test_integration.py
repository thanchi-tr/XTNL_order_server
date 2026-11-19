"""The real compiler against a DISPOSABLE PostgreSQL (ORDER_SERVER_TEST_DSN; the fixture of
tests/reports/test_export_pg.py truncates the entity tables) and the cache against a real Redis
(ORDER_SERVER_TEST_REDIS, database 15 of the throwaway test stack). Skipped when the variables are not set."""
from __future__ import annotations

import asyncio
import json
import os
import time

import pytest

from src.api.pinescript import CACHE_TTL_S, CompileError, PinescriptService, connect_readonly, pg_compiler, week_window
from src.reports.lifecycle import Window

from tests.api.conftest import FakeCompiler

DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
REDIS = os.environ.get("ORDER_SERVER_TEST_REDIS")
H = 3_600_000


@pytest.mark.integration
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
def test_pg_compiler_reads_the_real_database_read_only() -> None:
    from tests.reports.test_export_pg import seed
    assert DSN is not None
    now = int(time.time() * 1000)
    asyncio.run(seed(now))
    # the throwaway test DSN is a writable owner role: the role check is skipped here and tested just below
    scripts = asyncio.run(pg_compiler(DSN, verify_role=False)(Window(now - 3 * 24 * H, now), now))
    assert [s for s, _ in scripts] == ["AUDUSD", "EURUSD"]
    assert all(t.startswith("//@version=6") for _, t in scripts)

    async def write_attempt() -> None:
        conn, close = await connect_readonly(DSN)
        try:
            await conn.execute("CREATE TEMP TABLE api_probe (x int)")
            await conn.execute("INSERT INTO api_probe VALUES (1)")
            await conn.execute("CREATE TABLE api_probe_real (x int)")
        finally:
            await close()

    with pytest.raises(Exception) as exc:                                   # the session is read only
        asyncio.run(write_attempt())
    assert getattr(exc.value, "sqlstate", None) == "25006"


@pytest.mark.integration
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
def test_pg_compiler_refuses_the_writable_test_role() -> None:
    assert DSN is not None
    with pytest.raises(CompileError) as exc:
        asyncio.run(pg_compiler(DSN)(Window(0, 1), 1))
    assert exc.value.code == "db_role_refused"


@pytest.mark.integration
def test_pg_compiler_unreachable_is_db_unavailable() -> None:
    dead = "postgresql://xtnl:nope@127.0.0.1:1/xtnl"
    with pytest.raises(CompileError) as exc:
        asyncio.run(pg_compiler(dead)(Window(0, 1), 1))
    assert exc.value.code == "db_unavailable"


@pytest.mark.integration
@pytest.mark.skipif(not REDIS, reason="ORDER_SERVER_TEST_REDIS not set")
def test_cache_on_a_real_redis() -> None:
    from src.cache.client import create_redis
    assert REDIS is not None

    async def go() -> None:
        r = create_redis(REDIS)
        try:
            week, _ = week_window(int(time.time() * 1000))
            await r.delete(f"pinescript:week:{week}", f"pinescript:status:{week}")
            svc = PinescriptService(r, FakeCompiler())
            assert (await svc.start(force=False))["status"] == "compiling"
            for _ in range(100):
                if not svc.running(week):
                    break
                await asyncio.sleep(0.01)
            latest = await svc.latest()
            assert latest["status"] == "ready" and len(latest["scripts"]) == 2
            ttl = await r.ttl(f"pinescript:week:{week}")
            assert CACHE_TTL_S - 10 <= ttl <= CACHE_TTL_S
            assert json.loads(await r.get(f"pinescript:week:{week}"))["week"] == week
            await r.delete(f"pinescript:week:{week}", f"pinescript:status:{week}")
        finally:
            await r.aclose()

    asyncio.run(go())
