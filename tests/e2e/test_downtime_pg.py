"""Downtime catch-up end to end against a real PostgreSQL and Redis (single-PC mode; tests/e2e/downtime.py).

Skipped unless ORDER_SERVER_TEST_DSN and ORDER_SERVER_TEST_REDIS point at a DISPOSABLE PostgreSQL database and a
DISPOSABLE Redis (deploy/docker-compose.dev.yml): the test runs the migrations, truncates the engine tables
(including engine_runs, adapter_coverage and catchup_runs) and LEADER_INIT flushes the Redis database.

The production stores run: PgUptimeStore (engine_runs, adapter_coverage) and the leader ports' PgCatchupStore
(fenced catchup_runs), across two leader terms with a newer epoch, as two engine processes would.
"""
from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from src.cache.client import create_redis
from src.cache.live_reader import info_run_id
from src.db.epoch import Epoch
from src.db.migrate import migrate
from src.db.pg import AsyncpgSource, ConnectionSource, create_asyncpg_source
from src.db.uptime_store import CatchupRecord, Coverage, PgUptimeStore, RunRow
from src.runtime.pg_ports import LazyPgSource, control_application_name, pg_leader_ports
from tests.e2e.downtime import Backend, run_restart_scenario
from tests.e2e.scenario import ADAPTER, NODE
from tests.e2e.test_replay import TABLES

DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
REDIS = os.environ.get("ORDER_SERVER_TEST_REDIS")
pytestmark = [pytest.mark.integration,
              pytest.mark.skipif(not (DSN and REDIS), reason="ORDER_SERVER_TEST_DSN / ORDER_SERVER_TEST_REDIS not set")]

E1 = Epoch("downtime-cluster", 31)
E2 = Epoch("downtime-cluster", 32)


class PgDowntimeViews:
    def __init__(self, source: ConnectionSource) -> None:
        self._s = source

    async def runs(self) -> list[RunRow]:
        async with self._s.acquire() as conn:
            rows = await conn.fetch("SELECT run_id::text, started_at_ms, last_seen_ms, stopped_at_ms, end_reason, "
                                    "suspended_gaps FROM engine_runs ORDER BY started_at_ms")
        return [RunRow(str(r[0]), int(r[1]), int(r[2]), None if r[3] is None else int(r[3]),
                       None if r[4] is None else str(r[4]), int(r[5])) for r in rows]

    async def coverage(self, adapter_id: str) -> Coverage | None:
        async with self._s.acquire() as conn:
            r = await conn.fetchrow("SELECT adapter_id, last_streaming_ms, last_streaming_run_id::text "
                                    "FROM adapter_coverage WHERE adapter_id = $1", adapter_id)
        return None if r is None else Coverage(str(r[0]), int(r[1]), None if r[2] is None else str(r[2]))

    async def catchups(self) -> list[CatchupRecord]:
        async with self._s.acquire() as conn:
            rows = await conn.fetch(
                "SELECT run_id::text, adapter_id, reason, gap_start_ms, gap_end_ms, covered_from_ms, offline_from_ms, "
                "started_at_ms, finished_at_ms, orders_seen, orders_new, orders_updated, positions_seen, "
                "positions_new, positions_closed, cash_flows_new, anomalies, complete, notes FROM catchup_runs "
                "ORDER BY id")
        return [CatchupRecord(*r) for r in rows]


@pytest.fixture
async def admin() -> AsyncIterator[AsyncpgSource]:
    assert DSN is not None
    src = await create_asyncpg_source(DSN, application_name="xtnl-e2e-downtime-admin")
    async with src.acquire() as conn:
        await migrate(conn)
        await conn.execute(f"TRUNCATE {TABLES}, engine_runs, adapter_coverage, catchup_runs "
                           "RESTART IDENTITY CASCADE")
        await conn.execute("INSERT INTO cluster_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING")
        await conn.execute("UPDATE cluster_state SET safe_hold = false, safe_hold_since = NULL, "
                           "safe_hold_reason = NULL, risksweep_incident_id = NULL, risksweep_abdicated_at = NULL, "
                           "broker_abdications = '{}'::jsonb WHERE id = 1")
    try:
        yield src
    finally:
        await src.close()


@pytest.mark.parametrize("incomplete", [False, True], ids=["complete", "incomplete"])
async def test_restart_catchup_through_postgres_and_redis(admin: AsyncpgSource, tmp_path: Path,
                                                          incomplete: bool) -> None:
    assert DSN is not None and REDIS is not None
    redis: Any = create_redis(REDIS)
    control = LazyPgSource(DSN, application_name=control_application_name(NODE), max_size=2)
    fence_lost: list[str] = []

    def ports(epoch: Epoch) -> Any:
        return pg_leader_ports(epoch, control=control, dsn=DSN, node_id=NODE, redis=redis,
                               on_fence_lost=lambda exc: fence_lost.append(type(exc).__name__),
                               run_id_reader=info_run_id)

    b = Backend(PgUptimeStore(control), ports, PgDowntimeViews(admin), redis)
    try:
        p2, _ = await run_restart_scenario(b, tmp_path, (E1, E2), incomplete=incomplete)
        try:
            await p2.tracker.tick()
            cov = await b.views.coverage(ADAPTER)
            # complete or not (an incomplete broker answer is paged once, never re-read forever), coverage moves on
            assert cov is not None and cov.run_id == p2.tracker.run_id
            async with admin.acquire() as conn:
                epochs = [int(r[0]) for r in await conn.fetch("SELECT epoch FROM catchup_runs ORDER BY id")]
            assert epochs == [E1.token, E2.token]
        finally:
            await p2.stop()
        assert not fence_lost
    finally:
        await control.close()
        await redis.aclose()
