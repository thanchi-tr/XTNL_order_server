"""End-to-end replay against a real PostgreSQL and Redis (IMPLEMENTATION_PLAN 4.3).

Skipped unless ORDER_SERVER_TEST_DSN and ORDER_SERVER_TEST_REDIS point at a DISPOSABLE PostgreSQL database
and a DISPOSABLE Redis (deploy/docker-compose.dev.yml): the test runs the migrations, truncates the engine
tables, and LEADER_INIT step 3 runs FLUSHALL on that Redis server (every database, not only the selected
one).

The real composition runs: EngineSession on the PostgreSQL ports (src/runtime/pg_ports.py) through
LEADER_INIT, the MockBrokerAdapter fixture through the orchestrator -> PostgreSQL -> Redis apply_state ->
oversee:feed -> RiskSweep; see tests/e2e/scenario.py for the checks.
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
from src.db.rows import level_from_db
from src.runtime.pg_ports import LazyPgSource, control_application_name, pg_leader_ports
from src.runtime.session import EngineSession, SessionContext
from tests.e2e.scenario import FAST_TIMINGS, NODE, FakeLease, fixture, mock_adapter, run_scenario, wall_ms

DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
REDIS = os.environ.get("ORDER_SERVER_TEST_REDIS")
pytestmark = [pytest.mark.integration,
              pytest.mark.skipif(not (DSN and REDIS), reason="ORDER_SERVER_TEST_DSN / ORDER_SERVER_TEST_REDIS not set")]

TABLES = ("engine_epoch, leader_terms, orders_state, positions_state, account_state, account_snapshots, event_log, "
          "outbox_events, halt_breaches, breach_acks, halt_events, oversee_events, notification_outbox, "
          "notification_dedupe, stop_watch, stream_health, sod_marks, sweep_quarantine, cash_flows, instruments, "
          "last_clean_marks, events_dlq, events_quarantine, adapter_auth_state, market_sessions")
EPOCH = Epoch("e2e-cluster", 11)


class PgViews:
    def __init__(self, source: ConnectionSource) -> None:
        self._s = source

    async def breaches(self) -> list[tuple[str, str, str]]:
        async with self._s.acquire() as conn:
            rows = await conn.fetch("SELECT rule_id, subject_id, level FROM halt_breaches WHERE resolved_at IS NULL")
        return [(str(r[0]), str(r[1]), level_from_db(r[2]).name) for r in rows]

    async def halt_events(self) -> list[tuple[int, str]]:
        async with self._s.acquire() as conn:
            rows = await conn.fetch("SELECT id, effective_level FROM halt_events ORDER BY id")
        return [(int(r[0]), str(r[1])) for r in rows]

    async def outbox(self) -> list[tuple[str, str]]:
        async with self._s.acquire() as conn:
            rows = await conn.fetch("SELECT kind, subject FROM notification_outbox ORDER BY outbox_id")
        return [(str(r[0]), str(r[1])) for r in rows]


@pytest.fixture
async def admin() -> AsyncIterator[AsyncpgSource]:
    assert DSN is not None
    src = await create_asyncpg_source(DSN, application_name="xtnl-e2e-admin")
    async with src.acquire() as conn:
        await migrate(conn)
        await conn.execute(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE")
        await conn.execute("INSERT INTO cluster_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING")
        await conn.execute("UPDATE cluster_state SET safe_hold = false, safe_hold_since = NULL, "
                           "safe_hold_reason = NULL, risksweep_incident_id = NULL, risksweep_abdicated_at = NULL, "
                           "broker_abdications = '{}'::jsonb WHERE id = 1")
    try:
        yield src
    finally:
        await src.close()


async def test_replay_through_postgres_and_redis(admin: AsyncpgSource, tmp_path: Path) -> None:
    assert DSN is not None and REDIS is not None
    redis: Any = create_redis(REDIS)
    control = LazyPgSource(DSN, application_name=control_application_name(NODE), max_size=2)
    exits: list[str] = []

    def ports(epoch: Epoch) -> Any:
        return pg_leader_ports(epoch, control=control, dsn=DSN, node_id=NODE, redis=redis,
                               on_fence_lost=lambda exc: exits.append(f"fence: {exc}"), run_id_reader=info_run_id)

    fx = fixture(wall_ms())
    adapter = mock_adapter(fx)
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=ports,
                         adapters=lambda: [adapter], lease=FakeLease(), exit_fn=exits.append, timings=FAST_TIMINGS)
    session = EngineSession(ctx, EPOCH)
    try:
        leader = await run_scenario(session, adapter, fx, EPOCH, PgViews(admin))
        await leader.shutdown()
        async with admin.acquire() as conn:
            term = await conn.fetchrow("SELECT reached_leader_at, end_reason FROM leader_terms "
                                       "WHERE cluster_id = $1 AND epoch_revision = $2", EPOCH.cluster_id,
                                       EPOCH.revision)
            dropped = await conn.fetchrow("SELECT count(*) FROM event_log WHERE action = 'DROPPED_STALE' "
                                          "AND entity_key = 'position:mock:P-EU-1'")
        assert term is not None and term[0] is not None and term[1] == "switchover"
        assert dropped is not None and int(dropped[0]) >= 1
        assert not exits and not leader.exits
    finally:
        await session.aclose()
        await control.close()
        await redis.aclose()
