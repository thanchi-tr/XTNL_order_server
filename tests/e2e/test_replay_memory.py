"""No-network smoke run of the end-to-end replay scenario (IMPLEMENTATION_PLAN 4.3): the real EngineSession
composition on in-memory storage ports (tests/e2e/memory_backend.py) and fakeredis + lupa for the Redis
scripts. Always runs in CI; the PostgreSQL + Redis version is tests/e2e/test_replay.py."""
from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import fakeredis
import pytest

from src.db.epoch import Epoch
from src.runtime.session import EngineSession, SessionContext
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import NODE, FakeLease, fixture, mock_adapter, run_scenario, wall_ms, FAST_TIMINGS

EPOCH = Epoch("cluster-e2e", 7)


async def run_id(_: Any) -> str:
    return "run-e2e"


@pytest.fixture
async def redis() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.flushall()
    await r.aclose()


async def test_replay_scenario_in_memory(redis: Any, tmp_path: Path) -> None:
    backend = MemoryBackend(redis, run_id, EPOCH.token)
    fx = fixture(wall_ms())
    adapter = mock_adapter(fx)
    exits: list[str] = []
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
                         adapters=lambda: [adapter], lease=FakeLease(), run_id_reader=run_id, exit_fn=exits.append,
                         timings=FAST_TIMINGS)
    session = EngineSession(ctx, EPOCH)
    leader = await run_scenario(session, adapter, fx, EPOCH, backend)
    try:
        assert ("reached_leader", EPOCH.label) in backend.tables.terms
        assert not exits
        # the committed history: the stale REST read was dropped, never applied
        assert ("DROP_STALE", "position:mock:P-EU-1") in backend.tables.log
    finally:
        await leader.shutdown()
    assert ("end_reason", "switchover") in backend.tables.terms
    assert not leader.exits and not exits
