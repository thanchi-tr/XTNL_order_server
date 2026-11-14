"""The wired NodeController through a whole term, and the SM3 ingestion hooks, without any network:
FakeEtcd, fakeredis + lupa, in-memory storage ports, MockBrokerAdapter, httpx.MockTransport."""
from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import fakeredis
import httpx
import pytest

from src.db.epoch import Epoch
from src.dto import OrderStatus
from src.integrity.ports import IntegrityState
from src.pubsub.connection import ConnState
from src.runtime.config import RuntimeConfig
from src.runtime.pg_ports import SQL_PRIMARY_WRITABLE
from src.runtime.ports import LeaderPorts
from src.runtime.probes import BrokerEndpoint, BrokerTlsMonitor, NodeHealthProbe, parse_chrony_tracking
from src.runtime.roles import NodeRole
from src.runtime.session import EngineSession, SessionContext
from src.runtime.wiring import MOCK_BROKER_VAR, EngineHooks, build_engine
from tests.db.conftest import FakeConn, FakeSource
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import (
    ADAPTER,
    FAST_TIMINGS,
    NODE,
    FakeLease,
    become_leader,
    eventually,
    fixture,
    mock_adapter,
    wall_ms,
)
from tests.runtime.fakes import FakeClock, FakeEtcd

ENV = {"BROKER_ENV": "sandbox", "XTNL_NODE_ID": NODE, MOCK_BROKER_VAR: "1",
       "XTNL_ETCD_ENDPOINTS": "http://e1:2379,http://e2:2379,http://e3:2379",
       "XTNL_DEADMAN_URL": "https://deadman.example.invalid/beat",
       "ORDER_SERVER_DSN": "postgresql://u:pw@10.0.1.10:5432/xtnl", "REDIS_URL": "redis://127.0.0.1:6379/0"}


async def run_id(_: Any) -> str:
    return "run-w"


class ControlConn(FakeConn):
    async def fetchrow(self, sql: str, *args: object) -> Any:
        if sql == SQL_PRIMARY_WRITABLE:
            return (True,)
        return await super().fetchrow(sql, *args)


@pytest.fixture
async def redis() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.flushall()
    await r.aclose()


async def test_controller_runs_a_whole_term(redis: Any, tmp_path: Path) -> None:
    beats: list[httpx.Request] = []

    def http(request: httpx.Request) -> httpx.Response:
        beats.append(request)
        return httpx.Response(503 if request.url.port == 8008 else 200)

    backends: dict[int, MemoryBackend] = {}

    def ports(epoch: Epoch) -> LeaderPorts:
        backend = backends.setdefault(epoch.token, MemoryBackend(redis, run_id, epoch.token))
        return backend.ports(epoch)

    async def chrony() -> float:
        return 0.2

    exits: list[str] = []
    fx = fixture(wall_ms())
    hooks = EngineHooks(etcd=FakeEtcd(FakeClock()), redis=redis, control_source=FakeSource(ControlConn()),
                        ports=ports, run_id_reader=run_id, exit_fn=exits.append, chrony=chrony,
                        http_client=lambda: httpx.AsyncClient(transport=httpx.MockTransport(http)),
                        patroni_client=httpx.AsyncClient(transport=httpx.MockTransport(http)),
                        adapters=lambda: [mock_adapter(fx)], timings=FAST_TIMINGS, require_keeper_thread=False)
    config = RuntimeConfig.from_env(ENV, spill_path=str(tmp_path / "spill.jsonl"))
    engine = build_engine(config, environ=ENV, hooks=hooks)
    ctl = engine.controller
    try:
        await ctl.startup()
        assert ctl.role is NodeRole.STANDBY
        await ctl.step()
        assert ctl.machine.role.value == NodeRole.LEADER.value, ctl.machine.history
        assert ctl.last_record is not None and ctl.last_record.passes_hard_checks(ctl.last_record.ts_ms)
        session = ctl.session
        assert isinstance(session, EngineSession)
        backend = backends[session.epoch.token]
        await eventually(lambda: _is(session.orchestrator.runtime(ADAPTER).conn.state is ConnState.STREAMING),
                         "STREAMING")
        await eventually(lambda: _is(session.deadman is not None and session.deadman.sent > 0), "dead-man beat")
        beat = next(b for b in beats if b.url.host == "deadman.example.invalid")
        assert b'"epoch":"' in beat.content and b'"mock"' in beat.content
        links = session.broker_links()
        assert links[ADAPTER].streaming and not links[ADAPTER].auth_failed
        await ctl.step()                                  # LEADER: no abdication while healthy
        assert ctl.machine.role.value == NodeRole.LEADER.value
        ctl.request_drain("switchover", exit_after=False)
        await ctl.step()
        assert ctl.role is NodeRole.STANDBY and ctl.session is None
        assert ("end_reason", "switchover") in backend.tables.terms
        assert not engine.keeper.holds_lease()
        assert not exits
    finally:
        await engine.aclose()


async def test_incident_hooks_pause_commits_and_reconcile(redis: Any, tmp_path: Path) -> None:
    epoch = Epoch("c", 3)
    backend = MemoryBackend(redis, run_id, epoch.token)
    fx = fixture(wall_ms())
    adapter = mock_adapter(fx)
    exits: list[str] = []
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
                         adapters=lambda: [adapter], lease=FakeLease(), run_id_reader=run_id, exit_fn=exits.append,
                         timings=FAST_TIMINGS)
    session = EngineSession(ctx, epoch)
    leader = await become_leader(session, epoch)
    try:
        await eventually(lambda: _is(session.orchestrator.runtime(ADAPTER).conn.state is ConnState.STREAMING),
                         "STREAMING")
        await eventually(lambda: _is(session._ingestion_idle()), "idle")
        await session.ingestion_control.halt_ingestion("test damage")
        cancelled = next(o for o in fx.orders if o.broker_order_id == "PF-1-SL")
        adapter.push_order_update(dataclasses.replace(cancelled, status=OrderStatus.CANCELLED,
                                                      broker_updated_at_ms=wall_ms(), source="STREAM"))
        await asyncio.sleep(0.5)
        held = backend.tables.entities["order:mock:PF-1-SL"]   # drained from the socket, not committed
        assert held.status is OrderStatus.WORKING  # type: ignore[union-attr]
        assert not session._ingestion_idle()
        assert await session.reconciler.reconcile_all(None)
        assert session.bus.orders.qsize() == 0 and session._ingestion_idle()
        stored = backend.tables.entities["order:mock:PF-1-SL"]
        assert stored.status is OrderStatus.CANCELLED  # type: ignore[union-attr]
        await session.ingestion_control.resume_all()
        assert session.switches.entities.is_set() and session.switches.prices.is_set()
        guarded = session.incident
        assert guarded.state is None
        with pytest.raises(RuntimeError):
            await session.incident._patroni.switchover()  # no Patroni configured: the step fails
        assert session.integrity.state not in (IntegrityState.HALTED,)
        assert not exits and not leader.exits
    finally:
        await leader.shutdown()


async def test_health_probe(redis: Any) -> None:
    async def ok(host: str, port: int, timeout: float) -> None:
        if host == "bad.example":
            raise OSError("refused")

    clock = {"ms": 1_000}
    tls = BrokerTlsMonitor([BrokerEndpoint("stonex", "ok.example"), BrokerEndpoint("pepperstone", "bad.example")],
                           connector=ok, min_interval_s=0.0, clock_ms=lambda: clock["ms"])

    async def chrony() -> float:
        return 0.4

    async def primary() -> str:
        return "10.9.9.9"

    probe = NodeHealthProbe(NODE, pg=FakeSource(ControlConn()), redis=redis, chrony=chrony, tls=tls,
                            patroni_primary_host=primary, addresses=lambda: frozenset({"10.9.9.9"}))
    rec = await probe.probe(5_000)
    assert rec.pg_writable and rec.redis_writable and rec.chrony_offset_ms == 0.4 and rec.hosts_patroni_primary
    assert rec.broker_tls == {"stonex": True, "pepperstone": False}
    assert tls.failed_since_ms("pepperstone") == 1_000 and tls.failed_since_ms("stonex") is None
    probe.leader_streaming = lambda: {"pepperstone": True}
    probe.market_open = lambda a: False if a == "stonex" else True
    rec = await probe.probe(6_000)
    assert rec.broker_tls == {"stonex": None, "pepperstone": True}
    down = NodeHealthProbe(NODE, pg=FakeSource(FakeConn()), redis=redis, chrony=chrony)
    rec = await down.probe(7_000)
    assert not rec.pg_writable and not rec.passes_hard_checks(7_000)
    assert parse_chrony_tracking("A1B2,ntp,3,1700000000.1,-0.000012345,0,0") == pytest.approx(0.012345)
    assert parse_chrony_tracking("garbage") is None


async def _is(v: bool) -> bool:
    return v
