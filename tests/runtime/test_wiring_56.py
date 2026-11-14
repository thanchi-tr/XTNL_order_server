"""IMPLEMENTATION_PLAN 5.6 wiring: credential rotation within a term, notification settings from the secret,
transfer sources, the fence backstop, operator request payloads and the engine's metrics registry.
No network: fake etcd, scripted secrets, FakeConn."""
from __future__ import annotations

import asyncio
from pathlib import Path

from src.adapters.stonex.adapter import StoneXAdapter
from src.db.epoch import EngineDb
from src.observability.metrics import CONTRACT
from src.runtime.pg_ports import SQL_OPERATOR_REQUESTS_PENDING, PgOperatorRequests
from src.runtime.ports import OperatorRequest
from src.runtime.wiring import (
    DEFAULT_TRANSFER_SOURCES,
    AdapterRefresher,
    CredentialGenerations,
    EngineHooks,
    LeaseCheck,
    _plan_for,
    build_engine,
)
from tests.db.conftest import EPOCH as DB_EPOCH
from tests.db.conftest import FakeConn, FakeSource
from tests.runtime.fakes import FakeClock, FakeEtcd
from tests.runtime.test_wiring import MOCK, STONEX, cfg
from tests.runtime.test_wiring_credentials import FakeSource as FakeSecrets


def test_a_new_secret_version_swaps_in_a_fresh_adapter_within_the_term() -> None:
    """5.2: the orchestrator's refresh_adapter was never wired, so a rotated StoneX password needed a new term."""
    secrets = FakeSecrets({}, version="v1")
    gens = CredentialGenerations(secrets)
    plans = [_plan_for("stonex", STONEX)]
    refresher = AdapterRefresher(lambda: plans, lambda: True, secrets, gens)
    current = StoneXAdapter(plans[0].stonex, pre_connect_check=LeaseCheck(lambda: True))  # type: ignore[arg-type]
    gens.record(current)
    assert refresher("stonex", current) is None                 # same version: unchanged
    secrets.version_id = "v2"
    fresh = refresher("stonex", current)
    assert isinstance(fresh, StoneXAdapter) and fresh is not current
    assert gens(fresh) == "sm:v2" and gens(current) == "sm:v1"  # the replacement reports the new generation
    assert refresher("stonex", fresh) is None                   # and is not rebuilt again
    assert refresher("pepperstone", fresh) is None              # no plan for it
    secrets.version_id = None
    assert refresher("stonex", fresh) is None                   # env secrets never rotate


def test_build_engine_wires_the_56_session_context(tmp_path: Path) -> None:
    secrets = FakeSecrets({}, version="v1")
    engine = build_engine(cfg(STONEX, tmp_path), environ=STONEX,
                          hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), secrets=secrets, require_keeper_thread=False))
    try:
        ctx = engine.context
        assert isinstance(ctx.refresh_adapter, AdapterRefresher)
        assert ctx.notification_settings is not None and ctx.notification_settings() == engine.settings.notifications
        assert ctx.invalidate_secrets is not None and ctx.on_fence_lost is not None
        assert ctx.transfer_sources == DEFAULT_TRANSFER_SOURCES and DEFAULT_TRANSFER_SOURCES["stonex"] is False
        assert engine.patroni is not None and engine.chrony is not None and engine.environ["BROKER_ENV"]
    finally:
        asyncio.run(engine.aclose())


def test_env_secrets_do_not_rotate_adapters_or_channels(tmp_path: Path) -> None:
    engine = build_engine(cfg(MOCK, tmp_path), environ=MOCK,
                          hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False))
    try:
        assert engine.context.refresh_adapter is None and engine.context.notification_settings is None
    finally:
        asyncio.run(engine.aclose())


def test_engine_telemetry_renders_the_contract_in_standby(tmp_path: Path) -> None:
    from src.main import engine_telemetry
    engine = build_engine(cfg(MOCK, tmp_path), environ=MOCK,
                          hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False))
    try:
        tel = engine_telemetry(engine, None)
        text = tel.registry.render()
        assert 'xtnl_node_role{node="' in text and 'role="STARTING"} 1' in text
        for line in text.splitlines():
            if not line.startswith("#"):
                assert line.split("{", 1)[0] in CONTRACT
        assert {r.name for r in tel.readers} >= {"postgres", "patroni", "redis", "files"}
    finally:
        asyncio.run(engine.aclose())


async def test_pending_operator_requests_carry_their_payload() -> None:
    conn = FakeConn()
    conn.returns_all(SQL_OPERATOR_REQUESTS_PENDING,
                     [(9, "refresh_cash_flows", "ACC", "alice", '{"v":1,"account_id":"ACC"}')])
    q = PgOperatorRequests(EngineDb(FakeSource(conn), lambda: DB_EPOCH))
    assert await q.pending(3) == [OperatorRequest(9, "refresh_cash_flows", "ACC", "alice", '{"v":1,"account_id":"ACC"}')]
    assert "r.payload" in SQL_OPERATOR_REQUESTS_PENDING
