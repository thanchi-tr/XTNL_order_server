"""build_engine() / check_composition() / main --check-config (IMPLEMENTATION_PLAN 3.6). No network: the pools,
Redis, etcd and HTTP clients are lazy, and nothing here starts the controller."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest

from src import main as entry
from src.adapters.mock import MockBrokerAdapter
from src.adapters.pepperstone.adapter import PepperstoneAdapter
from src.adapters.stonex.adapter import StoneXAdapter
from src.config import ExecutionLockedError
from src.db.epoch import Epoch
from src.runtime.config import RuntimeConfig
from src.runtime.pg_ports import LazyPgSource
from src.runtime.session import EngineSession, default_credential_generation
from src.runtime.supervisor import TaskSupervisor
from src.runtime.wiring import (
    MOCK_BROKER_VAR,
    LeaseCheck,
    WiringError,
    adapter_factory,
    broker_endpoints,
    build_engine,
    check_composition,
    load_settings,
)
from src.adapters.base import TransientBrokerError

SECRET_PG = "pg-SeCrEt-123"
SECRET_REDIS = "redis-SeCrEt-456"
SECRET_STONEX = "stonex-SeCrEt-789"
SECRET_CTRADER = "ctrader-SeCrEt-abc"
SECRET_SMTP = "smtp-SeCrEt-def"
SECRET_DEADMAN = "deadman-SeCrEt-ghi"
SECRETS = (SECRET_PG, SECRET_REDIS, SECRET_STONEX, SECRET_CTRADER, SECRET_SMTP, SECRET_DEADMAN)

BASE = {
    "BROKER_ENV": "sandbox", "XTNL_NODE_ID": "node-a",
    "XTNL_ETCD_ENDPOINTS": "http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379",
    "XTNL_DEADMAN_URL": "https://deadman.example.invalid/beat", "XTNL_DEADMAN_TOKEN": SECRET_DEADMAN,
    "ORDER_SERVER_DSN": f"postgresql://xtnl_engine:{SECRET_PG}@10.0.1.10:5432,10.0.2.10:5432/xtnl",
    "REDIS_URL": f"redis://xtnl_engine:{SECRET_REDIS}@127.0.0.1:6379/0",
}
MOCK = {**BASE, MOCK_BROKER_VAR: "1"}
STONEX = {**BASE, "STONE_X_USERNAME": "user", "STONE_X_PASS": SECRET_STONEX, "STONE_X_APP_KEY": "key"}
CTRADER = {**BASE, "CTRADER_CLIENT_ID": "cid", "CTRADER_CLIENT_SECRET": SECRET_CTRADER, "CTRADER_ACCESS_TOKEN": "at",
           "CTRADER_REFRESH_TOKEN": "rt", "CTRADER_ACCOUNT_ID": "12345"}
EMAIL = {"XTNL_ALERT_EMAIL_TO": "ops@example.invalid", "XTNL_SMTP_HOST": "smtp.example.invalid",
         "XTNL_SMTP_FROM": "xtnl@example.invalid", "XTNL_SMTP_USERNAME": "u", "XTNL_SMTP_PASSWORD": SECRET_SMTP}


def cfg(env: dict[str, str], tmp_path: Path | None = None) -> RuntimeConfig:
    c = RuntimeConfig.from_env(env)
    if tmp_path is not None:
        c = RuntimeConfig.from_env(env, spill_path=str(tmp_path / "spill.jsonl"))
    return c


# --- settings --------------------------------------------------------------------------------------------
def test_refuses_without_any_broker() -> None:
    with pytest.raises(WiringError, match="no broker adapter configured"):
        load_settings(cfg(BASE), BASE)


def test_refuses_without_storage() -> None:
    env = {k: v for k, v in MOCK.items() if k != "ORDER_SERVER_DSN"}
    with pytest.raises(WiringError, match="ORDER_SERVER_DSN"):
        load_settings(cfg(env), env)


def test_adapter_selection_from_environment() -> None:
    assert load_settings(cfg(MOCK), MOCK).adapter_ids == ("mock",)
    assert load_settings(cfg(STONEX), STONEX).adapter_ids == ("stonex",)
    assert load_settings(cfg(CTRADER), CTRADER).adapter_ids == ("pepperstone",)
    both = {**STONEX, **CTRADER}
    assert load_settings(cfg(both), both).adapter_ids == ("stonex", "pepperstone")
    only = {**both, "XTNL_ADAPTERS": "pepperstone"}
    assert load_settings(cfg(only), only).adapter_ids == ("pepperstone",)


def test_mock_needs_the_flag_and_sandbox() -> None:
    env = {**BASE, "XTNL_ADAPTERS": "mock"}
    with pytest.raises(WiringError, match=MOCK_BROKER_VAR):
        load_settings(cfg(env), env)
    live = {**MOCK, "BROKER_ENV": "live_readonly"}
    with pytest.raises(WiringError, match="live_readonly"):
        load_settings(cfg(live), live)
    bad = {**MOCK, "XTNL_ADAPTERS": "stonex,oanda"}
    with pytest.raises(WiringError, match="oanda"):
        load_settings(cfg(bad), bad)


def test_incomplete_broker_credentials_name_variables_not_values() -> None:
    env = {**BASE, "STONE_X_USERNAME": "user", "STONE_X_PASS": SECRET_STONEX}
    with pytest.raises(WiringError) as err:
        load_settings(cfg(env), env)
    assert "STONE_X_APP_KEY" in str(err.value) and SECRET_STONEX not in str(err.value)


def test_refuses_unknown_broker_env() -> None:
    env = {**MOCK, "BROKER_ENV": "live"}
    with pytest.raises(ExecutionLockedError):
        cfg(env)


def test_settings_repr_hides_secrets() -> None:
    env = {**STONEX, **CTRADER, **EMAIL}
    settings = load_settings(cfg(env), env)
    text = repr(settings) + settings.describe()
    assert not [s for s in SECRETS if s in text]
    assert "10.0.1.10:5432,10.0.2.10:5432" in settings.describe()


# --- adapters --------------------------------------------------------------------------------------------
async def test_adapter_factory_builds_fresh_read_only_adapters() -> None:
    env = {**STONEX, **CTRADER, MOCK_BROKER_VAR: "1"}
    s = load_settings(cfg(env), env)
    lease = {"ok": True}
    build = adapter_factory(s.adapters, lambda: lease["ok"])
    first, second = list(build()), list(build())
    assert [type(a) for a in first] == [StoneXAdapter, PepperstoneAdapter, MockBrokerAdapter]
    assert all(a is not b for a, b in zip(first, second))
    assert [e.adapter_id for e in broker_endpoints(s.adapters)] == ["stonex", "pepperstone"]
    assert default_credential_generation(first[1]) == "token:0"
    assert default_credential_generation(first[2]) == "default"
    for a in first + second:
        await a.close()


def test_lease_check_refuses_connect_without_a_valid_lease() -> None:
    lease = {"ok": True}
    check = LeaseCheck(lambda: lease["ok"])
    check()
    lease["ok"] = False
    with pytest.raises(TransientBrokerError):
        check()


# --- build_engine ----------------------------------------------------------------------------------------
async def test_build_engine_composes_without_connecting(tmp_path: Path) -> None:
    env = {**MOCK, **EMAIL}
    engine = build_engine(cfg(env, tmp_path), environ=env)
    try:
        assert engine.settings.adapter_ids == ("mock",)
        assert engine.controller.node_id == "node-a"
        assert isinstance(engine.control, LazyPgSource) and not engine.control.created
        assert engine.context.deadman.url == BASE["XTNL_DEADMAN_URL"]
        assert engine.context.deadman.token == SECRET_DEADMAN
        session = engine.new_session(Epoch("c", 9))
        assert isinstance(session, EngineSession)
        deps = session.init_deps(TaskSupervisor([]))
        assert deps.hlc is session.hlc and deps.ingestion is session
        assert session.adapter_ids == ("mock",)
        assert set(session.broker_links()) == {"mock"}
        assert session.ports.state_store is not None
        await session.aclose()
    finally:
        await engine.aclose()


async def test_build_engine_refuses_without_broker(tmp_path: Path) -> None:
    with pytest.raises(WiringError):
        build_engine(cfg(BASE, tmp_path), environ=BASE)


async def test_build_engine_refuses_runtime_problems(tmp_path: Path) -> None:
    env = {**MOCK, "XTNL_ETCD_ENDPOINTS": "http://a:2379"}
    with pytest.raises(WiringError, match="XTNL_ETCD_ENDPOINTS"):
        build_engine(cfg(env, tmp_path), environ=env)


async def test_check_composition(tmp_path: Path) -> None:
    assert await check_composition(cfg(MOCK, tmp_path), environ=MOCK) == []
    problems = await check_composition(cfg(BASE, tmp_path), environ=BASE)
    assert problems and "no broker adapter" in problems[0]
    both = {**STONEX, **CTRADER, **EMAIL}
    assert await check_composition(cfg(both, tmp_path), environ=both) == []


async def test_composition_never_logs_secrets(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    env = {**STONEX, **CTRADER, **EMAIL, MOCK_BROKER_VAR: "1"}
    with caplog.at_level(logging.DEBUG):
        assert await check_composition(cfg(env, tmp_path), environ=env) == []
        bad = {**env, "STONE_X_APP_KEY": ""}
        problems = await check_composition(cfg(bad, tmp_path), environ=bad)
    text = caplog.text + " ".join(problems)
    assert problems and not [s for s in SECRETS if s in text]


def test_main_check_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: Any) -> None:
    for k in list(MOCK) + ["STONE_X_USERNAME", "STONE_X_PASS", "STONE_X_APP_KEY", "CTRADER_CLIENT_ID",
                           "XTNL_ADAPTERS", "XTNL_SPILL_PATH"]:
        monkeypatch.delenv(k, raising=False)
    for k, v in MOCK.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("XTNL_SPILL_PATH", str(tmp_path / "spill.jsonl"))
    assert entry.main(["--check-config"]) == 0
    monkeypatch.delenv(MOCK_BROKER_VAR)
    assert entry.main(["--check-config"]) == entry.EX_CONFIG
    err = capsys.readouterr().err
    assert "no broker adapter" in err and SECRET_PG not in err and SECRET_REDIS not in err


async def test_dev_clock_bypass_is_sandbox_only(tmp_path: Path) -> None:
    from src.runtime.wiring import DEV_SKIP_CLOCK_VAR
    env = {**MOCK, DEV_SKIP_CLOCK_VAR: "1"}
    engine = build_engine(cfg(env, tmp_path), environ=env)
    try:
        assert await engine.probe._chrony() == 0.0
    finally:
        await engine.aclose()
    plain = build_engine(cfg(MOCK, tmp_path), environ=MOCK)
    try:
        assert plain.probe._chrony is not None and plain.probe._chrony.__name__ != "dev_clock_offset_ms"
    finally:
        await plain.aclose()
    live = {**STONEX, "BROKER_ENV": "live_readonly", DEV_SKIP_CLOCK_VAR: "1"}
    with pytest.raises(WiringError, match=DEV_SKIP_CLOCK_VAR):
        build_engine(cfg(live, tmp_path), environ=live)
