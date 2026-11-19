"""This week's compile: ISO week window, single flight, force, cache key / TTL in Redis database 2 only, and the
states latest reports (missing / compiling / ready / failed)."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime

import fakeredis
import pytest

from src.api.pinescript import (CACHE_TTL_S, COMPILING_TTL_S, FAILED_TTL_S, CompileError, PinescriptService,
                                iso_week, week_window)

from tests.api.conftest import NOW, Clock, FakeCompiler

MONDAY = int(datetime(2026, 11, 9, tzinfo=UTC).timestamp() * 1000)


def ms(*args: int) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)   # type: ignore[misc]


@pytest.fixture
def server() -> fakeredis.FakeServer:
    return fakeredis.FakeServer()


def service(server: fakeredis.FakeServer, compiler: FakeCompiler, clock: Clock | None = None,
            timeout_s: float = 5.0) -> tuple[PinescriptService, fakeredis.FakeAsyncRedis]:
    r = fakeredis.FakeAsyncRedis(server=server, db=2)
    return PinescriptService(r, compiler, clock=clock or Clock(), timeout_s=timeout_s), r


async def settle(svc: PinescriptService) -> None:
    for _ in range(50):
        await asyncio.sleep(0)
        if not any(svc.running(w) for w in ("2026-W46", "2026-W47", "2026-W53")):
            return
    await asyncio.sleep(0.05)


@pytest.mark.parametrize("at,label,monday", [
    (NOW, "2026-W46", MONDAY),
    (MONDAY, "2026-W46", MONDAY),                                          # Monday 00:00:00 starts the week
    (MONDAY - 1, "2026-W45", ms(2026, 11, 2)),                             # Sunday 23:59:59.999 is the last one
    (ms(2027, 1, 1, 9), "2026-W53", ms(2026, 12, 28)),                     # ISO year differs from calendar year
    (ms(2027, 1, 4), "2027-W01", ms(2027, 1, 4)),
])
def test_iso_week(at: int, label: str, monday: int) -> None:
    assert iso_week(at) == (label, monday)


def test_week_window_runs_from_monday_to_now() -> None:
    week, window = week_window(NOW)
    assert week == "2026-W46" and (window.since_ms, window.until_ms) == (MONDAY, NOW)


async def test_compile_caches_in_db_2_only_with_an_8_day_ttl(server: fakeredis.FakeServer) -> None:
    comp = FakeCompiler()
    svc, r2 = service(server, comp)
    assert await svc.start(force=False) == {"week": "2026-W46", "status": "compiling"}
    await settle(svc)
    raw = await r2.get("pinescript:week:2026-W46")
    doc = json.loads(raw)
    assert doc["status"] == "ready" and doc["week"] == "2026-W46"
    assert doc["window"] == {"from": "2026-11-09T00:00:00Z", "to": "2026-11-14T12:00:00Z"}
    assert [s["symbol"] for s in doc["scripts"]] == ["EURUSD", "AUDUSD"]
    ttl = await r2.ttl("pinescript:week:2026-W46")
    assert CACHE_TTL_S - 5 <= ttl <= CACHE_TTL_S == 8 * 86_400
    assert comp.calls[0].since_ms == MONDAY and comp.calls[0].until_ms == NOW
    r0 = fakeredis.FakeAsyncRedis(server=server, db=0)
    assert await r0.keys("*") == []                                        # never the engine's database
    assert sorted(await r2.keys("*")) == [b"pinescript:status:2026-W46", b"pinescript:week:2026-W46"]


async def test_single_flight_and_force(server: fakeredis.FakeServer) -> None:
    gate = asyncio.Event()
    comp = FakeCompiler(gate=gate)
    svc, r2 = service(server, comp)
    first = await svc.start(force=False)
    await asyncio.sleep(0)
    again = await svc.start(force=False)
    forced = await svc.start(force=True)                    # a compile already running is reused
    assert first == again == forced == {"week": "2026-W46", "status": "compiling"}
    assert len(comp.calls) == 1
    state = json.loads(await r2.get("pinescript:status:2026-W46"))
    assert state["status"] == "compiling"
    assert 0 < await r2.ttl("pinescript:status:2026-W46") <= COMPILING_TTL_S
    gate.set()
    await settle(svc)
    assert await svc.start(force=False) == {"week": "2026-W46", "status": "ready"}   # cached: no new compile
    assert len(comp.calls) == 1
    assert await svc.start(force=True) == {"week": "2026-W46", "status": "compiling"}
    await settle(svc)
    assert len(comp.calls) == 2


async def test_latest_states(server: fakeredis.FakeServer) -> None:
    gate = asyncio.Event()
    comp = FakeCompiler(gate=gate)
    svc, _ = service(server, comp)
    missing = await svc.latest()
    assert missing == {"week": "2026-W46", "status": "missing", "generated_at": None,
                       "window": {"from": "2026-11-09T00:00:00Z", "to": "2026-11-14T12:00:00Z"}}
    await svc.start(force=False)
    compiling = await svc.latest()
    assert compiling["status"] == "compiling" and "scripts" not in compiling
    gate.set()
    await settle(svc)
    ready = await svc.latest()
    assert ready["status"] == "ready" and ready["generated_at"] == "2026-11-14T12:00:00Z"
    assert ready["scripts"] == [{"symbol": "EURUSD", "text": '//@version=6\nindicator("E")'},
                                {"symbol": "AUDUSD", "text": "//@version=6"}]
    assert "generated_ms" not in ready


@pytest.mark.parametrize("error,code", [(CompileError("db_unavailable"), "db_unavailable"),
                                        (CompileError("db_query_failed"), "db_query_failed"),
                                        (RuntimeError("password=hunter2 at 10.0.0.1"), "compile_failed")])
async def test_failed_compile_reports_a_short_code_only(server: fakeredis.FakeServer, error: BaseException,
                                                        code: str, caplog: pytest.LogCaptureFixture) -> None:
    svc, r2 = service(server, FakeCompiler(error=error))
    with caplog.at_level(logging.INFO, logger="xtnl.api"):
        await svc.start(force=False)
        await settle(svc)
    latest = await svc.latest()
    assert latest["status"] == "failed" and latest["error"] == code and "scripts" not in latest
    assert 0 < await r2.ttl("pinescript:status:2026-W46") <= FAILED_TTL_S
    stored = b"".join([await r2.get(k) or b"" for k in await r2.keys("*")])
    assert b"hunter2" not in stored
    logged = " ".join(f"{r.getMessage()} {vars(r)}" for r in caplog.records)
    assert "hunter2" not in logged and code in logged


async def test_timeout_is_reported(server: fakeredis.FakeServer) -> None:
    svc, _ = service(server, FakeCompiler(gate=asyncio.Event()), timeout_s=0.05)
    await svc.start(force=False)
    await asyncio.sleep(0.2)
    assert (await svc.latest())["error"] == "timeout"


async def test_failed_recompile_after_a_ready_one_reports_failed(server: fakeredis.FakeServer) -> None:
    clock = Clock()
    comp = FakeCompiler()
    svc, _ = service(server, comp, clock)
    await svc.start(force=False)
    await settle(svc)
    clock.ms += 60_000
    comp.error = CompileError("db_unavailable")
    await svc.start(force=True)
    await settle(svc)
    assert (await svc.latest())["status"] == "failed"
    clock.ms += 60_000
    comp.error = None
    await svc.start(force=True)
    await settle(svc)
    latest = await svc.latest()
    assert latest["status"] == "ready" and latest["generated_at"] == "2026-11-14T12:02:00Z"


async def test_compiling_left_by_a_crashed_process_is_reported_as_interrupted(server: fakeredis.FakeServer) -> None:
    svc, r2 = service(server, FakeCompiler())
    await r2.set("pinescript:status:2026-W46", json.dumps({"status": "compiling", "started_ms": NOW}),
                 ex=COMPILING_TTL_S)
    assert (await svc.latest())["error"] == "interrupted"
    assert (await svc.start(force=False))["status"] == "compiling"          # a new request compiles again
    await settle(svc)
    assert (await svc.latest())["status"] == "ready"


async def test_next_week_starts_missing(server: fakeredis.FakeServer) -> None:
    clock = Clock()
    svc, _ = service(server, FakeCompiler(), clock)
    await svc.start(force=False)
    await settle(svc)
    clock.ms = ms(2026, 11, 16, 0, 0, 1)
    latest = await svc.latest()
    assert latest["week"] == "2026-W47" and latest["status"] == "missing"


async def test_close_cancels_a_running_compile(server: fakeredis.FakeServer) -> None:
    svc, _ = service(server, FakeCompiler(gate=asyncio.Event()))
    await svc.start(force=False)
    await svc.close()
    assert not svc.running("2026-W46")
