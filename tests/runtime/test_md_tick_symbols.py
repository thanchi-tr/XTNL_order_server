"""S05 (design PR5, IN-8, IN-11): tick-only symbols in the session. They never decide SM10's adapter OPEN, are never
SM10 evidence, and get a broker schedule through `md_schedule_symbols` (empty until the market-data layer is wired)."""
from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator, Collection, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

import fakeredis
import pytest

from src.adapters.sessions import fx_default_schedule
from src.calendar.machine import OPEN_STATES
from src.calendar.schedule import CalendarSchedule, symbol_schedule_from_json
from src.db.epoch import Epoch
from src.db.market_sessions import MarketSessionRow, SaveResult
from src.dto import PriceDTO
from src.runtime.pg_ports import PgLeaderPorts
from src.runtime.ports import LeaderPorts
from src.runtime.session import EngineSession, SessionContext
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import ADAPTER, FAST_TIMINGS, NODE, FakeLease, fixture, mock_adapter, wall_ms
from tests.runtime.test_session_56 import ny_ms

EPOCH = Epoch("c", 7)
#: A weekend-only market (synthetic): OPEN on Saturday while the FX default schedule is CLOSED.
WEEKEND = {"sessions": [{"open": {"day": "SAT", "time": "00:00"}, "close": {"day": "SAT", "time": "23:59"}}]}


async def run_id(_: Any) -> str:
    return "run-s05"


class NoSessions:
    """A MarketSessionStore that is never reached in these tests (the refresher is only inspected)."""

    async def latest(self) -> tuple[int, list[MarketSessionRow]]:
        return 0, []

    async def save(self, rows: Sequence[MarketSessionRow], *, adapters: Collection[str]) -> SaveResult:
        raise AssertionError("not saved in this test")


@pytest.fixture
async def session(tmp_path: Path) -> AsyncIterator[EngineSession]:
    redis = fakeredis.FakeAsyncRedis()
    backend = MemoryBackend(redis, run_id, EPOCH.token)

    def ports(e: Epoch) -> LeaderPorts:
        base = backend.ports(e)
        fields = {f.name: getattr(base, f.name) for f in dataclasses.fields(LeaderPorts)}
        return PgLeaderPorts(**fields, market_sessions=NoSessions())

    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=ports,
                         adapters=lambda: [mock_adapter(fixture(wall_ms()))], lease=FakeLease(), run_id_reader=run_id,
                         exit_fn=lambda _r: None, timings=FAST_TIMINGS)
    s = EngineSession(ctx, EPOCH)
    try:
        yield s
    finally:
        await s.aclose(1.0)
        await redis.aclose()


def _calendar(s: EngineSession, now_ms: int) -> None:
    sched = {(ADAPTER, "EURUSD"): symbol_schedule_from_json(ADAPTER, "EURUSD", "America/New_York",
                                                            fx_default_schedule()),
             (ADAPTER, "BTCUSD"): symbol_schedule_from_json(ADAPTER, "BTCUSD", "America/New_York", WEEKEND)}
    s.calendar.init_from_schedule(CalendarSchedule(5, sched), now_ms)


def _hold(s: EngineSession, held: set[str], tick_only: set[str]) -> None:
    """The orchestrator's view after a prices stream started: `held` required, `tick_only` asked for apart."""
    rt = s.orchestrator.runtime(ADAPTER)
    rt.tick_symbols = set(held)
    s.orchestrator.set_md_tick_symbols(ADAPTER, sorted(tick_only))
    rt.md_subscribed = set(tick_only)


async def test_a_tick_only_symbol_open_while_every_held_symbol_is_closed_leaves_market_open(
        session: EngineSession) -> None:
    """PR5: SM10's adapter OPEN follows the held (subscribed) symbols only; a tick-only symbol with longer hours
    never keeps the adapter OPEN (dead-man market_open, liveness pages, broker-grounds abdication)."""
    saturday = ny_ms(2026, 11, 14, 12)
    _calendar(session, saturday)
    _hold(session, {"EURUSD"}, set())
    before = session.calendar.market_open(ADAPTER)
    _hold(session, {"EURUSD"}, {"BTCUSD"})
    assert session.calendar.state(ADAPTER, "BTCUSD") in OPEN_STATES          # scheduled and OPEN on Saturday
    assert session.calendar.state(ADAPTER, "EURUSD") not in OPEN_STATES
    assert session._wanted_symbols(ADAPTER) == ["EURUSD"]
    assert session.calendar.market_open(ADAPTER) is before is False
    assert not session._market_open_for_liveness(ADAPTER)
    _hold(session, {"EURUSD", "BTCUSD"}, set())                              # held: it would decide (control)
    assert session.calendar.market_open(ADAPTER) is True


async def test_a_tick_only_symbols_tick_is_never_sm10_evidence(session: EngineSession) -> None:
    seen: list[str] = []
    session.calendar.version = 1
    session.calendar.on_tick = (lambda a, s, b, k, ts, now, snapshot=False:  # type: ignore[method-assign]
                                seen.append(s))
    _hold(session, {"EURUSD"}, {"BTCUSD"})
    for sym in ("BTCUSD", "EURUSD"):
        session._on_price_tick(PriceDTO(adapter_id=ADAPTER, symbol=sym, bid=Decimal(1), ask=Decimal(1),
                                        spread_pips=None, timestamp_ms=1), False)
    assert seen == ["EURUSD"]


async def test_tick_only_and_prewarm_symbols_get_a_schedule(session: EngineSession) -> None:
    """IN-11: the broker schedule is fetched for the wanted symbols plus `md_schedule_symbols()` (empty until S32);
    SM10's adapter OPEN still follows the wanted symbols only."""
    assert session.schedule_refresher is not None
    assert session.schedule_refresher._wanted == session._schedule_symbols
    _hold(session, {"EURUSD"}, {"BTCUSD"})
    assert session._schedule_symbols(ADAPTER) == ["EURUSD"]                  # default: nothing extra
    session.md_schedule_symbols = lambda a: ["BTCUSD", "US500"] if a == ADAPTER else []
    assert session._schedule_symbols(ADAPTER) == ["BTCUSD", "EURUSD", "US500"]
    assert session._wanted_symbols(ADAPTER) == ["EURUSD"]


async def test_a_failing_md_schedule_symbols_keeps_the_held_schedules(session: EngineSession,
                                                                     caplog: pytest.LogCaptureFixture) -> None:
    def broken(adapter_id: str) -> list[str]:
        raise RuntimeError("market-data layer bug")

    _hold(session, {"EURUSD"}, set())
    session.md_schedule_symbols = broken
    with caplog.at_level("ERROR"):
        assert session._schedule_symbols(ADAPTER) == ["EURUSD"]
    assert any("md_schedule_symbols failed" in r.getMessage() for r in caplog.records)
