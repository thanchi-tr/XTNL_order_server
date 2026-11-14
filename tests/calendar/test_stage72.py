"""IMPLEMENTATION_PLAN 7.2 calendar (operations.md §10, SM10): no readiness (reconnect, liveness page) inside a
maintenance window and a re-run at its end; only subscribed symbols decide an adapter's OPEN; a post-close tick is
evidence only when its price differs from the last pre-close quote; OPEN_BY_EVIDENCE -> PRE_OPEN at open - 10 min
(m11); BROKER_MAINTENANCE marks as CLOSED."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from decimal import Decimal

from src.calendar.machine import MarketSession, SessionState
from src.calendar.market import MarketCalendar
from src.calendar.readiness import AFTER_MAINTENANCE_WAIT_S, ReadinessDispatcher, readiness_check
from src.calendar.schedule import MaintenanceWindow, symbol_schedule_from_json
from tests.calendar.helpers import calendar, fx, ny
from tests.calendar.test_readiness import FakeActions

S = SessionState
D = Decimal


@dataclass
class Rec:
    readiness: list[tuple[str, str]] = field(default_factory=list)
    flags: list[tuple[str, str, str]] = field(default_factory=list)

    def calendar(self, **kw: object) -> MarketCalendar:
        return MarketCalendar(on_readiness=lambda a, r: self.readiness.append((a, r)),
                              on_flag=lambda a, s, f: self.flags.append((a, s, f)), **kw)  # type: ignore[arg-type]


# ---------------------------------------------------------------- maintenance: no readiness inside, re-run at end
def test_a_roll_inside_a_maintenance_window_defers_its_readiness_to_the_window_end() -> None:
    rec = Rec()
    cal = rec.calendar()
    cal.init_from_schedule(calendar(fx()), ny(2026, 11, 11, 15))                # Wednesday
    cal.declare_maintenance(MaintenanceWindow("stonex", ny(2026, 11, 11, 16), ny(2026, 11, 11, 16, 30)))
    cal.declare_maintenance(MaintenanceWindow("stonex", ny(2026, 11, 11, 16, 30), ny(2026, 11, 11, 18)))
    cal.tick(ny(2026, 11, 11, 16))
    cal.tick(ny(2026, 11, 11, 17, 0))                                             # the 17:00 NY roll
    cal.tick(ny(2026, 11, 11, 17, 30))
    assert rec.readiness == []                                    # no reconnect / not-STREAMING page in the window
    cal.tick(ny(2026, 11, 11, 18))
    assert len(rec.readiness) == 1 and rec.readiness[0][1].startswith("maintenance ended")
    cal.tick(ny(2026, 11, 11, 18, 1))
    assert len(rec.readiness) == 1                                # once


def test_a_window_starting_on_the_roll_tick_defers_it_too_and_a_roll_outside_runs_at_once() -> None:
    rec = Rec()
    cal = rec.calendar()
    cal.init_from_schedule(calendar(fx(), fx(adapter="pepper", symbol="EURUSD")), ny(2026, 11, 10, 16))
    cal.declare_maintenance(MaintenanceWindow("stonex", ny(2026, 11, 10, 17), ny(2026, 11, 10, 17, 2)))
    cal.tick(ny(2026, 11, 10, 17))
    assert rec.readiness == [("pepper", "roll")]                  # the other adapter is checked as usual
    cal.tick(ny(2026, 11, 10, 17, 2))
    assert rec.readiness == [("pepper", "roll"), ("stonex", "maintenance ended after a roll")]


def test_a_window_with_no_open_or_roll_runs_no_readiness() -> None:
    rec = Rec()
    cal = rec.calendar()
    cal.init_from_schedule(calendar(fx()), ny(2026, 11, 14, 9))                  # Saturday, closed
    cal.declare_maintenance(MaintenanceWindow("stonex", ny(2026, 11, 14, 10), ny(2026, 11, 14, 12)))
    cal.tick(ny(2026, 11, 14, 10))
    cal.tick(ny(2026, 11, 14, 12))
    assert rec.readiness == []


async def test_the_end_of_window_readiness_waits_for_the_reconnect_before_it_checks() -> None:
    a = FakeActions(streaming=False)
    slept: list[float] = []

    async def sleep(s: float) -> None:
        slept.append(s)
        if len(slept) == 3:
            a.streaming = True                                     # the reconnect reached STREAMING after 3 s

    d = ReadinessDispatcher(a, sleep=sleep)
    d.request("stonex", "maintenance ended after a roll")
    report = await d.run_once()
    assert report.ok and a.pages == [] and slept == [1.0, 1.0, 1.0]
    assert d.wait_for("maintenance ended after an open") == AFTER_MAINTENANCE_WAIT_S and d.wait_for("roll") == 0


async def test_a_reconnect_that_never_comes_still_pages_after_the_bounded_wait() -> None:
    a = FakeActions(streaming=False)
    slept: list[float] = []

    async def sleep(s: float) -> None:
        slept.append(s)

    r = await readiness_check(a, "stonex", "maintenance ended after an open", reconnect_wait_s=2.5, sleep=sleep)
    assert not r.ok and slept == [1.0, 1.0, 0.5] and a.pages and a.pages[0][0] == "critical"
    slept.clear()
    await readiness_check(FakeActions(streaming=False), "stonex", "pre_open", sleep=sleep)
    assert slept == []                                             # a normal check never waits


async def test_one_adapters_wait_never_delays_another_adapters_check() -> None:
    gate = asyncio.Event()

    class Slow(FakeActions):
        async def reset_backoff_and_reconnect(self, adapter_id: str) -> None:
            self.calls.append(f"reconnect:{adapter_id}")
            if adapter_id == "stonex":
                await gate.wait()

    a = Slow()
    d = ReadinessDispatcher(a)
    stop = asyncio.Event()
    runner = asyncio.create_task(d.run(stop))
    d.request("stonex", "pre_open")
    d.request("pepper", "pre_open")
    for _ in range(20):
        await asyncio.sleep(0)
    assert [r.adapter_id for r in d.reports] == ["pepper"]        # stonex still reconnecting
    gate.set()
    for _ in range(20):
        await asyncio.sleep(0)
    stop.set()
    await asyncio.wait_for(runner, 1)
    assert sorted(r.adapter_id for r in d.reports) == ["pepper", "stonex"]


# ---------------------------------------------------------------- only subscribed symbols decide OPEN
def _always(symbol: str) -> object:
    return symbol_schedule_from_json("stonex", symbol, "UTC", {"sessions": [
        {"open": {"day": "MON", "time": "00:00"}, "close": {"day": "MON", "time": "00:00"}}]})


def test_only_currently_subscribed_symbols_decide_the_adapters_market() -> None:
    subscribed = {"stonex": {"EUR/USD"}}
    cal = MarketCalendar(subscribed_symbols=lambda a: subscribed.get(a, set()))
    sat = ny(2026, 11, 14, 12)
    cal.init_from_schedule(calendar(fx(), _always("BTC")), sat)                     # type: ignore[arg-type]
    assert cal.session_state("stonex", "BTC") is S.OPEN and cal.session_state("stonex", "EUR/USD") is S.CLOSED
    assert not cal.market_open("stonex")                     # BTC was held once: it no longer keeps it open
    subscribed["stonex"] = {"EUR/USD", "BTC"}
    assert cal.market_open("stonex")
    subscribed["stonex"] = {"EUR/GBP"}                        # nothing scheduled subscribed: every symbol decides
    assert cal.market_open("stonex")
    subscribed["stonex"] = set()
    assert cal.market_open("stonex")


def test_a_failing_subscription_provider_falls_back_to_every_scheduled_symbol() -> None:
    def broken(adapter_id: str) -> set[str]:
        raise RuntimeError("orchestrator gone")

    cal = MarketCalendar(subscribed_symbols=broken)
    cal.init_from_schedule(calendar(fx(), _always("BTC")), ny(2026, 11, 14, 12))     # type: ignore[arg-type]
    assert cal.market_open("stonex")
    assert MarketCalendar().market_open("stonex") is False                          # nothing loaded


# ---------------------------------------------------------------- evidence vs the last pre-close quote
def test_a_takeover_while_closed_never_opens_on_a_resent_closing_quote() -> None:
    m = MarketSession(fx())
    sat = ny(2026, 11, 14, 10)
    m.initialise(sat)                                              # no quote seen, nothing seeded
    assert m.on_tick(D("1.1000"), D("1.1002"), sat, sat) is None and m.state is S.CLOSED   # becomes the reference
    assert m.on_tick(D("1.1000"), D("1.1002"), sat + 1_000, sat + 1_000) is None
    t = m.on_tick(D("1.1010"), D("1.1012"), sat + 2_000, sat + 2_000)
    assert t is not None and t.new is S.OPEN_BY_EVIDENCE


def test_seeded_last_clean_mark_is_the_reference_and_post_close_ticks_never_move_it() -> None:
    rec = Rec()
    cal = rec.calendar()
    sat = ny(2026, 11, 14, 10)
    cal.init_from_schedule(calendar(fx()), sat)
    fri_close = ny(2026, 11, 13, 17)
    assert cal.seed_quotes([("stonex", "EUR/USD", D("1.1000"), D("1.1002"), fri_close - 5_000),
                            ("stonex", "NOPE", D("1"), D("2"), fri_close - 5_000)], sat) == 1
    cal.on_tick("stonex", "EUR/USD", D("1.1000"), D("1.1002"), sat, sat)             # the closing quote again
    cal.on_tick("stonex", "EUR/USD", D("1.1010"), D("1.1012"), sat, sat, snapshot=True)
    assert rec.flags == [] and cal.state("stonex", "EUR/USD") is S.CLOSED
    cal.on_tick("stonex", "EUR/USD", D("1.1000"), D("1.1002"), sat + 1, sat + 1)    # still the pre-close quote
    assert rec.flags == []
    cal.on_tick("stonex", "EUR/USD", D("1.1010"), D("1.1012"), sat + 2, sat + 2)
    assert rec.flags == [("stonex", "EUR/USD", "calendar_mismatch")]


def test_a_seed_stamped_after_the_close_is_not_a_pre_close_quote() -> None:
    m = MarketSession(fx())
    sat = ny(2026, 11, 14, 10)
    m.initialise(sat)
    m.seed_quote(D("1.2"), D("1.2002"), sat - 1_000, sat)          # after Friday's close: ignored
    assert m.preclose_quote is None
    m.seed_quote(D("1.1"), D("1.1002"), ny(2026, 11, 13, 16, 59), sat)
    assert m.preclose_quote == (D("1.1"), D("1.1002"))
    m.seed_quote(D("1.3"), D("1.3002"), ny(2026, 11, 13, 16, 58), sat)   # a known reference is never replaced
    assert m.preclose_quote == (D("1.1"), D("1.1002"))


def test_the_last_quote_while_open_is_the_reference_after_the_close() -> None:
    m = MarketSession(fx())
    m.initialise(ny(2026, 11, 13, 16, 30))
    m.on_tick(D("1.1000"), D("1.1002"), ny(2026, 11, 13, 16, 58), ny(2026, 11, 13, 16, 58))
    m.on_tick(D("1.1005"), D("1.1007"), ny(2026, 11, 13, 16, 59), ny(2026, 11, 13, 16, 59))
    m.evaluate(ny(2026, 11, 13, 17))
    now = ny(2026, 11, 14, 10)
    assert m.on_tick(D("1.1005"), D("1.1007"), now, now) is None             # the closing quote re-sent
    assert m.on_tick(D("1.1000"), D("1.1002"), now + 1, now + 1) is not None  # differs from the last pre-close


# ---------------------------------------------------------------- SM10 m11
def test_open_by_evidence_moves_to_pre_open_at_open_minus_10_min() -> None:
    rec = Rec()
    cal = rec.calendar()
    cal.init_from_schedule(calendar(fx()), ny(2026, 11, 15, 16, 40))             # Sunday, before the open
    cal.on_execution("stonex", "EUR/USD", ny(2026, 11, 15, 16, 45))
    assert cal.state("stonex", "EUR/USD") is S.OPEN_BY_EVIDENCE
    cal.tick(ny(2026, 11, 15, 16, 50))
    assert cal.state("stonex", "EUR/USD") is S.PRE_OPEN
    assert ("stonex", "pre_open") in rec.readiness
    cal.tick(ny(2026, 11, 15, 17))
    assert cal.state("stonex", "EUR/USD") is S.OPEN


# ---------------------------------------------------------------- BROKER_MAINTENANCE -> CLOSED for marks
def test_mark_session_maps_maintenance_to_closed() -> None:
    cal = MarketCalendar()
    assert cal.mark_session("stonex", "EUR/USD") == "OPEN"                        # no calendar: open-market cases
    wed = ny(2026, 11, 11, 12)
    cal.init_from_schedule(calendar(fx()), wed)
    assert cal.mark_session("stonex", "EUR/USD") == "OPEN"
    assert cal.mark_session("stonex", "UNKNOWN") == "OPEN"
    cal.declare_maintenance(MaintenanceWindow("stonex", wed + 1_000, wed + 60_000))
    cal.tick(wed + 1_000)
    assert cal.state("stonex", "EUR/USD") is S.BROKER_MAINTENANCE
    assert cal.mark_session("stonex", "EUR/USD") == "CLOSED"
    cal.tick(ny(2026, 11, 11, 17, 1))
    assert cal.mark_session("stonex", "EUR/USD") == "ROLLOVER"
    cal.tick(ny(2026, 11, 14, 12))
    assert cal.mark_session("stonex", "EUR/USD") == "CLOSED"
