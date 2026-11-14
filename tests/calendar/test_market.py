"""SM10 MarketSession / MarketCalendar: level guards, PRE_OPEN, evidence, maintenance overlay, roll."""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from src.calendar.machine import EVIDENCE_QUIET_MS, MarketSession, SessionState, SessionTransition
from src.calendar.market import MarketCalendar
from src.calendar.schedule import MaintenanceWindow
from tests.calendar.helpers import calendar, fx, ny

S = SessionState
D = Decimal


@dataclass
class Recorder:
    transitions: list[SessionTransition] = field(default_factory=list)
    readiness: list[tuple[str, str]] = field(default_factory=list)
    rolls: list[int] = field(default_factory=list)
    flags: list[tuple[str, str, str]] = field(default_factory=list)
    maintenance: list[tuple[str, bool, int | None]] = field(default_factory=list)

    def calendar(self) -> MarketCalendar:
        return MarketCalendar(
            on_transition=self.transitions.append,
            on_readiness=lambda a, r: self.readiness.append((a, r)),
            on_roll=self.rolls.append,
            on_flag=lambda a, s, f: self.flags.append((a, s, f)),
            on_maintenance=lambda a, on, until: self.maintenance.append((a, on, until)))


# --- MarketSession level guards -------------------------------------------------------------------
def test_initial_state_is_computed_from_the_schedule() -> None:
    cases = [
        (ny(2026, 11, 11, 12), S.OPEN),
        (ny(2026, 11, 11, 17, 1), S.ROLLOVER),
        (ny(2026, 11, 14, 12), S.CLOSED),
        (ny(2026, 11, 15, 16, 49), S.CLOSED),
        (ny(2026, 11, 15, 16, 50), S.PRE_OPEN),      # now >= open - 10 min
        (ny(2026, 11, 15, 17, 0), S.OPEN),
    ]
    for now, expected in cases:
        m = MarketSession(fx())
        t = m.initialise(now)
        assert m.state is expected and t.new is expected and t.reason.startswith("init")


def test_guards_walk_closed_preopen_open_rollover_open_closed() -> None:
    m = MarketSession(fx())
    m.initialise(ny(2026, 11, 14, 12))
    seen = []
    for now in (ny(2026, 11, 15, 16, 50), ny(2026, 11, 15, 17), ny(2026, 11, 16, 17, 1),
                ny(2026, 11, 16, 17, 5), ny(2026, 11, 20, 17)):
        t = m.evaluate(now)
        assert t is not None
        seen.append(t.new)
    assert seen == [S.PRE_OPEN, S.OPEN, S.ROLLOVER, S.OPEN, S.CLOSED]
    assert m.evaluate(ny(2026, 11, 20, 18)) is None      # no change, no transition


def test_pre_open_across_dst_change() -> None:
    m = MarketSession(fx())
    m.initialise(ny(2026, 3, 7, 12))
    assert m.evaluate(ny(2026, 3, 8, 16, 49)) is None
    t = m.evaluate(ny(2026, 3, 8, 16, 50))                 # EDT: 20:50 UTC, not 21:50
    assert t is not None and t.new is S.PRE_OPEN


def test_holiday_keeps_symbol_closed() -> None:
    m = MarketSession(fx(holidays=[{"date": "2026-12-25"}]))
    m.initialise(ny(2026, 12, 24, 12))
    assert m.state is S.OPEN
    t = m.evaluate(ny(2026, 12, 25, 0, 0))
    assert t is not None and t.new is S.CLOSED
    assert m.evaluate(ny(2026, 12, 25, 16)) is None


# --- evidence beats the calendar ------------------------------------------------------------------
def _closed_weekend() -> MarketSession:
    m = MarketSession(fx())
    m.initialise(ny(2026, 11, 13, 16, 30))
    m.on_tick(D("1.1000"), D("1.1002"), ny(2026, 11, 13, 16, 59), ny(2026, 11, 13, 16, 59))   # pre-close quote
    m.evaluate(ny(2026, 11, 13, 17, 0))
    assert m.state is S.CLOSED
    return m


def test_post_close_tick_with_new_price_is_evidence() -> None:
    m = _closed_weekend()
    now = ny(2026, 11, 14, 10)
    t = m.on_tick(D("1.1010"), D("1.1012"), now - 100, now)
    assert t is not None and t.new is S.OPEN_BY_EVIDENCE and m.state is S.OPEN_BY_EVIDENCE


def test_non_evidence_ticks() -> None:
    m = _closed_weekend()
    now = ny(2026, 11, 14, 10)
    assert m.on_tick(D("1.1010"), D("1.1012"), now, now, snapshot=True) is None     # subscription snapshot
    m2 = _closed_weekend()
    assert m2.on_tick(D("1.1000"), D("1.1002"), now, now) is None                    # same price
    m3 = _closed_weekend()
    assert m3.on_tick(D("1.2"), D("1.3"), ny(2026, 11, 13, 16, 58), now) is None     # broker ts before close
    assert m.state is S.CLOSED and m2.state is S.CLOSED and m3.state is S.CLOSED


def test_open_by_evidence_returns_to_closed_after_30_min_quiet() -> None:
    m = _closed_weekend()
    t0 = ny(2026, 11, 14, 10)
    m.on_tick(D("1.1010"), D("1.1012"), t0, t0)
    assert m.evaluate(t0 + EVIDENCE_QUIET_MS - 1) is None
    t1 = t0 + 20 * 60_000
    assert m.on_tick(D("1.1020"), D("1.1022"), t1, t1) is None        # refreshes, no new transition
    assert m.evaluate(t0 + EVIDENCE_QUIET_MS + 1) is None             # still within 30 min of t1
    t = m.evaluate(t1 + EVIDENCE_QUIET_MS)
    assert t is not None and t.new is S.CLOSED and m.last_evidence_ms is None


def test_execution_is_evidence_and_scheduled_open_takes_over() -> None:
    m = _closed_weekend()
    now = ny(2026, 11, 15, 16, 45)
    t = m.on_execution(now)
    assert t is not None and t.new is S.OPEN_BY_EVIDENCE
    t1 = m.evaluate(ny(2026, 11, 15, 16, 50))                        # SM10 m11: open - 10 min -> PRE_OPEN (7.2)
    assert t1 is not None and (t1.old, t1.new) == (S.OPEN_BY_EVIDENCE, S.PRE_OPEN)
    assert m.evaluate(ny(2026, 11, 15, 16, 55)) is None
    t2 = m.evaluate(ny(2026, 11, 15, 17))
    assert t2 is not None and t2.new is S.OPEN


def test_scheduled_open_with_no_ticks_is_never_closed() -> None:
    m = MarketSession(fx())
    m.initialise(ny(2026, 11, 11, 12))
    for h in range(13, 17):
        assert m.evaluate(ny(2026, 11, 11, h)) is None
    assert m.state is S.OPEN
    assert m.on_execution(ny(2026, 11, 11, 16)) is None


# --- MarketCalendar ---------------------------------------------------------------------------------
def test_takeover_just_before_open_runs_pre_open_readiness() -> None:
    rec = Recorder()
    cal = rec.calendar()
    cal.init_from_schedule(calendar(fx(), fx(symbol="GBP/USD")), ny(2026, 11, 15, 16, 52))
    assert cal.version == 7
    assert cal.state("stonex", "EUR/USD") is S.PRE_OPEN
    assert rec.readiness == [("stonex", "pre_open")]          # once per adapter per open
    cal.tick(ny(2026, 11, 15, 16, 53))
    assert rec.readiness == [("stonex", "pre_open")]
    assert not cal.market_open("stonex")
    cal.tick(ny(2026, 11, 15, 17, 0))
    assert cal.market_open("stonex")
    assert [t.new for t in rec.transitions[-2:]] == [S.OPEN, S.OPEN]


def test_market_open_is_any_symbol_and_per_adapter() -> None:
    rec = Recorder()
    cal = rec.calendar()
    weekday_only = fx(adapter="pepper", symbol="US500", holidays=[{"date": "2026-11-11"}])
    cal.init_from_schedule(calendar(fx(), weekday_only, fx(adapter="pepper", symbol="EURUSD")),
                           ny(2026, 11, 11, 12))
    assert cal.adapters() == ["pepper", "stonex"]
    assert cal.session_state("pepper", "US500") is S.CLOSED
    assert cal.market_open("pepper") and cal.market_open("stonex")
    assert cal.price_staleness_suppressed("pepper", "US500")
    assert not cal.price_staleness_suppressed("pepper", "EURUSD")
    assert cal.state("nope", "X") is None and not cal.market_open("nope")


def test_roll_trigger_at_17_new_york() -> None:
    rec = Recorder()
    cal = rec.calendar()
    cal.init_from_schedule(calendar(fx()), ny(2026, 11, 11, 16, 59))
    cal.tick(ny(2026, 11, 11, 16, 59, 59))
    assert rec.rolls == []
    cal.tick(ny(2026, 11, 11, 17, 0))
    assert rec.rolls == [ny(2026, 11, 11, 17)]
    assert ("stonex", "roll") in rec.readiness
    assert rec.transitions[-1].new is S.ROLLOVER


def test_roll_follows_dst() -> None:
    rec = Recorder()
    cal = rec.calendar()
    cal.init_from_schedule(calendar(fx()), ny(2026, 3, 9, 16, 0))      # Monday after spring forward
    cal.tick(ny(2026, 3, 9, 17, 0))
    assert rec.rolls == [ny(2026, 3, 9, 17)] and rec.rolls[0] % 86_400_000 == 21 * 3_600_000


def test_maintenance_overlay_from_closed_with_open_during_window() -> None:
    rec = Recorder()
    start, end = ny(2026, 11, 14, 10), ny(2026, 11, 15, 18)
    cal = rec.calendar()
    cal.init_from_schedule(calendar(fx(), maintenance=[MaintenanceWindow("stonex", start, end)]),
                           ny(2026, 11, 14, 9))
    assert cal.state("stonex", "EUR/USD") is S.CLOSED
    cal.tick(start)
    assert cal.state("stonex", "EUR/USD") is S.BROKER_MAINTENANCE and cal.in_maintenance("stonex")
    assert rec.maintenance == [("stonex", True, end)]
    assert cal.maintenance_until("stonex", start) == end
    cal.tick(ny(2026, 11, 15, 16, 55))           # pre-open readiness suppressed during maintenance
    assert rec.readiness == []
    cal.tick(ny(2026, 11, 15, 17, 0))
    assert cal.session_state("stonex", "EUR/USD") is S.OPEN      # underlying follows the schedule
    assert cal.market_open("stonex")
    assert cal.price_staleness_suppressed("stonex", "EUR/USD")
    cal.tick(end)
    assert cal.state("stonex", "EUR/USD") is S.OPEN
    assert rec.maintenance[-1] == ("stonex", False, None)
    assert rec.readiness == [("stonex", "maintenance ended after an open")]
    assert cal.maintenance_until("stonex", end) is None


def test_maintenance_entered_from_open_without_open_in_window() -> None:
    rec = Recorder()
    cal = rec.calendar()
    cal.init_from_schedule(calendar(fx()), ny(2026, 11, 11, 10))
    cal.declare_maintenance(MaintenanceWindow("stonex", ny(2026, 11, 11, 11), ny(2026, 11, 11, 12)))
    cal.tick(ny(2026, 11, 11, 11))
    assert cal.state("stonex", "EUR/USD") is S.BROKER_MAINTENANCE
    cal.tick(ny(2026, 11, 11, 12))
    assert cal.state("stonex", "EUR/USD") is S.OPEN
    assert rec.readiness == []


def test_evidence_through_calendar_flags_calendar_mismatch() -> None:
    rec = Recorder()
    cal = rec.calendar()
    cal.init_from_schedule(calendar(fx()), ny(2026, 11, 14, 9))
    cal.seed_quote("stonex", "EUR/USD", D("1.1"), D("1.2"))
    now = ny(2026, 11, 14, 10)
    cal.on_tick("stonex", "EUR/USD", D("1.1"), D("1.2"), now, now)          # same as pre-close quote
    assert rec.flags == []
    cal.on_tick("stonex", "OTHER", D("9"), D("9.1"), now, now)              # unknown symbol ignored
    cal.on_tick("stonex", "EUR/USD", D("1.15"), D("1.25"), now, now)
    assert rec.flags == [("stonex", "EUR/USD", "calendar_mismatch")]
    assert cal.state("stonex", "EUR/USD") is S.OPEN_BY_EVIDENCE and cal.market_open("stonex")
    assert cal.latest_open_ms("stonex", now) == now
    cal.on_execution("stonex", "EUR/USD", now + 1000)
    assert len(rec.flags) == 1


def test_latest_open_ms_is_scheduled_open() -> None:
    cal = MarketCalendar()
    cal.init_from_schedule(calendar(fx()), ny(2026, 11, 16, 9))
    assert cal.latest_open_ms("stonex", ny(2026, 11, 16, 9)) == ny(2026, 11, 15, 17)
    assert cal.tick(ny(2026, 11, 16, 9)) == []
    assert MarketCalendar().tick(0) == []


async def test_run_loop_ticks_and_stops() -> None:
    import asyncio
    cal = MarketCalendar()
    cal.init_from_schedule(calendar(fx()), ny(2026, 11, 14, 9))
    stop = asyncio.Event()
    clock = iter([ny(2026, 11, 15, 16, 55), ny(2026, 11, 15, 17, 1)] + [ny(2026, 11, 15, 17, 2)] * 100)
    stamps: list[int] = []

    async def stopper() -> None:
        while len(stamps) < 2:
            await asyncio.sleep(0.001)
        stop.set()

    await asyncio.gather(cal.run(stop, clock_ms=lambda: next(clock), interval_s=0.001,
                                 stamp=lambda: stamps.append(1)), stopper())
    assert cal.state("stonex", "EUR/USD") is S.OPEN


async def test_scheduled_calendar_init_for_leader_init_step_5() -> None:
    from src.calendar.market import ScheduledCalendarInit
    from src.calendar.schedule import CalendarSchedule

    class Loader:
        async def load(self) -> CalendarSchedule:
            return calendar(fx(), version=12)

    cal = MarketCalendar()
    assert await ScheduledCalendarInit(Loader(), cal).init_calendar(ny(2026, 11, 11, 12)) == 12
    assert cal.version == 12 and cal.market_open("stonex")


# ---- 4.9: page when an adapter has no calendar sessions --------------------------------------------------
def test_adapter_without_sessions_pages_at_init_and_again_after_each_roll() -> None:
    from src.calendar.market import NO_SESSIONS_FLAG

    rec = Recorder()
    cal = MarketCalendar(on_flag=lambda a, s, f: rec.flags.append((a, s, f)),
                         expected_adapters=lambda: ["stonex", "pepperstone"])
    cal.init_from_schedule(calendar(fx()), ny(2026, 11, 11, 12))
    assert rec.flags == [("pepperstone", "*", NO_SESSIONS_FLAG)]
    assert cal.latest_open_ms("pepperstone", ny(2026, 11, 11, 12)) is None      # the gate denies it
    cal.tick(ny(2026, 11, 11, 13))
    assert len(rec.flags) == 1                                                   # once per day, not per tick
    cal.tick(ny(2026, 11, 11, 17, 1))                                            # the 17:00 roll
    assert rec.flags == [("pepperstone", "*", NO_SESSIONS_FLAG)] * 2


async def test_adapter_without_sessions_pages_on_its_first_tick_and_via_the_init_list() -> None:
    from src.calendar.market import ScheduledCalendarInit
    from src.calendar.schedule import CalendarSchedule

    seen: list[str] = []
    cal = MarketCalendar(on_no_sessions=seen.append)
    cal.init_from_schedule(calendar(fx()), ny(2026, 11, 11, 12))
    assert seen == []
    cal.on_tick("stonex", "GBP/USD", D("1.3"), D("1.3001"), ny(2026, 11, 11, 12), ny(2026, 11, 11, 12))
    assert seen == []                                                             # its adapter has sessions
    for _ in range(3):
        cal.on_tick("ctrader", "XAUUSD", D("2000"), D("2000.5"), ny(2026, 11, 11, 12), ny(2026, 11, 11, 12))
    cal.on_execution("ctrader", "XAUUSD", ny(2026, 11, 11, 12))
    assert seen == ["ctrader"]                                                    # once

    class Loader:
        async def load(self) -> CalendarSchedule:
            return calendar(fx(), version=13)

    seen2: list[str] = []
    cal2 = MarketCalendar(on_no_sessions=seen2.append)
    await ScheduledCalendarInit(Loader(), cal2, adapters=("stonex", "ig")).init_calendar(ny(2026, 11, 11, 12))
    assert seen2 == ["ig"] and cal2.adapters_without_sessions(["stonex", "ig"]) == ["ig"]


def test_24x7_adapter_has_a_latest_open_through_the_calendar() -> None:
    from src.calendar.schedule import symbol_schedule_from_json

    btc = symbol_schedule_from_json("crypto", "BTC", "UTC", {"sessions": [
        {"open": {"day": "MON", "time": "00:00"}, "close": {"day": "MON", "time": "00:00"}}]})
    cal = MarketCalendar()
    cal.init_from_schedule(calendar(fx(), btc), ny(2026, 11, 11, 12))
    assert cal.market_open("crypto") and cal.latest_open_ms("crypto", ny(2026, 11, 11, 12)) is not None
