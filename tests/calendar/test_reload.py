"""IMPLEMENTATION_PLAN 6.3 calendar: a schedule reload keeps OPEN_BY_EVIDENCE, maintenance and readiness state;
one pre-acknowledged maintenance notice; declared windows; no-calendar-sessions page at init and after each
reload (operations.md §10, oversee-rules.md §8)."""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pytest

from src.calendar.machine import EVIDENCE_QUIET_MS, SessionState, SessionTransition
from src.calendar.market import NO_SESSIONS_FLAG, MarketCalendar
from src.calendar.schedule import MaintenanceWindow, ScheduleError, maintenance_window_from_json
from src.calendar.schedule import symbol_schedule_from_json
from tests.calendar.helpers import calendar, fx, ny

S = SessionState
D = Decimal
KEY = ("stonex", "EUR/USD")


@dataclass
class Rec:
    transitions: list[SessionTransition] = field(default_factory=list)
    readiness: list[tuple[str, str]] = field(default_factory=list)
    flags: list[tuple[str, str, str]] = field(default_factory=list)
    maintenance: list[tuple[str, bool, int | None]] = field(default_factory=list)
    notices: list[tuple[str, int, str]] = field(default_factory=list)

    def calendar(self, **kw: Any) -> MarketCalendar:
        return MarketCalendar(
            on_transition=self.transitions.append, on_readiness=lambda a, r: self.readiness.append((a, r)),
            on_flag=lambda a, s, f: self.flags.append((a, s, f)),
            on_maintenance=lambda a, on, until: self.maintenance.append((a, on, until)),
            on_maintenance_notice=lambda a, until, why: self.notices.append((a, until, why)), **kw)


def test_reload_keeps_open_by_evidence_and_its_evidence_clock() -> None:
    rec = Rec()
    cal = rec.calendar()
    sat = ny(2026, 11, 14, 12)
    cal.init_from_schedule(calendar(fx(), version=1), sat)
    cal.seed_quote("stonex", "EUR/USD", D("1.0990"), D("1.0992"))              # the pre-close quote (7.2)
    cal.on_tick("stonex", "EUR/USD", D("1.1"), D("1.1002"), sat + 1_000, sat + 1_000)
    assert cal.state(*KEY) is S.OPEN_BY_EVIDENCE
    opened = cal.latest_open_ms("stonex", sat + 1_000)
    assert opened == sat + 1_000
    rec.transitions.clear()
    out = cal.init_from_schedule(calendar(fx(), version=2), sat + 60_000)
    assert out == [] and rec.transitions == []                    # nothing moved: no transition at all
    assert cal.version == 2 and cal.state(*KEY) is S.OPEN_BY_EVIDENCE
    assert cal.latest_open_ms("stonex", sat + 60_000) == opened   # the gate's latest open is unchanged
    assert cal.market_open("stonex")
    cal.tick(sat + 1_000 + EVIDENCE_QUIET_MS)                     # the carried evidence time still expires it
    assert cal.state(*KEY) is S.CLOSED


def test_reload_that_moves_a_symbol_emits_a_reload_transition_and_new_symbols_init() -> None:
    rec = Rec()
    cal = rec.calendar()
    sat = ny(2026, 11, 14, 12)
    cal.init_from_schedule(calendar(fx(), version=1), sat)
    always = symbol_schedule_from_json("stonex", "EUR/USD", "America/New_York",
                                       {"sessions": [{"open": {"day": "MON", "time": "00:00"},
                                                      "close": {"day": "MON", "time": "00:00"}}]})
    out = cal.init_from_schedule(calendar(always, fx(symbol="GBP/USD"), version=2), sat + 1_000)
    by = {t.symbol: t for t in out}
    assert by["EUR/USD"].new is S.OPEN and by["EUR/USD"].reason.startswith("reload:")   # not "init:"
    assert by["GBP/USD"].reason.startswith("init:")


def test_reload_keeps_the_maintenance_overlay_without_a_second_notice() -> None:
    rec = Rec()
    cal = rec.calendar()
    now = ny(2026, 11, 11, 12)
    w = MaintenanceWindow("stonex", now - 1, now + 3_600_000, "core upgrade")
    cal.init_from_schedule(calendar(fx(), version=1, maintenance=[w]), now)
    assert rec.maintenance == [("stonex", True, w.end_ms)]
    assert rec.notices == [("stonex", w.end_ms, "core upgrade")]
    cal.init_from_schedule(calendar(fx(), version=2, maintenance=[w]), now + 60_000)
    cal.tick(now + 61_000)
    assert cal.in_maintenance("stonex") and cal.state(*KEY) is S.BROKER_MAINTENANCE
    assert len(rec.maintenance) == 1 and len(rec.notices) == 1


def test_declared_windows_survive_a_reload_and_extensions_send_no_notice() -> None:
    rec = Rec()
    cal = rec.calendar()
    now = ny(2026, 11, 11, 12)
    cal.init_from_schedule(calendar(fx(), version=1), now)
    first = MaintenanceWindow("stonex", now, now + 600_000)
    cal.declare_maintenance(first)
    cal.declare_maintenance(first)                                 # idempotent
    cal.tick(now + 1_000)
    assert cal.in_maintenance("stonex") and len(rec.notices) == 1
    cal.declare_maintenance(MaintenanceWindow("stonex", now + 500_000, now + 1_200_000))   # extends it
    cal.init_from_schedule(calendar(fx(), version=2), now + 2_000)                         # reload, no windows
    cal.tick(now + 700_000)
    assert cal.in_maintenance("stonex") and len(rec.notices) == 1
    assert cal.maintenance_until("stonex", now + 700_000) == now + 1_200_000
    cal.tick(now + 1_200_000)
    assert not cal.in_maintenance("stonex") and rec.maintenance[-1] == ("stonex", False, None)
    assert len(rec.notices) == 1                                   # the end sends no notice


def test_replace_declared_maintenance_is_idempotent_and_a_removed_window_ends_the_overlay() -> None:
    rec = Rec()
    cal = rec.calendar()
    now = ny(2026, 11, 11, 12)
    cal.init_from_schedule(calendar(fx(), version=1), now)
    w = MaintenanceWindow("stonex", now, now + 600_000)
    for i in range(3):                                             # a durable source re-read every few seconds
        cal.replace_declared_maintenance([w])
        cal.tick(now + 1_000 * (i + 1))
    assert cal.in_maintenance("stonex") and len(rec.notices) == 1 and len(rec.maintenance) == 1
    cal.replace_declared_maintenance([])
    cal.tick(now + 5_000)
    assert not cal.in_maintenance("stonex") and rec.maintenance[-1][1] is False


def test_reload_does_not_rerun_a_pre_open_readiness_check() -> None:
    rec = Rec()
    cal = rec.calendar()
    sun = ny(2026, 11, 15, 16, 52)
    cal.init_from_schedule(calendar(fx(), version=1), sun)
    assert rec.readiness == [("stonex", "pre_open")]
    cal.init_from_schedule(calendar(fx(), version=2), sun + 60_000)
    cal.tick(sun + 120_000)
    assert rec.readiness == [("stonex", "pre_open")]


def test_no_sessions_pages_at_init_and_again_after_each_reload() -> None:
    rec = Rec()
    cal = rec.calendar(expected_adapters=lambda: ["stonex", "ctrader"])
    now = ny(2026, 11, 11, 12)
    cal.init_from_schedule(calendar(fx(), version=1), now)
    page = ("ctrader", "*", NO_SESSIONS_FLAG)
    assert rec.flags == [page]
    cal.tick(now + 1_000)
    assert rec.flags == [page]                                     # once per load, not every tick
    cal.init_from_schedule(calendar(fx(), version=2), now + 2_000)
    assert rec.flags == [page, page]                               # after the reload: paged again
    cal.init_from_schedule(calendar(fx(), fx(adapter="ctrader"), version=3), now + 3_000)
    assert rec.flags == [page, page]                               # covered now: no page


def test_maintenance_window_from_json() -> None:
    w = maintenance_window_from_json("stonex", {"start": "2026-11-14T22:00:00Z", "end": "2026-11-15T02:00:00+00:00",
                                                "reason": "core upgrade"})
    assert (w.adapter_id, w.end_ms - w.start_ms, w.reason) == ("stonex", 4 * 3_600_000, "core upgrade")
    assert maintenance_window_from_json("x", {"start_ms": 1, "end_ms": 2}).reason == "broker maintenance"
    for bad in ({"start": "2026-11-14T22:00:00", "end": "2026-11-15T02:00:00"},   # naive
                {"start_ms": 5, "end_ms": 5}, {"start": "x", "end": "y"}, {"end_ms": 5}):
        with pytest.raises(ScheduleError):
            maintenance_window_from_json("x", bad)


def test_passed_readiness_checks_are_pruned_at_the_roll() -> None:
    # Reloads keep the readiness checks already run, so the set must not grow for the whole leader term.
    rec = Rec()
    cal = rec.calendar()
    start = ny(2026, 11, 15, 16, 52)
    cal.init_from_schedule(calendar(fx(), version=1), start)
    times = sorted({*range(start, ny(2026, 11, 30, 18), 3_600_000),
                    ny(2026, 11, 22, 16, 55), ny(2026, 11, 29, 16, 55)})
    for t in times:
        cal.tick(t)
    assert [r for r in rec.readiness if r[1] == "pre_open"] == [("stonex", "pre_open")] * 3
    assert len(cal._readiness_done) <= 1
