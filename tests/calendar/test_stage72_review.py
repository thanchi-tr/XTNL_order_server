"""IMPLEMENTATION_PLAN 7.2 review (operations.md §10, oversee-rules.md §8): an evidence open keeps bounding the gate's
latest open after the symbol leaves OPEN_BY_EVIDENCE (m11 PRE_OPEN, or CLOSED after 30 min quiet), and an
out-of-order older pre-close quote never replaces the newer closing quote as the evidence reference."""
from __future__ import annotations

from decimal import Decimal

from src.calendar.machine import EVIDENCE_QUIET_MS, MarketSession, SessionState
from src.calendar.market import MarketCalendar
from tests.calendar.helpers import calendar, fx, ny

S = SessionState
D = Decimal


def test_an_evidence_open_still_bounds_latest_open_after_m11_moves_it_to_pre_open() -> None:
    cal = MarketCalendar()
    cal.init_from_schedule(calendar(fx()), ny(2026, 11, 15, 16, 40))             # Sunday, before the open
    scheduled = cal.latest_open_ms("stonex", ny(2026, 11, 15, 16, 41))
    evidence_at = ny(2026, 11, 15, 16, 45)
    cal.on_execution("stonex", "EUR/USD", evidence_at)
    assert cal.latest_open_ms("stonex", evidence_at) == evidence_at
    cal.tick(ny(2026, 11, 15, 16, 50))
    assert cal.state("stonex", "EUR/USD") is S.PRE_OPEN
    assert scheduled is None or scheduled < evidence_at
    assert cal.latest_open_ms("stonex", ny(2026, 11, 15, 16, 51)) == evidence_at   # never back to `scheduled`
    cal.tick(ny(2026, 11, 15, 17))
    assert cal.latest_open_ms("stonex", ny(2026, 11, 15, 17, 0, 1)) == ny(2026, 11, 15, 17)   # the real open


def test_an_evidence_open_still_bounds_latest_open_after_30_min_quiet() -> None:
    cal = MarketCalendar()
    sat = ny(2026, 11, 14, 10)
    cal.init_from_schedule(calendar(fx()), sat)
    cal.on_execution("stonex", "EUR/USD", sat + 1_000)
    cal.tick(sat + 1_000 + EVIDENCE_QUIET_MS)
    assert cal.state("stonex", "EUR/USD") is S.CLOSED
    assert cal.latest_open_ms("stonex", sat + 2 * EVIDENCE_QUIET_MS) == sat + 1_000


def test_an_evidence_open_survives_a_schedule_reload() -> None:
    cal = MarketCalendar()
    sat = ny(2026, 11, 14, 10)
    cal.init_from_schedule(calendar(fx()), sat)
    cal.on_execution("stonex", "EUR/USD", sat + 1_000)
    cal.tick(sat + 1_000 + EVIDENCE_QUIET_MS)
    cal.init_from_schedule(calendar(fx(), version=8), sat + 2 * EVIDENCE_QUIET_MS)
    assert cal.latest_open_ms("stonex", sat + 2 * EVIDENCE_QUIET_MS) == sat + 1_000


def test_an_older_pre_close_quote_arriving_late_never_replaces_the_closing_quote() -> None:
    m = MarketSession(fx())
    m.initialise(ny(2026, 11, 13, 16, 30))                                        # Friday, open
    m.on_tick(D("1.1005"), D("1.1007"), ny(2026, 11, 13, 16, 59), ny(2026, 11, 13, 16, 59))
    m.evaluate(ny(2026, 11, 13, 17))
    now = ny(2026, 11, 14, 10)
    # a re-sent item stamped an hour before the close arrives after it: older than the reference, ignored
    assert m.on_tick(D("1.0900"), D("1.0902"), ny(2026, 11, 13, 16), now) is None
    assert m.preclose_quote == (D("1.1005"), D("1.1007"))
    assert m.on_tick(D("1.1005"), D("1.1007"), now + 1, now + 1) is None          # the closing quote: no evidence
    assert m.state is S.CLOSED
    # a later pre-close quote (by broker time) still becomes the reference
    assert m.on_tick(D("1.1006"), D("1.1008"), ny(2026, 11, 13, 16, 59, 59), now + 2) is None
    assert m.preclose_quote == (D("1.1006"), D("1.1008"))


def test_a_seeded_reference_is_replaced_by_a_newer_pre_close_quote_only() -> None:
    m = MarketSession(fx())
    sat = ny(2026, 11, 14, 10)
    m.initialise(sat)
    m.seed_quote(D("1.1"), D("1.1002"), ny(2026, 11, 13, 16, 50), sat)           # last clean mark
    m.on_tick(D("1.0"), D("1.0002"), ny(2026, 11, 13, 16, 40), sat)              # older: ignored
    assert m.preclose_quote == (D("1.1"), D("1.1002"))
    m.on_tick(D("1.2"), D("1.2004"), ny(2026, 11, 13, 16, 59), sat, snapshot=True)   # the real (wide) closing quote
    assert m.preclose_quote == (D("1.2"), D("1.2004"))


async def test_a_readiness_check_that_raises_in_run_is_paged_and_frees_its_adapter() -> None:
    import asyncio

    from src.calendar.readiness import ReadinessDispatcher
    from tests.calendar.test_readiness import FakeActions

    class Broken(FakeActions):
        def chrony_synced(self) -> bool:
            raise OSError("chronyc missing")

    a = Broken()
    d = ReadinessDispatcher(a)
    stop = asyncio.Event()
    runner = asyncio.create_task(d.run(stop))
    d.request("stonex", "pre_open")
    for _ in range(20):
        await asyncio.sleep(0)
    assert [(sev, title) for sev, title, _ in a.pages] == [("critical", "PRE_OPEN readiness failed: stonex")]
    d.request("stonex", "roll")                                    # the adapter is free again
    for _ in range(20):
        await asyncio.sleep(0)
    assert len(a.pages) == 2
    stop.set()
    await runner
