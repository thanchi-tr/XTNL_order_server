"""`S_at_T` golden cases through the BarClock (round 13 review, plan S38d; design §B6.4).

The stop child that held the stop at `T` is cancelled and replaced (a new order id, another level) after `T`, long
and short, on the three paths the plan names:

1. the POST waits for a late broker bar while the child is replaced at `T + 20 s`;
2. a Friday last-bar trigger (`MARKET_CLOSED` at the POST) with the cancel/replace over the weekend, before the
   REOPEN re-check at the open;
3. the cancel/replace happens before a LATE replay (the bar's POST notice went while the bar was pending).

The real `ArmTracker`, `BarClock`, S21 pipeline (`PipelineEvaluator`) and the real `PgArmVersionReader` over the
`event_log` / `orders_state` emulator, so the reader's "every stop child ever linked" read is what the BarClock's
decision sees. Each case decides finally on the pre-replace level with `stop_basis` STREAM, writes no span row and
pages nothing. Synthetic ids, levels and times only; no assertion message carries a value.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest

from src.cache.keys import order_key, position_key
from src.db import arm_versions as V
from src.db.epoch import EngineDb, Epoch
from src.marketdata.types import M15, Bar, SeriesId, Source
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.barclock import BarClock, BarClockSettings
from src.riskmgr.config import ArmSettings
from src.riskmgr.evaluation import PipelineEvaluator
from src.riskmgr.jobs import JobRunner
from src.riskmgr.ports import Pager
from src.riskmgr.sinks import DecisionSink, SinkChain
from src.riskmgr.stopbasis import StopAtT
from src.riskmgr.types import ArmEvent
from tests.db.arm_emulator import ArmEmulator
from tests.riskmgr.barclock_fakes import FakeControls
from tests.riskmgr.barclock_fakes import FakeGrid as ClosedGrid
from tests.riskmgr.failed_fakes import FakeCalendar
from tests.riskmgr.test_arm_join import FILL, KEYRING, TF, Join, join_rig
from tests.riskmgr.tracker_fakes import ADAPTER, SYMBOL, image, position, stop_child

D = Decimal
DAY = 86_400_000
UNITS = "1000"


@dataclass(frozen=True)
class Case:
    side: str
    held: str            # the child's level at T (the pre-replace level)
    moved: str           # the replacement's level
    below: str           # a close that does not meet the trigger
    above: str           # a close that meets it
    quote: str           # the exit-side quote after T (bid for a long, ask for a short)

    @property
    def trigger(self) -> QuoteSide:
        return QuoteSide.BID if self.side == "BUY" else QuoteSide.ASK

    @property
    def child_side(self) -> str:
        return "SELL" if self.side == "BUY" else "BUY"


LONG = Case("BUY", "1.1900", "1.1950", "1.2050", "1.2150", "1.2150")
SHORT = Case("SELL", "1.2100", "1.2050", "1.1950", "1.1850", "1.1850")


def _child(level: str, status: str = "WORKING") -> dict[str, Any]:
    return {"status": status, "entry_price": level, "remaining_units": UNITS, "order_kind": "STOP",
            "linked_position_id": "P1", "is_reduce_only": True}


class PostSent:
    """The route's POST notice for the named bars has gone (S28), so a decision of them now is LATE."""

    def __init__(self) -> None:
        self.sent: set[int] = set()

    def post_sent(self, row: Any, bar_spec: str, bar_end_ms: int) -> bool:
        return bar_end_ms in self.sent


@dataclass
class Rig:
    j: Join
    emu: ArmEmulator
    case: Case
    calendar: FakeCalendar
    seen: list[tuple[int, object]]
    seq: int = 10

    def image(self, oid: str, level: str) -> None:
        c = self.case
        self.j.snapshot.images = [image(position("P1", side=c.side, stop=None, updated_ms=FILL),
                                        orders=[stop_child(oid, "P1", units=UNITS, level=level, side=c.child_side)])]

    def version(self, oid: str, level: str, at_ms: int, status: str = "WORKING") -> None:
        self.seq += 1
        self.emu.add_version(entity_type="ORDER", entity_key=order_key(ADAPTER, oid), seq_id=self.seq,
                             payload=_child(level, status), origin="stream", broker_time_ms=at_ms)

    def replace(self, at_ms: int) -> None:
        """A StoneX cancel/replace at `at_ms`: O1 cancelled, O2 (a new order id, another level) working; the
        account image now names O2 only."""
        self.version("O1", self.case.held, at_ms, "CANCELLED")
        self.version("O2", self.case.moved, at_ms)
        self.image("O2", self.case.moved)

    def final(self, bar_end_ms: int, close: str) -> None:
        series = SeriesId(ADAPTER, SYMBOL, M15, self.case.trigger, Source.BROKER)
        c = D(close)
        self.j.md.bars_by_series.setdefault(series.canonical, []).append(
            Bar(series=series, bucket_start_ms=bar_end_ms - TF, open=c, high=c, low=c, close=c,
                final_at_ms=bar_end_ms + 1_000))

    def quote(self, at_ms: int) -> None:
        q = D(self.case.quote)
        self.j.md.live_quote(str(q if self.case.side == "BUY" else q - D("0.0002")), at_ms)


def _rig(case: Case, *, grid: Any = None, closes: Any = None, notices: Any = None) -> Rig:
    """The join with the real version reader over the emulator, and a BarClock term with the given grid, session
    closes and POST-notice view."""
    j = join_rig()
    emu = ArmEmulator()
    versions = V.PgArmVersionReader(EngineDb(emu, lambda: Epoch("c", 7)))  # type: ignore[arg-type]
    emu.add_version(entity_type="POSITION", entity_key=position_key(ADAPTER, "P1"), seq_id=1,
                    payload={"status": "OPEN", "units": UNITS}, origin="stream", broker_time_ms=FILL)
    calendar = FakeCalendar()
    seen: list[tuple[int, object]] = []
    r = Rig(j, emu, case, calendar, seen)
    r.version("O1", case.held, FILL + 1_000)
    r.image("O1", case.held)
    j.barclock.close()
    settings = ArmSettings()
    grid = grid or j.evaluator._grid  # noqa: SLF001
    j.tracker._versions = versions  # noqa: SLF001
    j.tracker._grid = grid  # noqa: SLF001
    j.evaluator = PipelineEvaluator(md=j.md, snapshot=j.snapshot, versions=versions,  # type: ignore[arg-type]
                                    decisions=j.store, grid=grid, calendar=calendar, settings=settings,
                                    keyring=lambda: KEYRING)
    stop_at = j.evaluator._stop_at  # noqa: SLF001

    async def spy(row: Any, pos: Any, detail: Any, bar_end_ms: int) -> Any:
        got = await stop_at(row, pos, detail, bar_end_ms)
        seen.append((bar_end_ms, got))
        return got

    j.evaluator._stop_at = spy  # type: ignore[method-assign]  # noqa: SLF001
    j.barclock = BarClock(store=j.store, controls=FakeControls(), md=j.md, grid=grid,  # type: ignore[arg-type]
                          evaluator=j.evaluator, jobs=JobRunner(), pager=Pager(j.rec.page, lambda t, b: None),
                          clock=j.clock, settings=settings, lease_valid=lambda: True,
                          sinks=SinkChain((DecisionSink(), j.notices.sink())), notices=notices or j.notices,
                          closes=closes, barclock=BarClockSettings(rows_ttl_ms=0), keyring=lambda: KEYRING)
    return r


async def _first_bar(r: Rig) -> tuple[int, int]:
    """Arm (late: one armed_late span) and decide B1 below the trigger; returns (b1, b2)."""
    j = r.j
    await j.tracker.step()
    await j.notices.step()
    cursor = j.row().last_decided_bar
    b1 = cursor + TF
    r.final(b1, r.case.below)
    await j.at(b1 + 2_000)
    assert [(x.outcome, x.trigger_met) for x in j.decisions("POST") if x.bar_end_ms == b1] == [
        ("STRATEGY_REASON", False)]
    return b1, b1 + TF


def _assert_on_held_level(r: Rig, bar_end: int, event: str) -> None:
    j = r.j
    final = [x for x in j.decisions(event) if x.bar_end_ms == bar_end]
    assert len(final) == 1
    d = final[0]
    assert d.outcome != "UNRESOLVED_RANGE" and d.category != "FAULT" and d.reason != "stop_at_bar_end"
    assert d.stop_basis == "STREAM" and d.trigger_met is True
    at_t = [got for end, got in r.seen if end == bar_end]
    assert at_t and all(isinstance(g, StopAtT) for g in at_t)
    assert {g.level for g in at_t if isinstance(g, StopAtT)} == {D(r.case.held)}          # the pre-replace level
    assert all([h.handle.id for h in g.handles] == ["O1"] for g in at_t if isinstance(g, StopAtT))
    assert [x for x in j.decisions() if x.outcome == "UNRESOLVED_RANGE" and x.bar_end_ms >= bar_end] == []
    assert j.rec.pages == []


CASES = pytest.mark.parametrize("case", [LONG, SHORT], ids=["long", "short"])


@CASES
async def test_a_post_waiting_for_a_late_bar_decides_on_the_pre_replace_level(case: Case) -> None:
    r = _rig(case)
    j = r.j
    try:
        _, b2 = await _first_bar(r)
        await j.at(b2 + 5_000)                                   # the broker's bar is late: pending
        assert [x for x in j.decisions("POST") if x.bar_end_ms == b2] == []
        r.replace(b2 + 20_000)                                   # cancel/replace while the POST waits
        r.quote(b2 + 21_000)
        r.final(b2, case.above)
        await j.at(b2 + 45_000)
        _assert_on_held_level(r, b2, "POST")
    finally:
        j.barclock.close()


@CASES
async def test_a_friday_trigger_rechecked_at_reopen_decides_on_the_pre_replace_level(case: Case) -> None:
    from tests.riskmgr.test_barclock import Closes
    probe = join_rig()
    probe.barclock.close()
    await probe.tracker.step()
    b2 = probe.row().last_decided_bar + 2 * TF                   # the same timeline as `_first_bar`
    open_ms = b2 + 2 * DAY
    closed = frozenset(range(b2 + TF, open_ms + 1, TF))
    r = _rig(case, grid=ClosedGrid(closed=closed), closes=Closes(b2, open_ms))
    j = r.j
    try:
        _, got_b2 = await _first_bar(r)
        assert got_b2 == b2
        r.calendar.closed.add((ADAPTER, SYMBOL))                 # Friday's close: the market shut at b2
        r.quote(b2 + 1_000)
        r.final(b2, case.above)
        await j.at(b2 + 2_000)
        post = [x for x in j.decisions("POST") if x.bar_end_ms == b2]
        assert [(x.outcome, x.trigger_met) for x in post] == [("MARKET_CLOSED", True)]
        assert j.row().reopen_bar_end_ms == b2
        r.replace(b2 + DAY)                                      # Saturday: the child is cancelled / replaced
        await j.at(b2 + DAY + 60_000)                            # closed: nothing
        r.calendar.closed.clear()
        r.quote(open_ms + 1_000)
        await j.at(open_ms + 60_000)
        _assert_on_held_level(r, b2, ArmEvent.REOPEN.value)
    finally:
        j.barclock.close()


@CASES
async def test_a_late_replay_after_a_cancel_replace_decides_on_the_pre_replace_level(case: Case) -> None:
    sent = PostSent()
    r = _rig(case, notices=sent)
    j = r.j
    try:
        _, b2 = await _first_bar(r)
        await j.at(b2 + 5_000)                                   # pending: the POST notice goes without it
        sent.sent.add(b2)
        r.replace(b2 + 30_000)                                   # cancel/replace before the LATE replay
        r.quote(b2 + 31_000)
        r.final(b2, case.above)
        await j.at(b2 + 50_000)
        assert [x for x in j.decisions("POST") if x.bar_end_ms == b2] == []
        _assert_on_held_level(r, b2, ArmEvent.LATE.value)
    finally:
        j.barclock.close()
