"""Live review round 2, finding 3: a guard-pending bar 0 is decided at its cap, never spanned as a FAULT.

Design §B6.5 ("`DATA_UNAVAILABLE(first_bar_pending)` only until bar 0's `decision_cap`; then bar 0 is decided per Q3
as `FIRST_BAR_UNKNOWN(timeout)` and `last_decided_bar` advances"), §B6.1 (PENDING maps to `unknown_policy` after its
`until`), §0 Q3 (default `hold_bar`), C1 / §B6.3 (`pre_basis`: a move made on an unconfirmed heads-up never makes a
later bar's test easier) and §B5.2 (the post_grace page is for a series still waiting for the broker's bar).

The whole path runs with the real `ArmTracker`, `BarClock`, `PipelineEvaluator`, sinks and `ArmNotices` on the
in-memory fakes of `test_arm_join`. Two ways bar 0 stays PENDING in FirstBarGuard: (a) the entry time is ESTIMATED
(the reader has no fill time), (b) a STREAM entry time with no ATR value held for bar 0 (cold ATR). Long and short,
with and without a heads-up move on bar 0. Synthetic instruments, ids, levels and times only.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest

from src.db.arm_ports import EntryTime, HandleVersionRow, PositionKey
from src.marketdata.timing import COLD_SKEW, decision_cap_ms, post_grace_end_ms, pre_fire_ms, timing_for
from src.marketdata.types import M15, Bar, Quote, SeriesId
from src.riskmgr.barclock import BarClock, BarClockSettings
from src.riskmgr.config import ArmSettings
from src.riskmgr.evaluation import PipelineEvaluator
from src.riskmgr.jobs import JobRunner
from src.riskmgr.notices import ArmNotices, NoticeSettings, RouteBook
from src.riskmgr.outcomes import BROKER_BAR_NOT_FINAL, FIRST_BAR_PENDING, GUARD_PENDING, is_guard_pending
from src.riskmgr.pipeline import pending_reason
from src.riskmgr.ports import Pager
from src.riskmgr.replay import BarSlot, Decide, Frontier, Span, next_step
from src.riskmgr.sinks import DecisionSink, SinkChain
from src.riskmgr.sm11 import ArmState
from src.riskmgr.tracker import ArmTracker
from src.riskmgr.types import ArmEvent
from tests.riskmgr.barclock_fakes import FakeControls
from tests.riskmgr.failed_fakes import FakeCalendar
from tests.riskmgr.notice_fakes import FakeOutbox, NStore, Policies
from tests.riskmgr.test_arm_join import KEYRING, JoinMd
from tests.riskmgr.tracker_fakes import (
    ADAPTER,
    SYMBOL,
    T0,
    FakeClock,
    FakeConfig,
    FakeFailed,
    FakeGrid,
    FakeSnapshot,
    FakeVersions,
    Recorder,
    document,
    image,
    position,
)

D = Decimal
TF = M15.timeframe_ms
FILL = T0 + 60_000                    # bar 0 is the bucket [T0, T0 + TF)
ARMED_AT = FILL + 5_000
B0 = T0 + TF
B1 = B0 + TF
CAP0 = decision_cap_ms(B0, M15, COLD_SKEW)
_ROW = timing_for(M15)
assert _ROW is not None
GRACE0 = post_grace_end_ms(B0, _ROW, COLD_SKEW)


class SidedMd(JoinMd):
    """`JoinMd` with final bars on both quote sides (a short's trigger close is the ask, Q5) and a two-sided quote."""

    def final_both(self, bar_end_ms: int, close: str) -> None:
        c = D(close)
        for side in ("BID", "ASK"):
            series = SeriesId.parse(f"{ADAPTER}:{SYMBOL}:15m:{side}:BROKER")
            self.bars_by_series.setdefault(series.canonical, []).append(Bar(
                series=series, bucket_start_ms=bar_end_ms - TF, open=c, high=c, low=c, close=c,
                final_at_ms=bar_end_ms + 1_000))

    def quote_at_fire(self, bid: str, ask: str, received_ms: int) -> None:
        self.quote_ = Quote(adapter_id=ADAPTER, symbol=SYMBOL, bid=D(bid), ask=D(ask), broker_ms=received_ms,
                            received_ms=received_ms)
        self.quote_history.append(self.quote_)


@dataclass(frozen=True)
class Case:
    """One side's levels: entry, S0, the PRE quote, bar 0's and bar 1's trigger-side closes (the mirror for a
    short), all derived from the long levels E 1.2000, S0 1.1900, PRE 1.2110, bar 0 1.2090, bar 1 1.2010."""

    side: str
    sign: int

    def lv(self, long_level: str) -> str:
        e = D("1.2000")
        return str(e + self.sign * (D(long_level) - e))


LONG, SHORT = Case("BUY", 1), Case("SELL", -1)


@dataclass
class Rig:
    store: NStore
    clock: FakeClock
    md: SidedMd
    snapshot: FakeSnapshot
    versions: FakeVersions
    rec: Recorder
    tracker: ArmTracker
    barclock: BarClock
    notices: ArmNotices
    case: Case
    seq: int = 10

    def stop(self, long_level: str, broker_ms: int) -> None:
        """The position's stop is (re)set: a stream version in `event_log` and the account image."""
        level = self.case.lv(long_level)
        self.seq += 1
        self.versions.history.setdefault("P1", []).append(HandleVersionRow(
            handle_kind="POSITION_FIELD", handle_id="P1", seq_id=self.seq, broker_time_ms=broker_ms,
            origin="stream", status="OPEN", level=D(level), units=D("1000"), live=True))
        self.snapshot.images = [image(position("P1", side=self.case.side, entry="1.2000", stop=level,
                                               updated_ms=broker_ms))]

    async def at(self, now: int) -> None:
        self.clock.now = now
        await self.barclock.step()
        await self.notices.step()

    def row(self) -> Any:
        return self.store.rows[PositionKey(adapter_id=ADAPTER, position_id="P1")]

    def decided(self, event: str, bar_end_ms: int) -> list[Any]:
        return [s.row for s in self.store.decisions if s.row.event == event and s.row.bar_end_ms == bar_end_ms]

    def bar_end(self, bar_end_ms: int) -> list[Any]:
        """The bar-end decision of a bar: `POST`, or `LATE` once its POST notice has gone (§B8)."""
        return [s.row for s in self.store.decisions if s.row.event in ("POST", "LATE")
                and s.row.bar_end_ms == bar_end_ms]

    def spans(self) -> list[Any]:
        return [s.row for s in self.store.decisions if s.row.outcome == "UNRESOLVED_RANGE"]


def rig(case: Case, *, estimated: bool) -> Rig:
    store = NStore()
    outbox = FakeOutbox()
    store.outbox = outbox
    clock = FakeClock(ARMED_AT)
    md = SidedMd()
    snapshot, versions, rec = FakeSnapshot(), FakeVersions(), Recorder()
    grid = FakeGrid()
    settings = ArmSettings()
    pager = Pager(rec.page, lambda t, b: None)
    notices = ArmNotices(store=store, outbox=outbox, routes=RouteBook({}), policies=Policies(), grid=grid,
                         pager=pager, clock=clock, settings=settings,
                         notices=NoticeSettings(test_at_ms_of_day=86_399_999))
    tracker = ArmTracker(store=store, versions=versions, snapshot=snapshot, md=md,  # type: ignore[arg-type]
                         grid=grid, config=FakeConfig(document()), notices=notices, answers=rec, pager=pager,
                         clock=clock, settings=settings, failed=FakeFailed())
    evaluator = PipelineEvaluator(md=md, snapshot=snapshot, versions=versions,  # type: ignore[arg-type]
                                  decisions=store, grid=grid, calendar=FakeCalendar(), settings=settings,
                                  keyring=lambda: KEYRING)
    barclock = BarClock(store=store, controls=FakeControls(), md=md, grid=grid,  # type: ignore[arg-type]
                        evaluator=evaluator, jobs=JobRunner(), pager=pager, clock=clock, settings=settings,
                        lease_valid=lambda: True, sinks=SinkChain((DecisionSink(), notices.sink())),
                        notices=notices, barclock=BarClockSettings(rows_ttl_ms=0), keyring=lambda: KEYRING)
    r = Rig(store, clock, md, snapshot, versions, rec, tracker, barclock, notices, case)
    if not estimated:                         # (b): a STREAM fill time; the ATR is never held (JoinMd)
        versions.entries["P1"] = EntryTime(source="STREAM", broker_time_ms=FILL, seq_id=1, origin="stream")
    r.stop("1.1900", FILL + 1_000)
    return r


async def run_bar0(case: Case, *, estimated: bool, heads_up: bool) -> Rig:
    """Arm in bar 0, PRE of bar 0 (a heads-up ADJUST when `heads_up`, then the stop is moved to the advised
    break-even before bar 0 ends), bar 0 closes short of the trigger against S0, a pass after post_grace, then the
    first pass at bar 0's cap with bar 1 final."""
    r = rig(case, estimated=estimated)
    await r.tracker.step()
    await r.notices.step()
    row = r.row()
    assert row.state == ArmState.ARMED.value
    assert row.entry_time_source == ("ESTIMATED" if estimated else "STREAM")
    assert M15.bucket_start_ms(row.entry_time_ms) == T0            # bar 0 is the bucket ending at B0
    fire0 = pre_fire_ms(B0, COLD_SKEW, ArmSettings().pre_offset_s)
    quote = case.lv("1.2110") if heads_up else case.lv("1.2050")
    r.md.quote_at_fire(quote, quote, fire0 - 1_000)
    await r.at(fire0 + 100)
    pre0 = r.decided("PRE", B0)
    if heads_up:
        assert [(d.outcome, D(d.proposed_stop)) for d in pre0] == [("ADJUST", D("1.2000"))]
        r.stop("1.2000", B0 - 30_000)                               # the user acts on the heads-up
    else:
        assert [d.outcome for d in pre0] == ["STRATEGY_REASON"]
    r.md.final_both(B0, case.lv("1.2090"))
    await r.at(B0 + 2_000)
    await r.at(GRACE0 + 1_000)                                     # past post_grace, before the cap
    assert r.bar_end(B0) == [] and r.spans() == []                # bar 0 waits on the guard (first_bar_pending)
    assert r.row().last_decided_bar < B0
    r.md.final_both(B1, case.lv("1.2010"))
    await r.at(CAP0)
    return r


@pytest.mark.parametrize("case", [LONG, SHORT], ids=["long", "short"])
@pytest.mark.parametrize("estimated", [True, False], ids=["estimated_entry", "cold_atr"])
async def test_a_heads_up_move_on_a_guard_pending_bar_0_never_releases_on_bar_1(case: Case, estimated: bool) -> None:
    """Bar 0 is `PRE_NOT_CONFIRMED(FIRST_BAR_UNKNOWN, timeout)` (a HOLD of the heads-up) with `pre_basis` = S0
    stored; bar 1 then tests against S0 (favour 0.0010 < risk 0.0100), so it is `AT_TARGET_UNCONFIRMED`, the position
    stays managed; no FAULT span, no page."""
    r = await run_bar0(case, estimated=estimated, heads_up=True)
    try:
        bar0 = r.bar_end(B0)
        assert [(d.outcome, d.reason_kind, d.reason, d.category) for d in bar0] == [
            ("PRE_NOT_CONFIRMED", "first_bar", "FIRST_BAR_UNKNOWN", "INFO")]
        assert r.spans() == [] and r.rec.pages == []
        bar1 = r.bar_end(B1)
        assert [(d.outcome, d.trigger_met, d.stop_basis) for d in bar1] == [
            ("AT_TARGET_UNCONFIRMED", False, "AT_PRE")]
        row = r.row()
        assert row.state != ArmState.STRATEGY_COMPLETE.value and row.last_decided_bar == B1
        assert row.pre_basis_json is not None and row.pre_basis_bar == B0   # the pre-move stop is kept (C1)
    finally:
        r.barclock.close()


@pytest.mark.parametrize("case", [LONG, SHORT], ids=["long", "short"])
@pytest.mark.parametrize("estimated", [True, False], ids=["estimated_entry", "cold_atr"])
async def test_a_guard_pending_bar_0_ends_first_bar_unknown_timeout_not_a_fault_span(case: Case,
                                                                                     estimated: bool) -> None:
    """Without a heads-up: bar 0 is the Q3 HOLD `GUARD_REASON(FIRST_BAR_UNKNOWN, timeout)` at its cap, the cursor
    advances, and nothing pages (no 'bars unresolved', no 'awaiting broker bar after post_grace')."""
    r = await run_bar0(case, estimated=estimated, heads_up=False)
    try:
        bar0 = r.bar_end(B0)
        assert [(d.outcome, d.reason_kind, d.reason, d.category) for d in bar0] == [
            ("GUARD_REASON", "first_bar", "FIRST_BAR_UNKNOWN", "HOLD")]
        assert "first_bar_unknown_stale" not in bar0[0].caveats
        assert r.spans() == [] and r.rec.pages == []
        assert r.row().last_decided_bar == B1 and len(r.bar_end(B1)) == 1
    finally:
        r.barclock.close()


async def test_a_cap_decision_that_is_not_final_is_spanned_on_the_next_pass() -> None:
    """The one decision at the cap fails (the account image went stale, pre-guard (1)): nothing is written for the
    bar then, and the next pass spans it, so the frontier never sticks on a guard wait."""
    r = rig(LONG, estimated=True)
    await r.tracker.step()
    r.md.final_both(B0, "1.2090")
    await r.at(B0 + 2_000)
    assert r.bar_end(B0) == [] and r.spans() == []
    r.snapshot.images = [image(position("P1", entry="1.2000", stop="1.1900", updated_ms=FILL), fresh=False)]
    await r.at(CAP0)
    assert r.bar_end(B0) == [] and r.spans() == []
    await r.at(CAP0 + 250)
    spans = r.spans()
    assert [(s.bar_end_ms, s.reason == "first_bar_pending") for s in spans] == [(B0, False)]
    assert r.row().last_decided_bar >= B0
    r.barclock.close()


def test_replay_decides_a_guard_wait_at_its_cap_and_spans_the_run_before_it() -> None:
    """`next_step` (§B6.6 one pass, §B6.5): an expired `guard_timeout` slot is a `Decide` (POST, or LATE once its
    notice has gone), never part of a span; an expired run before it is spanned first; once tried (flag off) the
    same slot is spanned."""
    cursor = Frontier(T0)
    now = decision_cap_ms(B1, M15, COLD_SKEW)
    wait = BarSlot(B0, reason="first_bar_pending", guard_timeout=True)
    assert next_step(cursor, [wait], now, spec=M15, skew=COLD_SKEW, bar0_end_ms=B0) == Decide(B0, ArmEvent.POST)
    late = BarSlot(B0, reason="first_bar_pending", post_sent=True, guard_timeout=True)
    assert next_step(cursor, [late], now, spec=M15, skew=COLD_SKEW, bar0_end_ms=B0) == Decide(B0, ArmEvent.LATE)
    before = BarSlot(B0, reason="downtime")
    after = BarSlot(B1, reason="first_bar_pending", guard_timeout=True)
    step = next_step(cursor, [before, after], now, spec=M15, skew=COLD_SKEW, bar0_end_ms=B0)
    assert isinstance(step, Span) and (step.from_bar_ms, step.to_bar_ms) == (B0, B0)
    tried = BarSlot(B0, reason="first_bar_pending")
    step = next_step(cursor, [tried], now, spec=M15, skew=COLD_SKEW, bar0_end_ms=B0)
    assert isinstance(step, Span) and step.reason == "first_bar_pending" and step.first_bar_stale


def test_only_a_guards_wait_is_a_guard_pending_reason() -> None:
    assert is_guard_pending(FIRST_BAR_PENDING) and is_guard_pending(GUARD_PENDING)
    assert is_guard_pending(pending_reason("first_bar"))
    for other in (BROKER_BAR_NOT_FINAL, "pending", "_pending", "previous_post_pending", "reopen_pending", "downtime",
                  "stop_stale", None):
        assert not is_guard_pending(other)
