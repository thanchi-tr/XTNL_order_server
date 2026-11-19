"""S39b gate `replay_rerun`: `arm replay --rerun` re-runs the real pipeline for a past bar (the real
`PipelineEvaluator` and `JobRunner` over as-of ports), read-only and with no notice, and reports MATCH, DIFF or
UNVERIFIABLE by the recomputed `inputs_digest` and the outcome.

Built on the S36b join rig: its decisions are committed by the real BarClock, then re-run. Synthetic ids, levels and
times only; no assertion message carries a value.
"""
from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from src.calendar.machine import SessionState
from src.db.arm_ports import PositionKey
from src.marketdata.types import DataUnavailable, M15, SeriesId, Unsupported
from src.riskmgr.config import ArmSettings
from src.riskmgr.evaluation import PipelineEvaluator
from src.riskmgr.jobs import JobRunner
from src.riskmgr.rerun import (
    CALENDAR_NOT_RECORDED,
    AsOfCalendar,
    AsOfDecisions,
    AsOfImages,
    AsOfMarketData,
    RerunInputs,
    RerunResult,
    RerunTarget,
    compare,
    eligible,
    rerun_one,
)
from src.marketdata.timing import COLD_SKEW
from src.marketdata.types import MdGeneration
from src.riskmgr.barclock import NotReady
from tests.riskmgr.failed_fakes import FakeCalendar
from tests.riskmgr.test_arm_join import KEYRING, Join, join_rig
from tests.riskmgr.test_round13 import _to_adjust, _to_complete
from tests.riskmgr.tracker_fakes import ADAPTER, SYMBOL, FakeGrid, image, position

TF = M15.timeframe_ms


class AsOfSpy:
    """Records the `as_of_ms` every read was pinned to (wraps the join's market data)."""

    def __init__(self, md: Any) -> None:
        self.md = md
        self.as_of: list[int | None] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.md, name)

    async def bars(self, series: SeriesId, **kw: Any) -> Any:
        self.as_of.append(kw.get("as_of_ms"))
        return await self.md.bars(series, **kw)


def target(j: Join, event: str, bar_end_ms: int, **over: Any) -> RerunTarget:
    """The stored decision as the ops wiring reads it (its handles' levels read included)."""
    s = [d for d in j.store.decisions if d.row.event == event and d.row.bar_end_ms == bar_end_ms][-1]
    t = RerunTarget(decision_id=s.ref.decision_id, key=s.row.key, bar_spec=s.row.bar_spec, event=s.row.event,
                    bar_end_ms=s.row.bar_end_ms, revision=s.row.revision, decision_seq=s.row.decision_seq,
                    decided_at_ms=s.row.decided_at_ms, outcome=s.row.outcome, reason=s.row.reason,
                    inputs_digest=s.row.inputs_digest,
                    handles=tuple((h.handle_id, h.level_read) for h in s.handles))
    return replace(t, **over)


async def rerun(j: Join, t: RerunTarget, md: Any | None = None, *, snapshot: Any | None = None,
                calendar: Any | None = None) -> Any:
    spy = md or AsOfSpy(j.md)
    evaluator = PipelineEvaluator(
        md=AsOfMarketData(spy, t.decided_at_ms), snapshot=snapshot or j.snapshot,  # type: ignore[arg-type]
        versions=j.versions,  # type: ignore[arg-type]
        decisions=AsOfDecisions(j.store, t.decision_seq), grid=FakeGrid(), calendar=calendar or FakeCalendar(),
        settings=ArmSettings(), keyring=lambda: KEYRING)
    jobs = JobRunner()
    try:
        return await rerun_one(evaluator, jobs, t, RerunInputs(j.row(), M15, COLD_SKEW, MdGeneration(1, 1)))
    finally:
        jobs.close()


async def test_a_past_bar_is_re_run_through_the_real_pipeline_and_matches() -> None:
    j = join_rig()
    try:
        b1, b2, b3 = await _to_adjust(j)
        decisions, outbox = len(j.store.decisions), len(j.outbox.rows)
        row_before = j.row()
        spy = AsOfSpy(j.md)
        t1 = target(j, "POST", b1)
        got = await rerun(j, t1, spy)
        assert (got.result, got.outcome) == (RerunResult.MATCH, "STRATEGY_REASON")
        assert spy.as_of and all(a == t1.decided_at_ms for a in spy.as_of)       # every read point-in-time
        # read-only: nothing written, no notice, no state change
        assert len(j.store.decisions) == decisions and len(j.outbox.rows) == outbox and j.row() == row_before
    finally:
        j.barclock.close()


async def test_an_adjust_needs_the_unrecorded_quote() -> None:
    j = join_rig()
    try:
        _, b2, _ = await _to_adjust(j)
        got = await rerun(j, target(j, "POST", b2))                               # the ADJUST used the exit quote
        assert (got.result, got.reason) == (RerunResult.UNVERIFIABLE, "exit_quote_not_recorded")
    finally:
        j.barclock.close()


async def test_a_release_re_run_matches_and_a_moved_stop_is_order_state_changed() -> None:
    """The COMPLETE of B3 re-runs to a MATCH; the ADJUST of B2 is not comparable any more because the stop was moved
    after it (the handle's level read now is not the one the decision read): never a false DIFF."""
    j = join_rig()
    try:
        b3 = await _to_complete(j)
        assert (await rerun(j, target(j, "POST", b3))).result is RerunResult.MATCH
        got = await rerun(j, target(j, "POST", b3 - TF))
        assert (got.result, got.reason) == (RerunResult.UNVERIFIABLE, "order_state_changed")
        unchecked = await rerun(j, target(j, "POST", b3 - TF, handles=None))     # without the stored handles
        assert unchecked.result is RerunResult.DIFF                              # what the check prevents
    finally:
        j.barclock.close()


async def test_same_inputs_another_outcome_is_a_diff() -> None:
    j = join_rig()
    try:
        b1, _, _ = await _to_adjust(j)
        got = await rerun(j, target(j, "POST", b1, outcome="COMPLETE", reason=None))
        assert (got.result, got.reason, got.outcome) == (RerunResult.DIFF, "outcome", "STRATEGY_REASON")
    finally:
        j.barclock.close()


async def test_a_revised_bar_or_a_closed_position_is_unverifiable() -> None:
    j = join_rig()
    try:
        b1, _, _ = await _to_adjust(j)
        t1 = target(j, "POST", b1)
        j.md.revise_bar(b1, "1.2040")                                             # another bar since (this fake
        got = await rerun(j, t1)                                                  # store ignores as_of)
        assert (got.result, got.reason) == (RerunResult.UNVERIFIABLE, "inputs_differ")
        j.snapshot.images = [image(position("P1", status="CLOSED"))]
        got = await rerun(j, t1)
        assert (got.result, got.reason) == (RerunResult.UNVERIFIABLE, "not_ready:position_not_open")
    finally:
        j.barclock.close()


async def test_only_bar_end_decisions_are_re_run() -> None:
    j = join_rig()
    try:
        _, b2, _ = await _to_adjust(j)
        pre = target(j, "PRE", b2)
        assert eligible(pre) == "not_a_bar_end_decision"
        assert (await rerun(j, pre)).reason == "not_a_bar_end_decision"
        span = [d for d in j.store.decisions if d.row.outcome == "UNRESOLVED_RANGE"][0]
        assert eligible(target(j, span.row.event, span.row.bar_end_ms)) == "span"
    finally:
        j.barclock.close()


class FencedNow:
    """Today's snapshot with a stream fence open now (the account image is stale at the time of the re-run)."""

    def __init__(self, snapshot: Any) -> None:
        self.snapshot = snapshot

    async def read(self, accounts: Any) -> Any:
        return [replace(i, fresh=False, reasons=("orders_fence",)) for i in await self.snapshot.read(accounts)]


async def test_a_fence_open_now_does_not_stop_the_re_run_at_pre_guard_one() -> None:
    """The ops wiring's `AsOfImages`: a fence open when the operator runs `--rerun` says nothing of the decision time;
    without it the re-run stops at pre-guard (1) and hides the cause as `not_final`."""
    j = join_rig()
    try:
        b1, _, _ = await _to_adjust(j)
        t1 = target(j, "POST", b1)
        stale = await rerun(j, t1, snapshot=FencedNow(j.snapshot))
        assert (stale.result, stale.reason) == (RerunResult.UNVERIFIABLE, "not_final")
        got = await rerun(j, t1, snapshot=AsOfImages(FencedNow(j.snapshot)))
        assert (got.result, got.outcome) == (RerunResult.MATCH, "STRATEGY_REASON")
    finally:
        j.barclock.close()


async def test_a_market_closed_on_one_side_only_is_calendar_not_recorded_never_a_diff() -> None:
    """The latest schedule says CLOSED at T but the stored decision was made open (an evidence overlay or an older
    schedule version): the calendar at T is not recorded, so the re-run is not comparable."""
    j = join_rig()
    try:
        _, b2, _ = await _to_adjust(j)
        got = await rerun(j, target(j, "POST", b2), calendar=FakeCalendar(closed={(ADAPTER, SYMBOL)}))
        assert (got.result, got.reason, got.outcome) == (RerunResult.UNVERIFIABLE, CALENDAR_NOT_RECORDED,
                                                         "MARKET_CLOSED")
    finally:
        j.barclock.close()


def test_compare_codes() -> None:
    t = RerunTarget(decision_id=1, key=PositionKey(adapter_id=ADAPTER, position_id="P1"), bar_spec="15m",
                    event="POST", bar_end_ms=TF,
                    revision=0, decision_seq=2, decided_at_ms=TF + 1, outcome="HOLD", reason=None,
                    inputs_digest=None)
    assert compare(t, NotReady("pin_unreadable")).reason == "not_ready:pin_unreadable"
    assert compare(t, "job_timeout").reason == "job:job_timeout"


async def test_the_as_of_ports_are_read_only_and_pinned() -> None:
    j = join_rig()
    md = AsOfMarketData(j.md, 123)                                                # type: ignore[arg-type]
    assert isinstance(await md.quote(ADAPTER, SYMBOL), DataUnavailable)
    assert isinstance(await md.quote_at(ADAPTER, SYMBOL, 1), DataUnavailable)
    assert await md.demand("x", "replay", None) is False                           # type: ignore[arg-type]
    assert isinstance(await md.refetch(SeriesId.parse(f"{ADAPTER}:{SYMBOL}:15m:BID:BROKER"), 0, 0), Unsupported)
    assert j.md.demands == []
    from src.calendar.schedule import symbol_schedule_from_json
    sched = symbol_schedule_from_json(ADAPTER, SYMBOL, "America/New_York",
                                      {"sessions": [{"open": {"day": "SUN", "time": "17:00"},
                                                     "close": {"day": "FRI", "time": "17:00"}}]})
    tuesday_noon_ny = 1_773_158_400_000                                            # 2026-03-10 16:00 UTC
    saturday = tuesday_noon_ny + 4 * 86_400_000
    assert AsOfCalendar(lambda a, s: sched, tuesday_noon_ny).state(ADAPTER, SYMBOL) is SessionState.OPEN
    assert AsOfCalendar(lambda a, s: sched, saturday).state(ADAPTER, SYMBOL) is SessionState.CLOSED
    assert AsOfCalendar(lambda a, s: None, saturday).state(ADAPTER, SYMBOL) is None
    j.barclock.close()


@pytest.mark.parametrize("seq_offset", [0, 1])
async def test_the_decision_memory_is_read_as_it_stood_before_the_decision(seq_offset: int) -> None:
    j = join_rig()
    try:
        _, b2, _ = await _to_adjust(j)
        t2 = target(j, "POST", b2)
        before = await AsOfDecisions(j.store, t2.decision_seq + seq_offset).decision_memory(t2.key, "15m", b2)
        newest = await j.store.decision_memory(t2.key, "15m", b2)
        if seq_offset == 0:
            assert before.last_outcome == "STRATEGY_REASON" and newest.last_outcome == "ADJUST"
        else:
            assert before.last_outcome == newest.last_outcome
    finally:
        j.barclock.close()
