"""Round-13 hardening of the decision path (plan S38d; design v11.4 §0b "Round-13 findings").

* D4 corrections wired: a broker bar revised after a decision gives a CORRECTED revision (a withdrawn release, a
  re-proposed or withdrawn ADJUST), never a silent release; a release waits for the bar's verification re-read.
* The PRE quote is frozen at fire (`quote_at`): a stream of ticks across the fire never gives
  `pre_quote_after_fire`.
* The ATR fault scope: an indicator fault fails its own guard-input scope; positions past bar 0 (long and short) do
  not depend on FirstBarGuard's inputs; a bar-0 position does.
* Every decision input is recorded (trigger, guard history bars, indicator lineage hashes) and covered by the digest.
* Q10 is the position's pinned policy, rendered per item.

Synthetic instruments, ids, levels and times only; no assertion message carries a value.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from collections.abc import Callable
from typing import Any

import pytest

from src.db.arm_ports import ArmPositionRow, PositionKey
from src.marketdata.hashing import Keyring, content_hash
from src.marketdata.indicators.base import IndicatorSpec, IndicatorValue, Lineage
from src.marketdata.timing import COLD_SKEW, decision_cap_ms, pre_fire_ms
from src.marketdata.types import INDICATOR_FAULTED, M15, Bar, Quality, SeriesId, Source
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.budget import ErrorBudget, Fault
from src.riskmgr.config import ArmSettings, FailSettings
from src.riskmgr.evaluation import PipelineEvaluator, _Reads, decision_inputs
from src.riskmgr.failed import scope_covers_row
from src.riskmgr.guards.first_bar import FIRST_BAR_V1
from src.riskmgr.notices import pinned_loosen_policy
from src.riskmgr.outcomes import RELEASE_UNVERIFIED
from src.riskmgr.registry import REGISTRY
from src.riskmgr.render import (
    COMPLETE_TEXT,
    ItemBody,
    NoticeBody,
    Q10_REARM,
    Q10_TELL,
    decode_body,
    encode_body,
    outcome_text,
    render,
    RenderSettings,
)
from src.riskmgr.sm11 import ArmState
from src.riskmgr.strategy import GuardSpec
from src.riskmgr.tracker import PositionSpecs, first_bar_end, past_guards
from src.riskmgr.types import ScopeKind, SeriesInput, indicator_of_scope, indicator_scope_id, inputs_digest
from src.runtime.optional_md import FaultRelay
from tests.marketdata.test_brokerbars import B as MD_B
from tests.marketdata.test_brokerbars import S1 as MD_S1
from tests.marketdata.test_brokerbars import T as MD_T
from tests.marketdata.test_brokerbars import make as make_bars
from tests.riskmgr.test_arm_join import KEYRING, Join, join_rig
from tests.riskmgr.tracker_fakes import ADAPTER, SYMBOL, T0, FakeGrid, Rig, image, position, rig

D = Decimal
TF = M15.timeframe_ms
run_async = asyncio.run
BID = SeriesId(ADAPTER, SYMBOL, M15, QuoteSide.BID, Source.BROKER)
ASK = SeriesId(ADAPTER, SYMBOL, M15, QuoteSide.ASK, Source.BROKER)


# ---------------------------------------------------------------------------------------------------- the join
async def _to_adjust(j: Join) -> tuple[int, int, int]:
    """Arm, POST of B1 (below the trigger), PRE and POST ADJUST of B2; returns (b1, b2, b3)."""
    await j.tracker.step()
    await j.notices.step()
    cursor = j.row().last_decided_bar
    b1 = cursor + TF
    b2, b3 = b1 + TF, b1 + 2 * TF
    j.md.final_bar(b1, "1.2050")
    fire2 = pre_fire_ms(b2, COLD_SKEW, ArmSettings().pre_offset_s)
    j.md.live_quote("1.2150", fire2 - 1_000)
    await j.at(fire2 + 100)
    j.md.final_bar(b2, "1.2150")
    await j.at(b2 + 2_000)
    assert [(r.outcome, r.revision) for r in j.decisions("POST") if r.bar_end_ms == b2] == [("ADJUST", 0)]
    return b1, b2, b3


async def _to_complete(j: Join) -> int:
    """... then the stop moved to the target and the POST of B3 releases the position; returns b3."""
    _, b2, b3 = await _to_adjust(j)
    j.stop_version("1.2000", b2 + 30_000)
    j.snapshot.images = [image(position("P1", stop="1.2000", updated_ms=b2 + 30_000))]
    fire3 = pre_fire_ms(b3, COLD_SKEW, ArmSettings().pre_offset_s)
    j.md.live_quote("1.2160", fire3 - 1_000)
    await j.at(fire3 + 100)
    j.md.final_bar(b3, "1.2160")
    await j.at(b3 + 2_000)
    assert [r.outcome for r in j.decisions("POST") if r.bar_end_ms == b3] == ["COMPLETE"]
    assert j.row().state == ArmState.STRATEGY_COMPLETE.value
    return b3


async def test_a_bar_revised_after_a_complete_withdraws_the_release() -> None:
    """D4: the release bar is revised below the trigger inside its window: a CORRECTED revision (revision 1,
    superseding the COMPLETE) withdraws the release; the position is managed again (TRACKING), and the CORRECTED
    item goes out at once with the withdrawal label. Never a silent release."""
    j = join_rig()
    try:
        b3 = await _to_complete(j)
        complete = [s for s in j.store.decisions if s.row.event == "POST" and s.row.bar_end_ms == b3][0]
        j.md.revise_bar(b3, "1.1950")                        # the revised close no longer confirms the trigger
        await j.at(b3 + 10_000)
        corrected = [r for r in j.decisions("CORRECTED") if r.bar_end_ms == b3]
        assert [(r.revision, r.supersedes_decision_id) for r in corrected] == [(1, complete.ref.decision_id)]
        assert corrected[0].outcome != "COMPLETE" and corrected[0].trigger_met is False
        assert j.row().state == ArmState.TRACKING.value
        assert j.barclock.counters.corrections == 1 and j.barclock.counters.corrections_shown == 1
        items = [i for b in j.outbox.bodies("CORRECTED") for i in b.items]
        assert [(i.outcome, i.supersedes) for i in items] == [(corrected[0].outcome, complete.ref.decision_id)]
        assert "earlier advice is withdrawn" in render(j.outbox.bodies("CORRECTED")[0]).text
        # the revision is the new recorded decision: an unchanged bar later gives no second revision
        await j.at(b3 + 20_000)
        assert len(j.decisions("CORRECTED")) == 1
    finally:
        j.barclock.close()


async def test_a_bar_revised_after_an_adjust_withdraws_the_advice() -> None:
    """D4: the latest bar's ADJUST, its bar revised below the trigger: CORRECTED (this bar's heads-up is not
    confirmed: `PRE_NOT_CONFIRMED`, the advice withdrawn, the state leaves ADVICE_OUTSTANDING); revised back above
    it: a second revision re-proposes the ADJUST (actionable)."""
    j = join_rig()
    try:
        _, b2, _ = await _to_adjust(j)
        assert j.row().state == ArmState.ADVICE_OUTSTANDING.value
        j.md.revise_bar(b2, "1.2050")
        await j.at(b2 + 10_000)
        first = [r for r in j.decisions("CORRECTED") if r.bar_end_ms == b2]
        assert [(r.revision, r.outcome, r.trigger_met) for r in first] == [(1, "PRE_NOT_CONFIRMED", False)]
        assert j.row().state == ArmState.TRACKING.value
        j.md.revise_bar(b2, "1.2150")
        await j.at(b2 + 20_000)
        again = [r for r in j.decisions("CORRECTED") if r.bar_end_ms == b2]
        assert [(r.revision, r.outcome) for r in again] == [(1, "PRE_NOT_CONFIRMED"), (2, "ADJUST")]
        assert again[1].advice and again[1].proposed_stop is not None
        assert j.row().state == ArmState.ADVICE_OUTSTANDING.value
    finally:
        j.barclock.close()


def _with_unreadable_input(j: Join, bar_end: int) -> None:
    """Add to the kept decision of `bar_end` a recorded history bar whose current hash cannot be read now (a bar of
    the ask series, which the join's market data does not hold)."""
    key = (j.row().ref, bar_end)
    kept = j.barclock._recorded[key]  # noqa: SLF001
    c = D("1.2")
    extra = SeriesInput(ASK.canonical, bar_end - TF, content_hash(KEYRING, ASK.canonical, bar_end - TF, c, c, c, c))
    j.barclock._recorded[key] = replace(kept, rec=replace(kept.rec, inputs=(*kept.rec.inputs, extra)))  # noqa: SLF001


async def test_a_revised_trigger_is_corrected_while_another_input_is_unreadable() -> None:
    """Round 13 review: an input that cannot be read now never hides a genuine change of another one. The release
    bar's trigger is revised while a recorded history bar is unreadable: a CORRECTED revision withdraws the release
    (no RETRY loop, no UNVERIFIABLE audit at the window end)."""
    j = join_rig()
    try:
        b3 = await _to_complete(j)
        _with_unreadable_input(j, b3)
        await j.at(b3 + 7_000)                                          # unchanged trigger: a RETRY only
        assert j.decisions("CORRECTED") == [] and j.barclock.counters.correction_retries >= 1
        j.md.revise_bar(b3, "1.1950")
        await j.at(b3 + 14_000)
        corrected = [r for r in j.decisions("CORRECTED") if r.bar_end_ms == b3]
        assert [r.revision for r in corrected] == [1] and corrected[0].outcome != "COMPLETE"
        assert j.row().state == ArmState.TRACKING.value
        await j.at(decision_cap_ms(b3, M15, COLD_SKEW) + 6_000)
        assert j.store.audits == []
    finally:
        j.barclock.close()


async def test_an_open_compare_is_audited_after_its_position_left_the_live_set() -> None:
    """Round 13 review: the open compare keeps its position, so its `UNVERIFIABLE` audit row is written at the window
    end even when the position closed meanwhile."""
    j = join_rig()
    try:
        b3 = await _to_complete(j)
        _with_unreadable_input(j, b3)
        await j.at(b3 + 7_000)
        row = j.row()
        j.store.rows[row.key] = replace(row, state=ArmState.CLOSED.value, row_version=row.row_version + 1)
        await j.at(decision_cap_ms(b3, M15, COLD_SKEW) + 6_000)
        assert [(a.adapter_id, a.position_id, a.reason) for a in j.store.audits] == [
            (ADAPTER, "P1", "input_unreadable")]
    finally:
        j.barclock.close()


async def test_a_correction_the_state_cannot_take_is_counted_and_audited() -> None:
    """Round 13 review: a confirmed correction of a position that is not deciding now (FAILED) is never dropped
    silently: counted, kept open, and audited `not_deciding` when its window ends."""
    j = join_rig()
    try:
        b3 = await _to_complete(j)
        row = j.row()
        j.store.rows[row.key] = replace(row, state=ArmState.FAILED.value, prior_state=row.state,
                                        row_version=row.row_version + 1)
        j.md.revise_bar(b3, "1.1950")
        await j.at(b3 + 7_000)
        assert j.decisions("CORRECTED") == [] and j.barclock.counters.corrections_not_deciding >= 1
        await j.at(decision_cap_ms(b3, M15, COLD_SKEW) + 6_000)
        assert [(a.position_id, a.reason) for a in j.store.audits] == [("P1", "not_deciding")]
    finally:
        j.barclock.close()


async def test_an_indicator_input_is_rechecked_on_its_own_timeframe() -> None:
    """Round 13 review: the correction re-check reads an indicator's current value at the bucket of the indicator's
    own timeframe (learned from the evaluation), never the decision's."""
    from src.marketdata.ports import IndicatorRead
    from src.marketdata.types import MdGeneration
    from src.riskmgr.corrections import DecisionSummary, RecordedDecision, input_id
    from src.riskmgr.types import ArmEvent, IndicatorInput, lineage_hash
    h1 = 3_600_000
    bucket = 400 * h1
    end = bucket + h1
    value = IndicatorValue(indicator_id="abcdef0123456789", output="atr", bucket_start_ms=bucket, value=D("0.001"),
                           quality=Quality.COMPLETE, warm=True, final_at_ms=end,
                           lineage=Lineage("abcdef0123456789", bucket - 14 * h1, "0123456789abcdef", 0))
    asked: list[int | None] = []

    class HourMd:
        async def indicator(self, indicator_id: str, output: str, *, at_bucket_ms: int | None = None,
                            n: int | None = None, as_of_ms: int | None = None) -> Any:
            asked.append(at_bucket_ms)
            return IndicatorRead((value,) if at_bucket_ms == bucket else (), MdGeneration(1, 1))

    j = join_rig()
    try:
        h = lineage_hash(KEYRING, value.indicator_id, "atr", end, bucket - 14 * h1, "0123456789abcdef", 0)
        ind = IndicatorInput(value.indicator_id, "atr", end, bucket - 14 * h1, h)
        rec = RecordedDecision(decision_id=1, event=ArmEvent.POST, bar_end_ms=end, inputs=(ind,),
                               summary=DecisionSummary(code=None))
        from src.riskmgr.barclock import _Recorded
        kept = _Recorded(f"{ADAPTER}:P1", ADAPTER, "P1", M15, rec, end + TF)
        j.barclock._md = HourMd()  # type: ignore[assignment]  # noqa: SLF001
        j.barclock._ind_tf[value.indicator_id] = h1  # noqa: SLF001  (as `Evaluated.indicator_tf` taught it)
        got = await j.barclock._current_hashes([kept])  # noqa: SLF001
        assert asked == [bucket] and got[input_id(ind)].content_hash == h
    finally:
        j.barclock.close()


def _gated(j: Join, verified: Callable[[SeriesId, int], bool]) -> None:
    """The join's evaluator with md-bars' verification gate (`MdBars.verified`, as the runtime wires it)."""
    j.evaluator = PipelineEvaluator(
        md=j.md, snapshot=j.snapshot, versions=j.versions,  # type: ignore[arg-type]
        decisions=j.store, grid=j.evaluator._grid, calendar=j.evaluator._calendar,  # noqa: SLF001
        settings=ArmSettings(), keyring=lambda: KEYRING, verified=verified)
    j.barclock._evaluator = j.evaluator  # noqa: SLF001


async def test_a_release_waits_for_the_verification_reread() -> None:
    """D4: a COMPLETE is held (no row, `release_awaiting_verification`) while md-bars has not confirmed the bar by
    its verification re-read; once it has, the release is decided. An ADJUST is never held."""
    j = join_rig()
    waiting = {"on": True}
    _gated(j, lambda series, bucket: not waiting["on"])
    try:
        _, b2, b3 = await _to_adjust(j)                      # the ADJUST went out while "awaiting" was True
        j.stop_version("1.2000", b2 + 30_000)
        j.snapshot.images = [image(position("P1", stop="1.2000", updated_ms=b2 + 30_000))]
        j.md.final_bar(b3, "1.2160")
        await j.at(b3 + 2_000)
        assert [r for r in j.decisions("POST") if r.bar_end_ms == b3] == []
        assert j.barclock._reason[(j.row().ref, b3)] == RELEASE_UNVERIFIED  # noqa: SLF001
        assert j.row().state != ArmState.STRATEGY_COMPLETE.value and j.rec.pages == []
        waiting["on"] = False                                # the verification re-read confirmed the bar
        await j.at(b3 + 62_000)
        assert [r.outcome for r in j.decisions("POST") if r.bar_end_ms == b3] == ["COMPLETE"]
    finally:
        j.barclock.close()


@pytest.mark.parametrize("case", ["inconclusive", "failed_reread", "no_time_left"])
async def test_a_release_on_a_bar_never_confirmed_is_never_silent(case: str) -> None:
    """Round 13 review: md-bars' verification re-read did not positively confirm the release bar (the re-read
    answer lacked the bucket, the re-read failed, or the bar became final with no verification time left). The
    COMPLETE is held until the bar's cap, then the bar is spanned (`release_awaiting_verification`, paged): the
    position is never released on it, and never silently."""
    md = await _md_case(case)
    assert not md.bars.verified(MD_S1, MD_B) and not md.bars.awaiting_verification(MD_S1, MD_B)
    j = join_rig()
    _gated(j, lambda series, bucket: md.bars.verified(MD_S1, MD_B))
    try:
        _, b2, b3 = await _to_adjust(j)
        j.stop_version("1.2000", b2 + 30_000)
        j.snapshot.images = [image(position("P1", stop="1.2000", updated_ms=b2 + 30_000))]
        j.md.final_bar(b3, "1.2160")
        await j.at(b3 + 2_000)
        await j.at(b3 + 120_000)
        assert [r for r in j.decisions("POST") if r.bar_end_ms == b3 and r.outcome == "COMPLETE"] == []
        assert j.row().state != ArmState.STRATEGY_COMPLETE.value
        cap = decision_cap_ms(b3, M15, COLD_SKEW)
        await j.at(cap + 1_000)
        spans = [r for r in j.decisions("POST") if r.bar_end_ms == b3]
        assert [(r.outcome, r.reason) for r in spans] == [("UNRESOLVED_RANGE", RELEASE_UNVERIFIED)]
        assert j.row().state != ArmState.STRATEGY_COMPLETE.value
        assert any(RELEASE_UNVERIFIED in page[2] for page in j.rec.pages)
    finally:
        j.barclock.close()


async def _md_case(case: str) -> Any:
    """An md-bars rig whose bucket `MD_B` ended in one of the unconfirmed cases."""
    from src.marketdata.types import Transient
    if case == "no_time_left":
        rig = make_bars(start=MD_T - 5_000)
        rig.broker.lag_ms = 880_000                       # the bar appears 20 s before its cap
        await rig.run_to(MD_T + 905_000, every=5_000)
        assert MD_B in rig.bars.unverified(MD_S1)
        return rig
    rig = make_bars()
    await rig.run_to(MD_T + 30_000)
    assert rig.bars.awaiting_verification(MD_S1, MD_B)
    if case == "inconclusive":
        rig.broker.hidden.add(MD_B)                       # the re-read answer lacks the bucket (truncated)
    else:
        rig.broker.fail = lambda symbol, lo, hi: Transient("timeout")
    await rig.run_to(MD_T + 70_000)
    assert rig.bars.counters.verify_inconclusive >= 1
    return rig


async def test_a_pre_quote_is_frozen_at_fire_under_ten_ticks_a_second() -> None:
    """A liquid market: ticks every 100 ms across the fire; the BarClock's pass lands after more ticks arrived. The
    PRE reads the quote as it was at fire, never `pre_quote_after_fire`."""
    j = join_rig()
    try:
        await j.tracker.step()
        cursor = j.row().last_decided_bar
        b1 = cursor + TF
        b2 = b1 + TF
        j.md.final_bar(b1, "1.2050")
        await j.at(b1 + 2_000)
        fire = pre_fire_ms(b2, COLD_SKEW, ArmSettings().pre_offset_s)
        for t in range(fire - 2_000, fire + 1_000, 100):
            j.md.live_quote("1.2150", t)
        await j.at(fire + 900)
        pre = [r for r in j.decisions("PRE") if r.bar_end_ms == b2]
        assert [(r.outcome, r.reason) for r in pre] == [("ADJUST", None)]
    finally:
        j.barclock.close()


# ---------------------------------------------------------------------------------------------------- ATR fault scope
def _armed(side: str) -> tuple[Rig, ArmPositionRow]:
    """A position armed through `tracker.step()` 5 minutes into its fill bucket (kind (a) with FirstBarGuard): the
    cursor is the fill bucket's start (bar 0 not decided, no span)."""
    r = rig()
    stop = "1.1900" if side == "BUY" else "1.2100"
    r.snapshot.images = [image(position("P1", side=side, stop=stop))]
    run_async(r.tracker.step())
    return r, r.store.row()


def _guard_leases(r: Rig) -> set[str]:
    return {sid for sid in r.tracker.leases() if isinstance(r.md.defined.get(sid), IndicatorSpec)}


def _bar0_decided(r: Rig, bar0_end: int) -> ArmPositionRow:
    """The BarClock decided bar 0 (the cursor moved to its end): the stored row as the next pass reads it."""
    row = r.store.row()
    r.store.rows[row.key] = replace(row, last_decided_bar=bar0_end, decision_seq=row.decision_seq + 1,
                                    row_version=row.row_version + 1)
    return r.store.row()


def test_a_position_armed_mid_bucket_keeps_the_guard_inputs_until_bar_0_is_decided_and_capped() -> None:
    """Round 13 review (critical): arming sets the cursor at the fill bucket's start, so "cursor set" is not "past
    bar 0". Armed mid-bucket, long and short, the position keeps FirstBarGuard's ATR lease and its indicator-scope
    coverage (and, for a short, the bid series); after bar 0 is decided they stay until bar 0's `decision_cap`
    (a correction of bar 0 can still read them), and only then are they released and dropped from coverage."""
    from src.riskmgr.budget import ScopeRef
    for side in ("BUY", "SELL"):
        r, row = _armed(side)
        assert row.last_decided_bar == T0 and row.decision_seq == 0            # no span: bar 0 is still ahead
        atr = [s for s in r.md.defined.values() if isinstance(s, IndicatorSpec)]
        assert len(atr) == 1
        scope = indicator_scope_id(ADAPTER, SYMBOL, atr[0].indicator_id)
        assert len(_guard_leases(r)) == 1                                        # the ATR lease is held
        series = r.tracker.series_of(row)
        assert series is not None and scope in series
        assert scope_covers_row(ScopeRef(ScopeKind.SERIES, scope), row, series)   # waits on bar 0
        if side == "SELL":
            assert BID.canonical in series                                       # the guard's bid history
        # bar 0 decided, its cap not passed: kept (lease renewals included)
        bar0_end = T0 + TF
        cap = decision_cap_ms(bar0_end, M15, COLD_SKEW)
        row = _bar0_decided(r, bar0_end)
        r.clock.now = cap - 1_000
        run_async(r.tracker.step())
        assert len(_guard_leases(r)) == 1 and scope in (r.tracker.series_of(row) or ())
        # bar 0's cap passed: the ATR lease is released and the guard inputs leave the coverage
        r.clock.now = cap + 16_000
        run_async(r.tracker.step())
        assert _guard_leases(r) == set() and r.tracker.leases()
        series = r.tracker.series_of(row)
        assert series is not None and scope not in series
        assert not scope_covers_row(ScopeRef(ScopeKind.SERIES, scope), row, series)   # still advising
        if side == "SELL":
            assert BID.canonical not in series                                   # a short never needs the bid now
            assert not scope_covers_row(ScopeRef(ScopeKind.SERIES, BID.canonical), row, series)
        else:
            assert BID.canonical in series                                       # the long's trigger series


def test_past_guards_needs_bar_0_itself_and_its_cap() -> None:
    _, row = _armed("BUY")
    bar0_end = T0 + TF
    cap = decision_cap_ms(bar0_end, M15, COLD_SKEW)
    assert not past_guards(row, bar0_end, guard_cap_ms=cap, now_ms=cap + 1)      # cursor at the bucket start
    decided = replace(row, last_decided_bar=bar0_end)
    assert not past_guards(decided, bar0_end, guard_cap_ms=cap, now_ms=cap - 1)  # bar 0's cap not passed
    assert past_guards(decided, bar0_end, guard_cap_ms=cap, now_ms=cap)
    assert not past_guards(decided, None, guard_cap_ms=None, now_ms=cap)         # horizon unknown: kept
    assert first_bar_end(FakeGrid(), ADAPTER, SYMBOL, M15, T0 + 5 * 60_000) == bar0_end


def test_the_guard_horizon_follows_the_pinned_anchor() -> None:
    """The horizon is asked of the pinned guards, not assumed: FirstBarGuard's default anchor (`fill_bucket`) ends
    at the first bar end; under `next_full_bucket` bar 0 is the next full bucket, so the guard inputs are kept one
    bar longer."""
    from src.riskmgr.strategy import StrategyState
    from src.riskmgr.tracker import GUARD_SCAN_BARS, guard_horizon_end, open_bar_ends_after
    from tests.riskmgr.tracker_fakes import FIRST_BAR, KIND_A
    _, row = _armed("BUY")
    ends = open_bar_ends_after(FakeGrid(), ADAPTER, SYMBOL, M15, row.entry_time_ms, GUARD_SCAN_BARS)
    assert ends == [T0 + k * TF for k in range(1, GUARD_SCAN_BARS + 1)]
    state = StrategyState.decode(row.strategy_state_json)
    assert state is not None
    for anchor, horizon in (("fill_bucket", T0 + TF), ("next_full_bucket", T0 + 2 * TF)):
        guard = dict(FIRST_BAR, params={"anchor": anchor})
        params = REGISTRY.parse_params(KIND_A, 1, {"guards": [guard]})
        assert guard_horizon_end(REGISTRY, row, M15, params, state, ends) == horizon
    assert guard_horizon_end(REGISTRY, row, M15, params, state, ends[:2]) is None    # applies at the last: kept
    assert guard_horizon_end(REGISTRY, row, M15, params, state, []) is None


def test_guard_only_inputs_are_read_and_recorded_only_where_a_guard_applies() -> None:
    """Round 13 review: FirstBarGuard's bid history and ATR belong to bar 0's decision only; a later bar neither
    reads them (their leases are released) nor records them."""
    from src.riskmgr.evaluation import needs_of
    from src.riskmgr.strategy import StrategyState
    from src.riskmgr.tracker import guards_apply
    from src.riskmgr.types import ArmEvent, BarContext, EntryTimeSource
    from tests.riskmgr.tracker_fakes import strategy_entry
    params = strategy_entry().params
    strategy = REGISTRY.strategy(params.kind, params.impl_version)
    assert strategy is not None
    for side in ("BUY", "SELL"):
        needs = needs_of(REGISTRY, strategy, params, side)  # type: ignore[arg-type]
        assert needs is not None and needs.guard_indicators
        core = needs.without_guards()
        assert core.indicators == () and not set(core.history) & set(needs.guard_history)
        assert core.trigger_side == needs.trigger_side
    fill = T0 + 60_000
    ctx = BarContext(position_id="P1", adapter_id=ADAPTER, symbol=SYMBOL, side="BUY", bar_spec=M15,
                     event=ArmEvent.POST, bar_end_ms=T0 + TF, bar_index=0, entry=D("1.2"), entry_time_ms=fill,
                     entry_time_source=EntryTimeSource.STREAM, s0=D("1.19"))
    state = StrategyState.decode(_armed("BUY")[1].strategy_state_json)
    assert state is not None
    assert guards_apply(REGISTRY, params, ctx, state)                                   # bar 0
    assert not guards_apply(REGISTRY, params, replace(ctx, bar_end_ms=T0 + 2 * TF, bar_index=1), state)


def test_specs_drop_only_guard_inputs_past_bar_0() -> None:
    long_specs, short_specs = _specs(QuoteSide.BID), _specs(QuoteSide.ASK)
    atr = long_specs.indicators[0]
    scope = indicator_scope_id(ADAPTER, SYMBOL, atr.indicator_id)
    assert indicator_of_scope(scope) == atr.indicator_id and indicator_of_scope(BID.canonical) is None
    for specs in (long_specs, short_specs):
        assert atr not in specs.all(past_guards=True) and atr in specs.all()
        assert scope in specs.scope_series() and scope not in specs.scope_series(past_guards=True)
    assert BID.canonical not in short_specs.scope_series(past_guards=True)
    assert BID.canonical in long_specs.scope_series(past_guards=True)


def _specs(side: QuoteSide) -> PositionSpecs:
    """Kind (a) with FirstBarGuard: trigger on the exit side; the guard reads the bid 15m history and ATR on it."""
    inc = D("0.00001")
    atr_need = FIRST_BAR_V1.required_indicators(GuardSpec(FIRST_BAR_V1.KIND, FIRST_BAR_V1.VERSION,
                                                          FIRST_BAR_V1.parse_params({})))[0]
    atr = atr_need.resolve(ADAPTER, SYMBOL, inc)
    trigger = BID if side is QuoteSide.BID else ASK
    series = tuple(sorted({trigger, BID}, key=lambda s: s.canonical))
    guard_series = () if trigger == BID else (BID,)
    return PositionSpecs(series, (atr,), (), guard_series=guard_series, guard_indicators=(atr,))


def _row(side: str, last_decided_bar: int | None) -> ArmPositionRow:
    return ArmPositionRow(
        key=PositionKey(adapter_id=ADAPTER, position_id="P1"), account_id="ACC", symbol=SYMBOL, side=side,  # type: ignore[arg-type]
        state=ArmState.TRACKING.value, edge_code="e", attribution_source="assignment", strategy_code="s",
        strategy_version=1, strategy_kind="break_even_on_risk_close", strategy_impl_version=1,
        strategy_params_json="{}", guards_hash="g", bar_spec="15m", config_doc_hash="c", entry_level=D("1.2"),
        entry_time_ms=0, entry_time_source="STREAM", s0_level=D("1.19"), s0_source="STREAM", pinned_units=D("1"),
        last_decided_bar=last_decided_bar, armed_at_ms=0, updated_at_ms=0)


class _Calendar:
    def state(self, adapter_id: str, symbol: str) -> Any:
        from src.calendar.machine import SessionState
        return SessionState.OPEN


class _Health:
    def is_streaming(self, adapter_id: str) -> bool:
        return True

    def rest_failing_everywhere(self, adapter_id: str) -> bool | None:
        return False


class _Pg:
    def last_fenced_commit_ms(self) -> int | None:
        return None

    def sweep_pg_unreachable(self) -> bool:
        return False


def test_an_indicator_fault_files_its_own_scope_never_the_input_series() -> None:
    faults: list[Fault] = []
    budget = ErrorBudget(FailSettings(), calendar=_Calendar(), health=_Health(), pg=_Pg())  # type: ignore[arg-type]
    budget.listen(faults.append)
    relay = FaultRelay(lambda: 1_000, input_series=lambda iid: (BID, ASK))
    relay.bind(budget)
    relay.indicator("abcdef0123456789", INDICATOR_FAULTED)
    filed = list(faults)
    scopes = [f.scope for f in filed]
    assert [(s.kind, s.scope_id) for s in scopes] == [
        (ScopeKind.SERIES, indicator_scope_id(ADAPTER, SYMBOL, "abcdef0123456789"))]
    assert all(f.reason == INDICATOR_FAULTED for f in filed)
    assert not any(f.scope.scope_id in (BID.canonical, ASK.canonical) for f in filed)   # the series stay readable


# ---------------------------------------------------------------------------------------------------- decision inputs
def _bar(series: SeriesId, start: int, close: str) -> Bar:
    c = D(close)
    return Bar(series=series, bucket_start_ms=start, open=c, high=c, low=c, close=c, final_at_ms=start + TF + 1)


def test_every_declared_input_of_a_golden_bar_0_decision_is_recorded() -> None:
    """The conformance check of round 13: FirstBarGuard's declared history and indicator needs all appear in the
    recorded inputs of a bar-0 decision (a short: trigger on the ask, the guard on the bid), and the digest covers
    every one of them (a missing one changes it)."""
    g = GuardSpec(FIRST_BAR_V1.KIND, FIRST_BAR_V1.VERSION, FIRST_BAR_V1.parse_params({}))
    t = 400 * TF
    history = []
    for need in FIRST_BAR_V1.required_history(g):
        sid = SeriesId(ADAPTER, SYMBOL, need.bar_spec, need.side, Source.BROKER)
        history.append((sid, tuple(_bar(sid, t - (k + 1) * TF, "1.20") for k in range(need.bars))))
    inc = D("0.00001")
    ind_values = []
    tf_of = {}
    for need in FIRST_BAR_V1.required_indicators(g):
        spec = need.resolve(ADAPTER, SYMBOL, inc)
        tf_of[spec.indicator_id] = spec.series.spec.timeframe_ms
        value = IndicatorValue(indicator_id=spec.indicator_id, output="atr", bucket_start_ms=t - TF,
                               value=D("0.001"), quality=Quality.COMPLETE, warm=True, final_at_ms=t,
                               lineage=Lineage(spec.indicator_id, t - 20 * TF, "0123456789abcdef", 0))
        ind_values.append(((spec.indicator_id, "atr"), (value,)))
    ctx = SimpleNamespace(history=tuple(history), indicators=tuple(ind_values))
    reads = _Reads(SimpleNamespace(ctx=ctx), ASK, KEYRING_HASH, None, tf_of)  # type: ignore[arg-type]
    got = decision_inputs(reads, t, KEYRING)
    recorded_series = {i.series_id for i in got.series}
    assert got.series[0] == SeriesInput(ASK.canonical, t, KEYRING_HASH)                # the trigger first
    for sid, bars in history:
        assert sid.canonical in recorded_series
        assert {b.bucket_end_ms for b in bars} <= {i.bar_end_ms for i in got.series if i.series_id == sid.canonical}
    assert {i.indicator_id for i in got.indicators} == set(tf_of)
    full = inputs_digest(KEYRING, got.series, got.indicators)
    assert full != inputs_digest(KEYRING, got.series[:1], ())


KEYRING_HASH = "01" + "0" * 32


# ---------------------------------------------------------------------------------------------------- Q10
def test_complete_texts_follow_each_positions_pinned_policy() -> None:
    base = dict(decision_id=1, decision_bar_end_ms=TF, adapter_id=ADAPTER, symbol=SYMBOL, side="BUY", event="POST",
                bar_end_ms=TF, outcome="COMPLETE", category="INFO")
    rearm = ItemBody(position_id="P1", loosen_policy="REARM", **base)  # type: ignore[arg-type]
    tell = ItemBody(position_id="P2", loosen_policy="TELL_ONLY", **base)  # type: ignore[arg-type]
    body = NoticeBody(notice_id=9, route="main", event="POST", bar_end_ms=TF, created_ms=TF, items=(rearm, tell))
    again = decode_body(encode_body(body))
    assert [i.loosen_policy for i in again.items] == ["REARM", "TELL_ONLY"]
    text = render(again).text
    assert f"{COMPLETE_TEXT} ({Q10_REARM})" in text and f"{COMPLETE_TEXT} ({Q10_TELL})" in text
    loosened = replace(rearm, outcome="LOOSENED_AFTER_COMPLETE")
    assert Q10_TELL in outcome_text(loosened, RenderSettings())               # always tell-only
    assert not hasattr(RenderSettings(), "rearm_on_loosen")


def test_the_pinned_policy_is_read_from_the_stored_pin() -> None:
    """Two positions pinned to different `rearm_on_loosen` values get their own policy (never a node setting)."""
    import json
    from tests.riskmgr.kind_a import strategy_row
    rows = {}
    for value in ("on", "off"):
        raw = dict(strategy_row(rearm_on_loosen=value))
        guards = raw.pop("guards")
        rows[value] = replace(_row("BUY", None), strategy_params_json=json.dumps(raw),
                              guards_json=json.dumps(guards))
    assert pinned_loosen_policy(REGISTRY, rows["on"]) == "REARM"
    assert pinned_loosen_policy(REGISTRY, rows["off"]) == "TELL_ONLY"
    assert pinned_loosen_policy(REGISTRY, replace(rows["on"], strategy_kind="unknown_kind")) is None


def test_the_design_states_one_pre_valid_until_formula() -> None:
    """§B5.2 states `fire + 10 s` only (the old `min(fire + 10 s, T + p50 − 30 s)` survives only as history in
    §0b's deviation row)."""
    from pathlib import Path
    text = Path(__file__).resolve().parents[2].joinpath("docs/design/automated-risk-manager.md").read_text("utf-8")
    assert "valid-until is `min(" not in text
    assert "its valid-until is `fire + 10 s`" in text
