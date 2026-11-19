"""Round-14 decision path (plan S38g; design v11.7 §0b "Round-14 findings: decision path, liveness and sleep").

* A stop move that answers an outstanding final ADJUST is never discounted by AT_PRE (long and short in
  `tests/riskmgr/test_pipeline.py`; here through the runtime join, with a POST on time and one DELAYED until after
  the next bar's PRE).
* PRIOR_VERSION direction: a loosened amend seen only through an untimed REST row is tested at the farther level,
  so the bar gives BELOW_TRIGGER, not ADJUST.

Synthetic instruments, ids, levels and times only; no assertion message carries a value.
"""
from __future__ import annotations

from decimal import Decimal

from src.db.arm_ports import HandleVersionRow
from src.marketdata.timing import COLD_SKEW, pre_fire_ms
from src.marketdata.types import M15
from src.riskmgr.config import ArmSettings
from src.riskmgr.sm11 import ArmState
from tests.riskmgr.test_arm_join import Join, join_rig
from tests.riskmgr.tracker_fakes import image, position

D = Decimal
TF = M15.timeframe_ms


def _fire(bar_end: int) -> int:
    return pre_fire_ms(bar_end, COLD_SKEW, ArmSettings().pre_offset_s)


async def _armed(j: Join) -> tuple[int, int, int]:
    """Arm (late: one span); returns the next three bar ends after the cursor."""
    await j.tracker.step()
    await j.notices.step()
    cursor = j.row().last_decided_bar
    assert cursor is not None
    return cursor + TF, cursor + 2 * TF, cursor + 3 * TF


def _move_stop(j: Join, level: str, at_ms: int) -> None:
    j.stop_version(level, at_ms)
    j.snapshot.images = [image(position("P1", stop=level, updated_ms=at_ms))]


# ---------------------------------------------------------------------------------------------------- final advice
async def test_golden_a_move_on_final_advice_releases_at_the_next_bar_end() -> None:
    """Long, E 1.2000, S 1.1900 (risk 0.0100). Bar n (B2) closes above the trigger: final POST ADJUST to E
    (ADVICE_OUTSTANDING). Bar n+1's PRE proposes the same move; the user moves to E 20 s before bar n+1 ends; bar
    n+1 closes between E and E + risk. Expected: COMPLETE (the move answered final advice), not PRE_NOT_CONFIRMED,
    no pre_basis, caveat `moved_on_final_advice`."""
    j = join_rig()
    try:
        b1, b2, b3 = await _armed(j)
        j.md.final_bar(b1, "1.2050")
        j.md.live_quote("1.2150", _fire(b2) - 1_000)
        await j.at(_fire(b2) + 100)
        j.md.final_bar(b2, "1.2150")
        await j.at(b2 + 2_000)
        assert [r.outcome for r in j.decisions("POST") if r.bar_end_ms == b2] == ["ADJUST"]
        assert j.row().state == ArmState.ADVICE_OUTSTANDING.value
        j.md.live_quote("1.2150", _fire(b3) - 1_000)
        await j.at(_fire(b3) + 100)
        assert [r.outcome for r in j.decisions("PRE") if r.bar_end_ms == b3] == ["ADJUST"]
        _move_stop(j, "1.2000", b3 - 20_000)
        j.md.final_bar(b3, "1.2050")
        await j.at(b3 + 2_000)
        post = [r for r in j.decisions("POST") if r.bar_end_ms == b3]
        assert [(r.outcome, r.trigger_met, r.stop_basis) for r in post] == [("COMPLETE", True, "STREAM")]
        assert "moved_on_final_advice" in post[0].caveats
        assert j.row().state == ArmState.STRATEGY_COMPLETE.value and j.row().pre_basis_json is None
    finally:
        j.barclock.close()


async def test_golden_a_post_delayed_until_after_the_next_pre_still_counts_the_move() -> None:
    """The same, with bar n's POST DELAYED (its broker bar arrives only after bar n+1's PRE fired): the late final
    ADJUST is still the advice the move answers, so bar n+1 releases."""
    j = join_rig()
    try:
        b1, b2, b3 = await _armed(j)
        j.md.final_bar(b1, "1.2050")
        j.md.live_quote("1.2150", _fire(b2) - 1_000)
        await j.at(_fire(b2) + 100)
        await j.at(b2 + 2_000)                                   # B2's broker bar not there yet: POST pending
        assert [r for r in j.decisions("POST") if r.bar_end_ms == b2] == []
        j.md.live_quote("1.2150", _fire(b3) - 1_000)
        await j.at(_fire(b3) + 100)                              # bar n+1's PRE fires before bar n's POST
        assert [r.outcome for r in j.decisions("PRE") if r.bar_end_ms == b3] == ["ADJUST"]
        j.md.final_bar(b2, "1.2150")
        await j.at(_fire(b3) + 5_000)                            # bar n's POST, delayed past bar n+1's PRE
        post2 = [r for r in j.decisions("POST") if r.bar_end_ms == b2]
        assert [r.outcome for r in post2] == ["ADJUST"] and "delayed" in post2[0].caveats
        assert j.row().state == ArmState.ADVICE_OUTSTANDING.value
        _move_stop(j, "1.2000", b3 - 20_000)
        j.md.final_bar(b3, "1.2050")
        await j.at(b3 + 2_000)
        post3 = [r for r in j.decisions("POST") if r.bar_end_ms == b3]
        assert [(r.outcome, r.trigger_met) for r in post3] == [("COMPLETE", True)]
        assert "moved_on_final_advice" in post3[0].caveats and j.row().pre_basis_json is None
    finally:
        j.barclock.close()


# ---------------------------------------------------------------------------------------------------- PRIOR_VERSION
async def test_golden_a_loosened_amend_seen_only_through_an_untimed_rest_row_is_below_trigger() -> None:
    """Long, E 1.2000, S 1.1900 (stream). The user loosened to 1.1800 before B1 ended; the engine saw that amend only
    through an untimed REST row (watermark-labelled, no broker time). B1 closes at 1.2150: above the old trigger
    (E + 0.0100), below the new one (E + 0.0200). The trigger uses the farther level (PRIOR_VERSION direction rule):
    BELOW_TRIGGER, not ADJUST."""
    j = join_rig()
    try:
        b1, b2, _ = await _armed(j)
        j.seq += 1
        j.versions.history["P1"].append(HandleVersionRow(
            handle_kind="POSITION_FIELD", handle_id="P1", seq_id=j.seq, broker_time_ms=None, origin="snapshot",
            status="OPEN", level=D("1.1800"), units=D("1000"), live=True))
        j.snapshot.images = [image(position("P1", stop="1.1800", updated_ms=b1 - 60_000))]
        j.md.final_bar(b1, "1.2150")
        j.md.live_quote("1.2150", _fire(b2) - 1_000)
        await j.at(b1 + 2_000)
        post = [r for r in j.decisions("POST") if r.bar_end_ms == b1]
        assert [(r.outcome, r.reason, r.trigger_met, r.stop_basis) for r in post] == [
            ("STRATEGY_REASON", "BELOW_TRIGGER", False, "PRIOR_VERSION")]
    finally:
        j.barclock.close()
