"""Live review round 4: a CORRECTED item for a bar older than the latest decided bar, through the BarClock.

Design §B6.6: "A CORRECTED item for a bar older than the position's latest decided bar is INFO only ... never
actionable". Every join test revised the latest bar, so the branches of `_revision_write` that keep the row's
strategy state, basis and acknowledged hash (the older-bar case) never ran. Tests only; synthetic levels.
"""
from __future__ import annotations

from src.marketdata.timing import COLD_SKEW, decision_cap_ms
from src.marketdata.types import M15
from tests.riskmgr.test_arm_join import join_rig
from tests.riskmgr.test_round13 import _to_adjust


async def test_an_older_bar_revised_after_a_newer_decision_is_info_only_and_keeps_the_row() -> None:
    j = join_rig()
    try:
        _, b2, b3 = await _to_adjust(j)
        j.md.final_bar(b3, "1.2050")                       # b3 below the trigger: a newer final decision
        await j.at(b3 + 500)
        b3_post = [r for r in j.decisions("POST") if r.bar_end_ms == b3]
        assert len(b3_post) == 1 and j.decisions("CORRECTED") == []
        snap = j.row()
        assert snap.last_decided_bar == b3 and snap.decision_seq == b3_post[0].decision_seq
        j.md.revise_bar(b2, "1.2050")                      # b2 revised below the trigger, inside its window
        j.barclock._corrections_at = None                  # noqa: SLF001  (the next re-check runs at once)
        await j.at(decision_cap_ms(b2, M15, COLD_SKEW) - 200)
        corrected = [r for r in j.decisions("CORRECTED") if r.bar_end_ms == b2]
        assert len(corrected) == 1
        c = corrected[0]
        assert (c.revision, c.category, c.advice) == (1, "INFO", False)
        assert c.decision_seq > b3_post[0].decision_seq
        after = j.row()
        assert after.last_decided_bar == b3 and after.decision_seq == snap.decision_seq + 1
        assert after.last_decided_hash == snap.last_decided_hash
        assert (after.pre_basis_json, after.pre_basis_bar) == (snap.pre_basis_json, snap.pre_basis_bar)
        assert after.strategy_state_json == snap.strategy_state_json and after.state == snap.state
        await j.at(decision_cap_ms(b2, M15, COLD_SKEW))     # the notices' timer pass sends the item
        items = [i for b in j.outbox.bodies("CORRECTED") for i in b.items if i.bar_end_ms == b2]
        assert items and all(i.category == "INFO" and not i.advice for i in items)   # never actionable
    finally:
        j.barclock.close()
