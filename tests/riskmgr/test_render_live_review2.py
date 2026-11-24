"""Live review round 2, finding 1 (the COMPLETE wording; design §B8, §20 Q6).

`COMPLETE` says "the bar end confirmed break-even" (the §B8 text) when the position's pinned target is the entry
itself (`release_at_entry`, from `SlStrategy.target_is_entry` of the pin: `be_offset_r = 0`, the Q6 default), and
"the target" when it is not (a buffer). An unknown pin (an older body) keeps the §B8 text, the Q6 default. The item
order part of the finding is not built here: it changes the plain text that `tests/notifications` pins byte for byte.

Synthetic ids and levels only.
"""
from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal

import pytest

from src.db.arm_ports import ArmPositionRow, PositionKey
from src.marketdata.timing import COLD_SKEW, pre_fire_ms
from src.riskmgr.config import ArmSettings
from src.riskmgr.notices import pinned_target_is_entry
from src.riskmgr.registry import REGISTRY
from src.riskmgr.render import (
    COMPLETE_TARGET_TEXT,
    COMPLETE_TEXT,
    ItemBody,
    NoticeBody,
    RenderSettings,
    decode_body,
    encode_body,
    outcome_text,
)
from src.riskmgr.sm11 import ArmState
from src.riskmgr.strategies.break_even_on_risk_close import BREAK_EVEN_ON_RISK_CLOSE_V1
from tests.riskmgr.kind_a import strategy_row
from tests.riskmgr.test_arm_join import TF, join_rig
from tests.riskmgr.test_pipeline import params
from tests.riskmgr.tracker_fakes import image, position

D = Decimal


def item(pid: str, outcome: str, category: str, **kw: object) -> ItemBody:
    base: dict[str, object] = dict(decision_id=int(pid[1:]), decision_bar_end_ms=TF, adapter_id="mock",
                                   position_id=pid, symbol="SYN.A", side="BUY", event="POST", bar_end_ms=TF,
                                   outcome=outcome, category=category)
    base.update(kw)
    return ItemBody(**base)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------- COMPLETE text
@pytest.mark.parametrize("at_entry,text", [(True, COMPLETE_TEXT), (False, COMPLETE_TARGET_TEXT),
                                           (None, COMPLETE_TEXT)])
def test_complete_says_the_target_when_the_pinned_target_is_not_the_entry(at_entry: bool | None, text: str) -> None:
    done = item("P1", "COMPLETE", "INFO", release_at_entry=at_entry, loosen_policy="TELL_ONLY")
    again = decode_body(encode_body(NoticeBody(notice_id=9, route="main", event="POST", bar_end_ms=TF,
                                               created_ms=TF, items=(done,))))
    assert again.items[0].release_at_entry is at_entry
    line = outcome_text(again.items[0], RenderSettings())
    assert line.startswith(text) and ("break-even" in line) is (at_entry is not False)
    if at_entry is None:                                  # additive key: an older body keeps its exact JSON
        assert "release_at_entry" not in encode_body(again)


def _pinned_row(offset: str) -> ArmPositionRow:
    raw = dict(strategy_row(be_offset_r=offset))
    guards = raw.pop("guards")
    return ArmPositionRow(
        key=PositionKey(adapter_id="mock", position_id="P1"), account_id="ACC", symbol="SYN.A", side="BUY",
        state=ArmState.TRACKING.value, edge_code="e", attribution_source="assignment", strategy_code="s",
        strategy_version=1, strategy_kind="break_even_on_risk_close", strategy_impl_version=1,
        strategy_params_json=json.dumps(raw), guards_json=json.dumps(guards), guards_hash="g", bar_spec="15m",
        config_doc_hash="c", entry_level=D("1.2"), entry_time_ms=0, entry_time_source="STREAM", s0_level=D("1.19"),
        s0_source="STREAM", pinned_units=D("1"), last_decided_bar=None, armed_at_ms=0, updated_at_ms=0)


def test_the_q6_fact_is_read_from_each_positions_pin() -> None:
    assert pinned_target_is_entry(REGISTRY, _pinned_row("0")) is True
    assert pinned_target_is_entry(REGISTRY, _pinned_row("0.1")) is False
    assert pinned_target_is_entry(REGISTRY, replace(_pinned_row("0"), strategy_kind="unknown_kind")) is None
    assert pinned_target_is_entry(REGISTRY, replace(_pinned_row("0"), strategy_params_json="[")) is None
    assert BREAK_EVEN_ON_RISK_CLOSE_V1.target_is_entry(params(be_offset_r="0")) is True
    assert BREAK_EVEN_ON_RISK_CLOSE_V1.target_is_entry(params(be_offset_r="0.25")) is False


async def test_a_release_through_the_runtime_says_break_even_at_the_default_offset() -> None:
    """The S36b join (real tracker, BarClock, pipeline, sinks, notices): the queued COMPLETE item carries
    `release_at_entry` from the pin (Q6 default, offset 0) and renders the §B8 break-even text."""
    j = join_rig()
    try:
        await j.tracker.step()
        await j.notices.step()
        b1 = j.row().last_decided_bar + TF
        b2, b3 = b1 + TF, b1 + 2 * TF
        j.md.final_bar(b1, "1.2050")
        fire2 = pre_fire_ms(b2, COLD_SKEW, ArmSettings().pre_offset_s)
        j.md.live_quote("1.2150", fire2 - 1_000)
        await j.at(fire2 + 100)
        j.md.final_bar(b2, "1.2150")
        await j.at(b2 + 2_000)
        assert j.row().state == ArmState.ADVICE_OUTSTANDING.value
        j.stop_version("1.2000", b2 + 30_000)
        j.snapshot.images = [image(position("P1", stop="1.2000", updated_ms=b2 + 30_000))]
        j.md.live_quote("1.2160", pre_fire_ms(b3, COLD_SKEW, ArmSettings().pre_offset_s) - 1_000)
        j.md.final_bar(b3, "1.2160")
        await j.at(b3 + 2_000)
        assert j.row().state == ArmState.STRATEGY_COMPLETE.value
        done = [i for b in j.outbox.bodies("POST") for i in b.items if i.outcome == "COMPLETE"]
        assert [i.release_at_entry for i in done] == [True]
        assert outcome_text(done[0], RenderSettings()).startswith(COMPLETE_TEXT)
    finally:
        j.barclock.close()
