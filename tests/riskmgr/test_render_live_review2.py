"""Live review round 2, finding 1 (the COMPLETE wording), as reverted in live review round 3.

Design §B8 "Release texts" gives one `COMPLETE` text: "released: the bar end confirmed break-even; the ARM no longer
manages this position", plus the pinned Q10 policy. §20 Q6 calls the target "break-even" both at the entry and at
the entry plus a buffer (`be_offset_r > 0`). Round 2 added a second text ("confirmed the target") for a buffered pin;
no requirement asks for it, so every release renders the §B8 text whatever the pinned offset. The item order part of
the finding is not built here: it changes the plain text that `tests/notifications` pins byte for byte.

Synthetic ids and levels only.
"""
from __future__ import annotations

import pytest

from src.marketdata.timing import COLD_SKEW, pre_fire_ms
from src.riskmgr.config import ArmSettings
from src.riskmgr.render import (
    COMPLETE_TEXT,
    ItemBody,
    NoticeBody,
    RenderSettings,
    decode_body,
    encode_body,
    outcome_text,
    q10_text,
)
from src.riskmgr.sm11 import ArmState
from tests.riskmgr.test_arm_join import TF, join_rig
from tests.riskmgr.tracker_fakes import FIRST_BAR, FakeConfig, document, image, position, strategy_entry


def item(pid: str, outcome: str, category: str, **kw: object) -> ItemBody:
    base: dict[str, object] = dict(decision_id=int(pid[1:]), decision_bar_end_ms=TF, adapter_id="mock",
                                   position_id=pid, symbol="SYN.A", side="BUY", event="POST", bar_end_ms=TF,
                                   outcome=outcome, category=category)
    base.update(kw)
    return ItemBody(**base)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------- COMPLETE text
@pytest.mark.parametrize("side", ["BUY", "SELL"])
@pytest.mark.parametrize("policy", ["TELL_ONLY", "REARM", None])
def test_complete_always_renders_the_b8_text(side: str, policy: str | None) -> None:
    done = item("P1", "COMPLETE", "INFO", side=side, loosen_policy=policy)
    again = decode_body(encode_body(NoticeBody(notice_id=9, route="main", event="POST", bar_end_ms=TF,
                                               created_ms=TF, items=(done,))))
    line = outcome_text(again.items[0], RenderSettings())
    assert line == f"{COMPLETE_TEXT} ({q10_text(side, policy == 'REARM')})"
    assert "the target;" not in line


@pytest.mark.parametrize("offset,target", [("0", "1.2000"), ("0.1", "1.2010")])
async def test_a_release_through_the_runtime_says_break_even_for_every_pinned_offset(offset: str,
                                                                                    target: str) -> None:
    """The S36b join (real tracker, BarClock, pipeline, sinks, notices): the queued COMPLETE item renders the §B8
    break-even text both at the Q6 default (offset 0, target E) and with a buffer (offset 0.1 R, target E + 0.1 R)."""
    j = join_rig()
    j.tracker._config = FakeConfig(document(strategies=(  # type: ignore[assignment]
        strategy_entry(raw={"be_offset_r": offset, "guards": [FIRST_BAR]}),)))
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
        j.stop_version(target, b2 + 30_000)
        j.snapshot.images = [image(position("P1", stop=target, updated_ms=b2 + 30_000))]
        j.md.live_quote("1.2160", pre_fire_ms(b3, COLD_SKEW, ArmSettings().pre_offset_s) - 1_000)
        j.md.final_bar(b3, "1.2160")
        await j.at(b3 + 2_000)
        assert j.row().state == ArmState.STRATEGY_COMPLETE.value
        done = [i for b in j.outbox.bodies("POST") for i in b.items if i.outcome == "COMPLETE"]
        assert len(done) == 1
        assert outcome_text(done[0], RenderSettings()).startswith(COMPLETE_TEXT)
        assert all("release_at_entry" not in raw.body for raw in j.outbox.rows)
    finally:
        j.barclock.close()
