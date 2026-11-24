"""Live review F2 / F3 on the HTML email part.

F2 (reverted in round 4): every break_even_on_risk_close target is "break-even" in the headline and preheader, at
the entry, at the entry plus a buffer or rounded off an off-grid entry, and with the entry unknown (§20 Q6, Q1 (a);
§B8 fixes no ADJUST headline). The preheader states the level; other strategy kinds keep the generic wording.
F3: a missing_stop_loss state with reason stop_kind_inconsistent gets its own action (a STOP child at or beyond the
market), not the generic "an input is unknown" text; it is still capped at SOFT.
"""
from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

import pytest

from src.notifications.email_html import (
    STOP_KIND_INCONSISTENT_ACTION,
    UNKNOWN_ACTION,
    UNKNOWN_REASONS,
    classify,
    render_alert_html,
    rule_action,
)
from src.notifications.email_html_arm import adjust_headline, arm_preheader, render_arm_html
from src.oversee.notify import AlertMessage, OutboxRow, render_message
from src.riskmgr.render import ItemBody, NoticeBody

D = Decimal
T = 1_795_147_200_000


def _item(**kw: Any) -> ItemBody:
    base: dict[str, Any] = dict(
        decision_id=9, decision_bar_end_ms=T, adapter_id="ad", position_id="P1", symbol="SYN.A", side="BUY",
        event="POST", bar_end_ms=T, outcome="ADJUST", category="ACTIONABLE", proposed_stop=D("1.10000"),
        current_risk_r=D("0.85"), handles=("O1",), stop_read_at_ms=T + 5_000, stop_now=D("1.09000"),
        entry=D("1.1"), s0=D("1.09000"), trigger_input="ad|SYN.A|M15|BID|BROKER|1|k1:abc",
        strategy_kind="break_even_on_risk_close", strategy_impl_version=1)
    base.update(kw)
    return ItemBody(**base)


def _notice(item: ItemBody) -> NoticeBody:
    return NoticeBody(notice_id=5, route="main", event=item.event, bar_spec="M15", bar_end_ms=T,
                      created_ms=T + 60_000, items=(item,))


# ------------------------------------------------------------------------------------------------ F2 break-even
# Round 4 reverts round 1's "break-even only at entry": §B8 fixes no ADJUST headline, and §20 Q6 / Q1 (a) call every
# break_even_on_risk_close target "break-even", at the entry and at the entry plus a buffer (as the single §B8
# COMPLETE text). The preheader still states the exact level.
def _hidden_preheader(page: str) -> str:
    hidden = re.search(r'display:none[^>]*>([^<]*)</div>', page)
    assert hidden is not None
    return hidden.group(1)


def test_proposed_equal_to_entry_keeps_break_even_wording() -> None:
    final, pre = _item(), _item(event="PRE")
    assert adjust_headline(final) == "Move stop to break-even: SYN.A long"
    assert adjust_headline(pre) == "Heads-up: SYN.A long stop would move to break-even"
    assert arm_preheader(_notice(final)).startswith("Move stop to break-even")
    assert "break-even" in render_arm_html(_notice(final), theme="xtnl")


@pytest.mark.parametrize(("side", "entry", "proposed", "word"), [
    ("BUY", D("1.1"), D("1.10000"), "long"),             # exactly the entry (Decimal compare)
    ("BUY", D("1.1"), D("1.10001"), "long"),             # entry plus a buffer (be_offset_r > 0)
    ("SELL", D("1.1"), D("1.09999"), "short"),           # a short's buffer is below entry
    ("BUY", D("1.100005"), D("1.10001"), "long"),        # rounded toward profit off an off-grid entry
    ("SELL", D("1.100005"), D("1.10000"), "short"),
    ("BUY", None, D("1.10001"), "long"),                 # entry unknown
    ("SELL", None, D("1.09999"), "short"),
])
def test_every_break_even_target_gets_the_break_even_wording(side: str, entry: Decimal | None, proposed: Decimal,
                                                             word: str) -> None:
    final = _item(side=side, entry=entry, proposed_stop=proposed)
    pre = _item(event="PRE", side=side, entry=entry, proposed_stop=proposed)
    assert adjust_headline(final) == f"Move stop to break-even: SYN.A {word}"
    assert adjust_headline(pre) == f"Heads-up: SYN.A {word} stop would move to break-even"
    level = format(proposed, "f")
    for item, lead in ((final, "Move stop to break-even"), (pre, "Heads-up")):
        pre_line = arm_preheader(_notice(item))
        assert pre_line.startswith(lead) and "break-even" in pre_line and pre_line.endswith(f"→ {level}")
        for theme in ("classic", "xtnl"):
            hidden = _hidden_preheader(render_arm_html(_notice(item), theme=theme))
            assert "break-even" in hidden and level in hidden, theme


def test_another_strategy_kind_gets_the_generic_wording() -> None:
    final = _item(strategy_kind="trail_atr")
    pre = _item(event="PRE", strategy_kind="trail_atr")
    assert adjust_headline(final) == "Move stop: SYN.A long"
    assert adjust_headline(pre) == "Heads-up: SYN.A long stop would move to 1.10000"
    assert adjust_headline(_item(event="PRE", strategy_kind="trail_atr", proposed_stop=None)) == (
        "Heads-up: SYN.A long stop would move to a new level")
    assert adjust_headline(_item(side="SELL", strategy_kind="")) == "Move stop: SYN.A short"
    for item in (final, pre):
        pre_line = arm_preheader(_notice(item))
        assert "break-even" not in pre_line and pre_line.endswith("→ 1.10000")


# ------------------------------------------------------------------------------------- F3 stop_kind_inconsistent
def _msg(kind: str, sev: str, body: str, episode: tuple[str, str, str, int, int]) -> AlertMessage:
    return render_message(OutboxRow(kind, sev, "x", body, 1_795_147_261_000,  # type: ignore[arg-type]
                                    episode=episode, account_id="A1"))


def test_rule_action_names_the_stop_child_condition() -> None:
    pos = rule_action("missing_stop_loss", 2, "stop_kind_inconsistent", "position")
    assert pos == STOP_KIND_INCONSISTENT_ACTION.format(what="position") and pos != UNKNOWN_ACTION
    assert "at or beyond the market" in pos and "capped at SOFT" in pos.replace("Capped", "capped")
    order = rule_action("missing_stop_loss", 2, "stop_kind_inconsistent", "order")
    assert order is not None and "this order" in order
    # the code stays an UNKNOWN reason (cap handling unchanged); the generic path is not broken
    assert "stop_kind_inconsistent" in UNKNOWN_REASONS
    assert rule_action("max_position_risk", 2, "risk_unknown", "position") == UNKNOWN_ACTION
    assert rule_action("missing_stop_loss", 2, "mark_unknown", "position") == UNKNOWN_ACTION


def test_episode_and_repage_render_the_specific_action_without_prices() -> None:
    ep = ("missing_stop_loss", "position", "P9", 0, 2)
    ctx = classify(_msg("episode", "high", "stop_kind_inconsistent (cap SOFT, sweep 9)", ep))
    assert ctx.action is not None and "at or beyond the market" in ctx.action
    assert not ctx.action.startswith("An input is unknown")
    rp = classify(_msg("repage", "high", "stop_kind_inconsistent (cap SOFT)", ep))
    assert rp.action is not None and "at or beyond the market" in rp.action and "Acknowledge" in rp.action
    page = render_alert_html(_msg("episode", "high", "stop_kind_inconsistent (cap SOFT, sweep 123)", ep))
    assert "at or beyond the market" in page
    text = re.sub(r"<[^>]*>", " ", page.split("</head>")[-1])
    assert re.search(r"\d+\.\d+", text) is None, "no decimal price in the visible text"
