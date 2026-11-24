"""Live review F2 / F3 on the HTML email part.

F2: "break-even" is said only when the proposed stop equals the pinned entry (Decimal compare), not whenever the
strategy kind is break_even_on_risk_close: with be_offset_r > 0 the target is beyond entry.
F3: a missing_stop_loss state with reason stop_kind_inconsistent gets its own action (a STOP child at or beyond the
market), not the generic "an input is unknown" text; it is still capped at SOFT.
"""
from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

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
def test_proposed_equal_to_entry_keeps_break_even_wording() -> None:
    final, pre = _item(), _item(event="PRE")
    assert adjust_headline(final) == "Move stop to break-even: SYN.A long"
    assert adjust_headline(pre) == "Heads-up: SYN.A long stop would move to break-even"
    assert arm_preheader(_notice(final)).startswith("Move stop to break-even")
    assert "break-even" in render_arm_html(_notice(final), theme="xtnl")


def test_an_offset_target_beyond_entry_gets_the_generic_wording() -> None:
    final = _item(proposed_stop=D("1.10001"))  # be_offset_r > 0 (or a target rounded toward profit)
    pre = _item(event="PRE", proposed_stop=D("1.10001"))
    assert adjust_headline(final) == "Move stop: SYN.A long"
    assert adjust_headline(pre) == "Heads-up: SYN.A long stop would move to 1.10001"
    for body in (_notice(final), _notice(pre)):
        pre_line = arm_preheader(body)
        assert "break-even" not in pre_line and "1.10001" in pre_line
    short = _item(side="SELL", proposed_stop=D("1.09999"))
    assert adjust_headline(short) == "Move stop: SYN.A short"
    page = render_arm_html(_notice(final), theme="xtnl")
    hidden = re.search(r'display:none[^>]*>([^<]*)</div>', page)
    assert hidden is not None and "break-even" not in hidden.group(1)


def test_an_unknown_entry_gets_the_generic_wording() -> None:
    assert adjust_headline(_item(entry=None)) == "Move stop: SYN.A long"
    assert adjust_headline(_item(event="PRE", entry=None)) == "Heads-up: SYN.A long stop would move to 1.10000"
    assert adjust_headline(_item(proposed_stop=None, event="PRE")) == (
        "Heads-up: SYN.A long stop would move to a new level")


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
