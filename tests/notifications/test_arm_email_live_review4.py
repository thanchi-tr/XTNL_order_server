"""Live review round 4 on the ARM email HTML: both themes carry the bar-0 PRE label and every decision caveat.

- §B6.5 / §B8: in actionable mode a bar-0 PRE that proposes a move is shown only with its conditional label ("first-bar
  check pending at bar end; if this bar closes calm, the trigger is met / not met"), never as a plain provisional
  ADJUST. The xtnl page (the production default) dropped it.
- §B5.2: a POST decided after `T + post_grace` is labelled DELAYED with its delay; §B6.3 `be_offset_first_seen`, §B7
  (an S0 from HISTORY_LAST_LEVEL is treated as FIRST_SEEN with a caveat), Q14 `roll_affected` and the other caveats
  are shown in words, for ADJUST and non-ADJUST items. Current risk R, the first-bar ratio and borderline are on the
  §B8 allow-list. Never favour R or favour / risk.
"""
from __future__ import annotations

import html
import re
from decimal import Decimal

import pytest

from src.notifications.email_html_arm import render_arm_html
from src.riskmgr.guards.first_bar import ROLL_AFFECTED, pre_label
from src.riskmgr.render import CAVEAT_TEXT, DELAYED, FIRST_BAR_CONDITIONAL, NoticeBody, RenderSettings, render
from src.riskmgr.strategies.break_even_on_risk_close import BE_OFFSET_FIRST_SEEN, BE_OFFSET_FIRST_SEEN_TEXT
from tests.notifications.test_email_html_live_review import T, _item, _notice

THEMES = ("classic", "xtnl")
CAVEATS = ("borderline", BE_OFFSET_FIRST_SEEN, ROLL_AFFECTED, "stop_changed_after_bar_end")


def _visible(page: str) -> str:
    """The page's visible text: the body without the hidden preheader, tags stripped, entities decoded."""
    body = page.split("</head>")[-1]
    body = re.sub(r'<div style="display:none[^>]*>[^<]*</div>', " ", body)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", body)))


# ---------------------------------------------------------------------------------------------- §B6.5 bar-0 PRE
@pytest.mark.parametrize("theme", THEMES)
@pytest.mark.parametrize("met", [True, False])
def test_a_bar_zero_pre_adjust_always_shows_its_conditional_label(theme: str, met: bool) -> None:
    pre = _item(event="PRE", caveats=(FIRST_BAR_CONDITIONAL,), trigger_met=met, trigger_input=None,
                valid_until_ms=T + 120_000)
    for show in (True, False):
        text = _visible(render_arm_html(_notice(pre), theme=theme, settings=RenderSettings(show_levels=show)))
        assert pre_label(met) in text, (theme, met, show)
        assert pre_label(not met) not in text
        assert "Valid until" in text
    # the label belongs to bar 0 only: a later PRE carries no first-bar text
    plain = _visible(render_arm_html(_notice(_item(event="PRE", trigger_input=None)), theme=theme))
    assert "first-bar check" not in plain


@pytest.mark.parametrize("theme", THEMES)
def test_every_bar_zero_pre_adjust_in_a_multi_item_notice_carries_the_label(theme: str) -> None:
    a = _item(event="PRE", caveats=(FIRST_BAR_CONDITIONAL,), trigger_met=True, trigger_input=None)
    b = _item(event="PRE", caveats=(FIRST_BAR_CONDITIONAL,), trigger_met=False, trigger_input=None, decision_id=10,
              position_id="P2", symbol="SYN.B", side="SELL", proposed_stop=Decimal("1.09999"))
    body = NoticeBody(notice_id=6, route="main", event="PRE", bar_spec="M15", bar_end_ms=T, created_ms=T,
                      items=(a, b))
    text = _visible(render_arm_html(body, theme=theme))
    assert pre_label(True) in text and pre_label(False) in text


# --------------------------------------------------------------------------------- §B5.2 DELAYED and the caveats
@pytest.mark.parametrize("theme", THEMES)
def test_a_delayed_post_shows_its_delay_caveats_and_allow_listed_figures(theme: str) -> None:
    item = _item(caveats=(DELAYED, *CAVEATS), borderline=True, first_bar_ratio=Decimal("1.318"),
                 decided_at_ms=T + 7 * 60_000 + 30_000)
    text = _visible(render_arm_html(_notice(item), theme=theme))
    assert "7 min after bar end" in text                # classic: the V7 time strip; xtnl: the Delayed fact
    for code in CAVEATS:
        assert CAVEAT_TEXT[code] in text, (theme, code)
    assert BE_OFFSET_FIRST_SEEN_TEXT in text            # §B6.3 / §B7: the S0 was first seen (HISTORY_LAST_LEVEL)
    assert "1.318" in text and "0.85 R" in text          # first-bar ratio and current risk (§B8 allow-list)
    assert "favour" not in text.lower()
    # the text part carries the same delay, caveats and ratio
    plain = render(_notice(item)).text
    assert "decided 7 min after bar end" in plain and "1.318" in plain
    assert all(CAVEAT_TEXT[c] in plain for c in CAVEATS)


@pytest.mark.parametrize("theme", THEMES)
def test_a_post_decided_in_time_has_no_delay_line(theme: str) -> None:
    on_time = _item(caveats=(DELAYED,), decided_at_ms=T)   # not after the bar end: no label (as the text part)
    assert "min after bar end" not in _visible(render_arm_html(_notice(on_time), theme=theme))


def test_xtnl_shows_the_delay_and_caveats_of_a_non_adjust_item() -> None:
    item = _item(outcome="COMPLETE", category="INFO", proposed_stop=None,
                 caveats=(DELAYED, BE_OFFSET_FIRST_SEEN, ROLL_AFFECTED, "unknown_code_x"),
                 decided_at_ms=T + 12 * 60_000)
    text = _visible(render_arm_html(_notice(item), theme="xtnl"))
    assert "decided 12 min after bar end" in text
    assert BE_OFFSET_FIRST_SEEN_TEXT in text and CAVEAT_TEXT[ROLL_AFFECTED] in text
    assert "unknown_code_x" in text                    # an unknown code is shown as its escaped id
    assert "released: the bar end confirmed break-even" in text


def test_xtnl_escapes_an_unknown_caveat_code() -> None:
    nasty = "<b>x</b>&"
    page = render_arm_html(_notice(_item(caveats=(nasty,))), theme="xtnl")
    assert "<b>x</b>" not in page and "&lt;b&gt;x&lt;/b&gt;&amp;" in page
