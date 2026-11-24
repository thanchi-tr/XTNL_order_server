"""Live review round 5 on the ARM email HTML (`docs/design/live-review-2026-11-25.md`, round 5).

- **Current stop with `show_levels` off.** `automated-risk-manager.md` Q13 (built default "proposed stop and R only")
  and §B8 ("entry and current stop only if Q13"): with `RenderSettings.show_levels` off no HTML theme shows the entry
  level or the current stop level (classic fields, the classic V4 stop ladder, the xtnl levels block); with it on both
  are shown. The handle ids and the stop's read time stay (§B8 allow-list). The text part is unchanged.
- **Increment leak.** `.claude/rules/riskmgr.md` §5: never a price increment or pip size in a notice; the owner's
  exception (2026-11-21) covers only each stop's distance from the entry in pips. The xtnl note gives only the move's
  direction ("Moves the stop tighter."), never a numeric move size, which equals the increment for a one-increment
  move.
"""
from __future__ import annotations

import re
from decimal import Decimal

import pytest

from src.notifications.email_html_arm import render_arm_html
from src.notifications.email_theme import PipUnit
from src.riskmgr.render import RenderSettings, render
from tests.notifications.test_arm_email_live_review4 import _visible
from tests.notifications.test_email_html_live_review import _item, _notice

D = Decimal
THEMES = ("classic", "xtnl")
FX = PipUnit(D("0.0001"), "pips")
UNITS = {("ad", "SYN.A"): FX}

# (side, entry, current stop, proposed stop): a tightening move on each side, every level distinct
SIDES = {
    "BUY": (D("1.27140"), D("1.26800"), D("1.27020")),
    "SELL": (D("1.27000"), D("1.27410"), D("1.27120")),
}


def _page(side: str, theme: str, show: bool) -> str:
    entry, now, proposed = SIDES[side]
    item = _item(side=side, entry=entry, stop_now=now, proposed_stop=proposed, strategy_kind="k")
    return render_arm_html(_notice(item), theme=theme, settings=RenderSettings(show_levels=show), pip_units=UNITS)


# ---------------------------------------------------------------------------------- Q13 / §B8 current stop level
@pytest.mark.parametrize("theme", THEMES)
@pytest.mark.parametrize("side", sorted(SIDES))
def test_show_levels_off_hides_the_entry_and_the_current_stop_level(theme: str, side: str) -> None:
    entry, now, proposed = SIDES[side]
    page = _page(side, theme, show=False)
    assert format(now, "f") not in page, (theme, side)          # fields, ladder and levels block
    assert format(entry, "f") not in page, (theme, side)
    assert "xv-stop-ladder" not in page                         # classic V4: the card shows no current stop level
    text = _visible(page)
    assert format(proposed, "f") in text and "0.85 R" in text   # proposed stop and R only
    if theme == "classic":
        assert "Current stop not shown (show_levels off)" in text
        assert "as read" in text and "handles O1" in text       # §B8: handle ids and the stop's read time


@pytest.mark.parametrize("theme", THEMES)
@pytest.mark.parametrize("side", sorted(SIDES))
def test_show_levels_on_shows_the_entry_and_the_current_stop_level(theme: str, side: str) -> None:
    entry, now, proposed = SIDES[side]
    text = _visible(_page(side, theme, show=True))
    for level in (entry, now, proposed):
        assert format(level, "f") in text, (theme, side, level)
    assert "0.85 R" in text


def test_show_levels_on_keeps_the_classic_ladder_with_the_current_stop() -> None:
    page = _page("BUY", "classic", show=True)
    ladder = page[page.index('class="xv-stop-ladder"'):]
    assert "CURRENT STOP" in ladder and "1.26800" in ladder and "ENTRY" in ladder


def test_xtnl_without_show_levels_lists_only_the_proposed_stop() -> None:
    text = _visible(_page("BUY", "xtnl", show=False))
    assert "Stop now" not in text and "Proposed stop" in text
    assert "reference" not in text                              # the proposed stop is not the entry


@pytest.mark.parametrize("theme", THEMES)
def test_an_older_body_without_stop_now_and_show_levels_off_shows_no_level(theme: str) -> None:
    text = _visible(render_arm_html(_notice(_item(stop_now=None, entry=D("1.27140"), proposed_stop=D("1.27020"))),
                                    theme=theme))
    assert "1.27140" not in text and "1.27020" in text


def test_xtnl_a_bar_zero_pre_without_a_proposed_level_keeps_its_levels_block() -> None:
    from src.riskmgr.guards.first_bar import pre_label
    from src.riskmgr.render import FIRST_BAR_CONDITIONAL
    pre = _item(event="PRE", caveats=(FIRST_BAR_CONDITIONAL,), trigger_met=True, trigger_input=None,
                proposed_stop=None)
    text = _visible(render_arm_html(_notice(pre), theme="xtnl"))
    assert pre_label(True) in text and "1.09000" not in text


def test_the_text_part_is_unchanged() -> None:
    entry, now, proposed = SIDES["BUY"]
    item = _item(entry=entry, stop_now=now, proposed_stop=proposed)
    for show in (False, True):
        s = RenderSettings(show_levels=show)
        assert render(_notice(item), settings=s).text == render(_notice(_item(entry=entry, stop_now=None,
                                                                                proposed_stop=proposed)),
                                                                 settings=s).text


# -------------------------------------------------------------------------------------- riskmgr.md §5 increment
@pytest.mark.parametrize("show", [True, False])
@pytest.mark.parametrize(("unit", "now", "proposed", "entry", "leak"), [
    (FX, D("1.26800"), D("1.26805"), D("1.27140"), "0.5 pips"),        # half-pip increment
    (FX, D("1.26800"), D("1.26810"), D("1.27140"), "1.0 pips"),
    (PipUnit(D("1"), "points"), D("7800.0"), D("7800.5"), D("7810.0"), "0.5 points"),
    (PipUnit(D("1"), "points"), D("7800"), D("7801"), D("7810"), "1.0 points"),
])
def test_the_xtnl_note_never_prints_the_move_size(show: bool, unit: PipUnit, now: Decimal, proposed: Decimal,
                                                  entry: Decimal, leak: str) -> None:
    item = _item(entry=entry, stop_now=now, proposed_stop=proposed, strategy_kind="k")
    page = render_arm_html(_notice(item), theme="xtnl", settings=RenderSettings(show_levels=show),
                           pip_units={("ad", "SYN.A"): unit})
    text = _visible(page)
    assert re.search(r"Moves the stop \d", page) is None
    assert leak not in text
    assert "Moves the stop tighter." in text


def test_the_xtnl_note_names_a_looser_move() -> None:
    item = _item(entry=D("1.27140"), stop_now=D("1.27020"), proposed_stop=D("1.26800"), strategy_kind="k")
    text = _visible(render_arm_html(_notice(item), theme="xtnl", settings=RenderSettings(show_levels=True),
                                    pip_units=UNITS))
    assert "Moves the stop looser." in text and "22.0" not in text
    assert "34.0 pips" in text and "12.0 pips" in text           # each stop's distance from the entry stays
