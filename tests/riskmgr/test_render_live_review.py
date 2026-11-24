"""Live review of the SL notice texts (design §B8, §20 Q10; findings 4 and 5). Synthetic levels only.

* Finding 4: the Q10 release sentence follows the position's side: "below the target" for a long, "above the target"
  for a short, side-neutral for an older body without a side; in the text part and both HTML themes.
* Finding 5: favour / risk divides by the current risk, so it is shown only when that is the risk the trigger
  tested (a plain on-time POST, no `pre_basis_held` / `stop_changed_after_bar_end` caveat, no `AT_PRE` basis) and
  when it agrees with the decision; otherwise omitted, never shown contradicting the decision.
"""
from __future__ import annotations

import html
from decimal import Decimal

import pytest

from src.marketdata.hashing import Keyring, content_hash
from src.marketdata.types import M15, Bar, SeriesId, Source
from src.marketdata.types import Side as QuoteSide
from src.notifications.email_html_arm import render_arm_html
from src.riskmgr.render import (
    COMPLETE_TEXT,
    Q10_REARM,
    Q10_TELL,
    ItemBody,
    NoticeBody,
    RenderSettings,
    favour_figures,
    outcome_text,
    q10_text,
    render,
)

D = Decimal
T = 1_700_001_000_000
KEY = Keyring.of(1, b"k" * 32)
BID = SeriesId("mock", "SYN.A", M15, QuoteSide.BID, Source.BROKER)
ENTRY, S0 = D("1.20000"), D("1.19000")          # 1 R = 0.01000


def _bar(close: Decimal) -> tuple[dict[tuple[str, int], Bar], str]:
    o = h = low = close
    b = Bar(series=BID, bucket_start_ms=T - M15.timeframe_ms, open=o, high=h, low=low, close=close, final_at_ms=T)
    return {(BID.canonical, T): b}, f"{BID.canonical}|{T}|{content_hash(KEY, BID.canonical, T, o, h, low, close)}"


def item(**kw: object) -> ItemBody:
    base: dict[str, object] = dict(
        decision_id=7, decision_bar_end_ms=T, adapter_id="mock", position_id="P1", symbol="SYN.A", side="BUY",
        event="POST", bar_end_ms=T, outcome="COMPLETE", category="INFO", current_risk_r=D("0.50"),
        entry=ENTRY, s0=S0, strategy_kind="break_even_on_risk_close", strategy_impl_version=1,
        trigger_source="BROKER")
    base.update(kw)
    return ItemBody(**base)  # type: ignore[arg-type]


def body(*items: ItemBody) -> NoticeBody:
    return NoticeBody(notice_id=42, route="main", event="POST", bar_spec="15m", bar_end_ms=T, created_ms=T + 70_000,
                      expires_at_ms=T + 970_000, items=items)


# ---------------------------------------------------------------------------------------------------- finding 4
def test_q10_text_follows_the_side() -> None:
    assert q10_text("BUY", False) == Q10_TELL and q10_text("BUY", True) == Q10_REARM
    assert "above the target" in q10_text("SELL", False) and "above the target" in q10_text("SELL", True)
    for neutral in (q10_text("", False), q10_text("?", True)):
        assert "below" not in neutral and "above" not in neutral and "past the target" in neutral


@pytest.mark.parametrize("policy", ["TELL_ONLY", "REARM"])
def test_a_short_release_says_above_the_target(policy: str) -> None:
    done = item(side="SELL", loosen_policy=policy)
    text = outcome_text(done, RenderSettings())
    assert text.startswith(COMPLETE_TEXT) and "above the target" in text and "below the target" not in text
    loosened = outcome_text(item(side="SELL", outcome="LOOSENED_AFTER_COMPLETE"), RenderSettings())
    assert "above the target" in loosened and "below the target" not in loosened and "you are told only" in loosened
    long_text = render(body(item(side="BUY", loosen_policy=policy))).text
    assert (Q10_REARM if policy == "REARM" else Q10_TELL) in long_text
    short_text = render(body(done)).text
    assert "above the target" in short_text and "below the target" not in short_text


@pytest.mark.parametrize("theme", ["classic", "xtnl"])
def test_a_short_release_says_above_the_target_in_both_html_themes(theme: str) -> None:
    b = body(item(side="SELL"), item(decision_id=8, position_id="P2", side="SELL", outcome="LOOSENED_AFTER_COMPLETE"))
    page = html.unescape(render_arm_html(b, theme=theme))
    assert "above the target" in page and "below the target" not in page
    single = html.unescape(render_arm_html(body(item(side="SELL")), theme=theme))
    assert "below the target" not in single


# ---------------------------------------------------------------------------------------------------- finding 5
def _favour(close: str, **kw: object) -> str | None:
    bars, ref = _bar(D(close))
    fav = favour_figures(item(outcome="ADJUST", category="ACTIONABLE", trigger_input=ref, **kw), bars, KEY)
    assert fav is not None
    return None if fav.per_risk is None else format(fav.per_risk.value, "f")


def test_favour_per_risk_never_contradicts_the_decision() -> None:
    """The stop was tightened after T: current risk 0.40 R, favour 0.80 R, but the trigger tested S_at_T (1.00 R)
    and was not met. "favour / risk 2.000" next to BELOW_TRIGGER is never shown."""
    assert _favour("1.20800", trigger_met=False, current_risk_r=D("0.40")) is None
    # the reverse: the stop was loosened after T, ratio < 1 next to a met trigger
    assert _favour("1.21200", trigger_met=True, current_risk_r=D("1.50")) is None
    bars, ref = _bar(D("1.20800"))
    text = render(body(item(outcome="STRATEGY_REASON", category="INFO", reason="BELOW_TRIGGER", trigger_met=False,
                            current_risk_r=D("0.40"), trigger_input=ref)), bars=bars, keyring=KEY).text
    assert "favour 0.80 R" in text and "favour / risk" not in text


def test_favour_per_risk_is_omitted_when_the_tested_stop_is_not_the_current_one() -> None:
    for kw in ({"caveats": ("pre_basis_held",)}, {"caveats": ("stop_changed_after_bar_end",)},
               {"stop_basis": "AT_PRE"}, {"event": "REOPEN"}, {"event": "LATE"}, {"late": True},
               {"trigger_met": None}):
        args: dict[str, object] = {"trigger_met": True, "current_risk_r": D("0.50"), **kw}
        assert _favour("1.21000", **args) is None, kw


def test_a_consistent_favour_per_risk_is_still_shown() -> None:
    assert _favour("1.21000", trigger_met=True, current_risk_r=D("0.50")) == "2.000"
    assert _favour("1.20400", trigger_met=False, current_risk_r=D("0.50")) == "0.800"
    assert _favour("1.20500", trigger_met=False, current_risk_r=D("0.50")) == "1.000"     # exactly 1: either way
    assert _favour("1.20510", trigger_met=False, current_risk_r=D("0.50")) == "1.020"     # rounding band
