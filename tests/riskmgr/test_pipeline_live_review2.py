"""Live review round 2, finding 4: every ADJUST level is stated on the price-increment grid in one form.

Design §B6.3 step (9) ("round toward profit to price_increment"), REOPEN `REUSE_TARGET` (the same target as the
original bar) and §B8 (notices state the proposed stop). A REOPEN reuses the stored target, which `StrategyState`
keeps normalised (`1.2`, `1.201`); before the fix the "rounded" level kept that exponent and the notice said
"move the stop to 1.2" where the POST said "1.20000". Synthetic levels only.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.marketdata.ports import HandleRef
from src.riskmgr.outcomes import Outcome
from src.riskmgr.pipeline import round_toward_profit
from src.riskmgr.render import ItemBody, RenderSettings, outcome_text
from src.riskmgr.stopbasis import StopAtT
from src.riskmgr.strategy import StrategyState
from src.riskmgr.types import ArmEvent, StopBasis
from tests.riskmgr.kind_a import s_ctx
from tests.riskmgr.test_pipeline import params, post_run

D = Decimal
O1 = HandleRef("CHILD_ORDER", "O1")


@pytest.mark.parametrize("level,increment,sig,text", [
    ("1.2", "0.0001", 1, "1.2000"), ("1.2", "0.0001", -1, "1.2000"), ("1.201", "0.0001", 1, "1.2010"),
    ("1.2", "0.00001", 1, "1.20000"), ("1.20004", "0.0001", 1, "1.2001"), ("1.20004", "0.0001", -1, "1.2000"),
    ("150.1", "0.001", 1, "150.100"), ("1.21", "0.25", 1, "1.25"), ("1.2", "1", 1, "2"), ("1.2", "1", -1, "1"),
])
def test_round_toward_profit_states_the_level_at_the_increments_exponent(level: str, increment: str, sig: int,
                                                                        text: str) -> None:
    out = round_toward_profit(D(level), D(increment), sig)
    assert str(out) == text and out.as_tuple().exponent == D(increment).as_tuple().exponent
    assert out % D(increment) == 0 and sig * (out - D(level)) >= 0     # on the grid, toward profit, value unchanged


def _adjust_item(level: Decimal, event: str) -> ItemBody:
    return ItemBody(decision_id=1, decision_bar_end_ms=0, adapter_id="mock", position_id="P1", event=event,
                    bar_end_ms=0, outcome=Outcome.ADJUST.value, category="ACTIONABLE", proposed_stop=level)


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
@pytest.mark.parametrize("offset,stored", [("0", "1.2"), ("0.1", "1.201")])
def test_a_reopen_states_the_reused_target_as_the_post_does(sign: int, side: str, offset: str, stored: str) -> None:
    """A REOPEN reusing a stored, normalised target (`1.2`, `1.201`; the short mirror) proposes it at the increment's
    exponent (`1.20000`, `1.20100` on the 0.00001 grid), exactly as the POST at the same level does, and the
    notice text says the same."""
    e = D("1.20000")
    target = e + sign * (D(stored) - e)
    state = StrategyState.of(latched=True, target=target)
    assert state.text("target") == str(target.normalize())         # the stored form has no trailing zeros
    s0 = str(e - sign * D("0.01000"))
    close = str(e + sign * D("0.02000"))
    p = params(be_offset_r=offset)
    reopen = post_run(close, ctx=s_ctx(close, side=side, event=ArmEvent.REOPEN, s0=s0, stop=s0), state=state,
                      s_at_t=StopAtT(D(s0), StopBasis.STREAM), p=p, event=ArmEvent.REOPEN)
    post = post_run(close, ctx=s_ctx(close, side=side, s0=s0, stop=s0), s_at_t=StopAtT(D(s0), StopBasis.STREAM),
                    p=p)
    assert reopen.outcome is Outcome.ADJUST and post.outcome is Outcome.ADJUST
    got, want = dict(reopen.targets)[O1], dict(post.targets)[O1]
    assert got == target and str(got) == str(want) == format(target, ".5f")
    assert outcome_text(_adjust_item(got, "REOPEN"), RenderSettings()) == \
        outcome_text(_adjust_item(want, "POST"), RenderSettings())
    assert f"move the stop to {format(target, '.5f')} " in outcome_text(_adjust_item(got, "REOPEN"), RenderSettings())
