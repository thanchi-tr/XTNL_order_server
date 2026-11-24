"""Live review of kind (a) `break_even_on_risk_close` v1 and FirstBarGuard (design §B6.3 (7a), §20 Q1, Q2, Q7;
findings 2, 6, 12). Synthetic levels only.

* Finding 2: `at_target` judges against the unrounded target; a handle off the grid between the target and the
  target rounded toward profit is at target and `propose` names no handle (so the envelope is never handed nothing).
* Finding 6: with S0 first seen already in profit (R < 0) the buffer is clamped (`max(R, 0)`): the target is E, never
  past entry on the loss side, whatever the offset.
* Finding 12 (Q7): a fill exactly on a bucket boundary belongs to the bucket it opens, so that bucket is bar 0.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.marketdata.ports import HandleRef
from src.riskmgr.guards.first_bar import FIRST_BAR_V1, bar0_bucket_ms
from src.riskmgr.pipeline import round_toward_profit
from src.riskmgr.registry import REGISTRY
from src.riskmgr.strategies.break_even_on_risk_close import BREAK_EVEN_ON_RISK_CLOSE_V1
from src.riskmgr.strategy import GuardSpec, Proposal, StrategyParams, StrategyState
from src.riskmgr.types import ArmEvent, S0Source
from tests.riskmgr.kind_a import B0, FIVE, Q, bar0_with_range, flat_history, guard_ctx, s_ctx, strategy_row

D = Decimal
S = BREAK_EVEN_ON_RISK_CLOSE_V1
G = FIRST_BAR_V1
FRESH = StrategyState.of(latched=False)
O1 = HandleRef("CHILD_ORDER", "O1")


def params(**over: object) -> StrategyParams:
    return REGISTRY.parse_params(S.KIND, 1, strategy_row(**over))


# ---------------------------------------------------------------------------------------------------- finding 2
def test_an_off_grid_handle_between_the_target_and_its_rounding_is_at_target() -> None:
    p = params(be_offset_r="0")
    ctx = s_ctx("1.21", entry="1.200005", s0="1.199005", handles=["1.200007"], stop="1.200007")
    # the handle sits strictly between the unrounded target (E) and the target rounded toward profit
    assert ctx.price_increment is not None
    assert round_toward_profit(D("1.200005"), ctx.price_increment, 1) == D("1.20001")
    assert S.at_target(p, ctx, FRESH)
    assert S.propose(p, ctx, FRESH, ArmEvent.POST).proposal == Proposal(())


# ---------------------------------------------------------------------------------------------------- finding 6
@pytest.mark.parametrize("side", ["BUY", "SELL"])
@pytest.mark.parametrize("offset", ["0.1", "0.5"])
def test_target_with_r_negative_never_crosses_entry(side: str, offset: str) -> None:
    sign = 1 if side == "BUY" else -1
    e0 = D("1.20000")
    s0 = str(e0 + sign * D("0.00050"))          # S0 first seen in profit: R < 0
    loose = str(e0 - sign * D("0.00050"))       # the handle since loosened past entry
    ctx = s_ctx(str(e0 + sign * D("0.00200")), side=side, s0=s0, stop=loose, handles=[loose],
                s0_source=S0Source.FIRST_SEEN)
    assert ctx.r < 0
    e = S.propose(params(be_offset_r=offset), ctx, FRESH, ArmEvent.POST)
    assert e.trigger_met and e.target == e0 and e.proposal == Proposal(((O1, e0),))
    assert S.target(params(be_offset_r=offset), ctx) == e0


# ---------------------------------------------------------------------------------------------------- finding 12
def _spec() -> GuardSpec:
    return GuardSpec(G.KIND, G.VERSION, G.parse_params({}))


def test_a_fill_on_a_bucket_boundary_belongs_to_the_bucket_it_opens() -> None:
    assert bar0_bucket_ms("fill_bucket", B0) == B0
    assert bar0_bucket_ms("fill_bucket", B0 + Q - 1) == B0
    assert bar0_bucket_ms("fill_bucket", B0 + Q) == B0 + Q
    assert bar0_bucket_ms("next_full_bucket", B0) == B0 + Q
    bars = [*flat_history(FIVE, D("0.00500")), bar0_with_range(FIVE, D("0.00900"))]
    assert G.applies(_spec(), guard_ctx(FIVE, bars, entry_time_ms=B0), StrategyState())
    assert not G.applies(_spec(), guard_ctx(FIVE, bars, bucket=B0 - Q, entry_time_ms=B0, bar_index=None),
                         StrategyState())
