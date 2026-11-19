"""Kind (a) `break_even_on_risk_close` v1 (plan S20; design §B6.3 (7), §B6.7, Q1, Q5, Q6, Q10, Q11, R11-m).

Long and short trigger boundaries for `gt` / `ge`, the target `E + σ·be_offset_r·max(R, 0)` (an `R ≤ 0` gives no
buffer) and its caveat `be_offset_first_seen`, `borderline`, the trigger close side (Q5), the stop the trigger uses
(`S_at_T`, the farther of it and `pre_basis`, the current stop at PRE), `at_target` (an off-grid `E` with offset 0
counts), `latch`, REOPEN reusing its target, `commit_state` and the param rules. Synthetic levels only.
"""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from src.marketdata.ports import HandleRef
from src.marketdata.types import M15, Quote, Side
from src.riskmgr.outcomes import Outcome
from src.riskmgr.registry import REGISTRY
from src.riskmgr.strategies.break_even_on_risk_close import (
    BE_OFFSET_FIRST_SEEN,
    BELOW_TRIGGER,
    BORDERLINE,
    BREAK_EVEN_ON_RISK_CLOSE_V1,
    NOT_IN_FAVOUR,
    RISK_R,
)
from src.riskmgr.strategy import (
    Evaluation,
    LoosenPolicy,
    ParamError,
    Proposal,
    Refuse,
    ReopenPolicy,
    StrategyParams,
    StrategyState,
)
from src.riskmgr.types import ArmEvent, BarEvent, RMultiple, S0Source
from tests.riskmgr.kind_a import ADAPTER, FIVE, guard_row, s_arm_ctx, s_ctx, strategy_row

D = Decimal
S = BREAK_EVEN_ON_RISK_CLOSE_V1
KIND = S.KIND
FRESH = StrategyState.of(latched=False)
O1 = HandleRef("CHILD_ORDER", "O1")
O2 = HandleRef("CHILD_ORDER", "O2")


def params(**over: object) -> StrategyParams:
    return REGISTRY.parse_params(KIND, 1, strategy_row(**over))


def propose(ctx_close: str, *, p: StrategyParams | None = None, state: StrategyState = FRESH,
            **kw: object) -> Evaluation:
    ctx = s_ctx(ctx_close, **kw)   # type: ignore[arg-type]
    return S.propose(p or params(be_offset_r="0"), ctx, state, ctx.event)


# ---------------------------------------------------------------------------------------------------- params
def test_params_defaults_and_declarations() -> None:
    p = REGISTRY.parse_params(KIND, 1, {"guards": [guard_row()]})
    assert dict(p.values) == {"trigger_basis": "exit_side", "trigger_comparison": "gt", "latch": "off",
                              "be_offset_r": "0", "rearm_on_loosen": "off", "bar_spec": "15m"}
    assert S.triggers(p) == (BarEvent(M15, ArmEvent.PRE), BarEvent(M15, ArmEvent.POST))
    assert S.guards(p) == p.guards and p.guards[0].kind == "first_bar"
    assert S.loosen_policy(p) is LoosenPolicy.TELL_ONLY
    assert S.loosen_policy(params(rearm_on_loosen="on")) is LoosenPolicy.REARM
    assert S.REOPEN_POLICY is ReopenPolicy.REUSE_TARGET
    assert S.REQUIRED_GUARDS == frozenset({"first_bar"})
    assert S.required_indicators(p) == () and S.required_history(p) == ()
    assert S.init_state(p, s_arm_ctx()) == FRESH


def test_rows_validation_refuses() -> None:
    """Exactly one `first_bar` entry; no `first_bar.*` or guard key among the strategy's own params; only 15m."""
    for bad in (strategy_row(guards=[]), strategy_row(guards=[guard_row(), guard_row()]),
                strategy_row(**{"first_bar.multiplier": "1.5"}), strategy_row(anchor="fill_bucket"),
                strategy_row(bar_spec="5m"), strategy_row(be_offset_r="0.51"), strategy_row(min_gap_r="0.1")):
        with pytest.raises(ParamError):
            REGISTRY.parse_params(KIND, 1, bad)


def test_trigger_side_q5() -> None:
    assert S.trigger_side(params(), "BUY") is Side.BID
    assert S.trigger_side(params(), "SELL") is Side.ASK
    assert S.trigger_side(params(trigger_basis="bid"), "SELL") is Side.BID


# ---------------------------------------------------------------------------------------------------- the trigger
@pytest.mark.parametrize("side", ["BUY", "SELL"])
@pytest.mark.parametrize("comparison", ["gt", "ge"])
def test_trigger_boundaries_long_and_short(side: str, comparison: str) -> None:
    """Risk is 100 increments (S0 = current stop); favour at risk − 1, risk, risk + 1 increment."""
    sign = 1 if side == "BUY" else -1
    p = params(be_offset_r="0", trigger_comparison=comparison)
    for steps, met in ((99, False), (100, comparison == "ge"), (101, True)):
        close = str(D("1.20000") + sign * steps * D("0.00001"))
        e = propose(close, p=p, side=side)
        assert e.trigger_met is met, (side, comparison, steps)
        if met:
            assert e.proposal is not None and e.proposal.target(O1) == D("1.20000")
        else:
            assert e.reason == BELOW_TRIGGER


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_not_in_favour_and_below_trigger(side: str) -> None:
    sign = 1 if side == "BUY" else -1
    assert propose("1.20000", side=side).reason == NOT_IN_FAVOUR
    assert propose(str(D("1.20000") - sign * D("0.00010")), side=side).reason == NOT_IN_FAVOUR
    assert propose(str(D("1.20000") + sign * D("0.00010")), side=side).reason == BELOW_TRIGGER


def test_borderline_within_three_percent_of_risk() -> None:
    assert BORDERLINE in propose("1.20098").caveats              # favour 0.00098 vs risk 0.00100
    assert BORDERLINE in propose("1.20103").caveats
    assert BORDERLINE not in propose("1.20090").caveats
    assert BORDERLINE not in propose("1.20010", stop="1.20000").caveats   # risk 0: no ratio


# ---------------------------------------------------------------------------------------------------- the target
@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_target_with_buffer_and_r_not_positive(side: str) -> None:
    sign = 1 if side == "BUY" else -1
    win = str(D("1.20000") + sign * D("0.00200"))
    e = propose(win, p=params(be_offset_r="0.25"), side=side)
    assert e.proposal is not None and e.proposal.target(O1) == D("1.20000") + sign * D("0.00025")
    assert BE_OFFSET_FIRST_SEEN not in e.caveats                  # S0 AT_FILL
    # R <= 0 (S0 at entry): no buffer, whatever be_offset_r says
    flat = propose(win, p=params(be_offset_r="0.5"), side=side, s0="1.20000", stop="1.20000",
                   handles=[str(D("1.20000") - sign * D("0.00050"))])
    assert flat.proposal is not None and flat.proposal.target(O1) == D("1.20000")
    assert flat.metrics == ()                                     # no R values when R <= 0


@pytest.mark.parametrize("source", [S0Source.FIRST_SEEN, S0Source.HISTORY_LAST_LEVEL])
def test_buffer_on_a_first_seen_stop_carries_the_caveat(source: S0Source) -> None:
    e = propose("1.20200", p=params(be_offset_r="0.1"), s0_source=source)
    assert e.proposal is not None and BE_OFFSET_FIRST_SEEN in e.caveats
    no_buffer = propose("1.20200", p=params(be_offset_r="0"), s0_source=source)
    assert BE_OFFSET_FIRST_SEEN not in no_buffer.caveats


def test_proposal_names_only_handles_short_of_the_target() -> None:
    e = propose("1.20200", handles=["1.19900", "1.20000"], stop="1.19900")
    assert e.proposal == Proposal(((O1, D("1.20000")),))
    done = propose("1.20200", handles=["1.20000", "1.20050"], stop="1.20000")
    assert done.trigger_met and done.proposal == Proposal(())


def test_at_target_and_an_off_grid_entry() -> None:
    p = params(be_offset_r="0")
    assert S.at_target(p, s_ctx("1.2", handles=["1.20000", "1.20050"]), FRESH)
    assert not S.at_target(p, s_ctx("1.2", handles=["1.20000", "1.19999"]), FRESH)
    assert S.at_target(p, s_ctx("1.2", side="SELL", handles=["1.20000"]), FRESH)
    assert not S.at_target(p, s_ctx("1.2", side="SELL", handles=["1.20001"]), FRESH)
    # a handle exactly at an off-grid E with offset 0 counts (§B6.3 (7a))
    assert S.at_target(p, s_ctx("1.2", entry="1.200005", s0="1.199005", handles=["1.200005"]), FRESH)
    assert not S.at_target(p, replace(s_ctx("1.2"), handles=()), FRESH)
    # with a buffer, at entry is short of the target
    assert not S.at_target(params(be_offset_r="0.2"), s_ctx("1.2", handles=["1.20000"]), FRESH)


# ---------------------------------------------------------------------------------------------------- stop and close inputs
def test_trigger_uses_the_farther_of_s_at_t_and_pre_basis() -> None:
    """R11-M04: a heads-up move never makes the test easier; a stop loosened past `pre_basis` is tested as it is."""
    assert propose("1.20101", pre_basis="1.19850").reason == BELOW_TRIGGER        # pre_basis farther: risk 150
    assert propose("1.20151", pre_basis="1.19850").trigger_met
    loosened = propose("1.20101", stop="1.19800", stop_at_t="1.19800", pre_basis="1.19900")
    assert loosened.reason == BELOW_TRIGGER                                        # current farther: risk 200
    assert propose("1.20201", stop="1.19800", stop_at_t="1.19800", pre_basis="1.19900").trigger_met
    short = propose("1.19899", side="SELL", pre_basis="1.20150")
    assert short.reason == BELOW_TRIGGER


def test_bar_end_uses_s_at_t_not_the_current_stop() -> None:
    """A stop tightened after the bar end does not change the bar-end test (`S_at_T`)."""
    e = propose("1.20101", stop="1.20000", stop_at_t="1.19900")
    assert e.trigger_met and e.metric(RISK_R) == RMultiple(D("0.00"))             # risk R from the current stop
    assert propose("1.20099", stop="1.20000", stop_at_t="1.19900").reason == BELOW_TRIGGER


def test_pre_uses_the_frozen_quote_and_the_current_stop() -> None:
    ctx = s_ctx("1.20000", event=ArmEvent.PRE, stop="1.19900")
    quote = Quote(adapter_id=ADAPTER, symbol=FIVE, bid=D("1.20101"), ask=D("1.20090"), broker_ms=1, received_ms=2)
    long = S.propose(params(be_offset_r="0"), replace(ctx, quote=quote), FRESH, ArmEvent.PRE)
    assert long.trigger_met                                                       # bid, current stop
    # S38c: the one stop-basis function: a held pre_basis farther from entry is tested at PRE too
    held = S.propose(params(be_offset_r="0"), replace(ctx, quote=quote, pre_basis=D("1.19500")), FRESH, ArmEvent.PRE)
    assert not held.trigger_met and held.reason == "BELOW_TRIGGER"
    sctx = s_ctx("1.20000", side="SELL", event=ArmEvent.PRE)
    squote = Quote(adapter_id=ADAPTER, symbol=FIVE, bid=D("1.19850"), ask=D("1.19899"), broker_ms=1, received_ms=2)
    assert S.propose(params(be_offset_r="0"), replace(sctx, quote=squote), FRESH, ArmEvent.PRE).trigger_met
    assert not S.propose(params(be_offset_r="0", trigger_basis="bid"), replace(sctx, quote=replace(
        squote, bid=D("1.19950"))), FRESH, ArmEvent.PRE).trigger_met


def test_missing_or_wrong_side_inputs_are_engine_faults() -> None:
    p = params()
    with pytest.raises(ValueError, match="trigger side"):
        S.propose(p, s_ctx("1.20200", side="SELL", trigger_side=Side.BID), FRESH, ArmEvent.POST)
    with pytest.raises(ValueError, match="trigger side"):
        S.propose(p, replace(s_ctx("1.20200"), bar=None), FRESH, ArmEvent.POST)
    with pytest.raises(ValueError, match="stop at bar end"):
        S.propose(p, replace(s_ctx("1.20200"), stop_at_t=None), FRESH, ArmEvent.POST)
    with pytest.raises(ValueError, match="PRE quote"):
        S.propose(p, replace(s_ctx("1.20200", event=ArmEvent.PRE), quote=None), FRESH, ArmEvent.PRE)
    # trigger_basis bid: a short reads the bid bar
    assert S.propose(params(trigger_basis="bid"), s_ctx("1.19800", side="SELL", trigger_side=Side.BID), FRESH,
                     ArmEvent.POST).trigger_met


def test_risk_r_metric_from_order_state() -> None:
    assert propose("1.20050").metric(RISK_R) == RMultiple(D("1.00"))
    assert propose("1.20050", stop="1.19950").metric(RISK_R) == RMultiple(D("0.50"))
    assert set(S.DECISION_METRICS) == {RISK_R}


# ---------------------------------------------------------------------------------------------------- state
def test_commit_state_records_the_trigger_and_ignores_faults() -> None:
    p = params(be_offset_r="0.1")
    met = propose("1.20200", p=p)
    after = S.commit_state(p, FRESH, met, Outcome.MARKET_CLOSED)       # recorded whatever the post-guards said
    assert after.flag("latched") and after.text("target") == "1.2001"
    for fault in (Outcome.DATA_UNAVAILABLE, Outcome.STRATEGY_ERROR, Outcome.WRITE_ERROR, Outcome.NO_STOP):
        assert S.commit_state(p, FRESH, met, fault) == FRESH
    below = propose("1.20050", p=p)
    assert S.commit_state(p, after, below, Outcome.STRATEGY_REASON) == FRESH        # latch off: re-test each bar
    # a trigger with every handle already at the target keeps the recorded target
    done = propose("1.20200", p=p, handles=["1.20010"], stop="1.20010")
    assert S.commit_state(p, after, done, Outcome.COMPLETE).text("target") == "1.2001"


def test_latch_re_proposes_but_never_counts_as_this_bars_trigger() -> None:
    p = params(be_offset_r="0", latch="on")
    state = S.commit_state(p, FRESH, propose("1.20200", p=p), Outcome.ADJUST)
    later = propose("1.20050", p=p, state=state)
    assert later.trigger_met is False and later.proposal == Proposal(((O1, D("1.20000")),))
    assert S.commit_state(p, state, later, Outcome.ADJUST) == state
    assert propose("1.20050", p=params(be_offset_r="0"), state=state).reason == BELOW_TRIGGER   # latch off


def test_reopen_reuses_the_recorded_target() -> None:
    p = params(be_offset_r="0.1")
    state = StrategyState.of(latched=True, target=D("1.20020"))
    e = propose("1.20200", p=p, state=state, event=ArmEvent.REOPEN)
    assert e.proposal is not None and e.proposal.target(O1) == D("1.20020")
    post = propose("1.20200", p=p, state=state)
    assert post.proposal is not None and post.proposal.target(O1) == D("1.20010")


def test_reopen_release_check_and_proposal_use_the_same_recorded_target() -> None:
    """After an S0 re-pin (a correction moved S0, so R changed) the recomputed target differs from the recorded
    one; on REOPEN `propose` and `at_target` both use the recorded target, so (7a) never releases a position the
    proposal still moves, nor the other way round. Other bar-end events use the recomputed target."""
    p = params(be_offset_r="0.1")
    state = StrategyState.of(latched=True, target=D("1.20020"))          # recorded with R = 0.00200
    reopen = s_ctx("1.20200", event=ArmEvent.REOPEN, handles=["1.20015"])
    assert S.target(p, reopen) == D("1.20010")                            # R = 0.00100 after the re-pin
    e = S.propose(p, reopen, state, ArmEvent.REOPEN)
    assert e.proposal is not None and e.proposal.target(O1) == D("1.20020") and e.target == D("1.20020")
    assert not S.at_target(p, reopen, state)                              # 1.20015 is short of 1.20020
    at = replace(reopen, handles=(replace(reopen.handles[0], level=D("1.20020")),))
    assert S.at_target(p, at, state) and S.propose(p, at, state, ArmEvent.REOPEN).proposal == Proposal(())
    # a POST on the same handles: the recomputed target, which 1.20015 is already beyond
    post = s_ctx("1.20200", handles=["1.20015"])
    assert S.at_target(p, post, state) and S.propose(p, post, state, ArmEvent.POST).proposal == Proposal(())
    # without a recorded target, REOPEN recomputes
    assert S.at_target(p, reopen, StrategyState.of(latched=True))


def test_commit_state_records_the_target_even_when_no_handle_moves() -> None:
    """A first trigger with every handle already beyond the target records that target, never None."""
    p = params(be_offset_r="0.1")
    done = propose("1.20200", p=p, handles=["1.20050"], stop="1.20050")
    assert done.trigger_met and done.proposal == Proposal(()) and done.target == D("1.2001")
    after = S.commit_state(p, FRESH, done, Outcome.COMPLETE)
    assert after.flag("latched") and after.text("target") == "1.2001"
    with pytest.raises(ValueError, match="target"):
        Evaluation(False, reason=BELOW_TRIGGER, target=D("1.2"))
    with pytest.raises(ValueError, match="target"):
        Evaluation(True, Proposal(), target=D("NaN"))


def test_migrate_state() -> None:
    state = StrategyState.of(latched=True, target=D("1.2001"))
    assert S.migrate_state(1, state) == state
    assert isinstance(S.migrate_state(1, StrategyState.of(other=1)), Refuse)
    assert isinstance(S.migrate_state(2, state), Refuse)
