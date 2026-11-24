"""The envelope pipeline (plan S21; design §B6.2, §B6.3, §B6.4, §B5.2, §B12; D1-D3, R11-M04, IN-10, R11-m).

D3 long and short: a PRE `ADJUST`, the stop moved to break-even before the bar end, a confirming close → `COMPLETE`; a
close below the trigger → `PRE_NOT_CONFIRMED`, still managed under `pre_basis` (a next close just beyond entry is still
managed until a bar end confirms against the pre-move stop); a stop loosened beyond `pre_basis` → tested at the
looser stop and `pre_basis` cleared; a StoneX cancel / replace giving the stop child a new id → `pre_basis` still
applies. Bar 0 PRE with a volatile first bar → `PRE_NOT_CONFIRMED(FIRST_BAR_VOLATILE)`. PRE at offset 30 s has a 10 s window (S38c) and
computes nothing from a quote received after fire; PRE and POST use one stop-basis function (S38c); an unknown calendar → `MARKET_CLOSED`; a fill in the last `pre_offset` gets no PRE; PRE never
runs FirstBarGuard and never changes the SM11 state; a trigger met with one handle short of break-even is no release.
The envelope property test: the post-guards only turn an `ADJUST` into a HOLD, a FAULT or `BROKER_AHEAD`.
Synthetic levels only.
"""
from __future__ import annotations

import random
from dataclasses import replace
from decimal import Decimal
from typing import ClassVar

import pytest

from src.calendar.machine import SessionState
from src.db.arm_ports import DecisionMemory
from src.marketdata.ports import HandleRef
from src.marketdata.timing import SkewEstimate
from src.marketdata.types import M15, Quote
from src.riskmgr.envelope import EnvelopeParams
from src.riskmgr.evaluation import final_advice_target_of
from src.riskmgr.guard import GuardResult
from src.riskmgr.guards.first_bar import CONDITIONAL_LABEL, FIRST_BAR_VOLATILE, PRE_ANCHOR, FirstBarGuard
from src.riskmgr.outcomes import Category, Outcome, OutcomeCode
from src.riskmgr.pipeline import (
    AT_TARGET,
    AWAITING_BROKER_BAR,
    FILL_IN_PRE_WINDOW,
    MARKET_CLOSED_AT_PRE,
    PRE_EXPIRED,
    PRE_QUOTE_AFTER_FIRE,
    PipelineFault,
    PipelineInput,
    PipelineResult,
    PreWindow,
    ResultKind,
    apply_envelope,
    pre_window,
    round_toward_profit,
    run_pipeline,
)
from src.riskmgr.registry import REGISTRY, ArmRegistry, RegistryError
from src.riskmgr.sm11 import ArmState, Sm11Event, Transition, steps
from src.riskmgr.stopbasis import PreBasis, StopAtT, StopUnavailable
from src.riskmgr.strategies.break_even_on_risk_close import (
    BELOW_TRIGGER,
    BREAK_EVEN_ON_RISK_CLOSE_V1,
    BreakEvenOnRiskClose,
)
from src.riskmgr.strategy import Evaluation, Proposal, ReleasePolicy, StrategyParams, StrategyState
from src.riskmgr.types import ArmEvent, BarContext, HandleView, StopBasis
from tests.riskmgr.kind_a import (
    ADAPTER,
    B0,
    FILL,
    FIVE,
    INC,
    Q,
    S0_LONG,
    S0_SHORT,
    atr_id,
    atr_value,
    bar,
    bar0_with_range,
    flat_history,
    s_ctx,
    series,
    strategy_row,
)
from tests.riskmgr.toys import ToyGuard, ToyStrategy, toy_row

D = Decimal
E = D("1.20000")
T = B0 + 4 * Q            # the bar end of `s_ctx`
SKEW = SkewEstimate(p50_ms=0, settle_ms=5_000)
FRESH = StrategyState.of(latched=False)


class Cal:
    def __init__(self, state: SessionState | None) -> None:
        self.s = state

    def state(self, adapter_id: str, symbol: str) -> SessionState | None:
        return self.s


OPEN = Cal(SessionState.OPEN)


def lv(sign: int, x: str) -> str:
    """The level `E + σ·x` as text."""
    return str(E + sign * D(x))


def params(**over: object) -> StrategyParams:
    return REGISTRY.parse_params(BREAK_EVEN_ON_RISK_CLOSE_V1.KIND, 1, strategy_row(**{"be_offset_r": "0", **over}))


def quote(price: str, received_ms: int) -> Quote:
    p = D(price)
    return Quote(adapter_id=ADAPTER, symbol=FIVE, bid=p, ask=p, broker_ms=received_ms - 100, received_ms=received_ms)


def pre_run(close: str, *, side: str = "BUY", stop: str | None = None, handles: list[str] | None = None,
            offset: int = 40, now: int | None = None, received: int | None = None, cal: Cal = OPEN,
            arm: ArmState = ArmState.TRACKING, registry: ArmRegistry = REGISTRY, ctx: BarContext | None = None,
            p: StrategyParams | None = None, **kw: object) -> PipelineResult:
    base = ctx if ctx is not None else s_ctx(close, side=side, stop=stop, handles=handles, event=ArmEvent.PRE)
    win = pre_window(base.bar_end_ms, M15, SKEW, offset)
    c = replace(base, quote=quote(close, win.fire_ms - 100 if received is None else received), exit_quote_live=True)
    inp = PipelineInput(params=p or params(), ctx=c, state=FRESH, arm_state=arm,
                        now_ms=win.fire_ms if now is None else now, calendar=cal, window=win, **kw)  # type: ignore[arg-type]
    return run_pipeline(inp, registry)


def post_run(close: str, *, side: str = "BUY", stop: str | None = None, handles: list[str] | None = None,
             s_at_t: StopAtT | StopUnavailable | None = None, live: str | None = None, cal: Cal = OPEN,
             arm: ArmState = ArmState.TRACKING, now: int | None = None, state: StrategyState = FRESH,
             p: StrategyParams | None = None, ctx: BarContext | None = None, registry: ArmRegistry = REGISTRY,
             event: ArmEvent = ArmEvent.POST, **kw: object) -> PipelineResult:
    c = ctx if ctx is not None else s_ctx(close, side=side, stop=stop, handles=handles, event=event)
    c = replace(c, exit_quote_live=True)
    basis = s_at_t if s_at_t is not None else StopAtT(c.stop if c.stop is not None else E, StopBasis.STREAM)
    t_now = c.bar_end_ms + 60_000 if now is None else now
    inp = PipelineInput(params=p or params(), ctx=c, state=state, arm_state=arm, now_ms=t_now, calendar=cal,
                        stop_at_t=basis, live_quote=quote(live or close, t_now - 500), **kw)  # type: ignore[arg-type]
    return run_pipeline(inp, registry)


def code_of(r: PipelineResult) -> tuple[Outcome | None, str]:
    return (r.outcome, r.code.reason if r.code is not None else "")


# ---------------------------------------------------------------------------------------------------- D3, long and short
@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_d3_pre_adjust_move_and_confirming_close_completes(sign: int, side: str) -> None:
    s0 = lv(sign, "-0.00100")
    pre = pre_run(lv(sign, "0.00150"), side=side)
    assert pre.outcome is Outcome.ADJUST and pre.sm11_events == () and pre.final is False
    assert pre.pre_levels is not None and pre.pre_levels.s_pre == D(s0)
    assert dict(pre.targets) == {HandleRef("CHILD_ORDER", "O1"): E}
    # the user moves the stop to break-even before the bar end
    post = post_run(lv(sign, "0.00150"), side=side, stop=str(E), this_bar_pre=pre.pre_levels,
                    s_at_t=StopAtT(E, StopBasis.STREAM))
    assert post.outcome is Outcome.COMPLETE and post.code is not None and post.code.category is Category.INFO
    assert post.stop_basis is StopBasis.AT_PRE and post.trigger_met is True and post.final and post.shown
    assert post.sm11_events == (Sm11Event.RELEASED,) and post.pre_basis is None
    assert steps(ArmState.TRACKING, post.sm11_events) == Transition(ArmState.STRATEGY_COMPLETE)


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_d3_close_below_trigger_is_pre_not_confirmed_then_pre_basis(sign: int, side: str) -> None:
    pre = pre_run(lv(sign, "0.00150"), side=side)
    assert pre.pre_levels is not None
    post = post_run(lv(sign, "0.00050"), side=side, stop=str(E), this_bar_pre=pre.pre_levels,
                    s_at_t=StopAtT(E, StopBasis.STREAM))
    assert post.outcome is Outcome.PRE_NOT_CONFIRMED and post.code is not None
    assert post.code.category is Category.INFO and post.shown and post.final
    assert post.code.reason == Outcome.AT_TARGET_UNCONFIRMED.value
    assert post.pre_basis == pre.pre_levels and Sm11Event.RELEASED not in post.sm11_events
    # next bar: a close just beyond entry is still managed against the pre-move stop
    nxt = post_run(lv(sign, "0.00010"), side=side, stop=str(E), pre_basis=post.pre_basis,
                   s_at_t=StopAtT(E, StopBasis.STREAM), prev_final_at_target=True)
    assert nxt.outcome is Outcome.AT_TARGET_UNCONFIRMED and nxt.stop_basis is StopBasis.AT_PRE
    assert nxt.trigger_met is False and nxt.shown is False and nxt.pre_basis == pre.pre_levels
    # a later bar end confirms against the pre-move stop: released, pre_basis cleared
    done = post_run(lv(sign, "0.00101"), side=side, stop=str(E), pre_basis=nxt.pre_basis,
                    s_at_t=StopAtT(E, StopBasis.STREAM), prev_final_at_target=True)
    assert done.outcome is Outcome.COMPLETE and done.stop_basis is StopBasis.AT_PRE and done.pre_basis is None


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_one_stop_basis_pre_agrees_with_post_after_a_partial_tighten(sign: int, side: str) -> None:
    """Round 12 / S38c: PRE_NOT_CONFIRMED, then the user tightens partway; the next PRE tests the trigger against
    the same basis as the next POST (the farther of S_pre and the current stop), so the two agree."""
    pre = pre_run(lv(sign, "0.00150"), side=side)
    post = post_run(lv(sign, "0.00050"), side=side, stop=str(E), this_bar_pre=pre.pre_levels,
                    s_at_t=StopAtT(E, StopBasis.STREAM))
    assert post.outcome is Outcome.PRE_NOT_CONFIRMED and post.pre_basis is not None
    partway = lv(sign, "-0.00050")                     # tightened partway between entry and S_pre
    for close, met in ((lv(sign, "0.00080"), False), (lv(sign, "0.00120"), True)):
        nxt_pre = pre_run(close, side=side, stop=partway, pre_basis=post.pre_basis)
        nxt_post = post_run(close, side=side, stop=partway, pre_basis=post.pre_basis,
                            s_at_t=StopAtT(D(partway), StopBasis.STREAM))
        assert nxt_pre.trigger_met is met and nxt_post.trigger_met is met
        assert nxt_pre.stop_basis is nxt_post.stop_basis is StopBasis.AT_PRE
        assert "pre_basis_held" in nxt_pre.caveats
        assert (nxt_pre.outcome is Outcome.ADJUST) is (nxt_post.outcome is Outcome.ADJUST) is met
        assert nxt_pre.pre_basis == post.pre_basis and nxt_pre.sm11_events == () and not nxt_pre.final


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_pre_with_a_held_basis_at_target_still_renders_an_item(sign: int, side: str) -> None:
    """Round 12 / S38c: the stop fully at break-even after PRE_NOT_CONFIRMED: PRE no longer goes silent; it shows
    AT_TARGET_UNCONFIRMED with the trigger as the bar end will test it (against the pre-move stop)."""
    pre = pre_run(lv(sign, "0.00150"), side=side)
    post = post_run(lv(sign, "0.00050"), side=side, stop=str(E), this_bar_pre=pre.pre_levels,
                    s_at_t=StopAtT(E, StopBasis.STREAM))
    for close, met in ((lv(sign, "0.00050"), False), (lv(sign, "0.00150"), True)):
        r = pre_run(close, side=side, stop=str(E), pre_basis=post.pre_basis)
        assert r.kind is ResultKind.DECISION and r.outcome is Outcome.AT_TARGET_UNCONFIRMED
        assert r.trigger_met is met and r.at_target is True and r.stop_basis is StopBasis.AT_PRE
        assert "pre_basis_held" in r.caveats and r.sm11_events == () and not r.final
        bar_end = post_run(close, side=side, stop=str(E), pre_basis=post.pre_basis,
                           s_at_t=StopAtT(E, StopBasis.STREAM))
        assert bar_end.trigger_met is met
        assert bar_end.outcome is (Outcome.COMPLETE if met else Outcome.AT_TARGET_UNCONFIRMED)
    # with no basis held, a position at its target still gets no PRE item
    assert pre_run(lv(sign, "0.00150"), side=side, stop=str(E)).reason == AT_TARGET


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_d3_loosened_beyond_pre_basis_tests_current_and_clears(sign: int, side: str) -> None:
    pb = PreBasis.of(side, E, [(HandleRef("CHILD_ORDER", "O1"), D(lv(sign, "-0.00100")))], T - Q)  # type: ignore[arg-type]
    looser = lv(sign, "-0.00200")
    r = post_run(lv(sign, "0.00150"), side=side, stop=looser, pre_basis=pb,
                 s_at_t=StopAtT(D(looser), StopBasis.STREAM))
    assert r.stop_basis is StopBasis.STREAM and r.pre_basis is None
    assert code_of(r) == (Outcome.STRATEGY_REASON, BELOW_TRIGGER)     # favour 1.5 R-units below the 2-unit risk


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_d3_cancel_replace_new_id_keeps_pre_basis(sign: int, side: str) -> None:
    pb = PreBasis.of(side, E, [(HandleRef("CHILD_ORDER", "O1"), D(lv(sign, "-0.00100")))], T - Q)  # type: ignore[arg-type]
    ctx = replace(s_ctx(lv(sign, "0.00050"), side=side, stop=str(E)),
                  handles=(HandleView(kind="CHILD_ORDER", handle_id="O7", level=E),))
    r = post_run("0", ctx=ctx, live=lv(sign, "0.00050"), pre_basis=pb, s_at_t=StopAtT(E, StopBasis.STREAM))
    assert r.stop_basis is StopBasis.AT_PRE and r.trigger_met is False and r.pre_basis == pb


def test_trigger_met_with_one_handle_short_is_no_release() -> None:
    r = post_run("1.20150", handles=[str(E), "1.19900"], s_at_t=StopAtT(D("1.19900"), StopBasis.STREAM))
    assert r.trigger_met is True and r.at_target is False
    assert r.outcome is Outcome.ADJUST and [h.id for h, _ in r.targets] == ["O2"]
    assert r.sm11_events == (Sm11Event.FINAL_ADJUST,)


def test_at_target_unconfirmed_shown_once_per_run() -> None:
    first = post_run("1.20050", stop=str(E), s_at_t=StopAtT(D("1.19900"), StopBasis.STREAM))
    assert first.outcome is Outcome.AT_TARGET_UNCONFIRMED and first.shown and first.at_target
    again = post_run("1.20050", stop=str(E), s_at_t=StopAtT(D("1.19900"), StopBasis.STREAM),
                     prev_final_at_target=True)
    assert again.outcome is Outcome.AT_TARGET_UNCONFIRMED and not again.shown


# ---------------------------------------------------------------------------------------------------- bar 0
def _bar0_ctx(event: ArmEvent, *, close_body: str = "0.00200") -> BarContext:
    hist = [*flat_history(FIVE, D("0.00500")), bar0_with_range(FIVE, D("0.00900"), body=D(close_body))]
    base = s_ctx("1.20200", event=event, bar_index=0)
    return replace(base, bar_end_ms=B0 + Q, bar=hist[-1] if event is not ArmEvent.PRE else None,
                   history=((series(FIVE), tuple(hist)),), indicators=(((atr_id(FIVE), "value"),
                                                                         (atr_value(FIVE, B0),)),),
                   entry_time_ms=FILL)


def test_bar0_pre_then_volatile_first_bar_is_pre_not_confirmed() -> None:
    pre = pre_run("1.20150", ctx=_bar0_ctx(ArmEvent.PRE))
    assert pre.outcome is Outcome.ADJUST and pre.labels == (CONDITIONAL_LABEL,)
    assert pre.guard_verdicts == ()                                   # PRE never runs FirstBarGuard
    post = post_run("0", ctx=_bar0_ctx(ArmEvent.POST), live="1.20150", this_bar_pre=pre.pre_levels,
                    s_at_t=StopAtT(E, StopBasis.STREAM), arm=ArmState.ARMED)
    assert post.code == OutcomeCode.pre_not_confirmed("first_bar", FIRST_BAR_VOLATILE)
    assert post.pre_basis == pre.pre_levels and post.final
    assert post.sm11_events == (Sm11Event.FIRST_POST,)
    assert steps(ArmState.ARMED, post.sm11_events) == Transition(ArmState.TRACKING)


def test_bar0_volatile_without_pre_is_a_guard_hold() -> None:
    post = post_run("0", ctx=_bar0_ctx(ArmEvent.POST), live="1.20150", s_at_t=StopAtT(E, StopBasis.STREAM))
    assert post.outcome is Outcome.GUARD_REASON and post.code is not None
    assert (post.code.owner, post.code.reason, post.code.category) == ("first_bar", FIRST_BAR_VOLATILE, Category.HOLD)


def _ctx_at(side: str, close: str, bucket: int) -> BarContext:
    """A bar-end context for the bar in `bucket` (bar 0 = the fill bucket `B0`): a flat trigger-side bar at `close`,
    plus the guard's inputs (bid history ending in a volatile bar 0, range 0.00900 against an ATR of 0.00500, and a
    warm ATR value). The guard inputs stay on later bars too, so a guard that wrongly applied after bar 0 would HOLD."""
    base = s_ctx(close, side=side, bar_index=(bucket - B0) // Q)
    assert base.bar is not None
    c = D(close)
    hist = (*flat_history(FIVE, D("0.00500")), bar0_with_range(FIVE, D("0.00900")))
    return replace(base, bar_end_ms=bucket + Q, entry_time_ms=FILL,
                   bar=bar(FIVE, bucket, c, c, c, c, side=base.bar.series.side),
                   history=((series(FIVE), hist),), indicators=(((atr_id(FIVE), "value"), (atr_value(FIVE, B0),)),))


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
@pytest.mark.parametrize("bar1_close,proposes", [("0.00150", True), ("0.00050", False)])
def test_volatile_bar0_then_bar1_retests(sign: int, side: str, bar1_close: str, proposes: bool) -> None:
    """Q2: a volatile trigger bar 0 delays the trail by exactly one bar. Bar 1 re-tests the trigger on its own
    close: met -> ADJUST straight to break-even; not met -> no proposal (the delay is a re-test, not a latch)."""
    s0 = S0_LONG if sign == 1 else S0_SHORT
    r0 = post_run("0", ctx=_ctx_at(side, lv(sign, "0.00150"), B0), live=lv(sign, "0.00150"),
                  s_at_t=StopAtT(s0, StopBasis.STREAM), arm=ArmState.ARMED)
    assert r0.outcome is Outcome.GUARD_REASON and r0.code is not None
    assert (r0.code.owner, r0.code.reason, r0.code.category) == ("first_bar", FIRST_BAR_VOLATILE, Category.HOLD)
    assert not r0.trigger_met and r0.targets == () and r0.final
    assert r0.state == FRESH and r0.pre_basis is None
    assert steps(ArmState.ARMED, r0.sm11_events) == Transition(ArmState.TRACKING)

    close1 = lv(sign, bar1_close)
    r1 = post_run("0", ctx=_ctx_at(side, close1, B0 + Q), live=close1, s_at_t=StopAtT(s0, StopBasis.STREAM),
                  state=r0.state, arm=ArmState.TRACKING, pre_basis=r0.pre_basis)
    assert r1.guard_verdicts == ()                                     # FirstBarGuard does not apply at bar 1
    if proposes:
        assert r1.outcome is Outcome.ADJUST and r1.trigger_met is True
        assert dict(r1.targets) == {HandleRef("CHILD_ORDER", "O1"): E}
        for _, target in r1.targets:                                   # never loosen, never past entry
            assert sign * (target - s0) > 0 and sign * (target - E) <= 0
    else:
        assert r1.outcome is not Outcome.ADJUST and not r1.trigger_met and r1.targets == ()


class SpyFirstBar(FirstBarGuard):
    calls: ClassVar[list[str]] = []

    def evaluate(self, params, ctx):  # type: ignore[no-untyped-def]
        SpyFirstBar.calls.append(ctx.event.value)
        return super().evaluate(params, ctx)


def test_pre_never_runs_first_bar_guard_and_never_changes_state() -> None:
    reg = ArmRegistry(strategies=[BREAK_EVEN_ON_RISK_CLOSE_V1], guards=[SpyFirstBar()])
    SpyFirstBar.calls.clear()
    for close in ("1.20150", "1.20050", "1.19950"):
        r = pre_run(close, ctx=_bar0_ctx(ArmEvent.PRE), registry=reg, pre_basis=None)
        assert r.sm11_events == () and r.state == FRESH and r.pre_basis is None and not r.final
    assert SpyFirstBar.calls == []
    post_run("0", ctx=_bar0_ctx(ArmEvent.POST), live="1.20150", registry=reg, s_at_t=StopAtT(E, StopBasis.STREAM))
    assert SpyFirstBar.calls == ["POST"]


def test_pre_anchor_known_at_pre() -> None:
    p = params(guards=[{"kind": "first_bar", "version": 1, "params": {"anchor": "next_full_bucket"}}])
    r = pre_run("1.20150", ctx=_bar0_ctx(ArmEvent.PRE), p=p)
    assert r.outcome is Outcome.GUARD_REASON and r.code is not None and r.code.reason == PRE_ANCHOR


def test_pre_at_target_gets_no_item_before_a_known_guard_hold() -> None:
    """§B6.3: at PRE a position at its target gets no item, even where a guard's HOLD is known before bar end."""
    p = params(guards=[{"kind": "first_bar", "version": 1, "params": {"anchor": "next_full_bucket"}}])
    ctx = replace(_bar0_ctx(ArmEvent.PRE), stop=E, handles=(HandleView(kind="CHILD_ORDER", handle_id="O1", level=E),))
    r = pre_run("1.20150", ctx=ctx, p=p)
    assert r.kind is ResultKind.NO_ITEM and r.reason == AT_TARGET and r.code is None


# ---------------------------------------------------------------------------------------------------- PRE timing
def test_pre_at_offset_30_has_a_ten_second_window() -> None:
    """S38c (round 12): valid until fire + 10 s, so 30 s (D1's lower bound) leaves PRE a 10 s window."""
    win = pre_window(T, M15, SKEW, 30)
    assert (win.fire_ms, win.valid_until_ms) == (T - 30_000, T - 20_000)
    assert pre_run("1.20150", offset=30).outcome is Outcome.ADJUST
    assert pre_run("1.20150", offset=30, now=T - 20_000).outcome is Outcome.ADJUST
    assert pre_run("1.20150", offset=30, now=T - 20_000 + 1).reason == PRE_EXPIRED
    late = pre_run("1.20150", offset=30, received=T - 30_000 + 1)
    assert code_of(late) == (Outcome.DATA_UNAVAILABLE, PRE_QUOTE_AFTER_FIRE)


def test_pre_default_window() -> None:
    win = pre_window(T, M15, SKEW, 40)
    assert win == PreWindow(T - 40_000, T - 30_000, 40)
    assert pre_run("1.20150", now=T - 30_000).outcome is Outcome.ADJUST
    assert pre_run("1.20150", now=T - 30_000 + 1).kind is ResultKind.NO_ITEM


def test_fill_in_last_pre_offset_gets_no_pre() -> None:
    ctx = replace(s_ctx("1.20150", event=ArmEvent.PRE), entry_time_ms=T - 30_000)
    assert pre_run("1.20150", ctx=ctx).reason == FILL_IN_PRE_WINDOW
    ctx2 = replace(ctx, entry_time_ms=T - 40_000)
    assert pre_run("1.20150", ctx=ctx2).outcome is Outcome.ADJUST


def test_pre_no_item_at_target_closed_or_released() -> None:
    assert pre_run("1.20150", stop=str(E)).reason == AT_TARGET
    assert pre_run("1.20150", cal=Cal(None)).reason == MARKET_CLOSED_AT_PRE
    assert pre_run("1.20150", arm=ArmState.STRATEGY_COMPLETE).kind is ResultKind.NO_ITEM


# ---------------------------------------------------------------------------------------------------- calendar, quote
@pytest.mark.parametrize("state", [None, SessionState.CLOSED, SessionState.ROLLOVER, SessionState.PRE_OPEN])
def test_unknown_or_closed_calendar_is_market_closed(state: SessionState | None) -> None:
    r = post_run("1.20150", cal=Cal(state))
    assert r.outcome is Outcome.MARKET_CLOSED and r.code is not None and r.code.category is Category.HOLD
    assert post_run("1.20150", cal=Cal(SessionState.OPEN_BY_EVIDENCE)).outcome is Outcome.ADJUST


def test_complete_needs_no_open_market() -> None:
    r = post_run("1.20150", stop=str(E), cal=Cal(None), s_at_t=StopAtT(D("1.19900"), StopBasis.STREAM))
    assert r.outcome is Outcome.COMPLETE


def test_post_guards_quote_gap_and_rounding() -> None:
    p = params(be_offset_r="0.123")                                  # target E + 0.123 R = 1.200123
    r = post_run("1.20500", p=p, live="1.20500")
    assert r.outcome is Outcome.ADJUST and dict(r.targets)[HandleRef("CHILD_ORDER", "O1")] == D("1.20013")
    short = post_run("1.19500", side="SELL", p=p)
    assert dict(short.targets)[HandleRef("CHILD_ORDER", "O1")] == D("1.19987")
    near = post_run("1.20150", live="1.20000")                       # quote at the target: through the level
    assert near.outcome is Outcome.PRICE_THROUGH_LEVEL
    gap = post_run("1.20150", live="1.20040", envelope=EnvelopeParams(min_gap_r=D("0.5")))
    assert gap.outcome is Outcome.PRICE_THROUGH_LEVEL                # 0.4 R < 0.5 R
    dead = replace(s_ctx("1.20150"), exit_quote_live=False)
    inp = PipelineInput(params=params(), ctx=dead, state=FRESH, arm_state=ArmState.TRACKING, now_ms=T + 60_000,
                        calendar=OPEN, stop_at_t=StopAtT(D("1.19900"), StopBasis.STREAM))
    assert code_of(run_pipeline(inp)) == (Outcome.DATA_UNAVAILABLE, "exit_quote_not_live")


def test_round_toward_profit() -> None:
    assert round_toward_profit(D("1.200001"), INC[FIVE], 1) == D("1.20001")
    assert round_toward_profit(D("1.200001"), INC[FIVE], -1) == D("1.20000")
    assert round_toward_profit(D("1.20000"), INC[FIVE], 1) == D("1.20000")


# ---------------------------------------------------------------------------------------------------- pre-guards, (4), (5)
def test_pre_guard_faults() -> None:
    stale = replace(s_ctx("1.20150"), account_image_fresh=False, image_reasons=("orders_fence",))
    assert code_of(post_run("0", ctx=stale)) == (Outcome.DATA_UNAVAILABLE, "orders_fence")
    no_stop = replace(s_ctx("1.20150"), stop=None, handles=())
    assert post_run("0", ctx=no_stop, live="1.20150").outcome is Outcome.NO_STOP
    no_inc = replace(s_ctx("1.20150"), price_increment=None)
    r = post_run("0", ctx=no_inc, live="1.20150")
    assert code_of(r) == (Outcome.DATA_UNAVAILABLE, "increment_unknown") and not r.final
    read = replace(s_ctx("1.20150"), handles=(HandleView(kind="CHILD_ORDER", handle_id="O1", level=D("1.19900"),
                                                         read_ms=T + 40_000),))
    assert code_of(post_run("0", ctx=read, live="1.20150", stop_reads=True)) == (Outcome.DATA_UNAVAILABLE,
                                                                                 "stop_stale")
    assert post_run("0", ctx=read, live="1.20150", stop_reads=True, now=T + 55_000).outcome is Outcome.ADJUST


def test_released_position() -> None:
    at = post_run("1.19950", stop=str(E), arm=ArmState.STRATEGY_COMPLETE)
    assert at.outcome is Outcome.COMPLETE and not at.shown and at.sm11_events == ()
    loose = post_run("1.20150", arm=ArmState.STRATEGY_COMPLETE)
    assert loose.outcome is Outcome.LOOSENED_AFTER_COMPLETE and loose.shown
    assert not post_run("1.20150", arm=ArmState.STRATEGY_COMPLETE, loosened_told=True).shown
    rearm = post_run("1.20150", arm=ArmState.STRATEGY_COMPLETE, p=params(rearm_on_loosen="on"))
    assert rearm.outcome is Outcome.ADJUST
    assert rearm.sm11_events == (Sm11Event.REARM_ON_LOOSEN, Sm11Event.FINAL_ADJUST)
    assert steps(ArmState.STRATEGY_COMPLETE, rearm.sm11_events) == Transition(ArmState.ADVICE_OUTSTANDING)
    fault = replace(s_ctx("1.20150"), stop=None, handles=())
    hidden = post_run("0", ctx=fault, live="1.20150", arm=ArmState.STRATEGY_COMPLETE)
    assert hidden.outcome is Outcome.NO_STOP and not hidden.shown


def test_awaiting_broker_bar_then_unresolved_at_cap() -> None:
    ctx = replace(s_ctx("1.20150"), bar=None, decision_cap_ms=T + Q + 5_000)
    wait = post_run("0", ctx=ctx, live="1.20150", now=T + 60_000)
    assert wait.kind is ResultKind.PENDING and wait.reason == AWAITING_BROKER_BAR and wait.code is None
    cap = post_run("0", ctx=ctx, live="1.20150", now=T + Q + 5_000, arm=ArmState.ARMED)
    assert code_of(cap) == (Outcome.UNRESOLVED_RANGE, "broker_bar_not_final") and cap.final
    assert cap.sm11_events == (Sm11Event.FIRST_POST,)


def test_guard_pending_is_data_unavailable_until_cap() -> None:
    ctx = replace(_bar0_ctx(ArmEvent.POST), indicators=(((atr_id(FIVE), "value"),
                                                         (atr_value(FIVE, B0, warm=False),)),),
                  decision_cap_ms=B0 + 2 * Q + 5_000)
    r = post_run("0", ctx=ctx, live="1.20150", now=B0 + Q + 60_000)
    assert code_of(r) == (Outcome.DATA_UNAVAILABLE, "first_bar_pending") and not r.final
    assert r.state == FRESH and r.sm11_events == ()
    late = post_run("0", ctx=ctx, live="1.20150", now=B0 + 2 * Q + 5_000)
    assert late.outcome is Outcome.GUARD_REASON and late.code is not None and late.code.detail == "timeout"


def test_final_fault_leaves_armed_and_withdraws_advice() -> None:
    """§B12: ARMED --(first POST, any outcome)--> TRACKING, a bar-end NO_STOP included; it also withdraws outstanding
    advice."""
    no_stop = replace(s_ctx("1.20150"), stop=None, handles=())
    armed = post_run("0", ctx=no_stop, live="1.20150", arm=ArmState.ARMED)
    assert armed.outcome is Outcome.NO_STOP and armed.final and armed.sm11_events == (Sm11Event.FIRST_POST,)
    out = post_run("0", ctx=no_stop, live="1.20150", arm=ArmState.ADVICE_OUTSTANDING)
    assert out.sm11_events == (Sm11Event.ADVICE_WITHDRAWN,)
    assert steps(ArmState.ADVICE_OUTSTANDING, out.sm11_events) == Transition(ArmState.TRACKING)


@pytest.mark.parametrize("kw", [dict(live="1.20000"), dict(cal=Cal(None))])
def test_post_guard_hold_withdraws_advice_only_with_latch_off(kw: dict[str, object]) -> None:
    """§B12: "POST without ADJUST and latch off: withdrawn". With the latch on the recorded trigger re-proposes, so
    the advice stands through a post-guard HOLD (PRICE_THROUGH_LEVEL, MARKET_CLOSED)."""
    off = post_run("1.20150", arm=ArmState.ADVICE_OUTSTANDING, **kw)  # type: ignore[arg-type]
    assert off.outcome in (Outcome.PRICE_THROUGH_LEVEL, Outcome.MARKET_CLOSED)
    assert off.sm11_events == (Sm11Event.ADVICE_WITHDRAWN,)
    on = post_run("1.20150", arm=ArmState.ADVICE_OUTSTANDING, p=params(latch="on"), **kw)  # type: ignore[arg-type]
    assert on.outcome is off.outcome and on.sm11_events == ()
    nothing = post_run("1.20050", arm=ArmState.ADVICE_OUTSTANDING, p=params(latch="on"))
    assert nothing.outcome is Outcome.STRATEGY_REASON and nothing.sm11_events == (Sm11Event.ADVICE_WITHDRAWN,)


def test_stop_at_bar_end_unavailable_is_not_final() -> None:
    r = post_run("1.20150", s_at_t=StopUnavailable())
    assert code_of(r) == (Outcome.DATA_UNAVAILABLE, "stop_at_bar_end") and not r.final


def test_commit_state_records_trigger_whatever_post_guards_said() -> None:
    r = post_run("1.20150", cal=Cal(None))
    assert r.outcome is Outcome.MARKET_CLOSED and r.state.flag("latched")
    r2 = post_run("1.20050")
    assert not r2.state.flag("latched")


def test_late_reopen_corrected_are_bar_end_events() -> None:
    for ev in (ArmEvent.LATE, ArmEvent.REOPEN, ArmEvent.CORRECTED):
        r = post_run("1.20150", stop=str(E), s_at_t=StopAtT(D("1.19900"), StopBasis.STREAM), event=ev)
        assert r.outcome is Outcome.COMPLETE and r.event is ev


# ---------------------------------------------------------------------------------------------------- release rule
def test_release_rule_is_defined_on_the_evaluation_for_any_kind() -> None:
    """A toy kind with its own reason codes gets the same (7a) rule."""
    toy = ToyStrategy()
    reg = ArmRegistry(strategies=[toy], guards=[ToyGuard()])
    p = reg.parse_params(toy.KIND, 1, toy_row())
    from tests.riskmgr.toys import ctx as toy_ctx
    c = toy_ctx("1.10150", stop="1.10000")
    inp = PipelineInput(params=p, ctx=replace(c, exit_quote_live=True), state=StrategyState(),
                        arm_state=ArmState.TRACKING, now_ms=c.bar_end_ms + 60_000, calendar=OPEN,
                        stop_at_t=StopAtT(D("1.09900"), StopBasis.STREAM))
    assert run_pipeline(inp, reg).outcome is Outcome.COMPLETE
    below = replace(inp, ctx=replace(toy_ctx("1.10050", stop="1.10000"), exit_quote_live=True))
    assert run_pipeline(below, reg).outcome is Outcome.AT_TARGET_UNCONFIRMED
    pb = PreBasis.of("BUY", D("1.10000"), [(HandleRef("CHILD_ORDER", "O1"), D("1.09900"))], c.bar_end_ms)
    assert run_pipeline(replace(below, this_bar_pre=pb), reg).code == OutcomeCode.pre_not_confirmed(
        "", Outcome.AT_TARGET_UNCONFIRMED.value)


class RaisingGuard(FirstBarGuard):
    def evaluate(self, params, ctx):  # type: ignore[no-untyped-def]
        raise RuntimeError("planted")


class UndeclaredReasonGuard(FirstBarGuard):
    def evaluate(self, params, ctx):  # type: ignore[no-untyped-def]
        return GuardResult.hold("NOT_DECLARED")


def test_an_undeclared_guard_reason_is_the_guards_fault_never_stored() -> None:
    """A guard HOLD with a reason outside its REASONS is a `PipelineFault` attributed to the guard version (not a
    bare ValueError charged to the strategy), and is never recorded as `PRE_NOT_CONFIRMED(NOT_DECLARED)` either."""
    reg = ArmRegistry(strategies=[BREAK_EVEN_ON_RISK_CLOSE_V1], guards=[UndeclaredReasonGuard()])
    with pytest.raises(PipelineFault) as e:
        post_run("0", ctx=_bar0_ctx(ArmEvent.POST), live="1.20150", registry=reg)
    assert e.value.scope_id == "guard:first_bar:1"
    pb = PreBasis.of("BUY", E, [(HandleRef("CHILD_ORDER", "O1"), D("1.19900"))], B0 + Q)
    with pytest.raises(PipelineFault) as e2:
        post_run("0", ctx=_bar0_ctx(ArmEvent.POST), live="1.20150", registry=reg, this_bar_pre=pb)
    assert e2.value.scope_id == "guard:first_bar:1"


class UndeclaredReasonStrategy(BreakEvenOnRiskClose):
    def propose(self, params, ctx, state, event):  # type: ignore[no-untyped-def]
        e = super().propose(params, ctx, state, event)
        return replace(e, reason="NOT_DECLARED") if e.proposal is None else e


def test_an_undeclared_strategy_reason_is_the_strategys_fault() -> None:
    reg = ArmRegistry(strategies=[UndeclaredReasonStrategy()], guards=[FirstBarGuard()])
    with pytest.raises(PipelineFault) as e:
        post_run("1.20050", registry=reg)
    assert e.value.scope_id == "strategy:break_even_on_risk_close:1"
    pb = PreBasis.of("BUY", E, [(HandleRef("CHILD_ORDER", "O1"), D("1.19900"))], T)
    with pytest.raises(PipelineFault):
        post_run("1.20050", registry=reg, this_bar_pre=pb)
    with pytest.raises(PipelineFault):
        pre_run("1.20050", registry=reg)


def test_a_raising_guard_is_attributed_to_its_version() -> None:
    reg = ArmRegistry(strategies=[BREAK_EVEN_ON_RISK_CLOSE_V1], guards=[RaisingGuard()])
    with pytest.raises(PipelineFault) as e:
        post_run("0", ctx=_bar0_ctx(ArmEvent.POST), live="1.20150", registry=reg)
    assert e.value.scope_id == "guard:first_bar:1" and "planted" not in str(e.value)


# ---------------------------------------------------------------------------------------------------- envelope property
def test_envelope_only_turns_an_adjust_into_a_hold_or_a_fault() -> None:
    """Over generated contexts: the post-guards never add a handle, never name one the strategy did not, move a
    target only toward profit by less than one increment, and otherwise give a HOLD, a FAULT or BROKER_AHEAD."""
    rng = random.Random(21)
    inc = INC[FIVE]
    allowed = {Outcome.ADJUST, Outcome.MARKET_CLOSED, Outcome.PRICE_THROUGH_LEVEL, Outcome.DATA_UNAVAILABLE,
               Outcome.BROKER_AHEAD}
    for _ in range(600):
        side = rng.choice(["BUY", "SELL"])
        sig = 1 if side == "BUY" else -1
        n = rng.randint(1, 3)
        levels = [E + sig * D(rng.randint(-300, 100)) * inc for _ in range(n)]
        handles = tuple(HandleView(kind="CHILD_ORDER", handle_id=f"O{k}", level=lvl) for k, lvl in enumerate(levels))
        ctx = replace(s_ctx("1.20150", side=side), handles=handles,
                      min_stop_distance=D(rng.randint(0, 30)) * inc)
        target = E + sig * D(rng.randint(-50, 50)) * inc / D(rng.choice([1, 3, 7]))
        named = [HandleRef(h.kind, h.handle_id) for h in handles if rng.random() < 0.8]
        ev = Evaluation(True, Proposal(tuple((h, target) for h in named)))
        q = quote(str(E + sig * D(rng.randint(-100, 400)) * inc), T)
        out = apply_envelope(ctx, ev, quote=q, quote_live=rng.random() < 0.9, market_open=rng.random() < 0.9,
                             envelope=EnvelopeParams(min_gap_r=D(rng.randint(0, 5)) / 10))
        if out is None:
            continue
        assert out.code.outcome in allowed
        if out.code.outcome is not Outcome.ADJUST:
            assert out.targets == () and out.code.category in (Category.HOLD, Category.FAULT, Category.INFO)
            continue
        current_of = {HandleRef(x.kind, x.handle_id): x.level for x in handles}
        assert {h for h, _ in out.targets} == {h for h in named if sig * (target - current_of[h]) > 0}
        for h, t in out.targets:
            assert 0 <= sig * (t - target) < inc and (t / inc) == (t / inc).to_integral_value()
            current = next(x.level for x in handles if x.handle_id == h.id)
            assert sig * (t - current) > 0                                    # never loosens a handle
    no = apply_envelope(s_ctx("1.20150"), Evaluation(False, reason="BELOW_TRIGGER"), quote=None, quote_live=True,
                        market_open=True, envelope=EnvelopeParams())
    assert no is None


# ---------------------------------------------------------------------------------------------------- RELEASE_POLICY
class _NeverReleases(BreakEvenOnRiskClose):
    """Kind (a)'s rule under another KIND that declares `RELEASE_POLICY = NEVER` (a trailing-style strategy)."""

    KIND: ClassVar[str] = "never_releases"
    RELEASE_POLICY: ClassVar[ReleasePolicy] = ReleasePolicy.NEVER


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_a_never_release_strategy_at_target_with_the_trigger_met_stays_tracking(sign: int, side: str) -> None:
    """Round 12 / S38c: (7a) applies only under `ON_CONFIRMED_TARGET`; a `NEVER` strategy at its target with the
    trigger met on a final bar end keeps the position managed (no RELEASED event, never STRATEGY_COMPLETE)."""
    never = _NeverReleases()
    reg = ArmRegistry(strategies=[never], guards=[FirstBarGuard()])
    p = reg.parse_params(never.KIND, 1, strategy_row(be_offset_r="0"))
    r = post_run(lv(sign, "0.00150"), side=side, stop=str(E), s_at_t=StopAtT(E, StopBasis.STREAM), p=p,
                 registry=reg)
    assert r.trigger_met is True and r.at_target is True and r.final
    assert r.outcome is Outcome.AT_TARGET_UNCONFIRMED and Sm11Event.RELEASED not in r.sm11_events
    assert steps(ArmState.TRACKING, r.sm11_events) == Transition(ArmState.TRACKING)
    again = post_run(lv(sign, "0.00150"), side=side, stop=str(E), s_at_t=StopAtT(E, StopBasis.STREAM), p=p,
                     registry=reg, prev_final_at_target=True)
    assert again.outcome is Outcome.AT_TARGET_UNCONFIRMED and again.shown is False
    # kind (a) declares ON_CONFIRMED_TARGET: the same bar end releases
    assert BREAK_EVEN_ON_RISK_CLOSE_V1.RELEASE_POLICY is ReleasePolicy.ON_CONFIRMED_TARGET
    done = post_run(lv(sign, "0.00150"), side=side, stop=str(E), s_at_t=StopAtT(E, StopBasis.STREAM))
    assert done.outcome is Outcome.COMPLETE and done.sm11_events == (Sm11Event.RELEASED,)


def test_a_strategy_without_a_release_policy_is_refused() -> None:
    class NoPolicy(ToyStrategy):
        KIND: ClassVar[str] = "no_policy"
        RELEASE_POLICY = None  # type: ignore[assignment]

    with pytest.raises(RegistryError, match="RELEASE_POLICY"):
        ArmRegistry(strategies=[NoPolicy()])


# ---------------------------------------------------------------------------------------------------- round 14 (S38g)
@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_golden_a_move_on_final_advice_is_never_discounted_by_at_pre(sign: int, side: str) -> None:
    """Round 14 (plan S38g): bar n's POST gave a final ADJUST to E (ADVICE_OUTSTANDING); bar n+1's PRE proposed the
    same move; the user moved to E after that PRE; bar n+1 closes between E and E + risk. The move answers the final
    advice: tested at `S_at_T` = E (Q8), so the bar releases (COMPLETE), never PRE_NOT_CONFIRMED, no pre_basis is
    stored, and the decision carries `moved_on_final_advice`."""
    pre = pre_run(lv(sign, "0.00150"), side=side, arm=ArmState.ADVICE_OUTSTANDING)
    assert pre.outcome is Outcome.ADJUST and pre.pre_levels is not None
    close = lv(sign, "0.00080")                                  # between E and E + risk (risk 0.00100)
    post = post_run(close, side=side, stop=str(E), arm=ArmState.ADVICE_OUTSTANDING, this_bar_pre=pre.pre_levels,
                    s_at_t=StopAtT(E, StopBasis.STREAM), final_advice_target=E)
    assert post.outcome is Outcome.COMPLETE and post.trigger_met is True and post.final
    assert post.stop_basis is StopBasis.STREAM and post.pre_basis is None
    assert "moved_on_final_advice" in post.caveats
    assert post.sm11_events == (Sm11Event.RELEASED,)
    # without the outstanding final advice the same bar is the PRE_NOT_CONFIRMED path (the heads-up rule)
    plain = post_run(close, side=side, stop=str(E), arm=ArmState.TRACKING, this_bar_pre=pre.pre_levels,
                     s_at_t=StopAtT(E, StopBasis.STREAM), final_advice_target=E)
    assert plain.outcome is Outcome.PRE_NOT_CONFIRMED and plain.pre_basis == pre.pre_levels
    assert "moved_on_final_advice" not in plain.caveats


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_golden_corrected_rerun_never_takes_withdrawn_advice(sign: int, side: str) -> None:
    """Round 14 review: bar N-3 gave a final ADJUST to E, bar N-2 withdrew it, bar N's own POST ADJUST set
    ADVICE_OUTSTANDING (today's row state). A CORRECTED re-run of bar N where the stop was moved to E after bar N's
    PRE heads-up: the re-run takes no advice target (`final_advice_target_of` reads `prior_final_adjust`, None after
    the withdrawal), so the move is a heads-up move and stays under AT_PRE (never `moved_on_final_advice`)."""
    memory = DecisionMemory(final_advice_target=E, prior_final_adjust=None)
    target = final_advice_target_of(ArmEvent.CORRECTED, memory)
    assert target is None and final_advice_target_of(ArmEvent.REOPEN, memory) is None
    assert final_advice_target_of(ArmEvent.POST, memory) == E                       # a POST still reads the newest
    pre = pre_run(lv(sign, "0.00150"), side=side, arm=ArmState.ADVICE_OUTSTANDING)
    close = lv(sign, "0.00080")
    rerun = post_run(close, side=side, stop=str(E), arm=ArmState.ADVICE_OUTSTANDING, this_bar_pre=pre.pre_levels,
                     s_at_t=StopAtT(E, StopBasis.STREAM), final_advice_target=target, event=ArmEvent.CORRECTED)
    assert rerun.stop_basis is StopBasis.AT_PRE and "moved_on_final_advice" not in rerun.caveats
    assert rerun.outcome is not Outcome.COMPLETE
    # with the advice not withdrawn (bar N-1's final ADJUST to E is the newest before N) the move answers it
    kept = final_advice_target_of(ArmEvent.CORRECTED, DecisionMemory(final_advice_target=E, prior_final_adjust=E))
    answered = post_run(close, side=side, stop=str(E), arm=ArmState.ADVICE_OUTSTANDING, this_bar_pre=pre.pre_levels,
                        s_at_t=StopAtT(E, StopBasis.STREAM), final_advice_target=kept, event=ArmEvent.CORRECTED)
    assert "moved_on_final_advice" in answered.caveats and answered.stop_basis is StopBasis.STREAM


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_a_move_past_the_final_advice_target_is_still_discounted(sign: int, side: str) -> None:
    """A move beyond the advised target is not covered by the advice: the PRE levels still apply (AT_PRE)."""
    pre = pre_run(lv(sign, "0.00150"), side=side, arm=ArmState.ADVICE_OUTSTANDING)
    beyond = lv(sign, "0.00020")                                 # past the target E toward profit
    post = post_run(lv(sign, "0.00080"), side=side, stop=beyond, arm=ArmState.ADVICE_OUTSTANDING,
                    this_bar_pre=pre.pre_levels, s_at_t=StopAtT(D(beyond), StopBasis.STREAM),
                    final_advice_target=E)
    assert post.stop_basis is StopBasis.AT_PRE and "moved_on_final_advice" not in post.caveats
    assert post.outcome is Outcome.PRE_NOT_CONFIRMED


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_a_partial_move_toward_the_final_advice_target_counts_at_s_at_t(sign: int, side: str) -> None:
    """A move short of the target (partway) also answers the advice: tested at `S_at_T`, no basis stored."""
    pre = pre_run(lv(sign, "0.00150"), side=side, arm=ArmState.ADVICE_OUTSTANDING)
    partway = lv(sign, "-0.00050")
    post = post_run(lv(sign, "0.00060"), side=side, stop=partway, arm=ArmState.ADVICE_OUTSTANDING,
                    this_bar_pre=pre.pre_levels, s_at_t=StopAtT(D(partway), StopBasis.STREAM),
                    final_advice_target=E)
    assert post.stop_basis is StopBasis.STREAM and post.trigger_met is True and post.pre_basis is None
    assert post.outcome is Outcome.ADJUST and "moved_on_final_advice" in post.caveats
