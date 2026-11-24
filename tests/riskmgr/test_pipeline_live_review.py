"""Live review of the SL edge rule through the pipeline (design §B6.3, §B6.4, §20 Q1, Q2, Q5; findings 1, 3, 7-11, 14).

* Finding 1: a REOPEN after an E re-pin (the tracker drops the recorded target, `state_after_entry_repin`) never
  gives a target on the loss side of E and never `COMPLETE` with a handle there; a final-advice target on the loss
  side of the current E never counts as answered (`answers_final_advice`).
* Finding 3: a stop loosened after `T` and then seen through an untimed row does not withhold a bar that met the
  trigger at the real `S_at_T`.
* Findings 7-11, 14: rounding toward profit on coarse and non-decimal grids, the step (12) gap from the rounded target
  and from the exit side of a two-sided quote, every handle short of break-even moved, the Q2 one-bar delay end to end,
  and `BROKER_AHEAD` naming exactly the handles a target would loosen.

Synthetic levels only.
"""
from __future__ import annotations

import random
from dataclasses import replace
from decimal import Decimal

import pytest

from src.marketdata.ports import HandleRef
from src.marketdata.types import Quote
from src.riskmgr.envelope import EnvelopeParams
from src.riskmgr.guards.first_bar import FIRST_BAR_VOLATILE
from src.riskmgr.outcomes import Outcome
from src.riskmgr.pipeline import PipelineInput, answers_final_advice, apply_envelope, round_toward_profit, run_pipeline
from src.riskmgr.registry import REGISTRY
from src.riskmgr.sm11 import ArmState
from src.riskmgr.stopbasis import PreBasis, StopAtT, StopVersion, stop_at_bar_end
from src.riskmgr.strategies.break_even_on_risk_close import BREAK_EVEN_ON_RISK_CLOSE_V1
from src.riskmgr.strategy import Evaluation, Proposal, StrategyState
from src.riskmgr.tracker import state_after_entry_repin
from src.riskmgr.types import ArmEvent, BarContext, HandleView, StopBasis
from tests.riskmgr.kind_a import ADAPTER, B0, FIVE, INC, Q, bar, s_ctx
from tests.riskmgr.test_pipeline import E, FRESH, OPEN, T, _bar0_ctx, lv, params, post_run, quote

D = Decimal
O1, O2 = HandleRef("CHILD_ORDER", "O1"), HandleRef("CHILD_ORDER", "O2")
S = BREAK_EVEN_ON_RISK_CLOSE_V1
SCHEMA = S.STATE_SCHEMA


def _repinned(state: StrategyState) -> StrategyState:
    out = StrategyState.decode(state_after_entry_repin(state.encode(), SCHEMA, repinned=True))
    assert out is not None
    return out


# ---------------------------------------------------------------------------------------------------- finding 1
@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_reopen_after_an_e_re_pin_advises_the_new_break_even(sign: int, side: str) -> None:
    """The repro: a target recorded at E = 1.20000 (offset 0), E then re-pinned 0.00010 further from the stop. With
    the target dropped at the re-pin, REOPEN advises the new E: a handle at S0 and a handle at the old target both
    get ADJUST to the new E, and neither is ever COMPLETE (break-even is not confirmed on the loss side)."""
    old_e, new_e = E, E + sign * D("0.00010")
    stored = StrategyState.of(latched=True, target=old_e)
    state = _repinned(stored)
    assert state.flag("latched") and state.text("target") is None
    s0 = str(new_e - sign * D("0.00100"))
    close = str(new_e + sign * D("0.00390"))
    for handle in (s0, str(old_e)):
        ctx = s_ctx(close, side=side, event=ArmEvent.REOPEN, entry=str(new_e), s0=s0, stop=handle, handles=[handle])
        r = post_run(close, ctx=ctx, state=state, s_at_t=StopAtT(D(handle), StopBasis.STREAM),
                     p=params(be_offset_r="0"))
        assert r.outcome is Outcome.ADJUST and dict(r.targets) == {O1: new_e} and r.at_target is False
        # the stored (stale) state is what the tracker no longer leaves behind: it would advise the loss side
        stale = S.propose(params(be_offset_r="0"), ctx, stored, ArmEvent.REOPEN)
        assert stale.target is not None and sign * (stale.target - new_e) < 0


def test_reopen_after_any_re_pin_never_targets_or_releases_on_the_loss_side() -> None:
    """Property: a trigger recorded under (E, S0, offset) and then any re-pin of E and S0 through the tracker's
    rule; a REOPEN never proposes or records a target on the loss side of the new E, `at_target` never holds with a
    handle on the loss side, and the pipeline never gives COMPLETE with one."""
    rng = random.Random(20261125)
    inc = INC[FIVE]
    pipeline_runs = 0
    for n in range(3_000):
        side = rng.choice(["BUY", "SELL"])
        sig = 1 if side == "BUY" else -1
        offset = rng.choice(["0", "0.1", "0.5"])
        p = params(be_offset_r=offset)
        old_e = E
        old_s0 = old_e - sig * D(rng.randint(-50, 300)) * inc
        old_ctx = s_ctx(str(old_e + sig * D("0.00500")), side=side, s0=str(old_s0), stop=str(old_s0))
        recorded = S.commit_state(p, FRESH, S.propose(p, old_ctx, FRESH, ArmEvent.POST), Outcome.ADJUST)
        new_e = old_e + sig * D(rng.randint(-200, 200)) * inc
        new_s0 = new_e - sig * D(rng.randint(-50, 300)) * inc if rng.random() < 0.7 else old_s0
        state = StrategyState.decode(state_after_entry_repin(
            recorded.encode(), SCHEMA, repinned=new_e != old_e or new_s0 != old_s0))
        assert state is not None
        handles = [str(new_e + sig * D(rng.randint(-300, 60)) * inc) for _ in range(rng.randint(1, 3))]
        close = str(new_e + sig * D(rng.randint(-100, 600)) * inc)
        stop = min(handles, key=lambda h: sig * D(h))           # the farthest handle from entry is S
        ctx = s_ctx(close, side=side, event=ArmEvent.REOPEN, entry=str(new_e), s0=str(new_s0), stop=stop,
                    handles=handles)
        ev = S.propose(p, ctx, state, ArmEvent.REOPEN)
        for t in ([ev.target] if ev.target is not None else []) + [t for _, t in
                                                                    (ev.proposal.targets if ev.proposal else ())]:
            assert sig * (t - new_e) >= 0, n
        if S.at_target(p, ctx, state):
            assert all(sig * (D(h) - new_e) >= 0 for h in handles), n
        if n % 10 == 0:
            pipeline_runs += 1
            r = post_run(close, ctx=ctx, state=state, s_at_t=StopAtT(D(stop), StopBasis.STREAM), p=p)
            if r.outcome is Outcome.COMPLETE:
                assert all(sig * (D(h) - new_e) >= 0 for h in handles), n
            for _, t in r.targets:
                assert sig * (t - new_e) >= 0, n
    assert pipeline_runs == 300


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_a_final_advice_target_on_the_loss_side_of_e_is_never_answered(sign: int, side: str) -> None:
    """A final ADJUST target advised under an earlier E (now on the loss side of the re-pinned E) never counts as
    answered, so the bar keeps its PRE levels; a target at the current E still does."""
    new_e = E + sign * D("0.00010")
    s_pre = new_e - sign * D("0.00100")
    pre = PreBasis.of(side, new_e, [(O1, s_pre)], T)  # type: ignore[arg-type]
    ctx = s_ctx(str(new_e + sign * D("0.00200")), side=side, entry=str(new_e), s0=str(s_pre), stop=str(E))

    def inp(target: Decimal, at_t: Decimal) -> PipelineInput:
        return PipelineInput(params=params(), ctx=ctx, state=FRESH, arm_state=ArmState.ADVICE_OUTSTANDING,
                             now_ms=T + 60_000, calendar=OPEN, stop_at_t=StopAtT(at_t, StopBasis.STREAM),
                             this_bar_pre=pre, final_advice_target=target)

    assert answers_final_advice(inp(new_e, new_e))
    assert not answers_final_advice(inp(E, E))          # the stale target, on the loss side of the new E


# ---------------------------------------------------------------------------------------------------- finding 3
@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_a_stop_loosened_after_t_and_seen_untimed_does_not_withhold_the_trigger(sign: int, side: str) -> None:
    """Q8 tests the stop at T: v1 (T-5min) is the stop at T; v2 (T+60s) loosened it; v3, an untimed row read after
    v2, carries a later level. The close is between the trigger at v1 (risk 0.00050) and at the loosened level, so the
    bar met the trigger: ADJUST, not BELOW_TRIGGER."""
    def ver(level: str, at: int | None, seq: int, origin: str = "stream") -> StopVersion:
        return StopVersion(handle=O1, level=D(level), units=D("1000"), order_kind="STOP",
                           origin=origin, broker_time_source="BROKER" if at else None,  # type: ignore[arg-type]
                           broker_time_ms=at, seq=seq)

    at_level, loose, later = lv(sign, "-0.00050"), lv(sign, "-0.00200"), lv(sign, "-0.00250")
    versions = [ver(at_level, T - 300_000, 1), ver(loose, T + 60_000, 2), ver(later, None, 3, "snapshot")]
    basis = stop_at_bar_end(versions, side=side, entry=E, units_to_cover=D("1000"), bar_end_ms=T)  # type: ignore[arg-type]
    assert isinstance(basis, StopAtT) and basis.level == D(at_level)
    close = lv(sign, "0.00060")
    r = post_run(close, s_at_t=basis, ctx=s_ctx(close, side=side, stop=later, s0=lv(sign, "-0.00100")))
    assert r.outcome is Outcome.ADJUST and r.trigger_met is True and dict(r.targets) == {O1: E}


# ---------------------------------------------------------------------------------------------------- finding 7
@pytest.mark.parametrize("inc,level,up,down", [
    ("0.00001", "1.200001", "1.20001", "1.20000"), ("0.001", "150.0004", "150.001", "150.000"),
    ("0.01", "4500.123", "4500.13", "4500.12"), ("0.25", "4500.10", "4500.25", "4500.00"),
    ("0.5", "38000.3", "38000.5", "38000.0"), ("5", "38003", "38005", "38000"),
    ("0.00005", "1.200012", "1.20005", "1.20000")])
def test_round_toward_profit_on_several_grids(inc: str, level: str, up: str, down: str) -> None:
    i = D(inc)
    for sig, want in ((1, D(up)), (-1, D(down))):
        got = round_toward_profit(D(level), i, sig)
        assert got == want and got / i == (got / i).to_integral_value() and 0 <= sig * (got - D(level)) < i
        assert round_toward_profit(got, i, sig) == got


# ---------------------------------------------------------------------------------------------------- finding 8
@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_gap_is_measured_from_the_rounded_target_on_a_coarse_grid(sign: int, side: str) -> None:
    p = params(be_offset_r="0.123")
    close = lv(sign, "0.00500")
    ctx = replace(s_ctx(close, side=side), price_increment=D("0.0005"))
    ok = post_run(close, side=side, p=p, ctx=ctx, live=lv(sign, "0.00100"))
    assert ok.outcome is Outcome.ADJUST and dict(ok.targets) == {O1: D(lv(sign, "0.0005"))}
    assert post_run(close, side=side, p=p, ctx=ctx, live=lv(sign, "0.00070")).outcome is Outcome.PRICE_THROUGH_LEVEL


# ---------------------------------------------------------------------------------------------------- finding 9
@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_gap_reads_the_exit_side_of_a_two_sided_quote(sign: int, side: str) -> None:
    """Q5: the step (12) gap is measured from the exit side (bid for a long, ask for a short)."""
    c = replace(s_ctx(lv(sign, "0.00150"), side=side), exit_quote_live=True)
    stop = c.stop
    assert stop is not None

    def run(exit_px: str, other_px: str) -> Outcome | None:
        bid, ask = (exit_px, other_px) if sign > 0 else (other_px, exit_px)
        q = Quote(adapter_id=ADAPTER, symbol=FIVE, bid=D(bid), ask=D(ask), broker_ms=T + 59_000, received_ms=T + 59_500)
        inp = PipelineInput(params=params(), ctx=c, state=FRESH, arm_state=ArmState.TRACKING, now_ms=T + 60_000,
                            calendar=OPEN, stop_at_t=StopAtT(stop, StopBasis.STREAM), live_quote=q)
        return run_pipeline(inp).outcome

    assert run(str(E), lv(sign, "0.00015")) is Outcome.PRICE_THROUGH_LEVEL   # exit side at the target
    assert run(lv(sign, "0.00010"), lv(sign, "0.00015")) is Outcome.ADJUST


# ---------------------------------------------------------------------------------------------------- finding 10
@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_every_handle_short_of_break_even_is_moved(sign: int, side: str) -> None:
    hs = [lv(sign, "-0.00100"), lv(sign, "-0.00050"), lv(sign, "0.00010")]
    ctx = s_ctx(lv(sign, "0.00200"), side=side, handles=hs, stop=hs[0])
    assert S.propose(params(), ctx, FRESH, ArmEvent.POST).proposal == Proposal(((O1, E), (O2, E)))
    r = post_run(lv(sign, "0.00200"), side=side, handles=hs, stop=hs[0])
    assert r.outcome is Outcome.ADJUST and dict(r.targets) == {O1: E, O2: E} and r.at_target is False


# ---------------------------------------------------------------------------------------------------- finding 11
@pytest.mark.parametrize("latch", ["off", "on"])
def test_volatile_bar0_delays_the_trail_by_exactly_one_bar(latch: str) -> None:
    p = params(latch=latch)
    s_at_t = StopAtT(D("1.19900"), StopBasis.STREAM)
    held = post_run("0", ctx=_bar0_ctx(ArmEvent.POST), live="1.20200", p=p, s_at_t=s_at_t)   # trigger met on bar 0
    assert held.outcome is Outcome.GUARD_REASON and held.code is not None and held.code.reason == FIRST_BAR_VOLATILE
    assert held.state == FRESH and held.targets == ()

    def bar1(close: str) -> BarContext:
        c = D(close)
        return replace(_bar0_ctx(ArmEvent.POST), bar_end_ms=B0 + 2 * Q, bar_index=1, bar=bar(FIVE, B0 + Q, c, c, c, c))

    nxt = post_run("0", ctx=bar1("1.20200"), live="1.20200", p=p, state=held.state, s_at_t=s_at_t)
    assert nxt.outcome is Outcome.ADJUST and dict(nxt.targets) == {O1: E}
    quiet = post_run("0", ctx=bar1("1.20050"), live="1.20050", p=p, state=held.state, s_at_t=s_at_t)
    assert quiet.outcome is Outcome.STRATEGY_REASON and quiet.targets == ()


# ---------------------------------------------------------------------------------------------------- finding 14
def test_broker_ahead_names_exactly_the_handles_the_target_would_loosen() -> None:
    rng = random.Random(8)
    inc = INC[FIVE]
    seen = 0
    for _ in range(600):
        side = rng.choice(["BUY", "SELL"])
        sig = 1 if side == "BUY" else -1
        levels = [E + sig * D(rng.randint(-300, 100)) * inc for _ in range(rng.randint(1, 3))]
        handles = tuple(HandleView(kind="CHILD_ORDER", handle_id=f"O{k}", level=x) for k, x in enumerate(levels))
        ctx = replace(s_ctx("1.20150", side=side), handles=handles, min_stop_distance=D(rng.randint(0, 30)) * inc)
        target = E + sig * D(rng.randint(-50, 50)) * inc / D(rng.choice([1, 3, 7]))
        named = [HandleRef(h.kind, h.handle_id) for h in handles if rng.random() < 0.8]
        out = apply_envelope(ctx, Evaluation(True, Proposal(tuple((h, target) for h in named))),
                             quote=quote(str(E + sig * D(rng.randint(-100, 400)) * inc), T),
                             quote_live=rng.random() < 0.9, market_open=rng.random() < 0.9,
                             envelope=EnvelopeParams(min_gap_r=D(rng.randint(0, 5)) / 10))
        cur = {HandleRef(x.kind, x.handle_id): x.level for x in handles}
        ahead = {h for h in named if sig * (target - cur[h]) < 0}
        if out is None:
            assert not ahead and not any(sig * (target - cur[h]) > 0 for h in named)
            continue
        assert set(out.broker_ahead) == ahead
        seen += bool(ahead)
    assert seen > 50


def test_registry_pins_the_version_these_tests_cover() -> None:
    assert REGISTRY.strategy(S.KIND, S.IMPL_VERSION) is S


def test_the_conformance_suite_flags_a_reopen_on_a_stale_target() -> None:
    """The property in the conformance suite (`break_even_target`) catches the pre-fix state: a REOPEN reusing a
    target recorded under the old E gives a loss-side target and an at-target release with a loss-side handle."""
    from tests.riskmgr import conformance as C
    case = C.STRATEGY_CASES[(S.KIND, S.IMPL_VERSION)]
    assert C.check_strategy(REGISTRY, S, case) == []
    stale = tuple(replace(ev, state=StrategyState.of(latched=True, target=E))
                  if ev.event is ArmEvent.REOPEN and ev.ctx.entry != E else ev for ev in case.evaluations)
    problems = C.check_strategy(REGISTRY, S, replace(case, evaluations=stale))
    assert any("target on the loss side" in p for p in problems)
    assert any("at_target with a handle on the loss side" in p for p in problems)
