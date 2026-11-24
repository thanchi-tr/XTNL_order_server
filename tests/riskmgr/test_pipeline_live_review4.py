"""Live review round 4: a `PRE_NOT_CONFIRMED` never stores a basis the bar-end stop has already passed (C1).

Design §B6.3 C1 (`pre_basis` "is also cleared once the current `S` is at or farther from entry than `S_pre` (the stop
was loosened back past it ...)"; the current S of a bar end is `S_at_T`). A stop still at the PRE's level at T keeps
the basis: a `PRE_NOT_CONFIRMED` stores "the handle levels the PRE used, before the move it prompted". Before the fix, the guard-HOLD and the strategy / at-target PRE_NOT_CONFIRMED
paths folded this bar's PRE levels into the stored basis without that check, so a stop loosened past `S_pre` before
the bar end left a basis behind, and a later bar tightened between `S_pre` and entry was tested at `S_pre` (AT_PRE)
instead of at its own stop. Long and short; synthetic levels only.
"""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from src.marketdata.ports import HandleRef
from src.riskmgr.guards.first_bar import FIRST_BAR_VOLATILE
from src.riskmgr.outcomes import Outcome, OutcomeCode
from src.riskmgr.sm11 import ArmState
from src.riskmgr.stopbasis import PreBasis, StopAtT, StopUnavailable
from src.riskmgr.strategies.break_even_on_risk_close import BELOW_TRIGGER
from src.riskmgr.types import ArmEvent, StopBasis
from tests.riskmgr.kind_a import B0, Q
from tests.riskmgr.test_pipeline import E, T, _ctx_at, lv, post_run, pre_run

D = Decimal
SIDES = [(1, "BUY"), (-1, "SELL")]


def _pre(sign: int, side: str) -> PreBasis:
    pre = pre_run(lv(sign, "0.00150"), side=side)
    assert pre.outcome is Outcome.ADJUST and pre.pre_levels is not None
    assert pre.pre_levels.s_pre == D(lv(sign, "-0.00100"))
    return pre.pre_levels


@pytest.mark.parametrize("sign,side", SIDES)
def test_a_stop_loosened_past_s_pre_before_t_stores_no_basis_and_the_next_bar_tests_its_own_stop(
        sign: int, side: str) -> None:
    levels = _pre(sign, side)
    looser = lv(sign, "-0.00200")                      # loosened past S_pre before the bar end
    post = post_run(lv(sign, "0.00150"), side=side, stop=looser, this_bar_pre=levels,
                    s_at_t=StopAtT(D(looser), StopBasis.STREAM))
    assert post.code == OutcomeCode.pre_not_confirmed("break_even_on_risk_close", BELOW_TRIGGER)
    assert post.final and post.shown and post.stop_basis is StopBasis.STREAM
    assert post.pre_basis is None                       # C1: S_at_T is beyond S_pre, so no basis is kept
    # next bar: tightened between S_pre and entry; the close beats the risk at that stop, not at S_pre
    partway = lv(sign, "-0.00050")
    nxt = post_run(lv(sign, "0.00080"), side=side, stop=partway, pre_basis=post.pre_basis,
                   s_at_t=StopAtT(D(partway), StopBasis.STREAM))
    assert nxt.stop_basis is StopBasis.STREAM and nxt.trigger_met is True
    assert nxt.outcome is Outcome.ADJUST and dict(nxt.targets) == {HandleRef("CHILD_ORDER", "O1"): E}


@pytest.mark.parametrize("sign,side", SIDES)
def test_a_stop_not_yet_moved_at_t_still_stores_the_basis_so_a_later_move_is_discounted(sign: int, side: str) -> None:
    """S_at_T exactly at S_pre: the heads-up was not acted on by T. §B6.3 stores "the handle levels the PRE used,
    before the move it prompted" (pinned by `test_evaluation`); a move made after T is still tested at S_pre."""
    levels = _pre(sign, side)
    at = lv(sign, "-0.00100")
    post = post_run(lv(sign, "0.00050"), side=side, stop=at, this_bar_pre=levels,
                    s_at_t=StopAtT(D(at), StopBasis.STREAM))
    assert post.outcome is Outcome.PRE_NOT_CONFIRMED and post.pre_basis == levels
    partway = lv(sign, "-0.00050")
    nxt = post_run(lv(sign, "0.00080"), side=side, stop=partway, pre_basis=post.pre_basis,
                   s_at_t=StopAtT(D(partway), StopBasis.STREAM))
    assert nxt.stop_basis is StopBasis.AT_PRE and nxt.trigger_met is False


@pytest.mark.parametrize("sign,side", SIDES)
def test_s_at_t_nearer_entry_than_s_pre_still_stores_the_basis(sign: int, side: str) -> None:
    levels = _pre(sign, side)
    nearer = lv(sign, "-0.00050")
    post = post_run(lv(sign, "0.00050"), side=side, stop=nearer, this_bar_pre=levels,
                    s_at_t=StopAtT(D(nearer), StopBasis.STREAM))
    assert post.outcome is Outcome.PRE_NOT_CONFIRMED and post.stop_basis is StopBasis.AT_PRE
    assert post.pre_basis == levels


@pytest.mark.parametrize("sign,side", SIDES)
def test_at_target_pre_not_confirmed_after_a_loosen_past_s_pre_stores_no_basis(sign: int, side: str) -> None:
    """The stop at T was loosened past S_pre and moved to break-even only after T: the trigger is tested at S_at_T
    (not met), the handles are at the target: PRE_NOT_CONFIRMED(AT_TARGET_UNCONFIRMED) with no basis."""
    levels = _pre(sign, side)
    looser = lv(sign, "-0.00200")
    post = post_run(lv(sign, "0.00150"), side=side, stop=str(E), this_bar_pre=levels,
                    s_at_t=StopAtT(D(looser), StopBasis.STREAM))
    assert post.code == OutcomeCode.pre_not_confirmed("", Outcome.AT_TARGET_UNCONFIRMED.value)
    assert post.pre_basis is None


def _bar0_pre(sign: int, side: str) -> PreBasis:
    close = lv(sign, "0.00150")
    ctx = replace(_ctx_at(side, close, B0), event=ArmEvent.PRE, bar=None)
    pre = pre_run(close, ctx=ctx)
    assert pre.outcome is Outcome.ADJUST and pre.pre_levels is not None
    return pre.pre_levels


def _bar0_post(sign: int, side: str, s_at_t: StopAtT | StopUnavailable, levels: PreBasis | None,
               pre_basis: PreBasis | None = None):  # type: ignore[no-untyped-def]
    close = lv(sign, "0.00150")
    return post_run("0", ctx=_ctx_at(side, close, B0), live=close, this_bar_pre=levels, s_at_t=s_at_t,
                    arm=ArmState.ARMED, pre_basis=pre_basis)


@pytest.mark.parametrize("sign,side", SIDES)
def test_guard_hold_pre_not_confirmed_after_a_loosen_past_s_pre_stores_no_basis(sign: int, side: str) -> None:
    levels = _bar0_pre(sign, side)
    looser = lv(sign, "-0.00200")
    post = _bar0_post(sign, side, StopAtT(D(looser), StopBasis.STREAM), levels)
    assert post.code == OutcomeCode.pre_not_confirmed("first_bar", FIRST_BAR_VOLATILE) and post.final
    assert post.pre_basis is None
    # the next bar, tightened between S_pre and entry, is tested at its own stop (STREAM)
    partway = lv(sign, "-0.00050")
    close1 = lv(sign, "0.00080")
    nxt = post_run("0", ctx=_ctx_at(side, close1, B0 + Q), live=close1, s_at_t=StopAtT(D(partway), StopBasis.STREAM),
                   pre_basis=post.pre_basis)
    assert nxt.stop_basis is StopBasis.STREAM and nxt.trigger_met is True


@pytest.mark.parametrize("sign,side", SIDES)
def test_guard_hold_keeps_the_basis_while_s_at_t_is_nearer_entry_or_unreadable(sign: int, side: str) -> None:
    levels = _bar0_pre(sign, side)
    nearer = _bar0_post(sign, side, StopAtT(D(lv(sign, "-0.00050")), StopBasis.STREAM), levels)
    assert nearer.code == OutcomeCode.pre_not_confirmed("first_bar", FIRST_BAR_VOLATILE)
    assert nearer.pre_basis == levels
    unreadable = _bar0_post(sign, side, StopUnavailable(), levels)
    assert unreadable.code == OutcomeCode.pre_not_confirmed("first_bar", FIRST_BAR_VOLATILE)
    assert unreadable.pre_basis == levels               # no current S known: C1 cannot be checked, kept


@pytest.mark.parametrize("sign,side", SIDES)
def test_guard_hold_clears_an_older_basis_the_bar_end_stop_has_passed(sign: int, side: str) -> None:
    old = PreBasis.of(side, E, [(HandleRef("CHILD_ORDER", "O1"), D(lv(sign, "-0.00100")))], T - Q)  # type: ignore[arg-type]
    looser = lv(sign, "-0.00200")
    post = _bar0_post(sign, side, StopAtT(D(looser), StopBasis.STREAM), None, pre_basis=old)
    assert post.outcome is Outcome.GUARD_REASON and post.pre_basis is None
    kept = _bar0_post(sign, side, StopAtT(D(lv(sign, "-0.00050")), StopBasis.STREAM), None, pre_basis=old)
    assert kept.outcome is Outcome.GUARD_REASON and kept.pre_basis == old
