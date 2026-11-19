"""SM11 as one pure transition table (plan S19; design §B12): every §B12 edge allowed, every other `(state, event)`
pair refused, PRE and between-bar stop moves never change the state, only a bar-end release reaches
STRATEGY_COMPLETE, and FAILED restores `prior_state` on clear while CLOSED / DISARMED / SUSPENDED still apply."""
from __future__ import annotations

import itertools

import pytest

from src.riskmgr.sm11 import EDGES, ArmState, Refused, Sm11Event, Transition, next_state, steps

S, E = ArmState, Sm11Event
NON_TERMINAL = [s for s in S if s not in (S.CLOSED, S.DISARMED)]

# The §B12 diagram, written out independently of the module.
EXPECTED: dict[tuple[ArmState, Sm11Event], ArmState] = {
    (S.DISCOVERED, E.ARMING_PASSED): S.ARMED,
    (S.ARMED, E.FIRST_POST): S.TRACKING,
    (S.TRACKING, E.FINAL_ADJUST): S.ADVICE_OUTSTANDING,
    (S.ADVICE_OUTSTANDING, E.FINAL_ADJUST): S.ADVICE_OUTSTANDING,
    (S.ADVICE_OUTSTANDING, E.ADVICE_WITHDRAWN): S.TRACKING,
    (S.ADVICE_OUTSTANDING, E.HANDLES_REACHED): S.TRACKING,
    (S.ARMED, E.RELEASED): S.STRATEGY_COMPLETE,
    (S.TRACKING, E.RELEASED): S.STRATEGY_COMPLETE,
    (S.ADVICE_OUTSTANDING, E.RELEASED): S.STRATEGY_COMPLETE,
    (S.STRATEGY_COMPLETE, E.RELEASE_WITHDRAWN): S.TRACKING,
    (S.STRATEGY_COMPLETE, E.REARM_ON_LOOSEN): S.TRACKING,
    (S.SUSPENDED, E.LOCAL_REARM): S.ARMED,
    (S.FAILED, E.FAILED_CLEARED): S.FAILED,   # target = the stored prior_state
}
for _s in NON_TERMINAL:
    EXPECTED[(_s, E.POSITION_CLOSED)] = S.CLOSED
    EXPECTED[(_s, E.LOCAL_DISARM)] = S.DISARMED
    if _s is not S.SUSPENDED:
        EXPECTED[(_s, E.SUSPEND)] = S.FAILED if _s is S.FAILED else S.SUSPENDED
    if _s is not S.FAILED:
        EXPECTED[(_s, E.SCOPE_FAILED)] = S.FAILED


def test_table_is_exactly_the_design_edges() -> None:
    assert dict(EDGES) == EXPECTED


@pytest.mark.parametrize(("state", "event"), sorted(EXPECTED, key=lambda k: (k[0].value, k[1].value)))
def test_every_edge_is_allowed(state: ArmState, event: Sm11Event) -> None:
    out = next_state(state, event, prior_state=S.TRACKING if state is S.FAILED else None)
    assert isinstance(out, Transition)
    if state is S.FAILED and event is E.FAILED_CLEARED:
        assert out == Transition(S.TRACKING)
    elif state is S.FAILED and event is E.SUSPEND:
        assert out == Transition(S.FAILED, S.SUSPENDED)
    elif EXPECTED[(state, event)] is S.FAILED:
        assert out == Transition(S.FAILED, state)
    else:
        assert out == Transition(EXPECTED[(state, event)])


def test_every_other_pair_is_refused() -> None:
    refused = 0
    for state, event in itertools.product(S, E):
        if (state, event) in EXPECTED:
            continue
        out = next_state(state, event, prior_state=S.TRACKING)
        assert isinstance(out, Refused), (state, event)
        refused += 1
    assert refused == len(S) * len(E) - len(EXPECTED)


@pytest.mark.parametrize("state", list(S))
def test_pre_and_between_bar_stop_moves_never_change_the_state(state: ArmState) -> None:
    for event in (E.PRE_DECIDED, E.STOP_MOVED):
        assert isinstance(next_state(state, event, prior_state=S.TRACKING), Refused)


def test_only_a_bar_end_release_reaches_strategy_complete() -> None:
    into = {ev for (st, ev), target in EDGES.items() if target is S.STRATEGY_COMPLETE}
    assert into == {E.RELEASED}


def test_failed_stores_and_restores_prior_state() -> None:
    for state in (S.DISCOVERED, S.ARMED, S.TRACKING, S.ADVICE_OUTSTANDING, S.STRATEGY_COMPLETE, S.SUSPENDED):
        failed = next_state(state, E.SCOPE_FAILED)
        assert failed == Transition(S.FAILED, state)
        assert isinstance(failed, Transition)
        assert next_state(failed.state, E.FAILED_CLEARED, prior_state=failed.prior_state) == Transition(state)


def test_closed_disarmed_and_suspended_still_apply_while_failed() -> None:
    assert next_state(S.FAILED, E.POSITION_CLOSED, prior_state=S.TRACKING) == Transition(S.CLOSED)
    assert next_state(S.FAILED, E.LOCAL_DISARM, prior_state=S.TRACKING) == Transition(S.DISARMED)
    suspended = next_state(S.FAILED, E.SUSPEND, prior_state=S.ADVICE_OUTSTANDING)
    assert suspended == Transition(S.FAILED, S.SUSPENDED)   # still FAILED: no advice
    assert next_state(S.FAILED, E.FAILED_CLEARED, prior_state=S.SUSPENDED) == Transition(S.SUSPENDED)
    assert next_state(S.FAILED, E.SUSPEND, prior_state=S.SUSPENDED) == Refused("already_suspended")


def test_failed_refusals() -> None:
    assert next_state(S.FAILED, E.SCOPE_FAILED, prior_state=S.TRACKING) == Refused("already_failed")
    for prior in (None, S.FAILED, S.CLOSED, S.DISARMED):
        assert isinstance(next_state(S.FAILED, E.FAILED_CLEARED, prior_state=prior), Refused)
    for event in (E.FINAL_ADJUST, E.RELEASED, E.FIRST_POST, E.LOCAL_REARM):   # no strategy transition while FAILED
        assert isinstance(next_state(S.FAILED, event, prior_state=S.TRACKING), Refused)


@pytest.mark.parametrize("state", [S.CLOSED, S.DISARMED])
def test_terminal_states_refuse_everything(state: ArmState) -> None:
    for event in E:
        out = next_state(state, event)
        assert out == Refused(f"{state.value} is terminal")


def test_a_lifecycle() -> None:
    path = [(E.ARMING_PASSED, S.ARMED), (E.FIRST_POST, S.TRACKING), (E.FINAL_ADJUST, S.ADVICE_OUTSTANDING),
            (E.FINAL_ADJUST, S.ADVICE_OUTSTANDING), (E.HANDLES_REACHED, S.TRACKING), (E.RELEASED, S.STRATEGY_COMPLETE),
            (E.REARM_ON_LOOSEN, S.TRACKING), (E.POSITION_CLOSED, S.CLOSED)]
    state = S.DISCOVERED
    for event, expected in path:
        out = next_state(state, event)
        assert isinstance(out, Transition) and out.state is expected
        state = out.state


@pytest.mark.parametrize("prior", [None, S.FAILED, S.CLOSED])
def test_closed_and_disarm_apply_out_of_failed_without_a_valid_prior_state(prior: ArmState | None) -> None:
    """A NULL or corrupt `prior_state` never leaves a closed position stuck in FAILED (§B12; local disarm is the
    operator's escape hatch). Only FAILED_CLEARED and SUSPEND read it."""
    assert next_state(S.FAILED, E.POSITION_CLOSED, prior_state=prior) == Transition(S.CLOSED)
    assert next_state(S.FAILED, E.LOCAL_DISARM, prior_state=prior) == Transition(S.DISARMED)
    assert next_state(S.FAILED, E.FAILED_CLEARED, prior_state=prior) == Refused("FAILED without a valid prior_state")
    assert next_state(S.FAILED, E.SUSPEND, prior_state=prior) == Refused("FAILED without a valid prior_state")


def test_a_first_post_adjust_is_two_events() -> None:
    """ARMED -FIRST_POST-> TRACKING -FINAL_ADJUST-> ADVICE_OUTSTANDING; FINAL_ADJUST alone is refused from ARMED,
    while a first POST that releases is the single edge ARMED -RELEASED-> STRATEGY_COMPLETE."""
    assert isinstance(next_state(S.ARMED, E.FINAL_ADJUST), Refused)
    assert steps(S.ARMED, (E.FIRST_POST, E.FINAL_ADJUST)) == Transition(S.ADVICE_OUTSTANDING)
    assert steps(S.ARMED, (E.RELEASED,)) == Transition(S.STRATEGY_COMPLETE)
    assert steps(S.ARMED, (E.FINAL_ADJUST, E.FIRST_POST)) == Refused("no edge ARMED --FINAL_ADJUST-->")
    assert steps(S.TRACKING, ()) == Transition(S.TRACKING)
    assert steps(S.TRACKING, (E.SCOPE_FAILED, E.FAILED_CLEARED)) == Transition(S.TRACKING)
    assert steps(S.FAILED, (E.SUSPEND, E.FAILED_CLEARED), prior_state=S.TRACKING) == Transition(S.SUSPENDED)


def test_rearm_is_reachable_from_discovered() -> None:
    """S26 must re-run the arming guard on LOCAL_REARM: SUSPEND applies from DISCOVERED."""
    assert steps(S.DISCOVERED, (E.SUSPEND, E.LOCAL_REARM)) == Transition(S.ARMED)
