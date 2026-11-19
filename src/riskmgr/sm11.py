"""SM11, per (position, resource, owner), as one pure transition table (design §B12; plan S19).

No other module sets `arm_positions.state`: the arm-tracker (S26), the BarClock (S27) and the FAILED logic (S30)
call `next_state` and write what it returns. Every §B12 edge is in `EDGES`; every other `(state, event)` pair is
`Refused`. PRE and a stop move seen between bars have no edge, so they never change the state, and only a final
bar-end check (`RELEASED`) reaches `STRATEGY_COMPLETE`.

**FAILED** stores the state it left in `prior_state` (`arm_positions.prior_state`) and `FAILED_CLEARED` restores it.
While FAILED, `POSITION_CLOSED` and `LOCAL_DISARM` still apply (terminal: nothing is left to advise), and `SUSPEND`
still applies by becoming the state FAILED restores to (the position stays FAILED, so it still gets no advice, and is
SUSPENDED once the scope is cleared). A second scope failing over a FAILED position is refused (`already_failed`);
the FAILED logic tracks which scopes cover a position and clears only when none does. `POSITION_CLOSED` and
`LOCAL_DISARM` apply out of FAILED even when the stored `prior_state` is missing or invalid; only `FAILED_CLEARED` and
`SUSPEND` read it.

**One decision may need two events.** Each call is one edge, so a caller applies the events of one decision in
order: the first POST of an ARMED position that ends in an ADJUST is `FIRST_POST` (ARMED → TRACKING) **then**
`FINAL_ADJUST` (TRACKING → ADVICE_OUTSTANDING); `FINAL_ADJUST` alone is refused from ARMED. A first POST that releases
is the single edge ARMED --`RELEASED`--> STRATEGY_COMPLETE. `steps()` applies such a sequence and stops at the first
refusal.

**Note for S26:** SUSPENDED --`LOCAL_REARM`--> ARMED is reachable from a position SUSPENDED straight out of DISCOVERED
(`SUSPEND` applies from any non-terminal state), so the re-arm handler must re-run the arming guard
(`ARMING_PASSED`'s checks: fresh image, entry known, a stop, an accepted assignment) before applying `LOCAL_REARM`.

Deviation from the §B12 signature `next_state(state, event)`: the current `prior_state` is passed as a keyword and
the result is a `Transition(state, prior_state)`, because the FAILED edges read and write it.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Final


class ArmState(str, Enum):
    DISCOVERED = "DISCOVERED"
    ARMED = "ARMED"
    TRACKING = "TRACKING"
    ADVICE_OUTSTANDING = "ADVICE_OUTSTANDING"
    STRATEGY_COMPLETE = "STRATEGY_COMPLETE"
    SUSPENDED = "SUSPENDED"
    FAILED = "FAILED"
    CLOSED = "CLOSED"
    DISARMED = "DISARMED"


class Sm11Event(str, Enum):
    ARMING_PASSED = "ARMING_PASSED"            # fresh image, entry known, a stop, an accepted assignment
    FIRST_POST = "FIRST_POST"                  # the first POST decision, any outcome
    FINAL_ADJUST = "FINAL_ADJUST"              # an ADJUST on the final bar-end close (POST, LATE, REOPEN, CORRECTED)
    ADVICE_WITHDRAWN = "ADVICE_WITHDRAWN"      # a POST without ADJUST, latch off
    HANDLES_REACHED = "HANDLES_REACHED"        # the handles reached the proposal
    RELEASED = "RELEASED"                      # bar-end check on the final close: trigger met, all handles at target
    RELEASE_WITHDRAWN = "RELEASE_WITHDRAWN"    # a correction of the release bar (still the latest) no longer releases
    REARM_ON_LOOSEN = "REARM_ON_LOOSEN"        # a handle below target after release, rearm_on_loosen on (Q10)
    POSITION_CLOSED = "POSITION_CLOSED"
    LOCAL_DISARM = "LOCAL_DISARM"
    SUSPEND = "SUSPEND"                        # 3 strategy errors, a pinned version missing, or a units increase
    LOCAL_REARM = "LOCAL_REARM"
    SCOPE_FAILED = "SCOPE_FAILED"              # a FAILED scope covers it: series, adapter, strategy version or ARM
    FAILED_CLEARED = "FAILED_CLEARED"          # operator clear, cause fixed
    PRE_DECIDED = "PRE_DECIDED"                # no edge: PRE never changes the state
    STOP_MOVED = "STOP_MOVED"                  # no edge: a stop move seen between bars never changes the state


TERMINAL: Final = frozenset({ArmState.CLOSED, ArmState.DISARMED})
NON_TERMINAL: Final = frozenset(ArmState) - TERMINAL
MANAGED: Final = frozenset({ArmState.ARMED, ArmState.TRACKING, ArmState.ADVICE_OUTSTANDING})


def _edges() -> dict[tuple[ArmState, Sm11Event], ArmState]:
    S, E = ArmState, Sm11Event
    edges: dict[tuple[ArmState, Sm11Event], ArmState] = {
        (S.DISCOVERED, E.ARMING_PASSED): S.ARMED,
        (S.ARMED, E.FIRST_POST): S.TRACKING,
        (S.TRACKING, E.FINAL_ADJUST): S.ADVICE_OUTSTANDING,
        (S.ADVICE_OUTSTANDING, E.FINAL_ADJUST): S.ADVICE_OUTSTANDING,
        (S.ADVICE_OUTSTANDING, E.ADVICE_WITHDRAWN): S.TRACKING,
        (S.ADVICE_OUTSTANDING, E.HANDLES_REACHED): S.TRACKING,
        (S.STRATEGY_COMPLETE, E.RELEASE_WITHDRAWN): S.TRACKING,
        (S.STRATEGY_COMPLETE, E.REARM_ON_LOOSEN): S.TRACKING,
        (S.SUSPENDED, E.LOCAL_REARM): S.ARMED,
    }
    for s in MANAGED:
        edges[(s, E.RELEASED)] = S.STRATEGY_COMPLETE
    for s in NON_TERMINAL:
        edges[(s, E.POSITION_CLOSED)] = S.CLOSED
        edges[(s, E.LOCAL_DISARM)] = S.DISARMED
        if s is not S.SUSPENDED:
            edges[(s, E.SUSPEND)] = S.FAILED if s is S.FAILED else S.SUSPENDED
        if s is not S.FAILED:
            edges[(s, E.SCOPE_FAILED)] = S.FAILED
    edges[(S.FAILED, E.FAILED_CLEARED)] = S.FAILED   # placeholder target: the restored prior_state (see next_state)
    return edges


EDGES: Final[Mapping[tuple[ArmState, Sm11Event], ArmState]] = _edges()
"""Every allowed `(state, event)` pair. The `FAILED_CLEARED` target is the stored `prior_state`."""


@dataclass(frozen=True)
class Transition:
    state: ArmState
    prior_state: ArmState | None = None
    """Set only while `state` is FAILED: the state the clear restores."""


@dataclass(frozen=True)
class Refused:
    reason: str


def next_state(state: ArmState, event: Sm11Event, *, prior_state: ArmState | None = None) -> Transition | Refused:
    """The one SM11 step. `prior_state` is the stored value (read only while `state` is FAILED)."""
    target = EDGES.get((state, event))
    if target is None:
        if state in TERMINAL:
            return Refused(f"{state.value} is terminal")
        if state is ArmState.FAILED and event is Sm11Event.SCOPE_FAILED:
            return Refused("already_failed")
        return Refused(f"no edge {state.value} --{event.value}-->")
    if state is ArmState.FAILED:
        if event in (Sm11Event.POSITION_CLOSED, Sm11Event.LOCAL_DISARM):
            return Transition(target)   # terminal: applies whatever prior_state holds (the operator's escape hatch)
        if prior_state is None or prior_state is ArmState.FAILED or prior_state in TERMINAL:
            return Refused("FAILED without a valid prior_state")
        if event is Sm11Event.FAILED_CLEARED:
            return Transition(prior_state)
        if event is Sm11Event.SUSPEND:
            if prior_state is ArmState.SUSPENDED:
                return Refused("already_suspended")
            return Transition(ArmState.FAILED, ArmState.SUSPENDED)
        return Transition(target)
    if target is ArmState.FAILED:
        return Transition(ArmState.FAILED, state)
    return Transition(target)


def steps(state: ArmState, events: Sequence[Sm11Event], *,
          prior_state: ArmState | None = None) -> Transition | Refused:
    """Apply the events of one decision in order (e.g. `FIRST_POST` then `FINAL_ADJUST`); the first `Refused` wins
    and nothing after it applies. With no events, the state is unchanged."""
    out = Transition(state, prior_state if state is ArmState.FAILED else None)
    for event in events:
        step = next_state(out.state, event, prior_state=out.prior_state)
        if isinstance(step, Refused):
            return step
        out = step
    return out
