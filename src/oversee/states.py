"""One rule state's level-triggered step (oversee-rules.md §2 Rule states). Pure: no I/O, no clock.

Every deadline is a timestamp stored on the row (pending_since, release_pending_since,
unknown_since, below_soft_since), compared against the pass's `now_ms`; there are no timers.
"""
from __future__ import annotations

from dataclasses import dataclass, replace

from src.oversee.config import SweepConfig
from src.oversee.types import (
    BreachState,
    Cap,
    Level,
    ResolvedBy,
    Result,
    RuleOutcome,
    Subject,
    Transition,
    TransitionKind,
)


@dataclass(frozen=True)
class StepParams:
    now_ms: int
    sweep_id: int
    confirm_ms: int
    cfg: SweepConfig


def _transition(s: BreachState, kind: TransitionKind, p: StepParams) -> Transition:
    return Transition(s.rule_id, s.subject_type, s.subject_id, s.account_id, kind, p.sweep_id, p.now_ms,
                      s.level, s.cap, s.reason, s.activation_id)


def _reopen(s: BreachState) -> BreachState:
    """A resolved row seen again starts a new episode; its escalation clock and cool-down marks carry over."""
    return replace(s, level=Level.PASS, cap=Cap.NONE, reason="", pending_level=None, pending_since=None,
                   active_since=None, activation_id=None, release_pending_since=None, unknown_since=None,
                   unknown_origin=False, resolved_at=None, resolved_by=None)


def _set_level(s: BreachState, new: Level, cap: Cap, reason: str, p: StepParams,
               by: ResolvedBy = "sweep") -> tuple[BreachState, list[Transition]]:
    """Move an active (or inactive) state to `new`, emitting activated / raised / lowered / resolved."""
    cur = s.level if s.active else Level.PASS
    if new == cur:
        return replace(s, cap=cap, reason=reason or s.reason), []
    base = replace(s, pending_level=None, pending_since=None, release_pending_since=None)
    if new == Level.PASS:
        out = replace(base, level=Level.PASS, resolved_at=p.now_ms, resolved_by=by, unknown_origin=False)
        return out, [replace(_transition(s, "resolved", p), level=Level.PASS)]
    if cur == Level.PASS:
        out = replace(base, level=new, cap=cap, reason=reason, active_since=p.now_ms, activation_id=p.sweep_id,
                      resolved_at=None, resolved_by=None)
        return out, [_transition(out, "activated", p)]
    out = replace(base, level=new, cap=cap, reason=reason)
    return out, [_transition(out, "raised" if new > cur else "lowered", p)]


def _below_soft(prev_level: Level, s: BreachState, now: int) -> BreachState:
    if s.active and s.level >= Level.SOFT_HALT:
        return replace(s, below_soft_since=None)
    if prev_level >= Level.SOFT_HALT:
        return replace(s, below_soft_since=now)
    return s


def _step_unknown(s: BreachState, reason: str, p: StepParams) -> tuple[BreachState, list[Transition]]:
    """UNKNOWN changes nothing for unknown_grace_ms; then a capped SOFT state (§2)."""
    since = s.unknown_since if s.unknown_since is not None else p.now_ms
    s = replace(s, unknown_since=since, pending_level=None, pending_since=None, release_pending_since=None)
    if p.now_ms - since < p.cfg.unknown_grace_ms:
        return s, []
    cur = s.level if s.active else Level.PASS
    if cur < Level.SOFT_HALT:
        out, trs = _set_level(s, Level.SOFT_HALT, Cap.SOFT, reason, p)
        return replace(out, unknown_origin=True), trs
    if s.cap is not Cap.SOFT:
        out = replace(s, cap=Cap.SOFT)
        return out, [_transition(out, "capped", p)]
    return s, []


def _step_known(s: BreachState, r: Result, p: StepParams) -> tuple[BreachState, list[Transition]]:
    s = replace(s, unknown_since=None)
    cur = s.level if s.active else Level.PASS
    level, release = r.effective_level, r.effective_release_level
    trs: list[Transition] = []
    if s.unknown_origin and s.active:
        if level < Level.SOFT_HALT:  # resolves once a known result is below SOFT
            out, trs = _set_level(s, level, r.cap, r.reason, p)
            return replace(out, unknown_origin=False), trs
        s = replace(s, unknown_origin=False)
    if level > cur:
        since = s.pending_since if s.pending_since is not None else p.now_ms
        pending = level if s.pending_level is None else min(s.pending_level, level)
        s = replace(s, pending_since=since, pending_level=pending, release_pending_since=None)
        if p.now_ms - since >= p.confirm_ms:
            return _set_level(s, pending, r.cap, r.reason, p)
        return s, []
    s = replace(s, pending_since=None, pending_level=None)
    if cur == Level.PASS:
        return s, []
    hard_capped = cur == Level.HARD_HALT and r.cap is Cap.SOFT   # capped never lowers or resolves HARD
    if release < cur and not hard_capped:
        rps = s.release_pending_since if s.release_pending_since is not None else p.now_ms
        if p.now_ms - rps >= p.cfg.release_ms:
            return _set_level(s, release, r.cap, r.reason, p)
        s = replace(s, release_pending_since=rps)
    else:
        s = replace(s, release_pending_since=None)
    if level == cur:   # before a capped / uncapped transition, so its alert names the current reason
        s = replace(s, reason=r.reason)
    if r.cap != s.cap and cur >= Level.SOFT_HALT:
        s = replace(s, cap=r.cap)
        trs.append(_transition(s, "capped" if r.cap is Cap.SOFT else "uncapped", p))
    elif r.cap != s.cap:
        s = replace(s, cap=r.cap)
    return s, trs


def step_state(prev: BreachState | None, rule_id: str, subject: Subject, outcome: RuleOutcome,
               p: StepParams) -> tuple[BreachState | None, list[Transition]]:
    """Advance one (rule, subject) state by one pass. Returns None when there is nothing to store."""
    s = prev if prev is not None else BreachState.blank(rule_id, subject)
    resolved = s.resolved_at is not None
    worth_reopening = isinstance(outcome, Result) and outcome.effective_level > Level.PASS
    if resolved and not (worth_reopening or not isinstance(outcome, Result)):
        reset = _cool_down_reset(s, p)
        return (reset if reset == s else replace(reset, last_sweep_id=p.sweep_id)), []
    if resolved:
        s = _reopen(s)
    prev_level = s.level if s.active else Level.PASS
    dt = 0 if s.last_pass_ms is None else max(0, min(p.now_ms - s.last_pass_ms, 2 * p.cfg.interval_ms))
    if isinstance(outcome, Result):
        s, trs = _step_known(s, outcome, p)
        if (s.active and s.level >= Level.SOFT_HALT and outcome.cap is Cap.NONE and s.cap is Cap.NONE
                and prev_level >= Level.SOFT_HALT):
            s = replace(s, uncapped_ms=s.uncapped_ms + dt)
    else:
        s, trs = _step_unknown(s, outcome.reason, p)
        if outcome.detail:   # alert text only (oversee-rules.md §6: stop_kind_inconsistent names its child ids)
            trs = [replace(t, detail=outcome.detail) if t.reason == outcome.reason else t for t in trs]
    s = _below_soft(prev_level, s, p.now_ms)
    s = _cool_down_reset(s, p)
    inert = not s.active and s.pending_since is None and s.unknown_since is None
    if inert and prev is None:
        return None, trs
    if inert and prev is not None and s.resolved_at is None:
        # A pending / UNKNOWN episode that ended without activating: store it resolved once, with no transition
        # (it was never alerted), so the row is not reloaded as unresolved on every pass. A later >PASS or
        # UNKNOWN result reopens it through _reopen.
        return replace(s, resolved_at=p.now_ms, resolved_by="sweep", last_pass_ms=p.now_ms,
                       last_sweep_id=p.sweep_id), trs
    if inert and prev is not None:   # nothing tracked and nothing changed: no write is needed
        unchanged = replace(s, last_pass_ms=prev.last_pass_ms, last_sweep_id=prev.last_sweep_id)
        if unchanged == prev:
            return prev, trs
    return replace(s, last_pass_ms=p.now_ms, last_sweep_id=p.sweep_id), trs


def _cool_down_reset(s: BreachState, p: StepParams) -> BreachState:
    """The escalation clock resets once the state has stayed below SOFT for cool_down_ms (§2)."""
    if (s.below_soft_since is not None and s.uncapped_ms
            and p.now_ms - s.below_soft_since >= p.cfg.cool_down_ms):
        return replace(s, uncapped_ms=0)
    return s


def resolve_state(prev: BreachState, by: ResolvedBy, p: StepParams) -> tuple[BreachState, list[Transition]]:
    """Resolve at once: entity terminal / CLOSED / gone, day end, ruleset reload or operator (§2)."""
    if not prev.active:
        cleared = _cool_down_reset(replace(prev, pending_level=None, pending_since=None, unknown_since=None,
                                           release_pending_since=None), p)
        if prev.resolved_at is None:   # never activated (pending / UNKNOWN only): resolve once, no transition
            return replace(cleared, resolved_at=p.now_ms, resolved_by=by, unknown_origin=False,
                           last_pass_ms=p.now_ms, last_sweep_id=p.sweep_id), []
        return cleared, []
    prev_level = prev.level
    out, trs = _set_level(prev, Level.PASS, prev.cap, prev.reason, p, by)
    out = _below_soft(prev_level, out, p.now_ms)
    return replace(out, last_pass_ms=p.now_ms, last_sweep_id=p.sweep_id), trs
