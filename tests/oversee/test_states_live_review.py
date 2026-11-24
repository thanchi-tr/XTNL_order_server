"""Live review 2026-11-25, finding 1: inactive rule states whose episode ended are resolved once, with no
transition, so `halt_breaches` rows never leak as 'unresolved' (oversee-rules.md §2 Rule states)."""
from __future__ import annotations

from src.oversee.config import SweepConfig
from src.oversee.halt import HaltSwitch
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.rules import Evaluation
from src.oversee.states import StepParams, resolve_state, step_state
from src.oversee.sweep import apply_pass
from src.oversee.types import BreachState, Level, Result, StateKey, Subject, Unknown

CFG = SweepConfig()
SOFT = Result(Level.SOFT_HALT, "soft")
PASS = Result(Level.PASS, "ok")
ORDER = Subject("order", "stonex:O1", "A1")
DAY = Subject("account_day", "A1:2026-11-19", "A1")
ACCT = Subject("account", "A1", "A1")


def pending_then_pass(rule: str, subject: Subject) -> BreachState:
    """A pending SOFT (confirm 1000) seen once, then the next pass with the subject still evaluated at PASS,
    as the pre-fix sweep stored it: inactive and unresolved."""
    s, trs = step_state(None, rule, subject, SOFT, StepParams(0, 1, 1000, CFG))
    assert s is not None and not s.active and s.pending_since == 0 and trs == []
    return s


def test_pending_soft_then_pass_on_a_still_evaluated_subject_resolves_by_sweep() -> None:
    s = pending_then_pass("max_aggregate_open_risk", ACCT)
    s2, trs = step_state(s, "max_aggregate_open_risk", ACCT, PASS, StepParams(500, 2, 1000, CFG))
    assert trs == []
    assert s2 is not None and not s2.active and s2.resolved_at == 500 and s2.resolved_by == "sweep"
    assert s2.pending_since is None and s2.last_sweep_id == 2
    # a further PASS pass writes nothing
    key: StateKey = ("max_aggregate_open_risk", "account", "A1")
    res = apply_pass({key: s2}, {key: Evaluation(ACCT, PASS, 1000)}, 1500, 3, CFG,
                     frozenset({"max_aggregate_open_risk"}))
    assert res.changed == [] and res.transitions == [] and res.states[key] == s2


def test_resolved_inert_row_reopens_on_a_new_soft_with_fresh_pending_since() -> None:
    s: BreachState | None = pending_then_pass("max_aggregate_open_risk", ACCT)
    s, _ = step_state(s, "max_aggregate_open_risk", ACCT, PASS, StepParams(500, 2, 1000, CFG))
    assert s is not None and s.resolved_at == 500
    s, trs = step_state(s, "max_aggregate_open_risk", ACCT, SOFT, StepParams(5000, 3, 1000, CFG))
    assert s is not None and s.resolved_at is None and s.resolved_by is None
    assert s.pending_since == 5000 and not s.active and trs == []
    s, trs = step_state(s, "max_aggregate_open_risk", ACCT, SOFT, StepParams(6000, 4, 1000, CFG))
    assert s is not None and s.active and [t.transition for t in trs] == ["activated"]


def test_unknown_then_pass_resolves_without_transition() -> None:
    s, _ = step_state(None, "margin_usage", ACCT, Unknown("live_unknown"), StepParams(0, 1, 0, CFG))
    assert s is not None and s.unknown_since == 0
    s, trs = step_state(s, "margin_usage", ACCT, PASS, StepParams(1000, 2, 0, CFG))
    assert trs == [] and s is not None and s.resolved_by == "sweep" and s.unknown_since is None


def test_inactive_state_of_a_gone_subject_is_resolved_once_by_entity_end() -> None:
    """The live leak (breach 5): pending, then PASS, then the order fills and leaves the evaluations."""
    prev = pending_then_pass("max_position_risk", ORDER)
    key = prev.key
    res = apply_pass({key: prev}, {}, 2000, 7, CFG, frozenset({"max_position_risk"}))
    new = res.states[key]
    assert res.transitions == []
    assert new.resolved_at == 2000 and new.resolved_by == "entity_end" and new.last_sweep_id == 7
    assert new.pending_since is None and new.pending_level is None
    assert res.changed == [(new, prev)]
    again = apply_pass({key: new}, {}, 3000, 8, CFG, frozenset({"max_position_risk"}))
    assert again.changed == [] and again.transitions == [] and again.states[key] == new


def test_inactive_states_resolve_by_day_end_and_reload() -> None:
    day = pending_then_pass("max_daily_loss", DAY)
    old = pending_then_pass("old_rule", ACCT)
    res = apply_pass({day.key: day, old.key: old}, {}, 2000, 7, CFG, frozenset({"max_daily_loss"}))
    assert res.transitions == []
    assert res.states[day.key].resolved_by == "day_end" and res.states[old.key].resolved_by == "reload"
    assert len(res.changed) == 2


def test_unknown_state_of_a_gone_subject_is_resolved_with_unknown_since_cleared() -> None:
    s, _ = step_state(None, "max_position_risk", ORDER, Unknown("risk_unknown"), StepParams(0, 1, 0, CFG))
    assert s is not None and s.unknown_since == 0 and not s.active
    out, trs = resolve_state(s, "entity_end", StepParams(1000, 2, 0, CFG))
    assert trs == [] and out.resolved_at == 1000 and out.resolved_by == "entity_end" and out.unknown_since is None


def test_already_resolved_inactive_state_is_not_rewritten() -> None:
    s = pending_then_pass("max_position_risk", ORDER)
    out, _ = resolve_state(s, "entity_end", StepParams(1000, 2, 0, CFG))
    again, trs = resolve_state(out, "entity_end", StepParams(2000, 3, 0, CFG))
    assert again == out and trs == []


async def test_resolve_unlisted_resolves_inactive_rows_without_an_event() -> None:
    db = InMemoryOverseeDb(engine_epoch=7)
    inert = BreachState("max_position_risk", "order", "stonex:G1", "GONE", pending_level=Level.SOFT_HALT,
                        pending_since=1_000, last_pass_ms=1_000, last_sweep_id=10)
    db.t.breaches[inert.key] = inert
    switch = HaltSwitch(db, db)
    resolved = await switch.resolve_unlisted({"A1"}, now_ms=5_000, sweep_id=20)
    assert [s.key for s in resolved] == [inert.key]
    after = db.t.breaches[inert.key]
    assert after.resolved_at == 5_000 and after.resolved_by == "entity_end" and after.pending_since is None
    assert after.clear_generation == 1 and after.last_sweep_id == 20
    assert db.t.events == {} and db.t.outbox == []
    assert await switch.resolve_unlisted({"A1"}, now_ms=6_000, sweep_id=21) == ()
