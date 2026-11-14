"""Rule-state machine (oversee-rules.md §2 Rule states) and the halt level decision (§2 Halt level)."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from decimal import Decimal

from src.oversee.config import BandRuleConfig, SweepConfig
from src.oversee.rules import Evaluation, band_result
from src.oversee.states import StepParams, step_state
from src.oversee.sweep import apply_pass, decide_level
from src.oversee.types import BreachState, Cap, HaltLevel, Level, Result, RuleOutcome, Subject, Transition, Unknown

D = Decimal
CFG = SweepConfig()
SUBJ = Subject("account", "A1", "A1")
BAND = BandRuleConfig(flag_above_pct=D("1.0"), halt_above_pct=D("1.5"))


def pct(v: str, stale: bool = False) -> Result:
    return band_result(D(v), BAND, CFG.release_margin, "position_risk", frozenset({("x", "y")}) if stale else frozenset())


SOFT = Result(Level.SOFT_HALT, "soft")
HARD = Result(Level.HARD_HALT, "hard")
PASS = Result(Level.PASS, "ok")


def drive(seq: Sequence[tuple[int, RuleOutcome]], confirm: int = 0, prev: BreachState | None = None,
          start_id: int = 1) -> tuple[BreachState | None, list[Transition]]:
    s, out = prev, []
    for i, (t, o) in enumerate(seq):
        s, trs = step_state(s, "r", SUBJ, o, StepParams(t, start_id + i, confirm, CFG))
        out += trs
    return s, out


def every(t0: int, t1: int, o: RuleOutcome, step: int = 1000) -> list[tuple[int, RuleOutcome]]:
    return [(t, o) for t in range(t0, t1 + 1, step)]


def kinds(trs: list[Transition]) -> list[str]:
    return [t.transition for t in trs]


def test_confirm_then_activate() -> None:
    s, trs = drive([(0, SOFT), (500, SOFT)], confirm=1000)
    assert s is not None and not s.active and s.pending_since == 0 and trs == []
    s, trs = drive([(1000, SOFT)], confirm=1000, prev=s, start_id=3)
    assert s is not None and s.active and s.level is Level.SOFT_HALT and kinds(trs) == ["activated"]
    assert s.activation_id == 3


def test_confirm_restarts_when_not_seen() -> None:
    s, _ = drive([(0, SOFT), (500, PASS), (1000, SOFT), (1500, SOFT)], confirm=1000)
    assert s is not None and not s.active and s.pending_since == 1000
    s, trs = drive([(2000, SOFT)], confirm=1000, prev=s)
    assert s is not None and s.active and kinds(trs) == ["activated"]


def test_pass_with_no_state_stores_nothing() -> None:
    assert drive([(0, PASS)])[0] is None


def test_release_hysteresis_no_flapping_at_band_edge() -> None:
    s, _ = drive([(0, pct("1.6"))])
    assert s is not None and s.level is Level.SOFT_HALT
    # 1.45 % is FLAG by band but inside the 10 % release margin of 1.5 %: never lowers
    s, trs = drive(every(1000, 30_000, pct("1.45")) + every(31_000, 40_000, pct("1.55")), prev=s)
    assert s is not None and s.level is Level.SOFT_HALT and trs == []
    # 1.30 % is below the margin: lowers to FLAG only after release_ms (5 s)
    s, trs = drive(every(41_000, 45_000, pct("1.30")), prev=s)
    assert s is not None and s.level is Level.SOFT_HALT and trs == []
    s, trs = drive([(46_000, pct("1.30"))], prev=s)
    assert s is not None and s.level is Level.FLAG and kinds(trs) == ["lowered"]


def test_release_restarts_if_value_returns() -> None:
    s, _ = drive([(0, pct("1.6"))])
    s, _ = drive([(1000, pct("0.5")), (4000, pct("1.6")), (5000, pct("0.5")), (9000, pct("0.5"))], prev=s)
    assert s is not None and s.level is Level.SOFT_HALT
    s, trs = drive([(10_000, pct("0.5"))], prev=s)
    assert s is not None and not s.active and s.resolved_by == "sweep" and kinds(trs) == ["resolved"]


def test_capped_result_cannot_create_hard() -> None:
    s, trs = drive([(0, HARD.capped())])
    assert s is not None and s.level is Level.SOFT_HALT and s.cap is Cap.SOFT and kinds(trs) == ["activated"]
    s, trs = drive([(1000, HARD)], prev=s)
    assert s is not None and s.level is Level.HARD_HALT and kinds(trs) == ["raised"]


def test_capped_cannot_resolve_hard() -> None:
    s, _ = drive([(0, HARD)])
    s, trs = drive(every(1000, 20_000, PASS.capped()), prev=s)
    assert s is not None and s.level is Level.HARD_HALT and s.cap is Cap.SOFT and kinds(trs) == ["capped"]
    s, trs = drive(every(21_000, 26_000, PASS), prev=s)
    assert s is not None and not s.active and "resolved" in kinds(trs)


def test_unknown_grace_then_capped_soft_and_resolve_on_known() -> None:
    unk = Unknown("mark_unknown")
    s, trs = drive(every(0, 29_000, unk))
    assert s is not None and not s.active and s.unknown_since == 0 and trs == []
    s, trs = drive([(30_000, unk)], prev=s)
    assert s is not None and s.level is Level.SOFT_HALT and s.cap is Cap.SOFT and s.reason == "mark_unknown"
    assert kinds(trs) == ["activated"]
    s, trs = drive([(31_000, PASS)], prev=s)   # a known result below SOFT resolves at once
    assert s is not None and not s.active and kinds(trs) == ["resolved"]


def test_unknown_grace_survives_restart_via_unknown_since() -> None:
    s, _ = drive([(0, Unknown("risk_unknown"))])
    assert s is not None
    restored = replace(s)    # re-read from halt_breaches after a failover
    s, trs = drive([(30_000, Unknown("risk_unknown"))], prev=restored)
    assert s is not None and s.active and kinds(trs) == ["activated"]


def test_unknown_on_hard_keeps_hard_capped() -> None:
    s, _ = drive([(0, HARD)])
    s, trs = drive(every(1000, 31_000, Unknown("risk_unknown")), prev=s)
    assert s is not None and s.level is Level.HARD_HALT and s.cap is Cap.SOFT and kinds(trs) == ["capped"]


def test_escalation_clock_accumulates_and_pauses_on_capped_spells() -> None:
    s, _ = drive(every(0, 10_000, SOFT))
    assert s is not None and s.uncapped_ms == 10_000
    s, trs = drive(every(11_000, 20_000, SOFT.capped()), prev=s)
    assert s is not None and s.uncapped_ms == 10_000 and kinds(trs) == ["capped"]
    s, trs = drive(every(21_000, 25_000, SOFT), prev=s)
    assert s is not None and s.uncapped_ms == 15_000 and kinds(trs) == ["uncapped"]


def test_escalation_clock_never_counts_unobserved_time() -> None:
    s, _ = drive([(0, SOFT), (1000, SOFT), (61_000, SOFT)])   # a 60 s failover gap counts 2 x interval
    assert s is not None and s.uncapped_ms == 1000 + 2 * CFG.interval_ms


def test_flag_lowering_pauses_and_cool_down_resets() -> None:
    s, _ = drive(every(0, 10_000, pct("1.6")))
    assert s is not None and s.uncapped_ms == 10_000
    s, _ = drive(every(11_000, 16_000, pct("1.2")), prev=s)
    # still active at SOFT during the 5 s release wait (counted), then paused at FLAG
    assert s is not None and s.level is Level.FLAG and s.below_soft_since == 16_000 and s.uncapped_ms == 15_000
    s, _ = drive([(16_000 + CFG.cool_down_ms - 1000, pct("1.2"))], prev=s)
    assert s is not None and s.uncapped_ms == 15_000
    s, _ = drive([(16_000 + CFG.cool_down_ms, pct("1.2"))], prev=s)
    assert s is not None and s.uncapped_ms == 0


def test_resolved_then_reactivated_keeps_clock_until_cool_down() -> None:
    s, _ = drive(every(0, 10_000, SOFT))
    s, _ = drive(every(11_000, 16_000, PASS), prev=s)
    assert s is not None and not s.active and s.uncapped_ms == 15_000
    s, trs = drive([(20_000, SOFT)], prev=s, start_id=99)
    assert s is not None and s.active and s.uncapped_ms == 15_000 and s.activation_id == 99


def ev(o: RuleOutcome, subject: Subject = SUBJ, confirm: int = 0) -> Evaluation:
    return Evaluation(subject, o, confirm)


def test_apply_pass_resolves_missing_subjects_with_reason() -> None:
    pos = Subject("position", "stonex:P", "A1")
    day = Subject("account_day", "A1:2026-11-11", "A1")
    prev = {}
    for rule, subj in (("max_position_risk", pos), ("max_daily_loss", day), ("old_rule", SUBJ)):
        s, _ = step_state(None, rule, subj, SOFT, StepParams(0, 1, 0, CFG))
        assert s is not None
        prev[s.key] = s
    res = apply_pass(prev, {}, 1000, 2, CFG, frozenset({"max_position_risk", "max_daily_loss"}))
    by = {k[0]: s.resolved_by for k, s in res.states.items()}
    assert by == {"max_position_risk": "entity_end", "max_daily_loss": "day_end", "old_rule": "reload"}
    assert [t.transition for t in res.transitions] == ["resolved"] * 3
    assert all(t.sweep_id == 2 for t in res.transitions)


def test_apply_pass_replay_produces_no_transition() -> None:
    first = apply_pass({}, {("r", "account", "A1"): ev(SOFT)}, 0, 1, CFG, frozenset({"r"}))
    again = apply_pass(first.states, {("r", "account", "A1"): ev(SOFT)}, 1000, 2, CFG, frozenset({"r"}))
    assert [t.transition for t in first.transitions] == ["activated"] and again.transitions == []


# ---------------------------------------------------------------- decide_level

def active(level: Level, cap: Cap = Cap.NONE, uncapped: int = 0, rule: str = "r", **kw: object) -> BreachState:
    return BreachState(rule, "account", "A1", "A1", level=level, cap=cap, reason=rule, active_since=0,
                       activation_id=1, uncapped_ms=uncapped, **kw)  # type: ignore[arg-type]


def test_decide_trips_soft_and_hard() -> None:
    d = decide_level([active(Level.SOFT_HALT)], HaltLevel.NONE, False, None, 0, CFG)
    assert (d.action, d.target) == ("trip", HaltLevel.SOFT)
    d = decide_level([active(Level.HARD_HALT)], HaltLevel.SOFT, False, None, 0, CFG)
    assert (d.action, d.target) == ("trip", HaltLevel.HARD)


def test_flag_states_never_affect_level() -> None:
    d = decide_level([active(Level.FLAG)], HaltLevel.NONE, False, None, 0, CFG)
    assert d.action == "none" and d.target is HaltLevel.NONE


def test_escalation_by_accumulated_uncapped_time() -> None:
    d = decide_level([active(Level.SOFT_HALT, uncapped=CFG.escalate_after_ms)], HaltLevel.SOFT, False, None, 0, CFG)
    assert (d.action, d.target, d.reason) == ("trip", HaltLevel.HARD, "escalation")
    d = decide_level([active(Level.SOFT_HALT, uncapped=CFG.escalate_after_ms - 1)], HaltLevel.SOFT, False, None, 0,
                     CFG)
    assert d.action == "none"
    d = decide_level([active(Level.SOFT_HALT, Cap.SOFT, uncapped=10 * CFG.escalate_after_ms)], HaltLevel.SOFT,
                     False, None, 0, CFG)
    assert d.action == "none"   # capped states never escalate


def test_hard_latch_held_until_operator_clear() -> None:
    d = decide_level([], HaltLevel.HARD, True, 0, 10 * CFG.cool_down_ms, CFG)
    assert d.action == "none" and d.target is HaltLevel.HARD


def test_cool_down_to_none() -> None:
    now = 1_000_000
    gone = replace(active(Level.PASS), resolved_at=now - 100_000, below_soft_since=now - CFG.cool_down_ms + 1)
    d = decide_level([gone], HaltLevel.SOFT, False, 0, now, CFG)
    assert d.action == "none" and d.target is HaltLevel.NONE
    d = decide_level([gone], HaltLevel.SOFT, False, 0, now + 1, CFG)
    assert (d.action, d.target) == ("clear", HaltLevel.NONE)
    d = decide_level([gone], HaltLevel.SOFT, False, now, now + 1, CFG)   # level rose later: wait from then
    assert d.action == "none"
