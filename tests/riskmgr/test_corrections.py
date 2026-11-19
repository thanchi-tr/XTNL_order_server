"""Corrections: hash compare within one key id, UNVERIFIABLE audits, CORRECTED versus suppressed, release and
withdrawal (plan S22; design §B6.6 Corrections, §B12). Synthetic bars; hashes are keyed under test keys."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.marketdata.hashing import Keyring, content_hash
from src.marketdata.types import M15, SeriesId, Source
from src.marketdata.types import Side as MdSide
from src.riskmgr.corrections import (
    ABSENT_AT_DECISION,
    INPUT_MISSING,
    KEY_ID_DIFFERS,
    CheckVerdict,
    CurrentInput,
    DecisionSummary,
    InputCheck,
    OpenChecks,
    RecordedDecision,
    ReleaseEffect,
    RevisionKind,
    UnverifiableAudit,
    check_decision,
    compare_input,
    input_id,
    plan_revision,
    revision_kind,
)
from src.riskmgr.outcomes import Category, Outcome, OutcomeCode
from src.riskmgr.sm11 import Sm11Event
from src.riskmgr.types import ArmEvent, IndicatorInput, SeriesInput

Q = M15.timeframe_ms
T = 1_700_000_100_000 - 1_700_000_100_000 % Q
CAP = T + Q + 5_000
SERIES = SeriesId("mock", "SYN.A", M15, MdSide.BID, Source.BROKER)
KEY1, KEY2 = b"\x01" * 32, b"\x02" * 32
RING1 = Keyring.of(1, KEY1)
RING2 = Keyring.of(2, KEY2, [(1, KEY1)])

ADJUST = OutcomeCode.engine(Outcome.ADJUST)
BELOW = OutcomeCode(Outcome.STRATEGY_REASON, Category.HOLD, "BELOW_TRIGGER", "break_even_on_risk_close")
COMPLETE = OutcomeCode.engine(Outcome.COMPLETE)
FAULT = OutcomeCode.engine(Outcome.DATA_UNAVAILABLE, "stop_at_bar_end")


def h(close: str, ring: Keyring = RING1, bar_end: int = T) -> str:
    c = Decimal(close)
    return content_hash(ring, SERIES.canonical, bar_end, c, c, c, c)


BAR = SeriesInput(SERIES.canonical, T, h("1.2"))
IND = IndicatorInput("a1b2c3d4e5f60718", "atr", T, T - 14 * Q, h("9.9"))


def rec(*, event: ArmEvent = ArmEvent.POST, summary: DecisionSummary | None = None, bar_end: int = T,
        **kw: object) -> RecordedDecision:
    return RecordedDecision(decision_id=7, event=event, bar_end_ms=bar_end, inputs=(BAR, IND),
                            summary=summary or DecisionSummary(code=ADJUST, trigger_met=True, first_bar="PASS"),
                            **kw)  # type: ignore[arg-type]


def now(**over: CurrentInput) -> dict[tuple[str, ...], CurrentInput]:
    cur = {input_id(BAR): CurrentInput(BAR.content_hash), input_id(IND): CurrentInput(IND.content_hash)}
    for name, value in over.items():
        cur[input_id(BAR if name == "bar" else IND)] = value
    return cur


def test_same_hash_is_a_no_op() -> None:
    assert check_decision(rec(), now(), now_ms=T + 60_000, window_end_ms=CAP).verdict is CheckVerdict.NO_CHANGE


def test_a_different_hash_is_a_correction() -> None:
    check = check_decision(rec(), now(bar=CurrentInput(h("1.3"))), now_ms=T + 60_000, window_end_ms=CAP)
    assert check.verdict is CheckVerdict.CORRECTION and check.changed == (input_id(BAR),)


def test_a_different_key_id_is_never_a_change() -> None:
    rotated = CurrentInput(h("1.3", RING2))                   # even with another close: never compared
    assert compare_input(BAR, rotated) == (InputCheck.UNVERIFIABLE, KEY_ID_DIFFERS)
    check = check_decision(rec(), now(bar=rotated), now_ms=T + 60_000, window_end_ms=CAP)
    assert check.verdict is CheckVerdict.RETRY and check.reasons == (KEY_ID_DIFFERS,)


@pytest.mark.parametrize("reading,reason", [
    (None, INPUT_MISSING),
    (CurrentInput(None, "not_final"), "not_final"),
    (CurrentInput(None, "quarantined"), "quarantined"),
    (CurrentInput(None, "unreadable"), "unreadable"),
])
def test_unreadable_inputs_are_never_a_change(reading: CurrentInput | None, reason: str) -> None:
    cur = now()
    if reading is None:
        del cur[input_id(BAR)]
    else:
        cur[input_id(BAR)] = reading
    check = check_decision(rec(), cur, now_ms=T + 60_000, window_end_ms=CAP)
    assert check.verdict is CheckVerdict.RETRY and check.reasons == (reason,) and check.changed == ()
    # round 13 review: a real change elsewhere is never hidden by the unreadable input (the re-run reads current
    # inputs; a re-run that is not final writes nothing)
    cur[input_id(IND)] = CurrentInput(h("8.8"))
    check = check_decision(rec(), cur, now_ms=T + 60_000, window_end_ms=CAP)
    assert check.verdict is CheckVerdict.CORRECTION and check.changed == (input_id(IND),)
    assert check.reasons == (reason,)


def test_an_input_absent_at_decision_time_is_never_a_change() -> None:
    check = check_decision(rec(absent=("indicator",)), now(), now_ms=T + 60_000, window_end_ms=CAP)
    assert check.verdict is CheckVerdict.RETRY and check.reasons == (ABSENT_AT_DECISION,)


def test_eligibility_and_the_window() -> None:
    assert check_decision(rec(event=ArmEvent.PRE), now(), now_ms=T, window_end_ms=CAP).verdict \
        is CheckVerdict.NOT_ELIGIBLE                         # PRE rows are never corrected
    assert check_decision(rec(superseded=True), now(), now_ms=T, window_end_ms=CAP).verdict \
        is CheckVerdict.NOT_ELIGIBLE
    assert check_decision(rec(span=True), now(), now_ms=T, window_end_ms=CAP).verdict is CheckVerdict.NOT_ELIGIBLE
    assert check_decision(rec(event=ArmEvent.REOPEN), now(), now_ms=T, window_end_ms=CAP).verdict \
        is CheckVerdict.NOT_ELIGIBLE
    changed = now(bar=CurrentInput(h("1.3")))
    assert check_decision(rec(), changed, now_ms=CAP, window_end_ms=CAP).verdict is CheckVerdict.FINAL
    assert check_decision(rec(event=ArmEvent.LATE), changed, now_ms=CAP - 1, window_end_ms=CAP).verdict \
        is CheckVerdict.CORRECTION
    assert check_decision(rec(event=ArmEvent.CORRECTED, revision=1), changed, now_ms=CAP - 1,
                          window_end_ms=CAP).verdict is CheckVerdict.CORRECTION


def test_an_open_compare_gets_one_unverifiable_audit_when_its_window_ends() -> None:
    checks = OpenChecks()
    r = rec()
    retry = check_decision(r, now(bar=CurrentInput(None, "not_final")), now_ms=T + 60_000, window_end_ms=CAP)
    checks.note(r, retry, CAP)
    assert len(checks) == 1 and checks.due_audits(CAP - 1) == []
    assert checks.due_audits(CAP) == [UnverifiableAudit(7, T, "not_final")]
    assert checks.due_audits(CAP + 1) == [] and len(checks) == 0
    # a later compare that resolves closes it
    checks.note(r, retry, CAP)
    checks.note(r, check_decision(r, now(), now_ms=T + 90_000, window_end_ms=CAP), CAP)
    assert len(checks) == 0


def test_an_open_compare_keeps_its_first_reason_across_retries() -> None:
    checks = OpenChecks()
    r = rec()
    first = check_decision(r, now(bar=CurrentInput(None, "not_final")), now_ms=T + 60_000, window_end_ms=CAP)
    second = check_decision(r, now(bar=CurrentInput(None, "quarantined")), now_ms=T + 90_000, window_end_ms=CAP)
    assert first.reasons[0] != second.reasons[0]
    checks.note(r, first, CAP)
    checks.note(r, second, CAP + 1_000)                    # the window end follows the latest compare
    assert checks.due_audits(CAP) == []
    assert checks.due_audits(CAP + 1_000) == [UnverifiableAudit(7, T, "not_final")]


def test_current_input_has_a_hash_or_a_reason() -> None:
    with pytest.raises(ValueError):
        CurrentInput(None)
    with pytest.raises(ValueError):
        CurrentInput(h("1.2"), "not_final")


# ---------------------------------------------------------------------------------------------------- revisions
def test_corrected_versus_suppressed() -> None:
    orig = DecisionSummary(code=ADJUST, trigger_met=True, first_bar="PASS")
    assert revision_kind(orig, DecisionSummary(code=BELOW, trigger_met=False, first_bar="PASS")) \
        is RevisionKind.CORRECTED
    assert revision_kind(orig, DecisionSummary(code=ADJUST, trigger_met=True, first_bar="FIRST_BAR_VOLATILE")) \
        is RevisionKind.CORRECTED                            # the first-bar verdict changed
    assert revision_kind(orig, DecisionSummary(code=ADJUST, trigger_met=True, first_bar="PASS")) \
        is RevisionKind.SUPPRESSED
    assert revision_kind(orig, DecisionSummary(code=FAULT)) is RevisionKind.SUPPRESSED   # a FAULT never counts
    assert revision_kind(orig, DecisionSummary(code=None)) is RevisionKind.SUPPRESSED


def test_a_correction_of_the_latest_bar_is_re_proposed_and_actionable() -> None:
    r = rec(summary=DecisionSummary(code=BELOW, trigger_met=False))
    plan = plan_revision(r, DecisionSummary(code=ADJUST, trigger_met=True), latest_decided_bar_ms=T,
                         released_on_bar_ms=None)
    assert plan.revision == 1 and plan.supersedes_decision_id == 7 and plan.kind is RevisionKind.CORRECTED
    assert plan.latest and plan.advice and plan.actionable and plan.reapply_commit_state and plan.shown
    assert not plan.info_only and plan.release is ReleaseEffect.NONE and plan.sm11_events == ()


def test_a_correction_of_an_older_bar_is_info_only() -> None:
    r = rec(summary=DecisionSummary(code=BELOW, trigger_met=False))
    plan = plan_revision(r, DecisionSummary(code=ADJUST, trigger_met=True), latest_decided_bar_ms=T + Q,
                         released_on_bar_ms=None)
    assert plan.kind is RevisionKind.CORRECTED and plan.info_only
    assert not plan.advice and not plan.actionable and not plan.reapply_commit_state


def test_a_suppressed_revision_is_never_advice() -> None:
    r = rec()
    plan = plan_revision(r, DecisionSummary(code=ADJUST, trigger_met=True, first_bar="PASS"),
                         latest_decided_bar_ms=T, released_on_bar_ms=None)
    assert plan.kind is RevisionKind.SUPPRESSED and not plan.advice and not plan.shown and not plan.actionable


def test_release_and_withdrawal_rules() -> None:
    not_released = rec(summary=DecisionSummary(code=BELOW, trigger_met=False))
    releases = DecisionSummary(code=COMPLETE, trigger_met=True, released=True)
    plan = plan_revision(not_released, releases, latest_decided_bar_ms=T, released_on_bar_ms=None)
    assert plan.release is ReleaseEffect.RELEASE and plan.sm11_events == (Sm11Event.RELEASED,)
    # older bar: never releases
    older = plan_revision(not_released, releases, latest_decided_bar_ms=T + Q, released_on_bar_ms=None)
    assert older.release is ReleaseEffect.NONE
    # a release made on this (latest) bar is withdrawn
    released = rec(summary=releases)
    back = plan_revision(released, DecisionSummary(code=BELOW, trigger_met=False), latest_decided_bar_ms=T,
                         released_on_bar_ms=T)
    assert back.release is ReleaseEffect.WITHDRAW and back.sm11_events == (Sm11Event.RELEASE_WITHDRAWN,)
    # never a release made on another bar
    other = plan_revision(released, DecisionSummary(code=BELOW, trigger_met=False), latest_decided_bar_ms=T,
                          released_on_bar_ms=T - Q)
    assert other.release is ReleaseEffect.NONE
    # a recomputed FAULT changes nothing
    fault = plan_revision(released, DecisionSummary(code=FAULT), latest_decided_bar_ms=T, released_on_bar_ms=T)
    assert fault.kind is RevisionKind.SUPPRESSED and fault.release is ReleaseEffect.NONE


def test_plan_revision_refuses_rows_that_are_never_revised() -> None:
    with pytest.raises(ValueError):
        plan_revision(rec(event=ArmEvent.PRE), DecisionSummary(code=ADJUST), latest_decided_bar_ms=T,
                      released_on_bar_ms=None)
    with pytest.raises(ValueError):
        plan_revision(rec(bar_end=T + Q), DecisionSummary(code=ADJUST), latest_decided_bar_ms=T,
                      released_on_bar_ms=None)


def test_input_ids_carry_no_hash() -> None:
    assert input_id(BAR) == ("S", SERIES.canonical, str(T))
    assert input_id(IND) == ("I", "a1b2c3d4e5f60718", "atr", str(T), str(T - 14 * Q))
    assert all(BAR.content_hash not in part for part in input_id(BAR))
