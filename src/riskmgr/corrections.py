"""Corrections: the hash compare, `UNVERIFIABLE` audits, CORRECTED versus `suppressed`, release and withdrawal
(design §B6.6 Corrections, §B12, §A13; plan S22).

There is no reconcile pass after `NO_BOOT` or a takeover: a correction comes only from this compare. Each decision
stores, per input, its id and keyed hash (`SeriesInput` / `IndicatorInput`, §A13); on a new md generation, a
`bar_revised` or `indicator_updated` event or a pulled id the ARM already acknowledged, the engine re-reads the
current hash of every input of each non-superseded `POST`, `LATE` or `CORRECTED` row still inside its correction
window (which ends at its bar's `decision_cap`) and calls `check_decision`:

* same id, same hash on every input: `NO_CHANGE`;
* an input missing, not final, quarantined or unreadable now, absent when the decision was made, or hashed under
  another key id (`compare_hashes` never compares across key ids, so a key rotation is never a change): **never a
  change**, `RETRY` on the next trigger; one still open when the window ends gets an audit-only
  `UNVERIFIABLE(reason)` row (`OpenChecks.due_audits`; no notice, no page, a metric);
* a different hash on a final, validated input: a genuine `CORRECTION`, **even when another input cannot be verified
  now** (round 13 review: an unreadable input never hides a real revision; the re-run reads the current inputs, and a
  re-run that is not final writes nothing, so the compare is retried).

After a `CORRECTION` the engine re-runs the pipeline on the recorded order state (entry, handles, `S_at_T`, pinned
versions) and `plan_revision` sets the new row: `revision + 1`, `supersedes_decision_id`; `CORRECTED` when the
outcome, `trigger_met` or the first-bar verdict changed, else the `suppressed` sentinel (which acknowledges the new
hash); a recomputed FAULT never counts as an outcome change. A `CORRECTED` row of a bar **older** than the position's
latest decided bar is INFO only (never actionable); for the latest bar an `ADJUST` is re-proposed (post-guards on the
live quote, as for `LATE`) and `commit_state` re-applied, and the row decides release: it can release the position,
or withdraw a release made on that bar, never one made on a later bar. `PRE` rows are never corrected.

Pure values: ids, codes and keyed hashes only, never a price; nothing here logs.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Final

from src.marketdata.hashing import HashComparison, compare_hashes
from src.riskmgr.ordering import advice
from src.riskmgr.outcomes import Category, Outcome, OutcomeCode
from src.riskmgr.sm11 import Sm11Event
from src.riskmgr.types import ArmEvent, IndicatorInput, SeriesInput

CORRECTABLE_EVENTS: Final = frozenset({ArmEvent.POST, ArmEvent.LATE, ArmEvent.CORRECTED})
"""Rows whose inputs are re-compared (a `CORRECTED` row is the revision that superseded a POST or LATE row)."""

# UNVERIFIABLE reasons (codes only).
INPUT_MISSING: Final = "input_missing"
KEY_ID_DIFFERS: Final = "key_id_differs"
ABSENT_AT_DECISION: Final = "absent_at_decision"

DecisionInput = SeriesInput | IndicatorInput
InputId = tuple[str, ...]


def input_id(item: DecisionInput) -> InputId:
    """The identity of an input, without its hash: `(S, series_id, bar_end)` or
    `(I, indicator_id, output, bar_end, window_start_bucket)`."""
    if isinstance(item, SeriesInput):
        return ("S", item.series_id, str(item.bar_end_ms))
    return ("I", item.indicator_id, item.output, str(item.bar_end_ms), str(item.window_start_bucket_ms))


@dataclass(frozen=True)
class CurrentInput:
    """The current reading of one input: its keyed hash when it is final and validated now, else None with the
    reason code (`missing`, `not_final`, `quarantined`, `unreadable`, ...)."""

    content_hash: str | None
    reason: str = ""

    def __post_init__(self) -> None:
        if (self.content_hash is None) == (not self.reason):
            raise ValueError("a current input has either a hash or a reason")


class InputCheck(str, Enum):
    SAME = "SAME"
    CHANGED = "CHANGED"
    UNVERIFIABLE = "UNVERIFIABLE"


def compare_input(recorded: DecisionInput, current: CurrentInput | None) -> tuple[InputCheck, str]:
    """One input: SAME / CHANGED only between two hashes under one key id; anything else is UNVERIFIABLE with its
    reason (never a change)."""
    if current is None:
        return InputCheck.UNVERIFIABLE, INPUT_MISSING
    if current.content_hash is None:
        return InputCheck.UNVERIFIABLE, current.reason
    result = compare_hashes(recorded.content_hash, current.content_hash)
    if result is HashComparison.INCOMPARABLE:
        return InputCheck.UNVERIFIABLE, KEY_ID_DIFFERS
    return (InputCheck.SAME, "") if result is HashComparison.SAME else (InputCheck.CHANGED, "")


@dataclass(frozen=True, kw_only=True)
class DecisionSummary:
    """What a correction compares between the recorded row and the re-run: the outcome code, `trigger_met`, the
    first-bar verdict code (None where FirstBarGuard did not apply) and whether the row released the position."""

    code: OutcomeCode | None
    trigger_met: bool | None = None
    first_bar: str | None = None
    released: bool = False


@dataclass(frozen=True, kw_only=True)
class RecordedDecision:
    """One stored decision as the correction check reads it (ids, codes and keyed hashes). `absent` names inputs the
    decision needed but did not have when it was made (never a change)."""

    decision_id: int
    event: ArmEvent
    bar_end_ms: int
    revision: int = 0
    superseded: bool = False
    span: bool = False
    inputs: tuple[DecisionInput, ...] = ()
    absent: tuple[str, ...] = ()
    summary: DecisionSummary


class CheckVerdict(str, Enum):
    NOT_ELIGIBLE = "NOT_ELIGIBLE"   # PRE, a span, or a superseded row: never corrected
    FINAL = "FINAL"                 # the correction window has ended: a later revision is visible to `arm replay` only
    NO_CHANGE = "NO_CHANGE"
    RETRY = "RETRY"                 # never a change now; compared again on the next trigger
    CORRECTION = "CORRECTION"


@dataclass(frozen=True)
class CorrectionCheck:
    verdict: CheckVerdict
    changed: tuple[InputId, ...] = ()
    reasons: tuple[str, ...] = ()


def check_decision(rec: RecordedDecision, current: Mapping[InputId, CurrentInput], *, now_ms: int,
                   window_end_ms: int) -> CorrectionCheck:
    """The §B6.6 compare of one recorded decision against the current input hashes. `window_end_ms` is the bar's
    `decision_cap` (§B5.2)."""
    if rec.span or rec.superseded or rec.event not in CORRECTABLE_EVENTS:
        return CorrectionCheck(CheckVerdict.NOT_ELIGIBLE)
    if now_ms >= window_end_ms:
        return CorrectionCheck(CheckVerdict.FINAL)
    changed: list[InputId] = []
    reasons: list[str] = [ABSENT_AT_DECISION] if rec.absent else []
    for item in rec.inputs:
        verdict, reason = compare_input(item, current.get(input_id(item)))
        if verdict is InputCheck.CHANGED:
            changed.append(input_id(item))
        elif verdict is InputCheck.UNVERIFIABLE and reason not in reasons:
            reasons.append(reason)
    if changed:              # a genuine change wins over an input that cannot be verified now
        return CorrectionCheck(CheckVerdict.CORRECTION, tuple(changed), tuple(reasons))
    if reasons:
        return CorrectionCheck(CheckVerdict.RETRY, (), tuple(reasons))
    return CorrectionCheck(CheckVerdict.NO_CHANGE)


# ---------------------------------------------------------------------------------------------------- stored forms
FIRST_BAR_OWNER: Final = "first_bar"
"""The `reason_kind` of FirstBarGuard's verdicts (its KIND): the first-bar verdict of a summary."""


def parse_series_text(text: str) -> SeriesInput | None:
    """`series|bar_end|hash` (the `arm_decisions.series_inputs` form) or `S|series|bar_end|hash`; None if malformed."""
    parts = text.split("|")
    if len(parts) == 4 and parts[0] == "S":
        parts = parts[1:]
    if len(parts) != 3 or not parts[1].isdigit():
        return None
    try:
        return SeriesInput(parts[0], int(parts[1]), parts[2])
    except ValueError:
        return None


def parse_indicator_text(text: str) -> IndicatorInput | None:
    """`I|id|output|bar_end|window_start|hash` (`IndicatorInput.text`) or the same without the tag."""
    parts = text.split("|")
    if len(parts) == 6 and parts[0] == "I":
        parts = parts[1:]
    if len(parts) != 5 or not parts[2].isdigit() or not parts[3].isdigit():
        return None
    try:
        return IndicatorInput(parts[0], parts[1], int(parts[2]), int(parts[3]), parts[4])
    except ValueError:
        return None


def recorded_inputs(series_texts: tuple[str, ...], indicator_texts: tuple[str, ...]) -> tuple[DecisionInput, ...] | None:
    """A stored decision's inputs, or None when one does not parse (such a row is never compared)."""
    out: list[DecisionInput] = []
    for t in series_texts:
        s = parse_series_text(t)
        if s is None:
            return None
        out.append(s)
    for t in indicator_texts:
        i = parse_indicator_text(t)
        if i is None:
            return None
        out.append(i)
    return tuple(out)


def summary_of(outcome: str, category: str, reason: str | None, reason_kind: str | None,
               trigger_met: bool | None) -> DecisionSummary:
    """The compare summary of a stored or re-run decision row (an unparseable code compares as None)."""
    try:
        code: OutcomeCode | None = OutcomeCode(Outcome(outcome), Category(category), reason or "", reason_kind or "")
    except ValueError:
        code = None
    return DecisionSummary(code=code, trigger_met=trigger_met,
                           first_bar=reason if reason_kind == FIRST_BAR_OWNER else None,
                           released=outcome == Outcome.COMPLETE.value)


# ---------------------------------------------------------------------------------------------------- audits
@dataclass(frozen=True)
class UnverifiableAudit:
    """An audit-only `arm_correction_audit` row `UNVERIFIABLE(reason)`: no notice, no page (a metric)."""

    decision_id: int
    bar_end_ms: int
    reason: str
    adapter_id: str = ""
    position_id: str = ""
    """The decision's position (kept with the open compare, so its audit is written even after the position left
    the engine's live set, round 13 review); empty when the caller did not give it."""


class OpenChecks:
    """Compares left at `RETRY`, until they resolve or their window ends (then one `UNVERIFIABLE` audit each).
    In memory per term: after a restart the next trigger opens them again."""

    def __init__(self) -> None:
        self._open: dict[int, tuple[int, int, str, tuple[str, str]]] = {}

    def note(self, rec: RecordedDecision, check: CorrectionCheck, window_end_ms: int,
             owner: tuple[str, str] = ("", "")) -> None:
        """Record the outcome of one compare: a RETRY stays open (keeping the first reason it was opened with; the
        window end follows the latest compare), anything else closes. `owner`: `(adapter_id, position_id)`."""
        if check.verdict is CheckVerdict.RETRY:
            held = self._open.get(rec.decision_id)
            reason = held[2] if held is not None else check.reasons[0]
            self._open[rec.decision_id] = (rec.bar_end_ms, window_end_ms, reason,
                                           owner if owner[0] or held is None else held[3])
        elif check.verdict is not CheckVerdict.FINAL:
            self._open.pop(rec.decision_id, None)

    def due_audits(self, now_ms: int) -> list[UnverifiableAudit]:
        """The open compares whose window has ended, as audits (each returned once)."""
        due = [(d, v) for d, v in self._open.items() if now_ms >= v[1]]
        for d, _ in due:
            del self._open[d]
        return [UnverifiableAudit(d, bar, reason, who[0], who[1]) for d, (bar, _, reason, who) in sorted(due)]

    def __len__(self) -> int:
        return len(self._open)


# ---------------------------------------------------------------------------------------------------- revision
class RevisionKind(str, Enum):
    CORRECTED = "CORRECTED"     # an item: the outcome, trigger_met or the first-bar verdict changed
    SUPPRESSED = "SUPPRESSED"   # stored with the `suppressed` sentinel: acknowledges the new hash, never shown


class ReleaseEffect(str, Enum):
    NONE = "NONE"
    RELEASE = "RELEASE"         # the corrected latest bar now releases the position
    WITHDRAW = "WITHDRAW"       # a release made on this (latest) bar no longer holds (INFO)


def _verdict_key(s: DecisionSummary) -> tuple[object, ...]:
    code = s.code
    return (None if code is None else (code.outcome, code.reason, code.owner), s.trigger_met, s.first_bar)


def revision_kind(original: DecisionSummary, rerun: DecisionSummary) -> RevisionKind:
    """CORRECTED when the outcome, `trigger_met` or the first-bar verdict changed; a recomputed FAULT never counts."""
    if rerun.code is None or rerun.code.category is Category.FAULT:
        return RevisionKind.SUPPRESSED
    return RevisionKind.CORRECTED if _verdict_key(original) != _verdict_key(rerun) else RevisionKind.SUPPRESSED


@dataclass(frozen=True, kw_only=True)
class RevisionPlan:
    """The row a genuine correction writes. `actionable`: a re-proposed ADJUST on the latest bar (post-guards were
    re-run on the live quote); `info_only`: a CORRECTED item of an older bar."""

    revision: int
    supersedes_decision_id: int
    kind: RevisionKind
    latest: bool
    advice: bool
    actionable: bool
    info_only: bool
    reapply_commit_state: bool
    release: ReleaseEffect
    sm11_events: tuple[Sm11Event, ...] = ()

    @property
    def shown(self) -> bool:
        return self.kind is RevisionKind.CORRECTED


def release_effect(original: DecisionSummary, rerun: DecisionSummary, *, bar_end_ms: int, latest: bool,
                   released_on_bar_ms: int | None) -> ReleaseEffect:
    """Only a correction of the latest decided bar decides release: it releases (when not released already) or
    withdraws a release made on that same bar, never one made on a later or earlier bar."""
    if not latest:
        return ReleaseEffect.NONE
    if rerun.released and released_on_bar_ms is None:
        return ReleaseEffect.RELEASE
    if original.released and not rerun.released and released_on_bar_ms == bar_end_ms:
        return ReleaseEffect.WITHDRAW
    return ReleaseEffect.NONE


def plan_revision(rec: RecordedDecision, rerun: DecisionSummary, *, latest_decided_bar_ms: int,
                  released_on_bar_ms: int | None) -> RevisionPlan:
    """The revision row after `check_decision` returned CORRECTION and the pipeline re-ran on the recorded order
    state. `released_on_bar_ms`: the bar end of the position's standing release, or None."""
    if rec.span or rec.superseded or rec.event not in CORRECTABLE_EVENTS:
        raise ValueError("only a non-superseded POST, LATE or CORRECTED row is revised")
    if rec.bar_end_ms > latest_decided_bar_ms:
        raise ValueError("a decided bar is never newer than the position's latest decided bar")
    kind = revision_kind(rec.summary, rerun)
    latest = rec.bar_end_ms == latest_decided_bar_ms
    corrected = kind is RevisionKind.CORRECTED
    release = (release_effect(rec.summary, rerun, bar_end_ms=rec.bar_end_ms, latest=latest,
                              released_on_bar_ms=released_on_bar_ms) if corrected else ReleaseEffect.NONE)
    events: tuple[Sm11Event, ...] = ()
    if release is ReleaseEffect.RELEASE:
        events = (Sm11Event.RELEASED,)
    elif release is ReleaseEffect.WITHDRAW:
        events = (Sm11Event.RELEASE_WITHDRAWN,)
    code = rerun.code
    return RevisionPlan(
        revision=rec.revision + 1,
        supersedes_decision_id=rec.decision_id,
        kind=kind,
        latest=latest,
        advice=advice(ArmEvent.CORRECTED, suppressed=not corrected, older_bar=not latest),
        actionable=corrected and latest and code is not None and code.outcome is Outcome.ADJUST,
        info_only=corrected and not latest,
        reapply_commit_state=corrected and latest,
        release=release,
        sm11_events=events,
    )
