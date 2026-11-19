"""Decision order: `decision_seq` and `advice` (design §B6.6 Order, §B10; plan S22).

Every decision row has `decided_at_ms` and a `decision_seq` that is monotonic per (position, resource, owner): it is
taken from `arm_positions.decision_seq` under the same `row_version` CAS that writes the row (§B5.3), so two rows of
one position never share a number and a later row always has a higher one. `LATE`, `REOPEN` and revised rows carry
the original bar's `bar_end_ms`, so the bar end alone does not order rows: `decision_seq` does.

`advice` (the row states the position's current verdict) is true for `POST`, `LATE` and `REOPEN` rows and for a
revision of the latest decided bar; false for `PRE` rows, `UNRESOLVED_RANGE` spans, `suppressed` revisions and INFO
corrections of an older bar. "Newest decision" for Phase 2 = the advice row with the highest `decision_seq`
(`newest_advice`).

Pure values; nothing here logs.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from src.riskmgr.types import ArmEvent


def next_decision_seq(current: int) -> int:
    """The `decision_seq` of the next row of a position whose `arm_positions.decision_seq` is `current` (0 before
    its first row). The CAS writes the returned value back with the row."""
    if isinstance(current, bool) or not isinstance(current, int) or current < 0:
        raise ValueError("decision_seq is a non-negative integer")
    return current + 1


def advice(event: ArmEvent, *, span: bool = False, suppressed: bool = False, older_bar: bool = False) -> bool:
    """Whether a row states the position's current verdict (§B6.6 Order).

    `span`: an `UNRESOLVED_RANGE` row; `suppressed`: a revision stored with the `suppressed` sentinel; `older_bar`: a
    `CORRECTED` revision of a bar older than the position's latest decided bar (INFO only, never actionable)."""
    if span or suppressed or event is ArmEvent.PRE:
        return False
    if event is ArmEvent.CORRECTED:
        return not older_bar
    return True


@dataclass(frozen=True, kw_only=True)
class OrderedRow:
    """The order fields of one stored decision row (ids, codes and times only)."""

    decision_id: int
    decision_seq: int
    decided_at_ms: int
    bar_end_ms: int
    event: ArmEvent
    advice: bool


def newest_advice(rows: Iterable[OrderedRow]) -> OrderedRow | None:
    """The advice row with the highest `decision_seq` (Phase 2's "newest decision"), or None."""
    best: OrderedRow | None = None
    for row in rows:
        if row.advice and (best is None or row.decision_seq > best.decision_seq):
            best = row
    return best


def in_decision_order(rows: Iterable[OrderedRow]) -> list[OrderedRow]:
    """Rows of one position in `decision_seq` order (refuses two rows with one number: the CAS rules it out)."""
    out = sorted(rows, key=lambda r: r.decision_seq)
    for a, b in zip(out, out[1:], strict=False):
        if a.decision_seq == b.decision_seq:
            raise ValueError("two decision rows of one position share a decision_seq")
    return out
