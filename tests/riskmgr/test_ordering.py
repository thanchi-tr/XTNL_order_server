"""`decision_seq` and `advice` (plan S22; design §B6.6 Order)."""
from __future__ import annotations

import pytest

from src.riskmgr.ordering import OrderedRow, advice, in_decision_order, newest_advice, next_decision_seq
from src.riskmgr.types import ArmEvent


def test_advice_table() -> None:
    assert advice(ArmEvent.POST) and advice(ArmEvent.LATE) and advice(ArmEvent.REOPEN)
    assert advice(ArmEvent.CORRECTED)                                  # a revision of the latest decided bar
    assert not advice(ArmEvent.CORRECTED, older_bar=True)              # INFO correction of an older bar
    assert not advice(ArmEvent.CORRECTED, suppressed=True)
    assert not advice(ArmEvent.PRE)
    assert not advice(ArmEvent.POST, span=True)


def test_decision_seq_is_monotonic() -> None:
    assert next_decision_seq(0) == 1 and next_decision_seq(41) == 42
    for bad in (-1, True):
        with pytest.raises(ValueError):
            next_decision_seq(bad)


def row(seq: int, event: ArmEvent, bar: int, adv: bool) -> OrderedRow:
    return OrderedRow(decision_id=100 + seq, decision_seq=seq, decided_at_ms=seq * 1_000, bar_end_ms=bar,
                      event=event, advice=adv)


def test_newest_decision_is_the_highest_seq_advice_row() -> None:
    rows = [row(1, ArmEvent.POST, 10, True), row(2, ArmEvent.PRE, 20, False), row(3, ArmEvent.REOPEN, 5, True),
            row(4, ArmEvent.CORRECTED, 10, False)]
    newest = newest_advice(rows)
    assert newest is not None and newest.decision_seq == 3            # a REOPEN row carries an older bar end
    assert newest_advice([rows[1]]) is None
    assert [r.decision_seq for r in in_decision_order(reversed(rows))] == [1, 2, 3, 4]
    with pytest.raises(ValueError):
        in_decision_order([rows[0], row(1, ArmEvent.LATE, 30, True)])
