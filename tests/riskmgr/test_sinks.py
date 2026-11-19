"""Sinks (S27, design §B3 Sinks, §B10): the fixed `ARM_SINKS` order, a savepoint per position, the notice sink
seeing only inserted decisions, and a fatal error propagating."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

import pytest

from src.db.arm_ports import ArmStateTxn, PositionCas
from src.db.fence import EpochMismatch
from src.riskmgr.lockout import PHASE1_SINKS
from src.riskmgr.sinks import (
    ARM_SINKS,
    Decided,
    DecisionSink,
    DeferredNoticeSink,
    SinkChain,
    WriteStatus,
    Written,
    default_sinks,
)
from src.riskmgr.types import ArmEvent
from tests.riskmgr.barclock_fakes import T1, BcStore, arm_row, template


class SpyNotices:
    def __init__(self) -> None:
        self.seen: list[tuple[str, ...]] = []

    @property
    def name(self) -> str:
        return "notice"

    async def on_decisions(self, tx: ArmStateTxn, event: ArmEvent, bar_end_ms: int,
                           decisions: Sequence[Written]) -> tuple[Written, ...]:
        self.seen.append(tuple(w.item.ref for w in decisions))
        return tuple(decisions)


def item(pid: str, *, seq: int = 1, decision: bool = True) -> Decided:
    row = arm_row(pid)
    d = replace(template(), key=row.key, bar_end_ms=T1, bar_spec="15m", decision_seq=seq) if decision else None
    return Decided(ref=row.ref, decision=d, position=PositionCas(row=replace(row, decision_seq=seq),
                                                                 expected_row_version=0))


def test_the_sink_order_is_fixed() -> None:
    assert ARM_SINKS == ("decision", "notice")
    assert ARM_SINKS == PHASE1_SINKS                          # the scanner's copy (sinks cannot import the scanner)
    assert default_sinks().names == ARM_SINKS
    with pytest.raises(ValueError):
        SinkChain((DeferredNoticeSink(), DecisionSink()))
    with pytest.raises(ValueError):
        SinkChain((DecisionSink(),))
    with pytest.raises(ValueError):
        Decided(ref="x", decision=None, position=None)


async def test_one_savepoint_per_position_and_notices_see_only_inserted() -> None:
    store = BcStore(arm_row("P1"), arm_row("P2"), arm_row("P3"))
    store.raise_once.add("P2")
    store.decisions[(arm_row("P3").key, "15m", "POST", T1, 0)] = template()      # P3 decided already
    spy = SpyNotices()
    chain = SinkChain((DecisionSink(), spy))
    async with store.transaction() as tx:
        out = await chain.run(tx, ArmEvent.POST, T1,  # type: ignore[arg-type]
                              [item("P1"), item("P2"), item("P3"), item("P4", decision=False)])
    assert [w.status for w in out] == [WriteStatus.INSERTED, WriteStatus.WRITE_ERROR, WriteStatus.ALREADY_DECIDED,
                                       WriteStatus.CAS_MISS]
    assert out[0].ref is not None
    assert spy.seen == [("mock:P1",)]


async def test_a_fatal_error_propagates() -> None:
    store = BcStore(arm_row("P1"))
    store.raise_once.add("P1")
    chain = SinkChain((DecisionSink(fatal=lambda e: isinstance(e, ConnectionError)), DeferredNoticeSink()))
    with pytest.raises(ConnectionError):
        async with store.transaction() as tx:
            await chain.run(tx, ArmEvent.POST, T1, [item("P1")])  # type: ignore[arg-type]


async def test_a_lost_fence_propagates_by_default() -> None:
    store = BcStore(arm_row("P1"))
    store.raise_with["P1"] = EpochMismatch("engine_epoch moved")
    with pytest.raises(EpochMismatch):
        async with store.transaction() as tx:
            await default_sinks().run(tx, ArmEvent.POST, T1, [item("P1")])  # type: ignore[arg-type]
