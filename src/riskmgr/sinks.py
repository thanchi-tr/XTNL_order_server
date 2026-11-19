"""Decision sinks (design §B3 Sinks, §B4, §B5.3, §B10; plan S27).

`Sink.on_decisions(tx, event, bar_end_ms, decisions)` runs inside the BarClock's one fenced transaction per (event,
bar end), after every market-data read and evaluation has finished (no I/O but the transaction's own statements).
The Phase 1 set is fixed: `ARM_SINKS` = (decision sink, notice sink), in that order; a Supabase document can never
name a sink (validation refuses the key) and `SinkChain` refuses any other set or order. Each sink receives what the
previous one returned, so the notice sink sees only the decisions the decision sink inserted.

* **Decision sink:** each position's decision row, its handle rows and its `row_version` CAS through
  `ArmStateTxn.record_decision`, which runs them in **their own savepoint** (a CAS miss or a transient error rolls back
  only that position); a position change with no row (the `pending_event` bookkeeping) is the CAS alone, through
  `ArmStateTxn.update_position`, which has its own savepoint too. A transient
  error is recorded as `WRITE_ERROR` for that position (the others still commit; it is retried on a later pass);
  a fatal one (a lost fence, `EpochMismatch`; by default `src.db.arm_ports.is_lost_fence`) propagates, so the caller
  hard-exits.
* **Notice sink:** `src.riskmgr.notices.NoticeSink` (S28): in the same transaction, the notice work of every route
  with a newly inserted decision ("decisions of this route with `notice_id` IS NULL", attached, suppressed or left for
  the end-of-window part). It keeps its in-memory notice state (part counters, closed windows, taken extras) staged
  per transaction until `SinkChain.settle` says the transaction committed; a rollback discards the stage and puts
  the taken extras back. `DeferredNoticeSink` (the default chain's, until the runtime wiring of S31 passes the
  notice sink) passes the decisions through with `notice_id` NULL, which the notices' timer pass then reads, so
  nothing is lost.

A future `StopMoveSink` (Phase 2) lives in its own package (§B10). Rows hold ids, codes, times and order state only.
"""
from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Final, Protocol, runtime_checkable

from src.db.arm_ports import (
    ArmStateTxn,
    DecisionHandleRow,
    DecisionRef,
    DecisionRow,
    DecisionWriteStatus,
    PositionCas,
    is_lost_fence,
)
from src.riskmgr.types import ArmEvent

DECISION_SINK: Final = "decision"
NOTICE_SINK: Final = "notice"
ARM_SINKS: Final = (DECISION_SINK, NOTICE_SINK)
"""The fixed Phase 1 sink order (§B3). It equals `src.riskmgr.lockout.PHASE1_SINKS`, which this module cannot import
(no scoped module imports the scanner); `tests/riskmgr/test_sinks.py` keeps the two equal."""


class WriteStatus(str, Enum):
    INSERTED = "INSERTED"
    ALREADY_DECIDED = "ALREADY_DECIDED"
    CAS_MISS = "CAS_MISS"
    UPDATED = "UPDATED"
    """A position change with no decision row committed."""
    WRITE_ERROR = "WRITE_ERROR"
    """A transient error inside the position's savepoint: rolled back, retried on a later pass."""


@dataclass(frozen=True, kw_only=True)
class Decided:
    """One position's write in one (event, bar end) transaction: a decision row with its handle rows and position
    CAS, or (`decision` None) the position CAS alone."""

    ref: str
    decision: DecisionRow | None
    position: PositionCas | None
    handles: tuple[DecisionHandleRow, ...] = ()
    shown: bool = True
    """False = the evaluation marked the decision silent (a released position, a repeated "at target, not yet
    confirmed", §B6.3): the notice sink gives it the `suppressed` sentinel (S28)."""
    on_commit: Callable[[], None] | None = field(default=None, repr=False, compare=False)
    """Run by the BarClock on the event loop once this decision row has committed (the evaluator's memory, which a
    job thread never changes: S36b review); never for a row that missed its CAS or rolled back."""

    def __post_init__(self) -> None:
        if self.decision is None and self.position is None:
            raise ValueError("a write holds a decision row, a position change or both")


@dataclass(frozen=True)
class Written:
    """A `Decided` after a sink: the write status and, once inserted, the decision's reference."""

    item: Decided
    status: WriteStatus | None = None
    ref: DecisionRef | None = None


class Sink(Protocol):
    @property
    def name(self) -> str: ...

    async def on_decisions(self, tx: ArmStateTxn, event: ArmEvent, bar_end_ms: int,
                           decisions: Sequence[Written]) -> tuple[Written, ...]:
        """Act on the decisions of one (event, bar end) inside the transaction; return what the next sink sees."""
        ...


@runtime_checkable
class SettlingSink(Protocol):
    def settle(self, tx: ArmStateTxn, *, committed: bool) -> None:
        """After the transaction ended: apply (committed) or discard what the sink staged for `tx`."""
        ...


_STATUS: Final = {
    DecisionWriteStatus.INSERTED: WriteStatus.INSERTED,
    DecisionWriteStatus.ALREADY_DECIDED: WriteStatus.ALREADY_DECIDED,
    DecisionWriteStatus.CAS_MISS: WriteStatus.CAS_MISS,
}


class DecisionSink:
    """Writes each position's row and CAS in its own savepoint (see the module docstring)."""

    def __init__(self, *, fatal: Callable[[BaseException], bool] | None = None) -> None:
        self._fatal = fatal or is_lost_fence

    @property
    def name(self) -> str:
        return DECISION_SINK

    async def on_decisions(self, tx: ArmStateTxn, event: ArmEvent, bar_end_ms: int,
                           decisions: Sequence[Written]) -> tuple[Written, ...]:
        out: list[Written] = []
        for w in decisions:
            d = w.item
            try:
                if d.decision is None:
                    assert d.position is not None
                    ok = await tx.update_position(d.position)
                    out.append(replace(w, status=WriteStatus.UPDATED if ok else WriteStatus.CAS_MISS))
                    continue
                result = await tx.record_decision(d.decision, d.handles, position=d.position)
            except Exception as e:
                if self._fatal(e):
                    raise
                out.append(replace(w, status=WriteStatus.WRITE_ERROR))
                continue
            out.append(replace(w, status=_STATUS[result.status], ref=result.ref))
        return tuple(out)


class DeferredNoticeSink:
    """A pass-through notice slot: the inserted decisions keep `notice_id` NULL (the notices' timer pass reads them).
    The runtime wiring (S31) passes `ArmNotices.sink()` instead."""

    @property
    def name(self) -> str:
        return NOTICE_SINK

    async def on_decisions(self, tx: ArmStateTxn, event: ArmEvent, bar_end_ms: int,
                           decisions: Sequence[Written]) -> tuple[Written, ...]:
        return tuple(decisions)


class SinkChain:
    """The fixed `ARM_SINKS` chain; refuses any other set or order."""

    def __init__(self, sinks: Sequence[Sink]) -> None:
        names = tuple(s.name for s in sinks)
        if names != ARM_SINKS:
            raise ValueError(f"the ARM sinks are fixed as {ARM_SINKS}")
        self._sinks = tuple(sinks)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(s.name for s in self._sinks)

    async def run(self, tx: ArmStateTxn, event: ArmEvent, bar_end_ms: int,
                  decisions: Sequence[Decided]) -> tuple[Written, ...]:
        """The decision sink sees every write; each later sink sees only the inserted decisions; the result keeps
        every write with its status."""
        written = await self._sinks[0].on_decisions(tx, event, bar_end_ms, tuple(Written(d) for d in decisions))
        inserted = tuple(w for w in written if w.status is WriteStatus.INSERTED)
        for sink in self._sinks[1:]:
            inserted = await sink.on_decisions(tx, event, bar_end_ms, inserted)
        return written

    def settle(self, tx: ArmStateTxn | None, *, committed: bool) -> None:
        """Called by the transaction's owner once it has ended: each sink that keeps per-transaction memory applies
        it (committed) or discards it (rolled back). `tx` None = the transaction never opened."""
        if tx is None:
            return
        for sink in self._sinks:
            if isinstance(sink, SettlingSink):
                sink.settle(tx, committed=committed)


def default_sinks(*, fatal: Callable[[BaseException], bool] | None = None) -> SinkChain:
    """The Phase 1 chain with the pass-through notice sink; the runtime (S31) builds the chain with
    `ArmNotices.sink()` as the second element."""
    return SinkChain((DecisionSink(fatal=fatal), DeferredNoticeSink()))


__all__ = [
    "ARM_SINKS", "DECISION_SINK", "Decided", "DecisionSink", "DeferredNoticeSink", "NOTICE_SINK", "SettlingSink",
    "Sink", "SinkChain", "WriteStatus", "Written", "default_sinks",
]
