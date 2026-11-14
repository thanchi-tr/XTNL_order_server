"""CommitBarrier: holds a reconcile pass's gap-clear until its REST rows are committed (or failed).

oversee-rules.md §2 / tier-persistence.md §4: `stream_health.conn_lost_at` (the account fence) is cleared
only once the stream is SUBSCRIBED **and its SNAPSHOTTING / RESYNCING reconcile has committed**. The
orchestrator publishes the pass's rows with put_nowait and never awaits the consumers, so it registers
the rows it handed over (by seq_id, which the HLC makes unique) and a callback; each topic consumer
settles a row after its commit returns (ok) or after it parked it (timeout / error). When the last row
of the pass settles, the callback receives the streams whose rows failed (empty = all committed).

The consumers run while the pass is still awaiting its by-id re-reads, so a row can commit before the
pass has finished publishing. The pass is therefore opened (`open`) before its first row is published,
each row is added (`add`) synchronously right after its put_nowait (no await in between, so no consumer
can settle it first), and the pass is sealed (`seal`) once publishing is done. A row settled while the
pass is open is simply removed; the callback fires when the pass is sealed and nothing is pending.

A newer pass for the same key (adapter) supersedes the pending one when it is sealed: the old callback
never fires, and the newer pass, which read newer broker state, clears the gap once its own rows commit.
A pass that is opened but never sealed (the REST pass raised) supersedes nothing and never fires.

Refusals (IMPLEMENTATION_PLAN 7.1): a consumer whose commit the store refused (an illegal transition against
the stored row, parked by the store) settles the row as failed and reports it with `refused(item, stored_status)`;
every listener added with `add_refusal_listener` (the orchestrator) is told, so a refused status is not kept in
its memory.

Everything runs on the event loop: track / settle / refused are synchronous and never await.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from src.ports import StreamKind

log = logging.getLogger(__name__)

PassDone = Callable[[frozenset[StreamKind]], None]
RefusalListener = Callable[[object, "str | None"], None]


@dataclass(eq=False)
class _Pass:
    key: str
    pending: dict[int, StreamKind] = field(default_factory=dict)
    on_done: PassDone | None = None        # set by seal()
    failed: set[StreamKind] = field(default_factory=set)


class CommitBarrier:
    def __init__(self) -> None:
        self._by_seq: dict[int, _Pass] = {}
        self._by_key: dict[str, _Pass] = {}    # sealed, still-pending pass per key
        self._refusal_listeners: list[RefusalListener] = []

    def add_refusal_listener(self, listener: RefusalListener) -> None:
        """`listener(item, stored_status)` runs for every row the store refused (see `refused`)."""
        self._refusal_listeners.append(listener)

    def refused(self, item: object, stored_status: str | None) -> None:
        """A consumer's commit of `item` was refused by the store (the stored row keeps `stored_status`).
        Settle the row separately (`settle(seq, False)`); this only informs the listeners. Never raises."""
        for listener in list(self._refusal_listeners):
            try:
                listener(item, stored_status)
            except Exception:   # runs inside a consumer's process(): never kill the consumer task
                log.exception("commit barrier: refusal listener failed")

    def open(self, key: str) -> _Pass:
        """Start a pass before its first row is published. It supersedes nothing until sealed."""
        return _Pass(key)

    def add(self, p: _Pass, seq_id: int, stream: StreamKind) -> None:
        """Register one published row (call synchronously right after its put_nowait)."""
        if p.on_done is not None:
            raise RuntimeError("CommitBarrier.add after seal")
        p.pending[seq_id] = stream
        self._by_seq[seq_id] = p

    def seal(self, p: _Pass, on_done: PassDone) -> None:
        """Publishing is done: supersede the key's pending pass; `on_done` fires once every row settled
        (at once if they all settled already)."""
        old = self._by_key.get(p.key)
        if old is not None and old is not p:
            self.cancel(p.key)
        p.on_done = on_done
        if not p.pending:
            on_done(frozenset(p.failed))
            return
        self._by_key[p.key] = p

    def track(self, key: str, rows: Iterable[tuple[int, StreamKind]], on_done: PassDone) -> None:
        """Register an already-published pass in one call (open + add + seal). Only safe when no await
        separated the publishes from this call. No rows: `on_done` runs at once."""
        p = self.open(key)
        for seq, stream in rows:
            self.add(p, seq, stream)
        self.seal(p, on_done)

    def cancel(self, key: str) -> None:
        """Drop the pending (sealed) pass of `key` (its callback never fires)."""
        p = self._by_key.pop(key, None)
        if p is not None:
            for seq in p.pending:
                self._by_seq.pop(seq, None)
            p.pending.clear()

    def settle(self, seq_id: int, ok: bool) -> None:
        """A consumer finished the row with this seq_id: committed (ok) or parked / failed (not ok).
        Rows that belong to no pass are ignored (one dict miss)."""
        p = self._by_seq.pop(seq_id, None)
        if p is None:
            return
        stream = p.pending.pop(seq_id)
        if not ok:
            p.failed.add(stream)
        if not p.pending and p.on_done is not None:
            if self._by_key.get(p.key) is p:
                del self._by_key[p.key]
            try:
                p.on_done(frozenset(p.failed))
            except Exception:   # runs inside a consumer's process(): never kill the consumer task
                log.exception("commit barrier: reconcile callback for %s failed; its gap stays open", p.key)

    def pending(self, key: str) -> int:
        """Rows of `key`'s pass not yet settled (0 when nothing is pending)."""
        p = self._by_key.get(key)
        return 0 if p is None else len(p.pending)


BarrierPass = _Pass

__all__ = ["BarrierPass", "CommitBarrier", "PassDone", "RefusalListener"]
