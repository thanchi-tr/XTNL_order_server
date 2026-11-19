"""In-memory Oversee store for tests and local runs: implements SnapshotReader, SweepWriter, HaltStore,
OutboxStore, DedupeStore and the halt:state mirror with the same CAS / epoch / lock semantics the
PostgreSQL and Redis implementations must provide (oversee-rules.md §2, §8; tier-persistence.md §4)."""
from __future__ import annotations

import asyncio
import copy
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field, fields, replace
from datetime import date
from types import MappingProxyType

from src.oversee.dayroll import trading_day
from src.oversee.halt import HaltEvent, HaltTxn, NewHaltEvent
from src.oversee.notify import ARM_OUTBOX_KIND, EpisodeKey, OutboxRow
from src.oversee.snapshot import AccountSnapshot, BreachAck, DependencyUnavailable, SodMark, StopWatchKey
from src.oversee.sweep import AccountWrite, CasConflict, EpochMismatch
from src.oversee.types import BreachState, HaltLevel, StateKey, Transition


@dataclass
class _Tables:
    breaches: dict[StateKey, BreachState] = field(default_factory=dict)
    stop_watch: dict[StopWatchKey, int] = field(default_factory=dict)
    events: dict[tuple[str, str, str, str, int], Transition] = field(default_factory=dict)
    outbox: list[OutboxRow] = field(default_factory=list)
    outbox_sent: dict[int, tuple[int, bool]] = field(default_factory=dict)
    sod_marks: dict[tuple[str, date], SodMark] = field(default_factory=dict)
    halt_events: list[HaltEvent] = field(default_factory=list)
    acks: list[BreachAck] = field(default_factory=list)
    dedupe: dict[EpisodeKey, int] = field(default_factory=dict)
    quarantine: dict[str, int] = field(default_factory=dict)


class InMemoryOverseeDb:
    def __init__(self, engine_epoch: int = 1) -> None:
        self.t = _Tables()
        self.engine_epoch = engine_epoch
        self.base: dict[str, AccountSnapshot] = {}
        self.failing_accounts: set[str] = set()
        self.dependency_down = False
        self.halt_state: tuple[HaltLevel, int] | None = None   # Redis halt:state mirror
        self.writes = 0
        self.dedupe_reads: list[tuple[EpisodeKey, ...]] = []    # keys of each last_sent_of call (last 100)
        self._halt_lock = asyncio.Lock()

    # -- SnapshotReader ---------------------------------------------------------------------
    def set_snapshot(self, snap: AccountSnapshot) -> None:
        self.base[snap.account_id] = snap

    async def list_accounts(self) -> Sequence[str]:
        if self.dependency_down:
            raise DependencyUnavailable("postgres down")
        return sorted(self.base)

    async def read_account(self, account_id: str, now_ms: int) -> AccountSnapshot:
        if self.dependency_down:
            raise DependencyUnavailable("postgres down")
        if account_id in self.failing_accounts:
            raise RuntimeError(f"snapshot failed for {account_id}")
        base = self.base[account_id]
        sod = base.sod or self.t.sod_marks.get((account_id, trading_day(now_ms)))
        return replace(
            base, read_at_ms=now_ms, sod=sod, breaches=self._breaches_of(base, now_ms),
            stop_watch=MappingProxyType(dict(self.t.stop_watch) | dict(base.stop_watch)),
            acks=tuple(self.t.acks),
        )

    def _breaches_of(self, base: AccountSnapshot, now_ms: int) -> tuple[BreachState, ...]:
        """The rows PgSnapshotReader loads (src/db/snapshot_reader.py SQL_BREACHES + SQL_LATEST_DROP):
        unresolved rows, resolved rows of a live subject, and the resolved row that dropped below SOFT
        last (the cool-down mark of a state whose entity has gone)."""
        acct = base.account_id
        live = {acct, f"{acct}:{trading_day(now_ms).isoformat()}"}
        live |= {o.entity_key for o in base.orders if not o.status.terminal}
        live |= {p.entity_key for p in base.positions if p.status == "OPEN"}
        rows = [s for s in self.t.breaches.values() if s.account_id == acct]
        out = [s for s in rows if s.resolved_at is None or s.subject_id in live]
        drops = [s for s in rows if s.resolved_at is not None and s.below_soft_since is not None]
        latest = max(drops, key=lambda s: s.below_soft_since or 0, default=None)
        if latest is not None and all(s.key != latest.key for s in out):
            out.append(latest)
        return tuple(out)

    # -- SweepWriter -------------------------------------------------------------------------
    async def write_account(self, batch: AccountWrite) -> None:
        if self.dependency_down:
            raise DependencyUnavailable("postgres down")
        if batch.epoch != self.engine_epoch:
            raise EpochMismatch(f"{batch.epoch} != {self.engine_epoch}")
        for w in batch.states:   # all CAS checks before any write: the transaction is atomic
            stored = self.t.breaches.get(w.state.key)
            if stored is None:
                if w.expected_sweep_id is not None:
                    raise CasConflict(f"{w.state.key} vanished")
            elif (stored.last_sweep_id, stored.clear_generation) != (w.expected_sweep_id, w.expected_generation):
                raise CasConflict(f"{w.state.key} moved")
        for w in batch.states:
            self.t.breaches[w.state.key] = w.state
        for key, since in batch.stop_watch_set:
            self.t.stop_watch.setdefault(key, since)
        for key in batch.stop_watch_clear:
            self.t.stop_watch.pop(key, None)
        for ev in batch.events:
            self.t.events.setdefault(ev.event_key, ev)
        self._add_outbox(batch.outbox)
        if batch.sod_mark is not None:
            self.t.sod_marks.setdefault((batch.account_id, batch.sod_mark.trading_day), batch.sod_mark)
        self.writes += 1

    async def set_quarantine(self, account_id: str, at_ms: int | None, reason: str) -> None:
        if at_ms is None:
            self.t.quarantine.pop(account_id, None)
        else:
            self.t.quarantine[account_id] = at_ms

    # -- HaltStore ---------------------------------------------------------------------------
    @asynccontextmanager
    async def _txn(self) -> AsyncIterator[HaltTxn]:
        async with self._halt_lock:
            if self.dependency_down:
                raise DependencyUnavailable("postgres down")
            saved = _Tables(**{f.name: copy.copy(getattr(self.t, f.name)) for f in fields(_Tables)})
            try:
                yield _MemHaltTxn(self)
            except BaseException:
                self.t = saved
                raise

    def transaction(self) -> AbstractAsyncContextManager[HaltTxn]:
        """HALT_LOCK: one transaction at a time; rolled back on any exception."""
        return self._txn()

    # -- halt:state mirror -------------------------------------------------------------------
    async def cas_set(self, level: HaltLevel, event_id: int) -> bool:
        if self.halt_state is not None and self.halt_state[1] >= event_id:
            return False
        self.halt_state = (level, event_id)
        return True

    # -- OutboxStore / DedupeStore -----------------------------------------------------------
    def _add_outbox(self, rows: Sequence[OutboxRow]) -> None:
        for row in rows:
            self.t.outbox.append(replace(row, outbox_id=len(self.t.outbox) + 1))

    async def add(self, rows: Sequence[OutboxRow]) -> None:
        self._add_outbox(rows)

    async def pending(self, limit: int) -> list[OutboxRow]:
        """Unsent rows, oldest first; never an ARM notice (kind `arm`, PR4), like the PostgreSQL store."""
        rows = [r for r in self.t.outbox if r.outbox_id is not None and r.outbox_id not in self.t.outbox_sent
                and str(r.kind) != ARM_OUTBOX_KIND]
        return rows[:limit]

    async def mark_sent(self, outbox_id: int, at_ms: int, delivered: bool) -> None:
        self.t.outbox_sent[outbox_id] = (at_ms, delivered)

    async def last_sent(self, key: EpisodeKey) -> int | None:
        return self.t.dedupe.get(key)

    async def record(self, key: EpisodeKey, at_ms: int) -> None:
        self.t.dedupe[key] = at_ms

    async def snapshot(self) -> Mapping[EpisodeKey, int]:
        return MappingProxyType(dict(self.t.dedupe))

    async def last_sent_of(self, keys: Sequence[EpisodeKey]) -> Mapping[EpisodeKey, int]:
        self.dedupe_reads.append(tuple(keys))
        del self.dedupe_reads[:-100]
        return MappingProxyType({k: self.t.dedupe[k] for k in keys if k in self.t.dedupe})


class _MemHaltTxn:
    def __init__(self, db: InMemoryOverseeDb) -> None:
        self._db = db

    async def latest_event(self) -> HaltEvent | None:
        return self._db.t.halt_events[-1] if self._db.t.halt_events else None

    async def level_rose_at_ms(self) -> int | None:
        prev, rose = HaltLevel.NONE, None
        for ev in self._db.t.halt_events:
            if ev.effective_level > prev:
                rose = ev.at_ms
            prev = ev.effective_level
        return rose

    async def latest_operator_clear_ms(self) -> int | None:
        times = [ev.at_ms for ev in self._db.t.halt_events if ev.by != "system"]
        return max(times) if times else None

    async def insert_event(self, ev: NewHaltEvent) -> HaltEvent:
        row = HaltEvent(len(self._db.t.halt_events) + 1, ev.level, ev.effective_level, ev.hard_latch, ev.rule_id,
                        ev.reason, ev.by, ev.at_ms)
        self._db.t.halt_events.append(row)
        return row

    async def breach_states(self) -> list[BreachState]:
        return list(self._db.t.breaches.values())

    async def save_states(self, states: Sequence[BreachState]) -> None:
        for s in states:
            self._db.t.breaches[s.key] = s

    async def insert_ack(self, operator: str, why: str, state: BreachState, at_ms: int) -> BreachAck:
        ack = BreachAck(len(self._db.t.acks) + 1, state.rule_id, state.subject_type, state.subject_id, state.level,
                        state.cap, state.reason, operator, why, at_ms)
        self._db.t.acks.append(ack)
        return ack

    async def acks(self, keys: Sequence[StateKey]) -> list[BreachAck]:
        live = {k for k in keys if k in self._db.t.breaches and self._db.t.breaches[k].resolved_at is None}
        return [a for a in self._db.t.acks if (a.rule_id, a.subject_type, a.subject_id) in live]

    async def insert_outbox(self, rows: Sequence[OutboxRow]) -> None:
        self._db._add_outbox(rows)

    async def insert_events(self, events: Sequence[Transition]) -> None:
        for ev in events:
            self._db.t.events.setdefault(ev.event_key, ev)
