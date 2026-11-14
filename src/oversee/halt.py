"""HaltSwitch: trip / system clear / operator clear / acknowledge (oversee-rules.md §4, §8).

PostgreSQL `halt_events` is the truth. Every call runs inside one `HaltStore.transaction()`, which
is `pg_advisory_xact_lock(HALT_LOCK)` plus the epoch check (operations.md §2); the rise check and
`effective_level` are computed inside it. Redis `halt:state` is mirrored afterwards by CAS only.
"""
from __future__ import annotations

from collections.abc import Callable, Sequence, Set as AbstractSet
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, replace
from typing import Literal, Protocol

from src.oversee.notify import OutboxRow, alerts_for_transitions
from src.oversee.snapshot import BreachAck
from src.oversee.types import (
    EVALUATION_RULE_ID,
    BreachState,
    HaltLevel,
    Level,
    ResolvedBy,
    StateKey,
    Transition,
)

SYSTEM = "system"


@dataclass(frozen=True)
class HaltEvent:
    """A `halt_events` row."""

    id: int
    level: HaltLevel
    effective_level: HaltLevel
    hard_latch: bool
    rule_id: str | None
    reason: str
    by: str
    at_ms: int


@dataclass(frozen=True)
class NewHaltEvent:
    level: HaltLevel
    effective_level: HaltLevel
    hard_latch: bool
    rule_id: str | None
    reason: str
    by: str
    at_ms: int


@dataclass(frozen=True)
class HaltStatus:
    effective_level: HaltLevel
    hard_latch: bool
    event_id: int | None
    rose_at_ms: int | None


class HaltTxn(Protocol):
    """Statements available inside HALT_LOCK."""

    async def latest_event(self) -> HaltEvent | None: ...

    async def level_rose_at_ms(self) -> int | None: ...

    async def latest_operator_clear_ms(self) -> int | None: ...

    async def insert_event(self, ev: NewHaltEvent) -> HaltEvent: ...

    async def breach_states(self) -> list[BreachState]: ...

    async def save_states(self, states: Sequence[BreachState]) -> None: ...

    async def insert_ack(self, operator: str, why: str, state: BreachState, at_ms: int) -> BreachAck: ...

    async def acks(self, keys: Sequence[StateKey]) -> list[BreachAck]:
        """The breach_acks rows of the unresolved halt_breaches rows with these keys (oldest first)."""
        ...

    async def insert_outbox(self, rows: Sequence[OutboxRow]) -> None: ...

    async def insert_events(self, events: Sequence[Transition]) -> None:
        """oversee_events rows, idempotent on (rule_id, subject_type, subject_id, transition, sweep_id)."""
        ...


class HaltStore(Protocol):
    def transaction(self) -> AbstractAsyncContextManager[HaltTxn]:
        """HALT_LOCK + epoch-checked READ COMMITTED transaction; commits on clean exit."""
        ...


class HaltMirror(Protocol):
    async def cas_set(self, level: HaltLevel, event_id: int) -> bool:
        """Redis halt:state CAS script: writes only when event_id is newer than the stored one."""
        ...


@dataclass(frozen=True)
class ClearOutcome:
    status: Literal["cleared", "refused"]
    reason: str
    event: HaltEvent | None = None
    resolved: tuple[StateKey, ...] = ()


def sweep_target(states: Sequence[BreachState]) -> HaltLevel:
    """SOFT if any active SOFT state, HARD if any active HARD state, else NONE (latch excluded)."""
    active = [s.level for s in states if s.active]
    if any(lv >= Level.HARD_HALT for lv in active):
        return HaltLevel.HARD
    if any(lv >= Level.SOFT_HALT for lv in active):
        return HaltLevel.SOFT
    return HaltLevel.NONE


def _status(ev: HaltEvent | None, rose_at: int | None) -> HaltStatus:
    if ev is None:
        return HaltStatus(HaltLevel.NONE, False, None, rose_at)
    return HaltStatus(ev.effective_level, ev.hard_latch, ev.id, rose_at)


class HaltSwitch:
    def __init__(self, store: HaltStore, mirror: HaltMirror | None = None) -> None:
        self._store = store
        self._mirror = mirror

    async def _mirror_event(self, ev: HaltEvent | None) -> None:
        if ev is None or self._mirror is None:
            return
        try:
            await self._mirror.cas_set(ev.effective_level, ev.id)
        except Exception:
            pass  # the gate denies while halt:state != MAX(halt_events.id)

    async def status(self) -> HaltStatus:
        async with self._store.transaction() as tx:
            return _status(await tx.latest_event(), await tx.level_rose_at_ms())

    async def trip(self, level: HaltLevel, *, rule_id: str | None, reason: str, now_ms: int,
                   basis_ms: int | None = None) -> HaltEvent | None:
        """Write a row and alert only when the level rises (§8). Every HARD trip sets the latch.
        Refused (None) when an operator clear is newer than `basis_ms`, the read time of the states
        that led to the trip, so a decision from before the clear can't re-trip."""
        async with self._store.transaction() as tx:
            if basis_ms is not None:
                cleared = await tx.latest_operator_clear_ms()
                if cleared is not None and cleared >= basis_ms:
                    return None
            latest = await tx.latest_event()
            current = latest.effective_level if latest else HaltLevel.NONE
            latch = latest.hard_latch if latest else False
            if level <= current:
                return None
            ev = await tx.insert_event(NewHaltEvent(level, level, latch or level is HaltLevel.HARD, rule_id,
                                                    reason, SYSTEM, now_ms))
            await tx.insert_outbox([OutboxRow("trip", "critical" if level is HaltLevel.HARD else "high",
                                              f"HALT {level.name}", f"{rule_id or '-'}: {reason}", now_ms)])
        await self._mirror_event(ev)
        return ev

    async def system_clear(self, *, reason: str, now_ms: int) -> HaltEvent | None:
        """RiskSweep cool-down: SOFT -> NONE, never while the HARD latch is set."""
        async with self._store.transaction() as tx:
            latest = await tx.latest_event()
            if latest is None or latest.hard_latch or latest.effective_level is not HaltLevel.SOFT:
                return None
            if sweep_target(await tx.breach_states()) is not HaltLevel.NONE:
                return None
            ev = await tx.insert_event(NewHaltEvent(HaltLevel.NONE, HaltLevel.NONE, False, None, reason,
                                                    SYSTEM, now_ms))
            await tx.insert_outbox([OutboxRow("info", "info", "HALT cleared", reason, now_ms)])
        await self._mirror_event(ev)
        return ev

    async def operator_clear(self, operator: str, why: str, reviewed_event_id: int | None, *,
                             force: bool = False, now_ms: int) -> ClearOutcome:
        """Release the HARD latch (§4). Refused on a newer halt_events row or an active HARD state,
        unless `force` and that state's latest results are capped or UNKNOWN; a forced clear needs a
        written reason and resolves that state. Resets every unresolved state's uncapped_ms and
        bumps its clear_generation."""
        if not operator:
            return ClearOutcome("refused", "operator_required")
        async with self._store.transaction() as tx:
            latest = await tx.latest_event()
            latest_id = latest.id if latest else None
            if latest_id != reviewed_event_id:
                return ClearOutcome("refused", f"newer_halt_event:{latest_id}")
            states = await tx.breach_states()
            hard = [s for s in states if s.active and s.level >= Level.HARD_HALT]
            if hard and not force:
                return ClearOutcome("refused", "active_hard_state:" + ";".join("/".join(s.key) for s in hard))
            if hard and not why.strip():
                return ClearOutcome("refused", "force_requires_reason")
            uncapped = [s for s in hard if not s.capped_or_unknown]
            if uncapped:
                return ClearOutcome("refused", "hard_state_uncapped:" + ";".join("/".join(s.key) for s in uncapped))
            hard_keys = {s.key for s in hard}
            updated: list[BreachState] = []
            for s in states:
                if s.key in hard_keys:
                    s = replace(s, level=Level.PASS, resolved_at=now_ms, resolved_by="operator",
                                below_soft_since=now_ms, pending_level=None, pending_since=None,
                                release_pending_since=None)
                elif s.resolved_at is not None:
                    continue
                updated.append(replace(s, uncapped_ms=0, clear_generation=s.clear_generation + 1))
            await tx.save_states(updated)
            after = {s.key: s for s in states} | {s.key: s for s in updated}
            target = sweep_target(list(after.values()))
            current = latest.effective_level if latest else HaltLevel.NONE
            ev: HaltEvent | None = None
            if latest is None or target != current or latest.hard_latch:
                ev = await tx.insert_event(NewHaltEvent(target, target, False, None, why or "operator clear",
                                                        operator, now_ms))
                await tx.insert_outbox([OutboxRow("info", "high", f"HALT cleared by {operator}",
                                                  f"{why} -> {target.name}", now_ms)])
        await self._mirror_event(ev)
        return ClearOutcome("cleared", "ok", ev, tuple(sorted(hard_keys)))

    async def resolve_unlisted(self, listed: AbstractSet[str], *, now_ms: int, sweep_id: int
                               ) -> tuple[BreachState, ...]:
        """Resolve every active rule state of an account that is no longer listed for the sweep (no
        account_state row, open position or live order): its entities are gone, so nothing will evaluate or
        resolve those states again, and they would hold the level forever (§2 Release: an entity gone from
        the snapshot resolves). Called by the RiskSweep. Under HALT_LOCK the current rows are read, so no
        CAS is needed; clear_generation is bumped so any in-flight write built from an older read fails."""
        async with self._store.transaction() as tx:
            gone = [s for s in await tx.breach_states()
                    if s.active and s.resolved_at is None and s.account_id not in listed]
            if not gone:
                return ()
            updated: list[BreachState] = []
            events: list[Transition] = []
            for s in sorted(gone, key=lambda b: b.key):
                by: ResolvedBy = "sweep" if s.rule_id == EVALUATION_RULE_ID else "entity_end"
                updated.append(replace(
                    s, level=Level.PASS, pending_level=None, pending_since=None, release_pending_since=None,
                    unknown_since=None, unknown_origin=False, resolved_at=now_ms, resolved_by=by,
                    below_soft_since=now_ms if s.level >= Level.SOFT_HALT else s.below_soft_since,
                    last_pass_ms=now_ms, last_sweep_id=sweep_id, clear_generation=s.clear_generation + 1))
                events.append(Transition(s.rule_id, s.subject_type, s.subject_id, s.account_id, "resolved", sweep_id,
                                         now_ms, Level.PASS, s.cap, "account_unlisted", s.activation_id))
            await tx.save_states(updated)
            await tx.insert_events(events)
            await tx.insert_outbox(alerts_for_transitions(events))
        return tuple(updated)

    async def upsert_state(self, key: StateKey,
                           build: Callable[[BreachState | None], tuple[BreachState, Sequence[Transition]] | None]
                           ) -> BreachState | None:
        """Write one rule state over the row read under HALT_LOCK (the RiskSweep's retry after a lost
        compare-and-set: the operator clear() and resolve_unlisted() write halt_breaches only under this lock,
        so the row read here can't move before the write). `build(current unresolved row or None)` returns
        the row and its transitions, or None to keep the current row. clear_generation is bumped, like every
        locked write, so any write built from an older read fails its CAS. The transitions get
        oversee_events rows only (no outbox rows: the caller pages). Returns the stored row."""
        async with self._store.transaction() as tx:
            current = next((s for s in await tx.breach_states() if s.key == key and s.resolved_at is None), None)
            built = build(current)
            if built is None:
                return current
            row, events = built
            generation = max(row.clear_generation, current.clear_generation if current is not None else 0)
            row = replace(row, clear_generation=generation + 1)
            await tx.save_states([row])
            if events:
                await tx.insert_events(events)   # no outbox rows: the caller pages (evaluation_failed)
        return row

    async def acks_for(self, keys: Sequence[StateKey]) -> list[BreachAck]:
        """The acknowledgements of these unresolved rule states (the RiskSweep checks them before re-paging a
        state it holds outside a snapshot, such as a failing account's `evaluation_failed`, §4 acknowledge)."""
        if not keys:
            return []
        async with self._store.transaction() as tx:
            return await tx.acks(keys)

    async def acknowledge(self, operator: str, why: str, key: StateKey, now_ms: int) -> BreachAck:
        """Silence a state's re-pages until its level, cap or reason changes; changes no state (§4)."""
        async with self._store.transaction() as tx:
            state = next((s for s in await tx.breach_states() if s.key == key and s.active), None)
            if state is None:
                raise LookupError(f"no active rule state {key}")
            return await tx.insert_ack(operator, why, state, now_ms)
