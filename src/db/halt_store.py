"""HaltStore / HaltTxn (src/oversee/halt.py) over PostgreSQL (oversee-rules.md §4, §8).

`transaction()` is a fenced READ COMMITTED transaction that first takes
`pg_advisory_xact_lock(HALT_LOCK)` as its own statement, so trip(), the system clear() and the operator
clear() are serialised and compute the rise check and `effective_level` inside the lock. HALT_LOCK is a
serialisation lock, not fencing: every write inside is still fenced on `engine_epoch.epoch` (src/db/fence.py).
`halt_events` is append-only; `breach_states()` returns the unresolved `halt_breaches` rows (resolved
rows are never active and are never touched by clear()).
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Final

from src.db.epoch import HALT_LOCK, EngineDb
from src.db.fence import FENCE, FencedTx
from src.db.notify_store import insert_outbox
from src.db.oversee_errors import oversee_errors
from src.db.rows import ACK_COLS, BREACH_COLS, HALT_EVENT_COLS, ack_from_row, breach_from_row, breach_params
from src.db.rows import halt_event_from_row, level_to_db, subject_to_db
from src.db.sweep_writer import SQL_BREACH_UPSERT, SQL_EVENTS, event_arrays
from src.oversee.halt import HaltEvent, HaltTxn, NewHaltEvent
from src.oversee.notify import OutboxRow
from src.oversee.snapshot import BreachAck
from src.oversee.types import BreachState, StateKey, Transition

SQL_HALT_LOCK: Final = "SELECT pg_advisory_xact_lock($1::bigint)"
SQL_LATEST_EVENT: Final = f"SELECT {HALT_EVENT_COLS} FROM halt_events ORDER BY id DESC LIMIT 1"
SQL_LEVEL_ROSE_AT: Final = (
    "SELECT at_ms FROM (SELECT id, at_ms, "
    "array_position(ARRAY['NONE', 'SOFT', 'HARD'], effective_level) AS lv, "
    "COALESCE(array_position(ARRAY['NONE', 'SOFT', 'HARD'], lag(effective_level) OVER (ORDER BY id)), 1) AS prev "
    "FROM halt_events) e WHERE lv > prev ORDER BY id DESC LIMIT 1"
)
SQL_LATEST_OPERATOR_CLEAR: Final = "SELECT max(at_ms) FROM halt_events WHERE \"by\" <> 'system'"
SQL_INSERT_EVENT: Final = (
    'INSERT INTO halt_events (level, effective_level, hard_latch, rule_id, reason, "by", at_ms) '
    f"SELECT $2::text, $3::text, $4::boolean, $5::text, $6::text, $7::text, $8::bigint WHERE {FENCE} RETURNING id"
)
SQL_UNRESOLVED_BREACHES: Final = (
    f"SELECT {BREACH_COLS} FROM halt_breaches WHERE resolved_at IS NULL ORDER BY rule_id, subject_type, subject_id"
)
SQL_INSERT_ACK: Final = (
    "INSERT INTO breach_acks (breach_id, operator, why, level, cap, reason, acked_at_ms) "
    "SELECT breach_id, $5::text, $6::text, $7::text, $8::text, $9::text, $10::bigint FROM halt_breaches "
    f"WHERE rule_id = $2 AND subject_type = $3 AND subject_id = $4 AND {FENCE} RETURNING ack_id"
)

# The acks of a set of unresolved rule states, by key (the three arrays are zipped: one entry per key).
SQL_ACKS_OF: Final = (
    f"SELECT {ACK_COLS} FROM breach_acks k JOIN halt_breaches b ON b.breach_id = k.breach_id "
    "JOIN unnest($1::text[], $2::text[], $3::text[]) AS w(rule_id, subject_type, subject_id) "
    "ON b.rule_id = w.rule_id AND b.subject_type = w.subject_type AND b.subject_id = w.subject_id "
    "WHERE b.resolved_at IS NULL ORDER BY k.ack_id"
)


class PgHaltTxn:
    """The statements available inside HALT_LOCK (HaltTxn)."""

    def __init__(self, tx: FencedTx) -> None:
        self._tx = tx

    async def latest_event(self) -> HaltEvent | None:
        row = await self._tx.fetchrow(SQL_LATEST_EVENT)
        return None if row is None else halt_event_from_row(row)

    async def level_rose_at_ms(self) -> int | None:
        row = await self._tx.fetchrow(SQL_LEVEL_ROSE_AT)
        return None if row is None or row[0] is None else int(row[0])

    async def latest_operator_clear_ms(self) -> int | None:
        row = await self._tx.fetchrow(SQL_LATEST_OPERATOR_CLEAR)
        return None if row is None or row[0] is None else int(row[0])

    async def insert_event(self, ev: NewHaltEvent) -> HaltEvent:
        row = await self._tx.write_returning(SQL_INSERT_EVENT, ev.level.name, ev.effective_level.name, ev.hard_latch,
                                             ev.rule_id, ev.reason, ev.by, ev.at_ms, must=True)
        assert row is not None   # must=True raises on 0 rows
        return HaltEvent(int(row[0]), ev.level, ev.effective_level, ev.hard_latch, ev.rule_id, ev.reason, ev.by,
                         ev.at_ms)

    async def breach_states(self) -> list[BreachState]:
        return [breach_from_row(r) for r in await self._tx.fetch(SQL_UNRESOLVED_BREACHES)]

    async def save_states(self, states: Sequence[BreachState]) -> None:
        for s in states:
            await self._tx.write(SQL_BREACH_UPSERT, *breach_params(s), must=True)

    async def insert_ack(self, operator: str, why: str, state: BreachState, at_ms: int) -> BreachAck:
        row = await self._tx.write_returning(SQL_INSERT_ACK, state.rule_id, subject_to_db(state.subject_type),
                                             state.subject_id, operator, why, level_to_db(state.level),
                                             state.cap.value, state.reason, at_ms)
        if row is None:
            raise LookupError(f"no halt_breaches row for {state.key}")
        return ack_from_row((int(row[0]), state.rule_id, subject_to_db(state.subject_type), state.subject_id,
                             level_to_db(state.level), state.cap.value, state.reason, operator, why, at_ms))

    async def acks(self, keys: Sequence[StateKey]) -> list[BreachAck]:
        if not keys:
            return []
        rows = await self._tx.fetch(SQL_ACKS_OF, [k[0] for k in keys], [subject_to_db(k[1]) for k in keys],
                                    [k[2] for k in keys])
        return [ack_from_row(r) for r in rows]

    async def insert_outbox(self, rows: Sequence[OutboxRow]) -> None:
        await insert_outbox(self._tx, rows)

    async def insert_events(self, events: Sequence[Transition]) -> None:
        by_account: dict[str, list[Transition]] = {}
        for ev in events:
            by_account.setdefault(ev.account_id, []).append(ev)
        for account_id, evs in by_account.items():
            await self._tx.write(SQL_EVENTS, *event_arrays(tuple(evs)), account_id)


class PgHaltStore:
    """HaltStore: HALT_LOCK + fenced READ COMMITTED transaction; commits on clean exit."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    @asynccontextmanager
    async def _txn(self) -> AsyncIterator[HaltTxn]:
        async with oversee_errors():
            async with self._db.transaction() as tx:
                await tx.fetchrow(SQL_HALT_LOCK, HALT_LOCK)
                yield PgHaltTxn(tx)

    def transaction(self) -> AbstractAsyncContextManager[HaltTxn]:
        return self._txn()
