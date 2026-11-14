"""SweepWriter (src/oversee/sweep.py) over PostgreSQL (oversee-rules.md §2 Writes).

`write_account` is ONE fenced READ COMMITTED transaction (fencing token = `AccountWrite.epoch`):
1. halt_breaches compare-and-set on (last_sweep_id, clear_generation): a new row must not exist yet
   (INSERT ... ON CONFLICT DO NOTHING), an existing row must still carry the expected pair (UPDATE ... WHERE).
   A miss raises `CasConflict` (after the fence re-check), rolling back the whole batch;
2. stop_watch first-seen rows (ON CONFLICT DO NOTHING keeps the original deadline) and clears;
3. oversee_events, idempotent on (rule_id, subject_type, subject_id, transition, sweep_id);
4. notification_outbox rows for the transitions;
5. the day's sod_marks row, idempotent on (adapter_id, account_id, trading_day); a placeholder row
   (sod_equity NULL, written only to carry `transfers_unverified`, src/db/integrity_store.py) is filled
   in, keeping its flag.
A lost fence raises `SweepFenced` (the db and Oversee `EpochMismatch`), always from inside
`EngineDb.transaction` (an empty batch's one-read fence check re-confirms a mismatch there), so a guarded
EngineDb reports it to the runtime hard exit; PostgreSQL unreachable raises `DependencyUnavailable`. `set_quarantine` writes `sweep_quarantine` (fenced).
"""
from __future__ import annotations

from typing import Final

from src.db.epoch import EngineDb
from src.db.fence import FENCE, EpochMismatch, FencedTx, assert_epoch
from src.db.notify_store import insert_outbox
from src.db.oversee_errors import oversee_errors
from src.db.rows import BREACH_COLS, breach_params, marks_json, subject_to_db
from src.oversee.snapshot import SodMark
from src.oversee.sweep import AccountWrite, CasConflict, StateWrite
from src.oversee.types import Transition

QUARANTINE_RETRY_MS: Final = 10 * 60_000

BREACH_COL_NAMES: Final = tuple(c.strip() for c in BREACH_COLS.split(","))
BREACH_TYPES: Final = (
    "text", "text", "text", "text", "text", "text", "text", "text", "bigint", "bigint", "bigint", "bigint",
    "bigint", "boolean", "bigint", "bigint", "bigint", "bigint", "text", "bigint", "bigint",
)
_KEY_COLS: Final = ("rule_id", "subject_type", "subject_id")


def adapter_of(param: str) -> str:
    """Oversee keys accounts by account_id; the NOT NULL adapter column is filled from account_state, else
    (like PgSnapshotReader's SQL_ADAPTER_OF) from the account's positions or orders, else from its existing
    halt_breaches rows. An account listed only through its entities, or no longer listed at all
    (HaltSwitch.resolve_unlisted), therefore still gets an adapter instead of a not-null violation."""
    p = f"{param}::text"
    return (
        f"COALESCE((SELECT adapter_id FROM account_state WHERE account_id = {p} ORDER BY adapter_id LIMIT 1), "
        f"(SELECT adapter_id FROM positions_state WHERE account_id = {p} "
        f"UNION SELECT adapter_id FROM orders_state WHERE account_id = {p} ORDER BY 1 LIMIT 1), "
        f"(SELECT adapter_id FROM halt_breaches WHERE account_id = {p} ORDER BY adapter_id LIMIT 1))"
    )


def _casts(first: int = 2) -> str:
    return ", ".join(f"${i}::{t}" for i, t in enumerate(BREACH_TYPES, first))


_ACCOUNT_PARAM: Final = f"${2 + BREACH_COL_NAMES.index('account_id')}"
_SETS: Final = ", ".join(f"{c} = ${i}" for i, c in enumerate(BREACH_COL_NAMES, 2) if c not in _KEY_COLS)
_N: Final = 2 + len(BREACH_COL_NAMES)   # first parameter after the breach columns

SQL_BREACH_INSERT: Final = (
    f"INSERT INTO halt_breaches (adapter_id, {BREACH_COLS}, updated_at_ms) "
    f"SELECT {adapter_of(_ACCOUNT_PARAM)}, {_casts()}, now_ms() WHERE {FENCE} "
    "ON CONFLICT (rule_id, subject_type, subject_id) DO NOTHING RETURNING breach_id"
)
SQL_BREACH_CAS: Final = (
    f"UPDATE halt_breaches SET {_SETS}, updated_at_ms = now_ms() "
    "WHERE rule_id = $2 AND subject_type = $3 AND subject_id = $4 "
    f"AND last_sweep_id IS NOT DISTINCT FROM ${_N} AND clear_generation = ${_N + 1} AND {FENCE}"
)
SQL_BREACH_UPSERT: Final = (
    f"INSERT INTO halt_breaches (adapter_id, {BREACH_COLS}, updated_at_ms) "
    f"SELECT {adapter_of(_ACCOUNT_PARAM)}, {_casts()}, now_ms() WHERE {FENCE} "
    "ON CONFLICT (rule_id, subject_type, subject_id) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in BREACH_COL_NAMES if c not in _KEY_COLS)
    + ", updated_at_ms = now_ms()"
)
SQL_STOP_WATCH_SET: Final = (
    "INSERT INTO stop_watch (entity_type, entity_id, adapter_id, account_id, effective_stop_missing_since_ms, "
    f"last_sweep_id) SELECT t.et, t.eid, {adapter_of('$5')}, $5::text, t.since, $6::bigint "
    "FROM unnest($2::text[], $3::text[], $4::bigint[]) AS t(et, eid, since) "
    f"WHERE {FENCE} ON CONFLICT (entity_type, entity_id) DO NOTHING"
)
SQL_STOP_WATCH_CLEAR: Final = (
    "DELETE FROM stop_watch WHERE (entity_type, entity_id) IN "
    f"(SELECT et, eid FROM unnest($2::text[], $3::text[]) AS t(et, eid)) AND {FENCE}"
)
SQL_EVENTS: Final = (
    "INSERT INTO oversee_events (sweep_id, rule_id, subject_type, subject_id, transition, adapter_id, account_id, "
    "level, cap, reason, at_ms, activation_id) "
    f"SELECT t.sweep_id, t.rule_id, t.st, t.sid, t.tr, {adapter_of('$12')}, $12::text, t.lvl, t.cap, t.reason, "
    "t.at_ms, t.act FROM unnest($2::bigint[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], "
    "$8::text[], $9::text[], $10::bigint[], $11::bigint[]) AS t(sweep_id, rule_id, st, sid, tr, lvl, cap, reason, "
    f"at_ms, act) WHERE {FENCE} ON CONFLICT (rule_id, subject_type, subject_id, transition, sweep_id) DO NOTHING"
)
SQL_SOD_INSERT: Final = (
    "INSERT INTO sod_marks (adapter_id, account_id, trading_day, roll_at_ms, balance, sod_equity, "
    "unexplained_increase_at_roll, partial_session, partial_reasons, clean_mids, written_by, sweep_id, "
    "written_at_ms) SELECT a.adapter_id, $2::text, $3::date, $4::bigint, $5::numeric, $6::numeric, $7::numeric, "
    "$8::boolean, $9::text[], $10::text::jsonb, 'sweep', $11::bigint, $12::bigint "
    "FROM (SELECT adapter_id FROM account_state WHERE account_id = $2::text ORDER BY adapter_id LIMIT 1) a "
    f"WHERE {FENCE} ON CONFLICT (adapter_id, account_id, trading_day) DO UPDATE SET "
    "roll_at_ms = EXCLUDED.roll_at_ms, balance = EXCLUDED.balance, sod_equity = EXCLUDED.sod_equity, "
    "unexplained_increase_at_roll = EXCLUDED.unexplained_increase_at_roll, "
    "partial_session = EXCLUDED.partial_session, partial_reasons = EXCLUDED.partial_reasons, "
    "clean_mids = EXCLUDED.clean_mids, written_by = EXCLUDED.written_by, sweep_id = EXCLUDED.sweep_id, "
    "written_at_ms = EXCLUDED.written_at_ms WHERE sod_marks.sod_equity IS NULL"
)
SQL_QUARANTINE_SET: Final = (
    "INSERT INTO sweep_quarantine (adapter_id, account_id, failing_since_ms, quarantined_at_ms, next_retry_at_ms, "
    f"reason) SELECT {adapter_of('$2')}, $2::text, $3::bigint, $3::bigint, $3::bigint + $4::bigint, $5::text "
    f"WHERE {FENCE} ON CONFLICT (account_id) DO UPDATE SET quarantined_at_ms = EXCLUDED.quarantined_at_ms, "
    "next_retry_at_ms = EXCLUDED.next_retry_at_ms, reason = EXCLUDED.reason, released_at_ms = NULL, "
    "released_by = NULL"
)
SQL_QUARANTINE_RELEASE: Final = (
    "UPDATE sweep_quarantine SET released_at_ms = now_ms(), released_by = 'pass' "
    f"WHERE account_id = $2 AND released_at_ms IS NULL AND {FENCE}"
)


async def write_state(tx: FencedTx, w: StateWrite) -> None:
    """One halt_breaches compare-and-set; CasConflict when another writer moved (or created) the row."""
    params = breach_params(w.state)
    if w.expected_sweep_id is None:
        if await tx.write_returning(SQL_BREACH_INSERT, *params) is None:
            raise CasConflict(f"{w.state.key} already exists")
        return
    if await tx.write(SQL_BREACH_CAS, *params, w.expected_sweep_id, w.expected_generation or 0) == 0:
        raise CasConflict(f"{w.state.key} moved (expected sweep {w.expected_sweep_id}, "
                          f"generation {w.expected_generation})")


def event_arrays(events: tuple[Transition, ...]) -> tuple[list[object], ...]:
    return ([e.sweep_id for e in events], [e.rule_id for e in events], [subject_to_db(e.subject_type) for e in events],
            [e.subject_id for e in events], [e.transition for e in events], [e.level.name for e in events],
            [e.cap.value for e in events], [e.reason for e in events], [e.at_ms for e in events],
            [e.activation_id for e in events])


def _sod_params(account_id: str, sod: SodMark, sweep_id: int) -> tuple[object, ...]:
    return (account_id, sod.trading_day, sod.roll_ms, sod.balance, sod.sod_equity, sod.unexplained_increase_at_roll,
            sod.partial_session, list(sod.partial_reasons), marks_json(sod.marks), sweep_id, sod.written_at_ms)


def _empty(b: AccountWrite) -> bool:
    return not (b.states or b.stop_watch_set or b.stop_watch_clear or b.events or b.outbox or b.sod_mark)


class PgSweepWriter:
    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def write_account(self, batch: AccountWrite) -> None:
        async with oversee_errors():
            if _empty(batch):   # nothing to write: still confirm the fence (one read, no transaction)
                try:
                    async with self._db.source.acquire() as conn:
                        await assert_epoch(conn, batch.epoch)
                except EpochMismatch as lost:
                    await self._report_lost_fence(batch.epoch)
                    raise lost
                return
            async with self._db.transaction(batch.epoch) as tx:
                await self._write(tx, batch)

    async def _report_lost_fence(self, epoch: int) -> None:
        """A lost fence seen outside a transaction (the empty-batch check) is confirmed once more inside
        `EngineDb.transaction`, so it surfaces where a guarded EngineDb (src/runtime/pg_ports.py
        GuardedEngineDb: the runtime hard exit, operations.md §3) sees every lost fence. Any other error
        of that re-check is ignored: the caller re-raises the original EpochMismatch."""
        try:
            async with self._db.transaction(epoch) as tx:
                await assert_epoch(tx.conn, epoch)
        except Exception:    # expected: EpochMismatch again (already reported); anything else is ignored
            pass

    @staticmethod
    async def _write(tx: FencedTx, b: AccountWrite) -> None:
        for w in b.states:
            await write_state(tx, w)
        if b.stop_watch_set:
            await tx.write(SQL_STOP_WATCH_SET, [k[0].upper() for k, _ in b.stop_watch_set],
                           [k[1] for k, _ in b.stop_watch_set], [since for _, since in b.stop_watch_set],
                           b.account_id, b.sweep_id)
        if b.stop_watch_clear:
            await tx.write(SQL_STOP_WATCH_CLEAR, [k[0].upper() for k in b.stop_watch_clear],
                           [k[1] for k in b.stop_watch_clear])
        if b.events:
            await tx.write(SQL_EVENTS, *event_arrays(b.events), b.account_id)
        await insert_outbox(tx, b.outbox)
        if b.sod_mark is not None:
            await tx.write(SQL_SOD_INSERT, *_sod_params(b.account_id, b.sod_mark, b.sweep_id))

    async def set_quarantine(self, account_id: str, at_ms: int | None, reason: str) -> None:
        async with oversee_errors():
            async with self._db.transaction() as tx:
                if at_ms is None:
                    await tx.write(SQL_QUARANTINE_RELEASE, account_id)
                else:
                    await tx.write(SQL_QUARANTINE_SET, account_id, at_ms, QUARANTINE_RETRY_MS, reason, must=True)


__all__ = ["PgSweepWriter", "event_arrays", "write_state"]
