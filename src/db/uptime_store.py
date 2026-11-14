"""Engine uptime and downtime catch-up stores (migration 0012).

* `PgUptimeStore` (every node, the control pool): `engine_runs` (start, heartbeat, suspended gaps, clean stop)
  and the `adapter_coverage` heartbeat. Node telemetry, not engine state: written WITHOUT the epoch fence (like the
  spill drain). The coverage upsert is still skipped (0 rows, never an EpochMismatch) when `engine_epoch` no longer
  holds the writer's epoch, and coverage only moves forward (GREATEST), so a fenced-out ex-leader can never claim
  coverage for data it could not commit.
* `PgCatchupStore` (the leader's term pool): reads for the catch-up window and its counts (coverage, stored
  versions by id, known cash-flow ids) and the fenced `catchup_runs` insert.

Counts and ids only: nothing here reads or writes an amount.
"""
from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from src.db.epoch import EngineDb
from src.db.fence import FENCE
from src.db.pg import ConnectionSource, rowcount

# ------------------------------------------------------------------------------------------------ engine_runs
SQL_RUN_START: Final = (
    "INSERT INTO engine_runs (run_id, node_id, started_at_ms, last_seen_ms) VALUES ($1::uuid, $2::text, $3::bigint, "
    "$3::bigint) ON CONFLICT (run_id) DO NOTHING"
)
SQL_RUN_HEARTBEAT: Final = (
    "UPDATE engine_runs SET last_seen_ms = GREATEST(last_seen_ms, $2::bigint) WHERE run_id = $1::uuid"
)
SQL_RUN_SUSPENDED: Final = (
    "UPDATE engine_runs SET suspended_gaps = suspended_gaps + 1, last_seen_ms = GREATEST(last_seen_ms, $2::bigint) "
    "WHERE run_id = $1::uuid"
)
SQL_RUN_STOP: Final = (
    "UPDATE engine_runs SET stopped_at_ms = $2::bigint, last_seen_ms = GREATEST(last_seen_ms, $2::bigint), "
    "end_reason = $3::text WHERE run_id = $1::uuid AND stopped_at_ms IS NULL"
)
#: The newest earlier run of this node (the previous process; its stop or last heartbeat bounds the offline gap).
SQL_PREVIOUS_RUN: Final = (
    "SELECT run_id::text, started_at_ms, last_seen_ms, stopped_at_ms, end_reason, suspended_gaps FROM engine_runs "
    "WHERE node_id = $1::text AND run_id <> $2::uuid ORDER BY started_at_ms DESC LIMIT 1"
)

# ------------------------------------------------------------------------------------------------ adapter_coverage
#: `$1` is the writer's epoch, as a soft condition (0 rows = skipped, never an EpochMismatch): see the module doc.
SQL_COVERAGE_UPSERT: Final = (
    "INSERT INTO adapter_coverage (adapter_id, last_streaming_ms, last_streaming_run_id, updated_at_ms) "
    "SELECT $2::text, $3::bigint, $4::uuid, now_ms() WHERE " + FENCE + " "
    "ON CONFLICT (adapter_id) DO UPDATE SET "
    "last_streaming_ms = GREATEST(adapter_coverage.last_streaming_ms, EXCLUDED.last_streaming_ms), "
    "last_streaming_run_id = CASE WHEN EXCLUDED.last_streaming_ms >= adapter_coverage.last_streaming_ms "
    "THEN EXCLUDED.last_streaming_run_id ELSE adapter_coverage.last_streaming_run_id END, "
    "updated_at_ms = now_ms()"
)
SQL_COVERAGE_GET: Final = (
    "SELECT adapter_id, last_streaming_ms, last_streaming_run_id::text FROM adapter_coverage WHERE adapter_id = $1"
)

# ------------------------------------------------------------------------------------------------ catch-up reads
SQL_STORED_ORDERS: Final = (
    "SELECT broker_order_id, checksum, status FROM orders_state WHERE adapter_id = $1 "
    "AND broker_order_id = ANY($2::text[])"
)
SQL_STORED_POSITIONS: Final = (
    "SELECT position_id, checksum, status FROM positions_state WHERE adapter_id = $1 AND position_id = ANY($2::text[])"
)
SQL_KNOWN_CASH_FLOWS: Final = (
    "SELECT transaction_id FROM cash_flows WHERE adapter_id = $1 AND transaction_id = ANY($2::text[])"
)
#: A window without a coverage row (src/runtime/catchup.py): the earliest last-seen / stop time of the engine runs
#: other than `$1` that started before `$2` (this term's start), and the newest event logged before `$3` (= `$2`
#: in HLC form, ms << 20: a backward scan of the event_log primary key). The engine's last known active time.
SQL_PRIOR_RUNS: Final = (
    "SELECT MIN(GREATEST(last_seen_ms, COALESCE(stopped_at_ms, last_seen_ms))) FROM engine_runs "
    "WHERE ($1::text IS NULL OR run_id::text <> $1::text) AND started_at_ms < $2::bigint"
)
SQL_LAST_EVENT_BEFORE: Final = "SELECT MAX(seq_id) FROM event_log WHERE seq_id < $1::bigint"
#: seq_id = epoch_ms << HLC_SHIFT (dto-contracts.md §2)
HLC_SHIFT: Final = 20
SQL_CATCHUP_INSERT: Final = (
    "INSERT INTO catchup_runs (run_id, epoch, adapter_id, reason, gap_start_ms, gap_end_ms, covered_from_ms, "
    "offline_from_ms, started_at_ms, finished_at_ms, orders_seen, orders_new, orders_updated, positions_seen, "
    "positions_new, positions_closed, cash_flows_new, anomalies, complete, notes) "
    "SELECT $2::uuid, $1::bigint, $3::text, $4::text, $5::bigint, $6::bigint, $7::bigint, $8::bigint, $9::bigint, "
    "$10::bigint, $11::int, $12::int, $13::int, $14::int, $15::int, $16::int, $17::int, $18::int, $19::boolean, "
    "$20::text WHERE " + FENCE + " RETURNING id"
)


@dataclass(frozen=True)
class RunRow:
    run_id: str
    started_at_ms: int
    last_seen_ms: int
    stopped_at_ms: int | None
    end_reason: str | None
    suspended_gaps: int


@dataclass(frozen=True)
class Coverage:
    adapter_id: str
    last_streaming_ms: int
    run_id: str | None


@dataclass(frozen=True)
class StoredRow:
    """The stored current row of one entity, as far as the catch-up counts need it (no amounts)."""

    checksum: str
    status: str


@dataclass(frozen=True)
class CatchupRecord:
    run_id: str | None
    adapter_id: str
    reason: str
    gap_start_ms: int
    gap_end_ms: int
    covered_from_ms: int | None
    offline_from_ms: int | None
    started_at_ms: int
    finished_at_ms: int
    orders_seen: int = 0
    orders_new: int = 0
    orders_updated: int = 0
    positions_seen: int = 0
    positions_new: int = 0
    positions_closed: int = 0
    cash_flows_new: int = 0
    anomalies: int = 0
    complete: bool = False
    notes: str = ""


class PgUptimeStore:
    """engine_runs + the adapter_coverage heartbeat over any node's connection source (the control pool)."""

    def __init__(self, source: ConnectionSource) -> None:
        self._source = source

    async def start_run(self, run_id: str, node_id: str, at_ms: int) -> None:
        async with self._source.acquire() as conn:
            await conn.execute(SQL_RUN_START, run_id, node_id, at_ms)

    async def heartbeat(self, run_id: str, at_ms: int) -> bool:
        """False when the run row is missing (its start was never written: the caller writes it again)."""
        async with self._source.acquire() as conn:
            return rowcount(await conn.execute(SQL_RUN_HEARTBEAT, run_id, at_ms)) > 0

    async def note_suspended(self, run_id: str, at_ms: int) -> None:
        async with self._source.acquire() as conn:
            await conn.execute(SQL_RUN_SUSPENDED, run_id, at_ms)

    async def stop_run(self, run_id: str, at_ms: int, reason: str) -> None:
        async with self._source.acquire() as conn:
            await conn.execute(SQL_RUN_STOP, run_id, at_ms, reason[:120])

    async def previous_run(self, node_id: str, run_id: str) -> RunRow | None:
        async with self._source.acquire() as conn:
            r = await conn.fetchrow(SQL_PREVIOUS_RUN, node_id, run_id)
        if r is None:
            return None
        return RunRow(str(r[0]), int(r[1]), int(r[2]), None if r[3] is None else int(r[3]),
                      None if r[4] is None else str(r[4]), int(r[5] or 0))

    async def advance_coverage(self, epoch_token: int, adapter_id: str, at_ms: int, run_id: str | None) -> bool:
        """Coverage moves forward to `at_ms`. False when skipped (the writer's epoch is no longer current)."""
        async with self._source.acquire() as conn:
            return rowcount(await conn.execute(SQL_COVERAGE_UPSERT, epoch_token, adapter_id, at_ms, run_id)) > 0


def _chunks(ids: Sequence[str], n: int = 1000) -> list[list[str]]:
    return [list(ids[i:i + n]) for i in range(0, len(ids), n)]


class PgCatchupStore:
    """The leader's catch-up reads and the fenced catchup_runs insert (term pool)."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def coverage(self, adapter_id: str) -> Coverage | None:
        async with self._db.source.acquire() as conn:
            r = await conn.fetchrow(SQL_COVERAGE_GET, adapter_id)
        if r is None:
            return None
        return Coverage(str(r[0]), int(r[1]), None if r[2] is None else str(r[2]))

    async def _stored(self, sql: str, adapter_id: str, ids: Collection[str]) -> dict[str, StoredRow]:
        out: dict[str, StoredRow] = {}
        wanted = sorted(set(ids))
        if not wanted:
            return out
        async with self._db.source.acquire() as conn:
            for chunk in _chunks(wanted):
                for r in await conn.fetch(sql, adapter_id, chunk):
                    out[str(r[0])] = StoredRow(str(r[1]).strip(), str(r[2]))
        return out

    async def stored_orders(self, adapter_id: str, broker_order_ids: Collection[str]) -> Mapping[str, StoredRow]:
        return await self._stored(SQL_STORED_ORDERS, adapter_id, broker_order_ids)

    async def stored_positions(self, adapter_id: str, position_ids: Collection[str]) -> Mapping[str, StoredRow]:
        return await self._stored(SQL_STORED_POSITIONS, adapter_id, position_ids)

    async def known_cash_flows(self, adapter_id: str, transaction_ids: Collection[str]) -> set[str]:
        wanted = sorted(set(transaction_ids))
        out: set[str] = set()
        if not wanted:
            return out
        async with self._db.source.acquire() as conn:
            for chunk in _chunks(wanted):
                out |= {str(r[0]) for r in await conn.fetch(SQL_KNOWN_CASH_FLOWS, adapter_id, chunk)}
        return out

    async def prior_activity_ms(self, adapter_id: str, run_id: str | None, before_ms: int) -> int | None:
        """The engine's last known active time before `before_ms` (engine-wide; `adapter_id` is not needed): the
        older of the earliest earlier run's last-seen / stop time and the newest event logged before `before_ms`
        (which also dates activity from before migration 0012). None when neither exists."""
        async with self._db.source.acquire() as conn:
            runs = await conn.fetchrow(SQL_PRIOR_RUNS, run_id, before_ms)
            last = await conn.fetchrow(SQL_LAST_EVENT_BEFORE, max(before_ms, 0) << HLC_SHIFT)
        found = [int(runs[0])] if runs is not None and runs[0] is not None else []
        if last is not None and last[0] is not None:
            found.append(int(last[0]) >> HLC_SHIFT)
        return min(found) if found else None

    async def record(self, epoch_token: int, rec: CatchupRecord) -> int:
        """Fenced insert (a lost fence raises EpochMismatch). Returns the row id."""
        async with self._db.transaction(epoch_token) as tx:
            row = await tx.write_returning(
                SQL_CATCHUP_INSERT, rec.run_id, rec.adapter_id, rec.reason, rec.gap_start_ms, rec.gap_end_ms,
                rec.covered_from_ms, rec.offline_from_ms, rec.started_at_ms, rec.finished_at_ms, rec.orders_seen,
                rec.orders_new, rec.orders_updated, rec.positions_seen, rec.positions_new, rec.positions_closed,
                rec.cash_flows_new, rec.anomalies, rec.complete, rec.notes[:2000], must=True)
        return int(row[0]) if row is not None else 0


__all__ = ["CatchupRecord", "Coverage", "PgCatchupStore", "PgUptimeStore", "RunRow", "StoredRow",
           "SQL_CATCHUP_INSERT", "SQL_COVERAGE_GET", "SQL_COVERAGE_UPSERT", "SQL_KNOWN_CASH_FLOWS",
           "SQL_LAST_EVENT_BEFORE", "SQL_PREVIOUS_RUN", "SQL_PRIOR_RUNS", "SQL_RUN_HEARTBEAT", "SQL_RUN_START", "SQL_RUN_STOP", "SQL_RUN_SUSPENDED",
           "SQL_STORED_ORDERS", "SQL_STORED_POSITIONS"]
