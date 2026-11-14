"""PostgreSQL implementations of the integrity ports (src/integrity/ports.py): IntegrityDb and DlqStore.

* `PgIntegrityDb` (SM3 / SM8 reads, tier-persistence.md §2, §3): unfenced reads of the current-row tables
  (orders_state, positions_state, account_state, instruments), halt_events, engine_epoch, last_clean_marks
  and account_snapshots. Rows are rebuilt as DTOs with the same decoders the RiskSweep snapshot uses
  (src/db/rows.py), so Step 2 can recompute their checksums. Each table is read with its own statement
  (typed columns, no JSON round trip) and the results are merged in Python in the documented order.
* `PgDlqStore` (SM2, tier-persistence.md §5): every update of `events_dlq` runs in a fenced READ COMMITTED
  transaction (`(SELECT epoch FROM engine_epoch WHERE id = 1) = $1` in the same statement, src/db/fence.py)
  and every per-row update checks the lease owner (`status = 'LEASED' AND lease_owner = $owner`), so a
  worker whose lease expired (and was re-leased) can never move a row it no longer holds. Lineage lookups
  filter `origin_dlq_id IS NULL` and order by `updated_at_ms DESC`.

Redis entity keys are `{type}:{adapter_id}:{id}` (src/cache/keys.py); adapter ids never contain ':'.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from src.cache import keys as K
from src.cache.codec import dto_json
from src.cache.scripts import entity_write
from src.db.epoch import EngineDb
from src.db.fence import FENCE
from src.db.pg import ConnectionSource, Row
from src.db.rows import (
    ACCOUNT_COLS,
    INSTRUMENT_COLS,
    ORDER_COLS,
    POSITION_COLS,
    account_from_row,
    instrument_from_row,
    order_from_row,
    position_from_row,
    to_decimal,
)
from src.dto import AccountLive, InstrumentSpec
from src.integrity.ports import (
    CleanMark,
    CommittedRow,
    DbHighWater,
    DlqRecord,
    DlqStatus,
    EntityPayload,
    HaltLevelName,
    HaltRow,
)
from src.oversee.dayroll import latest_roll_ms, trading_day_for_roll
from src.ports import DlqEventType

log = logging.getLogger(__name__)


class CorruptRowError(ValueError):
    """A current row that cannot be rebuilt as its DTO (PostgreSQL damage: the engine never writes one)."""

    def __init__(self, key: str, detail: str) -> None:
        super().__init__(f"{key}: {detail}")
        self.key = key


# ==================================================================================== IntegrityDb

#: Columns appended to every current-row SELECT, after the table's decoder columns.
_SUFFIX: Final = "version, seq_id, checksum, digest_hash, committed_at_ms"


@dataclass(frozen=True)
class CurrentTable:
    """One current-row table as SM3 reads it."""

    table: str
    entity_type: K.EntityType
    prefix: str                     # Redis key prefix, e.g. 'order'
    id_col: str                     # second primary-key column (the first is adapter_id)
    cols: str                       # decoder columns (src/db/rows.py)
    decode: Callable[[Row], EntityPayload]

    @property
    def ncols(self) -> int:
        return len(self.cols.split(","))

    @property
    def key_expr(self) -> str:
        return f"('{self.prefix}:' || adapter_id || ':' || {self.id_col}) COLLATE \"C\""

    @property
    def _select(self) -> str:
        return f"SELECT {self.cols}, {_SUFFIX}, {self.key_expr} AS k FROM {self.table}"

    @property
    def sql_window(self) -> str:
        """$1 from_ms, $2 to_ms (inclusive), $3 limit; oldest commit first, then key."""
        return (f"{self._select} WHERE committed_at_ms BETWEEN $1::bigint AND $2::bigint "
                "ORDER BY committed_at_ms, k LIMIT $3::int")

    @property
    def sql_after_key(self) -> str:
        """$1 after_key (exclusive), $2 limit; ordered by key (COLLATE "C")."""
        return f"{self._select} WHERE {self.key_expr} > $1::text ORDER BY k LIMIT $2::int"

    @property
    def sql_by_ids(self) -> str:
        """$1 adapter ids, $2 entity ids (parallel arrays)."""
        return (f"{self._select} WHERE (adapter_id, {self.id_col}) IN "
                "(SELECT u.a, u.i FROM unnest($1::text[], $2::text[]) AS u(a, i))")


ORDERS_CURRENT: Final = CurrentTable("orders_state", "ORDER", "order", "broker_order_id", ORDER_COLS,
                                     order_from_row)
POSITIONS_CURRENT: Final = CurrentTable("positions_state", "POSITION", "position", "position_id", POSITION_COLS,
                                        position_from_row)
ACCOUNTS_CURRENT: Final = CurrentTable("account_state", "ACCOUNT", "account", "account_id", ACCOUNT_COLS,
                                       account_from_row)
INSTRUMENTS_CURRENT: Final = CurrentTable("instruments", "INSTRUMENT", "instrument", "symbol", INSTRUMENT_COLS,
                                          instrument_from_row)
CURRENT_TABLES: Final = (ORDERS_CURRENT, POSITIONS_CURRENT, ACCOUNTS_CURRENT, INSTRUMENTS_CURRENT)
_BY_PREFIX: Final = {t.prefix: t for t in CURRENT_TABLES}


def _digest_chunks(table: str) -> str:
    return ", ".join(f"('x' || substr(digest_hash, {1 + 16 * i}, 16))::bit(64)::bigint AS h{i}" for i in range(4)) \
        + f" FROM {table}"


SQL_DIGEST: Final = (
    "SELECT " + " || ".join(f"lpad(to_hex(COALESCE(bit_xor(h{i}), 0)), 16, '0')" for i in range(4))
    + " FROM (" + " UNION ALL ".join(f"SELECT {_digest_chunks(t.table)}" for t in CURRENT_TABLES) + ") d"
)
SQL_LATEST_COMMIT: Final = (
    "SELECT GREATEST(" + ", ".join(f"(SELECT MAX(committed_at_ms) FROM {t.table})" for t in CURRENT_TABLES) + ")"
)
SQL_LATEST_HALT: Final = "SELECT id, effective_level FROM halt_events ORDER BY id DESC LIMIT 1"
SQL_HIGH_WATER: Final = (
    "SELECT (SELECT timeline_id FROM pg_control_checkpoint()), "
    "COALESCE((SELECT epoch FROM engine_epoch WHERE id = 1), 0), "
    "GREATEST((SELECT COALESCE(MAX(seq_id), 0) FROM event_log), (SELECT COALESCE(MAX(seq_id), 0) FROM cash_flows), "
    "(SELECT COALESCE(MAX(seq_id), 0) FROM orders_state), (SELECT COALESCE(MAX(seq_id), 0) FROM positions_state), "
    "(SELECT COALESCE(MAX(seq_id), 0) FROM account_state))"
)
SQL_CLEAN_MARKS: Final = "SELECT adapter_id, symbol, bid, ask, ts, spread_pips FROM last_clean_marks ORDER BY adapter_id, symbol"
SQL_LIVE_SAMPLES: Final = (
    "SELECT DISTINCT ON (account_id) account_id, adapter_id, currency, equity, margin_used, free_margin, "
    "unrealised_pnl, live_ts_ms FROM account_snapshots WHERE live_ts_ms IS NOT NULL "
    "ORDER BY account_id, live_ts_ms DESC, seq_id DESC"
)


def _strip(v: object) -> str:
    return str(v).strip()   # char(n) columns


def committed_from_row(t: CurrentTable, r: Row) -> CommittedRow:
    """A current-row SELECT result (decoder columns + _SUFFIX + key) as a CommittedRow."""
    n = t.ncols
    version, seq_id, checksum, digest_hash, committed_at_ms, key = r[n:n + 6]
    try:
        entity = t.decode(r[:n])
    except (ValueError, TypeError, ArithmeticError, KeyError) as exc:
        raise CorruptRowError(str(key), f"{t.table} row does not rebuild as a DTO: {exc}") from exc
    if isinstance(entity, InstrumentSpec):
        entity_id, account_id, payload = f"{entity.adapter_id}:{entity.symbol}", "", dto_json(entity)
    else:
        w = entity_write(entity)
        entity_id, account_id, payload = w.entity_id, w.account_id, w.payload
    return CommittedRow(key=str(key), entity_type=t.entity_type, entity_id=entity_id, account_id=account_id,
                        version=_strip(version), seq_id=int(seq_id), checksum=_strip(checksum),
                        digest_hash=_strip(digest_hash), payload=payload, committed_at_ms=int(committed_at_ms),
                        entity=entity)


def split_key(key: str) -> tuple[CurrentTable, str, str] | None:
    """'order:stonex:B1' -> (ORDERS_CURRENT, 'stonex', 'B1'); None for a key that is not an entity key."""
    parts = key.split(":", 2)
    if len(parts) != 3 or parts[0] not in _BY_PREFIX or not parts[1] or not parts[2]:
        return None
    return _BY_PREFIX[parts[0]], parts[1], parts[2]


class PgIntegrityDb:
    """IntegrityDb over PostgreSQL. Reads only (no fence: Redis heals are post-commit copies)."""

    def __init__(self, source: ConnectionSource) -> None:
        self._source = source

    async def committed_rows(self, from_ms: int, to_ms: int, limit: int) -> Sequence[CommittedRow]:
        if limit <= 0 or to_ms < from_ms:
            return []
        out: list[CommittedRow] = []
        async with self._source.acquire() as conn:
            for t in CURRENT_TABLES:
                out += [committed_from_row(t, r) for r in await conn.fetch(t.sql_window, from_ms, to_ms, limit)]
        out.sort(key=lambda c: (c.committed_at_ms, c.key))
        return out[:limit]

    async def rows_by_keys(self, keys: Collection[str]) -> Mapping[str, CommittedRow]:
        wanted: dict[CurrentTable, tuple[list[str], list[str]]] = {}
        for key in set(keys):
            parsed = split_key(key)
            if parsed is None:
                continue
            t, adapter_id, entity_id = parsed
            pair = wanted.setdefault(t, ([], []))
            pair[0].append(adapter_id)
            pair[1].append(entity_id)
        out: dict[str, CommittedRow] = {}
        if not wanted:
            return out
        requested = set(keys)
        async with self._source.acquire() as conn:
            for t in CURRENT_TABLES:
                if t not in wanted:
                    continue
                adapters, ids = wanted[t]
                for r in await conn.fetch(t.sql_by_ids, adapters, ids):
                    row = committed_from_row(t, r)
                    if row.key in requested:
                        out[row.key] = row
        return out

    async def rows_after_key(self, after_key: str, limit: int) -> Sequence[CommittedRow]:
        if limit <= 0:
            return []
        out: list[CommittedRow] = []
        async with self._source.acquire() as conn:
            for t in CURRENT_TABLES:
                out += [committed_from_row(t, r) for r in await conn.fetch(t.sql_after_key, after_key, limit)]
        out.sort(key=lambda c: c.key.encode("utf-8"))   # COLLATE "C" = byte order
        return out[:limit]

    async def digest(self) -> str:
        async with self._source.acquire() as conn:
            row = await conn.fetchrow(SQL_DIGEST)
        return "0" * 64 if row is None or row[0] is None else str(row[0]).lower()

    async def latest_commit_ms(self) -> int | None:
        async with self._source.acquire() as conn:
            row = await conn.fetchrow(SQL_LATEST_COMMIT)
        return None if row is None or row[0] is None else int(row[0])

    async def latest_halt_event(self) -> HaltRow | None:
        async with self._source.acquire() as conn:
            row = await conn.fetchrow(SQL_LATEST_HALT)
        if row is None:
            return None
        level = _strip(row[1])
        if level not in ("NONE", "SOFT", "HARD"):
            raise CorruptRowError(f"halt_events:{row[0]}", f"effective_level {level!r}")
        lvl: HaltLevelName = "NONE" if level == "NONE" else ("SOFT" if level == "SOFT" else "HARD")
        return HaltRow(int(row[0]), lvl)

    async def high_water(self) -> DbHighWater:
        async with self._source.acquire() as conn:
            row = await conn.fetchrow(SQL_HIGH_WATER)
        if row is None:
            raise RuntimeError("high-water query returned no row")
        return DbHighWater(timeline=int(row[0] or 0), epoch=int(row[1] or 0), max_seq_id=int(row[2] or 0))

    async def last_clean_marks(self) -> Sequence[CleanMark]:
        async with self._source.acquire() as conn:
            rows = await conn.fetch(SQL_CLEAN_MARKS)
        return [CleanMark(adapter_id=str(r[0]), symbol=str(r[1]), bid=to_decimal(r[2]), ask=to_decimal(r[3]),
                          ts=int(r[4]), spread_pips=None if r[5] is None else to_decimal(r[5])) for r in rows]

    async def latest_live_samples(self) -> Sequence[AccountLive]:
        async with self._source.acquire() as conn:
            rows = await conn.fetch(SQL_LIVE_SAMPLES)
        return [AccountLive(account_id=str(r[0]), adapter_id=str(r[1]), currency=str(r[2]),
                            equity=None if r[3] is None else to_decimal(r[3]),
                            margin_used=None if r[4] is None else to_decimal(r[4]),
                            free_margin=None if r[5] is None else to_decimal(r[5]),
                            unrealised_pnl=None if r[6] is None else to_decimal(r[6]),
                            timestamp_ms=int(r[7]), sampled=True) for r in rows if r[7] is not None]


# ==================================================================================== DlqStore

DLQ_COLS: Final = (
    "dlq_id, event_type, entity_id, adapter_id, raw_payload, payload_hash, error_reason, retry_count, status, "
    "next_retry_at_ms, parked_at_ms, origin_dlq_id, lease_owner, lease_until_ms, updated_at_ms"
)
_D_COLS: Final = ", ".join(f"d.{c.strip()}" for c in DLQ_COLS.split(","))
_OWNED: Final = "status = 'LEASED' AND lease_owner = $3::text"
_UNLEASE: Final = "lease_owner = NULL, lease_until_ms = NULL, updated_at_ms = now_ms()"

# Fenced writes: $1 is the epoch (prepended by FencedTx).
SQL_DLQ_LEASE: Final = (
    "UPDATE events_dlq d SET status = 'LEASED', lease_owner = $2::text, lease_until_ms = $3::bigint + $4::bigint, "
    "updated_at_ms = $3::bigint FROM (SELECT dlq_id FROM events_dlq WHERE status IN ('QUEUED', 'BACKOFF') "
    "AND next_retry_at_ms <= $3::bigint AND adapter_id = ANY($5::text[]) ORDER BY next_retry_at_ms, dlq_id "
    f"LIMIT $6::int FOR UPDATE SKIP LOCKED) due WHERE d.dlq_id = due.dlq_id AND {FENCE} RETURNING {_D_COLS}"
)
SQL_DLQ_RELEASE_EXPIRED: Final = (
    "UPDATE events_dlq SET status = 'QUEUED', lease_owner = NULL, lease_until_ms = NULL, updated_at_ms = $2::bigint "
    f"WHERE status = 'LEASED' AND lease_until_ms < $2::bigint AND {FENCE}"
)
SQL_DLQ_REQUEUE: Final = (
    f"UPDATE events_dlq SET status = 'QUEUED', {_UNLEASE} WHERE dlq_id = $2::bigint AND {_OWNED} AND {FENCE}"
)
SQL_DLQ_RESOLVE: Final = (
    f"UPDATE events_dlq SET status = 'RESOLVED', {_UNLEASE} WHERE dlq_id = $2::bigint AND {_OWNED} AND {FENCE}"
)
SQL_DLQ_BACKOFF: Final = (
    "UPDATE events_dlq SET status = 'BACKOFF', retry_count = $4::int, next_retry_at_ms = $5::bigint, "
    f"last_error = $6::text, {_UNLEASE} WHERE dlq_id = $2::bigint AND {_OWNED} AND {FENCE}"
)
SQL_DLQ_QUARANTINE_MARK: Final = (
    f"UPDATE events_dlq SET status = 'QUARANTINED', last_error = $4::text, {_UNLEASE} "
    "WHERE dlq_id = $2::bigint AND status <> 'QUARANTINED' "
    f"AND ($3::text IS NULL OR ({_OWNED})) AND {FENCE} "
    "RETURNING event_type, entity_id, adapter_id, raw_payload, error_reason, retry_count"
)
SQL_DLQ_QUARANTINE_INSERT: Final = (
    "INSERT INTO events_quarantine (source, dlq_id, event_type, entity_id, adapter_id, account_id, raw_payload, "
    "error_reason, retry_count) SELECT 'dlq', $2::bigint, $3::text, $4::text, $5::text, $6::text, $7::text, "
    f"$8::text, $9::int WHERE {FENCE}"
)
SQL_DLQ_MERGE_CHILD: Final = (
    f"UPDATE events_dlq SET status = 'RESOLVED', origin_dlq_id = $4::bigint, {_UNLEASE} "
    f"WHERE dlq_id = $2::bigint AND {_OWNED} AND {FENCE}"
)
# The origin must be the same entity of the same adapter as the child ($5 = the child's dlq_id): entity
# ids are unique only per adapter (dlq-lineage-not-adapter-scoped).
SQL_DLQ_MERGE_ORIGIN: Final = (
    "UPDATE events_dlq o SET status = 'BACKOFF', retry_count = $3::int, next_retry_at_ms = $4::bigint, "
    "updated_at_ms = now_ms() FROM events_dlq c WHERE o.dlq_id = $2::bigint "
    "AND o.status IN ('RESOLVED', 'QUEUED', 'BACKOFF') AND c.dlq_id = $5::bigint "
    "AND c.adapter_id = o.adapter_id AND c.event_type = o.event_type AND c.entity_id = o.entity_id "
    f"AND {FENCE}"
)
SQL_SOD_TRANSFERS_UNVERIFIED: Final = (
    "INSERT INTO sod_marks (adapter_id, account_id, trading_day, roll_at_ms, transfers_unverified, written_by) "
    f"SELECT $2::text, $3::text, $4::date, $5::bigint, true, 'startup' WHERE {FENCE} "
    "ON CONFLICT (adapter_id, account_id, trading_day) DO UPDATE SET transfers_unverified = true"
)
# Unfenced reads.
SQL_DLQ_LINEAGE: Final = (
    f"SELECT {DLQ_COLS} FROM events_dlq WHERE adapter_id = $6::text AND event_type = $1::text "
    "AND entity_id = $2::text AND error_reason = $3::text AND status = 'RESOLVED' AND origin_dlq_id IS NULL "
    "AND dlq_id < $4::bigint AND updated_at_ms >= $5::bigint ORDER BY updated_at_ms DESC, dlq_id DESC LIMIT 1"
)
SQL_LAST_CASH_FLOW_TS: Final = "SELECT MAX(broker_ts) FROM cash_flows WHERE adapter_id = $1::text"
SQL_PARKED_CASH_FLOWS: Final = (
    "SELECT raw_payload FROM events_dlq WHERE adapter_id = $1::text AND event_type = 'CASH_FLOW' "
    "AND status IN ('QUEUED', 'LEASED', 'BACKOFF')"
)

_EVENT_TYPES: Final = frozenset({"ORDER", "POSITION", "ACCOUNT", "CASH_FLOW"})


def dlq_record_from_row(r: Row) -> DlqRecord:
    event_type = str(r[1])
    if event_type not in _EVENT_TYPES:
        raise CorruptRowError(f"events_dlq:{r[0]}", f"event_type {event_type!r}")
    et: DlqEventType = "ORDER" if event_type == "ORDER" else (
        "POSITION" if event_type == "POSITION" else ("ACCOUNT" if event_type == "ACCOUNT" else "CASH_FLOW"))
    return DlqRecord(dlq_id=int(r[0]), event_type=et, entity_id=str(r[2]), adapter_id=str(r[3]),
                     raw_payload=str(r[4]), payload_hash=_strip(r[5]), error_reason=str(r[6]),
                     retry_count=int(r[7]), status=DlqStatus(str(r[8])), next_retry_at_ms=int(r[9]),
                     parked_at_ms=int(r[10]), origin_dlq_id=None if r[11] is None else int(r[11]),
                     lease_owner=None if r[12] is None else str(r[12]),
                     lease_until_ms=None if r[13] is None else int(r[13]), updated_at_ms=int(r[14] or 0))


def _payload_field(raw: str, name: str) -> object:
    """A top-level field of a parked JSON payload (None for a payload that is not a JSON object)."""
    try:
        obj = json.loads(raw)
    except ValueError:
        return None
    return obj.get(name) if isinstance(obj, dict) else None


class _MergeRefused(Exception):
    """The origin row cannot take the merge: roll the child's update back."""


class PgDlqStore:
    """DlqStore over PostgreSQL events_dlq / events_quarantine (fenced, lease-owner-checked)."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def lease(self, owner: str, now_ms: int, lease_ms: int, adapters: Collection[str],
                    limit: int) -> Sequence[DlqRecord]:
        if not adapters or limit <= 0:
            return []
        async with self._db.transaction() as tx:
            rows = await tx.write_returning_all(SQL_DLQ_LEASE, owner, now_ms, lease_ms, sorted(adapters), limit)
        out = [dlq_record_from_row(r) for r in rows]
        out.sort(key=lambda d: (d.next_retry_at_ms, d.dlq_id))
        return out

    async def release_expired(self, now_ms: int) -> int:
        async with self._db.transaction() as tx:
            return await tx.write(SQL_DLQ_RELEASE_EXPIRED, now_ms)

    async def requeue(self, dlq_id: int, owner: str) -> bool:
        async with self._db.transaction() as tx:
            return await tx.write(SQL_DLQ_REQUEUE, dlq_id, owner) > 0

    async def resolve(self, dlq_id: int, owner: str) -> bool:
        """-> RESOLVED. The parked payload is never re-submitted (the fresh broker row was); it stays on the
        row for audit and for a later lineage merge of a CASH_FLOW (its account_id / broker_ts)."""
        async with self._db.transaction() as tx:
            return await tx.write(SQL_DLQ_RESOLVE, dlq_id, owner) > 0

    async def backoff(self, dlq_id: int, owner: str, retry_count: int, next_retry_at_ms: int,
                      last_error: str) -> bool:
        async with self._db.transaction() as tx:
            return await tx.write(SQL_DLQ_BACKOFF, dlq_id, owner, retry_count, next_retry_at_ms,
                                  last_error[:2000]) > 0

    async def quarantine(self, dlq_id: int, owner: str | None, reason: str) -> bool:
        async with self._db.transaction() as tx:
            row = await tx.write_returning(SQL_DLQ_QUARANTINE_MARK, dlq_id, owner, reason[:2000])
            if row is None:
                return False
            event_type, entity_id, adapter_id, raw, error_reason, retry_count = row
            account = _payload_field(str(raw), "account_id")
            if event_type == "ACCOUNT" and not isinstance(account, str):
                account = entity_id
            await tx.write(SQL_DLQ_QUARANTINE_INSERT, dlq_id, event_type, entity_id, adapter_id,
                           account if isinstance(account, str) else None, raw,
                           f"{error_reason}: {reason}"[:2000], int(retry_count), must=True)
        return True

    async def find_lineage_origin(self, event_type: DlqEventType, entity_id: str, error_reason: str,
                                  before_dlq_id: int, since_ms: int, *, adapter_id: str) -> DlqRecord | None:
        async with self._db.source.acquire() as conn:
            row = await conn.fetchrow(SQL_DLQ_LINEAGE, event_type, entity_id, error_reason, before_dlq_id, since_ms,
                                      adapter_id)
        return None if row is None else dlq_record_from_row(row)

    async def merge_into_origin(self, dlq_id: int, owner: str, origin_dlq_id: int, retry_count: int,
                                next_retry_at_ms: int) -> bool:
        try:
            async with self._db.transaction() as tx:
                if await tx.write(SQL_DLQ_MERGE_CHILD, dlq_id, owner, origin_dlq_id) == 0:
                    return False
                if await tx.write(SQL_DLQ_MERGE_ORIGIN, origin_dlq_id, retry_count, next_retry_at_ms, dlq_id) == 0:
                    raise _MergeRefused(origin_dlq_id)
        except _MergeRefused:
            log.warning("dlq %d: origin %d can no longer take a lineage merge", dlq_id, origin_dlq_id)
            return False
        return True

    async def cash_flow_since_ms(self, adapter_id: str) -> int | None:
        async with self._db.source.acquire() as conn:
            last = await conn.fetchrow(SQL_LAST_CASH_FLOW_TS, adapter_id)
            parked = await conn.fetch(SQL_PARKED_CASH_FLOWS, adapter_id)
        candidates: list[int] = []
        if last is not None and last[0] is not None:
            candidates.append(int(last[0]))
        for (raw,) in parked:
            ts = _payload_field(str(raw), "broker_ts")
            if isinstance(ts, int) and not isinstance(ts, bool):
                candidates.append(ts)
        return min(candidates) if candidates else None

    async def mark_transfers_unverified(self, adapter_id: str, account_id: str, at_ms: int) -> None:
        """Flag the account's current trading day. When the sweep has not written the day's row yet, a
        placeholder (sod_equity NULL) carries the flag; the sweep's day-roll insert fills it in."""
        roll = latest_roll_ms(at_ms)
        async with self._db.transaction() as tx:
            await tx.write(SQL_SOD_TRANSFERS_UNVERIFIED, adapter_id, account_id, trading_day_for_roll(roll), roll,
                           must=True)


__all__ = [
    "CURRENT_TABLES", "CorruptRowError", "CurrentTable", "DLQ_COLS", "PgDlqStore", "PgIntegrityDb",
    "committed_from_row", "dlq_record_from_row", "split_key",
]
