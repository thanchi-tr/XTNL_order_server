"""PostgreSQL implementations of the storage ports (src/ports.py) and the tiered StateCommitter.

* PgStateStore: one fenced READ COMMITTED transaction per entity: current-row version CAS
  (src.dto.decide), event_log append, outbox row (tier-persistence.md §1, §4; operations.md §2).
  Terminal-state guard (dto-contracts.md §5, IMPLEMENTATION_PLAN 6.4): a newer version whose status change
  is illegal against the **stored** row (an order leaving FILLED / CANCELLED / REJECTED or any SM1 edge
  outside `is_legal_transition`, a position going CLOSED -> OPEN) is never committed. In the same fenced
  transaction it is parked in `events_dlq` (`illegal_transition`, the row `dlq_row()` builds, so a re-park
  of the same DTO is a no-op on `payload_hash`), then `IllegalTransitionParked` is raised: the consumer
  counts the row as failed, so a reconcile pass that carried it keeps its stream gap open (the 4.9 rule),
  and pages. This holds whether or not the orchestrator still had the entity in memory.
* TieredStateCommitter: PgStateStore first, then Redis apply_state.lua after commit (StateCommitter,
  InstrumentSink). A NO_BOOT reply or a Redis error after commit queues the write in the cache's
  `NoBootQueue` for the SM8 replay (tier-persistence.md §2 step 5); `no_boot` exposes it.
* PgDlqSink: events_dlq insert (spill-drain exception) with deterministic-reason lineage (§5).
* PgStreamHealth / TieredStreamHealth: stream_health + conn: keys (StreamHealthSink).

Every state write below carries the fence `(SELECT epoch FROM engine_epoch WHERE id = 1) = $1`.
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Final

from redis.exceptions import RedisError

from src.cache.codec import dto_json
from src.cache.scripts import ApplyResult, EntityWrite, NoBootQueue, RedisState, RedisStreamHealth
from src.cache.scripts import entity_write, instrument_write
from src.db.entity_sql import ACCOUNTS, EntityTable, StatefulDTO, table_for
from src.db.epoch import EngineDb
from src.db.fence import FENCE, EpochMismatch, FencedTx
from src.db.pg import ConnectionSource, Row, is_connection_error
from src.dto import (AccountLive, CashFlowDTO, Decision, InstrumentSpec, OrderDTO, OrderStatus, PositionDTO, PriceDTO,
                     decide, is_legal_transition)
from src.ports import CommitOutcome, DlqRow, StreamKind
from src.pubsub.dlq_rows import DETERMINISTIC_REASONS, ILLEGAL_TRANSITION, dlq_row

log = logging.getLogger(__name__)

CLEAN_MARK_INTERVAL_MS: Final = 10_000

SQL_EVENT_LOG: Final = (
    "INSERT INTO event_log (seq_id, entity_type, entity_key, adapter_id, account_id, action, version, checksum, "
    "payload, epoch_revision) SELECT $2::bigint, $3::text, $4::text, $5::text, $6::text, $7::text, $8::text, "
    f"$9::text, $10::text::jsonb, $1::bigint WHERE {FENCE}"
)
SQL_OUTBOX: Final = (
    "INSERT INTO outbox_events (seq_id, topic, entity_type, entity_key, payload) "
    f"SELECT $2::bigint, $3::text, $4::text, $5::text, $6::text::jsonb WHERE {FENCE}"
)
SQL_ACCOUNT_SNAPSHOT: Final = (
    "INSERT INTO account_snapshots (adapter_id, account_id, seq_id, currency, balance, realised_pnl_today, "
    "version, checksum) SELECT $2::text, $3::text, $4::bigint, $5::text, $6::numeric, $7::numeric, $8::text, "
    f"$9::text WHERE {FENCE} ON CONFLICT DO NOTHING"
)
SQL_CASH_FLOW: Final = (
    "INSERT INTO cash_flows (adapter_id, transaction_id, account_id, kind, amount, currency, broker_ts, seq_id, "
    "checksum) SELECT $2::text, $3::text, $4::text, $5::text, $6::numeric, $7::text, $8::bigint, $9::bigint, "
    f"$10::text WHERE {FENCE} ON CONFLICT (adapter_id, transaction_id) DO NOTHING RETURNING 1"
)
SQL_INSTRUMENT_UPSERT: Final = (
    "INSERT INTO instruments (adapter_id, symbol, base_ccy, quote_ccy, pnl_ccy, contract_size, unit_kind, "
    "point_size, min_qty, swap_booked_to_balance, broker_symbol, version, seq_id, checksum, digest_hash) "
    "SELECT $2::text, $3::text, $4::text, $5::text, $6::text, $7::numeric, $8::text, $9::numeric, $10::numeric, "
    f"$11::boolean, $12::text, $13::text, $14::bigint, $15::text, $16::text WHERE {FENCE} "
    "ON CONFLICT (adapter_id, symbol) DO UPDATE SET base_ccy = EXCLUDED.base_ccy, quote_ccy = EXCLUDED.quote_ccy, "
    "pnl_ccy = EXCLUDED.pnl_ccy, contract_size = EXCLUDED.contract_size, unit_kind = EXCLUDED.unit_kind, "
    "point_size = EXCLUDED.point_size, min_qty = EXCLUDED.min_qty, "
    "swap_booked_to_balance = EXCLUDED.swap_booked_to_balance, broker_symbol = EXCLUDED.broker_symbol, "
    "version = EXCLUDED.version, seq_id = EXCLUDED.seq_id, checksum = EXCLUDED.checksum, "
    "digest_hash = EXCLUDED.digest_hash, committed_at_ms = now_ms() "
    'WHERE instruments.checksum <> EXCLUDED.checksum AND instruments.version COLLATE "C" < EXCLUDED.version COLLATE "C" '
    "RETURNING 1"
)
SQL_LAST_CLEAN_MARK: Final = (
    "INSERT INTO last_clean_marks (adapter_id, symbol, bid, ask, spread_pips, ts) "
    f"SELECT $2::text, $3::text, $4::numeric, $5::numeric, $6::numeric, $7::bigint WHERE {FENCE} "
    "ON CONFLICT (adapter_id, symbol) DO UPDATE SET bid = EXCLUDED.bid, ask = EXCLUDED.ask, "
    "spread_pips = EXCLUDED.spread_pips, ts = EXCLUDED.ts, written_at_ms = now_ms() "
    "WHERE last_clean_marks.ts < EXCLUDED.ts"
)
#: Deterministic re-park, idempotent per re-parked payload_hash ($6): the bump is recorded in
#: dlq_lineage_bumps (0009) in the same statement, so a re-delivered batch (spooler retry after an unknown
#: COMMIT, a spill re-drain) never counts the same failure twice. Returns (bumped rows, already counted):
#: (1, false) counted now; (0, true) counted before (nothing to insert); (0, false) no matching origin or a
#: lost fence (the caller inserts the row as a plain one).
SQL_DLQ_LINEAGE_BUMP: Final = (
    "WITH seen AS (SELECT 1 FROM dlq_lineage_bumps WHERE payload_hash = $6::text::bpchar), "  # char(64) PK index
    "ins AS (INSERT INTO dlq_lineage_bumps (payload_hash, origin_dlq_id) "
    "SELECT $6::text, d.dlq_id FROM events_dlq d WHERE d.dlq_id = $2::bigint AND d.entity_id = $3::text "
    "AND d.error_reason = $4::text AND d.adapter_id = $5::text "   # entity ids are unique only per adapter
    f"AND NOT EXISTS (SELECT 1 FROM seen) AND {FENCE} "
    "ON CONFLICT (payload_hash) DO NOTHING RETURNING origin_dlq_id), "
    "upd AS (UPDATE events_dlq SET retry_count = retry_count + 1, status = 'QUEUED', next_retry_at_ms = now_ms(), "
    "updated_at_ms = now_ms() WHERE dlq_id IN (SELECT origin_dlq_id FROM ins) RETURNING dlq_id) "
    "SELECT (SELECT count(*) FROM upd)::int, EXISTS (SELECT 1 FROM seen)"
)
#: Terminal-state guard: park the refused row in the same fenced transaction (idempotent on payload_hash; a
#: hash already moved to events_dlq_history is not re-parked, like the spill drain).
SQL_PARK_ILLEGAL: Final = (
    "INSERT INTO events_dlq (event_type, entity_id, adapter_id, raw_payload, payload_hash, error_reason) "
    "SELECT $2::text, $3::text, $4::text, $5::text, $6::text, $7::text "
    f"WHERE {FENCE} AND NOT EXISTS (SELECT 1 FROM events_dlq_history x WHERE x.payload_hash = $6::text::bpchar) "
    "ON CONFLICT (payload_hash) DO NOTHING"
)
SQL_STREAM_LOST: Final = (
    "INSERT INTO stream_health (adapter_id, stream, conn_lost_at, updated_at_ms) "
    f"SELECT $2::text, $3::text, now_ms(), now_ms() WHERE {FENCE} "
    "ON CONFLICT (adapter_id, stream) DO UPDATE SET "
    "conn_lost_at = COALESCE(stream_health.conn_lost_at, EXCLUDED.conn_lost_at), updated_at_ms = now_ms()"
)
SQL_STREAM_RECONCILED: Final = (
    "INSERT INTO stream_health (adapter_id, stream, conn_lost_at, reconciled_at, reconciled_epoch, updated_at_ms) "
    f"SELECT $2::text, $3::text, NULL, now_ms(), $1::bigint, now_ms() WHERE {FENCE} "
    "ON CONFLICT (adapter_id, stream) DO UPDATE SET "
    "conn_lost_at = NULL, reconciled_at = now_ms(), reconciled_epoch = EXCLUDED.reconciled_epoch, "
    "updated_at_ms = now_ms()"
)
SQL_MAX_SEQ_ID: Final = (
    "SELECT GREATEST((SELECT COALESCE(MAX(seq_id), 0) FROM event_log), "
    "(SELECT COALESCE(MAX(seq_id), 0) FROM cash_flows), (SELECT COALESCE(MAX(seq_id), 0) FROM orders_state), "
    "(SELECT COALESCE(MAX(seq_id), 0) FROM positions_state), (SELECT COALESCE(MAX(seq_id), 0) FROM account_state))"
)


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(frozen=True)
class StoredVersion:
    """The current row's version, shaped for src.dto.decide()."""

    broker_seq: int | None
    broker_updated_at_ms: int
    observed_seq: int
    seq_id: int
    checksum: str

    @staticmethod
    def from_row(row: Row) -> StoredVersion:
        return StoredVersion(None if row[0] is None else int(row[0]), int(row[1]), int(row[2]), int(row[3]),
                             str(row[4]).strip())


@dataclass(frozen=True)
class PgCommit:
    decision: Decision
    write: EntityWrite


class IllegalTransitionParked(Exception):
    """A newer version with an illegal status change against the stored row: parked in events_dlq
    (`illegal_transition`) in a committed transaction, the current row untouched. The message is safe to log
    (entity key and the two statuses only)."""

    reason: Final = ILLEGAL_TRANSITION

    def __init__(self, entity_key: str, stored_status: str, new_status: str) -> None:
        super().__init__(f"illegal transition {stored_status} -> {new_status} for {entity_key}: parked in events_dlq")
        self.entity_key = entity_key
        self.stored_status = stored_status
        self.new_status = new_status


def illegal_transition(dto: StatefulDTO, stored_status: object) -> tuple[str, str] | None:
    """(stored, new) when moving the stored row to `dto` breaks SM1 / SM7, else None. An unknown stored status
    (an account, or a value this code does not know) is never a violation."""
    if stored_status is None:
        return None
    old = str(stored_status).strip()
    if isinstance(dto, OrderDTO):
        try:
            prev = OrderStatus(old)
        except ValueError:
            return None
        return None if is_legal_transition(prev, dto.status) else (old, dto.status.value)
    if isinstance(dto, PositionDTO):
        return (old, dto.status) if old == "CLOSED" and dto.status == "OPEN" else None
    return None


class PgStateStore:
    """PostgreSQL half of the StateCommitter."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def _decide(self, tx: FencedTx, t: EntityTable, dto: StatefulDTO,
                      w: EntityWrite) -> tuple[Decision, bool, object]:
        """Lock the current row and compare; a missing row is inserted at once. Returns (decision, inserted,
        stored status); the stored status is None after an insert or for a table without a lifecycle.

        A 0-row insert is re-checked against the fence (EpochMismatch) before it is read as a lost race."""
        key = t.key_values(dto)
        row = await tx.fetchrow(t.select_for_update, *key)
        if row is None:
            if await tx.write_returning(t.insert_new, *t.insert_params(dto, w)) is not None:
                return Decision.APPLY, True, None
            row = await tx.fetchrow(t.select_for_update, *key)  # lost an insert race: compare with the winner
            if row is None:
                raise RuntimeError(f"{t.table} row vanished during commit")
        status = row[5] if t.status_col and len(row) > 5 else None
        return decide(StoredVersion.from_row(row), dto), False, status

    @staticmethod
    async def _park_illegal(tx: FencedTx, dto: StatefulDTO) -> None:
        r = dlq_row(dto, ILLEGAL_TRANSITION)
        await tx.write(SQL_PARK_ILLEGAL, r.event_type, r.entity_id, r.adapter_id, r.raw_payload, r.payload_hash,
                       r.error_reason)

    @staticmethod
    async def _log(tx: FencedTx, entity_type: str, key: str, adapter_id: str, account_id: str | None,
                   action: str, seq_id: int, version: str | None, checksum: str | None, payload: str) -> None:
        await tx.write(SQL_EVENT_LOG, seq_id, entity_type, key, adapter_id, account_id, action, version, checksum,
                       payload, must=True)

    async def commit_entity(self, dto: StatefulDTO) -> PgCommit:
        t = table_for(dto)
        w = entity_write(dto)
        illegal: tuple[str, str] | None = None
        async with self._db.transaction() as tx:
            decision, inserted, stored_status = await self._decide(tx, t, dto, w)
            if decision is Decision.APPLY and not inserted:
                illegal = illegal_transition(dto, stored_status)
            if illegal is not None:
                await self._park_illegal(tx, dto)     # the current row, event_log and outbox stay untouched
            elif decision is Decision.APPLY:
                if not inserted:
                    await tx.write(t.update_apply, *t.apply_params(dto, w), must=True)
                await self._log(tx, t.entity_type, w.key, dto.adapter_id, dto.account_id, "APPLIED", dto.seq_id,
                                w.version, dto.checksum, w.payload)
                await tx.write(SQL_OUTBOX, dto.seq_id, t.topic, t.entity_type, w.key, w.payload, must=True)
                if t is ACCOUNTS:
                    await tx.write(SQL_ACCOUNT_SNAPSHOT, dto.adapter_id, dto.account_id, dto.seq_id,
                                   getattr(dto, "currency"), getattr(dto, "balance"),
                                   getattr(dto, "realised_pnl_today"), w.version, dto.checksum)
            elif decision is Decision.NOOP_ADVANCE:
                await tx.write(t.update_advance, *t.advance_params(dto, w), must=True)
            elif decision is Decision.DROP_STALE:
                await self._log(tx, t.entity_type, w.key, dto.adapter_id, dto.account_id, "DROPPED_STALE",
                                dto.seq_id, w.version, dto.checksum, w.payload)
        if illegal is not None:
            log.error("%s: illegal transition %s -> %s refused by the store and parked (%s)", dto.adapter_id,
                      illegal[0], illegal[1], w.key)
            raise IllegalTransitionParked(w.key, illegal[0], illegal[1])
        return PgCommit(decision, w)

    async def commit_cash_flow(self, cf: CashFlowDTO) -> bool:
        payload = dto_json(cf)
        async with self._db.transaction() as tx:
            new = await tx.write_returning(SQL_CASH_FLOW, cf.adapter_id, cf.transaction_id, cf.account_id, cf.kind,
                                           cf.amount, cf.currency, cf.broker_ts, cf.seq_id, cf.checksum) is not None
            if new:
                key = f"cash_flow:{cf.adapter_id}:{cf.transaction_id}"
                await self._log(tx, "CASH_FLOW", key, cf.adapter_id, cf.account_id, "CASH_FLOW", cf.seq_id, None,
                                cf.checksum, payload)
                await tx.write(SQL_OUTBOX, cf.seq_id, "account.state.dto", "CASH_FLOW", key, payload, must=True)
        return new

    @staticmethod
    async def _upsert_instrument(tx: FencedTx, spec: InstrumentSpec, seq_id: int) -> EntityWrite | None:
        w = instrument_write(spec, seq_id)
        changed = await tx.write_returning(
            SQL_INSTRUMENT_UPSERT, spec.adapter_id, spec.symbol, spec.base_ccy, spec.quote_ccy, spec.pnl_ccy,
            spec.contract_size, spec.unit_kind, spec.point_size, spec.min_qty, spec.swap_booked_to_balance,
            spec.broker_symbol, w.version, seq_id, w.checksum, w.digest_hash) is not None
        if not changed:
            return None
        await PgStateStore._log(tx, "INSTRUMENT", w.key, spec.adapter_id, None, "APPLIED", seq_id, w.version,
                                w.checksum, w.payload)
        return w

    async def commit_instrument(self, spec: InstrumentSpec, seq_id: int) -> EntityWrite | None:
        """Upsert when the content changed; returns the Redis write, or None when unchanged."""
        return (await self.commit_instruments([spec], seq_id))[0]

    async def commit_instruments(self, specs: Sequence[InstrumentSpec], seq_id: int) -> list[EntityWrite | None]:
        """One fenced transaction for an instrument refresh (SM4 SNAPSHOTTING)."""
        out: list[EntityWrite | None] = []
        if not specs:
            return out
        async with self._db.transaction() as tx:
            for spec in specs:
                out.append(await self._upsert_instrument(tx, spec, seq_id))
        return out

    async def upsert_last_clean_mark(self, p: PriceDTO) -> None:
        """last_clean_marks (operations.md §11), fenced; never moves a mark backwards in time."""
        async with self._db.transaction() as tx:
            await tx.write(SQL_LAST_CLEAN_MARK, p.adapter_id, p.symbol, p.bid, p.ask, p.spread_pips, p.timestamp_ms)


async def max_seq_id(source: ConnectionSource) -> int:
    """HLC re-seed value: MAX(seq_id) over the committed tables (dto-contracts.md §2)."""
    async with source.acquire() as conn:
        row = await conn.fetchrow(SQL_MAX_SEQ_ID)
    return 0 if row is None or row[0] is None else int(row[0])


class TieredStateCommitter:
    """StateCommitter + InstrumentSink: PostgreSQL commit first, Redis apply_state.lua after commit
    (tier-persistence.md §1)."""

    def __init__(
        self,
        store: PgStateStore,
        cache: RedisState,
        clean_spread: Callable[[str, str], Decimal | None],
        *,
        clock_ms: Callable[[], int] = _now_ms,
    ) -> None:
        self._store = store
        self._cache = cache
        self._clean_spread = clean_spread
        self._clock_ms = clock_ms
        self._last_mark_ms: dict[tuple[str, str], int] = {}
        self._latest_clean: dict[tuple[str, str], PriceDTO] = {}

    @property
    def no_boot(self) -> NoBootQueue:
        """Writes committed to PostgreSQL whose Redis copy is deferred until SM8 replays them."""
        return self._cache.no_boot

    async def commit_entity(self, dto: StatefulDTO) -> CommitOutcome:
        res = await self._store.commit_entity(dto)
        if res.decision is Decision.DROP_STALE:
            return CommitOutcome.STALE
        if res.decision is Decision.NOOP:
            return CommitOutcome.NOOP_ADVANCED   # identical re-observation: nothing to copy
        applied = await self._cache.apply_after_commit(res.write)
        if applied is ApplyResult.NO_BOOT:
            return CommitOutcome.NO_BOOT
        if applied is ApplyResult.STALE:
            # Redis ahead of PostgreSQL: the integrity worker DELs and re-applies (never a CAS fix).
            log.warning("redis ahead of postgres after commit: key=%s", res.write.key)
        return CommitOutcome.APPLIED if res.decision is Decision.APPLY else CommitOutcome.NOOP_ADVANCED

    async def commit_cash_flow(self, cf: CashFlowDTO) -> bool:
        new = await self._store.commit_cash_flow(cf)
        if new:
            try:
                await self._cache.feed_cash_flow(cf)
            except (RedisError, OSError) as exc:  # the sweep re-evaluates every account each second anyway
                log.warning("cash-flow feed entry lost: account=%s err=%s", cf.account_id, exc)
        return new

    async def commit_instrument(self, spec: InstrumentSpec, seq_id: int) -> bool:
        return await self.upsert_instruments([spec], seq_id) == 1

    async def upsert_instruments(self, specs: Sequence[InstrumentSpec], seq_id: int) -> int:
        """InstrumentSink: persist an instrument refresh; changed rows are copied to instrument: keys."""
        writes = [w for w in await self._store.commit_instruments(specs, seq_id) if w is not None]
        for w in writes:
            await self._cache.apply_after_commit(w)
        return len(writes)

    async def write_live(self, item: AccountLive | PriceDTO) -> None:
        try:
            if isinstance(item, AccountLive):
                await self._cache.write_account_live(item)
                return
            _, clean = await self._cache.write_price(item, self._clean_spread(item.adapter_id, item.symbol))
        except (RedisError, OSError) as exc:
            log.warning("live write failed (Redis is disposable): %s", exc)
            return
        if clean:
            await self._maybe_persist_clean(item)

    async def _maybe_persist_clean(self, p: PriceDTO) -> None:
        k = (p.adapter_id, p.symbol)
        self._latest_clean[k] = p
        now = self._clock_ms()
        if now - self._last_mark_ms.get(k, -CLEAN_MARK_INTERVAL_MS) >= CLEAN_MARK_INTERVAL_MS:
            await self._persist_mark(p, now)

    async def _persist_mark(self, p: PriceDTO, now: int) -> None:
        try:
            await self._store.upsert_last_clean_mark(p)
            self._last_mark_ms[(p.adapter_id, p.symbol)] = now
        except EpochMismatch:
            raise   # fence lost: the caller hard-exits
        except Exception as exc:  # retried on the next clean tick
            log.warning("last_clean_marks upsert failed: %s/%s %s", p.adapter_id, p.symbol, exc)

    async def force_clean_marks(self, adapter_id: str, symbols: Sequence[str] | None = None) -> int:
        """Forced upsert at each open / closed / roll transition (operations.md §11)."""
        done = 0
        for (a, s), p in list(self._latest_clean.items()):
            if a == adapter_id and (symbols is None or s in symbols):
                await self._persist_mark(p, self._clock_ms())
                done += 1
        return done


class PgDlqSink:
    """DlqSink: idempotent insert on payload_hash; ConnectionError when PostgreSQL is down."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def insert_batch(self, rows: Sequence[DlqRow]) -> None:
        try:
            plain = [r for r in rows if not self._is_lineage(r)]
            for r in rows:
                if self._is_lineage(r) and not await self._bump_origin(r):
                    plain.append(r)
            if plain:
                await self._db.spill_drain_insert(plain)
        except ConnectionError:
            raise
        except Exception as exc:
            if is_connection_error(exc):
                raise ConnectionError(f"PostgreSQL unavailable for events_dlq: {exc}") from exc
            raise

    @staticmethod
    def _is_lineage(r: DlqRow) -> bool:
        return r.origin_dlq_id is not None and r.error_reason in DETERMINISTIC_REASONS

    async def _bump_origin(self, r: DlqRow) -> bool:
        """Deterministic re-park: increment the original row instead of starting a new count. Idempotent
        per `payload_hash`: a row whose bump was already counted (a retried batch) is handled (True) without
        a second increment. False -> the caller inserts it as a plain row (no origin match, lost fence)."""
        try:
            async with self._db.transaction() as tx:
                row = await tx.write_returning(SQL_DLQ_LINEAGE_BUMP, r.origin_dlq_id, r.entity_id, r.error_reason,
                                               r.adapter_id, r.payload_hash)
        except EpochMismatch:
            return False   # a fenced node still delivers the row through the idempotent insert
        if row is None:
            return False
        return int(row[0] or 0) > 0 or bool(row[1])


class PgStreamHealth:
    """PostgreSQL half of StreamHealthSink: stream_health.conn_lost_at (the account fence)."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def mark_lost(self, adapter_id: str, stream: StreamKind) -> None:
        async with self._db.transaction() as tx:
            await tx.write(SQL_STREAM_LOST, adapter_id, stream, must=True)

    async def mark_reconciled(self, adapter_id: str, stream: StreamKind) -> None:
        async with self._db.transaction() as tx:
            await tx.write(SQL_STREAM_RECONCILED, adapter_id, stream, must=True)

    async def heartbeat(self, adapter_id: str, stream: StreamKind) -> None:
        return None  # liveness lives in Redis conn: keys only


class TieredStreamHealth:
    """StreamHealthSink: lost -> DEL conn: key then set conn_lost_at; reconciled -> clear, then refresh key."""

    def __init__(self, pg: PgStreamHealth, redis: RedisStreamHealth) -> None:
        self._pg = pg
        self._redis = redis

    async def mark_lost(self, adapter_id: str, stream: StreamKind) -> None:
        try:
            await self._redis.mark_lost(adapter_id, stream)
        except (RedisError, OSError) as exc:
            log.warning("conn: key delete failed: %s", exc)
        await self._pg.mark_lost(adapter_id, stream)

    async def mark_reconciled(self, adapter_id: str, stream: StreamKind) -> None:
        await self._pg.mark_reconciled(adapter_id, stream)
        try:
            await self._redis.mark_reconciled(adapter_id, stream)
        except (RedisError, OSError) as exc:
            log.warning("conn: key refresh failed: %s", exc)

    async def heartbeat(self, adapter_id: str, stream: StreamKind) -> None:
        try:
            await self._redis.heartbeat(adapter_id, stream)
        except (RedisError, OSError) as exc:
            log.warning("conn: heartbeat failed: %s", exc)
