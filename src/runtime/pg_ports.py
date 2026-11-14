"""PostgreSQL implementations of the leader ports (src/runtime/ports.py) and the connection pools.

Pools (operations.md §2, §4): connect timeout 3 s, statement_timeout 5 s (COMMIT runs with 0).
* The **control pool** (`xtnl-ctl:<node>`, every node): cluster_state / leader_terms reads, the
  health probe, the epoch bump. Its application_name does not match `^xtnl:[0-9]+:`, so a newer
  leader's post-bump backend termination never touches it.
* The **term pool** (`xtnl:<revision>:<node>`, one per leader term): every engine read and fenced
  write of that term; a newer leader terminates its backends after its own bump.
Both are created lazily on first use, so building the engine (and `--check-config`) connects nothing.

`GuardedEngineDb` turns any `EpochMismatch` (a lost fence) into the hard exit operations.md §3
requires, wherever it surfaces, so no store can swallow it.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any, Final

import redis.asyncio as aioredis

from src.cache.live_reader import RunIdReader, info_run_id
from src.calendar.schedule import CalendarSchedule, ScheduleError, SymbolKey, SymbolSchedule, symbol_schedule_from_json
from src.db.account_sampler import LiveSampleStore, PgLiveSampleStore
from src.db.auth_state import PgAdapterAuthStore
from src.db.epoch import EngineDb, Epoch, application_name, bump_epoch, spill_drain_insert
from src.db.fence import FENCE, EpochMismatch, FencedTx
from src.db.gate_reader import GateInputs, TieredGateContextReader
from src.db.halt_store import PgHaltStore
from src.db.integrity_store import PgDlqStore, PgIntegrityDb
from src.db.leader import leader_init_fence, mark_reached_leader, record_end_reason
from src.db.lifecycle import PartitionExporter, PartitionMaintenance, PgPartitionMaintenance
from src.db.local_state import PgLocalStateReader
from src.db.rows import ACCOUNT_COLS, account_from_row
from src.db.market_sessions import PgMarketSessionStore
from src.db.operator_requests import SQL_PENDING_WITH_PAYLOAD
from src.db.notify_store import PgDedupeStore, PgOutboxStore
from src.db.pg import AsyncpgSource, ConnectionSource, PgConnection, create_asyncpg_source, is_connection_error
from src.db.quarantine import PgQuarantineSink
from src.db.repository import PgDlqSink, PgStateStore, PgStreamHealth, max_seq_id
from src.db.ruleset_store import PgRulesetStore
from src.db.settings import redact
from src.dto import AccountDTO
from src.db.snapshot_reader import PgSnapshotReader, SnapshotInputs
from src.db.sweep_writer import PgSweepWriter
from src.db.uptime_store import PgCatchupStore
from src.oversee.gate import GateContextReader
from src.oversee.ruleset import RulesetSource
from src.oversee.snapshot import SnapshotReader
from src.ports import DlqRow
from src.runtime.catchup import CatchupStore
from src.runtime.leader_init import FenceOutcome
from src.runtime.ports import (
    AuthRearmReader,
    LeaderPorts,
    MarketSessionStore,
    OperatorRequest,
    OperatorRequestQueue,
    SodStatus,
)
from src.runtime.roles import EndReason

log = logging.getLogger(__name__)

SourceFactory = Callable[..., Awaitable[AsyncpgSource]]

SQL_LATEST_CALENDAR: Final = (
    "SELECT calendar_version, adapter_id, symbol, timezone, schedule, holidays FROM market_sessions "
    "WHERE calendar_version = (SELECT max(calendar_version) FROM market_sessions) ORDER BY adapter_id, symbol"
)
SQL_SOD_MISSING: Final = (
    "SELECT DISTINCT a.account_id FROM account_state a WHERE NOT EXISTS (SELECT 1 FROM sod_marks s "
    "WHERE s.account_id = a.account_id AND s.trading_day = $1::date AND s.sod_equity IS NOT NULL) ORDER BY 1"
)
SQL_SOD_STATUS: Final = (
    "SELECT count(DISTINCT account_id), COALESCE(bool_or(partial_session), false) FROM sod_marks "
    "WHERE trading_day = $1::date AND account_id = ANY($2::text[]) AND sod_equity IS NOT NULL"
)
SQL_PRIMARY_WRITABLE: Final = "SELECT NOT pg_is_in_recovery()"
SQL_AUTH_REARMED_AT: Final = (
    "SELECT state, rearmed_at_ms FROM adapter_auth_state WHERE adapter_id = $1 AND credential_generation = $2"
)
# 5.6: the pending requests with their payload (release_account / refresh_cash_flows, migration 0009).
SQL_OPERATOR_REQUESTS_PENDING: Final = SQL_PENDING_WITH_PAYLOAD
SQL_OPERATOR_REQUEST_CONSUME: Final = (
    "UPDATE operator_requests SET consumed_at_ms = now_ms(), consumed_epoch = $1::bigint, result = $3::text "
    f"WHERE request_id = $2::bigint AND consumed_at_ms IS NULL AND {FENCE}"
)


def safe_redact(url: str) -> str:
    """Alias of `src.db.settings.redact`, which keeps multi-host DSNs (h1:5432,h2:5432) and never raises."""
    return redact(url)


def control_application_name(node: str) -> str:
    return f"xtnl-ctl:{node}"[:63]


class LazyPgSource:
    """ConnectionSource over an asyncpg pool created on first `acquire()` (a failed creation is retried
    on the next acquire). `close()` closes the pool if it was created."""

    def __init__(self, dsn: str, *, application_name: str, min_size: int = 1, max_size: int = 10,
                 statement_timeout_ms: int = 5_000, connect_timeout_s: float = 3.0,
                 factory: SourceFactory = create_asyncpg_source) -> None:
        self._dsn = dsn
        self.application_name = application_name
        self._kw: dict[str, Any] = {"application_name": application_name, "min_size": min_size,
                                    "max_size": max_size, "statement_timeout_ms": statement_timeout_ms,
                                    "connect_timeout_s": connect_timeout_s}
        self._factory = factory
        self._source: AsyncpgSource | None = None
        self._lock = asyncio.Lock()
        self.closed = False

    def __repr__(self) -> str:
        return f"LazyPgSource({safe_redact(self._dsn)!r}, application_name={self.application_name!r})"

    @property
    def created(self) -> bool:
        return self._source is not None

    async def _get(self) -> AsyncpgSource:
        if self.closed:
            raise ConnectionError(f"pool {self.application_name} is closed")
        async with self._lock:
            if self._source is None:
                self._source = await self._factory(self._dsn, **self._kw)
            return self._source

    @asynccontextmanager
    async def _acquire(self) -> AsyncIterator[PgConnection]:
        src = await self._get()
        async with src.acquire() as conn:
            yield conn

    def acquire(self) -> AbstractAsyncContextManager[PgConnection]:
        return self._acquire()

    async def close(self) -> None:
        self.closed = True
        src, self._source = self._source, None
        if src is not None:
            try:
                await asyncio.wait_for(src.close(), 5.0)
            except Exception as exc:
                log.warning("closing pool %s failed: %r", self.application_name, exc)


class GuardedEngineDb(EngineDb):
    """EngineDb whose fenced transactions report a lost fence (EpochMismatch) to `on_fence_lost`
    (the runtime hard-exits) before the exception propagates."""

    def __init__(self, source: ConnectionSource, epoch: Callable[[], Epoch],
                 on_fence_lost: Callable[[BaseException], None]) -> None:
        super().__init__(source, epoch)
        self._on_fence_lost = on_fence_lost

    @asynccontextmanager
    async def _guarded(self, token: int | None) -> AsyncIterator[FencedTx]:
        try:
            async with EngineDb.transaction(self, token) as tx:
                yield tx
        except EpochMismatch as exc:
            self._on_fence_lost(exc)
            raise

    def transaction(self, token: int | None = None) -> AbstractAsyncContextManager[FencedTx]:
        return self._guarded(token)


class PgEpochFencer:
    """LEADER_INIT step 1: `bump_epoch(..., in_txn=leader_init_fence)` on the control pool."""

    def __init__(self, source: ConnectionSource, *,
                 on_safe_hold: Callable[[], Awaitable[object]] | None = None) -> None:
        self._source = source
        self._on_safe_hold = on_safe_hold

    async def bump_and_fence(self, epoch: Epoch, node_id: str, now_ms: int) -> FenceOutcome:
        async def in_txn(tx: PgConnection) -> Any:
            return await leader_init_fence(tx, epoch, node_id, now_ms)

        async with self._source.acquire() as conn:
            res = await bump_epoch(conn, epoch, node_id, in_txn=in_txn)
        if res is None:
            raise RuntimeError("epoch bump returned no LEADER_INIT fence result")
        out = FenceOutcome(bool(res.safe_hold), res.safe_hold_reason, int(res.streams_fenced),
                           int(res.predecessors_closed))
        if out.safe_hold and self._on_safe_hold is not None:
            try:
                await self._on_safe_hold()
            except Exception as exc:          # the PostgreSQL row is the truth; the mirror is best effort
                log.warning("SAFE_HOLD etcd mirror failed: %r", exc)
        return out


class PgSeqSource:
    def __init__(self, source: ConnectionSource) -> None:
        self._source = source

    async def max_seq_id(self) -> int:
        return await max_seq_id(self._source)


class PgTermWriter:
    """leader_terms.reached_leader_at (LEADER entry) and end_reason (DRAINING), fenced."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def mark_reached_leader(self, epoch: Epoch, now_ms: int) -> None:
        async with self._db.transaction(epoch.token) as tx:
            await mark_reached_leader(tx, epoch, now_ms)

    async def record_end_reason(self, epoch: Epoch, reason: EndReason) -> None:
        async with self._db.transaction(epoch.token) as tx:
            await record_end_reason(tx, epoch, reason)


class PgOperatorRequests:
    """OperatorRequestQueue over `operator_requests` (migration 0007). The consume is fenced (`$1` = epoch); a
    request another consume already marked is a legitimate 0-row result (the fence is re-checked)."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def pending(self, epoch_token: int) -> Sequence[OperatorRequest]:
        async with self._db.source.acquire() as conn:
            rows = await conn.fetch(SQL_OPERATOR_REQUESTS_PENDING, epoch_token)
        return [OperatorRequest(int(r[0]), str(r[1]), str(r[2]), str(r[3]), r[4] if len(r) > 4 else None)
                for r in rows]

    async def consume(self, epoch_token: int, request_id: int, result: str) -> None:
        async with self._db.transaction(epoch_token) as tx:
            await tx.write(SQL_OPERATOR_REQUEST_CONSUME, request_id, result[:200])


class PgAuthRearms:
    """AuthRearmReader over `adapter_auth_state`: a marker (`rearmed_at_ms`) only for a row an operator set to
    REARMED (a missing row, e.g. an AUTH_FAILED record that could not be written, is never read as a re-arm)."""

    def __init__(self, source: ConnectionSource) -> None:
        self._source = source

    async def rearmed_at(self, adapter_id: str, credential_generation: str) -> int | None:
        async with self._source.acquire() as conn:
            row = await conn.fetchrow(SQL_AUTH_REARMED_AT, adapter_id, credential_generation)
        if row is None or str(row[0]) != "REARMED":
            return None
        return int(row[1]) if row[1] is not None else 0


def _json_value(v: object) -> Any:
    return json.loads(v) if isinstance(v, (str, bytes)) else v


class PgScheduleLoader:
    """ScheduleLoader over `market_sessions` (the latest calendar_version). A row that does not parse
    is skipped with an error log (its symbol then has no SM10 state); no rows -> version 0, empty."""

    def __init__(self, source: ConnectionSource) -> None:
        self._source = source
        self.skipped: list[str] = []

    async def load(self) -> CalendarSchedule:
        async with self._source.acquire() as conn:
            rows = await conn.fetch(SQL_LATEST_CALENDAR)
        symbols: dict[SymbolKey, SymbolSchedule] = {}
        version = 0
        self.skipped = []
        for r in rows:
            version = int(r[0])
            adapter_id, symbol = str(r[1]), str(r[2])
            try:
                holidays = _json_value(r[5]) or []
                symbols[(adapter_id, symbol)] = symbol_schedule_from_json(
                    adapter_id, symbol, str(r[3]), _json_value(r[4]), holidays)
            except (ScheduleError, ValueError, TypeError, KeyError) as exc:
                self.skipped.append(f"{adapter_id}:{symbol}")
                log.error("market_sessions v%d %s:%s unusable, skipped: %s", version, adapter_id, symbol, exc)
        if not rows:
            log.warning("market_sessions is empty: no SM10 schedule (calendar version 0)")
        return CalendarSchedule(version, symbols)


class PgSodLedger:
    def __init__(self, source: ConnectionSource) -> None:
        self._source = source

    async def missing_accounts(self, trading_day: date) -> Sequence[str]:
        async with self._source.acquire() as conn:
            return [str(r[0]) for r in await conn.fetch(SQL_SOD_MISSING, trading_day)]

    async def status(self, trading_day: date, accounts: Sequence[str]) -> SodStatus:
        async with self._source.acquire() as conn:
            row = await conn.fetchrow(SQL_SOD_STATUS, trading_day, list(accounts))
        if row is None:
            return SodStatus(0, False)
        return SodStatus(int(row[0] or 0), bool(row[1]))


class SpillOnlySink:
    """DlqSink for a node that is not leader: the idempotent spill-drain insert only (named exception 2),
    so any node delivers its own spill file. ConnectionError when PostgreSQL is down."""

    def __init__(self, source: ConnectionSource) -> None:
        self._source = source

    async def insert_batch(self, rows: Sequence[DlqRow]) -> None:
        try:
            async with self._source.acquire() as conn:
                await spill_drain_insert(conn, rows)
        except ConnectionError:
            raise
        except Exception as exc:
            if is_connection_error(exc):
                raise ConnectionError(f"PostgreSQL unavailable for events_dlq: {type(exc).__name__}") from exc
            raise


async def primary_writable(source: ConnectionSource) -> bool:
    async with source.acquire() as conn:
        row = await conn.fetchrow(SQL_PRIMARY_WRITABLE)
    return row is not None and bool(row[0])


#: The stored account rows of one adapter (broker_seq inheritance after a restart, IMPLEMENTATION_PLAN 7.1).
SQL_ADAPTER_ACCOUNTS: Final = f"SELECT {ACCOUNT_COLS} FROM account_state WHERE adapter_id = $1 ORDER BY account_id"


class PgLocalStateWithAccounts(PgLocalStateReader):
    """PgLocalStateReader plus the orchestrator's optional `account_states(adapter_id)` (IMPLEMENTATION_PLAN 7.1):
    the stored `account_state` rows of the adapter, read once before its first snapshot of a term, so an account
    row without broker_seq (cTrader ACCOUNT declares broker_seq) inherits the stored one after a restart or a
    leader change instead of opening the gap. A plain read, no fence (it writes nothing)."""

    def __init__(self, db: EngineDb) -> None:
        super().__init__(db)
        self._source = db.source

    async def account_states(self, adapter_id: str) -> Sequence[AccountDTO]:
        async with self._source.acquire() as conn:
            return [account_from_row(r) for r in await conn.fetch(SQL_ADAPTER_ACCOUNTS, adapter_id)]


@dataclass(frozen=True)
class PgLeaderPorts(LeaderPorts):
    """LeaderPorts plus the term's optional housekeeping ports (IMPLEMENTATION_PLAN 4.5 / 4.8 / 4.9 / 5.6). A
    session built on plain LeaderPorts (the in-memory e2e ports) runs without them: base ruleset, no live
    account sampling, no partition maintenance, no operator requests, no operator re-arm polling and no
    broker schedule fetch (the calendar then runs from whatever market_sessions holds)."""

    rulesets: RulesetSource | None = None
    live_samples: LiveSampleStore | None = None
    lifecycle: PartitionMaintenance | None = None
    operator_requests: OperatorRequestQueue | None = None
    auth_rearms: AuthRearmReader | None = None
    market_sessions: MarketSessionStore | None = None
    catchup: CatchupStore | None = None      # downtime catch-up (migration 0012; src/runtime/catchup.py)


def pg_leader_ports(epoch: Epoch, *, control: ConnectionSource, dsn: str, node_id: str, redis: aioredis.Redis,
                    on_fence_lost: Callable[[BaseException], None],
                    on_safe_hold: Callable[[], Awaitable[object]] | None = None,
                    run_id_reader: RunIdReader = info_run_id, pool_max_size: int = 10,
                    source_factory: SourceFactory = create_asyncpg_source) -> LeaderPorts:
    """The production ports of one term. Nothing connects until first use."""
    term = LazyPgSource(dsn, application_name=application_name(epoch, node_id), max_size=pool_max_size,
                        factory=source_factory)
    db = GuardedEngineDb(term, lambda: epoch, on_fence_lost)

    def snapshot_reader(inputs: SnapshotInputs) -> SnapshotReader:
        return PgSnapshotReader(db, redis, inputs)

    def gate_reader(inputs: GateInputs) -> GateContextReader:
        return TieredGateContextReader(db, redis, inputs, run_id_reader)

    return PgLeaderPorts(
        fencer=PgEpochFencer(control, on_safe_hold=on_safe_hold), seq=PgSeqSource(term), terms=PgTermWriter(db),
        state_store=PgStateStore(db), stream_health=PgStreamHealth(db), dlq_sink=PgDlqSink(db),
        local_state=PgLocalStateWithAccounts(db), auth_store=PgAdapterAuthStore(db), snapshot_reader=snapshot_reader,
        sweep_writer=PgSweepWriter(db), halt_store=PgHaltStore(db), outbox=PgOutboxStore(db),
        dedupe=PgDedupeStore(db), feed_quarantine=PgQuarantineSink(db), integrity_db=PgIntegrityDb(term),
        dlq_store=PgDlqStore(db), schedule=PgScheduleLoader(term), gate_reader=gate_reader, sod=PgSodLedger(term),
        close=term.close, rulesets=PgRulesetStore(db, node_id), live_samples=PgLiveSampleStore(db),
        lifecycle=PgPartitionMaintenance(db, exporter=PartitionExporter.from_env(db, node_id)), operator_requests=PgOperatorRequests(db), auth_rearms=PgAuthRearms(term),
        market_sessions=PgMarketSessionStore(db), catchup=PgCatchupStore(db))
