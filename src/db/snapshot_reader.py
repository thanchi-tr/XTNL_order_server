"""SnapshotReader (src/oversee/snapshot.py) over PostgreSQL + Redis (oversee-rules.md §2 Inputs).

One READ ONLY REPEATABLE READ transaction per account (named exception 1, operations.md §2) reads every
PostgreSQL input, then the Redis live keys are read with their timestamps. Redis is optional: on a Redis
error the snapshot carries `redis_ok=False`, no live quotes and no account_live, so marks fall back to
`last_clean_marks` (stale, so capped) and live fields are UNKNOWN.

Inputs the database cannot know yet are injected (`SnapshotInputs`): the SM10 session per symbol, SM3's
integrity incident flag, per-adapter TRADE_PNL / transfer sources and scheduled closes before the roll.
`leaderless_before_roll` is derived from `leader_terms` (no term covering the 5 min before the roll), and the
terms themselves go into the snapshot (`AccountSnapshot.leader_terms`, read from the earliest held-symbol
reference point - its last scheduled close before the roll - to the roll), so the sod row's leaderless test runs
per held symbol from its own reference point (tier-persistence.md §4). The
day's `sod_marks.transfers_unverified` / `pnl_unverified` turn `transfer_source` / `pnl_source` off, so
MaxDailyLossRule FLAGs the day (`transfers_unverified` / `pnl_unverified`) instead of trusting the ledger.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Final

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from src.cache.live_reader import LiveQuote, read_account_live, read_quotes
from src.db.epoch import EngineDb
from src.db.oversee_errors import oversee_errors
from src.db.pg import PgConnection
from src.db.rows import (
    ACCOUNT_COLS,
    ACK_COLS,
    BREACH_COLS,
    CASH_FLOW_COLS,
    INSTRUMENT_COLS,
    ORDER_COLS,
    POSITION_COLS,
    SOD_COLS,
    account_from_row,
    ack_from_row,
    breach_from_row,
    cash_flow_from_row,
    instrument_from_row,
    order_from_row,
    position_from_row,
    sod_from_row,
    stream_health_from_row,
    to_decimal,
)
from src.dto import AccountDTO, AccountLive, InstrumentSpec, OrderDTO, PositionDTO
from src.oversee.dayroll import LEADERLESS_WINDOW_MS, latest_roll_ms, previous_roll_ms, terms_cover
from src.oversee.dayroll import trading_day_for_roll
from src.oversee.snapshot import (
    AccountSnapshot,
    BreachAck,
    CashFlowRow,
    CleanMark,
    SessionState,
    SodMark,
    StopWatchKey,
    StreamHealthRow,
    SymbolQuotes,
    TimedMid,
)
from src.oversee.types import BreachState, SymbolKey

log = logging.getLogger(__name__)

OPEN_ORDER_STATUSES: Final = ("PENDING", "WORKING", "PARTIALLY_FILLED")

SQL_LIST_ACCOUNTS: Final = (
    "SELECT account_id FROM account_state "
    "UNION SELECT account_id FROM positions_state WHERE status = 'OPEN' "
    "UNION SELECT account_id FROM orders_state WHERE status IN ('PENDING', 'WORKING', 'PARTIALLY_FILLED') "
    "ORDER BY 1"
)
SQL_ACCOUNT: Final = f"SELECT {ACCOUNT_COLS} FROM account_state WHERE account_id = $1 ORDER BY adapter_id LIMIT 1"
SQL_ADAPTER_OF: Final = (
    "SELECT adapter_id FROM positions_state WHERE account_id = $1 "
    "UNION SELECT adapter_id FROM orders_state WHERE account_id = $1 ORDER BY 1 LIMIT 1"
)
SQL_OPEN_ORDERS: Final = (
    f"SELECT {ORDER_COLS} FROM orders_state WHERE adapter_id = $1 AND account_id = $2 "
    "AND status IN ('PENDING', 'WORKING', 'PARTIALLY_FILLED') ORDER BY broker_order_id"
)
SQL_OPEN_POSITIONS: Final = (
    f"SELECT {POSITION_COLS} FROM positions_state WHERE adapter_id = $1 AND account_id = $2 AND status = 'OPEN' "
    "ORDER BY position_id"
)
SQL_LAST_CHANGE: Final = (
    "SELECT GREATEST((SELECT max(status_units_changed_at_ms) FROM orders_state WHERE adapter_id = $1 AND account_id = $2), "
    "(SELECT max(status_units_changed_at_ms) FROM positions_state WHERE adapter_id = $1 AND account_id = $2))"
)
SQL_INSTRUMENTS: Final = f"SELECT {INSTRUMENT_COLS} FROM instruments WHERE adapter_id = $1"
SQL_CLEAN_MARKS: Final = "SELECT symbol, bid, ask, ts FROM last_clean_marks WHERE adapter_id = $1"
SQL_CASH_FLOWS_SINCE: Final = (
    f"SELECT {CASH_FLOW_COLS} FROM cash_flows WHERE adapter_id = $1 AND account_id = $2 "
    "AND (broker_ts >= $3 OR committed_at_ms >= $3) ORDER BY broker_ts, transaction_id"
)
SQL_CASH_FLOWS_BETWEEN: Final = (
    f"SELECT {CASH_FLOW_COLS} FROM cash_flows WHERE adapter_id = $1 AND account_id = $2 "
    "AND broker_ts >= $3 AND broker_ts < $4 ORDER BY broker_ts, transaction_id"
)
SQL_SOD: Final = f"SELECT {SOD_COLS} FROM sod_marks WHERE adapter_id = $1 AND account_id = $2 AND trading_day = $3"
# The day's verification flags (tier-persistence.md §4): a day marked transfers_unverified (no transfer source,
# or a cash flow quarantined unconverged) or pnl_unverified makes MaxDailyLossRule FLAG instead of trusting
# net_transfers_today / the P&L split as complete.
SQL_SOD_FLAGS: Final = (
    "SELECT transfers_unverified, pnl_unverified FROM sod_marks WHERE adapter_id = $1 AND account_id = $2 "
    "AND trading_day = $3"
)
SQL_PREV_SOD: Final = (
    f"SELECT {SOD_COLS} FROM sod_marks WHERE adapter_id = $1 AND account_id = $2 AND trading_day < $3 "
    "ORDER BY trading_day DESC LIMIT 1"
)
SQL_STOP_WATCH: Final = (
    "SELECT entity_type, entity_id, effective_stop_missing_since_ms FROM stop_watch WHERE account_id = $1"
)
SQL_STREAM_HEALTH: Final = "SELECT adapter_id, stream, conn_lost_at FROM stream_health WHERE adapter_id = $1"
SQL_BREACHES: Final = (
    f"SELECT {BREACH_COLS} FROM halt_breaches WHERE account_id = $1 "
    "AND (resolved_at IS NULL OR subject_id = ANY($2::text[])) ORDER BY rule_id, subject_type, subject_id"
)
SQL_LATEST_DROP: Final = (   # the cool-down mark of a state resolved with its entity (oversee-rules.md §2)
    f"SELECT {BREACH_COLS} FROM halt_breaches WHERE account_id = $1 AND resolved_at IS NOT NULL "
    "AND below_soft_since IS NOT NULL ORDER BY below_soft_since DESC LIMIT 1"
)
SQL_ACKS: Final = (
    f"SELECT {ACK_COLS} FROM breach_acks k JOIN halt_breaches b ON b.breach_id = k.breach_id "
    "WHERE b.account_id = $1 AND b.resolved_at IS NULL ORDER BY k.ack_id"
)
SQL_TERMS_NEAR: Final = (
    "SELECT COALESCE(reached_leader_at, started_at), ended_at FROM leader_terms "
    "WHERE COALESCE(reached_leader_at, started_at) <= $2 AND (ended_at IS NULL OR ended_at >= $1)"
)


def _always_open(adapter_id: str, symbol: str) -> SessionState:
    return "OPEN"


def _true(adapter_id: str) -> bool:
    return True


def _false() -> bool:
    return False


def _no_closes(adapter_id: str, roll_ms: int) -> Mapping[SymbolKey, int]:
    return MappingProxyType({})


@dataclass(frozen=True)
class SnapshotInputs:
    """Inputs owned by other components (SM10 calendar, SM3, adapter capabilities)."""

    session: Callable[[str, str], SessionState] = _always_open
    integrity_incident: Callable[[], bool] = _false
    pnl_source: Callable[[str], bool] = _true
    transfer_source: Callable[[str], bool] = _true
    scheduled_closes: Callable[[str, int], Mapping[SymbolKey, int]] = _no_closes
    extra_symbols: Callable[[str], Sequence[str]] = field(default=lambda adapter_id: ())


def covered(window: tuple[int, int], terms: Sequence[tuple[int, int | None]]) -> bool:
    """True when the union of leader terms [start, end or open] covers the whole window."""
    return terms_cover(window, terms)


@dataclass
class _PgPart:
    adapter_id: str
    account_id: str
    account: AccountDTO | None = None
    orders: tuple[OrderDTO, ...] = ()
    positions: tuple[PositionDTO, ...] = ()
    instruments: dict[SymbolKey, InstrumentSpec] = field(default_factory=dict)
    clean_marks: dict[SymbolKey, CleanMark] = field(default_factory=dict)
    cash_flows: tuple[CashFlowRow, ...] = ()
    sod: SodMark | None = None
    prev_sod: SodMark | None = None
    prev_flows: tuple[CashFlowRow, ...] = ()
    stop_watch: dict[StopWatchKey, int] = field(default_factory=dict)
    stream_health: tuple[StreamHealthRow, ...] = ()
    breaches: tuple[BreachState, ...] = ()
    acks: tuple[BreachAck, ...] = ()
    last_change_ms: int | None = None
    balance_committed_ms: int | None = None
    leaderless: bool = False
    terms: tuple[tuple[int, int | None], ...] | None = None
    closes: Mapping[SymbolKey, int] = field(default_factory=lambda: MappingProxyType({}))
    transfers_unverified: bool = False
    pnl_unverified: bool = False


class PgSnapshotReader:
    """SnapshotReader: PostgreSQL snapshot per account + optional Redis live keys. Writes nothing."""

    def __init__(self, db: EngineDb, redis: aioredis.Redis | None, inputs: SnapshotInputs = SnapshotInputs()) -> None:
        self._db = db
        self._redis = redis
        self._in = inputs

    async def list_accounts(self) -> Sequence[str]:
        async with oversee_errors():
            async with self._db.source.acquire() as conn:
                return [str(r[0]) for r in await conn.fetch(SQL_LIST_ACCOUNTS)]

    async def read_account(self, account_id: str, now_ms: int) -> AccountSnapshot:
        async with oversee_errors():
            async with self._db.snapshot() as conn:
                part = await self._read_pg(conn, account_id, now_ms)
        quotes, live, redis_ok = await self._read_redis(part)
        return self._assemble(part, quotes, live, redis_ok, now_ms)

    # ---- PostgreSQL (one REPEATABLE READ snapshot) -----------------------------------------------

    async def _read_pg(self, conn: PgConnection, account_id: str, now_ms: int) -> _PgPart:
        acct_row = await conn.fetchrow(SQL_ACCOUNT, account_id)
        if acct_row is not None:
            adapter_id = str(acct_row[1])
        else:
            other = await conn.fetchrow(SQL_ADAPTER_OF, account_id)
            adapter_id = "" if other is None else str(other[0])
        p = _PgPart(adapter_id, account_id)
        if acct_row is not None:
            p.account = account_from_row(acct_row)
            p.balance_committed_ms = int(acct_row[10])
        p.orders = tuple(order_from_row(r) for r in await conn.fetch(SQL_OPEN_ORDERS, adapter_id, account_id))
        p.positions = tuple(position_from_row(r) for r in await conn.fetch(SQL_OPEN_POSITIONS, adapter_id, account_id))
        last = await conn.fetchrow(SQL_LAST_CHANGE, adapter_id, account_id)
        p.last_change_ms = None if last is None or last[0] is None else int(last[0])
        for r in await conn.fetch(SQL_INSTRUMENTS, adapter_id):
            spec = instrument_from_row(r)
            p.instruments[(spec.adapter_id, spec.symbol)] = spec
        for r in await conn.fetch(SQL_CLEAN_MARKS, adapter_id):
            p.clean_marks[(adapter_id, str(r[0]))] = CleanMark(to_decimal(r[1]), to_decimal(r[2]), int(r[3]))
        roll = latest_roll_ms(now_ms)
        day = trading_day_for_roll(roll)
        p.closes = self._in.scheduled_closes(adapter_id, roll)
        p.cash_flows = tuple(cash_flow_from_row(r) for r in await conn.fetch(SQL_CASH_FLOWS_SINCE, adapter_id,
                                                                              account_id, roll))
        sod_row = await conn.fetchrow(SQL_SOD, adapter_id, account_id, day)
        p.sod = None if sod_row is None else sod_from_row(sod_row)
        flags = await conn.fetchrow(SQL_SOD_FLAGS, adapter_id, account_id, day)
        if flags is not None:
            p.transfers_unverified, p.pnl_unverified = bool(flags[0]), bool(flags[1])
        if p.sod is None:   # the inputs for writing the day's row (dayroll.build_sod_mark)
            prev_row = await conn.fetchrow(SQL_PREV_SOD, adapter_id, account_id, day)
            p.prev_sod = None if prev_row is None else sod_from_row(prev_row)
            since = p.prev_sod.roll_ms if p.prev_sod is not None else previous_roll_ms(roll)
            p.prev_flows = tuple(cash_flow_from_row(r) for r in await conn.fetch(
                SQL_CASH_FLOWS_BETWEEN, adapter_id, account_id, since, roll))
            # From the earliest reference point a held symbol can have (its last scheduled close before the
            # roll) to the roll: build_sod_mark tests each held symbol's own 5 min window.
            earliest = min([roll, *(int(v) for v in p.closes.values() if int(v) <= roll)])
            terms = [(int(r[0]), None if r[1] is None else int(r[1]))
                     for r in await conn.fetch(SQL_TERMS_NEAR, earliest - LEADERLESS_WINDOW_MS, roll)]
            p.terms = tuple(sorted(terms))
            p.leaderless = not covered((roll - LEADERLESS_WINDOW_MS, roll), terms)
        for r in await conn.fetch(SQL_STOP_WATCH, account_id):
            p.stop_watch[(str(r[0]).lower(), str(r[1]))] = int(r[2])
        p.stream_health = tuple(stream_health_from_row(r) for r in await conn.fetch(SQL_STREAM_HEALTH, adapter_id))
        live_subjects = [account_id, f"{account_id}:{day.isoformat()}"]
        live_subjects += [o.entity_key for o in p.orders] + [q.entity_key for q in p.positions]
        p.breaches = tuple(breach_from_row(r) for r in await conn.fetch(SQL_BREACHES, account_id, live_subjects))
        # A state resolved because its order, position or trading day ended is not loaded above, but its
        # below_soft_since still bounds the cool-down; only the latest such time matters (MAX in §2).
        drop = await conn.fetchrow(SQL_LATEST_DROP, account_id)
        if drop is not None:
            dropped = breach_from_row(drop)
            if all(b.key != dropped.key for b in p.breaches):
                p.breaches += (dropped,)
        p.acks = tuple(ack_from_row(r) for r in await conn.fetch(SQL_ACKS, account_id))
        return p

    # ---- Redis (optional) ------------------------------------------------------------------------

    def _symbols(self, p: _PgPart) -> set[SymbolKey]:
        keys: set[SymbolKey] = set(p.instruments) | set(p.clean_marks)
        keys |= {(o.adapter_id, o.symbol) for o in p.orders} | {(q.adapter_id, q.symbol) for q in p.positions}
        keys |= {(p.adapter_id, s) for s in self._in.extra_symbols(p.adapter_id)}
        return keys

    async def _read_redis(self, p: _PgPart) -> tuple[dict[SymbolKey, LiveQuote], AccountLive | None, bool]:
        if self._redis is None:
            return {}, None, False
        try:
            quotes = await read_quotes(self._redis, self._symbols(p))
            live = await read_account_live(self._redis, p.account_id)
            return quotes, live, True
        except (RedisError, OSError) as exc:
            log.warning("redis read failed for sweep snapshot %s: %s", p.account_id, exc)
            return {}, None, False

    # ---- assembly --------------------------------------------------------------------------------

    def _assemble(self, p: _PgPart, live_quotes: Mapping[SymbolKey, LiveQuote], live: AccountLive | None,
                  redis_ok: bool, now_ms: int) -> AccountSnapshot:
        quotes: dict[SymbolKey, SymbolQuotes] = {}
        for key in set(live_quotes) | set(p.clean_marks):
            lq = live_quotes.get(key)
            quotes[key] = SymbolQuotes(
                latest=lq.latest if lq else None,
                clean_mid=TimedMid(*lq.clean_mid) if lq and lq.clean_mid else None,
                last_clean=p.clean_marks.get(key), session=self._in.session(*key))
        roll = latest_roll_ms(now_ms)
        return AccountSnapshot(
            account_id=p.account_id, adapter_id=p.adapter_id, read_at_ms=now_ms, account=p.account,
            orders=p.orders, positions=p.positions, instruments=MappingProxyType(dict(p.instruments)),
            quotes=MappingProxyType(quotes), account_live=live, cash_flows=p.cash_flows, sod=p.sod,
            prev_sod=p.prev_sod, prev_day_cash_flows=p.prev_flows, stop_watch=MappingProxyType(dict(p.stop_watch)),
            stream_health=p.stream_health, breaches=p.breaches, acks=p.acks,
            redis_ok=redis_ok, integrity_incident=self._in.integrity_incident(),
            last_status_or_units_change_ms=p.last_change_ms, balance_committed_at_ms=p.balance_committed_ms,
            pnl_source=self._in.pnl_source(p.adapter_id) and not p.pnl_unverified,
            transfer_source=self._in.transfer_source(p.adapter_id) and not p.transfers_unverified,
            scheduled_close_before_roll_ms=p.closes,
            leaderless_before_roll=p.leaderless, leader_terms=p.terms,
        )


__all__ = ["PgSnapshotReader", "SnapshotInputs", "covered"]
