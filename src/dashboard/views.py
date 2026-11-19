"""Dashboard data: read-only PostgreSQL views and freshness-gated Redis live values (IMPLEMENTATION_PLAN 5.6).

Every SQL statement here is a SELECT with a LIMIT, run on a connection whose session has
`default_transaction_read_only = on` (src/dashboard/server.py), as role `xtnl_dashboard`. Redis is only read
(GET / HGETALL), with the dashboard's own Redis ACL user (`redis:boot_id`, `price:*`, `account_live:*` only;
deploy/redis/redis.conf), through each node's TLS proxy.

Live values are shown only when the Redis read has `redis:boot_id` = `<run_id>:<epoch>` with `<epoch>` equal to
the epoch in the latest leader heartbeat (operations.md §9; IMPLEMENTATION_PLAN 7.4: the leader's copy of its
dead-man heartbeat, `src/dashboard/heartbeat.py`, no older than its `max_age_ms`), and that epoch is also the
current leader term's in PostgreSQL (the latest `leader_terms` row that reached LEADER and has not ended, and
whose revision is `engine_epoch.revision`). No fresh heartbeat (none configured, none received, a dead leader's
beats stopped), a heartbeat epoch PostgreSQL does not confirm, or any other Redis (a standby's, an ex-leader's,
one not rebuilt by SM8 yet) is reported as stale and its values are not shown. Each live value carries its age;
one older than `fresh_ms` is hidden.
"""
from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Final

import redis.asyncio as aioredis

from src.cache import keys as K
from src.cache.live_reader import account_live_from_hash, price_from_hash
from src.dashboard.heartbeat import HeartbeatBoard
from src.db.epoch import EngineDb, Epoch
from src.db.notify_store import PgOutboxStore
from src.db.pg import ConnectionSource

log = logging.getLogger(__name__)

ROW_LIMIT: Final = 500
DEFAULT_FRESH_MS: Final = 30_000

SQL_ORDERS: Final = (
    "SELECT adapter_id, broker_order_id, client_order_id, account_id, symbol, side, status, ordered_units, "
    "remaining_units, entry_price, stop_loss, take_profit, is_reduce_only, linked_position_id, order_kind, seq_id, "
    "committed_at_ms FROM orders_state "
    "ORDER BY (status IN ('PENDING', 'WORKING', 'PARTIALLY_FILLED')) DESC, committed_at_ms DESC LIMIT 500"
)
ORDER_COLS: Final = ("adapter_id", "broker_order_id", "client_order_id", "account_id", "symbol", "side", "status",
                     "ordered_units", "remaining_units", "entry_price", "stop_loss", "take_profit", "is_reduce_only",
                     "linked_position_id", "order_kind", "seq_id", "committed_at_ms")
SQL_POSITIONS: Final = (
    "SELECT adapter_id, position_id, account_id, symbol, side, units, entry_price, stop_loss, take_profit, status, "
    "accrued_swap, commission, seq_id, committed_at_ms FROM positions_state "
    "ORDER BY (status = 'OPEN') DESC, committed_at_ms DESC LIMIT 500"
)
POSITION_COLS: Final = ("adapter_id", "position_id", "account_id", "symbol", "side", "units", "entry_price",
                        "stop_loss", "take_profit", "status", "accrued_swap", "commission", "seq_id",
                        "committed_at_ms")
SQL_ACCOUNTS: Final = (
    "SELECT adapter_id, account_id, currency, balance, realised_pnl_today, seq_id, committed_at_ms "
    "FROM account_state ORDER BY adapter_id, account_id LIMIT 500"
)
ACCOUNT_COLS: Final = ("adapter_id", "account_id", "currency", "balance", "realised_pnl_today", "seq_id",
                       "committed_at_ms")
SQL_HALT: Final = (
    'SELECT id, level, effective_level, hard_latch, rule_id, reason, "by", at_ms, forced FROM halt_events '
    "ORDER BY id DESC LIMIT 1"
)
HALT_COLS: Final = ("halt_event_id", "level", "effective_level", "hard_latch", "rule_id", "reason", "by", "at_ms",
                    "forced")
SQL_RULE_STATES: Final = (
    "SELECT breach_id, rule_id, subject_type, subject_id, adapter_id, account_id, level, pending_level, cap, reason, "
    "pending_since, active_since, unknown_since, updated_at_ms FROM halt_breaches WHERE resolved_at IS NULL "
    "ORDER BY updated_at_ms DESC LIMIT 500"
)
RULE_COLS: Final = ("breach_id", "rule_id", "subject_type", "subject_id", "adapter_id", "account_id", "level",
                    "pending_level", "cap", "reason", "pending_since", "active_since", "unknown_since",
                    "updated_at_ms")
SQL_DLQ: Final = "SELECT status, count(*) FROM events_dlq GROUP BY status ORDER BY status LIMIT 10"
SQL_STREAMS: Final = (
    "SELECT adapter_id, stream, conn_lost_at, reconciled_at, reconciled_epoch, updated_at_ms FROM stream_health "
    "ORDER BY adapter_id, stream LIMIT 100"
)
STREAM_COLS: Final = ("adapter_id", "stream", "conn_lost_at", "reconciled_at", "reconciled_epoch", "updated_at_ms")
SQL_EPOCH: Final = "SELECT cluster_id, revision, node, bumped_at_ms FROM engine_epoch WHERE id = 1"
SQL_TERMS: Final = (
    "SELECT epoch_revision, node, started_at, reached_leader_at, ended_at, end_reason FROM leader_terms "
    "ORDER BY started_at DESC LIMIT 10"
)
TERM_COLS: Final = ("epoch", "node", "started_at", "reached_leader_at", "ended_at", "end_reason")
SQL_CLUSTER: Final = "SELECT safe_hold, safe_hold_since, safe_hold_reason FROM cluster_state WHERE id = 1"
SQL_OPEN_SYMBOLS: Final = (
    "SELECT DISTINCT adapter_id, symbol FROM positions_state WHERE status = 'OPEN' ORDER BY 1, 2 LIMIT 200"
)
#: Every statement the dashboard runs (tests assert they are all read-only SELECTs).
ALL_SQL: Final = (SQL_ORDERS, SQL_POSITIONS, SQL_ACCOUNTS, SQL_HALT, SQL_RULE_STATES, SQL_DLQ, SQL_STREAMS,
                  SQL_EPOCH, SQL_TERMS, SQL_CLUSTER, SQL_OPEN_SYMBOLS)

_READ_EPOCH: Final = Epoch("dashboard-read-only", 0)


def jsonable(v: Any) -> Any:
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace")
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    return v


def as_dicts(rows: Sequence[Sequence[Any]], cols: Sequence[str]) -> list[dict[str, Any]]:
    return [{c: jsonable(r[i]) for i, c in enumerate(cols)} for r in rows]


def _s(v: Any) -> str:
    return v.decode("utf-8") if isinstance(v, bytes) else str(v)


@dataclass(frozen=True)
class LeaderTerm:
    epoch: int | None           # engine_epoch.revision
    term_epoch: int | None      # the open leader term that reached LEADER (None: none)
    node: str | None
    bumped_at_ms: int | None
    safe_hold: bool | None
    terms: list[dict[str, Any]] = field(default_factory=list)

    @property
    def live_epoch(self) -> int | None:
        """The epoch Redis live values must carry: the current leader term's, when it is engine_epoch's."""
        if self.epoch is None or self.term_epoch is None or self.epoch != self.term_epoch:
            return None
        return self.epoch


class DashboardViews:
    """PostgreSQL views (read-only source) and Redis live values (read-only clients)."""

    def __init__(self, source: ConnectionSource, redis_clients: Mapping[str, aioredis.Redis] | None = None, *,
                 fresh_ms: int = DEFAULT_FRESH_MS, heartbeats: HeartbeatBoard | None = None) -> None:
        self._source = source
        self._redis = dict(redis_clients or {})
        self.fresh_ms = fresh_ms
        # The production entrypoint always passes a board (`src.dashboard.__main__.heartbeat_board`: without a
        # heartbeat file an unconfigured one, so every live value is stale). None = the caller checks no heartbeat
        # (library use, tests): only the PostgreSQL leader term decides.
        self.heartbeats = heartbeats
        self._outbox = PgOutboxStore(EngineDb(source, lambda: _READ_EPOCH))   # escalated_since() only (a read)

    def replace_redis(self, clients: Mapping[str, aioredis.Redis]) -> dict[str, aioredis.Redis]:
        """K10: use rebuilt Redis clients (rotated credentials); returns the replaced ones for the caller to close."""
        old, self._redis = self._redis, dict(clients)
        return old

    async def _fetch(self, sql: str, *args: object) -> list[Any]:
        async with self._source.acquire() as conn:
            return list(await conn.fetch(sql, *args))

    async def _fetchrow(self, sql: str) -> Any:
        async with self._source.acquire() as conn:
            return await conn.fetchrow(sql)

    async def orders(self) -> list[dict[str, Any]]:
        return as_dicts(await self._fetch(SQL_ORDERS), ORDER_COLS)

    async def positions(self) -> list[dict[str, Any]]:
        return as_dicts(await self._fetch(SQL_POSITIONS), POSITION_COLS)

    async def accounts(self) -> list[dict[str, Any]]:
        return as_dicts(await self._fetch(SQL_ACCOUNTS), ACCOUNT_COLS)

    async def halt(self) -> dict[str, Any] | None:
        row = await self._fetchrow(SQL_HALT)
        return None if row is None else as_dicts([row], HALT_COLS)[0]

    async def rule_states(self) -> list[dict[str, Any]]:
        return as_dicts(await self._fetch(SQL_RULE_STATES), RULE_COLS)

    async def dlq(self) -> dict[str, int]:
        return {_s(r[0]): int(r[1]) for r in await self._fetch(SQL_DLQ)}

    async def streams(self) -> list[dict[str, Any]]:
        return as_dicts(await self._fetch(SQL_STREAMS), STREAM_COLS)

    async def escalations(self, now_ms: int, window_ms: int = 24 * 3_600_000) -> list[dict[str, Any]]:
        rows = await self._outbox.escalated_since(now_ms - window_ms, 50)
        return [{"created_ms": e.row.created_ms, "severity": e.row.severity, "title": e.row.title,
                 "channels": list(e.row.channels), "delivered": e.delivered, "sent_at_ms": e.sent_at_ms,
                 "last_error": e.last_error} for e in rows]

    async def leader(self) -> LeaderTerm:
        ep = await self._fetchrow(SQL_EPOCH)
        terms = as_dicts(await self._fetch(SQL_TERMS), TERM_COLS)
        cl = await self._fetchrow(SQL_CLUSTER)
        current = next((t for t in terms if t["ended_at"] is None and t["reached_leader_at"] is not None), None)
        return LeaderTerm(
            epoch=None if ep is None else int(ep[1]),
            term_epoch=None if current is None else int(current["epoch"]),
            node=None if current is None else str(current["node"]),
            bumped_at_ms=None if ep is None or ep[3] is None else int(ep[3]),
            safe_hold=None if cl is None else bool(cl[0]), terms=terms)

    async def _boot_epoch(self, client: aioredis.Redis) -> int | None:
        raw = await client.get(K.BOOT_ID)
        if raw is None:
            return None
        tail = _s(raw).rsplit(":", 1)[-1]
        try:
            return int(tail)
        except ValueError:
            return None

    async def live(self, term: LeaderTerm, now_ms: int, accounts: Sequence[str] = ()) -> dict[str, Any]:
        """Live values from the Redis whose boot_id epoch is the latest leader heartbeat's (and the current leader
        term's in PostgreSQL), else stale."""
        out: dict[str, Any] = {"epoch": None, "source": None, "stale": True, "reason": "", "prices": [],
                               "accounts": [], "heartbeat": None}
        want = term.live_epoch
        if self.heartbeats is not None:
            beat, why = self.heartbeats.current(now_ms)
            if beat is None:
                out["reason"] = why
                return out
            out["heartbeat"] = {"node": beat.node, "epoch": beat.epoch, "age_ms": max(0, now_ms - beat.received_ms)}
            if beat.epoch_revision is None:
                out["reason"] = f"latest leader heartbeat ({beat.node}) carries no epoch"
                return out
            if want != beat.epoch_revision:
                out["reason"] = (f"latest leader heartbeat epoch {beat.epoch_revision} is not the current leader term "
                                 f"in PostgreSQL ({want})")
                return out
        if want is None:
            out["reason"] = "no current leader term in PostgreSQL"
            return out
        out["epoch"] = want
        chosen: aioredis.Redis | None = None
        seen: list[str] = []
        for name, client in self._redis.items():
            try:
                boot = await self._boot_epoch(client)
            except Exception as exc:              # an unreachable Redis is simply not a source
                seen.append(f"{name}: unreachable ({type(exc).__name__})")
                continue
            seen.append(f"{name}: boot epoch {boot}")
            if boot == want:
                chosen, out["source"] = client, name
                break
        if chosen is None:
            out["reason"] = ("no Redis carries the leader's epoch (" + "; ".join(seen) + ")") if seen \
                else "no Redis configured"
            return out
        out["stale"] = False
        symbols = [(_s(r[0]), _s(r[1])) for r in await self._fetch(SQL_OPEN_SYMBOLS)]
        pipe = chosen.pipeline(transaction=False)
        for a, sym in symbols:
            pipe.hgetall(K.price_key(a, sym))
        for acct in accounts:
            pipe.hgetall(K.account_live_key(acct))
        raw = await pipe.execute()
        for (a, sym), h in zip(symbols, raw[:len(symbols)]):
            p = price_from_hash(a, sym, {_s(k): _s(v) for k, v in (h or {}).items()})
            if p is None:
                continue
            age = now_ms - p.timestamp_ms
            entry: dict[str, Any] = {"adapter_id": a, "symbol": sym, "age_ms": age, "fresh": age <= self.fresh_ms}
            if entry["fresh"]:
                entry.update(bid=str(p.bid), ask=str(p.ask))
            out["prices"].append(entry)
        for acct, h in zip(accounts, raw[len(symbols):]):
            lv = account_live_from_hash(acct, {_s(k): _s(v) for k, v in (h or {}).items()})
            if lv is None:
                continue
            age = now_ms - lv.timestamp_ms
            entry = {"account_id": acct, "age_ms": age, "fresh": age <= self.fresh_ms, "sampled": lv.sampled}
            if entry["fresh"]:
                entry.update(equity=jsonable(lv.equity), margin_used=jsonable(lv.margin_used),
                             free_margin=jsonable(lv.free_margin), unrealised_pnl=jsonable(lv.unrealised_pnl))
            out["accounts"].append(entry)
        return out


__all__ = ["ALL_SQL", "DEFAULT_FRESH_MS", "DashboardViews", "LeaderTerm", "as_dicts", "jsonable"]
