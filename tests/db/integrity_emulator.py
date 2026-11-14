"""Emulation of the tables behind PgIntegrityDb / PgDlqStore for the exact SQL constants they issue.

Like tests/db/pg_emulator.py it is NOT a SQL engine: each handler implements the documented semantics of
one statement (fence, lease-owner check, RETURNING, ORDER BY / LIMIT), so the stores' control flow and row
mapping can be checked against the in-memory fakes in tests/integrity/fakes.py. The real statements run in
the `pg` integration tests.
"""
from __future__ import annotations

import copy
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from src.cache.digest import fold
from src.db import epoch as E
from src.db import integrity_store as I
from src.db.fence import SQL_CHECK_EPOCH
from src.dto import AccountLive, InstrumentSpec
from src.integrity.ports import CleanMark, StatefulDTO, committed_row

DLQ_COLS = [c.strip() for c in I.DLQ_COLS.split(",")]


@dataclass
class Tables:
    current: dict[str, dict[str, tuple[Any, ...]]] = field(default_factory=dict)   # table -> key -> row
    halt_events: list[tuple[int, str]] = field(default_factory=list)
    marks: list[CleanMark] = field(default_factory=list)
    lives: list[AccountLive] = field(default_factory=list)
    dlq: dict[int, dict[str, Any]] = field(default_factory=dict)
    quarantine: list[tuple[Any, ...]] = field(default_factory=list)
    cash_flow_ts: dict[str, list[int]] = field(default_factory=dict)
    sod_flags: dict[tuple[str, str, Any], int] = field(default_factory=dict)   # (adapter, account, day) -> roll
    timeline: int = 1
    max_seq_id: int = 0


def _value(dto: object, col: str, committed: int) -> object:
    if col in ("committed_at_ms", "balance_changed_at_ms"):
        return committed
    v = getattr(dto, col)
    return v.value if isinstance(v, Enum) else v


class IntegrityEmulator:
    def __init__(self, clock: Callable[[], int], epoch: int = 7) -> None:
        self.clock = clock
        self.epoch = epoch
        self.t = Tables()
        self.down = False
        self.sql: list[str] = []
        self._saved: Tables | None = None
        self._next_dlq = 1
        h: dict[str, Callable[[tuple[Any, ...]], Any]] = {
            E.SQL_BEGIN_RC: self._begin, E.SQL_COMMIT: self._commit, E.SQL_ROLLBACK: self._rollback,
            E.SQL_COMMIT_TIMEOUT_OFF: lambda a: "SET", SQL_CHECK_EPOCH: lambda a: (self.epoch,),
            I.SQL_DIGEST: self._digest, I.SQL_LATEST_COMMIT: self._latest_commit,
            I.SQL_LATEST_HALT: lambda a: max(self.t.halt_events, default=None),
            I.SQL_HIGH_WATER: lambda a: (self.t.timeline, self.epoch, self.t.max_seq_id),
            I.SQL_CLEAN_MARKS: lambda a: [(m.adapter_id, m.symbol, m.bid, m.ask, m.ts, m.spread_pips)
                                          for m in sorted(self.t.marks, key=lambda m: (m.adapter_id, m.symbol))],
            I.SQL_LIVE_SAMPLES: lambda a: [(x.account_id, x.adapter_id, x.currency, x.equity, x.margin_used,
                                            x.free_margin, x.unrealised_pnl, x.timestamp_ms) for x in self.t.lives],
            I.SQL_DLQ_LEASE: self._lease, I.SQL_DLQ_RELEASE_EXPIRED: self._release,
            I.SQL_DLQ_REQUEUE: lambda a: self._owned_update(a, status="QUEUED"),
            I.SQL_DLQ_RESOLVE: lambda a: self._owned_update(a, status="RESOLVED"),
            I.SQL_DLQ_BACKOFF: lambda a: self._owned_update(a, status="BACKOFF", retry_count=a[3],
                                                            next_retry_at_ms=a[4], last_error=a[5]),
            I.SQL_DLQ_QUARANTINE_MARK: self._q_mark, I.SQL_DLQ_QUARANTINE_INSERT: self._q_insert,
            I.SQL_DLQ_MERGE_CHILD: lambda a: self._owned_update(a, status="RESOLVED", origin_dlq_id=a[3]),
            I.SQL_DLQ_MERGE_ORIGIN: self._merge_origin, I.SQL_SOD_TRANSFERS_UNVERIFIED: self._sod_flag,
            I.SQL_DLQ_LINEAGE: self._lineage,
            I.SQL_LAST_CASH_FLOW_TS: lambda a: (max(self.t.cash_flow_ts.get(a[0], []), default=None),),
            I.SQL_PARKED_CASH_FLOWS: lambda a: [(r["raw_payload"],) for r in self.t.dlq.values()
                                                if r["adapter_id"] == a[0] and r["event_type"] == "CASH_FLOW"
                                                and r["status"] in ("QUEUED", "LEASED", "BACKOFF")],
        }
        for t in I.CURRENT_TABLES:
            h[t.sql_window] = self._window(t)
            h[t.sql_after_key] = self._after(t)
            h[t.sql_by_ids] = self._by_ids(t)
        self._h = h

    # ---- driver protocol ---------------------------------------------------------------------------
    def _run(self, sql: str, args: tuple[Any, ...]) -> Any:
        if self.down:
            raise ConnectionRefusedError("postgres down")
        self.sql.append(sql)
        handler = self._h.get(sql)
        if handler is None:
            raise AssertionError(f"unexpected SQL: {sql[:100]}")
        return handler(args)

    async def execute(self, sql: str, *args: object) -> str:
        res = self._run(sql, args)
        return res if isinstance(res, str) else f"UPDATE {res if isinstance(res, int) else 1}"

    async def fetchrow(self, sql: str, *args: object) -> tuple[Any, ...] | None:
        res = self._run(sql, args)
        if isinstance(res, list):
            return res[0] if res else None
        return res  # type: ignore[no-any-return]

    async def fetch(self, sql: str, *args: object) -> list[tuple[Any, ...]]:
        return list(self._run(sql, args))

    @asynccontextmanager
    async def _acq(self) -> AsyncIterator[Any]:
        yield self

    def acquire(self) -> AbstractAsyncContextManager[Any]:
        return self._acq()

    def _begin(self, a: tuple[Any, ...]) -> str:
        self._saved = copy.deepcopy(self.t)
        return "BEGIN"

    def _commit(self, a: tuple[Any, ...]) -> str:
        self._saved = None
        return "COMMIT"

    def _rollback(self, a: tuple[Any, ...]) -> str:
        if self._saved is not None:
            self.t = self._saved
        self._saved = None
        return "ROLLBACK"

    def _fenced(self, a: tuple[Any, ...]) -> bool:
        return bool(a) and a[0] == self.epoch

    # ---- current rows ------------------------------------------------------------------------------
    def put(self, item: StatefulDTO | InstrumentSpec, committed_at_ms: int, *, seq_id: int | None = None,
            **override: Any) -> tuple[Any, ...]:
        """Store a DTO's current row as PgStateStore would (`override` corrupts a suffix column)."""
        c = committed_row(item, committed_at_ms, seq_id=seq_id)
        table = next(t for t in I.CURRENT_TABLES if t.entity_type == c.entity_type)
        cols = [x.strip() for x in table.cols.split(",")]
        suffix = {"version": c.version, "seq_id": c.seq_id, "checksum": c.checksum, "digest_hash": c.digest_hash,
                  "committed_at_ms": committed_at_ms} | override
        row = tuple(_value(item, col, committed_at_ms) for col in cols) + tuple(suffix.values()) + (c.key,)
        self.t.current.setdefault(table.table, {})[c.key] = row
        self.t.max_seq_id = max(self.t.max_seq_id, c.seq_id)
        return row

    def _rows(self, t: I.CurrentTable) -> list[tuple[Any, ...]]:
        return list(self.t.current.get(t.table, {}).values())

    def _window(self, t: I.CurrentTable) -> Callable[[tuple[Any, ...]], Any]:
        n = t.ncols

        def run(a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
            rows = [r for r in self._rows(t) if a[0] <= r[n + 4] <= a[1]]
            return sorted(rows, key=lambda r: (r[n + 4], r[n + 5].encode()))[:a[2]]
        return run

    def _after(self, t: I.CurrentTable) -> Callable[[tuple[Any, ...]], Any]:
        n = t.ncols

        def run(a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
            rows = [r for r in self._rows(t) if r[n + 5].encode() > a[0].encode()]
            return sorted(rows, key=lambda r: r[n + 5].encode())[:a[1]]
        return run

    def _by_ids(self, t: I.CurrentTable) -> Callable[[tuple[Any, ...]], Any]:
        n = t.ncols

        def run(a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
            wanted = {f"{t.prefix}:{ad}:{i}" for ad, i in zip(a[0], a[1])}
            return [r for r in self._rows(t) if r[n + 5] in wanted]
        return run

    def _all(self) -> list[tuple[I.CurrentTable, tuple[Any, ...]]]:
        return [(t, r) for t in I.CURRENT_TABLES for r in self._rows(t)]

    def _digest(self, a: tuple[Any, ...]) -> tuple[str]:
        return (fold(r[t.ncols + 3] for t, r in self._all()),)

    def _latest_commit(self, a: tuple[Any, ...]) -> tuple[int | None]:
        return (max((r[t.ncols + 4] for t, r in self._all()), default=None),)

    # ---- events_dlq --------------------------------------------------------------------------------
    def park(self, event_type: str, entity_id: str, reason: str, *, adapter_id: str = "stonex",
             payload: str = "{}", parked_at_ms: int | None = None, retry_count: int = 0) -> int:
        dlq_id = self._next_dlq
        self._next_dlq += 1
        now = self.clock()
        self.t.dlq[dlq_id] = {
            "dlq_id": dlq_id, "event_type": event_type, "entity_id": entity_id, "adapter_id": adapter_id,
            "raw_payload": payload, "payload_hash": f"h{dlq_id}", "error_reason": reason, "retry_count": retry_count,
            "status": "QUEUED", "next_retry_at_ms": now, "parked_at_ms": now if parked_at_ms is None else parked_at_ms,
            "origin_dlq_id": None, "lease_owner": None, "lease_until_ms": None, "updated_at_ms": now,
            "last_error": None,
        }
        return dlq_id

    def _tuple(self, r: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(r[c] for c in DLQ_COLS)

    def _lease(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        if not self._fenced(a):
            return []
        _, owner, now, lease_ms, adapters, limit = a
        due = sorted((r for r in self.t.dlq.values() if r["status"] in ("QUEUED", "BACKOFF")
                      and r["next_retry_at_ms"] <= now and r["adapter_id"] in adapters),
                     key=lambda r: (r["next_retry_at_ms"], r["dlq_id"]))[:limit]
        for r in due:
            r.update(status="LEASED", lease_owner=owner, lease_until_ms=now + lease_ms, updated_at_ms=now)
        return [self._tuple(r) for r in reversed(due)]   # RETURNING order is not guaranteed

    def _release(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        n = 0
        for r in self.t.dlq.values():
            if r["status"] == "LEASED" and r["lease_until_ms"] is not None and r["lease_until_ms"] < a[1]:
                r.update(status="QUEUED", lease_owner=None, lease_until_ms=None, updated_at_ms=a[1])
                n += 1
        return n

    def _owned_update(self, a: tuple[Any, ...], **changes: Any) -> int:
        if not self._fenced(a):
            return 0
        r = self.t.dlq.get(a[1])
        if r is None or r["status"] != "LEASED" or r["lease_owner"] != a[2]:
            return 0
        r.update(lease_owner=None, lease_until_ms=None, updated_at_ms=self.clock(), **changes)
        return 1

    def _q_mark(self, a: tuple[Any, ...]) -> tuple[Any, ...] | None:
        if not self._fenced(a):
            return None
        r = self.t.dlq.get(a[1])
        if r is None or r["status"] == "QUARANTINED":
            return None
        if a[2] is not None and (r["status"] != "LEASED" or r["lease_owner"] != a[2]):
            return None
        r.update(status="QUARANTINED", last_error=a[3], lease_owner=None, lease_until_ms=None,
                 updated_at_ms=self.clock())
        return (r["event_type"], r["entity_id"], r["adapter_id"], r["raw_payload"], r["error_reason"],
                r["retry_count"])

    def _q_insert(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        self.t.quarantine.append(a[1:])
        return 1

    def _merge_origin(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        r, c = self.t.dlq.get(a[1]), self.t.dlq.get(a[4])
        if r is None or r["status"] not in ("RESOLVED", "QUEUED", "BACKOFF"):
            return 0
        if c is None or any(c[k] != r[k] for k in ("adapter_id", "event_type", "entity_id")):
            return 0
        r.update(status="BACKOFF", retry_count=a[2], next_retry_at_ms=a[3], updated_at_ms=self.clock())
        return 1

    def _lineage(self, a: tuple[Any, ...]) -> tuple[Any, ...] | None:
        et, eid, reason, before, since, adapter = a
        cands = [r for r in self.t.dlq.values() if r["adapter_id"] == adapter and r["event_type"] == et
                 and r["entity_id"] == eid
                 and r["error_reason"] == reason and r["status"] == "RESOLVED" and r["origin_dlq_id"] is None
                 and r["dlq_id"] < before and r["updated_at_ms"] >= since]
        best = max(cands, key=lambda r: (r["updated_at_ms"], r["dlq_id"]), default=None)
        return None if best is None else self._tuple(best)

    def _sod_flag(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        self.t.sod_flags[(a[1], a[2], a[3])] = a[4]
        return 1
