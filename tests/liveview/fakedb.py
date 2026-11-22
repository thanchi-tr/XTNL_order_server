"""A fake of the lv_ views for the live view tests: rows become visible when "committed", ids are issued in nextval
order regardless of commit order, and every statement of src.liveview.sql is answered by dispatch on its text."""
from __future__ import annotations

import copy
from collections.abc import Sequence
from typing import Any

from src.liveview import sql as Q

SOURCES = {s.name: s for s in Q.TAIL_SOURCES}


class FakeDB:
    def __init__(self, now_ms: int = 1_795_000_000_000) -> None:
        self.now = now_ms
        self.next_id: dict[str, int] = {name: 0 for name in SOURCES}
        self.rows: dict[str, dict[int, dict[str, Any]]] = {name: {} for name in SOURCES}
        self.streams: list[dict[str, Any]] = []
        self.runs: list[dict[str, Any]] = []
        self.cluster: dict[str, Any] | None = {"safe_hold": False, "safe_hold_since": None, "hold_code": None,
                                               "hold_count": None, "last_resume_at": None, "resumed_by": None,
                                               "updated_at_ms": 0}
        self.epoch: dict[str, Any] | None = {"cluster_id": "c1", "revision": 7, "epoch": 7, "node": "pc1",
                                             "bumped_at_ms": 0}
        self.quarantine: list[dict[str, Any]] = []
        self.auth: list[dict[str, Any]] = []
        self.breaches: list[dict[str, Any]] = []
        self.orders: list[dict[str, Any]] = []
        self.positions: list[dict[str, Any]] = []
        self.calls: list[str] = []
        self.down = False

    # ---- writing -------------------------------------------------------------------------------------------------
    def reserve(self, source: str) -> int:
        """nextval: an id is drawn now, the row commits later (or never)."""
        self.next_id[source] += 1
        return self.next_id[source]

    def commit(self, source: str, k: int, row: dict[str, Any]) -> dict[str, Any]:
        src = SOURCES[source]
        full = {src.key: k, **row}
        self.rows[source][k] = full
        return full

    def add(self, source: str, **row: Any) -> dict[str, Any]:
        return self.commit(source, self.reserve(source), row)

    # ---- reading -------------------------------------------------------------------------------------------------
    async def fetchval(self, sql: str, *args: object) -> Any:
        rows = await self.fetch(sql, *args)
        if not rows:
            return None
        first = rows[0]
        return next(iter(first.values())) if isinstance(first, dict) else first[0]

    async def fetch(self, sql: str, *args: object) -> Sequence[Any]:
        if self.down:
            raise ConnectionError("db down")
        self.calls.append(sql)
        if sql == Q.SQL_NOW:
            return [{"now_ms": self.now}]
        for name, src in SOURCES.items():
            data = self.rows[name]
            key = src.key

            def prune_ok(r: dict[str, Any], bound: object) -> bool:
                return src.prune is None or int(r.get(src.prune) or 0) >= int(bound)  # type: ignore[call-overload]

            if sql == src.sql_max_pruned and src.prune:
                vals = [k for k, r in data.items() if prune_ok(r, args[0])]
                return [{"max": max(vals) if vals else None}]
            if sql == src.sql_max:
                return [{"max": max(data) if data else None}]
            if sql == src.sql_present:
                lo, hi = int(args[0]), int(args[1])  # type: ignore[call-overload]
                bound = args[2] if len(args) > 2 else 0
                return [{key: k} for k in sorted(data) if lo < k <= hi and prune_ok(data[k], bound)][:1001]
            if sql == src.sql_tail:
                h, pend, last = int(args[0]), set(args[1]), int(args[2])  # type: ignore[call-overload, arg-type]
                bound = args[3] if len(args) > 3 else 0
                out = [copy.deepcopy(data[k]) for k in sorted(data)
                       if (k > h or k in pend) and k > last and prune_ok(data[k], bound)]
                return out[:Q.PAGE_ROWS]
            if sql == src.sql_recheck:
                ids = set(args[0])  # type: ignore[call-overload]
                bound = args[1] if len(args) > 1 else 0
                return [copy.deepcopy(data[k]) for k in sorted(data) if k in ids and prune_ok(data[k], bound)]
            if sql == src.sql_backfill:
                h, since, limit = int(args[0]), int(args[1]), int(args[2])  # type: ignore[call-overload]
                out = [copy.deepcopy(data[k]) for k in sorted(data, reverse=True)
                       if k <= h and int(data[k].get(src.backfill_time) or 0) >= since]
                return out[:limit]
        if sql == Q.SQL_ALERTS_WATCHED:
            ids = set(args[0])  # type: ignore[call-overload]
            return [copy.deepcopy(r) for k, r in sorted(self.rows["alerts"].items()) if k in ids]
        if sql == Q.SQL_ALERTS_SEED:
            return [copy.deepcopy(r) for r in self.rows["alerts"].values()
                    if r.get("delivery") in ("queued", "retrying") and r.get("kind") != "arm"
                    and int(r.get("created_at_ms") or 0) >= int(args[0])]  # type: ignore[call-overload]
        if sql == Q.SQL_DLQ_WATCHED:
            ids = set(args[0])  # type: ignore[call-overload]
            return [copy.deepcopy(r) for k, r in sorted(self.rows["dlq"].items()) if k in ids]
        if sql == Q.SQL_DLQ_SEED:
            return [copy.deepcopy(r) for r in self.rows["dlq"].values()
                    if r.get("status") in ("QUEUED", "LEASED", "BACKOFF", "QUARANTINED")]
        if sql == Q.SQL_TERMS_WATCHED:
            ids = set(args[0])  # type: ignore[call-overload]
            return [copy.deepcopy(r) for k, r in sorted(self.rows["terms"].items()) if k in ids]
        if sql == Q.SQL_TERMS_SEED:
            return [copy.deepcopy(r) for r in self.rows["terms"].values() if r.get("ended_at") is None]
        if sql == Q.SQL_RUNS_DISCOVER:
            return [copy.deepcopy(r) for r in sorted(self.runs, key=lambda r: -int(r["started_at_ms"]))][:50]
        if sql == Q.SQL_STREAMS:
            return copy.deepcopy(self.streams)
        if sql == Q.SQL_CLUSTER:
            return [copy.deepcopy(self.cluster)] if self.cluster else []
        if sql == Q.SQL_EPOCH:
            return [copy.deepcopy(self.epoch)] if self.epoch else []
        if sql == Q.SQL_QUARANTINE:
            return copy.deepcopy(self.quarantine)
        if sql == Q.SQL_AUTH:
            return copy.deepcopy(self.auth)
        if sql == Q.SQL_HALT_LATEST:
            rows = sorted(self.rows["halt"].values(), key=lambda r: -r["id"])
            return [copy.deepcopy(rows[0])] if rows else []
        if sql == Q.SQL_HALT_BEFORE:
            rows = sorted((r for r in self.rows["halt"].values() if int(r["at_ms"]) < int(args[0])),  # type: ignore
                          key=lambda r: -r["id"])
            return [copy.deepcopy(rows[0])] if rows else []
        if sql == Q.SQL_BREACHES_OPEN:
            return copy.deepcopy([b for b in self.breaches if b.get("resolved_at") is None])
        if sql == Q.SQL_OPEN_ORDERS:
            return copy.deepcopy(self.orders)
        if sql == Q.SQL_OPEN_POSITIONS:
            return copy.deepcopy(self.positions)
        if sql == Q.SQL_TIMING:
            return []
        if sql == Q.SQL_ALERTS_WAITING:
            rows = [r for r in self.rows["alerts"].values() if r.get("delivery") in ("queued", "retrying")]
            return [{"waiting": len(rows), "oldest_ms": min((r["created_at_ms"] for r in rows), default=None)}]
        if sql == Q.SQL_ALERTS_TODAY:
            return [{"undeliverable": 0, "escalated_today": 0}]
        if sql == Q.SQL_DLQ_COUNTS:
            return []
        if sql == Q.SQL_TERM_LATEST:
            rows = sorted(self.rows["terms"].values(), key=lambda r: -r["term_id"])
            return [copy.deepcopy(rows[0])] if rows else []
        raise AssertionError(f"unexpected SQL: {sql[:80]}")


def entity(db: FakeDB, *, etype: str = "ORDER", key: str = "stonex:1", facts: dict[str, Any] | None = None,
           at: int | None = None, action: str = "APPLIED", path: str = "stream", k: int | None = None,
           commit: bool = True) -> dict[str, Any]:
    at = db.now if at is None else at
    row = {"seq_id": at << 20, "entity_type": etype, "entity_key": key, "adapter_id": key.split(":")[0],
           "account_id": "acc1", "action": action, "logged_at_ms": at, "version_path": path, "broker_ms": at,
           "facts": facts or {}}
    if k is None:
        k = db.reserve("entity")
    return db.commit("entity", k, row) if commit else row


__all__ = ["FakeDB", "entity"]
