"""A tiny emulation of the Oversee tables for the exact SQL constants the PostgreSQL stores issue.

It is NOT a SQL engine: each handler implements the documented semantics of one statement (fence,
ON CONFLICT, compare-and-set, RETURNING), so the stores' control flow and row mapping can be checked
against src/oversee/memory.py without a server. The real statements run in the `pg` integration tests.
"""
from __future__ import annotations

import copy
import json
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from src.db import epoch as E
from src.db import halt_store as H
from src.db import notify_store as N
from src.db import quarantine as Q
from src.db import sweep_writer as W
from src.db.fence import SQL_CHECK_EPOCH
from src.db.rows import BREACH_COLS

COLS = [c.strip() for c in BREACH_COLS.split(",")]
LEVELS = ["NONE", "SOFT", "HARD"]


@dataclass
class Tables:
    breaches: dict[tuple[str, str, str], dict[str, Any]] = field(default_factory=dict)
    halt_events: list[tuple[Any, ...]] = field(default_factory=list)
    acks: list[dict[str, Any]] = field(default_factory=list)
    outbox: list[dict[str, Any]] = field(default_factory=list)
    dedupe: dict[str, int] = field(default_factory=dict)
    stop_watch: dict[tuple[str, str], int] = field(default_factory=dict)
    events: dict[tuple[Any, ...], tuple[Any, ...]] = field(default_factory=dict)
    sod: dict[tuple[str, Any], tuple[Any, ...]] = field(default_factory=dict)
    quarantine: dict[str, tuple[Any, ...]] = field(default_factory=dict)
    feed_quarantine: list[tuple[Any, ...]] = field(default_factory=list)
    next_breach_id: int = 1


class PgEmulator:
    def __init__(self, epoch: int = 7) -> None:
        self.t = Tables()
        self.epoch = epoch
        self.down = False
        self.sql: list[str] = []
        self._saved: Tables | None = None
        h: dict[str, Callable[[tuple[Any, ...]], Any]] = {
            E.SQL_BEGIN_RC: self._begin, E.SQL_COMMIT: self._commit, E.SQL_ROLLBACK: self._rollback,
            E.SQL_COMMIT_TIMEOUT_OFF: lambda a: "SET", H.SQL_HALT_LOCK: lambda a: (None,),
            SQL_CHECK_EPOCH: lambda a: (self.epoch,),
            W.SQL_BREACH_INSERT: self._breach_insert, W.SQL_BREACH_CAS: self._breach_cas,
            W.SQL_BREACH_UPSERT: self._breach_upsert, W.SQL_STOP_WATCH_SET: self._sw_set,
            W.SQL_STOP_WATCH_CLEAR: self._sw_clear, W.SQL_EVENTS: self._events, W.SQL_SOD_INSERT: self._sod,
            W.SQL_QUARANTINE_SET: self._q_set, W.SQL_QUARANTINE_RELEASE: self._q_release,
            N.SQL_OUTBOX_INSERT: self._outbox_insert, N.SQL_OUTBOX_PENDING: self._outbox_pending,
            N.SQL_OUTBOX_MARK: self._outbox_mark, N.SQL_DEDUPE_GET: self._dedupe_get,
            N.SQL_DEDUPE_ALL: lambda a: [(k, v) for k, v in self.t.dedupe.items()],
            N.SQL_DEDUPE_RECORD: self._dedupe_record,
            H.SQL_LATEST_EVENT: lambda a: self.t.halt_events[-1] if self.t.halt_events else None,
            H.SQL_LEVEL_ROSE_AT: self._rose_at, H.SQL_LATEST_OPERATOR_CLEAR: self._op_clear,
            H.SQL_INSERT_EVENT: self._insert_event, H.SQL_UNRESOLVED_BREACHES: self._unresolved,
            H.SQL_INSERT_ACK: self._insert_ack, H.SQL_ACKS_OF: self._acks_of,
            Q.SQL_FEED_QUARANTINE: self._feed_quarantine,
        }
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
        res = self._run(sql, args)
        return list(res)

    @asynccontextmanager
    async def _acq(self) -> AsyncIterator[Any]:
        yield self

    def acquire(self) -> AbstractAsyncContextManager[Any]:
        return self._acq()

    # ---- transactions --------------------------------------------------------------------------------
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

    # ---- halt_breaches -------------------------------------------------------------------------------
    def _row(self, a: tuple[Any, ...]) -> dict[str, Any]:
        return dict(zip(COLS, a[1:1 + len(COLS)]))

    def _breach_insert(self, a: tuple[Any, ...]) -> Any:
        if not self._fenced(a):
            return None
        row = self._row(a)
        key = (row["rule_id"], row["subject_type"], row["subject_id"])
        if key in self.t.breaches:
            return None
        row["breach_id"] = self.t.next_breach_id
        self.t.next_breach_id += 1
        self.t.breaches[key] = row
        return (row["breach_id"],)

    def _breach_cas(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        row = self._row(a)
        key = (row["rule_id"], row["subject_type"], row["subject_id"])
        cur = self.t.breaches.get(key)
        exp_sweep, exp_gen = a[1 + len(COLS)], a[2 + len(COLS)]
        if cur is None or cur["last_sweep_id"] != exp_sweep or cur["clear_generation"] != exp_gen:
            return 0
        cur.update(row)
        return 1

    def _breach_upsert(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        row = self._row(a)
        key = (row["rule_id"], row["subject_type"], row["subject_id"])
        if key in self.t.breaches:
            self.t.breaches[key].update(row)
        else:
            row["breach_id"] = self.t.next_breach_id
            self.t.next_breach_id += 1
            self.t.breaches[key] = row
        return 1

    def _unresolved(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        rows = [r for r in self.t.breaches.values() if r["resolved_at"] is None]
        return [tuple(r[c] for c in COLS) for r in sorted(rows, key=lambda r: (r["rule_id"], r["subject_type"],
                                                                                r["subject_id"]))]

    # ---- stop_watch / events / sod / quarantine --------------------------------------------------------
    def _sw_set(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        n = 0
        for et, eid, since in zip(a[1], a[2], a[3]):
            if (et, eid) not in self.t.stop_watch:
                self.t.stop_watch[(et, eid)] = since
                n += 1
        return n

    def _sw_clear(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        return sum(1 for k in zip(a[1], a[2]) if self.t.stop_watch.pop(k, None) is not None)

    def _events(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        n = 0
        for sweep_id, rule, st, sid, tr, lvl, cap, reason, at, act in zip(*a[1:11]):
            key = (rule, st, sid, tr, sweep_id)
            if key not in self.t.events:
                self.t.events[key] = (lvl, cap, reason, at, act, a[11])
                n += 1
        return n

    def _sod(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        key = (a[1], a[2])
        if key in self.t.sod:
            return 0
        self.t.sod[key] = a[1:]
        return 1

    def _q_set(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        self.t.quarantine[a[1]] = (a[2], a[3], a[4], None)
        return 1

    def _q_release(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a) or a[1] not in self.t.quarantine:
            return 0
        del self.t.quarantine[a[1]]
        return 1

    def _feed_quarantine(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        self.t.feed_quarantine.append(a[1:])
        return 1

    # ---- outbox / dedupe ------------------------------------------------------------------------------
    def _outbox_insert(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        n = 0
        for created, dkey, bypass, sev, title, body, kind, acct in zip(*a[1:9]):
            self.t.outbox.append({"outbox_id": len(self.t.outbox) + 1, "created": created, "episode": dkey,
                                  "bypass": bypass, "severity": sev, "title": title, "body": body, "kind": kind,
                                  "account_id": acct, "sent_at": None, "delivered": None})
            n += 1
        return n

    def _outbox_pending(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        rows = [r for r in self.t.outbox if r["sent_at"] is None][:a[0]]
        return [(r["outbox_id"], r["kind"], r["severity"], r["title"], r["body"], r["created"], r["episode"],
                 r["account_id"]) for r in rows]

    def _outbox_mark(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        for r in self.t.outbox:
            if r["outbox_id"] == a[1]:
                r["sent_at"], r["delivered"] = a[2], a[3]
                return 1
        return 0

    def _dedupe_get(self, a: tuple[Any, ...]) -> Any:
        v = self.t.dedupe.get(a[0])
        return None if v is None else (v,)

    def _dedupe_record(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        self.t.dedupe[a[1]] = a[2]
        return 1

    # ---- halt_events / acks ------------------------------------------------------------------------------
    def _rose_at(self, a: tuple[Any, ...]) -> Any:
        prev, rose = 0, None
        for ev in self.t.halt_events:
            lv = LEVELS.index(ev[2])
            if lv > prev:
                rose = ev[7]
            prev = lv
        return None if rose is None else (rose,)

    def _op_clear(self, a: tuple[Any, ...]) -> Any:
        times = [ev[7] for ev in self.t.halt_events if ev[6] != "system"]
        return (max(times) if times else None,)

    def _insert_event(self, a: tuple[Any, ...]) -> Any:
        if not self._fenced(a):
            return None
        row = (len(self.t.halt_events) + 1, *a[1:8])
        self.t.halt_events.append(row)
        return (row[0],)

    def _insert_ack(self, a: tuple[Any, ...]) -> Any:
        if not self._fenced(a):
            return None
        cur = self.t.breaches.get((a[1], a[2], a[3]))
        if cur is None:
            return None
        self.t.acks.append({"ack_id": len(self.t.acks) + 1, "breach_id": cur["breach_id"], "operator": a[4],
                            "why": a[5], "level": a[6], "cap": a[7], "reason": a[8], "at": a[9]})
        return (len(self.t.acks),)

    def _acks_of(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        """SQL_ACKS_OF (read, not fenced): breach_acks JOIN halt_breaches JOIN unnest($1, $2, $3), only
        unresolved rule states, ORDER BY ack_id. Like the JOIN, a key listed twice yields its rows twice."""
        wanted = list(zip(a[0], a[1], a[2]))
        by_id = {r["breach_id"]: r for r in self.t.breaches.values()}
        out: list[tuple[Any, ...]] = []
        for k in sorted(self.t.acks, key=lambda k: k["ack_id"]):
            b = by_id.get(k["breach_id"])
            if b is None or b["resolved_at"] is not None:
                continue
            for key in wanted:
                if key == (b["rule_id"], b["subject_type"], b["subject_id"]):
                    out.append((k["ack_id"], b["rule_id"], b["subject_type"], b["subject_id"], k["level"], k["cap"],
                                k["reason"], k["operator"], k["why"], k["at"]))
        return out

    def outbox_titles(self) -> list[str]:
        return [r["title"] for r in self.t.outbox]

    def dedupe_keys(self) -> list[Any]:
        return [json.loads(k) for k in self.t.dedupe]
