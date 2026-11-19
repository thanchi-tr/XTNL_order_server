"""A tiny emulation of the ARM tables (0017) for the exact SQL constants the S24 stores issue.

Like `tests/db/pg_emulator.py`, it is NOT a SQL engine: each handler implements the documented semantics of one
statement (fence, ON CONFLICT, the `row_version` CAS, RETURNING, the advisory lock, the `event_log` filters), and
transactions / savepoints snapshot the tables, so the stores' control flow can be checked without a server. The real
statements run in the `pg` integration test (`test_s24_arm_stores_as_xtnl_arm_on_postgres`).
"""
from __future__ import annotations

import copy
import json
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from src.db import arm_budget_store as BS
from src.db import arm_config_store as C
from src.db import arm_failures_store as F
from src.db import arm_status_store as S
from src.db import arm_store as A
from src.db import arm_versions as V
from src.db import epoch as E
from src.db import notify_store as N
from src.db.fence import SQL_CHECK_EPOCH, SQL_RELEASE_SAVEPOINT, SQL_ROLLBACK_TO_SAVEPOINT, SQL_SAVEPOINT

POS_COLS = [c for c, _ in A.POSITION_INSERT_COLUMNS]
DEC_COLS = [c for c, _ in A.DECISION_COLUMNS]
HANDLE_COLS = [c for c, _ in A.HANDLE_COLUMNS]
KEY_COLS = ("adapter_id", "position_id", "resource", "owner")
DEC_KEY = tuple(c.strip() for c in A.DECISION_KEY.split(","))
COUNTING = {"stream", "stop_read", "snapshot", "dlq"}


class InFailedTransaction(Exception):
    """asyncpg's `InFailedSQLTransactionError`: a statement sent after an error aborted the transaction."""


@dataclass
class ArmTables:
    positions: dict[tuple[Any, ...], dict[str, Any]] = field(default_factory=dict)
    decisions: dict[tuple[int, int], dict[str, Any]] = field(default_factory=dict)
    handles: list[dict[str, Any]] = field(default_factory=list)
    notices: dict[tuple[Any, ...], int] = field(default_factory=dict)
    audits: dict[tuple[int, int], tuple[Any, ...]] = field(default_factory=dict)
    failures: dict[str, dict[str, Any]] = field(default_factory=dict)
    outbox: list[dict[str, Any]] = field(default_factory=list)
    status: dict[str, dict[str, Any]] = field(default_factory=dict)
    cache: dict[str, Any] | None = None
    config_audit: list[tuple[Any, ...]] = field(default_factory=list)
    accepted: list[tuple[Any, ...]] = field(default_factory=list)
    controls: tuple[Any, ...] | None = (False, None, None, False, None, 1)
    event_log: list[dict[str, Any]] = field(default_factory=list)
    requests: dict[int, dict[str, Any]] = field(default_factory=dict)
    budget: dict[tuple[str, str, str], tuple[Any, ...]] = field(default_factory=dict)
    """0020 `arm_budget_counters` (S38f): key -> (count, streak_start_ms, last_event_ms, epoch, updated_at_ms)."""
    next_decision: int = 1
    next_notice: int = 1


class ArmEmulator:
    def __init__(self, epoch: int = 7) -> None:
        self.t = ArmTables()
        self.epoch = epoch
        self.sql: list[str] = []
        self.locks: list[int] = []
        self.fail_on: dict[str, BaseException] = {}
        self._saved: ArmTables | None = None
        self._savepoints: list[ArmTables] = []
        #: PostgreSQL's aborted transaction: after a failed statement inside a transaction every statement but
        #: `ROLLBACK` / `ROLLBACK TO SAVEPOINT` fails until one of those runs.
        self._aborted = False
        #: The `event_log` a READ ONLY REPEATABLE READ transaction sees (taken at its BEGIN), else None.
        self._snapshot_log: list[dict[str, Any]] | None = None
        #: A failure chosen per statement and arguments (None = run it), after `fail_on`.
        self.fail_when: Callable[[str, tuple[Any, ...]], BaseException | None] | None = None
        #: Called after every statement (a test commits a concurrent `event_log` row between two reads).
        self.after_statement: Callable[[str], None] | None = None
        self._h: dict[str, Callable[[tuple[Any, ...]], Any]] = {
            E.SQL_BEGIN_RC: self._begin, E.SQL_COMMIT: self._commit, E.SQL_ROLLBACK: self._rollback,
            E.SQL_BEGIN_SNAPSHOT: self._begin_snapshot,
            E.SQL_COMMIT_TIMEOUT_OFF: lambda a: "SET", SQL_CHECK_EPOCH: lambda a: (self.epoch,),
            SQL_SAVEPOINT: self._savepoint, SQL_RELEASE_SAVEPOINT: self._release,
            SQL_ROLLBACK_TO_SAVEPOINT: self._rollback_to,
            A.SQL_POSITION_INSERT: self._pos_insert, A.SQL_POSITION_CAS: self._pos_cas,
            A.SQL_POSITIONS_LIVE: self._pos_live, A.SQL_POSITION_GET: self._pos_get,
            A.SQL_DECISION_INSERT: self._dec_insert, A.SQL_HANDLES_INSERT: self._handles_insert,
            A.SQL_DECISIONS_ATTACH: self._attach, A.SQL_DECISIONS_UNNOTICED: self._unnoticed,
            A.SQL_NOTICE_INSERT: self._notice_insert, A.SQL_CORRECTION_AUDIT_INSERT: self._audit_insert,
            A.SQL_PRE_ADJUST_LEVELS: self._pre_levels, A.SQL_LAST_BAR_END_DECISION: self._last_bar_end,
            A.SQL_FINAL_ADVICE_TARGET: self._final_advice, A.SQL_PRIOR_FINAL_DECISION: self._prior_final,
            # S39b: the correction-window rebuild and `arm replay --rerun`'s reads (read-only)
            A.SQL_RECENT_FINAL_DECISIONS: self._recent_final, A.SQL_DECISION_HANDLES_READ: self._handles_read,
            A.SQL_PRE_ADJUST_LEVELS_BEFORE: lambda a: self._before(self._pre_levels, a, 6),
            A.SQL_LAST_BAR_END_DECISION_BEFORE: lambda a: self._before(self._last_bar_end, a, 5),
            A.SQL_FINAL_ADVICE_TARGET_BEFORE: lambda a: self._before(self._final_advice, a, 6),
            A.SQL_PRIOR_FINAL_DECISION_BEFORE: lambda a: self._before(self._prior_final, a, 6),
            V.SQL_ENTRY_STREAM: self._entry_stream, V.SQL_ENTRY_ORDER_FILL: self._entry_fill,
            V.SQL_STOP_COUNTING: self._stop_counting, V.SQL_STOP_NONCOUNTING: self._stop_noncounting,
            V.SQL_STOP_READ_AFTER: self._stop_read_after, V.SQL_LINKED_STOP_CHILDREN: self._linked_children,
            F.SQL_FAILURE_INSERT: self._failure_insert, F.SQL_ARM_PING_INSERT: self._arm_ping,
            N.SQL_OUTBOX_INSERT: self._outbox_insert, F.SQL_FAILURE_REPING: self._reping,
            F.SQL_FAILURE_PROBE: self._probe, F.SQL_FAILURES_OPEN: self._failures_open,
            F.SQL_ARM_FAILED_LOCK: self._lock, F.SQL_FAILURE_CLEAR: self._clear,
            S.SQL_STATUS_UPSERT: self._status_upsert, S.SQL_STATUS_ALL: self._status_all,
            S.SQL_MD_RECORD_SAVE: self._md_save, S.SQL_MD_RECORD_GET: self._md_get,
            C.SQL_CONFIG_CACHE_UPSERT: self._cache_upsert, C.SQL_CONFIG_CACHE_GET: self._cache_get,
            C.SQL_CONFIG_AUDIT_INSERT: self._config_audit, C.SQL_ACCEPTED_LATEST: self._accepted_latest,
            C.SQL_CONTROLS_GET: lambda a: self.t.controls,
            BS.SQL_BUDGET_UPSERT: self._budget_upsert, BS.SQL_BUDGET_LOAD: self._budget_load,
        }

    # ---- driver protocol ---------------------------------------------------------------------------
    def _run(self, sql: str, args: tuple[Any, ...]) -> Any:
        self.sql.append(sql)
        if self._aborted and sql not in (E.SQL_ROLLBACK, SQL_ROLLBACK_TO_SAVEPOINT):
            raise InFailedTransaction("current transaction is aborted, commands ignored until end of transaction")
        failure = self.fail_on.pop(sql, None)
        if failure is None and self.fail_when is not None:
            failure = self.fail_when(sql, args)
        if failure is not None:
            self._aborted = self._saved is not None
            raise failure
        handler = self._h.get(sql)
        if handler is None:
            raise AssertionError(f"unexpected SQL: {sql[:100]}")
        result = handler(args)
        if self.after_statement is not None:
            self.after_statement(sql)
        return result

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

    # ---- transactions --------------------------------------------------------------------------------
    def _begin(self, a: tuple[Any, ...]) -> str:
        self._saved = copy.deepcopy(self.t)
        return "BEGIN"

    def _commit(self, a: tuple[Any, ...]) -> str:
        self._snapshot_log = None
        self._saved = None
        self._savepoints.clear()
        self.locks.clear()
        return "COMMIT"

    def _begin_snapshot(self, a: tuple[Any, ...]) -> str:
        self._snapshot_log = copy.deepcopy(self.t.event_log)
        return "BEGIN"

    def _rollback(self, a: tuple[Any, ...]) -> str:
        self._aborted = False
        self._snapshot_log = None
        if self._saved is not None:
            self.t = self._saved
        self._saved = None
        self._savepoints.clear()
        self.locks.clear()
        return "ROLLBACK"

    def _savepoint(self, a: tuple[Any, ...]) -> str:
        assert self._saved is not None, "SAVEPOINT outside a transaction"
        self._savepoints.append(copy.deepcopy(self.t))
        return "SAVEPOINT"

    def _rollback_to(self, a: tuple[Any, ...]) -> str:
        self._aborted = False
        self.t = copy.deepcopy(self._savepoints[-1])
        return "ROLLBACK"

    def _release(self, a: tuple[Any, ...]) -> str:
        self._savepoints.pop()
        return "RELEASE"

    def _fenced(self, a: tuple[Any, ...]) -> bool:
        return bool(a) and a[0] == self.epoch

    def _in_tx(self) -> None:
        assert self._saved is not None, "a write outside a transaction"

    # ---- arm_positions -----------------------------------------------------------------------------
    @staticmethod
    def _key(d: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(d[c] for c in KEY_COLS)

    def _pos_insert(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        if not self._fenced(a):
            return 0
        row = dict(zip(POS_COLS, a[1:]))
        row["epoch"] = a[0]
        if self._key(row) in self.t.positions:
            return 0
        self.t.positions[self._key(row)] = row
        return 1

    def _pos_cas(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        if not self._fenced(a):
            return 0
        mutable = [c for c, _ in A.POSITION_MUTABLE_COLUMNS]
        values = dict(zip(mutable, a[1:1 + len(mutable)]))
        key = tuple(a[1 + len(mutable):5 + len(mutable)])
        expected = a[5 + len(mutable)]
        row = self.t.positions.get(key)
        if row is None or row["row_version"] != expected or row["state"] in ("CLOSED", "DISARMED"):
            return 0
        row.update(values)
        row["row_version"] += 1
        row["epoch"] = a[0]
        return 1

    @staticmethod
    def _pos_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(row[c] for c, _ in A.POSITION_SELECT_COLUMNS)

    def _pos_live(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        return [self._pos_tuple(r) for k, r in sorted(self.t.positions.items())
                if r["state"] not in ("CLOSED", "DISARMED")]

    def _pos_get(self, a: tuple[Any, ...]) -> tuple[Any, ...] | None:
        r = self.t.positions.get(tuple(a))
        return None if r is None else self._pos_tuple(r)

    # ---- arm_decisions, handles, notices, audit --------------------------------------------------------
    def _dec_insert(self, a: tuple[Any, ...]) -> Any:
        self._in_tx()
        if not self._fenced(a):
            return None
        row = dict(zip(DEC_COLS, a[1:]))
        row["epoch"] = a[0]
        if any(tuple(d[c] for c in DEC_KEY) == tuple(row[c] for c in DEC_KEY) for d in self.t.decisions.values()):
            return None
        did = self.t.next_decision
        self.t.next_decision += 1
        row["decision_id"] = did
        self.t.decisions[(did, row["bar_end_ms"])] = row
        return (did, row["bar_end_ms"])

    def _handles_insert(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        if not self._fenced(a):
            return 0
        n = 0
        for values in zip(*a[3:]):
            self.t.handles.append({"decision_id": a[1], "bar_end_ms": a[2], **dict(zip(HANDLE_COLS, values))})
            n += 1
        return n

    def _pre_levels(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        key, spec, bar = tuple(a[:4]), a[4], a[5]
        ids = {did for (did, b), d in self.t.decisions.items()
               if b == bar and tuple(d[c] for c in KEY_COLS) == key and d["bar_spec"] == spec
               and d["event"] == "PRE" and d["outcome"] == "ADJUST"}
        return sorted((h["handle_role"], h["level_at_pre"]) for h in self.t.handles
                      if h["decision_id"] in ids and h["bar_end_ms"] == bar and h["level_at_pre"] is not None)

    def _last_bar_end(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        key, spec = tuple(a[:4]), a[4]
        rows = [d for d in self.t.decisions.values()
                if tuple(d[c] for c in KEY_COLS) == key and d["bar_spec"] == spec and d["event"] != "PRE"
                and d["outcome"] != "NO_BROKER_BAR"]
        rows.sort(key=lambda d: d["decision_seq"], reverse=True)
        return [(d["outcome"], d["reason"], d["reason_kind"]) for d in rows[:1]]

    def _final_advice(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        key, spec, bar = tuple(a[:4]), a[4], a[5]
        rows = [d for d in self.t.decisions.values()
                if tuple(d[c] for c in KEY_COLS) == key and d["bar_spec"] == spec
                and d["event"] in ("POST", "LATE", "REOPEN", "CORRECTED") and d["outcome"] == "ADJUST"
                and d["bar_end_ms"] < bar and d["proposed_stop"] is not None]
        rows.sort(key=lambda d: d["decision_seq"], reverse=True)
        return [(d["proposed_stop"],) for d in rows[:1]]

    def _prior_final(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        key, spec, bar = tuple(a[:4]), a[4], a[5]
        rows = [d for d in self.t.decisions.values()
                if tuple(d[c] for c in KEY_COLS) == key and d["bar_spec"] == spec and d["event"] != "PRE"
                and d["outcome"] != "NO_BROKER_BAR" and d["bar_end_ms"] < bar]
        rows.sort(key=lambda d: (d["bar_end_ms"], d["decision_seq"]), reverse=True)
        return [(d["outcome"], d["proposed_stop"]) for d in rows[:1]]

    def _recent_final(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        rows = [d for d in self.t.decisions.values() if d["bar_end_ms"] >= a[0]
                and d["event"] in ("POST", "LATE", "CORRECTED") and d["outcome"] != "UNRESOLVED_RANGE"]
        rows.sort(key=lambda d: (d["bar_end_ms"], d["decision_seq"]), reverse=True)
        return [(d["decision_id"], *(d[c] for c in DEC_COLS), d["epoch"]) for d in rows[:a[1]]]

    def _handles_read(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        return sorted((h["handle_id"], h["level_read"]) for h in self.t.handles
                      if h["decision_id"] == a[0] and h["bar_end_ms"] == a[1])

    def _before(self, handler: Callable[[tuple[Any, ...]], list[tuple[Any, ...]]], a: tuple[Any, ...],
                seq_at: int) -> list[tuple[Any, ...]]:
        """A memory read bounded by `decision_seq < a[seq_at]`: the handler over the decisions before it."""
        saved = self.t.decisions
        self.t.decisions = {k: d for k, d in saved.items() if d["decision_seq"] < a[seq_at]}
        try:
            return handler(tuple(a[:seq_at]))
        finally:
            self.t.decisions = saved

    def _attach(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        if not self._fenced(a):
            return 0
        n = 0
        for ref in zip(a[2], a[3]):
            d = self.t.decisions.get(ref)
            if d is not None and d["notice_id"] is None:
                d["notice_id"] = a[1]
                n += 1
        return n

    def _unnoticed(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        route = set(zip(a[0], a[1]))
        out = []
        for d in self.t.decisions.values():
            p = self.t.positions.get(tuple(d[c] for c in KEY_COLS))
            if (p is None or (p["adapter_id"], p["account_id"]) not in route or d["notice_id"] is not None
                    or d["bar_spec"] != a[2] or d["bar_end_ms"] < a[3]):
                continue
            out.append(d)
        out.sort(key=lambda d: (d["bar_end_ms"], d["decided_at_ms"], d["decision_id"]))
        return [(d["decision_id"], *(d[c] for c in DEC_COLS), d["epoch"]) for d in out[:a[4]]]

    def _notice_insert(self, a: tuple[Any, ...]) -> Any:
        self._in_tx()
        if not self._fenced(a):
            return None
        key = tuple(a[1:7])
        if key in self.t.notices:
            return None
        nid = self.t.next_notice
        self.t.next_notice += 1
        self.t.notices[key] = nid
        return (nid,)

    def _audit_insert(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        if not self._fenced(a) or (a[1], a[2]) in self.t.audits:
            return 0
        self.t.audits[(a[1], a[2])] = a[3:]
        return 1

    # ---- event_log -------------------------------------------------------------------------------------
    def add_version(self, *, entity_type: str, entity_key: str, seq_id: int, payload: dict[str, Any],
                    origin: str | None, broker_time_ms: int | None, action: str = "APPLIED") -> None:
        self.t.event_log.append({"entity_type": entity_type, "entity_key": entity_key, "seq_id": seq_id,
                                 "payload": payload, "origin": origin, "broker_time_ms": broker_time_ms,
                                 "action": action})

    def _versions(self, et: str, keys: list[str] | None = None, key: str | None = None) -> list[dict[str, Any]]:
        log = self.t.event_log if self._snapshot_log is None else self._snapshot_log
        return [v for v in log if v["entity_type"] == et and v["action"] == "APPLIED"
                and (v["entity_key"] in keys if keys is not None else v["entity_key"] == key)]

    @staticmethod
    def _first(rows: list[dict[str, Any]]) -> tuple[Any, ...] | None:
        rows = sorted(rows, key=lambda v: (v["broker_time_ms"], v["seq_id"]))
        return None if not rows else (rows[0]["broker_time_ms"], rows[0]["seq_id"], rows[0]["origin"])

    def _entry_stream(self, a: tuple[Any, ...]) -> Any:
        return self._first([v for v in self._versions("POSITION", key=a[0]) if v["origin"] == "stream"
                            and v["broker_time_ms"] is not None and v["payload"].get("status") == "OPEN"])

    def _entry_fill(self, a: tuple[Any, ...]) -> Any:
        return self._first([v for v in self._versions("ORDER", key=a[0]) if v["broker_time_ms"] is not None
                            and v["payload"].get("status") in ("FILLED", "PARTIALLY_FILLED")])

    @staticmethod
    def _newest(rows: list[dict[str, Any]], by_time: bool = True) -> list[dict[str, Any]]:
        best: dict[str, dict[str, Any]] = {}
        for v in rows:
            k = (v["broker_time_ms"], v["seq_id"]) if by_time else (0, v["seq_id"])
            cur = best.get(v["entity_key"])
            if cur is None or k > ((cur["broker_time_ms"], cur["seq_id"]) if by_time else (0, cur["seq_id"])):
                best[v["entity_key"]] = v
        return [best[k] for k in sorted(best)]

    @staticmethod
    def _fields(v: dict[str, Any]) -> tuple[Any, ...]:
        p = v["payload"]
        pos = v["entity_type"] == "POSITION"
        return (p.get("stop_loss") if pos else p.get("entry_price"), p.get("status"),
                p.get("units") if pos else p.get("remaining_units"), p.get("order_kind"))

    def _stop_counting(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        rows = [v for v in self._versions(a[0], keys=a[1]) if v["origin"] in COUNTING
                and v["broker_time_ms"] is not None and v["broker_time_ms"] <= a[2]]
        return [(v["entity_key"], v["seq_id"], v["broker_time_ms"], v["origin"], *self._fields(v))
                for v in self._newest(rows)]

    def _marker(self, v: dict[str, Any]) -> tuple[Any, ...]:
        return (v["entity_key"], v["seq_id"], v["broker_time_ms"], v["origin"], v["payload"].get("order_kind"))

    def _stop_noncounting(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        """Every non-counting version newer than the handle's newest counting one at or before T, at most `a[3]` per
        handle, newest by seq first (`SQL_STOP_NONCOUNTING`, round 14 review)."""
        mine = self._versions(a[0], keys=a[1])
        counting = [v for v in mine if v["origin"] in COUNTING and v["broker_time_ms"] is not None
                    and v["broker_time_ms"] <= a[2]]

        def newer(v: dict[str, Any]) -> bool:
            same = [c for c in counting if c["entity_key"] == v["entity_key"]]
            if v["broker_time_ms"] is None:
                return not any(c["seq_id"] > v["seq_id"] for c in same)
            return not any((c["broker_time_ms"], c["seq_id"]) > (v["broker_time_ms"], v["seq_id"]) for c in same)

        picked = [v for v in mine if newer(v) and (
            v["broker_time_ms"] is None or (v["broker_time_ms"] <= a[2] and v["origin"] not in COUNTING))]
        out: list[dict[str, Any]] = []
        for key in sorted({v["entity_key"] for v in picked}):
            rows = sorted((v for v in picked if v["entity_key"] == key), key=lambda v: v["seq_id"], reverse=True)
            out.extend(sorted(rows[:a[3]], key=lambda v: v["seq_id"]))
        return [(v["entity_key"], v["seq_id"], v["broker_time_ms"], v["origin"], *self._fields(v)) for v in out]

    def _linked_children(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        """`orders_state` as the newest version of each ORDER entity: reduce-only orders linked to the position that
        are not terminal now or whose last version is after T (`SQL_LINKED_STOP_CHILDREN`)."""
        adapter_id, position_id, t = a
        prefix = f"order:{adapter_id}:"
        newest: dict[str, dict[str, Any]] = {}
        for v in (self.t.event_log if self._snapshot_log is None else self._snapshot_log):
            if v["entity_type"] != "ORDER" or v["action"] != "APPLIED" or not v["entity_key"].startswith(prefix):
                continue
            cur = newest.get(v["entity_key"])
            if cur is None or v["seq_id"] > cur["seq_id"]:
                newest[v["entity_key"]] = v
        out: list[tuple[Any, ...]] = []
        for key, v in sorted(newest.items()):
            p = v["payload"]
            if p.get("linked_position_id") != position_id or not p.get("is_reduce_only"):
                continue
            updated = v["broker_time_ms"] if v["broker_time_ms"] is not None else p.get("broker_updated_at_ms", 0)
            if p.get("status") not in ("FILLED", "CANCELLED", "REJECTED") or updated > t:
                out.append((key[len(prefix):],))
        return out

    def _stop_read_after(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        rows = [v for v in self._versions(a[0], keys=a[1]) if v["origin"] == "stop_read"
                and v["broker_time_ms"] is not None and v["broker_time_ms"] > a[2]]
        return [self._marker(v) for v in self._newest(rows)]

    # ---- arm_failures, outbox -------------------------------------------------------------------------
    def _failure_insert(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        if not self._fenced(a):
            return 0
        fid, scope_kind, scope_id = a[1], a[2], a[3]
        if fid in self.t.failures or any(f["scope"] == (scope_kind, scope_id) and f["cleared_at_ms"] is None
                                         for f in self.t.failures.values()):
            return 0
        self.t.failures[fid] = {
            "failure_id": fid, "scope": (scope_kind, scope_id), "edge_code": a[4], "reason": a[5],
            "probe_kind": a[6], "ping_id": a[7], "counters": a[8], "position_ids": list(a[9]),
            "first_fault_at_ms": a[10], "entered_at_ms": a[11], "epoch": a[0], "last_ping_at_ms": a[12],
            "ping_count": 1, "last_probe_at_ms": None, "last_probe_result": None, "cleared_at_ms": None,
            "clear_request_id": None}
        return 1

    def _outbox_insert(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        if not self._fenced(a):
            return 0
        n = 0
        for created, _dkey, _bypass, sev, title, body, kind, _acct in zip(*a[1:9]):
            channels = ["email", "mobile_push"] if sev in ("HIGH", "CRITICAL", "SOFT", "HARD") else ["email"]
            self.t.outbox.append({"created": created, "severity": sev, "title": title, "body": body, "kind": kind,
                                  "channels": channels})
            n += 1
        return n

    def _arm_ping(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        if not self._fenced(a):
            return 0
        self.t.outbox.append({"created": a[1], "severity": "CRITICAL", "title": a[2], "body": a[3], "kind": "arm",
                              "channels": ["arm_email", "arm_push"]})
        return 1

    def _open(self, fid: str) -> dict[str, Any] | None:
        f = self.t.failures.get(fid)
        return f if f is not None and f["cleared_at_ms"] is None else None

    def _reping(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        f = self._open(a[1])
        if not self._fenced(a) or f is None or f["ping_id"] != a[3]:
            return 0
        f["last_ping_at_ms"] = a[2]
        f["ping_count"] += 1
        return 1

    def _probe(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        f = self._open(a[1])
        if not self._fenced(a) or f is None:
            return 0
        f["last_probe_at_ms"], f["last_probe_result"] = a[2], a[3]
        return 1

    def _failures_open(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        rows = sorted((f for f in self.t.failures.values() if f["cleared_at_ms"] is None),
                      key=lambda f: (f["entered_at_ms"], f["failure_id"]))
        return [(f["failure_id"], *f["scope"], f["edge_code"], f["reason"], f["probe_kind"], f["ping_id"],
                 f["counters"], f["position_ids"], f["first_fault_at_ms"], f["entered_at_ms"], f["epoch"],
                 f["ping_count"], f["last_ping_at_ms"], f["last_probe_at_ms"], f["last_probe_result"])
                for f in rows]

    def _lock(self, a: tuple[Any, ...]) -> tuple[Any, ...]:
        self._in_tx()
        self.locks.append(a[0])
        return (None,)

    def add_request(self, request_id: int, *, kind: str, target: str, epoch: int | None = None) -> None:
        self.t.requests[request_id] = {"kind": kind, "target": target,
                                       "epoch": self.epoch if epoch is None else epoch}

    def _clear(self, a: tuple[Any, ...]) -> Any:
        self._in_tx()
        assert F.ARM_FAILED_LOCK in self.locks, "the clear runs under ARM_FAILED_LOCK"
        f = self._open(a[1])
        req = self.t.requests.get(a[3])
        if (not self._fenced(a) or f is None or req is None or req["kind"] != "arm_clear_failed"
                or req["epoch"] != a[0] or req["target"] != f"{f['scope'][0]}:{f['scope'][1]}"
                or any(g["clear_request_id"] == a[3] for g in self.t.failures.values())):
            return None
        f["cleared_at_ms"], f["clear_request_id"] = a[2], a[3]
        return (f["failure_id"], *f["scope"])

    # ---- arm_budget_counters (0020, S38f) ---------------------------------------------------------------
    def _budget_upsert(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        if not self._fenced(a):
            return 0
        assert a[4] >= 0, "CHECK (count >= 0)"
        self.t.budget[(a[1], a[2], a[3])] = (a[4], a[5], a[6], a[0], a[7])
        return 1

    def _budget_load(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        return [(*k, v[0], v[1], v[2]) for k, v in sorted(self.t.budget.items())]

    # ---- arm_status, config ---------------------------------------------------------------------------
    def _status_upsert(self, a: tuple[Any, ...]) -> int:
        cur = self.t.status.get(a[0])
        if cur is not None and cur["row"][2] > a[2]:
            return 0
        self.t.status.setdefault(a[0], {})["row"] = a              # the md record columns are kept
        return 1

    def _md_save(self, a: tuple[Any, ...]) -> int:
        cur = self.t.status.setdefault(a[0], {"row": (a[0], None, 0, None, False, False, False, None, None, 0, 0, 0,
                                                       0, False, "{}")})
        if cur.get("md") is not None and cur["md"][1] > a[2]:
            return 0
        cur["md"] = (a[1], a[2])
        return 1

    def _md_get(self, a: tuple[Any, ...]) -> tuple[Any, ...] | None:
        cur = self.t.status.get(a[0])
        return None if cur is None or cur.get("md") is None else cur["md"]  # type: ignore[no-any-return]

    def _status_all(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        return [v["row"] for _, v in sorted(self.t.status.items())]

    def _cache_upsert(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        if not self._fenced(a):
            return 0
        self.t.cache = {"row": (a[1], a[2], json.dumps(json.loads(a[3])), a[4], a[5], a[6], a[7]), "epoch": a[0]}
        return 1

    def _cache_get(self, a: tuple[Any, ...]) -> Any:
        return None if self.t.cache is None else self.t.cache["row"]

    def _config_audit(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        if not self._fenced(a):
            return 0
        self.t.config_audit.append(a)
        return 1

    def _accepted_latest(self, a: tuple[Any, ...]) -> Any:
        rows = sorted(self.t.accepted, key=lambda r: (r[4], r[0]), reverse=True)
        return rows[0] if rows else None

    # helpers -------------------------------------------------------------------------------------------
    def outbox_kinds(self) -> list[tuple[str, str]]:
        return [(r["kind"], r["severity"]) for r in self.t.outbox]
