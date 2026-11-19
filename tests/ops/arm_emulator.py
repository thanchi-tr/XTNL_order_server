"""OpsPg (tests/ops/emulator.py) extended with the ARM tables and the exact SQL constants of `src/ops/arm_sql.py`
(plan S34). Not a SQL engine: each handler implements its statement's documented semantics. Every table lives in
`self.t.arm`, so BEGIN / ROLLBACK snapshot and restore it. Synthetic instruments and accounts only."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.db.arm_config_store import SQL_ACCEPTED_LATEST, SQL_CONFIG_CACHE_GET, SQL_CONTROLS_GET
from src.db.operator_requests import REQUEST_KINDS
from src.ops import arm_sql as A

from tests.ops.emulator import NOW, CheckViolation, OpsPg


@dataclass
class ArmTables:
    controls: dict[str, Any] | None = field(default_factory=lambda: {
        "paused": False, "pause_reason": None, "paused_at_ms": None, "drain_notices": False,
        "notify_test_at_ms": None, "updated_at_ms": NOW - 86_400_000, "updated_by": None, "audit_id": None})
    accepted: list[dict[str, Any]] = field(default_factory=list)
    cache: tuple[Any, ...] | None = None
    status: dict[str, dict[str, Any]] = field(default_factory=dict)
    instruments: list[tuple[str, str]] = field(default_factory=lambda: [("mock", "SYN.A"), ("mock", "SYN.B")])
    accounts: list[tuple[str, str]] = field(default_factory=lambda: [("mock", "acct-1")])
    held: list[tuple[str, str]] = field(default_factory=list)
    positions: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    failures: list[dict[str, Any]] = field(default_factory=list)


class ArmOpsPg(OpsPg):
    def __init__(self) -> None:
        super().__init__()
        self.t.arm = ArmTables()        # type: ignore[attr-defined]
        self._h.update({
            A.SQL_ARM_HEARTBEAT: self._heartbeat, A.SQL_ARM_PENDING_COUNT: self._pending_count,
            A.SQL_REQUEST_RESULT: self._result, A.SQL_PENDING_CLEARS: self._pending_clears,
            A.SQL_ARM_STATUS_NODES: self._status_nodes,
            A.SQL_CONTROLS_LOCK: lambda a: "LOCK TABLE", A.SQL_CONTROLS_FOR_UPDATE: self._controls_for_update,
            A.SQL_CONTROLS_PAUSE: self._pause, A.SQL_CONTROLS_RESUME: self._resume_controls,
            A.SQL_CONTROLS_NOTIFY_TEST: self._notify_test, SQL_CONTROLS_GET: self._controls_get,
            A.SQL_ACCEPTED_LOCK: lambda a: "LOCK TABLE", A.SQL_ACCEPTED_INSERT: self._accepted_insert,
            A.SQL_ACCEPTED_RECENT: self._accepted_recent, SQL_ACCEPTED_LATEST: self._accepted_latest,
            SQL_CONFIG_CACHE_GET: lambda a: self.a.cache,
            A.SQL_INSTRUMENT_SYMBOLS: lambda a: sorted(self.a.instruments),
            A.SQL_ACCOUNT_IDS: lambda a: sorted(self.a.accounts), A.SQL_HELD_SYMBOLS: lambda a: sorted(self.a.held),
            A.SQL_ARMED_LOAD: self._armed_load, A.SQL_ARM_POSITION_STATE: self._position_state,
            A.SQL_FAILED_LIST: self._failed_list, A.SQL_FAILED_OPEN_SCOPE: self._failed_open,
        })

    @property
    def a(self) -> ArmTables:
        return self.t.arm  # type: ignore[attr-defined,no-any-return]

    # ---- seeding ---------------------------------------------------------------------------------------
    def heartbeat(self, age_ms: int = 2_000, *, node: str = "node-a", leader: bool = True,
                  epoch: int | None = None) -> None:
        self.a.status[node] = {"node": node, "epoch": self.token() if epoch is None else epoch,
                               "heartbeat_at_ms": self.now - age_ms,
                               "leader": leader, "running": False, "paused": False, "pause_reason": None,
                               "config_doc_hash": None, "armed": 0, "overdue": 0, "failed_scopes": 0,
                               "failed_global": False}

    def seed_position(self, adapter: str, position: str, state: str = "ARMED", account: str = "acct-1") -> None:
        self.a.positions[(adapter, position)] = {"state": state, "account_id": account}

    def seed_failure(self, scope_kind: str, scope_id: str, *, cleared: bool = False, positions: int = 2) -> None:
        n = len(self.a.failures) + 1
        self.a.failures.append({
            "failure_id": f"f{n}", "scope_kind": scope_kind, "scope_id": scope_id, "edge_code": None,
            "reason": "invalid_bar", "probe_kind": "series_reread", "ping_id": f"p{n}",
            "counters": '{"invalid_bars": 3, "note": "x"}',
            "position_ids": [f"mock:P{i}" for i in range(positions)], "first_fault_at_ms": NOW - 600_000,
            "entered_at_ms": NOW - 500_000 + n, "epoch": 7, "last_ping_at_ms": NOW - 500_000, "ping_count": 1,
            "last_probe_at_ms": None, "last_probe_result": None,
            "cleared_at_ms": NOW - 1000 if cleared else None, "clear_request_id": 1 if cleared else None})

    def answer(self, request_id: int, result: str) -> None:
        for r in self.o.requests:
            if r["request_id"] == request_id:
                r["consumed_at_ms"] = self.now
                r["result"] = result

    # ---- operator_requests: every kind of 0017 ------------------------------------------------------------
    def _req_insert_payload(self, a: tuple[Any, ...]) -> Any:
        if a[1] not in REQUEST_KINDS:
            raise CheckViolation("operator_requests_kind_check")
        row = self._req_insert(a[:4])
        if row is not None:
            self.o.requests[-1]["payload"] = a[4]
            self.o.requests[-1]["result"] = None
        return row

    def _pending_count(self, a: tuple[Any, ...]) -> Any:
        return (sum(1 for r in self.o.requests if r["requested_epoch"] == a[0] and r["consumed_at_ms"] is None
                    and r["kind"] in a[1]),)

    def _result(self, a: tuple[Any, ...]) -> Any:
        for r in self.o.requests:
            if r["request_id"] == a[0]:
                return (r["consumed_at_ms"], r.get("result"))
        return None

    def _pending_clears(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        return [(r["request_id"], r["target"]) for r in self.o.requests
                if r["kind"] == "arm_clear_failed" and r["consumed_at_ms"] is None and r["requested_epoch"] == a[0]]

    # ---- arm_status ------------------------------------------------------------------------------------------
    def _heartbeat(self, a: tuple[Any, ...]) -> Any:
        rows = sorted((r for r in self.a.status.values()
                       if r["leader"] and r["epoch"] == a[0] and r["heartbeat_at_ms"] is not None),
                      key=lambda r: r["heartbeat_at_ms"], reverse=True)
        if not rows:
            return None
        r = rows[0]
        return (r["node"], r["heartbeat_at_ms"], self.now - r["heartbeat_at_ms"], r["epoch"])

    def _status_nodes(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        return [(r["node"], r["epoch"], r["heartbeat_at_ms"], None if r["heartbeat_at_ms"] is None
                 else self.now - r["heartbeat_at_ms"], r["leader"], r["running"], r["paused"], r["pause_reason"],
                 r["config_doc_hash"], r["armed"], r["overdue"], r["failed_scopes"], r["failed_global"])
                for _, r in sorted(self.a.status.items())]

    # ---- arm_controls ------------------------------------------------------------------------------------------
    def _controls_tuple(self) -> Any:
        c = self.a.controls
        if c is None:
            return None
        return (c["paused"], c["pause_reason"], c["paused_at_ms"], c["drain_notices"], c["notify_test_at_ms"],
                c["updated_at_ms"], c["updated_by"])

    def _controls_for_update(self, a: tuple[Any, ...]) -> Any:
        return self._controls_tuple()

    def _controls_get(self, a: tuple[Any, ...]) -> Any:
        t = self._controls_tuple()
        return None if t is None else t[:6]

    def _audit_known(self, audit_id: int) -> None:
        if not any(x["audit_id"] == audit_id for x in self.o.audit):
            raise CheckViolation("audit row missing")

    def _pause(self, a: tuple[Any, ...]) -> Any:
        c = self.a.controls
        if c is None:
            return None
        self._audit_known(a[3])
        if not (c["paused"] and c["paused_at_ms"] is not None):
            c["paused_at_ms"] = self.now
        c.update(paused=True, pause_reason=a[0], drain_notices=a[1], updated_at_ms=self.now, updated_by=a[2],
                 audit_id=a[3])
        return (c["paused_at_ms"],)

    def _resume_controls(self, a: tuple[Any, ...]) -> Any:
        c = self.a.controls
        if c is None or not c["paused"]:
            return None
        self._audit_known(a[1])
        c.update(paused=False, pause_reason=None, paused_at_ms=None, drain_notices=False, updated_at_ms=self.now,
                 updated_by=a[0], audit_id=a[1])
        return (self.now,)

    def _notify_test(self, a: tuple[Any, ...]) -> Any:
        c = self.a.controls
        if c is None:
            return None
        self._audit_known(a[1])
        c.update(notify_test_at_ms=self.now, updated_at_ms=self.now, updated_by=a[0], audit_id=a[1])
        return (self.now,)

    # ---- arm_accepted / cache --------------------------------------------------------------------------------
    def _accepted_insert(self, a: tuple[Any, ...]) -> Any:
        self._audit_known(a[4])
        row = {"accepted_id": len(self.a.accepted) + 1, "doc_hash": a[0], "schema_version": a[1], "mode": "manual",
               "accepted_at_ms": a[2], "accepted_by": a[3], "audit_id": a[4]}
        self.a.accepted.append(row)
        return (row["accepted_id"],)

    def _accepted_sorted(self) -> list[dict[str, Any]]:
        return sorted(self.a.accepted, key=lambda r: (r["accepted_at_ms"], r["accepted_id"]), reverse=True)

    def _accepted_recent(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        return [(r["accepted_id"], r["doc_hash"], r["schema_version"], r["mode"], r["accepted_at_ms"],
                 r["accepted_by"], r["audit_id"]) for r in self._accepted_sorted()[:a[0]]]

    def _accepted_latest(self, a: tuple[Any, ...]) -> Any:
        rows = self._accepted_sorted()
        if not rows:
            return None
        r = rows[0]
        return (r["accepted_id"], r["doc_hash"], r["schema_version"], r["mode"], r["accepted_at_ms"],
                r["accepted_by"])

    # ---- arm_positions / arm_failures ------------------------------------------------------------------------
    def _armed_load(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        out: dict[str, tuple[int, set[str]]] = {}
        for (adapter, _), p in self.a.positions.items():
            if p["state"] in ("CLOSED", "DISARMED"):
                continue
            n, accts = out.get(adapter, (0, set()))
            out[adapter] = (n + 1, accts | {p["account_id"]})
        return [(k, n, len(accts)) for k, (n, accts) in sorted(out.items())]

    def _position_state(self, a: tuple[Any, ...]) -> Any:
        p = self.a.positions.get((a[0], a[1]))
        return None if p is None else (p["state"],)

    def _failed_tuple(self, r: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(r[k] for k in ("failure_id", "scope_kind", "scope_id", "edge_code", "reason", "probe_kind",
                                    "ping_id", "counters", "position_ids", "first_fault_at_ms", "entered_at_ms",
                                    "epoch", "last_ping_at_ms", "ping_count", "last_probe_at_ms",
                                    "last_probe_result", "cleared_at_ms", "clear_request_id"))

    def _failed_list(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        rows = [r for r in self.a.failures if a[0] or r["cleared_at_ms"] is None]
        rows.sort(key=lambda r: (-r["entered_at_ms"], r["failure_id"]))
        return [self._failed_tuple(r) for r in rows[:a[1]]]

    def _failed_open(self, a: tuple[Any, ...]) -> Any:
        for r in self.a.failures:
            if (r["scope_kind"], r["scope_id"]) == (a[0], a[1]) and r["cleared_at_ms"] is None:
                return (r["failure_id"], r["reason"], r["probe_kind"])
        return None
