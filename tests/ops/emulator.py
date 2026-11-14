"""PgEmulator (tests/db/pg_emulator.py) extended with the tables and exact SQL constants of src/ops.

Not a SQL engine: each handler implements the documented semantics of one statement (fence on `$1`,
compare-and-set, RETURNING, the operator_audit CHECK constraints), and every table lives in `self.t`, so
BEGIN / ROLLBACK snapshot and restore it like the halt tables. `before[sql]` hooks run just before a
statement (to bump the epoch mid-command or to make a statement fail).
"""
from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from src.db import epoch as E
from src.db.auth_state import SQL_AUTH_REARM
from src.db.cluster_state import SQL_READ_CLUSTER
from src.db.fence import SQL_CHECK_EPOCH
from src.db.pg import json_loads
from src.db.rows import breach_params
from src.ops import sql as S
from src.oversee.types import BreachState

from tests.db.pg_emulator import COLS, PgEmulator

NOW = 1_800_000_000_000
NAME = re.compile(r"^[A-Za-z0-9._@-]{2,64}$")


class CheckViolation(Exception):
    sqlstate = "23514"


class UniqueViolation(Exception):
    sqlstate = "23505"


@dataclass
class OpsTables:
    epoch: list[Any] | None = field(default_factory=lambda: ["cluster-a", 7, "node-a", NOW - 60_000])
    cluster: dict[str, Any] = field(default_factory=lambda: {
        "safe_hold": False, "since": None, "reason": None, "last_resume_at": None, "resumed_by": None})
    auth: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    dlq: dict[int, dict[str, Any]] = field(default_factory=dict)
    quarantine: dict[int, dict[str, Any]] = field(default_factory=dict)
    audit: list[dict[str, Any]] = field(default_factory=list)
    requests: list[dict[str, Any]] = field(default_factory=list)
    accounts: set[str] = field(default_factory=set)
    terms: list[tuple[Any, ...]] = field(default_factory=list)
    rulesets: dict[int, dict[str, Any]] = field(default_factory=dict)
    sweep_quarantine: dict[str, dict[str, Any]] = field(default_factory=dict)
    account_adapter: dict[str, str] = field(default_factory=dict)
    archive: dict[str, dict[str, Any]] = field(default_factory=dict)
    partition_rows: dict[str, int] = field(default_factory=dict)


class OpsPg(PgEmulator):
    def __init__(self) -> None:
        super().__init__(epoch=7)
        self.t.ops = OpsTables()          # type: ignore[attr-defined]
        self.now = NOW
        self.before: dict[str, Callable[[], None]] = {}
        self._h.update({
            SQL_CHECK_EPOCH: lambda a: (self.token(),),
            S.SQL_AUDIT_INSERT: self._audit_insert, S.SQL_AUDIT_TAIL: self._audit_tail,
            S.SQL_SAFE_HOLD_RESUME: self._resume, S.SQL_SAFE_HOLD_ROW: self._cluster_row,
            SQL_READ_CLUSTER: self._read_cluster,
            SQL_AUTH_REARM: self._rearm, S.SQL_AUTH_ROW: self._auth_row,
            S.SQL_REQUEST_PENDING: self._req_pending, S.SQL_REQUEST_INSERT: self._req_insert,
            S.SQL_DLQ_COUNTS: self._dlq_counts, S.SQL_DLQ_LIST: self._dlq_list, S.SQL_DLQ_GET: self._dlq_get,
            S.SQL_QUARANTINE_LIST: self._q_list, S.SQL_QUARANTINE_GET: self._q_get,
            S.SQL_DLQ_REQUEUE: self._dlq_requeue, S.SQL_DLQ_UNQUARANTINE: self._dlq_unquarantine,
            S.SQL_QUARANTINE_RELEASE: self._q_release_row,
            S.SQL_EPOCH_ROW: self._epoch_row, S.SQL_TERMS_RECENT: lambda a: list(self.o.terms[:a[0]]),
            E.SQL_LOCK_TIMEOUT_2S: lambda a: "SET", E.SQL_READ_EPOCH_FOR_UPDATE: self._epoch_for_update,
            S.SQL_EPOCH_ADOPT: self._adopt, S.SQL_ACCOUNT_KNOWN: self._account_known,
            S.SQL_RULESET_LOCK: lambda a: "LOCK TABLE", S.SQL_RULESET_LIST: self._rs_list,
            S.SQL_RULESET_GET: self._rs_get, S.SQL_RULESET_ACTIVE: self._rs_active,
            S.SQL_RULESET_NEXT_VERSION: lambda a: (max(self.o.rulesets, default=0) + 1,),
            S.SQL_RULESET_INSERT: self._rs_insert, S.SQL_RULESET_DEACTIVATE: self._rs_deactivate,
            S.SQL_RULESET_ACTIVATE: self._rs_activate,
            S.SQL_BREACH_IDS: self._breach_ids, S.SQL_BREACH_BY_ID: self._breach_by_id,
            S.SQL_BREACH_ACKS: self._breach_acks,
            S.SQL_SWEEP_QUARANTINE_GET: self._sq_get, S.SQL_SWEEP_QUARANTINE_LIST: self._sq_list,
            S.SQL_SWEEP_QUARANTINE_RELEASE: self._sq_release, S.SQL_ACCOUNT_ADAPTER: self._account_adapter,
            S.SQL_REQUEST_INSERT_PAYLOAD: self._req_insert_payload,
            S.SQL_ARCHIVE_GET: self._archive_get, S.SQL_ARCHIVE_LIST: self._archive_list,
            S.SQL_ARCHIVE_FORGET: self._archive_forget, S.SQL_PARTITION_ROWS: self._partition_rows,
        })

    # ---- plumbing ------------------------------------------------------------------------------------
    @property
    def o(self) -> OpsTables:
        return self.t.ops  # type: ignore[attr-defined,no-any-return]

    def token(self) -> int | None:
        return None if self.o.epoch is None else int(self.o.epoch[1])

    def _fenced(self, a: tuple[Any, ...]) -> bool:
        return bool(a) and self.token() is not None and a[0] == self.token()

    def _run(self, sql: str, args: tuple[Any, ...]) -> Any:
        hook = self.before.get(sql)
        if hook is not None:
            hook()
        return super()._run(sql, args)

    def bump(self, revision: int) -> None:
        """A LEADER_INIT bump committed by another session."""
        assert self.o.epoch is not None
        self.o.epoch[1] = revision

    def statements(self, sql: str) -> int:
        return self.sql.count(sql)

    # ---- seeding -------------------------------------------------------------------------------------
    def seed_state(self, s: BreachState) -> None:
        row = dict(zip(COLS, breach_params(s)))
        row["breach_id"] = self.t.next_breach_id
        self.t.next_breach_id += 1
        self.t.breaches[(row["rule_id"], row["subject_type"], row["subject_id"])] = row
        self.o.accounts.add(s.account_id)

    def seed_event(self, level: str, effective: str, latch: bool, by: str = "system", rule: str | None = "r",
                   reason: str = "trip", at: int = NOW - 10_000) -> int:
        eid = len(self.t.halt_events) + 1
        self.t.halt_events.append((eid, level, effective, latch, rule, reason, by, at))
        return eid

    def seed_dlq(self, dlq_id: int, status: str, *, retry: int = 2, payload: str = "{}") -> None:
        self.o.dlq[dlq_id] = {"dlq_id": dlq_id, "event_type": "ORDER", "entity_id": f"E{dlq_id}", "adapter_id": "stonex",
                              "error_reason": "schema_invalid", "retry_count": retry, "status": status,
                              "next_retry_at_ms": NOW + 600_000, "parked_at_ms": NOW - 900_000,
                              "updated_at_ms": NOW - 1000, "raw_payload": payload, "lease_owner": None}

    def seed_quarantine(self, qid: int, *, source: str = "dlq", dlq_id: int | None = None) -> None:
        self.o.quarantine[qid] = {"quarantine_id": qid, "source": source, "dlq_id": dlq_id,
                                  "feed_entry_id": None if source == "dlq" else "1-0", "event_type": "ORDER",
                                  "entity_id": "E", "adapter_id": "stonex", "account_id": "ACC",
                                  "error_reason": "schema_invalid", "retry_count": 5, "quarantined_at_ms": NOW,
                                  "released_at_ms": None, "released_by": None, "release_audit_id": None}

    # ---- operator_audit --------------------------------------------------------------------------------
    def _audit_insert(self, a: tuple[Any, ...]) -> Any:
        command, target, operator, approver, reason, outcome, detail, epoch, os_user = a
        if not NAME.match(operator) or operator.lower() == "system":
            raise CheckViolation("operator_audit_operator_check")
        if approver is not None and (not NAME.match(approver) or approver.lower() == operator.lower()):
            raise CheckViolation("operator_audit_approver_check")
        if len(reason.strip()) < 8 or outcome not in ("applied", "refused", "noop"):
            raise CheckViolation("operator_audit_check")
        row = {"audit_id": len(self.o.audit) + 1, "at_ms": self.now, "command": command, "target": target,
               "operator": operator, "approver": approver, "reason": reason, "outcome": outcome,
               "detail": detail, "epoch": epoch, "db_user": "xtnl_operator", "os_user": os_user}
        self.o.audit.append(row)
        return (row["audit_id"], row["at_ms"])

    def _audit_tail(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        rows = [r for r in reversed(self.o.audit) if a[0] is None or r["command"] == a[0]][:a[1]]
        return [(r["audit_id"], r["at_ms"], r["command"], r["target"], r["operator"], r["approver"], r["outcome"],
                 r["reason"], r["epoch"], r["db_user"], r["detail"]) for r in rows]

    # ---- cluster_state -----------------------------------------------------------------------------------
    def _resume(self, a: tuple[Any, ...]) -> Any:
        c = self.o.cluster
        if not self._fenced(a) or not c["safe_hold"]:
            return None
        c.update(safe_hold=False, last_resume_at=self.now, resumed_by=a[1])
        return (c["since"], c["reason"], c["last_resume_at"])

    def _cluster_row(self, a: tuple[Any, ...]) -> Any:
        c = self.o.cluster
        return (c["safe_hold"], c["since"], c["reason"], c["last_resume_at"], c["resumed_by"], self.now)

    def _read_cluster(self, a: tuple[Any, ...]) -> Any:
        c = self.o.cluster
        return (c["safe_hold"], c["since"], c["reason"], c["last_resume_at"], None, None, "{}")

    # ---- adapter_auth_state ------------------------------------------------------------------------------
    def _rearm(self, a: tuple[Any, ...]) -> int:
        row = self.o.auth.get((a[1], a[2]))
        if not self._fenced(a) or row is None or row["state"] != "AUTH_FAILED":
            return 0
        row.update(state="REARMED", rearmed_by=a[3], rearmed_at_ms=self.now)
        return 1

    def _auth_row(self, a: tuple[Any, ...]) -> Any:
        row = self.o.auth.get((a[0], a[1]))
        return None if row is None else (row["state"], 3, NOW, row.get("rearmed_by"), row.get("rearmed_at_ms"))

    # ---- operator_requests -------------------------------------------------------------------------------
    def _req_pending(self, a: tuple[Any, ...]) -> Any:
        for r in self.o.requests:
            if (r["kind"], r["target"], r["requested_epoch"]) == a and r["consumed_at_ms"] is None:
                return (r["request_id"],)
        return None

    def _req_insert(self, a: tuple[Any, ...]) -> Any:
        if not self._fenced(a):
            return None
        if not any(x["audit_id"] == a[3] for x in self.o.audit):
            raise CheckViolation("operator_requests_audit_id_fkey")
        r = {"request_id": len(self.o.requests) + 1, "kind": a[1], "target": a[2], "audit_id": a[3],
             "requested_epoch": a[0], "consumed_at_ms": None, "payload": "{}"}
        self.o.requests.append(r)
        return (r["request_id"],)

    def _req_insert_payload(self, a: tuple[Any, ...]) -> Any:
        if a[1] not in ("reset_task", "release_account", "refresh_cash_flows"):
            raise CheckViolation("operator_requests_kind_check")
        row = self._req_insert(a[:4])
        if row is not None:
            self.o.requests[-1]["payload"] = a[4]
        return row

    # ---- DLQ ---------------------------------------------------------------------------------------------
    @staticmethod
    def _dlq_tuple(r: dict[str, Any]) -> tuple[Any, ...]:
        return (r["dlq_id"], r["event_type"], r["entity_id"], r["adapter_id"], r["error_reason"], r["retry_count"],
                r["status"], r["next_retry_at_ms"], r["parked_at_ms"], r["updated_at_ms"])

    def _dlq_counts(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        counts: dict[str, int] = {}
        for r in self.o.dlq.values():
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        return sorted(counts.items())

    def _dlq_list(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        rows = [r for _, r in sorted(self.o.dlq.items(), reverse=True) if a[0] is None or r["status"] == a[0]]
        return [self._dlq_tuple(r) for r in rows[:a[1]]]

    def _dlq_get(self, a: tuple[Any, ...]) -> Any:
        r = self.o.dlq.get(a[0])
        return None if r is None else self._dlq_tuple(r)

    @staticmethod
    def _q_tuple(r: dict[str, Any]) -> tuple[Any, ...]:
        return (r["quarantine_id"], r["source"], r["dlq_id"], r["feed_entry_id"], r["event_type"], r["entity_id"],
                r["adapter_id"], r["account_id"], r["error_reason"], r["retry_count"], r["quarantined_at_ms"],
                r["released_at_ms"], r["released_by"])

    def _q_list(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        rows = [r for _, r in sorted(self.o.quarantine.items(), reverse=True) if a[0] or r["released_at_ms"] is None]
        return [self._q_tuple(r) for r in rows[:a[1]]]

    def _q_get(self, a: tuple[Any, ...]) -> Any:
        r = self.o.quarantine.get(a[0])
        return None if r is None else self._q_tuple(r)

    def _dlq_requeue(self, a: tuple[Any, ...]) -> Any:
        r = self.o.dlq.get(a[1])
        if not self._fenced(a) or r is None or r["status"] not in ("QUEUED", "BACKOFF"):
            return None
        r.update(status="QUEUED", next_retry_at_ms=self.now, updated_at_ms=self.now)
        return (r["retry_count"],)

    def _dlq_unquarantine(self, a: tuple[Any, ...]) -> Any:
        r = self.o.dlq.get(a[1])
        if not self._fenced(a) or r is None or r["status"] != "QUARANTINED":
            return None
        r.update(status="QUEUED", retry_count=0, next_retry_at_ms=self.now, updated_at_ms=self.now)
        return (r["dlq_id"],)

    def _q_release_row(self, a: tuple[Any, ...]) -> int:
        r = self.o.quarantine.get(a[1])
        if not self._fenced(a) or r is None or r["released_at_ms"] is not None:
            return 0
        r.update(released_at_ms=self.now, released_by=a[2], release_audit_id=a[3])
        return 1

    # ---- engine_epoch ------------------------------------------------------------------------------------
    def _epoch_row(self, a: tuple[Any, ...]) -> Any:
        e = self.o.epoch
        return None if e is None else (e[0], e[1], e[1], e[2], e[3])

    def _epoch_for_update(self, a: tuple[Any, ...]) -> Any:
        e = self.o.epoch
        return None if e is None else (e[0], e[1])

    def _adopt(self, a: tuple[Any, ...]) -> int:
        e = self.o.epoch
        if e is None or e[0] != a[2] or e[1] != a[3]:
            return 0
        self.o.epoch = [a[0], 0, a[1], self.now]
        return 1

    # ---- rulesets (jsonb comes back decoded, as src.db.pg's codec does) -------------------------------------
    def seed_ruleset(self, version: int, config: Any, *, active: bool = False) -> None:
        self.o.rulesets[version] = {"config": config, "active": active, "created_by": "seed",
                                    "reason": "seeded row", "created_at_ms": NOW - 1000}

    def _rs_tuple(self, v: int) -> tuple[Any, ...]:
        r = self.o.rulesets[v]
        return (v, r["config"], r["active"], r["created_by"], r["reason"], r["created_at_ms"])

    def _rs_list(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        return [self._rs_tuple(v) for v in sorted(self.o.rulesets, reverse=True)[:a[0]]]

    def _rs_get(self, a: tuple[Any, ...]) -> Any:
        return self._rs_tuple(a[0]) if a[0] in self.o.rulesets else None

    def _rs_active(self, a: tuple[Any, ...]) -> Any:
        act = sorted((v for v, r in self.o.rulesets.items() if r["active"]), reverse=True)
        return self._rs_tuple(act[0]) if act else None

    def _rs_insert(self, a: tuple[Any, ...]) -> Any:
        version, text, operator, reason = a
        if version in self.o.rulesets:
            raise UniqueViolation("rulesets_pkey")
        self.o.rulesets[version] = {"config": json_loads(text), "active": False, "created_by": operator,
                                    "reason": reason, "created_at_ms": self.now}
        return (self.now,)

    def _rs_deactivate(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        out = []
        for v, r in self.o.rulesets.items():
            if r["active"] and v != a[0]:
                r["active"] = False
                out.append((v,))
        return out

    def _rs_activate(self, a: tuple[Any, ...]) -> int:
        r = self.o.rulesets.get(a[0])
        if r is None or r["active"]:
            return 0
        if any(x["active"] for x in self.o.rulesets.values()):
            raise UniqueViolation("rulesets_one_active")
        r["active"] = True
        return 1

    # ---- halts -------------------------------------------------------------------------------------------
    def _account_known(self, a: tuple[Any, ...]) -> Any:
        return (a[0] in self.o.accounts,)

    # ---- halt acknowledge ----------------------------------------------------------------------------------
    def _breach_ids(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        return [(r["breach_id"], k[0], k[1], k[2]) for k, r in self.t.breaches.items() if r["resolved_at"] is None]

    def _breach_by_id(self, a: tuple[Any, ...]) -> Any:
        for k, r in self.t.breaches.items():
            if r["breach_id"] == a[0]:
                return (k[0], k[1], k[2], r["account_id"], r["resolved_at"])
        return None

    def _breach_acks(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        keys = {r["breach_id"]: k for k, r in self.t.breaches.items()}
        out = []
        for ack in self.t.acks:
            if ack["breach_id"] == a[0]:
                k = keys[a[0]]
                out.append((ack["ack_id"], k[0], k[1], k[2], ack["level"], ack["cap"], ack["reason"], ack["operator"],
                            ack["why"], ack["at"]))
        return out

    # ---- sweep_quarantine / accounts ------------------------------------------------------------------------
    def seed_sweep_quarantine(self, account: str, *, adapter: str | None = "stonex", released: bool = False) -> None:
        self.o.sweep_quarantine[account] = {
            "adapter_id": adapter, "account_id": account, "failing_since_ms": NOW - 900_000,
            "quarantined_at_ms": NOW - 600_000, "reason": "consecutive pass failures",
            "released_at_ms": NOW - 1000 if released else None, "released_by": "pass" if released else None}

    @staticmethod
    def _sq_tuple(r: dict[str, Any]) -> tuple[Any, ...]:
        return (r["adapter_id"], r["account_id"], r["failing_since_ms"], r["quarantined_at_ms"], r["reason"],
                r["released_at_ms"], r["released_by"])

    def _sq_get(self, a: tuple[Any, ...]) -> Any:
        r = self.o.sweep_quarantine.get(a[0])
        return None if r is None else self._sq_tuple(r)

    def _sq_list(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        rows = [r for r in self.o.sweep_quarantine.values() if a[0] or r["released_at_ms"] is None]
        return [self._sq_tuple(r) for r in rows[:a[1]]]

    def _sq_release(self, a: tuple[Any, ...]) -> Any:
        r = self.o.sweep_quarantine.get(a[1])
        if not self._fenced(a) or r is None or r["released_at_ms"] is not None:
            return None
        r.update(released_at_ms=self.now, released_by=a[2])
        return (r["quarantined_at_ms"], r["reason"])

    def _account_adapter(self, a: tuple[Any, ...]) -> Any:
        adapter = self.o.account_adapter.get(a[0])
        return None if adapter is None else (adapter,)

    # ---- partition_archive -----------------------------------------------------------------------------------
    def seed_archive(self, name: str, rows: int, *, detached: bool = False) -> None:
        self.o.archive[name] = {"partition_name": name, "parent_table": name.rsplit("_p", 1)[0], "row_count": rows,
                                "checksum": "sha256:" + "a" * 64, "archive_uri": f"file:///var/lib/xtnl/archive/{name}",
                                "verified_at_ms": NOW - 86_400_000, "verified_by": "engine:node-a",
                                "detached_at_ms": NOW - 1000 if detached else None,
                                "detached_epoch": 7 if detached else None}

    @staticmethod
    def _archive_tuple(r: dict[str, Any]) -> tuple[Any, ...]:
        return (r["partition_name"], r["parent_table"], r["row_count"], r["checksum"], r["archive_uri"],
                r["verified_at_ms"], r["verified_by"], r["detached_at_ms"], r["detached_epoch"])

    def _archive_get(self, a: tuple[Any, ...]) -> Any:
        r = self.o.archive.get(a[0])
        return None if r is None else self._archive_tuple(r)

    def _archive_list(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        return [self._archive_tuple(self.o.archive[k]) for k in sorted(self.o.archive, reverse=True)[:a[0]]]

    def _archive_forget(self, a: tuple[Any, ...]) -> Any:
        r = self.o.archive.get(a[1])
        if not self._fenced(a) or r is None or r["detached_at_ms"] is not None:
            return None
        del self.o.archive[a[1]]
        return (r["parent_table"], r["row_count"], r["checksum"], r["archive_uri"])

    def _partition_rows(self, a: tuple[Any, ...]) -> Any:
        n = self.o.partition_rows.get(a[0])
        return (n,)
