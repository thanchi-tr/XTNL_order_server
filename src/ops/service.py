"""The operator commands (backlog 4.6; operations.md s2, s3, s4, s8, s13, s16; oversee-rules.md s4, s8;
tier-persistence.md s5).

Rules every mutating command follows:

* **Authenticated and audited:** the PostgreSQL login is recorded (`operator_audit.db_user`) with the
  named operator, the approver where two-person control applies, the written reason and the OS user.
  The audit row is inserted in the **same transaction** as the change, so a change without its audit row
  (or the reverse) cannot commit. A refused or no-op command gets its own audit row.
* **Lock-free epoch fence** for engine state (operations.md s2): the command reads `engine_epoch.epoch`
  once, then every state write carries `(SELECT epoch FROM engine_epoch WHERE id = 1) = $1` in the same
  statement. If a leader bumps the epoch meanwhile, the write affects 0 rows, the transaction rolls back
  and the command is refused (`epoch_moved`), never half-applied under a different term. Epoch adoption
  changes the fence itself, so it is a compare-and-set on the reviewed row under the row lock instead.
* **Phase 1 boundary:** no command sends anything to a broker or enables any order action. A halt clear
  only lowers the recorded level (oversee-rules.md s4); ActionGate keeps every other condition.
* Two-person control (`--approver`): SAFE_HOLD resume and epoch adoption (runbooks: two people, ticket),
  a forced halt clear, a ruleset activation and `archive forget` (partition-archive.md: two people).
* **In-process requests** (`operator_requests`, src/db/operator_requests.py): `task reset`, `account release`
  and `cash-flows refresh` queue a request for the leader of the current epoch in the same transaction as
  the audit row (and, for `account release`, the `sweep_quarantine` release); the leader consumes it.
* `halt ack` is `HaltSwitch.acknowledge` (oversee-rules.md s4) under HALT_LOCK in the CLI's fenced
  transaction: one `breach_acks` row that silences the state's re-pages until its level, cap or reason
  changes; it never changes a state or the level.
* **Rulesets** (operator-owned config the engine only reads): `ruleset add` stores a document validated by
  the engine's `parse_ruleset` as the next version, inactive, written as `ruleset_json` and re-parsed
  from the stored row before commit; `ruleset activate` re-validates the stored row and makes it the single
  active row. Both run unfenced under a table lock (not engine state) with the audit row in the same
  transaction.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any, Final

from src.db.auth_state import SQL_AUTH_REARM
from src.db.cluster_state import read_cluster_state
from src.db.epoch import (
    HALT_LOCK,
    SQL_BEGIN_RC,
    SQL_COMMIT,
    SQL_COMMIT_TIMEOUT_OFF,
    SQL_LOCK_TIMEOUT_2S,
    SQL_READ_EPOCH_FOR_UPDATE,
    SQL_ROLLBACK,
    CommitUnknownError,
    epoch_transaction,
)
from src.db.fence import EpochMismatch, FencedTx, stored_epoch
from src.db.halt_store import SQL_HALT_LOCK, SQL_LATEST_EVENT, SQL_UNRESOLVED_BREACHES, PgHaltTxn
from src.db.operator_requests import AccountRelease, CashFlowRefresh
from src.db.pg import PgConnection, rowcount
from src.db.rows import ack_from_row, breach_from_row, halt_event_from_row, subject_from_db, subject_to_db
from src.oversee.halt import HaltSwitch
from src.oversee.notify import is_acknowledged
from src.oversee.types import Level
from src.runtime.cluster import mirror_json, parse_mirror
from src.runtime.etcd import SAFE_HOLD_KEY
from src.runtime.roles import ClusterState

from src.ops import sql as S
from src.ops.etcd import OpsEtcd
from src.ops.halt import InTxHaltStore, OpsHaltStore, OpsHaltTxn, clear_detail, resolved_transitions
from src.ops.model import Actor, OpsRefused, OpsUsageError, Report, iso_ms
from src.ops.rulesets import check_document, check_row, effective_json, same_ruleset, summary

log = logging.getLogger(__name__)

TASK_NAME_MAX: Final = 64
DLQ_STATUSES: Final = frozenset({"QUEUED", "LEASED", "BACKOFF", "RESOLVED", "QUARANTINED"})
ACCOUNT_MAX: Final = 128
PARTITION_RE: Final = re.compile(r"^[a-z_]+_p\d{6}$")


def _dumps(d: dict[str, Any]) -> str:
    return json.dumps(d, sort_keys=True, separators=(",", ":"), default=str)


def _loads(v: object) -> Any:
    if isinstance(v, (str, bytes)):
        return json.loads(v)
    return v


def _span(start_ms: int, end_ms: int) -> str:
    """A duration as '1d 2h 3m' / '2h 3m' / '3m 4s' (never negative)."""
    s = max(0, end_ms - start_ms) // 1000
    d, s = divmod(s, 86_400)
    h, s = divmod(s, 3_600)
    m, s = divmod(s, 60)
    if d:
        return f"{d}d {h}h {m}m"
    if h:
        return f"{h}h {m}m"
    return f"{m}m {s}s"


def _ms() -> int:
    return int(time.time() * 1000)


class OpsService:
    """One CLI invocation: one PostgreSQL connection (application_name `xtnl-ops`, never matched by the
    post-bump backend termination) and an optional etcd client."""

    def __init__(self, conn: PgConnection, etcd: OpsEtcd | None = None, *, etcd_timeout_s: float = 2.0,
                 clock_ms: Callable[[], int] = _ms) -> None:
        self._conn = conn
        self._etcd = etcd
        self._etcd_timeout = etcd_timeout_s
        self._clock_ms = clock_ms

    # ------------------------------------------------------------------ transactions and audit
    async def _audit(self, conn: PgConnection | FencedTx, command: str, target: str | None, actor: Actor,
                     outcome: str, detail: dict[str, Any], epoch: int | None) -> int:
        row = await conn.fetchrow(S.SQL_AUDIT_INSERT, command, target, actor.operator, actor.approver,
                                  actor.reason.strip(), outcome, _dumps(detail), epoch, actor.os_user)
        if row is None:
            raise RuntimeError("operator_audit insert returned no row")
        return int(row[0])

    async def audit_standalone(self, command: str, target: str | None, actor: Actor, outcome: str,
                               detail: dict[str, Any]) -> int | None:
        """A refused / no-op command's own audit row (autocommit). Never raises: a failure is logged."""
        try:
            epoch = await stored_epoch(self._conn)
            return await self._audit(self._conn, command, target, actor, outcome, detail, epoch)
        except Exception as exc:
            log.error("operator_audit row for refused %s not written: %s", command, type(exc).__name__)
            return None

    @asynccontextmanager
    async def _fenced(self) -> AsyncIterator[FencedTx]:
        token = await stored_epoch(self._conn)
        if token is None:
            raise OpsRefused("no_engine_epoch", "engine_epoch has no row: there is no fence to write under")
        try:
            async with epoch_transaction(self._conn, token) as tx:
                yield tx
        except EpochMismatch as exc:
            raise OpsRefused("epoch_moved", f"engine_epoch changed from {token} during the command; "
                                            "re-check the state and re-run") from exc

    @asynccontextmanager
    async def _plain(self) -> AsyncIterator[PgConnection]:
        """READ COMMITTED transaction without the fence: epoch adoption (it changes the fence itself) and the
        operator-owned `rulesets` table (not engine state; the engine only reads it)."""
        await self._conn.execute(SQL_BEGIN_RC)
        try:
            yield self._conn
            await self._conn.execute(SQL_COMMIT_TIMEOUT_OFF)
        except BaseException:
            try:
                await self._conn.execute(SQL_ROLLBACK)
            except Exception as exc:   # the connection may be gone; the server rolls back
                log.debug("rollback failed: %s", type(exc).__name__)
            raise
        try:
            await self._conn.execute(SQL_COMMIT)
        except Exception as exc:
            raise CommitUnknownError("COMMIT outcome unknown; check `audit tail` before re-running") from exc

    async def _refused(self, report: Report, refusal: OpsRefused, target: str | None, actor: Actor,
                       detail: dict[str, Any] | None = None) -> Report:
        report.outcome = "refused"
        report.fields["refused"] = refusal.code
        if refusal.detail:
            report.fields["detail"] = refusal.detail
        report.audit_id = await self.audit_standalone(
            report.command, target, actor, "refused", {"code": refusal.code, "detail": refusal.detail,
                                                        **(detail or {})})
        return report

    # ------------------------------------------------------------------ SAFE_HOLD
    async def safe_hold_status(self) -> Report:
        rep = Report("safe_hold.status")
        row = await self._conn.fetchrow(S.SQL_SAFE_HOLD_ROW)
        if row is None:
            rep.warnings.append("cluster_state has no row (migration 0001 inserts it)")
            pg: ClusterState | None = None
        else:
            pg = ClusterState(bool(row[0]), None if row[1] is None else int(row[1]),
                              None if row[2] is None else str(row[2]), None if row[3] is None else int(row[3]))
            rep.fields.update({"safe_hold": pg.safe_hold, "since": iso_ms(pg.safe_hold_since),
                               "reason": pg.safe_hold_reason, "last_resume_at": iso_ms(pg.last_resume_at),
                               "resumed_by": row[4]})
        if self._etcd is None:
            rep.fields["etcd_mirror"] = "not checked (XTNL_ETCD_ENDPOINTS not set)"
            return rep
        etcd = self._etcd
        try:
            kv = await asyncio.to_thread(etcd.get, SAFE_HOLD_KEY, timeout_s=self._etcd_timeout)
        except Exception as exc:
            rep.warnings.append(f"etcd mirror unreadable ({type(exc).__name__})")
            return rep
        if kv is None:
            rep.fields["etcd_mirror"] = "absent"
            if pg is not None and pg.safe_hold:
                rep.warnings.append("PostgreSQL has SAFE_HOLD but the etcd mirror is absent")
            return rep
        try:
            mirror = parse_mirror(kv.value)
        except (ValueError, TypeError) as exc:
            rep.warnings.append(f"etcd mirror unparseable ({type(exc).__name__})")
            return rep
        rep.fields["etcd_mirror"] = "set" if mirror.safe_hold else "clear"
        if pg is not None and mirror.safe_hold != pg.safe_hold:
            rep.warnings.append("etcd mirror disagrees with PostgreSQL (PostgreSQL is the truth; the leader "
                                "re-mirrors it, and nodes read the mirror only while PostgreSQL is unreadable)")
        return rep

    async def safe_hold_resume(self, actor: Actor) -> Report:
        actor.validate(two_person=True)
        rep = Report("safe_hold.resume")
        try:
            async with self._fenced() as tx:
                row = await tx.write_returning(S.SQL_SAFE_HOLD_RESUME, actor.operator)
                if row is None:
                    raise OpsRefused("not_in_safe_hold", "cluster_state.safe_hold is false; nothing to resume")
                detail = {"safe_hold_since": row[0], "safe_hold_reason": row[1], "last_resume_at": row[2]}
                rep.audit_id = await self._audit(tx, rep.command, "cluster", actor, "applied", detail, tx.epoch)
                rep.fields.update({"resumed": True, "epoch": tx.epoch, "last_resume_at": iso_ms(row[2]),
                                   "was_reason": row[1]})
        except OpsRefused as r:
            return await self._refused(rep, r, "cluster", actor)
        rep.outcome = "applied"
        await self._mirror_safe_hold(rep)
        return rep

    async def _mirror_safe_hold(self, rep: Report) -> None:
        """Write PostgreSQL's SAFE_HOLD to /xtnl/safe_hold exactly as the leader does (cluster.py)."""
        if self._etcd is None:
            rep.degraded = True
            rep.warnings.append("etcd mirror not written: XTNL_ETCD_ENDPOINTS is not set")
            return
        etcd = self._etcd
        try:
            cs = await read_cluster_state(self._conn)
            text = mirror_json(ClusterState(cs.safe_hold, cs.safe_hold_since, cs.safe_hold_reason,
                                            cs.last_resume_at))
            await asyncio.to_thread(etcd.put, SAFE_HOLD_KEY, text, timeout_s=self._etcd_timeout)
        except Exception as exc:
            rep.degraded = True
            rep.warnings.append(f"etcd mirror not written ({type(exc).__name__}); PostgreSQL is the truth and "
                                "nodes read PostgreSQL first; re-run `safe-hold status` to check")
            return
        rep.fields["etcd_mirror"] = "written"

    # ------------------------------------------------------------------ adapter AUTH_FAILED re-arm
    async def adapter_rearm(self, adapter_id: str, generation: str, actor: Actor) -> Report:
        actor.validate(two_person=False)
        if not adapter_id or not generation:
            raise OpsUsageError("adapter and credential generation are required")
        target = f"{adapter_id}:{generation}"
        rep = Report("adapter.rearm")
        try:
            async with self._fenced() as tx:
                n = await tx.write(SQL_AUTH_REARM, adapter_id, generation, actor.operator)
                if n == 0:
                    row = await tx.fetchrow(S.SQL_AUTH_ROW, adapter_id, generation)
                    state = "missing" if row is None else str(row[0])
                    raise OpsRefused("not_auth_failed", f"adapter_auth_state for {target} is {state}")
                rep.audit_id = await self._audit(tx, rep.command, target, actor, "applied",
                                                 {"adapter_id": adapter_id, "credential_generation": generation},
                                                 tx.epoch)
                rep.fields.update({"rearmed": target, "epoch": tx.epoch})
        except OpsRefused as r:
            return await self._refused(rep, r, target, actor)
        rep.outcome = "applied"
        rep.warnings.append("the leader retries the adapter when it next reads adapter_auth_state for this "
                            "generation; a new credential generation re-arms by itself")
        return rep

    # ------------------------------------------------------------------ reset_task
    async def task_reset(self, name: str, actor: Actor) -> Report:
        actor.validate(two_person=False)
        if not name or len(name) > TASK_NAME_MAX or not name.isprintable() or " " in name:
            raise OpsUsageError(f"task name must be 1-{TASK_NAME_MAX} printable characters without spaces")
        rep = Report("task.reset")
        try:
            async with self._fenced() as tx:
                pending = await tx.fetchrow(S.SQL_REQUEST_PENDING, "reset_task", name, tx.epoch)
                if pending is not None:
                    raise OpsRefused("already_pending", f"request {pending[0]} for {name} is not consumed yet")
                audit_id = await self._audit(tx, rep.command, name, actor, "applied",
                                             {"kind": "reset_task", "task": name}, tx.epoch)
                row = await tx.write_returning(S.SQL_REQUEST_INSERT, "reset_task", name, audit_id, must=True)
                assert row is not None
                rep.audit_id = audit_id
                rep.fields.update({"request_id": int(row[0]), "task": name, "epoch": tx.epoch})
        except OpsRefused as r:
            return await self._refused(rep, r, name, actor)
        rep.outcome = "applied"
        rep.warnings.append("the leader of this epoch closes the circuit when it consumes the request "
                            "(operator_requests); a new leader starts with closed circuits")
        return rep

    # ------------------------------------------------------------------ DLQ
    async def dlq_list(self, *, status: str | None, limit: int, include_released: bool) -> Report:
        if status is not None and status not in DLQ_STATUSES:
            raise OpsUsageError(f"--status must be one of {', '.join(sorted(DLQ_STATUSES))}")
        rep = Report("dlq.list")
        for r in await self._conn.fetch(S.SQL_DLQ_COUNTS):
            rep.fields[f"count_{str(r[0]).lower()}"] = int(r[1])
        rep.sections["dlq"] = [{
            "dlq_id": r[0], "event_type": r[1], "entity_id": r[2], "adapter_id": r[3], "error_reason": r[4],
            "retry_count": r[5], "status": r[6], "next_retry_at": iso_ms(r[7]), "parked_at": iso_ms(r[8]),
        } for r in await self._conn.fetch(S.SQL_DLQ_LIST, status, limit)]
        rep.sections["quarantine"] = [{
            "quarantine_id": r[0], "source": r[1], "dlq_id": r[2], "feed_entry_id": r[3], "event_type": r[4],
            "entity_id": r[5], "adapter_id": r[6], "account_id": r[7], "error_reason": r[8], "retry_count": r[9],
            "quarantined_at": iso_ms(r[10]), "released_at": iso_ms(r[11]), "released_by": r[12],
        } for r in await self._conn.fetch(S.SQL_QUARANTINE_LIST, include_released, limit)]
        return rep

    async def dlq_requeue(self, dlq_id: int, actor: Actor) -> Report:
        actor.validate(two_person=False)
        rep = Report("dlq.requeue")
        target = str(dlq_id)
        try:
            async with self._fenced() as tx:
                before = await tx.fetchrow(S.SQL_DLQ_GET, dlq_id)
                if before is None:
                    raise OpsRefused("not_found", f"events_dlq has no row {dlq_id}")
                status = str(before[6])
                row = await tx.write_returning(S.SQL_DLQ_REQUEUE, dlq_id)
                if row is None:
                    hint = {"LEASED": "the DLQ worker holds it; wait for the lease to end",
                            "QUARANTINED": "use `dlq release-quarantine <quarantine_id>` after the fix",
                            "RESOLVED": "already resolved; nothing to retry"}.get(status, "")
                    raise OpsRefused(f"status_{status.lower()}", hint)
                detail = {"previous_status": status, "previous_next_retry_at_ms": before[7],
                          "retry_count": int(row[0]), "event_type": before[1], "adapter_id": before[3]}
                rep.audit_id = await self._audit(tx, rep.command, target, actor, "applied", detail, tx.epoch)
                rep.fields.update({"dlq_id": dlq_id, "previous_status": status, "status": "QUEUED",
                                   "retry_count": int(row[0]), "epoch": tx.epoch})
        except OpsRefused as r:
            return await self._refused(rep, r, target, actor)
        rep.outcome = "applied"
        return rep

    async def dlq_release_quarantine(self, quarantine_id: int, actor: Actor) -> Report:
        actor.validate(two_person=False)
        rep = Report("dlq.release_quarantine")
        target = str(quarantine_id)
        try:
            async with self._fenced() as tx:
                q = await tx.fetchrow(S.SQL_QUARANTINE_GET, quarantine_id)
                if q is None:
                    raise OpsRefused("not_found", f"events_quarantine has no row {quarantine_id}")
                if str(q[1]) != "dlq" or q[2] is None:
                    raise OpsRefused("feed_entry", "a feed entry has no DLQ row to re-queue; the RiskSweep "
                                                   "evaluates the account every second regardless")
                if q[11] is not None:
                    raise OpsRefused("already_released", f"released by {q[12]} at {iso_ms(q[11])}")
                dlq_id = int(q[2])
                if await tx.write_returning(S.SQL_DLQ_UNQUARANTINE, dlq_id) is None:
                    cur = await tx.fetchrow(S.SQL_DLQ_GET, dlq_id)
                    raise OpsRefused("dlq_not_quarantined",
                                     f"events_dlq {dlq_id} is {'missing' if cur is None else cur[6]}")
                audit_id = await self._audit(tx, rep.command, target, actor, "applied",
                                             {"dlq_id": dlq_id, "event_type": q[4], "adapter_id": q[6],
                                              "error_reason": q[8]}, tx.epoch)
                await tx.write(S.SQL_QUARANTINE_RELEASE, quarantine_id, actor.operator, audit_id, must=True)
                rep.audit_id = audit_id
                rep.fields.update({"quarantine_id": quarantine_id, "dlq_id": dlq_id, "status": "QUEUED",
                                   "retry_count": 0, "epoch": tx.epoch})
        except OpsRefused as r:
            return await self._refused(rep, r, target, actor)
        rep.outcome = "applied"
        rep.warnings.append("the DLQ worker re-reads the broker row before applying anything "
                            "(check-before-retry); the parked payload is never replayed as is")
        return rep

    # ------------------------------------------------------------------ epoch
    async def _etcd_header(self) -> tuple[str, int]:
        if self._etcd is None:
            raise OpsRefused("etcd_unconfigured", "XTNL_ETCD_ENDPOINTS is not set")
        etcd = self._etcd
        try:
            return await asyncio.to_thread(etcd.header, timeout_s=self._etcd_timeout)
        except Exception as exc:
            raise OpsRefused("etcd_unreachable", type(exc).__name__) from exc

    async def epoch_show(self) -> Report:
        rep = Report("epoch.show")
        row = await self._conn.fetchrow(S.SQL_EPOCH_ROW)
        if row is None:
            rep.fields["engine_epoch"] = "absent (no leader has ever bumped it)"
        else:
            rep.fields.update({"cluster_id": row[0], "revision": row[1], "epoch": row[2], "node": row[3],
                               "bumped_at": iso_ms(row[4])})
        if self._etcd is not None:
            try:
                cid, rev = await self._etcd_header()
                rep.fields.update({"etcd_cluster_id": cid, "etcd_revision": rev})
                if row is not None and cid and cid != str(row[0]):
                    rep.warnings.append("etcd reports a different cluster_id: LEADER_INIT is FENCED until the "
                                        "new cluster_id is adopted (epoch adopt, two people, DR runbook)")
            except OpsRefused as r:
                rep.warnings.append(f"etcd not checked: {r}")
        rep.rows = [{"node": r[0], "cluster_id": r[1], "epoch_revision": r[2], "started_at": iso_ms(r[3]),
                     "reached_leader_at": iso_ms(r[4]), "ended_at": iso_ms(r[5]), "end_reason": r[6]}
                    for r in await self._conn.fetch(S.SQL_TERMS_RECENT, 5)]
        return rep

    async def epoch_adopt(self, new_cluster_id: str, from_cluster_id: str, actor: Actor) -> Report:
        actor.validate(two_person=True)
        if not new_cluster_id or not from_cluster_id:
            raise OpsUsageError("the new cluster id and --from-cluster-id are required")
        rep = Report("epoch.adopt")
        target = new_cluster_id
        try:
            etcd_cid, etcd_rev = await self._etcd_header()
            if etcd_cid != new_cluster_id:
                raise OpsRefused("cluster_id_not_reported_by_etcd",
                                 f"etcd at XTNL_ETCD_ENDPOINTS reports cluster_id {etcd_cid or '?'}")
            async with self._plain() as conn:
                await conn.execute(SQL_LOCK_TIMEOUT_2S)
                row = await conn.fetchrow(SQL_READ_EPOCH_FOR_UPDATE)
                if row is None:
                    raise OpsRefused("no_engine_epoch", "nothing to adopt: the first LEADER_INIT creates the row")
                cur_cid, cur_rev = str(row[0]), int(row[1])
                if cur_cid == new_cluster_id:
                    raise OpsRefused("already_adopted", f"engine_epoch.cluster_id is already {new_cluster_id}")
                if cur_cid != from_cluster_id:
                    raise OpsRefused("cluster_id_changed",
                                     f"engine_epoch.cluster_id is {cur_cid}, not the reviewed {from_cluster_id}")
                n = rowcount(await conn.execute(S.SQL_EPOCH_ADOPT, new_cluster_id, f"operator:{actor.operator}",
                                                cur_cid, cur_rev))
                if n != 1:
                    raise OpsRefused("epoch_moved", "engine_epoch changed during the adoption; re-run")
                detail = {"from_cluster_id": cur_cid, "from_revision": cur_rev, "to_cluster_id": new_cluster_id,
                          "to_revision": 0, "etcd_revision": etcd_rev}
                rep.audit_id = await self._audit(conn, rep.command, target, actor, "applied", detail, 0)
                rep.fields.update({"adopted_cluster_id": new_cluster_id, "from_cluster_id": cur_cid,
                                   "from_revision": cur_rev, "revision": 0})
        except OpsRefused as r:
            return await self._refused(rep, r, target, actor, {"from_cluster_id": from_cluster_id})
        rep.outcome = "applied"
        rep.warnings.append("every node of the old cluster is now fenced (its token no longer matches); the "
                            "new cluster's next LEADER_INIT bumps the epoch")
        return rep

    # ------------------------------------------------------------------ halts
    async def halt_status(self, account_id: str | None) -> Report:
        rep = Report("halt.status")
        row = await self._conn.fetchrow(SQL_LATEST_EVENT)
        if row is None:
            rep.fields.update({"effective_level": "NONE", "hard_latch": False, "halt_event_id": None})
        else:
            ev = halt_event_from_row(row)
            rep.fields.update({"effective_level": ev.effective_level.name, "hard_latch": ev.hard_latch,
                               "halt_event_id": ev.id, "by": ev.by, "reason": ev.reason, "at": iso_ms(ev.at_ms)})
        states = [breach_from_row(r) for r in await self._conn.fetch(SQL_UNRESOLVED_BREACHES)]
        ids = {(str(r[1]), str(r[2]), str(r[3])): int(r[0]) for r in await self._conn.fetch(S.SQL_BREACH_IDS)}
        rep.rows = [{"breach_id": ids.get((s.rule_id, subject_to_db(s.subject_type), s.subject_id)),
                     "rule_id": s.rule_id, "subject": f"{s.subject_type}/{s.subject_id}", "account_id": s.account_id,
                     "level": s.level.name, "cap": s.cap.value, "unknown": s.unknown_since is not None,
                     "reason": s.reason, "active_since": iso_ms(s.active_since)}
                    for s in states if s.active and (account_id is None or s.account_id == account_id)]
        rep.fields["reviewed_event_id_for_clear"] = rep.fields["halt_event_id"]
        return rep

    async def halt_clear(self, account_id: str, reviewed_event_id: int | None, *, force: bool,
                         actor: Actor) -> Report:
        actor.validate(two_person=force)
        if not account_id:
            raise OpsUsageError("account is required")
        rep = Report("halt.clear")
        now = self._clock_ms()
        target = account_id
        base_detail: dict[str, Any] = {"reviewed_event_id": reviewed_event_id, "force": force}
        try:
            known = await self._conn.fetchrow(S.SQL_ACCOUNT_KNOWN, account_id)
            if known is None or not bool(known[0]):
                raise OpsRefused("unknown_account", f"no account_state or halt_breaches row for {account_id}")
            token = await stored_epoch(self._conn)
            if token is None:
                raise OpsRefused("no_engine_epoch", "engine_epoch has no row: there is no fence to write under")
            audit: dict[str, int] = {}

            async def after(tx: FencedTx, htx: OpsHaltTxn) -> None:
                events = resolved_transitions(htx.resolved_by_operator(now), now_ms=now, reason=actor.reason.strip())
                if events:
                    await htx.insert_events(events)
                detail = clear_detail(htx, now_ms=now, reviewed_event_id=reviewed_event_id, force=force)
                audit["id"] = await self._audit(tx, rep.command, target, actor, "applied", detail, tx.epoch)

            store = OpsHaltStore(self._conn, token, account_id=account_id, force=force, after=after)
            try:
                outcome = await HaltSwitch(store).operator_clear(actor.operator, actor.reason.strip(),
                                                                 reviewed_event_id, force=force, now_ms=now)
            except EpochMismatch as exc:
                raise OpsRefused("epoch_moved", f"engine_epoch changed from {token} during the clear") from exc
            if outcome.status == "refused":
                raise OpsRefused(outcome.reason.split(":", 1)[0],
                                 outcome.reason.split(":", 1)[1] if ":" in outcome.reason else "")
        except OpsRefused as r:
            return await self._refused(rep, r, target, actor, base_detail)
        htx = store.last
        ev = outcome.event
        rep.fields.update({"cleared": True, "epoch": token, "halt_event_id": None if ev is None else ev.id,
                           "effective_level": None if ev is None else ev.effective_level.name,
                           "resolved": ["/".join(k) for k in outcome.resolved]})
        if "id" in audit:
            rep.outcome = "applied"
            rep.audit_id = audit["id"]
        else:
            rep.outcome = "noop"
            rep.audit_id = await self.audit_standalone(rep.command, target, actor, "noop",
                                                       {**base_detail, "states_reviewed": 0
                                                        if htx is None else len(htx.saved)})
        rep.warnings.append("no order action is enabled by a clear: Phase 1 sends nothing, ActionGate keeps every "
                            "other condition, and the RiskSweep keeps evaluating; the leader's integrity tick "
                            "re-applies Redis halt:state")
        if any(s for s in (htx.saved if htx else []) if s.level >= Level.SOFT_HALT and s.resolved_at is None):
            rep.warnings.append("SOFT-or-higher states remain active: the level stays at the sweep's target")
        return rep

    # ------------------------------------------------------------------ halt acknowledge (oversee-rules.md s4)
    async def halt_ack(self, breach_id: int, actor: Actor) -> Report:
        """acknowledge(operator, why, breach_id): one breach_acks row, written by HaltSwitch.acknowledge under
        HALT_LOCK in the same fenced transaction as the audit row. An identical earlier ack (same level, cap and
        reason, made since the state activated) is a no-op. Never changes a state or the halt level."""
        actor.validate(two_person=False)
        if breach_id <= 0:
            raise OpsUsageError("breach_id must be a positive halt_breaches id (see `halt status`)")
        rep = Report("halt.ack")
        target = str(breach_id)
        now = self._clock_ms()
        why = actor.reason.strip()
        try:
            async with self._fenced() as tx:
                await tx.fetchrow(SQL_HALT_LOCK, HALT_LOCK)
                row = await tx.fetchrow(S.SQL_BREACH_BY_ID, breach_id)
                if row is None:
                    raise OpsRefused("not_found", f"halt_breaches has no row {breach_id}")
                if row[4] is not None:
                    raise OpsRefused("resolved", f"rule state {breach_id} was resolved at {iso_ms(row[4])}")
                key = (str(row[0]), subject_from_db(str(row[1])), str(row[2]))
                htx = PgHaltTxn(tx)
                state = next((s for s in await htx.breach_states() if s.key == key), None)
                if state is None or not state.active:
                    raise OpsRefused("not_active", "the rule state is not active (pending or released): nothing "
                                                   "re-pages, so there is nothing to acknowledge")
                detail: dict[str, Any] = {
                    "breach_id": breach_id, "rule_id": state.rule_id,
                    "subject": f"{subject_to_db(state.subject_type)}/{state.subject_id}",
                    "account_id": state.account_id, "level": state.level.name, "cap": state.cap.value,
                    "reason": state.reason}
                acks = [ack_from_row(r) for r in await tx.fetch(S.SQL_BREACH_ACKS, breach_id)]
                rep.fields.update({"breach_id": breach_id, "rule_id": state.rule_id, "subject": detail["subject"],
                                   "account_id": state.account_id, "level": state.level.name,
                                   "cap": state.cap.value, "reason": state.reason, "epoch": tx.epoch})
                if is_acknowledged(state, acks):
                    rep.outcome = "noop"
                    rep.audit_id = await self._audit(tx, rep.command, target, actor, "noop",
                                                     {**detail, "already_acknowledged": True}, tx.epoch)
                else:
                    ack = await HaltSwitch(InTxHaltStore(htx)).acknowledge(actor.operator, why, key, now)
                    detail["ack_id"] = ack.ack_id
                    rep.outcome = "applied"
                    rep.audit_id = await self._audit(tx, rep.command, target, actor, "applied", detail, tx.epoch)
                    rep.fields["ack_id"] = ack.ack_id
        except LookupError as exc:   # the state vanished between the read and the insert
            return await self._refused(rep, OpsRefused("not_active", str(exc)), target, actor)
        except OpsRefused as r:
            return await self._refused(rep, r, target, actor)
        rep.warnings.append("re-pages of this state stop until its level, cap or reason changes; the RiskSweep "
                            "keeps evaluating it and the halt level is unchanged")
        return rep

    # ------------------------------------------------------------------ sweep quarantine (oversee-rules.md s2, s4)
    @staticmethod
    def _account(account_id: str) -> str:
        if not account_id or len(account_id) > ACCOUNT_MAX or not account_id.isprintable() or " " in account_id:
            raise OpsUsageError(f"account must be 1-{ACCOUNT_MAX} printable characters without spaces")
        return account_id

    async def account_status(self, account_id: str | None, *, include_released: bool, limit: int) -> Report:
        rep = Report("account.status")
        rows = await self._conn.fetch(S.SQL_SWEEP_QUARANTINE_LIST, include_released, limit)
        rep.rows = [{"account_id": r[1], "adapter_id": r[0], "failing_since": iso_ms(r[2]),
                     "quarantined_at": iso_ms(r[3]), "reason": r[4], "released_at": iso_ms(r[5]),
                     "released_by": r[6]}
                    for r in rows if account_id is None or str(r[1]) == account_id]
        rep.fields["quarantined"] = sum(1 for r in rep.rows if r["released_at"] is None)
        return rep

    async def account_release(self, account_id: str, actor: Actor) -> Report:
        """release_account(operator, account): releases the stored sweep_quarantine row and queues a
        `release_account` request so the leader's RiskSweep drops its in-memory quarantine (5.6 consumer), all in
        one fenced transaction with the audit row."""
        actor.validate(two_person=False)
        self._account(account_id)
        rep = Report("account.release")
        target = account_id
        try:
            async with self._fenced() as tx:
                q = await tx.fetchrow(S.SQL_SWEEP_QUARANTINE_GET, account_id)
                if q is None:
                    raise OpsRefused("not_quarantined", f"sweep_quarantine has no row for {account_id}")
                if q[5] is not None:
                    raise OpsRefused("already_released", f"released by {q[6]} at {iso_ms(q[5])}")
                pending = await tx.fetchrow(S.SQL_REQUEST_PENDING, "release_account", account_id, tx.epoch)
                if pending is not None:
                    raise OpsRefused("already_pending", f"request {pending[0]} for {account_id} is not consumed yet")
                released = await tx.write_returning(S.SQL_SWEEP_QUARANTINE_RELEASE, account_id, actor.operator)
                if released is None:
                    raise OpsRefused("already_released", "a good pass released it during the command")
                adapter = None if q[0] is None else str(q[0])
                detail = {"kind": "release_account", "adapter_id": adapter, "quarantined_at_ms": released[0],
                          "quarantine_reason": released[1], "failing_since_ms": q[2]}
                audit_id = await self._audit(tx, rep.command, target, actor, "applied", detail, tx.epoch)
                req = await tx.write_returning(S.SQL_REQUEST_INSERT_PAYLOAD, "release_account", account_id, audit_id,
                                               AccountRelease(account_id, adapter).to_json(), must=True)
                assert req is not None
                rep.audit_id = audit_id
                rep.fields.update({"account_id": account_id, "adapter_id": adapter, "released": True,
                                   "request_id": int(req[0]), "was_quarantined_at": iso_ms(released[0]),
                                   "was_reason": released[1], "epoch": tx.epoch})
        except OpsRefused as r:
            return await self._refused(rep, r, target, actor)
        rep.outcome = "applied"
        rep.warnings.append("the leader of this epoch evaluates the account at once when it consumes the request; "
                            "if the cause persists the account fails again and is re-quarantined after 5 min")
        return rep

    # ------------------------------------------------------------------ cash flows (oversee-rules.md s4)
    async def cash_flows_refresh(self, account_id: str, since_ms: int | None, actor: Actor) -> Report:
        """refresh_cash_flows(operator, account): queues a `refresh_cash_flows` request (payload: account,
        adapter, since_ms or null = since the roll) for the leader's orchestrator, with the audit row."""
        actor.validate(two_person=False)
        self._account(account_id)
        now = self._clock_ms()
        if since_ms is not None and not 0 <= since_ms <= now:
            raise OpsUsageError("--since-ms must be an epoch-milliseconds time in the past")
        rep = Report("cash_flows.refresh")
        target = account_id
        try:
            async with self._fenced() as tx:
                a = await tx.fetchrow(S.SQL_ACCOUNT_ADAPTER, account_id)
                if a is None:
                    raise OpsRefused("unknown_account", f"no account_state row for {account_id}")
                adapter = str(a[0])
                pending = await tx.fetchrow(S.SQL_REQUEST_PENDING, "refresh_cash_flows", account_id, tx.epoch)
                if pending is not None:
                    raise OpsRefused("already_pending", f"request {pending[0]} for {account_id} is not consumed yet")
                payload = CashFlowRefresh(account_id, adapter, since_ms)
                audit_id = await self._audit(tx, rep.command, target, actor, "applied",
                                             {"kind": "refresh_cash_flows", "adapter_id": adapter,
                                              "since_ms": since_ms}, tx.epoch)
                req = await tx.write_returning(S.SQL_REQUEST_INSERT_PAYLOAD, "refresh_cash_flows", account_id,
                                               audit_id, payload.to_json(), must=True)
                assert req is not None
                rep.audit_id = audit_id
                rep.fields.update({"account_id": account_id, "adapter_id": adapter, "request_id": int(req[0]),
                                   "since": "roll" if since_ms is None else iso_ms(since_ms), "epoch": tx.epoch})
        except OpsRefused as r:
            return await self._refused(rep, r, target, actor)
        rep.outcome = "applied"
        rep.warnings.append("read-only: the leader's orchestrator calls fetch_cash_flows for this account (at most "
                            "once a minute per account); new cash flows are booked through the normal commit path")
        return rep

    # ------------------------------------------------------------------ partition archive (operations.md s11)
    async def archive_list(self, *, limit: int) -> Report:
        rep = Report("archive.list")
        rep.rows = [{"partition": r[0], "parent": r[1], "row_count": r[2], "checksum": r[3], "archive_uri": r[4],
                     "verified_at": iso_ms(r[5]), "verified_by": r[6], "detached_at": iso_ms(r[7]),
                     "detached_epoch": r[8]}
                    for r in await self._conn.fetch(S.SQL_ARCHIVE_LIST, limit)]
        return rep

    async def _partition_rows(self, partition: str) -> int | None:
        """Best effort, outside any transaction: the partition's current row count, None if unknown."""
        try:
            row = await self._conn.fetchrow(S.SQL_PARTITION_ROWS, partition)
        except Exception as exc:
            log.warning("row count of %s unavailable: %s", partition, type(exc).__name__)
            return None
        return None if row is None or row[0] is None else int(row[0])

    async def archive_forget(self, partition: str, actor: Actor) -> Report:
        """Remove the `partition_archive` row of a partition that was never detached (count mismatch, lost
        file), so the next lifecycle run re-exports it (two people, partition-archive.md). Fenced; refused for a
        detached partition."""
        actor.validate(two_person=True)
        if not PARTITION_RE.match(partition or ""):
            raise OpsUsageError("partition must look like <parent>_pYYYYMM (see `archive list`)")
        rep = Report("archive.forget")
        target = partition
        current = await self._partition_rows(partition)
        try:
            async with self._fenced() as tx:
                row = await tx.fetchrow(S.SQL_ARCHIVE_GET, partition)
                if row is None:
                    raise OpsRefused("not_found", f"partition_archive has no row {partition}")
                if row[7] is not None:
                    raise OpsRefused("detached", f"detached at {iso_ms(row[7])} (epoch {row[8]}); re-attach it "
                                                 "first (runbook rollback) or keep the archive")
                gone = await tx.write_returning(S.SQL_ARCHIVE_FORGET, partition)
                if gone is None:
                    raise OpsRefused("detached", "the lifecycle task detached it during the command")
                detail = {"parent_table": gone[0], "row_count": gone[1], "checksum": gone[2], "archive_uri": gone[3],
                          "verified_at_ms": row[5], "verified_by": row[6], "current_rows": current}
                rep.audit_id = await self._audit(tx, rep.command, target, actor, "applied", detail, tx.epoch)
                rep.fields.update({"partition": partition, "parent": gone[0], "forgotten": True,
                                   "archived_row_count": gone[1], "current_rows": current,
                                   "archive_uri": gone[3], "epoch": tx.epoch})
        except OpsRefused as r:
            return await self._refused(rep, r, target, actor, {"current_rows": current})
        rep.outcome = "applied"
        if current is not None and current == rep.fields["archived_row_count"]:
            rep.warnings.append("the partition still holds the archived row count: the next run re-exports the same "
                                "rows (only useful when the archive file was lost)")
        rep.warnings.append("move the old archive file aside (`mv <file>{,.stale}`) for the ticket; the next lifecycle "
                            "run (every 6 h, leader) re-exports, verifies and records the partition, then detaches it")
        return rep

    # ------------------------------------------------------------------ rulesets (oversee-rules.md s7)
    @staticmethod
    def _ruleset_row(r: Any) -> dict[str, Any]:
        version = int(r[0])
        cfg, problems = check_row(r[1], version)
        return {"version": version, "active": bool(r[2]), "valid": cfg is not None,
                "problems": "; ".join(problems)[:300] or None, "created_by": r[3], "reason": r[4],
                "created_at": iso_ms(r[5])}

    async def ruleset_show(self, version: int | None, *, limit: int) -> Report:
        rep = Report("ruleset.show")
        if version is None:
            active = await self._conn.fetchrow(S.SQL_RULESET_ACTIVE)
            rep.fields["active_version"] = None if active is None else int(active[0])
            rep.rows = [self._ruleset_row(r) for r in await self._conn.fetch(S.SQL_RULESET_LIST, limit)]
            if active is None:
                rep.warnings.append("no active rulesets row: the engine runs the spec (base) ruleset")
            elif not self._ruleset_row(active)["valid"]:
                rep.warnings.append(f"the active row v{int(active[0])} is invalid: the engine keeps the last good "
                                    "ruleset and pages; activate a valid version")
            return rep
        row = await self._conn.fetchrow(S.SQL_RULESET_GET, version)
        if row is None:
            raise OpsUsageError(f"rulesets has no version {version}")
        info = self._ruleset_row(row)
        rep.fields.update(info)
        cfg, _ = check_row(row[1], version)
        if cfg is not None:
            rep.fields.update(summary(cfg))
            rep.fields["effective"] = effective_json(cfg)
        return rep

    async def ruleset_add(self, text: str, source: str, actor: Actor) -> Report:
        """Store a validated document as the next version, inactive (activation is a separate two-person step)."""
        actor.validate(two_person=False)
        rep = Report("ruleset.add")
        target: str | None = None
        try:
            async with self._plain() as conn:
                await conn.execute(SQL_LOCK_TIMEOUT_2S)
                await conn.execute(S.SQL_RULESET_LOCK)
                nxt = await conn.fetchrow(S.SQL_RULESET_NEXT_VERSION)
                version = 1 if nxt is None or nxt[0] is None else int(nxt[0])
                target = str(version)
                checked = check_document(text, version)
                await conn.fetchrow(S.SQL_RULESET_INSERT, version, checked.canonical, actor.operator,
                                    actor.reason.strip())
                stored = await conn.fetchrow(S.SQL_RULESET_GET, version)
                cfg, problems = check_row(None if stored is None else stored[1], version)
                if cfg is None or cfg != checked.config:   # what the engine will read is what was checked
                    raise OpsRefused("roundtrip_mismatch", "; ".join(problems) or "stored row parses differently")
                active = await conn.fetchrow(S.SQL_RULESET_ACTIVE)
                active_version = None if active is None else int(active[0])
                active_cfg = None if active is None or active_version is None else check_row(active[1],
                                                                                             active_version)[0]
                detail = {"version": version, "source": source, "sections": list(checked.sections),
                          "active_version": active_version}
                epoch = await stored_epoch(conn)
                rep.audit_id = await self._audit(conn, rep.command, target, actor, "applied", detail, epoch)
                rep.fields.update({"version": version, "active": False, "sections": list(checked.sections),
                                   "active_version": detail["active_version"]})
                rep.fields.update(summary(cfg))
                rep.warnings.extend(checked.warnings)
                if active_cfg is not None and same_ruleset(active_cfg, cfg):
                    rep.warnings.append(f"identical thresholds to the active v{active_version}")
        except OpsRefused as r:
            return await self._refused(rep, r, target, actor, {"source": source})
        rep.outcome = "applied"
        rep.warnings.append(f"stored inactive; activate with `ruleset activate {target}` (two people)")
        return rep

    async def ruleset_activate(self, version: int, actor: Actor) -> Report:
        """Make `version` the single active row (two-person). Refused unless the engine's parser accepts it."""
        actor.validate(two_person=True)
        rep = Report("ruleset.activate")
        target = str(version)
        try:
            async with self._plain() as conn:
                await conn.execute(SQL_LOCK_TIMEOUT_2S)
                await conn.execute(S.SQL_RULESET_LOCK)
                row = await conn.fetchrow(S.SQL_RULESET_GET, version)
                if row is None:
                    raise OpsRefused("not_found", f"rulesets has no version {version}")
                if bool(row[2]):
                    raise OpsRefused("already_active", f"v{version} is already the active ruleset")
                cfg, problems = check_row(row[1], version)
                if cfg is None:
                    raise OpsRefused("invalid_ruleset", "; ".join(problems))
                previous = [int(r[0]) for r in await conn.fetch(S.SQL_RULESET_DEACTIVATE, version)]
                if rowcount(await conn.execute(S.SQL_RULESET_ACTIVATE, version)) != 1:
                    raise OpsRefused("activation_failed", f"v{version} changed during the activation; re-run")
                detail = {"version": version, "previous_active": previous}
                epoch = await stored_epoch(conn)
                rep.audit_id = await self._audit(conn, rep.command, target, actor, "applied", detail, epoch)
                rep.fields.update({"active_version": version, "previous_active": previous})
                rep.fields.update(summary(cfg))
        except OpsRefused as r:
            return await self._refused(rep, r, target, actor)
        rep.outcome = "applied"
        rep.warnings.append("the leader installs it at its next ruleset poll, at a sweep boundary, records it in "
                            "ruleset_audit and sweeps every account; to roll back, activate the previous version")
        return rep

    # ------------------------------------------------------------------ uptime / downtime catch-up (0012)
    async def uptime_show(self, *, limit: int, node: str | None = None) -> Report:
        """Read-only: the engine runs (start, last heartbeat, stop, reason, suspended gaps; a run with no stop that
        is not the newest of its node ended without a clean stop) and each adapter's coverage. No amounts."""
        rep = Report("uptime.show")
        rows = await self._conn.fetch(S.SQL_UPTIME_RUNS, node, limit)
        newest: set[str] = set()
        for r in rows:
            node_id = str(r[1])
            stopped = r[4]
            if stopped is not None:
                state = "stopped"
            elif node_id in newest:
                state = "no clean stop"
            else:
                state = "running or no clean stop"
            newest.add(node_id)
            end = stopped if stopped is not None else r[3]
            rep.rows.append({"run_id": r[0], "node": node_id, "started": iso_ms(r[2]), "last_seen": iso_ms(r[3]),
                             "stopped": iso_ms(stopped), "reason": r[5], "suspended_gaps": r[6], "state": state,
                             "up": _span(int(r[2]), int(end))})
        # offline gaps between consecutive runs of the same node (newest first)
        gaps: list[dict[str, Any]] = []
        for prev, cur in zip(rows[1:], rows[:-1], strict=False):
            if str(prev[1]) != str(cur[1]):
                continue
            last = int(prev[4] if prev[4] is not None else prev[3])
            gaps.append({"node": str(cur[1]), "from": iso_ms(last), "to": iso_ms(cur[2]),
                         "offline": _span(last, int(cur[2]))})
        rep.sections["offline gaps"] = gaps
        rep.sections["adapter coverage"] = [
            {"adapter": r[0], "last_streaming": iso_ms(r[1]), "run_id": r[2]}
            for r in await self._conn.fetch(S.SQL_UPTIME_COVERAGE)]
        return rep

    async def catchup_show(self, *, limit: int, adapter: str | None = None) -> Report:
        """Read-only: the downtime catch-ups (window, counts, complete). Counts and ids only."""
        rep = Report("catchup.show")
        for r in await self._conn.fetch(S.SQL_CATCHUP_RUNS, adapter, limit):
            rep.rows.append({
                "id": r[0], "adapter": r[3], "reason": r[4], "window": f"{iso_ms(r[5])}..{iso_ms(r[6])}",
                "covered_from": iso_ms(r[7]), "offline_from": iso_ms(r[8]), "finished": iso_ms(r[10]),
                "orders_seen": r[11], "orders_new": r[12], "orders_updated": r[13], "positions_seen": r[14],
                "positions_new": r[15], "positions_closed": r[16], "cash_flows_new": r[17], "anomalies": r[18],
                "complete": bool(r[19]), "epoch": r[2], "run_id": r[1], "notes": r[20] or None})
        if any(not row["complete"] for row in rep.rows):
            rep.warnings.append("an incomplete catch-up means orders may be missing for its window: check the "
                                "broker's order history for it (it was paged as 'downtime catch-up incomplete')")
        return rep

    # ------------------------------------------------------------------ audit
    async def audit_tail(self, *, limit: int, command: str | None) -> Report:
        rep = Report("audit.tail")
        rows = await self._conn.fetch(S.SQL_AUDIT_TAIL, command, limit)
        rep.rows = [{"audit_id": r[0], "at": iso_ms(r[1]), "command": r[2], "target": r[3], "operator": r[4],
                     "approver": r[5], "outcome": r[6], "reason": r[7], "epoch": r[8], "db_user": r[9],
                     "detail": _dumps(_loads(r[10]) or {})}
                    for r in reversed(rows)]
        return rep
