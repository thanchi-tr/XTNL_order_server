"""DLQ requeue / quarantine release, adapter AUTH_FAILED re-arm, reset_task requests, audit tail
(tier-persistence.md s5, operations.md s3, s13, s16)."""
from __future__ import annotations

import json

import pytest

from src.db.auth_state import SQL_AUTH_REARM
from src.db.epoch import SQL_COMMIT
from src.ops import sql as S
from src.ops.model import Actor, OpsUsageError
from src.ops.service import OpsService

from tests.ops.conftest import outcomes
from tests.ops.emulator import NOW, OpsPg

OP = Actor("carol", "BUG-77 mapping fix shipped in 1.4.2", os_user="carol")


# ---- adapter re-arm ---------------------------------------------------------------------------------
async def test_rearm_flips_auth_failed_and_audits_in_one_transaction(svc: OpsService, pg: OpsPg) -> None:
    pg.o.auth[("stonex", "v-12")] = {"state": "AUTH_FAILED"}
    rep = await svc.adapter_rearm("stonex", "v-12", OP)
    assert rep.outcome == "applied" and pg.o.auth[("stonex", "v-12")]["state"] == "REARMED"
    assert pg.o.auth[("stonex", "v-12")]["rearmed_by"] == "carol"
    assert pg.sql.index(SQL_AUTH_REARM) < pg.sql.index(S.SQL_AUDIT_INSERT) < pg.sql.index(SQL_COMMIT)
    assert pg.o.audit[0]["target"] == "stonex:v-12" and pg.o.audit[0]["epoch"] == 7


async def test_rearm_refused_unless_auth_failed(svc: OpsService, pg: OpsPg) -> None:
    pg.o.auth[("stonex", "v-12")] = {"state": "REARMED"}
    rep = await svc.adapter_rearm("stonex", "v-12", OP)
    assert rep.fields["refused"] == "not_auth_failed" and "REARMED" in rep.fields["detail"]
    rep = await svc.adapter_rearm("stonex", "v-13", OP)
    assert "missing" in rep.fields["detail"]
    assert outcomes(pg) == [("adapter.rearm", "refused")] * 2


async def test_rearm_with_no_epoch_row_is_refused(svc: OpsService, pg: OpsPg) -> None:
    pg.o.epoch = None
    rep = await svc.adapter_rearm("stonex", "v-12", OP)
    assert rep.fields["refused"] == "no_engine_epoch"


# ---- reset_task -------------------------------------------------------------------------------------
async def test_task_reset_writes_an_epoch_scoped_request_with_its_audit_row(svc: OpsService, pg: OpsPg) -> None:
    rep = await svc.task_reset("risksweep", OP)
    assert rep.outcome == "applied"
    (req,) = pg.o.requests
    assert (req["kind"], req["target"], req["requested_epoch"], req["audit_id"]) == ("reset_task", "risksweep", 7,
                                                                                    rep.audit_id)
    rep = await svc.task_reset("risksweep", OP)
    assert rep.fields["refused"] == "already_pending" and len(pg.o.requests) == 1
    with pytest.raises(OpsUsageError):
        await svc.task_reset("bad name", OP)


# ---- DLQ --------------------------------------------------------------------------------------------
async def test_requeue_makes_a_waiting_row_due_now_and_keeps_its_retry_count(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_dlq(5, "BACKOFF", retry=3)
    rep = await svc.dlq_requeue(5, OP)
    assert rep.outcome == "applied"
    row = pg.o.dlq[5]
    assert (row["status"], row["retry_count"], row["next_retry_at_ms"]) == ("QUEUED", 3, NOW)
    assert json.loads(pg.o.audit[0]["detail"])["previous_status"] == "BACKOFF"


@pytest.mark.parametrize(("status", "code"), [("LEASED", "status_leased"), ("QUARANTINED", "status_quarantined"),
                                              ("RESOLVED", "status_resolved"), (None, "not_found")])
async def test_requeue_refusals(svc: OpsService, pg: OpsPg, status: str | None, code: str) -> None:
    if status is not None:
        pg.seed_dlq(5, status)
    rep = await svc.dlq_requeue(5, OP)
    assert rep.outcome == "refused" and rep.fields["refused"] == code
    if status is not None:
        assert pg.o.dlq[5]["status"] == status
    assert outcomes(pg) == [("dlq.requeue", "refused")]


async def test_release_quarantine_requeues_with_a_fresh_budget_and_marks_the_row(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_dlq(5, "QUARANTINED", retry=5)
    pg.seed_quarantine(40, dlq_id=5)
    rep = await svc.dlq_release_quarantine(40, OP)
    assert rep.outcome == "applied"
    assert (pg.o.dlq[5]["status"], pg.o.dlq[5]["retry_count"]) == ("QUEUED", 0)
    q = pg.o.quarantine[40]
    assert (q["released_by"], q["release_audit_id"]) == ("carol", rep.audit_id)
    assert pg.sql.index(S.SQL_DLQ_UNQUARANTINE) < pg.sql.index(S.SQL_AUDIT_INSERT) < \
        pg.sql.index(S.SQL_QUARANTINE_RELEASE) < pg.sql.index(SQL_COMMIT)
    rep = await svc.dlq_release_quarantine(40, OP)
    assert rep.fields["refused"] == "already_released"


async def test_release_quarantine_refusals(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_quarantine(41, source="feed")
    assert (await svc.dlq_release_quarantine(41, OP)).fields["refused"] == "feed_entry"
    pg.seed_dlq(6, "RESOLVED")
    pg.seed_quarantine(42, dlq_id=6)
    assert (await svc.dlq_release_quarantine(42, OP)).fields["refused"] == "dlq_not_quarantined"
    assert pg.o.quarantine[42]["released_at_ms"] is None
    assert (await svc.dlq_release_quarantine(99, OP)).fields["refused"] == "not_found"


async def test_release_quarantine_is_atomic_with_its_audit(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_dlq(5, "QUARANTINED", retry=5)
    pg.seed_quarantine(40, dlq_id=5)
    pg.before[S.SQL_QUARANTINE_RELEASE] = lambda: pg.bump(8)
    rep = await svc.dlq_release_quarantine(40, OP)
    assert rep.fields["refused"] == "epoch_moved"
    assert pg.o.dlq[5]["status"] == "QUARANTINED" and pg.o.quarantine[40]["released_at_ms"] is None
    assert outcomes(pg) == [("dlq.release_quarantine", "refused")]


async def test_dlq_list_never_shows_payloads(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_dlq(5, "BACKOFF", payload='{"account":"SECRET-ACCOUNT-123"}')
    pg.seed_quarantine(40, dlq_id=5)
    rep = await svc.dlq_list(status=None, limit=20, include_released=False)
    assert rep.fields["count_backoff"] == 1 and len(rep.sections["dlq"]) == 1 and len(rep.sections["quarantine"]) == 1
    assert "SECRET-ACCOUNT-123" not in repr(rep)
    with pytest.raises(OpsUsageError):
        await svc.dlq_list(status="bogus", limit=20, include_released=False)


async def test_audit_tail_is_oldest_first_and_filters(svc: OpsService, pg: OpsPg) -> None:
    pg.o.auth[("stonex", "v-1")] = {"state": "AUTH_FAILED"}
    await svc.adapter_rearm("stonex", "v-1", OP)
    await svc.task_reset("housekeeping", OP)
    rep = await svc.audit_tail(limit=10, command=None)
    assert [r["command"] for r in rep.rows] == ["adapter.rearm", "task.reset"]
    rep = await svc.audit_tail(limit=10, command="task.reset")
    assert [r["target"] for r in rep.rows] == ["housekeeping"]
