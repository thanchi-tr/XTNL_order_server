"""SAFE_HOLD resume and epoch show / adopt (operations.md s2, s4, s8, s16)."""
from __future__ import annotations

import json

import pytest

from src.db.epoch import SQL_BEGIN_RC, SQL_COMMIT, SQL_ROLLBACK
from src.ops import sql as S
from src.ops.model import Actor, OpsUsageError
from src.ops.service import OpsService
from src.runtime.cluster import parse_mirror
from src.runtime.etcd import SAFE_HOLD_KEY

from tests.ops.conftest import FakeEtcd, outcomes
from tests.ops.emulator import NOW, CheckViolation, OpsPg

A2 = Actor("alice", "INC-42 etcd latency fixed, both nodes healthy", approver="bob", os_user="alice")


def hold(pg: OpsPg) -> None:
    pg.o.cluster.update(safe_hold=True, since=NOW - 300_000, reason="3 abnormal terms in 15 min")


async def test_resume_clears_safe_hold_audits_in_the_same_transaction_and_mirrors(
        svc: OpsService, pg: OpsPg, etcd: FakeEtcd) -> None:
    hold(pg)
    rep = await svc.safe_hold_resume(A2)
    assert rep.outcome == "applied" and not rep.degraded and rep.fields["etcd_mirror"] == "written"
    c = pg.o.cluster
    assert c["safe_hold"] is False and c["resumed_by"] == "alice" and c["last_resume_at"] == NOW
    (audit,) = pg.o.audit
    assert (audit["command"], audit["outcome"], audit["approver"], audit["epoch"]) == (
        "safe_hold.resume", "applied", "bob", 7)
    assert json.loads(audit["detail"])["safe_hold_reason"] == "3 abnormal terms in 15 min"
    # the resume UPDATE and the audit INSERT sit between one BEGIN and one COMMIT
    i_begin = pg.sql.index(SQL_BEGIN_RC)
    i_commit = pg.sql.index(SQL_COMMIT)
    assert i_begin < pg.sql.index(S.SQL_SAFE_HOLD_RESUME) < pg.sql.index(S.SQL_AUDIT_INSERT) < i_commit
    # the mirror has the leader's format (src/runtime/cluster.py) and says clear
    assert etcd.puts and etcd.puts[-1][0] == SAFE_HOLD_KEY
    mirrored = parse_mirror(etcd.puts[-1][1])
    assert mirrored.safe_hold is False and mirrored.last_resume_at == NOW


async def test_resume_needs_two_people(svc: OpsService, pg: OpsPg) -> None:
    hold(pg)
    with pytest.raises(OpsUsageError, match="two-person"):
        await svc.safe_hold_resume(Actor("alice", "INC-42 resume after fix"))
    with pytest.raises(OpsUsageError, match="second person"):
        await svc.safe_hold_resume(Actor("alice", "INC-42 resume after fix", approver="ALICE"))
    with pytest.raises(OpsUsageError, match="reason"):
        await svc.safe_hold_resume(Actor("alice", "short", approver="bob"))
    assert pg.o.cluster["safe_hold"] is True and pg.o.audit == []


async def test_resume_when_not_held_is_refused_and_audited(svc: OpsService, pg: OpsPg, etcd: FakeEtcd) -> None:
    rep = await svc.safe_hold_resume(A2)
    assert rep.outcome == "refused" and rep.fields["refused"] == "not_in_safe_hold"
    assert outcomes(pg) == [("safe_hold.resume", "refused")] and etcd.puts == []


async def test_resume_refused_when_the_epoch_moves_mid_command(svc: OpsService, pg: OpsPg) -> None:
    hold(pg)
    pg.before[S.SQL_SAFE_HOLD_RESUME] = lambda: pg.bump(8)   # a LEADER_INIT bump commits in between
    rep = await svc.safe_hold_resume(A2)
    assert rep.outcome == "refused" and rep.fields["refused"] == "epoch_moved"
    assert pg.o.cluster["safe_hold"] is True and SQL_ROLLBACK in pg.sql
    assert outcomes(pg) == [("safe_hold.resume", "refused")]


async def test_resume_rolls_back_when_the_audit_row_cannot_be_written(svc: OpsService, pg: OpsPg) -> None:
    hold(pg)

    def boom() -> None:
        raise CheckViolation("audit")

    pg.before[S.SQL_AUDIT_INSERT] = boom
    with pytest.raises(CheckViolation):
        await svc.safe_hold_resume(A2)
    assert pg.o.cluster["safe_hold"] is True and pg.o.audit == []   # no change without its audit row


async def test_resume_without_a_mirror_is_degraded(pg: OpsPg, etcd: FakeEtcd) -> None:
    hold(pg)
    etcd.fail_put = True
    rep = await OpsService(pg, etcd, clock_ms=lambda: NOW).safe_hold_resume(A2)
    assert rep.outcome == "applied" and rep.degraded and pg.o.cluster["safe_hold"] is False
    hold(pg)
    rep = await OpsService(pg, None, clock_ms=lambda: NOW).safe_hold_resume(A2)
    assert rep.outcome == "applied" and rep.degraded


async def test_safe_hold_status_compares_the_mirror(svc: OpsService, pg: OpsPg, etcd: FakeEtcd) -> None:
    hold(pg)
    rep = await svc.safe_hold_status()
    assert rep.fields["safe_hold"] is True and rep.fields["etcd_mirror"] == "absent" and rep.warnings
    etcd.kv[SAFE_HOLD_KEY] = json.dumps({"safe_hold": False})
    rep = await svc.safe_hold_status()
    assert rep.fields["etcd_mirror"] == "clear" and any("disagrees" in w for w in rep.warnings)
    assert pg.o.audit == []   # reads are never audited


# ---- epoch ------------------------------------------------------------------------------------------
async def test_epoch_show_flags_a_new_etcd_cluster(svc: OpsService, pg: OpsPg, etcd: FakeEtcd) -> None:
    etcd.cluster_id = "cluster-dr"
    rep = await svc.epoch_show()
    assert rep.fields["cluster_id"] == "cluster-a" and rep.fields["etcd_cluster_id"] == "cluster-dr"
    assert any("adopted" in w for w in rep.warnings)


async def test_epoch_adopt_is_a_reviewed_cas_that_fences_the_old_cluster(svc: OpsService, pg: OpsPg,
                                                                         etcd: FakeEtcd) -> None:
    etcd.cluster_id = "cluster-dr"
    rep = await svc.epoch_adopt("cluster-dr", "cluster-a", A2)
    assert rep.outcome == "applied"
    assert pg.o.epoch is not None and pg.o.epoch[:3] == ["cluster-dr", 0, "operator:alice"]
    (audit,) = pg.o.audit
    assert audit["command"] == "epoch.adopt" and audit["approver"] == "bob"
    assert json.loads(audit["detail"])["from_revision"] == 7
    # the CAS, the audit and COMMIT are one transaction
    assert pg.sql.index(S.SQL_EPOCH_ADOPT) < pg.sql.index(S.SQL_AUDIT_INSERT) < pg.sql.index(SQL_COMMIT)
    # a writer still holding the old token is now fenced out
    hold(pg)
    assert not pg._fenced((7,)) and pg._fenced((0,))


async def test_epoch_adopt_refusals(svc: OpsService, pg: OpsPg, etcd: FakeEtcd) -> None:
    with pytest.raises(OpsUsageError, match="two-person"):
        await svc.epoch_adopt("cluster-dr", "cluster-a", Actor("alice", "DR restore ticket 7"))
    # etcd does not report the id typed by the operator
    rep = await svc.epoch_adopt("cluster-dr", "cluster-a", A2)
    assert rep.fields["refused"] == "cluster_id_not_reported_by_etcd"
    etcd.cluster_id = "cluster-dr"
    rep = await svc.epoch_adopt("cluster-dr", "cluster-x", A2)
    assert rep.fields["refused"] == "cluster_id_changed"
    etcd.fail_all = True
    rep = await svc.epoch_adopt("cluster-dr", "cluster-a", A2)
    assert rep.fields["refused"] == "etcd_unreachable"
    assert pg.o.epoch is not None and pg.o.epoch[:2] == ["cluster-a", 7]
    assert [o for _, o in outcomes(pg)] == ["refused"] * 3


async def test_epoch_adopt_needs_etcd(pg: OpsPg) -> None:
    rep = await OpsService(pg, None).epoch_adopt("cluster-dr", "cluster-a", A2)
    assert rep.fields["refused"] == "etcd_unconfigured"
