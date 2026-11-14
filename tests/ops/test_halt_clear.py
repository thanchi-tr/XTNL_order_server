"""Operator halt clear through the CLI (oversee-rules.md s4, s8): HaltSwitch.operator_clear under HALT_LOCK in
the CLI's fenced transaction, writing halt_events + oversee_events + operator_audit together."""
from __future__ import annotations

import json

import pytest

from src.db import halt_store as H
from src.db.epoch import SQL_COMMIT
from src.db.sweep_writer import SQL_EVENTS
from src.ops import sql as S
from src.ops.halt import operator_sweep_id
from src.ops.model import Actor, OpsUsageError
from src.ops.service import OpsService
from src.oversee.types import BreachState, Cap, Level

from tests.ops.conftest import outcomes
from tests.ops.emulator import NOW, CheckViolation, OpsPg

ONE = Actor("alice", "reviewed INC-9: loss explained by a withdrawal", os_user="alice")
TWO = Actor("alice", "reviewed INC-9: HARD input stale, cannot be confirmed", approver="bob")


def state(rule: str, account: str, level: Level, *, cap: Cap = Cap.NONE, subject: str | None = None,
          unknown: bool = False) -> BreachState:
    return BreachState(rule, "account", subject or account, account, level=level, cap=cap, reason=rule,
                       active_since=NOW - 60_000, activation_id=11, uncapped_ms=45_000, last_sweep_id=5,
                       unknown_since=NOW - 40_000 if unknown else None)


def latched(pg: OpsPg) -> int:
    return pg.seed_event("HARD", "HARD", True, rule="MaxDailyLossRule", reason="escalation")


async def test_clear_releases_the_latch_to_the_sweep_target_and_audits(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_state(state("MarginUsageRule", "ACC", Level.SOFT_HALT))
    eid = latched(pg)
    rep = await svc.halt_clear("ACC", eid, force=False, actor=ONE)
    assert rep.outcome == "applied" and rep.fields["effective_level"] == "SOFT"
    ev = pg.t.halt_events[-1]
    assert ev[1:4] == ("SOFT", "SOFT", False) and ev[6] == "alice"        # level, effective, latch, by
    row = pg.t.breaches[("MarginUsageRule", "ACCOUNT", "ACC")]
    assert row["uncapped_ms"] == 0 and row["clear_generation"] == 1    # reviewed: escalation clock restarts
    (audit,) = pg.o.audit
    assert audit["command"] == "halt.clear" and audit["target"] == "ACC" and audit["outcome"] == "applied"
    assert json.loads(audit["detail"])["halt_event_id"] == ev[0]
    # HALT_LOCK first; halt_events and the audit row commit together
    sql = pg.sql
    assert sql.index(H.SQL_HALT_LOCK) < sql.index(H.SQL_INSERT_EVENT) < sql.index(S.SQL_AUDIT_INSERT) < \
        sql.index(SQL_COMMIT)
    assert pg.t.events == {}           # a non-forced clear resolves no rule state, so no transition row
    assert any("no order action" in w for w in rep.warnings)


async def test_forced_clear_resolves_the_accounts_capped_hard_state_with_oversee_event(
        svc: OpsService, pg: OpsPg) -> None:
    pg.seed_state(state("MaxDailyLossRule", "ACC", Level.HARD_HALT, cap=Cap.SOFT, subject="ACC:2026-11-13"))
    eid = latched(pg)
    with pytest.raises(OpsUsageError, match="two-person"):
        await svc.halt_clear("ACC", eid, force=True, actor=ONE)
    rep = await svc.halt_clear("ACC", eid, force=True, actor=TWO)
    assert rep.outcome == "applied" and rep.fields["resolved"] == ["MaxDailyLossRule/account/ACC:2026-11-13"]
    row = pg.t.breaches[("MaxDailyLossRule", "ACCOUNT", "ACC:2026-11-13")]
    assert row["resolved_by"] == "operator" and row["resolved_at"] == NOW
    ((key, val),) = pg.t.events.items()
    assert key == ("MaxDailyLossRule", "ACCOUNT", "ACC:2026-11-13", "resolved", operator_sweep_id(NOW))
    assert val[2].startswith("operator_clear: ") and val[5] == "ACC"   # reason, account
    assert pg.t.halt_events[-1][2] == "NONE"
    assert pg.sql.index(SQL_EVENTS) < pg.sql.index(S.SQL_AUDIT_INSERT) < pg.sql.index(SQL_COMMIT)
    assert json.loads(pg.o.audit[-1]["detail"])["resolved"] == ["MaxDailyLossRule/account/ACC:2026-11-13"]


async def test_forced_clear_is_scoped_to_the_named_account(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_state(state("MaxDailyLossRule", "ACC", Level.HARD_HALT, cap=Cap.SOFT))
    pg.seed_state(state("MaxDailyLossRule", "OTHER", Level.HARD_HALT, unknown=True))
    eid = latched(pg)
    n_events = len(pg.t.halt_events)
    rep = await svc.halt_clear("ACC", eid, force=True, actor=TWO)
    assert rep.outcome == "refused" and rep.fields["refused"] == "active_hard_state_on_other_account"
    assert "OTHER" in rep.fields["detail"]
    assert len(pg.t.halt_events) == n_events and pg.t.events == {}
    assert pg.t.breaches[("MaxDailyLossRule", "ACCOUNT", "ACC")]["resolved_at"] is None
    assert outcomes(pg) == [("halt.clear", "refused")]


@pytest.mark.parametrize("case", ["stale_review", "uncapped_hard", "active_hard_no_force", "unknown_account"])
async def test_clear_refusals_change_nothing_and_are_audited(svc: OpsService, pg: OpsPg, case: str) -> None:
    eid = latched(pg)
    force, reviewed, account, actor = False, eid, "ACC", ONE
    if case == "stale_review":
        pg.seed_state(state("MarginUsageRule", "ACC", Level.SOFT_HALT))
        reviewed = eid - 1 if eid > 1 else None
        expected = "newer_halt_event"
    elif case == "uncapped_hard":
        pg.seed_state(state("MaxDailyLossRule", "ACC", Level.HARD_HALT))
        force, actor, expected = True, TWO, "hard_state_uncapped"
    elif case == "active_hard_no_force":
        pg.seed_state(state("MaxDailyLossRule", "ACC", Level.HARD_HALT, cap=Cap.SOFT))
        expected = "active_hard_state"
    else:
        account, expected = "NOPE", "unknown_account"
    before = (list(pg.t.halt_events), {k: dict(v) for k, v in pg.t.breaches.items()})
    rep = await svc.halt_clear(account, reviewed, force=force, actor=actor)
    assert rep.outcome == "refused" and rep.fields["refused"] == expected
    assert (pg.t.halt_events, pg.t.breaches) == before and pg.t.events == {}
    assert outcomes(pg) == [("halt.clear", "refused")]


async def test_clear_refused_when_the_epoch_moves_under_halt_lock(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_state(state("MarginUsageRule", "ACC", Level.SOFT_HALT))
    eid = latched(pg)
    pg.before[H.SQL_HALT_LOCK] = lambda: pg.bump(9)
    rep = await svc.halt_clear("ACC", eid, force=False, actor=ONE)
    assert rep.outcome == "refused" and rep.fields["refused"] == "epoch_moved"
    assert len(pg.t.halt_events) == 1 and pg.t.breaches[("MarginUsageRule", "ACCOUNT", "ACC")]["uncapped_ms"] == 45_000


async def test_clear_rolls_back_when_the_audit_row_fails(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_state(state("MarginUsageRule", "ACC", Level.SOFT_HALT))
    eid = latched(pg)

    def boom() -> None:
        raise CheckViolation("audit")

    pg.before[S.SQL_AUDIT_INSERT] = boom
    with pytest.raises(CheckViolation):
        await svc.halt_clear("ACC", eid, force=False, actor=ONE)
    assert len(pg.t.halt_events) == 1 and pg.o.audit == []


async def test_clear_with_nothing_to_change_is_a_noop(svc: OpsService, pg: OpsPg) -> None:
    pg.o.accounts.add("ACC")
    eid = pg.seed_event("NONE", "NONE", False, reason="cool_down")
    rep = await svc.halt_clear("ACC", eid, force=False, actor=ONE)
    assert rep.outcome == "noop" and outcomes(pg) == [("halt.clear", "noop")] and len(pg.t.halt_events) == 1


async def test_halt_status_shows_the_review_id_and_active_states(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_state(state("MarginUsageRule", "ACC", Level.SOFT_HALT))
    pg.seed_state(state("MarginUsageRule", "B", Level.FLAG))
    eid = latched(pg)
    rep = await svc.halt_status("ACC")
    assert rep.fields["reviewed_event_id_for_clear"] == eid and rep.fields["hard_latch"] is True
    assert [r["account_id"] for r in rep.rows] == ["ACC"]
    assert pg.o.audit == []
