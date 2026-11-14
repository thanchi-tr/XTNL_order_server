"""5.5 operator commands: `halt ack`, `account release`, `cash-flows refresh`, `archive forget`
(oversee-rules.md s4 acknowledge / release_account / refresh_cash_flows; operations.md s11, s16).
Every change commits with its operator_audit row in one fenced transaction; refusals are audited too."""
from __future__ import annotations

import io
import json

import pytest

from src.db import halt_store as H
from src.db.epoch import SQL_COMMIT, SQL_ROLLBACK
from src.db.operator_requests import (
    REQUEST_KINDS,
    RequestPayloadError,
    parse_cash_flow_refresh,
    parse_release,
)
from src.db.pg import PgConnection
from src.ops import cli
from src.ops import sql as S
from src.ops.model import Actor, OpsUsageError
from src.ops.service import OpsService
from src.oversee.types import BreachState, Cap, Level

from tests.ops.conftest import FakeEtcd, outcomes
from tests.ops.emulator import NOW, OpsPg

OP = Actor("carol", "INC-12: withdrawal explains the SOFT, reviewed", os_user="carol")
TWO = Actor("carol", "INC-13: count mismatch, re-export the partition", approver="dave")


def soft(account: str = "ACC", *, level: Level = Level.SOFT_HALT, cap: Cap = Cap.SOFT,
         active: bool = True) -> BreachState:
    return BreachState("MarginUsageRule", "account", account, account, level=level, cap=cap, reason="margin_used",
                       active_since=NOW - 60_000 if active else None, activation_id=3, last_sweep_id=5)


def breach_id(pg: OpsPg, account: str = "ACC") -> int:
    return int(pg.t.breaches[("MarginUsageRule", "ACCOUNT", account)]["breach_id"])


# ============================================================================================ halt ack
async def test_ack_writes_one_breach_ack_under_halt_lock_with_its_audit_row(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_state(soft())
    bid = breach_id(pg)
    rep = await svc.halt_ack(bid, OP)
    assert rep.outcome == "applied" and rep.fields["ack_id"] == 1
    (ack,) = pg.t.acks
    assert (ack["breach_id"], ack["operator"], ack["level"], ack["cap"], ack["reason"]) == (
        bid, "carol", "SOFT", "SOFT", "margin_used")
    assert ack["why"] == OP.reason and ack["at"] == NOW
    (audit,) = pg.o.audit
    assert (audit["command"], audit["target"], audit["outcome"], audit["epoch"]) == ("halt.ack", str(bid), "applied", 7)
    assert json.loads(audit["detail"])["ack_id"] == 1
    sql = pg.sql
    assert sql.index(H.SQL_HALT_LOCK) < sql.index(H.SQL_INSERT_ACK) < sql.index(S.SQL_AUDIT_INSERT) < \
        sql.index(SQL_COMMIT)
    # an acknowledgement never changes a state or the level
    assert pg.t.halt_events == [] and pg.t.breaches[("MarginUsageRule", "ACCOUNT", "ACC")]["level"] == "SOFT"


async def test_second_identical_ack_is_an_audited_noop(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_state(soft())
    bid = breach_id(pg)
    await svc.halt_ack(bid, OP)
    rep = await svc.halt_ack(bid, OP)
    assert rep.outcome == "noop" and len(pg.t.acks) == 1
    assert outcomes(pg) == [("halt.ack", "applied"), ("halt.ack", "noop")]


async def test_ack_refusals_are_audited_and_write_nothing(svc: OpsService, pg: OpsPg) -> None:
    rep = await svc.halt_ack(99, OP)
    assert rep.fields["refused"] == "not_found"
    pg.seed_state(soft(active=False, level=Level.PASS))          # pending, not active: nothing re-pages
    rep = await svc.halt_ack(breach_id(pg), OP)
    assert rep.fields["refused"] == "not_active"
    pg.t.breaches[("MarginUsageRule", "ACCOUNT", "ACC")]["resolved_at"] = NOW - 5
    rep = await svc.halt_ack(breach_id(pg), OP)
    assert rep.fields["refused"] == "resolved"
    assert pg.t.acks == [] and outcomes(pg) == [("halt.ack", "refused")] * 3
    with pytest.raises(OpsUsageError):
        await svc.halt_ack(0, OP)


async def test_ack_under_a_moved_epoch_is_refused_and_rolled_back(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_state(soft())
    pg.before[H.SQL_INSERT_ACK] = lambda: pg.bump(8)
    pg.epoch = 8   # the halt tables' fence follows the bumped epoch
    rep = await svc.halt_ack(breach_id(pg), OP)
    assert rep.fields["refused"] == "epoch_moved" and pg.t.acks == []
    assert SQL_ROLLBACK in pg.sql


async def test_halt_status_lists_the_breach_id_to_acknowledge(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_state(soft())
    rep = await svc.halt_status(None)
    assert rep.rows[0]["breach_id"] == breach_id(pg)


# ============================================================================================ account release
async def test_release_releases_the_row_and_queues_the_in_process_request(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_sweep_quarantine("ACC")
    rep = await svc.account_release("ACC", OP)
    assert rep.outcome == "applied"
    q = pg.o.sweep_quarantine["ACC"]
    assert q["released_at_ms"] == NOW and q["released_by"] == "carol"
    (req,) = pg.o.requests
    assert (req["kind"], req["target"], req["requested_epoch"], req["audit_id"]) == (
        "release_account", "ACC", 7, rep.audit_id)
    assert parse_release("ACC", req["payload"]).adapter_id == "stonex"
    sql = pg.sql
    assert sql.index(S.SQL_SWEEP_QUARANTINE_RELEASE) < sql.index(S.SQL_AUDIT_INSERT) < \
        sql.index(S.SQL_REQUEST_INSERT_PAYLOAD) < sql.index(SQL_COMMIT)


async def test_release_refusals(svc: OpsService, pg: OpsPg) -> None:
    rep = await svc.account_release("NOPE", OP)
    assert rep.fields["refused"] == "not_quarantined"
    pg.seed_sweep_quarantine("ACC", released=True)
    rep = await svc.account_release("ACC", OP)
    assert rep.fields["refused"] == "already_released" and "pass" in rep.fields["detail"]
    assert pg.o.requests == [] and outcomes(pg) == [("account.release", "refused")] * 2
    with pytest.raises(OpsUsageError):
        await svc.account_release("bad account", OP)


async def test_release_under_a_moved_epoch_changes_nothing(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_sweep_quarantine("ACC")
    pg.before[S.SQL_SWEEP_QUARANTINE_RELEASE] = lambda: pg.bump(8)
    rep = await svc.account_release("ACC", OP)
    assert rep.fields["refused"] == "epoch_moved"
    assert pg.o.sweep_quarantine["ACC"]["released_at_ms"] is None and pg.o.requests == []


async def test_account_status_lists_quarantined_accounts(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_sweep_quarantine("ACC")
    pg.seed_sweep_quarantine("OLD", released=True)
    rep = await svc.account_status(None, include_released=False, limit=20)
    assert [r["account_id"] for r in rep.rows] == ["ACC"] and rep.fields["quarantined"] == 1
    rep = await svc.account_status(None, include_released=True, limit=20)
    assert len(rep.rows) == 2


# ============================================================================================ cash-flows refresh
async def test_refresh_queues_a_request_with_its_payload(svc: OpsService, pg: OpsPg) -> None:
    pg.o.account_adapter["ACC"] = "pepperstone"
    rep = await svc.cash_flows_refresh("ACC", None, OP)
    assert rep.outcome == "applied" and rep.fields["since"] == "roll"
    (req,) = pg.o.requests
    assert (req["kind"], req["target"], req["audit_id"]) == ("refresh_cash_flows", "ACC", rep.audit_id)
    cf = parse_cash_flow_refresh("ACC", req["payload"])
    assert (cf.account_id, cf.adapter_id, cf.since_ms) == ("ACC", "pepperstone", None)
    rep = await svc.cash_flows_refresh("ACC", NOW - 3_600_000, OP)
    assert rep.fields["refused"] == "already_pending" and len(pg.o.requests) == 1
    pg.o.requests[0]["consumed_at_ms"] = NOW
    rep = await svc.cash_flows_refresh("ACC", NOW - 3_600_000, OP)
    assert rep.outcome == "applied"
    assert parse_cash_flow_refresh("ACC", pg.o.requests[1]["payload"]).since_ms == NOW - 3_600_000


async def test_refresh_refusals(svc: OpsService, pg: OpsPg) -> None:
    rep = await svc.cash_flows_refresh("NOPE", None, OP)
    assert rep.fields["refused"] == "unknown_account" and pg.o.requests == []
    with pytest.raises(OpsUsageError):
        await svc.cash_flows_refresh("ACC", NOW + 1, OP)      # the future is not a fetch start


def test_request_payload_parsers_reject_mismatches() -> None:
    assert REQUEST_KINDS == {"reset_task", "release_account", "refresh_cash_flows"}
    with pytest.raises(RequestPayloadError):
        parse_cash_flow_refresh("ACC", '{"account_id": "OTHER"}')
    with pytest.raises(RequestPayloadError):
        parse_cash_flow_refresh("ACC", {"account_id": "ACC", "since_ms": -1})
    with pytest.raises(RequestPayloadError):
        parse_cash_flow_refresh("ACC", {"account_id": "ACC", "since_ms": True})
    with pytest.raises(RequestPayloadError):
        parse_release("ACC", "[1]")
    assert parse_release("ACC", {}).account_id == "ACC"


# ============================================================================================ archive forget
async def test_forget_is_two_person_removes_the_ledger_row_and_audits(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_archive("event_log_p202607", 100)
    pg.o.partition_rows["event_log_p202607"] = 103
    with pytest.raises(OpsUsageError, match="two-person"):
        await svc.archive_forget("event_log_p202607", OP)
    rep = await svc.archive_forget("event_log_p202607", TWO)
    assert rep.outcome == "applied" and "event_log_p202607" not in pg.o.archive
    assert rep.fields["archived_row_count"] == 100 and rep.fields["current_rows"] == 103
    (audit,) = pg.o.audit
    assert (audit["command"], audit["approver"], audit["outcome"]) == ("archive.forget", "dave", "applied")
    assert json.loads(audit["detail"])["checksum"].startswith("sha256:")


async def test_forget_refuses_a_detached_or_unknown_partition(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_archive("event_log_p202606", 100, detached=True)
    rep = await svc.archive_forget("event_log_p202606", TWO)
    assert rep.fields["refused"] == "detached" and "event_log_p202606" in pg.o.archive
    rep = await svc.archive_forget("event_log_p202605", TWO)
    assert rep.fields["refused"] == "not_found"
    with pytest.raises(OpsUsageError):
        await svc.archive_forget("event_log; DROP TABLE x", TWO)


async def test_forget_warns_when_the_counts_still_match(svc: OpsService, pg: OpsPg) -> None:
    pg.seed_archive("events_dlq_history_p202607", 5)
    pg.o.partition_rows["events_dlq_history_p202607"] = 5
    rep = await svc.archive_forget("events_dlq_history_p202607", TWO)
    assert rep.outcome == "applied" and any("re-exports the same rows" in w for w in rep.warnings)
    rep = await svc.archive_list(limit=5)
    assert rep.rows == []


# ============================================================================================ CLI end to end
@pytest.mark.parametrize("argv", [
    ["halt", "ack", "4", "--operator", "carol", "--reason", "INC-12 reviewed and understood"],
    ["account", "status", "--all"],
    ["account", "release", "ACC", "--operator", "carol", "--reason", "INC-12 cause fixed in 1.4.3"],
    ["cash-flows", "refresh", "ACC", "--since-ms", "1700000000000", "--operator", "carol", "--reason", "INC-12 transfer"],
    ["archive", "list", "--limit", "5"],
    ["archive", "forget", "event_log_p202607", "--operator", "carol", "--approver", "dave", "--reason", "INC-13 mismatch"],
])
def test_new_commands_parse(argv: list[str]) -> None:
    cli.build_parser().parse_args(argv)


def test_archive_forget_needs_an_approver_on_the_command_line() -> None:
    with pytest.raises(cli.OpsUsageError):
        cli.build_parser().parse_args(["archive", "forget", "event_log_p202607", "--operator", "carol",
                                       "--reason", "INC-13 mismatch"])


def test_halt_ack_end_to_end_exit_codes() -> None:
    pg = OpsPg()
    pg.seed_state(soft())

    async def connect(dsn: str) -> tuple[PgConnection, cli.Closer]:
        async def close() -> None:
            return None
        return pg, close

    def run(*argv: str) -> tuple[int, str]:
        out, err = io.StringIO(), io.StringIO()
        code = cli.main(list(argv), environ={"ORDER_SERVER_DSN": "postgresql://u:p@h/db"}, connect=connect,
                        etcd_factory=lambda e: FakeEtcd(), out=out, err=err)
        return code, out.getvalue()

    bid = str(breach_id(pg))
    code, out = run("halt", "ack", bid, "--operator", "carol", "--reason", "INC-12 reviewed and understood")
    assert code == cli.EXIT_OK and "outcome: applied" in out and "ack_id: 1" in out
    code, out = run("halt", "ack", "999", "--operator", "carol", "--reason", "INC-12 reviewed and understood")
    assert code == cli.EXIT_REFUSED and "refused: not_found" in out
