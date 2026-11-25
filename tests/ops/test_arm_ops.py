"""The ARM and market-data operator commands (plan S34): controls under the table lock with their audit row, the
local accept with every refusal (hash changed, tampered, invalid, no broker bars, admission, PR13 registry), the
one-shot requests (heartbeat, budget, pending, moot, the answer), `arm failed show`, and no secret or row text
outside `config show --pending`. Synthetic instruments and accounts only; no network."""
from __future__ import annotations

import inspect
import io
import json
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from src.armconfig.admission import AdmissionLimits
from src.armconfig.chain import SeenRows
from src.armconfig.client import ConfigSource, ConfigSourceError, SelfCheckError
from src.armconfig.document import Document, DocumentProblem
from src.armconfig.installer import document_json
from src.db.pg import PgConnection
from src.ops import arm_sql as A
from src.ops import cli
from src.ops.arm import bar_capable_adapters as ops_bar_capable_adapters
from src.ops.arm import (
    HEARTBEAT_MAX_AGE_MS,
    MAX_PENDING_ARM_REQUESTS,
    TRACKER_REQUEST_KINDS,
    ArmContext,
    ArmOps,
    bars_caps,
    no_bar_refusals,
    referenced_tokens,
)
from src.ops.model import Actor, OpsUsageError
from src.ops.service import OpsService
from src.riskmgr.registry import REGISTRY
from src.runtime.etcd import HEALTH_PREFIX
from src.runtime.health import HealthRecord

from tests.armconfig.docs import assignment_row, parsed, raw_doc, route_row, strategy_row
from tests.db.test_arm_stores_static import sql_write_violations
from tests.ops.arm_emulator import ArmOpsPg
from tests.ops.conftest import FakeEtcd
from tests.ops.emulator import NOW

ACTOR = Actor("alice", "INC-77 operator action")
DSN = "postgresql://xtnl_arm_reader:Pl4nted-Secret@db.planted-host.example:6543/postgres"
SECRETS = ("Pl4nted-Secret", "planted-host", DSN)
TOKENS = REGISTRY.registry_tokens()


class Source:
    """A `ConfigSource` returning a fixed read (or raising)."""

    def __init__(self, doc: Document | DocumentProblem | BaseException) -> None:
        self.doc = doc
        self.reads = 0

    async def read(self) -> Document | DocumentProblem:
        self.reads += 1
        if isinstance(self.doc, BaseException):
            raise self.doc
        return self.doc


def doc(**kw: Any) -> Document:
    kw.setdefault("route_policy", [route_row(route="*")])
    return parsed(raw_doc(**kw))


def ctx(source: ConfigSource | None = None, **kw: Any) -> ArmContext:
    async def no_sleep(_s: float) -> None:
        return None

    base: dict[str, Any] = {"config_source": lambda: source, "bar_capable": frozenset({"mock"}),
                            "bars_per_min": {"mock": 60}, "sleep": no_sleep}
    base.update(kw)
    return ArmContext(**base)


def node(etcd: FakeEtcd, name: str = "node-a", *, registry: Sequence[str] | None = TOKENS, age_ms: int = 1_000,
         drop_field: bool = False) -> None:
    rec = HealthRecord(node=name, ts_ms=NOW - age_ms, role="LEADER", pg_writable=True, etcd_reachable=True,
                       redis_writable=True, chrony_offset_ms=0.1, registry=tuple(registry or ()))
    text = rec.to_json()
    if drop_field:
        d = json.loads(text)
        del d["registry"]
        text = json.dumps(d)
    etcd.kv[HEALTH_PREFIX + name] = text


def seen_cache(pg: ArmOpsPg, accepted: Document) -> None:
    """The engine installed `accepted`: its rows are seen and it is the cached document."""
    seen = SeenRows.build(accepted.row_hashes().items())
    pg.a.cache = (accepted.doc_hash, accepted.schema_version, document_json(accepted.schema_version, accepted.rows),
                  json.dumps(dict(seen.rows)), seen.head, seen.count, NOW - 3_600_000)
    pg.a.accepted.append({"accepted_id": 1, "doc_hash": accepted.doc_hash, "schema_version": 1, "mode": "manual",
                          "accepted_at_ms": NOW - 7_200_000, "accepted_by": "bob", "audit_id": None})


@pytest.fixture
def pg() -> ArmOpsPg:
    return ArmOpsPg()


@pytest.fixture
def etcd() -> FakeEtcd:
    e = FakeEtcd()
    node(e)
    return e


def ops(pg: ArmOpsPg, etcd: FakeEtcd | None, context: ArmContext | None = None) -> ArmOps:
    return OpsService(pg, etcd, clock_ms=lambda: NOW, arm=context or ctx()).arm


def audits(pg: ArmOpsPg) -> list[tuple[str, str]]:
    return [(r["command"], r["outcome"]) for r in pg.o.audit]


# ============================================================================================== controls
async def test_pause_writes_the_flag_with_its_audit_row_under_the_table_lock(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    rep = await ops(pg, etcd).pause(ACTOR, drain_notices=False)
    assert rep.outcome == "applied" and rep.fields["pause_reason"] == "operator" and not rep.fields["drain_notices"]
    c = pg.a.controls
    assert c is not None and c["paused"] and c["pause_reason"] == "operator" and c["paused_at_ms"] == NOW
    assert c["updated_by"] == "alice" and c["audit_id"] == rep.audit_id == pg.o.audit[-1]["audit_id"]
    assert audits(pg) == [("arm.pause", "applied")]
    sql = pg.sql
    assert sql.index(A.SQL_CONTROLS_LOCK) < sql.index(A.SQL_CONTROLS_FOR_UPDATE) < sql.index(A.SQL_CONTROLS_PAUSE)
    assert pg.statements(A.SQL_CONTROLS_LOCK) == 1


async def test_a_second_pause_is_a_noop_and_drain_notices_widens_it(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    o = ops(pg, etcd)
    await o.pause(ACTOR, drain_notices=False)
    pg.now += 60_000
    again = await o.pause(ACTOR, drain_notices=False)
    assert again.outcome == "noop" and pg.a.controls is not None and pg.a.controls["paused_at_ms"] == NOW
    wider = await o.pause(ACTOR, drain_notices=True)
    assert wider.outcome == "applied" and pg.a.controls["drain_notices"]
    assert pg.a.controls["paused_at_ms"] == NOW                      # the first pause time is kept
    assert any("suppressed" in w for w in wider.warnings)
    assert (await o.pause(ACTOR, drain_notices=False)).outcome == "noop"    # a pause never narrows the drain
    assert pg.a.controls["drain_notices"]
    assert audits(pg) == [("arm.pause", "applied"), ("arm.pause", "noop"), ("arm.pause", "applied"),
                          ("arm.pause", "noop")]


async def test_resume_ends_the_pause_and_the_drain_and_never_mentions_clearing_failed(pg: ArmOpsPg,
                                                                                     etcd: FakeEtcd) -> None:
    o = ops(pg, etcd)
    assert (await o.resume(ACTOR)).outcome == "noop"
    await o.pause(ACTOR, drain_notices=True)
    rep = await o.resume(ACTOR)
    c = pg.a.controls
    assert rep.outcome == "applied" and c is not None
    assert (c["paused"], c["pause_reason"], c["paused_at_ms"], c["drain_notices"]) == (False, None, None, False)
    assert any("never clears FAILED" in w for w in rep.warnings)
    assert audits(pg)[-1] == ("arm.resume", "applied")


async def test_notify_test_records_the_time(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    rep = await ops(pg, etcd).notify_test(ACTOR)
    assert rep.outcome == "applied" and pg.a.controls is not None and pg.a.controls["notify_test_at_ms"] == NOW
    assert rep.fields["notify_test_at"] is not None and audits(pg) == [("arm.notify_test", "applied")]
    # S34 deviation 3: nothing sends a test notice yet, and the output says so
    assert rep.warnings == ["recorded in arm_controls.notify_test_at_ms only: the ARM test-notice sender is not "
                            "wired in this build (S32 deviation 1), so no notice is sent yet"]


async def test_controls_without_their_row_are_refused_and_a_failed_write_leaves_no_audit_row(
        pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    pg.a.controls = None
    rep = await ops(pg, etcd).pause(ACTOR, drain_notices=False)
    assert rep.outcome == "refused" and rep.fields["refused"] == "no_controls_row"
    assert audits(pg) == [("arm.pause", "refused")]
    pg2 = ArmOpsPg()

    def boom() -> None:
        raise ConnectionResetError("lost")

    pg2.before[A.SQL_CONTROLS_PAUSE] = boom
    with pytest.raises(ConnectionResetError):
        await ops(pg2, etcd).pause(ACTOR, drain_notices=False)
    assert pg2.o.audit == [] and pg2.a.controls is not None and not pg2.a.controls["paused"]   # rolled back


async def test_controls_validate_the_actor(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    with pytest.raises(OpsUsageError):
        await ops(pg, etcd).pause(Actor("alice", "short"), drain_notices=False)
    assert pg.o.audit == []


# ============================================================================================== requests
async def test_a_request_is_refused_without_a_fresh_leader_heartbeat(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    pg.seed_position("mock", "P1")
    o = ops(pg, etcd)
    rep = await o.disarm("mock:P1", ACTOR, wait_s=0)
    assert rep.outcome == "refused" and rep.fields["refused"] == "no_arm_heartbeat" and "none" in rep.fields["detail"]
    pg.heartbeat(age_ms=HEARTBEAT_MAX_AGE_MS + 1_000)
    assert (await o.disarm("mock:P1", ACTOR, wait_s=0)).fields["refused"] == "no_arm_heartbeat"
    pg.heartbeat(age_ms=1_000, leader=False)                 # a standby's row is no leader heartbeat
    assert (await o.disarm("mock:P1", ACTOR, wait_s=0)).fields["refused"] == "no_arm_heartbeat"
    epoch = pg.token()
    assert epoch is not None
    pg.heartbeat(age_ms=1_000, epoch=epoch - 1)              # a fresh row of the old leader's epoch does not count
    rep = await o.disarm("mock:P1", ACTOR, wait_s=0)
    assert rep.fields["refused"] == "no_arm_heartbeat" and f"epoch {epoch}" in rep.fields["detail"]
    assert pg.o.requests == [] and audits(pg) == [("arm.disarm", "refused")] * 4
    pg.heartbeat(age_ms=1_000)
    assert (await o.disarm("mock:P1", ACTOR, wait_s=0)).outcome == "applied"


async def test_a_request_is_refused_at_twenty_pending_arm_requests(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    pg.heartbeat()
    o = ops(pg, etcd)
    for i in range(MAX_PENDING_ARM_REQUESTS):
        rep = await o.md_rebuild(f"{i:016x}", ACTOR, wait_s=0)
        assert rep.outcome == "applied", rep.fields
    pg.o.requests.append({"request_id": 99, "kind": "reset_task", "target": "x", "audit_id": 1, "requested_epoch": 7,
                          "consumed_at_ms": None, "payload": "{}"})
    rep = await o.md_writer_reset(ACTOR, wait_s=0)
    assert rep.outcome == "refused" and rep.fields["refused"] == "too_many_pending"
    assert "20 ARM / market-data requests" in rep.fields["detail"]         # the core reset_task does not count
    pg.answer(1, "applied")
    assert (await o.md_writer_reset(ACTOR, wait_s=0)).outcome == "applied"


async def test_unanswered_tracker_requests_have_their_own_budget(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    """The arm-tracker kinds have their own budget (S34; kept in S36b): 20 pending disarms (the ARM off, nothing
    answers them) never block a FAILED clear or a repair."""
    pg.heartbeat()
    o = ops(pg, etcd)
    for i in range(MAX_PENDING_ARM_REQUESTS):
        pg.seed_position("mock", f"P{i}")
        rep = await o.disarm(f"mock:P{i}", ACTOR, wait_s=0)
        assert rep.outcome == "applied" and rep.fields["request"] == "pending", rep.fields
    assert any("arm-tracker answers it" in w for w in rep.warnings)
    pg.seed_position("mock", "PX")
    rep = await o.rearm("mock:PX", ACTOR, wait_s=0)
    assert rep.fields["refused"] == "too_many_pending" and "arm-tracker" in rep.fields["detail"]
    pg.seed_failure("SERIES", "mock:SYN.A:15m:BID:BROKER")
    rep = await o.failed_clear("SERIES:mock:SYN.A:15m:BID:BROKER", ACTOR, wait_s=0)
    assert rep.outcome == "applied" and rep.fields["request"] == "pending"
    assert not any("arm-tracker" in w for w in rep.warnings)
    assert (await o.md_rehash(ACTOR, wait_s=0)).outcome == "applied"
    assert TRACKER_REQUEST_KINDS == {"arm_disarm", "arm_rearm", "arm_repin"}


def test_the_tracker_kinds_are_the_ones_the_arm_tracker_answers() -> None:
    """S36b: every ARM request kind has a handler; the tracker kinds are exactly the arm-tracker's."""
    from src.db.operator_requests import ARM_REQUEST_KINDS
    from src.riskmgr.tracker import REQUEST_KINDS
    from src.runtime.optional_plan import ARM_KINDS, MD_KINDS

    assert TRACKER_REQUEST_KINDS == REQUEST_KINDS and TRACKER_REQUEST_KINDS <= ARM_KINDS
    assert ARM_REQUEST_KINDS <= ARM_KINDS | MD_KINDS


async def test_the_same_pending_request_is_refused(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    pg.heartbeat()
    o = ops(pg, etcd)
    assert (await o.md_rehash(ACTOR, wait_s=0)).outcome == "applied"
    rep = await o.md_rehash(ACTOR, wait_s=0)
    assert rep.fields["refused"] == "already_pending" and len(pg.o.requests) == 1


async def test_a_position_that_is_not_armed_is_moot_and_files_nothing(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    pg.heartbeat()
    pg.seed_position("mock", "P9", state="DISARMED")
    o = ops(pg, etcd)
    for target in ("mock:P1", "mock:P9"):
        for call in (o.disarm, o.rearm):
            rep = await call(target, ACTOR, wait_s=0)
            assert rep.outcome == "noop" and rep.fields["request"] == "moot:not_armed", target
    assert pg.o.requests == [] and set(audits(pg)) == {("arm.disarm", "noop"), ("arm.rearm", "noop")}
    assert json.loads(pg.o.audit[0]["detail"]) == {"kind": "arm_disarm", "moot": "not_armed"}


async def test_disarm_files_the_request_with_its_audit_row_fenced_on_the_epoch(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    pg.heartbeat()
    pg.seed_position("mock", "P1", state="TRACKING")
    rep = await ops(pg, etcd).disarm("mock:P1", ACTOR, wait_s=0)
    assert rep.outcome == "applied" and rep.fields["request"] == "pending" and rep.fields["epoch"] == 7
    (req,) = pg.o.requests
    assert (req["kind"], req["target"], req["payload"], req["requested_epoch"]) == ("arm_disarm", "mock:P1", "{}", 7)
    assert req["audit_id"] == rep.audit_id and audits(pg) == [("arm.disarm", "applied")]


async def test_the_answer_is_reported_applied_moot_or_refused(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    pg.heartbeat()
    answers = iter(["applied", "moot:already_pinned", "refused:kind_changed"])

    async def answer_on_sleep(_s: float) -> None:
        pg.answer(pg.o.requests[-1]["request_id"], current)

    o = ops(pg, etcd, ctx(sleep=answer_on_sleep))
    for i, expected in enumerate(("applied", "applied", "refused")):
        current = next(answers)
        pg.seed_position("mock", f"P{i}")
        rep = await o.repin(f"mock:P{i}", ACTOR, strategy_code=None, strategy_version=None, wait_s=2)
        assert rep.fields["request"] == current and rep.outcome == expected
    assert rep.fields["refused"] == "kind_changed"
    pg.seed_position("mock", "PX")

    async def never(_s: float) -> None:
        return None

    slow = ops(pg, etcd, ctx(sleep=never))
    rep = await slow.disarm("mock:PX", ACTOR, wait_s=1)
    assert rep.outcome == "applied" and rep.fields["request"] == "pending"
    assert pg.statements(A.SQL_REQUEST_RESULT) >= 3 + 3          # polled until the wait ran out


async def test_repin_payload_names_the_strategy_or_nothing(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    pg.heartbeat()
    pg.seed_position("mock", "P1")
    o = ops(pg, etcd)
    with pytest.raises(OpsUsageError):
        await o.repin("mock:P1", ACTOR, strategy_code="be_a", strategy_version=None, wait_s=0)
    with pytest.raises(OpsUsageError):
        await o.repin("mock:P1", ACTOR, strategy_code="BAD CODE", strategy_version=1, wait_s=0)
    rep = await o.repin("mock:P1", ACTOR, strategy_code="be_a", strategy_version=2, wait_s=0)
    assert rep.outcome == "applied"
    assert json.loads(pg.o.requests[-1]["payload"]) == {"v": 1, "strategy_code": "be_a", "strategy_version": 2}


async def test_failed_clear_files_the_scope_or_is_moot(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    pg.heartbeat()
    o = ops(pg, etcd)
    for bad in ("SERIES", "NOPE:x", " ARM:arm", "series:x"):
        with pytest.raises(OpsUsageError):
            await o.failed_clear(bad, ACTOR, wait_s=0)
    rep = await o.failed_clear("SERIES:mock:SYN.A:15m:BID:BROKER", ACTOR, wait_s=0)
    assert rep.outcome == "noop" and rep.fields["request"] == "moot:not_failed" and pg.o.requests == []
    pg.seed_failure("SERIES", "mock:SYN.A:15m:BID:BROKER")
    rep = await o.failed_clear("SERIES:mock:SYN.A:15m:BID:BROKER", ACTOR, wait_s=0)
    assert rep.outcome == "applied"
    (req,) = pg.o.requests
    assert (req["kind"], req["target"], req["payload"]) == ("arm_clear_failed", "SERIES:mock:SYN.A:15m:BID:BROKER",
                                                             "{}")


async def test_market_data_requests_carry_their_target_and_payload(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    pg.heartbeat()
    o = ops(pg, etcd)
    with pytest.raises(OpsUsageError):
        await o.md_backfill("not-a-series", 0, 1, ACTOR, wait_s=0)
    with pytest.raises(OpsUsageError):
        await o.md_backfill("mock:SYN.A:15m:BID:BROKER", 900_000, 0, ACTOR, wait_s=0)
    with pytest.raises(OpsUsageError):
        await o.md_rebuild("ATR:1", ACTOR, wait_s=0)
    with pytest.raises(OpsUsageError):
        await o.md_rebuild("0123456789abcdef\n", ACTOR, wait_s=0)        # a trailing newline is no id
    with pytest.raises(OpsUsageError):
        await o.md_rehash(ACTOR, wait_s=61)
    await o.md_backfill("mock:SYN.A:15m:BID:BROKER", 0, 1_800_000, ACTOR, wait_s=0)
    await o.md_rebuild("0123456789abcdef", ACTOR, wait_s=0)
    await o.md_writer_reset(ACTOR, wait_s=0)
    await o.md_rehash(ACTOR, wait_s=0)
    got = [(r["kind"], r["target"], json.loads(r["payload"])) for r in pg.o.requests]
    assert got == [("md_backfill", "mock:SYN.A:15m:BID:BROKER", {"v": 1, "from_bucket_ms": 0,
                                                                 "to_bucket_ms": 1_800_000}),
                   ("md_rebuild", "0123456789abcdef", {}), ("md_writer_reset", "md-writer", {}),
                   ("md_rehash", "md", {})]


async def test_an_epoch_bump_during_the_filing_refuses_it_and_writes_nothing(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    from src.ops import sql as S

    pg.heartbeat()
    pg.before[S.SQL_REQUEST_INSERT_PAYLOAD] = lambda: pg.bump(8)
    rep = await ops(pg, etcd).md_rehash(ACTOR, wait_s=0)
    assert rep.outcome == "refused" and rep.fields["refused"] == "epoch_moved"
    assert pg.o.requests == [] and audits(pg) == [("md.rehash", "refused")]   # the in-transaction audit rolled back


# ============================================================================================== FAILED view
async def test_failed_show_lists_open_episodes_counters_and_pending_clears(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    pg.heartbeat()
    pg.seed_failure("SERIES", "mock:SYN.A:15m:BID:BROKER", positions=25)
    pg.seed_failure("ARM", "arm", cleared=True)
    o = ops(pg, etcd)
    await o.failed_clear("SERIES:mock:SYN.A:15m:BID:BROKER", ACTOR, wait_s=0)
    rep = await o.failed_show(include_cleared=False, limit=20)
    assert rep.fields["open"] == 1 and len(rep.rows) == 1
    row = rep.rows[0]
    assert row["scope"] == "SERIES:mock:SYN.A:15m:BID:BROKER" and row["probe"] == "series_reread"
    assert row["counters"] == "invalid_bars=3"                 # integer counts only
    assert row["positions"] == 25 and row["position_ids"].endswith("and 5 more")
    assert row["pending_clears"] == [1]
    everything = await o.failed_show(include_cleared=True, limit=20)
    assert len(everything.rows) == 2 and everything.fields["open"] == 1


# ============================================================================================== config
async def test_config_show_reports_the_accept_cache_controls_and_nodes(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    pg.heartbeat()
    a = doc()
    seen_cache(pg, a)
    pg.a.accepted.append({"accepted_id": 2, "doc_hash": "b" * 64, "schema_version": 1, "mode": "manual",
                          "accepted_at_ms": NOW - 1000, "accepted_by": "carol", "audit_id": 5})
    rep = await ops(pg, etcd).config_show(pending=False)
    assert rep.fields["accepted_hash"] == "b" * 64 and rep.fields["cached_hash"] == a.doc_hash
    assert rep.fields["paused"] is False and rep.sections["nodes"][0]["node"] == "node-a"
    assert [r["accepted_id"] for r in rep.sections["accepted"]] == [2, 1]
    assert any("config_hash" in w for w in rep.warnings)
    assert pg.o.audit == []                                   # a read is not audited


async def test_config_show_warns_when_the_accepted_document_is_not_installed(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    pg.a.accepted.append({"accepted_id": 1, "doc_hash": "b" * 64, "schema_version": 1, "mode": "manual",
                          "accepted_at_ms": NOW - 1000, "accepted_by": "carol", "audit_id": 5})
    rep = await ops(pg, etcd).config_show(pending=False)
    assert rep.fields["cached_hash"] is None
    assert any("not installed yet" in w and "config_hash" in w for w in rep.warnings)


async def test_config_show_pending_shows_the_row_diff_escaped(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    old = doc()
    seen_cache(pg, old)
    new = doc(assignments=[assignment_row(1), assignment_row(2, symbol="SYN.A")])
    rep = await ops(pg, etcd, ctx(Source(new))).config_show(pending=True)
    assert rep.fields["chain"] == "pending" and rep.fields["pending_rows"] == 1 and rep.fields["valid"]
    statuses = {r["ref"]: r["status"] for r in rep.sections["rows"]}
    assert statuses == {"strategies:1": "seen", "assignments:1": "seen", "assignments:2": "pending",
                        "route_policy:1": "seen"}
    pending_row = next(r for r in rep.sections["rows"] if r["ref"] == "assignments:2")
    assert '"symbol":"SYN.A"' in pending_row["row"] and "row" not in rep.sections["rows"][0]   # seen rows: no text
    assert rep.fields["acceptable"] and rep.fields["accept_command"] == f"arm config accept {new.doc_hash}"
    assert rep.fields["referenced_versions"] == ["ATR:1", "break_even_on_risk_close:1", "first_bar:1"]
    assert pg.o.audit == []
    # untrusted row text is escaped: a planted terminal escape never reaches the output
    planted = doc(assignments=[assignment_row(1), assignment_row(2, symbol="SYN.A", session_user="ops\x1b[2J")])
    rep = await ops(pg, etcd, ctx(Source(planted))).config_show(pending=True)
    assert rep.fields["valid"] is False and rep.fields["refused_on_accept"] == "invalid"
    assert [p["code"] for p in rep.sections["problems"]] == ["metadata_form"]
    bad = next(r for r in rep.sections["rows"] if r["ref"] == "assignments:2")
    assert "\x1b" not in bad["row"] and "u001b[2J" in bad["row"]       # ASCII-escaped by the canonical form
    out = io.StringIO()
    cli.render(rep, out)
    assert "\x1b" not in out.getvalue()


async def test_config_show_pending_marks_a_changed_seen_row_with_what_it_was(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    seen_cache(pg, doc())
    changed = doc(assignments=[assignment_row(1, symbol="SYN.B")])
    rep = await ops(pg, etcd, ctx(Source(changed))).config_show(pending=True)
    row = next(r for r in rep.sections["rows"] if r["ref"] == "assignments:1")
    assert row["status"] == "tampered:changed" and "SYN.B" in row["row"] and '"symbol":null' in row["was"]
    assert rep.fields["acceptable"] is False and rep.fields["refused_on_accept"] == "tampered"


async def test_config_show_pending_without_a_dsn_is_refused_and_not_audited(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    rep = await ops(pg, etcd, ctx(None)).config_show(pending=True)
    assert rep.outcome == "refused" and rep.fields["refused"] == "no_config_source" and pg.o.audit == []


async def test_accept_writes_the_accepted_row_with_its_audit_row(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    old = doc()
    seen_cache(pg, old)
    new = doc(assignments=[assignment_row(1), assignment_row(2, symbol="SYN.A", adapter="mock")])
    rep = await ops(pg, etcd, ctx(Source(new))).config_accept(new.doc_hash, ACTOR)
    assert rep.outcome == "applied", rep.fields
    latest = pg.a.accepted[-1]
    assert (latest["doc_hash"], latest["mode"], latest["accepted_at_ms"], latest["accepted_by"]) == (
        new.doc_hash, "manual", NOW, "alice")
    assert latest["audit_id"] == rep.audit_id and rep.fields["previous"] == old.doc_hash
    detail = json.loads(pg.o.audit[-1]["detail"])
    assert detail["pending"] == [r.audit_ref for r in new.rows if r.ref == "assignments:2"]
    assert detail["referenced_versions"] == ["ATR:1", "break_even_on_risk_close:1", "first_bar:1"]
    assert pg.sql.index(A.SQL_ACCEPTED_LOCK) < pg.sql.index(A.SQL_ACCEPTED_INSERT)
    # accepting it again is a no-op (audited)
    again = await ops(pg, etcd, ctx(Source(new))).config_accept(new.doc_hash, ACTOR)
    assert again.outcome == "noop" and len(pg.a.accepted) == 2
    assert audits(pg) == [("arm.config.accept", "applied"), ("arm.config.accept", "noop")]


async def test_an_accept_after_the_clock_stepped_back_still_becomes_the_latest(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    old = doc()
    seen_cache(pg, old)
    pg.a.accepted[-1]["accepted_at_ms"] = NOW + 5_000           # accepted while this host's clock was ahead
    new = doc(assignments=[assignment_row(1), assignment_row(2, symbol="SYN.A", adapter="mock")])
    rep = await ops(pg, etcd, ctx(Source(new))).config_accept(new.doc_hash, ACTOR)
    assert rep.outcome == "applied", rep.fields
    latest = pg._accepted_latest(())
    assert latest[1] == new.doc_hash and latest[4] == NOW + 5_001
    assert any("behind the latest accept" in w for w in rep.warnings)
    assert json.loads(pg.o.audit[-1]["detail"])["clock_behind_ms"] == 5_001


async def test_accept_is_refused_when_the_document_changed_after_review(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    reviewed = doc()
    now = doc(assignments=[assignment_row(1), assignment_row(2, symbol="SYN.B")])
    rep = await ops(pg, etcd, ctx(Source(now))).config_accept(reviewed.doc_hash, ACTOR)
    assert rep.outcome == "refused" and rep.fields["refused"] == "hash_changed" and now.doc_hash in rep.fields["detail"]
    assert pg.a.accepted == [] and audits(pg) == [("arm.config.accept", "refused")]
    with pytest.raises(OpsUsageError):
        await ops(pg, etcd, ctx(Source(now))).config_accept("ABC", ACTOR)
    with pytest.raises(OpsUsageError):
        await ops(pg, etcd, ctx(Source(now))).config_accept(now.doc_hash + "\n", ACTOR)


@pytest.mark.parametrize("case", ["tampered", "vanished", "schema_ahead", "invalid", "no_bars", "admission"])
async def test_accept_refuses_a_document_that_fails_a_check(pg: ArmOpsPg, etcd: FakeEtcd, case: str) -> None:
    base = doc(assignments=[assignment_row(1), assignment_row(2, symbol="SYN.A")])
    context = ctx()
    if case in ("tampered", "vanished"):
        seen_cache(pg, base)
        new = (doc(assignments=[assignment_row(1), assignment_row(2, symbol="SYN.B")]) if case == "tampered"
               else doc(assignments=[assignment_row(1)]))
        expected = "tampered"
    elif case == "schema_ahead":
        new, expected = parsed(raw_doc(schema_version=2, route_policy=[route_row(route="*")])), "schema_ahead"
    elif case == "invalid":
        new, expected = doc(assignments=[assignment_row(1, symbol="SYN.UNKNOWN")]), "invalid"
    elif case == "no_bars":
        new, expected = doc(assignments=[assignment_row(1, adapter="mock")]), "no_broker_bars"
        context = ctx(bar_capable=frozenset())
    else:
        new = doc(assignments=[assignment_row(1, symbol="SYN.A", adapter="mock"),
                               assignment_row(2, symbol="SYN.B", adapter="mock")])
        expected = "admission"
        context = ctx(limits=AdmissionLimits(decision_symbols_per_adapter=1))
    rep = await ops(pg, etcd, ctx(Source(new), **{k: getattr(context, k) for k in ("bar_capable", "limits")})
                    ).config_accept(new.doc_hash, ACTOR)
    assert rep.outcome == "refused" and rep.fields["refused"] == expected, rep.fields
    if case == "admission":
        assert "2 decision symbols > ceiling 1" in rep.fields["detail"]
    assert audits(pg) == [("arm.config.accept", "refused")]
    assert all(r["accepted_by"] != "alice" for r in pg.a.accepted)


@pytest.mark.parametrize("case", ["lacking", "no_field", "unreadable", "no_etcd", "etcd_down"])
async def test_accept_is_refused_while_a_fresh_node_lacks_a_referenced_version(pg: ArmOpsPg, case: str) -> None:
    etcd: FakeEtcd | None = FakeEtcd()
    assert etcd is not None
    if case == "lacking":
        node(etcd, registry=[t for t in TOKENS if t != "first_bar:1"])
    elif case == "no_field":
        node(etcd, drop_field=True)
    elif case == "unreadable":
        etcd.kv[HEALTH_PREFIX + "node-b"] = "{not json"
    elif case == "no_etcd":
        etcd = None
    else:
        etcd.fail_all = True
    new = doc()
    rep = await ops(pg, etcd, ctx(Source(new))).config_accept(new.doc_hash, ACTOR)
    assert rep.outcome == "refused" and rep.fields["refused"] == "registry", rep.fields
    if case == "lacking":
        assert "node-a lacks first_bar:1" in rep.fields["detail"] and "XTNL_MD_ENABLED" not in rep.fields["detail"]
    if case == "no_field":
        assert "(no registry published: XTNL_MD_ENABLED is off on that node)" in rep.fields["detail"]
    assert pg.a.accepted == []


async def test_a_bad_rest_setting_refuses_the_accept(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    c = ArmContext.from_env({"CTRADER_REST_BURST": "0"})
    assert c.settings_problems and "pepperstone" not in c.bars_per_min
    new = doc()
    rep = await ops(pg, etcd, ctx(Source(new), settings_problems=c.settings_problems)).config_accept(
        new.doc_hash, ACTOR)
    assert rep.outcome == "refused" and rep.fields["refused"] == "settings" and "CTRADER_REST" in rep.fields["detail"]
    assert pg.a.accepted == []


async def test_a_stale_node_record_is_not_checked(pg: ArmOpsPg) -> None:
    etcd = FakeEtcd()
    node(etcd, "node-old", registry=[], age_ms=60_000)
    new = doc()
    rep = await ops(pg, etcd, ctx(Source(new))).config_accept(new.doc_hash, ACTOR)
    assert rep.outcome == "applied" and any("no fresh node" in w for w in rep.warnings)


@pytest.mark.parametrize("error", [
    RuntimeError(f"could not connect to {DSN}"), ConfigSourceError("connect", "OSError"),
    SelfCheckError(["rls_not_forced:strategies"]), TimeoutError()])
async def test_a_failing_source_refuses_the_accept_without_any_secret(pg: ArmOpsPg, etcd: FakeEtcd,
                                                                      error: BaseException) -> None:
    rep = await ops(pg, etcd, ctx(Source(error))).config_accept("a" * 64, ACTOR)
    assert rep.outcome == "refused" and rep.fields["refused"] in ("source_unreadable", "self_check_failed")
    text = repr(rep) + json.dumps(pg.o.audit)
    assert not any(s in text for s in SECRETS)


async def test_a_source_document_problem_is_refused_by_its_code(pg: ArmOpsPg, etcd: FakeEtcd) -> None:
    problem = DocumentProblem("row_too_large", "strategies", 3)
    rep = await ops(pg, etcd, ctx(Source(problem))).config_accept("a" * 64, ACTOR)
    assert rep.fields["refused"] == "document_problem" and "row_too_large" in rep.fields["detail"]


# ============================================================================================== pure helpers
def test_referenced_tokens_and_no_bar_refusals() -> None:
    from tests.armconfig.docs import valid
    from src.armconfig.validate import KnownIds

    known = KnownIds(symbols=frozenset({"SYN.A"}), adapters=frozenset({"mock", "mock2"}),
                     accounts={"acct-1": "acct-1"}, routes=frozenset({"*"}))
    d = valid(raw_doc(strategies=[strategy_row(1)], assignments=[assignment_row(1, adapter="mock2"),
                                                                 assignment_row(2, adapter=None)],
                      route_policy=[route_row(route="*")]), known)
    assert referenced_tokens(d) == ("ATR:1", "break_even_on_risk_close:1", "first_bar:1")
    assert no_bar_refusals(d, frozenset({"mock"}), NOW) == ["assignments:1: adapter mock2 serves no broker bars"]
    assert no_bar_refusals(d, frozenset({"mock", "mock2"}), NOW) == []


def test_bars_cap_follows_the_adapters_rule() -> None:
    from src.adapters import ratelimit
    from src import config

    def cap(env: dict[str, str]) -> int:
        caps, problems = bars_caps(env)
        assert problems == ()
        return caps["stonex"]

    assert cap({}) == 75                                             # a quarter of 5/s per minute
    assert cap({"STONE_X_REST_BARS_PER_MIN": "20"}) == 20
    assert cap({"STONE_X_REST_BARS_PER_MIN": "1000"}) == 150         # clamped to half
    assert cap({"STONE_X_REST_BARS_PER_MIN": "zero"}) == 75
    assert cap({"STONE_X_REST_RATE_PER_S": "2"}) == 30
    # a rate the adapter's start-up refuses is refused here too (never a silent default)
    caps, problems = bars_caps({"STONE_X_REST_RATE_PER_S": "nan"})
    assert "stonex" not in caps and caps["pepperstone"] == 300 and len(problems) == 1
    assert problems[0].startswith("stonex: STONE_X_REST_RATE_PER_S")
    # src/config's copy of the rule gives the adapters' caps (src/ops may not import src.adapters)
    for rate in (0.01, 0.5, 1.0, 2.5, 5.0, 7.3, 20.0, 50.0):
        for configured in (None, 1, 20, 75, 149, 150, 151, 10_000):
            assert config.effective_bars_per_min(rate, configured) == ratelimit.effective_bars_per_min(
                rate, configured), (rate, configured)
        assert config.default_bars_per_min(rate) == ratelimit.default_bars_per_min(rate)
        assert config.max_bars_per_min(rate) == ratelimit.max_bars_per_min(rate)
    c = ArmContext.from_env({"XTNL_ARM_STOP_READS": "1", "CTRADER_REST_BARS_PER_MIN": "30"})
    # the bar-capable set follows the go-live gate table (since 2026-11-26 no S39 blocker is open: {stonex})
    assert c.stop_reads and c.bars_per_min["pepperstone"] == 30
    assert c.bar_capable == ops_bar_capable_adapters() == frozenset({"stonex"})
    assert ArmContext.from_env({}).config_source() is None           # no DSN: no source


def test_the_static_write_check_covers_the_new_sql(tmp_path: Path) -> None:
    """S24's §B3 scan: the controls and accept writes are legal only in src/ops (the real tree passes; the same file
    under another package is reported)."""
    source = (Path(A.__file__)).read_text(encoding="utf-8")
    (tmp_path / "src" / "riskmgr").mkdir(parents=True)
    (tmp_path / "src" / "riskmgr" / "arm_sql_copy.py").write_text(source, encoding="utf-8")
    (tmp_path / "src" / "ops").mkdir()
    (tmp_path / "src" / "ops" / "arm_sql.py").write_text(source, encoding="utf-8")
    found = sql_write_violations(tmp_path / "src")
    assert found and all(f.startswith("src/riskmgr/arm_sql_copy.py") for f in found)
    assert {f.rsplit(": ", 1)[1] for f in found} == {"writes arm_controls / arm_accepted"}


# ============================================================================================== CLI end to end
SECRET = "pa55-Wo3d-xyz"
ENV = {"ORDER_SERVER_DSN": f"postgresql://xtnl_operator:{SECRET}@10.0.1.10:5432/xtnl",
       "XTNL_ETCD_ENDPOINTS": "https://10.0.1.10:2379", "XTNL_ARM_CONFIG_DSN": DSN}


class Harness:
    def __init__(self, context: ArmContext | None = None) -> None:
        self.pg = ArmOpsPg()
        self.etcd = FakeEtcd()
        node(self.etcd)
        self.context = context

    async def connect(self, dsn: str) -> tuple[PgConnection, cli.Closer]:
        async def close() -> None:
            return None

        return self.pg, close

    def run(self, *argv: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        code = cli.main(list(argv), environ=ENV, connect=self.connect, etcd_factory=lambda _e: self.etcd, out=out,
                        err=err, arm_context=self.context)
        return code, out.getvalue(), err.getvalue()


ACT = ("--operator", "alice", "--reason", "INC-77 operator action")


def test_cli_controls_requests_and_views_end_to_end() -> None:
    h = Harness(ctx())
    code, out, err = h.run("arm", "pause", "--drain-notices", *ACT)
    assert code == cli.EXIT_OK and "outcome: applied" in out and "drain_notices: True" in out, err
    code, out, _ = h.run("arm", "resume", *ACT)
    assert code == cli.EXIT_OK and "outcome: applied" in out
    code, out, _ = h.run("md", "writer-reset", "--wait", "0", *ACT)
    assert code == cli.EXIT_REFUSED and "refused: no_arm_heartbeat" in out
    h.pg.heartbeat()
    code, out, _ = h.run("md", "writer-reset", "--wait", "0", *ACT)
    assert code == cli.EXIT_OK and "request: pending" in out
    code, out, _ = h.run("arm", "failed", "show")
    assert code == cli.EXIT_OK and "open: 0" in out
    code, _, err = h.run("arm", "disarm", "no-colon", *ACT)
    assert code == cli.EXIT_USAGE and "adapter_id:position_id" in err
    code, _, err = h.run("arm", "repin", "mock:P1", "--strategy", "be_a", *ACT)
    assert code == cli.EXIT_USAGE
    assert SECRET not in out + err


def test_cli_a_refused_answer_exits_three() -> None:
    pg_holder: list[ArmOpsPg] = []

    async def answer(_s: float) -> None:
        pg = pg_holder[0]
        pg.answer(pg.o.requests[-1]["request_id"], "refused:queue_full")

    h = Harness(ctx(sleep=answer))
    pg_holder.append(h.pg)
    h.pg.heartbeat()
    code, out, _ = h.run("md", "rehash", "--wait", "1", *ACT)
    assert code == cli.EXIT_REFUSED and "request: refused:queue_full" in out and "audit_id:" in out


def test_cli_config_never_prints_the_supabase_dsn() -> None:
    h = Harness(ctx(Source(RuntimeError(f"connect failed: {DSN}"))))
    code, out, err = h.run("arm", "config", "accept", "a" * 64, *ACT)
    assert code == cli.EXIT_REFUSED and "source_unreadable" in out
    code2, out2, err2 = h.run("arm", "config", "show", "--pending")
    assert code2 == cli.EXIT_REFUSED
    text = out + err + out2 + err2 + json.dumps(h.pg.o.audit)
    assert not any(s in text for s in SECRETS)


def test_every_documented_arm_command_parses() -> None:
    p = cli.build_parser()
    for argv in (["arm", "config", "show"], ["arm", "config", "show", "--pending"],
                 ["arm", "config", "accept", "a" * 64, *ACT], ["arm", "pause", *ACT],
                 ["arm", "pause", "--drain-notices", *ACT], ["arm", "resume", *ACT], ["arm", "notify-test", *ACT],
                 ["arm", "failed", "show", "--all"], ["arm", "failed", "clear", "ARM:arm", *ACT],
                 ["arm", "disarm", "mock:P1", *ACT], ["arm", "rearm", "mock:P1", "--wait", "0", *ACT],
                 ["arm", "repin", "mock:P1", "--strategy", "be_a", "--strategy-version", "2", *ACT],
                 ["md", "backfill", "mock:SYN.A:15m:BID:BROKER", "--from-ms", "0", "--to-ms", "900000", *ACT],
                 ["md", "rebuild", "0123456789abcdef", *ACT], ["md", "writer-reset", *ACT], ["md", "rehash", *ACT]):
        p.parse_args(argv)
    assert re.search(r"arm config accept <hash>", cli.__doc__ or "")


def test_the_arm_commands_send_nothing_to_a_broker() -> None:
    """Phase 1 boundary: the ARM commands only write operator rows and requests; no adapter, sender or order verb."""
    import src.ops.arm as arm_module

    text = Path(A.__file__).read_text(encoding="utf-8") + Path(arm_module.__file__).read_text(encoding="utf-8")
    assert not re.search(r"src\.adapters|outbound_intent|\b(place|modify|cancel)_order\b", text)
    assert inspect.iscoroutinefunction(ArmOps.config_accept)


def test_the_scope_usage_text_names_the_position_scope_kind() -> None:
    """S38c review: `arm failed clear` tells the operator how to name a POSITION scope."""
    from src.ops.arm import check_scope_target

    assert check_scope_target("POSITION:mock:P1") == "POSITION:mock:P1"
    with pytest.raises(OpsUsageError) as err:
        check_scope_target("BOGUS:x")
    assert "POSITION:adapter_id:position_id" in str(err.value)
