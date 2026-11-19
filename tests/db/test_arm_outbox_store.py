"""S29: the ARM outbox store (`src/db/arm_outbox_store.py`) and the housekeeping expiry (`src/db/lifecycle.py` step 5).

Static: every statement names `kind = 'arm'`, every write is fenced, the insert has no RETURNING (`xtnl_arm` has INSERT
only) and nothing writes `+escalated`. Bodies: a finished body keeps the notice id, key, decision ids and outcome codes
only (R11-M14), a ping body is kept; the rescheduled channel form always passes the 0016 CHECK."""
from __future__ import annotations

import json
import re
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from src.db import arm_outbox_store as A
from src.db import lifecycle as LC
from src.db.epoch import EngineDb, Epoch
from src.db.fence import FENCE, EpochMismatch
from src.riskmgr.render import ItemBody, NoticeBody, encode_body

from tests.db.arm_outbox_emulator import ArmOutboxEmulator, arm_check
from tests.db.conftest import FakeConn

T = 1_700_001_000_000
GRACE = LC.LifecycleConfig().arm_expiry_grace_ms
STATEMENTS = {n: getattr(A, n) for n in dir(A) if n.startswith("SQL_")}
WRITES = {n: s for n, s in STATEMENTS.items() if s.split()[0] in ("INSERT", "UPDATE")}
FINISHED_KEYS = {"v", "status", "notice_id", "route", "bar_spec", "event", "bar_end_ms", "part", "item_key",
                 "decision_ids", "outcomes"}


def notice_body(**kw: object) -> NoticeBody:
    item = ItemBody(decision_id=7, decision_bar_end_ms=T, adapter_id="mock", position_id="P1", symbol="SYN.A",
                    side="BUY", event="POST", bar_end_ms=T, outcome="ADJUST", category="ACTIONABLE",
                    proposed_stop=Decimal("1.23500"), current_risk_r=Decimal("0.50"), entry=Decimal("1.23456"),
                    s0=Decimal("1.21456"), trigger_input=f"mock:SYN.A:15m:BID:BROKER|{T}|1:abcd")
    base: dict[str, object] = dict(notice_id=42, route="main", event="POST", bar_spec="15m", bar_end_ms=T, part=2,
                                   created_ms=T + 70_000, expires_at_ms=T + 970_000, items=(item,))
    base.update(kw)
    return NoticeBody(**base)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------- static
@pytest.mark.parametrize("name", sorted(STATEMENTS))
def test_every_statement_names_the_arm_kind(name: str) -> None:
    sql = STATEMENTS[name]
    assert "kind = 'arm'" in sql or "'arm' WHERE" in sql or "'arm', $2" in sql, name
    assert "+escalated" not in sql and "escalat" not in sql.lower()


@pytest.mark.parametrize("name", sorted(WRITES))
def test_every_write_is_fenced_with_contiguous_parameters(name: str) -> None:
    sql = WRITES[name]
    assert FENCE in sql
    ph = {int(n) for n in re.findall(r"\$(\d+)", sql)}
    assert ph == set(range(1, max(ph) + 1))


def test_the_insert_has_no_returning_and_writes_info_rows_with_an_expiry() -> None:
    sql = A.SQL_ARM_OUTBOX_INSERT
    assert "RETURNING" not in sql and "ON CONFLICT" not in sql
    assert "'INFO'" in sql and "expires_at_ms" in sql and "next_attempt_at_ms" in sql
    assert "CRITICAL" not in sql


def test_the_relay_statements_never_set_the_oversee_escalation_and_finish_replaces_the_body() -> None:
    assert "body = $8::text" in A.SQL_ARM_OUTBOX_FINISH and "superseded_by = $9::bigint" in A.SQL_ARM_OUTBOX_FINISH
    assert "sent_at_ms IS NULL" in A.SQL_ARM_OUTBOX_FINISH and "sent_at_ms IS NULL" in A.SQL_ARM_OUTBOX_RESCHEDULE
    assert "body = u.body" in A.SQL_ARM_OUTBOX_EXPIRE


# ---------------------------------------------------------------------------------------------------- bodies
def test_a_finished_body_keeps_ids_key_and_outcome_codes_only() -> None:
    text = encode_body(notice_body())
    for status in ("sent", "expired", "superseded", "suppressed"):
        out = json.loads(A.finished_body(text, status))
        assert set(out) == FINISHED_KEYS and out["status"] == status
        assert (out["notice_id"], out["route"], out["event"], out["part"], out["decision_ids"], out["outcomes"]) == (
            42, "main", "POST", 2, [7], ["ADJUST"])
        finished = A.finished_body(text, status)
        for level in ("1.23500", "0.50", "1.23456", "1.21456", "abcd"):   # order state and hashes are dropped
            assert level not in finished
    with pytest.raises(ValueError):
        A.finished_body(text, "delivered")


def test_a_ping_body_is_kept_and_odd_bodies_never_leak_values() -> None:
    assert A.finished_body("scope series x; reason invalid_bar", "sent") == "scope series x; reason invalid_bar"
    odd = json.loads(A.finished_body('{"notice_id": 3, "items": [{"decision_id": 1, "outcome": "1.2345 x"}],'
                                     ' "close": "1.2345"}', "expired"))
    assert odd == {"v": 1, "status": "expired", "notice_id": 3, "decision_ids": [1], "outcomes": []}
    assert json.loads(A.finished_body("[1, 2]", "sent")) == {"v": 1, "status": "sent", "decision_ids": [],
                                                             "outcomes": []}


def test_the_notice_key_reads_both_the_full_and_the_finished_body() -> None:
    text = encode_body(notice_body(item_key="mock:P1:stop:arm:2", part=0))
    key = A.notice_key(text)
    assert key == A.NoticeKey(notice_id=42, route="main", bar_spec="15m", event="POST", bar_end_ms=T, part=0,
                              item_key="mock:P1:stop:arm:2")
    assert A.notice_key(A.finished_body(text, "sent")) == key
    assert A.notice_key("ARM FAILED ping p-1") is None and A.notice_key('{"notice_id": 1}') is None


@pytest.mark.parametrize("remaining, failures, encoded", [
    (["arm_email"], {}, ["arm_email"]),
    (["arm_email", "arm_push"], {"arm_push": 3}, ["arm_email", "arm_push:3"]),
    (["arm_push"], {"arm_push": 240, "arm_email": 2}, ["arm_push:240"]),
])
def test_rescheduled_channels_always_pass_the_check(remaining: list[str], failures: dict[str, int],
                                                    encoded: list[str]) -> None:
    out = A.encode_arm_channels(remaining, failures)
    assert out == encoded and arm_check("arm", out)


@pytest.mark.parametrize("remaining", [[], ["email"], ["arm_email", "+escalated"], ["mobile_push"]])
def test_a_reschedule_that_would_break_the_check_is_refused(remaining: list[str]) -> None:
    with pytest.raises(ValueError):
        A.encode_arm_channels(remaining, {})


# ---------------------------------------------------------------------------------------------------- reads
async def test_pending_and_oldest_read_only_arm_rows() -> None:
    emu = ArmOutboxEmulator(epoch=7)
    store = A.PgArmOutboxStore(EngineDb(emu, lambda: Epoch("c", 7)))
    emu.add_outbox_row(kind="info", channels=["email"], created=10)
    emu.add_outbox_row(kind="arm", channels=["arm_email:2", "arm_push"], created=20, body="x", expires=99)
    emu.add_outbox_row(kind="arm", channels=["arm_push"], created=30, severity="CRITICAL", body="ping")
    pending = await store.pending(10)
    assert [r.outbox_id for r in pending] == [2, 3]
    assert pending[0].channels == ("arm_email", "arm_push") and pending[0].failures == (("arm_email", 2),)
    assert not pending[0].is_ping and pending[1].is_ping and pending[0].expires_at_ms == 99
    assert await store.oldest_undelivered_ms() == 20
    assert [r.outbox_id for r in await store.recent(25, 10)] == [3]


# ---------------------------------------------------------------------------------------------------- housekeeping
def ms(y: int, m: int, d: int) -> int:
    return int(datetime(y, m, d, tzinfo=UTC).timestamp() * 1000)


async def test_housekeeping_finishes_expired_arm_rows_with_the_body_scrubbed(conn: FakeConn, db: EngineDb) -> None:
    now = ms(2026, 11, 13)
    text = encode_body(notice_body())
    conn.returns_all(A.SQL_ARM_OUTBOX_EXPIRED, [(5, 100, text), (6, 101, "ping text")])
    conn.status_for(A.SQL_ARM_OUTBOX_EXPIRE, "UPDATE 2")
    rep = LC.LifecycleReport()
    await LC.PgPartitionMaintenance(db, LC.LifecycleConfig(arm_expiry_batch=10))._arm_expiry(now, rep)
    assert rep.arm_expired == 2 and not rep.errors
    (args,) = conn.args_of(A.SQL_ARM_OUTBOX_EXPIRE)
    assert args[1:4] == (now, [5, 6], [100, 101])
    assert json.loads(args[4][0])["status"] == "expired" and "1.23500" not in args[4][0]
    assert args[4][1] == "ping text"
    assert conn.args_of(A.SQL_ARM_OUTBOX_EXPIRED)[0] == (now - GRACE, 10)    # the relay gets the first hour


async def test_housekeeping_expiry_errors_are_reported_and_a_lost_fence_raises(conn: FakeConn, db: EngineDb) -> None:
    conn.fail(A.SQL_ARM_OUTBOX_EXPIRED, RuntimeError("password=hunter2 host=db.internal"))
    rep = LC.LifecycleReport()
    await LC.PgPartitionMaintenance(db)._arm_expiry(ms(2026, 11, 13), rep)
    assert rep.errors == ["arm notice expiry: RuntimeError"]      # no driver text
    conn.returns_all(A.SQL_ARM_OUTBOX_EXPIRED, [(5, 100, "x")])
    conn.stored_epoch = 99
    with pytest.raises(EpochMismatch):
        await LC.PgPartitionMaintenance(db)._arm_expiry(ms(2026, 11, 13), LC.LifecycleReport())


async def test_run_once_runs_the_expiry_step(conn: FakeConn, db: EngineDb) -> None:
    conn.returns(LC.SQL_ENSURE_PARTITION, *[("p",)] * 21)
    rep = await LC.PgPartitionMaintenance(db).run_once(ms(2026, 11, 13))
    assert rep.arm_expired == 0 and not rep.errors
    assert conn.args_of(A.SQL_ARM_OUTBOX_EXPIRED) == [(ms(2026, 11, 13) - GRACE, 500)]


async def test_housekeeping_expiry_on_the_shared_table_leaves_pings_and_oversee_rows() -> None:
    emu = ArmOutboxEmulator(epoch=7)
    db = EngineDb(emu, lambda: Epoch("c", 7))
    text = encode_body(notice_body())
    emu.add_outbox_row(kind="arm", channels=["arm_email"], created=T, body=text, expires=T + 10)
    emu.add_outbox_row(kind="arm", channels=["arm_email"], created=T, body=text, expires=T + 10_000_000)
    emu.add_outbox_row(kind="arm", channels=["arm_push"], created=T, severity="CRITICAL", body="ping")
    emu.add_outbox_row(kind="info", channels=["email"], created=T)
    job = LC.PgPartitionMaintenance(db)
    rep = LC.LifecycleReport()
    now = T + 10 + GRACE
    await job._arm_expiry(now, rep)
    assert rep.arm_expired == 1 and not rep.errors
    rows = emu.t.outbox
    assert rows[0]["sent_at"] == now and rows[0]["last_error"] == "expired"
    assert set(json.loads(rows[0]["body"])) == FINISHED_KEYS
    assert all(r["sent_at"] is None for r in rows[1:])


async def test_housekeeping_leaves_a_freshly_expired_row_to_the_relay_and_pages_an_adjust_it_finishes() -> None:
    emu = ArmOutboxEmulator(epoch=7)
    db = EngineDb(emu, lambda: Epoch("c", 7))
    post = encode_body(notice_body())                                                    # POST, ADJUST: pages
    pre = encode_body(notice_body(notice_id=43, event="PRE", items=(replace(
        notice_body().items[0], event="PRE", trigger_input=None),)))                    # PRE expiry: a metric only
    emu.add_outbox_row(kind="arm", channels=["arm_email"], created=T, body=post, expires=T + 10)
    emu.add_outbox_row(kind="arm", channels=["arm_email"], created=T, body=pre, expires=T + 10)
    job = LC.PgPartitionMaintenance(db)
    rep = LC.LifecycleReport()
    await job._arm_expiry(T + 10 + GRACE - 1, rep)          # inside the grace: the relay's turn, nothing finished
    assert rep.arm_expired == 0 and all(r["sent_at"] is None for r in emu.t.outbox)
    await job._arm_expiry(T + 10 + GRACE, rep)
    assert rep.arm_expired == 2 and rep.arm_expired_adjust == [42]
    runner = LC.LifecycleRunner()
    (page,) = runner.pages(rep)
    assert page[1] == "ARM notices expired undelivered" and "#42" in page[2] and "#43" not in page[2]
    assert "1.23500" not in page[2]                                                     # ids only
    assert runner.pages(rep) == []                                                      # paged once
    assert runner.pages(LC.LifecycleReport()) == []


@pytest.mark.parametrize("event, item_event, outcome, pages", [
    ("POST", "POST", "ADJUST", True), ("LATE", "LATE", "ADJUST", True), ("CORRECTED", "LATE", "ADJUST", True),
    ("PRE", "PRE", "ADJUST", False), ("POST", "POST", "HOLD_REASON", False)])
def test_the_stored_body_expiry_page_rule_matches_the_relay(event: str, item_event: str, outcome: str,
                                                            pages: bool) -> None:
    from src.notifications.arm_relay import expired_adjust
    b = notice_body(event=event, items=(replace(notice_body().items[0], event=item_event, outcome=outcome),))
    assert A.pages_when_expired(encode_body(b)) is expired_adjust(b) is pages
    assert A.pages_when_expired("ARM FAILED ping p-1") is False
