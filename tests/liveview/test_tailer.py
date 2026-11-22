"""The tailer (docs/design/live-view.md s4.3, s6 "tailer"): exactly once by database-issued ids, no clock in any
correctness rule, watched sets and silent first readings."""
from __future__ import annotations

import logging

import pytest

from src.liveview.tailer import IdCursor, Tailer, hold_ms
from tests.liveview.fakedb import FakeDB, entity

DAY0 = 1_795_000_000_000 - 3_600_000


async def started(db: FakeDB, **kw: int) -> Tailer:
    t = Tailer(poll_ms=1000, **kw)
    await t.start(db, db.now, DAY0)
    return t


async def poll(t: Tailer, db: FakeDB, advance: int = 1000) -> dict[str, list[dict]]:
    db.now += advance
    out = await t.poll(db, db.now)
    return out.tail


async def collect(t: Tailer, db: FakeDB, polls: int = 1, advance: int = 1000) -> list[int]:
    ids: list[int] = []
    for _ in range(polls):
        tail = await poll(t, db, advance)
        ids += [r["log_id"] for r in tail["entity"]]
    return ids


async def test_1200_rows_in_one_second_are_all_emitted_once() -> None:
    db = FakeDB()
    t = await started(db)
    for i in range(1200):
        entity(db, key=f"stonex:{i}")
    ids = await collect(t, db, polls=3)
    assert sorted(ids) == list(range(1, 1201))


async def test_an_older_seq_with_a_higher_id_committed_45s_later_is_emitted() -> None:
    db = FakeDB()
    t = await started(db)
    slow = db.reserve("entity")                         # the order row: id drawn first, seq_id older
    entity(db, etype="POSITION", key="stonex:9")        # a fresher position row commits first
    assert await collect(t, db) == [2]
    for _ in range(44):
        await collect(t, db)
    entity(db, key="stonex:9", at=db.now - 45_000, k=slow)
    assert await collect(t, db) == [slow]


@pytest.mark.parametrize("skew", [-120_000, 120_000])
async def test_an_engine_clock_off_by_two_minutes_loses_nothing(skew: int) -> None:
    db = FakeDB()
    t = await started(db)
    for i in range(20):
        entity(db, key=f"stonex:{i}", at=db.now + skew)
    assert sorted(await collect(t, db)) == list(range(1, 21))


async def test_a_five_minute_outage_loses_nothing() -> None:
    db = FakeDB()
    t = await started(db)
    entity(db)
    db.now += 300_000
    for i in range(5):
        entity(db, key=f"stonex:{i}")
    assert sorted(await collect(t, db)) == list(range(1, 7))


async def test_a_90s_commit_is_found_by_the_late_recheck_and_marked_late() -> None:
    db = FakeDB()
    t = await started(db)
    slow = db.reserve("entity")
    entity(db)
    await collect(t, db)
    late_found = []
    for _ in range(89):
        db.now += 1000
        out = await t.poll(db, db.now)
        late_found += out.late.get("entity", [])
    entity(db, k=slow)
    for _ in range(70):
        db.now += 1000
        out = await t.poll(db, db.now)
        assert slow not in [r["log_id"] for r in out.tail["entity"]]
        late_found += out.late.get("entity", [])
    assert [r["log_id"] for r in late_found] == [slow]


async def test_an_empty_source_and_old_rows_replay_nothing() -> None:
    db = FakeDB()
    for i in range(5):
        entity(db, key=f"stonex:{i}", at=db.now - 90 * 86_400_000)
    t = await started(db)
    assert await collect(t, db, polls=5) == []
    empty = FakeDB()
    t2 = await started(empty)
    assert await collect(t2, empty, polls=3) == []


async def test_a_quiet_hour_repeats_nothing() -> None:
    db = FakeDB()
    db.add("halt", level="NONE", effective_level="NONE", hard_latch=False, at_ms=db.now)
    db.add("terms", cluster_id="c1", epoch_revision=7, node="pc1", started_at=db.now, reached_leader_at=db.now,
           ended_at=None, end_reason=None)
    db.add("operator", at_ms=db.now, command="halt.clear", operator="june", outcome="applied")
    t = await started(db)
    for _ in range(600):
        tail = await poll(t, db)
        assert not any(tail.values())


async def test_a_rolled_back_id_expires_and_the_count_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    db = FakeDB()
    t = await started(db)
    db.reserve("entity")                    # rolled back: never commits
    entity(db)
    caplog.set_level(logging.INFO)
    await collect(t, db)
    for _ in range(50):
        await collect(t, db)
    assert 1 in t.cursors["entity"].expired
    assert any(getattr(r, "event", None) == "expired" for r in caplog.records)


async def test_a_backlog_over_the_page_cap_is_paged_without_dropping() -> None:
    db = FakeDB()
    t = await started(db)
    for i in range(6200):
        entity(db, key=f"stonex:{i}")
    db.now += 1000
    out = await t.poll(db, db.now)
    assert len(out.tail["entity"]) == 5000 and out.backlog == 1200
    ids = [r["log_id"] for r in out.tail["entity"]] + await collect(t, db)
    assert sorted(ids) == list(range(1, 6201))


async def test_restart_backfill_emits_nothing_twice() -> None:
    db = FakeDB()
    for i in range(10):
        entity(db, key=f"stonex:{i}")
    t = await started(db)
    bf_ids = [r["log_id"] for r in (await Tailer(poll_ms=1000).start(db, db.now, DAY0)).rows["entity"]]
    assert bf_ids == list(range(1, 11))
    assert await collect(t, db, polls=3) == []


async def test_expired_is_capped_oldest_first() -> None:
    cur = IdCursor()
    cur.start(0, set(), 0)
    cur.accept([20_001], 0)                   # ids 1..20 000 are missing
    assert cur.expire(10**9, hold_ms(1000)) == 20_000
    assert len(cur.expired) == 10_000 and min(cur.expired) == 10_001 and cur.evicted == 10_000


async def test_watched_dlq_moved_to_history_and_purged_alert_leave_silently() -> None:
    db = FakeDB()
    d = db.add("dlq", origin_dlq_id=None, event_type="ORDER", entity_id="1", adapter_id="stonex",
               dlq_reason="illegal_transition", retry_count=0, status="QUEUED", parked_at_ms=db.now,
               updated_at_ms=db.now)
    a = db.add("alerts", created_at_ms=db.now, severity="SOFT", kind="episode", delivery="queued", escalated=False,
               attempts=0, sent_at_ms=None)
    t = await started(db)
    assert d["dlq_id"] in t.dlq.rows and a["outbox_id"] in t.alerts.rows
    del db.rows["dlq"][d["dlq_id"]]
    del db.rows["alerts"][a["outbox_id"]]
    db.now += 1000
    out = await t.poll(db, db.now)
    assert out.dlq_changed == [] and out.alerts_changed == []
    assert not t.dlq.rows and not t.alerts.rows


async def test_an_arm_outbox_id_is_seen_but_never_watched() -> None:
    db = FakeDB()
    t = await started(db)
    db.add("alerts", created_at_ms=db.now, kind="arm", delivery=None, sent_at_ms=None)
    tail = await poll(t, db)
    assert [r["kind"] for r in tail["alerts"]] == ["arm"]
    assert not t.alerts.rows


async def test_an_alert_queued_before_a_60h_shutdown_is_followed() -> None:
    db = FakeDB()
    a = db.add("alerts", created_at_ms=db.now - 60 * 3_600_000, kind="episode", delivery="queued", escalated=False,
               attempts=0, sent_at_ms=None)
    t = await started(db)
    assert a["outbox_id"] in t.alerts.rows
    db.rows["alerts"][a["outbox_id"]].update(delivery="delivered", attempts=1, sent_at_ms=db.now)
    db.now += 1000
    out = await t.poll(db, db.now)
    assert [n["delivery"] for _o, n in out.alerts_changed] == ["delivered"]


async def test_dlq_quarantined_is_not_final_and_a_release_is_reported() -> None:
    db = FakeDB()
    d = db.add("dlq", origin_dlq_id=None, event_type="ORDER", entity_id="1", adapter_id="stonex",
               dlq_reason="illegal_transition", retry_count=5, status="QUARANTINED", parked_at_ms=db.now,
               updated_at_ms=db.now)
    t = await started(db)
    for _ in range(5):
        await poll(t, db)
    db.rows["dlq"][d["dlq_id"]]["status"] = "QUEUED"
    db.now += 1000
    out = await t.poll(db, db.now)
    assert [(o["status"], n["status"]) for o, n in out.dlq_changed] == [("QUARANTINED", "QUEUED")]


async def test_engine_runs_are_discovered_and_the_seed_is_silent() -> None:
    db = FakeDB()
    db.runs.append({"run_id": "r1", "node_id": "pc1", "started_at_ms": db.now - 5000, "last_seen_ms": db.now,
                    "stopped_at_ms": None, "end_code": None, "suspended_gaps": 0})
    t = await started(db)
    db.now += 1000
    out = await t.poll(db, db.now)
    assert out.runs_new == []
    db.runs.append({"run_id": "r2", "node_id": "pc1", "started_at_ms": db.now, "last_seen_ms": db.now,
                    "stopped_at_ms": None, "end_code": None, "suspended_gaps": 0})
    db.now += 1000
    out = await t.poll(db, db.now)
    assert [r["run_id"] for r in out.runs_new] == ["r2"]


async def test_auth_two_generations_give_one_row_and_a_new_failure_is_new() -> None:
    db = FakeDB()
    t = await started(db)
    db.auth = [{"adapter_id": "stonex", "state": "AUTH_FAILED", "failed_at_ms": 200, "rearmed_at_ms": None}]
    db.now += 1000
    out = await t.poll(db, db.now)
    assert len(out.auth_changed) == 1
    for _ in range(3):
        db.now += 1000
        assert (await t.poll(db, db.now)).auth_changed == []
    db.auth = [{"adapter_id": "stonex", "state": "AUTH_FAILED", "failed_at_ms": 300, "rearmed_at_ms": None}]
    db.now += 1000
    assert len((await t.poll(db, db.now)).auth_changed) == 1


async def test_cash_flow_and_instrument_ids_never_enter_pending() -> None:
    db = FakeDB()
    t = await started(db)
    entity(db)
    entity(db, etype="CASH_FLOW", key="stonex:tx")
    entity(db, etype="INSTRUMENT", key="stonex:EURUSD")
    entity(db)
    await collect(t, db)
    assert not t.cursors["entity"].pending
