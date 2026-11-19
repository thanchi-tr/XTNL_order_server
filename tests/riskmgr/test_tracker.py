"""The arm-tracker task (plan S26; design §B4, §B7, §B11, §B12, §B13). Fakes from `tracker_fakes.py`; synthetic
instruments and levels only."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime
from zoneinfo import ZoneInfo

from src.db.arm_ports import EntryTime, HandleVersionRow
from src.dto import OrderStatus
from src.marketdata.types import LeaseClass, MdGeneration
from src.riskmgr import arming as A
from src.riskmgr.config import ArmSettings
from src.riskmgr.guards import FIRST_BAR_V1
from src.riskmgr.registry import ArmRegistry
from src.riskmgr.strategies import BREAK_EVEN_ON_RISK_CLOSE_V1
from src.riskmgr.sm11 import ArmState
from src.riskmgr.tracker import (
    SPAN_FIRST_BAR_STALE,
    AccountImage,
    AnswerStatus,
    NoticeKind,
    TrackerRequest,
    TrackerSettings,
    params_json,
)
from tests.riskmgr.toys import ToyGuard, ToyStrategy
from tests.riskmgr.tracker_fakes import (
    ACCOUNT,
    ADAPTER,
    DOC_HASH,
    T0,
    D,
    assignment,
    document,
    entry_order,
    image,
    Rig,
    position,
    rig,
    stop_child,
    strategy_entry,
)

M15_MS = 900_000
HOUR = 3_600_000


def run(coro: object) -> None:
    asyncio.run(coro)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------- arming
def test_arms_with_the_single_edge_pins_and_the_fill_bucket_cursor() -> None:
    r = rig(now=T0 + 7 * 60_000)
    r.versions.entries["P1"] = EntryTime(source="STREAM", broker_time_ms=T0 + 60_000, seq_id=1, origin="stream")
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    row = r.store.row()
    assert row.state == ArmState.ARMED.value and row.prior_state is None
    assert (row.edge_code, row.attribution_source) == ("default", A.ATTRIBUTION_SINGLE_EDGE)
    assert (row.strategy_code, row.strategy_version, row.strategy_kind) == ("be", 1, "break_even_on_risk_close")
    assert row.config_doc_hash == DOC_HASH and row.bar_spec == "15m"
    assert (row.entry_time_ms, row.entry_time_source) == (T0 + 60_000, "STREAM")
    assert row.last_decided_bar == T0 and row.decision_seq == 0 and not r.store.decisions
    assert "0.0001" not in row.indicator_specs_json            # templates only: no increment in PostgreSQL
    assert r.rec.pages == [] and r.tracker.counters.armed == 1


def _stop_entry_case(*, live: bool) -> None:
    """A stop-entry order placed hours before it fills: the entry time is the fill, never the placement."""
    placed, filled = T0 - 5 * HOUR, T0 + 2 * 60_000
    now = filled + 60_000 if live else filled + 2 * HOUR          # catch-up: armed long after the fill
    r = rig(now=now)
    source = "STREAM" if live else "BROKER"
    r.versions.entries["P1"] = EntryTime(source=source, broker_time_ms=filled, seq_id=7)
    pos = position("P1", opened_by="E1", updated_ms=placed)
    filled_order = entry_order("E1", status=OrderStatus.FILLED, remaining="0")
    if not live:
        r.snapshot.images = [image(pos, orders=[filled_order], fresh=False)]     # catch-up still running
        run(r.tracker.step())
        assert not r.store.rows
    r.snapshot.images = [image(pos, orders=[filled_order])]
    run(r.tracker.step())
    row = r.store.row()
    assert r.versions.entry_calls[-1] == ("P1", "E1")
    assert (row.entry_time_ms, row.entry_time_source) == (filled, source)
    if live:
        assert row.last_decided_bar == T0 and not r.store.decisions
    else:
        (span,) = r.store.decisions.values()
        assert span.outcome == "UNRESOLVED_RANGE" and span.reason == "armed_late" and not span.advice
        assert span.range_from_bucket == T0 and SPAN_FIRST_BAR_STALE in span.caveats
        assert row.last_decided_bar == span.bar_end_ms and row.decision_seq == 1 == span.decision_seq


def test_stop_entry_placed_hours_before_its_fill_live() -> None:
    _stop_entry_case(live=True)


def test_stop_entry_placed_hours_before_its_fill_through_the_catch_up() -> None:
    _stop_entry_case(live=False)


def test_late_arming_writes_one_armed_late_span_and_decides_at_most_the_current_bar() -> None:
    fill = T0 - 21 * 24 * HOUR + 60_000
    now = T0 + 3 * 60_000
    r = rig(now=now)
    r.versions.entries["P1"] = EntryTime(source="BROKER", broker_time_ms=fill, seq_id=1)
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    row = r.store.row()
    (span,) = r.store.decisions.values()
    assert span.reason == "armed_late" and span.category == "INFO"
    assert span.bar_end_ms == T0 - M15_MS           # the newest bar end whose decision_cap has passed
    assert row.last_decided_bar == span.bar_end_ms   # so only the current bar (ending T0) is left to decide


def test_unassigned_gets_one_notice_and_no_page() -> None:
    r = rig(doc=document(assignments=[assignment(adapter="other")]))
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    run(r.tracker.step())
    assert not r.store.rows and r.rec.pages == []
    (n,) = r.rec.notices
    assert n.kind is NoticeKind.POSITION_NOT_MANAGED and n.reason == A.NO_ASSIGNMENT
    assert r.tracker.unmanaged() == ((f"{ADAPTER}:P1", "UNASSIGNED", A.NO_ASSIGNMENT),)


def test_handle_ceiling_gives_unsupported_stop_read_budget() -> None:
    r = rig(settings=ArmSettings(stop_reads=True), tracker=TrackerSettings(armed_handle_ceiling=3))
    children = [stop_child("C1", "P1", units="500", level="1.19"), stop_child("C2", "P1", units="500", level="1.18"),
                stop_child("C3", "P2", units="500", level="1.19"), stop_child("C4", "P2", units="500", level="1.18")]
    r.snapshot.images = [image(position("P1", stop=None), orders=children)]
    run(r.tracker.step())
    assert r.store.row("P1").state == ArmState.ARMED.value
    r.snapshot.images = [image(position("P1", stop=None), position("P2", stop=None), orders=children)]
    run(r.tracker.step())
    run(r.tracker.step())
    assert "P2" not in {k.position_id for k in r.store.rows}
    assert (f"{ADAPTER}:P2", "UNSUPPORTED", A.STOP_READ_BUDGET) in r.tracker.unmanaged()
    assert len([p for p in r.rec.pages if "UNSUPPORTED" in p[1]]) == 1          # pages once
    assert [n.reason for n in r.rec.notices] == [A.STOP_READ_BUDGET]           # one-shot notice
    # without PR9 the ceiling does not apply
    r2 = rig(tracker=TrackerSettings(armed_handle_ceiling=3))
    r2.snapshot.images = [image(position("P1", stop=None), position("P2", stop=None), orders=children)]
    run(r2.tracker.step())
    assert {k.position_id for k in r2.store.rows} == {"P1", "P2"}


def test_demand_ceiling_and_missing_broker_bars_are_unsupported() -> None:
    r = rig()
    r.grid.sides = frozenset()
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    assert not r.store.rows and r.tracker.unmanaged()[0][2] == A.NO_BROKER_BARS
    r2 = rig()
    r2.snapshot.images = [image(position("P1"))]
    calls: list[str] = []

    async def refuse_third(spec_id: str, consumer: str, lease_class: LeaseClass) -> bool:
        calls.append(spec_id)
        return len(calls) < 3
    r2.md.demand = refuse_third  # type: ignore[method-assign]
    run(r2.tracker.step())
    assert not r2.store.rows and r2.tracker.unmanaged()[0][2] == A.DEMAND_CEILING
    assert r2.md.released == calls[:2]                       # the leases this arming added are given back


def test_second_enabled_edge_in_the_accepted_document_is_refused() -> None:
    r = rig(doc=document(assignments=[assignment(1), assignment(2, edge="momentum")]))
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    run(r.tracker.step())
    assert not r.store.rows
    edge_pages = [p for p in r.rec.pages if "second edge" in p[1]]
    assert len(edge_pages) == 1 and "momentum" in edge_pages[0][2]
    assert r.tracker.counters.config_refused == 1


def test_new_position_on_a_failed_scope_arms_into_failed() -> None:
    r = rig()
    r.failed.failed = True
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    row = r.store.row()
    assert (row.state, row.prior_state) == (ArmState.FAILED.value, ArmState.ARMED.value)


def test_assigned_position_without_a_stop_pages_after_five_minutes() -> None:
    r = rig()
    r.snapshot.images = [image(position("P1", stop=None))]
    run(r.tracker.step())
    r.clock.now += 299_000
    run(r.tracker.step())
    assert r.rec.pages == []
    r.clock.now += 2_000
    run(r.tracker.step())
    run(r.tracker.step())
    (page,) = r.rec.pages
    assert "not armed after 5 minutes" in page[1] and A.NO_STOP in page[2]


# ---------------------------------------------------------------------------------------------------- changes
def test_partial_fills_repin_entry_units_and_s0_keeping_bar_zero() -> None:
    r = rig()
    r.versions.entries["P1"] = EntryTime(source="STREAM", broker_time_ms=T0 + 60_000, seq_id=1)
    working = entry_order("E1", status=OrderStatus.PARTIALLY_FILLED)
    r.snapshot.images = [image(position("P1", opened_by="E1"), orders=[working])]
    run(r.tracker.step())
    before = r.store.row()
    r.snapshot.images = [image(position("P1", units="2000", entry="1.2010", stop="1.1950", opened_by="E1"),
                               orders=[working])]
    r.clock.now += 2_000
    run(r.tracker.step())
    row = r.store.row()
    assert (row.pinned_units, row.entry_level, row.s0_level) == (D(2000), D("1.2010"), D("1.1950"))
    assert row.entry_time_ms == before.entry_time_ms and row.last_decided_bar == before.last_decided_bar
    assert row.state == ArmState.ARMED.value and row.state_reason == "fill_progress"
    assert [n.kind for n in r.rec.notices] == [NoticeKind.REPINNED]
    # the entry order is done: a further units increase suspends (notice and page)
    done = entry_order("E1", status=OrderStatus.FILLED, remaining="0")
    r.snapshot.images = [image(position("P1", units="3000", entry="1.2010", stop="1.1950", opened_by="E1"),
                               orders=[done])]
    run(r.tracker.step())
    assert r.store.row().state == ArmState.SUSPENDED.value
    assert r.rec.notices[-1].kind is NoticeKind.SUSPENDED and any("SUSPENDED" in p[1] for p in r.rec.pages)
    r.clock.now += 6 * HOUR
    run(r.tracker.step())
    assert sum("still SUSPENDED" in p[1] for p in r.rec.pages) == 1


def test_closed_and_replaced_positions_end() -> None:
    r = rig()
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    r.snapshot.images = [image(position("P2"))]          # P1 gone, P2 same symbol and side: replaced
    run(r.tracker.step())
    p1 = r.store.row("P1")
    assert (p1.state, p1.state_reason, p1.ended_at_ms) == (ArmState.CLOSED.value, "replaced", r.clock.now)
    assert r.rec.notices[-1].kind is NoticeKind.REPLACED
    assert r.store.row("P2").state == ArmState.ARMED.value
    r.snapshot.images = [image(position("P2", status="CLOSED"))]
    run(r.tracker.step())
    assert (r.store.row("P2").state, r.store.row("P2").state_reason) == (ArmState.CLOSED.value, "position_closed")


def test_a_stale_image_never_arms_or_ends() -> None:
    r = rig()
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    r.snapshot.images = [image(fresh=False)]
    run(r.tracker.step())
    assert r.store.row().state == ArmState.ARMED.value
    assert r.snapshot.asked[-1] == ((ADAPTER, ACCOUNT),)


# ---------------------------------------------------------------------------------------------------- leases
def test_leases_renewed_every_15_s_and_reasserted_after_no_boot() -> None:
    r = rig()
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    armed = {sid for sid, cls in r.tracker.leases().items() if cls is LeaseClass.ARMED}
    # BROKER 15m bid, TICK 1m / 15m bid and ask, and the ATR (FirstBarGuard only): armed 5 minutes into its fill
    # bucket, no span was written and the cursor is the bucket's start, so bar 0 is still ahead and the guard's ATR
    # lease is kept until bar 0 is decided and its cap has passed (round 13; test_round13 covers the release)
    assert r.store.row().last_decided_bar == T0 and r.store.row().decision_seq == 0
    assert len(armed) == 6
    prewarm = {sid for sid, cls in r.tracker.leases().items() if cls is LeaseClass.PREWARM}
    assert len(prewarm) == 1        # the held symbol's 15m ask under the wildcard assignment
    renewals = r.tracker.counters.lease_renewals
    r.clock.now += 2_000
    run(r.tracker.step())
    assert r.tracker.counters.lease_renewals == renewals
    r.md.generation = MdGeneration(1, 2)          # NO_BOOT: the store re-bootstrapped
    r.md.demands.clear()
    r.clock.now += 2_000
    run(r.tracker.step())
    assert r.tracker.counters.lease_reasserts == 1
    assert armed <= {sid for sid, _ in r.md.demands}
    r.md.demands.clear()
    r.clock.now += 15_000
    run(r.tracker.step())
    assert armed <= {sid for sid, _ in r.md.demands}
    # the position ends: its leases are released
    r.snapshot.images = [image(position("P1", status="CLOSED"))]
    r.clock.now += 15_000
    run(r.tracker.step())
    assert armed <= set(r.md.released)


# ---------------------------------------------------------------------------------------------------- requests
def test_disarm_then_moot() -> None:
    r = rig()
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    r.tracker.submit(TrackerRequest.of(11, "arm_disarm", f"{ADAPTER}:P1"))
    run(r.tracker.step())
    row = r.store.row()
    assert (row.state, row.state_reason, row.ended_at_ms) == (ArmState.DISARMED.value, "local_disarm", r.clock.now)
    r.tracker.submit(TrackerRequest.of(12, "arm_disarm", f"{ADAPTER}:P1"))
    run(r.tracker.step())
    assert [(i, a.status, a.reason) for i, a in r.rec.answers] == [
        (11, AnswerStatus.APPLIED, ""), (12, AnswerStatus.MOOT, "not_armed")]
    run(r.tracker.step())
    assert r.store.row().state == ArmState.DISARMED.value          # never re-armed while its row exists
    # still OPEN at the broker: never re-planned, no ARMED demand, no "not armed after 5 minutes" page
    r.md.demands.clear()
    entry_calls = len(r.versions.entry_calls)
    for _ in range(4):
        r.clock.now += 120_000
        run(r.tracker.step())
    assert r.rec.pages == [] and not [sid for sid, cls in r.md.demands if cls is LeaseClass.ARMED]
    assert len(r.versions.entry_calls) == entry_calls and r.tracker.counters.armed == 1


def test_repin_migrates_state_and_retries_a_cas_miss() -> None:
    v2 = strategy_entry("be", 2, raw={"be_offset_r": "0.1", "guards": [{"kind": "first_bar", "version": 1,
                                                                         "params": {}}]})
    r = rig(doc=document(strategies=[strategy_entry(), v2]))
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    state_before = r.store.row().strategy_state_json
    r.store.miss_next = 1
    r.tracker.submit(TrackerRequest.of(21, "arm_repin", f"{ADAPTER}:P1", strategy_code="be", strategy_version=2))
    run(r.tracker.step())
    row = r.store.row()
    assert (row.strategy_version, row.state_reason) == (2, "repinned")
    assert row.strategy_params_json == params_json(v2.params) and row.strategy_state_json == state_before
    assert r.tracker.counters.cas_miss == 1
    assert r.rec.answers == [(21, r.rec.answers[0][1])] and r.rec.answers[0][1].status is AnswerStatus.APPLIED
    r.tracker.submit(TrackerRequest.of(22, "arm_repin", f"{ADAPTER}:P1", strategy_code="be", strategy_version=2))
    run(r.tracker.step())
    assert (r.rec.answers[-1][1].status, r.rec.answers[-1][1].reason) == (AnswerStatus.MOOT, "already_pinned")


def test_repin_refused_when_the_target_version_is_not_registered() -> None:
    toy_registry = ArmRegistry(strategies=[ToyStrategy()], guards=[ToyGuard()])
    toy = strategy_entry("toy", 1, registry=toy_registry, kind=ToyStrategy.KIND,
                         raw={"guards": [{"kind": "toy_calm", "version": 1, "params": {}}]})
    r = rig(doc=document(strategies=[strategy_entry(), toy]))
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    r.tracker.submit(TrackerRequest.of(31, "arm_repin", f"{ADAPTER}:P1", strategy_code="toy", strategy_version=1))
    run(r.tracker.step())
    assert (r.rec.answers[-1][1].status, r.rec.answers[-1][1].reason) == (AnswerStatus.REFUSED,
                                                                         "version_not_registered")
    assert r.store.row().strategy_code == "be"


def test_rearm_of_a_suspended_position() -> None:
    r = rig()
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    r.tracker.submit(TrackerRequest.of(40, "arm_rearm", f"{ADAPTER}:P1"))
    run(r.tracker.step())
    assert (r.rec.answers[-1][1].status, r.rec.answers[-1][1].reason) == (AnswerStatus.MOOT, "not_suspended")
    r.snapshot.images = [image(position("P1", units="2000"))]       # no live entry order: suspends
    run(r.tracker.step())
    assert r.store.row().state == ArmState.SUSPENDED.value
    r.clock.now += 3 * HOUR                                          # a long suspension
    r.tracker.submit(TrackerRequest.of(41, "arm_rearm", f"{ADAPTER}:P1"))
    run(r.tracker.step())
    row = r.store.row()
    assert r.rec.answers[-1] == (41, r.rec.answers[-1][1]) and r.rec.answers[-1][1].status is AnswerStatus.APPLIED
    assert (row.state, row.pinned_units, row.state_reason) == (ArmState.ARMED.value, D(2000), "local_rearm")
    spans = [d for d in r.store.decisions.values() if d.reason == "armed_late"]
    assert len(spans) == 1 and row.last_decided_bar == spans[0].bar_end_ms and row.decision_seq == 1
    r.clock.now += 2_000
    run(r.tracker.step())
    assert r.store.row().state == ArmState.ARMED.value               # the re-pinned units no longer suspend


def test_rearm_refused_while_the_arming_guard_fails() -> None:
    r = rig()
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    r.snapshot.images = [image(position("P1", units="2000", stop=None))]
    run(r.tracker.step())
    r.tracker.submit(TrackerRequest.of(50, "arm_rearm", f"{ADAPTER}:P1"))
    run(r.tracker.step())
    assert (r.rec.answers[-1][1].status, r.rec.answers[-1][1].reason) == (AnswerStatus.REFUSED, A.NO_STOP)
    assert r.store.row().state == ArmState.SUSPENDED.value


def test_s0_at_fill_from_a_stream_version() -> None:
    r = rig()
    r.versions.entries["P1"] = EntryTime(source="STREAM", broker_time_ms=T0 + 60_000, seq_id=1)
    r.versions.history["P1"] = [HandleVersionRow(handle_kind="POSITION_FIELD", handle_id="P1", seq_id=2,
                                                 broker_time_ms=T0 + 62_000, origin="stream", status="OPEN",
                                                 level=D("1.1900"))]
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    assert (r.store.row().s0_source, r.store.row().s0_level) == ("AT_FILL", D("1.1900"))


def test_nothing_in_pages_or_notices_carries_a_level_or_units() -> None:
    r = rig(settings=ArmSettings(stop_reads=True), tracker=TrackerSettings(armed_handle_ceiling=0))
    r.snapshot.images = [image(position("P1", units="1234", entry="1.2345", stop="1.2222"))]
    run(r.tracker.step())
    text = repr(r.rec.pages) + repr(r.rec.notices) + repr(replace(r.rec.notices[0]))
    for value in ("1234", "1.2345", "1.2222", "0.0001"):
        assert value not in text


def test_request_and_answer_forms() -> None:
    import pytest

    from src.riskmgr.tracker import RequestAnswer, refused
    with pytest.raises(ValueError):
        TrackerRequest.of(1, "arm_disarm", "no-separator")
    with pytest.raises(ValueError):
        TrackerRequest.of(1, "md_backfill", f"{ADAPTER}:P1")
    with pytest.raises(ValueError):
        refused("price 1.23")
    with pytest.raises(ValueError):
        RequestAnswer(AnswerStatus.APPLIED, "x")
    r = rig(tracker=TrackerSettings(max_requests=1))
    assert r.tracker.submit(TrackerRequest.of(1, "arm_disarm", f"{ADAPTER}:P1"))
    assert not r.tracker.submit(TrackerRequest.of(2, "arm_disarm", f"{ADAPTER}:P1"))


def test_run_stamps_and_survives_a_failing_pass() -> None:
    r = rig(tracker=TrackerSettings(snapshot_every_s=0.01))
    stamps: list[int] = []

    class Stamp:
        def stamp(self) -> None:
            stamps.append(1)
            if len(stamps) >= 4:
                stop.set()

        def set_exempt(self, reason: str | None) -> None:
            pass

    async def broken(accounts: object) -> list[object]:
        raise RuntimeError("snapshot down")
    r.snapshot.read = broken  # type: ignore[method-assign,assignment]

    async def main() -> None:
        await r.tracker.run(stop, Stamp())

    stop = asyncio.Event()
    asyncio.run(main())
    assert r.tracker.counters.errors >= 2 and len(stamps) >= 4


def test_takeover_reasserts_leases_from_stored_pins_even_with_a_stale_image() -> None:
    r = rig()
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    armed = {sid for sid, cls in r.tracker.leases().items() if cls is LeaseClass.ARMED}
    successor = rig(now=r.clock.now + 30_000)          # a new term: a new tracker over the same rows
    successor.store.rows = dict(r.store.rows)
    successor.snapshot.images = [image(position("P1"), fresh=False)]
    run(successor.tracker.step())
    assert armed <= {sid for sid, cls in successor.md.demands if cls is LeaseClass.ARMED}


# ---------------------------------------------------------------------------------------------------- review fixes
ROLL = int(datetime(2023, 11, 14, 17, tzinfo=ZoneInfo("America/New_York")).timestamp()) * 1000


def _armed(**kw: object) -> Rig:
    r = rig()
    r.versions.entries["P1"] = EntryTime(source="STREAM", broker_time_ms=T0 + 60_000, seq_id=1)
    r.snapshot.images = [image(position("P1", **kw))]  # type: ignore[arg-type]
    run(r.tracker.step())
    return r


def test_the_final_fill_of_a_partial_entry_is_fill_progress() -> None:
    r = rig()
    r.versions.entries["P1"] = EntryTime(source="STREAM", broker_time_ms=T0 + 60_000, seq_id=1)
    working = entry_order("E1", status=OrderStatus.PARTIALLY_FILLED)            # 1000 of 2000
    r.snapshot.images = [image(position("P1", opened_by="E1"), orders=[working])]
    run(r.tracker.step())
    done = entry_order("E1", status=OrderStatus.FILLED, remaining="0")           # FILLED and the units in one image
    r.snapshot.images = [image(position("P1", units="2000", entry="1.2010", opened_by="E1"), orders=[done])]
    r.clock.now += 2_000
    run(r.tracker.step())
    row = r.store.row()
    assert (row.state, row.state_reason, row.pinned_units) == (ArmState.ARMED.value, "fill_progress", D(2000))
    assert [n.kind for n in r.rec.notices] == [NoticeKind.REPINNED]
    # a last partial fill and the CANCELLED remainder together: still fill progress, never beyond its fills
    cancelled = entry_order("E1", status=OrderStatus.CANCELLED, units="3000", remaining="500")
    r.snapshot.images = [image(position("P1", units="2500", entry="1.2010", opened_by="E1"), orders=[cancelled])]
    run(r.tracker.step())
    assert (r.store.row().state, r.store.row().pinned_units) == (ArmState.ARMED.value, D(2500))
    r.snapshot.images = [image(position("P1", units="2600", entry="1.2010", opened_by="E1"), orders=[cancelled])]
    run(r.tracker.step())
    assert r.store.row().state == ArmState.SUSPENDED.value


def test_pepperstone_partial_fill_suspends_as_a_known_limitation() -> None:
    """cTrader positions carry no entry order id, so a partial fill cannot be matched to its order (plan S26
    known limitation): it suspends, with a notice and a page, rather than being re-pinned on a guess."""
    r = rig()
    working = entry_order("E1", status=OrderStatus.PARTIALLY_FILLED)
    r.snapshot.images = [image(position("P1"), orders=[working])]
    run(r.tracker.step())
    r.snapshot.images = [image(position("P1", units="2000"), orders=[working])]
    run(r.tracker.step())
    assert (r.store.row().state, r.store.row().state_reason) == (ArmState.SUSPENDED.value, "units_increase")


def test_a_decrease_lowers_the_pin_so_a_later_scale_in_suspends() -> None:
    r = _armed()
    r.snapshot.images = [image(position("P1", units="500"))]               # 1000 -> 500: partly closed
    run(r.tracker.step())
    row = r.store.row()
    assert (row.state, row.pinned_units, row.state_reason) == (ArmState.ARMED.value, D(500), "units_decrease")
    assert row.entry_level == D("1.2000") and r.rec.notices == []
    r.snapshot.images = [image(position("P1", units="800", entry="1.2050"))]   # 300 added: below the old pin
    run(r.tracker.step())
    assert (r.store.row().state, r.store.row().state_reason) == (ArmState.SUSPENDED.value, "units_increase")


def test_a_decrease_with_an_entry_change_re_pins_the_entry() -> None:
    r = _armed()
    r.snapshot.images = [image(position("P1", units="500", entry="1.2050", updated_ms=ROLL + 3 * HOUR))]
    run(r.tracker.step())
    row = r.store.row()
    assert (row.pinned_units, row.entry_level, row.state_reason) == (D(500), D("1.2050"), "entry_corrected")
    assert [n.kind for n in r.rec.notices] == [NoticeKind.ENTRY_CORRECTED]


def test_entry_correction_re_pins_and_notes() -> None:
    r = _armed()
    r.snapshot.images = [image(position("P1", entry="1.2005", updated_ms=ROLL + 3 * HOUR))]
    run(r.tracker.step())
    run(r.tracker.step())
    row = r.store.row()
    assert (row.entry_level, row.state_reason, row.state) == (D("1.2005"), "entry_corrected", ArmState.ARMED.value)
    assert [n.kind for n in r.rec.notices] == [NoticeKind.ENTRY_CORRECTED]


def test_roll_adjust_keeps_e_notes_once_and_survives_a_takeover() -> None:
    r = _armed()
    r.snapshot.images = [image(position("P1", entry="1.2005", updated_ms=ROLL + 60_000))]
    run(r.tracker.step())
    run(r.tracker.step())
    row = r.store.row()
    assert row.entry_level == D("1.2000") and row.state_reason == A.roll_note(row.ref, D("1.2005"))
    assert [n.kind for n in r.rec.notices] == [NoticeKind.ROLL_ADJUST]
    # a decrease after the roll keeps the note (E is still the original)
    r.snapshot.images = [image(position("P1", units="600", entry="1.2005", updated_ms=ROLL + HOUR))]
    run(r.tracker.step())
    assert (r.store.row().pinned_units, r.store.row().state_reason) == (D(600), row.state_reason)
    # a takeover: a new tracker, the position's newest update well after the roll
    successor = rig(now=r.clock.now + 3 * HOUR)
    successor.store.rows = dict(r.store.rows)
    successor.snapshot.images = [image(position("P1", units="600", entry="1.2005", updated_ms=ROLL + 3 * HOUR))]
    run(successor.tracker.step())
    after = successor.store.row()
    assert after.entry_level == D("1.2000") and after.state_reason == row.state_reason
    assert successor.rec.notices == []
    # the next day's roll adjusts again: noted again
    successor.snapshot.images = [image(position("P1", units="600", entry="1.2009",
                                               updated_ms=ROLL + 24 * HOUR + 30_000))]
    run(successor.tracker.step())
    assert [n.kind for n in successor.rec.notices] == [NoticeKind.ROLL_ADJUST]
    assert successor.store.row().entry_level == D("1.2000")


def test_a_position_gone_from_a_fresh_image_ends_and_is_never_re_armed() -> None:
    r = _armed()
    r.snapshot.images = [image()]
    run(r.tracker.step())
    row = r.store.row()
    assert (row.state, row.state_reason) == (ArmState.CLOSED.value, "position_gone") and r.rec.notices == []
    r.snapshot.images = [image(position("P1"))]                        # it shows up OPEN again
    for _ in range(4):
        r.clock.now += 120_000
        run(r.tracker.step())
    assert r.store.row().state == ArmState.CLOSED.value and r.tracker.counters.armed == 1
    assert r.rec.pages == []
    # a fresh tracker (takeover) reads the ended row once and leaves it too
    successor = rig(now=r.clock.now)
    successor.store.rows = dict(r.store.rows)
    successor.snapshot.images = [image(position("P1"))]
    run(successor.tracker.step())
    run(successor.tracker.step())
    assert successor.tracker.counters.armed == 0 and not successor.versions.entry_calls


def test_a_pinned_version_this_node_cannot_load_suspends() -> None:
    r = _armed()
    key = r.store.row().key
    r.store.rows[key] = replace(r.store.row(), strategy_impl_version=99)
    run(r.tracker.step())
    row = r.store.row()
    assert (row.state, row.state_reason) == (ArmState.SUSPENDED.value, "pinned_version_missing")
    assert r.rec.notices[-1].kind is NoticeKind.SUSPENDED


def test_estimated_entry_time_upgrades_until_bar_zero_cap() -> None:
    r = rig()                                                   # no fill time yet: ESTIMATED at first sight
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    row = r.store.row()
    assert (row.entry_time_source, row.entry_time_ms, row.last_decided_bar) == ("ESTIMATED", r.clock.now, T0)
    fill = T0 - 3 * 60_000                                      # the fill was in the previous bucket
    r.versions.entries["P1"] = EntryTime(source="BROKER", broker_time_ms=fill, seq_id=4)
    r.clock.now += 2_000
    run(r.tracker.step())
    row = r.store.row()
    assert (row.entry_time_source, row.entry_time_ms) == ("BROKER", fill)
    assert row.last_decided_bar == T0 - M15_MS and row.decision_seq == 0      # no decision yet: bar 0 moved
    calls = len(r.versions.entry_calls)
    run(r.tracker.step())
    assert len(r.versions.entry_calls) == calls                 # no more reads once exact
    # after bar 0's decision_cap there is no upgrade
    late = rig()
    late.snapshot.images = [image(position("P1"))]
    run(late.tracker.step())
    late.versions.entries["P1"] = EntryTime(source="STREAM", broker_time_ms=T0 + 60_000, seq_id=1)
    late.clock.now += 2 * HOUR
    run(late.tracker.step())
    assert late.store.row().entry_time_source == "ESTIMATED"


def test_estimated_entry_time_is_never_the_placement() -> None:
    placed = T0 - 5 * HOUR
    r = rig()
    r.snapshot.images = [image(position("P1", updated_ms=placed))]
    run(r.tracker.step())
    row = r.store.row()
    assert (row.entry_time_source, row.entry_time_ms) == ("ESTIMATED", r.clock.now)
    assert not r.store.decisions                                 # no armed_late span from the placement time


def test_repin_to_another_kind_is_refused() -> None:
    both = ArmRegistry(strategies=[BREAK_EVEN_ON_RISK_CLOSE_V1, ToyStrategy()], guards=[FIRST_BAR_V1, ToyGuard()])
    toy = strategy_entry("toy", 1, registry=both, kind=ToyStrategy.KIND,
                         raw={"guards": [{"kind": "toy_calm", "version": 1, "params": {}}]})
    r = rig(doc=document(strategies=[strategy_entry(registry=both), toy]), registry=both)
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    before = r.store.row()
    r.tracker.submit(TrackerRequest.of(32, "arm_repin", f"{ADAPTER}:P1", strategy_code="toy", strategy_version=1))
    run(r.tracker.step())
    assert (r.rec.answers[-1][1].status, r.rec.answers[-1][1].reason) == (AnswerStatus.REFUSED, "kind_changed")
    assert r.store.row().strategy_state_json == before.strategy_state_json and r.store.row().strategy_code == "be"


def test_rearm_demands_its_leases_before_the_cas() -> None:
    r = _armed()
    armed = {sid for sid, cls in r.tracker.leases().items() if cls is LeaseClass.ARMED}
    r.snapshot.images = [image(position("P1", units="2000"))]
    r.clock.now += 2_000
    run(r.tracker.step())
    assert r.store.row().state == ArmState.SUSPENDED.value
    r.md.refuse = set(armed)                                     # over the demand ceiling
    r.tracker.submit(TrackerRequest.of(60, "arm_rearm", f"{ADAPTER}:P1"))
    r.clock.now += 2_000
    run(r.tracker.step())
    assert (r.rec.answers[-1][1].status, r.rec.answers[-1][1].reason) == (AnswerStatus.REFUSED, A.DEMAND_CEILING)
    assert r.store.row().state == ArmState.SUSPENDED.value
    r.md.refuse = set()
    r.md.demands.clear()
    r.tracker.submit(TrackerRequest.of(61, "arm_rearm", f"{ADAPTER}:P1"))
    r.clock.now += 2_000                                         # the 15 s renewal is not due: the rearm demands
    run(r.tracker.step())
    assert r.rec.answers[-1][1].status is AnswerStatus.APPLIED
    assert armed <= {sid for sid, cls in r.md.demands if cls is LeaseClass.ARMED}
    assert r.tracker.counters.lease_renewals == 1


def test_handle_ceiling_counts_accounts_whose_image_is_stale() -> None:
    other = "acct2"
    r = rig(settings=ArmSettings(stop_reads=True), tracker=TrackerSettings(armed_handle_ceiling=3))
    kids = [replace(stop_child("C1", "P9", units="500", level="1.19"), account_id=other),
            replace(stop_child("C2", "P9", units="500", level="1.18"), account_id=other)]
    p9 = replace(position("P9", stop=None), account_id=other)
    acct2 = AccountImage(adapter_id=ADAPTER, account_id=other, fresh=True, positions=(p9,), orders=tuple(kids),
                         instruments=image().instruments)
    r.snapshot.images = [acct2]
    run(r.tracker.step())
    assert r.store.row("P9").state == ArmState.ARMED.value
    successor = rig(now=r.clock.now + 30_000, settings=ArmSettings(stop_reads=True),
                    tracker=TrackerSettings(armed_handle_ceiling=3))
    successor.store.rows = dict(r.store.rows)
    mine = [stop_child("C3", "P1", units="500", level="1.19"), stop_child("C4", "P1", units="500", level="1.18")]
    successor.snapshot.images = [replace(acct2, fresh=False), image(position("P1", stop=None), orders=mine)]
    run(successor.tracker.step())
    assert "P1" not in {k.position_id for k in successor.store.rows}
    assert (f"{ADAPTER}:P1", "UNSUPPORTED", A.STOP_READ_BUDGET) in successor.tracker.unmanaged()
