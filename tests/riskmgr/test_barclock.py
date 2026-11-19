"""BarClock (S27): due events, one transaction per (event, bar end), replay through the S22 planner, controls,
pause, clock steps and read sessions. The evaluation is a fake (`FakeEval`); no S21 module is imported."""
from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from src.db.arm_ports import ArmPositionRow, PositionKey
from src.db.arm_store import POSITION_MUTABLE_COLUMNS, SQL_POSITION_CAS, PgArmStateStore
from src.db.epoch import EngineDb, Epoch
from src.db.fence import EpochMismatch
from src.marketdata.timing import COLD_SKEW, TIMING_TABLE, decision_cap_ms, pre_fire_ms, pre_valid_until_ms
from src.marketdata.types import M15
from src.riskmgr.barclock import (
    ARMED_AFTER_PRE_READ,
    CONTROLS_UNREADABLE,
    DELAYED,
    PRE_MARKET_CLOSED,
    PREPARE_TIMEOUT,
    SM11_REFUSED,
    BarClock,
    BarClockSettings,
    EvalRequest,
    Evaluated,
    NotReady,
    Prepared,
    Verdict,
)
from src.riskmgr.config import ArmSettings
from src.riskmgr.sm11 import Sm11Event
from src.riskmgr.types import ArmEvent
from tests.riskmgr.barclock_fakes import (
    ADAPTER,
    SETTLE,
    T0,
    T1,
    T2,
    T3,
    TF,
    BcStore,
    FakeGrid,
    arm_row,
    bc_rig,
    decided,
    handles_for,
)
from tests.db.arm_emulator import ArmEmulator

ROW = TIMING_TABLE[M15]
NOT_FINAL = "broker_bar_not_final"


def row_of(store: BcStore, pid: str = "P1") -> ArmPositionRow:
    return store.rows[PositionKey(adapter_id=ADAPTER, position_id=pid)]


# ---------------------------------------------------------------------------------------------------- POST on time
async def test_post_on_time_is_one_fenced_transaction_and_moves_the_cursor() -> None:
    r = bc_rig(now=T1 + SETTLE + 1_000)
    await r.step()
    assert r.eval.events() == [(ArmEvent.POST, T1)]
    [d] = r.store.rows_of()
    assert (d.event, d.bar_end_ms, d.decision_seq, d.advice, d.edge_code) == ("POST", T1, 1, True, "default")
    assert DELAYED not in d.caveats
    row = row_of(r.store)
    assert (row.last_decided_bar, row.decision_seq, row.state) == (T1, 1, "ADVICE_OUTSTANDING")
    assert row.last_decided_hash is not None
    assert len(r.store.transactions) == 1
    await r.step(T1 + SETTLE + 2_000)
    assert len(r.store.rows_of()) == 1                        # decided once


async def test_a_replayed_post_gives_no_epoch_mismatch_and_no_duplicate() -> None:
    a = bc_rig(now=T1 - 60_000)
    b = bc_rig(now=T1 - 60_000, store=a.store, rows_ttl_ms=10**9)
    await b.step()                                            # b caches the row (cursor T0)
    await a.step(T1 + SETTLE + 1_000)                         # a decides T1
    await b.step(T1 + SETTLE + 1_000)                         # b replays the POST of T1 from its stale row
    assert b.clock_.counters.already_decided == 1
    assert b.clock_.counters.errors == 0
    assert [(d.event, d.bar_end_ms) for d in a.store.rows_of()] == [("POST", T1)]
    await b.step(T1 + SETTLE + 2_000)                         # the row is re-read: nothing more to do
    assert len(a.store.rows_of()) == 1
    assert b.eval.events() == [(ArmEvent.POST, T1)]


async def test_a_transient_error_in_one_savepoint_leaves_the_others_committed() -> None:
    r = bc_rig(arm_row("P1"), arm_row("P2"), arm_row("P3"), handles=handles_for("P1", "P2", "P3"))
    r.store.raise_once.add("P2")
    await r.step()
    assert len(r.store.transactions) == 1                     # one transaction for (POST, T1)
    assert [len(r.store.rows_of(p)) for p in ("P1", "P2", "P3")] == [1, 0, 1]
    assert row_of(r.store, "P2").last_decided_bar == T0
    assert r.clock_.counters.write_errors == 1
    await r.step(T1 + SETTLE + 2_000)
    assert [len(r.store.rows_of(p)) for p in ("P1", "P2", "P3")] == [1, 1, 1]
    assert row_of(r.store, "P2").last_decided_bar == T1


async def test_a_failed_pending_cas_never_aborts_the_other_decisions_of_its_transaction() -> None:
    """S27 review: P2's `pending_event` CAS (no decision row) sits in the same (POST, T1) transaction as P1's and
    P3's final POSTs; its error (a statement timeout) must roll back only its own savepoint. The real store runs over
    the emulator, which aborts a transaction after an error outside a savepoint as PostgreSQL does."""
    emu = ArmEmulator(epoch=7)
    store = PgArmStateStore(EngineDb(emu, lambda: Epoch("c", 7)))
    async with store.transaction() as tx:
        for pid in ("P1", "P2", "P3"):
            await tx.insert_position(arm_row(pid))
    at = 1 + len(POSITION_MUTABLE_COLUMNS) + 1                # $1 epoch, the mutable columns, adapter_id, position_id

    def timeout_p2(sql: str, args: tuple[object, ...]) -> BaseException | None:
        return TimeoutError("statement timeout") if sql == SQL_POSITION_CAS and args[at] == "P2" else None

    emu.fail_when = timeout_p2
    r = bc_rig(store=store, rule=lambda req: NotReady(NOT_FINAL) if req.row.key.position_id == "P2" else None)
    await r.step()
    assert sorted((d["position_id"], d["event"], d["bar_end_ms"]) for d in emu.t.decisions.values()) == [
        ("P1", "POST", T1), ("P3", "POST", T1)]
    rows = {k[1]: v for k, v in emu.t.positions.items()}
    assert (rows["P1"]["last_decided_bar"], rows["P3"]["last_decided_bar"]) == (T1, T1)
    assert (rows["P2"]["pending_event"], rows["P2"]["row_version"]) == (None, 0)   # its CAS alone rolled back
    assert r.clock_.counters.write_errors == 1 and r.clock_.counters.decisions == 2


async def test_a_lost_fence_propagates_without_an_injected_predicate() -> None:
    r = bc_rig()
    r.store.raise_with["P1"] = EpochMismatch("engine_epoch moved")
    with pytest.raises(EpochMismatch):
        await r.step()


async def test_a_refused_sm11_step_writes_nothing_and_the_bar_stays_pending() -> None:
    r = bc_rig(arm_row(state="STRATEGY_COMPLETE"))          # FINAL_ADJUST has no edge from STRATEGY_COMPLETE
    await r.step()
    assert r.store.rows_of() == [] and r.clock_.counters.sm11_refused == 1
    row = row_of(r.store)
    assert (row.state, row.last_decided_bar, row.pending_event, row.pending_bar_end_ms) == (
        "STRATEGY_COMPLETE", T0, "POST", T1)
    await r.step(T2 + SETTLE + 1)                             # T1's cap passed: spanned with that reason
    span = r.store.rows_of()[0]
    assert (span.outcome, span.reason, span.bar_end_ms) == ("UNRESOLVED_RANGE", SM11_REFUSED, T1)
    assert row_of(r.store).state == "STRATEGY_COMPLETE"


async def test_unreadable_controls_keep_the_bars_earlier_pending_reason() -> None:
    r = bc_rig(rule=lambda req: NotReady(NOT_FINAL) if req.event is ArmEvent.POST and req.bar_end_ms == T1 else None)
    await r.step(T1 + SETTLE + 1_000)                         # pending: broker_bar_not_final
    r.controls.fail = True
    await r.step(T1 + SETTLE + 60_000)
    await r.step(T2 + SETTLE + 1)                             # T1's cap passed while unreadable
    [span] = r.store.rows_of()
    assert (span.outcome, span.reason, span.bar_end_ms) == ("UNRESOLVED_RANGE", NOT_FINAL, T1)


async def test_prepares_run_together_and_the_stamp_is_refreshed_between_them() -> None:
    r = bc_rig(arm_row("P1"), arm_row("P2"), arm_row("P3"), handles=handles_for("P1", "P2", "P3"))
    stamps: list[int] = []
    await r.clock_.step(stamp=lambda: stamps.append(1))
    assert r.eval.max_inflight == 3
    assert len(stamps) >= 4                                   # after the group of prepares and after each job
    assert [len(r.store.rows_of(p)) for p in ("P1", "P2", "P3")] == [1, 1, 1]


async def test_only_postgres_waits_are_exempt_and_a_hung_prepare_times_out() -> None:
    """S36b review: the task is exempt from its stamp age only while it awaits its own PostgreSQL calls (controls,
    rows, transactions; operations.md §3); a prepare (market-data reads) runs unexempt and a hung one becomes
    `prepare_timeout` for that ask, so the pass ends and the bar stays pending."""
    r = bc_rig(arm_row("P1"), arm_row("P2"), handles=handles_for("P1", "P2"))
    r.clock_._cfg = BarClockSettings(rows_ttl_ms=0, prepare_timeout_ms=20)   # noqa: SLF001
    state: list[str | None] = [None]
    log: list[str | None] = []
    seen_in_prepare: list[str | None] = []
    hang = asyncio.Event()
    real = r.eval.prepare

    async def prepare(request: EvalRequest) -> Prepared | NotReady:
        seen_in_prepare.append(state[0])
        if request.row.key.position_id == "P1":
            await hang.wait()                                 # a hung market-data read
        return await real(request)

    def exempt(reason: str | None) -> None:
        state[0] = reason
        log.append(reason)

    r.eval.prepare = prepare                                  # type: ignore[method-assign]
    await r.clock_.step(exempt=exempt)
    assert seen_in_prepare == [None, None] and state[0] is None
    assert log and log[0::2] == ["postgres"] * (len(log) // 2) and log[1::2] == [None] * (len(log) // 2)
    assert [len(r.store.rows_of(p)) for p in ("P1", "P2")] == [0, 1]
    assert row_of(r.store, "P1").pending_event == "POST"
    assert r.clock_._reason[(arm_row("P1").ref, T1)] == PREPARE_TIMEOUT   # noqa: SLF001 (its expiry reason)


async def test_the_commit_hook_runs_only_for_a_committed_row() -> None:
    """S36b review: an evaluation's `committed` hook (its memory) runs on the loop after the commit, never for a row
    whose write failed."""
    ran: list[str] = []

    def rule(req: EvalRequest) -> Evaluated:
        pid = req.row.key.position_id
        return replace(decided(events=(Sm11Event.FINAL_ADJUST,)), committed=lambda: ran.append(pid))

    r = bc_rig(arm_row("P1"), arm_row("P2"), handles=handles_for("P1", "P2"), rule=rule)
    r.store.raise_once.add("P2")
    await r.step()
    assert ran == ["P1"] and r.clock_.counters.write_errors == 1


async def test_closed_positions_are_forgotten() -> None:
    r = bc_rig(stop_reads=True)
    ref = arm_row().ref
    bc = r.clock_
    r.reads.note_stale(ref, ArmEvent.POST, T1)
    bc._reopen_last[ref] = T1
    bc._span_paged.add(ref)
    bc._grace_paged.add((ADAPTER, arm_row().symbol, "15m"))
    key = PositionKey(adapter_id=ADAPTER, position_id="P1")
    r.store.rows[key] = replace(r.store.rows[key], state="CLOSED")
    await r.step()
    assert r.reads.stale_counts() == {}
    assert (bc._reopen_last, bc._span_paged, bc._grace_paged) == ({}, set(), set())


# ---------------------------------------------------------------------------------------------------- late broker bars
async def test_broker_bar_final_at_three_minutes_gives_a_delayed_post() -> None:
    final_at = T1 + 180_000

    def rule(req):  # type: ignore[no-untyped-def]
        if req.event is ArmEvent.POST and req.now_ms < final_at:
            return NotReady(NOT_FINAL)
        return None

    r = bc_rig(rule=rule)
    await r.step(T1 + SETTLE + 1_000)
    row = row_of(r.store)
    assert (row.pending_event, row.pending_bar_end_ms, row.last_decided_bar) == ("POST", T1, T0)
    assert r.store.rows_of() == []                            # a POST-time wait writes no decision row
    await r.step(T1 + 61_000)                                 # past post_grace: one page per series episode
    await r.step(T1 + 90_000)
    grace = [p for p in r.rec.pages if "post_grace" in p[1]]
    assert len(grace) == 1
    await r.step(final_at)
    [d] = r.store.rows_of()
    assert (d.event, d.bar_end_ms) == ("POST", T1) and DELAYED in d.caveats
    assert r.eval.requests[-1].delayed and r.eval.requests[-1].event is ArmEvent.POST
    row = row_of(r.store)
    assert (row.pending_event, row.pending_bar_end_ms, row.last_decided_bar) == (None, None, T1)


async def test_a_bar_never_final_is_unresolved_before_the_next_post() -> None:
    def rule(req):  # type: ignore[no-untyped-def]
        return NotReady(NOT_FINAL) if req.bar_end_ms == T1 and req.event is ArmEvent.POST else None

    r = bc_rig(rule=rule)
    await r.step(T1 + SETTLE + 1_000)
    assert decision_cap_ms(T1, M15, COLD_SKEW) == T2 + SETTLE
    await r.step(T2 + SETTLE + 1_000)                         # T1's cap passed: span, then T2 in the same pass
    span, post = r.store.rows_of()
    assert (span.outcome, span.reason, span.bar_end_ms, span.decision_seq, span.advice) == (
        "UNRESOLVED_RANGE", NOT_FINAL, T1, 1, False)
    assert span.category == "FAULT" and span.range_from_bucket == T0
    assert (post.event, post.bar_end_ms, post.decision_seq) == ("POST", T2, 2)
    assert row_of(r.store).last_decided_bar == T2
    assert any("unresolved" in p[1] for p in r.rec.pages)


# ---------------------------------------------------------------------------------------------------- PRE
@pytest.mark.parametrize("offset", [31, 45])
async def test_pre_fires_at_its_offset_and_never_moves_the_cursor(offset: int) -> None:
    r = bc_rig(arm_row(cursor=T1), settings=ArmSettings(pre_offset_s=offset))
    fire = pre_fire_ms(T2, COLD_SKEW, offset)
    until = pre_valid_until_ms(fire, T2, ROW, COLD_SKEW)
    await r.step(fire - 100)
    assert r.eval.requests == []
    assert r.clock_.sleep_ms(fire - 100) == 100              # the loop wakes at the fire time, not up to 250 ms late
    await r.step(fire + 100)                                  # a pass never lands exactly on the fire millisecond
    req = r.eval.requests[-1]
    assert (req.event, req.bar_end_ms, req.fire_ms, req.valid_until_ms) == (ArmEvent.PRE, T2, fire, until)
    assert not req.previous_post_pending
    [d] = r.store.rows_of()
    assert (d.event, d.advice, d.decision_seq) == ("PRE", False, 1)
    row = row_of(r.store)
    assert (row.last_decided_bar, row.state) == (T1, "TRACKING")
    await r.step(min(fire + 1_000, until))
    assert len(r.store.rows_of()) == 1


def test_a_pre_offset_outside_the_row_is_refused_and_30_s_is_allowed() -> None:
    """S38c: valid until fire + 10 s, so 30 s (the D1 lower bound) has a 10 s window; 29 s is outside the row."""
    with pytest.raises(ValueError, match="pre_offset"):
        bc_rig(settings=ArmSettings(pre_offset_s=29))
    assert isinstance(bc_rig(settings=ArmSettings(pre_offset_s=30)).clock_, BarClock)
    r = bc_rig(arm_row(cursor=T1))
    assert r.clock_.sleep_ms(T1) == 250 and isinstance(r.clock_, BarClock)


async def test_pre_not_decided_by_its_valid_until_is_not_sent() -> None:
    def rule(req):  # type: ignore[no-untyped-def]
        return NotReady("pre_quote_unavailable") if req.event is ArmEvent.PRE else None

    r = bc_rig(arm_row(cursor=T1), rule=rule)
    fire = pre_fire_ms(T2, COLD_SKEW, 40)
    await r.step(fire)
    await r.step(pre_valid_until_ms(fire, T2, ROW, COLD_SKEW) + 1)
    assert r.store.rows_of() == []
    assert r.clock_.counters.pre_dropped == {"pre_quote_unavailable": 1}


async def test_pre_says_the_previous_post_is_pending_and_none_in_a_closed_period() -> None:
    r = bc_rig(rule=lambda req: NotReady(NOT_FINAL) if req.event is ArmEvent.POST else None)
    await r.step(pre_fire_ms(T2, COLD_SKEW, 40))
    pre = [q for q in r.eval.requests if q.event is ArmEvent.PRE]
    assert pre and pre[-1].previous_post_pending
    closed = bc_rig(arm_row(cursor=T1), grid=FakeGrid(closed=frozenset({T2})))
    await closed.step(pre_fire_ms(T2, COLD_SKEW, 40))
    assert closed.eval.requests == [] and closed.clock_.counters.pre_dropped == {PRE_MARKET_CLOSED: 1}


# ---------------------------------------------------------------------------------------------------- controls, pause
async def test_unreadable_controls_hold_the_event_and_end_it_by_its_window() -> None:
    r = bc_rig(arm_row(cursor=T1))
    r.controls.fail = True
    fire = pre_fire_ms(T2, COLD_SKEW, 40)
    await r.step(fire)
    await r.step(pre_valid_until_ms(fire, T2, ROW, COLD_SKEW) + 1)
    assert r.clock_.counters.pre_dropped == {CONTROLS_UNREADABLE: 1}
    await r.step(T2 + SETTLE + 1_000)
    assert r.eval.requests == [] and r.store.rows_of() == []
    r.controls.fail = False
    await r.step(T2 + SETTLE + 1_250)                         # readable again: the held POST goes ahead
    assert [(d.event, d.bar_end_ms) for d in r.store.rows_of()] == [("POST", T2)]


async def test_controls_unreadable_until_the_cap_end_the_post_as_unresolved() -> None:
    r = bc_rig(arm_row(cursor=T1))
    r.controls.fail = True
    await r.step(T2 + SETTLE + 1_000)
    await r.step(T3 + SETTLE + 1)                             # T2's cap passed while unreadable
    [span] = r.store.rows_of()
    assert (span.outcome, span.reason, span.bar_end_ms) == ("UNRESOLVED_RANGE", CONTROLS_UNREADABLE, T2)
    assert r.eval.requests == []


async def test_paused_means_no_decisions_and_pages_once_after_six_hours() -> None:
    r = bc_rig(now=T1 + SETTLE + 1_000)
    r.controls.paused, r.controls.paused_at_ms = True, T1 - 6 * 3_600_000
    await r.step()
    await r.step(T1 + SETTLE + 2_000)
    assert r.eval.requests == [] and r.store.rows_of() == []
    assert len([p for p in r.rec.pages if "paused" in p[1]]) == 1
    await r.step(T2 + SETTLE + 1_000)                         # still paused past T1's cap: cursor stays put
    assert r.store.rows_of() == [] and row_of(r.store).last_decided_bar == T0
    r.controls.paused = False
    await r.step(T2 + SETTLE + 2_000)                         # resume: T1 spanned `paused` (INFO), T2 decided
    span, post = r.store.rows_of()
    assert (span.reason, span.category, post.event, post.bar_end_ms) == ("paused", "INFO", "POST", T2)
    assert not [p for p in r.rec.pages if "unresolved" in p[1]]


async def test_paused_queues_no_stop_reads() -> None:
    r = bc_rig(stop_reads=True, now=T1)
    r.controls.paused, r.controls.paused_at_ms = True, T1 - 60_000
    await r.step(T1)                                          # the POST anchor time of T1
    await r.step(T1 + SETTLE + 1_000)
    assert r.port is not None and r.port.calls == [] and r.reads.counters.reads == 0


async def test_the_config_hash_pause_means_no_decisions_like_the_operator_pause() -> None:
    pause: list[str | None] = ["config_hash"]
    r = bc_rig(now=T1 + SETTLE + 1_000, config_pause=lambda: pause[0])
    await r.step()
    await r.step(T2 + SETTLE + 1_000)                         # still paused past T1's cap: cursor stays put
    assert r.eval.requests == [] and r.store.rows_of() == [] and row_of(r.store).last_decided_bar == T0
    assert r.clock_.counters.config_paused_passes == 2 and not [p for p in r.rec.pages if "paused" in p[1]]
    r.controls.fail = True                                    # unreadable controls do not lift the config pause
    await r.step(T2 + SETTLE + 1_250)
    assert r.store.rows_of() == []
    r.controls.fail = False
    pause[0] = None                                           # the accepted hash installed: T1 spanned, T2 decided
    await r.step(T2 + SETTLE + 2_000)
    span, post = r.store.rows_of()
    assert (span.reason, span.category, post.event, post.bar_end_ms) == ("paused", "INFO", "POST", T2)


async def test_a_config_pause_port_that_raises_counts_as_paused() -> None:
    def broken() -> str | None:
        raise RuntimeError("port down")

    r = bc_rig(now=T1 + SETTLE + 1_000, config_pause=broken)
    await r.step()
    assert r.eval.requests == [] and r.store.rows_of() == [] and r.clock_.counters.config_paused_passes == 1


async def test_a_failed_position_is_held() -> None:
    r = bc_rig(arm_row(state="FAILED", prior="TRACKING"))
    await r.step()
    assert r.eval.requests == [] and r.store.rows_of() == []


# ---------------------------------------------------------------------------------------------------- clock, lease, reads
async def test_a_clock_step_holds_the_pass() -> None:
    r = bc_rig(now=T1 - 60_000)
    await r.step()
    r.clock.mono_offset = -10_000                              # the wall clock jumped 10 s against monotonic time
    await r.step(T1 + SETTLE + 1_000)
    assert r.clock_.counters.clock_steps == 1 and r.eval.requests == []
    await r.step(T1 + SETTLE + 1_250)
    assert [(d.event, d.bar_end_ms) for d in r.store.rows_of()] == [("POST", T1)]


async def test_no_lease_no_decision() -> None:
    r = bc_rig()
    r.lease[0] = False
    await r.step()
    assert r.eval.requests == [] and r.clock_.counters.lease_holds == 1


async def test_a_changed_md_generation_discards_the_reads_of_the_round() -> None:
    r = bc_rig()
    r.md.bump_every_call = True
    await r.step()
    assert r.eval.requests and r.store.rows_of() == []
    assert r.clock_.counters.generation_discards >= 1
    r.md.bump_every_call = False
    await r.step(T1 + SETTLE + 2_000)
    assert len(r.store.rows_of()) == 1


async def test_a_job_fault_writes_no_row_and_keeps_the_bar_pending() -> None:
    r = bc_rig()
    r.eval.raise_job.add("P1")
    await r.step()
    assert r.store.rows_of() == [] and r.clock_.counters.job_faults == 1
    row = row_of(r.store)
    assert (row.pending_event, row.last_decided_bar) == ("POST", T0)


async def test_a_non_final_post_writes_no_row() -> None:
    unavailable = Evaluated(verdict=Verdict.DECISION, decision=decided().decision, final=False)
    r = bc_rig(rule=lambda req: unavailable if req.event is ArmEvent.POST else None)
    await r.step()
    assert r.store.rows_of() == [] and row_of(r.store).last_decided_bar == T0


# ---------------------------------------------------------------------------------------------------- REOPEN
class Closes:
    def __init__(self, bar: int, open_ms: int) -> None:
        self.bar, self.open_ms = bar, open_ms

    def next_open_ms(self, adapter_id: str, symbol: str, bar_end_ms: int) -> int | None:
        return self.open_ms if bar_end_ms == self.bar else None


async def test_market_closed_post_with_trigger_is_rechecked_as_reopen_at_the_open() -> None:
    open_ms = T1 + 8 * TF
    closed = frozenset(T1 + k * TF for k in range(1, 9))
    closed_post = decided(outcome="MARKET_CLOSED", category="HOLD", reopen=True)

    def rule(req):  # type: ignore[no-untyped-def]
        return closed_post if req.event is ArmEvent.POST else None

    r = bc_rig(rule=rule, grid=FakeGrid(closed=closed), closes=Closes(T1, open_ms))
    await r.step(T1 + SETTLE + 1_000)
    assert row_of(r.store).reopen_bar_end_ms == T1
    await r.step(T1 + 4 * TF)                                 # closed: nothing
    assert [q.event for q in r.eval.requests] == [ArmEvent.POST]
    await r.step(open_ms + SETTLE + 1)
    post, reopen = r.store.rows_of()
    assert (reopen.event, reopen.bar_end_ms, reopen.advice) == ("REOPEN", T1, True)
    row = row_of(r.store)
    assert (row.reopen_bar_end_ms, row.last_decided_bar, row.state) == (None, T1, "ADVICE_OUTSTANDING")


async def test_pre_with_stop_reads_skips_a_position_armed_after_its_read_was_queued() -> None:
    from src.riskmgr.stopreads import pre_queue_ms

    r = bc_rig(arm_row("P1", cursor=T1), stop_reads=True, handles=handles_for("P1"))
    await r.step(pre_queue_ms(T2, COLD_SKEW, 40))              # the PRE read is queued for P1 only
    r.store.rows[PositionKey(adapter_id=ADAPTER, position_id="P2")] = arm_row("P2", cursor=T1)
    await r.step(pre_fire_ms(T2, COLD_SKEW, 40))
    assert [(d.event, d.key.position_id) for d in r.store.decisions.values()] == [("PRE", "P1")]
    assert r.clock_.counters.pre_dropped == {ARMED_AFTER_PRE_READ: 1}


async def test_release_events_go_through_sm11() -> None:
    r = bc_rig(rule=lambda req: decided(events=(Sm11Event.RELEASED,), outcome="COMPLETE", category="INFO")
               if req.event is ArmEvent.POST else None)
    await r.step()
    assert row_of(r.store).state == "STRATEGY_COMPLETE"
