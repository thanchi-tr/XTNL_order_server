"""Round-12 fixes (plan S38c; design §B5.2, §B6.3, §B6.6, §B12, §B14): `NO_BROKER_BAR` for an OPEN bucket the broker
did not trade (a holiday gives no FAILED, a weekend no stuck frontier), the stuck frontier scoped to its position,
`prior_state` kept while another FAILED scope still covers a position, and the page when a FAILED episode can only
be spooled (the AWS leader-change gap). Synthetic ids, symbols and levels only.
"""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal as D

import pytest

from src.marketdata.brokerbars import Coverage
from src.marketdata.types import INVALID_BAR
from src.riskmgr import budget as B
from src.riskmgr import probes as P
from src.riskmgr.config import FailSettings
from src.riskmgr.frontier import STUCK_FRONTIER, STUCK_GRACE_MS, FrontierView
from src.riskmgr.outcomes import Category, Outcome
from src.riskmgr.pipeline import NO_TRADE_PRE_NOT_CONFIRMED, PipelineInput, ResultKind, run_pipeline
from src.riskmgr.render import ItemBody, RenderSettings, outcome_text
from src.riskmgr.sm11 import ArmState
from src.riskmgr.stopbasis import PreBasis, StopAtT
from src.riskmgr.types import ArmEvent, ScopeKind, StopBasis
from src.runtime.optional_plan import _no_trade
from tests.riskmgr.barclock_fakes import TF, T1, FakeGrid, arm_row, bc_rig
from tests.riskmgr.failed_fakes import FailedRig, failed_rig, row, series
from tests.riskmgr.kind_a import s_ctx
from tests.riskmgr.test_failed_clear import VERSION, clearing
from tests.riskmgr.test_pipeline import E, OPEN, lv, params, post_run, pre_run, quote

S_A = series("SYN.A")


# ---------------------------------------------------------------------------------------------------- NO_BROKER_BAR
def _no_bar_input(arm: ArmState, *, no_trade: bool, now_offset: int = 60_000) -> PipelineInput:
    ctx = replace(s_ctx("1.20150", event=ArmEvent.POST), bar=None, exit_quote_live=True)
    now = ctx.bar_end_ms + now_offset
    return PipelineInput(params=params(), ctx=ctx, state=params_state(), arm_state=arm, now_ms=now, calendar=OPEN,
                         stop_at_t=StopAtT(ctx.stop or ctx.entry, StopBasis.STREAM),
                         live_quote=quote("1.20150", now - 500), bar_no_trade=no_trade)


def params_state():  # type: ignore[no-untyped-def]
    from src.riskmgr.strategy import StrategyState
    return StrategyState.of(latched=False)


def test_an_open_bucket_resolved_no_trade_is_no_broker_bar_info_and_decides_the_bar() -> None:
    for arm in (ArmState.ARMED, ArmState.TRACKING, ArmState.ADVICE_OUTSTANDING):
        r = run_pipeline(_no_bar_input(arm, no_trade=True))
        assert r.kind is ResultKind.DECISION and r.outcome is Outcome.NO_BROKER_BAR and r.final
        assert r.code is not None and r.code.category is Category.INFO and r.shown is False
        assert r.trigger_met is None and r.sm11_events == () and r.state == params_state()
    # without no_trade evidence the same missing bar waits for the broker's bar (unchanged)
    waiting = run_pipeline(_no_bar_input(ArmState.TRACKING, no_trade=False))
    assert waiting.kind is ResultKind.PENDING


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_no_broker_bar_after_a_pre_heads_up_keeps_the_basis_and_says_not_confirmed(sign: int, side: str) -> None:
    """S38c review: a PRE heads-up fired, the user tightened the stop to break-even, then the broker did not trade
    the bucket. `NO_BROKER_BAR` folds the PRE levels into the stored basis (as every other non-confirming bar end
    does) and is shown as "not confirmed"; the next traded bar tests against the pre-move stop, not the tightened
    one."""
    pre = pre_run(lv(sign, "0.00150"), side=side)
    assert pre.pre_levels is not None
    ctx = replace(s_ctx(lv(sign, "0.00150"), side=side, stop=str(E), event=ArmEvent.POST), bar=None)
    r = post_run(lv(sign, "0.00150"), ctx=ctx, s_at_t=StopAtT(E, StopBasis.STREAM), this_bar_pre=pre.pre_levels,
                 bar_no_trade=True)
    assert r.outcome is Outcome.NO_BROKER_BAR and r.code is not None and r.code.category is Category.INFO
    assert r.code.reason == NO_TRADE_PRE_NOT_CONFIRMED and r.shown and r.final
    assert r.pre_basis == pre.pre_levels and r.trigger_met is None and r.sm11_events == ()
    assert r.state == pre_state()
    nxt = post_run(lv(sign, "0.00050"), side=side, stop=str(E), pre_basis=r.pre_basis,
                   s_at_t=StopAtT(E, StopBasis.STREAM))
    assert nxt.stop_basis is StopBasis.AT_PRE and nxt.trigger_met is False
    assert nxt.outcome is Outcome.AT_TARGET_UNCONFIRMED and nxt.pre_basis == pre.pre_levels
    # a farther basis already stored is kept (a newer PRE never makes the stored basis easier)
    farther = PreBasis(tuple((role, lvl - sign * D("0.00100")) for role, lvl in pre.pre_levels.levels),
                       pre.pre_levels.bar_end_ms - TF)
    kept = post_run(lv(sign, "0.00150"), ctx=ctx, s_at_t=StopAtT(E, StopBasis.STREAM), pre_basis=farther,
                    this_bar_pre=pre.pre_levels, bar_no_trade=True)
    assert kept.pre_basis == farther
    # the item renders as not confirmed
    text = outcome_text(ItemBody(decision_id=1, decision_bar_end_ms=T1, adapter_id="mock", position_id="P1",
                                 event="POST", bar_end_ms=T1, outcome=Outcome.NO_BROKER_BAR.value, category="INFO",
                                 reason=NO_TRADE_PRE_NOT_CONFIRMED), RenderSettings())
    assert "not confirmed" in text


def pre_state():  # type: ignore[no-untyped-def]
    return params_state()


def test_a_holiday_the_broker_does_not_trade_gives_no_failed() -> None:
    """A calendar-OPEN holiday: md-bars resolves each bucket `no_trade`; the decisions are `NO_BROKER_BAR` and the
    error budget counts none of them, so `fail_unresolved_n` is never met and the series never fails.

    Note (S38c review): the runtime does not call `ErrorBudget.note_unresolved` / `note_no_trade` today (S30 left
    the unresolved streak unwired, plan S38c deviation 5), so in production the streak is not counted at all; this
    test calls `note_no_trade` by hand and proves only the budget's own rule (a no-trade bucket neither adds to nor
    resets the streak) for when it is wired."""
    rig = _bare(FailSettings(unresolved_n=1))
    budget = rig.budget
    for k in range(40):                                                  # ten hours of 15m buckets
        r = run_pipeline(_no_bar_input(ArmState.TRACKING, no_trade=True))
        assert r.outcome is Outcome.NO_BROKER_BAR and r.final
        budget.note_no_trade(S_A, k * TF)
    assert budget.unresolved_streak(S_A) == 0 and budget.take() == []
    assert budget.counters.no_trade_buckets == 40


def test_no_trade_is_read_from_md_bars_coverage_and_unknown_is_not_no_trade() -> None:
    class Bars:
        def coverage_of(self, s, bucket):  # type: ignore[no-untyped-def]
            got = {100: Coverage.NO_TRADE, 200: Coverage.UNFILLABLE} if s == S_A else {}
            return got.get(bucket)

    bars = Bars()
    assert _no_trade(bars, S_A, 100) is True                            # type: ignore[arg-type]
    assert _no_trade(bars, S_A, 200) is False and _no_trade(bars, S_A, 300) is False   # type: ignore[arg-type]
    assert _no_trade(bars, series("SYN.B"), 100) is False               # type: ignore[arg-type]


async def test_a_weekend_gives_no_stuck_frontier() -> None:
    """The decision-bar sequence is the evaluable (calendar-OPEN) buckets only: the ~190 weekend buckets after the
    cursor are no bars, so no position has an overdue bar and the stuck-frontier watch files nothing."""
    weekend = frozenset(T1 + k * TF for k in range(192))
    r = bc_rig(arm_row(cursor=T1 - TF), grid=FakeGrid(closed=weekend | {T1 - TF + 0}), now=T1 - TF + 1)
    rig = _bare()
    for k in range(1, 190, 7):                                           # passes all weekend long
        now = T1 + k * TF + 30_000
        await r.step(now)
        views = r.clock_.frontier_views(now)
        assert [v.overdue_bar_ms for v in views] == [None]
        rig.budget.observe_frontier(views, now, running=True)
    assert rig.budget.take() == []


# ---------------------------------------------------------------------------------------------------- stuck frontier
def _over(rig: FailedRig, views: list[FrontierView]) -> None:
    """Three counting passes 60 s apart: the positions with an overdue bar pass the 120 s grace."""
    for _ in range(3):
        rig.budget.observe_frontier(views, rig.clock.now, running=True)
        rig.clock.now += STUCK_GRACE_MS // 2


def _bare(fail: FailSettings | None = None) -> FailedRig:
    """A rig whose budget queues its faults (`take()`) instead of handing them to a FAILED manager."""
    rig = failed_rig(fail=fail)
    rig.budget = B.ErrorBudget(fail or FailSettings(), calendar=rig.calendar, health=rig.health, pg=rig.pg)
    return rig


def test_a_stuck_frontier_on_one_position_fails_only_that_position() -> None:
    rig = _bare()
    views = [FrontierView(position_id="mock:P1", overdue_bar_ms=T1), FrontierView(position_id="mock:P2",
                                                                                  overdue_bar_ms=None)]
    _over(rig, views)
    (fault,) = rig.budget.take()
    assert fault.scope == B.position_scope("mock:P1") and fault.reason == STUCK_FRONTIER
    assert fault.position_ids == ("mock:P1",) and fault.probe_kind == "span_write"
    # the position is now held (its scope FAILED): a later poisoned row on P2 alone is its own POSITION scope
    later = [FrontierView(position_id="mock:P1", overdue_bar_ms=T1, held=True),
             FrontierView(position_id="mock:P2", overdue_bar_ms=T1 + TF)]
    _over(rig, later)
    (second,) = rig.budget.take()
    assert second.scope == B.position_scope("mock:P2") and second.scope.kind is ScopeKind.POSITION


def test_a_stuck_frontier_shared_by_n_positions_is_arm_global() -> None:
    rig = _bare()
    views = [FrontierView(position_id=f"mock:P{i}", overdue_bar_ms=T1) for i in (1, 2)]
    _over(rig, views)
    (fault,) = rig.budget.take()
    assert fault.scope == B.ARM_SCOPE and fault.position_ids == ("mock:P1", "mock:P2")
    # with fail_frontier_positions_n = 1 every stuck frontier is global (the pre-S38c rule)
    one = _bare(FailSettings(frontier_positions_n=1))
    _over(one, views[:1])
    (g,) = one.budget.take()
    assert g.scope == B.ARM_SCOPE


async def test_one_poisoned_row_leaves_the_other_positions_advising() -> None:
    """The POSITION scope covers only its own row: the other positions keep advising; its clear is refused while
    the row is still poisoned (the probe re-runs that row's span write)."""
    c = await clearing()
    r = c.rig
    _over(r, [FrontierView(position_id="mock:P1", overdue_bar_ms=T1),
              FrontierView(position_id="mock:P2", overdue_bar_ms=None),
              FrontierView(position_id="mock:P3", overdue_bar_ms=None)])
    await c.m.tick()
    assert c.states()["P1"][0] == "FAILED" and c.states()["P2"][0] == "TRACKING" and c.states()["P3"][0] == "TRACKING"
    assert not c.m.scopes.global_failed
    c.span.result = P.refused_probe("span_write_failed")
    rid = c.request("POSITION:mock:P1")
    await c.run()
    assert c.answer(rid)[0] == "refused" and c.span.calls == ["span:mock:P1"]


async def test_a_position_stuck_frontier_clear_is_applied_once_the_real_span_write_succeeds() -> None:
    """S38c review: the `span_write` probe is wired to the BarClock's store (the ARM pool): it re-runs the recorded
    position's span write, fenced, inside a transaction it rolls back. While the row's write still fails the clear
    is refused; once it would commit the POSITION clear is applied, and nothing the probe wrote is kept."""
    from src.db.arm_store import SQL_DECISION_INSERT, PgArmStateStore
    from src.db.epoch import EngineDb, Epoch
    from src.riskmgr.barclock import SPAN_WRITE_FAILED
    from src.runtime.optional_plan import LateSpanProbe
    from tests.db.arm_emulator import ArmEmulator

    emu = ArmEmulator(epoch=7)
    store = PgArmStateStore(EngineDb(emu, lambda: Epoch("c", 7)))
    async with store.transaction() as tx:
        await tx.insert_position(arm_row("P1", state="FAILED", prior="TRACKING"))
        await tx.insert_position(arm_row("P2"))
    bc = bc_rig(store=store, now=T1 + 12 * TF)
    poisoned = {"P1"}
    emu.fail_when = lambda sql, args: (ValueError("poisoned row") if sql == SQL_DECISION_INSERT
                                       and "P1" in poisoned and "P1" in args else None)
    span = LateSpanProbe()
    assert await span.test_span_write(["mock:P1"]) == P.refused_probe("probe_not_wired")   # not bound yet
    span.bind(bc.clock_)
    assert await bc.clock_.test_span_write(["mock:P1"]) == P.refused_probe(SPAN_WRITE_FAILED)
    assert await bc.clock_.test_span_write(["mock:P2", "mock:P9"]) == P.PASSED   # P9 is gone: nothing to write

    c = await clearing()
    c.span = span                                                        # type: ignore[assignment]
    c.build()
    assert await c.m.start()
    c.m.enter(B.Fault(scope=B.position_scope("mock:P1"), reason=STUCK_FRONTIER, first_fault_at_ms=c.rig.clock.now,
                      counters=(("stuck_positions", 1),), position_ids=("mock:P1",)))
    await c.m.tick()
    rid = c.request("POSITION:mock:P1")
    await c.run()
    assert c.answer(rid) == ("refused", f"cause_present_{SPAN_WRITE_FAILED}")
    poisoned.clear()                                                     # the row was fixed
    rid = c.request("POSITION:mock:P1")
    await c.run()
    assert c.answer(rid) == ("applied", "")
    assert emu.t.decisions == {}                                         # every probe write was rolled back
    assert {k[1]: v["row_version"] for k, v in emu.t.positions.items()} == {"P1": 0, "P2": 0}


# ---------------------------------------------------------------------------------------------------- prior_state
async def test_clearing_one_of_two_covering_scopes_keeps_the_position_failed() -> None:
    """Round 12: a position covered by a SERIES and a STRATEGY_VERSION scope stays FAILED (silent) when only the
    series scope clears; it returns to its prior_state only once no open scope covers it."""
    c = await clearing()
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    r.budget.version_fault(VERSION, 3, ("mock:P1",), r.clock.now)
    await c.m.tick()
    assert c.states()["P1"] == ("FAILED", "TRACKING", "scope_failed")
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.run()
    assert c.answer(rid) == ("applied", "")
    await c.m.tick()
    assert c.states()["P1"][0] == "FAILED"                              # the version scope still covers it
    assert c.m.covers("mock", [S_A.canonical], [VERSION])
    rid = c.request(f"STRATEGY_VERSION:{VERSION}")
    await c.run()
    assert c.answer(rid) == ("applied", "")
    await c.m.tick()
    assert c.states()["P1"] == ("TRACKING", None, "failed_cleared")


# ---------------------------------------------------------------------------------------------------- the spool page
async def test_a_spooled_episode_pages_at_once_naming_the_leader_change_gap() -> None:
    """S38c (AWS leader change): the database is tried first; when it fails the episode is spooled and a page says
    it is held on this node only, once per episode."""
    rig = failed_rig()
    rig.add(row("P1", symbol="SYN.A"))
    assert await rig.manager.start()
    rig.arm.down = rig.engine.down = True
    rig.budget.series_fault(S_A, INVALID_BAR, rig.clock.now)
    for _ in range(3):
        await rig.manager.tick()
        rig.clock.now += 5_000
    pages = [p for p in rig.mirror.pages if p[1].startswith("ARM FAILED not recorded")]
    assert len(pages) == 1 and pages[0][0] == "critical" and len(rig.spool.records) == 1
    assert "leader change" in pages[0][2] and S_A.canonical in pages[0][2]
    # S38c review: that page goes through the PostgreSQL outbox; the dead-man beat (HTTPS from memory, no
    # PostgreSQL) carries the unrecorded count off-node, and the receiver pages on it at once
    from src.deadman_receiver.evaluate import Evaluator, parse_beat
    from src.runtime.deadman import AdapterBeat, ArmFailedBeat, DeadmanPayload

    summary = rig.manager.summary()
    assert summary.unrecorded == 1

    def beat(at: int, failed: ArmFailedBeat, node: str = "node-a", epoch: str = "7") -> bytes:
        return DeadmanPayload(node=node, epoch=epoch, sent_at_ms=at, gate_state="OPEN", halt_level="NONE",
                              adapters={"mock": AdapterBeat("STREAMING", True, None)},
                              last_committed_broker_event_age_ms=1, oldest_unacked_feed_age_ms=None,
                              oldest_risksweep_pass_age_ms=1, calendar_version=1,
                              oldest_undelivered_notification_age_ms=None, sm8_state="READY", sm5_state="NONE",
                              spill_bytes=0, degraded=[], arm_failed=failed).to_json().encode()

    at = 1_700_000_000_000
    ev = Evaluator(started_ms=at)
    ev.accept(parse_beat(beat(at, ArmFailedBeat(summary.count, summary.global_failed, summary.unrecorded))), at)
    assert "arm_failed_unrecorded" in ev.active(at)
    # the leader changes before PostgreSQL returns: the new leader reports nothing, the receiver keeps paging
    ev.accept(parse_beat(beat(at + 15_000, ArmFailedBeat(0, False, 0), node="node-b", epoch="8")), at + 15_000)
    assert "arm_failed_unrecorded" in ev.active(at + 15_000)
    rig.arm.down = rig.engine.down = False
    await rig.manager.tick()
    assert len(rig.emu.t.failures) == 1                                 # recorded once PostgreSQL is back
    assert rig.manager.summary().unrecorded == 0


def test_restart_required_is_an_arm_global_fault_with_the_trial_probe() -> None:
    rig = _bare()
    rig.budget.restart_required(4, rig.clock.now)
    (f,) = rig.budget.take()
    assert f.scope == B.ARM_SCOPE and f.reason == B.RESTART_REQUIRED and f.probe_kind == "arm_global"
    assert dict(f.counters) == {"hung_threads": 4}



async def test_a_restart_required_clear_is_refused_while_threads_are_hung_and_accepted_after() -> None:
    """S38c review: the ARM-global probe of a `restart_required` episode asks the job runner whether more threads
    are hung than it may abandon (`threads_hung`); the clear passes only once they are gone (a restart, or the
    threads ending), and without that view the probe is not wired."""
    import asyncio
    import threading

    from src.riskmgr.jobs import JOB_TIMEOUT, JobRunner, JobSpec

    release = threading.Event()
    runner = JobRunner(cap_ms=50, max_abandoned=0)
    try:
        hang = await runner.run(JobSpec("strategy:toy:1"), "mock:P1", lambda: release.wait(10))
        assert hang.reason == JOB_TIMEOUT and runner.take_restart_required() == 1
        assert runner.restart_still_required()
        c = await clearing()
        c.rig.budget.restart_required(1, c.rig.clock.now)
        await c.m.tick()
        assert c.episode("ARM", "arm")["probe_kind"] == "arm_global"
        rid = c.request("ARM:arm")                                       # the view is not wired: refused
        await c.run()
        assert c.answer(rid) == ("refused", "cause_present_probe_not_wired")
        c.hung = runner
        c.build()
        rid = c.request("ARM:arm")
        await c.run()
        assert c.answer(rid) == ("refused", "cause_present_threads_hung") and c.trial.calls == 0
        release.set()                                                    # the hung thread ends (or a restart)
        for _ in range(200):
            if not runner.restart_still_required():
                break
            await asyncio.sleep(0.01)
        rid = c.request("ARM:arm")
        await c.run()
        assert c.answer(rid) == ("applied", "") and c.trial.calls == 1
    finally:
        release.set()
        runner.close()


# ---------------------------------------------------------------------------------------------------- arming under FAILED
def test_arming_and_rearm_ask_about_the_positions_own_scope_and_arm_into_failed() -> None:
    """Round 12 / S38c: arming and `arm_rearm` re-run the arming guard and arm into FAILED while a covering scope is
    open; the tracker now passes the position's ref, so its own POSITION scope counts (P2 is not covered)."""
    import asyncio

    from src.riskmgr.tracker import TrackerRequest
    from tests.riskmgr.tracker_fakes import ADAPTER, image, position
    from tests.riskmgr.tracker_fakes import rig as tracker_rig

    r = tracker_rig()
    asked: list[str | None] = []
    covered = {f"{ADAPTER}:P1"}

    def covers(adapter_id: str, series: object, versions: object, position: str | None = None) -> bool:
        asked.append(position)
        return position in covered

    r.failed.covers = covers  # type: ignore[method-assign]
    r.snapshot.images = [image(position("P1"), position("P2"))]
    asyncio.run(r.tracker.step())
    states = {k.position_id: v.state for k, v in r.store.rows.items()}
    assert states == {"P1": ArmState.FAILED.value, "P2": ArmState.ARMED.value}
    assert f"{ADAPTER}:P1" in asked and f"{ADAPTER}:P2" in asked
    # P2 suspends (units grew), then its own scope opens: the re-arm arms it into FAILED
    r.snapshot.images = [image(position("P1"), position("P2", units="2000"))]
    asyncio.run(r.tracker.step())
    assert r.store.rows[next(k for k in r.store.rows if k.position_id == "P2")].state == ArmState.SUSPENDED.value
    covered.add(f"{ADAPTER}:P2")
    r.tracker.submit(TrackerRequest.of(41, "arm_rearm", f"{ADAPTER}:P2"))
    asyncio.run(r.tracker.step())
    assert r.store.rows[next(k for k in r.store.rows if k.position_id == "P2")].state == ArmState.FAILED.value


# ---------------------------------------------------------------------------------------------------- S38c review
async def test_a_no_trade_bucket_is_no_broker_bar_through_the_evaluator_and_the_barclock() -> None:
    """S38c review: `PipelineEvaluator(no_trade=...)` reads md-bars' coverage for the missing trigger bar, the
    pipeline decides `NO_BROKER_BAR` and the BarClock commits it and advances `last_decided_bar`; a bucket the
    coverage map does not know waits for the broker's bar as before."""
    from tests.riskmgr.test_arm_join import TF as JTF
    from tests.riskmgr.test_arm_join import join_rig

    j = join_rig()
    try:
        await j.tracker.step()
        await j.notices.step()
        cursor = j.row().last_decided_bar
        assert cursor is not None
        b1, b2 = cursor + JTF, cursor + 2 * JTF
        asked: list[int] = []

        def no_trade(series: object, bucket: int) -> bool:
            asked.append(bucket)
            return bucket == b1 - JTF                                    # only B1's bucket resolved no_trade

        j.new_term(no_trade=no_trade)
        j.md.live_quote("1.2050", b1 + 1_000)
        await j.at(b1 + 5_000)
        post = [r for r in j.decisions("POST") if r.bar_end_ms == b1]
        assert [(r.outcome, r.category) for r in post] == [("NO_BROKER_BAR", "INFO")]
        assert j.row().last_decided_bar == b1 and b1 - JTF in asked
        await j.at(b2 + 5_000)                                           # B2 is unknown to the coverage map
        assert [r for r in j.decisions("POST") if r.bar_end_ms == b2] == []
        assert j.row().last_decided_bar == b1
    finally:
        j.barclock.close()


def test_positions_stuck_one_after_another_still_reach_the_arm_global_bound() -> None:
    """S38c review: P1 is stuck and its POSITION scope FAILED (held, out of the pass); P2 gets stuck on a later
    bar. The open POSITION stuck-frontier scopes count toward `frontier_positions_n`, so the shared cause goes
    ARM-global naming both."""
    rig = _bare()
    _over(rig, [FrontierView(position_id="mock:P1", overdue_bar_ms=T1)])
    (first,) = rig.budget.take()
    assert first.scope == B.position_scope("mock:P1")
    _over(rig, [FrontierView(position_id="mock:P1", overdue_bar_ms=T1, held=True),
                FrontierView(position_id="mock:P2", overdue_bar_ms=T1 + TF)])
    assert [f.scope for f in rig.budget.take()] == [B.position_scope("mock:P2")]   # without the earlier scope
    rig2 = _bare()
    _over(rig2, [FrontierView(position_id="mock:P1", overdue_bar_ms=T1)])
    rig2.budget.take()
    for _ in range(3):
        rig2.budget.observe_frontier([FrontierView(position_id="mock:P1", overdue_bar_ms=T1, held=True),
                                      FrontierView(position_id="mock:P2", overdue_bar_ms=T1 + TF)],
                                     rig2.clock.now, running=True, failed_stuck=("mock:P1",))
        rig2.clock.now += STUCK_GRACE_MS // 2
    (g,) = rig2.budget.take()
    assert g.scope == B.ARM_SCOPE and g.position_ids == ("mock:P1", "mock:P2")


async def test_the_failed_manager_lists_its_open_position_stuck_frontier_scopes() -> None:
    c = await clearing()
    c.m.enter(B.Fault(scope=B.position_scope("mock:P1"), reason=STUCK_FRONTIER, first_fault_at_ms=c.rig.clock.now,
                      counters=(("stuck_positions", 1),), position_ids=("mock:P1",)))
    c.rig.budget.series_fault(S_A, INVALID_BAR, c.rig.clock.now)
    await c.m.tick()
    assert c.m.stuck_positions() == ("mock:P1",)
