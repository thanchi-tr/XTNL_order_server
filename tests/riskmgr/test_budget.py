"""S30a: the D7 error budget: one counting unit, counting pauses, scopes and the riskmgr probe-kind copy (design
§B14; R11-M08, R11-M10, R11-M11, R11-M17). Synthetic series and ids only."""
from __future__ import annotations

import pytest

from src.db import arm_schema as A
from src.marketdata.types import INDICATOR_FAULTED, INVALID_BAR, SOURCE_MISMATCH
from src.riskmgr import budget as B
from src.riskmgr.config import FailSettings
from src.riskmgr.frontier import STUCK_FRONTIER, FrontierView
from src.riskmgr.types import ScopeKind
from tests.riskmgr.failed_fakes import FakeCalendar, FakeHealth, FakeMaintenance, FakePg, T0, series
from tests.riskmgr.tracker_fakes import ADAPTER, SYMBOL, FakeClock

TF = 900_000
S1 = series()
S2 = series("SYN.B")


class Rig:
    def __init__(self, **fail: int) -> None:
        self.clock = FakeClock(T0)
        self.calendar, self.health, self.pg = FakeCalendar(), FakeHealth(), FakePg(self.clock)
        self.maintenance = FakeMaintenance()
        self.budget = B.ErrorBudget(FailSettings(**fail), calendar=self.calendar, health=self.health, pg=self.pg,
                                    maintenance=self.maintenance)

    @property
    def now(self) -> int:
        return self.clock.now

    def advance(self, ms: int) -> None:
        self.clock.now += ms

    def unresolved(self, s: object, n: int) -> None:
        """`n` consecutive OPEN buckets of `s`, each still not final at its live cap."""
        for _ in range(n):
            bucket = self.now - self.now % TF
            self.budget.note_unresolved(s, bucket, self.now - 5_000, self.now)  # type: ignore[arg-type]
            self.advance(TF)


@pytest.fixture
def r() -> Rig:
    return Rig()


# ---------------------------------------------------------------------------------------------------- probe kinds
def test_reason_codes_and_probe_kinds_equal_the_migration() -> None:
    """The riskmgr's probe-kind copy equals `src.db.arm_schema.probe_kind_for` for every reason it files, and every
    reason passes the `arm_failures.reason` CHECK."""
    assert dict(B.PROBE_KIND_BY_REASON) == dict(A.REASON_PROBE_KIND)
    assert {B.ADAPTER_PROBE, B.ARM_GLOBAL_PROBE} == set(A.SCOPE_PROBE_KIND.values())
    cases = ([(ScopeKind.SERIES, r) for r in B.SERIES_REASONS] + [(ScopeKind.ARM, r) for r in B.ARM_REASONS]
             + [(ScopeKind.ADAPTER, B.SERIES_FAILED), (ScopeKind.STRATEGY_VERSION, B.STRATEGY_VERSION_FAULT)])
    import re
    for kind, reason in cases:
        assert re.fullmatch(A.REASON_PATTERN, reason), reason
        assert B.probe_kind(kind, reason) == A.probe_kind_for(kind.value, reason)
    assert B.probe_kind(ScopeKind.ARM, STUCK_FRONTIER) == "span_write"
    assert B.probe_kind(ScopeKind.ARM, B.POOL_DOWN) == "arm_global"
    with pytest.raises(ValueError):
        B.probe_kind(ScopeKind.SERIES, B.MD_DOWN)


@pytest.mark.parametrize("scope, reason", [
    (B.ScopeRef(ScopeKind.SERIES, "x:y:15m:BID:BROKER"), B.MD_DOWN),
    (B.ARM_SCOPE, INVALID_BAR),
    (B.ScopeRef(ScopeKind.ADAPTER, "mock"), INVALID_BAR),
    (B.ScopeRef(ScopeKind.STRATEGY_VERSION, "strategy:k:1"), INVALID_BAR),
])
def test_a_fault_reason_must_belong_to_its_scope(scope: B.ScopeRef, reason: str) -> None:
    with pytest.raises(ValueError):
        B.Fault(scope=scope, reason=reason, first_fault_at_ms=0)


def test_scope_refs() -> None:
    assert B.series_scope(S1).target == f"SERIES:{S1.canonical}"
    assert B.adapter_of(B.series_scope(S1)) == ADAPTER and B.adapter_of(B.ARM_SCOPE) is None
    with pytest.raises(ValueError):
        B.ScopeRef(ScopeKind.SERIES, "")


# ---------------------------------------------------------------------------------------------------- SERIES
def test_invalid_bars_at_k_fail_only_that_series(r: Rig) -> None:
    r.budget.series_fault(S1, INVALID_BAR, r.now, counters=(("invalid_buckets", 3),))
    (f,) = r.budget.take()
    assert f.scope == B.series_scope(S1) and f.reason == INVALID_BAR and f.probe_kind == "series_reread"
    assert f.counters == (("invalid_buckets", 3),)
    assert r.budget.take() == []


@pytest.mark.parametrize("reason", [SOURCE_MISMATCH, B.REFETCH_FAILED, INDICATOR_FAULTED])
def test_other_one_shot_series_faults_file_at_once(r: Rig, reason: str) -> None:
    r.budget.series_fault(S1, reason, r.now)
    assert [f.reason for f in r.budget.take()] == [reason]


def test_unresolved_is_not_a_one_shot_reason(r: Rig) -> None:
    with pytest.raises(ValueError):
        r.budget.series_fault(S1, B.UNRESOLVED, r.now)


@pytest.mark.parametrize("pause", ["closed", "maintenance", "not_streaming", "rest_failing"])
def test_a_one_shot_fault_during_a_pause_is_deferred_then_filed(r: Rig, pause: str) -> None:
    if pause == "closed":
        r.calendar.closed.add((ADAPTER, SYMBOL))
    elif pause == "maintenance":
        r.maintenance.adapters.add(ADAPTER)
    elif pause == "not_streaming":
        r.health.down.add(ADAPTER)
    else:
        r.health.rest_failing[ADAPTER] = True
    r.budget.series_fault(S1, INVALID_BAR, r.now)
    r.budget.poll(r.now)
    assert r.budget.take() == [] and r.budget.deferred() == (S1.canonical,)
    r.calendar.closed.clear()
    r.maintenance.adapters.clear()
    r.health.down.clear()
    r.health.rest_failing.clear()
    r.budget.poll(r.now)
    assert [f.scope for f in r.budget.take()] == [B.series_scope(S1)] and r.budget.deferred() == ()


def test_a_second_reason_during_a_pause_is_kept_and_one_fault_is_filed(r: Rig) -> None:
    r.health.down.add(ADAPTER)
    r.budget.series_fault(S1, INVALID_BAR, r.now)
    r.advance(1_000)
    r.budget.series_fault(S1, INDICATOR_FAULTED, r.now)
    r.budget.series_fault(S1, INDICATOR_FAULTED, r.now)
    assert r.budget.deferred() == (S1.canonical,) and dict(r.budget.snapshot())["deferred_faults"] == 2
    r.health.down.clear()
    r.budget.poll(r.now)
    (f,) = r.budget.take()                       # the series' one episode, from the oldest signal
    assert f.reason == INVALID_BAR and r.budget.deferred() == ()


def test_a_direct_fault_drops_the_deferred_ones_of_its_series(r: Rig) -> None:
    r.health.down.add(ADAPTER)
    r.budget.series_fault(S1, INVALID_BAR, r.now)
    r.budget.series_fault(S2, SOURCE_MISMATCH, r.now)
    r.health.down.clear()
    r.budget.series_fault(S1, INDICATOR_FAULTED, r.now)     # the pause ended before poll(): filed at once
    assert [f.reason for f in r.budget.take()] == [INDICATOR_FAULTED]
    assert r.budget.deferred() == (S2.canonical,)
    r.budget.poll(r.now)
    assert [f.scope for f in r.budget.take()] == [B.series_scope(S2)]   # never S1 a second time


@pytest.mark.parametrize("outage", ["rest_failing", "not_streaming"])
def test_a_refetch_failure_during_a_broker_outage_is_never_a_fault(r: Rig, outage: str) -> None:
    if outage == "rest_failing":
        r.health.rest_failing[ADAPTER] = True
    else:
        r.health.down.add(ADAPTER)
    r.budget.series_fault(S1, B.REFETCH_FAILED, r.now)
    assert r.budget.deferred() == () and r.budget.counters.outage_refetch == 1
    r.health.rest_failing.clear()
    r.health.down.clear()
    r.budget.poll(r.now)
    assert r.budget.take() == []


def test_a_deferred_refetch_failure_is_dropped_when_an_outage_follows(r: Rig) -> None:
    r.calendar.closed.add((ADAPTER, SYMBOL))
    r.budget.series_fault(S1, B.REFETCH_FAILED, r.now)
    assert r.budget.deferred() == (S1.canonical,)
    r.calendar.closed.clear()
    r.health.rest_failing[ADAPTER] = True
    r.budget.poll(r.now)
    r.health.rest_failing.clear()
    r.budget.poll(r.now)
    assert r.budget.take() == [] and r.budget.deferred() == ()


def test_unresolved_counts_buckets_not_reads(r: Rig) -> None:
    """One bucket adds at most 1, however often it is reported; 2 consecutive buckets fail the series."""
    bucket = r.now - r.now % TF
    for _ in range(5):
        r.budget.note_unresolved(S1, bucket, r.now - 1_000, r.now)
    assert r.budget.unresolved_streak(S1) == 1 and r.budget.take() == []
    r.advance(TF)
    r.budget.note_unresolved(S1, bucket + TF, r.now - 1_000, r.now)
    (f,) = r.budget.take()
    assert f.reason == B.UNRESOLVED and f.probe_kind == "fresh_bar"
    assert dict(f.counters) == {"unresolved_buckets": 2, "last_bucket_ms": bucket + TF}


def test_a_final_bar_resets_the_streak(r: Rig) -> None:
    r.unresolved(S1, 1)
    r.budget.note_final(S1, r.now - r.now % TF)
    r.unresolved(S1, 1)
    assert r.budget.take() == [] and r.budget.unresolved_streak(S1) == 1


async def test_undecodable_reads_then_a_valid_bar_leave_k_at_zero(r: Rig) -> None:
    """R11-M08, driven through md-bars with the budget as its fault sink: one earlier bucket not final at its cap
    (streak 1, one short of `fail_unresolved_n` = 2), then 3 undecodable re-reads of the next bucket and a valid final
    bar. The decode errors are retries inside the bucket's healing: nothing reaches `note_unresolved` or the fault
    sink, and the final bar resets the streak, so K stays at zero and nothing is filed."""
    from tests.marketdata import test_brokerbars as md

    rig = md.make()
    left = [3]

    def fail(symbol: str, lo: int, hi: int) -> object:
        if lo == md.B and left[0] > 0:
            left[0] -= 1
            return md.Transient("decode_error")
        return None

    rig.broker.fail = fail
    series_ = md.S1
    r.budget.note_unresolved(series_, md.B - TF, r.now - 5_000, r.now)      # the earlier bucket: streak 1
    assert r.budget.unresolved_streak(series_) == 1
    await rig.run_to(md.T + 20_000)
    for s_, why in rig.faults:                                             # the fault sink (S31 wires it)
        r.budget.series_fault(s_, why, r.now)
    assert rig.bars.counters.decode_errors == 3 and rig.bars.counters.capped_unresolved == 0
    for _ in range(rig.bars.counters.capped_unresolved):                   # a counted cap would reach the budget
        r.budget.note_unresolved(series_, md.B, r.now - 5_000, r.now)
    assert len(rig.sink.of(series_, md.B)) == 1                            # the valid final bar
    r.budget.note_final(series_, md.B)
    assert r.budget.unresolved_streak(series_) == 0 and r.budget.take() == []
    r.budget.note_unresolved(series_, md.B + TF, r.now - 5_000, r.now)     # K restarts from zero
    assert r.budget.unresolved_streak(series_) == 1 and r.budget.take() == []


def test_a_30_minute_broker_rest_outage_gives_no_failed(r: Rig) -> None:
    r.health.rest_failing[ADAPTER] = True
    r.unresolved(S1, 4)                          # 60 minutes of buckets with no final bar
    assert r.budget.take() == [] and r.budget.counters.paused_buckets == 4
    r.health.rest_failing[ADAPTER] = None        # unknown: bar-read failures do not count either
    r.unresolved(S1, 2)
    assert r.budget.take() == []


def test_engine_postgresql_down_pauses_the_decision_cap_counter(r: Rig) -> None:
    r.pg.down = True
    r.unresolved(S1, 3)
    assert r.budget.take() == []
    r.pg.down = False
    r.pg.unreachable = True                      # the RiskSweep's unreachable alert
    r.unresolved(S1, 3)
    assert r.budget.take() == []


def test_a_closed_market_bucket_neither_counts_nor_breaks_the_run(r: Rig) -> None:
    r.unresolved(S1, 1)
    r.calendar.closed.add((ADAPTER, SYMBOL))
    r.unresolved(S1, 3)
    r.calendar.closed.clear()
    r.unresolved(S1, 1)
    assert [f.reason for f in r.budget.take()] == [B.UNRESOLVED]


def test_a_pc_sleep_longer_than_decision_cap_counts_nothing(r: Rig) -> None:
    """Caps that passed while the PC slept go through replay: they never count."""
    bucket = r.now - r.now % TF
    r.advance(3 * TF)                            # woke up: three caps passed while asleep
    for k in range(3):
        r.budget.note_unresolved(S1, bucket + k * TF, bucket + k * TF + TF + 30_000, r.now)
    assert r.budget.take() == [] and r.budget.counters.stale_caps == 3
    r.budget.observe_md(usable=False, market_open=True, now_ms=r.now)
    r.advance(2 * 3_600_000)                     # asleep for two hours with redis-md unusable
    r.budget.observe_md(usable=False, market_open=True, now_ms=r.now)
    assert r.budget.take() == [] and r.budget.counters.sleeps == 1


# ---------------------------------------------------------------------------------------------------- STRATEGY
def test_a_version_fault_fails_that_version_with_its_positions(r: Rig) -> None:
    r.budget.version_fault("strategy:k:1", 3, ("mock:P2", "mock:P1", "mock:P1"), r.now, edge_code="default")
    (f,) = r.budget.take()
    assert f.scope == B.ScopeRef(ScopeKind.STRATEGY_VERSION, "strategy:k:1") and f.edge_code == "default"
    assert f.position_ids == ("mock:P1", "mock:P2") and f.probe_kind == "strategy_conformance"
    with pytest.raises(ValueError):
        r.budget.version_fault("k:1", 3, (), r.now)


# ---------------------------------------------------------------------------------------------------- ARM
def test_redis_md_down_past_fail_md_down_s_is_a_global_failed(r: Rig) -> None:
    for _ in range(int(900 / 5)):
        r.budget.observe_md(usable=False, market_open=True, now_ms=r.now)
        r.advance(5_000)
    assert r.budget.take() == []                 # 895 s counted
    r.budget.observe_md(usable=False, market_open=True, now_ms=r.now)
    (f,) = r.budget.take()
    assert f.scope == B.ARM_SCOPE and f.reason == B.MD_DOWN and f.probe_kind == "arm_global"
    r.advance(5_000)
    r.budget.observe_md(usable=False, market_open=True, now_ms=r.now)
    assert r.budget.take() == []                 # once per episode


def test_redis_md_down_counts_only_while_a_market_is_open_and_resets_when_usable(r: Rig) -> None:
    for _ in range(300):
        r.budget.observe_md(usable=False, market_open=False, now_ms=r.now)
        r.advance(5_000)
    for _ in range(100):
        r.budget.observe_md(usable=False, market_open=True, now_ms=r.now)
        r.advance(5_000)
    r.budget.observe_md(usable=True, market_open=True, now_ms=r.now)
    for _ in range(100):
        r.budget.observe_md(usable=False, market_open=True, now_ms=r.now)
        r.advance(5_000)
    assert r.budget.take() == []


def test_the_arm_pool_down_is_one_arm_fault_and_never_a_stuck_frontier(r: Rig) -> None:
    views = [FrontierView(position_id=f"mock:P{i}", overdue_bar_ms=T0) for i in range(5)]
    for _ in range(int(900 / 5) + 1):
        r.budget.observe_pool(arm_ok=False, now_ms=r.now)
        r.budget.observe_frontier(views, r.now, running=True)
        r.advance(5_000)
    faults = r.budget.take()
    assert [(f.scope, f.reason) for f in faults] == [(B.ARM_SCOPE, B.POOL_DOWN)]
    assert r.budget.pool_failing


def test_the_pool_counts_only_while_the_main_pool_works(r: Rig) -> None:
    r.pg.down = True
    for _ in range(400):
        r.budget.observe_pool(arm_ok=False, now_ms=r.now)
        r.advance(5_000)
    assert r.budget.take() == []


def test_a_stuck_frontier_of_data_cause_is_an_arm_fault_after_the_grace(r: Rig) -> None:
    views = [FrontierView(position_id="mock:P1", overdue_bar_ms=T0)]
    for _ in range(25):
        r.budget.observe_frontier(views, r.now, running=True)
        r.advance(5_000)
    (f,) = r.budget.take()
    assert f.reason == STUCK_FRONTIER and f.probe_kind == "span_write" and f.position_ids == ("mock:P1",)
    assert dict(f.counters) == {"oldest_bar_ms": T0, "stuck_positions": 1}


def test_a_series_failed_held_for_10_minutes_never_gives_a_stuck_frontier(r: Rig) -> None:
    held = [FrontierView(position_id="mock:P1", overdue_bar_ms=T0, held=True)]
    for _ in range(120):
        r.budget.observe_frontier(held, r.now, running=True)
        r.advance(5_000)
    assert r.budget.take() == []


def test_the_frontier_does_not_count_while_engine_postgresql_is_down_or_the_arm_is_stopped(r: Rig) -> None:
    views = [FrontierView(position_id="mock:P1", overdue_bar_ms=T0)]
    r.pg.down = True
    for _ in range(60):
        r.budget.observe_frontier(views, r.now, running=True)
        r.advance(5_000)
    r.pg.down = False
    for _ in range(60):
        r.budget.observe_frontier(views, r.now, running=False)
        r.advance(5_000)
    assert r.budget.take() == []


@pytest.mark.parametrize("reason", [B.MD_RESETS, B.BOOTSTRAP_FAILED, B.MD_OOM])
def test_store_faults_are_global_at_once(r: Rig, reason: str) -> None:
    r.budget.store_fault(reason, r.now)
    assert [(f.scope, f.reason) for f in r.budget.take()] == [(B.ARM_SCOPE, reason)]
    with pytest.raises(ValueError):
        r.budget.store_fault(INVALID_BAR, r.now)


def test_a_circuit_reopen_of_an_arm_task_is_global(r: Rig) -> None:
    r.budget.on_circuit_reopen("md-bars", r.now)
    r.budget.on_circuit_reopen("ingest", r.now)          # not an ARM task: never the ARM's fault
    (f,) = r.budget.take()
    assert f.reason == B.CIRCUIT_REOPEN and f.counters == (("circuit_md_bars", 1),)
    assert r.budget.counters.unknown_circuits == 1


# ---------------------------------------------------------------------------------------------------- ADAPTER
def test_adapter_due_needs_n_series_of_one_adapter() -> None:
    open_series = {"mock": ["mock:A:15m:BID:BROKER", "mock:B:15m:BID:BROKER"], "other": ["other:A:15m:BID:BROKER"]}
    assert B.adapter_due(open_series, set(), 3) == []
    (due,) = B.adapter_due(open_series, set(), 2)
    assert due.adapter_id == "mock" and len(due.series) == 2
    assert B.adapter_due(open_series, {"mock"}, 2) == []


# ---------------------------------------------------------------------------------------------------- loop
def test_the_listener_gets_faults_at_once_and_the_queue_first(r: Rig) -> None:
    r.budget.series_fault(S1, INVALID_BAR, r.now)
    got: list[B.Fault] = []
    r.budget.listen(got.append)
    r.budget.series_fault(S2, INVALID_BAR, r.now)
    assert [f.scope.scope_id for f in got] == [S1.canonical, S2.canonical] and r.budget.take() == []


def test_reset_clears_the_scope_counters(r: Rig) -> None:
    r.unresolved(S1, 1)
    r.budget.reset(B.series_scope(S1))
    r.unresolved(S1, 1)
    assert r.budget.take() == []
    r.budget.observe_md(usable=False, market_open=True, now_ms=r.now)
    r.advance(5_000)
    r.budget.observe_md(usable=False, market_open=True, now_ms=r.now)
    assert dict(r.budget.snapshot())["md_down_s"] == 5
    r.budget.reset(B.ARM_SCOPE)
    assert dict(r.budget.snapshot())["md_down_s"] == 0


def test_an_adapter_reset_keeps_every_series_state(r: Rig) -> None:
    """An ADAPTER scope's only counter is the derived count of its open series: its clear keeps the series'
    unresolved streaks and deferred one-shot faults (§B14 Clearing)."""
    r.unresolved(S1, 1)
    r.calendar.closed.add((ADAPTER, "SYN.B"))
    r.budget.series_fault(S2, INVALID_BAR, r.now)                      # deferred: SYN.B's market is closed
    r.budget.reset(B.ScopeRef(ScopeKind.ADAPTER, ADAPTER))
    assert r.budget.unresolved_streak(S1) == 1 and r.budget.deferred() == (S2.canonical,)


def test_settings_bounds_from_fail_settings() -> None:
    r = Rig(unresolved_n=1)
    r.unresolved(S1, 1)
    assert [f.reason for f in r.budget.take()] == [B.UNRESOLVED]
