"""S36b (plan RT-3): the ARM runtime join end to end on fakes.

The real `ArmTracker`, `BarClock`, S21 pipeline (through `PipelineEvaluator`), `JobRunner`, sink chain and
`ArmNotices` over one in-memory ARM store (`NStore`: positions, decisions, notices and the outbox rows it queued);
the order state comes from fake account images and `event_log` versions, the market data from a fake read port
holding the broker's final bars and the live quote. Synthetic instruments, ids, levels and times only; no
assertion message carries a value.

Scenario: a long position is armed (late: one `armed_late` span); bar B ends below the trigger (POST, a strategy
reason); the forming bar B2 gets a PRE `ADJUST` on the frozen quote; B2 ends above the trigger (POST `ADJUST`,
queued in the ARM outbox with its trigger input); the stop is moved to the target, the PRE of B3 says nothing
(at target: no release at PRE) and only the POST of B3 releases the position (`COMPLETE`, SM11 RELEASED); then
`arm_disarm` is applied and a second one is moot.
"""
from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any

from src.db.arm_ports import EntryTime, HandleVersionRow, PositionKey
from src.marketdata.hashing import Keyring
from src.marketdata.indicators.base import IndicatorSpec
from src.marketdata.ports import BarsRead, QuoteRead
from src.marketdata.timing import COLD_SKEW, pre_fire_ms
from src.marketdata.types import (
    M15,
    Bar,
    CursorExpired,
    DataUnavailable,
    EventCursor,
    LeaseClass,
    Liveness,
    MdGeneration,
    Quote,
    SeriesId,
)
from src.riskmgr.barclock import BarClock, BarClockSettings, EvalRequest, NotReady
from src.riskmgr.config import ArmSettings
from src.riskmgr.evaluation import PipelineEvaluator
from src.riskmgr.jobs import JobRunner
from src.riskmgr.notices import ArmNotices, NoticeSettings, RouteBook
from src.riskmgr.ports import Pager
from src.riskmgr.sinks import DecisionSink, SinkChain
from src.riskmgr.sm11 import ArmState
from src.riskmgr.tracker import AnswerStatus, ArmTracker, TrackerRequest
from src.riskmgr.types import ArmEvent
from src.marketdata.demand import spec_id_of
from tests.riskmgr.barclock_fakes import FakeControls
from tests.riskmgr.failed_fakes import FakeCalendar
from tests.riskmgr.notice_fakes import FakeOutbox, NStore, Policies
from tests.riskmgr.tracker_fakes import (
    ACCOUNT,
    ADAPTER,
    SYMBOL,
    T0,
    FakeClock,
    FakeConfig,
    FakeFailed,
    FakeGrid,
    FakeSnapshot,
    FakeVersions,
    Recorder,
    document,
    image,
    position,
)

D = Decimal
TF = M15.timeframe_ms
FILL = T0 - 3 * TF + 60_000
ARMED_AT = T0 + TF + 6_000
KEYRING = Keyring.of(1, b"k" * 32)


# ---------------------------------------------------------------------------------------------------- market data
@dataclass
class JoinMd:
    """The read port of the join: the lease side (tracker), the md generation (BarClock), the broker's final bars,
    the live quote; indicators are not held (the first-bar guard only applies on bar 0, before the cursor)."""

    generation: MdGeneration = MdGeneration(1, 1)
    bars_by_series: dict[str, list[Bar]] = field(default_factory=dict)
    quote_: Quote | None = None
    quote_history: list[Quote] = field(default_factory=list)
    demands: list[tuple[str, LeaseClass]] = field(default_factory=list)

    def define(self, spec: SeriesId | IndicatorSpec) -> str:
        return spec_id_of(spec)

    async def demand(self, spec_id: str, consumer: str, lease_class: LeaseClass) -> bool:
        self.demands.append((spec_id, lease_class))
        return True

    async def release(self, spec_id: str, consumer: str) -> None:
        return None

    async def md_generation(self) -> MdGeneration:
        return self.generation

    async def quote(self, adapter_id: str, symbol: str) -> QuoteRead | DataUnavailable:
        if self.quote_ is None:
            return DataUnavailable("no_quote")
        return QuoteRead(self.quote_, Liveness.LIVE, self.generation)

    async def quote_at(self, adapter_id: str, symbol: str, at_ms: int) -> QuoteRead | DataUnavailable:
        """The quote as it was at `at_ms` (the hook's quote ring, round 13): the newest one received by then."""
        held = [q for q in self.quote_history if q.received_ms <= at_ms]
        if not held:
            return QuoteRead(None, Liveness.STALE, self.generation)
        return QuoteRead(held[-1], Liveness.LIVE, self.generation)

    async def bars(self, series: SeriesId, *, at_bucket_ms: int | None = None, n: int | None = None,
                   from_bucket_ms: int | None = None, to_bucket_ms: int | None = None,
                   as_of_ms: int | None = None) -> BarsRead | DataUnavailable:
        held = self.bars_by_series.get(series.canonical, [])
        if at_bucket_ms is not None:
            got = [b for b in held if b.bucket_start_ms == at_bucket_ms]
        elif from_bucket_ms is not None and to_bucket_ms is not None:
            got = [b for b in held if from_bucket_ms <= b.bucket_start_ms <= to_bucket_ms]
        else:
            got = held[-(n or 1):]
        return BarsRead(tuple(got), self.generation)

    async def indicator(self, indicator_id: str, output: str, *, at_bucket_ms: int | None = None,
                        n: int | None = None, as_of_ms: int | None = None) -> DataUnavailable:
        return DataUnavailable("indicator_not_held")

    async def events_since(self, series: SeriesId, cursor: EventCursor) -> CursorExpired:
        """No rollback set is held: the BarClock's correction re-check reads by bucket (round 13)."""
        return CursorExpired(self.generation)

    def revise_bar(self, bar_end_ms: int, close: str) -> None:
        """The broker revises bar `T` after it was final (a new close, revision + 1)."""
        series = SeriesId.parse(f"{ADAPTER}:{SYMBOL}:15m:BID:BROKER")
        held = self.bars_by_series[series.canonical]
        c = D(close)
        old = [b for b in held if b.bucket_start_ms == bar_end_ms - TF][-1]
        held.remove(old)
        held.append(replace(old, open=c, high=c, low=c, close=c, revision=old.revision + 1))

    def final_bar(self, bar_end_ms: int, close: str) -> None:
        series = SeriesId.parse(f"{ADAPTER}:{SYMBOL}:15m:BID:BROKER")
        c = D(close)
        bar = Bar(series=series, bucket_start_ms=bar_end_ms - TF, open=c, high=c, low=c, close=c,
                  final_at_ms=bar_end_ms + 1_000)
        self.bars_by_series.setdefault(series.canonical, []).append(bar)

    def live_quote(self, bid: str, received_ms: int) -> None:
        self.quote_ = Quote(adapter_id=ADAPTER, symbol=SYMBOL, bid=D(bid), ask=D(bid) + D("0.0002"),
                            broker_ms=received_ms, received_ms=received_ms)
        self.quote_history.append(self.quote_)


# ---------------------------------------------------------------------------------------------------- the rig
@dataclass
class Join:
    store: NStore
    outbox: FakeOutbox
    clock: FakeClock
    md: JoinMd
    snapshot: FakeSnapshot
    versions: FakeVersions
    rec: Recorder
    tracker: ArmTracker
    barclock: BarClock
    notices: ArmNotices
    evaluator: PipelineEvaluator
    seq: int = 10

    def stop_version(self, level: str, broker_ms: int) -> None:
        """A stream version of the position's `stop_loss` field (`event_log`, PR8)."""
        self.seq += 1
        self.versions.history.setdefault("P1", []).append(HandleVersionRow(
            handle_kind="POSITION_FIELD", handle_id="P1", seq_id=self.seq, broker_time_ms=broker_ms,
            origin="stream", status="OPEN", level=D(level), units=D("1000"), live=True))

    async def at(self, now: int) -> None:
        """One `arm` loop round at `now`: the BarClock's pass, then the notices' timer pass."""
        self.clock.now = now
        await self.barclock.step()
        await self.notices.step()

    def row(self) -> Any:
        return self.store.rows[PositionKey(adapter_id=ADAPTER, position_id="P1")]

    def new_term(self, *, no_trade: Callable[[SeriesId, int], bool] | None = None, recent: bool = False) -> None:
        """A leader change (or a restart): a fresh evaluator and BarClock over the same committed store, with none
        of the previous term's memory (`no_trade`: md-bars' coverage predicate, S38c; `recent`: the BarClock
        rebuilds its correction window from the committed decisions, S39b, as the runtime wires it)."""
        self.barclock.close()
        settings = ArmSettings()
        grid = FakeGrid()
        self.evaluator = PipelineEvaluator(
            md=self.md, snapshot=self.snapshot, versions=self.versions,  # type: ignore[arg-type]
            decisions=self.store, grid=grid, calendar=FakeCalendar(), settings=settings, keyring=lambda: KEYRING,
            no_trade=no_trade)
        self.barclock = BarClock(
            store=self.store, controls=FakeControls(), md=self.md, grid=grid,  # type: ignore[arg-type]
            evaluator=self.evaluator, jobs=JobRunner(), pager=Pager(self.rec.page, lambda t, b: None),
            clock=self.clock, settings=settings, lease_valid=lambda: True,
            sinks=SinkChain((DecisionSink(), self.notices.sink())), notices=self.notices,
            barclock=BarClockSettings(rows_ttl_ms=0), keyring=lambda: KEYRING,
            recent=self.store if recent else None)

    def decisions(self, event: str | None = None) -> list[Any]:
        return [s.row for s in self.store.decisions if event is None or s.row.event == event]


def join_rig() -> Join:
    store = NStore()
    outbox = FakeOutbox()
    store.outbox = outbox
    clock = FakeClock(ARMED_AT)
    md = JoinMd()
    snapshot, versions, rec = FakeSnapshot(), FakeVersions(), Recorder()
    grid = FakeGrid()
    settings = ArmSettings()
    pager = Pager(rec.page, lambda t, b: None)
    notices = ArmNotices(store=store, outbox=outbox, routes=RouteBook({}), policies=Policies(), grid=grid,
                         pager=pager, clock=clock, settings=settings,
                         notices=NoticeSettings(test_at_ms_of_day=86_399_999))
    tracker = ArmTracker(store=store, versions=versions, snapshot=snapshot, md=md,  # type: ignore[arg-type]
                         grid=grid, config=FakeConfig(document()), notices=notices, answers=rec, pager=pager,
                         clock=clock, settings=settings, failed=FakeFailed())
    evaluator = PipelineEvaluator(md=md, snapshot=snapshot, versions=versions,  # type: ignore[arg-type]
                                  decisions=store, grid=grid, calendar=FakeCalendar(), settings=settings,
                                  keyring=lambda: KEYRING)
    barclock = BarClock(store=store, controls=FakeControls(), md=md, grid=grid,  # type: ignore[arg-type]
                        evaluator=evaluator, jobs=JobRunner(), pager=pager, clock=clock, settings=settings,
                        lease_valid=lambda: True, sinks=SinkChain((DecisionSink(), notices.sink())),
                        notices=notices, barclock=BarClockSettings(rows_ttl_ms=0), keyring=lambda: KEYRING)
    j = Join(store, outbox, clock, md, snapshot, versions, rec, tracker, barclock, notices, evaluator)
    versions.entries["P1"] = EntryTime(source="STREAM", broker_time_ms=FILL, seq_id=1, origin="stream")
    j.stop_version("1.1900", FILL + 1_000)
    snapshot.images = [image(position("P1", updated_ms=FILL))]
    return j


def outcomes(rows: Sequence[Any]) -> list[tuple[str, int, str]]:
    return [(r.event, r.bar_end_ms, r.outcome) for r in rows]


# ---------------------------------------------------------------------------------------------------- the test
async def test_arm_pre_post_adjust_notice_release_at_bar_end_and_disarm() -> None:
    j = join_rig()
    try:
        # arming: the late cursor and its one armed_late span, in the position's transaction
        await j.tracker.step()
        await j.notices.step()
        row = j.row()
        assert row.state == ArmState.ARMED.value and row.last_decided_bar is not None
        assert [r.outcome for r in j.decisions()] == ["UNRESOLVED_RANGE"]
        cursor = row.last_decided_bar
        b1 = cursor + TF
        b2 = b1 + TF
        b3 = b2 + TF

        # bar B1 ends below the trigger: POST with a strategy reason (no advice), cursor on B1, state TRACKING
        j.md.final_bar(b1, "1.2050")
        fire2 = pre_fire_ms(b2, COLD_SKEW, ArmSettings().pre_offset_s)
        j.md.live_quote("1.2150", fire2 - 1_000)
        await j.at(fire2 + 100)
        post1 = [r for r in j.decisions("POST") if r.bar_end_ms == b1]
        assert [(r.outcome, r.reason, r.trigger_met) for r in post1] == [("STRATEGY_REASON", "BELOW_TRIGGER", False)]
        assert post1[0].series_inputs and post1[0].inputs_digest is not None and post1[0].close_source == "BROKER"
        assert j.row().state == ArmState.TRACKING.value and j.row().last_decided_bar == b1

        # the forming bar B2: PRE ADJUST on the quote frozen at fire (provisional: no advice, no SM11 change)
        pre2 = [r for r in j.decisions("PRE") if r.bar_end_ms == b2]
        assert [(r.outcome, r.advice, r.close_source) for r in pre2] == [("ADJUST", False, "QUOTE")]
        assert pre2[0].proposed_stop is not None and pre2[0].series_inputs == ()
        assert j.row().state == ArmState.TRACKING.value and j.row().last_decided_bar == b1

        # B2 ends above the trigger: POST ADJUST (advice), queued in the ARM outbox with its trigger input
        j.md.final_bar(b2, "1.2150")
        await j.at(b2 + 2_000)
        post2 = [r for r in j.decisions("POST") if r.bar_end_ms == b2]
        assert [(r.outcome, r.advice, r.trigger_met) for r in post2] == [("ADJUST", True, True)]
        assert j.row().state == ArmState.ADVICE_OUTSTANDING.value and j.row().last_decided_bar == b2
        items = [i for b in j.outbox.bodies("POST") for i in b.items if i.bar_end_ms == b2]
        assert [(i.outcome, i.handles) for i in items] == [("ADJUST", ("P1",))]
        assert items[0].trigger_input is not None and items[0].trigger_input.startswith(f"{ADAPTER}:{SYMBOL}:15m:BID")

        # the user moves the stop to the target: at PRE (at target) nothing, never a release
        j.stop_version("1.2000", b2 + 30_000)
        j.snapshot.images = [image(position("P1", stop="1.2000", updated_ms=b2 + 30_000))]
        fire3 = pre_fire_ms(b3, COLD_SKEW, ArmSettings().pre_offset_s)
        j.md.live_quote("1.2160", fire3 - 1_000)
        await j.at(fire3 + 100)
        assert [r for r in j.decisions("PRE") if r.bar_end_ms == b3] == []
        assert j.row().state != ArmState.STRATEGY_COMPLETE.value

        # the POST of B3 confirms the trigger at target: released (COMPLETE) at the bar-end check only
        j.md.final_bar(b3, "1.2160")
        await j.at(b3 + 2_000)
        post3 = [r for r in j.decisions("POST") if r.bar_end_ms == b3]
        assert [(r.outcome, r.trigger_met) for r in post3] == [("COMPLETE", True)]
        assert j.row().state == ArmState.STRATEGY_COMPLETE.value

        # every decision has a notice id or the suppressed sentinel once the timer pass has run
        await j.at(b3 + 120_000)
        assert all(s.notice_id is not None for s in j.store.decisions if s.row.event != "PRE")

        # arm_disarm through the tracker: applied, then moot
        j.tracker.submit(TrackerRequest.of(41, "arm_disarm", f"{ADAPTER}:P1"))
        await j.tracker.step()
        assert j.row().state == ArmState.DISARMED.value
        j.tracker.submit(TrackerRequest.of(42, "arm_disarm", f"{ADAPTER}:P1"))
        await j.tracker.step()
        assert [(i, a.status, a.reason) for i, a in j.rec.answers] == [
            (41, AnswerStatus.APPLIED, ""), (42, AnswerStatus.MOOT, "not_armed")]
        assert j.rec.pages == [] and j.row().account_id == ACCOUNT
    finally:
        assert j.barclock.close() is False


async def test_the_tracker_views_feed_the_runtime() -> None:
    """`armed_adapters`, `armed_count`, `stop_handles`, `handle_count`, `series_of` and `unarmed` (S36b views)."""
    j = join_rig()
    try:
        await j.tracker.step()
        await j.tracker.step()                                   # the second pass counts the live row's handles
        assert j.tracker.armed_adapters() == (ADAPTER,) and j.tracker.armed_count() == 1
        handles = j.tracker.stop_handles(ADAPTER)
        assert list(handles) == [f"{ADAPTER}:P1"] and [h.kind for h in handles[f"{ADAPTER}:P1"]] == ["POSITION_FIELD"]
        assert j.tracker.handle_count(ADAPTER) == 1 and j.tracker.handle_count("other") == 0
        series = j.tracker.series_of(j.row())
        assert series is not None and any(s.endswith(":BID:BROKER") for s in series)
        assert j.tracker.unarmed(j.clock.now) == 0
    finally:
        j.barclock.close()


async def test_a_failed_scope_holds_the_evaluation() -> None:
    """The evaluator's `held` predicate (a FAILED scope covers the row): `NotReady(failed)`, nothing decided."""
    j = join_rig()
    try:
        await j.tracker.step()
        held = PipelineEvaluator(md=j.md, snapshot=j.snapshot, versions=j.versions,  # type: ignore[arg-type]
                                 decisions=j.store, grid=FakeGrid(),
                                 calendar=FakeCalendar(), settings=ArmSettings(), keyring=lambda: KEYRING,
                                 held=lambda _row: True)
        req = EvalRequest(row=j.row(), spec=M15, event=ArmEvent.POST,
                          bar_end_ms=T0 + 2 * TF, now_ms=j.clock.now, skew=COLD_SKEW,
                          decision_cap_ms=T0 + 3 * TF + 5_000, generation=MdGeneration(1, 1))
        got = await held.prepare(req)
        assert isinstance(got, NotReady) and got.reason == "failed"
    finally:
        j.barclock.close()
