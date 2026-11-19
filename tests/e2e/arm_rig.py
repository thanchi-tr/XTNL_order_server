"""The S38 end-to-end rig for the market-data layer and the ARM (plan S38; design §C3).

Everything below the BarClock is real code; only the edges are stand-ins:

* **market data:** the real `MdWriteClient`, `MdWriter`, `MdBootstrap`, `MdWriterService`, `MdHook` and `MdPort` on
  fakeredis + lupa (the S16 rig, `tests/marketdata/bootrig.py`): bars are queued, flushed with their keyed hash and
  read back through the hash-checked read path; the PRE quote is a tick through the real hook. Only the demand book
  is a stand-in (every lease granted).
* **PostgreSQL:** the real `PgArmStateStore`, `PgArmVersionReader` and `PgArmOutboxInserter` over one
  `ArmEmulator` (`tests/db/arm_emulator.py`, the exact SQL constants with fence, CAS and savepoint semantics), plus
  the ARM outbox INSERT, whose every payload is kept (`E2eEmulator.inserts`) for the no-market-data scan. The
  `event_log` versions are written from real DTOs through the repository's own `dto_json` / `version_labels`.
* **the ARM:** the real `ArmTracker`, `PipelineEvaluator` (the S21 pipeline, kind (a) with FirstBarGuard),
  `JobRunner`, `BarClock`, sink chain and `ArmNotices`.
* **order state:** the tracker's account images come from `PositionDTO`s (the engine-pool snapshot is S36b's
  `PgTrackerSnapshot`, tested there over its SQL).

Synthetic instrument `mock:SYN.A`, synthetic ids, levels and times only. The levels and the planted increment use
digit runs (`PLANT_*`) that appear nowhere else, so the no-market-data test can look for them in every byte the
run wrote.
"""
from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any

import fakeredis

from src.cache.codec import dto_json
from src.cache.keys import position_key
from src.db import arm_outbox_store as O
from src.db.arm_outbox_store import PgArmOutboxInserter
from src.db.arm_ports import PositionKey, is_lost_fence
from src.db.arm_store import PgArmStateStore
from src.db.arm_versions import PgArmVersionReader
from src.db.epoch import EngineDb, Epoch
from src.db.repository import version_labels
from src.dto import PositionDTO, PriceDTO
from src.marketdata.bootstrap import Sm12Event
from src.marketdata.demand import spec_id_of
from src.marketdata.indicators.base import IndicatorSpec, IndicatorValue, Lineage
from src.marketdata.ports import IndicatorRead
from src.marketdata.timing import COLD_SKEW, pre_fire_ms
from src.marketdata.types import M15, Bar, LeaseClass, Quality, SeriesId, Side, Source
from src.riskmgr.barclock import BarClock, BarClockSettings
from src.riskmgr.config import ArmSettings
from src.riskmgr.evaluation import PipelineEvaluator
from src.riskmgr.jobs import JobRunner
from src.riskmgr.notices import ArmNotices, NoticeSettings, RouteBook
from src.riskmgr.ports import Pager
from src.riskmgr.sinks import DecisionSink, SinkChain
from src.riskmgr.sm11 import ArmState
from src.riskmgr.tracker import ArmTracker
from tests.db.arm_emulator import ArmEmulator
from tests.marketdata.bootrig import Rig as MdRig
from tests.marketdata.bootrig import make_rig
from tests.marketdata.conftest import KEYRING, RunIds
from tests.riskmgr.barclock_fakes import FakeControls
from tests.riskmgr.failed_fakes import FakeCalendar
from tests.riskmgr.notice_fakes import Policies
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
    Recorder,
    document,
    image,
    instrument,
)

D = Decimal
TF = M15.timeframe_ms
EPOCH = 7
FILL = T0 - 3 * TF + 60_000
ARMED_AT = T0 + TF + 6_000
SERIES_BID = SeriesId(ADAPTER, SYMBOL, M15, Side.BID, Source.BROKER)
SERIES_ASK = SeriesId(ADAPTER, SYMBOL, M15, Side.ASK, Source.BROKER)
POST_AFTER_MS = COLD_SKEW.settle_ms + 1_000
"""The POST runs on time: after `T + p50 + settle` (cold skew), well before `decision_cap`."""

# Planted digit runs (design §C3 "a planted instrument increment and indicator quantum appear nowhere"). The bar
# closes, the quote and the increment carry these runs; no order-state level (entry, stop, proposed stop) does.
PLANT_INCREMENT = D("0.0000173")
PLANT_QUANTUM = D("0.00000917")
PLANT_ATR = PLANT_QUANTUM * 101  # 0.00092617: an indicator value, a whole number of planted quanta
ENTRY = "1.2000"
STOP0 = "1.1900"
CLOSE_BELOW = "1.20583"          # bar B1: below the trigger (entry + 1R = 1.2100)
CLOSE_ABOVE = "1.21647"          # bar B2: above the trigger
CLOSE_AT_TARGET = "1.21729"      # bar B3: above the trigger, stop at target
QUOTE_PRE2 = "1.21538"
QUOTE_PRE3 = "1.21661"
SPREAD = D("0.00019")
PLANTED = ("0000173", "00000917", "92617", "20583", "21647", "21729", "21538", "21661", "21666", "21748",
           "21557", "21680", "20602")
"""Digit runs of every market value the run sees: the closes, the ASK closes (close + spread), the quotes, the
planted increment, the planted indicator quantum and the indicator value; looked for inside every decimal number
written anywhere."""


# ---------------------------------------------------------------------------------------------------- PostgreSQL
class E2eEmulator(ArmEmulator):
    """`ArmEmulator` plus the ARM outbox INSERT (`SQL_ARM_OUTBOX_INSERT`, fenced, in the notice's transaction)."""

    def __init__(self, epoch: int = EPOCH) -> None:
        super().__init__(epoch)
        self.inserts: list[tuple[Any, ...]] = []
        """Every ARM outbox INSERT's arguments as sent (a rolled-back one too): the INSERT payload."""
        self._h[O.SQL_ARM_OUTBOX_INSERT] = self._arm_outbox_insert
        self.args: list[tuple[str, tuple[Any, ...]]] = []
        """Every statement with its arguments as sent (rolled back or not): what reached PostgreSQL's wire."""

    def _run(self, sql: str, args: tuple[Any, ...]) -> Any:
        self.args.append((sql, args))
        return super()._run(sql, args)

    def _arm_outbox_insert(self, a: tuple[Any, ...]) -> int:
        self._in_tx()
        self.inserts.append(a)
        if not self._fenced(a):
            return 0
        self.t.outbox.append({"created": a[1], "severity": "INFO", "channels": list(a[2]), "title": a[3],
                              "body": a[4], "kind": "arm", "expires": a[5]})
        return 1

    def arm_rows(self) -> list[dict[str, Any]]:
        return [r for r in self.t.outbox if r["kind"] == "arm"]


def engine_db(emu: Any, epoch: int = EPOCH) -> EngineDb:
    """An `EngineDb` over an emulator (`ArmEmulator`, `ArmOutboxEmulator`) fenced at `epoch`."""
    return EngineDb(emu, lambda: Epoch("cluster-e2e", epoch))


def log_position(emu: ArmEmulator, dto: PositionDTO, seq_id: int) -> None:
    """One APPLIED `event_log` version of `dto`, as `PgStateStore.commit` writes it (payload `dto_json`, origin and
    broker time from `version_labels`)."""
    origin, broker_ms = version_labels(dto)
    emu.add_version(entity_type="POSITION", entity_key=position_key(dto.adapter_id, dto.position_id), seq_id=seq_id,
                    payload=json.loads(dto_json(dto)), origin=origin, broker_time_ms=broker_ms)


def labelled(dto: PositionDTO, broker_ms: int, origin: str = "stream") -> PositionDTO:
    return replace(dto, broker_updated_at_ms=broker_ms, origin=origin,  # type: ignore[arg-type]
                   broker_time_source="BROKER")


# ---------------------------------------------------------------------------------------------------- market data
@dataclass
class Demand:
    """The demand book (S18) stand-in: every lease granted, each one recorded."""

    leases: list[tuple[str, LeaseClass]] = field(default_factory=list)

    def define(self, spec: SeriesId | IndicatorSpec) -> str:
        return spec_id_of(spec)

    async def demand(self, spec_id: str, consumer: str, lease_class: LeaseClass) -> bool:
        self.leases.append((spec_id, lease_class))
        return True

    async def release(self, spec_id: str, consumer: str) -> None:
        return None


class PlantedIndicators:
    """`MarketDataPort.indicator` serving one planted value (a whole number of `PLANT_QUANTUM`) for every id and
    bucket asked, so the evaluator's indicator reads carry a market value the no-market-data test can look for."""

    def __init__(self, md: MdRig) -> None:
        self._md = md
        self.reads = 0

    async def __call__(self, indicator_id: str, output: str, *, at_bucket_ms: int | None = None,
                       n: int | None = None, as_of_ms: int | None = None) -> IndicatorRead:
        self.reads += 1
        bucket = at_bucket_ms if at_bucket_ms is not None else T0
        generation = await self._md.port.md_generation()
        value = IndicatorValue(indicator_id=indicator_id, output=output, bucket_start_ms=bucket, value=PLANT_ATR,
                               quality=Quality.COMPLETE, warm=True, final_at_ms=bucket + TF + 1_000,
                               lineage=Lineage(indicator_id, bucket - 13 * TF, "d" * 16, 0))
        return IndicatorRead((value,), generation)  # type: ignore[arg-type]


def broker_bar(series: SeriesId, bar_end_ms: int, close: str) -> Bar:
    c = D(close)
    return Bar(series=series, bucket_start_ms=bar_end_ms - TF, open=c - PLANT_INCREMENT, high=c + PLANT_INCREMENT,
               low=c - 2 * PLANT_INCREMENT, close=c, tick_count=9, quality=Quality.COMPLETE,
               final_at_ms=bar_end_ms + 1_000)


# ---------------------------------------------------------------------------------------------------- the rig
@dataclass
class E2e:
    emu: E2eEmulator
    md: MdRig
    demand: Demand
    clock: FakeClock
    snapshot: FakeSnapshot
    rec: Recorder
    tracker: ArmTracker
    barclock: BarClock
    notices: ArmNotices
    evaluator: PipelineEvaluator
    store: PgArmStateStore
    versions: PgArmVersionReader
    seq: int = 10
    stop: str = STOP0
    notice_errors: int = 0
    tracker_errors: int = 0

    # -- time ---------------------------------------------------------------------------------------------------
    async def at(self, now: int) -> None:
        """One `arm` loop round at `now` as the runtime makes it (`optional_plan`): the md writer's step, the
        BarClock's `guarded_step` (an error is counted, a lost fence propagates), the notices' timer pass (an error is
        counted, a lost fence propagates)."""
        self.clock.now = now
        self.md.clock.now = now
        await self.md.svc.step()
        await self.barclock.guarded_step()
        try:
            await self.notices.step()
        except Exception as exc:
            if is_lost_fence(exc):
                raise
            self.notice_errors += 1

    async def tracker_round(self) -> None:
        """One `arm-tracker` pass as `ArmTracker.run` makes it (an error is counted, a lost fence propagates)."""
        try:
            await self.tracker.step()
        except Exception as exc:
            if is_lost_fence(exc):
                raise
            self.tracker_errors += 1

    def new_term(self, *, md: MdRig | None = None, epoch: int = EPOCH) -> E2e:
        """A new leader term (a restart or a takeover): a fresh ARM graph with none of this term's memory over the
        same committed ARM tables (and `md`, or this term's market data). This term's graph is left as it is (a
        takeover drill drives it on to see it fenced)."""
        md = md or self.md
        term = build_term(self.emu, md, self.clock, self.snapshot, self.rec, epoch=epoch)
        return E2e(self.emu, md, md.port._demand, self.clock, self.snapshot, self.rec,  # type: ignore[arg-type]
                   term.tracker, term.barclock, term.notices, term.evaluator, term.store, term.versions,
                   seq=self.seq, stop=self.stop)

    # -- broker side ----------------------------------------------------------------------------------------------
    def position(self, stop: str | None = None, updated_ms: int = FILL) -> PositionDTO:
        from tests.riskmgr.tracker_fakes import position
        return position("P1", entry=ENTRY, stop=stop if stop is not None else self.stop, updated_ms=updated_ms)

    def stream_stop(self, level: str, broker_ms: int) -> None:
        """The stream reports a stop at `level` (`event_log` version, origin `stream`) and the account image shows
        it."""
        self.seq += 1
        self.stop = level
        dto = labelled(self.position(level, broker_ms), broker_ms)
        log_position(self.emu, dto, self.seq)
        self.snapshot.images = [image(self.position(level, broker_ms), instruments=(planted_instrument(),))]

    # -- market side ----------------------------------------------------------------------------------------------
    async def final_bars(self, bar_end_ms: int, close: str) -> None:
        """The broker's final BID and ASK bars of the bucket ending at `bar_end_ms` (md-bars' output), flushed."""
        self.md.writer.queue_bar(broker_bar(SERIES_BID, bar_end_ms, close))
        self.md.writer.queue_bar(broker_bar(SERIES_ASK, bar_end_ms, str(D(close) + SPREAD)))
        self.md.clock.now = max(self.md.clock.now, bar_end_ms + 1_500)
        await self.md.svc.step()

    def tick(self, bid: str, received_ms: int) -> None:
        b = D(bid)
        self.md.hook.on_price(PriceDTO(adapter_id=ADAPTER, symbol=SYMBOL, bid=b, ask=b + SPREAD, spread_pips=None,
                                       timestamp_ms=received_ms - 40, ts_source="BROKER", received_ms=received_ms))

    # -- views ----------------------------------------------------------------------------------------------------
    def row(self) -> dict[str, Any]:
        return next(r for k, r in self.emu.t.positions.items() if r["position_id"] == "P1")

    def decisions(self, event: str | None = None) -> list[dict[str, Any]]:
        return sorted((d for d in self.emu.t.decisions.values() if event is None or d["event"] == event),
                      key=lambda d: d["decision_id"])

    def key(self) -> PositionKey:
        return PositionKey(adapter_id=ADAPTER, position_id="P1")


def planted_instrument() -> Any:
    return instrument(increment=PLANT_INCREMENT)


@dataclass
class Term:
    """One leader term's ARM graph (the S36b wiring) over the given md rig and ARM pool."""

    store: PgArmStateStore
    versions: PgArmVersionReader
    tracker: ArmTracker
    barclock: BarClock
    notices: ArmNotices
    evaluator: PipelineEvaluator


def build_term(emu: E2eEmulator, md: MdRig, clock: FakeClock, snapshot: FakeSnapshot, rec: Recorder, *,
               epoch: int = EPOCH) -> Term:
    db = engine_db(emu, epoch)
    store, versions = PgArmStateStore(db), PgArmVersionReader(db)
    grid = FakeGrid()
    settings = ArmSettings()
    pager = Pager(rec.page, lambda t, b: None)
    notices = ArmNotices(store=store, outbox=PgArmOutboxInserter(), routes=RouteBook({}), policies=Policies(),
                         grid=grid, pager=pager, clock=clock, settings=settings,
                         notices=NoticeSettings(test_at_ms_of_day=86_399_999))
    tracker = ArmTracker(store=store, versions=versions, snapshot=snapshot, md=md.port,
                         grid=grid, config=FakeConfig(document()), notices=notices, answers=rec, pager=pager,
                         clock=clock, settings=settings, failed=FakeFailed())
    evaluator = PipelineEvaluator(md=md.port, snapshot=snapshot, versions=versions,
                                  decisions=store, grid=grid, calendar=FakeCalendar(), settings=settings,
                                  keyring=lambda: KEYRING)
    barclock = BarClock(store=store, controls=FakeControls(), md=md.port, grid=grid,
                        evaluator=evaluator, jobs=JobRunner(), pager=pager, clock=clock, settings=settings,
                        lease_valid=lambda: True, sinks=SinkChain((DecisionSink(), notices.sink())),
                        notices=notices, barclock=BarClockSettings(rows_ttl_ms=0))
    return Term(store, versions, tracker, barclock, notices, evaluator)


async def md_term(redis: Any, run_ids: RunIds, *, start: int, token: Any = None,
                  previous_term_epoch: int | None = None, records: Any = None) -> MdRig:
    """One term's market-data services over `redis` (the S16 rig), booted, both BROKER sides LIVE, the demand book
    and the planted indicator reads in place."""
    kw: dict[str, Any] = {} if token is None else {"token": token}
    if records is not None:
        kw["records"] = records                                    # this node's `arm_status` md record (S16)
    md = await make_rig(redis, run_ids, start=start, previous_term_epoch=previous_term_epoch, **kw)
    await md.at(start)                                             # term-start bootstrap: md:boot written
    for s in (SERIES_BID, SERIES_ASK):
        md.book.apply(s, Sm12Event.LIVE)
    md.port._demand = Demand()                                     # the demand book stand-in (S18's is real)
    md.port.indicator = PlantedIndicators(md)                      # type: ignore[method-assign]
    return md


async def e2e_rig(*, redis: Any | None = None, run_id: str | None = None) -> E2e:
    """The rig at `ARMED_AT`: the md store booted and LIVE for both BROKER sides, P1 OPEN since `FILL` with a
    stream stop, nothing armed yet. `redis` defaults to fakeredis; a real `redis-md` (as `md_writer`) needs its
    `run_id` (`INFO server`)."""
    emu = E2eEmulator()
    r = redis if redis is not None else fakeredis.FakeAsyncRedis()
    md = await md_term(r, RunIds() if run_id is None else RunIds(run_id), start=FILL - 2 * TF)
    clock = FakeClock(ARMED_AT)
    snapshot, rec = FakeSnapshot(), Recorder()
    term = build_term(emu, md, clock, snapshot, rec)
    e = E2e(emu, md, md.port._demand, clock, snapshot, rec, term.tracker, term.barclock,  # type: ignore[arg-type]
            term.notices, term.evaluator, term.store, term.versions)
    open_dto = labelled(e.position(STOP0, FILL), FILL)
    log_position(emu, open_dto, 1)                                 # the OPEN version: entry time STREAM
    e.stream_stop(STOP0, FILL + 1_000)
    return e


async def run_main_scenario(e: E2e) -> dict[str, int]:
    """arm → POST (strategy reason) → PRE ADJUST → POST ADJUST → the stop moved to target → PRE silent → POST
    COMPLETE. Returns the bar ends."""
    await e.tracker_round()
    await e.notices.step()
    row = e.row()
    assert row["state"] == ArmState.ARMED.value and row["last_decided_bar"] is not None
    b1 = int(row["last_decided_bar"]) + TF
    b2, b3 = b1 + TF, b1 + 2 * TF
    settings = ArmSettings()

    await e.final_bars(b1, CLOSE_BELOW)
    e.tick(CLOSE_BELOW, b1 + POST_AFTER_MS - 500)           # the live exit quote of the POST
    await e.at(b1 + POST_AFTER_MS)
    fire2 = pre_fire_ms(b2, COLD_SKEW, settings.pre_offset_s)
    e.tick(QUOTE_PRE2, fire2 - 1_000)
    await e.at(fire2 + 100)

    await e.final_bars(b2, CLOSE_ABOVE)
    e.tick(CLOSE_ABOVE, b2 + POST_AFTER_MS - 500)           # the live exit quote of the POST
    await e.at(b2 + POST_AFTER_MS)

    e.stream_stop("1.2000", b2 + 30_000)                    # the user moves the stop to the target (break even)
    fire3 = pre_fire_ms(b3, COLD_SKEW, settings.pre_offset_s)
    e.tick(QUOTE_PRE3, fire3 - 1_000)
    await e.at(fire3 + 100)

    await e.final_bars(b3, CLOSE_AT_TARGET)
    e.tick(CLOSE_AT_TARGET, b3 + POST_AFTER_MS - 500)           # the live exit quote of the POST
    await e.at(b3 + POST_AFTER_MS)
    await e.at(b3 + 120_000)                                 # the notices' timer pass catches up
    return {"b1": b1, "b2": b2, "b3": b3}


def bodies(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [json.loads(r["body"]) for r in rows]
