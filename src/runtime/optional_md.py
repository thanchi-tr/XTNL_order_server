"""The market-data layer's object graph for one term (design §A2, §A4, §A6-§A8, §A13; plan S31).

`build_md_services` wires the S12-S18 components exactly as their stages left for S31, and nothing runs here: the
supervised tasks that drive them (`md-writer`, `md-bars`, `md-indicators`) are added by the `optional-bootstrap`
task through its plan (S32 builds their specs, cadences and DRAINING behaviour). The wiring obligations:

* S14: the tick hook (`MdHook.on_price`) is bound late to the session's composite price hook, and the open price
  intervals and the adapters' own `dropped_price_frames` are seeded when it binds (`bind_tick_hook`), else the first
  buckets would read `partial`; the per-market SUBSCRIBED intervals reach `MdHook.on_price_subscription`.
* S15: `md-writer` owns `MdHook.advance` and the clock-step detector (nothing here calls them).
* S16: `MdPort.wake` and the service's own listener on `MdWriter.on_released`; `RefetchQueue` as the writer's
  `refetch` (and the read port's queue); the writer built with `boot=None` (the service runs the term-start
  bootstrap); `previous_term_epoch` passed; this node's record through `NodeRecordPortImpl`.
* S17a: `MdWriter` as the sink, `StoreBarsReader` over the reader, the term's `SeriesStateBook`, the same
  `RefetchQueue`, the health port, the hook's ClockSkew estimate, `trusts_partial_flag` False until PR3, faults into
  the error budget (`FaultRelay`), completions into the operator-request answers, `global_failed`.
* S17b (D6): one `TickRefBook` per term fed by `MdWriter.on_closed_tick`, one `CrossChecker` with the same fault
  sink as `md-bars`, passed to `MdBars` as `crosscheck`.
* S18: one `DemandBook` (BARS cap per adapter; the stop-read rate: 0 with `XTNL_ARM_STOP_READS` off, else the
  armed-handle cost S32's tracker binds through `OptionalBootstrapRunner.stop_read_cost`, 0 until it does;
  `host` = the indicator engine) and one
  `MdIndicators` (bars through the read port, writes over the writer client with the writer's token and sentinel,
  `blind` = the md-bars backfill lane, `each_cycle` = the lease sync); `MdWriter.on_released` and
  `MdBars.on_resolved` feed the engine, its updates wake the read port; `book.broker_demand` is md-bars' demand,
  `book.is_demanded` the writer's, `book` the bootstrap's lease source; the read port gets `demand_book` and
  `indicators`.

Faults are relayed (`FaultRelay`) to the S30 error budget once the ARM graph binds it (S32); until then they are
counted and logged by code. The relay also carries md-bars' bucket notes (`note_unresolved` / `note_final` /
`note_no_trade`, plan S38e: the D7 `fail_unresolved_n` streak), dropped (counted) while no budget is bound. S38e also
gives `md-writer` the widest declared backfill per BROKER series (`broker_keep_ms`, `RETAIN_BROKER` keeps at least 1
year) and the demand book `md-writer`'s measured memory ratio (the memory ceiling). The ARM-global FAILED flag `md-bars`, `md-indicators` and the read port obey (§B14
Effect; `md-bars` and `md-indicators` still run the `md_backfill` / `md_rebuild` repairs, §B13) is late-bound too
(`LateFlag`, bound by the S32 ARM graph to `FailedManager.summary().global_failed`; False while the ARM is off).
Nothing here logs or pages a price.
"""
from __future__ import annotations

import logging
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from src.marketdata.bootstrap import MdBootstrap, MdWriterService, SeriesStateBook
from src.marketdata.brokerbars import BrokerBarsSettings, MdBars, StoreBarsReader
from src.marketdata.clockstep import SUSPEND_AFTER_MS
from src.marketdata.config import MdSettings
from src.marketdata.crosscheck import CrossChecker, TickRefBook
from src.marketdata.demand import DemandBook, DemandSettings
from src.marketdata.events import RefetchQueue
from src.marketdata.indicators.engine import MdIndicators
from src.marketdata.port_impl import MdPort
from src.marketdata.ports import AdapterHealthPort, BarsRead, BrokerBarsPort, CalendarPort, NodeRecordPort
from src.marketdata.rehash import MdRehash
from src.marketdata.store.client import MdClients
from src.marketdata.store.reads import StoreReader
from src.marketdata.ticks import MdHook
from src.marketdata.types import DataUnavailable, SeriesId
from src.marketdata.writer import MdWriter
from src.runtime.md_ports import LeaderCheckPortImpl, PagerWrapper
from src.runtime.request_handlers import Applied, Moot, Refused, RequestResult

if TYPE_CHECKING:
    from src.marketdata.store import keys as K

log = logging.getLogger(__name__)

FAULT_BUFFER: Final = 256


# ---------------------------------------------------------------------------------------------------- faults
class FaultTarget(Protocol):
    """The S30 `ErrorBudget` methods the market-data faults feed."""

    def series_fault(self, series: SeriesId, reason: str, now_ms: int) -> object: ...

    def store_fault(self, reason: str, now_ms: int) -> None: ...

    def indicator_fault(self, indicator_id: str, series: SeriesId, reason: str, now_ms: int) -> None: ...


@runtime_checkable
class BucketTarget(Protocol):
    """The error budget's bucket counter (S38e: `ErrorBudget.note_unresolved` / `note_final` / `note_no_trade`)."""

    def note_unresolved(self, series: SeriesId, bucket_ms: int, cap_ms: int, now_ms: int) -> None: ...

    def note_final(self, series: SeriesId, bucket_ms: int) -> None: ...

    def note_no_trade(self, series: SeriesId, bucket_ms: int) -> None: ...


class FaultRelay:
    """The one fault sink of the market-data layer: `md-bars` and the cross-check (series faults), the bootstrap
    (store faults: `md_resets`, `bootstrap_failed`) and `md-indicators` (an indicator fault: its own guard-input
    scope `adapter:symbol:ind:<indicator_id>`, never its input series, round 13). Until the ARM graph binds the error budget, faults are counted, logged by
    code and kept (the newest `FAULT_BUFFER`), then handed over on `bind`."""

    def __init__(self, clock_ms: Callable[[], int], *,
                 input_series: Callable[[str], tuple[SeriesId, ...] | None] | None = None) -> None:
        self._clock_ms = clock_ms
        self._input_series = input_series
        self._target: FaultTarget | None = None
        self._pending: deque[tuple[SeriesId | None, str, int, str | None]] = deque(maxlen=FAULT_BUFFER)
        self.counts: dict[str, int] = {}

    def set_input_series(self, input_series: Callable[[str], tuple[SeriesId, ...] | None]) -> None:
        self._input_series = input_series

    def bind(self, target: FaultTarget | None) -> None:
        self._target = target
        if target is None:
            return
        while self._pending:
            series, reason, at, indicator = self._pending.popleft()
            self._deliver(series, reason, at, indicator)

    @property
    def bound(self) -> bool:
        return self._target is not None

    def _deliver(self, series: SeriesId | None, reason: str, at: int, indicator: str | None = None) -> None:
        target = self._target
        if target is None:
            self._pending.append((series, reason, at, indicator))
            return
        try:
            if series is None:
                target.store_fault(reason, at)
            elif indicator is not None:
                target.indicator_fault(indicator, series, reason, at)
            else:
                target.series_fault(series, reason, at)
        except Exception as exc:
            log.warning("market-data fault %s not taken by the error budget (%s)", reason, type(exc).__name__)

    def _note(self, series: SeriesId | None, reason: str, indicator: str | None = None) -> None:
        self.counts[reason] = self.counts.get(reason, 0) + 1
        what = "store" if series is None else series.canonical if indicator is None else f"indicator {indicator}"
        log.warning("market-data fault %s on %s", reason, what)
        self._deliver(series, reason, self._clock_ms(), indicator)

    def series(self, series: SeriesId, reason: str) -> None:
        self._note(series, reason)

    # md-bars' bucket notes (S38e, D7 `fail_unresolved_n`): forwarded to the bound budget, else dropped and counted
    def _bucket(self, call: Callable[[], object] | None) -> None:
        if call is None:
            self.counts["bucket_notes_dropped"] = self.counts.get("bucket_notes_dropped", 0) + 1
            return
        try:
            call()
        except Exception as exc:
            log.warning("market-data bucket note not taken by the error budget (%s)", type(exc).__name__)

    def note_unresolved(self, series: SeriesId, bucket_ms: int, cap_ms: int, now_ms: int) -> None:
        t = self._target
        self._bucket(None if not isinstance(t, BucketTarget) else
                     lambda: t.note_unresolved(series, bucket_ms, cap_ms, now_ms))

    def note_final(self, series: SeriesId, bucket_ms: int) -> None:
        t = self._target
        self._bucket(None if not isinstance(t, BucketTarget) else lambda: t.note_final(series, bucket_ms))

    def note_no_trade(self, series: SeriesId, bucket_ms: int) -> None:
        t = self._target
        self._bucket(None if not isinstance(t, BucketTarget) else lambda: t.note_no_trade(series, bucket_ms))

    def store(self, reason: str) -> None:
        self._note(None, reason)

    def indicator(self, indicator_id: str, reason: str) -> None:
        """An indicator fault goes to the indicator's own scope (one per (adapter, symbol) of its inputs), never
        to its input series (round 13): the input series stays readable for every trigger."""
        series = self._input_series(indicator_id) if self._input_series is not None else None
        seen: set[tuple[str, str]] = set()
        for s in series or ():
            if (s.adapter_id, s.symbol) in seen:
                continue
            seen.add((s.adapter_id, s.symbol))
            self._note(s, reason, indicator_id)
        if not series:
            self.counts[reason] = self.counts.get(reason, 0) + 1
            log.warning("market-data fault %s on an indicator without known inputs", reason)


class LateFlag:
    """A boolean read late-bound to a source (the ARM's `FailedManager.summary().global_failed`, S32: the ARM
    scope FAILED or the open episodes unreadable), read by `md-bars`, `md-indicators` and the read port. False while
    unbound; a raising source reads True (fail closed: no market data is served on an unknown FAILED state)."""

    __slots__ = ("_source",)

    def __init__(self) -> None:
        self._source: Callable[[], bool] | None = None

    def bind(self, source: Callable[[], bool] | None) -> None:
        self._source = source

    @property
    def bound(self) -> bool:
        return self._source is not None

    def __call__(self) -> bool:
        source = self._source
        if source is None:
            return False
        try:
            return bool(source())
        except Exception:
            return True


# ---------------------------------------------------------------------------------------------------- completions
def completion(complete: Callable[[int, RequestResult], bool]) -> Callable[[int, str, str], None]:
    """The market-data `complete(request_id, outcome, reason)` callback over `EngineSession.complete_request`."""

    def done(request_id: int, outcome: str, reason: str) -> None:
        result: RequestResult
        try:
            if outcome == "applied":
                result = Applied()
            elif outcome == "moot":
                result = Moot(reason or "moot")
            else:
                result = Refused(reason or "refused")
        except ValueError:
            result = Refused("refused")
        if not complete(request_id, result):
            log.info("request %d answered after it left this session", request_id)

    return done


class _LateBars:
    """The indicator engine's bar reads go through the read port, which itself needs the engine (indicator reads):
    this forwards to the port once both exist."""

    def __init__(self) -> None:
        self.port: MdPort | None = None

    async def bars(self, series: SeriesId, *, at_bucket_ms: int | None = None, n: int | None = None,
                   from_bucket_ms: int | None = None, to_bucket_ms: int | None = None,
                   as_of_ms: int | None = None) -> BarsRead | DataUnavailable:
        if self.port is None:
            return DataUnavailable("not_live")
        return await self.port.bars(series, at_bucket_ms=at_bucket_ms, n=n, from_bucket_ms=from_bucket_ms,
                                    to_bucket_ms=to_bucket_ms, as_of_ms=as_of_ms)


# ---------------------------------------------------------------------------------------------------- the graph
@dataclass(frozen=True)
class MdInputs:
    """What the graph needs from the session and the bootstrap (ports already wrapped)."""

    node: str
    token: K.WriterToken
    leader: LeaderCheckPortImpl
    leader_epoch: Callable[[], int | None]
    pager: PagerWrapper
    bars_port: BrokerBarsPort
    health: AdapterHealthPort
    calendar: CalendarPort
    complete: Callable[[int, str, str], None]
    clock_ms: Callable[[], int]
    records: NodeRecordPort | None = None
    previous_term_epoch: int | None = None
    bars_per_min: Callable[[str], int | None] | None = None
    stop_reads_per_min: Callable[[str], int] | None = None
    unfiltered: tuple[str, ...] = ()
    global_failed: Callable[[], bool] | None = None
    """None: a fresh `LateFlag` (the S32 ARM graph binds it)."""


@dataclass
class MdServices:
    settings: MdSettings
    clients: MdClients
    hook: MdHook
    refetch: RefetchQueue
    writer: MdWriter
    book: SeriesStateBook
    bootstrap: MdBootstrap
    rehash: MdRehash
    service: MdWriterService
    port: MdPort
    tick_refs: TickRefBook
    crosscheck: CrossChecker
    bars: MdBars
    demand: DemandBook
    indicators: MdIndicators
    faults: FaultRelay
    global_failed: LateFlag = field(default_factory=LateFlag)
    extras: dict[str, object] = field(default_factory=dict)


def build_md_services(settings: MdSettings, clients: MdClients, inp: MdInputs) -> MdServices:
    """Build (no I/O) the market-data graph of one term over `clients` (the `md_writer` connections)."""
    keyring = settings.keyring
    write_client = clients.writer
    reader = StoreReader(clients.reader.redis)
    faults = FaultRelay(inp.clock_ms)
    flag = LateFlag()
    if inp.global_failed is not None:
        flag.bind(inp.global_failed)
    global_failed: Callable[[], bool] = flag
    hook = MdHook(clock_ms=inp.clock_ms, unfiltered=inp.unfiltered)
    refetch = RefetchQueue()
    late_demand: list[DemandBook] = []

    def is_demanded(series: SeriesId) -> bool:
        return late_demand[0].is_demanded(series) if late_demand else False

    def broker_demand() -> Mapping[SeriesId, int]:
        return late_demand[0].broker_demand() if late_demand else {}

    def broker_keep_ms(series: SeriesId) -> int:
        """The widest declared backfill of a demanded BROKER series in ms (0 when undemanded: 1 year applies)."""
        return int(broker_demand().get(series, 0)) * series.spec.timeframe_ms

    writer = MdWriter(hook, write_client, token=inp.token, keyring=keyring, boot=None, memory=write_client,
                      pager=inp.pager, rollback_n=settings.rollback_n, refetch=refetch, is_demanded=is_demanded,
                      clock_ms=inp.clock_ms, broker_keep_ms=broker_keep_ms,
                      suspend_after_ms=SUSPEND_AFTER_MS)   # round 14: a Windows sleep blinds its TICK buckets
    book = SeriesStateBook()
    rehash = MdRehash(writer, write_client, reader, keyring=keyring, clock_ms=inp.clock_ms)

    # D6 (S17b): one tick-reference book per term, fed by the writer's closed TICK bars.
    tick_refs = TickRefBook()
    writer.on_closed_tick(tick_refs.note)
    crosscheck = CrossChecker(tick_refs, k=settings.crosscheck_k, mismatch_k=settings.mismatch_k, pager=inp.pager,
                              on_fault=faults.series)
    md_bars = MdBars(inp.bars_port, writer, book, keyring=keyring, demand=broker_demand,
                     settings=BrokerBarsSettings.from_md(settings), stored=StoreBarsReader(reader),
                     health=inp.health, calendar=inp.calendar, pager=inp.pager, skew=hook.skew_estimate,
                     trusts_partial_flag=lambda _adapter: False, trusts_next_bucket=lambda _adapter: False,
                     refetch=refetch, on_fault=faults.series,
                     complete=inp.complete, global_failed=global_failed, crosscheck=crosscheck, buckets=faults,
                     clock_ms=inp.clock_ms)

    late_bars = _LateBars()

    async def lease_sync() -> object:
        return await late_demand[0].sync(write_client, writer.token) if late_demand else 0

    indicators = MdIndicators(late_bars, write=write_client, token=lambda: writer.token, boot=lambda: writer.boot,
                              blind=md_bars.lane, pager=inp.pager, on_fault=faults.indicator,
                              complete=inp.complete, clock_ms=inp.clock_ms, each_cycle=lease_sync,
                              global_failed=global_failed)
    faults.set_input_series(indicators.input_series)
    demand = DemandBook(settings=DemandSettings(decision_symbols_per_adapter=settings.decision_symbols_per_adapter),
                        bars_per_min=inp.bars_per_min, stop_reads_per_min=inp.stop_reads_per_min, host=indicators,
                        memory_ratio=lambda: writer.memory_ratio, clock_ms=inp.clock_ms)
    late_demand.append(demand)

    boot = MdBootstrap(writer, write_client, reader, keyring=keyring, node=inp.node, leader=inp.leader, book=book,
                       records=inp.records, leases=demand, on_fault=faults.store,
                       previous_term_epoch=inp.previous_term_epoch, clock_ms=inp.clock_ms)
    service = MdWriterService(writer, boot, rehash=rehash, complete=inp.complete)
    port = MdPort(StoreReader(clients.reader.redis), keyring=keyring, book=book, leader_epoch=inp.leader_epoch,
                  hook=hook, quarantine_sink=writer.quarantine_stored, refetch_queue=refetch,
                  global_failed=global_failed, clock_ms=inp.clock_ms, demand_book=demand, indicators=indicators)
    late_bars.port = port
    writer.on_released(port.wake)
    writer.on_released(indicators.on_released)
    indicators.on_updated(port.wake_indicator)
    md_bars.on_resolved(indicators.on_resolved)
    return MdServices(settings=settings, clients=clients, hook=hook, refetch=refetch, writer=writer, book=book,
                      bootstrap=boot, rehash=rehash, service=service, port=port, tick_refs=tick_refs,
                      crosscheck=crosscheck, bars=md_bars, demand=demand, indicators=indicators, faults=faults,
                      global_failed=flag)


def seed_tick_hook(hook: MdHook, adapter_ids: tuple[str, ...], subscriptions: Callable[[str], Mapping[str, int]],
                   drops: Callable[[str], Mapping[str, int] | None], now_ms: int) -> None:
    """S14 obligation 7: the open price intervals and a first drop reading per adapter, when the hook binds."""
    for aid in adapter_ids:
        try:
            hook.seed_subscriptions(aid, subscriptions(aid))
        except Exception as exc:
            log.warning("%s: price intervals not seeded (%s)", aid, type(exc).__name__)
        try:
            totals = drops(aid)
            if totals is not None:
                hook.note_drop_totals(aid, totals, now_ms)
        except Exception as exc:
            log.warning("%s: drop totals not seeded (%s)", aid, type(exc).__name__)


__all__ = ["FaultRelay", "FaultTarget", "LateFlag", "MdInputs", "MdServices", "build_md_services", "completion",
           "seed_tick_hook"]
