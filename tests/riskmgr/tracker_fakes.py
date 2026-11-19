"""In-memory fakes for the arm-tracker tests (S26). Synthetic instruments, ids and levels only."""
from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from decimal import Decimal

from src.armconfig.validate import Assignment, StrategyEntry, ValidDocument
from src.db.arm_ports import (
    TERMINAL_ARM_STATES,
    ArmPositionRow,
    DecisionHandleRow,
    DecisionRef,
    DecisionRow,
    DecisionWrite,
    DecisionWriteStatus,
    EntryTime,
    HandleVersionRow,
    PositionCas,
    PositionKey,
)
from src.dto import InstrumentSpec, OrderDTO, OrderStatus, PositionDTO
from src.marketdata.demand import spec_id_of
from src.marketdata.indicators.base import IndicatorSpec
from src.marketdata.timing import COLD_SKEW, SkewEstimate
from src.marketdata.types import BarSpec, LeaseClass, MdGeneration, SeriesId
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.config import ArmSettings
from src.riskmgr.ports import Pager
from src.riskmgr.registry import REGISTRY, ArmRegistry
from src.riskmgr.tracker import (
    AccountImage,
    ArmTracker,
    RequestAnswer,
    TrackerNotice,
    TrackerSettings,
)

D = Decimal
ADAPTER = "mock"
ACCOUNT = "acct1"
SYMBOL = "SYN.A"
T0 = 1_700_000_100_000 - 1_700_000_100_000 % 900_000   # a 15m bucket start
DOC_HASH = "ab" * 32
KIND_A = "break_even_on_risk_close"
FIRST_BAR = {"kind": "first_bar", "version": 1, "params": {}}


# ---------------------------------------------------------------------------------------------------- documents
def strategy_entry(code: str = "be", version: int = 1, *, registry: ArmRegistry = REGISTRY, kind: str = KIND_A,
                   impl: int = 1, raw: dict[str, object] | None = None) -> StrategyEntry:
    params = registry.parse_params(kind, impl, raw if raw is not None else {"guards": [FIRST_BAR]})
    return StrategyEntry(row_id=version, code=code, version=version, params=params, resource="STOP_LEVEL")


def assignment(row_id: int = 1, *, code: str = "be", version: int = 1, adapter: str | None = ADAPTER,
               symbol: str | None = None, edge: str | None = None, enabled: bool = True,
               effective_from_ms: int = 0) -> Assignment:
    return Assignment(row_id=row_id, edge_code=edge, symbol=symbol, account=None, adapter=adapter,
                      strategy_code=code, strategy_version=version, resource="STOP_LEVEL", enabled=enabled,
                      effective_from_ms=effective_from_ms)


def document(*, strategies: Sequence[StrategyEntry] | None = None,
             assignments: Sequence[Assignment] | None = None, doc_hash: str = DOC_HASH) -> ValidDocument:
    return ValidDocument(schema_version=1, doc_hash=doc_hash,
                         strategies=tuple(strategies if strategies is not None else (strategy_entry(),)),
                         assignments=tuple(assignments if assignments is not None else (assignment(),)),
                         route_policies=(), row_hashes=())


# ---------------------------------------------------------------------------------------------------- entities
def instrument(symbol: str = SYMBOL, *, increment: Decimal | None = D("0.0001"),
               asset_class: str | None = "fx") -> InstrumentSpec:
    return InstrumentSpec(adapter_id=ADAPTER, symbol=symbol, base_ccy="AAA", quote_ccy="BBB", pnl_ccy="BBB",
                          contract_size=D(1), unit_kind="UNITS", point_size=D("0.0001"), min_qty=D(1),
                          swap_booked_to_balance=False, price_increment=increment, asset_class=asset_class)


def position(pid: str = "P1", *, symbol: str = SYMBOL, side: str = "BUY", units: str = "1000",
             entry: str | None = "1.2000", stop: str | None = "1.1900", status: str = "OPEN",
             opened_by: str | None = None, updated_ms: int = 0) -> PositionDTO:
    return PositionDTO(position_id=pid, account_id=ACCOUNT, adapter_id=ADAPTER, symbol=symbol, side=side,  # type: ignore[arg-type]
                       units=D(units), entry_price=None if entry is None else D(entry),
                       status=status, stop_loss=None if stop is None else D(stop),  # type: ignore[arg-type]
                       opened_by_client_order_id=opened_by, broker_updated_at_ms=updated_ms)


def stop_child(oid: str, pid: str, *, units: str, level: str, side: str = "SELL") -> OrderDTO:
    return OrderDTO(client_order_id=None, broker_order_id=oid, account_id=ACCOUNT, adapter_id=ADAPTER,
                    symbol=SYMBOL, side=side, status=OrderStatus.WORKING, entry_price=D(level),  # type: ignore[arg-type]
                    ordered_units=D(units), remaining_units=D(units), is_reduce_only=True, linked_position_id=pid,
                    order_kind="STOP")


def entry_order(oid: str, *, status: OrderStatus, units: str = "2000", remaining: str = "1000") -> OrderDTO:
    return OrderDTO(client_order_id=None, broker_order_id=oid, account_id=ACCOUNT, adapter_id=ADAPTER,
                    symbol=SYMBOL, side="BUY", status=status, entry_price=D("1.2000"), ordered_units=D(units),
                    remaining_units=D(remaining), order_kind="STOP")


def image(*positions: PositionDTO, orders: Sequence[OrderDTO] = (), fresh: bool = True,
          instruments: Sequence[InstrumentSpec] | None = None) -> AccountImage:
    return AccountImage(adapter_id=ADAPTER, account_id=ACCOUNT, fresh=fresh, positions=tuple(positions),
                        orders=tuple(orders),
                        instruments=tuple(instruments if instruments is not None else (instrument(),)))


# ---------------------------------------------------------------------------------------------------- the store
class FakeTxn:
    def __init__(self, store: FakeStore) -> None:
        self.store = store

    async def insert_position(self, row: ArmPositionRow) -> bool:
        if row.key in self.store.rows:
            return False
        self.store.rows[row.key] = replace(row, row_version=0)
        return True

    async def update_position(self, cas: PositionCas) -> bool:
        key = cas.row.key
        cur = self.store.rows.get(key)
        if cur is None or cur.state in TERMINAL_ARM_STATES:
            return False
        if self.store.miss_next > 0:        # a concurrent writer bumped the row just before this CAS
            self.store.miss_next -= 1
            self.store.rows[key] = replace(cur, row_version=cur.row_version + 1)
            return False
        if cur.row_version != cas.expected_row_version:
            return False
        self.store.rows[key] = replace(cas.row, row_version=cur.row_version + 1)
        self.store.cas_writes += 1
        return True

    async def record_decision(self, decision: DecisionRow, handles: Sequence[DecisionHandleRow] = (), *,
                              position: PositionCas | None = None) -> DecisionWrite:
        k = (decision.key, decision.bar_spec, decision.event, decision.bar_end_ms, decision.revision)
        if k in self.store.decisions:
            return DecisionWrite(DecisionWriteStatus.ALREADY_DECIDED)
        if position is not None and not await self.update_position(position):
            return DecisionWrite(DecisionWriteStatus.CAS_MISS)
        self.store.decisions[k] = decision
        return DecisionWrite(DecisionWriteStatus.INSERTED, DecisionRef(decision_id=len(self.store.decisions),
                                                                       bar_end_ms=decision.bar_end_ms))


class FakeStore:
    def __init__(self) -> None:
        self.rows: dict[PositionKey, ArmPositionRow] = {}
        self.decisions: dict[tuple[object, ...], DecisionRow] = {}
        self.miss_next = 0
        self.cas_writes = 0

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[FakeTxn]:
        yield FakeTxn(self)

    async def live_positions(self) -> list[ArmPositionRow]:
        return [r for _, r in sorted(self.rows.items(), key=lambda kv: kv[0].position_id)
                if r.state not in TERMINAL_ARM_STATES]

    async def position(self, key: PositionKey) -> ArmPositionRow | None:
        return self.rows.get(key)

    def row(self, pid: str = "P1") -> ArmPositionRow:
        return self.rows[PositionKey(adapter_id=ADAPTER, position_id=pid)]


class FakeVersions:
    def __init__(self) -> None:
        self.entries: dict[str, EntryTime] = {}
        self.history: dict[str, list[HandleVersionRow]] = {}
        self.entry_calls: list[tuple[str, str | None]] = []

    async def entry_time(self, adapter_id: str, position_id: str, entry_order_id: str | None) -> EntryTime | None:
        self.entry_calls.append((position_id, entry_order_id))
        return self.entries.get(position_id)

    async def stop_versions(self, adapter_id: str, position_id: str, children: Sequence[str],
                            bar_end_ms: int) -> list[HandleVersionRow]:
        return [r for r in self.history.get(position_id, [])
                if r.broker_time_ms is None or r.broker_time_ms <= bar_end_ms]


# ---------------------------------------------------------------------------------------------------- other ports
class FakeSnapshot:
    def __init__(self) -> None:
        self.images: list[AccountImage] = []
        self.asked: list[tuple[tuple[str, str], ...]] = []

    async def read(self, accounts: Sequence[tuple[str, str]]) -> Sequence[AccountImage]:
        self.asked.append(tuple(accounts))
        return list(self.images)


class FakeMd:
    """The lease side of `MarketDataPort` (`define`, `demand`, `release`, `md_generation`)."""

    def __init__(self) -> None:
        self.generation = MdGeneration(1, 1)
        self.demands: list[tuple[str, LeaseClass]] = []
        self.released: list[str] = []
        self.refuse: set[str] = set()
        self.defined: dict[str, SeriesId | IndicatorSpec] = {}

    def define(self, spec: SeriesId | IndicatorSpec) -> str:
        sid = spec_id_of(spec)
        self.defined[sid] = spec
        return sid

    async def demand(self, spec_id: str, consumer: str, lease_class: LeaseClass) -> bool:
        self.demands.append((spec_id, lease_class))
        return spec_id not in self.refuse

    async def release(self, spec_id: str, consumer: str) -> None:
        self.released.append(spec_id)

    async def md_generation(self) -> MdGeneration:
        return self.generation


class FakeGrid:
    def __init__(self, sides: frozenset[QuoteSide] = frozenset({QuoteSide.BID, QuoteSide.ASK})) -> None:
        self.sides = sides

    def skew(self, adapter_id: str) -> SkewEstimate:
        return COLD_SKEW

    def open_bar_ends(self, adapter_id: str, symbol: str, spec: BarSpec, from_ms: int, to_ms: int) -> list[int]:
        first = spec.bucket_start_ms(from_ms) + spec.timeframe_ms
        return list(range(first, to_ms + 1, spec.timeframe_ms))

    def bar_sides(self, adapter_id: str) -> frozenset[QuoteSide]:
        return self.sides


@dataclass
class FakeConfig:
    doc: ValidDocument | None = None

    def accepted(self) -> ValidDocument | None:
        return self.doc


@dataclass
class FakeFailed:
    failed: bool = False

    def covers(self, adapter_id: str, series: Sequence[str], version_scopes: Sequence[str],
               position: str | None = None) -> bool:
        return self.failed


@dataclass
class Recorder:
    notices: list[TrackerNotice] = field(default_factory=list)
    answers: list[tuple[int, RequestAnswer]] = field(default_factory=list)
    pages: list[tuple[str, str, str]] = field(default_factory=list)

    def notice(self, item: TrackerNotice) -> None:
        self.notices.append(item)

    def complete(self, request_id: int, answer: RequestAnswer) -> None:
        self.answers.append((request_id, answer))

    def page(self, severity: str, title: str, body: str) -> None:
        self.pages.append((severity, title, body))


@dataclass
class FakeClock:
    now: int

    def now_ms(self) -> int:
        return self.now

    def monotonic_ms(self) -> int:
        return self.now


@dataclass
class Rig:
    tracker: ArmTracker
    store: FakeStore
    versions: FakeVersions
    snapshot: FakeSnapshot
    md: FakeMd
    grid: FakeGrid
    config: FakeConfig
    failed: FakeFailed
    rec: Recorder
    clock: FakeClock


def rig(*, now: int = T0 + 5 * 60_000, doc: ValidDocument | None = None, settings: ArmSettings | None = None,
        tracker: TrackerSettings | None = None, registry: ArmRegistry = REGISTRY) -> Rig:
    store, versions, snapshot, md = FakeStore(), FakeVersions(), FakeSnapshot(), FakeMd()
    grid, config, failed, rec, clock = FakeGrid(), FakeConfig(doc or document()), FakeFailed(), Recorder(), \
        FakeClock(now)
    t = ArmTracker(store=store, versions=versions, snapshot=snapshot, md=md, grid=grid,  # type: ignore[arg-type]
                   config=config, notices=rec, answers=rec, pager=Pager(rec.page, lambda t, b: None), clock=clock,
                   settings=settings or ArmSettings(), failed=failed, tracker=tracker, registry=registry)
    return Rig(t, store, versions, snapshot, md, grid, config, failed, rec, clock)
