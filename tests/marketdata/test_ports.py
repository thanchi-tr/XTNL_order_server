"""Market-data ports (plan S12; design §A10, PR12): typed fakes satisfy the Protocols, results are values."""
from __future__ import annotations

from collections.abc import Sequence

import pytest

from src.calendar.machine import SessionState
from src.dto.bars import PriceBarsResponse
from src.marketdata.ports import (
    AdapterHealthPort,
    BarCallback,
    BarsRead,
    BarsResult,
    BrokerBarsPort,
    CalendarPort,
    EventsRead,
    FormingRead,
    HandleRead,
    HandleRef,
    HandleStatus,
    IndicatorCallback,
    IndicatorRead,
    MarketDataPort,
    NodeMdRecord,
    NodeRecordPort,
    ProvisionalRead,
    QuoteRead,
    SeriesStatus,
    StampPort,
    StopHandleRead,
    StopHandleReadPort,
    StopHandleReadResult,
    calendar_open,
)
from src.marketdata.demand import spec_id_of
from src.marketdata.indicators.base import IndicatorSpec
from src.marketdata.types import (
    CursorExpired,
    DataUnavailable,
    EventCursor,
    LeaseClass,
    MdGeneration,
    SeriesId,
    Side,
    Transient,
    Unsupported,
)


class FakeCalendar:
    def __init__(self, states: dict[tuple[str, str], SessionState]) -> None:
        self.states = states

    def state(self, adapter_id: str, symbol: str) -> SessionState | None:
        return self.states.get((adapter_id, symbol))


class FakeBars:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Side, int]] = []

    async def fetch_price_bars(self, adapter_id: str, symbol: str, side: Side, timeframe_s: int, *,
                               count: int | None = None, from_ms: int | None = None,
                               to_ms: int | None = None) -> BarsResult:
        self.calls.append((adapter_id, symbol, side, timeframe_s))
        if adapter_id == "pepper":
            return Unsupported("no bar capability")
        if symbol == "SLOW":
            return Transient("bars_cap")
        return PriceBarsResponse(adapter_id=adapter_id, symbol=symbol, side=side.value, timeframe_s=timeframe_s,
                                 bars=(), truncated=False, received_ms=1, count=count)


class FakeStopReads:
    async def read_stop_handles(self, adapter_id: str, handles: Sequence[HandleRef]) -> StopHandleReadResult:
        return StopHandleRead(adapter_id, tuple(HandleRead(h, HandleStatus.OK, sent_ms=5, version="v1")
                                                for h in handles), sent_ms=5)


class FakeStamp:
    def __init__(self) -> None:
        self.stamps = 0
        self.exempt: str | None = None

    def stamp(self) -> None:
        self.stamps += 1

    def set_exempt(self, reason: str | None) -> None:
        self.exempt = reason


class FakeHealth:
    def is_streaming(self, adapter_id: str) -> bool:
        return adapter_id == "mock"

    def rest_failing_everywhere(self, adapter_id: str) -> bool:
        return False


class FakeRecords:
    def __init__(self) -> None:
        self.record: NodeMdRecord | None = None

    async def load_record(self) -> NodeMdRecord | None:
        return self.record

    async def save_record(self, record: NodeMdRecord) -> None:
        self.record = record


class FakeMarketData:
    """Minimal: every read is unavailable. Typed so mypy checks the `MarketDataPort` signatures now (S31 builds it)."""

    def __init__(self) -> None:
        self.callbacks: list[BarCallback] = []

    async def quote(self, adapter_id: str, symbol: str) -> QuoteRead | DataUnavailable:
        return DataUnavailable("no_data")

    async def quote_at(self, adapter_id: str, symbol: str, at_ms: int) -> QuoteRead | DataUnavailable:
        return DataUnavailable("no_data")

    async def forming_bar(self, series: SeriesId) -> FormingRead | DataUnavailable:
        return DataUnavailable("no_data")

    async def bars(self, series: SeriesId, *, at_bucket_ms: int | None = None, n: int | None = None,
                   from_bucket_ms: int | None = None, to_bucket_ms: int | None = None,
                   as_of_ms: int | None = None) -> BarsRead | DataUnavailable:
        return DataUnavailable("no_data")

    async def best_bars(self, adapter_id: str, symbol: str, spec_name: str, side: Side,
                        n: int) -> BarsRead | DataUnavailable:
        return DataUnavailable("no_data")

    async def indicator(self, indicator_id: str, output: str, *, at_bucket_ms: int | None = None,
                        n: int | None = None, as_of_ms: int | None = None) -> IndicatorRead | DataUnavailable:
        return DataUnavailable("no_data")

    async def provisional(self, indicator_id: str, output: str) -> ProvisionalRead | DataUnavailable:
        return DataUnavailable("no_data")

    def define(self, spec: SeriesId | IndicatorSpec) -> str:
        return spec_id_of(spec)

    async def demand(self, spec_id: str, consumer: str, lease_class: LeaseClass) -> bool:
        return False

    async def release(self, spec_id: str, consumer: str) -> None:
        return None

    def on_bar_closed(self, callback: BarCallback) -> None:
        self.callbacks.append(callback)

    def on_bar_revised(self, callback: BarCallback) -> None:
        self.callbacks.append(callback)

    def on_indicator_updated(self, callback: IndicatorCallback) -> None:
        return None

    async def events_since(self, series: SeriesId,
                           cursor: EventCursor) -> EventsRead | CursorExpired | DataUnavailable:
        return DataUnavailable("no_data")

    async def refetch(self, series: SeriesId, from_bucket_ms: int, to_bucket_ms: int) -> None | Unsupported:
        return Unsupported("tick series")

    async def md_generation(self) -> MdGeneration | DataUnavailable:
        return DataUnavailable("no_data")

    async def series_status(self, series: SeriesId) -> SeriesStatus | DataUnavailable:
        return DataUnavailable("no_data")


def test_fakes_satisfy_the_protocols() -> None:
    # Typed assignments: mypy (strict) checks these fakes against the Protocols.
    calendar: CalendarPort = FakeCalendar({})
    bars: BrokerBarsPort = FakeBars()
    stops: StopHandleReadPort = FakeStopReads()
    stamp: StampPort = FakeStamp()
    health: AdapterHealthPort = FakeHealth()
    records: NodeRecordPort = FakeRecords()
    md: MarketDataPort = FakeMarketData()
    assert all(p is not None for p in (calendar, bars, stops, stamp, health, records, md))


@pytest.mark.parametrize(("state", "is_open"), [
    (SessionState.OPEN, True), (SessionState.OPEN_BY_EVIDENCE, True), (SessionState.CLOSED, False),
    (SessionState.PRE_OPEN, False), (SessionState.ROLLOVER, False), (SessionState.BROKER_MAINTENANCE, False),
    (None, False),
])
def test_calendar_none_counts_as_closed(state: SessionState | None, is_open: bool) -> None:
    states = {} if state is None else {("mock", "SYN1"): state}
    assert calendar_open(FakeCalendar(states), "mock", "SYN1") is is_open


async def test_port_results_are_values() -> None:
    port: BrokerBarsPort = FakeBars()
    assert isinstance(await port.fetch_price_bars("pepper", "SYN1", Side.BID, 900, count=3), Unsupported)
    assert isinstance(await port.fetch_price_bars("mock", "SLOW", Side.BID, 900, count=3), Transient)
    assert isinstance(await port.fetch_price_bars("mock", "SYN1", Side.ASK, 900, count=3), PriceBarsResponse)
    stops: StopHandleReadPort = FakeStopReads()
    read = await stops.read_stop_handles("mock", [HandleRef("CHILD_ORDER", "o1"), HandleRef("POSITION_FIELD", "p1")])
    assert isinstance(read, StopHandleRead) and [h.status for h in read.handles] == [HandleStatus.OK] * 2
    records = FakeRecords()
    await records.save_record(NodeMdRecord("node-a", "run:cluster:node-a:7.1", 123))
    assert await records.load_record() == NodeMdRecord("node-a", "run:cluster:node-a:7.1", 123)
    md: MarketDataPort = FakeMarketData()
    assert isinstance(await md.quote("mock", "SYN1"), DataUnavailable)
