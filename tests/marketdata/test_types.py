"""Market-data core types (plan S12): bar specs on the UTC grid, canonical series ids, price-free reprs."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.marketdata.types import (
    H1,
    M1,
    M15,
    Bar,
    BarEndEvent,
    BarSpec,
    EventCursor,
    MdGeneration,
    Quality,
    Quote,
    SeriesId,
    Side,
    Source,
)

SERIES = SeriesId("mock", "SYN1", M15, Side.BID, Source.BROKER)
T0 = 1_700_000_100_000   # on the 15m grid


def test_bar_spec_names_and_grid() -> None:
    assert [BarSpec.parse(n).timeframe_s for n in ("1m", "5m", "15m", "1h")] == [60, 300, 900, 3600]
    assert M15.name == "15m" and str(H1) == "1h" and M1.timeframe_ms == 60_000
    assert M15.on_grid(T0) and not M15.on_grid(T0 + 60_000)
    assert M15.bucket_start_ms(T0 + 899_999) == T0 and M15.bucket_start_ms(T0 + 900_000) == T0 + 900_000
    assert M15.bucket_end_ms(T0) == T0 + 900_000
    assert M15.bucket_start_ms(-1) == -900_000


@pytest.mark.parametrize("bad", [0, 30, 120, 14_400, 86_400])
def test_bar_spec_refuses_unknown_timeframes(bad: int) -> None:
    with pytest.raises(ValueError):
        BarSpec(bad)


def test_bar_spec_parse_refuses_unknown_names() -> None:
    with pytest.raises(ValueError):
        BarSpec.parse("4h")


def test_series_canonical_round_trip() -> None:
    assert SERIES.canonical == "mock:SYN1:15m:BID:BROKER"
    assert SeriesId.parse(SERIES.canonical) == SERIES
    tick = SeriesId("stonex", "SYN.X", M1, Side.ASK, Source.TICK)
    assert SeriesId.parse(str(tick)) == tick


@pytest.mark.parametrize("text", ["mock:SYN1:15m:BID", "mock:SYN1:4h:BID:BROKER", "mock:SYN1:15m:MID:BROKER",
                                  "mock:SYN1:15m:BID:BEST", ":SYN1:15m:BID:BROKER", "mock:SY N:15m:BID:BROKER"])
def test_series_parse_refuses_malformed_ids(text: str) -> None:
    with pytest.raises(ValueError):
        SeriesId.parse(text)


@pytest.mark.parametrize("symbol", ["A|B", "A*", "A{x}", "A:B", ""])
def test_series_parts_refuse_key_unsafe_characters(symbol: str) -> None:
    with pytest.raises(ValueError):
        SeriesId("mock", symbol, M15, Side.BID, Source.BROKER)


def test_bar_and_quote_reprs_hold_no_prices() -> None:
    bar = Bar(series=SERIES, bucket_start_ms=T0, open=Decimal("1.23457"), high=Decimal("1.23999"),
              low=Decimal("1.23001"), close=Decimal("1.23888"), quality=Quality.COMPLETE, revision=2)
    quote = Quote(adapter_id="mock", symbol="SYN1", bid=Decimal("1.23457"), ask=Decimal("1.23999"), broker_ms=T0,
                  received_ms=T0)
    for text in (repr(bar), repr(quote)):
        assert "1.23" not in text
    assert bar.bucket_end_ms == T0 + 900_000
    assert bar.event_id == ("mock:SYN1:15m:BID:BROKER", T0 + 900_000)


def test_generation_and_cursor_order() -> None:
    assert MdGeneration(7, 2) < MdGeneration(7, 3) < MdGeneration(8, 1)
    assert str(MdGeneration(7, 2)) == "7.2"
    assert EventCursor(T0, 5) < EventCursor(T0, 6) < EventCursor(T0 + 1, 0)
    event = BarEndEvent(SERIES, T0 + 900_000, "96" + "0" * 32, write_seq=9, revision=1)
    assert event.event_id == (SERIES.canonical, T0 + 900_000)
    assert event.cursor == EventCursor(T0 + 900_000, 9)
