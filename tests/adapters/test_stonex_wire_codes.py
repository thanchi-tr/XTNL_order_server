"""Status / type / direction tables, date parsing and instrument mapping for StoneX."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.adapters.stonex.codes import (
    StoneXOrderStatus,
    StoneXOrderType,
    canonical_status,
    is_tradeable,
    parse_order_type,
    parse_side,
    parse_status,
)
from src.adapters.stonex.instruments import parse_market_name, spec_from_market_info, to_units
from src.adapters.stonex.wire import get_ci, parse_broker_time_ms, to_decimal, to_id, to_int
from src.dto import OrderStatus

from .conftest import MARKETS, USDJPY_ID

MS = 1_767_225_600_123


@pytest.mark.parametrize(("raw", "code", "canonical"), [
    ("1", StoneXOrderStatus.PENDING, OrderStatus.PENDING),
    ("2", StoneXOrderStatus.ACCEPTED, OrderStatus.WORKING),
    (3, StoneXOrderStatus.OPEN, OrderStatus.FILLED),
    ("4", StoneXOrderStatus.CANCELLED, OrderStatus.CANCELLED),
    ("5", StoneXOrderStatus.REJECTED, OrderStatus.REJECTED),
    ("6", StoneXOrderStatus.SUSPENDED, OrderStatus.WORKING),
    ("8", StoneXOrderStatus.YELLOW_CARD, OrderStatus.PENDING),
    ("9", StoneXOrderStatus.CLOSED, OrderStatus.FILLED),
    ("10", StoneXOrderStatus.RED_CARD, OrderStatus.PENDING),
    ("11.0", StoneXOrderStatus.TRIGGERED, OrderStatus.WORKING),
    ("Accepted", StoneXOrderStatus.ACCEPTED, OrderStatus.WORKING),
    ("Yellow Card", StoneXOrderStatus.YELLOW_CARD, OrderStatus.PENDING),
])
def test_status_table(raw: object, code: StoneXOrderStatus, canonical: OrderStatus) -> None:
    assert parse_status(raw) == code
    assert canonical_status(code) == canonical


@pytest.mark.parametrize("raw", [None, "", "7", "99", "bogus", "1.5"])
def test_unknown_status_is_none(raw: object) -> None:
    assert parse_status(raw) is None


def test_every_status_code_has_a_canonical_mapping() -> None:
    for code in StoneXOrderStatus:
        assert isinstance(canonical_status(code), OrderStatus)


@pytest.mark.parametrize(("raw", "expected"), [
    ("1", StoneXOrderType.TRADE), ("2", StoneXOrderType.STOP), ("3", StoneXOrderType.LIMIT),
    ("Stop", StoneXOrderType.STOP), ("LIMIT", StoneXOrderType.LIMIT), ("4", None), (None, None),
])
def test_order_type_table(raw: object, expected: StoneXOrderType | None) -> None:
    assert parse_order_type(raw) == expected


@pytest.mark.parametrize(("raw", "expected"), [
    ("1", "BUY"), ("0", "SELL"), (1, "BUY"), (0, "SELL"), ("Buy", "BUY"), ("sell", "SELL"),
    ("1.0", "BUY"), ("2", None), ("", None), (None, None),
])
def test_direction_table(raw: object, expected: str | None) -> None:
    assert parse_side(raw) == expected


@pytest.mark.parametrize(("raw", "expected"), [("0", True), ("1", False), ("2", False), ("4", False), (None, False)])
def test_price_status_tradeable(raw: object, expected: bool) -> None:
    assert is_tradeable(raw) is expected


@pytest.mark.parametrize(("raw", "expected"), [
    (f"/Date({MS})/", MS),
    (f"\\/Date({MS})\\/", MS),
    (f"/Date({MS}+0100)/", MS),
    (str(MS // 1000), (MS // 1000) * 1000),       # seconds
    (str(MS), MS),                                 # milliseconds
    (str(MS * 1000), MS),                          # microseconds
    (str(MS * 1_000_000), MS),                     # nanoseconds
    (MS, MS),
    ("2026-01-01T00:00:00Z", 1_767_225_600_000),
    ("2026-01-01T00:00:00", 1_767_225_600_000),   # naive -> UTC
    ("", None), (None, None), ("garbage", None), ("/Date(0)/", None), ("-5", None),
])
def test_broker_time_parsing(raw: object, expected: int | None) -> None:
    assert parse_broker_time_ms(raw) == expected


def test_scalar_coercion_is_total() -> None:
    assert to_decimal("1.08500") == Decimal("1.08500")
    assert to_decimal(" ") is None and to_decimal("NaN") is None and to_decimal(True) is None
    assert to_int("3.0") == 3 and to_int("3.5") is None
    assert to_id(" 42 ") == "42" and to_id("x") is None
    assert get_ci({"clientAccountId": 5}, "ClientAccountId") == 5
    assert get_ci({"Cash": None, "cash": 1}, "Cash") == 1


@pytest.mark.parametrize(("name", "symbol", "base", "quote", "per"), [
    ("GBP/USD (per 0.0001) CFD", "GBPUSD", "GBP", "USD", Decimal("0.0001")),
    ("USD/JPY (per 0.01) CFD", "USDJPY", "USD", "JPY", Decimal("0.01")),
    ("EUR/USD", "EURUSD", "EUR", "USD", None),
    ("UK 100 CFD", "UK100", None, None, None),
])
def test_market_name_parsing(name: str, symbol: str, base: str | None, quote: str | None,
                             per: Decimal | None) -> None:
    parsed = parse_market_name(name)
    assert parsed is not None
    assert (parsed.symbol, parsed.base, parsed.quote, parsed.per) == (symbol, base, quote, per)


def test_fx_market_information_maps_to_instrument_spec() -> None:
    spec = spec_from_market_info(MARKETS["401484347"])
    assert spec is not None
    assert (spec.symbol, spec.base_ccy, spec.quote_ccy, spec.pnl_ccy) == ("EURUSD", "EUR", "USD", "USD")
    assert spec.point_size == Decimal("0.0001")
    assert spec.unit_kind == "UNITS" and spec.contract_size == 1 and spec.min_qty == 1000
    assert spec.broker_symbol == "401484347" and spec.adapter_id == "stonex" and spec.swap_booked_to_balance
    jpy = spec_from_market_info(MARKETS[USDJPY_ID])
    assert jpy is not None and jpy.point_size == Decimal("0.01") and jpy.contract_size == 1


def test_fx_pip_defaults_without_per_in_name() -> None:
    eur = spec_from_market_info({"MarketId": 1, "Name": "EUR/USD", "PriceDecimalPlaces": 5,
                                 "MarketUnderlyingTypeId": 4})
    jpy = spec_from_market_info({"MarketId": 2, "Name": "EUR/JPY", "PriceDecimalPlaces": 3})
    assert eur is not None and eur.point_size == Decimal("0.0001")
    assert jpy is not None and jpy.point_size == Decimal("0.01")


def test_non_fx_needs_a_currency_or_is_left_unmapped() -> None:
    assert spec_from_market_info({"MarketId": 3, "Name": "UK 100 CFD", "PriceDecimalPlaces": 1}) is None
    idx = spec_from_market_info({"MarketId": 3, "Name": "UK 100 CFD", "PriceDecimalPlaces": 1,
                                 "MarketCurrency": "GBP"})
    assert idx is not None and idx.pnl_ccy == "GBP" and idx.point_size == Decimal("0.1")
    assert spec_from_market_info({"Name": "EUR/USD"}) is None


def test_units_conversion_and_unknown_instrument() -> None:
    spec = spec_from_market_info(MARKETS["401484347"])
    known = to_units(spec, Decimal(25000))
    assert (known.units, known.instrument_unknown, known.raw_qty) == (Decimal(25000), False, Decimal(25000))
    unknown = to_units(None, Decimal(25000))
    assert (unknown.units, unknown.instrument_unknown, unknown.raw_qty) == (None, True, Decimal(25000))
