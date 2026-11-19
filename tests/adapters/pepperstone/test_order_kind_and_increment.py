"""cTrader order kind (design PR1) and price increment (PR7): the pure mapping, no network. Synthetic symbols."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any

import pytest

from src.adapters.pepperstone.instruments import SymbolInfo, spec_from_symbol
from src.adapters.pepperstone.mapping import map_order, order_kind

from .conftest import ACCOUNT, EURUSD, USDJPY

ASSETS = {1: "EUR", 2: "USD", 4: "JPY"}
LIGHT = {EURUSD: {"symbolId": EURUSD, "symbolName": "EURUSD", "baseAssetId": 1, "quoteAssetId": 2},
         USDJPY: {"symbolId": USDJPY, "symbolName": "USDJPY", "baseAssetId": 2, "quoteAssetId": 4}}


def info(symbol_id: int = EURUSD, **full: Any) -> SymbolInfo:
    row = {"symbolId": symbol_id, "digits": 5, "pipPosition": 4, "lotSize": 10_000_000, "minVolume": 100_000}
    row.update(full)
    found = spec_from_symbol(LIGHT[symbol_id], row, ASSETS)
    assert found is not None
    return found


def order(order_type: object, **extra: Any) -> dict[str, Any]:
    row: dict[str, Any] = {"orderId": 77, "orderType": order_type, "orderStatus": 1, "utcLastUpdateTimestamp": 1,
                           "tradeData": {"symbolId": EURUSD, "volume": 1_000_000, "tradeSide": "SELL"}}
    row.update(extra)
    return row


@pytest.mark.parametrize(("order_type", "extra", "kind"), [
    (2, {"limitPrice": 1.09}, "LIMIT"),
    ("LIMIT", {"limitPrice": 1.09}, "LIMIT"),
    (3, {"stopPrice": 1.07}, "STOP"),
    ("STOP", {"stopPrice": 1.07}, "STOP"),
    (6, {"stopPrice": 1.07, "limitPrice": 1.069}, "STOP"),                  # STOP_LIMIT
    ("STOP_LOSS_TAKE_PROFIT", {"stopPrice": 1.07, "positionId": 5}, "STOP"),
    (4, {"limitPrice": 1.09, "positionId": 5}, "LIMIT"),                     # take-profit leg
    (4, {"stopPrice": 0, "limitPrice": 1.09, "positionId": 5}, "LIMIT"),     # 0 means "not set"
    (1, {"executionPrice": 1.08}, None),                                     # MARKET
    ("MARKET_RANGE", {}, None),
    (None, {}, None),
    (99, {}, None),
])
def test_order_kind_table(order_type: object, extra: dict[str, Any], kind: str | None) -> None:
    eur = info()
    dto = map_order(order(order_type, **extra), account_id=str(ACCOUNT), lookup=lambda _sid: eur, source="REST")
    assert dto.order_kind == kind


def test_order_kind_helper_matches_map_order() -> None:
    assert order_kind({"stopPrice": 1.07}, 4) == "STOP" and order_kind({}, 4) == "LIMIT"
    assert order_kind({}, 2) == "LIMIT" and order_kind({}, 3) == "STOP" and order_kind({}, 6) == "STOP"
    assert order_kind({}, 1) is None and order_kind({}, 5) is None and order_kind({}, None) is None


def test_order_kind_changes_the_checksum_only_when_set() -> None:
    eur = info()
    market = map_order(order(1), account_id=str(ACCOUNT), lookup=lambda _sid: eur, source="REST")
    stop = map_order(order(3), account_id=str(ACCOUNT), lookup=lambda _sid: eur, source="REST")
    assert market.order_kind is None and stop.order_kind == "STOP"
    assert replace(stop, order_kind=None).checksum != stop.checksum


def test_price_increment_comes_from_digits_never_from_pip_position() -> None:
    eur, jpy = info(EURUSD), info(USDJPY, digits=3, pipPosition=2)
    assert eur.spec.price_increment == Decimal("0.00001") and jpy.spec.price_increment == Decimal("0.001")
    assert eur.spec.price_increment != eur.spec.point_size and jpy.spec.price_increment != jpy.spec.point_size
    no_pip = info(EURUSD, pipPosition=None)
    assert no_pip.spec.price_increment == Decimal("0.00001")
    assert eur.spec.min_stop_distance is None and eur.spec.asset_class is None
