"""Broker price-bar transport types (design PR3, plan S03): request forms, the unsupported answer, no price in repr."""
from __future__ import annotations

import dataclasses
from decimal import Decimal

import pytest

from src.dto import PRICE_BARS_NOT_SUPPORTED, BrokerBar, PriceBarsResponse, bar_request_problem

T0 = 1_767_225_600_000


def bar(start: int = T0, *, partial: bool = False) -> BrokerBar:
    return BrokerBar(bucket_start_ms=start, open=Decimal("1.23451"), high=Decimal("1.23462"),
                     low=Decimal("1.23447"), close=Decimal("1.23458"), side="BID", partial=partial)


def test_bar_and_response_are_frozen() -> None:
    b = bar()
    with pytest.raises(dataclasses.FrozenInstanceError):
        b.close = Decimal(1)   # type: ignore[misc]
    resp = PriceBarsResponse(adapter_id="mock", symbol="EURUSD", side="BID", timeframe_s=900, bars=(b,),
                             truncated=False, received_ms=T0 + 905_000, count=1)
    with pytest.raises(dataclasses.FrozenInstanceError):
        resp.truncated = True   # type: ignore[misc]


def test_repr_never_shows_a_price() -> None:
    b = bar()
    resp = PriceBarsResponse(adapter_id="mock", symbol="EURUSD", side="BID", timeframe_s=900, bars=(b,),
                             truncated=False, received_ms=T0)
    for text in (repr(b), repr(resp)):
        assert "1.234" not in text
        assert "bucket_start_ms" in text or "bars=" in text


@pytest.mark.parametrize(("args", "problem"), [
    (("BID", 900, 5, None, None), None),
    (("ASK", 60, None, T0, T0 + 60_000), None),
    (("ASK", 60, None, T0, T0), None),
    (("MID", 900, 5, None, None), "side"),
    (("BID", 0, 5, None, None), "timeframe_s"),
    (("BID", 900, 0, None, None), "count must be positive"),
    (("BID", 900, 5, T0, T0 + 1), "not both"),
    (("BID", 900, None, None, None), "give count"),
    (("BID", 900, None, T0, None), "give count"),
    (("BID", 900, None, T0 + 1, T0), "after"),
])
def test_request_forms(args: tuple[str, int, int | None, int | None, int | None], problem: str | None) -> None:
    found = bar_request_problem(*args)
    if problem is None:
        assert found is None
    else:
        assert found is not None and problem in found


def test_unsupported_answer_covers_nothing() -> None:
    resp = PriceBarsResponse.unsupported("pepperstone", "EURUSD", "BID", 900, from_ms=T0, to_ms=T0 + 900_000)
    assert not resp.supported and resp.bars == () and not resp.truncated
    assert resp.note == PRICE_BARS_NOT_SUPPORTED and (resp.from_ms, resp.to_ms) == (T0, T0 + 900_000)
