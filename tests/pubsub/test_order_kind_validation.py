"""S01: schema_problem() checks OrderDTO.order_kind like status (an unknown kind is schema_invalid)."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from src.dto import OrderDTO, OrderStatus
from src.pubsub.validation import checksum_valid, schema_problem


def order(**kw: Any) -> OrderDTO:
    base: dict[str, Any] = dict(
        client_order_id=None, broker_order_id="B1", account_id="ACC", adapter_id="mock", symbol="SYN.A",
        side="BUY", status=OrderStatus.WORKING, entry_price=None, stop_loss=Decimal("9"), ordered_units=Decimal(1),
        remaining_units=Decimal(1))
    base.update(kw)
    return OrderDTO(**base)


@pytest.mark.parametrize("kind", [None, "STOP", "LIMIT"])
def test_known_or_missing_order_kind_passes(kind: str | None) -> None:
    o = order(order_kind=kind)
    assert schema_problem(o, "mock") is None and checksum_valid(o)


@pytest.mark.parametrize("kind", ["STOP_LIMIT", "stop", "", "MARKET", 2])
def test_unknown_order_kind_is_rejected(kind: object) -> None:
    assert schema_problem(order(order_kind=kind), "mock") == "order_kind invalid"


def test_status_is_still_checked_first() -> None:
    assert schema_problem(order(status="WORKING", order_kind="BAD"), "mock") == "status invalid"
