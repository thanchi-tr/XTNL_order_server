from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import fakeredis
import pytest

from src.cache import keys as K
from src.cache.scripts import RedisState
from src.dto import OrderDTO, OrderStatus

BOOT = "run123:7"   # redis:boot_id = run_id:epoch (the fencing token)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "redis: integration test against a real Redis (ORDER_SERVER_TEST_REDIS)")


@dataclass
class Boot:
    value: str = BOOT


@pytest.fixture
async def redis() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.flushall()
    await r.aclose()


@pytest.fixture
def boot() -> Boot:
    return Boot()


@pytest.fixture
async def state(redis: Any, boot: Boot) -> RedisState:
    await redis.set(K.BOOT_ID, BOOT)
    return RedisState(redis, lambda: boot.value)


def order(**kw: Any) -> OrderDTO:
    base: dict[str, Any] = dict(
        client_order_id="c1", broker_order_id="B1", account_id="ACC", adapter_id="stonex", symbol="EURUSD",
        side="BUY", status=OrderStatus.WORKING, entry_price=Decimal("1.1000"), stop_loss=Decimal("1.0950"),
        ordered_units=Decimal("10000"), remaining_units=Decimal("10000"), broker_updated_at_ms=1_700_000_000_000,
        broker_seq=None, observed_seq=100, seq_id=1000,
    )
    base.update(kw)
    return OrderDTO(**base)
