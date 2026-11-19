"""fakeredis (with lupa for the Lua scripts) fixtures for the `redis-md` store tests, as `tests/cache/conftest.py`.

fakeredis has no `INFO` and no `WAITAOF` (ST-10): the runner gets a fixed `run_id` reader and the write client a fake
`AofWaiter`. Synthetic instruments only."""
from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import fakeredis
import pytest

from src.marketdata.hashing import Keyring, content_hash
from src.marketdata.store import keys as K
from src.marketdata.store.runner import ScriptRunner, StoreUnavailable
from src.marketdata.types import M15, Bar, MdGeneration, Quality, SeriesId, Side, Source

RUN_ID = "a1b2c3d4e5"
TOKEN = K.WriterToken("1234567890", 7, "node-a", "n0nce1")
SENTINEL = K.BootSentinel(RUN_ID, "1234567890", "node-a", MdGeneration(7, 1))
KEYRING = Keyring.of(3, bytes(range(32)))
SERIES = SeriesId("mock", "SYN1", M15, Side.BID, Source.BROKER)
T0 = 1_700_000_100_000 - 1_700_000_100_000 % 900_000
STORE_GLOBAL_KEYS = (K.WRITER, K.BOOT, K.BOOT_LOCK, K.EVENTS, K.REGISTRY_SERIES, K.REGISTRY_IND, K.LEASE_INDEX,
                     K.STORED)
"""The store-wide keys a bootstrap or a script writes (writer token, boot sentinel, event stream, registries). A
test against the real dev `redis-md` deletes them in its teardown with its own series keys, so the next test (its
own token epoch, its own run) finds an unbooted store and the order of the `md` marker tests never matters."""


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "md: test against the dev redis-md (ORDER_SERVER_TEST_REDIS_MD)")


@dataclass
class RunIds:
    """A switchable `run_id` reader (a Redis restart gives a new one)."""

    value: str = RUN_ID
    reads: int = 0

    async def __call__(self) -> str:
        self.reads += 1
        return self.value


@dataclass
class FakeAof:
    """`AofWaiter` for fakeredis: returns `local` (or raises) and counts calls. `on_wait` runs first (a test makes
    redis-py's silent reconnect-on-checkout happen there)."""

    local: int = 1
    error: bool = False
    calls: list[int] = field(default_factory=list)
    on_wait: Callable[[], None] | None = None

    async def wait(self) -> int:
        if self.on_wait is not None:
            self.on_wait()
        self.calls.append(self.local)
        if self.error:
            raise StoreUnavailable("timeout", "TimeoutError")
        return self.local


@pytest.fixture
async def md_redis() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.flushall()
    await r.aclose()


@pytest.fixture
def run_ids() -> RunIds:
    return RunIds()


@pytest.fixture
async def runner(md_redis: Any, run_ids: RunIds) -> ScriptRunner:
    await md_redis.set(K.WRITER, TOKEN.text)
    await md_redis.set(K.BOOT, SENTINEL.text)
    return ScriptRunner(md_redis, run_id_reader=run_ids)


def bar(start: int, close: str = "1.2345", *, revision: int = 0, series: SeriesId = SERIES,
        final_at_ms: int | None = None) -> Bar:
    c = Decimal(close)
    return Bar(series=series, bucket_start_ms=start, open=Decimal("1.2300"), high=max(c, Decimal("1.2400")),
               low=min(c, Decimal("1.2200")), close=c, tick_count=12, quality=Quality.COMPLETE, revision=revision,
               final_at_ms=final_at_ms if final_at_ms is not None else start + 960_000)


def hash_of(b: Bar) -> str:
    return content_hash(KEYRING, b.series.canonical, b.bucket_end_ms, b.open, b.high, b.low, b.close)
