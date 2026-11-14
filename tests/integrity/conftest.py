from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import fakeredis
import pytest

from src.cache import keys as K
from src.cache.scripts import NoBootQueue, RedisState
from src.integrity.redis_ops import BootIdentity
from tests.integrity.fakes import FakeIntegrityDb, ManualClock, Pages

EPOCH = "cluster-a:7"


class RunId:
    def __init__(self) -> None:
        self.value = "run-1"

    async def __call__(self) -> str:
        return self.value


@pytest.fixture
async def redis() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.flushall()
    await r.aclose()


@pytest.fixture
def run_id() -> RunId:
    return RunId()


@pytest.fixture
def identity(run_id: RunId) -> BootIdentity:
    return BootIdentity(lambda: EPOCH, run_id)


@pytest.fixture
def no_boot() -> NoBootQueue:
    return NoBootQueue()


@pytest.fixture
def cache(redis: Any, identity: BootIdentity, no_boot: NoBootQueue) -> RedisState:
    return RedisState(redis, identity.expected, no_boot=no_boot)


@pytest.fixture
def db() -> FakeIntegrityDb:
    return FakeIntegrityDb()


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def pages() -> Pages:
    return Pages()


async def boot(redis: Any, identity: BootIdentity) -> str:
    """Mark Redis as booted for this run/epoch."""
    expected = await identity.refresh()
    await redis.set(K.BOOT_ID, expected)
    return expected
