"""Live review round 2 (docs/design/live-review-2026-11-25.md): the feed observer the session builds.

* The production proxy `_PacedFeedStream` must pass `read_own_pending` through, or `FeedObserver.drain_own` sees no
  `OwnPendingReader` and entries delivered to a lost XREADGROUP reply wait 30-60 s for XAUTOCLAIM (oversee-rules.md
  §3 Consumer-Group Recovery).
* The session gives the feed observer its own Redis client with socket-timeout headroom over the BLOCK time and
  closes it with the term; the gate / sweep / SM8 keep the engine client (oversee-rules.md §1, §3; operations.md §3).
No network: fakeredis and the e2e memory backend.
"""
from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import fakeredis
import pytest

from src.cache import keys as K
from src.cache.client import create_redis, feed_timeout_ok, socket_timeout_of
from src.cache.feed_stream import FEED_GROUP, FeedEntry, RedisFeedStream
from src.db.epoch import Epoch
from src.oversee.feed import FeedConfig, FeedObserver, OwnPendingReader, RecordingQuarantine
from src.runtime.ports import LeaderPorts
from src.runtime.session import EngineSession, SessionContext, _PacedFeedStream
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import NODE, FakeLease, fixture, mock_adapter, wall_ms

BOOT = "run-x:c:7"
CONSUMER = "node-a"
EPOCH = Epoch("c", 7)


@pytest.fixture
async def redis() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.flushall()
    await r.aclose()


class Sweep:
    def __init__(self) -> None:
        self.woken: list[str] = []

    def sweep_now(self, account_id: str) -> None:
        self.woken.append(account_id)


class SpyStream(RedisFeedStream):
    """The production RedisFeedStream, recording its own-pending reads; `fail_next` makes the next '>' read raise."""

    def __init__(self, client: Any, consumer: str) -> None:
        super().__init__(client, consumer)
        self.own_reads = 0
        self.fail_next = False

    async def read_own_pending(self, count: int) -> list[FeedEntry]:
        self.own_reads += 1
        return await super().read_own_pending(count)

    async def read_group(self, count: int, block_ms: int) -> list[FeedEntry]:
        if self.fail_next:
            self.fail_next = False
            raise ConnectionError("reply lost")
        return await super().read_group(count, block_ms)


async def _no_sleep(_s: float) -> None:
    return None


async def _setup(redis: Any) -> tuple[SpyStream, _PacedFeedStream, FeedObserver, Sweep]:
    await redis.set(K.BOOT_ID, BOOT)
    inner = SpyStream(redis, CONSUMER)
    await inner.ensure_group()
    paced = _PacedFeedStream(inner, monotonic=lambda: 0.0)
    sweep = Sweep()
    obs = FeedObserver(paced, sweep, lambda: BOOT, RecordingQuarantine(), clock=lambda: 0,
                       config=FeedConfig(block_ms=1, claim_every_ms=10**12))
    return inner, paced, obs, sweep


async def _lose_a_reply(redis: Any, account_id: str) -> None:
    """Redis moves an entry into this consumer's PEL but the reply never reaches the observer."""
    await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": account_id})
    await redis.xreadgroup(FEED_GROUP, CONSUMER, {K.FEED: ">"}, count=10)


async def _pending(redis: Any) -> int:
    return int((await redis.xpending(K.FEED, FEED_GROUP))["pending"])


async def test_paced_proxy_is_an_own_pending_reader(redis: Any) -> None:
    inner, paced, _obs, _ = await _setup(redis)
    assert isinstance(paced, OwnPendingReader)          # before: False, so drain_own() returned 0 in production
    await _lose_a_reply(redis, "A1")
    got = await paced.read_own_pending(10)
    assert [e.account_id for e in got] == ["A1"] and inner.own_reads == 1


async def test_drain_runs_at_start_through_the_production_proxy(redis: Any) -> None:
    inner, _paced, obs, sweep = await _setup(redis)
    await _lose_a_reply(redis, "A1")                    # e.g. the previous run's last read lost its reply
    assert await obs.drain_own() == 1
    assert inner.own_reads >= 1 and sweep.woken == ["A1"] and await _pending(redis) == 0


async def test_drain_runs_after_an_empty_read_through_the_production_proxy(redis: Any) -> None:
    inner, _paced, obs, sweep = await _setup(redis)
    await obs.poll_once()                               # start-of-run drain, then an empty '>' read
    assert sweep.woken == []
    reads = inner.own_reads
    await _lose_a_reply(redis, "A2")                    # the empty read may have been a timed-out one
    await obs.poll_once()                               # drains own pending before the next '>' read
    assert inner.own_reads > reads
    assert sweep.woken == ["A2"] and await _pending(redis) == 0     # swept and XACKed at once, no XAUTOCLAIM wait


async def test_drain_runs_after_a_read_error_through_the_production_proxy(redis: Any) -> None:
    inner, _paced, obs, sweep = await _setup(redis)
    await obs.poll_once()
    await _lose_a_reply(redis, "A3")
    inner.fail_next = True
    with pytest.raises(ConnectionError):
        await obs.poll_once()
    await obs.poll_once()
    assert sweep.woken == ["A3"] and await _pending(redis) == 0


# ------------------------------------------------------------------------------- the session's feed client


async def _run_id(_: Any) -> str:
    return "run-x"


def _session(redis: Any, tmp_path: Path) -> EngineSession:
    backend = MemoryBackend(redis, _run_id, EPOCH.token)

    def ports(epoch: Epoch) -> LeaderPorts:
        return dataclasses.replace(backend.ports(epoch))

    fx = fixture(wall_ms())
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=ports,
                         adapters=lambda: [mock_adapter(fx)], lease=FakeLease(), run_id_reader=_run_id,
                         exit_fn=lambda reason: None)
    return EngineSession(ctx, EPOCH)


async def test_session_feed_stream_has_its_own_client_with_headroom(tmp_path: Path) -> None:
    engine = create_redis("redis://127.0.0.1:6399/0")     # the production client (2 s socket timeout); no connect
    session = _session(engine, tmp_path)
    try:
        paced = session.feed._stream
        assert isinstance(paced, _PacedFeedStream)
        inner = paced._inner
        assert isinstance(inner, RedisFeedStream)
        client = inner._r
        assert client is session.feed_redis and client is not engine
        assert feed_timeout_ok(client, FeedConfig().block_ms)
        assert not feed_timeout_ok(engine, FeedConfig().block_ms)
        assert socket_timeout_of(engine) == 2.0         # the gate / sweep / SM8 client keeps its budget
        assert session.redis is engine
    finally:
        await session.aclose(1.0)
        await engine.aclose()


async def test_session_aclose_closes_the_feed_client_and_its_pool(redis: Any, tmp_path: Path) -> None:
    session = _session(redis, tmp_path)
    feed = session.feed_redis
    await redis.set(K.BOOT_ID, BOOT)
    assert await RedisFeedStream(feed, CONSUMER).boot_id() == BOOT     # same server, own pool
    closed: list[bool] = []
    pool_close = feed.connection_pool.aclose

    async def spy_close(*a: Any, **kw: Any) -> None:
        closed.append(True)
        await pool_close(*a, **kw)
    feed.connection_pool.aclose = spy_close            # type: ignore[method-assign]
    await asyncio.wait_for(session.aclose(1.0), 5)
    assert closed == [True]
    assert await redis.get(K.BOOT_ID) is not None       # the engine client stays usable
