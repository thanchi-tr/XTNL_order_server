"""Live review 3: the own-pending drain read (`RedisFeedStream.read_own_pending`, XREADGROUP id 0) must be as
timeout-tolerant as `read_group` (oversee-rules.md §1, §3; operations.md §3).

Before the fix, one redis TimeoutError on the XREADGROUP 0, the trimmed-entry XACK or the XPENDING range
propagated out of `FeedObserver.drain_own` and ended the feed-observer task. Now it reads as empty, counts toward
the same continuous-timeout bound, and leaves the drain owed: the next `read_group` drains the own PEL before its
`>` read (§3: drained after a read error, before the next `>` read).
"""
from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import pytest
import redis.exceptions as rex

from src.cache import keys as K
from src.cache.feed_stream import MAX_TIMEOUT_STREAK_S, RedisFeedStream
from src.oversee.feed import FeedConfig, FeedObserver, RecordingQuarantine

from tests.cache.conftest import BOOT

_TIMEOUT = "Timeout reading from host:port"


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


class Sweep:
    def __init__(self) -> None:
        self.woken: list[str] = []

    def sweep_now(self, account_id: str) -> None:
        self.woken.append(account_id)


async def _lost_delivery(redis: Any, *accts: str) -> RedisFeedStream:
    """Entries delivered into node-a's PEL whose '>' reply never reached the observer."""
    feed = RedisFeedStream(redis, "node-a")
    await feed.ensure_group()
    for a in accts:
        await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": a})
    await redis.xreadgroup("oversee", "node-a", {K.FEED: ">"}, count=100)
    return feed


def _fail_id0(redis: Any, times: int = 1) -> dict[str, int]:
    """Make the next `times` XREADGROUP id-0 reads raise redis TimeoutError; '>' reads stay real."""
    real = redis.xreadgroup
    left = {"n": times, "id0_calls": 0, "gt_calls": 0}

    async def flaky(group: Any, consumer: Any, streams: dict[Any, Any], *a: Any, **kw: Any) -> Any:
        if list(streams.values()) == ["0"]:
            left["id0_calls"] += 1
            if left["n"] > 0:
                left["n"] -= 1
                raise rex.TimeoutError(_TIMEOUT)
        else:
            left["gt_calls"] += 1
        return await real(group, consumer, streams, *a, **kw)

    redis.xreadgroup = flaky
    return left


# ---------------------------------------------------------------- each round trip of the drain


async def test_id0_read_timeout_reads_as_empty_and_is_counted(redis: Any) -> None:
    feed = await _lost_delivery(redis, "A1")
    _fail_id0(redis)
    assert await feed.read_own_pending(10) == []          # raised TimeoutError before the fix
    assert feed.read_timeouts == 1
    assert (await redis.xpending(K.FEED, "oversee"))["pending"] == 1   # nothing lost: still in the own PEL


async def test_xpending_timeout_reads_as_empty_and_entries_stay_pending(redis: Any) -> None:
    feed = await _lost_delivery(redis, "A1", "A2")

    async def timeout(*a: Any, **kw: Any) -> Any:
        raise rex.TimeoutError(_TIMEOUT)

    real = redis.xpending_range
    redis.xpending_range = timeout
    assert await feed.read_own_pending(10) == []
    redis.xpending_range = real
    got = await feed.read_own_pending(10)
    assert [(e.account_id, e.deliveries) for e in got] == [("A1", 3), ("A2", 3)]   # '>' + two id-0 reads


async def test_trimmed_entry_xack_timeout_reads_as_empty(redis: Any) -> None:
    feed = RedisFeedStream(redis, "node-a")
    await feed.ensure_group()
    gone = await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": "A1"})
    await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": "A2"})
    await redis.xreadgroup("oversee", "node-a", {K.FEED: ">"}, count=10)
    await redis.xdel(K.FEED, gone)

    async def timeout(*a: Any, **kw: Any) -> Any:
        raise rex.TimeoutError(_TIMEOUT)

    real = redis.xack
    redis.xack = timeout
    assert await feed.read_own_pending(10) == []
    redis.xack = real
    assert [e.account_id for e in await feed.read_own_pending(10)] == ["A2"]   # the retry XACKs the trimmed one


async def test_connection_error_in_the_drain_is_not_swallowed(redis: Any) -> None:
    feed = await _lost_delivery(redis, "A1")

    async def down(*a: Any, **kw: Any) -> Any:
        raise rex.ConnectionError("down")

    redis.xreadgroup = down
    with pytest.raises(rex.ConnectionError):
        await feed.read_own_pending(10)


# ------------------------------------------------------------------ the shared continuous-timeout bound


async def test_drain_timeouts_persisting_past_the_bound_reraise(redis: Any) -> None:
    clock = Clock()
    feed = RedisFeedStream(redis, "node-a", monotonic=clock)
    _fail_id0(redis, times=10)
    assert await feed.read_own_pending(10) == []
    clock.t = MAX_TIMEOUT_STREAK_S - 0.5
    assert await feed.read_own_pending(10) == []
    clock.t = MAX_TIMEOUT_STREAK_S
    with pytest.raises(rex.TimeoutError):
        await feed.read_own_pending(10)


async def test_drain_and_group_timeouts_share_one_streak() -> None:
    class AllTimeout:
        async def xreadgroup(self, *a: Any, **kw: Any) -> Any:
            raise rex.TimeoutError(_TIMEOUT)

    clock = Clock()
    feed = RedisFeedStream(AllTimeout(), "node-a", monotonic=clock)   # type: ignore[arg-type]
    assert await feed.read_own_pending(10) == []          # starts the streak
    clock.t = MAX_TIMEOUT_STREAK_S - 0.5
    assert await feed.read_group(10, 1000) == []
    clock.t = MAX_TIMEOUT_STREAK_S
    with pytest.raises(rex.TimeoutError):
        await feed.read_group(10, 1000)


async def test_a_good_drain_does_not_end_a_streak_of_group_timeouts(redis: Any) -> None:
    """Only a good '>' read ends the streak, so a Redis whose BLOCK reads keep timing out still reaches the
    supervisor even while the id-0 drain between them succeeds (operations.md §3)."""
    clock = Clock()
    feed = RedisFeedStream(redis, "node-a", monotonic=clock)
    await feed.ensure_group()
    real = redis.xreadgroup

    async def gt_timeout(group: Any, consumer: Any, streams: dict[Any, Any], *a: Any, **kw: Any) -> Any:
        if list(streams.values()) == [">"]:
            raise rex.TimeoutError(_TIMEOUT)
        return await real(group, consumer, streams, *a, **kw)

    redis.xreadgroup = gt_timeout
    assert await feed.read_group(10, 10) == []
    clock.t = MAX_TIMEOUT_STREAK_S - 0.5
    assert await feed.read_own_pending(10) == []          # a good (empty) drain
    clock.t = MAX_TIMEOUT_STREAK_S
    with pytest.raises(rex.TimeoutError):
        await feed.read_group(10, 10)


# ------------------------------------------------------------------- the owed drain before the next '>'


async def test_read_group_finishes_a_timed_out_drain_before_reading_new_entries(redis: Any) -> None:
    feed = await _lost_delivery(redis, "A1")
    calls = _fail_id0(redis)
    assert await feed.read_own_pending(10) == []          # the drain's reply is lost
    await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": "N1"})   # a busy feed: '>' is never empty
    first = await feed.read_group(10, 10)
    # own PEL first, with its count ('>' + this id-0 read; the simulated timeout never reached Redis)
    assert [(e.account_id, e.deliveries) for e in first] == [("A1", 2)]
    assert calls["gt_calls"] == 0
    await feed.ack([e.entry_id for e in first])
    second = await feed.read_group(10, 10)
    assert [e.account_id for e in second] == ["N1"]
    assert calls["gt_calls"] == 1


async def test_read_group_reads_new_entries_once_the_owed_drain_is_empty(redis: Any) -> None:
    feed = RedisFeedStream(redis, "node-a")
    await feed.ensure_group()
    _fail_id0(redis)
    assert await feed.read_own_pending(10) == []
    await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": "N1"})
    assert [e.account_id for e in await feed.read_group(10, 10)] == ["N1"]   # nothing owed after all


async def test_an_owed_drain_that_times_out_again_reads_as_empty(redis: Any) -> None:
    feed = await _lost_delivery(redis, "A1")
    calls = _fail_id0(redis, times=2)
    assert await feed.read_own_pending(10) == []
    assert await feed.read_group(10, 10) == []            # the owed drain timed out again: no '>' read
    assert calls["gt_calls"] == 0 and feed.read_timeouts == 2
    assert [e.account_id for e in await feed.read_group(10, 10)] == ["A1"]


async def test_a_full_drain_batch_stays_owed(redis: Any) -> None:
    feed = await _lost_delivery(redis, "A1", "A2", "A3")
    calls = _fail_id0(redis, times=0)
    got = await feed.read_own_pending(2)
    assert [e.account_id for e in got] == ["A1", "A2"]
    await feed.ack([e.entry_id for e in got])
    await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": "N1"})
    assert [e.account_id for e in await feed.read_group(2, 10)] == ["A3"]   # the rest before '>'
    assert calls["gt_calls"] == 0


# -------------------------------------------------------------------------- FeedObserver.run end to end


async def test_observer_run_survives_a_timed_out_drain_then_wakes_and_acks(redis: Any) -> None:
    await redis.set(K.BOOT_ID, BOOT)
    feed = await _lost_delivery(redis, "ACC")
    real = redis.xreadgroup
    calls = {"id0": 0}

    async def first_drain_times_out(group: Any, consumer: Any, streams: dict[Any, Any], *a: Any,
                                    **kw: Any) -> Any:
        await asyncio.sleep(0.005)                # fakeredis ignores BLOCK: yield like a real read would
        if list(streams.values()) == ["0"]:
            calls["id0"] += 1
            if calls["id0"] == 1:                 # the observer's first drain at run()
                raise rex.TimeoutError(_TIMEOUT)
        return await real(group, consumer, streams, *a, **kw)

    redis.xreadgroup = first_drain_times_out
    sweep = Sweep()
    ticks = {"ms": 0}

    def clock() -> int:
        ticks["ms"] += 1
        return ticks["ms"]

    obs = FeedObserver(feed, sweep, lambda: BOOT, RecordingQuarantine(), clock=clock,
                       config=FeedConfig(block_ms=10, paused_poll_ms=10))
    task = asyncio.create_task(obs.run())
    try:
        for _ in range(200):
            if sweep.woken or task.done():
                break
            await asyncio.sleep(0.01)
        assert not task.done(), task.exception() if task.done() else None   # died before the fix
        assert sweep.woken == ["ACC"]
        assert (await redis.xpending(K.FEED, "oversee"))["pending"] == 0     # XACKed
        assert calls["id0"] >= 2
    finally:
        obs.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
