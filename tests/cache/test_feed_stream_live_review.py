"""Live review: a timed-out XREADGROUP BLOCK read must not kill the feed observer, and the feed observer's
client needs socket-timeout headroom over its BLOCK time (2026-11-23 'task feed-observer died: TimeoutError')."""
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any

import pytest
import redis.exceptions as rex

from src.cache import keys as K
from src.cache.client import create_redis, feed_redis_from, feed_timeout_ok, socket_timeout_of
from src.cache.feed_stream import MAX_TIMEOUT_STREAK_S, RedisFeedStream
from src.oversee.feed import FeedConfig, FeedObserver, RecordingQuarantine

from tests.cache.conftest import BOOT


class Sweep:
    def __init__(self) -> None:
        self.woken: list[str] = []

    def sweep_now(self, account_id: str) -> None:
        self.woken.append(account_id)


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


class TimeoutClient:
    """xreadgroup raises redis TimeoutError (what redis-py raises on a socket timeout during BLOCK)."""

    def __init__(self, exc: Exception | None = None) -> None:
        self.exc = exc if exc is not None else rex.TimeoutError("Timeout reading from host:port")
        self.calls = 0

    async def xreadgroup(self, *a: Any, **kw: Any) -> Any:
        self.calls += 1
        raise self.exc


# ------------------------------------------------------------------------------------- read_group


async def test_read_group_timeout_reads_as_empty() -> None:
    feed = RedisFeedStream(TimeoutClient(), "node-a")   # type: ignore[arg-type]
    assert await feed.read_group(10, 1000) == []
    assert feed.read_timeouts == 1


async def test_read_group_connection_error_is_not_swallowed() -> None:
    feed = RedisFeedStream(TimeoutClient(rex.ConnectionError("down")), "node-a")   # type: ignore[arg-type]
    with pytest.raises(rex.ConnectionError):
        await feed.read_group(10, 1000)


async def test_read_group_timeouts_persisting_past_the_bound_reraise() -> None:
    clock = Clock()
    feed = RedisFeedStream(TimeoutClient(), "node-a", monotonic=clock)   # type: ignore[arg-type]
    assert await feed.read_group(10, 1000) == []
    clock.t = MAX_TIMEOUT_STREAK_S - 0.5
    assert await feed.read_group(10, 1000) == []
    clock.t = MAX_TIMEOUT_STREAK_S
    with pytest.raises(rex.TimeoutError):
        await feed.read_group(10, 1000)
    assert MAX_TIMEOUT_STREAK_S < 30.0   # below the feed task's max stamp age


async def test_a_good_read_resets_the_timeout_streak(redis: Any) -> None:
    clock = Clock()
    feed = RedisFeedStream(redis, "node-a", monotonic=clock)
    await feed.ensure_group()
    real = redis.xreadgroup
    fail = {"on": True}

    async def flaky(*a: Any, **kw: Any) -> Any:
        if fail["on"]:
            raise rex.TimeoutError("Timeout reading from host:port")
        return await real(*a, **kw)

    redis.xreadgroup = flaky
    assert await feed.read_group(10, 10) == []
    clock.t = MAX_TIMEOUT_STREAK_S - 1
    fail["on"] = False
    assert await feed.read_group(10, 10) == []           # a good (empty) read ends the streak
    fail["on"] = True
    clock.t = MAX_TIMEOUT_STREAK_S + 5                   # would be past the bound without the reset
    assert await feed.read_group(10, 10) == []


# ------------------------------------------------------------------------------- FeedObserver.run


async def test_observer_run_survives_a_timed_out_read_then_wakes_and_acks(redis: Any) -> None:
    await redis.set(K.BOOT_ID, BOOT)
    feed = RedisFeedStream(redis, "node-a")
    await feed.ensure_group()
    entry_id = await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": "ACC"})
    real = redis.xreadgroup
    calls = {"n": 0}

    async def once_then_real(*a: Any, **kw: Any) -> Any:
        await asyncio.sleep(0.005)   # fakeredis ignores BLOCK: yield like a real BLOCK read would
        if kw.get("block") is not None:          # only the BLOCK '>' read times out (not an id-0 drain)
            calls["n"] += 1
            if calls["n"] == 1:
                raise rex.TimeoutError("Timeout reading from host:port")
        return await real(*a, **kw)

    redis.xreadgroup = once_then_real
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
            if sweep.woken:
                break
            await asyncio.sleep(0.01)
        assert not task.done(), task.exception() if task.done() else None
        assert sweep.woken == ["ACC"]
        pending = await redis.xpending(K.FEED, "oversee")
        assert pending["pending"] == 0                    # XACKed
        assert calls["n"] >= 2
        assert obs.liveness_ms is not None
        assert entry_id is not None
    finally:
        obs.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# --------------------------------------------------------------- real socket: redis-py TimeoutError


_REPLY = {b"HELLO": b"%1\r\n+proto\r\n:3\r\n", b"GET": b"_\r\n", b"PING": b"+PONG\r\n"}   # RESP3


@contextlib.asynccontextmanager
async def silent_xreadgroup_server() -> AsyncIterator[int]:
    """A RESP3 server: HELLO / PING / GET (nil) answered, anything else +OK, XREADGROUP never answered."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    return
                n = int(line[1:].strip())
                args = []
                for _ in range(n):
                    size = int((await reader.readline())[1:].strip())
                    args.append((await reader.readexactly(size + 2))[:-2])
                cmd = args[0].upper()
                if cmd == b"XREADGROUP":
                    continue                      # never answer: the client's socket timeout fires
                writer.write(_REPLY.get(cmd, b"+OK\r\n"))
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            return
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield port
    finally:
        server.close()
        await server.wait_closed()


async def test_real_socket_timeout_on_block_read_returns_empty_and_client_recovers() -> None:
    async with silent_xreadgroup_server() as port:
        r = create_redis(f"redis://127.0.0.1:{port}/0", socket_timeout_s=0.3)
        try:
            feed = RedisFeedStream(r, "node-a")
            assert await feed.read_group(10, 100) == []      # redis-py raised TimeoutError underneath
            assert feed.read_timeouts == 1
            assert await feed.boot_id() is None              # the client reconnects for the next command
        finally:
            await r.aclose()


# ------------------------------------------------------------------- feed client timeout headroom


async def test_engine_client_has_too_little_headroom_for_the_feed_block() -> None:
    r = create_redis("redis://127.0.0.1:6399/0")
    try:
        assert socket_timeout_of(r) == 2.0
        assert not feed_timeout_ok(r, FeedConfig().block_ms)   # 2.0 s < 1.0 s BLOCK + 2 s margin
    finally:
        await r.aclose()


async def test_feed_client_derived_from_engine_client_has_headroom_and_same_target() -> None:
    base = create_redis("redis://user:secret@127.0.0.1:6399/3")
    feed = feed_redis_from(base, FeedConfig().block_ms)
    try:
        kw = feed.connection_pool.connection_kwargs
        base_kw = base.connection_pool.connection_kwargs
        assert socket_timeout_of(feed) == FeedConfig().block_ms / 1000 + 5.0
        assert feed_timeout_ok(feed, FeedConfig().block_ms)
        assert socket_timeout_of(base) == 2.0                  # the gate / sweep client keeps its budget
        for k in ("host", "port", "db", "username", "password", "decode_responses", "socket_connect_timeout"):
            assert kw[k] == base_kw[k], k
        assert feed.connection_pool is not base.connection_pool
    finally:
        await feed.aclose()
        await base.aclose()


async def test_feed_client_derived_from_fakeredis_reads_the_same_server(redis: Any) -> None:
    feed_client = feed_redis_from(redis, 1000)
    try:
        await redis.set(K.BOOT_ID, BOOT)
        assert await RedisFeedStream(feed_client, "node-a").boot_id() == BOOT
    finally:
        await feed_client.aclose()


# ------------------------------------------------- read_own_pending (entries of a read whose reply was lost)


async def test_read_own_pending_returns_entries_of_a_lost_read_with_delivery_counts(redis: Any) -> None:
    feed = RedisFeedStream(redis, "node-a")
    await feed.ensure_group()
    for acct in ("A1", "A2"):
        await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": acct})
    # Redis delivers both to node-a's PEL, but the reply never reaches the observer (timed-out BLOCK read).
    await redis.xreadgroup("oversee", "node-a", {K.FEED: ">"}, count=10)
    await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": "B1"})
    await redis.xreadgroup("oversee", "node-b", {K.FEED: ">"}, count=10)   # another consumer's entry
    got = await feed.read_own_pending(10)
    assert [(e.account_id, e.deliveries) for e in got] == [("A1", 2), ("A2", 2)]
    await feed.ack([e.entry_id for e in got])
    assert await feed.read_own_pending(10) == []
    assert (await redis.xpending(K.FEED, "oversee"))["pending"] == 1      # node-b's entry is untouched


async def test_read_own_pending_acks_a_trimmed_entry(redis: Any) -> None:
    feed = RedisFeedStream(redis, "node-a")
    await feed.ensure_group()
    gone = await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": "A1"})
    await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": "A2"})
    await redis.xreadgroup("oversee", "node-a", {K.FEED: ">"}, count=10)
    await redis.xdel(K.FEED, gone)
    got = await feed.read_own_pending(10)
    assert [e.account_id for e in got] == ["A2"]                           # nothing to wake for the trimmed one
    pending = await redis.xpending_range(K.FEED, "oversee", min="-", max="+", count=10)
    assert [p["message_id"] for p in pending] == [got[0].entry_id.encode()]
