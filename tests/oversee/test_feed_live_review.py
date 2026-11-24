"""Live review 2026-11-25, finding 4: entries Redis delivered to a lost XREADGROUP reply are drained from this
consumer's own pending list at once, not after claim_idle_ms (oversee-rules.md §1, §3)."""
from __future__ import annotations

from typing import Any, cast

import fakeredis

from src.oversee.feed import (
    FEED_STREAM,
    FeedEntry,
    FeedObserver,
    InMemoryFeedStream,
    RecordingQuarantine,
    RedisFeedStream,
)


class Clock:
    def __init__(self, t: int = 1000) -> None:
        self.t = t

    def __call__(self) -> int:
        return self.t


class Signals:
    def __init__(self) -> None:
        self.woken: list[str] = []

    def sweep_now(self, account_id: str) -> None:
        self.woken.append(account_id)


def setup() -> tuple[InMemoryFeedStream, Signals, RecordingQuarantine, Clock]:
    clock = Clock()
    return InMemoryFeedStream(boot="run:1", clock=clock), Signals(), RecordingQuarantine(), clock


def make(stream: Any, sig: Signals, q: RecordingQuarantine, clock: Clock) -> FeedObserver:
    return FeedObserver(stream, sig, lambda: "run:1", q, clock)


async def test_own_pending_entry_is_drained_on_the_first_poll_of_a_new_observer() -> None:
    stream, sig, q, clock = setup()
    entry = stream.add("A1")
    await stream.read_group(10, 0)            # Redis delivered it; the reply was lost (no ack)
    obs = make(stream, sig, q, clock)         # restarted observer, clock far below claim_idle_ms
    assert await obs.poll_once() == 1
    assert sig.woken == ["A1"] and stream.acked == [entry] and stream.pending == {}


async def test_run_re_arms_the_drain_on_the_same_instance() -> None:
    stream, sig, q, clock = setup()
    obs = make(stream, sig, q, clock)
    await obs.poll_once()
    entry = stream.add("A1")
    await stream.read_group(10, 0)            # a lost reply, then the task crashed
    obs._drain_own = False
    obs.stop()
    await obs.run()                           # the supervisor re-enters run(): the drain is armed again
    obs._stopping = False
    assert await obs.poll_once() == 1 and sig.woken == ["A1"] and stream.acked == [entry]


async def test_a_read_error_arms_the_drain_for_the_next_poll() -> None:
    stream, sig, q, clock = setup()
    obs = make(stream, sig, q, clock)
    stream.add("A0")
    await obs.poll_once()
    real = stream.read_group

    async def lost_reply(count: int, block_ms: int) -> list[FeedEntry]:
        await real(count, block_ms)           # Redis moved the entry into the PEL ...
        raise ConnectionError("reply lost")   # ... and the reply never arrived

    stream.read_group = lost_reply  # type: ignore[method-assign]
    entry = stream.add("A1")
    try:
        await obs.poll_once()
    except ConnectionError:
        pass
    stream.read_group = real  # type: ignore[method-assign]
    assert await obs.poll_once() == 1 and sig.woken == ["A0", "A1"] and entry in stream.acked


async def test_an_empty_read_arms_the_drain() -> None:
    """A swallowed read timeout reads as empty; the next poll drains whatever Redis delivered to it."""
    stream, sig, q, clock = setup()
    obs = make(stream, sig, q, clock)
    assert await obs.poll_once() == 0
    entry = stream.add("A1")
    await stream.read_group(10, 0)            # delivered to the timed-out read
    assert await obs.poll_once() == 1 and stream.acked == [entry]


async def test_an_own_entry_at_max_deliveries_is_quarantined_on_drain() -> None:
    stream, sig, q, clock = setup()
    entry = stream.add("A1")
    await stream.read_group(10, 0)
    fields, _, at = stream.pending[entry]
    stream.pending[entry] = (fields, 4, at)
    obs = make(stream, sig, q, clock)
    await obs.poll_once()
    assert q.rows and q.rows[0][0].deliveries == 5 and q.rows[0][1] == "max_deliveries"
    assert sig.woken == [] and stream.acked == [entry]


async def test_another_consumers_pending_entry_is_left_to_autoclaim() -> None:
    stream, sig, q, clock = setup()
    stream.add("A1")
    await stream.read_group_as("node-dead", 10)
    obs = make(stream, sig, q, clock)
    assert await obs.poll_once() == 0 and sig.woken == [] and len(stream.pending) == 1
    clock.t += 31_000
    assert await obs.claim_once() == 1 and sig.woken == ["A1"]


async def test_a_stream_without_own_pending_reads_still_works() -> None:
    stream, sig, q, clock = setup()

    class Plain:
        boot_id = stream.boot_id
        read_group = stream.read_group
        autoclaim = stream.autoclaim
        ack = stream.ack

    stream.add("A1")
    obs = make(Plain(), sig, q, clock)
    assert await obs.poll_once() == 1 and sig.woken == ["A1"]


class OwnPendingRedisFeed(RedisFeedStream):
    """The intended RedisFeedStream.read_own_pending (src/cache, wired by the cache owner): XREADGROUP id 0 for
    this consumer, trimmed entries dropped, delivery counts from one XPENDING range scoped to this consumer."""

    async def read_own_pending(self, count: int) -> list[FeedEntry]:
        resp: Any = await self._r.xreadgroup(self._group, self._consumer, {self._stream: "0"}, count=count)
        items: list[tuple[str, Any]] = []
        for _name, rows in cast(list[tuple[object, list[tuple[object, object]]]], resp or []):
            items += [(i.decode() if isinstance(i, bytes) else str(i), f) for i, f in rows if f]
        if not items:
            return []
        pending = cast(list[dict[str, Any]], await self._r.xpending_range(
            self._stream, self._group, min=items[0][0], max=items[-1][0], count=max(count, len(items)),
            consumername=self._consumer))
        times = {(p["message_id"].decode() if isinstance(p["message_id"], bytes) else str(p["message_id"])):
                 int(p["times_delivered"]) for p in pending}
        return [FeedEntry(i, {k.decode(): v.decode() for k, v in f.items()}, times.get(i, 1)) for i, f in items]


async def test_redis_lost_reply_is_drained_without_waiting_claim_idle() -> None:
    r = fakeredis.FakeAsyncRedis()
    stream = OwnPendingRedisFeed(r, consumer="node-a")
    await stream.ensure_group()
    await r.set("redis:boot_id", "run:1")
    await r.xadd(FEED_STREAM, {"account_id": "A1", "entity_type": "ORDER"})
    await r.xreadgroup("oversee", "node-a", {FEED_STREAM: ">"}, count=10)   # the reply that was lost
    sig = Signals()
    obs = FeedObserver(stream, sig, lambda: "run:1", RecordingQuarantine(), Clock())
    await obs.poll_once()
    assert sig.woken == ["A1"]
    assert (await r.xpending(FEED_STREAM, "oversee"))["pending"] == 0
    await r.aclose()
