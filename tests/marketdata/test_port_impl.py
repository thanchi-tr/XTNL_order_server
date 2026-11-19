"""The `MarketDataPort` implementation, the pull cursor and the re-fetch queue (plan S16; design §A10, §A13, §A4
check 5, §A7 AWS). fakeredis + lupa; synthetic instrument `mock:SYN1`, synthetic prices, no price in an assertion
message."""
from __future__ import annotations

from dataclasses import replace
from typing import Any

from src.marketdata.bootstrap import Sm12Event
from src.marketdata.codec import encode_member
from src.marketdata.events import RefetchQueue, events_after, rollback_events
from src.marketdata.port_impl import as_market_data_port
from src.marketdata.ports import BarsRead, EventsRead, FormingRead, QuoteRead, SeriesStatus
from src.marketdata.store import keys as K
from src.marketdata.store.reads import RollbackEntry, StoreReader, parse_rollback, parse_superseded, removable_member
from src.marketdata.store.runner import StoreUnavailable
from src.marketdata.types import (
    M15,
    CursorExpired,
    DataUnavailable,
    EventCursor,
    Liveness,
    MdGeneration,
    Quality,
    SeriesId,
    Side,
    Sm12State,
    Source,
    Unsupported,
)

from .bootrig import Rig, make_rig
from .conftest import SERIES, T0, RunIds, bar, hash_of

Q = 900_000
TICK_15M = replace(SERIES, source=Source.TICK)


def end_of(k: int) -> int:
    return T0 + (k + 1) * Q + 60_000


async def live_rig(md_redis: Any, run_ids: RunIds, bars: int = 3) -> Rig:
    rig = await make_rig(md_redis, run_ids, start=T0)
    await rig.at(T0)
    for k in range(bars):
        rig.writer.queue_bar(bar(T0 + k * Q))
        await rig.at(end_of(k))
    rig.book.apply(SERIES, Sm12Event.LIVE)
    return rig


async def test_mdport_satisfies_the_protocol(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, bars=0)
    port = as_market_data_port(rig.port)
    for name in ("quote", "forming_bar", "bars", "best_bars", "demand", "release", "on_bar_closed", "on_bar_revised",
                 "events_since", "refetch", "md_generation", "series_status", "indicator", "provisional", "define",
                 "on_indicator_updated"):
        assert callable(getattr(port, name))


async def test_decision_reads_need_live_or_a_complete_backfill(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids)
    read = await rig.port.bars(SERIES, from_bucket_ms=T0, to_bucket_ms=T0 + 2 * Q)
    assert isinstance(read, BarsRead) and [b.bucket_start_ms for b in read.bars] == [T0, T0 + Q, T0 + 2 * Q]
    assert read.generation == rig.writer.boot.generation if rig.writer.boot else False
    rig.book.apply(SERIES, Sm12Event.GAP)
    assert await rig.port.bars(SERIES, at_bucket_ms=T0) == DataUnavailable("not_live")
    rig.book.apply(SERIES, Sm12Event.BACKFILL)
    rig.book.note_complete(SERIES, T0 + Q, T0 + 2 * Q)
    assert isinstance(await rig.port.bars(SERIES, at_bucket_ms=T0 + Q), BarsRead)
    assert await rig.port.bars(SERIES, at_bucket_ms=T0) == DataUnavailable("not_live")
    assert await rig.port.bars(SERIES, n=2) == DataUnavailable("not_live")     # n-form: LIVE only
    rig.book.apply(SERIES, Sm12Event.FAIL)
    assert await rig.port.bars(SERIES, at_bucket_ms=T0 + Q) == DataUnavailable("failed")


async def test_a_global_failed_serves_no_decision_reads(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids)
    rig.port._global_failed = lambda: True                                       # type: ignore[method-assign]
    assert await rig.port.bars(SERIES, at_bucket_ms=T0) == DataUnavailable("failed")


async def test_bad_requests_are_values(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, bars=1)
    assert await rig.port.bars(SERIES) == DataUnavailable("bad_request")
    assert await rig.port.bars(SERIES, at_bucket_ms=T0, n=1) == DataUnavailable("bad_request")
    assert await rig.port.bars(SERIES, from_bucket_ms=T0) == DataUnavailable("bad_request")
    assert await rig.port.bars(SERIES, from_bucket_ms=T0 + Q, to_bucket_ms=T0) == DataUnavailable("bad_request")
    assert await rig.port.bars(SERIES, n=0) == DataUnavailable("bad_request")


async def test_newest_n(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids)
    read = await rig.port.bars(SERIES, n=2)
    assert isinstance(read, BarsRead) and [b.bucket_start_ms for b in read.bars] == [T0 + Q, T0 + 2 * Q]


async def test_stale_node_when_the_writer_epoch_is_not_the_leaders(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, bars=1)
    rig.port._leader_epoch = lambda: 8                                           # type: ignore[method-assign]
    assert await rig.port.bars(SERIES, at_bucket_ms=T0) == DataUnavailable("stale_node")
    quote = await rig.port.quote("mock", "SYN1")
    assert isinstance(quote, QuoteRead) and quote.liveness is Liveness.STALE_NODE and quote.quote is None
    rig.port._leader_epoch = lambda: None                                        # type: ignore[method-assign]
    assert await rig.port.md_generation() == DataUnavailable("stale_node")       # unknown epoch: fail closed


async def test_a_generation_change_during_a_read_discards_it(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, bars=1)

    class Flipping(StoreReader):
        async def bars_range(self, series: SeriesId, lo: int, hi: int) -> list[tuple[int, str]]:
            out = await super().bars_range(series, lo, hi)
            boot = await md_redis.get(K.BOOT)
            await md_redis.set(K.BOOT, boot.decode().rsplit(".", 1)[0] + ".99")
            return out

    rig.port._reader = Flipping(md_redis)                                        # type: ignore[assignment]
    assert await rig.port.bars(SERIES, at_bucket_ms=T0) == DataUnavailable("generation_changed")


async def test_store_errors_are_values(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, bars=1)

    class Down(StoreReader):
        async def head(self) -> Any:
            raise StoreUnavailable("timeout", "TimeoutError")

    rig.port._reader = Down(md_redis)                                            # type: ignore[assignment]
    assert await rig.port.bars(SERIES, at_bucket_ms=T0) == DataUnavailable("store_timeout")
    assert await rig.port.quote("mock", "SYN1") == DataUnavailable("store_timeout")
    assert await rig.port.events_since(SERIES, EventCursor(T0, 0)) == DataUnavailable("store_timeout")


async def test_a_corrupt_stored_bar_is_returned_blind_and_moved_to_quarantine(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, bars=2)
    (member,) = await md_redis.zrangebyscore(K.bars(SERIES), T0, T0)
    good = member.decode()
    forged = bar(T0, "1.2399")
    tampered = encode_member(forged, hash_of(bar(T0)))                           # the hash of another bar
    await md_redis.zrem(K.bars(SERIES), good)
    await md_redis.zadd(K.bars(SERIES), {tampered: T0})
    read = await rig.port.bars(SERIES, from_bucket_ms=T0, to_bucket_ms=T0 + Q)
    assert isinstance(read, BarsRead)
    assert [b.quality for b in read.bars] == [Quality.BLIND, Quality.COMPLETE]
    assert rig.port.quarantine_requests == 1
    await rig.at(end_of(1) + 250)                                                 # md-writer moves it
    assert await md_redis.zcard(K.quarantine(SERIES)) == 1
    assert await md_redis.zrangebyscore(K.bars(SERIES), T0, T0) == []            # absent now: blind
    await rig.port.bars(SERIES, from_bucket_ms=T0, to_bucket_ms=T0 + Q)
    assert rig.port.quarantine_requests == 1


async def test_as_of_reads_pick_the_revision_current_then(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, bars=1)
    first_final = T0 + Q + 60_000
    rig.writer.queue_bar(bar(T0, "1.2399", revision=1, final_at_ms=T0 + Q + 120_000))
    await rig.at(T0 + Q + 120_000)                                               # superseded at this time
    now = await rig.port.bars(SERIES, at_bucket_ms=T0)
    then = await rig.port.bars(SERIES, at_bucket_ms=T0, as_of_ms=T0 + Q + 90_000)
    before = await rig.port.bars(SERIES, at_bucket_ms=T0, as_of_ms=T0 + Q - 1)
    assert isinstance(now, BarsRead) and [b.revision for b in now.bars] == [1]
    assert isinstance(then, BarsRead) and [b.revision for b in then.bars] == [0]
    assert then.bars[0].final_at_ms is not None and then.bars[0].final_at_ms <= first_final + Q
    assert isinstance(before, BarsRead) and before.bars == ()


async def test_quote_forming_status_and_generation(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, bars=1)
    quote = await rig.port.quote("mock", "SYN1")
    assert isinstance(quote, QuoteRead) and quote.liveness is Liveness.STALE and quote.quote is None
    forming = await rig.port.forming_bar(TICK_15M)
    assert isinstance(forming, FormingRead) and forming.bar is None
    status = await rig.port.series_status(SERIES)
    assert isinstance(status, SeriesStatus) and status.state is Sm12State.LIVE and status.gaps == 0
    assert await rig.port.md_generation() == rig.writer.boot.generation if rig.writer.boot else False


async def test_best_bars_falls_back_to_tick_for_display(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, bars=1)
    read = await rig.port.best_bars("mock", "SYN1", "15m", Side.BID, 5)
    assert isinstance(read, BarsRead) and len(read.bars) == 1
    none = await rig.port.best_bars("mock", "SYN9", "15m", Side.BID, 5)
    assert isinstance(none, BarsRead) and none.bars == ()
    assert await rig.port.best_bars("mock", "SYN1", "2m", Side.BID, 5) == DataUnavailable("bad_request")


async def test_wake_ups_route_closed_and_revised(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, bars=0)
    closed: list[Any] = []
    revised: list[Any] = []
    rig.port.on_bar_closed(closed.append)
    rig.port.on_bar_revised(revised.append)
    rig.port.on_bar_closed(lambda e: 1 / 0)                                     # a faulty consumer is contained
    rig.writer.queue_bar(bar(T0))
    await rig.at(end_of(0))
    rig.writer.queue_bar(bar(T0, "1.2399", revision=1))
    await rig.at(end_of(0) + 250)
    assert [(e.bar_end_ms, e.write_seq) for e in closed] == [(T0 + Q, 1)]
    assert [(e.bar_end_ms, e.write_seq) for e in revised] == [(T0 + Q, 2)]


async def test_demand_is_refused_without_a_demand_book(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, bars=0)
    from src.marketdata.types import LeaseClass
    assert await rig.port.demand("specA", "arm", LeaseClass.ARMED) is False
    assert await rig.port.release("specA", "arm") is None


# ---------------------------------------------------------------------------------------------------- pull cursor
async def test_events_since_follows_write_seq_and_expires_by_generation_and_trim(md_redis: Any,
                                                                                run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, bars=3)
    gen = rig.writer.boot.generation if rig.writer.boot else MdGeneration(0, 0)
    read = await rig.port.events_since(SERIES, EventCursor(T0 + Q, 1, gen))
    assert isinstance(read, EventsRead) and [e.write_seq for e in read.events] == [2, 3]
    assert read.next_cursor(EventCursor(T0 + Q, 1, gen)) == EventCursor(T0 + 3 * Q, 3, gen)
    empty = await rig.port.events_since(SERIES, EventCursor(T0 + 3 * Q, 3, gen))
    assert isinstance(empty, EventsRead) and empty.events == ()
    other = MdGeneration(gen.epoch_rev, gen.n + 1)
    assert isinstance(await rig.port.events_since(SERIES, EventCursor(T0 + Q, 1, other)), CursorExpired)
    assert isinstance(await rig.port.events_since(SERIES, EventCursor(T0 + Q, 1)), CursorExpired)
    assert isinstance(await rig.port.events_since(SERIES, EventCursor(T0 + Q, 9, gen)), CursorExpired)


def _entries(*pairs: tuple[int, int]) -> list[RollbackEntry]:
    out = []
    for ws, k in pairs:
        b = bar(T0 + k * Q)
        out.append(RollbackEntry(ws, b.bucket_end_ms, encode_member(b, hash_of(b))))
    return out


def test_events_after_rules() -> None:
    gen = MdGeneration(7, 1)
    entries = _entries((4, 1), (5, 2), (6, 3))
    assert isinstance(events_after(SERIES, entries, EventCursor(0, 2, gen), gen), CursorExpired)   # trimmed
    got = events_after(SERIES, entries, EventCursor(0, 3, gen), gen)
    assert not isinstance(got, CursorExpired) and [e.write_seq for e in got] == [4, 5, 6]
    assert events_after(SERIES, [], EventCursor(0, 0, gen), gen) == CursorExpired(gen)
    corrupt = [RollbackEntry(4, T0 + 9 * Q, entries[0].member)]                   # names another bar end
    assert events_after(SERIES, corrupt, EventCursor(0, 3, gen), gen) == CursorExpired(gen)
    events = rollback_events(SERIES, entries)
    assert events is not None and all(not e.revised for e in events)


def test_stored_member_forms() -> None:
    b = bar(T0)
    member = encode_member(b, hash_of(b))
    entry = parse_rollback(f"{7:020d}|{b.bucket_end_ms:013d}|{member}")
    assert entry is not None and (entry.write_seq, entry.bar_end_ms, entry.member) == (7, b.bucket_end_ms, member)
    assert parse_rollback("junk") is None
    assert parse_superseded(f"{member}|{T0:013d}") == (member, T0)
    assert removable_member(member) and not removable_member("x" * 149)
    assert "1.23" not in repr(entry)


def test_refetch_queue() -> None:
    q = RefetchQueue(limit=2)
    assert q.request(TICK_15M, T0, T0) == Unsupported("tick_series")
    assert q.request(SERIES, T0 + 5, T0 + Q) == Unsupported("bucket_range")
    assert q.request(SERIES, T0 + Q, T0 + 2 * Q) is None
    q(SERIES, T0, T0)                                                            # the writer callback form
    other = SeriesId("mock", "SYN2", M15, Side.ASK, Source.BROKER)
    third = SeriesId("mock", "SYN3", M15, Side.ASK, Source.BROKER)
    q.request(other, T0, T0)
    q.request(third, T0, T0)
    assert q.dropped == 1 and len(q) == 2
    drained = q.drain()
    assert [(r.series, r.from_bucket_ms, r.to_bucket_ms) for r in drained] == [(other, T0, T0), (third, T0, T0)]
    assert len(q) == 0


async def test_refetch_through_the_port(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, bars=0)
    assert await rig.port.refetch(SERIES, T0, T0 + Q) is None
    assert await rig.port.refetch(TICK_15M, T0, T0) == Unsupported("tick_series")
    assert [(r.from_bucket_ms, r.to_bucket_ms) for r in rig.port.refetch_queue.drain()] == [(T0, T0 + Q)]
