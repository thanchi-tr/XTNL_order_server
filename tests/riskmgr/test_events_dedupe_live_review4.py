"""Live review round 4: duplicate bar-end events in the rollback set (the production `events_since` path).

Design §A13 (the rollback set holds the last bar-end events; a revision is a newer `write_seq` entry for the same id)
and §B6.6 corrections (same id, same hash: a no-op; a different hash: a correction). `JoinMd.events_since` always
answers `CursorExpired`, so the newest-`write_seq`-per-bar-end dedupe in `BarClock._series_events` and its
`EventsRead` branch were never run by the suite. Tests only; synthetic levels and hashes.
"""
from __future__ import annotations

from src.marketdata.hashing import content_hash
from src.marketdata.ports import EventsRead
from src.marketdata.types import BarEndEvent, EventCursor, SeriesId
from tests.riskmgr.test_arm_join import KEYRING, Join, join_rig
from tests.riskmgr.test_round13 import _to_adjust


def _sid(j: Join) -> SeriesId:
    return SeriesId.parse(next(iter(j.md.bars_by_series)))


def _hash_of(j: Join, sid: SeriesId, bar_end: int) -> str:
    b = [x for x in j.md.bars_by_series[sid.canonical] if x.bucket_end_ms == bar_end][-1]
    return content_hash(KEYRING, sid.canonical, bar_end, b.open, b.high, b.low, b.close)


def _serve(j: Join, events: tuple[tuple[str, int, int, bool], ...], bar_end: int) -> list[EventCursor]:
    """`events_since` answers these (hash, write_seq, revision, revised) events of `bar_end`, in the given order."""
    calls: list[EventCursor] = []

    async def events_since(series: SeriesId, cursor: EventCursor) -> EventsRead:
        calls.append(cursor)
        return EventsRead(tuple(BarEndEvent(series, bar_end, h, seq, rev, revised=revised)
                                for h, seq, rev, revised in events), j.md.generation)

    j.md.events_since = events_since  # type: ignore[method-assign]
    return calls


async def test_a_revision_seen_through_events_wins_over_a_stale_duplicate_in_either_order() -> None:
    j = join_rig()
    try:
        _, b2, _ = await _to_adjust(j)
        sid = _sid(j)
        old_hash = _hash_of(j, sid, b2)
        j.md.revise_bar(b2, "1.2050")
        new_hash = _hash_of(j, sid, b2)
        assert new_hash != old_hash
        # the reversed order (newest first) gives the same answer: neither "first wins" nor "last wins"
        _serve(j, ((new_hash, 7, 1, True), (old_hash, 3, 0, False)), b2)
        assert await j.barclock._series_events(sid, b2) == {b2: new_hash}  # noqa: SLF001
        # production order: ascending write_seq, as the rollback set serves it
        calls = _serve(j, ((old_hash, 3, 0, False), (new_hash, 7, 1, True)), b2)
        assert await j.barclock._series_events(sid, b2) == {b2: new_hash}  # noqa: SLF001
        calls.clear()
        await j.at(b2 + 10_000)
        assert calls and all(c.write_seq == 0 for c in calls)          # §A13 restart-form cursor
        corrected = [r for r in j.decisions("CORRECTED") if r.bar_end_ms == b2]
        assert [r.revision for r in corrected] == [1] and corrected[0].trigger_met is False
    finally:
        j.barclock.close()


async def test_duplicate_same_hash_events_are_a_no_op() -> None:
    j = join_rig()
    try:
        _, b2, _ = await _to_adjust(j)
        sid = _sid(j)
        h = _hash_of(j, sid, b2)
        calls = _serve(j, ((h, 3, 0, False), (h, 4, 0, False)), b2)
        await j.at(b2 + 10_000)
        await j.at(b2 + 20_000)
        assert calls
        assert j.decisions("CORRECTED") == []
        assert [r.outcome for r in j.decisions("POST") if r.bar_end_ms == b2] == ["ADJUST"]
    finally:
        j.barclock.close()


async def test_a_newer_event_with_the_old_hash_is_no_correction() -> None:
    """The direction of the dedupe: the newest `write_seq` decides, even when an older entry has another hash."""
    j = join_rig()
    try:
        _, b2, _ = await _to_adjust(j)
        sid = _sid(j)
        old_hash = _hash_of(j, sid, b2)
        j.md.revise_bar(b2, "1.2050")
        new_hash = _hash_of(j, sid, b2)
        calls = _serve(j, ((new_hash, 3, 1, True), (old_hash, 7, 0, False)), b2)
        assert await j.barclock._series_events(sid, b2) == {b2: old_hash}  # noqa: SLF001
        await j.at(b2 + 10_000)
        assert calls and j.decisions("CORRECTED") == []
    finally:
        j.barclock.close()
