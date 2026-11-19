"""`md_rehash` and key rotation (plan S16; design §A13 Key rotation, R11-M13): a rotation causes no quarantine, no
FAILED and no CORRECTED item, and readers accept both key ids until the re-hash completes. fakeredis + lupa;
synthetic instrument and prices."""
from __future__ import annotations

from typing import Any

from src.marketdata.bootstrap import BootReason, Sm12Event, WorkItem, WorkKind
from src.marketdata.codec import DecodedMember, decode_member
from src.marketdata.hashing import HashComparison, Keyring, compare_hashes, key_id_of
from src.marketdata.port_impl import MdPort
from src.marketdata.ports import BarsRead
from src.marketdata.store import keys as K
from src.marketdata.store.reads import StoreReader
from src.marketdata.types import Quality

from .bootrig import Records, Rig, make_rig
from .conftest import SERIES, T0, TOKEN, RunIds, bar

Q = 900_000
KEY3 = bytes(range(32))
KEY4 = bytes(range(100, 132))
ROTATED = Keyring.of(4, KEY4, [(3, KEY3)])
NEW_ONLY = Keyring.of(4, KEY4)


def end_of(k: int) -> int:
    return T0 + (k + 1) * Q + 60_000


async def stored_key_ids(r: Any) -> list[int | None]:
    out = []
    for member in await r.zrange(K.bars(SERIES), 0, -1):
        decoded = decode_member(SERIES, member)
        assert isinstance(decoded, DecodedMember)
        out.append(key_id_of(decoded.content_hash))
    return out


async def rotated_rig(md_redis: Any, run_ids: RunIds, bars: int = 5) -> Rig:
    records = Records()
    term1 = await make_rig(md_redis, run_ids, start=T0, records=records)
    await term1.at(T0)
    for k in range(bars):
        term1.writer.queue_bar(bar(T0 + k * Q))
        await term1.at(end_of(k))
    await term1.boot.save_record()
    token2 = K.WriterToken(TOKEN.cluster_id, 8, TOKEN.node, "n0nce2")
    rig = await make_rig(md_redis, run_ids, start=end_of(bars), token=token2, keyring=ROTATED, records=records,
                         previous_term_epoch=7)
    await rig.at(end_of(bars))
    rig.book.apply(SERIES, Sm12Event.LIVE)
    return rig


async def read_all(port: MdPort, n: int) -> list[Quality]:
    read = await port.bars(SERIES, from_bucket_ms=T0, to_bucket_ms=T0 + (n - 1) * Q)
    assert isinstance(read, BarsRead)
    return [b.quality for b in read.bars]


async def test_a_key_rotation_quarantines_nothing_and_readers_accept_both_ids(md_redis: Any,
                                                                             run_ids: RunIds) -> None:
    rig = await rotated_rig(md_redis, run_ids)
    assert rig.boot.last_result is not None and rig.boot.last_result.rejected_entries == 0
    assert await read_all(rig.port, 5) == [Quality.COMPLETE] * 5                 # old id verifies (retained)
    events_before = [f[b"k"] for _, f in await md_redis.xrange(K.EVENTS)]
    old_hashes = [e.member[-34:] for e in rig.writer.rollback_memory(SERIES)]
    rig.rehash._page = 2                                                          # several steps
    assert rig.svc.submit(WorkItem(WorkKind.MD_REHASH, 9))
    await rig.at(end_of(5) + 250)                                                 # taken and started
    await rig.at(end_of(5) + 500)                                                 # first page
    mixed = await stored_key_ids(md_redis)
    assert sorted(set(mixed)) == [3, 4]                                           # mid-way: both ids stored
    assert await read_all(rig.port, 5) == [Quality.COMPLETE] * 5                 # and both verify
    assert rig.svc.submit(WorkItem(WorkKind.MD_REHASH, 10))
    for i in range(6):
        await rig.at(end_of(5) + 750 + 250 * i)
    assert (10, "moot", "already_running") in rig.completed
    assert (9, "applied", "rehash_complete") in rig.completed
    assert await stored_key_ids(md_redis) == [4] * 5
    assert rig.rehash.status().complete and rig.rehash.status().rewritten == 5
    assert await md_redis.hget(K.meta(SERIES), "rehash_key") == b"4"
    rb = [m.decode() for m in await md_redis.zrange(K.rollback(SERIES), 0, -1)]   # the rollback set follows
    assert [key_id_of(m[-34:]) for m in rb] == [4] * 5 and [int(m[:20]) for m in rb] == [1, 2, 3, 4, 5]
    memory = rig.writer.rollback_memory(SERIES)
    assert [key_id_of(e.member[-34:]) for e in memory] == [4] * 5 and [e.stored for e in memory] == rb
    assert [f[b"k"] for _, f in await md_redis.xrange(K.EVENTS)] == events_before   # no bar_revised
    assert await md_redis.zcard(K.quarantine(SERIES)) == 0
    assert rig.port.quarantine_requests == 0 and rig.faults == []
    new_only = MdPort(StoreReader(md_redis), keyring=NEW_ONLY, book=rig.book, leader_epoch=lambda: 8,
                      clock_ms=rig.clock)
    assert await read_all(new_only, 5) == [Quality.COMPLETE] * 5                 # the previous key can go
    fresh = await stored_hashes(md_redis)
    assert all(compare_hashes(a, b) is HashComparison.INCOMPARABLE for a, b in zip(old_hashes, fresh))   # never CORRECTED


async def stored_hashes(r: Any) -> list[str]:
    out = []
    for member in await r.zrange(K.bars(SERIES), 0, -1):
        decoded = decode_member(SERIES, member)
        assert isinstance(decoded, DecodedMember)
        out.append(decoded.content_hash)
    return out


async def test_the_previous_key_retires_only_after_completion_and_one_correction_window(md_redis: Any,
                                                                                       run_ids: RunIds) -> None:
    rig = await rotated_rig(md_redis, run_ids, bars=2)
    window = 15 * 60_000 + 5_000
    assert not rig.rehash.retire_allowed(end_of(9), window)                      # not run yet
    rig.svc.submit(WorkItem(WorkKind.MD_REHASH, 1))
    for i in range(4):
        await rig.at(end_of(2) + 250 * (i + 1))
    done_at = rig.rehash.status().completed_at_ms
    assert done_at is not None
    assert not rig.rehash.retire_allowed(done_at + window - 1, window)
    assert rig.rehash.retire_allowed(done_at + window, window)


async def test_a_member_under_a_key_no_longer_held_is_skipped_not_quarantined(md_redis: Any,
                                                                              run_ids: RunIds) -> None:
    rig = await rotated_rig(md_redis, run_ids, bars=2)
    other = Keyring.of(5, bytes(range(50, 82)), [(4, KEY4)])                     # id 3 is no longer held
    rig.rehash._keyring = other                                                   # type: ignore[assignment]
    rig.rehash.start()
    for _ in range(3):
        await rig.rehash.step()
    status = rig.rehash.status()
    assert status.complete and status.skipped == 2 and status.rewritten == 0
    assert await md_redis.zcard(K.quarantine(SERIES)) == 0
    port = MdPort(StoreReader(md_redis), keyring=other, book=rig.book, leader_epoch=lambda: 8, clock_ms=rig.clock,
                  quarantine_sink=rig.writer.quarantine_stored)
    assert await read_all(port, 2) == [Quality.BLIND] * 2                        # missing: blind, never quarantined
    assert port.quarantine_requests == 0 and port.hash_missing == 2


async def test_a_rehash_refused_no_boot_makes_an_idle_writer_bootstrap(md_redis: Any, run_ids: RunIds) -> None:
    rig = await rotated_rig(md_redis, run_ids, bars=2)
    await md_redis.delete(K.BOOT)                                                 # no ticks: no flush would see it
    rig.rehash.start()
    assert await rig.rehash.step() is None                                        # the re-hash write: NO_BOOT
    assert rig.writer.bootstrap_wanted == "no_boot" and rig.writer.boot is None   # the writer was told
    await rig.at(end_of(2) + 250)                                                 # the service bootstraps
    assert rig.resets[-1].reason is BootReason.NO_BOOT and rig.writer.boot is not None
    outcomes = [await rig.rehash.step() for _ in range(4)]                        # retried after the bootstrap
    assert "applied" in outcomes and await stored_key_ids(md_redis) == [4] * 2
