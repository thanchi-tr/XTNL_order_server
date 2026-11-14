from __future__ import annotations

import asyncio
import inspect
import struct
from pathlib import Path

import pytest

from src.pubsub.dlq_rows import dlq_row
from src.pubsub.spooler import DlqSpooler, decode_records, encode_record
from tests.pubsub.conftest import FakeDlqSink, account, cash_flow, order, position


def rows(n: int) -> list:  # type: ignore[type-arg]
    return [dlq_row(order(f"O{i}"), "QueueFull") for i in range(n)]


def test_offer_is_sync_pure_and_creates_no_file(tmp_path: Path) -> None:
    spill = tmp_path / "var" / "dlq_spill.bin"
    sp = DlqSpooler(FakeDlqSink(), capacity=3, spill_path=spill)
    assert not inspect.iscoroutinefunction(DlqSpooler.offer)
    assert all(sp.offer(r) for r in rows(3))
    assert sp.is_full and sp.fill_ratio == 1.0
    assert sp.offer(rows(1)[0]) is False
    assert sp.rejected == 1
    assert not spill.exists() and not spill.parent.exists()


def test_dlq_rows_reference_entities() -> None:
    r = dlq_row(order("B42"), "illegal_transition")
    assert (r.event_type, r.entity_id, r.adapter_id, r.error_reason) == ("ORDER", "B42", "mock", "illegal_transition")
    assert dlq_row(position("P9"), "x").entity_id == "P9"
    assert dlq_row(account(), "x").event_type == "ACCOUNT"
    cf = dlq_row(cash_flow("T7"), "x")
    assert (cf.event_type, cf.entity_id) == ("CASH_FLOW", "T7")
    assert len(r.payload_hash) == 64 and '"_type":"OrderDTO"' in r.raw_payload


async def test_flush_batches_into_sink(tmp_path: Path) -> None:
    sink = FakeDlqSink()
    sp = DlqSpooler(sink, capacity=100, spill_path=tmp_path / "s.bin", batch_size=4)
    for r in rows(10):
        sp.offer(r)
    assert await sp.flush() == 10
    assert [len(b) for b in sink.batches] == [4, 4, 2]
    assert len(sp) == 0
    assert not (tmp_path / "s.bin").exists() or (tmp_path / "s.bin").stat().st_size == 0


async def test_connection_error_spills_then_drain_after_recovery(tmp_path: Path) -> None:
    spill = tmp_path / "s.bin"
    sink = FakeDlqSink()
    sink.down = True
    sp = DlqSpooler(sink, capacity=100, spill_path=spill, batch_size=3)
    batch = rows(7)
    for r in batch:
        sp.offer(r)
    assert await sp.flush() == 0
    assert len(sp) == 0 and spill.stat().st_size > 0 and sp.spill_bytes == spill.stat().st_size
    decoded, good = decode_records(spill.read_bytes())
    assert decoded == batch and good == spill.stat().st_size

    # still down: new rows are appended to the file too, nothing lost
    sp.offer(dlq_row(order("X"), "QueueFull"))
    assert await sp.flush() == 0
    assert len(decode_records(spill.read_bytes())[0]) == 8

    sink.down = False
    assert await sp.flush() == 8
    assert len(sink.rows) == 8
    assert spill.stat().st_size == 0 and sp.spill_bytes == 0
    # idempotent: draining again delivers nothing new
    assert await sp.drain_spill() == 0
    assert len(sink.rows) == 8


async def test_replaying_the_same_file_twice_is_idempotent(tmp_path: Path) -> None:
    spill = tmp_path / "s.bin"
    data = b"".join(encode_record(r) for r in rows(3))
    spill.write_bytes(data)
    sink = FakeDlqSink()
    sp = DlqSpooler(sink, spill_path=spill)
    assert await sp.drain_spill() == 3
    spill.write_bytes(data)        # crash between insert and truncate
    assert await sp.drain_spill() == 3
    assert len(sink.rows) == 3     # ON CONFLICT (payload_hash) DO NOTHING


async def test_crash_left_spill_is_drained_on_first_flush(tmp_path: Path) -> None:
    spill = tmp_path / "s.bin"
    spill.write_bytes(b"".join(encode_record(r) for r in rows(2)))
    sink = FakeDlqSink()
    sp = DlqSpooler(sink, spill_path=spill)
    assert await sp.flush() == 2
    assert len(sink.rows) == 2


@pytest.mark.parametrize("damage", ["partial", "bad_crc", "garbage"])
async def test_torn_tail_moved_aside_never_raises(tmp_path: Path, damage: str) -> None:
    spill = tmp_path / "s.bin"
    good = b"".join(encode_record(r) for r in rows(2))
    last = encode_record(rows(3)[2])
    if damage == "partial":
        tail = last[: len(last) // 2]
    elif damage == "bad_crc":
        length, crc = struct.unpack(">II", last[:8])
        tail = struct.pack(">II", length, crc ^ 1) + last[8:]
    else:
        tail = b"\x00\x00"
    spill.write_bytes(good + tail)
    sink = FakeDlqSink()
    sp = DlqSpooler(sink, spill_path=spill)
    assert await sp.drain_spill() == 2
    assert len(sink.rows) == 2
    torn = tmp_path / "s.bin.torn"
    assert torn.read_bytes() == tail
    assert spill.stat().st_size == 0


async def test_drain_with_sink_down_keeps_file(tmp_path: Path) -> None:
    spill = tmp_path / "s.bin"
    spill.write_bytes(b"".join(encode_record(r) for r in rows(2)))
    sink = FakeDlqSink()
    sink.down = True
    sp = DlqSpooler(sink, spill_path=spill)
    assert await sp.drain_spill() is None
    assert len(decode_records(spill.read_bytes())[0]) == 2


async def test_missing_spill_file_is_fine(tmp_path: Path) -> None:
    sp = DlqSpooler(FakeDlqSink(), spill_path=tmp_path / "nope.bin")
    assert await sp.drain_spill() == 0


async def test_non_connection_sink_error_spills(tmp_path: Path) -> None:
    class Broken(FakeDlqSink):
        async def insert_batch(self, rows):  # type: ignore[no-untyped-def]
            raise RuntimeError("bug")

    spill = tmp_path / "s.bin"
    sp = DlqSpooler(Broken(), spill_path=spill)
    sp.offer(rows(1)[0])
    await sp.flush()
    assert len(decode_records(spill.read_bytes())[0]) == 1


async def test_run_task_flushes_and_stops(tmp_path: Path) -> None:
    sink = FakeDlqSink()
    sp = DlqSpooler(sink, spill_path=tmp_path / "s.bin", flush_interval_s=0.01)
    stop = asyncio.Event()
    task = asyncio.create_task(sp.run(stop))
    sp.offer(rows(1)[0])
    for _ in range(100):
        if sink.rows:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert len(sink.rows) == 1


async def test_failed_append_leaves_no_partial_frame(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """partial-append-loses-later-records: an fsync/ENOSPC failure after part of the blob reached the file
    must not leave a torn frame that later good appends land behind (they would all go to .torn)."""
    import os

    from src.pubsub import spooler as S

    spill = tmp_path / "s.bin"
    sink = FakeDlqSink()
    sink.down = True
    sp = DlqSpooler(sink, capacity=100, spill_path=spill, batch_size=50)
    first = rows(3)
    for r in first:
        sp.offer(r)
    real_fsync = os.fsync
    calls = {"n": 0}

    def torn_fsync(fd: int) -> None:
        calls["n"] += 1
        if calls["n"] == 1:                      # only part of the blob is on disk, then the device fails
            size = os.fstat(fd).st_size
            os.ftruncate(fd, size - 7)
            raise OSError(28, "No space left on device")
        real_fsync(fd)

    monkeypatch.setattr(S.os, "fsync", torn_fsync)
    assert await sp.flush() == 0                 # failed append, rows back to the ring, re-spilled
    assert calls["n"] >= 2 and len(sp) == 0
    decoded, good = decode_records(spill.read_bytes())
    assert good == spill.stat().st_size and decoded == first   # no torn frame in front of the retry
    monkeypatch.setattr(S.os, "fsync", real_fsync)
    later = [dlq_row(order(f"L{i}"), "QueueFull") for i in range(3)]
    for r in later:
        sp.offer(r)
    assert await sp.flush() == 0                 # still down: the whole ring is spilled
    sink.down = False
    assert await sp.flush() == 6
    assert set(sink.rows) == {r.payload_hash for r in first + later}
    torn = spill.with_name(spill.name + ".torn")
    assert not torn.exists() or torn.stat().st_size == 0


async def test_failed_spill_never_drops_accepted_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """spill-failure-silently-drops-ring-rows: rows offer() accepted while the spill write was in flight
    and the rows being spilled all survive a failed write; offer() then rejects (backpressure signal)."""
    import threading

    from src.pubsub import spooler as S

    sink = FakeDlqSink()
    sink.down = True
    sp = DlqSpooler(sink, capacity=8, spill_path=tmp_path / "s.bin", batch_size=50)
    old = rows(5)
    for r in old:
        assert sp.offer(r)
    entered, release = threading.Event(), threading.Event()

    def failing_append(path: Path, batch: object) -> int:
        entered.set()
        release.wait(5)
        raise OSError(5, "I/O error")

    monkeypatch.setattr(S, "_append_spill", failing_append)
    task = asyncio.create_task(sp.flush())
    while not entered.is_set():
        await asyncio.sleep(0.001)
    new = [dlq_row(order(f"N{i}"), "QueueFull") for i in range(3)]
    for r in new:
        assert sp.offer(r)                       # room left beside the 5 rows in flight: accepted
    assert sp.offer(dlq_row(order("X"), "QueueFull")) is False   # in-flight rows still count
    release.set()
    assert await task == 0
    assert len(sp) == 8 and sp.rejected == 1 and sp.spill_failures >= 1
    assert sp.is_full and sp.offer(dlq_row(order("Z"), "QueueFull")) is False
    monkeypatch.undo()
    sink.down = False
    assert await sp.flush() == 8
    assert [r.payload_hash for b in sink.batches for r in b] == [r.payload_hash for r in old + new]


async def test_repeated_spill_failures_keep_the_ring_bounded(tmp_path: Path,
                                                             monkeypatch: pytest.MonkeyPatch) -> None:
    """Rows taken out of the ring for a spill write still count against capacity until they are durable:
    offer() must not refill the emptied ring during the write. Otherwise every failed spill (disk full,
    PostgreSQL down) adds up to `capacity` more accepted rows, memory grows without bound and offer()
    never returns False, so no gap / backpressure_overflow is ever raised."""
    import threading

    from src.pubsub import spooler as S

    sink = FakeDlqSink()
    sink.down = True
    sp = DlqSpooler(sink, capacity=5, spill_path=tmp_path / "s.bin", batch_size=50)
    first = rows(5)
    for r in first:
        assert sp.offer(r)
    entered = threading.Event()

    def failing_append(path: Path, batch: object) -> int:
        entered.set()
        raise OSError(28, "No space left on device")

    accepted: list[bool] = []
    real_to_thread = asyncio.to_thread

    async def to_thread_with_offers(fn: object, *a: object) -> object:
        if fn is failing_append:                  # the parser keeps offering while the write is in flight
            accepted.extend(sp.offer(dlq_row(order(f"N{len(accepted)}"), "QueueFull")) for _ in range(5))
        return await real_to_thread(fn, *a)  # type: ignore[arg-type]

    monkeypatch.setattr(S, "_append_spill", failing_append)
    monkeypatch.setattr(S.asyncio, "to_thread", to_thread_with_offers)
    for _ in range(4):
        assert await sp.flush() == 0
        assert len(sp) <= sp.capacity and sp.is_full
    assert entered.is_set() and not any(accepted) and sp.rejected == len(accepted)
    monkeypatch.undo()
    sink.down = False
    assert await sp.flush() == 5
    assert [r.payload_hash for b in sink.batches for r in b] == [r.payload_hash for r in first]


# ===================================================================== 4.9: flush() safe to cancel
class _BlockingSink(FakeDlqSink):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.block = True

    async def insert_batch(self, rows: object) -> None:
        if self.block:
            self.entered.set()
            await asyncio.Event().wait()                 # hangs until cancelled
        await super().insert_batch(rows)  # type: ignore[arg-type]


async def test_cancelled_insert_requeues_the_popped_batch(tmp_path: Path) -> None:
    """A flush cancelled while its batch is in the sink insert puts the batch back in front of the ring,
    in order (nothing proves it durable); the next flush delivers every row exactly as offered."""
    sink = _BlockingSink()
    sp = DlqSpooler(sink, capacity=10, spill_path=tmp_path / "s.bin", batch_size=3)
    batch = rows(5)
    for r in batch:
        sp.offer(r)
    task = asyncio.create_task(sp.flush())
    await sink.entered.wait()
    assert len(sp) == 2 and sp.fill_ratio == 0.5        # 3 in flight still count
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(sp) == 5 and sp.fill_ratio == 0.5 and sp._inflight == 0
    assert [r.payload_hash for r in sp._ring] == [r.payload_hash for r in batch]
    sink.block = False
    assert await sp.flush() == 5
    assert [r.payload_hash for b in sink.batches for r in b] == [r.payload_hash for r in batch]


@pytest.mark.parametrize("write_fails", [False, True])
async def test_cancelled_spill_waits_for_the_write_and_keeps_every_row(tmp_path: Path,
                                                                       monkeypatch: pytest.MonkeyPatch,
                                                                       write_fails: bool) -> None:
    """A flush cancelled during a spill write cannot stop the worker thread: it waits for it (so the file
    never gets a second writer), keeps the rows in the file if the write succeeded or puts them back in the
    ring if it failed, and only then re-raises the cancellation."""
    import threading

    from src.pubsub import spooler as S

    spill = tmp_path / "s.bin"
    sink = FakeDlqSink()
    sink.down = True
    sp = DlqSpooler(sink, capacity=10, spill_path=spill, batch_size=50)
    batch = rows(4)
    for r in batch:
        sp.offer(r)
    await sp.drain_spill()                               # spill state known: flush goes straight to the ring
    entered, release = threading.Event(), threading.Event()
    real_append = S._append_spill

    def slow_append(path: Path, rs: object) -> int:
        entered.set()
        release.wait(5)
        if write_fails:
            raise OSError(28, "No space left on device")
        return real_append(path, rs)  # type: ignore[arg-type]

    monkeypatch.setattr(S, "_append_spill", slow_append)
    task = asyncio.create_task(sp.flush())
    while not entered.is_set():
        await asyncio.sleep(0.001)
    task.cancel()
    await asyncio.sleep(0.02)
    assert not task.done()                               # still waiting for the thread
    assert sp._lock.locked()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sp._inflight == 0 and not sp._lock.locked()
    if write_fails:
        assert [r.payload_hash for r in sp._ring] == [r.payload_hash for r in batch] and sp.spill_failures == 1
    else:
        assert len(sp) == 0 and sp.spill_bytes == spill.stat().st_size > 0
    monkeypatch.undo()
    sink.down = False
    assert await sp.flush() == 4
    assert set(sink.rows) == {r.payload_hash for r in batch}


async def test_cancelled_spill_replay_keeps_the_file(tmp_path: Path) -> None:
    """Cancelled while replaying the spill file into PostgreSQL: the file is not truncated, so the rows
    are replayed again later (idempotent on payload_hash)."""
    spill = tmp_path / "s.bin"
    down = FakeDlqSink()
    down.down = True
    sp = DlqSpooler(down, capacity=10, spill_path=spill)
    for r in rows(3):
        sp.offer(r)
    await sp.flush()
    assert spill.stat().st_size > 0
    sink = _BlockingSink()
    sp2 = DlqSpooler(sink, capacity=10, spill_path=spill)
    task = asyncio.create_task(sp2.flush())
    await sink.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert spill.stat().st_size > 0
    sink.block = False
    assert await sp2.flush() == 3 and spill.stat().st_size == 0


async def test_cancel_every_task_sweep_still_waits_for_the_spill_thread(tmp_path: Path,
                                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    """A cancel-everything sweep (asyncio.run shutdown, a supervisor) cancels every Task, not just the
    flush. The spill thread is awaited through the executor's own future, so the flush still waits for the
    write to finish (no second writer can start beside it) and keeps the rows it wrote."""
    import threading

    from src.pubsub import spooler as S

    spill = tmp_path / "s.bin"
    sink = FakeDlqSink()
    sink.down = True
    sp = DlqSpooler(sink, capacity=10, spill_path=spill, batch_size=50)
    batch = rows(3)
    for r in batch:
        sp.offer(r)
    await sp.drain_spill()
    entered, release = threading.Event(), threading.Event()
    real_append = S._append_spill

    def slow_append(path: Path, rs: object) -> int:
        entered.set()
        release.wait(5)
        return real_append(path, rs)  # type: ignore[arg-type]

    monkeypatch.setattr(S, "_append_spill", slow_append)
    task = asyncio.create_task(sp.flush())
    while not entered.is_set():
        await asyncio.sleep(0.001)
    me = asyncio.current_task()
    for t in asyncio.all_tasks():
        if t is not me:
            t.cancel()
    await asyncio.sleep(0.02)
    assert not task.done() and sp._lock.locked()         # still waiting for the thread
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(sp) == 0 and sp._inflight == 0 and sp.spill_bytes == spill.stat().st_size > 0
    monkeypatch.undo()
    sink.down = False
    assert await sp.flush() == 3
    assert set(sink.rows) == {r.payload_hash for r in batch}
