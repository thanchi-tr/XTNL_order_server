"""DlqSpooler: in-memory DLQ ring + batch flush + crash-safe spill file (dto-contracts.md §4,
tier-persistence.md §5).

offer() is a pure in-memory append: no I/O, no fsync, no await. flush() batch-inserts through a
DlqSink; on ConnectionError the batch goes to a local spill file via asyncio.to_thread
(record = 4-byte big-endian length + 4-byte CRC32 + JSON, fsync'd off the loop). drain_spill()
replays the file idempotently (payload_hash) and truncates it after success. A torn tail is moved
aside to <file>.torn with a warning and never raises.

flush() is safe to cancel (4.9): a batch popped from the ring for a sink insert goes back in front of the
ring when the insert is cancelled (the sink is idempotent on payload_hash, so a row the cancelled insert
did commit is only a no-op duplicate later). A spill-file operation runs in a worker thread that cannot
be stopped, so a cancelled flush first waits for that thread to finish (one append + fsync), keeps its
rows if the write succeeded or puts them back in the ring if it failed, and only then re-raises the
cancellation, still under the lock: the file never has two writers.
"""
from __future__ import annotations

import asyncio
import contextvars
import functools
import json
import logging
import os
import struct
import zlib
from collections import deque
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any, TypeVar

from src.ports import DlqRow, DlqSink

log = logging.getLogger(__name__)

DEFAULT_SPILL_PATH = Path("var/dlq_spill.jsonl")  # CRC-framed records (skill §3), not plain JSON lines
_HEADER = struct.Struct(">II")  # length, crc32
MAX_RECORD_BYTES = 16 * 1024 * 1024
_T = TypeVar("_T")


# --- spill-file framing (pure functions, run in a worker thread) -----------------------------
def encode_record(row: DlqRow) -> bytes:
    body = json.dumps(asdict(row), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return _HEADER.pack(len(body), zlib.crc32(body)) + body


def _row_from(obj: Any) -> DlqRow:
    if not isinstance(obj, dict):
        raise ValueError("record is not an object")
    origin = obj.get("origin_dlq_id")
    return DlqRow(
        event_type=obj["event_type"],
        entity_id=str(obj["entity_id"]),
        adapter_id=str(obj["adapter_id"]),
        raw_payload=str(obj["raw_payload"]),
        payload_hash=str(obj["payload_hash"]),
        error_reason=str(obj["error_reason"]),
        origin_dlq_id=None if origin is None else int(origin),
    )


def decode_records(data: bytes) -> tuple[list[DlqRow], int]:
    """Parse framed records; returns (rows, good_length). Bytes after good_length are torn."""
    rows: list[DlqRow] = []
    pos = 0
    while pos < len(data):
        if len(data) - pos < _HEADER.size:
            break
        length, crc = _HEADER.unpack_from(data, pos)
        start = pos + _HEADER.size
        end = start + length
        if length > MAX_RECORD_BYTES or end > len(data):
            break
        body = data[start:end]
        if zlib.crc32(body) != crc:
            break
        try:
            rows.append(_row_from(json.loads(body)))
        except (ValueError, KeyError, TypeError):
            break
        pos = end
    return rows, pos


_APPEND_FLAGS = os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_BINARY", 0)


def _append_spill(path: Path, rows: Sequence[DlqRow]) -> int:
    """Append framed records and fsync. All or nothing: if the write or fsync fails part-way (ENOSPC,
    EIO), the file is truncated back to its size before the append (and fsync'd) before the error is
    re-raised, so a failed append never leaves a partial frame for later appends to land behind (the
    reader stops at the first bad frame and would move every later good record aside to .torn).

    Unbuffered OS writes, so no buffered remainder can be flushed after the rollback on close."""
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = memoryview(b"".join(encode_record(r) for r in rows))
    fd = os.open(path, _APPEND_FLAGS, 0o600)
    try:
        start = os.lseek(fd, 0, os.SEEK_END)
        try:
            done = 0
            while done < len(blob):
                n = os.write(fd, blob[done:])
                if n <= 0:
                    raise OSError(f"short write to {path}")
                done += n
            os.fsync(fd)
        except BaseException:
            _rollback_append(fd, start)
            raise
        return os.fstat(fd).st_size
    finally:
        os.close(fd)


def _rollback_append(fd: int, start: int) -> None:
    """Best effort: cut a failed append back to `start`. If even that fails, the torn frame stays and
    the reader moves it (and what follows) aside with an alert, as for a SIGKILL."""
    try:
        os.ftruncate(fd, start)
        os.fsync(fd)
    except OSError:
        log.exception("dlq spill: could not roll back a failed append at offset %d", start)


def _read_spill(path: Path) -> tuple[list[DlqRow], int]:
    """Read the spill file; move a torn tail aside to <file>.torn and truncate to the good prefix."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return [], 0
    rows, good = decode_records(data)
    if good < len(data):
        torn = path.with_name(path.name + ".torn")
        with open(torn, "ab") as fh:
            fh.write(data[good:])
            fh.flush()
            os.fsync(fh.fileno())
        with open(path, "r+b") as fh:
            fh.truncate(good)
            fh.flush()
            os.fsync(fh.fileno())
        log.warning("dlq spill: torn tail of %d bytes moved aside to %s", len(data) - good, torn)
    return rows, good


def _truncate_spill(path: Path) -> None:
    try:
        with open(path, "r+b") as fh:
            fh.truncate(0)
            fh.flush()
            os.fsync(fh.fileno())
    except FileNotFoundError:
        pass


async def _settle_thread(fut: asyncio.Future[_T]) -> bool:
    """Wait for a worker-thread future even through cancellation (the thread cannot be stopped, and a
    second spill-file writer must never start beside it). Returns True if the caller was cancelled
    meanwhile; the caller then re-raises CancelledError once it has accounted for the result."""
    cancelled = False
    while not fut.done():
        try:
            await asyncio.shield(fut)
        except asyncio.CancelledError:
            cancelled = True
        except BaseException:
            break                      # the future's own exception: read by the caller via result()
    return cancelled


async def _in_thread(fn: Any, *args: Any) -> tuple[Any, BaseException | None, bool]:
    """Run fn in a worker thread, cancellation-safe: (result, error, cancelled).

    The thread is awaited through the executor's own future, not a Task wrapping asyncio.to_thread: a
    Task is cancelled by a cancel-everything sweep (asyncio.run's shutdown, a supervisor), which would
    end the wait while the thread is still writing the file, letting a second writer start beside it."""
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    fut: asyncio.Future[Any] = loop.run_in_executor(None, functools.partial(ctx.run, fn, *args))
    cancelled = await _settle_thread(fut)
    if fut.cancelled():                # only if someone cancelled the executor future itself
        return None, asyncio.CancelledError(), True
    err = fut.exception()
    return (None if err is not None else fut.result()), err, cancelled


class DlqSpooler:
    """Bounded in-memory DLQ ring with a durable spill file for PostgreSQL outages."""

    def __init__(self, sink: DlqSink, capacity: int = 10_000, spill_path: Path | str = DEFAULT_SPILL_PATH,
                 batch_size: int = 500, flush_interval_s: float = 0.5) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._sink = sink
        self._capacity = capacity
        self._ring: deque[DlqRow] = deque()
        self.spill_path = Path(spill_path)
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s
        self._lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._spill_known = False     # until the first drain we do not know whether a crash left a file
        self.spill_bytes = 0          # cached size of the spill file (health record; no I/O to read it)
        self.offered = 0
        self.rejected = 0
        self.spilled = 0
        self.torn_drops = 0
        self.spill_failures = 0       # spill writes that failed (their rows went back to the ring)
        #: Rows taken out of the ring for a sink insert or a spill write that are not durable yet. They
        #: still count against `capacity` (offer / is_full / fill_ratio): a failed write puts them back,
        #: so the emptied ring must not be refilled meanwhile, or every failed spill would add up to
        #: `capacity` more accepted rows (unbounded memory, and offer() would never signal backpressure).
        self._inflight = 0

    # --- producer side: pure in-memory ---------------------------------------------------------
    def offer(self, row: DlqRow) -> bool:
        """Append to the ring. False when the ring is full (caller sets backpressure_overflow).

        Rows in flight to the sink or the spill file count as held until they are durable, so the ring
        plus the in-flight rows never exceed `capacity`, and a failed write that returns its rows to the
        ring never drops one and never pushes the ring past `capacity`."""
        if self._held() >= self._capacity:
            self.rejected += 1
            return False
        self._ring.append(row)
        self.offered += 1
        if len(self._ring) >= self._batch_size:
            self._wake.set()
        return True

    @property
    def capacity(self) -> int:
        return self._capacity

    def __len__(self) -> int:
        return len(self._ring)

    def _held(self) -> int:
        return len(self._ring) + self._inflight

    @property
    def is_full(self) -> bool:
        return self._held() >= self._capacity

    @property
    def fill_ratio(self) -> float:
        """Ring plus in-flight (not yet durable) rows over capacity."""
        return self._held() / self._capacity

    # --- flush side ---------------------------------------------------------------------------
    async def flush(self) -> int:
        """Drain the spill file (if any), then the ring, into the sink. Returns rows delivered."""
        async with self._lock:
            delivered = 0
            if self.spill_bytes > 0 or not self._spill_known:
                drained = await self._drain_spill_locked()
                if drained is None:      # sink still down: keep spilling the ring
                    await self._spill_ring_locked()
                    return 0
                delivered += drained
            while self._ring:
                batch = [self._ring.popleft() for _ in range(min(self._batch_size, len(self._ring)))]
                self._inflight += len(batch)
                try:
                    try:
                        await self._sink.insert_batch(batch)
                    except asyncio.CancelledError:
                        # Cancelled mid-insert: nothing proves the rows are durable. Back in front of the
                        # ring, in order (they were counted in flight, so the ring stays within capacity).
                        self._ring.extendleft(reversed(batch))
                        raise
                    except ConnectionError:
                        await self._spill_locked(batch)
                        await self._spill_ring_locked()
                        break
                    except Exception:
                        log.exception("dlq sink failed on a batch of %d rows; spilling", len(batch))
                        await self._spill_locked(batch)
                        break
                finally:
                    self._inflight -= len(batch)
                delivered += len(batch)
            return delivered

    async def drain_spill(self) -> int | None:
        """Replay the spill file into the sink and truncate it. None if the sink is still down."""
        async with self._lock:
            return await self._drain_spill_locked()

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """The flush task: flush every interval, or sooner once a batch is waiting."""
        stop = stop or asyncio.Event()
        while not stop.is_set():
            try:
                await self.flush()
            except Exception:
                log.exception("dlq flush failed")
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), self._flush_interval_s)
            except TimeoutError:
                pass
        await self.flush()

    # --- internals ----------------------------------------------------------------------------
    async def _drain_spill_locked(self) -> int | None:
        rows_good, err, cancelled = await _in_thread(_read_spill, self.spill_path)
        if cancelled:
            raise asyncio.CancelledError
        if err is not None:
            if not isinstance(err, OSError):
                raise err
            log.error("dlq spill: cannot read %s: %r", self.spill_path, err)
            return 0
        rows, good = rows_good
        self._spill_known = True
        self.spill_bytes = good
        if not rows:
            return 0
        # A cancelled replay leaves the file untouched (replayed again later, idempotent on payload_hash).
        try:
            for i in range(0, len(rows), self._batch_size):
                await self._sink.insert_batch(rows[i:i + self._batch_size])
        except ConnectionError:
            return None
        except Exception:
            log.exception("dlq spill: sink rejected replay of %s", self.spill_path)
            return None
        _, err, cancelled = await _in_thread(_truncate_spill, self.spill_path)
        if err is None:
            self.spill_bytes = 0
            log.info("dlq spill: drained %d rows from %s", len(rows), self.spill_path)
        if cancelled:
            raise asyncio.CancelledError
        if err is not None:
            raise err
        return len(rows)

    async def _spill_ring_locked(self) -> None:
        if self._ring:
            batch = list(self._ring)
            self._ring.clear()
            self._inflight += len(batch)
            try:
                await self._spill_locked(batch)
            finally:
                self._inflight -= len(batch)

    async def _spill_locked(self, rows: Sequence[DlqRow]) -> None:
        if not rows:
            return
        size, err, cancelled = await _in_thread(_append_spill, self.spill_path, rows)
        if err is None:
            self.spill_bytes = size
            self._spill_known = True
            self.spilled += len(rows)
            log.warning("dlq sink unavailable: spilled %d rows to %s", len(rows), self.spill_path)
        else:
            # Every one of these rows was accepted by offer() (True), so the caller opened no gap for
            # it: never drop one. All go back in front of the ring, in order. They were counted in
            # flight during the write, so offer() could not refill the ring meanwhile and the ring
            # stays within `capacity`; offer() keeps rejecting (the backpressure_overflow signal).
            self.spill_failures += 1
            log.error("dlq spill write failed (%r); returning %d rows to the ring (%d queued, capacity %d)",
                      err, len(rows), len(self._ring) + len(rows), self._capacity)
            self._ring.extendleft(reversed(rows))
        if cancelled:
            raise asyncio.CancelledError
