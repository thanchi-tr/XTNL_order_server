"""Spill drain service (tier-persistence.md §5): start-up barrier, drain on recovery, torn tail, health."""
from __future__ import annotations

import asyncio
from collections.abc import Sequence
from pathlib import Path

from src.ports import DlqRow
from src.pubsub.dlq_rows import dlq_row
from src.pubsub.spooler import DlqSpooler, encode_record
from src.integrity.spill import SpillDrainService
from tests.integrity.fakes import ManualClock, Pages, order


class Sink:
    def __init__(self) -> None:
        self.rows: dict[str, DlqRow] = {}
        self.down = False

    async def insert_batch(self, rows: Sequence[DlqRow]) -> None:
        if self.down:
            raise ConnectionError("postgres down")
        for r in rows:
            self.rows.setdefault(r.payload_hash, r)


def rows(n: int) -> list[DlqRow]:
    return [dlq_row(order(f"B{i}"), "QueueFull") for i in range(n)]


def make(tmp_path: Path, sink: Sink) -> tuple[SpillDrainService, DlqSpooler, ManualClock, Pages]:
    clock, pages = ManualClock(), Pages()
    sp = DlqSpooler(sink, spill_path=tmp_path / "dlq_spill.bin")
    return SpillDrainService(sp, node="node-a", clock_ms=clock, pager=pages, retry_s=0.0, idle_poll_s=0.0), \
        sp, clock, pages


async def test_startup_drains_crash_leftover(tmp_path: Path) -> None:
    sink = Sink()
    (tmp_path / "dlq_spill.bin").write_bytes(b"".join(encode_record(r) for r in rows(3)))
    svc, sp, _, pages = make(tmp_path, sink)
    assert await svc.drain_at_startup() == 3
    assert len(sink.rows) == 3 and svc.spill_size() == 0
    assert (tmp_path / "dlq_spill.bin").stat().st_size == 0
    assert pages.codes() == [] and svc.drained_rows == 3


async def test_torn_tail_is_moved_aside_with_a_page(tmp_path: Path) -> None:
    sink = Sink()
    blob = b"".join(encode_record(r) for r in rows(2))
    (tmp_path / "dlq_spill.bin").write_bytes(blob + encode_record(rows(3)[2])[:-5])   # SIGKILL mid-write
    svc, _, _, pages = make(tmp_path, sink)
    assert await svc.drain_at_startup() == 2
    assert pages.codes() == ["spill_torn_tail"]
    assert (tmp_path / "dlq_spill.bin.torn").exists()
    assert svc.health().torn_bytes > 0
    assert await svc.drain_once() == 0 and pages.codes() == ["spill_torn_tail"]   # no repeat


async def test_garbage_spill_never_raises(tmp_path: Path) -> None:
    (tmp_path / "dlq_spill.bin").write_bytes(b"\xff" * 64)
    svc, _, _, pages = make(tmp_path, Sink())
    assert await svc.drain_at_startup() == 0
    assert pages.codes() == ["spill_torn_tail"]


async def test_database_down_at_startup_then_drained_on_recovery(tmp_path: Path) -> None:
    sink = Sink()
    sink.down = True
    (tmp_path / "dlq_spill.bin").write_bytes(b"".join(encode_record(r) for r in rows(2)))
    svc, sp, clock, pages = make(tmp_path, sink)
    assert await svc.drain_at_startup() is None
    assert svc.spill_size() > 0 and svc.health().non_empty_since_ms == clock()
    clock.advance(61_000)
    assert await svc.drain_once() is None
    assert pages.codes() == ["spill_file_non_empty"]
    assert pages.alerts[0].extra["node"] == "node-a"
    sink.down = False
    stop = asyncio.Event()
    task = asyncio.create_task(svc.run(stop))
    for _ in range(100):
        if svc.spill_size() == 0:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, 1.0)
    assert len(sink.rows) == 2 and svc.health().non_empty_since_ms is None
    assert svc.liveness_ms == clock()


async def test_flush_spills_then_service_drains(tmp_path: Path) -> None:
    sink = Sink()
    svc, sp, _, _ = make(tmp_path, sink)
    sink.down = True
    for r in rows(4):
        sp.offer(r)
    await sp.flush()                                  # PostgreSQL down: ring spilled to the file
    assert svc.spill_size() > 0 and not sink.rows
    sink.down = False
    assert await svc.drain_once() == 4
    assert svc.health().spill_bytes == 0
