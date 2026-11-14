"""EngineSession review fixes (operations.md §3, §4): DRAINING spills the DLQ when PostgreSQL does not answer
within 2 s, the batch in flight included; the RiskSweep's PostgreSQL writes count as alive. No network:
fakeredis, the e2e memory backend, a hanging DLQ sink."""
from __future__ import annotations

import asyncio
import contextlib
import dataclasses
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import fakeredis
import pytest

from src.db.epoch import Epoch
from src.oversee.sweep import AccountWrite
from src.ports import DlqRow
from src.pubsub.spooler import decode_records
from src.runtime.ports import LeaderPorts
from src.runtime.session import EngineSession, SessionContext, SessionTimings
from src.runtime.supervisor import TaskContext
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import NODE, FakeLease, fixture, mock_adapter, wall_ms
from tests.db.test_oversee_conformance import state as breach_state

EPOCH = Epoch("c", 7)
TIMINGS = SessionTimings(drain_close_s=0.05, drain_flush_s=0.2, drain_spill_s=2.0, drain_record_s=0.5)


async def run_id(_: Any) -> str:
    return "run-d"


@pytest.fixture
async def redis() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.flushall()
    await r.aclose()


class HangingSink:
    """events_dlq on a blackholed / stalled primary: the insert never returns."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.calls = 0

    async def insert_batch(self, rows: Any) -> None:
        self.calls += 1
        self.started.set()
        await asyncio.Event().wait()


def row(i: int) -> DlqRow:
    return DlqRow(event_type="ORDER", entity_id=f"o{i}", adapter_id="mock", raw_payload=f'{{"i":{i}}}',
                  payload_hash=f"h{i}", error_reason="QueueFull")


def build(redis: Any, tmp_path: Path, **replace: Any) -> tuple[EngineSession, MemoryBackend]:
    backend = MemoryBackend(redis, run_id, EPOCH.token)

    def ports(epoch: Epoch) -> LeaderPorts:
        return dataclasses.replace(backend.ports(epoch), **replace)

    fx = fixture(wall_ms())
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=ports,
                         adapters=lambda: [mock_adapter(fx)], lease=FakeLease(), run_id_reader=run_id,
                         exit_fn=lambda reason: None, timings=TIMINGS)
    return EngineSession(ctx, EPOCH), backend


def spilled(session: EngineSession) -> set[str]:
    path = session.spooler.spill_path
    rows, _ = decode_records(path.read_bytes()) if path.exists() else ([], 0)
    return {r.payload_hash for r in rows}


async def test_draining_spills_the_ring_and_the_batch_in_flight(redis: Any, tmp_path: Path) -> None:
    """drain-dlq-flush-loses-rows / draining-flush-timeout-drops-dlq: PostgreSQL hangs past drain_flush_s.
    Before: wait_for cancelled the flush, the popped batch and the ring were lost, no spill file."""
    sink = HangingSink()
    session, _ = build(redis, tmp_path, dlq_sink=sink)
    session.spooler._batch_size = 2                       # several batches: one in flight, the rest in the ring
    for i in range(5):
        assert session.spooler.offer(row(i))
    await asyncio.wait_for(session.drain("abdicate"), 10)
    assert spilled(session) == {f"h{i}" for i in range(5)}
    assert len(session.spooler) == 0 and session.spooler.spill_bytes > 0 and sink.calls == 1


async def test_draining_spills_a_batch_held_by_the_background_flush(redis: Any, tmp_path: Path) -> None:
    """The supervisor's dlq-spooler task is mid-insert (holding the spooler lock) when DRAINING starts;
    the supervisor later cancels it. Its batch still reaches the spill file."""
    sink = HangingSink()
    session, _ = build(redis, tmp_path, dlq_sink=sink)
    session.spooler._batch_size = 3
    for i in range(3):
        session.spooler.offer(row(i))
    background = asyncio.create_task(session.spooler.flush())
    await asyncio.wait_for(sink.started.wait(), 2)
    for i in range(3, 6):
        session.spooler.offer(row(i))
    await asyncio.wait_for(session.drain("switchover"), 10)
    background.cancel()                                   # _stop_supervisor
    with contextlib.suppress(BaseException):
        await background
    assert spilled(session) == {f"h{i}" for i in range(6)}


class StubbornSink:
    """A driver that unwinds slowly when cancelled (connection cleanup on a blackholed primary)."""

    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def insert_batch(self, rows: Any) -> None:
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(3)
            raise


async def test_cancelling_the_spooler_task_after_abandon_is_never_swallowed(redis: Any, tmp_path: Path) -> None:
    """Skeptic review: the abandoned sink waited up to 1 s for the cancelled insert under
    suppress(BaseException), so stop_all's cancel of the dlq-spooler task was swallowed, the task looped on
    and stop_all never returned (DRAINING then ran into the keeper's 10 s deadline and hard-exited). The
    task must end promptly on cancel, and its batch must still reach the spill file."""
    sink = StubbornSink()
    session, _ = build(redis, tmp_path, dlq_sink=sink)
    session.spooler.offer(row(0))
    task = asyncio.create_task(session._spooler_body(TaskContext("dlq-spooler", lambda: 0.0)))
    await asyncio.wait_for(sink.started.wait(), 2)
    session.dlq_sink.abandon()
    await asyncio.sleep(0.05)
    task.cancel()                                          # TaskSupervisor.stop_all
    done, _ = await asyncio.wait({task}, timeout=1.0)
    assert done and task.cancelled()
    for _ in range(100):                                   # a cancelled to_thread append still completes
        if spilled(session) == {"h0"}:
            break
        await asyncio.sleep(0.02)
    assert spilled(session) == {"h0"}


async def test_draining_with_a_healthy_database_delivers_and_spills_nothing(redis: Any, tmp_path: Path) -> None:
    session, backend = build(redis, tmp_path)
    for i in range(4):
        session.spooler.offer(row(i))
    await asyncio.wait_for(session.drain("switchover"), 10)
    assert {r.payload_hash for r in backend.tables.dlq} == {f"h{i}" for i in range(4)}
    assert spilled(session) == set()
    session.spooler.offer(row(9))                         # offered after DRAINING: the file, never lost
    await session.spooler.flush()
    assert spilled(session) == {"h9"}


class Probe:
    """Records whether the RiskSweep task counts as alive (exempt) while each PostgreSQL call waits."""

    def __init__(self, ctx: TaskContext) -> None:
        self.ctx = ctx
        self.seen: dict[str, bool] = {}

    def note(self, what: str) -> None:
        self.seen[what] = self.ctx.exempt


class SlowWriter:
    def __init__(self, probe: Probe) -> None:
        self.probe = probe

    async def write_account(self, batch: AccountWrite) -> None:
        self.probe.note("write_account")

    async def set_quarantine(self, account_id: str, at_ms: int | None, reason: str) -> None:
        self.probe.note("set_quarantine")


class HaltStoreProbe:
    def __init__(self, inner: Any, probe: Probe, label: str) -> None:
        self.inner, self.probe, self.label = inner, probe, label

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[Any]:
        self.probe.note(self.label())  # type: ignore[operator]
        async with self.inner.transaction() as tx:
            yield tx


class DedupeProbe:
    def __init__(self, inner: Any, probe: Probe) -> None:
        self.inner, self.probe = inner, probe

    async def snapshot(self) -> Any:
        self.probe.note("queue_repages")
        return await self.inner.snapshot()

    async def last_sent_of(self, keys: Any) -> Any:
        self.probe.note("queue_repages")
        return await self.inner.last_sent_of(keys)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


async def test_risksweep_postgres_writes_count_as_alive(redis: Any, tmp_path: Path) -> None:
    """risksweep-writes-not-exempt: write_account, halt status / trip / clear and queue_repages run with the
    task exempt (a ~20 s sync-standby COMMIT stall must not make the 5 s stamp go stale). Before: only the
    snapshot reads stamped, every write ran un-exempt."""
    ticks = [0.0]
    ctx = TaskContext("risksweep", lambda: ticks[0])
    probe = Probe(ctx)
    current = ["?"]
    backend0 = MemoryBackend(redis, run_id, EPOCH.token)
    halt_probe = HaltStoreProbe(backend0.oversee, probe, lambda: current[0])  # type: ignore[arg-type]
    session, _ = build(redis, tmp_path, sweep_writer=SlowWriter(probe), halt_store=halt_probe,
                       dedupe=DedupeProbe(backend0.oversee, probe))

    async def fake_run() -> None:
        sweep = session.sweep
        await sweep._writer.write_account(cast(AccountWrite, None))
        await sweep._writer.set_quarantine("A1", 1, "x")
        current[0] = "halt.status"
        await session.halt.status()
        current[0] = "halt.system_clear"
        await session.halt.system_clear(reason="cool_down", now_ms=1)
        assert session.dispatcher is not None
        # One active state: the dispatcher reads only its candidates' notification_dedupe rows (8.2).
        await session.dispatcher.queue_repages([breach_state()], [], 1)
        session._stop.set()

    session.sweep.run = fake_run  # type: ignore[method-assign]
    body = asyncio.create_task(session._risksweep_body(ctx))
    await asyncio.wait_for(session._stop.wait(), 5)
    body.cancel()
    with contextlib.suppress(BaseException):
        await body
    assert probe.seen == {"write_account": True, "set_quarantine": True, "halt.status": True,
                          "halt.system_clear": True, "queue_repages": True}
    # The same HaltSwitch called from another task (housekeeping) never exempts the RiskSweep.
    probe.seen.clear()
    current[0] = "housekeeping"
    await session.halt.status()
    assert probe.seen == {"housekeeping": False}
