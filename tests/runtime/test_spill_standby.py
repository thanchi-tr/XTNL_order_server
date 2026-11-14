"""Node-level spill drain (tier-persistence.md §5): a STANDBY node delivers its spill file once PostgreSQL
accepts writes again, and the start-up drain pages a torn tail it moves aside. Review findings
standby-never-redrains-spill and startup-torn-tail-never-paged. No network: in-memory sinks."""
from __future__ import annotations

import asyncio
from collections.abc import Sequence
from pathlib import Path

import pytest

from src.ports import DlqRow
from src.pubsub.spooler import _append_spill
from src.runtime.spill_standby import NodeSpillDrain


def row(i: int) -> DlqRow:
    return DlqRow(event_type="CASH_FLOW", entity_id=f"t{i}", adapter_id="stonex", raw_payload=f'{{"i":{i}}}',
                  payload_hash=f"h{i}", error_reason="QueueFull")


class Sink:
    def __init__(self, up: bool) -> None:
        self.up = up
        self.rows: list[DlqRow] = []

    async def insert_batch(self, rows: Sequence[DlqRow]) -> None:
        if not self.up:
            raise ConnectionError("PostgreSQL down")
        self.rows.extend(rows)


class Pages:
    def __init__(self) -> None:
        self.got: list[tuple[str, str, str]] = []

    def page(self, severity: str, title: str, body: str) -> None:
        self.got.append((severity, title, body))


def clock() -> int:
    return 1_000


async def test_standby_loop_delivers_a_spill_written_while_draining(tmp_path: Path) -> None:
    """DRAINING spilled while PostgreSQL was down; the node is now STANDBY with no restart. Before: only
    the one start-up attempt existed, so the rows stayed in the file until the next restart."""
    path = tmp_path / "spill.jsonl"
    sink = Sink(up=False)
    drain = NodeSpillDrain(sink, path, node="n1", clock_ms=clock, idle_s=0.01, retry_s=0.01, max_retry_s=0.02)
    assert await drain.drain_at_startup() == 0                 # nothing yet
    _append_spill(path, [row(1), row(2)])                       # what DRAINING wrote
    drain.start()
    try:
        await asyncio.sleep(0.1)
        assert sink.rows == [] and path.stat().st_size > 0     # still down: kept, retried
        sink.up = True
        for _ in range(200):
            if sink.rows:
                break
            await asyncio.sleep(0.01)
    finally:
        await drain.stop()
    assert [r.payload_hash for r in sink.rows] == ["h1", "h2"]
    assert path.stat().st_size == 0 and not drain.running


async def test_stop_waits_for_the_current_attempt_then_cancels(tmp_path: Path) -> None:
    class Hanging:
        async def insert_batch(self, rows: Sequence[DlqRow]) -> None:
            await asyncio.Event().wait()

    path = tmp_path / "spill.jsonl"
    _append_spill(path, [row(1)])
    drain = NodeSpillDrain(Hanging(), path, node="n1", clock_ms=clock, stop_grace_s=0.05)
    drain.start()
    await asyncio.sleep(0.05)
    await asyncio.wait_for(drain.stop(), 2)
    assert not drain.running and path.stat().st_size > 0      # cancelled mid-insert: the file is intact


async def test_startup_drain_pages_a_torn_tail(tmp_path: Path) -> None:
    """A SIGKILL tore the last record. Before: the start-up drain moved it to .torn with a log line only,
    and the session's later baseline already included it, so spill_torn_tail was never paged."""
    path = tmp_path / "spill.jsonl"
    _append_spill(path, [row(1)])
    with open(path, "ab") as fh:
        fh.write(b"\x00\x00\x01\x00torn")                   # header claims 256 bytes; 4 present
    pages = Pages()
    sink = Sink(up=True)
    drain = NodeSpillDrain(sink, path, node="n1", clock_ms=clock, pager=pages)
    assert await drain.drain_at_startup() == 1
    assert [r.payload_hash for r in sink.rows] == ["h1"]
    assert path.with_name(path.name + ".torn").stat().st_size == 8
    assert [(s, t) for s, t, _ in pages.got] == [("high", "integrity:spill_torn_tail")]
    assert "n1" in pages.got[0][2]


async def test_startup_torn_tail_is_paged_even_when_postgres_is_down(tmp_path: Path) -> None:
    path = tmp_path / "spill.jsonl"
    with open(path, "wb") as fh:
        fh.write(b"\x00\x00\x01\x00torn")
    pages = Pages()
    drain = NodeSpillDrain(Sink(up=False), path, node="n1", clock_ms=clock, pager=pages)
    await drain.drain_at_startup()
    assert [t for _, t, _ in pages.got] == ["integrity:spill_torn_tail"]


async def test_engine_startup_drain_pages_a_torn_tail(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Through the real wiring (main.run_engine -> wiring.drain_spill_at_startup): the torn tail the start-up
    drain moves aside is paged (the node pager logs it off-node when no term is running)."""
    import logging

    from src.runtime.config import RuntimeConfig
    from src.runtime.wiring import MOCK_BROKER_VAR, EngineHooks, build_engine, drain_spill_at_startup
    from tests.db.conftest import FakeConn, FakeSource
    from tests.runtime.fakes import FakeClock, FakeEtcd

    env = {"BROKER_ENV": "sandbox", "XTNL_NODE_ID": "node-a", MOCK_BROKER_VAR: "1",
           "XTNL_ETCD_ENDPOINTS": "http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379",
           "XTNL_DEADMAN_URL": "https://deadman.example.invalid/beat",
           "ORDER_SERVER_DSN": "postgresql://u:pw@10.0.1.10:5432/xtnl", "REDIS_URL": "redis://127.0.0.1:6379/0"}
    path = tmp_path / "spill.jsonl"
    with open(path, "wb") as fh:
        fh.write(b"\x00\x00\x01\x00torn")
    config = RuntimeConfig.from_env(env, spill_path=str(path))
    engine = build_engine(config, environ=env, hooks=EngineHooks(etcd=FakeEtcd(FakeClock()),
                                                                 control_source=FakeSource(FakeConn())))
    try:
        with caplog.at_level(logging.WARNING):
            await drain_spill_at_startup(engine, timeout_s=1.0)
    finally:
        await engine.aclose()
    assert any("spill_torn_tail" in r.getMessage() and "PAGE" in r.getMessage() for r in caplog.records)
    assert engine.spill is not None and engine.controller._standby_work is engine.spill
