"""Downtime catch-up end to end on the in-memory storage ports (no network; always runs). The PostgreSQL + Redis
version of the restart scenario is tests/e2e/test_downtime_pg.py; see tests/e2e/downtime.py for the checks."""
from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import fakeredis
import pytest

from src.adapters.base import StreamEvent, StreamEventKind
from src.cache import keys as K
from src.db.epoch import Epoch
from src.dto import OrderDTO, OrderStatus, PositionDTO
from tests.e2e.downtime import (
    HOUR_MS,
    MINUTE_MS,
    OVERLAP_MS,
    Process,
    assert_offline_changes_committed,
    assert_pages_carry_no_amounts,
    caught_up,
    entity,
    gate_ctx_verified,
    memory_backend,
    offline_fixture,
    run_restart_scenario,
    script_offline_changes,
    start_process,
)
from tests.e2e.scenario import ACCOUNT, ADAPTER, eventually, wall_ms

E1 = Epoch("cluster-downtime", 21)
E2 = Epoch("cluster-downtime", 22)


async def run_id(_: Any) -> str:
    return "run-downtime"


@pytest.fixture
async def redis() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.flushall()
    await r.aclose()


async def test_restart_catches_up_everything_the_broker_changed_while_the_engine_was_off(
        redis: Any, tmp_path: Path) -> None:
    b, _, store = memory_backend(redis, run_id, E1)
    p2, _ = await run_restart_scenario(b, tmp_path, (E1, E2))
    try:
        # the next heartbeat moves coverage forward to this run (the adapter is caught up and verified)
        await p2.tracker.tick()
        cov = await store.coverage(ADAPTER)
        assert cov is not None and cov.run_id == p2.tracker.run_id and cov.last_streaming_ms >= wall_ms() - MINUTE_MS
    finally:
        await p2.stop("SIGTERM")
    assert (await store.runs())[-1].end_reason == "SIGTERM"


async def test_an_incomplete_history_pages_with_the_fence_held_and_keeps_the_gate_closed(
        redis: Any, tmp_path: Path) -> None:
    b, _, _ = memory_backend(redis, run_id, E1)
    p2, _ = await run_restart_scenario(b, tmp_path, (E1, E2), incomplete=True)
    try:
        # the hold itself is released after the page (Oversee is not capped forever), but the adapter stays
        # unverified for ActionGate for the rest of the term
        assert not p2.session.orchestrator.fence_held(ADAPTER)
        assert not await gate_ctx_verified(p2)
    finally:
        await p2.stop()


async def test_a_wake_from_sleep_re_subscribes_and_catches_up_the_slept_window(redis: Any, tmp_path: Path) -> None:
    """A simulated wall-clock jump (monotonic time stood still while the PC slept): the tracker notices it, the
    session forces RESYNCING and runs a `resume_after_sleep` catch-up from the coverage written before the sleep."""
    b, _, store = memory_backend(redis, run_id, E1)
    asleep_for = 2 * HOUR_MS
    then = wall_ms() - asleep_for
    fx = offline_fixture(then)
    from src.adapters.mock import MockBrokerAdapter
    adapter = MockBrokerAdapter.from_fixture(fx, finite_streams=False)
    offset = [asleep_for]
    mono = [10_000.0]
    p = await start_process(b, adapter, E1, tmp_path, clock_ms=lambda: wall_ms() - offset[0],
                            monotonic=lambda: mono[0])
    try:
        await caught_up(p, E1)

        async def stream_row() -> bool:
            eu = await entity(p, K.position_key(ADAPTER, "P-EU-1"))
            return isinstance(eu, PositionDTO) and eu.units == Decimal(10000)
        await eventually(stream_row, "the stream's P-EU-1 update committed")
        mono[0] += 5.0
        await p.tracker.tick()
        cov = await store.coverage(ADAPTER)
        assert cov is not None and cov.last_streaming_ms <= then + 5 * MINUTE_MS

        # --- the PC sleeps for two hours; the broker moves on ---------------------------------------------
        changes = script_offline_changes(adapter, fx, cov.last_streaming_ms, wall_ms() - MINUTE_MS)
        offset[0] = 0
        mono[0] += 5.0                                   # monotonic barely moved: a wall-clock jump
        await p.tracker.tick()
        assert p.tracker.suspended_gaps == 1

        async def resumed() -> bool:
            recs = await store.catchups()
            return bool(recs) and recs[-1].reason == "resume_after_sleep" and p.session.catchup is not None \
                and p.session.catchup.finished(ADAPTER)
        await eventually(resumed, "resume_after_sleep catch-up recorded")
        rec = (await store.catchups())[-1]
        assert rec.complete and rec.offline_from_ms == cov.last_streaming_ms, rec
        assert rec.gap_start_ms == cov.last_streaming_ms - OVERLAP_MS
        assert rec.orders_new == 4 and rec.positions_closed >= 1, rec
        await assert_offline_changes_committed(p, changes)
        assert_pages_carry_no_amounts(p)
        assert any("engine was offline from" in body for s, _, body, _ in p.pages if s == "info")
        runs = await store.runs()
        assert len(runs) == 1 and runs[0].suspended_gaps == 1

        await caught_up(p, E1)                           # verified and offered for coverage again
        assert p.session.catchup is not None and p.session.catchup.verified(ADAPTER)
        mono[0] += 61.0
        await p.tracker.tick()
        cov2 = await store.coverage(ADAPTER)
        assert cov2 is not None and cov2.last_streaming_ms > cov.last_streaming_ms + HOUR_MS
    finally:
        await p.stop()
    assert not p.exits and not p.leader.exits


async def test_a_lost_broker_socket_reconnects_and_re_reads_what_happened_during_the_outage(
        redis: Any, tmp_path: Path) -> None:
    """The mock now survives a close (reconnect): the socket drops (DISCONNECTED), SM4 goes through BACKOFF, closes
    and reconnects (the mock's streams serve again after `connect()`), and a `reconnect` catch-up finds an order
    placed and filled during the outage (in no reconcile snapshot)."""
    from src.adapters.mock import MockBrokerAdapter
    b, _, store = memory_backend(redis, run_id, E1)
    fx = offline_fixture(wall_ms())
    adapter = MockBrokerAdapter.from_fixture(fx, finite_streams=False)
    p: Process = await start_process(b, adapter, E1, tmp_path)
    try:
        await caught_up(p, E1)
        await p.tracker.tick()
        assert await store.coverage(ADAPTER) is not None
        connects = adapter.connect_count
        filled = OrderDTO(client_order_id="OUT-1", broker_order_id="OUT-1", account_id=ACCOUNT, adapter_id=ADAPTER,
                          symbol="EURUSD", side="BUY", status=OrderStatus.FILLED, entry_price=Decimal("1.0860"),
                          ordered_units=Decimal(1000), remaining_units=Decimal(0), raw_qty=Decimal(1000),
                          broker_updated_at_ms=wall_ms(), source="REST")
        adapter.change_while_off(filled)                 # an order placed and filled while ...
        adapter.push_order_update(StreamEvent(StreamEventKind.DISCONNECTED, "orders", "socket dropped"))  # ... down

        async def reconnected() -> bool:
            recs = await store.catchups()
            return adapter.connect_count > connects and bool(recs) and recs[-1].reason == "reconnect" \
                and p.session.catchup is not None and p.session.catchup.finished(ADAPTER)
        await eventually(reconnected, "reconnect catch-up after the socket loss")
        o = await entity(p, K.order_key(ADAPTER, "OUT-1"))
        assert isinstance(o, OrderDTO) and o.status is OrderStatus.FILLED
        rec = (await store.catchups())[-1]
        assert rec.complete and rec.orders_new == 1, rec
        await caught_up(p, E1)
    finally:
        await p.stop()
    assert not p.exits and not p.leader.exits
