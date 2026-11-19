"""S39b gate `entry_time_source` (ingestion side): `IngestionOrchestrator.history_execution_time` reads the broker's
trade history (`fetch_history`) at the reconcile REST priority and returns only the entry order's opening fill times;
it publishes, versions and writes nothing. Synthetic ids and times only."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from src.adapters.mock import MockBrokerAdapter
from src.adapters.ratelimit import RestPriority
from src.dto import OrderStatus
from src.runtime.arm_ports import OrchestratorExecutionTimes
from tests.pubsub.conftest import ManualClock, order
from tests.pubsub.test_orchestrator import make


async def test_the_history_gives_the_entry_orders_fill_time_read_only(tmp_path: Path, clock: ManualClock) -> None:
    mock = MockBrokerAdapter(wall_ms=lambda: clock.ms)
    env = make(tmp_path, clock, adapter=mock)  # type: ignore[arg-type]
    fill = clock.ms - 600_000
    mock.list_in_history(
        order("E1", OrderStatus.WORKING, t=fill - 60_000),                             # its placement
        order("E1", OrderStatus.FILLED, t=fill),                                       # the entry fill
        replace(order("C1", OrderStatus.FILLED, t=fill + 5_000), is_reduce_only=True,  # a closing row: never entry
                linked_position_id="E1"),
        order("E2", OrderStatus.FILLED, t=fill - 1_000))                               # another order
    mock.rest_calls.clear()
    got = await env.orch.history_execution_time("mock", "E1", fill - 3_600_000, clock.ms)
    assert got.fill_times_ms == (fill,) and got.first_fill_ms == fill and got.complete
    assert mock.rest_calls == [("fetch_history", RestPriority.RECONCILE)]
    assert env.orders() == [] and env.positions() == []                                 # nothing published
    missing = await env.orch.history_execution_time("mock", "NOPE", fill - 3_600_000, clock.ms)
    assert missing.fill_times_ms == () and missing.first_fill_ms is None


async def test_the_runtime_port_maps_the_answer(tmp_path: Path, clock: ManualClock) -> None:
    mock = MockBrokerAdapter(wall_ms=lambda: clock.ms)
    env = make(tmp_path, clock, adapter=mock)  # type: ignore[arg-type]
    fill = clock.ms - 120_000
    mock.list_in_history(order("E9", OrderStatus.PARTIALLY_FILLED, t=fill))
    port = OrchestratorExecutionTimes(env.orch, adapters=frozenset({"mock"}))
    got = await port.execution_time("mock", "E9", fill - 60_000, clock.ms)
    assert (got.fill_ms, got.fills, got.complete) == (fill, 1, True) and port.reads == 1
    reduce_only = await port.execution_time("mock", "C9", fill - 60_000, clock.ms)
    assert reduce_only.fill_ms is None
    mock.set_history_supported(False)
    none = await port.execution_time("mock", "E9", fill - 60_000, clock.ms)
    assert none.fill_ms is None and none.complete is False


async def test_the_runtime_port_does_no_history_read_on_an_adapter_whose_ids_cannot_match(
        tmp_path: Path, clock: ManualClock) -> None:
    """Only StoneX history ids can match an entry order: another adapter is answered at once, with no read."""
    mock = MockBrokerAdapter(wall_ms=lambda: clock.ms)
    env = make(tmp_path, clock, adapter=mock)  # type: ignore[arg-type]
    mock.list_in_history(order("E9", OrderStatus.FILLED, t=clock.ms - 120_000))
    mock.rest_calls.clear()
    port = OrchestratorExecutionTimes(env.orch)                     # the runtime default: StoneX only
    assert port.supports("stonex") and not port.supports("mock") and not port.supports("pepperstone")
    got = await port.execution_time("mock", "E9", clock.ms - 600_000, clock.ms)
    assert (got.fill_ms, got.complete) == (None, False) and port.reads == 0 and mock.rest_calls == []
