"""OrchestratorDlqBridge over a real IngestionOrchestrator + MockBrokerAdapter (no network)."""
from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path

from src.adapters.mock import MockBrokerAdapter
from src.dto import AccountDTO, CashFlowDTO
from src.integrity.bridge import OrchestratorDlqBridge
from src.integrity.dlq_recovery import DlqOutcome, DlqRecoveryWorker
from src.ports import DlqRow, StreamKind
from src.pubsub.bus import EventBus
from src.pubsub.connection import ConnState
from src.pubsub.hlc import HybridLogicalClock
from src.pubsub.orchestrator import IngestionOrchestrator
from src.pubsub.spooler import DlqSpooler
from tests.integrity.fakes import T0, FakeDlqStore, ManualClock, Pages, order, position


class Sink:
    async def insert_batch(self, rows: Sequence[DlqRow]) -> None:
        return None


class Health:
    async def mark_lost(self, adapter_id: str, stream: StreamKind) -> None: ...

    async def mark_reconciled(self, adapter_id: str, stream: StreamKind) -> None: ...

    async def heartbeat(self, adapter_id: str, stream: StreamKind) -> None: ...


def make(tmp_path: Path) -> tuple[OrchestratorDlqBridge, IngestionOrchestrator, MockBrokerAdapter, EventBus]:
    clock = ManualClock()
    bus = EventBus()
    orch = IngestionOrchestrator(bus, DlqSpooler(Sink(), spill_path=tmp_path / "s.bin"), Health(),
                                 HybridLogicalClock(clock), clock_ms=clock, roll_ms=lambda now: T0 - 3_600_000)
    adapter = MockBrokerAdapter(
        adapter_id="stonex", orders=[order("B1")], positions=[position("P1")],
        accounts=[AccountDTO(account_id="ACC", adapter_id="stonex", currency="USD", balance=Decimal("1"))],
        cash_flows=[CashFlowDTO(transaction_id="T1", account_id="ACC", adapter_id="stonex", kind="DEPOSIT",
                                amount=Decimal("5"), currency="USD", broker_ts=T0 - 1_000)])
    orch.add_adapter(adapter)
    return OrchestratorDlqBridge(orch, lambda: ["stonex"]), orch, adapter, bus


async def test_streaming_adapters_follow_sm4(tmp_path: Path) -> None:
    bridge, orch, _, _ = make(tmp_path)
    assert bridge.streaming_adapters() == frozenset()
    orch.runtime("stonex").conn.state = ConnState.STREAMING
    assert bridge.streaming_adapters() == frozenset({"stonex"})


async def test_reread_publishes_and_read_does_not(tmp_path: Path) -> None:
    bridge, _, _, bus = make(tmp_path)
    fresh = await bridge.reread_entity("stonex", "ORDER", "B1")
    assert fresh is not None and bus.orders.qsize() == 1
    got = bus.orders.get_nowait()
    assert got.source == "REST" and got.seq_id > 0
    assert await bridge.read_entity("stonex", "ORDER", "B1") is not None
    assert await bridge.read_entity("stonex", "POSITION", "P1") is not None
    assert await bridge.read_entity("stonex", "ACCOUNT", "ACC") is not None
    assert await bridge.read_entity("stonex", "ACCOUNT", "NOPE") is None
    assert bus.orders.qsize() == 0 and bus.positions.qsize() == 0 and bus.account.qsize() == 0


async def test_cash_flow_found_is_resubmitted_without_touching_heartbeat(tmp_path: Path) -> None:
    bridge, orch, _, bus = make(tmp_path)
    rt = orch.runtime("stonex")
    rt.last_inbound_ms = 123
    assert await bridge.reread_cash_flow("stonex", "MISSING", T0 - 10_000) is None
    assert bus.account.qsize() == 0
    cf = await bridge.reread_cash_flow("stonex", "T1", T0 - 10_000)
    assert cf is not None and cf.transaction_id == "T1"
    item = bus.account.get_nowait()
    assert isinstance(item, CashFlowDTO) and item.seq_id > 0
    assert rt.last_inbound_ms == 123


async def test_dlq_worker_drops_a_row_only_on_the_adapters_explicit_not_found(tmp_path: Path) -> None:
    """IMPLEMENTATION_PLAN 8.1, end to end: DlqRecoveryWorker -> OrchestratorDlqBridge -> IngestionOrchestrator ->
    adapter. The adapter's explicit "not found" (note_absent) reaches the worker through the orchestrator's awaits;
    a None the adapter did not mark as "not found" (here: an account missing from the snapshot, and an order read
    that proved nothing) backs off instead of dropping the parked payload."""
    bridge, orch, adapter, _ = make(tmp_path)
    orch.runtime("stonex").conn.state = ConnState.STREAMING
    clock = ManualClock()
    store = FakeDlqStore(clock)
    w = DlqRecoveryWorker(store, bridge, clock_ms=clock, pager=Pages(), owner="w1", jitter=lambda: 0.0)
    gone = store.park("ORDER", "GONE", "QueueFull")
    pos_gone = store.park("POSITION", "PGONE", "QueueFull")
    acct = store.park("ACCOUNT", "NOPE", "QueueFull")
    assert dict(await w.run_once()) == {gone.dlq_id: DlqOutcome.RESOLVED, pos_gone.dlq_id: DlqOutcome.RESOLVED,
                                        acct.dlq_id: DlqOutcome.BACKOFF}

    async def proves_nothing(self: MockBrokerAdapter, broker_order_id: str) -> None:
        return None                                    # a None without note_absent
    adapter.fetch_order_status = proves_nothing.__get__(adapter)  # type: ignore[method-assign]
    unproven = store.park("ORDER", "B1", "QueueFull")
    assert await w.run_once() == [(unproven.dlq_id, DlqOutcome.BACKOFF)]


async def test_reread_maps_the_orchestrator_result_back_to_the_dlq_contract(tmp_path: Path) -> None:
    """Plan S04: the orchestrator answers a RereadResult; the bridge keeps `StatefulDTO | None` (the broker's row
    whenever it had one, also when the row was stale or could not be handed over) and re-reads with origin `dlq`."""
    bridge, orch, adapter, bus = make(tmp_path)
    fresh = await bridge.reread_entity("stonex", "ORDER", "B1")
    assert fresh is not None and fresh.origin == "dlq" and bus.orders.get_nowait().origin == "dlq"
    assert await bridge.reread_entity("stonex", "ORDER", "B1") is not None           # NOOP: still converged
    bad = order("B2")
    object.__setattr__(bad, "checksum", "0" * 64)          # parked again (checksum), never handed over
    adapter.set_order(bad)
    assert (await orch.reread_entity("stonex", "ORDER", "B2")).status.value == "TRANSIENT"
    assert await bridge.reread_entity("stonex", "ORDER", "B2") is bad                 # converged as before
    assert await bridge.reread_entity("stonex", "POSITION", "NOPE") is None
