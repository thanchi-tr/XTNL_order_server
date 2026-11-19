"""DlqBroker over the IngestionOrchestrator (tier-persistence.md §5; dto-contracts.md §6).

The orchestrator stays the sole publisher: entity re-submissions go through its reread_entity(), and a
found cash flow through its resubmit_cash_flow() (CashFlowDTO is unversioned; the orchestrator stamps
seq_id and leaves its heartbeat-timeout clock alone, since a REST read is not stream traffic).

The orchestrator's `reread_entity` answers a `RereadResult` (design PR8, IN-4); this bridge keeps the DLQ worker's
`StatefulDTO | None` contract (`DlqBroker.reread_entity`): the broker's row whenever the broker had one (published,
stale or not handed over, exactly as before), None only when it had none. Re-reads here carry origin `dlq`.
"""
from __future__ import annotations

from collections.abc import Callable, Collection
from typing import Literal

from src.dto import CashFlowDTO
from src.integrity.ports import StatefulDTO
from src.pubsub.connection import ConnState
from src.pubsub.orchestrator import IngestionOrchestrator


class OrchestratorDlqBridge:
    def __init__(self, orchestrator: IngestionOrchestrator, adapter_ids: Callable[[], Collection[str]]) -> None:
        self._orch = orchestrator
        self._adapter_ids = adapter_ids

    def streaming_adapters(self) -> Collection[str]:
        return frozenset(a for a in self._adapter_ids()
                         if self._orch.runtime(a).conn.state is ConnState.STREAMING)

    async def reread_entity(self, adapter_id: str, event_type: Literal["ORDER", "POSITION", "ACCOUNT"],
                            entity_id: str) -> StatefulDTO | None:
        res = await self._orch.reread_entity(adapter_id, event_type, entity_id, origin="dlq")
        return res.dto if res.found else None

    async def read_entity(self, adapter_id: str, event_type: Literal["ORDER", "POSITION", "ACCOUNT"],
                          entity_id: str) -> StatefulDTO | None:
        adapter = self._orch.runtime(adapter_id).adapter
        if event_type == "ORDER":
            return await adapter.fetch_order_status(entity_id)
        if event_type == "POSITION":
            return await adapter.fetch_position_status(entity_id)
        accounts, _ = await adapter.fetch_account_snapshot()
        return next((a for a in accounts if a.account_id == entity_id), None)

    async def reread_cash_flow(self, adapter_id: str, transaction_id: str, since_ms: int) -> CashFlowDTO | None:
        rt = self._orch.runtime(adapter_id)
        flows = await rt.adapter.fetch_cash_flows(since_ms)
        found = next((cf for cf in flows if cf.transaction_id == transaction_id), None)
        if found is not None:
            self._orch.resubmit_cash_flow(found)
        return found
