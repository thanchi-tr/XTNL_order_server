"""SM3 incident hooks over the ingestion orchestrator (tier-persistence.md §3 step 2; IMPLEMENTATION_PLAN 3.6).

* `ConsumerSwitches` + `OrchestratorIngestionControl` (src/integrity/ports.py IngestionControl):
  HALTED stops **committing**: the topic consumers pause, while the adapters' sockets keep draining
  into the bounded topics (overflow parks in the DLQ ring and opens gaps, which SM4 resyncs later).
  `resume_prices()` lets price ticks through first (RECONCILING), `resume_all()` everything (after SM8).
* `OrchestratorReconciler` (BrokerReconciler): for every adapter, the orchestrator's watermark
  reconcile (`run_snapshot`: open orders, positions, account, cash flows since the roll, re-reads by id,
  never closing on absence), plus `fetch_cash_flows(since)` re-submitted when an earlier start is
  given; then it waits until the consumers have committed everything published. The entity consumers
  are resumed for that: the reconcile must commit (the new primary is healthy by then).
  Success is reported only once the gaps are cleared too (IMPLEMENTATION_PLAN 4.9): for every reconciled
  adapter, no reconcile pass still waits on the CommitBarrier, no subscription is still `lost` (a parked
  row keeps its stream's gap open) and the HealthMirror has no `lost` / `reconciled` op left to write,
  so `stream_health.conn_lost_at` is cleared in PostgreSQL. Otherwise the open gaps are named and SM3
  retries (fail closed).

Nothing here places, changes or cancels an order: Phase 1 is read-only.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Collection, Iterable, Sequence

from src.adapters.base import BrokerError
from src.pubsub.connection import ConnState
from src.pubsub.orchestrator import IngestionOrchestrator

log = logging.getLogger(__name__)


class ConsumerSwitches:
    """Two gates the consumer loops wait on: entity topics (orders, positions, account) and prices."""

    def __init__(self) -> None:
        self.entities = asyncio.Event()
        self.prices = asyncio.Event()
        self.entities.set()
        self.prices.set()
        self.halted_reason: str | None = None


class OrchestratorIngestionControl:
    def __init__(self, switches: ConsumerSwitches) -> None:
        self._sw = switches

    async def halt_ingestion(self, reason: str) -> None:
        self._sw.halted_reason = reason
        self._sw.entities.clear()
        self._sw.prices.clear()
        log.critical("ingestion halted (sockets keep draining): %s", reason)

    async def resume_prices(self) -> None:
        self._sw.prices.set()
        log.warning("ingestion: price ticks resumed")

    async def resume_all(self) -> None:
        self._sw.halted_reason = None
        self._sw.entities.set()
        self._sw.prices.set()
        log.warning("ingestion: full ingestion resumed")


_RECONCILABLE = frozenset({ConnState.STREAMING, ConnState.RESYNCING})


class OrchestratorReconciler:
    def __init__(self, orchestrator: IngestionOrchestrator, adapter_ids: Callable[[], Collection[str]],
                 switches: ConsumerSwitches, idle: Callable[[], bool], *, commit_timeout_s: float = 30.0,
                 poll_s: float = 0.05, monotonic: Callable[[], float] = time.monotonic) -> None:
        self._orch = orchestrator
        self._adapter_ids = adapter_ids
        self._sw = switches
        self._idle = idle
        self._timeout = commit_timeout_s
        self._poll = poll_s
        self._mono = monotonic
        self.last_problems: list[str] = []

    async def reconcile_all(self, cash_flows_since_ms: int | None) -> bool:
        problems: list[str] = []
        reconciled: list[str] = []
        self._sw.entities.set()
        for adapter_id in sorted(self._adapter_ids()):
            rt = self._orch.runtime(adapter_id)
            if rt.conn.state not in _RECONCILABLE:
                problems.append(f"{adapter_id} is {rt.conn.state.value}, not STREAMING")
                continue
            try:
                await self._orch.run_snapshot(adapter_id, refresh_instruments=False)
                if cash_flows_since_ms is not None:
                    flows = await rt.adapter.fetch_cash_flows(cash_flows_since_ms)
                    # 7.1: a partial ledger turns pnl_source off (MaxDailyLossRule FLAGs pnl_unverified)
                    self._orch.note_cash_flows(adapter_id, flows, cash_flows_since_ms)
                    for cf in flows:
                        self._orch.resubmit_cash_flow(cf)
            except (BrokerError, OSError, TimeoutError) as exc:
                problems.append(f"{adapter_id}: {type(exc).__name__}: {exc}")
                continue
            reconciled.append(adapter_id)
        deadline = self._mono() + self._timeout
        if not await self._wait_committed(deadline):
            problems.append(f"consumers did not commit the reconcile within {self._timeout:.0f} s")
        else:
            open_gaps = await self._wait_gaps_cleared(reconciled, deadline)
            if open_gaps:
                problems.append(f"gaps still open after the reconcile: {', '.join(open_gaps)}")
        self.last_problems = problems
        for p in problems:
            log.warning("reconcile: %s", p)
        return not problems

    async def _wait_committed(self, deadline: float) -> bool:
        while not self._idle():
            if self._mono() >= deadline:
                return False
            await asyncio.sleep(self._poll)
        return True

    def open_gaps(self, adapter_ids: Iterable[str]) -> list[str]:
        """What still keeps a reconciled adapter fenced: a reconcile pass whose rows have not all settled
        (CommitBarrier), a subscription still `lost`, or a `lost` / `reconciled` stream_health write the
        HealthMirror has not applied yet. Empty = every gap the reconcile had to clear is cleared."""
        out: list[str] = []
        barrier = getattr(self._orch.bus, "commit_barrier", None)
        unwritten = {(a, s) for op, a, s in self._orch.health.pending() if op != "heartbeat"}
        for adapter_id in adapter_ids:
            rt = self._orch.runtime(adapter_id)
            if barrier is not None and barrier.pending(adapter_id):
                out.append(f"{adapter_id}: {barrier.pending(adapter_id)} reconcile rows not settled")
            for stream, sub in sorted(rt.subs.items()):
                if sub.lost:
                    out.append(f"{adapter_id}:{stream} gap open")
                elif (adapter_id, stream) in unwritten:
                    out.append(f"{adapter_id}:{stream} stream_health not written yet")
        return out

    async def _wait_gaps_cleared(self, adapter_ids: Sequence[str], deadline: float) -> list[str]:
        while True:
            gaps = self.open_gaps(adapter_ids)
            if not gaps or self._mono() >= deadline:
                return gaps
            await asyncio.sleep(self._poll)
