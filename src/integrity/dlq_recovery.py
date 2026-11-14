"""SM2 DlqRecoveryWorker: check-before-retry for events_dlq rows (tier-persistence.md §5; diagram page 3).

QUEUED --[due and adapter STREAMING] / lease--> CHECKING_BROKER (status LEASED)
  [converged, not illegal]                   -> RESOLVED (fresh broker row re-submitted via the orchestrator)
  [converged and illegal transition]         -> QUARANTINED (events_quarantine)
  [not converged or error, still STREAMING]  -> BACKOFF (retry_count += 1, next = now + min(60, 2**n + jitter))
                                                or QUARANTINED once retry_count reaches MAX_DLQ_RETRIES (5)
  [lease expired or adapter left STREAMING]  -> QUEUED, retry_count unchanged
Lineage: the orchestrator re-parks a re-submitted event as a new row. When a new row matches the row this
worker last re-submitted for the same (event_type, entity_id, error_reason) and the reason is deterministic,
the new row is merged into that origin (origin.retry_count += 1, so an event invalid every time is quarantined
after 5). Transient reasons start a fresh count and page once the entity has been parked for 15 min.
CASH_FLOW converges only when fetch_cash_flows(since) returns the transaction_id; at MAX_DLQ_RETRIES it is
quarantined, paged, and the account's day is marked transfers_unverified.
An ORDER / POSITION / ACCOUNT read that returns no row converges (payload dropped) only on an explicit broker
"not found" recorded by the adapter (src.adapters.base.note_absent); every other None is "not converged".
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import uuid
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Final, Literal

from src.adapters.base import absence_probe
from src.integrity.ports import (
    MAX_DLQ_RETRIES,
    DlqBroker,
    DlqRecord,
    DlqStore,
    IntegrityAlert,
    Pager,
    null_pager,
)
from src.pubsub.connection import backoff_delay
from src.pubsub.dlq_rows import DETERMINISTIC_REASONS, ILLEGAL_TRANSITION, is_deterministic

__all__ = ["DETERMINISTIC_REASONS", "ILLEGAL_TRANSITION", "DlqConfig", "DlqOutcome", "DlqRecoveryWorker",
           "is_deterministic"]

log = logging.getLogger(__name__)

TRANSIENT_PAGE_AFTER_MS: Final = 15 * 60_000
LINEAGE_WINDOW_MS: Final = 10 * 60_000
CASH_FLOW_LOOKBACK_MS: Final = 7 * 24 * 3_600_000


class DlqOutcome(str, Enum):
    RESOLVED = "RESOLVED"
    QUARANTINED = "QUARANTINED"
    BACKOFF = "BACKOFF"
    REQUEUED = "REQUEUED"
    MERGED = "MERGED"
    LOST_LEASE = "LOST_LEASE"


@dataclass(frozen=True)
class DlqConfig:
    poll_interval_s: float = 1.0
    lease_ms: int = 30_000
    batch: int = 50
    max_retries: int = MAX_DLQ_RETRIES
    lineage_window_ms: int = LINEAGE_WINDOW_MS
    transient_page_after_ms: int = TRANSIENT_PAGE_AFTER_MS


def _payload(record: DlqRecord) -> dict[str, object]:
    try:
        obj = json.loads(record.raw_payload)
    except ValueError:
        return {}
    return obj if isinstance(obj, dict) else {}


class DlqRecoveryWorker:
    def __init__(
        self,
        store: DlqStore,
        broker: DlqBroker,
        *,
        clock_ms: Callable[[], int],
        pager: Pager = null_pager,
        config: DlqConfig = DlqConfig(),
        owner: str | None = None,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        self._store = store
        self._broker = broker
        self._clock_ms = clock_ms
        self._pager = pager
        self.cfg = config
        self.owner = owner or f"dlq:{uuid.uuid4().hex}"
        self._jitter = jitter
        self._chain_start: OrderedDict[tuple[str, str, str], int] = OrderedDict()   # (adapter, type, id)
        self._paged_transient: set[tuple[str, str, str]] = set()
        self.outcomes: dict[DlqOutcome, int] = {o: 0 for o in DlqOutcome}
        self.liveness_ms: int | None = None

    # ---- loop -------------------------------------------------------------------------------------
    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.run_once()
            except Exception:
                log.exception("dlq recovery pass failed")
            self.liveness_ms = self._clock_ms()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), self.cfg.poll_interval_s)

    async def run_once(self) -> list[tuple[int, DlqOutcome]]:
        """Release expired leases, lease due rows of STREAMING adapters, process them in order."""
        now = self._clock_ms()
        released = await self._store.release_expired(now)
        if released:
            log.info("dlq: %d expired leases returned to QUEUED", released)
        streaming = frozenset(self._broker.streaming_adapters())
        if not streaming:
            return []
        rows = await self._store.lease(self.owner, now, self.cfg.lease_ms, streaming, self.cfg.batch)
        done: list[tuple[int, DlqOutcome]] = []
        for record in rows:
            outcome = await self.process(record)
            self.outcomes[outcome] += 1
            done.append((record.dlq_id, outcome))
        return done

    # ---- one row ------------------------------------------------------------------------------------
    async def process(self, record: DlqRecord) -> DlqOutcome:
        if not self._streaming(record.adapter_id):
            return await self._requeue(record)
        if record.origin_dlq_id is None and record.retry_count == 0:
            merged = await self._lineage(record)
            if merged is not None:
                return merged
        self._maybe_page_transient(record)
        try:
            not_converged = await self._check(record)
        except Exception as exc:
            log.warning("dlq %d %s %s: broker read failed: %s", record.dlq_id, record.event_type,
                        record.entity_id, exc)
            return await self._failed(record, f"read failed: {exc}")
        if not_converged is not None:
            return await self._failed(record, not_converged)
        if record.error_reason == ILLEGAL_TRANSITION:
            return await self._quarantine(record, "converged after illegal_transition")
        return await self._finish(await self._store.resolve(record.dlq_id, self.owner), DlqOutcome.RESOLVED,
                                  record)

    async def _check(self, record: DlqRecord) -> str | None:
        """Read the broker's row for that id; re-submit it unless the row was an illegal transition.

        Returns None when converged, else why not. A None row converges only when the adapter recorded an
        explicit broker "not found" for this id during the read (``absence_probe``, IMPLEMENTATION_PLAN 8.1):
        then the parked payload is dropped. Any other None (a read that proved nothing) fails closed, so the
        row backs off and is quarantined and paged after MAX_DLQ_RETRIES, never silently discarded."""
        if record.event_type == "CASH_FLOW":
            since = await self._cash_flow_since(record)
            found = await self._broker.reread_cash_flow(record.adapter_id, record.entity_id, since)
            return None if found is not None else "cash flow not found by transaction_id"
        with absence_probe() as probe:
            if record.error_reason == ILLEGAL_TRANSITION:
                fresh = await self._broker.read_entity(record.adapter_id, record.event_type, record.entity_id)
            else:
                fresh = await self._broker.reread_entity(record.adapter_id, record.event_type, record.entity_id)
        if fresh is not None:
            return None
        if probe.absent(record.event_type, record.entity_id):
            log.info("dlq %d: broker reports no %s %s (explicit not found); parked payload dropped",
                     record.dlq_id, record.event_type, record.entity_id)
            return None
        log.warning("dlq %d: broker read of %s %s returned no row without an explicit not found; not converged",
                    record.dlq_id, record.event_type, record.entity_id)
        return "broker returned no row without an explicit not found"

    async def _cash_flow_since(self, record: DlqRecord) -> int:
        candidates: list[int] = []
        stored = await self._store.cash_flow_since_ms(record.adapter_id)
        if stored is not None:
            candidates.append(stored)
        ts = _payload(record).get("broker_ts")
        if isinstance(ts, int):
            candidates.append(ts)
        if not candidates:
            return self._clock_ms() - CASH_FLOW_LOOKBACK_MS
        return min(candidates)

    async def _lineage(self, record: DlqRecord) -> DlqOutcome | None:
        now = self._clock_ms()
        origin = await self._store.find_lineage_origin(record.event_type, record.entity_id, record.error_reason,
                                                       record.dlq_id, now - self.cfg.lineage_window_ms,
                                                       adapter_id=record.adapter_id)
        if origin is not None and origin.adapter_id != record.adapter_id:
            origin = None   # entity ids are per adapter: another broker's row is never this row's origin
        key = (record.adapter_id, record.event_type, record.entity_id)   # entity ids are per adapter
        if origin is None:
            # A new episode for this entity: its 15-min transient clock and page start afresh.
            self._chain_start.pop(key, None)
            self._paged_transient.discard(key)
            return None
        if not is_deterministic(record.error_reason):
            start = min(origin.parked_at_ms, self._chain_start.get(key, origin.parked_at_ms))
            self._chain_start[key] = start
            self._chain_start.move_to_end(key)
            while len(self._chain_start) > 10_000:
                self._chain_start.popitem(last=False)
            return None
        n = origin.retry_count + 1
        ok = await self._store.merge_into_origin(record.dlq_id, self.owner, origin.dlq_id, n,
                                                 now + self._delay_ms(n))
        if not ok:
            return DlqOutcome.LOST_LEASE
        log.info("dlq %d merged into origin %d (%s, retry_count %d)", record.dlq_id, origin.dlq_id,
                 record.error_reason, n)
        if n >= self.cfg.max_retries:
            await self._store.quarantine(origin.dlq_id, None, f"{record.error_reason} x{n} (lineage)")
            self._page("high", "dlq_quarantined",
                       f"{record.event_type} {record.entity_id} invalid {n} times ({record.error_reason})", record)
        return DlqOutcome.MERGED

    async def _failed(self, record: DlqRecord, why: str) -> DlqOutcome:
        if not self._streaming(record.adapter_id):
            return await self._requeue(record)
        n = record.retry_count + 1
        if n >= self.cfg.max_retries:
            return await self._quarantine(record, f"{why} after {n} attempts")
        ok = await self._store.backoff(record.dlq_id, self.owner, n, self._clock_ms() + self._delay_ms(n), why)
        return await self._finish(ok, DlqOutcome.BACKOFF, record)

    async def _quarantine(self, record: DlqRecord, why: str) -> DlqOutcome:
        ok = await self._store.quarantine(record.dlq_id, self.owner, why)
        if not ok:
            return DlqOutcome.LOST_LEASE
        if record.event_type == "CASH_FLOW":
            account = _payload(record).get("account_id")
            if isinstance(account, str) and account:
                await self._store.mark_transfers_unverified(record.adapter_id, account, self._clock_ms())
            self._page("critical", "dlq_cash_flow_unverified",
                       f"cash flow {record.entity_id} not confirmed by the broker ({why}); day marked "
                       f"transfers_unverified", record)
        else:
            self._page("high", "dlq_quarantined", f"{record.event_type} {record.entity_id}: {why}", record)
        return DlqOutcome.QUARANTINED

    async def _requeue(self, record: DlqRecord) -> DlqOutcome:
        ok = await self._store.requeue(record.dlq_id, self.owner)
        return await self._finish(ok, DlqOutcome.REQUEUED, record)

    async def _finish(self, ok: bool, outcome: DlqOutcome, record: DlqRecord) -> DlqOutcome:
        if ok:
            return outcome
        log.warning("dlq %d: lease lost before %s", record.dlq_id, outcome.value)
        return DlqOutcome.LOST_LEASE

    # ---- helpers ------------------------------------------------------------------------------------
    def _streaming(self, adapter_id: str) -> bool:
        return adapter_id in self._broker.streaming_adapters()

    def _delay_ms(self, attempt: int) -> int:
        """min(60, 2**n + U(0, 0.5)) s, as SM4's backoff."""
        return int(backoff_delay(attempt, self._jitter) * 1000)

    def _maybe_page_transient(self, record: DlqRecord) -> None:
        if is_deterministic(record.error_reason):
            return
        key = (record.adapter_id, record.event_type, record.entity_id)   # entity ids are per adapter
        start = min(record.parked_at_ms, self._chain_start.get(key, record.parked_at_ms))
        if key not in self._paged_transient and self._clock_ms() - start > self.cfg.transient_page_after_ms:
            if len(self._paged_transient) >= 10_000:
                self._paged_transient.clear()          # bounded memory; worst case one repeated page
            self._paged_transient.add(key)
            self._page("high", "dlq_parked_15min",
                       f"{record.event_type} {record.entity_id} parked for more than 15 min ({record.error_reason})",
                       record)

    def _page(self, severity: Literal["info", "high", "critical"], code: str, detail: str,
              record: DlqRecord) -> None:
        log.warning("dlq %s: %s", code, detail)
        self._pager(IntegrityAlert(severity, code, detail, self._clock_ms(),
                                   {"dlq_id": str(record.dlq_id), "adapter_id": record.adapter_id}))
