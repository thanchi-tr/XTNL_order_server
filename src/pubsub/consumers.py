"""Topic consumers that persist through the StateCommitter port (dto-contracts.md §4, §7).

Each consumer owns one topic and runs as its own task, so it never blocks the producers.
A failure (timeout / exception) of a parkable item goes to the DlqSpooler with reason
"timeout" / "consumer_error"; prices and AccountLive are never parked (latest value wins).
A failed commit is **always** parked (dto-contracts.md §4, tier-persistence.md §5): when the DLQ ring is full
the consumer waits (re-offering every `park_retry_s`) until the spooler's flush makes room, instead of
dropping the row. Waiting stops this consumer, so its topic fills and the orchestrator's own QueueFull /
backpressure_overflow path opens the gap (fail closed). Every failure is also paged through the optional
`pager`, throttled per (consumer, reason).
A commit the store **refused** as an illegal transition (`src.db.repository.IllegalTransitionParked`, recognised by
its `reason == "illegal_transition"`; src.db is not imported here) was already parked by the store in the same
transaction: it is counted (`illegal_transitions`), settled as failed (a reconcile pass that carried it keeps its
gap open), reported to the topic's CommitBarrier listeners so the orchestrator does not keep the refused status in
memory, and parked / paged as `illegal_transition` (not `consumer_error`); that park is the store's own row again,
so the payload_hash-idempotent events_dlq insert adds nothing.
Oversee is not a bus subscriber: it reads the post-commit Redis feed instead.
"""
from __future__ import annotations

import abc
import asyncio
import logging
from collections.abc import Callable
from typing import Generic, TypeVar

from src.dto import AccountDTO, CashFlowDTO, OrderDTO, PositionDTO, PriceDTO
from src.ports import CommitOutcome, StateCommitter
from src.pubsub.alerts import HIGH, PageThrottle, Pager
from src.pubsub.bus import AccountItem, Topic
from src.pubsub.dlq_rows import ILLEGAL_TRANSITION, dlq_row
from src.pubsub.hlc import wall_clock_ms
from src.pubsub.spooler import DlqSpooler

log = logging.getLogger(__name__)

T = TypeVar("T")
DEFAULT_TIMEOUT_S = 5.0
PARK_RETRY_S = 0.05
FAILURE_PAGE_INTERVAL_MS = 60_000


def store_refusal(exc: BaseException) -> str | None:
    """The stored status when `exc` is the store's illegal-transition refusal (IllegalTransitionParked: the row
    was parked by the store, the current row untouched), else None. Duck-typed (no src.db import)."""
    if getattr(exc, "reason", None) != ILLEGAL_TRANSITION:
        return None
    if not any(cls.__name__ == "IllegalTransitionParked" for cls in type(exc).__mro__):
        return None
    stored = getattr(exc, "stored_status", None)
    return "" if stored is None else str(stored)


class TopicConsumer(abc.ABC, Generic[T]):
    """Drains one topic and hands each item to handle() under a timeout."""

    name = "consumer"

    def __init__(self, topic: Topic[T], committer: StateCommitter, spooler: DlqSpooler,
                 timeout_s: float = DEFAULT_TIMEOUT_S, *, pager: Pager | None = None,
                 clock_ms: Callable[[], int] = wall_clock_ms, park_retry_s: float = PARK_RETRY_S,
                 page_interval_ms: int = FAILURE_PAGE_INTERVAL_MS) -> None:
        self.topic = topic
        self.committer = committer
        self.spooler = spooler
        self.timeout_s = timeout_s
        self.park_retry_s = park_retry_s
        self.processed = 0
        self.failed = 0
        self.park_waits = 0          # failed rows that had to wait for room in the DLQ ring
        self.illegal_transitions = 0  # commits the store refused (parked by the store itself)
        self.outcomes: dict[CommitOutcome, int] = {}
        self._pages = PageThrottle(pager, clock_ms, page_interval_ms)

    @abc.abstractmethod
    async def handle(self, item: T) -> None: ...

    def parkable(self, item: T) -> OrderDTO | PositionDTO | AccountDTO | CashFlowDTO | None:
        if isinstance(item, (OrderDTO, PositionDTO, AccountDTO, CashFlowDTO)):
            return item
        return None

    async def process(self, item: T) -> bool:
        """Handle one item; on failure park it. True on success."""
        try:
            await asyncio.wait_for(self.handle(item), self.timeout_s)
        except TimeoutError:
            self._settle(item, False)
            await self._failed(item, "timeout")
            return False
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stored = store_refusal(exc)
            if stored is not None:
                await self._refused(item, exc, stored)
                return False
            log.exception("%s failed on %s", self.name, type(item).__name__)
            self._settle(item, False)
            await self._failed(item, "consumer_error")
            return False
        self.processed += 1
        self._settle(item, True)
        return True

    async def _refused(self, item: T, exc: BaseException, stored_status: str) -> None:
        """The store refused the row as an illegal transition and parked it itself (same transaction). Settle it as
        failed, tell the barrier's listeners (the orchestrator restores the stored status in its memory), then park
        and page it under its real reason, `illegal_transition`. The consumer's park is the same `dlq_row()` the
        store wrote, so its payload_hash makes the idempotent events_dlq insert a no-op there (no second row, no
        lineage: it has no origin_dlq_id); it only matters if the store's row were ever missing (fail closed)."""
        self.illegal_transitions += 1
        log.error("%s: commit refused: %s", self.name, exc)     # message: entity key and the two statuses only
        self._settle(item, False)
        barrier = getattr(self.topic, "commit_barrier", None)
        refused = getattr(barrier, "refused", None)
        if callable(refused):
            refused(item, stored_status or None)
        await self._failed(item, ILLEGAL_TRANSITION, f" (refused by the store; stored {stored_status or '?'})")

    def _settle(self, item: T, ok: bool) -> None:
        """Tell the topic's CommitBarrier (if any) this row is committed / failed, so a reconcile pass
        waiting on it can clear (or keep) its stream gap (src/pubsub/barrier.py)."""
        barrier = getattr(self.topic, "commit_barrier", None)
        seq = getattr(item, "seq_id", None)
        if barrier is not None and isinstance(seq, int):
            barrier.settle(seq, ok)

    async def _failed(self, item: T, reason: str, detail: str = "") -> None:
        """Park a failed parkable row, waiting for ring room if needed, and page (throttled)."""
        self.failed += 1
        dto = self.parkable(item)
        if dto is None:
            return   # prices / live marks: the next value supersedes it
        row = dlq_row(dto, reason)
        self._pages.page(f"{self.name}:{reason}", HIGH, f"consumer commit failed: {self.name}",
                         f"{type(dto).__name__} {row.entity_id} ({row.adapter_id}) parked in events_dlq: {reason}"
                         + detail)
        if self.spooler.offer(row):
            return
        self.park_waits += 1
        log.error("%s: DLQ ring full; waiting to park %s %s (%s)", self.name, type(dto).__name__,
                  row.entity_id, reason)
        self._pages.page(f"{self.name}:ring_full", HIGH, f"DLQ ring full: {self.name}",
                         "a failed commit waits for room in the DLQ ring; this consumer is paused")
        while not self.spooler.offer(row):
            await asyncio.sleep(self.park_retry_s)

    def _count(self, outcome: CommitOutcome) -> None:
        self.outcomes[outcome] = self.outcomes.get(outcome, 0) + 1

    async def process_available(self) -> int:
        """Process everything currently queued (tests / shutdown)."""
        n = 0
        while not self.topic.empty():
            await self.process(self.topic.get_nowait())
            n += 1
        return n

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        while not stop.is_set():
            item = await self.topic.get()
            await self.process(item)


class OrderStateConsumer(TopicConsumer[OrderDTO]):
    name = "orders"

    async def handle(self, item: OrderDTO) -> None:
        self._count(await self.committer.commit_entity(item))


class PositionStateConsumer(TopicConsumer[PositionDTO]):
    name = "positions"

    async def handle(self, item: PositionDTO) -> None:
        self._count(await self.committer.commit_entity(item))


class AccountImageProjector(TopicConsumer[AccountItem]):
    """AccountDTO -> commit_entity, CashFlowDTO -> commit_cash_flow, AccountLive -> write_live."""

    name = "account"

    async def handle(self, item: AccountItem) -> None:
        if isinstance(item, AccountDTO):
            self._count(await self.committer.commit_entity(item))
        elif isinstance(item, CashFlowDTO):
            await self.committer.commit_cash_flow(item)
        else:
            await self.committer.write_live(item)


class PriceSubscriber(TopicConsumer[PriceDTO]):
    """PriceDTO -> write_live (price:/clean_mid: keys). Never parked."""

    name = "prices"

    async def handle(self, item: PriceDTO) -> None:
        await self.committer.write_live(item)


__all__ = ["AccountImageProjector", "OrderStateConsumer", "PositionStateConsumer",
           "PriceSubscriber", "TopicConsumer"]
