"""Non-blocking event bus: four isolated bounded topics (dto-contracts.md §1, §4).

publish_*_nowait never awaits. The entity topics raise asyncio.QueueFull to the caller
(the orchestrator routes it to the DlqSpooler); the price topic keeps the latest tick per
(adapter_id, symbol) and never blocks, never parks.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Final, Generic, Protocol, TypeVar

from src.dto import AccountDTO, AccountLive, CashFlowDTO, OrderDTO, PositionDTO, PriceDTO
from src.pubsub.barrier import CommitBarrier

ORDERS_TOPIC: Final = "orders.internal.dto"
POSITIONS_TOPIC: Final = "position.state.dto"
PRICES_TOPIC: Final = "market.price.dto"
ACCOUNT_TOPIC: Final = "account.state.dto"
TOPICS: Final = (ORDERS_TOPIC, POSITIONS_TOPIC, PRICES_TOPIC, ACCOUNT_TOPIC)

AccountItem = AccountDTO | CashFlowDTO | AccountLive

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class Topic(Protocol[T_co]):
    """Consumer view of one topic."""

    name: str

    async def get(self) -> T_co: ...

    def get_nowait(self) -> T_co: ...

    def qsize(self) -> int: ...

    def empty(self) -> bool: ...

    @property
    def maxsize(self) -> int: ...


@dataclass
class TopicStats:
    published: int = 0
    queue_full: int = 0
    coalesced: int = 0
    dropped: int = 0


class EntityTopic(Generic[T]):
    """A bounded asyncio.Queue for one entity topic; put_nowait raises asyncio.QueueFull."""

    def __init__(self, name: str, maxsize: int, commit_barrier: CommitBarrier | None = None) -> None:
        if maxsize <= 0:
            raise ValueError("topics must be bounded")
        self.name = name
        self._q: asyncio.Queue[T] = asyncio.Queue(maxsize)
        self.stats = TopicStats()
        #: Shared with the orchestrator: the topic's consumer settles each row here after its commit
        #: (src/pubsub/barrier.py), so a reconcile pass clears its gap only once its rows committed.
        self.commit_barrier = commit_barrier

    def put_nowait(self, item: T) -> None:
        try:
            self._q.put_nowait(item)
        except asyncio.QueueFull:
            self.stats.queue_full += 1
            raise
        self.stats.published += 1

    async def get(self) -> T:
        return await self._q.get()

    def get_nowait(self) -> T:
        return self._q.get_nowait()

    def qsize(self) -> int:
        return self._q.qsize()

    def empty(self) -> bool:
        return self._q.empty()

    @property
    def maxsize(self) -> int:
        return self._q.maxsize


class PricePut(str, Enum):
    QUEUED = "QUEUED"        # new (adapter, symbol) key queued
    COALESCED = "COALESCED"  # replaced the pending tick for the same key (latest wins)
    DROPPED = "DROPPED"      # distinct-key capacity exhausted: dropped at the parser


class PriceTopic:
    """market.price.dto: at most one pending tick per (adapter_id, symbol), FIFO across keys.

    A newer tick for a key that is already pending replaces it in place (dto-contracts.md §4:
    the topic keeps the latest tick per key). maxsize bounds the number of distinct pending keys.
    """

    def __init__(self, maxsize: int, name: str = PRICES_TOPIC) -> None:
        if maxsize <= 0:
            raise ValueError("topics must be bounded")
        self.name = name
        self._maxsize = maxsize
        self._pending: OrderedDict[tuple[str, str], PriceDTO] = OrderedDict()
        self._ready = asyncio.Event()
        self.stats = TopicStats()

    def put_nowait(self, tick: PriceDTO) -> PricePut:
        key = (tick.adapter_id, tick.symbol)
        if key in self._pending:
            self._pending[key] = tick  # keeps its FIFO position
            self.stats.coalesced += 1
            return PricePut.COALESCED
        if len(self._pending) >= self._maxsize:
            self.stats.dropped += 1
            return PricePut.DROPPED
        self._pending[key] = tick
        self.stats.published += 1
        self._ready.set()
        return PricePut.QUEUED

    async def get(self) -> PriceDTO:
        while not self._pending:
            self._ready.clear()
            await self._ready.wait()
        return self.get_nowait()

    def get_nowait(self) -> PriceDTO:
        if not self._pending:
            raise asyncio.QueueEmpty
        _, tick = self._pending.popitem(last=False)
        return tick

    def qsize(self) -> int:
        return len(self._pending)

    def empty(self) -> bool:
        return not self._pending

    @property
    def maxsize(self) -> int:
        return self._maxsize


AnyTopic = EntityTopic[OrderDTO] | EntityTopic[PositionDTO] | PriceTopic | EntityTopic[AccountItem]


class EventBus:
    """Exactly four isolated bounded topics; nothing is multiplexed (dto-contracts.md §1)."""

    def __init__(self, orders_maxsize: int = 10_000, positions_maxsize: int = 10_000,
                 prices_maxsize: int = 5_000, account_maxsize: int = 10_000) -> None:
        self.commit_barrier = CommitBarrier()
        self.orders: EntityTopic[OrderDTO] = EntityTopic(ORDERS_TOPIC, orders_maxsize, self.commit_barrier)
        self.positions: EntityTopic[PositionDTO] = EntityTopic(POSITIONS_TOPIC, positions_maxsize,
                                                               self.commit_barrier)
        self.prices = PriceTopic(prices_maxsize)
        self.account: EntityTopic[AccountItem] = EntityTopic(ACCOUNT_TOPIC, account_maxsize, self.commit_barrier)

    # --- producer side (the ingestion orchestrator only, dto-contracts.md §6) -------------
    def publish_order_nowait(self, dto: OrderDTO) -> None:
        """Raises asyncio.QueueFull."""
        self.orders.put_nowait(dto)

    def publish_position_nowait(self, dto: PositionDTO) -> None:
        """Raises asyncio.QueueFull."""
        self.positions.put_nowait(dto)

    def publish_account_nowait(self, item: AccountItem) -> None:
        """Raises asyncio.QueueFull."""
        self.account.put_nowait(item)

    def publish_price_nowait(self, tick: PriceDTO) -> PricePut:
        """Never raises for capacity: coalesces or reports DROPPED."""
        return self.prices.put_nowait(tick)

    # --- introspection ---------------------------------------------------------------------
    def queue(self, name: str) -> AnyTopic:
        topics: dict[str, AnyTopic] = {ORDERS_TOPIC: self.orders, POSITIONS_TOPIC: self.positions, PRICES_TOPIC: self.prices,
                  ACCOUNT_TOPIC: self.account}
        return topics[name]

    def depths(self) -> dict[str, int]:
        """Current depth per topic (metrics / health record)."""
        return {t.name: t.qsize() for t in (self.orders, self.positions, self.prices, self.account)}

    def fill_ratios(self) -> dict[str, float]:
        return {t.name: t.qsize() / t.maxsize for t in (self.orders, self.positions, self.prices, self.account)}

    def stats(self) -> dict[str, TopicStats]:
        return {t.name: t.stats for t in (self.orders, self.positions, self.prices, self.account)}
