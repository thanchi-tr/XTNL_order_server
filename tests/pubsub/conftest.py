"""Fakes for the pubsub tests: a scriptable read-only BrokerAdapter and the src.ports sinks."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal
from typing import Any

import pytest

from src.adapters.base import BrokerAdapter, EntityType, StreamEvent, StreamEventKind, StreamName, VersionDecl
from src.dto import (
    AccountDTO,
    AccountLive,
    CashFlowDTO,
    InstrumentSpec,
    OrderDTO,
    OrderStatus,
    PositionDTO,
    PriceDTO,
)
from src.ports import CommitOutcome, DlqRow, StreamKind

_END = object()


class FakeAdapter(BrokerAdapter):
    """In-memory read-only adapter. Streams are fed through push(); connect() errors are scripted."""

    def __init__(self, adapter_id: str = "mock",
                 versioning: dict[EntityType, VersionDecl] | None = None) -> None:
        self.adapter_id = adapter_id
        self.VERSIONING = versioning or {
            "ORDER": VersionDecl(has_broker_seq=False, has_broker_time=True),
            "POSITION": VersionDecl(has_broker_seq=False, has_broker_time=True),
            "ACCOUNT": VersionDecl(has_broker_seq=False, has_broker_time=False),
        }
        self.instruments: list[InstrumentSpec] = []
        self.open_orders: list[OrderDTO] = []
        self.positions: list[PositionDTO] = []
        self.accounts: list[AccountDTO] = []
        self.lives: list[AccountLive] = []
        self.flows: list[CashFlowDTO] = []
        self.orders_by_id: dict[str, OrderDTO | None] = {}
        self.positions_by_id: dict[str, PositionDTO | None] = {}
        self.connect_errors: list[BaseException] = []
        self.calls: list[tuple[str, Any]] = []
        self.queues: dict[str, asyncio.Queue[Any]] = {}
        self.auto_subscribe = True
        self.on_fetch_open_orders: Any = None

    def _q(self, stream: str) -> asyncio.Queue[Any]:
        return self.queues.setdefault(stream, asyncio.Queue())

    def push(self, stream: str, item: Any) -> None:
        self._q(stream).put_nowait(item)

    def end(self, stream: str) -> None:
        self._q(stream).put_nowait(_END)

    async def _gen(self, stream: StreamName) -> AsyncIterator[Any]:
        self.calls.append(("subscribe", stream))
        q = asyncio.Queue[Any]()
        self.queues[stream] = q
        if self.auto_subscribe:
            yield StreamEvent(StreamEventKind.SUBSCRIBED, stream)
        while True:
            item = await q.get()
            if item is _END:
                return
            if isinstance(item, BaseException):
                raise item
            yield item

    async def connect(self) -> None:
        self.calls.append(("connect", None))
        if self.connect_errors:
            raise self.connect_errors.pop(0)

    async def close(self) -> None:
        self.calls.append(("close", None))

    async def fetch_instruments(self) -> list[InstrumentSpec]:
        self.calls.append(("fetch_instruments", None))
        return list(self.instruments)

    async def fetch_open_orders(self) -> list[OrderDTO]:
        self.calls.append(("fetch_open_orders", None))
        if self.on_fetch_open_orders is not None:
            self.on_fetch_open_orders()
        return list(self.open_orders)

    async def fetch_order_status(self, broker_order_id: str) -> OrderDTO | None:
        self.calls.append(("fetch_order_status", broker_order_id))
        return self.orders_by_id.get(broker_order_id)

    async def fetch_positions(self) -> list[PositionDTO]:
        self.calls.append(("fetch_positions", None))
        return list(self.positions)

    async def fetch_position_status(self, position_id: str) -> PositionDTO | None:
        self.calls.append(("fetch_position_status", position_id))
        return self.positions_by_id.get(position_id)

    async def fetch_account_snapshot(self) -> tuple[list[AccountDTO], list[AccountLive]]:
        self.calls.append(("fetch_account_snapshot", None))
        return list(self.accounts), list(self.lives)

    async def fetch_cash_flows(self, since_ms: int) -> list[CashFlowDTO]:
        self.calls.append(("fetch_cash_flows", since_ms))
        return list(self.flows)

    def stream_ticks(self, symbols: Sequence[str]) -> AsyncIterator[PriceDTO | StreamEvent]:
        self.calls.append(("stream_ticks", tuple(symbols)))
        return self._gen("prices")

    async def ensure_ticks(self, symbols: Sequence[str]) -> None:
        self.calls.append(("ensure_ticks", tuple(symbols)))

    def stream_order_updates(self) -> AsyncIterator[OrderDTO | PositionDTO | StreamEvent]:
        return self._gen("orders")

    def stream_account(self) -> AsyncIterator[AccountDTO | AccountLive | CashFlowDTO | StreamEvent]:
        return self._gen("account")

    def called(self, name: str) -> list[Any]:
        return [arg for n, arg in self.calls if n == name]


class FakeDlqSink:
    def __init__(self) -> None:
        self.rows: dict[str, DlqRow] = {}
        self.batches: list[list[DlqRow]] = []
        self.down = False

    async def insert_batch(self, rows: Sequence[DlqRow]) -> None:
        if self.down:
            raise ConnectionError("postgres down")
        self.batches.append(list(rows))
        for r in rows:
            self.rows.setdefault(r.payload_hash, r)   # ON CONFLICT (payload_hash) DO NOTHING


class FakeHealth:
    def __init__(self) -> None:
        self.ops: list[tuple[str, str, StreamKind]] = []

    async def mark_lost(self, adapter_id: str, stream: StreamKind) -> None:
        self.ops.append(("lost", adapter_id, stream))

    async def mark_reconciled(self, adapter_id: str, stream: StreamKind) -> None:
        self.ops.append(("reconciled", adapter_id, stream))

    async def heartbeat(self, adapter_id: str, stream: StreamKind) -> None:
        self.ops.append(("heartbeat", adapter_id, stream))


class FakeCommitter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.fail: BaseException | None = None
        self.delay_s = 0.0

    async def _maybe_fail(self) -> None:
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        if self.fail is not None:
            raise self.fail

    async def commit_entity(self, dto: OrderDTO | PositionDTO | AccountDTO) -> CommitOutcome:
        await self._maybe_fail()
        self.calls.append(("commit_entity", dto))
        return CommitOutcome.APPLIED

    async def commit_cash_flow(self, cf: CashFlowDTO) -> bool:
        await self._maybe_fail()
        self.calls.append(("commit_cash_flow", cf))
        return True

    async def write_live(self, item: AccountLive | PriceDTO) -> None:
        await self._maybe_fail()
        self.calls.append(("write_live", item))


class ManualClock:
    def __init__(self, ms: int = 1_700_000_000_000) -> None:
        self.ms = ms

    def __call__(self) -> int:
        return self.ms


# ---------------------------------------------------------------- DTO builders
def order(oid: str = "O1", status: OrderStatus = OrderStatus.WORKING, t: int = 0, adapter_id: str = "mock",
          bseq: int | None = None, stop: str | None = None, symbol: str = "EURUSD",
          remaining: str = "1000") -> OrderDTO:
    return OrderDTO(client_order_id=f"c-{oid}", broker_order_id=oid, account_id="A1", adapter_id=adapter_id,
                    symbol=symbol, side="BUY", status=status, entry_price=Decimal("1.1"),
                    stop_loss=None if stop is None else Decimal(stop), ordered_units=Decimal(1000),
                    remaining_units=Decimal(remaining), broker_updated_at_ms=t, broker_seq=bseq)


def position(pid: str = "P1", status: str = "OPEN", t: int = 0, units: str = "1000",
             adapter_id: str = "mock") -> PositionDTO:
    return PositionDTO(position_id=pid, account_id="A1", adapter_id=adapter_id, symbol="EURUSD", side="BUY",
                       units=Decimal(units), entry_price=Decimal("1.1"), status=status,  # type: ignore[arg-type]
                       broker_updated_at_ms=t)


def account(balance: str = "10000", t: int = 0, bseq: int | None = None) -> AccountDTO:
    return AccountDTO(account_id="A1", adapter_id="mock", currency="USD", balance=Decimal(balance),
                      broker_updated_at_ms=t, broker_seq=bseq)


def live(equity: str = "10000") -> AccountLive:
    return AccountLive(account_id="A1", adapter_id="mock", currency="USD", equity=Decimal(equity),
                       margin_used=Decimal(0), free_margin=Decimal(equity), unrealised_pnl=Decimal(0),
                       timestamp_ms=1)


def cash_flow(tx: str = "T1", amount: str = "100") -> CashFlowDTO:
    return CashFlowDTO(transaction_id=tx, account_id="A1", adapter_id="mock", kind="DEPOSIT",
                       amount=Decimal(amount), currency="USD", broker_ts=1)


def price(symbol: str = "EURUSD", bid: str = "1.1000", ts: int = 1, adapter_id: str = "mock") -> PriceDTO:
    return PriceDTO(adapter_id=adapter_id, symbol=symbol, bid=Decimal(bid), ask=Decimal(bid) + Decimal("0.0001"),
                    spread_pips=Decimal(1), timestamp_ms=ts)


def ev(kind: StreamEventKind, stream: StreamName) -> StreamEvent:
    return StreamEvent(kind, stream)


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()
