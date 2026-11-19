"""MockBrokerAdapter: deterministic, in-memory BrokerAdapter scripted from Python lists of DTOs and
StreamEvents (adapter-isolation.md §1, §4). No network I/O. Used by the other modules' tests.

REST state (orders, positions, accounts, cash flows, instruments) can be changed between calls;
streams replay a script and then either end (finite, the default) or wait for pushed items.
``build_fx_fixture()`` gives EURUSD / AUDUSD fixtures with an AUD-denominated account and a
StoneX-style partially filled parent with an if-done stop child.
``fetch_price_bars`` (design PR3) serves bars scripted per (symbol, side, timeframe) with ``set_price_bars``,
including partial (forming) bars, truncated answers, failing reads and undecodable answers.
"""
from __future__ import annotations

import asyncio
import collections
import dataclasses
import functools
import time
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Generic, TypeVar

from src.adapters.base import (
    AccountStreamItem,
    ActiveStopOrders,
    BrokerAdapter,
    BrokerError,
    BrokerHistory,
    CashFlowBatch,
    HistoryNotes,
    EntityType,
    MarketSessions,
    OrderStreamItem,
    PriceBarsDecodeError,
    PriceStreamItem,
    StreamEvent,
    StreamEventKind,
    StreamName,
    VersionDecl,
    covered_from,
    history_read,
    new_unmapped_rows,
    note_absent,
    note_unmapped,
)
from src.adapters.ratelimit import RestBudget, RestPriority, current_rest_priority, mark_rest_sent
from src.adapters.sessions import fx_default_sessions, looks_like_fx_pair
from src.dto import (
    AccountDTO,
    AccountLive,
    BarSide,
    BrokerBar,
    CashFlowDTO,
    InstrumentSpec,
    OrderDTO,
    OrderKind,
    OrderStatus,
    PositionDTO,
    PriceBarsResponse,
    PriceDTO,
    bar_request_problem,
)

T = TypeVar("T")
BarKey = tuple[str, str, int]   # (symbol, side, timeframe_s)

DEFAULT_VERSIONING: dict[EntityType, VersionDecl] = {
    "ORDER": VersionDecl(has_broker_seq=False, has_broker_time=True),
    "POSITION": VersionDecl(has_broker_seq=False, has_broker_time=True),
    "ACCOUNT": VersionDecl(has_broker_seq=False, has_broker_time=False),
}


class _Script(Generic[T]):
    """A replayable stream script plus items pushed while it runs."""

    def __init__(self, items: Iterable[T], *, finite: bool) -> None:
        self.items: collections.deque[T] = collections.deque(items)
        self.finite = finite
        self.ended = False
        self._wake = asyncio.Event()

    def push(self, item: T) -> None:
        self.items.append(item)
        self._wake.set()

    def end(self) -> None:
        self.ended = True
        self._wake.set()

    def rearm(self) -> None:
        """A new connection: a live script that a close ended serves again (items pushed meanwhile are kept)."""
        if self.ended and not self.finite:
            self.ended = False
            self._wake = asyncio.Event()

    async def next(self) -> tuple[bool, T | None]:
        while True:
            if self.items:
                return True, self.items.popleft()
            if self.finite or self.ended:
                return False, None
            self._wake.clear()
            await self._wake.wait()


@dataclass
class MockFixture:
    """Everything a MockBrokerAdapter serves, as plain lists."""

    instruments: list[InstrumentSpec] = field(default_factory=list)
    orders: list[OrderDTO] = field(default_factory=list)
    positions: list[PositionDTO] = field(default_factory=list)
    accounts: list[AccountDTO] = field(default_factory=list)
    account_live: list[AccountLive] = field(default_factory=list)
    cash_flows: list[CashFlowDTO] = field(default_factory=list)
    prices: list[PriceDTO] = field(default_factory=list)
    order_stream: list[OrderStreamItem] = field(default_factory=list)
    account_stream: list[AccountStreamItem] = field(default_factory=list)
    market_sessions: list[MarketSessions] = field(default_factory=list)
    #: What ``fetch_history`` serves: the broker changes scripted as made while the engine was off.
    history_orders: list[OrderDTO] = field(default_factory=list)
    history_positions: list[PositionDTO] = field(default_factory=list)


class MockBrokerAdapter(BrokerAdapter):
    """Deterministic in-memory adapter implementing every BrokerAdapter method."""

    def __init__(
        self,
        *,
        adapter_id: str = "mock",
        instruments: Iterable[InstrumentSpec] = (),
        orders: Iterable[OrderDTO] = (),
        positions: Iterable[PositionDTO] = (),
        accounts: Iterable[AccountDTO] = (),
        account_live: Iterable[AccountLive] = (),
        cash_flows: Iterable[CashFlowDTO] = (),
        order_stream: Iterable[OrderStreamItem] = (),
        account_stream: Iterable[AccountStreamItem] = (),
        price_stream: Iterable[PriceStreamItem] = (),
        versioning: dict[EntityType, VersionDecl] | None = None,
        finite_streams: bool = True,
        announce_subscribed: bool = True,
        market_sessions: Iterable[MarketSessions] = (),
        default_fx_sessions: bool = True,
        rest_budget: RestBudget | None = None,
        history_orders: Iterable[OrderDTO] = (),
        history_positions: Iterable[PositionDTO] = (),
        history_retention_ms: int | None = None,
        wall_ms: Callable[[], int] | None = None,
    ) -> None:
        self.adapter_id = adapter_id
        self.VERSIONING = dict(versioning or DEFAULT_VERSIONING)
        self._instruments: dict[str, InstrumentSpec] = {i.symbol: i for i in instruments}
        self._orders: dict[str, OrderDTO] = {o.broker_order_id: o for o in orders}
        self._positions: dict[str, PositionDTO] = {p.position_id: p for p in positions}
        self._accounts: dict[str, AccountDTO] = {a.account_id: a for a in accounts}
        self._account_live: dict[str, AccountLive] = {a.account_id: a for a in account_live}
        self._cash_flows: dict[str, CashFlowDTO] = {c.transaction_id: c for c in cash_flows}
        self._order_script: _Script[OrderStreamItem] = _Script(order_stream, finite=finite_streams)
        self._account_script: _Script[AccountStreamItem] = _Script(account_stream, finite=finite_streams)
        self._price_script: _Script[PriceStreamItem] = _Script(price_stream, finite=finite_streams)
        self._announce = announce_subscribed
        self._market_sessions: dict[str, MarketSessions] = {m.symbol: m for m in market_sessions}
        self._default_fx_sessions = default_fx_sessions
        self._failures: dict[str, BrokerError] = {}
        self.calls: list[str] = []
        self._budget = rest_budget
        self.rest_calls: list[tuple[str, RestPriority]] = []   # (fetch_*, the caller's rest_priority)
        self.connected = False
        self.connect_count = 0
        self.subscribed_symbols: set[str] = set()
        #: Broker rows the adapter could not map (orchestrator UNMAPPED_ATTR); scripted with ``skip_row``.
        self.unmapped_rows: dict[str, int] = new_unmapped_rows()
        self._cash_flows_partial: tuple[str, ...] = ()
        self._history_orders: list[OrderDTO] = list(history_orders)
        self._history_positions: list[PositionDTO] = list(history_positions)
        self._history_retention_ms = history_retention_ms
        self._history_notes: tuple[str, ...] = ()
        self._history_notes_retryable = False
        self._history_unmapped = 0
        self._history_supported = True
        self.history_requests: list[tuple[int, int]] = []
        self._wall_ms = wall_ms if wall_ms is not None else (lambda: time.time_ns() // 1_000_000)
        self._bars: dict[BarKey, list[BrokerBar]] = {}
        self._bars_cap: dict[BarKey, int | None] = {}
        self._bars_truncated: dict[BarKey, bool] = {}
        self._bars_field_names: dict[BarKey, tuple[str, ...]] = {}
        self._bar_failures: dict[BarKey, collections.deque[BrokerError]] = {}
        self._bars_supported = True
        #: Every fetch_price_bars request: (symbol, side, timeframe_s, count, from_ms, to_ms).
        self.price_bar_requests: list[tuple[str, str, int, int | None, int | None, int | None]] = []
        self._stop_orders_supported = True
        self._stop_order_positions = False
        self.stop_order_reads = 0       # fetch_active_stop_orders calls that read the book

    @classmethod
    def from_fixture(cls, fixture: MockFixture, **kwargs: object) -> MockBrokerAdapter:
        adapter_id = str(kwargs.pop("adapter_id", fixture.instruments[0].adapter_id if fixture.instruments else "mock"))
        finite = bool(kwargs.pop("finite_streams", True))
        announce = bool(kwargs.pop("announce_subscribed", True))
        retention = kwargs.pop("history_retention_ms", None)
        if kwargs:
            raise TypeError(f"unexpected arguments: {sorted(kwargs)}")
        return cls(adapter_id=adapter_id, instruments=fixture.instruments, orders=fixture.orders,
                   positions=fixture.positions, accounts=fixture.accounts, account_live=fixture.account_live,
                   cash_flows=fixture.cash_flows, order_stream=fixture.order_stream,
                   account_stream=fixture.account_stream, price_stream=fixture.prices,
                   finite_streams=finite, announce_subscribed=announce, market_sessions=fixture.market_sessions,
                   history_orders=fixture.history_orders, history_positions=fixture.history_positions,
                   history_retention_ms=retention if isinstance(retention, int) else None)

    # --- scripting helpers ----------------------------------------------------------------------
    def fail_next(self, method: str, error: BrokerError) -> None:
        """The next call of ``method`` (e.g. "fetch_positions", "connect") raises ``error``."""
        self._failures[method] = error

    def set_order(self, order: OrderDTO) -> None:
        self._orders[order.broker_order_id] = order

    def set_position(self, position: PositionDTO) -> None:
        self._positions[position.position_id] = position

    def set_account(self, account: AccountDTO, live: AccountLive | None = None) -> None:
        self._accounts[account.account_id] = account
        if live is not None:
            self._account_live[live.account_id] = live

    def add_cash_flow(self, flow: CashFlowDTO) -> None:
        self._cash_flows[flow.transaction_id] = flow

    def skip_row(self, stream: StreamName = "orders", why: str = "scripted unmappable row") -> None:
        """Script a broker row this adapter could not map (counted in ``unmapped_rows``)."""
        note_unmapped(self.unmapped_rows, self.adapter_id, stream, why)

    def set_cash_flows_partial(self, *reasons: str) -> None:
        """Every later ``fetch_cash_flows`` result is partial for ``reasons`` (none = complete again)."""
        self._cash_flows_partial = tuple(reasons)

    def change_while_off(self, *items: OrderDTO | PositionDTO) -> None:
        """Script broker changes made while the engine was off: each item becomes the broker's current row (as
        ``set_order`` / ``set_position``) and is listed by ``fetch_history`` for any window holding its
        ``broker_updated_at_ms``. Nothing is streamed."""
        for item in items:
            if isinstance(item, OrderDTO):
                self._orders[item.broker_order_id] = item
                self._history_orders.append(item)
            else:
                self._positions[item.position_id] = item
                self._history_positions.append(item)

    def list_in_history(self, *items: OrderDTO | PositionDTO) -> None:
        """Script a historic row: listed by ``fetch_history`` for any window holding its ``broker_updated_at_ms``,
        without changing the broker's current book (e.g. an older revision of an entity that changed since)."""
        for item in items:
            if isinstance(item, OrderDTO):
                self._history_orders.append(item)
            else:
                self._history_positions.append(item)

    def set_history_incomplete(self, *notes: str, retryable: bool = False) -> None:
        """Every later ``fetch_history`` answer is incomplete for ``notes`` (none = complete again). By default the
        notes are broker limits (a re-read would repeat them); ``retryable=True`` scripts a passing cause on this
        side (a failed page, a timeout), so the catch-up holds coverage back and retries."""
        self._history_notes = tuple(notes)
        self._history_notes_retryable = retryable

    def set_history_unmapped(self, rows: int) -> None:
        """Every later ``fetch_history`` answer holds ``rows`` broker rows the adapter could not map: counted on the
        read's own channel (``history_read``), never on the live-stream ``unmapped_rows`` (0 = none again)."""
        self._history_unmapped = rows

    def set_history_supported(self, supported: bool) -> None:
        """False: ``fetch_history`` answers like an adapter without the facility (the base default)."""
        self._history_supported = supported

    def set_instrument(self, spec: InstrumentSpec) -> None:
        self._instruments[spec.symbol] = spec

    def set_order_kind(self, broker_order_id: str, kind: OrderKind | None) -> OrderDTO:
        """Script the broker's order type (design PR1) on an order of the current book: later REST reads serve it
        with ``order_kind = kind`` (None: the broker states no type). Returns the new row; KeyError when unknown."""
        order = dataclasses.replace(self._orders[broker_order_id], order_kind=kind)
        self._orders[broker_order_id] = order
        return order

    def set_instrument_metadata(self, symbol: str, *, price_increment: Decimal | None = None,
                                min_stop_distance: Decimal | None = None,
                                asset_class: str | None = None) -> InstrumentSpec:
        """Script the PR7 instrument metadata of ``symbol`` (None: the broker states none, so an ARM position on it
        is UNSUPPORTED). Later ``fetch_instruments`` calls serve it. Returns the new spec; KeyError when unknown."""
        spec = dataclasses.replace(self._instruments[symbol], price_increment=price_increment,
                                   min_stop_distance=min_stop_distance, asset_class=asset_class)
        self._instruments[symbol] = spec
        return spec

    def set_price_bars(self, symbol: str, side: BarSide, timeframe_s: int, bars: Iterable[BrokerBar], *,
                       max_bars: int | None = None, truncated: bool = False,
                       field_names: Sequence[str] = ()) -> None:
        """Script the bars of one series (design PR3), any order (served oldest first); a bar with
        ``partial=True`` is the forming one. ``max_bars``: the cap on complete bars per answer (a range answer with
        more keeps the oldest ``max_bars``, a count answer the newest; either is then truncated). ``truncated``: every
        answer says it was capped.
        ``field_names``: the names the scripted answer reports (PR3 probe)."""
        key: BarKey = (symbol, side, timeframe_s)
        self._bars[key] = sorted(bars, key=lambda b: b.bucket_start_ms)
        self._bars_cap[key] = max_bars
        self._bars_truncated[key] = truncated
        self._bars_field_names[key] = tuple(field_names)

    def fail_price_bars(self, symbol: str, side: BarSide, timeframe_s: int, error: BrokerError,
                        times: int = 1) -> None:
        """The next ``times`` reads of this series raise ``error`` (e.g. ``TransientBrokerError``)."""
        queue = self._bar_failures.setdefault((symbol, side, timeframe_s), collections.deque())
        queue.extend([error] * times)

    def undecodable_price_bars(self, symbol: str, side: BarSide, timeframe_s: int, times: int = 1) -> None:
        """The next ``times`` reads of this series get an answer that cannot be decoded (``PriceBarsDecodeError``)."""
        self.fail_price_bars(symbol, side, timeframe_s,
                             PriceBarsDecodeError("mock bars: scripted undecodable answer"), times)

    def set_price_bars_supported(self, supported: bool) -> None:
        """False: ``fetch_price_bars`` answers like an adapter without the capability (the base default)."""
        self._bars_supported = supported

    def set_stop_orders_supported(self, supported: bool, *, positions: bool = False) -> None:
        """``fetch_active_stop_orders`` (design PR9): False answers like an adapter without the read (the base
        default). ``positions=True`` lists the open positions (cTrader POSITION_FIELD handles) instead of the
        working stop / limit orders (StoneX CHILD_ORDER handles)."""
        self._stop_orders_supported = supported
        self._stop_order_positions = positions

    def set_market_sessions(self, sessions: MarketSessions) -> None:
        self._market_sessions[sessions.symbol] = sessions

    def push_order_update(self, item: OrderStreamItem) -> None:
        self._order_script.push(item)

    def push_account_update(self, item: AccountStreamItem) -> None:
        self._account_script.push(item)

    def push_price(self, item: PriceStreamItem) -> None:
        self._price_script.push(item)

    def end_streams(self) -> None:
        for script in (self._order_script, self._account_script, self._price_script):
            script.end()

    @property
    def rest_budget(self) -> RestBudget | None:
        return self._budget

    async def _rest(self, method: str) -> None:
        """A REST read: record the caller's priority and take a token when a budget is attached. The send time is
        stamped (``rest_sent_ms``): by the budget after its token wait, else from the mock's wall clock."""
        self.rest_calls.append((method, current_rest_priority()))
        if self._budget is not None:
            await self._budget.acquire()
        else:
            mark_rest_sent(self._wall_ms())

    def _enter(self, method: str) -> None:
        self.calls.append(method)
        error = self._failures.pop(method, None)
        if error is not None:
            raise error

    # --- BrokerAdapter ---------------------------------------------------------------------------
    async def connect(self) -> None:
        """A (re)connect: live streams ended by an earlier ``close()`` (a socket loss, a drain) serve again, so the
        mock broker survives reconnects and engine restarts like a real one."""
        self._enter("connect")
        self.connected = True
        self.connect_count += 1
        for script in (self._order_script, self._account_script, self._price_script):
            script.rearm()

    async def close(self) -> None:
        self._enter("close")
        self.connected = False
        self.end_streams()

    async def fetch_instruments(self) -> list[InstrumentSpec]:
        await self._rest("fetch_instruments")
        self._enter("fetch_instruments")
        return list(self._instruments.values())

    async def fetch_open_orders(self) -> list[OrderDTO]:
        await self._rest("fetch_open_orders")
        self._enter("fetch_open_orders")
        return [o for o in self._orders.values() if not o.status.terminal]

    async def fetch_order_status(self, broker_order_id: str) -> OrderDTO | None:
        await self._rest("fetch_order_status")
        self._enter("fetch_order_status")
        found = self._orders.get(broker_order_id)
        if found is None:
            note_absent("ORDER", broker_order_id)     # the scripted book is the mock broker's whole truth
        return found

    async def fetch_positions(self) -> list[PositionDTO]:
        await self._rest("fetch_positions")
        self._enter("fetch_positions")
        return [p for p in self._positions.values() if p.status == "OPEN"]

    async def fetch_position_status(self, position_id: str) -> PositionDTO | None:
        await self._rest("fetch_position_status")
        self._enter("fetch_position_status")
        found = self._positions.get(position_id)
        if found is None:
            note_absent("POSITION", position_id)
        return found

    async def fetch_account_snapshot(self) -> tuple[list[AccountDTO], list[AccountLive]]:
        await self._rest("fetch_account_snapshot")
        self._enter("fetch_account_snapshot")
        return list(self._accounts.values()), list(self._account_live.values())

    async def fetch_cash_flows(self, since_ms: int) -> list[CashFlowDTO]:
        await self._rest("fetch_cash_flows")
        self._enter("fetch_cash_flows")
        return CashFlowBatch(sorted((c for c in self._cash_flows.values() if c.broker_ts >= since_ms),
                                    key=lambda c: (c.broker_ts, c.transaction_id)), self._cash_flows_partial)

    async def fetch_market_sessions(self, symbols: Sequence[str]) -> dict[str, MarketSessions]:
        """Scripted sessions per symbol; a known FX-pair instrument without a script gets the flagged FX
        default (``default_fx_sessions``), like StoneX; anything else (indices, metals) is absent."""
        await self._rest("fetch_market_sessions")
        self._enter("fetch_market_sessions")
        out: dict[str, MarketSessions] = {}
        for symbol in symbols:
            scripted = self._market_sessions.get(symbol)
            if scripted is not None:
                out[symbol] = scripted
            elif self._default_fx_sessions and symbol in self._instruments and looks_like_fx_pair(symbol):
                out[symbol] = fx_default_sessions(self.adapter_id, symbol, source="mock")
        return out

    @property
    def history_retention_ms(self) -> int | None:
        return self._history_retention_ms

    async def fetch_history(self, since_ms: int, until_ms: int) -> BrokerHistory:
        """The scripted history rows whose broker time lies in the covered part of ``[since_ms, until_ms]``,
        latest row per order / position. A window older than ``history_retention_ms`` is cut and incomplete."""
        await self._rest("fetch_history")
        self._enter("fetch_history")
        self.history_requests.append((since_ms, until_ms))
        if not self._history_supported:
            return BrokerHistory.unsupported(self.adapter_id, since_ms, until_ms)
        start = covered_from(since_ms, until_ms, self._history_retention_ms)
        notes = HistoryNotes()
        for note in self._history_notes:
            if self._history_notes_retryable:
                notes.append(note)
            else:
                notes.limit(note)
        if start > since_ms:
            notes.limit(f"broker history covers only the last {self._history_retention_ms} ms of the window")
        with history_read() as read:
            for _ in range(self._history_unmapped):
                note_unmapped(self.unmapped_rows, self.adapter_id, "orders", "scripted unmappable history row")
        if read.unmapped:
            notes.append(f"{read.unmapped} history row(s) could not be mapped")

        orders: dict[str, OrderDTO] = {}
        for o in self._history_orders:
            if start <= o.broker_updated_at_ms <= until_ms and (
                    o.broker_order_id not in orders
                    or orders[o.broker_order_id].broker_updated_at_ms <= o.broker_updated_at_ms):
                orders[o.broker_order_id] = o
        positions: dict[str, PositionDTO] = {}
        for p in self._history_positions:
            if start <= p.broker_updated_at_ms <= until_ms and (
                    p.position_id not in positions
                    or positions[p.position_id].broker_updated_at_ms <= p.broker_updated_at_ms):
                positions[p.position_id] = p
        return BrokerHistory(
            adapter_id=self.adapter_id, since_ms=since_ms, until_ms=until_ms, covered_from_ms=start,
            orders=tuple(sorted(orders.values(), key=lambda o: (o.broker_updated_at_ms, o.broker_order_id))),
            positions=tuple(sorted(positions.values(), key=lambda p: (p.broker_updated_at_ms, p.position_id))),
            complete=not notes, notes=notes.unique(), retryable=notes.retryable, unmapped=read.unmapped)

    @property
    def price_bars_supported(self) -> bool:
        return self._bars_supported

    async def fetch_price_bars(self, symbol: str, side: BarSide, timeframe_s: int, *, count: int | None = None,
                               from_ms: int | None = None, to_ms: int | None = None) -> PriceBarsResponse:
        """The scripted bars of the series: the newest ``count`` complete bars plus the forming one (like StoneX's
        ``PriceBars`` count and ``PartialPriceBar``), or those starting in ``[from_ms, to_ms]``. A series never scripted answers with no bars (a complete, empty answer). Records the
        request and the caller's REST priority."""
        problem = bar_request_problem(side, timeframe_s, count, from_ms, to_ms)
        if problem is not None:
            raise ValueError(f"fetch_price_bars: {problem}")
        self.price_bar_requests.append((symbol, side, timeframe_s, count, from_ms, to_ms))
        if not self._bars_supported:
            self._enter("fetch_price_bars")
            return PriceBarsResponse.unsupported(self.adapter_id, symbol, side, timeframe_s, count=count,
                                                 from_ms=from_ms, to_ms=to_ms)
        await self._rest("fetch_price_bars")
        self._enter("fetch_price_bars")
        key: BarKey = (symbol, side, timeframe_s)
        failures = self._bar_failures.get(key)
        if failures:
            raise failures.popleft()
        series = self._bars.get(key, [])
        cap = self._bars_cap.get(key)
        truncated = self._bars_truncated.get(key, False)
        if count is not None:
            bars = [b for b in series if not b.partial][-count:]
            if cap is not None and len(bars) > cap:
                bars, truncated = bars[-cap:], True
            forming = [b for b in series if b.partial]
            bars.extend(forming[-1:])
        else:
            assert from_ms is not None and to_ms is not None
            bars = [b for b in series if from_ms <= b.bucket_start_ms <= to_ms]
            if cap is not None and len(bars) > cap:
                bars, truncated = bars[:cap], True
        return PriceBarsResponse(adapter_id=self.adapter_id, symbol=symbol, side=side, timeframe_s=timeframe_s,
                                 bars=tuple(bars), truncated=truncated, received_ms=self._wall_ms(), count=count,
                                 from_ms=from_ms, to_ms=to_ms, field_names=self._bars_field_names.get(key, ()))

    async def fetch_active_stop_orders(self) -> ActiveStopOrders:
        """One list read (design PR9): every working order of the scripted book with a known STOP / LIMIT kind (an
        untyped order is no stop / limit order), or with ``set_stop_orders_supported(True, positions=True)`` the
        open positions. Records the caller's REST priority like every read."""
        if not self._stop_orders_supported:
            self._enter("fetch_active_stop_orders")
            return ActiveStopOrders.unsupported(self.adapter_id)
        await self._rest("fetch_active_stop_orders")
        self._enter("fetch_active_stop_orders")
        self.stop_order_reads += 1
        if self._stop_order_positions:
            return ActiveStopOrders(adapter_id=self.adapter_id,
                                    positions=tuple(p for p in self._positions.values() if p.status == "OPEN"))
        return ActiveStopOrders(adapter_id=self.adapter_id, orders=tuple(
            o for o in self._orders.values() if not o.status.terminal and o.order_kind is not None))

    async def ensure_ticks(self, symbols: Sequence[str]) -> None:
        self._enter("ensure_ticks")
        self.subscribed_symbols.update(symbols)

    def _subscribed(self, stream: StreamName) -> StreamEvent:
        return StreamEvent(StreamEventKind.SUBSCRIBED, stream, "mock")

    async def stream_ticks(self, symbols: Sequence[str]) -> AsyncIterator[PriceStreamItem]:
        self._enter("stream_ticks")
        self.subscribed_symbols.update(symbols)
        if self._announce:
            yield self._subscribed("prices")
        while True:
            ok, item = await self._price_script.next()
            if not ok:
                return
            if isinstance(item, PriceDTO) and item.symbol not in self.subscribed_symbols:
                continue
            if item is not None:
                yield item

    async def stream_order_updates(self) -> AsyncIterator[OrderStreamItem]:
        self._enter("stream_order_updates")
        if self._announce:
            yield self._subscribed("orders")
        while True:
            ok, item = await self._order_script.next()
            if not ok:
                return
            if isinstance(item, OrderDTO):
                self._orders[item.broker_order_id] = item
            elif isinstance(item, PositionDTO):
                self._positions[item.position_id] = item
            if item is not None:
                yield item

    async def stream_account(self) -> AsyncIterator[AccountStreamItem]:
        self._enter("stream_account")
        if self._announce:
            yield self._subscribed("account")
        while True:
            ok, item = await self._account_script.next()
            if not ok:
                return
            if isinstance(item, AccountDTO):
                self._accounts[item.account_id] = item
            elif isinstance(item, AccountLive):
                self._account_live[item.account_id] = item
            elif isinstance(item, CashFlowDTO):
                self._cash_flows[item.transaction_id] = item
            if item is not None:
                yield item


# --- fixtures --------------------------------------------------------------------------------------
def fx_spec(symbol: str, *, adapter_id: str = "mock", min_qty: Decimal = Decimal(1000),
            broker_symbol: str = "", price_increment: Decimal | None = None,
            min_stop_distance: Decimal | None = None, asset_class: str | None = None) -> InstrumentSpec:
    """An FX InstrumentSpec in base-currency units (pip 0.0001, 0.01 for JPY quotes). The PR7 metadata
    (``price_increment``, ``min_stop_distance``, ``asset_class``) is None unless scripted, so the default fixtures
    keep their checksums."""
    base, quote = symbol[:3], symbol[3:]
    return InstrumentSpec(
        adapter_id=adapter_id, symbol=symbol, base_ccy=base, quote_ccy=quote, pnl_ccy=quote,
        contract_size=Decimal(1), unit_kind="UNITS",
        point_size=Decimal("0.01") if quote == "JPY" else Decimal("0.0001"),
        min_qty=min_qty, swap_booked_to_balance=True, broker_symbol=broker_symbol or symbol,
        price_increment=price_increment, min_stop_distance=min_stop_distance, asset_class=asset_class,
    )


def eurusd_spec(adapter_id: str = "mock") -> InstrumentSpec:
    return fx_spec("EURUSD", adapter_id=adapter_id, broker_symbol="401484347")


def audusd_spec(adapter_id: str = "mock") -> InstrumentSpec:
    return fx_spec("AUDUSD", adapter_id=adapter_id, broker_symbol="401484317")


FIXTURE_TS_MS = 1_767_225_600_000   # 2026-01-01T00:00:00Z


def build_fx_fixture(adapter_id: str = "mock", account_id: str = "MOCK-AUD-1",
                     ts_ms: int = FIXTURE_TS_MS) -> MockFixture:
    """EURUSD / AUDUSD fixture: an AUD account (balance 100 000 AUD) with
    - EURUSD long 20 000 @ 1.0850 (position P-EU-1) protected by a separate reduce-only stop child;
    - a StoneX-style partially filled AUDUSD parent PF-1 (ordered 100 000, 60 000 filled, so a 60 000
      position is open) with an if-done stop child PF-1-SL linked to the parent;
    - one deposit, prices for both symbols, and a short order / account stream script."""
    eu, au = eurusd_spec(adapter_id), audusd_spec(adapter_id)
    d = Decimal

    order = functools.partial(OrderDTO, account_id=account_id, adapter_id=adapter_id,
                              broker_updated_at_ms=ts_ms, source="REST")

    eu_position = PositionDTO(
        position_id="P-EU-1", account_id=account_id, adapter_id=adapter_id, symbol="EURUSD", side="BUY",
        units=d(20000), raw_qty=d(20000), entry_price=d("1.0850"), status="OPEN",
        opened_by_client_order_id="P-EU-1", broker_updated_at_ms=ts_ms, source="REST")
    eu_stop = order(client_order_id="P-EU-1-SL", broker_order_id="P-EU-1-SL", symbol="EURUSD", side="SELL",
                    status=OrderStatus.WORKING, entry_price=d("1.0800"), ordered_units=d(20000),
                    remaining_units=d(20000), raw_qty=d(20000), is_reduce_only=True,
                    linked_position_id="P-EU-1")
    parent = order(client_order_id="PF-1", broker_order_id="PF-1", symbol="AUDUSD", side="BUY",
                   status=OrderStatus.PARTIALLY_FILLED, entry_price=d("0.6550"), ordered_units=d(100000),
                   remaining_units=d(40000), raw_qty=d(100000))
    parent_position = PositionDTO(
        position_id="PF-1", account_id=account_id, adapter_id=adapter_id, symbol="AUDUSD", side="BUY",
        units=d(60000), raw_qty=d(60000), entry_price=d("0.6550"), status="OPEN",
        opened_by_client_order_id="PF-1", broker_updated_at_ms=ts_ms, source="REST")
    child_stop = order(client_order_id="PF-1-SL", broker_order_id="PF-1-SL", symbol="AUDUSD", side="SELL",
                       status=OrderStatus.WORKING, entry_price=d("0.6500"), ordered_units=d(100000),
                       remaining_units=d(100000), raw_qty=d(100000), is_reduce_only=True,
                       linked_position_id="PF-1")
    account = AccountDTO(account_id=account_id, adapter_id=adapter_id, currency="AUD", balance=d(100000),
                         realised_pnl_today=d(0), source="REST")
    live = AccountLive(account_id=account_id, adapter_id=adapter_id, currency="AUD", equity=d("100150.00"),
                       margin_used=d("2500.00"), free_margin=d("97650.00"), unrealised_pnl=d("150.00"),
                       timestamp_ms=ts_ms)
    deposit = CashFlowDTO(transaction_id=f"{adapter_id}:deposit:1", account_id=account_id, adapter_id=adapter_id,
                          kind="DEPOSIT", amount=d(100000), currency="AUD", broker_ts=ts_ms - 86_400_000)
    prices = [
        PriceDTO(adapter_id=adapter_id, symbol="EURUSD", bid=d("1.08600"), ask=d("1.08610"),
                 spread_pips=d("1.0"), timestamp_ms=ts_ms, broker_market_id=eu.broker_symbol),
        PriceDTO(adapter_id=adapter_id, symbol="AUDUSD", bid=d("0.65600"), ask=d("0.65610"),
                 spread_pips=d("1.0"), timestamp_ms=ts_ms, broker_market_id=au.broker_symbol),
    ]
    stream_ts = ts_ms + 1_000
    order_stream: list[OrderStreamItem] = [
        dataclasses.replace(child_stop, entry_price=d("0.6520"), broker_updated_at_ms=stream_ts, source="STREAM"),
        dataclasses.replace(eu_position, units=d(10000), raw_qty=d(10000), broker_updated_at_ms=stream_ts,
                            source="STREAM"),
    ]
    account_stream: list[AccountStreamItem] = [
        dataclasses.replace(live, equity=d("100180.00"), unrealised_pnl=d("180.00"), timestamp_ms=stream_ts),
    ]
    return MockFixture(
        instruments=[eu, au], orders=[eu_stop, parent, child_stop], positions=[eu_position, parent_position],
        accounts=[account], account_live=[live], cash_flows=[deposit], prices=prices,
        order_stream=order_stream, account_stream=account_stream,
    )


#: Offline window of ``build_offline_history_fixture``: the engine was off from OFF_SINCE_MS to OFF_UNTIL_MS.
OFF_SINCE_MS = FIXTURE_TS_MS + 3_600_000           # 2026-01-01T01:00Z: last heartbeat before the PC slept
OFF_UNTIL_MS = OFF_SINCE_MS + 8 * 3_600_000        # eight hours later: the engine starts again


def build_offline_history_fixture(adapter_id: str = "mock", account_id: str = "MOCK-AUD-1",
                                  since_ms: int = OFF_SINCE_MS, until_ms: int = OFF_UNTIL_MS) -> MockFixture:
    """``build_fx_fixture`` plus broker changes made entirely while the engine was off (``since_ms`` to
    ``until_ms``), listed by ``fetch_history`` and already in the broker's current book:
    - H-MKT-1: an EURUSD market BUY placed and FILLED in the window; its position H-MKT-1 is still OPEN;
    - H-RT-1: an AUDUSD market SELL placed and FILLED in the window; its position H-RT-1 was CLOSED later in
      the window by the reduce-only closing trade H-RT-1-X (FILLED, linked to H-RT-1);
    - H-STP-1: an EURUSD stop SELL entry placed and CANCELLED in the window (never filled).
    Each history row carries the broker time of its last change in the window (source REST)."""
    fx = build_fx_fixture(adapter_id, account_id)
    d = Decimal
    span = max(until_ms - since_ms, 10)
    t = [since_ms + span * k // 10 for k in range(1, 10)]
    order = functools.partial(OrderDTO, account_id=account_id, adapter_id=adapter_id, source="REST")
    position = functools.partial(PositionDTO, account_id=account_id, adapter_id=adapter_id, source="REST")

    mkt = order(client_order_id="H-MKT-1", broker_order_id="H-MKT-1", symbol="EURUSD", side="BUY",
                status=OrderStatus.FILLED, entry_price=d("1.0870"), ordered_units=d(10000), remaining_units=d(0),
                raw_qty=d(10000), broker_updated_at_ms=t[0])
    mkt_pos = position(position_id="H-MKT-1", symbol="EURUSD", side="BUY", units=d(10000), raw_qty=d(10000),
                       entry_price=d("1.0870"), status="OPEN", opened_by_client_order_id="H-MKT-1",
                       broker_updated_at_ms=t[0])
    rt = order(client_order_id="H-RT-1", broker_order_id="H-RT-1", symbol="AUDUSD", side="SELL",
               status=OrderStatus.FILLED, entry_price=d("0.6560"), ordered_units=d(20000), remaining_units=d(0),
               raw_qty=d(20000), broker_updated_at_ms=t[2])
    rt_close = order(client_order_id="H-RT-1-X", broker_order_id="H-RT-1-X", symbol="AUDUSD", side="BUY",
                     status=OrderStatus.FILLED, entry_price=d("0.6540"), ordered_units=d(20000),
                     remaining_units=d(0), raw_qty=d(20000), is_reduce_only=True, linked_position_id="H-RT-1",
                     broker_updated_at_ms=t[5])
    rt_pos = position(position_id="H-RT-1", symbol="AUDUSD", side="SELL", units=d(0), raw_qty=d(0),
                      entry_price=d("0.6560"), status="CLOSED", opened_by_client_order_id="H-RT-1",
                      broker_updated_at_ms=t[5])
    stop = order(client_order_id="H-STP-1", broker_order_id="H-STP-1", symbol="EURUSD", side="SELL",
                 status=OrderStatus.CANCELLED, entry_price=d("1.0790"), ordered_units=d(5000),
                 remaining_units=d(5000), raw_qty=d(5000), broker_updated_at_ms=t[7])
    history_orders = [mkt, rt, rt_close, stop]
    history_positions = [mkt_pos, rt_pos]
    return dataclasses.replace(fx, orders=[*fx.orders, *history_orders],
                               positions=[*fx.positions, *history_positions],
                               history_orders=history_orders, history_positions=history_positions)
