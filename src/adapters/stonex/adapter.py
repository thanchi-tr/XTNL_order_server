"""StoneXAdapter: read-only StoneX (GAIN Capital CIAPI) BrokerAdapter (adapter-isolation.md).

REST reads go through StoneXRestClient (ReadOnlyTransport); streams come from Lightstreamer
(STREAMINGALL: ORDERS, CLIENTACCOUNTMARGIN, PRICES) through the StreamTransport bridge.
account_id = ClientAccountId (the margin / risk account); trading accounts map onto it.
No order-changing capability exists here. Never imports db / cache / pubsub / integrity / oversee.

Every REST read takes a token from the adapter's one RestBudget (operations.md §15). Stream mapping never
waits on REST (SKILL.md §2): the only inline read is the rate-limited missing-instrument refresh. The
if-done parent read for a stop / limit order of unknown relation runs as a background task; that order's
frames wait (in order) until it finishes, while every other frame keeps flowing.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import math
import re
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from decimal import Decimal

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
    PriceStreamItem,
    SessionExpiredError,
    StreamEvent,
    StreamEventKind,
    StreamName,
    TransientBrokerError,
    VersionDecl,
    history_read,
    new_unmapped_rows,
    note_absent,
    note_unmapped,
)
from src.adapters.ratelimit import RestBudget, RestPriority, rest_priority
from src.adapters.sessions import canonical
from src.adapters.stonex.bars import RANGE_CAP, decode_bar_history, interval_for
from src.adapters.stonex.codes import StoneXOrderType, canonical_status, order_kind_of, parse_side, parse_status
from src.adapters.stonex.history import (
    map_stop_limit_history,
    map_trade_history_window,
    market_ids,
    row_key,
    row_time_ms,
)
from src.adapters.stonex.instruments import ADAPTER_ID, parse_market_name, spec_from_market_info
from src.adapters.stonex.sessions import sessions_from_market_info
from src.adapters.stonex.mapping import (
    PRICE_FIELDS,
    AccountDirectory,
    UnmappableError,
    attached_orders,
    child_side,
    map_margin,
    map_open_position,
    map_price,
    map_stop_limit_order,
    map_trade_history_checked,
    map_trade_order,
    parent_of,
    parse_account_directory,
    rest_ts,
)
from src.adapters.stonex.orders import ORDER_FIELDS, OrderItem, OrderStreamMapper
from src.adapters.stonex.rest import OrderNotAccessibleError, StoneXRestClient, rest_get_only
from src.adapters.stonex.streaming import (
    MARKET_KEY,
    RECEIVED_KEY,
    BoundSink,
    Frame,
    FrameChannel,
    LightstreamerTransport,
    MarketDropCounter,
    StreamTransport,
    now_ms,
)
from src.adapters.stonex.wire import JsonObj, get_ci, get_obj, to_decimal, to_id
from src.config import StoneXSettings
from src.dto import (
    AccountDTO,
    AccountLive,
    BarSide,
    CashFlowDTO,
    InstrumentSpec,
    OrderDTO,
    OrderStatus,
    PositionDTO,
    PriceBarsResponse,
    bar_request_problem,
)

log = logging.getLogger(__name__)

ACCOUNT_FIELDS: tuple[str, ...] = (
    "Cash", "Margin", "MarginIndicator", "NetEquity", "OpenTradeEquity", "TradeableFunds", "PendingFunds",
    "TradingResource", "TotalMarginRequirement", "CurrencyId", "CurrencyISO",
)
INSTRUMENT_REFRESH_MIN_S = 60.0
MARKET_INFO_CHUNK = 50
TRADE_HISTORY_MAX = 200
HISTORY_MAX_PAGES = 50             # fetch_history: trade history pages per trading account at most
_FX_SYMBOL = re.compile(r"^[A-Z]{6}$")
PARENT_LOOKUP_TIMEOUT_S = 10.0     # a held order is mapped without the REST answer after this
MAX_DEFERRED_ORDERS = 64           # more unknown stop / limit orders at once: map them by the heuristic
_LOOKUP_DONE = "_xtnl_parent_lookup_done"   # adapter-internal frame key (never a broker field)


class StoneXAdapter(BrokerAdapter):
    """Read-only StoneX adapter. One instance per credential set; run it on the leader only."""

    adapter_id = ADAPTER_ID
    VERSIONING: dict[EntityType, VersionDecl] = {
        "ORDER": VersionDecl(has_broker_seq=False, has_broker_time=True),      # ORDERS LastChangedTime
        "POSITION": VersionDecl(has_broker_seq=False, has_broker_time=True),   # same item timestamp
        "ACCOUNT": VersionDecl(has_broker_seq=False, has_broker_time=False),   # local clock domain
    }

    def __init__(
        self,
        settings: StoneXSettings,
        *,
        rest: StoneXRestClient | None = None,
        stream_transport_factory: Callable[[], StreamTransport] | None = None,
        market_ids: Iterable[str | int] = (),
        symbol_market_ids: Mapping[str, str | int] | None = None,
        pre_connect_check: Callable[[], None] | None = None,
        queue_size: int = 10_000,
        heartbeat_s: float = 5.0,
        monotonic: Callable[[], float] = time.monotonic,
        wall_ms: Callable[[], int] = now_ms,
        rest_budget: RestBudget | None = None,
        parent_lookup_timeout_s: float = PARENT_LOOKUP_TIMEOUT_S,
        stop_limit_history: bool = False,
        history_page_size: int = TRADE_HISTORY_MAX,
        history_max_pages: int = HISTORY_MAX_PAGES,
        history_paging_confirmed: bool = False,
    ) -> None:
        self._settings = settings
        #: order/stoplimitorderhistory is not confirmed against the live API: off unless asked for.
        self._stop_limit_history = stop_limit_history
        self._history_page_size = max(1, history_page_size)
        self._history_max_pages = max(1, history_max_pages)
        #: Which time order/tradehistory's ``from`` filters on (executed or last changed) is not confirmed: until it
        #: is, an answer that needed more than one page is incomplete (a row executed before a page boundary but
        #: changed after it could fall between two pages).
        self._history_paging_confirmed = history_paging_confirmed
        self._owns_rest = rest is None
        if rest_budget is None and rest is not None and rest.budget is not None:
            rest_budget = rest.budget
        self._budget = rest_budget if rest_budget is not None else RestBudget(
            ADAPTER_ID, rate_per_s=settings.rest_rate_per_s, burst=settings.rest_burst,
            bars_per_min=settings.rest_bars_per_min)
        self._rest = rest if rest is not None else StoneXRestClient.create(settings, budget=self._budget)
        self._rest.budget = self._budget      # one bucket per adapter, across re-created REST clients
        self._rest_closed = False
        self._parent_lookup_timeout_s = parent_lookup_timeout_s
        self._logon_ms: int | None = None
        self._transport_factory = stream_transport_factory or (lambda: LightstreamerTransport(
            settings.stream_url, price_unfiltered=settings.price_unfiltered))
        self._pre_connect_check = pre_connect_check
        self._queue_size = queue_size
        self._heartbeat_s = heartbeat_s
        self._monotonic = monotonic
        self._wall_ms = wall_ms

        self._directory: AccountDirectory | None = None
        self._instruments: dict[str, InstrumentSpec] = {}     # by canonical symbol (read-only map)
        self._by_market: dict[str, InstrumentSpec] = {}       # by StoneX MarketId
        self._market_ids: set[str] = {str(m) for m in market_ids}
        self._symbol_market_ids = {k.upper(): str(v) for k, v in (symbol_market_ids or {}).items()}
        self._unresolved: dict[str, float] = {}
        self._last_on_demand: float | None = None
        self._refresh_lock = asyncio.Lock()
        self._mapper = OrderStreamMapper(lookup=self._lookup, account_for=self._account_for)
        self._account_last: dict[str, AccountDTO] = {}
        #: Broker rows (stream or REST) that could not be mapped, per stream (orchestrator UNMAPPED_ATTR):
        #: any increase opens that stream's gap, so a reconcile pass that misses a row cannot clear the fence.
        self.unmapped_rows: dict[str, int] = new_unmapped_rows()

        self._transport: StreamTransport | None = None
        self._connecting: asyncio.Task[StreamTransport] | None = None   # single-flight connect
        self._ls_gen = 0
        self._ls_status = "DISCONNECTED"
        self._channels: dict[StreamName, FrameChannel] = {}
        self._channels_lock = threading.Lock()
        # (channel, subscriptions, transport the subscriptions live on)
        self._price_state: tuple[FrameChannel, dict[str, object], StreamTransport] | None = None
        self._wanted_markets: set[str] = set()
        #: Price frames dropped for a full channel plus lost updates, per StoneX MarketId (PR6, IN-13).
        self._price_drops = MarketDropCounter()

    # --- introspection (not part of BrokerAdapter) ---------------------------------------------
    @property
    def account_directory(self) -> AccountDirectory | None:
        return self._directory

    @property
    def stream_watermark_ms(self) -> int | None:
        """Highest ORDERS LastChangedTime seen: the broker-time watermark for SM4 reconciliation."""
        return self._mapper.watermark_ms

    def instrument_for_market(self, market_id: str) -> InstrumentSpec | None:
        return self._by_market.get(market_id)

    @property
    def subscribed_price_markets(self) -> frozenset[str]:
        """The canonical symbols the current price stream has a market subscription for (design PR6; a symbol
        `_markets_for` could not resolve is not in it). Each market acks with its own SUBSCRIBED; a whole-stream
        SUBSCRIBED is only sent for an empty subscription, so it opens no market interval."""
        state = self._price_state
        if state is None:
            return frozenset()
        out: set[str] = set()
        for market_id in state[1]:
            spec = self._by_market.get(market_id)
            out.add(spec.symbol if spec is not None else market_id)
        return frozenset(out)

    @property
    def dropped_price_frames(self) -> dict[str, int]:
        """Price frames that never reached the consumer, per canonical symbol: dropped for a full channel plus the
        updates Lightstreamer reports lost (design PR6, IN-13). Monotonic over the adapter's life (metrics, never a
        gap: a lost price update is only counted). A market without a known instrument is keyed by its MarketId."""
        out: dict[str, int] = {}
        for market_id, count in self._price_drops.snapshot().items():
            spec = self._by_market.get(market_id)
            key = spec.symbol if spec is not None else market_id
            out[key] = out.get(key, 0) + count
        return out

    @property
    def rest_budget(self) -> RestBudget:
        return self._budget

    @property
    def token_issued_at_ms(self) -> int | None:
        """When the current StoneX session was opened (LogOn); None without a session."""
        return self._logon_ms if self._rest.has_session else None

    # --- session ---------------------------------------------------------------------------------
    def _check_lease(self) -> None:
        """operations.md §2: every connect / LogOn first checks the lease deadline synchronously."""
        if self._pre_connect_check is not None:
            self._pre_connect_check()

    async def connect(self) -> None:
        self._check_lease()
        if self._rest_closed:
            if not self._owns_rest:
                raise BrokerError("injected StoneX REST client was closed")
            self._rest = StoneXRestClient.create(self._settings, budget=self._budget)
            self._rest_closed = False
        if self._transport is not None or self._connecting is not None:
            # A new session token needs a new Lightstreamer session; an in-flight connect still
            # carries the old token, so it is superseded too.
            await self._stop_transport("re-authenticated")
        self._logon_ms = None
        await self._rest.log_on()
        self._logon_ms = self._wall_ms()
        directory = parse_account_directory(await self._rest.account_information())
        if not directory.accounts:
            raise BrokerError("StoneX returned no client accounts")
        self._directory = directory

    async def close(self) -> None:
        for ch in self._channel_snapshot():
            ch.put_event(StreamEventKind.DISCONNECTED, "adapter closed")
            ch.close()
        await self._stop_transport("adapter closed")
        await self._rest.log_off()
        if self._owns_rest and not self._rest_closed:
            await self._rest.aclose()
            self._rest_closed = True

    def _require_directory(self) -> AccountDirectory:
        if self._directory is None or not self._rest.has_session:
            raise SessionExpiredError("StoneX adapter is not connected")
        return self._directory

    def _account_for(self, trading_account_id: str | None) -> str | None:
        return None if self._directory is None else self._directory.client_for(trading_account_id)

    def _lookup(self, market_id: str | None) -> InstrumentSpec | None:
        return None if market_id is None else self._by_market.get(market_id)

    # --- instruments -----------------------------------------------------------------------------
    async def fetch_instruments(self) -> list[InstrumentSpec]:
        directory = self._require_directory()
        ids = set(self._market_ids) | set(self._symbol_market_ids.values())
        for trading_id, _client in directory.trading_ids:
            for row in [*await self._rest.open_positions(trading_id),
                        *await self._rest.active_stop_limit_orders(trading_id)]:
                market_id = to_id(get_ci(row, "MarketId"))
                if market_id is not None:
                    ids.add(market_id)
        await self._load_markets(ids)
        await self._load_conversion_pairs(directory)
        return list(self._instruments.values())

    async def _load_conversion_pairs(self, directory: AccountDirectory) -> None:
        """Also load the FX pair that converts each instrument's pnl_ccy into each account currency (e.g.
        AUDUSD for a USD-quoted EURUSD on an AUD account). Oversee marks risk in the account currency and the
        orchestrator subscribes the crosses it finds in the instrument map, so without this a held market in
        another currency stays `mark_unknown` (live finding, 2026-11-13). Read-only market search; a pair
        StoneX does not list is logged and skipped (Oversee then reports the mark as unknown, fail closed)."""
        targets = {a.currency for a in directory.accounts if a.currency}
        for spec in list(self._instruments.values()):
            pnl = (spec.pnl_ccy or "").upper()
            for ccy in targets:
                if not pnl or pnl == ccy or f"{pnl}{ccy}" in self._instruments or f"{ccy}{pnl}" in self._instruments:
                    continue
                for symbol in (f"{ccy}{pnl}", f"{pnl}{ccy}"):
                    try:
                        if await self._market_for_symbol(symbol) is not None:
                            break
                    except SessionExpiredError:
                        raise
                    except BrokerError as exc:
                        log.warning("stonex: conversion pair %s not loaded: %s", symbol, exc)

    async def _load_markets(self, ids: Iterable[str]) -> None:
        directory = self._require_directory()
        wanted = sorted({m for m in ids if m and m.isdigit()})
        for i in range(0, len(wanted), MARKET_INFO_CHUNK):
            chunk = wanted[i:i + MARKET_INFO_CHUNK]
            for info in await self._rest.market_information(directory.client_ids[0], chunk):
                spec = spec_from_market_info(info, self.adapter_id)
                if spec is not None:
                    self._add_spec(spec)
            self._market_ids.update(chunk)

    def _add_spec(self, spec: InstrumentSpec) -> None:
        self._by_market[spec.broker_symbol] = spec
        current = self._instruments.get(spec.symbol)
        if current is None or current.broker_symbol == spec.broker_symbol:
            self._instruments[spec.symbol] = spec
        else:
            log.info("stonex: %s also listed as market %s; prices keep market %s",
                     spec.symbol, spec.broker_symbol, current.broker_symbol)

    async def _ensure_markets(self, ids: Iterable[str | None]) -> None:
        """A market missing from the map triggers a refresh, at most once per 60 s (§1)."""
        missing = {m for m in ids if m is not None and m not in self._by_market}
        if not missing:
            return
        async with self._refresh_lock:
            missing = {m for m in missing if m not in self._by_market}
            now = self._monotonic()
            if not missing or (self._last_on_demand is not None
                               and now - self._last_on_demand < INSTRUMENT_REFRESH_MIN_S):
                return
            self._last_on_demand = now
            # The map serves stream mapping, which waits on this lock: never refresh it at the class of a
            # background caller (a DLQ / poll read that met an unknown market) - no priority inversion.
            with rest_priority(RestPriority.RECONCILE):
                await self._load_markets(missing)

    async def _market_for_symbol(self, symbol: str) -> str | None:
        symbol = symbol.upper()
        spec = self._instruments.get(symbol)
        if spec is not None:
            return spec.broker_symbol
        configured = self._symbol_market_ids.get(symbol)
        if configured is not None:
            await self._load_markets([configured])
            spec = self._by_market.get(configured)
            return configured if spec is not None else None
        retry_at = self._unresolved.get(symbol)
        if retry_at is not None and self._monotonic() < retry_at:
            return None
        directory = self._require_directory()
        query = f"{symbol[:3]}/{symbol[3:]}" if _FX_SYMBOL.match(symbol) else symbol
        candidates: list[str] = []
        for row in await self._rest.market_search(query, directory.client_ids[0]):
            name, market_id = get_ci(row, "Name", "MarketName"), to_id(get_ci(row, "MarketId"))
            parsed = parse_market_name(name) if isinstance(name, str) else None
            if market_id is not None and parsed is not None and parsed.symbol == symbol:
                candidates.append(market_id)
        if candidates:
            await self._load_markets(candidates[:5])
        spec = self._instruments.get(symbol)
        if spec is None:
            self._unresolved[symbol] = self._monotonic() + INSTRUMENT_REFRESH_MIN_S
            return None
        return spec.broker_symbol

    async def _markets_for(self, symbols: Sequence[str]) -> list[str]:
        found: list[str] = []
        for symbol in symbols:
            try:
                market_id = await self._market_for_symbol(symbol)
            except SessionExpiredError:
                raise
            except BrokerError as exc:
                log.warning("stonex: resolving %s failed: %s", symbol, exc)
                continue
            if market_id is None:
                log.warning("stonex: no market found for symbol %s (no prices)", symbol)
            elif market_id not in found:
                found.append(market_id)
        return found

    # --- market sessions (SM10) ------------------------------------------------------------------
    async def fetch_market_sessions(self, symbols: Sequence[str]) -> dict[str, MarketSessions]:
        """Trading hours per symbol from market information (read-only POST market/information, the
        call fetch_instruments makes). No hours -> the flagged FX default for an FX pair; any other
        market without hours, and a symbol with no StoneX market, is absent. StoneX has no holidays."""
        directory = self._require_directory()
        by_market: dict[str, list[str]] = {}
        for symbol in symbols:
            sym = canonical(symbol)          # 'EUR/USD' -> 'EURUSD': keyed like InstrumentSpec.symbol
            if not sym:
                continue
            market_id = await self._market_for_symbol(sym)
            if market_id is None:
                log.warning("stonex: no market found for %s (no market sessions)", sym)
                continue
            names = by_market.setdefault(market_id, [])
            if sym not in names:
                names.append(sym)
        infos: dict[str, JsonObj] = {}
        ids = sorted(by_market)
        for i in range(0, len(ids), MARKET_INFO_CHUNK):
            for info in await self._rest.market_information(directory.client_ids[0], ids[i:i + MARKET_INFO_CHUNK]):
                market_id = to_id(get_ci(info, "MarketId"))
                if market_id is not None:
                    infos[market_id] = info
        out: dict[str, MarketSessions] = {}
        for market_id, names in by_market.items():
            for sym in names:
                sessions = sessions_from_market_info(sym, infos.get(market_id), adapter_id=self.adapter_id)
                if sessions is None:
                    log.warning("stonex: market %s (%s) reports no trading hours and is not FX (no sessions)",
                                market_id, sym)
                else:
                    out[sym] = sessions
        return out

    # --- REST reads --------------------------------------------------------------------------
    async def fetch_positions(self) -> list[PositionDTO]:
        directory = self._require_directory()
        rows: list[tuple[str, JsonObj]] = []
        for trading_id, client in directory.trading_ids:
            rows.extend((client, row) for row in await self._rest.open_positions(trading_id))
        await self._ensure_markets(to_id(get_ci(r, "MarketId")) for _, r in rows)
        out: list[PositionDTO] = []
        for client, row in rows:
            try:
                position = map_open_position(row, account_id=client, lookup=self._lookup)
            except UnmappableError as exc:
                self._unmapped("orders", f"open position skipped: {exc}")
                continue
            self._mapper.learn_position(position, to_id(get_ci(row, "MarketId")))
            for child in attached_orders(row):
                self._mapper.learn_parent(str(to_id(get_ci(child.raw, "OrderId"))), child.parent_id)
            out.append(position)
        return out

    async def fetch_position_status(self, position_id: str) -> PositionDTO | None:
        """None when StoneX no longer reports it open (the caller never closes on absence alone): an HTTP 404 or
        an explicit ``"OpenPosition": null``, both noted as an explicit not-found. A body that proves nothing, or a
        row that cannot be mapped, counts in ``unmapped_rows`` and raises (IMPLEMENTATION_PLAN 8.1)."""
        self._require_directory()
        try:
            body = await self._rest.order_open_position(position_id)
        except OrderNotAccessibleError:
            # A closed position's openposition read answers 403 / 80001 (live, 2026-11-14). It is no longer open,
            # but this is not the explicit not-found the DLQ may act on (no note_absent): the close itself comes
            # from the trade history (downtime catch-up), never from absence alone.
            return None
        if body is None or _names_none(body, "OpenPosition"):
            note_absent("POSITION", position_id)
            return None
        row = get_obj(body, "OpenPosition")
        if row is None:
            self._unmapped("orders", f"position {position_id}: by-id answer without an OpenPosition object")
            raise BrokerError(f"stonex position {position_id} unmappable: no OpenPosition object in the answer")
        client = self._account_for(to_id(get_ci(row, "TradingAccountId")))
        if client is None:
            # A row of an account the directory does not know cannot be mapped (as on the stream): count it.
            self._unmapped("orders", f"position {position_id}: unknown trading account")
            raise BrokerError(f"stonex position {position_id} unmappable: unknown trading account")
        await self._ensure_markets([to_id(get_ci(row, "MarketId"))])
        try:
            return map_open_position(row, account_id=client, lookup=self._lookup)
        except UnmappableError as exc:
            # Never None ("the broker has none"): the caller would drop the entity's parked row.
            self._unmapped("orders", f"position {position_id} unmappable: {exc}")
            raise BrokerError(f"stonex position {position_id} unmappable: {exc}") from None

    async def fetch_open_orders(self) -> list[OrderDTO]:
        """Active stop / limit orders (entries and if-done children). Children attached to an open
        position but missing from the active list are added from the position's StopOrder / LimitOrder."""
        directory = self._require_directory()
        positions: list[tuple[str, JsonObj]] = []
        actives: list[tuple[str, JsonObj]] = []
        for trading_id, client in directory.trading_ids:
            positions.extend((client, r) for r in await self._rest.open_positions(trading_id))
            actives.extend((client, r) for r in await self._rest.active_stop_limit_orders(trading_id))
        await self._ensure_markets(to_id(get_ci(r, "MarketId")) for _, r in [*positions, *actives])
        return self._map_open_orders(positions, actives)

    async def fetch_active_stop_orders(self) -> ActiveStopOrders:
        """Design PR9 (R11-M20): one list read per trading account, ``GET order/activestoplimitorders`` (inside
        ``rest_get_only``, on top of ``ReadOnlyTransport``'s deny-list), never one call per handle. Mapped like the
        active rows of ``fetch_open_orders`` (typed STOP / LIMIT, if-done children linked); a row that cannot be
        mapped counts in ``unmapped_rows``. Children attached to an open position but absent from the active list
        are not listed (they are no handle this read can prove); nothing absent is ever closed. A market the map
        does not know yet is loaded as in ``fetch_open_orders`` (the read-only ``market/information`` lookup,
        rate-limited, at reconcile class)."""
        directory = self._require_directory()
        actives: list[tuple[str, JsonObj]] = []
        with rest_get_only():
            for trading_id, client in directory.trading_ids:
                actives.extend((client, r) for r in await self._rest.active_stop_limit_orders(trading_id))
        await self._ensure_markets(to_id(get_ci(r, "MarketId")) for _, r in actives)
        return ActiveStopOrders(adapter_id=self.adapter_id, orders=tuple(self._map_open_orders([], actives)))

    async def fetch_attached_stop_ids(self) -> list[tuple[str, str]]:
        """PR9 evidence for ``probe_bars --check-stops`` (plan S39 review): ``(child order id, position id)`` for every
        StopOrder child attached to an ``order/openpositions`` row, read on its own (GET only, ``rest_get_only``, on
        top of ``ReadOnlyTransport``'s deny-list), so the check can compare a source independent of
        ``activestoplimitorders`` with that list (``fetch_open_orders`` merges the two). Ids only: no level, no
        quantity. Nothing is mapped, learned or closed."""
        directory = self._require_directory()
        rows: list[JsonObj] = []
        with rest_get_only():
            for trading_id, _client in directory.trading_ids:
                rows.extend(await self._rest.open_positions(trading_id))
        out: list[tuple[str, str]] = []
        for row in rows:
            for child in attached_orders(row):
                child_id = to_id(get_ci(child.raw, "OrderId"))
                if child_id is not None and child.order_type is StoneXOrderType.STOP:
                    out.append((child_id, child.parent_id))
        return out

    def _map_open_orders(self, positions: Sequence[tuple[str, JsonObj]],
                         actives: Sequence[tuple[str, JsonObj]]) -> list[OrderDTO]:
        nested = [(client, child, False) for client, row in positions for child in attached_orders(row)]
        nested += [(client, child, True) for client, row in actives for child in attached_orders(row)]
        for _client, child, _entry in nested:
            self._mapper.learn_parent(str(to_id(get_ci(child.raw, "OrderId"))), child.parent_id)
        entries = self._working_entries(actives)

        out: list[OrderDTO] = []
        seen: set[str] = set()
        for client, row in actives:
            order_id = to_id(get_ci(row, "OrderId"))
            if order_id is None:
                self._unmapped("orders", "active order without OrderId skipped")
                continue
            explicit = parent_of(row)
            self._mapper.learn_parent(order_id, explicit)
            parent = self._mapper.parent_of(order_id)
            # A child of a working entry listed as its own row (live review 2026-11-24: no Status or time of its
            # own) is Pending at the entry row's broker time, as for a nested child below.
            entry_ts = entries.get(parent) if parent is not None else None
            try:
                dto = map_stop_limit_order(row, account_id=client, lookup=self._lookup, parent_id=parent,
                                           default_status=OrderStatus.PENDING if entry_ts is not None else None,
                                           default_ts=entry_ts or 0)
            except UnmappableError as exc:
                self._unmapped("orders", f"active order skipped: {exc}")
                continue
            self._mapper.learn_order(dto, to_id(get_ci(row, "MarketId")))
            seen.add(order_id)
            out.append(dto)
        for client, child, of_entry in nested:
            order_id = to_id(get_ci(child.raw, "OrderId"))
            if order_id is None or order_id in seen:
                continue
            # A child of a working (not yet filled) entry is Pending on the stream and its nested row has no status
            # or time of its own: it takes Pending and the entry row's broker time, so a snapshot agrees with the
            # stream (and heals a child the stream committed unlinked) instead of an untimed WORKING. Whatever the
            # nesting (IfDone, or directly StopOrder / LimitOrder: live review 2026-11-24); a position's children,
            # and those of an active row that is itself a child, stay WORKING.
            bracket = of_entry and child.parent_id in entries
            try:
                dto = map_stop_limit_order(child.raw, account_id=client, lookup=self._lookup,
                                           parent_id=child.parent_id, default_side=child_side(child.parent_side),
                                           default_qty=child.parent_qty, default_market_id=child.parent_market_id,
                                           order_kind=order_kind_of(child.order_type),
                                           default_status=OrderStatus.PENDING if bracket else None,
                                           default_ts=child.parent_ts if bracket else 0)
            except UnmappableError as exc:
                self._unmapped("orders", f"attached order skipped: {exc}")
                continue
            self._mapper.learn_order(dto, child.parent_market_id)
            seen.add(order_id)
            out.append(dto)
        return out

    def _working_entries(self, actives: Sequence[tuple[str, JsonObj]]) -> dict[str, int]:
        """``{entry OrderId: its row's LastChangedDateTimeUTC (0 when untimed)}`` for every active row that is a
        working entry: no ParentOrderId and no parent learnt (call after the nested children's ``learn_parent``,
        so a row that is itself someone's nested child is never an entry), and no terminal Status stated. Rows on
        activestoplimitorders are unfilled by definition; the Status check is only a guard."""
        entries: dict[str, int] = {}
        for _client, row in actives:
            order_id = to_id(get_ci(row, "OrderId"))
            if order_id is None or parent_of(row) is not None or self._mapper.parent_of(order_id) is not None:
                continue
            code = parse_status(get_ci(row, "Status"))
            if code is not None and canonical_status(code).terminal:
                continue
            entries[order_id] = rest_ts(row)
        return entries

    async def fetch_order_status(self, broker_order_id: str) -> OrderDTO | None:
        """GET order/{id} (active orders only); None when StoneX no longer lists it: an HTTP 404 or an answer whose
        StopLimitOrder and TradeOrder are both explicitly null, noted as an explicit not-found. A body that proves
        nothing, or a row that cannot be mapped, counts in ``unmapped_rows`` and raises (IMPLEMENTATION_PLAN 8.1)."""
        self._require_directory()
        body = await self._rest.order(broker_order_id)
        if body is None or _names_none(body, "StopLimitOrder", "TradeOrder"):
            note_absent("ORDER", broker_order_id)
            return None
        stop_limit = get_obj(body, "StopLimitOrder")
        trade = get_obj(body, "TradeOrder")
        raw = stop_limit or trade
        if raw is None:
            self._unmapped("orders", f"order {broker_order_id}: by-id answer without a StopLimitOrder / TradeOrder")
            raise BrokerError(f"stonex order {broker_order_id} unmappable: no StopLimitOrder / TradeOrder object")
        client = self._account_for(to_id(get_ci(raw, "TradingAccountId")))
        if client is None:
            # A row of an account the directory does not know cannot be mapped (as on the stream): count it.
            self._unmapped("orders", f"order {broker_order_id}: unknown trading account")
            raise BrokerError(f"stonex order {broker_order_id} unmappable: unknown trading account")
        await self._ensure_markets([to_id(get_ci(raw, "MarketId"))])
        try:
            return await self._map_order_status(broker_order_id, client, stop_limit, trade)
        except UnmappableError as exc:
            # Never None ("the broker has none"): the caller would drop the entity's parked row.
            self._unmapped("orders", f"order {broker_order_id} unmappable: {exc}")
            raise BrokerError(f"stonex order {broker_order_id} unmappable: {exc}") from None

    async def _map_order_status(self, broker_order_id: str, client: str, stop_limit: JsonObj | None,
                                trade: JsonObj | None) -> OrderDTO:
        if stop_limit is not None:
            parent = parent_of(stop_limit) or self._mapper.parent_of(broker_order_id)
            if parent is None:
                active = await self._rest.order_active_stop_limit(broker_order_id)
                parent = parent_of(active) if active is not None else None
            if parent is not None:
                self._mapper.learn_parent(broker_order_id, parent)
            return map_stop_limit_order(stop_limit, account_id=client, lookup=self._lookup, parent_id=parent,
                                        strict_status=True)
        assert trade is not None
        return map_trade_order(trade, account_id=client, lookup=self._lookup,
                               reduce_only=self._mapper.is_closing(broker_order_id), linked_position_id=None)

    async def fetch_account_snapshot(self) -> tuple[list[AccountDTO], list[AccountLive]]:
        directory = self._require_directory()
        accounts: list[AccountDTO] = []
        lives: list[AccountLive] = []
        for client in directory.client_ids:
            raw = await self._rest.client_account_margin(client)
            mapped = None if raw is None else map_margin(
                raw, account_id=client, currency_fallback=directory.currency_of(client),
                source="REST", received_ms=self._wall_ms(), adapter_id=self.adapter_id)
            if mapped is None:
                self._unmapped("account", f"margin of account {client} without Cash / currency")
                raise BrokerError(f"stonex margin for account {client} has no Cash / currency")
            accounts.append(mapped[0])
            lives.append(mapped[1])
        return accounts, lives

    async def fetch_cash_flows(self, since_ms: int) -> list[CashFlowDTO]:
        """TRADE_PNL of closing deals only: StoneX exposes no transfer history in this API, so
        deposits / withdrawals are unverified on StoneX. A history page that hit ``maxResults`` (older deals of
        the window may be missing) or a closing row that could not be mapped makes the result a partial
        ``CashFlowBatch`` (never one that looks complete; IMPLEMENTATION_PLAN 7.1)."""
        directory = self._require_directory()
        flows: dict[str, CashFlowDTO] = {}
        partial: list[str] = []
        for trading_id, client in directory.trading_ids:
            rows = await self._rest.trade_history(trading_id, max(since_ms, 0) // 1000, TRADE_HISTORY_MAX)
            if len(rows) >= TRADE_HISTORY_MAX:
                log.warning("stonex trade history for %s hit maxResults=%d; older deals may be missing",
                            trading_id, TRADE_HISTORY_MAX)
                partial.append(f"trade history of {trading_id} hit maxResults={TRADE_HISTORY_MAX}")
            mapped, skipped = map_trade_history_checked(rows, account_id=client, since_ms=since_ms,
                                                        adapter_id=self.adapter_id)
            for why in skipped:
                log.error("stonex: trade history of %s: %s", trading_id, why)
                partial.append(f"trade history of {trading_id}: {why}")
            for flow in mapped:
                flows[flow.transaction_id] = flow
        return CashFlowBatch(sorted(flows.values(), key=lambda f: (f.broker_ts, f.transaction_id)), partial)

    # --- history (off-time catch-up) ------------------------------------------------------------------
    @property
    def history_retention_ms(self) -> int | None:
        """StoneX does not document how far back order/tradehistory reaches: unknown."""
        return None

    async def fetch_history(self, since_ms: int, until_ms: int) -> BrokerHistory:
        """Every order / position StoneX changed in ``[since_ms, until_ms]`` (the catch-up after the engine was off).

        A stop / limit order yields its placement too (a WORKING version at ``CreatedDateTimeUtc``) before its
        terminal version, and its stop-loss / take-profit children are reduce-only and linked to their entry
        (``src.adapters.stonex.history`` module docstring: the parent rules confirmed live 2026-11-14).

        Reads, per trading account and at ``RestPriority.RECONCILE`` on the adapter's one REST budget: the trade
        history since ``since_ms`` (paged while pages come back full and oldest-first), the stop / limit order
        history when ``stop_limit_history`` is on (unconfirmed endpoint, default off), then the open positions
        and the active stop / limit orders (newest truth, read last). Rows the broker returns after
        ``until_ms`` are kept (they are newer; the version compare orders them). Of the working rows, only those
        changed in the window are listed: an untimed one (no time of its own nor its working entry's) is present
        state the snapshot before this catch-up committed, never a history row (live review 2026-11-24).

        Incomplete (``complete=False``) when: the stop / limit history is off (stop / limit orders placed and
        cancelled while offline cannot be listed), a page stays full, a row cannot be mapped (counted on this
        read's own channel, ``history_read``, never on the live-stream ``unmapped_rows``), or a position named by
        the history cannot be placed as open or closed. Retryable (``BrokerHistory.retryable``) unless every cause
        is a broker limit: the stop / limit history switch off, a page that stays full or cannot be paged, the
        unconfirmed paging rule. A failed request raises (the catch-up retries the read)."""
        directory = self._require_directory()
        notes = HistoryNotes()
        covered = max(since_ms, 0)
        trades: list[tuple[str, JsonObj]] = []
        stop_limits: list[tuple[str, JsonObj]] = []
        positions: list[tuple[str, JsonObj]] = []
        actives: list[tuple[str, JsonObj]] = []
        with rest_priority(RestPriority.RECONCILE):
            for trading_id, client in directory.trading_ids:
                rows, start = await self._trade_history_since(trading_id, since_ms, notes)
                covered = max(covered, start)
                trades.extend((client, r) for r in rows)
                if self._stop_limit_history:
                    rows, start = await self._stop_limit_history_since(trading_id, since_ms, notes)
                    covered = max(covered, start)
                    stop_limits.extend((client, r) for r in rows)
            if not self._stop_limit_history:
                notes.limit("stop/limit orders cancelled while offline cannot be listed "
                            "(StoneX stop/limit order history is off)")
            for trading_id, client in directory.trading_ids:
                positions.extend((client, r) for r in await self._rest.open_positions(trading_id))
                actives.extend((client, r) for r in await self._rest.active_stop_limit_orders(trading_id))
            await self._ensure_markets([*market_ids(trades), *market_ids(stop_limits),
                                        *market_ids(positions), *market_ids(actives)])
        # Rows skipped while mapping count on this read (`history_read`), never on the live-stream counters.
        with history_read() as read:
            mapped = map_trade_history_window(trades, positions, since_ms=since_ms, lookup=self._lookup,
                                              adapter_id=self.adapter_id, known=self._mapper.known_position)
            map_stop_limit_history(stop_limits, since_ms=since_ms, until_ms=max(until_ms, since_ms),
                                   lookup=self._lookup, known_parent=self._mapper.parent_of,
                                   adapter_id=self.adapter_id, out=mapped, trade_rows=trades)
            # Working now and changed in the window. An untimed working row is present state, not history: the
            # snapshot this catch-up follows committed it (the orchestrator refuses an untimed history row in a
            # broker-time domain, operations.md §17 step 3), and the by-id re-read covers the rest. Always mapped,
            # so its parent and market are still learnt.
            untimed: list[str] = []
            for dto in self._map_open_orders(positions, actives):
                if dto.broker_updated_at_ms <= 0:
                    untimed.append(dto.broker_order_id)
                elif dto.broker_updated_at_ms >= since_ms:
                    mapped.add_order(dto)
            if untimed:
                log.info("stonex history: %d untimed working order(s) left to the snapshot: %s", len(untimed),
                         ",".join(untimed[:20]))
            for why in mapped.unmapped:
                self._unmapped("orders", f"history: {why}")
        if read.unmapped > 0:                 # history rows + open rows of the window: retryable (this side)
            notes.append(f"{read.unmapped} history row(s) could not be mapped")
        for why in [*mapped.notes, *mapped.limits]:
            log.warning("stonex history: %s", why)
        if mapped.unlinked:                   # stand-alone orders, or children the history cannot link (ids only)
            log.info("stonex history: %d stop/limit order(s) without a provable parent: %s", len(mapped.unlinked),
                     ",".join(mapped.unlinked[:20]))
        notes.extend(mapped.notes)            # evidence that did not add up between the reads: retryable
        for why in mapped.limits:             # the rows themselves lack it: every re-read repeats (broker limit)
            notes.limit(why)
        return BrokerHistory(
            adapter_id=self.adapter_id, since_ms=since_ms, until_ms=until_ms, covered_from_ms=covered,
            orders=tuple(mapped.order_versions()),        # every version, oldest first per order (placement first)
            positions=tuple(sorted(mapped.positions.values(), key=lambda p: (p.broker_updated_at_ms, p.position_id))),
            complete=not notes, notes=notes.unique(), retryable=notes.retryable, unmapped=read.unmapped)

    async def _trade_history_since(self, trading_id: str, since_ms: int,
                                   notes: HistoryNotes) -> tuple[list[JsonObj], int]:
        """order/tradehistory from ``since_ms`` (see _paged_history)."""
        return await self._paged_history(f"trade history of {trading_id}", since_ms, notes,
                                         lambda from_s, size: self._rest.trade_history(trading_id, from_s, size))

    async def _stop_limit_history_since(self, trading_id: str, since_ms: int,
                                        notes: HistoryNotes) -> tuple[list[JsonObj], int]:
        """order/stoplimitorderhistory from ``since_ms`` (see _paged_history): stop / limit orders placed and
        finished while the engine was off (cancelled, rejected, triggered)."""
        return await self._paged_history(
            f"stop/limit order history of {trading_id}", since_ms, notes,
            lambda from_s, size: self._rest.stop_limit_order_history(trading_id, from_s, size))

    async def _paged_history(self, label: str, since_ms: int, notes: HistoryNotes,
                             read: Callable[[int, int], Awaitable[list[JsonObj]]]) -> tuple[list[JsonObj], int]:
        """Page a StoneX history read from ``since_ms`` (``from`` in whole seconds) until a page is not full.
        A full page is followed only when it is oldest-first (the next page starts at its last row's second;
        overlapping rows are deduplicated). A full newest-first page covers only its oldest row onward; a full
        page that cannot be ordered, or the page limit, leaves the window incomplete. Returns the rows and the
        earliest time they are known to cover. A full newest-first page whose oldest row is at or before
        ``since_ms`` covers the window. Until ``history_paging_confirmed``, following a page leaves the answer
        incomplete. Both StoneX reads were confirmed on the live account (2026-11-14): ``from`` filters by last
        change and a capped answer is oldest-first."""
        size = self._history_page_size
        from_s = max(since_ms, 0) // 1000
        rows: list[JsonObj] = []
        seen: set[tuple[str | None, int | None]] = set()
        for page_no in range(self._history_max_pages):
            page = await read(from_s, size)
            for row in page:
                key = row_key(row)
                if key not in seen:
                    seen.add(key)
                    rows.append(row)
            if len(page) < size:
                return rows, since_ms
            stamps = [row_time_ms(r) for r in page]
            if any(s is None for s in stamps):
                notes.limit(f"{label}: full page of {size} with untimed rows (cannot page)")
                return rows, since_ms
            times = [s for s in stamps if s is not None]
            if times == sorted(times) and times[-1] // 1000 > from_s:
                if not self._history_paging_confirmed and page_no == 0:
                    notes.limit(f"{label}: more than {size} rows, paged by time while the "
                                "'from' filter time is unconfirmed; rows may fall between pages")
                from_s = times[-1] // 1000
                continue
            if times[0] > times[-1] and times == sorted(times, reverse=True):
                if times[-1] <= since_ms:
                    return rows, since_ms      # newest-first back past the window start: the window is covered
                notes.limit(f"{label}: full newest-first page of {size}; rows before {times[-1]} may be missing")
                return rows, times[-1]
            notes.limit(f"{label}: full page of {size} could not be paged further")
            return rows, since_ms
        notes.limit(f"{label}: still full after {self._history_max_pages} pages")
        return rows, since_ms

    def _unmapped(self, stream: StreamName, why: str) -> None:
        note_unmapped(self.unmapped_rows, self.adapter_id, stream, why)

    # --- streaming plumbing --------------------------------------------------------------------
    def _is_connected(self) -> bool:
        return self._transport is not None and self._ls_status.startswith("CONNECTED")

    def _channel_snapshot(self) -> list[FrameChannel]:
        with self._channels_lock:
            return list(self._channels.values())

    def _on_ls_status(self, gen: int, status: str) -> None:
        """Library thread: a stalled or lost socket ends every subscription on it (§5 Liveness)."""
        if gen != self._ls_gen:
            return
        previous, self._ls_status = self._ls_status, status
        down = ("STALLED", "DISCONNECTED")
        if status.startswith(down) and not previous.startswith(down):
            for ch in self._channel_snapshot():
                ch.on_event(StreamEventKind.DISCONNECTED, status)

    def _on_ls_server_error(self, gen: int, code: int, message: str) -> None:
        """Code 1 (user / password check failed) or a metadata-adapter code <= 0 means the session
        token was refused (to confirm with StoneX); anything else is a lost socket."""
        if gen != self._ls_gen:
            return
        kind = StreamEventKind.AUTH_LOST if code == 1 or code <= 0 else StreamEventKind.DISCONNECTED
        for ch in self._channel_snapshot():
            ch.on_event(kind, f"server error {code}: {message}")

    async def _ensure_transport(self) -> StreamTransport:
        """Single-flight: the orders, account and prices pumps start together, and all of them
        must share one Lightstreamer session. Concurrent callers await the same connect; a caller
        being cancelled never cancels the connect the others are waiting on."""
        if self._transport is not None:
            return self._transport
        task = self._connecting
        if task is None:
            self._require_directory()
            token = self._rest.session_token
            if token is None:
                raise SessionExpiredError("StoneX adapter is not connected")
            self._check_lease()
            self._ls_gen += 1
            self._ls_status = "CONNECTING"
            task = asyncio.get_running_loop().create_task(self._connect_transport(self._ls_gen, token))
            task.add_done_callback(_consume_result)
            self._connecting = task
        transport = await asyncio.shield(task)
        # close() / re-auth may run between the connect finishing and this waiter resuming.
        return self._require_current(transport)

    def _require_current(self, transport: StreamTransport) -> StreamTransport:
        """Subscribe only on the current session: a superseded one was (or is being) disconnected,
        its channels would never hear about it, and heartbeats would follow the new session."""
        if transport is not self._transport:
            raise TransientBrokerError("StoneX Lightstreamer session was replaced; resubscribe")
        return transport

    async def _connect_transport(self, gen: int, token: str) -> StreamTransport:
        transport = self._transport_factory()
        try:
            await asyncio.to_thread(transport.connect, self._settings.username, token,
                                    lambda s: self._on_ls_status(gen, s),
                                    lambda c, m: self._on_ls_server_error(gen, c, m))
        except BaseException:
            if self._connecting is asyncio.current_task():
                self._connecting = None
                self._ls_status = "DISCONNECTED"
            await _disconnect_quietly(transport)
            raise
        if gen != self._ls_gen:
            # Stopped (close / re-auth) while connecting: never install a superseded session.
            await _disconnect_quietly(transport)
            raise TransientBrokerError("StoneX Lightstreamer session superseded while connecting")
        self._connecting = None
        self._transport = transport
        return transport

    async def _stop_transport(self, reason: str) -> None:
        transport, self._transport = self._transport, None
        self._connecting = None   # an in-flight connect sees the new generation and disconnects itself
        self._ls_gen += 1
        self._ls_status = "DISCONNECTED"
        self._price_state = None
        if transport is None:
            return
        for ch in self._channel_snapshot():
            ch.put_event(StreamEventKind.DISCONNECTED, reason)
        try:
            await asyncio.to_thread(transport.disconnect)
        except Exception:  # noqa: BLE001 - shutting down regardless
            log.exception("stonex: Lightstreamer disconnect failed")

    def _open_channel(self, stream: StreamName, *, drop_when_full: bool) -> FrameChannel:
        """One consumer per stream: a new call replaces (ends) the previous consumer. The price channel stamps
        each frame's receipt time on the Lightstreamer thread and counts its drops per market (PR2, PR6)."""
        prices = stream == "prices"
        ch = FrameChannel(stream, asyncio.get_running_loop(), maxsize=self._queue_size,
                          drop_when_full=drop_when_full, heartbeat_s=self._heartbeat_s,
                          is_connected=self._is_connected, clock_ms=self._wall_ms, stamp_receipt=prices,
                          drops=self._price_drops if prices else None)
        with self._channels_lock:
            old = self._channels.get(stream)
            self._channels[stream] = ch
        if old is not None:
            old.close()
        return ch

    def _release_channel(self, ch: FrameChannel, transport: StreamTransport,
                         handles: Iterable[object]) -> None:
        """Unsubscribe on the transport the handles were created on. A transport that is no longer
        current was already disconnected, and its subscriptions ended with it."""
        ch.close()
        with self._channels_lock:
            if self._channels.get(ch.stream) is ch:
                del self._channels[ch.stream]
        if transport is not self._transport:
            return
        for handle in handles:
            try:
                transport.unsubscribe(handle)
            except Exception:  # noqa: BLE001
                log.exception("stonex: unsubscribe failed")

    # --- streams ---------------------------------------------------------------------------------
    async def stream_order_updates(self) -> AsyncIterator[OrderStreamItem]:
        """ORDERS (orders and the positions derived from them). Consume this or stream_positions(),
        not both: they share the one ORDERS subscription."""
        directory = self._require_directory()
        transport = await self._ensure_transport()
        ch = self._open_channel("orders", drop_when_full=False)
        handles: list[object] = []
        held: dict[str, list[OrderItem]] = {}       # order id -> its frames waiting for the parent read
        lookups: set[asyncio.Task[None]] = set()
        holding = _Holding(ch, held, lookups)
        try:
            for client in directory.client_ids:
                handles.append(transport.subscribe("ORDERS", [f"ID.{client}"], ORDER_FIELDS,
                                                   BoundSink(ch, "_client", client), mode="DISTINCT"))
            while True:
                msg = await ch.get()
                if msg is None:
                    return
                if isinstance(msg, StreamEvent):
                    yield msg
                    continue
                done = msg.get(_LOOKUP_DONE)
                if done is not None:
                    # A finished read is acted on only behind the broker frames already queued when it finished
                    # (the channel hands local frames over first): an entry's frame still in the queue must be
                    # registered as a bracket-parent candidate before its child's read releases the child.
                    holding.behind.append([done, ch.qsize()])
                else:
                    for out in await self._map_order_frame(msg, holding):
                        yield out
                    for entry in holding.behind:
                        entry[1] = int(entry[1]) - 1
                while holding.behind and int(holding.behind[0][1]) <= 0:
                    finished = str(holding.behind.pop(0)[0])
                    holding.reading.discard(finished)
                    for out in self._finish_held(finished, holding):
                        yield out
        finally:
            for task in lookups:
                task.cancel()
            self._release_channel(ch, transport, handles)

    async def _map_order_frame(self, frame: Frame, holding: _Holding | None = None) -> list[OrderStreamItem]:
        """Map one ORDERS frame. Only the rate-limited missing-instrument refresh is awaited here. A stop /
        limit order whose parent relation is unknown is read in the background: its frames are held, in
        order, until the read finishes or times out, so the stream itself never waits on REST."""
        item = OrderItem.from_frame(frame)
        if item is None:
            self._unmapped("orders", "ORDERS frame without OrderId")
            return [StreamEvent(StreamEventKind.GAP, "orders", "ORDERS frame without OrderId", self._wall_ms())]
        self._mapper.note_frame(item)        # a held entry is a bracket-parent candidate from its first frame
        out: list[OrderStreamItem] = []
        try:
            await self._ensure_markets([item.market_id])
        except SessionExpiredError as exc:
            out.append(StreamEvent(StreamEventKind.AUTH_LOST, "orders", str(exc), self._wall_ms()))
        except BrokerError as exc:
            log.warning("stonex: instrument refresh for order %s failed: %s", item.order_id, exc)
        if holding is not None:
            frames = holding.held.get(item.order_id)
            if frames is not None:
                frames.append(item)          # behind the frames already waiting for the parent read
                return out
            relation = self._mapper.needs_parent_lookup(item) or self._mapper.needs_side_lookup(item)
            entry = self._mapper.needs_entry_lookup(item)     # K8: a first fill's entry price comes from REST
            if (relation or entry) and len(holding.held) < MAX_DEFERRED_ORDERS:
                if relation:
                    self._mapper.mark_looked_up(item.order_id)
                if entry:
                    self._mapper.mark_entry_looked_up(item.order_id)
                holding.held[item.order_id] = [item]
                holding.reading.add(item.order_id)
                task = asyncio.get_running_loop().create_task(
                    self._lookup_parent(item.order_id, holding.ch, relation=relation, entry=entry))
                holding.lookups.add(task)
                task.add_done_callback(holding.lookups.discard)
                return out
            wait_for = self._held_parent(item, holding)
            if wait_for is not None:
                holding.held[item.order_id] = [item]
                holding.block(item.order_id, wait_for)
                return out
        out.extend(self._map_items([item]))
        return out

    def _held_parent(self, item: OrderItem, holding: _Holding) -> str | None:
        """The held order this order's frames must wait for: its known parent while that parent's frames are held,
        else (a Pending stop / limit with no known parent) any bracket-parent candidate of its key whose own read is
        still running. A child is then mapped once, after its parent, and linked, never before it and unlinked
        (live finding 2026-11-20)."""
        parent = self._mapper.parent_of(item.order_id)
        if parent is not None:
            return parent if parent in holding.held and parent not in holding.blocked else None
        for candidate in self._mapper.bracket_candidates(item):
            if candidate in holding.reading:
                return candidate
        return None

    def _finish_held(self, order_id: str, holding: _Holding) -> list[OrderStreamItem]:
        """Map an order's held frames once nothing it waits for is held any more, then the orders that waited for
        it, in order."""
        frames = holding.held.get(order_id)
        if frames:
            wait_for = self._held_parent(frames[0], holding)
            if wait_for is not None and wait_for != order_id:
                holding.block(order_id, wait_for)
                return []
        outs = self._map_items(holding.held.pop(order_id, []))
        holding.blocked.pop(order_id, None)
        for waiter in holding.after.pop(order_id, []):
            holding.blocked.pop(waiter, None)
            outs.extend(self._finish_held(waiter, holding))
        return outs

    def _map_items(self, items: Iterable[OrderItem]) -> list[OrderStreamItem]:
        out: list[OrderStreamItem] = []
        for item in items:
            try:
                out.extend(self._mapper.map(item))
            except UnmappableError as exc:
                self._unmapped("orders", f"ORDERS item {item.order_id} unmappable: {exc}")
                out.append(StreamEvent(StreamEventKind.GAP, "orders", f"unmappable: {exc}", self._wall_ms()))
        return out

    async def _lookup_parent(self, order_id: str, ch: FrameChannel, *, relation: bool = True,
                             entry: bool = False) -> None:
        """Background: with ``relation``, GET order/{id}/activestoplimitorder for the if-done parent (and the side),
        then, when the side is still unknown, GET order/{id} and order/{id}/openposition. With ``entry`` (K8: the
        order's first fill emits PositionDTO(OPEN)), the entry price from GET order/{id}/openposition, else the
        TradeOrder of GET order/{id} (each body read at most once). A failed body read (403 / 80001, another
        BrokerError, a timeout) counts as "no body" and the next source is tried; only an expired session stops.
        Then release the order's held frames. A failure of the activestoplimitorder read releases them too (mapped
        by the stream heuristic: the stream price stands in for the entry; no side -> unmappable -> gap)."""
        timeout = self._parent_lookup_timeout_s
        bodies: dict[str, JsonObj | None] = {}

        async def read(kind: str) -> JsonObj | None:
            if kind not in bodies:
                call = self._rest.order if kind == "order" else self._rest.order_open_position
                try:
                    bodies[kind] = await asyncio.wait_for(call(order_id), timeout)
                except SessionExpiredError:
                    raise
                except (BrokerError, asyncio.TimeoutError) as exc:   # incl. OrderNotAccessibleError (closed)
                    log.info("stonex: %s read for order %s gave no body: %s", kind, order_id,
                             str(exc) or type(exc).__name__)
                    bodies[kind] = None
            return bodies[kind]

        try:
            with rest_priority(RestPriority.RECONCILE):
                if relation:
                    active = await asyncio.wait_for(self._rest.order_active_stop_limit(order_id), timeout)
                    if active is not None:
                        self._mapper.learn_parent(order_id, parent_of(active))
                        self._mapper.learn_side(order_id, parse_side(get_ci(active, "Direction")))
                        # A stop / limit entry's row nests its if-done children (IfDone): learn their parent from
                        # it, as a snapshot does; a pending entry's child has no active row of its own.
                        for child in attached_orders(active):
                            child_id = to_id(get_ci(child.raw, "OrderId"))
                            if child_id is not None:
                                self._mapper.learn_parent(child_id, child.parent_id)
                    if not self._mapper.side_known(order_id):
                        body = await read("order")
                        for key in ("TradeOrder", "StopLimitOrder"):
                            row = get_obj(body, key) if body is not None else None
                            if row is not None:
                                self._mapper.learn_side(order_id, parse_side(get_ci(row, "Direction")))
                    if not self._mapper.side_known(order_id):
                        body = await read("position")
                        row = get_obj(body, "OpenPosition") if body is not None else None
                        if row is not None:
                            self._mapper.learn_side(order_id, parse_side(get_ci(row, "Direction")))
                if entry and self._mapper.parent_of(order_id) is None:
                    body = await read("position")    # None when closed already (live 403 / 80001) or failed
                    row = get_obj(body, "OpenPosition") if body is not None else None
                    price = _entry_price(row, "Price", "OpenPrice") if row is not None else None
                    if row is not None:
                        self._mapper.learn_side(order_id, parse_side(get_ci(row, "Direction")))
                    if price is None:
                        body = await read("order")
                        trade = get_obj(body, "TradeOrder") if body is not None else None
                        if trade is not None:
                            price = _entry_price(trade, "Price", "ExecutionPrice", "OpenPrice")
                            self._mapper.learn_side(order_id, parse_side(get_ci(trade, "Direction")))
                    self._mapper.learn_entry(order_id, price)
        except SessionExpiredError as exc:
            ch.put_event(StreamEventKind.AUTH_LOST, str(exc))
        except Exception as exc:  # noqa: BLE001 - BrokerError, timeout, a malformed id: map by the heuristic
            log.warning("stonex: parent read for order %s failed: %s", order_id, str(exc) or type(exc).__name__)
        finally:
            ch.put_local({_LOOKUP_DONE: order_id})

    async def stream_account(self) -> AsyncIterator[AccountStreamItem]:
        """CLIENTACCOUNTMARGIN -> AccountDTO (only when balance / currency change) + AccountLive."""
        directory = self._require_directory()
        transport = await self._ensure_transport()
        ch = self._open_channel("account", drop_when_full=False)
        handles: list[object] = []
        self._account_last.clear()
        try:
            for client in directory.client_ids:
                handles.append(transport.subscribe("CLIENTACCOUNTMARGIN", [f"ID.{client}"], ACCOUNT_FIELDS,
                                                   BoundSink(ch, "_client", client), mode="MERGE"))
            while True:
                msg = await ch.get()
                if msg is None:
                    return
                if isinstance(msg, StreamEvent):
                    if msg.kind == StreamEventKind.SUBSCRIBED:
                        self._account_last.clear()   # re-emit state after every (re)subscribe
                    yield msg
                    continue
                for out in self._map_account_frame(directory, msg):
                    yield out
        finally:
            self._release_channel(ch, transport, handles)

    def _map_account_frame(self, directory: AccountDirectory, frame: Frame) -> list[AccountStreamItem]:
        client = frame.get("_client")
        if not client:
            return []
        last = self._account_last.get(client)
        mapped = map_margin(frame, account_id=client,
                            currency_fallback=directory.currency_of(client) or (last.currency if last else None),
                            source="STREAM", received_ms=self._wall_ms(), adapter_id=self.adapter_id)
        if mapped is None:
            self._unmapped("account", f"CLIENTACCOUNTMARGIN frame of {client} without Cash / currency")
            return [StreamEvent(StreamEventKind.GAP, "account", "CLIENTACCOUNTMARGIN frame without Cash / currency",
                                self._wall_ms())]
        account, live = mapped
        out: list[AccountStreamItem] = []
        if last is None or last.checksum != account.checksum:
            self._account_last[client] = account
            out.append(account)
        out.append(live)
        return out

    async def stream_ticks(self, symbols: Sequence[str]) -> AsyncIterator[PriceStreamItem]:
        """PRICES, one subscription per market (item ID.<MarketId>). Overflow drops price frames."""
        self._require_directory()
        transport = await self._ensure_transport()
        ch = self._open_channel("prices", drop_when_full=True)
        subs: dict[str, object] = {}
        self._price_state = (ch, subs, transport)
        try:
            markets = [*await self._markets_for(symbols), *sorted(self._wanted_markets)]
            self._require_current(transport)   # the REST lookups above may outlive the session
            for market_id in markets:
                self._subscribe_price(transport, ch, subs, market_id)
            if not markets:
                # Nothing to subscribe yet (no holdings before the first snapshot, no configured symbols): the
                # empty set is trivially subscribed. ensure_ticks() adds markets later. Without this the price
                # stream never reports SUBSCRIBED and SM4 closes the socket on its subscribe timeout forever.
                yield StreamEvent(StreamEventKind.SUBSCRIBED, "prices", "no markets yet", self._wall_ms())
            while True:
                msg = await ch.get()
                if msg is None:
                    return
                if isinstance(msg, StreamEvent):
                    yield self._market_event(msg)
                    continue
                spec = self._by_market.get(str(msg.get(MARKET_KEY)))
                price = map_price(msg, spec, self._receipt_ms(msg), self.adapter_id) if spec else None
                if price is not None:
                    yield price
        finally:
            if self._price_state is not None and self._price_state[0] is ch:
                self._price_state = None
            self._release_channel(ch, transport, list(subs.values()))

    def _receipt_ms(self, frame: Frame) -> int:
        """The receipt time the price channel stamped on the Lightstreamer thread (PR2); now when absent."""
        try:
            return int(frame.get(RECEIVED_KEY) or "")
        except ValueError:
            return self._wall_ms()

    def _market_event(self, event: StreamEvent) -> StreamEvent:
        """A per-market price subscription event names its canonical symbol (PR6); a MarketId without a known
        instrument keeps the raw MarketId (as ``dropped_price_frames`` does), so it is never read as a whole-stream
        event (``market=None``)."""
        if event.market is None:
            return event
        spec = self._by_market.get(event.market)
        return event if spec is None else dataclasses.replace(event, market=spec.symbol)

    def _subscribe_price(self, transport: StreamTransport, ch: FrameChannel, subs: dict[str, object],
                         market_id: str) -> None:
        self._wanted_markets.add(market_id)
        if market_id in subs:
            return
        subs[market_id] = transport.subscribe("PRICES", [f"ID.{market_id}"], PRICE_FIELDS,
                                              BoundSink(ch, MARKET_KEY, market_id, check_field="MarketId"),
                                              mode="MERGE")

    async def ensure_ticks(self, symbols: Sequence[str]) -> None:
        """Add symbols to the live PRICES subscription (read-only subscribe); idempotent."""
        self._require_directory()
        market_ids = await self._markets_for(symbols)
        state = self._price_state
        for market_id in market_ids:
            if state is not None and state[2] is self._transport:
                self._subscribe_price(state[2], state[0], state[1], market_id)
            else:
                self._wanted_markets.add(market_id)

    # --- broker price bars (design PR3; enabled in S39 after the PR3 probe) ---------------------------
    @property
    def price_bars_supported(self) -> bool:
        """StoneX serves 1m / 5m / 15m / 1h bars, bid and ask (``bars.INTERVALS``; 1m and 15m measured by PR3)."""
        return True

    async def fetch_price_bars(self, symbol: str, side: BarSide, timeframe_s: int, *, count: int | None = None,
                               from_ms: int | None = None, to_ms: int | None = None) -> PriceBarsResponse:
        """The broker's own bars of one series (``BrokerAdapter.fetch_price_bars``; ``md-bars`` and the PR3 probe):
        ``GET market/{id}/barhistory`` (count form) or ``barhistorybetween`` (range form), confirmed by the PR3
        probe (2026-11-17), GET only (``rest_get_only``) through this adapter's ``ReadOnlyTransport`` and REST budget
        at the caller's priority (``md-bars`` reads at ``BARS``). ``bucket_start_ms`` is ``BarDate``, the bucket
        start (PR3). StoneX bars name no side: ``BrokerBar.side`` stays None and the side is the request's
        (``PriceBarsResponse.side``, which the §A4 side check compares). A timeframe without a StoneX interval is
        answered "not supported" for that series. A range answer with ``RANGE_CAP`` bars or more is reported
        truncated (the cap is unconfirmed: PR3 saw no range answer truncated; counted before the range filter); so is a
        count answer with fewer complete bars than ``count`` (the forming bar is extra). The range form asks for whole
        seconds (``from_ms`` floored, ``to_ms`` ceiled) and answers only the bars whose bucket starts inside
        ``[from_ms, to_ms]``."""
        problem = bar_request_problem(side, timeframe_s, count, from_ms, to_ms)
        if problem is not None:
            raise ValueError(f"fetch_price_bars: {problem}")
        self._require_directory()
        sym = canonical(symbol)
        interval = interval_for(timeframe_s)
        if interval is None:
            return PriceBarsResponse.unsupported(self.adapter_id, sym, side, timeframe_s, count=count,
                                                 from_ms=from_ms, to_ms=to_ms,
                                                 note=f"no StoneX bar interval for {timeframe_s} s")
        market_id = await self._market_for_symbol(sym)
        if market_id is None:
            raise BrokerError(f"stonex: no market found for {sym} (no bars)")
        with rest_get_only():
            if count is not None:
                body = await self._rest.price_bar_history(market_id, interval[0], interval[1], count, side)
            else:
                assert from_ms is not None and to_ms is not None
                body = await self._rest.price_bar_history_between(
                    market_id, interval[0], interval[1], from_ms // 1000, math.ceil(to_ms / 1000), side)
        received = self._wall_ms()
        bars, names = decode_bar_history(body)
        complete = sum(1 for b in bars if not b.partial)
        if count is None:
            # The range GET is in whole seconds (from floored, to ceiled), so a bucket starting in the widened part
            # can come back: keep only what was asked for (`[from_ms, to_ms]`, §A4 check 2), so a healthy series
            # never fails GRID_RANGE for it; the forming bar, when in range, is left to `is_forming` (S39 review).
            assert from_ms is not None and to_ms is not None
            bars = [b for b in bars if from_ms <= b.bucket_start_ms <= to_ms]
        return PriceBarsResponse(adapter_id=self.adapter_id, symbol=sym, side=side, timeframe_s=timeframe_s,
                                 bars=tuple(bars),
                                 truncated=complete >= RANGE_CAP if count is None else complete < count,
                                 received_ms=received, count=count, from_ms=from_ms, to_ms=to_ms,
                                 field_names=names)


class _Holding:
    """The ORDERS consumer's held frames (per order id), its background parent reads (``reading``: the ids whose
    read has not finished), and the held orders waiting for another held order (``blocked`` child -> the order it
    waits for, ``after`` that order -> its waiters, in order). A waiter is released when the order it waits for is
    mapped, which happens when that order's read finishes or times out, so no wait outlives the read timeout."""

    __slots__ = ("ch", "held", "lookups", "reading", "blocked", "after", "behind")

    def __init__(self, ch: FrameChannel, held: dict[str, list[OrderItem]], lookups: set[asyncio.Task[None]]) -> None:
        self.ch = ch
        self.held = held
        self.lookups = lookups
        self.reading: set[str] = set()
        self.blocked: dict[str, str] = {}
        self.after: dict[str, list[str]] = {}
        # finished reads not acted on yet: [order id, broker frames still queued ahead of it], in finishing order
        self.behind: list[list[str | int]] = []

    def block(self, order_id: str, wait_for: str) -> None:
        self.blocked[order_id] = wait_for
        waiters = self.after.setdefault(wait_for, [])
        if order_id not in waiters:
            waiters.append(order_id)


def _entry_price(row: JsonObj, *names: str) -> Decimal | None:
    """A REST entry price (K8): the first of ``names`` that is a positive decimal, else None."""
    for name in names:
        value = to_decimal(get_ci(row, name))
        if value is not None and value > 0:
            return value
    return None


def _names_none(body: JsonObj, *keys: str) -> bool:
    """True when the by-id answer explicitly says "no such row": every one of ``keys`` is present (matched
    case-insensitively) and null. A missing key, or a non-null value that is not a row, proves nothing."""
    lowered = {k.lower(): v for k, v in body.items() if isinstance(k, str)}
    return all(key.lower() in lowered and lowered[key.lower()] is None for key in keys)


def _consume_result(task: asyncio.Task[StreamTransport]) -> None:
    """Retrieve a connect failure even when every waiter was cancelled (no 'never retrieved' noise)."""
    if not task.cancelled():
        task.exception()


async def _disconnect_quietly(transport: StreamTransport) -> None:
    try:
        await asyncio.to_thread(transport.disconnect)
    except Exception:  # noqa: BLE001 - best effort on an abandoned session
        log.exception("stonex: Lightstreamer disconnect failed")
