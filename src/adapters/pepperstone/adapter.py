"""PepperstoneAdapter: read-only Pepperstone BrokerAdapter over the cTrader Open API
(JSON over WebSocket, 'accounts' OAuth scope only; adapter-isolation.md).

One socket per adapter (one ctidTraderAccountId). Requests and server events share it:
- reads (reconcile, trader, deal list, order details, cash-flow history, symbols, assets) are
  request / response pairs on the read-only payloadType allow-list (protocol.py);
- the reader task drains the socket continuously and fans events out to per-stream channels
  (orders, positions, account, prices); mapping happens in the consumers, never in the reader.
The orders, positions and account streams count as SUBSCRIBED from the account auth response until
AccountDisconnect, TokenInvalidated or ClientDisconnect (AUTH_LOST). A lost socket ends every
subscription (DISCONNECTED); when connect() re-establishes the session while a stream is still being
consumed, that stream gets GAP and is subscribed again.
No order-changing capability exists here. Never imports db / cache / pubsub / integrity / oversee.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from decimal import Decimal
from typing import Any

from src.adapters.base import (
    AccountStreamItem,
    AuthFailedError,
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
    covered_from,
    history_read,
    new_unmapped_rows,
    note_absent,
    note_unmapped,
    unmapped_mark,
)
from src.adapters.pepperstone.channel import EventChannel
from src.adapters.pepperstone.history import (
    closed_from_closing_deal,
    deal_filled,
    deal_time_ms,
    is_closing_deal,
    latest_by_id,
)
from src.adapters.pepperstone.instruments import (
    ADAPTER_ID,
    SymbolInfo,
    canonical_symbol,
    parse_assets,
    relative_price,
    spec_from_symbol,
)
from src.adapters.pepperstone.mapping import (
    DEFAULT_MONEY_DIGITS,
    EXECUTION_TYPE,
    UnmappableError,
    account_currency,
    account_from_balance,
    broker_ms,
    enum_value,
    is_protection_order,
    map_closing_deal,
    map_deposit_withdraw,
    map_order,
    map_position,
    map_trader,
    money,
    money_digits,
    obj,
    objs,
    position_from_deals,
)
from src.adapters.pepperstone.protocol import (
    ALREADY_DONE_CODES,
    CREDENTIAL_ERROR_CODES,
    NOT_FOUND_CODES,
    PT_ACCOUNT_AUTH_REQ,
    PT_ACCOUNT_DISCONNECT_EVENT,
    PT_ACCOUNTS_BY_TOKEN_REQ,
    PT_APPLICATION_AUTH_REQ,
    PT_ASSET_LIST_REQ,
    PT_CASH_FLOW_HISTORY_REQ,
    PT_CLIENT_DISCONNECT_EVENT,
    PT_DEAL_LIST_BY_POSITION_REQ,
    PT_DEAL_LIST_REQ,
    PT_EXECUTION_EVENT,
    PT_MARGIN_CHANGED_EVENT,
    PT_ORDER_DETAILS_REQ,
    PT_ORDER_ERROR_EVENT,
    PT_ORDER_LIST_REQ,
    PT_RECONCILE_REQ,
    PT_REFRESH_TOKEN_REQ,
    PT_SPOT_EVENT,
    PT_SUBSCRIBE_SPOTS_REQ,
    PT_SYMBOL_BY_ID_REQ,
    PT_SYMBOL_CHANGED_EVENT,
    PT_SYMBOLS_LIST_REQ,
    PT_TOKEN_INVALIDATED_EVENT,
    PT_TRADER_REQ,
    PT_TRADER_UPDATED_EVENT,
    PT_TRAILING_SL_CHANGED_EVENT,
    PT_UNSUBSCRIBE_SPOTS_REQ,
    PT_VERSION_REQ,
    SCOPE_VIEW,
    TOKEN_ERROR_CODES,
    Frame,
    JsonObj,
    scope_name,
)
from src.adapters.pepperstone.schedule import sessions_from_symbol
from src.adapters.pepperstone.session import OpenApiError, OpenApiSession, WsConnector, websockets_connector
from src.adapters.pepperstone.tokens import RefreshedTokens, TokenManager, TokenPair, TokenStore
from src.adapters.ratelimit import RestBudget, RestPriority, rest_priority
from src.adapters.stonex.wire import to_bool, to_decimal, to_id, to_int
from src.config import PepperstoneSettings
from src.dto import AccountDTO, AccountLive, CashFlowDTO, InstrumentSpec, OrderDTO, PositionDTO, PriceDTO, Source

log = logging.getLogger(__name__)

DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS                 # deal list / cash-flow history window limit (to confirm)
INSTRUMENT_REFRESH_MIN_S = 60.0
SYMBOL_BY_ID_CHUNK = 100
DEAL_LIST_MAX_ROWS = 1000
MAX_WINDOW_SPLITS = 8
HISTORY_MAX_WEEKS = 13               # fetch_history reads at most this many one-week windows back
SESSION_ENDING = (StreamEventKind.DISCONNECTED, StreamEventKind.AUTH_LOST)


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class PepperstoneAdapter(BrokerAdapter):
    """Read-only cTrader adapter for one ctidTraderAccountId. Run it on the leader only."""

    adapter_id = ADAPTER_ID
    VERSIONING: dict[EntityType, VersionDecl] = {
        "ORDER": VersionDecl(has_broker_seq=False, has_broker_time=True),      # utcLastUpdateTimestamp
        "POSITION": VersionDecl(has_broker_seq=False, has_broker_time=True),   # utcLastUpdateTimestamp
        "ACCOUNT": VersionDecl(has_broker_seq=True, has_broker_time=False),    # balanceVersion
    }

    def __init__(
        self,
        settings: PepperstoneSettings,
        *,
        connector: WsConnector | None = None,
        token_store: TokenStore | None = None,
        pre_connect_check: Callable[[], None] | None = None,
        symbols: Iterable[str] = (),
        allow_token_refresh: bool = True,
        refresh_margin_s: float = 3 * 86_400.0,
        queue_size: int = 10_000,
        heartbeat_s: float = 10.0,
        idle_timeout_s: float = 30.0,
        request_timeout_s: float = 15.0,
        historical_interval_s: float = 0.25,
        position_lookback_ms: int = WEEK_MS,
        monotonic: Callable[[], float] = time.monotonic,
        wall_ms: Callable[[], int] = now_ms,
        rest_budget: RestBudget | None = None,
    ) -> None:
        self._settings = settings
        self._connector: WsConnector = connector or websockets_connector
        self._pre_connect_check = pre_connect_check
        self._tokens = TokenManager(TokenPair(settings.access_token, settings.refresh_token, None, 0), token_store,
                                    lease_check=self._check_lease, wall_ms=wall_ms)
        # One request bucket per adapter, shared by every session it opens (operations.md §15).
        self._budget = rest_budget if rest_budget is not None else RestBudget(
            ADAPTER_ID, rate_per_s=settings.rest_rate_per_s, burst=settings.rest_burst)
        self._allow_refresh = allow_token_refresh
        self._refresh_margin_ms = int(refresh_margin_s * 1000)
        self._queue_size = queue_size
        self._heartbeat_s = heartbeat_s
        self._idle_timeout_s = idle_timeout_s
        self._request_timeout_s = request_timeout_s
        self._historical_interval_s = historical_interval_s
        self._position_lookback_ms = position_lookback_ms
        self._monotonic = monotonic
        self._wall_ms = wall_ms
        self._account = settings.account_id
        self.account_id = str(settings.account_id)

        self._session: OpenApiSession | None = None
        self._authorized = False
        self._closing = False
        self._sessions_opened = 0
        self.permission_scope: str | None = None
        self.is_live_account: bool | None = None

        self._assets: dict[int, str] = {}
        self._currency: str | None = None
        self._money_digits = DEFAULT_MONEY_DIGITS
        self._light: dict[str, JsonObj] = {}                  # canonical symbol -> ProtoOALightSymbol
        self._light_by_id: dict[int, JsonObj] = {}
        self._instruments: dict[str, InstrumentSpec] = {}     # by canonical symbol (read-only map)
        self._by_id: dict[int, SymbolInfo] = {}
        self._configured_symbols = {canonical_symbol(s) for s in symbols}
        self._last_on_demand: float | None = None
        self._refresh_lock = asyncio.Lock()

        self._channels: dict[str, EventChannel] = {}
        self._wanted_spots: set[int] = set()
        self._subscribed_spots: set[int] = set()
        self._used_margin: dict[str, Decimal] = {}
        self._positions_last: dict[str, PositionDTO] = {}
        self._watermark_ms: int | None = None
        self._background: set[asyncio.Task[None]] = set()
        #: Broker rows (stream or REST) that could not be mapped, per stream (orchestrator UNMAPPED_ATTR):
        #: any increase opens that stream's gap, so a reconcile pass that misses a row cannot clear the fence.
        self.unmapped_rows: dict[str, int] = new_unmapped_rows()

    # --- introspection (not part of BrokerAdapter) ------------------------------------------------
    @property
    def stream_watermark_ms(self) -> int | None:
        """Highest utcLastUpdateTimestamp seen on an order / position frame (SM4 watermark)."""
        return self._watermark_ms

    @property
    def token_generation(self) -> int:
        """Credential generation for adapter_auth_state (operations.md §13)."""
        return self._tokens.generation

    @property
    def currency(self) -> str | None:
        return self._currency

    @property
    def rest_budget(self) -> RestBudget:
        return self._budget

    @property
    def token_issued_at_ms(self) -> int | None:
        """When the token pair in use was saved (operations.md §13); None when unknown."""
        return self._tokens.issued_at_ms

    @property
    def token_expires_at_ms(self) -> int | None:
        return self._tokens.pair.expires_at_ms

    @property
    def session(self) -> OpenApiSession | None:
        return self._session

    def instrument_for_symbol_id(self, symbol_id: int) -> InstrumentSpec | None:
        info = self._by_id.get(symbol_id)
        return None if info is None else info.spec

    # --- session -----------------------------------------------------------------------------------
    def _check_lease(self) -> None:
        """operations.md §2: every connect, auth and token refresh first checks the lease deadline."""
        if self._pre_connect_check is not None:
            self._pre_connect_check()

    def _is_live(self) -> bool:
        session = self._session
        return (session is not None and session.is_open and self._authorized
                and self._monotonic() - session.last_inbound <= self._idle_timeout_s)

    async def connect(self) -> None:
        self._check_lease()
        if self._session is not None:
            await self._drop_session("re-authenticating")
        self._closing = False
        await self._tokens.load()
        session = OpenApiSession(self._settings.ws_url, self._connector, on_event=self._on_frame,
                                 on_closed=self._on_session_closed, heartbeat_s=self._heartbeat_s,
                                 idle_timeout_s=self._idle_timeout_s, request_timeout_s=self._request_timeout_s,
                                 historical_interval_s=self._historical_interval_s, monotonic=self._monotonic,
                                 budget=self._budget, on_dropped=self._on_frame_dropped)
        try:
            await session.open()
        except BrokerError:
            raise
        except (OSError, TimeoutError) as exc:
            raise TransientBrokerError(f"cTrader connect failed: {type(exc).__name__}") from exc
        except Exception as exc:  # noqa: BLE001 - websockets handshake errors
            raise TransientBrokerError(f"cTrader connect failed: {type(exc).__name__}") from exc
        self._session = session
        try:
            await self._authenticate(session)
            await self._load_account_basics()
        except BaseException:
            await self._drop_session("connect failed")
            raise
        self._sessions_opened += 1
        if self._sessions_opened > 1:
            await self._resume_channels()

    async def _authenticate(self, session: OpenApiSession) -> None:
        try:
            await session.request(PT_APPLICATION_AUTH_REQ, {"clientId": self._settings.client_id,
                                                            "clientSecret": self._settings.client_secret})
        except OpenApiError as exc:
            if exc.error_code not in ALREADY_DONE_CODES:
                raise self._auth_error(exc) from None
        if self._allow_refresh and self._tokens.pair.expires_within(self._wall_ms(), self._refresh_margin_ms):
            await self._refresh_tokens()
        refreshed = False
        while True:
            try:
                await self._check_scope(session)
                self._check_lease()
                await session.request(PT_ACCOUNT_AUTH_REQ, {"ctidTraderAccountId": self._account,
                                                            "accessToken": self._tokens.pair.access_token})
                break
            except OpenApiError as exc:
                if exc.error_code in ALREADY_DONE_CODES:
                    break
                if exc.error_code in TOKEN_ERROR_CODES and not refreshed and self._allow_refresh:
                    refreshed = True
                    await self._refresh_tokens()
                    continue
                if exc.error_code in TOKEN_ERROR_CODES:
                    raise AuthFailedError(f"cTrader access token refused after a fresh token "
                                          f"(generation {self._tokens.generation}): {exc.error_code}") from None
                raise self._auth_error(exc) from None
        self._authorized = True

    async def _check_scope(self, session: OpenApiSession) -> None:
        """Only the 'accounts' scope (SCOPE_VIEW); under live_readonly a missing scope is refused too."""
        res = await session.request(PT_ACCOUNTS_BY_TOKEN_REQ, {"accessToken": self._tokens.pair.access_token})
        scope = scope_name(res.payload.get("permissionScope"))
        self.permission_scope = scope
        if scope is not None and scope != SCOPE_VIEW:
            raise AuthFailedError(f"cTrader token scope {scope} refused: Phase 1 uses the 'accounts' scope only")
        if scope is None and self._settings.env == "live_readonly":
            raise AuthFailedError("cTrader token scope not reported; refused under live_readonly")
        accounts = {to_int(a.get("ctidTraderAccountId")): a for a in objs(res.payload, "ctidTraderAccount")}
        account = accounts.get(self._account)
        if account is None:
            raise AuthFailedError(f"cTrader account {self._account} is not reachable with this access token")
        is_live = to_bool(account.get("isLive"))
        self.is_live_account = bool(is_live)
        if self._settings.env == "live_readonly" and is_live is not True:
            raise AuthFailedError(f"cTrader account {self._account} is not a live account (BROKER_ENV=live_readonly)")
        if self._settings.env == "sandbox" and is_live is True:
            raise AuthFailedError(f"cTrader account {self._account} is live but BROKER_ENV=sandbox")

    def _auth_error(self, exc: OpenApiError) -> BrokerError:
        if exc.error_code in CREDENTIAL_ERROR_CODES:
            return AuthFailedError(f"cTrader credential error: {exc.error_code}")
        if exc.error_code in TOKEN_ERROR_CODES:
            return SessionExpiredError(f"cTrader token error: {exc.error_code}")
        return TransientBrokerError(str(exc))

    async def _refresh_tokens(self) -> None:
        if not self._allow_refresh:
            raise AuthFailedError("cTrader access token needs a refresh, which is disabled here")
        used = self._tokens.generation
        await self._tokens.refresh(self._refresh_at_broker, used_generation=used)

    async def _refresh_at_broker(self, refresh_token: str) -> RefreshedTokens:
        session = self._require_session_open()
        try:
            res = await session.request(PT_REFRESH_TOKEN_REQ, {"refreshToken": refresh_token})
        except OpenApiError as exc:
            if exc.error_code in ("REQUEST_FREQUENCY_EXCEEDED", "SERVER_IS_UNDER_MAINTENANCE", "TIMEOUT_ERROR"):
                raise TransientBrokerError(str(exc)) from None
            raise AuthFailedError(f"cTrader token refresh refused: {exc.error_code}") from None
        access = res.payload.get("accessToken")
        refresh = res.payload.get("refreshToken")
        expires_in = to_int(res.payload.get("expiresIn"))
        if not isinstance(access, str) or not access:
            raise AuthFailedError("cTrader token refresh returned no access token")
        return RefreshedTokens(access_token=access, refresh_token=refresh if isinstance(refresh, str) else "",
                               expires_at_ms=None if expires_in is None else self._wall_ms() + expires_in * 1000)

    async def _load_account_basics(self) -> None:
        self._assets = parse_assets(objs((await self._req(PT_ASSET_LIST_REQ, {})).payload, "asset"))
        trader = obj((await self._req(PT_TRADER_REQ, {})).payload, "trader")
        self._money_digits = money_digits(trader, DEFAULT_MONEY_DIGITS)
        currency = account_currency(trader, self._assets)
        if currency is None:
            raise BrokerError(f"cTrader account {self._account}: deposit asset unknown")
        self._currency = currency

    async def api_version(self) -> str | None:
        res = await self._session_request(PT_VERSION_REQ, {})
        version = res.payload.get("version")
        return None if version is None else str(version)

    async def close(self) -> None:
        self._closing = True
        for ch in list(self._channels.values()):
            ch.put_event(StreamEventKind.DISCONNECTED, "adapter closed")
            ch.close()
        self._channels.clear()
        await self._drop_session("adapter closed")
        for task in list(self._background):
            task.cancel()

    async def _drop_session(self, reason: str) -> None:
        session, self._session = self._session, None
        self._authorized = False
        self._subscribed_spots.clear()
        if session is not None:
            await session.close(reason)

    def _on_session_closed(self, reason: str) -> None:
        """Reader / heartbeat side: a lost socket ends every subscription on it."""
        self._authorized = False
        self._subscribed_spots.clear()
        if self._closing:
            return
        for ch in list(self._channels.values()):
            ch.clear_frames()
            ch.put_event(StreamEventKind.DISCONNECTED, reason)

    async def _resume_channels(self) -> None:
        """A new session while streams are still consumed: report the GAP, then subscribe again."""
        for ch in list(self._channels.values()):
            ch.put_event(StreamEventKind.GAP, "socket reconnected: events between the sessions may be missing")
            if ch.name != "prices":
                ch.put_event(StreamEventKind.SUBSCRIBED, "account re-authorized")
        prices = self._channels.get("prices")
        if prices is not None:
            try:
                await self._subscribe_spots(sorted(self._wanted_spots))
            except BrokerError as exc:
                prices.put_event(StreamEventKind.ERROR, f"spot re-subscribe failed: {exc}")
                return
            prices.put_event(StreamEventKind.SUBSCRIBED, "spots re-subscribed")

    def _require_session_open(self) -> OpenApiSession:
        session = self._session
        if session is None or not session.is_open:
            raise SessionExpiredError("cTrader adapter is not connected")
        return session

    def _require_authorized(self) -> OpenApiSession:
        session = self._require_session_open()
        if not self._authorized:
            raise SessionExpiredError("cTrader account is not authorized")
        return session

    async def _session_request(self, payload_type: int, payload: JsonObj) -> Frame:
        session = self._require_session_open()
        try:
            return await session.request(payload_type, payload)
        except OpenApiError as exc:
            if exc.error_code in TOKEN_ERROR_CODES or exc.error_code in ("CH_CLIENT_NOT_AUTHENTICATED",
                                                                       "ACCOUNT_NOT_AUTHORIZED"):
                raise SessionExpiredError(f"cTrader session refused: {exc.error_code}") from None
            raise

    async def _req(self, payload_type: int, payload: JsonObj) -> Frame:
        """An account-scoped read (adds ctidTraderAccountId)."""
        self._require_authorized()
        return await self._session_request(payload_type, {"ctidTraderAccountId": self._account, **payload})

    # --- instruments ---------------------------------------------------------------------------------
    def _lookup(self, symbol_id: int | None) -> SymbolInfo | None:
        return None if symbol_id is None else self._by_id.get(symbol_id)

    async def _load_light_symbols(self) -> None:
        res = await self._req(PT_SYMBOLS_LIST_REQ, {"includeArchivedSymbols": False})
        light: dict[str, JsonObj] = {}
        by_id: dict[int, JsonObj] = {}
        for row in objs(res.payload, "symbol"):
            symbol_id, name = to_int(row.get("symbolId")), row.get("symbolName")
            if symbol_id is None or not isinstance(name, str):
                continue
            by_id[symbol_id] = row
            if to_bool(row.get("enabled")) is not False:
                light.setdefault(canonical_symbol(name), row)
        self._light, self._light_by_id = light, by_id

    async def _load_symbols(self, ids: Iterable[int]) -> None:
        wanted = sorted({i for i in ids if i is not None})
        if not wanted:
            return
        if not self._light_by_id:
            await self._load_light_symbols()
        for i in range(0, len(wanted), SYMBOL_BY_ID_CHUNK):
            chunk = wanted[i:i + SYMBOL_BY_ID_CHUNK]
            res = await self._req(PT_SYMBOL_BY_ID_REQ, {"symbolId": chunk})
            for full in [*objs(res.payload, "symbol"), *objs(res.payload, "archivedSymbol")]:
                symbol_id = to_int(full.get("symbolId"))
                info = spec_from_symbol(self._light_by_id.get(symbol_id or -1), full, self._assets, self.adapter_id)
                if info is None:
                    log.warning("ctrader: symbol %s could not be mapped (left instrument_unknown)", symbol_id)
                    continue
                self._by_id[info.symbol_id] = info
                current = self._instruments.get(info.spec.symbol)
                if current is None or current.broker_symbol == info.spec.broker_symbol:
                    self._instruments[info.spec.symbol] = info.spec

    async def fetch_instruments(self) -> list[InstrumentSpec]:
        self._require_authorized()
        if not self._assets:
            self._assets = parse_assets(objs((await self._req(PT_ASSET_LIST_REQ, {})).payload, "asset"))
        await self._load_light_symbols()
        ids: set[int] = set(self._by_id)
        for symbol in self._configured_symbols:
            row = self._light.get(symbol)
            symbol_id = to_int(row.get("symbolId")) if row is not None else None
            if symbol_id is not None:
                ids.add(symbol_id)
        recon = (await self._req(PT_RECONCILE_REQ, {"returnProtectionOrders": False})).payload
        for row in [*objs(recon, "position"), *objs(recon, "order")]:
            symbol_id = to_int(obj(row, "tradeData").get("symbolId"))
            if symbol_id is not None:
                ids.add(symbol_id)
        await self._load_symbols(ids)
        return list(self._instruments.values())

    async def _ensure_symbols(self, ids: Iterable[int | None]) -> None:
        """A symbolId missing from the map triggers a refresh, at most once per 60 s (§1)."""
        missing = {i for i in ids if i is not None and i not in self._by_id}
        if not missing:
            return
        async with self._refresh_lock:
            missing = {i for i in missing if i not in self._by_id}
            now = self._monotonic()
            if not missing or (self._last_on_demand is not None
                               and now - self._last_on_demand < INSTRUMENT_REFRESH_MIN_S):
                return
            self._last_on_demand = now
            # The map serves stream mapping, which waits on this lock: never refresh it at the class of a
            # background caller (a DLQ / poll read that met an unknown symbol) - no priority inversion.
            with rest_priority(RestPriority.RECONCILE):
                await self._load_symbols(missing)

    async def _symbol_ids_for(self, symbols: Sequence[str]) -> list[int]:
        if not self._light:
            await self._load_light_symbols()
        found: list[int] = []
        need: list[int] = []
        for symbol in symbols:
            row = self._light.get(canonical_symbol(symbol))
            symbol_id = to_int(row.get("symbolId")) if row is not None else None
            if symbol_id is None:
                log.warning("ctrader: no symbol found for %s (no prices)", symbol)
                continue
            if symbol_id not in found:
                found.append(symbol_id)
            if symbol_id not in self._by_id:
                need.append(symbol_id)
        if need:
            await self._load_symbols(need)
        return [i for i in found if i in self._by_id]

    # --- market sessions (SM10) ----------------------------------------------------------------------
    async def fetch_market_sessions(self, symbols: Sequence[str]) -> dict[str, MarketSessions]:
        """Weekly schedule + holidays per canonical symbol from ProtoOASymbolByIdReq (read-only, on the
        allow-list; the symbol carries its schedule, scheduleTimeZone and holidays, so no holiday
        payloadType is needed). Unknown symbols, and symbols with no usable schedule, are absent."""
        self._require_authorized()
        if not self._light:
            await self._load_light_symbols()
        wanted: dict[int, str] = {}
        for symbol in symbols:
            canon = canonical_symbol(symbol)
            row = self._light.get(canon)
            symbol_id = to_int(row.get("symbolId")) if row is not None else None
            if symbol_id is None:
                log.warning("ctrader: no symbol found for %s (no market sessions)", symbol)
                continue
            wanted.setdefault(symbol_id, canon)
        out: dict[str, MarketSessions] = {}
        ids = sorted(wanted)
        now = self._wall_ms()
        for i in range(0, len(ids), SYMBOL_BY_ID_CHUNK):
            res = await self._req(PT_SYMBOL_BY_ID_REQ, {"symbolId": ids[i:i + SYMBOL_BY_ID_CHUNK]})
            for full in objs(res.payload, "symbol"):
                symbol_id = to_int(full.get("symbolId"))
                name = wanted.get(symbol_id) if symbol_id is not None else None
                if name is None:
                    continue
                sessions = sessions_from_symbol(full, adapter_id=self.adapter_id, symbol=name, now_ms=now)
                if sessions is not None:
                    out[name] = sessions
        return out

    # --- reads ------------------------------------------------------------------------------------
    async def _reconcile(self) -> JsonObj:
        return (await self._req(PT_RECONCILE_REQ, {"returnProtectionOrders": False})).payload

    def _remember_margin(self, positions: Iterable[JsonObj], *, replace: bool) -> None:
        if replace:
            self._used_margin.clear()
        for row in positions:
            pid = to_id(row.get("positionId"))
            used = money(row.get("usedMargin"), money_digits(row, self._money_digits))
            if pid is None:
                continue
            if used is None or enum_value(row.get("positionStatus"), {"POSITION_STATUS_CLOSED": 2}) == 2:
                self._used_margin.pop(pid, None)
            else:
                self._used_margin[pid] = used

    def _map_positions(self, rows: Iterable[JsonObj], source: Source) -> list[PositionDTO]:
        out: list[PositionDTO] = []
        for row in rows:
            try:
                dto = map_position(row, account_id=self.account_id, lookup=self._lookup, source=source,
                                   default_money_digits=self._money_digits, adapter_id=self.adapter_id)
            except UnmappableError as exc:
                self._unmapped("orders", f"position skipped: {exc}")
                continue
            if dto is not None:
                self._positions_last[dto.position_id] = dto
                out.append(dto)
        return out

    async def fetch_positions(self) -> list[PositionDTO]:
        recon = await self._reconcile()
        rows = objs(recon, "position")
        await self._ensure_symbols(to_int(obj(r, "tradeData").get("symbolId")) for r in rows)
        self._remember_margin(rows, replace=True)
        return self._map_positions(rows, "REST")

    async def fetch_position_status(self, position_id: str) -> PositionDTO | None:
        """From the reconcile, else rebuilt from its deals; None when neither proves its state."""
        recon = await self._reconcile()
        for row in objs(recon, "position"):
            if to_id(row.get("positionId")) == position_id:
                await self._ensure_symbols([to_int(obj(row, "tradeData").get("symbolId"))])
                before = unmapped_mark(self.unmapped_rows, "orders")   # also counts inside a catch-up read
                mapped = self._map_positions([row], "REST")
                if not mapped and unmapped_mark(self.unmapped_rows, "orders") > before:
                    # Never None ("the broker has none"): the caller would drop the entity's parked row.
                    raise BrokerError(f"cTrader position {position_id} unmappable")
                return mapped[0] if mapped else None
        pid = to_int(position_id)
        if pid is None:
            return None
        to_ms = self._wall_ms()
        deals = await self._deals_in_windows(PT_DEAL_LIST_BY_POSITION_REQ, {"positionId": pid},
                                             to_ms - self._position_lookback_ms, to_ms)
        await self._ensure_symbols(to_int(d.get("symbolId")) for d in deals)
        return position_from_deals(deals, position_id, account_id=self.account_id, lookup=self._lookup,
                                   default_money_digits=self._money_digits, adapter_id=self.adapter_id)

    def _map_orders(self, rows: Iterable[JsonObj], source: Source, execution_type: int | None = None,
                    *, keep_live_protection: bool = False) -> list[OrderDTO]:
        out: list[OrderDTO] = []
        for row in rows:
            try:
                dto = map_order(row, account_id=self.account_id, lookup=self._lookup, source=source,
                                execution_type=execution_type, adapter_id=self.adapter_id)
            except UnmappableError as exc:
                self._unmapped("orders", f"order skipped: {exc}")
                continue
            # Protection (SL/TP) orders are the position's own stopLoss / takeProfit; a working one is
            # never emitted, so it can never stand in for a stop the position no longer has.
            if is_protection_order(row) and not dto.status.terminal and not keep_live_protection:
                continue
            out.append(dto)
        return out

    async def fetch_open_orders(self) -> list[OrderDTO]:
        recon = await self._reconcile()
        rows = objs(recon, "order")
        await self._ensure_symbols(to_int(obj(r, "tradeData").get("symbolId")) for r in rows)
        return self._map_orders(rows, "REST")

    async def fetch_order_status(self, broker_order_id: str) -> OrderDTO | None:
        order_id = to_int(broker_order_id)
        if order_id is None:
            return None
        try:
            res = await self._req(PT_ORDER_DETAILS_REQ, {"orderId": order_id})
        except OpenApiError as exc:
            if exc.error_code in NOT_FOUND_CODES:
                note_absent("ORDER", broker_order_id)   # the only explicit "not found" (IMPLEMENTATION_PLAN 8.1)
                return None
            raise
        order = obj(res.payload, "order")
        if not order:
            return None
        await self._ensure_symbols([to_int(obj(order, "tradeData").get("symbolId"))])
        before = unmapped_mark(self.unmapped_rows, "orders")   # also counts inside a catch-up read
        mapped = self._map_orders([order], "REST", keep_live_protection=True)
        if not mapped and unmapped_mark(self.unmapped_rows, "orders") > before:
            # Never None ("the broker has none"): the caller would drop the entity's parked row.
            raise BrokerError(f"cTrader order {broker_order_id} unmappable")
        return mapped[0] if mapped else None

    def _live(self, timestamp_ms: int) -> AccountLive:
        return AccountLive(account_id=self.account_id, adapter_id=self.adapter_id, currency=self._currency or "",
                           equity=None,   # broker equity is not available on cTrader
                           margin_used=sum(self._used_margin.values(), Decimal(0)), free_margin=None,
                           unrealised_pnl=None, timestamp_ms=timestamp_ms)

    async def fetch_account_snapshot(self) -> tuple[list[AccountDTO], list[AccountLive]]:
        trader = obj((await self._req(PT_TRADER_REQ, {})).payload, "trader")
        self._money_digits = money_digits(trader, self._money_digits)
        currency = account_currency(trader, self._assets) or self._currency
        if currency is None:
            raise BrokerError(f"cTrader account {self._account}: deposit asset unknown")
        self._currency = currency
        try:
            account = map_trader(trader, account_id=self.account_id, currency=currency, source="REST",
                                 adapter_id=self.adapter_id)
        except UnmappableError as exc:
            self._unmapped("account", f"trader unmappable: {exc}")
            raise BrokerError(f"cTrader trader: {exc}") from None
        self._remember_margin(objs(await self._reconcile(), "position"), replace=True)
        return [account], [self._live(self._wall_ms())]

    async def _deals_in_windows(self, payload_type: int, extra: JsonObj, from_ms: int, to_ms: int,
                                truncated: list[str] | None = None) -> list[JsonObj]:
        deals: dict[str, JsonObj] = {}
        start = max(from_ms, 0)
        while start < to_ms:
            end = min(start + WEEK_MS, to_ms)
            for deal in await self._deal_window(payload_type, extra, start, end, 0, truncated):
                deal_id = to_id(deal.get("dealId"))
                if deal_id is not None:
                    deals[deal_id] = deal
            start = end
        return list(deals.values())

    async def _deal_window(self, payload_type: int, extra: JsonObj, start: int, end: int,
                           depth: int, truncated: list[str] | None = None) -> list[JsonObj]:
        payload: dict[str, Any] = {**extra, "fromTimestamp": start, "toTimestamp": end}
        if payload_type == PT_DEAL_LIST_REQ:
            payload["maxRows"] = DEAL_LIST_MAX_ROWS
        res = await self._req(payload_type, payload)
        rows = objs(res.payload, "deal")
        if to_bool(res.payload.get("hasMore")) and end - start > 1:
            if depth >= MAX_WINDOW_SPLITS:
                log.warning("ctrader: deal list still truncated for [%d, %d]", start, end)
                if truncated is not None:
                    truncated.append(f"deal list still truncated for [{start}, {end}]")
                return rows
            mid = start + (end - start) // 2
            return [*await self._deal_window(payload_type, extra, start, mid, depth + 1, truncated),
                    *await self._deal_window(payload_type, extra, mid, end, depth + 1, truncated)]
        return rows

    async def fetch_cash_flows(self, since_ms: int) -> list[CashFlowDTO]:
        """Deposits / withdrawals (cash-flow history) and TRADE_PNL of closing deals since ``since_ms``.
        A deal list still truncated after the window splits, or a deposit / withdrawal or closing deal that
        could not be mapped, makes the result a partial ``CashFlowBatch`` (never one that looks complete;
        IMPLEMENTATION_PLAN 7.1)."""
        self._require_authorized()
        currency = self._currency
        if currency is None:
            raise BrokerError("cTrader account currency unknown")
        to_ms = self._wall_ms()
        flows: dict[str, CashFlowDTO] = {}
        partial: list[str] = []
        start = max(since_ms, 0)
        while start < to_ms:
            end = min(start + WEEK_MS, to_ms)
            res = await self._req(PT_CASH_FLOW_HISTORY_REQ, {"fromTimestamp": start, "toTimestamp": end})
            for dw in objs(res.payload, "depositWithdraw"):
                flow = map_deposit_withdraw(dw, account_id=self.account_id, currency=currency,
                                            default_money_digits=self._money_digits, adapter_id=self.adapter_id)
                if flow is None:
                    why = f"deposit / withdrawal {to_id(dw.get('balanceHistoryId')) or '?'} unmappable"
                    log.error("ctrader: cash-flow history: %s", why)
                    partial.append(why)
                elif flow.broker_ts >= since_ms:
                    flows[flow.transaction_id] = flow
            start = end
        for deal in await self._deals_in_windows(PT_DEAL_LIST_REQ, {}, since_ms, to_ms, partial):
            flow = map_closing_deal(deal, account_id=self.account_id, currency=currency,
                                    default_money_digits=self._money_digits, adapter_id=self.adapter_id)
            if flow is None and obj(deal, "closePositionDetail"):
                why = f"closing deal {to_id(deal.get('dealId')) or '?'} unmappable"
                log.error("ctrader: deal list: %s", why)
                partial.append(why)
            elif flow is not None and flow.broker_ts >= since_ms:
                flows[flow.transaction_id] = flow
        return CashFlowBatch(sorted(flows.values(), key=lambda f: (f.broker_ts, f.transaction_id)), partial)

    # --- history (off-time catch-up) ------------------------------------------------------------------
    @property
    def history_retention_ms(self) -> int | None:
        """How far back the cTrader deal / order lists reach is not documented: unknown."""
        return None

    async def fetch_history(self, since_ms: int, until_ms: int) -> BrokerHistory:
        """Every order / position cTrader changed in ``[since_ms, until_ms]`` (the catch-up after the engine was off).

        At ``RestPriority.RECONCILE`` on the adapter's one request budget: the deal list and the order list by time
        range (one-week windows, split in halves while ``hasMore``), order details for a dealt order the order
        list lacks, then the reconcile (newest truth). A position a deal names is OPEN from the reconcile, or
        rebuilt from its own deals (deal list by position id, from ``position_lookback_ms`` before the window).
        Working protection (SL/TP) orders are not emitted (accepted deviation); terminal ones are.

        Incomplete (``complete=False``) on any failed or still-truncated page, an unmappable row (counted on this
        read's own channel, ``history_read``, never on the live-stream ``unmapped_rows``), or a position whose
        state the deals do not prove. Retryable (``BrokerHistory.retryable``) unless every cause is a broker limit:
        a page still truncated after the splits, or the 13-week request cap. A failed page (a timeout, a throttle
        or maintenance code, a refused request budget, any other broker error) is retryable."""
        self._require_authorized()
        notes = HistoryNotes()
        since = max(since_ms, 0)
        until = max(until_ms, since)
        covered = covered_from(since, until, self.history_retention_ms)
        if until - covered > HISTORY_MAX_WEEKS * WEEK_MS:
            covered = until - HISTORY_MAX_WEEKS * WEEK_MS     # bounded request count; noted below
        with rest_priority(RestPriority.RECONCILE), history_read() as read:
            raw_deals = await self._history_rows(PT_DEAL_LIST_REQ, "deal", {}, covered, until, notes)
            raw_orders = await self._history_rows(PT_ORDER_LIST_REQ, "order", {}, covered, until, notes)
            for row in raw_deals:
                if to_id(row.get("dealId")) is None or to_id(row.get("positionId")) is None:
                    self._unmapped("orders", "history: deal without dealId / positionId")
            for row in raw_orders:
                if to_id(row.get("orderId")) is None:
                    self._unmapped("orders", "history: order without orderId")
            deals = latest_by_id(raw_deals, "dealId", deal=True)
            order_rows = latest_by_id(raw_orders, "orderId", deal=False)
            for oid in sorted({i for i in (to_id(d.get("orderId")) for d in deals.values()) if i is not None}
                              - set(order_rows), key=int):
                detail = await self._history_order_details(oid, notes)
                if detail is not None:
                    order_rows[oid] = detail
            recon = await self._reconcile()
            open_rows = {pid: r for r in objs(recon, "position") if (pid := to_id(r.get("positionId"))) is not None}
            await self._ensure_symbols([
                *(to_int(d.get("symbolId")) for d in deals.values()),
                *(to_int(obj(r, "tradeData").get("symbolId")) for r in [*order_rows.values(), *open_rows.values()]),
            ])
            # Rows skipped here count on this read (`history_read`), never on the live-stream counters.
            orders = {o.broker_order_id: o for o in self._map_orders(order_rows.values(), "REST")}
            positions: dict[str, PositionDTO] = {}
            by_position: dict[str, list[JsonObj]] = {}
            for deal in deals.values():
                pid = to_id(deal.get("positionId"))
                # A deal known not to have filled (rejected, error, missed) moved no position: its order is
                # still listed (order list / details). A missing or unknown status counts (fail closed).
                if pid is not None and deal_filled(deal) is not False:
                    by_position.setdefault(pid, []).append(deal)
            for pid in sorted(by_position, key=int):
                if pid in open_rows:
                    for held in self._map_positions([open_rows[pid]], "REST"):
                        positions[pid] = held
                    continue
                closed = await self._closed_position(pid, by_position[pid], covered, notes)
                if closed is not None:
                    positions[pid] = closed
        if read.unmapped > 0:
            notes.append(f"{read.unmapped} history row(s) could not be mapped")
        if covered > since:
            notes.limit(f"cTrader history covers only from {covered}")
        return BrokerHistory(
            adapter_id=self.adapter_id, since_ms=since_ms, until_ms=until_ms, covered_from_ms=covered,
            orders=tuple(sorted(orders.values(), key=lambda o: (o.broker_updated_at_ms, o.broker_order_id))),
            positions=tuple(sorted(positions.values(), key=lambda p: (p.broker_updated_at_ms, p.position_id))),
            complete=not notes, notes=notes.unique(), retryable=notes.retryable, unmapped=read.unmapped)

    async def _history_rows(self, payload_type: int, key: str, extra: JsonObj, from_ms: int, to_ms: int,
                            notes: HistoryNotes) -> list[JsonObj]:
        """Rows of one history list over ``[from_ms, to_ms)`` in one-week windows. A failed page (any broker
        error but a lost session or credential) is noted and skipped: the answer is then incomplete and
        retryable (a timeout, a throttle or maintenance code, a refused budget: a re-read may answer)."""
        rows: list[JsonObj] = []
        start = max(from_ms, 0)
        while start < to_ms:
            end = min(start + WEEK_MS, to_ms)
            rows.extend(await self._history_window(payload_type, key, extra, start, end, 0, notes))
            start = end
        return rows

    async def _history_window(self, payload_type: int, key: str, extra: JsonObj, start: int, end: int,
                              depth: int, notes: HistoryNotes) -> list[JsonObj]:
        payload: dict[str, Any] = {**extra, "fromTimestamp": start, "toTimestamp": end}
        if payload_type == PT_DEAL_LIST_REQ:
            payload["maxRows"] = DEAL_LIST_MAX_ROWS
        try:
            res = await self._req(payload_type, payload)
        except (SessionExpiredError, AuthFailedError):
            raise
        except BrokerError as exc:
            code = exc.error_code if isinstance(exc, OpenApiError) else type(exc).__name__
            log.warning("ctrader history: %s page [%d, %d] failed: %s", key, start, end, code)
            notes.append(f"{key} list page [{start}, {end}] failed: {code}")
            return []
        rows = objs(res.payload, key)
        if to_bool(res.payload.get("hasMore")):
            if depth >= MAX_WINDOW_SPLITS or end - start <= 1:
                notes.limit(f"{key} list still truncated for [{start}, {end}]")
                return rows
            mid = start + (end - start) // 2
            return [*await self._history_window(payload_type, key, extra, start, mid, depth + 1, notes),
                    *await self._history_window(payload_type, key, extra, mid, end, depth + 1, notes)]
        return rows

    async def _history_order_details(self, order_id: str, notes: HistoryNotes) -> JsonObj | None:
        """A dealt order the order list did not return, read by id; a failure leaves the answer incomplete."""
        try:
            res = await self._req(PT_ORDER_DETAILS_REQ, {"orderId": int(order_id)})
        except (SessionExpiredError, AuthFailedError):
            raise
        except BrokerError as exc:
            code = exc.error_code if isinstance(exc, OpenApiError) else type(exc).__name__
            notes.append(f"order {order_id} named by a deal could not be read: {code}")
            return None
        order = obj(res.payload, "order")
        if not order:
            notes.append(f"order {order_id} named by a deal: order details answered without the order")
            return None
        return order

    async def _closed_position(self, pid: str, window_deals: list[JsonObj], since_ms: int,
                               notes: HistoryNotes) -> PositionDTO | None:
        """A position a deal of the window names that the reconcile (read after the deals) no longer lists:
        rebuilt from its own deals; CLOSED only on deal evidence. Its deals are read from ``position_lookback_ms``
        before ``since_ms`` (the covered window start, so a capped window never multiplies the requests)."""
        deals = latest_by_id([*window_deals, *await self._history_rows(
            PT_DEAL_LIST_BY_POSITION_REQ, "deal", {"positionId": int(pid)},
            since_ms - self._position_lookback_ms, self._wall_ms() + 1, notes)], "dealId", deal=True)
        await self._ensure_symbols(to_int(d.get("symbolId")) for d in deals.values())
        rebuilt = position_from_deals(deals.values(), pid, account_id=self.account_id, lookup=self._lookup,
                                      default_money_digits=self._money_digits, adapter_id=self.adapter_id)
        if rebuilt is not None and rebuilt.status == "CLOSED":
            return rebuilt
        if rebuilt is None:
            closing = [d for d in deals.values() if is_closing_deal(d) and deal_filled(d) is True]
            last = max(closing, key=deal_time_ms, default=None)
            fallback = None if last is None else closed_from_closing_deal(
                last, pid, account_id=self.account_id, lookup=self._lookup, adapter_id=self.adapter_id)
            if fallback is not None:
                return fallback
            notes.append(f"position {pid} is not in the reconcile and its deals do not prove it closed")
            return None
        notes.append(f"position {pid} is not in the reconcile but its deals show it open")
        return None

    def _unmapped(self, stream: StreamName, why: str) -> None:
        note_unmapped(self.unmapped_rows, self.adapter_id, stream, why)

    def _on_frame_dropped(self, why: str) -> None:
        """The session read a frame it could not hand over (undecodable, or its handler failed). Its stream is
        unknown, so it counts on the orders and the account stream (fail closed: both resync)."""
        for stream in ("orders", "account"):
            self._unmapped(stream, f"frame lost: {why}")

    # --- socket events (reader side: synchronous, never blocks) ------------------------------------
    def _for_this_account(self, payload: JsonObj) -> bool:
        account = to_int(payload.get("ctidTraderAccountId"))
        return account is None or account == self._account

    def _offer(self, frame: Frame, *names: str) -> None:
        for name in names:
            ch = self._channels.get(name)
            if ch is not None:
                ch.offer(frame)

    def _on_frame(self, frame: Frame) -> None:
        pt, payload = frame.payload_type, frame.payload
        if pt == PT_TOKEN_INVALIDATED_EVENT:
            ids = {to_int(v) for v in payload.get("ctidTraderAccountIds") or []}
            if not ids or self._account in ids:
                self._auth_lost(f"token invalidated: {payload.get('reason') or ''}".strip())
            return
        if pt == PT_CLIENT_DISCONNECT_EVENT:
            self._auth_lost(f"client disconnect: {payload.get('reason') or ''}".strip())
            return
        if not self._for_this_account(payload):
            return
        if pt == PT_ACCOUNT_DISCONNECT_EVENT:
            self._auth_lost("account disconnect")
        elif pt == PT_SPOT_EVENT:
            # _wanted_spots is updated before the subscribe request is sent, so the snapshot spot that
            # follows the subscribe response is never dropped while the requester has not resumed yet.
            if to_int(payload.get("symbolId")) in self._wanted_spots:
                self._offer(frame, "prices")
        elif pt == PT_EXECUTION_EVENT:
            position = obj(payload, "position")
            if position:
                self._remember_margin([position], replace=False)
            self._offer(frame, "orders", "positions", "account")
        elif pt in (PT_TRADER_UPDATED_EVENT,):
            self._offer(frame, "account")
        elif pt == PT_MARGIN_CHANGED_EVENT:
            pid = to_id(payload.get("positionId"))
            used = money(payload.get("usedMargin"), money_digits(payload, self._money_digits))
            if pid is not None and used is not None:
                self._used_margin[pid] = used
            self._offer(frame, "account")
        elif pt == PT_TRAILING_SL_CHANGED_EVENT:
            self._offer(frame, "positions")
        elif pt == PT_SYMBOL_CHANGED_EVENT:
            self._last_on_demand = None   # allow the next lookup to refresh the map
        elif pt == PT_ORDER_ERROR_EVENT:
            log.warning("ctrader: order error event %s", payload.get("errorCode"))

    def _auth_lost(self, detail: str) -> None:
        self._authorized = False
        for ch in list(self._channels.values()):
            ch.put_event(StreamEventKind.AUTH_LOST, detail)

    # --- channels -----------------------------------------------------------------------------------
    def _open_channel(self, name: str, stream: StreamName, *, drop_when_full: bool) -> EventChannel:
        """One consumer per stream: a new call replaces (ends) the previous consumer."""
        ch = EventChannel(name, stream, maxsize=self._queue_size, drop_when_full=drop_when_full,
                          heartbeat_s=self._heartbeat_s, is_live=self._is_live, clock_ms=self._wall_ms)
        old = self._channels.get(name)
        self._channels[name] = ch
        if old is not None:
            old.close()
        return ch

    def _release_channel(self, ch: EventChannel) -> None:
        ch.close()
        if self._channels.get(ch.name) is ch:
            del self._channels[ch.name]

    def _note_watermark(self, ms: int) -> None:
        if ms > 0 and (self._watermark_ms is None or ms > self._watermark_ms):
            self._watermark_ms = ms

    async def _ensure_quietly(self, ids: Iterable[int | None]) -> StreamEvent | None:
        try:
            await self._ensure_symbols(ids)
        except SessionExpiredError as exc:
            return StreamEvent(StreamEventKind.AUTH_LOST, "orders", str(exc), self._wall_ms())
        except BrokerError as exc:
            log.warning("ctrader: instrument refresh failed: %s", exc)
        return None

    # --- streams -----------------------------------------------------------------------------------
    async def stream_order_updates(self) -> AsyncIterator[OrderStreamItem]:
        """Orders from execution events (positions come from stream_positions())."""
        self._require_authorized()
        ch = self._open_channel("orders", "orders", drop_when_full=False)
        ch.put_event(StreamEventKind.SUBSCRIBED, "account authorized")
        try:
            while True:
                msg = await ch.get()
                if msg is None:
                    return
                if isinstance(msg, StreamEvent):
                    yield msg
                    continue
                for item in await self._map_order_frame(msg):
                    yield item
        finally:
            self._release_channel(ch)

    async def _map_order_frame(self, frame: Frame) -> list[OrderStreamItem]:
        order = obj(frame.payload, "order")
        if not order:
            return []
        out: list[OrderStreamItem] = []
        lost = await self._ensure_quietly([to_int(obj(order, "tradeData").get("symbolId"))])
        if lost is not None:
            out.append(lost)
        execution_type = enum_value(frame.payload.get("executionType"), EXECUTION_TYPE)
        try:
            dto = map_order(order, account_id=self.account_id, lookup=self._lookup, source="STREAM",
                            execution_type=execution_type, adapter_id=self.adapter_id)
        except UnmappableError as exc:
            self._unmapped("orders", f"order event unmappable: {exc}")
            out.append(StreamEvent(StreamEventKind.GAP, "orders", f"unmappable: {exc}", self._wall_ms()))
            return out
        self._note_watermark(dto.broker_updated_at_ms)
        if is_protection_order(order) and not dto.status.terminal:
            return out
        out.append(dto)
        return out

    async def stream_positions(self) -> AsyncIterator[OrderStreamItem]:
        """Positions from execution events (native stopLoss / takeProfit) and trailing-stop moves."""
        self._require_authorized()
        ch = self._open_channel("positions", "orders", drop_when_full=False)
        ch.put_event(StreamEventKind.SUBSCRIBED, "account authorized")
        try:
            while True:
                msg = await ch.get()
                if msg is None:
                    return
                if isinstance(msg, StreamEvent):
                    yield msg
                    continue
                for item in await self._map_position_frame(msg):
                    yield item
        finally:
            self._release_channel(ch)

    async def _map_position_frame(self, frame: Frame) -> list[OrderStreamItem]:
        out: list[OrderStreamItem] = []
        if frame.payload_type == PT_TRAILING_SL_CHANGED_EVENT:
            pid = to_id(frame.payload.get("positionId"))
            last = self._positions_last.get(pid) if pid is not None else None
            stop = to_decimal(frame.payload.get("stopPrice"))
            ts = broker_ms(frame.payload, "utcLastUpdateTimestamp")
            if last is None or stop is None or stop <= 0 or ts == 0:
                self._unmapped("orders", f"trailing stop for position {pid} unmappable")
                out.append(StreamEvent(StreamEventKind.GAP, "orders", f"trailing stop for unknown position {pid}",
                                       self._wall_ms()))
                return out
            moved = dataclasses.replace(last, stop_loss=stop, broker_updated_at_ms=ts, source="STREAM")
            self._positions_last[moved.position_id] = moved
            self._note_watermark(ts)
            out.append(moved)
            return out
        position = obj(frame.payload, "position")
        if not position:
            return out
        lost = await self._ensure_quietly([to_int(obj(position, "tradeData").get("symbolId"))])
        if lost is not None:
            out.append(lost)
        try:
            mapped = self._map_positions_strict(position)
        except UnmappableError as exc:
            self._unmapped("orders", f"position event unmappable: {exc}")
            out.append(StreamEvent(StreamEventKind.GAP, "orders", f"unmappable: {exc}", self._wall_ms()))
            return out
        if mapped is not None:
            self._note_watermark(mapped.broker_updated_at_ms)
            out.append(mapped)
        return out

    def _map_positions_strict(self, position: JsonObj) -> PositionDTO | None:
        dto = map_position(position, account_id=self.account_id, lookup=self._lookup, source="STREAM",
                           default_money_digits=self._money_digits, adapter_id=self.adapter_id)
        if dto is not None:
            if dto.status == "CLOSED":
                self._positions_last.pop(dto.position_id, None)
            else:
                self._positions_last[dto.position_id] = dto
        return dto

    async def stream_account(self) -> AsyncIterator[AccountStreamItem]:
        """Balance (trader updates, deposits / withdrawals, closing deals), cash flows and live margin."""
        self._require_authorized()
        ch = self._open_channel("account", "account", drop_when_full=False)
        ch.put_event(StreamEventKind.SUBSCRIBED, "account authorized")
        try:
            while True:
                msg = await ch.get()
                if msg is None:
                    return
                if isinstance(msg, StreamEvent):
                    yield msg
                    continue
                for item in self._map_account_frame(msg):
                    yield item
        finally:
            self._release_channel(ch)

    def _map_account_frame(self, frame: Frame) -> list[AccountStreamItem]:
        currency = self._currency
        if currency is None:
            self._unmapped("account", "account event before the account currency is known")
            return [StreamEvent(StreamEventKind.GAP, "account", "account currency unknown", self._wall_ms())]
        payload = frame.payload
        out: list[AccountStreamItem] = []
        md = self._money_digits
        if frame.payload_type == PT_TRADER_UPDATED_EVENT:
            trader = obj(payload, "trader")
            try:
                out.append(map_trader(trader, account_id=self.account_id, currency=currency, source="STREAM",
                                      adapter_id=self.adapter_id))
            except UnmappableError as exc:
                self._unmapped("account", f"trader event unmappable: {exc}")
                out.append(StreamEvent(StreamEventKind.GAP, "account", f"unmappable: {exc}", self._wall_ms()))
        elif frame.payload_type == PT_MARGIN_CHANGED_EVENT:
            out.append(self._live(self._wall_ms()))
        elif frame.payload_type == PT_EXECUTION_EVENT:
            dw = obj(payload, "depositWithdraw")
            if dw:
                flow = map_deposit_withdraw(dw, account_id=self.account_id, currency=currency,
                                            default_money_digits=md, adapter_id=self.adapter_id)
                account = account_from_balance(dw, account_id=self.account_id, currency=currency,
                                               default_money_digits=md, adapter_id=self.adapter_id)
                if flow is None or account is None:
                    self._unmapped("account", "deposit / withdrawal event unmappable")
                    out.append(StreamEvent(StreamEventKind.GAP, "account", "unmappable deposit / withdrawal",
                                           self._wall_ms()))
                out.extend(x for x in (flow, account) if x is not None)
            deal = obj(payload, "deal")
            detail = obj(deal, "closePositionDetail")
            if detail:
                flow = map_closing_deal(deal, account_id=self.account_id, currency=currency,
                                        default_money_digits=md, adapter_id=self.adapter_id)
                account = account_from_balance(detail, account_id=self.account_id, currency=currency,
                                               default_money_digits=money_digits(deal, md),
                                               adapter_id=self.adapter_id)
                if flow is None or account is None:
                    self._unmapped("account", "closing deal event unmappable")
                    out.append(StreamEvent(StreamEventKind.GAP, "account", "unmappable closing deal",
                                           self._wall_ms()))
                out.extend(x for x in (flow, account) if x is not None)
            if obj(payload, "position"):
                out.append(self._live(self._wall_ms()))
        return out

    async def stream_ticks(self, symbols: Sequence[str]) -> AsyncIterator[PriceStreamItem]:
        """Spot events for the symbols (plus every symbol added by ensure_ticks). Overflow drops prices."""
        self._require_authorized()
        ch = self._open_channel("prices", "prices", drop_when_full=True)
        book: dict[int, tuple[Decimal | None, Decimal | None]] = {}
        try:
            ids = await self._symbol_ids_for(symbols)
            self._wanted_spots.update(ids)
            await self._subscribe_spots(sorted(self._wanted_spots))
            ch.put_event(StreamEventKind.SUBSCRIBED, f"{len(self._subscribed_spots)} symbol(s)")
            while True:
                msg = await ch.get()
                if msg is None:
                    return
                if isinstance(msg, StreamEvent):
                    if msg.kind in SESSION_ENDING:
                        book.clear()
                    yield msg
                    continue
                price = self._map_spot(msg, book)
                if price is not None:
                    yield price
        finally:
            spots = sorted(self._subscribed_spots)
            self._release_channel(ch)
            if spots and self._channels.get("prices") is None:
                self._spawn(self._unsubscribe_spots(spots))

    def _map_spot(self, frame: Frame, book: dict[int, tuple[Decimal | None, Decimal | None]]) -> PriceDTO | None:
        payload = frame.payload
        symbol_id = to_int(payload.get("symbolId"))
        info = self._lookup(symbol_id)
        if symbol_id is None or info is None:
            return None
        last_bid, last_ask = book.get(symbol_id, (None, None))
        bid = relative_price(payload.get("bid"), info.digits) or last_bid   # absent = unchanged
        ask = relative_price(payload.get("ask"), info.digits) or last_ask
        book[symbol_id] = (bid, ask)
        if bid is None or ask is None:
            return None
        ts = broker_ms(payload, "timestamp") or self._wall_ms()   # prices are unversioned
        spec = info.spec
        spread = (ask - bid) / spec.point_size if spec.point_size > 0 else None
        return PriceDTO(adapter_id=self.adapter_id, symbol=spec.symbol, bid=bid, ask=ask, spread_pips=spread,
                        timestamp_ms=ts, broker_market_id=spec.broker_symbol or None)

    async def _subscribe_spots(self, symbol_ids: Sequence[int]) -> None:
        new = [i for i in symbol_ids if i not in self._subscribed_spots]
        if not new:
            return
        try:
            await self._req(PT_SUBSCRIBE_SPOTS_REQ, {"symbolId": new, "subscribeToSpotTimestamp": True})
            self._subscribed_spots.update(new)
            return
        except OpenApiError as exc:
            if exc.error_code in ALREADY_DONE_CODES:
                self._subscribed_spots.update(new)
                return
            if len(new) == 1:
                raise
            log.warning("ctrader: batch spot subscribe failed (%s); subscribing one by one", exc.error_code)
        for symbol_id in new:
            try:
                await self._req(PT_SUBSCRIBE_SPOTS_REQ, {"symbolId": [symbol_id], "subscribeToSpotTimestamp": True})
            except OpenApiError as exc:
                if exc.error_code not in ALREADY_DONE_CODES:
                    log.warning("ctrader: spot subscribe for symbol %s failed: %s", symbol_id, exc.error_code)
                    continue
            self._subscribed_spots.add(symbol_id)

    async def _unsubscribe_spots(self, symbol_ids: Sequence[int]) -> None:
        if not self._is_live():
            return
        try:
            await self._req(PT_UNSUBSCRIBE_SPOTS_REQ, {"symbolId": list(symbol_ids)})
        except BrokerError as exc:
            log.info("ctrader: spot unsubscribe failed: %s", exc)
        self._subscribed_spots.difference_update(symbol_ids)

    def _spawn(self, coro: Any) -> None:
        try:
            task: asyncio.Task[None] = asyncio.get_running_loop().create_task(coro)
        except RuntimeError:
            coro.close()
            return
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def ensure_ticks(self, symbols: Sequence[str]) -> None:
        """Add symbols to the live spot subscription (read-only subscribe); idempotent."""
        self._require_authorized()
        ids = await self._symbol_ids_for(symbols)
        self._wanted_spots.update(ids)
        if self._channels.get("prices") is not None:
            await self._subscribe_spots(ids)
