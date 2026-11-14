"""Read-only StoneX CIAPI REST client (adapter-isolation.md §4, §5).

Every request goes through ReadOnlyTransport, which refuses order-changing paths.
Errors map onto the BrokerAdapter error classes:
- ErrorCode 4010 (invalid credentials) or PasswordChangeRequired -> AuthFailedError
- HTTP 401 or ErrorCode 4011 (invalid session)                    -> SessionExpiredError
- HTTP 429 / ErrorCode 5002 (throttled), 5xx, network errors      -> TransientBrokerError
The client never retries or re-authenticates by itself: the SM4 owner decides.

Every request takes a token from the adapter's shared RestBudget first (operations.md §15); LogOn and
log-off spend one without waiting (LogOn sits right behind the synchronous lease check). A throttle
answer empties the bucket for Retry-After. The default HTTP client sets SO_LINGER(on, 0) and
TCP_USER_TIMEOUT on every pooled socket (src/adapters/sockets.py).
"""
from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

import httpx

from src.adapters.base import AuthFailedError, BrokerError, SessionExpiredError, TransientBrokerError
from src.adapters.ratelimit import RestBudget, parse_retry_after
from src.adapters.sockets import reset_http_client
from src.adapters.stonex.wire import JsonObj, get_ci, get_list, get_obj, to_bool, to_int
from src.adapters.transport import ReadOnlyTransport
from src.config import StoneXSettings

log = logging.getLogger(__name__)

ERR_INVALID_CREDENTIALS = 4010
ERR_INVALID_SESSION = 4011
ERR_THROTTLED = 5002
# Live finding (2026-11-14): GET order/{id}/openposition of a position that has closed answers HTTP 403 with
# ErrorCode 80001 'You are not authorized to access this order' instead of 404 / OpenPosition null.
ERR_ORDER_NOT_ACCESSIBLE = 80001
DEFAULT_TIMEOUT_S = 10.0


def _order_id(order_id: str) -> str:
    """Order ids are integers; refuse anything else so no id can alter the path."""
    text = str(order_id).strip()
    if not text.isdigit():
        raise ValueError(f"invalid StoneX order id: {order_id!r}")
    return text


class OrderNotAccessibleError(BrokerError):
    """HTTP 403 + ErrorCode 80001 on a by-id order read: StoneX no longer serves this order id on that path (seen
    for a position that has closed). Only ``fetch_position_status`` reads it as "no longer open"; anywhere else it
    stays a broker error."""


class StoneXRestClient:
    """Session-holding REST reader. Methods return raw (case-tolerant) JSON objects."""

    def __init__(self, settings: StoneXSettings, transport: ReadOnlyTransport, *,
                 budget: RestBudget | None = None) -> None:
        self._s = settings
        self._t = transport
        self._session: str | None = None
        self.budget = budget                  # None = unmetered; StoneXAdapter attaches its own

    @classmethod
    def create(cls, settings: StoneXSettings, *, timeout_s: float = DEFAULT_TIMEOUT_S,
               http: httpx.AsyncClient | None = None, budget: RestBudget | None = None) -> StoneXRestClient:
        client = http if http is not None else reset_http_client(timeout_s)
        return cls(settings, ReadOnlyTransport(client, env=settings.env), budget=budget)

    @property
    def has_session(self) -> bool:
        return self._session is not None

    @property
    def session_token(self) -> str | None:
        """Needed as the Lightstreamer password. Never log it."""
        return self._session

    # --- transport -------------------------------------------------------------------
    async def _send(self, method: str, url: str, *, json: Any = None, params: dict[str, Any] | None = None,
                    logon: bool = False, allow_not_found: bool = False, wait: bool = True) -> JsonObj | None:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if not logon and self._session is None:
            raise SessionExpiredError("no StoneX session: log on first")
        budget = self.budget
        if budget is not None:
            if wait:
                await budget.acquire()
            else:
                budget.spend()
        if not logon:
            session = self._session
            if session is None:      # lost while waiting for a token
                raise SessionExpiredError("no StoneX session: log on first")
            headers.update({"UserName": self._s.username, "Session": session})
        try:
            resp = await self._t.request(method, url, headers=headers, json=json, params=params)
        except httpx.TimeoutException as exc:
            raise TransientBrokerError(f"StoneX timeout: {method} {_path(url)}") from exc
        except httpx.TransportError as exc:
            raise TransientBrokerError(f"StoneX network error: {method} {_path(url)}: {type(exc).__name__}") from exc

        body = _json(resp)
        status = resp.status_code
        obj = body if isinstance(body, dict) else None
        err_code = to_int(get_ci(obj, "ErrorCode"))
        if err_code is not None and status < 400:
            status = to_int(get_ci(obj, "HttpStatus")) or 400
        message = str(get_ci(obj, "ErrorMessage") or "")[:200]

        if status == 429 or err_code == ERR_THROTTLED:
            if budget is not None:
                budget.throttled(parse_retry_after(resp.headers.get("Retry-After")))
            raise TransientBrokerError(f"StoneX throttled (wait 1 s): {_path(url)}")
        if err_code == ERR_INVALID_CREDENTIALS:
            raise AuthFailedError(f"StoneX invalid credentials (4010): {message}")
        if status >= 500:
            raise TransientBrokerError(f"StoneX HTTP {status}: {_path(url)} {message}")
        if status in (401, 403) and logon:
            raise AuthFailedError(f"StoneX LogOn refused (HTTP {status}, ErrorCode {err_code}): {message}")
        if status == 401 or err_code == ERR_INVALID_SESSION:
            self._session = None
            raise SessionExpiredError(f"StoneX session expired (HTTP {status}, ErrorCode {err_code})")
        if status == 404 and allow_not_found:
            return None
        if status == 403 and err_code == ERR_ORDER_NOT_ACCESSIBLE:
            raise OrderNotAccessibleError(f"StoneX HTTP 403 ErrorCode {err_code}: {_path(url)} {message}")
        if status >= 400:
            raise BrokerError(f"StoneX HTTP {status} ErrorCode {err_code}: {_path(url)} {message}")
        if body is None:
            if allow_not_found:
                # A by-id read answers None only for an explicit HTTP 404 (IMPLEMENTATION_PLAN 8.1): an empty
                # or unparsable 2xx body proves nothing, so it is never read as "the broker has none".
                raise BrokerError(f"StoneX returned an empty body for {_path(url)}")
            return None
        if not isinstance(body, dict):
            raise BrokerError(f"StoneX returned a non-object body for {_path(url)}")
        return body

    async def _get(self, url: str, params: dict[str, Any] | None = None, *,
                   allow_not_found: bool = False) -> JsonObj | None:
        return await self._send("GET", url, params=params, allow_not_found=allow_not_found)

    # --- session -------------------------------------------------------------------------
    async def log_on(self) -> None:
        """LogOn v2. Raises AuthFailedError on bad credentials or PasswordChangeRequired."""
        self._session = None
        body = await self._send("POST", f"{self._s.base_url_v2}/Session", logon=True, wait=False, json={
            "UserName": self._s.username,
            "Password": self._s.password,
            "AppKey": self._s.app_key,
            "AppVersion": self._s.app_version,
            "AppComments": "",
        })
        if to_bool(get_ci(body, "PasswordChangeRequired")):
            raise AuthFailedError("StoneX LogOn: PasswordChangeRequired")
        session = get_ci(body, "Session")
        if not isinstance(session, str) or not session:
            two_fa = to_bool(get_ci(body, "Is2FAEnabled"))
            raise AuthFailedError(f"StoneX LogOn returned no session (Is2FAEnabled={two_fa})")
        self._session = session

    async def validate(self) -> bool:
        if self._session is None:
            return False
        try:
            body = await self._send("POST", f"{self._s.base_url_v2}/Session/validate",
                                    json={"UserName": self._s.username, "Session": self._session})
        except SessionExpiredError:
            return False
        flag = to_bool(get_ci(body, "IsAuthenticated", "IsValid"))
        return True if flag is None else flag

    async def log_off(self) -> None:
        """Best effort; never raises."""
        if self._session is None:
            return
        try:
            # The token goes only in the Session header and the JSON body, never the query string:
            # httpx logs every request URL at INFO, and logs are shipped off-node.
            await self._send("POST", f"{self._s.base_url_v1}/session/deleteSession", wait=False,
                             json={"UserName": self._s.username, "Session": self._session})
        except BrokerError as exc:
            log.info("StoneX log off failed (ignored): %s", exc)
        finally:
            self._session = None

    async def aclose(self) -> None:
        await self._t.aclose()

    # --- reads -----------------------------------------------------------------------------
    async def account_information(self) -> JsonObj:
        body = await self._get(f"{self._s.base_url_v2}/userAccount/ClientAndTradingAccount")
        return body or {}

    async def client_account_margin(self, client_account_id: str) -> JsonObj | None:
        return await self._get(f"{self._s.base_url_v2}/margin/clientAccountMargin",
                               {"clientAccountId": client_account_id})

    async def open_positions(self, trading_account_id: str) -> list[JsonObj]:
        body = await self._get(f"{self._s.base_url_v1}/order/openpositions",
                               {"TradingAccountId": trading_account_id})
        return get_list(body, "OpenPositions")

    async def active_stop_limit_orders(self, trading_account_id: str) -> list[JsonObj]:
        body = await self._get(f"{self._s.base_url_v1}/order/activestoplimitorders",
                               {"TradingAccountId": trading_account_id})
        return get_list(body, "ActiveStopLimitOrders")

    async def order(self, order_id: str) -> JsonObj | None:
        """GET order/{id}: {TradeOrder | StopLimitOrder} for active orders only. None only on HTTP 404."""
        return await self._get(f"{self._s.base_url_v1}/order/{_order_id(order_id)}", allow_not_found=True)

    async def order_open_position(self, order_id: str) -> JsonObj | None:
        """GET order/{id}/openposition: the whole body ({"OpenPosition": {...} | null}); None only on HTTP 404,
        so the caller can tell an explicit "not open" from a body that proves nothing."""
        return await self._get(f"{self._s.base_url_v1}/order/{_order_id(order_id)}/openposition",
                               allow_not_found=True)

    async def order_active_stop_limit(self, order_id: str) -> JsonObj | None:
        body = await self._get(f"{self._s.base_url_v1}/order/{_order_id(order_id)}/activestoplimitorder",
                               allow_not_found=True)
        return get_obj(body, "ActiveStopLimitOrder")

    async def trade_history(self, trading_account_id: str, from_s: int, max_results: int = 200) -> list[JsonObj]:
        body = await self._get(f"{self._s.base_url_v1}/order/tradehistory",
                               {"TradingAccountId": trading_account_id, "from": from_s, "maxResults": max_results})
        return get_list(body, "TradeHistory")

    async def stop_limit_order_history(self, trading_account_id: str, from_s: int,
                                       max_results: int = 200) -> list[JsonObj]:
        """GET order/stoplimitorderhistory (stop / limit orders no longer active: filled, cancelled, rejected).
        Confirmed read-only on the live account (2026-11-14): ``from`` (whole seconds) filters by last change,
        a capped answer is oldest-first, the hard cap is 1000 rows, and rows carry ``StatusId`` / ``TypeId``."""
        body = await self._get(f"{self._s.base_url_v1}/order/stoplimitorderhistory",
                               {"TradingAccountId": trading_account_id, "from": from_s,
                                "maxResults": max_results})
        return get_list(body, "StopLimitOrderHistory")

    async def market_information(self, client_account_id: str, market_ids: Sequence[str]) -> list[JsonObj]:
        if not market_ids:
            return []
        body = await self._send("POST", f"{self._s.base_url_v2}/market/information", json={
            "clientAccountId": int(client_account_id),
            "marketIds": [int(m) for m in market_ids],
        })
        return get_list(body, "MarketInformations", "MarketInformation")

    async def market_search(self, query: str, client_account_id: str, max_results: int = 20) -> list[JsonObj]:
        """GET market/search by code and name (to confirm against the live API)."""
        body = await self._get(f"{self._s.base_url_v1}/market/search", {
            "SearchByMarketCode": "true", "SearchByMarketName": "true", "Query": query,
            "MaxResults": max_results, "ClientAccountId": client_account_id,
        })
        return get_list(body, "Markets")


def _json(resp: httpx.Response) -> Any:
    if not resp.content:
        return None
    try:
        return resp.json()
    except ValueError:
        return None


def _path(url: str) -> str:
    """URL path only, for error messages (never the query: it can carry the session)."""
    return httpx.URL(url).path
