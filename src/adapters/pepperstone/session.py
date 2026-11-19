"""cTrader Open API session over one JSON WebSocket (adapter-isolation.md §4-§5).

- One reader task drains the socket continuously; responses are matched to requests by
  ``clientMsgId`` and everything else is handed synchronously to ``on_event`` (which never blocks).
  Consumers awaiting a request never stall the reader.
- One heartbeat task sends ``ProtoHeartbeatEvent`` every ``heartbeat_s`` (10 s) and drops the socket
  after ``idle_timeout_s`` (30 s) without inbound traffic.
- :meth:`OpenApiSession.send` is the only path to the socket; it builds every frame with
  :func:`encode_request`, which refuses any payloadType outside the read-only allow-list.
- Every request takes a token from the adapter's shared RestBudget (operations.md §15) first; the
  lease-checked auth / token-refresh requests spend one without waiting. Heartbeats are not metered.
  REQUEST_FREQUENCY_EXCEEDED empties the bucket for a second. Each request's outcome is reported to the bucket
  by priority (answered, or timeout / socket lost; a throttle answer is counted apart), and its send time is
  stamped right before the send, after the token wait and any historical-request spacing, and only when the
  socket is open (``rest_sent_ms``).
The socket sits behind the small :class:`WsConnection` protocol so tests inject a fake (no network).
"""
from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Protocol

from src.adapters.base import BrokerError, TransientBrokerError
from src.adapters.pepperstone.protocol import (
    HISTORICAL_REQUESTS,
    PT_ACCOUNT_AUTH_REQ,
    PT_APPLICATION_AUTH_REQ,
    PT_HEARTBEAT_EVENT,
    PT_REFRESH_TOKEN_REQ,
    Frame,
    JsonObj,
    decode_frame,
    encode_request,
    is_error,
)
from src.adapters.ratelimit import RestBudget, current_rest_priority, mark_rest_sent
from src.adapters.sockets import apply_reset_options

log = logging.getLogger(__name__)


class WsConnection(Protocol):
    """The surface of a websockets client connection the session uses."""

    async def send(self, message: str) -> None: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...


WsConnector = Callable[[str], Awaitable[WsConnection]]
EventHandler = Callable[[Frame], None]
ClosedHandler = Callable[[str], None]


class OpenApiError(BrokerError):
    """ProtoOAErrorRes / ProtoErrorRes answering a request."""

    def __init__(self, error_code: str, description: str, payload_type: int) -> None:
        super().__init__(f"cTrader error {error_code}: {description}".strip())
        self.error_code = error_code
        self.description = description
        self.payload_type = payload_type


class SessionClosedError(TransientBrokerError):
    """The socket closed while a request was waiting."""


async def websockets_connector(url: str) -> WsConnection:
    """Default connector: websockets client with SO_LINGER(on, 0) and TCP_USER_TIMEOUT where the
    platform has it, so a fence resets the socket instead of flushing it (adapter-isolation.md §5)."""
    from websockets.asyncio.client import connect

    ws = await connect(url, ping_interval=None, max_size=16 * 1024 * 1024, open_timeout=10)
    sock = ws.transport.get_extra_info("socket")
    if sock is not None:
        apply_reset_options(sock)
    return ws


# Lease-checked session steps: they must not wait for a token between the lease check and the send.
UNWAITED_REQUESTS: frozenset[int] = frozenset({PT_APPLICATION_AUTH_REQ, PT_ACCOUNT_AUTH_REQ, PT_REFRESH_TOKEN_REQ})
THROTTLE_CODES: frozenset[str] = frozenset({"REQUEST_FREQUENCY_EXCEEDED"})


def _monotonic() -> float:
    return time.monotonic()


class OpenApiSession:
    """One authenticated-or-not cTrader socket. Create a new session per connection."""

    def __init__(self, url: str, connector: WsConnector, *, on_event: EventHandler, on_closed: ClosedHandler,
                 heartbeat_s: float = 10.0, idle_timeout_s: float = 30.0, request_timeout_s: float = 15.0,
                 historical_interval_s: float = 0.25, monotonic: Callable[[], float] = _monotonic,
                 budget: RestBudget | None = None, on_dropped: Callable[[str], None] | None = None) -> None:
        self._url = url
        self._on_dropped = on_dropped     # a frame read but not handed over (never silently dropped)
        self._budget = budget
        self._connector = connector
        self._on_event = on_event
        self._on_closed = on_closed
        self._heartbeat_s = heartbeat_s
        self._idle_timeout_s = idle_timeout_s
        self._request_timeout_s = request_timeout_s
        self._historical_interval_s = historical_interval_s
        self._monotonic = monotonic
        self._ws: WsConnection | None = None
        self._pending: dict[str, asyncio.Future[Frame]] = {}
        self._ids = itertools.count(1)
        self._send_lock = asyncio.Lock()
        self._historical_lock = asyncio.Lock()
        self._last_historical = -1e9
        self._tasks: list[asyncio.Task[None]] = []
        self._closed_reason: str | None = None
        self.last_inbound = 0.0
        self.heartbeats_sent = 0
        self.heartbeats_received = 0
        self._heartbeat_at = 0.0

    @property
    def is_open(self) -> bool:
        return self._ws is not None and self._closed_reason is None

    async def open(self) -> None:
        self._ws = await self._connector(self._url)
        self.last_inbound = self._heartbeat_at = self._monotonic()
        self._tasks = [asyncio.create_task(self._read_loop(), name="ctrader-reader"),
                       asyncio.create_task(self._heartbeat_loop(), name="ctrader-heartbeat")]

    # --- the one guarded send ---------------------------------------------------------------------
    async def send(self, payload_type: int, payload: JsonObj, client_msg_id: str | None = None) -> None:
        text = encode_request(payload_type, payload, client_msg_id)   # allow-list check first
        ws = self._ws
        if ws is None or self._closed_reason is not None:
            raise SessionClosedError(f"cTrader socket is closed ({self._closed_reason or 'not open'})")
        async with self._send_lock:
            try:
                await ws.send(text)
            except Exception as exc:  # noqa: BLE001 - any send failure means the socket is gone
                self._shutdown(f"send failed: {type(exc).__name__}")
                raise SessionClosedError(f"cTrader send failed: {type(exc).__name__}") from exc

    async def request(self, payload_type: int, payload: JsonObj, *, timeout_s: float | None = None) -> Frame:
        """Send one allowed request and wait for its response (matched on clientMsgId)."""
        budget = self._budget
        if budget is not None:
            # The send is stamped below, right before it leaves (after any historical spacing; IN-5).
            if payload_type in UNWAITED_REQUESTS:
                budget.spend(stamp=False)
            else:
                await budget.acquire(stamp=False)
        if payload_type in HISTORICAL_REQUESTS:
            async with self._historical_lock:
                wait = self._last_historical + self._historical_interval_s - self._monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last_historical = self._monotonic()
        priority = current_rest_priority()
        msg_id = f"xtnl-{next(self._ids)}"
        fut: asyncio.Future[Frame] = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = fut
        try:
            if self._ws is None or self._closed_reason is not None:     # nothing leaves, so nothing is stamped
                raise SessionClosedError(f"cTrader socket is closed ({self._closed_reason or 'not open'})")
            if budget is not None:
                budget.mark_sent()
            else:
                mark_rest_sent()
            await self.send(payload_type, payload, msg_id)
            try:
                frame = await asyncio.wait_for(fut, timeout_s if timeout_s is not None else self._request_timeout_s)
            except TimeoutError:
                raise TransientBrokerError(f"cTrader request {payload_type} timed out") from None
        except TransientBrokerError:
            if budget is not None:
                budget.note_result(priority, False)     # no answer: timeout or socket lost (R11-M10)
            raise
        finally:
            self._pending.pop(msg_id, None)
        if is_error(frame.payload_type):
            code = str(frame.payload.get("errorCode") or "UNKNOWN_ERROR")
            if budget is not None:
                if code in THROTTLE_CODES:      # counted apart: a throttle is never read as an outage
                    budget.note_throttled(priority)
                    budget.throttled(None)
                else:
                    budget.note_result(priority, True)
            raise OpenApiError(code, str(frame.payload.get("description") or ""), payload_type)
        if budget is not None:
            budget.note_result(priority, True)
        return frame

    # --- background tasks -------------------------------------------------------------------------
    async def _read_loop(self) -> None:
        ws = self._ws
        assert ws is not None
        reason = "socket closed"
        try:
            while True:
                raw = await ws.recv()
                self.last_inbound = self._monotonic()
                frame = decode_frame(raw)
                if frame is None:
                    log.warning("ctrader: undecodable frame lost")
                    self._dropped("undecodable frame")
                    continue
                if frame.payload_type == PT_HEARTBEAT_EVENT:
                    self.heartbeats_received += 1
                fut = self._pending.get(frame.client_msg_id) if frame.client_msg_id is not None else None
                if fut is not None:
                    if not fut.done():
                        fut.set_result(frame)
                    continue
                try:
                    self._on_event(frame)
                except Exception:  # noqa: BLE001 - a mapping bug must never stop the drain
                    log.exception("ctrader: event handler failed for payloadType %s", frame.payload_type)
                    self._dropped(f"event handler failed for payloadType {frame.payload_type}")
        except asyncio.CancelledError:
            reason = "session closed"
            raise
        except Exception as exc:  # noqa: BLE001 - connection closed / transport error
            reason = f"socket lost: {type(exc).__name__}"
        finally:
            self._shutdown(reason)

    def _dropped(self, why: str) -> None:
        if self._on_dropped is None:
            return
        try:
            self._on_dropped(why)
        except Exception:  # noqa: BLE001 - never stop the drain
            log.exception("ctrader: dropped-frame handler failed")

    async def _heartbeat_loop(self) -> None:
        tick = min(self._heartbeat_s, 1.0)
        while self._closed_reason is None:
            await asyncio.sleep(tick)
            now = self._monotonic()
            if now - self.last_inbound > self._idle_timeout_s:
                log.warning("ctrader: no inbound traffic for %.1f s; dropping the socket", self._idle_timeout_s)
                self._shutdown("idle timeout")
                await self._close_ws()
                return
            if now - self._heartbeat_at >= self._heartbeat_s:
                try:
                    await self.send(PT_HEARTBEAT_EVENT, {})
                except SessionClosedError:
                    return
                self._heartbeat_at = now
                self.heartbeats_sent += 1

    def _shutdown(self, reason: str) -> None:
        if self._closed_reason is not None:
            return
        self._closed_reason = reason
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(SessionClosedError(f"cTrader socket closed: {reason}"))
        self._pending.clear()
        try:
            self._on_closed(reason)
        except Exception:  # noqa: BLE001
            log.exception("ctrader: closed handler failed")

    async def _close_ws(self) -> None:
        ws = self._ws
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()

    async def close(self, reason: str = "session closed") -> None:
        """Close the socket; the closed handler runs once with ``reason``."""
        self._shutdown(reason)
        current = asyncio.current_task()
        for task in self._tasks:
            if task is not current and not task.done():
                task.cancel()
        for task in self._tasks:
            if task is not current:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        await self._close_ws()
        self._ws = None

