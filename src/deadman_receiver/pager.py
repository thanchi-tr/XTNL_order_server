"""Page delivery for the dead-man receiver: every configured transport (outside eu-west-2) gets every page.

Every delivery attempt is bounded by `delivery_timeout_s` (default 20 s): a transport that hangs (a slow-drip
provider, an SMTP thread that never returns) counts as a failed attempt, so the receiver's 1 s evaluation tick
never stalls behind one channel and silence keeps being detected. A failed delivery is retried per transport
with min(60, 2^n) s back-off (the SM6 cadence of the engine's relay)
until it succeeds or `max_attempts` is reached; pending retries are bounded (`max_pending`, oldest dropped with an
error log). Logs name the backend and the failure class only (`safe_error`), never a credential, URL or body.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Final

from src.notifications.errors import safe_error
from src.oversee.notify import AlertMessage, AlertTransport

log = logging.getLogger(__name__)

MAX_BACKOFF_S: Final = 60
DEFAULT_MAX_ATTEMPTS: Final = 30          # ~25 min of retries at the 60 s cap
DEFAULT_MAX_PENDING: Final = 200
DEFAULT_DELIVERY_TIMEOUT_S: Final = 20.0


@dataclass
class _Pending:
    transport: AlertTransport
    msg: AlertMessage
    attempts: int
    next_at_ms: int


def backoff_ms(attempts: int) -> int:
    return int(min(MAX_BACKOFF_S, 1 << min(max(0, attempts - 1), 16))) * 1000


class Pager:
    def __init__(self, transports: Sequence[AlertTransport], clock_ms: Callable[[], int], *,
                 max_attempts: int = DEFAULT_MAX_ATTEMPTS, max_pending: int = DEFAULT_MAX_PENDING,
                 delivery_timeout_s: float = DEFAULT_DELIVERY_TIMEOUT_S) -> None:
        if not transports:
            raise ValueError("the receiver needs at least one paging transport")
        self._transports = tuple(transports)
        self._clock = clock_ms
        self._max_attempts = max_attempts
        self._max_pending = max_pending
        self._timeout_s = delivery_timeout_s
        self._pending: list[_Pending] = []
        self.sent = 0
        self.failed = 0
        self.dropped = 0

    @property
    def backends(self) -> tuple[str, ...]:
        return tuple(t.backend for t in self._transports)

    @property
    def pending(self) -> int:
        return len(self._pending)

    async def _try(self, t: AlertTransport, msg: AlertMessage) -> bool:
        try:
            await asyncio.wait_for(t.deliver(msg), self._timeout_s)
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            self.failed += 1
            log.error("dead-man page via %s timed out after %.0f s", t.backend, self._timeout_s)
            return False
        except Exception as exc:          # ChannelError, or anything a transport raised
            self.failed += 1
            log.error("dead-man page via %s failed: %s", t.backend, safe_error(exc))
            return False
        self.sent += 1
        return True

    def _queue(self, t: AlertTransport, msg: AlertMessage, attempts: int) -> None:
        if attempts >= self._max_attempts:
            self.dropped += 1
            log.critical("dead-man page via %s given up after %d attempts: %s", t.backend, attempts, msg.subject)
            return
        if len(self._pending) >= self._max_pending:
            old = self._pending.pop(0)
            self.dropped += 1
            log.critical("dead-man retry queue full: dropped a page via %s (%s)", old.transport.backend,
                         old.msg.subject)
        self._pending.append(_Pending(t, msg, attempts, self._clock() + backoff_ms(attempts)))

    async def page(self, msg: AlertMessage) -> bool:
        """Deliver through every transport concurrently; True when at least one accepted it now."""
        results = await asyncio.gather(*(self._try(t, msg) for t in self._transports))
        for t, ok in zip(self._transports, results):
            if not ok:
                self._queue(t, msg, 1)
        if not any(results):
            log.critical("dead-man page not delivered on any channel yet (retrying): %s", msg.subject)
        return any(results)

    async def retry_due(self) -> int:
        """Retry the pending deliveries whose back-off has passed; returns how many succeeded."""
        now = self._clock()
        due = [p for p in self._pending if p.next_at_ms <= now]
        if not due:
            return 0
        self._pending = [p for p in self._pending if p.next_at_ms > now]
        results = await asyncio.gather(*(self._try(p.transport, p.msg) for p in due))
        for p, done in zip(due, results):
            if not done:
                self._queue(p.transport, p.msg, p.attempts + 1)
        return sum(results)
