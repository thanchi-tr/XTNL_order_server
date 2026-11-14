"""One REST token bucket per adapter, shared by every REST caller (operations.md §15).

Every broker request/response read of an adapter (StoneX CIAPI REST, cTrader ProtoOA requests) takes a
token from that adapter's :class:`RestBudget` before it is sent: snapshots, resync re-reads, DLQ
check-before-retry reads, cash-flow fetches, the 60 s flat-account poll, schedule fetches and the
adapter's own enrichment reads all draw from the same bucket.

Priority (highest first): ``RECONCILE`` > ``CASH_FLOWS`` (on-demand fetches relayed by the orchestrator)
> ``DLQ`` > ``POLL``. A caller names its class with the :func:`rest_priority` context manager around the
adapter call; the class travels in a ``ContextVar``, so it reaches the adapter's REST layer through any
number of awaits. A call made outside any ``rest_priority`` block counts as ``RECONCILE``: an untagged
caller is never starved, so tagging is what demotes the background callers.

- Waiters are served strictly by (priority, arrival): only the head waiter may take a token.
- Lower classes keep a reserve back: ``DLQ`` and ``POLL`` take a token only while the bucket holds more
  than their reserve, so a reconcile that starts right after a burst of background reads still finds tokens.
- A caller waits at most ``max_wait_s`` for its class, then gets ``TransientBrokerError`` (back off, retry).
- A broker throttle answer (HTTP 429 / ErrorCode 5002 / REQUEST_FREQUENCY_EXCEEDED) empties the bucket and
  blocks it for the broker's Retry-After (:meth:`RestBudget.throttled`).
- Session-management calls that sit right behind a synchronous lease check (LogOn, cTrader application /
  account auth, token refresh) must not wait between that check and the send: they use :meth:`spend`,
  which takes the token without waiting (the bucket may go into debt, so later calls wait instead).

:meth:`RestBudget.stats` feeds the ``xtnl_rest_bucket_tokens`` / ``xtnl_rest_requests_total`` metrics.
"""
from __future__ import annotations

import asyncio
import bisect
import contextlib
import itertools
import math
import time
from collections.abc import Callable, Iterator, Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from enum import IntEnum

from src.adapters.base import TransientBrokerError


class RestPriority(IntEnum):
    """Lower value = served first."""

    RECONCILE = 0     # SNAPSHOTTING / RESYNCING / SM3 reconcile re-reads, connect, enrichment
    CASH_FLOWS = 1    # on-demand fetch_cash_flows (Oversee or operator, relayed by the orchestrator)
    DLQ = 2           # DLQ check-before-retry reads
    POLL = 3          # the 60 s flat-account poll and other periodic reads (schedule refresh)

    @property
    def label(self) -> str:
        """Metric label value (`reconcile`, `cash_flows`, `dlq`, `poll`)."""
        return self.name.lower()


DEFAULT_RESERVE: Mapping[RestPriority, float] = {
    RestPriority.RECONCILE: 0.0, RestPriority.CASH_FLOWS: 0.1, RestPriority.DLQ: 0.25, RestPriority.POLL: 0.5,
}
DEFAULT_MAX_WAIT_S: Mapping[RestPriority, float] = {
    RestPriority.RECONCILE: 30.0, RestPriority.CASH_FLOWS: 30.0, RestPriority.DLQ: 15.0, RestPriority.POLL: 10.0,
}
DEFAULT_RETRY_AFTER_S = 1.0
MAX_RETRY_AFTER_S = 60.0

_PRIORITY: ContextVar[RestPriority] = ContextVar("xtnl_rest_priority", default=RestPriority.RECONCILE)


@contextlib.contextmanager
def rest_priority(priority: RestPriority) -> Iterator[None]:
    """Run the adapter calls inside this block at ``priority`` (they share the adapter's one bucket)."""
    token = _PRIORITY.set(priority)
    try:
        yield
    finally:
        _PRIORITY.reset(token)


def current_rest_priority() -> RestPriority:
    return _PRIORITY.get()


@dataclass(frozen=True)
class RestBudgetStats:
    adapter_id: str
    tokens: float                    # tokens left now (negative while in debt after spend())
    capacity: int
    rate_per_s: float
    requests: Mapping[str, int]      # by priority label, cumulative (xtnl_rest_requests_total)
    waiting: int                     # callers queued right now
    refused: int                     # callers that gave up after max_wait_s
    throttled: int                   # broker throttle answers seen


class RestBudget:
    """Token bucket with strict-priority waiters. Use it from one event loop."""

    def __init__(self, adapter_id: str, *, rate_per_s: float, burst: int,
                 reserve: Mapping[RestPriority, float] | None = None,
                 max_wait_s: Mapping[RestPriority, float] | None = None,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        if not (rate_per_s > 0 and math.isfinite(rate_per_s)):
            raise ValueError(f"REST rate must be finite and > 0 (got {rate_per_s})")
        if burst < 1:
            raise ValueError(f"REST burst must be >= 1 (got {burst})")
        self.adapter_id = adapter_id
        self.rate_per_s = float(rate_per_s)
        self.capacity = int(burst)
        self._reserve = {p: float((reserve or DEFAULT_RESERVE).get(p, 0.0)) for p in RestPriority}
        self._max_wait = {p: float((max_wait_s or DEFAULT_MAX_WAIT_S).get(p, 30.0)) for p in RestPriority}
        self._monotonic = monotonic
        self._tokens = float(burst)
        self._last = monotonic()
        self._blocked_until = 0.0
        self._waiters: list[tuple[int, int]] = []
        self._seq = itertools.count()
        self._changed: asyncio.Event | None = None
        self._requests = {p: 0 for p in RestPriority}
        self._refused = 0
        self._throttled = 0

    # --- accounting ---------------------------------------------------------------------------------
    def _refill(self) -> float:
        now = self._monotonic()
        if now > self._last:
            self._tokens = min(float(self.capacity), self._tokens + (now - self._last) * self.rate_per_s)
            self._last = now
        return now

    def floor(self, priority: RestPriority) -> float:
        """Tokens this class must leave in the bucket (never so high that it could never take one)."""
        return min(self._reserve[priority] * self.capacity, self.capacity - 1.0)

    def tokens(self) -> float:
        self._refill()
        return self._tokens

    def stats(self) -> RestBudgetStats:
        return RestBudgetStats(adapter_id=self.adapter_id, tokens=round(self.tokens(), 3), capacity=self.capacity,
                               rate_per_s=self.rate_per_s,
                               requests={p.label: n for p, n in self._requests.items()},
                               waiting=len(self._waiters), refused=self._refused, throttled=self._throttled)

    # --- taking tokens ------------------------------------------------------------------------------
    def spend(self, priority: RestPriority | None = None) -> None:
        """Take one token now without waiting (lease-checked session calls). The bucket may go into debt
        (down to -capacity); the debt delays later callers instead of this one."""
        p = priority if priority is not None else current_rest_priority()
        self._refill()
        self._tokens = max(-float(self.capacity), self._tokens - 1.0)
        self._requests[p] += 1

    def _try_take(self, entry: tuple[int, int], priority: RestPriority) -> float | None:
        """0 when a token was taken; else seconds until it might be possible (None: wait for a change)."""
        now = self._refill()
        if now < self._blocked_until:
            return self._blocked_until - now
        if not self._waiters or self._waiters[0] != entry:
            return None
        need = self.floor(priority) + 1.0 - self._tokens
        if need <= 1e-9:
            self._tokens -= 1.0
            return 0.0
        return need / self.rate_per_s

    async def acquire(self, priority: RestPriority | None = None) -> None:
        """Wait for a token at ``priority`` (default: the caller's :func:`rest_priority`)."""
        p = priority if priority is not None else current_rest_priority()
        entry = (int(p), next(self._seq))
        bisect.insort(self._waiters, entry)
        if self._waiters[0] == entry:
            self._notify()      # a new head (higher class): the old head re-checks and stands back
        deadline = self._monotonic() + self._max_wait[p]
        try:
            while True:
                wait = self._try_take(entry, p)
                if wait == 0.0:
                    self._requests[p] += 1
                    return
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    self._refused += 1
                    raise TransientBrokerError(
                        f"{self.adapter_id} REST budget exhausted: no token for a {p.label} call "
                        f"within {self._max_wait[p]:.0f} s")
                timeout = remaining if wait is None else min(remaining, max(wait, 0.001))
                event = self._event()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(event.wait(), timeout)
        finally:
            self._waiters.remove(entry)
            self._notify()

    def throttled(self, retry_after_s: float | None = None) -> None:
        """The broker said 'too many requests': empty the bucket and hold every caller for Retry-After."""
        delay = DEFAULT_RETRY_AFTER_S if retry_after_s is None or retry_after_s <= 0 else retry_after_s
        now = self._refill()
        self._tokens = min(self._tokens, 0.0)
        self._blocked_until = max(self._blocked_until, now + min(delay, MAX_RETRY_AFTER_S))
        self._throttled += 1
        self._notify()

    # --- wake-ups -----------------------------------------------------------------------------------
    def _event(self) -> asyncio.Event:
        if self._changed is None:
            self._changed = asyncio.Event()
        return self._changed

    def _notify(self) -> None:
        event, self._changed = self._changed, None
        if event is not None:
            event.set()


def parse_retry_after(value: str | None) -> float | None:
    """Retry-After in seconds (the HTTP-date form is not used by the brokers; it reads as unknown)."""
    if value is None:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        return None
    return seconds if seconds > 0 else None
