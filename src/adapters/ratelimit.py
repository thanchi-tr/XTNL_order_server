"""One REST token bucket per adapter, shared by every REST caller (operations.md §15).

Every broker request/response read of an adapter (StoneX CIAPI REST, cTrader ProtoOA requests) takes a
token from that adapter's :class:`RestBudget` before it is sent: snapshots, resync re-reads, DLQ
check-before-retry reads, cash-flow fetches, the 60 s flat-account poll, schedule fetches and the
adapter's own enrichment reads all draw from the same bucket.

Priority (highest first): ``RECONCILE`` > ``CASH_FLOWS`` (on-demand fetches relayed by the orchestrator)
> ``BARS`` (market-data bar reads and the ARM's stop reads, design PR11; no caller while the optional services
are off) > ``DLQ`` > ``POLL``. A caller names its class with the :func:`rest_priority` context manager around the
adapter call; the class travels in a ``ContextVar``, so it reaches the adapter's REST layer through any
number of awaits. A call made outside any ``rest_priority`` block counts as ``RECONCILE``: an untagged
caller is never starved, so tagging is what demotes the background callers.

- Waiters are served strictly by (priority, arrival): only the head waiter may take a token.
- Lower classes keep a reserve back: ``BARS``, ``DLQ`` and ``POLL`` take a token only while the bucket holds
  more than their reserve, so a reconcile that starts right after a burst of background reads still finds tokens.
- ``BARS`` also has a per-adapter cap on calls per minute (default a quarter of the bucket's rate,
  :func:`default_bars_per_min`; never more than half of it, :func:`max_bars_per_min`): a ``BARS`` call over the
  cap is refused at once with ``TransientBrokerError`` (never queued), so bar re-reads can never take DLQ's and
  POLL's share.
- A caller waits at most ``max_wait_s`` for its class, then gets ``TransientBrokerError`` (back off, retry).
- A broker throttle answer (HTTP 429 / ErrorCode 5002 / REQUEST_FREQUENCY_EXCEEDED) empties the bucket and
  blocks it for the broker's Retry-After (:meth:`RestBudget.throttled`).
- Session-management calls that sit right behind a synchronous lease check (LogOn, cTrader application /
  account auth, token refresh) must not wait between that check and the send: they use :meth:`spend`,
  which takes the token without waiting (the bucket may go into debt, so later calls wait instead).

:meth:`RestBudget.stats` feeds the ``xtnl_rest_bucket_tokens`` / ``xtnl_rest_requests_total`` metrics.

Send-time stamp (IN-5): a successful :meth:`RestBudget.acquire` (or :meth:`RestBudget.spend`) records the wall
time in a ``ContextVar`` right after the token wait. The REST layers take their token with ``stamp=False`` and
stamp right before the send instead (:meth:`RestBudget.mark_sent`, or :func:`mark_rest_sent` when unmetered), after
any further wait (the cTrader historical-request spacing) and only when the request really leaves. The context
keeps the EARLIEST and the newest send since :func:`clear_rest_sent_ms`: :func:`rest_sent_ms` is the earliest, so
the age of a read made of several requests (one StoneX ``fetch_open_orders`` reads two lists per trading account,
then market details) counts from its first request and is never understated; :func:`rest_last_sent_ms` is the
newest. The age counts from when a request was sent, never from when it was queued. The stamp reaches the caller
only through awaits in the same task (a ``ContextVar`` set in a child task stays there); a caller that needs the
stamp of one adapter call clears it first.

Outcome counters (design R11-M10): the REST layer reports each request's outcome with
:meth:`RestBudget.note_result` (``ok`` = the broker answered; a timeout, a network error or HTTP 5xx is a failure).
A throttle answer is counted apart (:meth:`RestBudget.note_throttled`): the broker did answer, and a throttle that
our own callers caused (it empties the bucket for every priority) must never read as an outage.
:class:`RestBudgetStats` exposes per priority the counts, the consecutive failures and the last success / failure
time, so "REST failing at every priority" (a broker outage) can be told from "failing only at ``BARS``"
(:meth:`RestBudgetStats.failing_at_every_priority`, judged on recent outcomes only).
"""
from __future__ import annotations

import asyncio
import bisect
import contextlib
import itertools
import logging
import math
import time
from collections import deque
from collections.abc import Callable, Iterator, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from enum import IntEnum

from src.adapters.base import TransientBrokerError

log = logging.getLogger(__name__)


class RestPriority(IntEnum):
    """Lower value = served first. Metric labels come from the name, never the value."""

    RECONCILE = 0     # SNAPSHOTTING / RESYNCING / SM3 reconcile re-reads, connect, enrichment
    CASH_FLOWS = 1    # on-demand fetch_cash_flows (Oversee or operator, relayed by the orchestrator)
    BARS = 2          # md-bars broker bar reads and the ARM's stop reads (design PR11; capped per minute)
    DLQ = 3           # DLQ check-before-retry reads
    POLL = 4          # the 60 s flat-account poll and other periodic reads (schedule refresh)

    @property
    def label(self) -> str:
        """Metric label value (`reconcile`, `cash_flows`, `bars`, `dlq`, `poll`)."""
        return self.name.lower()


DEFAULT_RESERVE: Mapping[RestPriority, float] = {
    RestPriority.RECONCILE: 0.0, RestPriority.CASH_FLOWS: 0.1, RestPriority.BARS: 0.15, RestPriority.DLQ: 0.25,
    RestPriority.POLL: 0.5,
}
DEFAULT_MAX_WAIT_S: Mapping[RestPriority, float] = {
    RestPriority.RECONCILE: 30.0, RestPriority.CASH_FLOWS: 30.0, RestPriority.BARS: 15.0, RestPriority.DLQ: 15.0,
    RestPriority.POLL: 10.0,
}
DEFAULT_RETRY_AFTER_S = 1.0
MAX_RETRY_AFTER_S = 60.0
BARS_WINDOW_S = 60.0          # the BARS cap counts calls per minute (sliding window)
MAX_BARS_SHARE = 0.5          # the BARS cap never exceeds half the bucket's rate (PR11: DLQ and POLL keep a share)
OUTAGE_WINDOW_MS = 60_000     # failing_at_every_priority() judges only outcomes newer than this

_PRIORITY: ContextVar[RestPriority] = ContextVar("xtnl_rest_priority", default=RestPriority.RECONCILE)
# (earliest, newest) send since the last clear_rest_sent_ms(), or None.
_SENT_MS: ContextVar[tuple[int, int] | None] = ContextVar("xtnl_rest_sent_ms", default=None)


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


def default_bars_per_min(rate_per_s: float) -> int:
    """The default ``BARS`` cap: a quarter of the bucket's rate, per minute (at least 1)."""
    return max(1, int(rate_per_s * BARS_WINDOW_S / 4))


def max_bars_per_min(rate_per_s: float) -> int:
    """The highest ``BARS`` cap allowed: half the bucket's rate, per minute (at least 1)."""
    return max(1, int(rate_per_s * BARS_WINDOW_S * MAX_BARS_SHARE))


def effective_bars_per_min(rate_per_s: float, bars_per_min: int | None) -> int:
    """The cap a bucket uses for a configured value (None: the default), clamped to :func:`max_bars_per_min`."""
    cap = default_bars_per_min(rate_per_s) if bars_per_min is None else int(bars_per_min)
    return min(cap, max_bars_per_min(rate_per_s))


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


def rest_sent_ms() -> int | None:
    """Wall time (ms) the EARLIEST REST request of this context left for the broker since
    :func:`clear_rest_sent_ms` (after its token wait), or None when none was sent. The read age of an adapter call
    that sends several requests counts from this one (IN-5, PR9)."""
    cur = _SENT_MS.get()
    return None if cur is None else cur[0]


def rest_last_sent_ms() -> int | None:
    """Wall time (ms) the newest REST request of this context left since :func:`clear_rest_sent_ms`, or None."""
    cur = _SENT_MS.get()
    return None if cur is None else cur[1]


def clear_rest_sent_ms() -> None:
    """Forget the stamps, so the next :func:`rest_sent_ms` names only requests sent after this call."""
    _SENT_MS.set(None)


def mark_rest_sent(ms: int | None = None) -> None:
    """Stamp a request leaving now (``ms``: the caller's wall clock; default the system clock). The earliest send
    since the last clear is kept; the newest is updated."""
    now = _wall_ms() if ms is None else int(ms)
    cur = _SENT_MS.get()
    _SENT_MS.set((now, now) if cur is None else (min(cur[0], now), now))


@dataclass(frozen=True)
class RestOutcome:
    """Broker answers seen at one priority (``ok`` = the broker answered; see :meth:`RestBudget.note_result`)."""

    ok: int = 0
    failed: int = 0
    consecutive_failures: int = 0          # failures since the newest success (0 right after a success)
    last_success_ms: int | None = None     # wall time of the newest success
    last_failure_ms: int | None = None     # wall time of the newest failure
    throttled: int = 0                     # throttle answers (counted apart: never a success or a failure)
    last_throttled_ms: int | None = None   # wall time of the newest throttle answer

    @property
    def seen(self) -> bool:
        return self.ok + self.failed > 0

    @property
    def newest_ms(self) -> int | None:
        """Wall time of the newest success or failure (throttles excluded), or None."""
        times = [t for t in (self.last_success_ms, self.last_failure_ms) if t is not None]
        return max(times) if times else None


@dataclass(frozen=True)
class RestBudgetStats:
    adapter_id: str
    tokens: float                    # tokens left now (negative while in debt after spend())
    capacity: int
    rate_per_s: float
    requests: Mapping[str, int]      # by priority label, cumulative (xtnl_rest_requests_total)
    waiting: int                     # callers queued right now
    refused: int                     # callers that gave up after max_wait_s or were over the BARS cap
    throttled: int                   # broker throttle answers seen
    bars_per_min: int = 0            # the BARS cap
    bars_in_window: int = 0          # BARS calls admitted in the last minute (waiting, in flight or sent)
    bars_capped: int = 0             # BARS calls refused by the cap, cumulative (also counted in `refused`)
    outcomes: Mapping[str, RestOutcome] = field(default_factory=dict)   # by priority label

    def failing_at_every_priority(self, now_ms: int, *, within_ms: int = OUTAGE_WINDOW_MS) -> bool | None:
        """A broker outage seen from REST, judged on recent outcomes only: the priorities whose newest success or
        failure is at most ``within_ms`` old (throttle answers never count). None (unknown) when no priority other
        than ``BARS`` has such an outcome: ``BARS`` alone never decides an outage either way, so bar-read failures
        must then not count toward FAILED and no ``broker_unavailable`` page is raised (design §B14, R11-M10).
        Otherwise True when the newest outcome of every recent priority is a failure, else False."""
        cutoff = now_ms - within_ms
        recent = {label: o for label, o in self.outcomes.items()
                  if (t := o.newest_ms) is not None and t >= cutoff}
        if not any(label != RestPriority.BARS.label for label in recent):
            return None
        return all(o.consecutive_failures > 0 for o in recent.values())


class RestBudget:
    """Token bucket with strict-priority waiters. Use it from one event loop."""

    def __init__(self, adapter_id: str, *, rate_per_s: float, burst: int,
                 reserve: Mapping[RestPriority, float] | None = None,
                 max_wait_s: Mapping[RestPriority, float] | None = None,
                 bars_per_min: int | None = None,
                 monotonic: Callable[[], float] = time.monotonic,
                 wall_ms: Callable[[], int] = _wall_ms) -> None:
        if not (rate_per_s > 0 and math.isfinite(rate_per_s)):
            raise ValueError(f"REST rate must be finite and > 0 (got {rate_per_s})")
        if burst < 1:
            raise ValueError(f"REST burst must be >= 1 (got {burst})")
        if bars_per_min is not None and bars_per_min < 1:
            raise ValueError(f"REST BARS cap must be >= 1 per minute (got {bars_per_min})")
        self.adapter_id = adapter_id
        self.rate_per_s = float(rate_per_s)
        self.capacity = int(burst)
        self._bars_per_min = default_bars_per_min(rate_per_s)
        self.bars_per_min = default_bars_per_min(rate_per_s) if bars_per_min is None else int(bars_per_min)
        self._reserve = {p: float((reserve or DEFAULT_RESERVE).get(p, 0.0)) for p in RestPriority}
        self._max_wait = {p: float((max_wait_s or DEFAULT_MAX_WAIT_S).get(p, 30.0)) for p in RestPriority}
        self._monotonic = monotonic
        self._wall_ms = wall_ms
        self._tokens = float(burst)
        self._last = monotonic()
        self._blocked_until = 0.0
        self._waiters: list[tuple[int, int]] = []
        self._seq = itertools.count()
        self._changed: asyncio.Event | None = None
        self._requests = {p: 0 for p in RestPriority}
        self._refused = 0
        self._throttled = 0
        self._bars_window: deque[float] = deque()     # monotonic admit times of BARS calls in the last minute
        self._bars_capped = 0
        self._outcomes = {p: RestOutcome() for p in RestPriority}

    @property
    def bars_per_min(self) -> int:
        """The ``BARS`` cap in use (calls per minute)."""
        return self._bars_per_min

    @bars_per_min.setter
    def bars_per_min(self, value: int) -> None:
        """Change the cap in place (the bucket, its tokens and its counters stay). A value above
        :func:`max_bars_per_min` is clamped, with a warning naming the setting only."""
        if value < 1:
            raise ValueError(f"REST BARS cap must be >= 1 per minute (got {value})")
        cap = effective_bars_per_min(self.rate_per_s, int(value))
        if cap < value:
            log.warning("%s: the REST BARS cap (*_REST_BARS_PER_MIN) is above half the REST rate; clamped to %d "
                        "per minute", self.adapter_id, cap)
        self._bars_per_min = cap

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
                               waiting=len(self._waiters), refused=self._refused, throttled=self._throttled,
                               bars_per_min=self.bars_per_min, bars_in_window=self._bars_admitted(),
                               bars_capped=self._bars_capped,
                               outcomes={p.label: o for p, o in self._outcomes.items()})

    def note_result(self, priority: RestPriority | None, ok: bool) -> None:
        """Record one request's outcome at ``priority`` (None: the caller's :func:`rest_priority`). ``ok`` means
        the broker answered (an error answer about the request itself included); a timeout, a network error or
        HTTP 5xx is a failure. A throttle answer goes to :meth:`note_throttled`. A refusal by this bucket is no
        broker outcome."""
        p = priority if priority is not None else current_rest_priority()
        cur = self._outcomes[p]
        now = self._wall_ms()
        if ok:
            self._outcomes[p] = replace(cur, ok=cur.ok + 1, consecutive_failures=0, last_success_ms=now)
        else:
            self._outcomes[p] = replace(cur, failed=cur.failed + 1,
                                        consecutive_failures=cur.consecutive_failures + 1, last_failure_ms=now)

    def note_throttled(self, priority: RestPriority | None) -> None:
        """Record a throttle answer at ``priority``: counted apart, it leaves the success / failure counters and
        the outage verdict alone (a throttle our own callers caused is no broker outage)."""
        p = priority if priority is not None else current_rest_priority()
        cur = self._outcomes[p]
        self._outcomes[p] = replace(cur, throttled=cur.throttled + 1, last_throttled_ms=self._wall_ms())

    def mark_sent(self) -> None:
        """Stamp a request leaving now on this bucket's wall clock (see :func:`rest_sent_ms`)."""
        mark_rest_sent(self._wall_ms())

    # --- the BARS cap -------------------------------------------------------------------------------
    def _bars_admitted(self) -> int:
        cutoff = self._monotonic() - BARS_WINDOW_S
        window = self._bars_window
        while window and window[0] <= cutoff:
            window.popleft()
        return len(window)

    def _admit_bars(self) -> float:
        """Count one BARS call against the cap (by its admit time), or refuse it at once at the cap."""
        if self._bars_admitted() >= self.bars_per_min:
            self._bars_capped += 1
            self._refused += 1
            raise TransientBrokerError(
                f"{self.adapter_id} REST BARS cap reached: {self.bars_per_min} bar reads per minute")
        admitted = self._monotonic()
        self._bars_window.append(admitted)
        return admitted

    def _release_bars(self, admitted: float) -> None:
        """A BARS call that never got its token gives its place under the cap back."""
        with contextlib.suppress(ValueError):
            self._bars_window.remove(admitted)

    # --- taking tokens ------------------------------------------------------------------------------
    def spend(self, priority: RestPriority | None = None, *, stamp: bool = True) -> None:
        """Take one token now without waiting (lease-checked session calls). The bucket may go into debt
        (down to -capacity); the debt delays later callers instead of this one. Not subject to the BARS cap.
        ``stamp=False``: the caller stamps the send itself (:meth:`mark_sent`)."""
        p = priority if priority is not None else current_rest_priority()
        self._refill()
        self._tokens = max(-float(self.capacity), self._tokens - 1.0)
        self._requests[p] += 1
        if stamp:
            self.mark_sent()

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

    async def acquire(self, priority: RestPriority | None = None, *, stamp: bool = True) -> None:
        """Wait for a token at ``priority`` (default: the caller's :func:`rest_priority`). A ``BARS`` call over
        the per-minute cap is refused at once. A taken token sets the send-time stamp (:func:`rest_sent_ms`)
        unless ``stamp=False`` (a REST layer that stamps right before its send, :meth:`mark_sent`)."""
        p = priority if priority is not None else current_rest_priority()
        bars_admit = self._admit_bars() if p is RestPriority.BARS else None
        taken = False
        try:
            await self._wait_for_token(p)
            taken = True
        finally:
            if bars_admit is not None and not taken:
                self._release_bars(bars_admit)
        if stamp:
            self.mark_sent()

    async def _wait_for_token(self, p: RestPriority) -> None:
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
