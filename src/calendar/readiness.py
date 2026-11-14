"""PRE_OPEN readiness check (operations.md §10): at open - 10 min, after each roll, and after a
maintenance window during which an open (or a roll, whose check is deferred to the window's end) passed.

For each adapter: reset backoff and reconnect, take a fresh account snapshot and `fetch_cash_flows`,
confirm tokens are valid for more than 24 h and chrony is synced, and page if the adapter is not
STREAMING. `ReadinessDispatcher.request()` is synchronous (it is MarketCalendar's `on_readiness`
hook); the async `run()` loop performs the checks, one in flight per adapter.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Final, Protocol

log = logging.getLogger(__name__)

TOKEN_MIN_VALIDITY_S: Final = 24 * 3_600
MAINTENANCE_END_PREFIX: Final = "maintenance ended"   # MarketCalendar's end-of-window readiness reasons
AFTER_MAINTENANCE_WAIT_S: Final = 60.0                # reconnect + SNAPSHOTTING allowed after a window ends
STREAMING_POLL_S: Final = 1.0


class ReadinessActions(Protocol):
    """What the readiness check needs from the ingestion side (wired by the runtime)."""

    async def reset_backoff_and_reconnect(self, adapter_id: str) -> None: ...

    async def refresh_account_snapshot(self, adapter_id: str) -> None: ...

    async def fetch_cash_flows(self, adapter_id: str) -> None: ...

    def token_valid_for_s(self, adapter_id: str) -> float | None:
        """Seconds of token validity left (None: the adapter has no expiring token)."""
        ...

    def chrony_synced(self) -> bool: ...

    def adapter_streaming(self, adapter_id: str) -> bool: ...

    def page(self, severity: str, title: str, body: str) -> None: ...


@dataclass
class ReadinessReport:
    adapter_id: str
    reason: str
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems


async def _await_streaming(actions: ReadinessActions, adapter_id: str, wait_s: float,
                           sleep: Callable[[float], Awaitable[None]]) -> None:
    """Bounded wait for the reconnect to reach STREAMING (polled; returns at once when it already is)."""
    left = wait_s
    while left > 0 and not actions.adapter_streaming(adapter_id):
        step = min(STREAMING_POLL_S, left)
        await sleep(step)
        left -= step


async def readiness_check(actions: ReadinessActions, adapter_id: str, reason: str, *,
                          reconnect_wait_s: float = 0.0,
                          sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> ReadinessReport:
    """Run every readiness step; a failing step is recorded, the others still run. `reconnect_wait_s`: how long
    the reconnect may take to reach STREAMING before the snapshot step (used at the end of a maintenance window,
    when the adapter only starts reconnecting; 0 = check at once)."""
    report = ReadinessReport(adapter_id, reason)
    steps: tuple[tuple[str, Callable[[str], Awaitable[None]]], ...] = (
        ("reconnect", actions.reset_backoff_and_reconnect),
        ("account snapshot", actions.refresh_account_snapshot),
        ("cash flows", actions.fetch_cash_flows),
    )
    for name, step in steps:
        try:
            await step(adapter_id)
        except Exception as exc:
            report.problems.append(f"{name} failed: {exc!r}")
        if name == "reconnect" and reconnect_wait_s > 0:
            await _await_streaming(actions, adapter_id, reconnect_wait_s, sleep)
    left = actions.token_valid_for_s(adapter_id)
    if left is not None and left <= TOKEN_MIN_VALIDITY_S:
        report.problems.append(f"token valid for only {left / 3600:.1f} h")
    if not actions.chrony_synced():
        report.problems.append("chrony not synced")
    if not actions.adapter_streaming(adapter_id):
        report.problems.append("adapter not STREAMING")
    if report.problems:
        actions.page("critical", f"PRE_OPEN readiness failed: {adapter_id}",
                     f"{reason}: " + "; ".join(report.problems))
    return report


class ReadinessDispatcher:
    """Queues readiness requests from the calendar and runs them, one in flight per adapter. In `run()` the
    checks of different adapters run concurrently, so one adapter's bounded reconnect wait never delays another's
    check. A request whose reason starts with `MAINTENANCE_END_PREFIX` (the calendar's end-of-window re-run) waits
    up to `after_maintenance_wait_s` for the reconnect to reach STREAMING before it checks (7.2)."""

    def __init__(self, actions: ReadinessActions, *,
                 after_maintenance_wait_s: float = AFTER_MAINTENANCE_WAIT_S,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> None:
        self._actions = actions
        self._queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._inflight: set[str] = set()
        self._after_maintenance_wait_s = after_maintenance_wait_s
        self._sleep = sleep
        self._tasks: set[asyncio.Task[ReadinessReport]] = set()
        self.reports: list[ReadinessReport] = []

    def request(self, adapter_id: str, reason: str) -> None:
        if adapter_id in self._inflight:
            return
        self._inflight.add(adapter_id)
        self._queue.put_nowait((adapter_id, reason))

    def wait_for(self, reason: str) -> float:
        """The reconnect wait for a request with this reason."""
        return self._after_maintenance_wait_s if reason.startswith(MAINTENANCE_END_PREFIX) else 0.0

    async def _check(self, adapter_id: str, reason: str) -> ReadinessReport:
        try:
            report = await readiness_check(self._actions, adapter_id, reason,
                                           reconnect_wait_s=self.wait_for(reason), sleep=self._sleep)
        finally:
            self._inflight.discard(adapter_id)
        self.reports = (self.reports + [report])[-100:]
        return report

    async def run_once(self) -> ReadinessReport:
        adapter_id, reason = await self._queue.get()
        return await self._check(adapter_id, reason)

    def _finished(self, task: "asyncio.Task[ReadinessReport]") -> None:
        """A check that raised (outside its recorded steps, e.g. a failing page or chrony probe) is logged and
        paged, never lost silently in a detached task; its adapter is already free for the next request."""
        self._tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        adapter_id = task.get_name().removeprefix("readiness:")
        log.error("readiness check for %s raised %s", adapter_id, type(exc).__name__, exc_info=exc)
        try:
            self._actions.page("critical", f"PRE_OPEN readiness failed: {adapter_id}",
                               f"check raised {type(exc).__name__}")
        except Exception:
            log.exception("readiness failure page for %s failed", adapter_id)

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                getter = asyncio.ensure_future(self._queue.get())
                stopper = asyncio.ensure_future(stop.wait())
                done, _ = await asyncio.wait({getter, stopper}, return_when=asyncio.FIRST_COMPLETED)
                stopper.cancel()
                if getter not in done:
                    getter.cancel()
                    continue
                adapter_id, reason = getter.result()
                task = asyncio.create_task(self._check(adapter_id, reason), name=f"readiness:{adapter_id}")
                self._tasks.add(task)
                task.add_done_callback(self._finished)
        finally:
            for t in list(self._tasks):
                t.cancel()
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)
