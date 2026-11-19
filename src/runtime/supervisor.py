"""TaskSupervisor and the event-loop watchdog (operations.md §3 "Task supervision").

* Every long-running task is restarted with exponential backoff if it dies (raises or returns).
* Liveness: each task stamps against its own maximum stamp age. While it awaits PostgreSQL, sleeps in
  SM4 BACKOFF, sits in AUTH_FAILED or waits for SM8 READY / LEADER_INIT it marks itself exempt
  (`ctx.exempt(reason)` / `ctx.set_exempt(reason)`) and is neither restarted nor counted as a death.
  A stale stamp pages and restarts that task; it never kills the process.
* More than N deaths in 10 min opens a per-task circuit: no more restarts, quarantine the input it was
  processing, page critical, report `degraded: <task>`. The circuit half-opens for one retry every
  10 min and closes after a clean run; an audited operator `reset_task()` closes it at once.
* Only leadership-critical tasks (the ingestion orchestrator, the lease-keeper link) hard-exit instead.
* A half-open retry that dies keeps the circuit open and calls the task's `on_circuit_reopen(name)` (counted in
  `reopen_count`, per incident: reset when the circuit closes), so an owner can budget repeated failures (design
  RT-M3); `has(name)` lets optional services
  add tasks idempotently (RT-M2).
* LoopWatchdog: a 1 s event-loop heartbeat that measures loop lag (alert at 100 ms) and feeds systemd
  `WATCHDOG=1` only while the lease (if held) is valid, so systemd's SIGKILL covers loop lag and
  lease expiry (§3).
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Final, Protocol

from src.runtime.exits import ExitFn, hard_exit
from src.runtime.sdnotify import SdNotifier

log = logging.getLogger(__name__)

DEATH_WINDOW_S: Final = 600.0
HALF_OPEN_AFTER_S: Final = 600.0
LOOP_LAG_ALERT_S: Final = 0.1


class Pager(Protocol):
    def page(self, severity: str, title: str, body: str) -> None: ...


class LogPager:
    """Fallback pager (the coordinator wires the notification outbox / off-node channel)."""

    def page(self, severity: str, title: str, body: str) -> None:
        log.error("PAGE[%s] %s: %s", severity, title, body)


class TaskContext:
    """Handed to each task: liveness stamping and exempt waits."""

    def __init__(self, name: str, clock: Callable[[], float]) -> None:
        self.name = name
        self._clock = clock
        self.last_stamp = clock()
        self._exempt_depth = 0
        self.exempt_reason: str | None = None
        self._standing_exempt: str | None = None

    def stamp(self) -> None:
        self.last_stamp = self._clock()

    @property
    def exempt(self) -> bool:
        return self._exempt_depth > 0 or self._standing_exempt is not None

    @contextmanager
    def exempt_wait(self, reason: str) -> Iterator[None]:
        """`with ctx.exempt_wait("postgres"): await conn.fetch(...)`: alive while awaiting."""
        self._exempt_depth += 1
        self.exempt_reason = reason
        try:
            yield
        finally:
            self._exempt_depth -= 1
            if self._exempt_depth == 0:
                self.exempt_reason = self._standing_exempt
            self.stamp()

    def set_exempt(self, reason: str | None) -> None:
        """Standing exemption, e.g. 'BACKOFF', 'AUTH_FAILED', 'awaiting SM8'; None clears it."""
        self._standing_exempt = reason
        self.exempt_reason = reason if self._exempt_depth == 0 else self.exempt_reason
        self.stamp()


TaskFactory = Callable[[TaskContext], Awaitable[None]]


@dataclass(frozen=True)
class TaskSpec:
    name: str
    factory: TaskFactory
    max_stamp_age_s: float
    critical: bool = False
    max_deaths: int = 5
    death_window_s: float = DEATH_WINDOW_S
    half_open_after_s: float = HALF_OPEN_AFTER_S
    clean_run_s: float = 60.0
    backoff_base_s: float = 1.0
    backoff_max_s: float = 60.0
    on_quarantine: Callable[[], None] | None = None
    # Called with the task name when a half-open retry dies and the circuit stays open (RT-M3); an error in it is
    # logged and never changes the supervision.
    on_circuit_reopen: Callable[[str], None] | None = None


@dataclass
class _Runtime:
    spec: TaskSpec
    ctx: TaskContext | None = None
    task: asyncio.Task[None] | None = None
    started_at: float = 0.0
    deaths: deque[float] = field(default_factory=deque)
    consecutive: int = 0
    restart_at: float = 0.0
    circuit_open_since: float | None = None
    next_half_open: float | None = None
    half_open: bool = False
    restarts: int = 0
    reopens: int = 0
    last_error: str | None = None


class TaskSupervisor:
    def __init__(self, specs: Sequence[TaskSpec], *, clock: Callable[[], float] = time.monotonic,
                 exit_fn: ExitFn = hard_exit, pager: Pager | None = None,
                 audit: Callable[[str, str, str], None] | None = None, poll_interval_s: float = 0.5) -> None:
        names = [s.name for s in specs]
        if len(set(names)) != len(names):
            raise ValueError("task names must be unique")
        self._rt = {s.name: _Runtime(s) for s in specs}
        self._clock = clock
        self._exit = exit_fn
        self._pager: Pager = pager or LogPager()
        self._audit = audit or (lambda op, name, who: log.warning("AUDIT %s %s by %s", op, name, who))
        self._poll = poll_interval_s
        self._zombies: set[asyncio.Task[None]] = set()
        self._stopping = False

    # --- lifecycle ---------------------------------------------------------------------------------
    def _spawn(self, rt: _Runtime) -> None:
        ctx = TaskContext(rt.spec.name, self._clock)
        rt.ctx = ctx
        rt.started_at = self._clock()
        rt.task = asyncio.create_task(self._wrap(rt.spec, ctx), name=f"sup:{rt.spec.name}")

    @staticmethod
    async def _wrap(spec: TaskSpec, ctx: TaskContext) -> None:
        await spec.factory(ctx)

    def add(self, spec: TaskSpec, *, start: bool = True) -> None:
        """Register (and start) a task after construction, e.g. adapters spawned by LEADER_INIT step 6."""
        if spec.name in self._rt:
            raise ValueError(f"task {spec.name} already supervised")
        rt = _Runtime(spec)
        self._rt[spec.name] = rt
        if start and not self._stopping:
            self._spawn(rt)

    def start(self) -> None:
        now = self._clock()
        for rt in self._rt.values():
            if rt.task is None and rt.circuit_open_since is None and rt.restart_at <= now:
                self._spawn(rt)

    async def run(self, stop: asyncio.Event) -> None:
        self.start()
        try:
            while not stop.is_set():
                self.poll()
                try:
                    await asyncio.wait_for(stop.wait(), self._poll)
                except TimeoutError:
                    pass
        finally:
            await self.stop_all()

    async def stop_all(self) -> None:
        self._stopping = True
        tasks = [rt.task for rt in self._rt.values() if rt.task is not None] + list(self._zombies)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for rt in self._rt.values():
            rt.task = None
        self._zombies.clear()

    # --- supervision -------------------------------------------------------------------------------
    def poll(self) -> None:
        """One supervision pass: deaths, stale stamps, restarts, half-open probes, clean runs."""
        if self._stopping:
            return
        now = self._clock()
        for rt in self._rt.values():
            task = rt.task
            if task is not None and task.done():
                self._on_death(rt, now, self._describe(task))
            elif task is not None and rt.ctx is not None:
                if not rt.ctx.exempt and now - rt.ctx.last_stamp > rt.spec.max_stamp_age_s:
                    age = now - rt.ctx.last_stamp
                    self._pager.page("high", f"task {rt.spec.name} stale",
                                     f"no liveness stamp for {age:.1f} s (max {rt.spec.max_stamp_age_s} s); restarting")
                    task.cancel()
                    self._zombies.add(task)
                    task.add_done_callback(self._zombies.discard)
                    self._on_death(rt, now, f"stale stamp ({age:.1f} s)")
                elif now - rt.started_at >= rt.spec.clean_run_s:
                    if rt.half_open:
                        self._close_circuit(rt, "half-open retry ran clean")
                    rt.consecutive = 0
            if rt.task is None:
                if rt.circuit_open_since is None:
                    if now >= rt.restart_at:
                        rt.restarts += 1
                        self._spawn(rt)
                elif rt.next_half_open is not None and now >= rt.next_half_open:
                    rt.half_open = True
                    rt.next_half_open = None
                    log.warning("task %s circuit half-open: one retry", rt.spec.name)
                    self._spawn(rt)

    @staticmethod
    def _describe(task: asyncio.Task[None]) -> str:
        if task.cancelled():
            return "cancelled"
        exc = task.exception()
        return f"{type(exc).__name__}: {exc}" if exc is not None else "returned unexpectedly"

    def _on_death(self, rt: _Runtime, now: float, why: str) -> None:
        spec = rt.spec
        rt.task = None
        rt.ctx = None
        rt.last_error = why
        log.error("task %s died: %s", spec.name, why)
        if spec.critical:
            self._exit(f"critical_task: {spec.name}: {why}")
            return
        rt.deaths.append(now)
        while rt.deaths and now - rt.deaths[0] > spec.death_window_s:
            rt.deaths.popleft()
        if rt.half_open:
            rt.half_open = False
            rt.next_half_open = now + spec.half_open_after_s
            rt.reopens += 1
            if spec.on_circuit_reopen is not None:
                try:
                    spec.on_circuit_reopen(spec.name)
                except Exception:
                    log.exception("circuit-reopen hook of %s failed", spec.name)
            self._pager.page("critical", f"task {spec.name} circuit still open", f"half-open retry died: {why}")
            return
        if len(rt.deaths) > spec.max_deaths:
            rt.circuit_open_since = now
            rt.next_half_open = now + spec.half_open_after_s
            if spec.on_quarantine is not None:
                try:
                    spec.on_quarantine()
                except Exception:
                    log.exception("quarantine hook of %s failed", spec.name)
            self._pager.page("critical", f"task {spec.name} circuit open",
                             f"{len(rt.deaths)} deaths in {spec.death_window_s:.0f} s; last: {why}")
            return
        rt.consecutive += 1
        delay = min(spec.backoff_max_s, spec.backoff_base_s * 2 ** (rt.consecutive - 1))
        rt.restart_at = now + delay

    def _close_circuit(self, rt: _Runtime, why: str) -> None:
        log.warning("task %s circuit closed: %s", rt.spec.name, why)
        rt.circuit_open_since = None
        rt.next_half_open = None
        rt.half_open = False
        rt.deaths.clear()
        rt.consecutive = 0
        rt.reopens = 0                     # reopen_count is per incident: a closed circuit starts a fresh one

    # --- operator / queries ------------------------------------------------------------------------
    def reset_task(self, name: str, operator: str) -> None:
        """Audited operator action (operations.md §16): close the circuit and restart at once."""
        rt = self._rt[name]
        self._audit("reset_task", name, operator)
        self._close_circuit(rt, f"reset_task by {operator}")
        rt.restart_at = 0.0
        if rt.task is None and not self._stopping:
            self._spawn(rt)

    def degraded(self) -> list[str]:
        """Tasks whose circuit is open (dead-man payload `degraded`)."""
        return sorted(n for n, rt in self._rt.items() if rt.circuit_open_since is not None)

    def circuit_open_age_s(self, name: str) -> float | None:
        rt = self._rt.get(name)
        if rt is None or rt.circuit_open_since is None:
            return None
        return self._clock() - rt.circuit_open_since

    def has(self, name: str) -> bool:
        """True when a task of that name is registered (running, waiting to restart or with its circuit open)."""
        return name in self._rt

    def reopen_count(self, name: str) -> int:
        """Half-open retries of the task that died (each kept its circuit open) since its circuit last opened: the
        count of the current incident, reset to 0 whenever the circuit closes (a clean half-open run or an audited
        operator `reset_task`), so a budget built on it never counts failures from before an operator reset.
        0 for an unknown name."""
        rt = self._rt.get(name)
        return 0 if rt is None else rt.reopens

    def running(self, name: str) -> bool:
        rt = self._rt[name]
        return rt.task is not None and not rt.task.done()

    def context(self, name: str) -> TaskContext | None:
        return self._rt[name].ctx

    def deaths(self, name: str) -> int:
        return len(self._rt[name].deaths)

    def restarts(self, name: str) -> int:
        return self._rt[name].restarts


class LoopWatchdog:
    """1 s event-loop heartbeat: lag metric + sd_notify WATCHDOG=1 while the lease (if held) is valid."""

    def __init__(self, notifier: SdNotifier, *, lease_ok: Callable[[], bool] | None = None,
                 interval_s: float = 1.0, lag_alert_s: float = LOOP_LAG_ALERT_S,
                 on_lag: Callable[[float], None] | None = None, clock: Callable[[], float] = time.monotonic) -> None:
        self._notifier = notifier
        self._lease_ok = lease_ok
        self._interval = interval_s
        self._lag_alert = lag_alert_s
        self._on_lag = on_lag
        self._clock = clock
        self.last_lag_s = 0.0
        self.max_lag_s = 0.0
        self.beats = 0
        self.withheld = 0
        self._recent: deque[tuple[float, float]] = deque(maxlen=120)   # (monotonic, lag) per beat

    def recent_max_lag_s(self, window_s: float = 10.0) -> float | None:
        """Worst lag over the last `window_s` (xtnl_event_loop_lag_seconds); None before the first beat."""
        if not self._recent:
            return None
        cutoff = self._clock() - window_s
        lags = [lag for at, lag in self._recent if at >= cutoff]
        return max(lags) if lags else 0.0

    def beat(self, lag_s: float) -> None:
        self.last_lag_s = lag_s
        self.max_lag_s = max(self.max_lag_s, lag_s)
        self._recent.append((self._clock(), lag_s))
        if lag_s > self._lag_alert:
            log.warning("event-loop lag %.0f ms", lag_s * 1000)
            if self._on_lag is not None:
                self._on_lag(lag_s)
        if self._lease_ok is not None and not self._lease_ok():
            self.withheld += 1
            log.critical("lease_deadline passed: withholding WATCHDOG=1 (systemd SIGKILL)")
            return
        self.beats += 1
        self._notifier.notify("WATCHDOG=1")

    async def run(self, stop: asyncio.Event) -> None:
        self._notifier.notify("READY=1")
        expected = self._clock() + self._interval
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), max(0.0, expected - self._clock()))
                return
            except TimeoutError:
                pass
            now = self._clock()
            self.beat(max(0.0, now - expected))
            expected = now + self._interval
