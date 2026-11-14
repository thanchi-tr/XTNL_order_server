"""UptimeTracker: when was this engine process on (every node, any SM9 role; migration 0012).

The engine runs on a PC that is not on 24/7, so it records its own uptime:

* at start: an `engine_runs` row (run_id, node, started_at_ms);
* a light heartbeat every XTNL_UPTIME_PING_S (60..1800 s, default 900) updating `last_seen_ms`, and, while this
  node is the LEADER, `adapter_coverage.last_streaming_ms` for every adapter that is STREAMING, reconciled and
  whose downtime catch-up has finished (an adapter that becomes eligible is written at once, not only at the next
  heartbeat). The next start-up catch-up reads from there (src/runtime/catchup.py);
* at a clean stop (the DRAINING / SIGINT / SIGTERM path): `stopped_at_ms` + `end_reason`. A hard exit (fence,
  watchdog) or a power loss leaves the row open: its `last_seen_ms` is the last proof of life.

**Sleep detection.** A tick runs every `tick_s` (5 s). Between ticks the wall-clock delta and the monotonic delta
are compared: a wall-clock jump larger than max(2 x tick, 120 s) that monotonic time does not match (the
monotonic clock stood still while the PC slept), or both deltas far larger than the tick (the process did not run:
suspended, or the event loop stalled that long), means the process was suspended. Then `suspended_gaps` is
incremented, the heartbeat is written at once, and `on_suspended` asks the leader's session to force SM4
RESYNCING and run a `resume_after_sleep` catch-up for every adapter.

Supervised: every step catches its own errors (logged, retried on the next tick); the loop restarts with backoff
if it dies. Node telemetry, written without the epoch fence (src/db/uptime_store.py). Never logs an amount.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Final, Protocol

from src.observability.metrics import Sample, gauge

log = logging.getLogger(__name__)

PING_VAR: Final = "XTNL_UPTIME_PING_S"
DEFAULT_PING_S: Final = 900
MIN_PING_S: Final = 60
MAX_PING_S: Final = 1800
DEFAULT_TICK_S: Final = 5.0
MIN_SUSPEND_GAP_S: Final = 120.0


def ping_s_from_env(environ: Mapping[str, str] | None = None) -> int:
    """XTNL_UPTIME_PING_S: whole seconds in 60..1800 (default 900). Raises ValueError (name only)."""
    e = os.environ if environ is None else environ
    raw = (e.get(PING_VAR) or "").strip()
    if not raw:
        return DEFAULT_PING_S
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{PING_VAR} must be a whole number of seconds") from None
    if not MIN_PING_S <= value <= MAX_PING_S:
        raise ValueError(f"{PING_VAR} must be between {MIN_PING_S} and {MAX_PING_S} s")
    return value


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(frozen=True)
class SuspendEvent:
    at_ms: int          # wall time the process noticed it
    wall_gap_ms: int    # wall-clock delta since the previous tick
    mono_gap_ms: int    # monotonic delta since the previous tick
    kind: str           # 'wall_jump' (monotonic stood still) | 'stalled' (both far beyond the tick)


class SleepDetector:
    """Compares wall-clock and monotonic deltas between consecutive observations (see the module doc)."""

    def __init__(self, tick_s: float = DEFAULT_TICK_S) -> None:
        self.tick_ms = int(tick_s * 1000)
        self.threshold_ms = int(max(2 * tick_s, MIN_SUSPEND_GAP_S) * 1000)
        self._last: tuple[int, float] | None = None

    def reset(self, wall_ms: int, mono_s: float) -> None:
        self._last = (wall_ms, mono_s)

    def observe(self, wall_ms: int, mono_s: float) -> SuspendEvent | None:
        last, self._last = self._last, (wall_ms, mono_s)
        if last is None:
            return None
        dw = wall_ms - last[0]
        dm = int((mono_s - last[1]) * 1000)
        if dw - dm > self.threshold_ms:
            return SuspendEvent(wall_ms, dw, dm, "wall_jump")
        if min(dw, dm) - self.tick_ms > self.threshold_ms:
            return SuspendEvent(wall_ms, dw, dm, "stalled")
        return None


class UptimeStore(Protocol):
    """src/db/uptime_store.py PgUptimeStore."""

    async def start_run(self, run_id: str, node_id: str, at_ms: int) -> None: ...

    async def heartbeat(self, run_id: str, at_ms: int) -> bool: ...

    async def note_suspended(self, run_id: str, at_ms: int) -> None: ...

    async def stop_run(self, run_id: str, at_ms: int, reason: str) -> None: ...

    async def advance_coverage(self, epoch_token: int, adapter_id: str, at_ms: int, run_id: str | None) -> bool: ...


#: () -> (epoch token, adapter ids eligible for coverage) while this node is the LEADER, else None.
CoverageTargets = Callable[[], "tuple[int, Sequence[str]] | None"]


class UptimeTracker:
    def __init__(self, store: UptimeStore, node_id: str, *, ping_s: int = DEFAULT_PING_S,
                 tick_s: float = DEFAULT_TICK_S, clock_ms: Callable[[], int] = _wall_ms,
                 monotonic: Callable[[], float] = time.monotonic, coverage: CoverageTargets | None = None,
                 on_suspended: Callable[[SuspendEvent], None] | None = None, run_id: str | None = None,
                 stop_timeout_s: float = 3.0) -> None:
        if not MIN_PING_S <= ping_s <= MAX_PING_S:
            raise ValueError(f"{PING_VAR} must be between {MIN_PING_S} and {MAX_PING_S} s")
        self._store = store
        self.node_id = node_id
        self.ping_s = ping_s
        self.tick_s = tick_s
        self._clock = clock_ms
        self._mono = monotonic
        self.coverage = coverage
        self.on_suspended = on_suspended
        self.run_id = run_id or str(uuid.uuid4())
        self._stop_timeout_s = stop_timeout_s
        self.started_ms = clock_ms()
        self.detector = SleepDetector(tick_s)
        self.detector.reset(self.started_ms, monotonic())
        self.suspended_gaps = 0
        self._suspend_writes = 0              # suspended gaps not yet written to engine_runs
        self.row_written = False
        self.last_seen_ms: int | None = None
        self._last_beat_mono: float | None = None
        self._covered: dict[str, int] = {}    # adapter -> last coverage written by this run
        self.stop_reason: str | None = None
        self.stopped = False
        self.liveness_ms: int | None = None

    # ------------------------------------------------------------------ metrics (no I/O)
    def samples(self) -> list[Sample]:
        out = gauge("xtnl_engine_uptime_seconds", max(0, self._clock() - self.started_ms) / 1000.0)
        out.append(Sample("xtnl_suspended_gaps_total", float(self.suspended_gaps), {}))
        return out

    def note_stop_reason(self, reason: str) -> None:
        """The first reason wins (a SIGINT followed by the drain's own stop stays 'SIGINT')."""
        if self.stop_reason is None:
            self.stop_reason = reason

    # ------------------------------------------------------------------ task
    async def run(self, stop: asyncio.Event) -> None:
        """Until `stop`: the loop restarts with backoff if it ever dies (never takes the engine down)."""
        delay = 1.0
        while not stop.is_set():
            try:
                await self._loop(stop)
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("uptime tracker loop failed; restarting in %.0f s", delay)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), delay)
            delay = min(delay * 2, 60.0)

    async def _loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.tick()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), self.tick_s)

    async def tick(self) -> None:
        """One observation: sleep detection, the start row (retried until written), the heartbeat when due,
        coverage for newly eligible adapters. Each write's failure is logged and retried on a later tick."""
        now, mono = self._clock(), self._mono()
        self.liveness_ms = now
        event = self.detector.observe(now, mono)
        forced = False
        if event is not None:
            self.suspended_gaps += 1
            self._suspend_writes += 1
            forced = True
            log.warning("process was suspended (%s: wall %d s, monotonic %d s since the previous tick); "
                        "suspended gaps this run: %d", event.kind, event.wall_gap_ms // 1000,
                        event.mono_gap_ms // 1000, self.suspended_gaps)
            if self.on_suspended is not None:
                try:
                    self.on_suspended(event)
                except Exception:
                    log.exception("resume-after-sleep hook failed")
        if not self.row_written:
            try:
                await self._store.start_run(self.run_id, self.node_id, self.started_ms)
                self.row_written = True
                log.info("engine run %s started on %s", self.run_id, self.node_id)
            except Exception as exc:
                log.warning("engine_runs start row not written (%s); retrying", type(exc).__name__)
                return
        if self._suspend_writes:
            try:
                for _ in range(self._suspend_writes):
                    await self._store.note_suspended(self.run_id, now)
                    self._suspend_writes -= 1
            except Exception as exc:
                log.warning("engine_runs suspended gap not written (%s); retrying", type(exc).__name__)
        due = self._last_beat_mono is None or mono - self._last_beat_mono >= self.ping_s
        if due or forced:
            await self.heartbeat(now, mono)
        else:
            await self._coverage(now, only_new=True)

    async def heartbeat(self, now: int | None = None, mono: float | None = None) -> bool:
        now = self._clock() if now is None else now
        mono = self._mono() if mono is None else mono
        try:
            if not await self._store.heartbeat(self.run_id, now):
                self.row_written = False          # the start row is missing: written again on the next tick
                return False
        except Exception as exc:
            log.warning("engine_runs heartbeat not written (%s)", type(exc).__name__)
            return False
        self._last_beat_mono = mono
        self.last_seen_ms = now
        await self._coverage(now, only_new=False)
        return True

    async def _coverage(self, now: int, *, only_new: bool) -> None:
        if self.coverage is None:
            return
        try:
            target = self.coverage()
        except Exception:
            log.exception("coverage targets unreadable")
            return
        if target is None:
            self._covered.clear()
            return
        epoch, adapters = target
        for gone in set(self._covered) - set(adapters):
            del self._covered[gone]           # eligible again later (e.g. after a resume catch-up): written at once
        for aid in adapters:
            if only_new and aid in self._covered:
                continue
            try:
                if await self._store.advance_coverage(epoch, aid, now, self.run_id):
                    self._covered[aid] = now
                else:
                    log.warning("%s: adapter_coverage not advanced (epoch %d is no longer current)", aid, epoch)
            except Exception as exc:
                log.warning("%s: adapter_coverage not written (%s)", aid, type(exc).__name__)

    async def stop(self, reason: str | None = None) -> None:
        """Clean stop: `stopped_at_ms` + `end_reason` (bounded; a failure is logged). Idempotent."""
        if self.stopped:
            return
        self.stopped = True
        if reason is not None:
            self.note_stop_reason(reason)
        why = self.stop_reason or "stopped"
        now = self._clock()
        try:
            if not self.row_written:
                await asyncio.wait_for(self._store.start_run(self.run_id, self.node_id, self.started_ms),
                                       self._stop_timeout_s)
                self.row_written = True
            await asyncio.wait_for(self._store.stop_run(self.run_id, now, why), self._stop_timeout_s)
            log.warning("engine run %s stopped (%s)", self.run_id, why)
        except Exception as exc:
            log.error("engine_runs stop not written (%s)", type(exc).__name__)


__all__ = ["DEFAULT_PING_S", "DEFAULT_TICK_S", "MAX_PING_S", "MIN_PING_S", "PING_VAR", "CoverageTargets",
           "SleepDetector", "SuspendEvent", "UptimeStore", "UptimeTracker", "ping_s_from_env"]
