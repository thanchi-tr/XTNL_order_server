"""Node-level DLQ spill drain outside a leader term (tier-persistence.md §5; operations.md §4, §9).

Any node delivers its own spill file into `events_dlq` through the idempotent spill-drain insert (named
exception 2, `SpillOnlySink`): at process start, and again as soon as PostgreSQL accepts writes, not only at
start-up, so an ex-leader that spilled in DRAINING and went to STANDBY still delivers its rows.

* `drain_at_startup()` runs once before the NodeController starts. It goes through the integrity
  `SpillDrainService`, which samples the `<file>.torn` size **before** the spooler reads the file, so a
  torn tail a SIGKILL left is paged (`spill_torn_tail`) by the start-up drain that moves it aside.
* `start()` / `stop()` are the NodeController's `StandbyWork`: the loop runs only while the node holds no
  leader term, because the term's own DlqSpooler appends to the same file. `stop()` asks the loop to
  finish its current attempt (bounded) before cancelling it; a cancelled attempt never truncates the file,
  so no row is lost, and the idempotent insert makes a replay harmless.
* After DRAINING (IMPLEMENTATION_PLAN 4.9) the NodeController starts the loop with `delay_s` = one spill period
  (a spill append DRAINING stopped waiting for may still be writing in a worker thread; reading the file
  meanwhile could take its half-written record for a torn tail) and with `torn_baseline` = the `.torn` size
  the term's own drain service last saw, so a tail that service already paged is not paged again, while a
  tail moved aside after it still is. It also waits while the term's DRAINING flush is still running
  (`busy`, EngineSession.spill_busy): a spill append that outlives the DRAINING budget must not land between
  this drain's read and its truncate.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from pathlib import Path

from src.integrity.ports import IntegrityAlert
from src.integrity.spill import SpillDrainService
from src.ports import DlqSink
from src.pubsub.spooler import DlqSpooler
from src.runtime.supervisor import Pager

log = logging.getLogger(__name__)

BUSY_POLL_S = 0.1     # how often the standby drain re-checks an ended term's DRAINING flush
BUSY_WARN_S = 10.0    # log once when that flush is still running after this long


class _NodeSpillService(SpillDrainService):
    """SpillDrainService whose torn-tail baseline can be reset when a leader term ends."""

    def set_torn_baseline(self, torn_bytes: int) -> None:
        self._torn_bytes = max(0, int(torn_bytes))


class NodeSpillDrain:
    def __init__(self, sink: DlqSink, spill_path: Path, *, node: str, clock_ms: Callable[[], int],
                 pager: Pager | None = None, attempt_timeout_s: float = 5.0, idle_s: float = 5.0,
                 retry_s: float = 1.0, max_retry_s: float = 30.0, stop_grace_s: float = 2.0) -> None:
        self.spooler = DlqSpooler(sink, spill_path=spill_path)
        self._pager = pager
        self.service = _NodeSpillService(self.spooler, node=node, clock_ms=clock_ms, pager=self._page)
        self._attempt_timeout_s = attempt_timeout_s
        self._idle_s = idle_s
        self._retry_s = retry_s
        self._max_retry_s = max_retry_s
        self._stop_grace_s = stop_grace_s
        self._task: asyncio.Task[None] | None = None
        self._stop: asyncio.Event | None = None
        self.attempts = 0

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def _page(self, alert: IntegrityAlert) -> None:
        if self._pager is None:
            log.error("PAGE[%s] integrity:%s: %s", alert.severity, alert.code, alert.detail)
            return
        self._pager.page(alert.severity, f"integrity:{alert.code}", alert.detail)

    async def _attempt(self, first: bool) -> int | None:
        self.attempts += 1
        aw = self.service.drain_at_startup() if first else self.service.drain_once()
        try:
            return await asyncio.wait_for(aw, self._attempt_timeout_s)
        except Exception as exc:   # TimeoutError included: the file is truncated only after a full insert
            log.warning("spill drain attempt failed: %s", type(exc).__name__)
            return None

    async def drain_at_startup(self) -> int | None:
        """Any node, at process start: deliver what a crash left. Never raises; None = PostgreSQL did not
        accept it (the standby loop or the leader's drain task retries)."""
        return await self._attempt(first=True)

    async def run(self, stop: asyncio.Event, delay_s: float = 0.0, busy: Callable[[], bool] | None = None) -> None:
        """Retry while the file is non-empty or unchecked; capped backoff while the sink is down. The first
        attempt waits `delay_s`, then while `busy()` (the ended term's DRAINING flush may still append to the
        file); both waits end early on stop."""
        if delay_s > 0:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), delay_s)
        waited = 0.0
        warned = False
        while busy is not None and not stop.is_set() and busy():
            if waited >= BUSY_WARN_S and not warned:
                warned = True
                log.warning("standby spill drain: the last term's DRAINING flush is still running after %.0f s; "
                            "waiting before reading %s", waited, self.spooler.spill_path)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), BUSY_POLL_S)
            waited += BUSY_POLL_S
        delay = self._retry_s
        while not stop.is_set():
            delivered = await self._attempt(first=False)
            if delivered is None:
                wait = delay
                delay = min(delay * 2, self._max_retry_s)
            else:
                wait = self._idle_s
                delay = self._retry_s
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), wait)

    # --- StandbyWork --------------------------------------------------------------------------------------
    def start(self, *, delay_s: float = 0.0, torn_baseline: int | None = None,
              busy: Callable[[], bool] | None = None) -> None:
        if self.running:
            return
        if torn_baseline is not None:
            self.service.set_torn_baseline(torn_baseline)
        self._stop = asyncio.Event()
        self._task = asyncio.get_running_loop().create_task(self.run(self._stop, delay_s, busy),
                                                            name="standby-spill-drain")

    async def stop(self) -> None:
        task, stop = self._task, self._stop
        self._task, self._stop = None, None
        if task is None:
            return
        if stop is not None:
            stop.set()
        done, _ = await asyncio.wait({task}, timeout=self._stop_grace_s)
        if not done:
            task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise                         # the caller itself is being cancelled: never swallow it
        except Exception:
            log.exception("standby spill drain ended with an error")
