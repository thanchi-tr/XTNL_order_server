"""Spill drain service (tier-persistence.md §5; operations.md §3, §4, §9).

Drains this node's DlqSpooler spill file into events_dlq through the spooler's own drain_spill() (idempotent
INSERT ... ON CONFLICT (payload_hash) DO NOTHING, the epoch-check exception, so ANY node may run it):
* at start-up (the DLQ start-up barrier; a parse error or a down database never fails LEADER_INIT);
* again as soon as the DLQ sink accepts writes, retried with a capped backoff while the file is non-empty.
A torn tail left by a SIGKILL is moved aside to <file>.torn by the spooler; this service pages when that
file grows. It exposes the spill size for the health record / dead-man payload and pages, naming the node,
when the file has been non-empty for more than 60 s.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

from src.integrity.ports import IntegrityAlert, Pager, null_pager
from src.pubsub.spooler import DlqSpooler

log = logging.getLogger(__name__)

NON_EMPTY_PAGE_AFTER_MS: Final = 60_000


@dataclass(frozen=True)
class SpillHealth:
    """Health-record / dead-man fields (operations.md §4, §9)."""

    node: str
    spill_bytes: int
    non_empty_since_ms: int | None
    torn_bytes: int


def _size(path: Path) -> int:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return 0


class SpillDrainService:
    def __init__(self, spooler: DlqSpooler, *, node: str, clock_ms: Callable[[], int], pager: Pager = null_pager,
                 retry_s: float = 1.0, max_retry_s: float = 30.0, idle_poll_s: float = 1.0) -> None:
        self._spooler = spooler
        self.node = node
        self._clock_ms = clock_ms
        self._pager = pager
        self._retry_s = retry_s
        self._max_retry_s = max_retry_s
        self._idle_poll_s = idle_poll_s
        self._non_empty_since: int | None = None
        self._paged_non_empty = False
        self._torn_bytes: int | None = None
        self.drained_rows = 0
        self.liveness_ms: int | None = None

    @property
    def torn_path(self) -> Path:
        p = self._spooler.spill_path
        return p.with_name(p.name + ".torn")

    def spill_size(self) -> int:
        """Cached spill-file size in bytes (no I/O)."""
        return self._spooler.spill_bytes

    def health(self) -> SpillHealth:
        return SpillHealth(self.node, self.spill_size(), self._non_empty_since, self._torn_bytes or 0)

    async def drain_at_startup(self) -> int | None:
        """Start-up barrier: drain whatever a crash left. Never raises. None = the database is still down."""
        await self._note_torn_baseline()
        return await self.drain_once()

    async def drain_once(self) -> int | None:
        """One drain attempt. Returns rows delivered, or None when the sink is still unavailable."""
        try:
            delivered = await self._spooler.drain_spill()
        except Exception:
            log.exception("spill drain failed on %s", self.node)
            delivered = None
        await self._check_torn()
        self._track_non_empty()
        if delivered:
            self.drained_rows += delivered
            log.info("spill drain on %s: %d rows into events_dlq", self.node, delivered)
        return delivered

    async def run(self, stop: asyncio.Event) -> None:
        """Keep draining while the spill file is non-empty; back off (capped) while the sink is down."""
        delay = self._retry_s
        while not stop.is_set():
            wait = self._idle_poll_s
            if self.spill_size() > 0:
                if await self.drain_once() is None:
                    wait = delay
                    delay = min(delay * 2, self._max_retry_s)
                else:
                    delay = self._retry_s
            else:
                self._track_non_empty()
            self.liveness_ms = self._clock_ms()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), wait)

    # ---- internals ------------------------------------------------------------------------------------
    def _track_non_empty(self) -> None:
        now = self._clock_ms()
        if self.spill_size() <= 0:
            self._non_empty_since = None
            self._paged_non_empty = False
            return
        if self._non_empty_since is None:
            self._non_empty_since = now
        if not self._paged_non_empty and now - self._non_empty_since > NON_EMPTY_PAGE_AFTER_MS:
            self._paged_non_empty = True
            self._page("critical", "spill_file_non_empty",
                       f"node {self.node}: DLQ spill file non-empty for {now - self._non_empty_since} ms "
                       f"({self.spill_size()} bytes)")

    async def _note_torn_baseline(self) -> None:
        if self._torn_bytes is None:
            self._torn_bytes = await asyncio.to_thread(_size, self.torn_path)

    async def _check_torn(self) -> None:
        size = await asyncio.to_thread(_size, self.torn_path)
        before = self._torn_bytes if self._torn_bytes is not None else 0
        if size > before:
            self._page("high", "spill_torn_tail",
                       f"node {self.node}: torn spill tail of {size - before} bytes moved aside to {self.torn_path}")
        self._torn_bytes = size

    def _page(self, severity: Literal["info", "high", "critical"], code: str, detail: str) -> None:
        log.warning("%s: %s", code, detail)
        self._pager(IntegrityAlert(severity, code, detail, self._clock_ms(), {"node": self.node}))
