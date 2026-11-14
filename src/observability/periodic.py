"""Periodic metric readers (IMPLEMENTATION_PLAN 5.6): I/O happens here, off the scrape path.

A `PeriodicReader` runs `read()` every `interval_s` (each read bounded by `timeout_s`) and caches the samples
it returned. Its collector returns the cache only while it is younger than `max_age_s`; after a failed or
hung read the old values age out and the series become **absent** (never a stale value presented as
current, never a fake 0).
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Sequence

from src.observability.metrics import Sample

log = logging.getLogger(__name__)


class PeriodicReader:
    def __init__(self, name: str, read: Callable[[], Awaitable[Sequence[Sample]]], *, interval_s: float = 5.0,
                 timeout_s: float = 4.0, max_age_s: float | None = None,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        self.name = name
        self._read = read
        self.interval_s = interval_s
        self.timeout_s = timeout_s
        self.max_age_s = max_age_s if max_age_s is not None else 3 * interval_s
        self._mono = monotonic
        self._samples: list[Sample] = []
        self._at: float | None = None
        self.failures = 0
        self.reads = 0

    async def read_once(self) -> bool:
        try:
            samples = await asyncio.wait_for(self._read(), self.timeout_s)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failures += 1
            lvl = logging.WARNING if self.failures in (1, 10) or self.failures % 100 == 0 else logging.DEBUG
            log.log(lvl, "metrics reader %s failed (%d in a row): %s", self.name, self.failures, type(exc).__name__)
            return False
        self.failures = 0
        self.reads += 1
        self._samples = list(samples)
        self._at = self._mono()
        return True

    def collect(self) -> list[Sample]:
        if self._at is None or self._mono() - self._at > self.max_age_s:
            return []
        return list(self._samples)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.read_once()
            try:
                await asyncio.wait_for(stop.wait(), self.interval_s)
            except TimeoutError:
                pass


__all__ = ["PeriodicReader"]
