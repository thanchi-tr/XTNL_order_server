"""Ingestion pages (operations.md §9, §13; adapter-isolation.md §5; dto-contracts.md §2, §4).

The orchestrator and the topic consumers page through an optional `Pager` callable
`(severity, title, body)`, the same shape as `EngineSession.page` (the 5.6 wiring passes it). Pages are
synchronous and never raise into the caller: they run on the parser path, so a failing pager is logged and
ignored. `PageThrottle` keeps a storm (thousands of parked rows) down to one page per key and interval, with
the number suppressed in between carried by the next page; `Episodes` pages once when a condition starts
and re-arms when it clears.
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Final

log = logging.getLogger(__name__)

Pager = Callable[[str, str, str], None]          # (severity, title, body)

CRITICAL: Final = "critical"
HIGH: Final = "high"
INFO: Final = "info"


def send_page(pager: Pager | None, severity: str, title: str, body: str) -> bool:
    """Call the pager (if any); never raises. True when a pager accepted it."""
    if pager is None:
        return False
    try:
        pager(severity, title, body)
    except Exception:
        log.exception("pager failed for %s", title)
        return False
    return True


class PageThrottle:
    """At most one page per key per `interval_ms`; later ones are counted and reported with the next."""

    def __init__(self, pager: Pager | None, clock_ms: Callable[[], int], interval_ms: int = 60_000) -> None:
        self._pager = pager
        self._clock_ms = clock_ms
        self._interval_ms = interval_ms
        self._last: dict[str, int] = {}
        self._suppressed: dict[str, int] = {}
        self.sent = 0

    def page(self, key: str, severity: str, title: str, body: str) -> bool:
        now = self._clock_ms()
        last = self._last.get(key)
        if last is not None and now - last < self._interval_ms:
            self._suppressed[key] = self._suppressed.get(key, 0) + 1
            return False
        self._last[key] = now
        extra = self._suppressed.pop(key, 0)
        if extra:
            body = f"{body} (+{extra} more since the last page)"
        if send_page(self._pager, severity, title, body):
            self.sent += 1
            return True
        return False


class Episodes:
    """Once-per-episode pages: `active(key, ...)` pages the first time a condition holds, `clear(key)`
    re-arms it once the condition is gone."""

    def __init__(self, pager: Pager | None) -> None:
        self._pager = pager
        self._open: set[str] = set()

    def active(self, key: str, severity: str, title: str, body: str) -> bool:
        if key in self._open:
            return False
        self._open.add(key)
        send_page(self._pager, severity, title, body)
        return True

    def clear(self, key: str) -> bool:
        if key not in self._open:
            return False
        self._open.discard(key)
        return True

    def is_open(self, key: str) -> bool:
        return key in self._open


__all__ = ["CRITICAL", "HIGH", "INFO", "Episodes", "PageThrottle", "Pager", "send_page"]
