"""Hybrid logical clock for seq_id / observed_seq (dto-contracts.md §2).

seq = max(epoch_ms << 20, last + 1). Seeded at start-up from MAX(seq_id) in PostgreSQL
(before SM4 SNAPSHOTTING) and re-seeded after every epoch bump (LEADER_INIT).
"""
from __future__ import annotations

import threading
import time
from collections.abc import Callable

LOGICAL_BITS = 20
LOGICAL_MASK = (1 << LOGICAL_BITS) - 1


def wall_clock_ms() -> int:
    return time.time_ns() // 1_000_000


class HybridLogicalClock:
    """Strictly monotonic 64-bit HLC: physical milliseconds in the high bits, a counter in the low 20."""

    def __init__(self, clock_ms: Callable[[], int] = wall_clock_ms, seed: int = 0) -> None:
        self._clock_ms = clock_ms
        self._last = max(0, seed)
        self._lock = threading.Lock()  # uncontended on one event loop; guards to_thread callers too

    @property
    def last(self) -> int:
        """The most recently issued (or seeded) value."""
        return self._last

    def seed(self, max_seq: int) -> None:
        """Raise the floor to MAX(seq_id) read from PostgreSQL; never moves the clock backwards."""
        with self._lock:
            if max_seq > self._last:
                self._last = max_seq

    def reseed(self, max_seq: int) -> None:
        """Re-seed after an epoch bump (LEADER_INIT): same floor rule as seed()."""
        self.seed(max_seq)

    def next(self) -> int:
        """Issue the next value: max(now_ms << 20, last + 1)."""
        with self._lock:
            value = max(self._clock_ms() << LOGICAL_BITS, self._last + 1)
            self._last = value
            return value

    @staticmethod
    def physical_ms(seq: int) -> int:
        """The millisecond part of an HLC value."""
        return seq >> LOGICAL_BITS

    @staticmethod
    def logical(seq: int) -> int:
        """The counter part of an HLC value."""
        return seq & LOGICAL_MASK
