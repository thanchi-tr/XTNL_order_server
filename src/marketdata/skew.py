"""ClockSkew per adapter (design §A9, §A7 "Suspend and clock steps"; plan S14).

Pure: no clock, no store. The tick hook (`src/marketdata/ticks.py`) offers each tick's `received_ms − broker time`
here when the tick is a **fresh sample** (`fresh_sample`): broker-timed, tradeable, not a subscription snapshot, not
within 10 s of that market's (re)subscribe, and with a changed bid or ask. The window keeps the newest
`WINDOW_SAMPLES`; percentiles are nearest-rank over a sorted copy, computed lazily (never on the price path).

* **Estimate:** below `MIN_SAMPLES` fresh samples since the last baseline the adapter is cold (`COLD_SKEW`: p50 0,
  settle 5 s); then `p50`, `p99` and `settle = clamp(p99 − p50 + 250 ms, 250 ms, 5 s) + resolution`
  (`src.marketdata.timing.settle_ms`), where `resolution_ms` is the adapter's `TickDateRaw` resolution (PR3).
* **Re-baseline** after a clock step (`rebaseline`): every sample taken before the step is dropped, the adapter is
  cold again and "not settled".
* **Settled:** at least `MIN_SAMPLES` fresh samples since the baseline with `p99 − p50 ≤ 250 ms`; never compared
  with the pre-step value. Once settled it stays settled until the next baseline.
* **Page signals** (`evaluate`, edge-triggered; codes only, never a value): `SKEW_P50_HIGH` when `|p50|` exceeds the
  resolution + 1 s (raised at once, cleared only after the condition has been false for 5 minutes, then
  `SKEW_P50_CLEARED`); `SKEW_NOT_SETTLED` once per baseline when the market has been OPEN for 10 minutes (counted
  from the later of the open and the baseline) and the skew is still not settled.

The skew values are timing figures (milliseconds), not prices; they go to `md:skew:{adapter}` (S15) and never to
PostgreSQL.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Final

from src.marketdata.timing import COLD_SKEW, SkewEstimate, settle_ms

WINDOW_SAMPLES: Final = 512
MIN_SAMPLES: Final = 20
SETTLED_SPREAD_MS: Final = 250
RESUBSCRIBE_QUIET_MS: Final = 10_000
P50_PAGE_MARGIN_MS: Final = 1_000
P50_CLEAR_AFTER_MS: Final = 300_000
NOT_SETTLED_AFTER_OPEN_MS: Final = 600_000


class SkewSignal(str, Enum):
    SKEW_P50_HIGH = "skew_p50_high"
    SKEW_P50_CLEARED = "skew_p50_cleared"
    SKEW_NOT_SETTLED = "skew_not_settled"


def fresh_sample(*, broker_timed: bool, tradeable: bool, snapshot: bool, received_ms: int | None,
                 subscribed_since_ms: int | None, bid: Decimal, ask: Decimal,
                 prev: tuple[Decimal, Decimal] | None) -> bool:
    """The §A9 fresh-sample filter. `prev` is the previous (bid, ask) of the same market (None: no earlier tick);
    `subscribed_since_ms` the start of the market's current subscription interval (None: not known)."""
    if not (broker_timed and tradeable) or snapshot or received_ms is None:
        return False
    if subscribed_since_ms is not None and received_ms - subscribed_since_ms < RESUBSCRIBE_QUIET_MS:
        return False
    return prev is None or prev != (bid, ask)


def _rank(ordered: list[int], q: int) -> int:
    """Nearest-rank percentile `q` (1-100) of a non-empty sorted list."""
    k = max(1, -(-q * len(ordered) // 100))
    return ordered[k - 1]


@dataclass(frozen=True)
class SkewSnapshot:
    """What `md:skew:{adapter}` keeps (S15): timing values only."""

    adapter_id: str
    p50_ms: int
    p99_ms: int
    settle_ms: int
    samples: int
    settled: bool
    baseline_ms: int | None


class ClockSkew:
    """One adapter's skew estimator. `add` is O(1); `estimate()` sorts the window only when it changed."""

    def __init__(self, adapter_id: str, *, resolution_ms: int = 0, window: int = WINDOW_SAMPLES) -> None:
        self.adapter_id = adapter_id
        self.resolution_ms = max(resolution_ms, 0)
        self._samples: deque[int] = deque(maxlen=window)
        self._since_baseline = 0
        self._baseline_ms: int | None = None
        self._settled = False
        self._dirty = True
        self._cached: tuple[int, int] | None = None
        self._p50_alarm = False
        self._p50_ok_since: int | None = None
        self._open_since: int | None = None
        self._not_settled_paged = False
        self.steps = 0

    # -------------------------------------------------------------------------------------------- samples
    def add(self, received_ms: int, broker_ms: int) -> None:
        """One fresh sample (the caller applied `fresh_sample`)."""
        self._samples.append(received_ms - broker_ms)
        self._since_baseline += 1
        self._dirty = True

    def rebaseline(self, at_ms: int) -> None:
        """A clock step: drop every earlier sample; cold and not settled until `MIN_SAMPLES` new ones."""
        self._samples.clear()
        self._since_baseline = 0
        self._baseline_ms = at_ms
        self._settled = False
        self._not_settled_paged = False
        self._dirty = True
        self.steps += 1

    @property
    def samples(self) -> int:
        return len(self._samples)

    def _percentiles(self) -> tuple[int, int] | None:
        if self._dirty:
            self._dirty = False
            if len(self._samples) < MIN_SAMPLES:
                self._cached = None
            else:
                ordered = sorted(self._samples)
                self._cached = (_rank(ordered, 50), _rank(ordered, 99))
                if (not self._settled and self._since_baseline >= MIN_SAMPLES
                        and self._cached[1] - self._cached[0] <= SETTLED_SPREAD_MS):
                    self._settled = True
        return self._cached

    @property
    def settled(self) -> bool:
        self._percentiles()
        return self._settled

    def estimate(self) -> SkewEstimate:
        """`COLD_SKEW` below `MIN_SAMPLES`, else p50 and the clamped settle plus the resolution."""
        pct = self._percentiles()
        if pct is None:
            return COLD_SKEW if self.resolution_ms == 0 else SkewEstimate(0, COLD_SKEW.settle_ms + self.resolution_ms)
        p50, p99 = pct
        return SkewEstimate(p50, settle_ms(p50, p99, self.resolution_ms))

    def snapshot(self) -> SkewSnapshot:
        pct = self._percentiles()
        est = self.estimate()
        p50, p99 = pct if pct is not None else (est.p50_ms, est.p50_ms)
        return SkewSnapshot(self.adapter_id, p50, p99, est.settle_ms, len(self._samples), self._settled,
                            self._baseline_ms)

    # -------------------------------------------------------------------------------------------- signals
    def evaluate(self, now_ms: int, market_open: bool) -> tuple[SkewSignal, ...]:
        """Edge-triggered page signals (codes only). Call it periodically (the writer's loop)."""
        out: list[SkewSignal] = []
        pct = self._percentiles()
        high = pct is not None and abs(pct[0]) > self.resolution_ms + P50_PAGE_MARGIN_MS
        if high:
            self._p50_ok_since = None
            if not self._p50_alarm:
                self._p50_alarm = True
                out.append(SkewSignal.SKEW_P50_HIGH)
        elif self._p50_alarm:
            if self._p50_ok_since is None:
                self._p50_ok_since = now_ms
            elif now_ms - self._p50_ok_since >= P50_CLEAR_AFTER_MS:
                self._p50_alarm = False
                self._p50_ok_since = None
                out.append(SkewSignal.SKEW_P50_CLEARED)
        if not market_open:
            self._open_since = None
        else:
            if self._open_since is None:
                self._open_since = now_ms
            start = max(self._open_since, self._baseline_ms if self._baseline_ms is not None else self._open_since)
            if (not self._settled and not self._not_settled_paged
                    and now_ms - start >= NOT_SETTLED_AFTER_OPEN_MS):
                self._not_settled_paged = True
                out.append(SkewSignal.SKEW_NOT_SETTLED)
        return tuple(out)

    @property
    def p50_alarm(self) -> bool:
        return self._p50_alarm
