"""The `required_history` reader (plan S21, from the S20 review; design §B6.1, §A4, §A6).

`BarContext.history` holds the newest `bars` evaluable BROKER bars ending at the decided bar: `no_trade` / `blind`
buckets and absent buckets (a weekend) are read past, never counted; the reader stops when the read returns nothing
older or the retention is reached. FirstBarGuard's window read this way equals the `md-indicators` window. Synthetic
bars only.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal

import pytest

from src.marketdata.indicators.atr import window_bars
from src.marketdata.indicators.registry import BROKER_RETENTION_MS
from src.marketdata.ports import BarsRead
from src.marketdata.types import Bar, DataUnavailable, MdGeneration, Quality, SeriesId
from src.riskmgr.guards.first_bar import FIRST_BAR_V1, canonical_atr, evaluable_bars
from src.riskmgr.history import read_histories, read_history
from src.riskmgr.strategy import GuardSpec, HistoryNeed
from tests.riskmgr.kind_a import ADAPTER, B0, FIVE, Q, bar, series

D = Decimal
GEN = MdGeneration(1, 1)
NEED = HistoryNeed(series().spec, series().side, 5)


class Store:
    """A range-form `MarketDataPort.bars` over memory (absent buckets are simply missing)."""

    def __init__(self, bars: list[Bar]) -> None:
        self.by: dict[SeriesId, dict[int, Bar]] = {}
        for b in bars:
            self.by.setdefault(b.series, {})[b.bucket_start_ms] = b
        self.reads: list[tuple[int, int]] = []
        self.unavailable: str | None = None

    async def bars(self, s: SeriesId, *, at_bucket_ms: int | None = None, n: int | None = None,
                   from_bucket_ms: int | None = None, to_bucket_ms: int | None = None,
                   as_of_ms: int | None = None) -> BarsRead | DataUnavailable:
        assert from_bucket_ms is not None and to_bucket_ms is not None and n is None
        assert (to_bucket_ms - from_bucket_ms) // Q < 5_000
        self.reads.append((from_bucket_ms, to_bucket_ms))
        if self.unavailable:
            return DataUnavailable(self.unavailable)
        cur = self.by.get(s, {})
        return BarsRead(tuple(cur[k] for k in sorted(cur) if from_bucket_ms <= k <= to_bucket_ms), GEN)


def run(store: Store, need: HistoryNeed, upto_end_ms: int, now_ms: int | None = None) -> tuple[Bar, ...] | DataUnavailable:
    return asyncio.run(read_history(store, series(), need, upto_end_ms,   # type: ignore[arg-type]
                                    now_ms=upto_end_ms + Q if now_ms is None else now_ms))


def flat(start: int, n: int) -> list[Bar]:
    v = D("1.20000")
    return [bar(FIVE, start + k * Q, v, v + D("0.00010"), v, v) for k in range(n)]


def test_reads_past_quiet_buckets() -> None:
    bars = flat(B0 - 9 * Q, 10)
    bars[7] = replace(bars[7], quality=Quality.NO_TRADE)
    bars[8] = replace(bars[8], quality=Quality.BLIND)
    got = run(Store(bars), NEED, B0 + Q)
    assert isinstance(got, tuple)
    assert len(evaluable_bars(got, B0 + Q)) == 5 and got[-1].bucket_start_ms == B0
    assert got[0].bucket_start_ms == B0 - 6 * Q            # 5 evaluable: -6, -5, -4, -3 and 0 (two quiet ones in between)


def test_reads_back_across_a_long_closure() -> None:
    """Three days with no buckets at all (absent = blind) inside the span: the reader steps further back."""
    gap = 3 * 96 * Q
    older = flat(B0 - gap - 10 * Q, 4)
    newer = flat(B0 - Q, 2)
    store = Store([*older, *newer])
    got = run(store, NEED, B0 + Q)
    assert isinstance(got, tuple) and len(evaluable_bars(got, B0 + Q)) == 5
    assert len(store.reads) >= 2


@pytest.mark.parametrize("days", [4.5, 5, 9])
def test_reads_back_across_an_easter_length_closure(days: float) -> None:
    """A closure of 4.5, 5 or 9 days (no buckets at all) between two newer bars and three older ones: the reader
    reads full `MAX_READ_BUCKETS` steps (the `md-indicators` step), so it still finds the 5 evaluable bars."""
    gap = int(days * 96) * Q
    older = flat(B0 - gap - 10 * Q, 3)
    newer = flat(B0 - Q, 2)
    store = Store([*older, *newer])
    got = run(store, NEED, B0 + Q)
    assert isinstance(got, tuple) and len(evaluable_bars(got, B0 + Q)) == 5
    assert got[0].bucket_start_ms == older[0].bucket_start_ms


def test_stops_when_nothing_older_or_at_retention() -> None:
    store = Store(flat(B0 - 2 * Q, 3))
    got = run(store, NEED, B0 + Q)
    assert isinstance(got, tuple) and len(got) == 3
    now = B0 - Q + BROKER_RETENTION_MS                     # the retention floor is then bucket B0 - Q
    got2 = run(Store(flat(B0 - 2 * Q, 3)), NEED, B0 + Q, now_ms=now)
    assert isinstance(got2, tuple) and [b.bucket_start_ms for b in got2] == [B0 - Q, B0]


def test_nothing_after_the_decided_bar_and_unavailable_passes_through() -> None:
    store = Store(flat(B0 - 6 * Q, 10))
    got = run(store, NEED, B0 + Q)
    assert isinstance(got, tuple) and got[-1].bucket_end_ms == B0 + Q and len(got) == 5
    store.unavailable = "not_live"
    assert run(store, NEED, B0 + Q) == DataUnavailable("not_live")


def test_read_histories_takes_the_largest_need_per_series() -> None:
    store = Store(flat(B0 - 9 * Q, 10))
    needs = [HistoryNeed(series().spec, series().side, 3), HistoryNeed(series().spec, series().side, 7)]
    got = asyncio.run(read_histories(store, needs, ADAPTER, FIVE, B0 + Q, now_ms=B0 + 2 * Q))  # type: ignore[arg-type]
    assert isinstance(got, tuple) and len(got) == 1 and got[0][0] == series() and len(got[0][1]) == 7


def test_first_bar_window_matches_md_indicators_count_rule() -> None:
    """FirstBarGuard's `W + 2` need read past two `no_trade` buckets gives the ATR over the newest `W + 1` evaluable
    bars ending at bar 0, the same rule `md-indicators` uses (the lent close included)."""
    spec = GuardSpec(FIRST_BAR_V1.KIND, FIRST_BAR_V1.VERSION, FIRST_BAR_V1.parse_params({}))
    need = FIRST_BAR_V1.required_history(spec)[0]
    w = window_bars(14)
    bars = flat(B0 - (w + 5) * Q, w + 6)
    bars[10] = replace(bars[10], quality=Quality.NO_TRADE)
    bars[50] = replace(bars[50], quality=Quality.NO_TRADE)
    got = run(Store(bars), need, B0 + Q)
    assert isinstance(got, tuple)
    ev = evaluable_bars(got, B0 + Q)
    assert len(ev) == w + 2
    expected = [b for b in bars if b.quality is Quality.COMPLETE][-(w + 1):]
    assert canonical_atr(ev, 14) == canonical_atr(expected, 14)
