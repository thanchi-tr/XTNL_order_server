"""The `required_history` reader (design §B6.1 BarContext, §A4, §A6; plan S21, from the S20 review).

`BarContext.history` holds, per `HistoryNeed`, the newest `bars` **evaluable** BROKER bars ending at the decided bar
(the `md-indicators` `Count` rule: `blind` / `no_trade` and not-final bars are never counted). A raw-bucket count
would shorten FirstBarGuard's window across a quiet bucket (a PENDING, or an ATR without the close the oldest bar
lends, which then differs from `md-indicators`). So the reader reads the decided bar's span (plus two days of buckets,
as `md-indicators` does, so a weekend does not end the read early) and then further back, in steps of
`MAX_READ_BUCKETS` buckets (the `md-indicators` step, so a market closure of any length inside one step is read
past), until `HistoryNeed.held_by` is true, a step returns nothing or the series retention is reached.
The non-evaluable bars inside the kept span come along; consumers skip them.

Every read goes through `MarketDataPort.bars` (range form, `as_of_ms` passed through for point-in-time re-runs); a
`DataUnavailable` from any read is returned as is (the decision is then `DATA_UNAVAILABLE`, never a guess).
"""
from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from src.marketdata.indicators.registry import DAY_MS, series_retention_ms
from src.marketdata.ports import BarsRead, MarketDataPort
from src.marketdata.types import Bar, DataUnavailable, Quality, SeriesId
from src.riskmgr.strategy import HistoryNeed

MAX_READ_BUCKETS: Final = 4_999
"""Buckets per read (the port refuses 5 000 or more in one range)."""


def _evaluable(bar: Bar) -> bool:
    return bar.quality not in (Quality.BLIND, Quality.NO_TRADE) and bar.final_at_ms is not None


def _trim(bars: Sequence[Bar], need: int, upto_end_ms: int) -> tuple[Bar, ...]:
    """The bars ending at or before `upto_end_ms` from the oldest of the newest `need` evaluable buckets on (all of
    them when fewer are held), oldest first, one bar per bucket."""
    by_bucket: dict[int, Bar] = {}
    for b in bars:
        if b.bucket_end_ms <= upto_end_ms:
            by_bucket[b.bucket_start_ms] = b
    ordered = [by_bucket[k] for k in sorted(by_bucket)]
    evaluable = [b.bucket_start_ms for b in ordered if _evaluable(b)]
    if len(evaluable) <= need:
        return tuple(ordered)
    start = evaluable[-need]
    return tuple(b for b in ordered if b.bucket_start_ms >= start)


async def read_history(port: MarketDataPort, series: SeriesId, need: HistoryNeed, upto_end_ms: int, *,
                       now_ms: int, as_of_ms: int | None = None) -> tuple[Bar, ...] | DataUnavailable:
    """The newest `need.bars` evaluable bars of `series` ending at `upto_end_ms` (see the module docstring)."""
    spec = series.spec
    tf = spec.timeframe_ms
    hi = spec.bucket_start_ms(upto_end_ms - 1)
    floor = spec.bucket_start_ms(max(0, now_ms - series_retention_ms(series)) + tf - 1)
    span = min(need.bars + 2 * DAY_MS // tf, MAX_READ_BUCKETS)
    lo = max(hi - (span - 1) * tf, floor)
    if lo > hi:
        return ()
    read = await port.bars(series, from_bucket_ms=lo, to_bucket_ms=hi, as_of_ms=as_of_ms)
    if not isinstance(read, BarsRead):
        return read
    bars = list(read.bars)
    while not need.held_by(bars, upto_end_ms):
        top = lo - tf
        if top < floor:
            break
        # Full `MAX_READ_BUCKETS` steps, as `md-indicators` (engine `_extend_back`): an empty step then means
        # nothing older is held, never a closure longer than the step.
        new_lo = spec.bucket_start_ms(max(top - (MAX_READ_BUCKETS - 1) * tf, floor))
        more = await port.bars(series, from_bucket_ms=new_lo, to_bucket_ms=top, as_of_ms=as_of_ms)
        if not isinstance(more, BarsRead):
            return more
        if not more.bars:
            break
        bars = [*more.bars, *bars]
        lo = new_lo
    return _trim(bars, need.bars, upto_end_ms)


async def read_histories(port: MarketDataPort, needs: Sequence[HistoryNeed], adapter_id: str, symbol: str,
                         upto_end_ms: int, *, now_ms: int, as_of_ms: int | None = None
                         ) -> tuple[tuple[SeriesId, tuple[Bar, ...]], ...] | DataUnavailable:
    """`BarContext.history` for every need of a position (the strategy's and its guards'): one read per series, at
    the largest count any need on it asks for, in series order."""
    wanted: dict[SeriesId, HistoryNeed] = {}
    for need in needs:
        series = need.resolve(adapter_id, symbol)
        if series not in wanted or need.bars > wanted[series].bars:
            wanted[series] = need
    out: list[tuple[SeriesId, tuple[Bar, ...]]] = []
    for series in sorted(wanted):
        got = await read_history(port, series, wanted[series], upto_end_ms, now_ms=now_ms, as_of_ms=as_of_ms)
        if isinstance(got, DataUnavailable):
            return got
        out.append((series, got))
    return tuple(out)


__all__ = ["MAX_READ_BUCKETS", "read_history", "read_histories"]
