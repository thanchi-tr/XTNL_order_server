"""Fixtures for kind (a) `break_even_on_risk_close` and FirstBarGuard (plan S20), plus their conformance case data.

Synthetic instruments only: `SYN.FIVE` (a 5-digit pair, increment 0.00001) and `SYN.THREE` (a 3-digit pair,
increment 0.001). Levels and bars are made up; nothing here is a real price, size or spread.

This module imports no test helper of its own package, so `tests/riskmgr/conformance.py` can build its cases from it.
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal

from src.marketdata.indicators.atr import window_bars
from src.marketdata.indicators.base import IndicatorValue, Lineage
from src.marketdata.types import M15, Bar, Quality, Quote, SeriesId, Side, Source
from src.riskmgr.strategy import IndicatorNeed, StrategyState
from src.riskmgr.types import (
    BAR_END_EVENTS,
    ArmContext,
    ArmEvent,
    BarContext,
    EntryTimeSource,
    HandleView,
    S0Source,
)

D = Decimal
ADAPTER = "mock"
FIVE = "SYN.FIVE"
THREE = "SYN.THREE"
INC = {FIVE: D("0.00001"), THREE: D("0.001")}
BASE = {FIVE: D("1.20000"), THREE: D("150.000")}
Q = M15.timeframe_ms


def utc_ms(*args: int) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)   # type: ignore[misc]


B0 = utc_ms(2026, 3, 10, 14, 0)
"""Bar 0's bucket start in most tests: a Tuesday 10:00 New York, far from the 17:00 roll."""
FILL = B0 + 6 * 60_000
"""A fill 6 minutes into bar 0's bucket."""


def series(symbol: str = FIVE, side: Side = Side.BID) -> SeriesId:
    return SeriesId(ADAPTER, symbol, M15, side, Source.BROKER)


def bar(symbol: str, bucket: int, o: Decimal, h: Decimal, low: Decimal, c: Decimal, *, side: Side = Side.BID,
        quality: Quality = Quality.COMPLETE, final: bool = True) -> Bar:
    return Bar(series=series(symbol, side), bucket_start_ms=bucket, open=o, high=h, low=low, close=c, tick_count=12,
               quality=quality, final_at_ms=bucket + Q + 30_000 if final else None)


def flat_history(symbol: str, a: Decimal, *, end_bucket: int = B0, n: int | None = None,
                 length: int = 14) -> list[Bar]:
    """`n` (default `W + 1`) bars before `end_bucket`, each with open = close = base and true range exactly `a`, so
    the ATR over them is exactly `a`."""
    count = window_bars(length) + 1 if n is None else n
    base = BASE[symbol]
    return [bar(symbol, end_bucket - (count - k) * Q, base, base + a, base, base) for k in range(count)]


def bar0_with_range(symbol: str, x: Decimal, *, bucket: int = B0, body: Decimal = D(0)) -> Bar:
    """Bar 0 opening at base: low = base, high = base + x (so high − low = true range = x), close = base + body."""
    base = BASE[symbol]
    return bar(symbol, bucket, base, base + x, base, base + body)


def atr_id(symbol: str, length: int = 14) -> str:
    need = IndicatorNeed("ATR", 1, M15, Side.BID, (("length", str(length)),))
    return need.resolve(ADAPTER, symbol, INC[symbol]).indicator_id


def atr_value(symbol: str, bucket: int, *, length: int = 14, warm: bool = True,
              faulted: bool = False) -> IndicatorValue:
    """The `md-indicators` value the guard checks for warmth (its value is never used for the verdict)."""
    iid = atr_id(symbol, length)
    return IndicatorValue(indicator_id=iid, output="value", bucket_start_ms=bucket,
                          value=None if faulted else D("0.001"), quality=Quality.COMPLETE, warm=warm,
                          final_at_ms=bucket + Q + 30_000,
                          lineage=Lineage(iid, bucket - 140 * Q, "0" * 16, 0, "planted" if faulted else ""))


def guard_ctx(symbol: str, bars: Sequence[Bar], values: Sequence[IndicatorValue] = (), *,
              bucket: int = B0, entry_time_ms: int | None = FILL, side: str = "BUY",
              event: ArmEvent = ArmEvent.POST, source: EntryTimeSource = EntryTimeSource.STREAM,
              ambiguous: bool = False, cap: int | None = None, bar_index: int | None = 0, length: int = 14,
              increment: Decimal | None = None) -> BarContext:
    """A bar-end context for FirstBarGuard on bucket `bucket` with the bid history `bars` and ATR values."""
    base = BASE[symbol]
    long = side == "BUY"
    stop = base - 100 * INC[symbol] if long else base + 100 * INC[symbol]
    return BarContext(
        position_id="P1", adapter_id=ADAPTER, symbol=symbol, side="BUY" if long else "SELL", bar_spec=M15,
        event=event, bar_end_ms=bucket + Q, bar_index=bar_index, entry=base, entry_time_ms=entry_time_ms,
        entry_time_source=source, entry_time_ambiguous=ambiguous, decision_cap_ms=cap, s0=stop,
        s0_source=S0Source.AT_FILL, handles=(HandleView(kind="CHILD_ORDER", handle_id="O1", level=stop),),
        stop=stop, stop_at_t=stop if event in BAR_END_EVENTS else None,
        price_increment=INC[symbol] if increment is None else increment,
        history=((series(symbol), tuple(bars)),), indicators=(((atr_id(symbol, length), "value"), tuple(values)),),
    )


# ---------------------------------------------------------------------------------------------------- strategy contexts
ENTRY = D("1.20000")
S0_LONG = D("1.19900")
S0_SHORT = D("1.20100")


def s_ctx(close: str, *, side: str = "BUY", stop: str | None = None, stop_at_t: str | None = None,
          pre_basis: str | None = None, event: ArmEvent = ArmEvent.POST, s0: str | None = None,
          s0_source: S0Source = S0Source.AT_FILL, handles: Sequence[str] | None = None,
          entry: str | None = None, trigger_side: Side | None = None, bar_index: int = 3) -> BarContext:
    """A strategy context: entry 1.20000, S0 one hundred increments on the losing side, one child stop at the
    current stop (or `handles`), the trigger-side final bar (bar-end events) or the frozen quote (PRE)."""
    long = side == "BUY"
    e = D(entry) if entry is not None else ENTRY
    first = D(s0) if s0 is not None else (S0_LONG if long else S0_SHORT)
    current = D(stop) if stop is not None else first
    levels = [D(h) for h in handles] if handles is not None else [current]
    bar_end = event in BAR_END_EVENTS
    t_side = trigger_side or (Side.BID if long else Side.ASK)
    c = D(close)
    b = bar(FIVE, B0 + 3 * Q, c, c, c, c, side=t_side) if bar_end else None
    quote = None if bar_end else Quote(adapter_id=ADAPTER, symbol=FIVE, bid=c, ask=c, broker_ms=B0 + 4 * Q - 40_000,
                                       received_ms=B0 + 4 * Q - 39_900)
    hv = tuple(HandleView(kind="CHILD_ORDER", handle_id=f"O{n + 1}", level=lv) for n, lv in enumerate(levels))
    return BarContext(
        position_id="P1", adapter_id=ADAPTER, symbol=FIVE, side="BUY" if long else "SELL", bar_spec=M15,
        event=event, bar_end_ms=B0 + 4 * Q, bar_index=bar_index, entry=e, entry_time_ms=FILL,
        entry_time_source=EntryTimeSource.STREAM, s0=first, s0_source=s0_source, handles=hv, stop=current,
        stop_at_t=(D(stop_at_t) if stop_at_t is not None else current) if bar_end else None,
        pre_basis=D(pre_basis) if pre_basis is not None else None, price_increment=INC[FIVE], bar=b, quote=quote,
    )


def s_arm_ctx(side: str = "BUY", s0_source: S0Source = S0Source.AT_FILL) -> ArmContext:
    long = side == "BUY"
    return ArmContext(position_id="P1", adapter_id=ADAPTER, symbol=FIVE, side="BUY" if long else "SELL",
                      bar_spec=M15, entry=ENTRY, s0=S0_LONG if long else S0_SHORT, s0_source=s0_source,
                      entry_time_ms=FILL, entry_time_source=EntryTimeSource.STREAM, armed_at_ms=FILL + 1_000)


# ---------------------------------------------------------------------------------------------------- conformance data
def strategy_row(**over: object) -> dict[str, object]:
    row: dict[str, object] = {"trigger_basis": "exit_side", "trigger_comparison": "gt", "latch": "off",
                              "be_offset_r": "0.1", "rearm_on_loosen": "off", "bar_spec": "15m",
                              "guards": [{"kind": "first_bar", "version": 1, "params": {}}]}
    row.update(over)
    return row


def guard_row(**params: object) -> dict[str, object]:
    return {"kind": "first_bar", "version": 1, "params": params}


STRATEGY_INVALID: tuple[dict[str, object], ...] = (
    strategy_row(be_offset_r="0.6"), strategy_row(be_offset_r="-0.1"), strategy_row(trigger_comparison="lt"),
    strategy_row(trigger_basis="ask"), strategy_row(latch="maybe"), strategy_row(rearm_on_loosen="yes"),
    strategy_row(bar_spec="1h"), strategy_row(be_offset_r=0.25), strategy_row(multiplier="1.5"),
    strategy_row(guards=[guard_row(multiplier="9.5")]), strategy_row(guards=[guard_row(atr_length=51)]),
)

GUARD_PARAMS: dict[str, object] = {"anchor": "fill_bucket", "measure": "high_low", "comparison": "gt",
                                   "atr_bar": "current", "multiplier": "1.318", "atr_length": 14,
                                   "unknown": "hold_bar"}
GUARD_INVALID: tuple[dict[str, object], ...] = (
    {**GUARD_PARAMS, "multiplier": "0.49"}, {**GUARD_PARAMS, "multiplier": "5.01"}, {**GUARD_PARAMS, "atr_length": 4},
    {**GUARD_PARAMS, "atr_length": 51}, {**GUARD_PARAMS, "atr_length": "14.5"}, {**GUARD_PARAMS, "anchor": "fill"},
    {**GUARD_PARAMS, "measure": "range"}, {**GUARD_PARAMS, "comparison": "lt"}, {**GUARD_PARAMS, "atr_bar": "next"},
    {**GUARD_PARAMS, "unknown": "ignore"}, {**GUARD_PARAMS, "multiplier": 1.5},
)


def strategy_evals() -> tuple[tuple[BarContext, StrategyState, ArmEvent, bool], ...]:
    """`(ctx, state, event, trigger_met)`: what the bar-end condition is in each context (risk 0.00100 = 1 R)."""
    s0 = StrategyState.of(latched=False)
    post, pre, late, reopen = ArmEvent.POST, ArmEvent.PRE, ArmEvent.LATE, ArmEvent.REOPEN
    return (
        (s_ctx("1.20101"), s0, post, True),                          # favour 0.00101 > risk 0.00100
        (s_ctx("1.20100"), s0, post, False),                         # equal: not > (gt)
        (s_ctx("1.20050"), s0, post, False),                         # BELOW_TRIGGER
        (s_ctx("1.19950"), s0, post, False),                         # NOT_IN_FAVOUR
        (s_ctx("1.19899", side="SELL"), s0, post, True),
        (s_ctx("1.19900", side="SELL", event=late), s0, late, False),
        (s_ctx("1.20150", event=pre), s0, pre, True),
        (s_ctx("1.20010", stop="1.20000"), s0, post, True),          # stop at entry: risk 0
        (s_ctx("1.20101", pre_basis="1.19850"), s0, post, False),    # pre_basis farther: risk 0.00150
        (s_ctx("1.20101", event=reopen), StrategyState.of(latched=True, target=D("1.20010")), reopen, True),
    )


def guard_contexts() -> tuple[tuple[BarContext, StrategyState], ...]:
    s = StrategyState()
    a = D("0.00500")
    hist = flat_history(FIVE, a)
    calm = [*hist, bar0_with_range(FIVE, D("0.00400"))]
    wild = [*hist, bar0_with_range(FIVE, D("0.00900"))]
    warm = [atr_value(FIVE, B0)]
    return (
        (guard_ctx(FIVE, calm, warm), s),
        (guard_ctx(FIVE, wild, warm), s),
        (guard_ctx(FIVE, calm, [atr_value(FIVE, B0, warm=False)]), s),       # not warm: PENDING
        (guard_ctx(FIVE, calm, warm, source=EntryTimeSource.ESTIMATED), s),
        (guard_ctx(FIVE, calm, warm, bucket=B0 + Q, bar_index=1), s),       # bar 1: not applicable
        (guard_ctx(FIVE, calm, warm, event=ArmEvent.PRE), s),
        (guard_ctx(FIVE, calm, warm, ambiguous=True), s),
    )
