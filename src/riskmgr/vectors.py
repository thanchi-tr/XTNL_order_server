"""Runtime conformance vectors of the pinned strategy and guard versions (go-live gate `conformance_probe_partial`,
plan S39b; design §B14 Clearing, `strategy_conformance`).

The `strategy_conformance` clear probe (`src.riskmgr.probes.VersionConformanceProbe`) runs, as **one job on the
version's own lane** under the job runner's time cap, the registry's declaration checks **and** the version's own code
against the vectors below: `parse_params` on the vector's row, `propose` (strategies) or `applies` / `evaluate`
(guards) on the vector's context, and `at_target` on bar-end events. Each vector states what a conforming version
answers (the trigger condition, the reason, how many handles a proposal moves, whether the stop is at target; the
guard's applicability, status and reason). A version that hangs, raises or answers differently fails the probe, so a
STRATEGY_VERSION scope FAILED for `job_raised` / `job_timeout` is never cleared on declarations alone. A registered
version **without** vectors fails it too (`no_vectors`): a new version cannot be cleared until it declares its own.

The full build-time suite (`tests/riskmgr/conformance.py`, many more cases, replay determinism, hostile contexts) still
gates every registered version; these vectors are its runtime part. The contexts are synthetic: one made-up 5-digit
instrument (`SYN.FIVE`, increment 0.00001), levels around 1.2; nothing here is a real price, size or spread, and a
problem text names the vector and the field only, never a value.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Final

from src.marketdata.indicators.atr import window_bars
from src.marketdata.indicators.base import IndicatorValue, Lineage
from src.marketdata.types import M15, Bar, Quality, SeriesId, Side, Source
from src.riskmgr.guard import GuardStatus
from src.riskmgr.registry import ArmRegistry
from src.riskmgr.strategy import GuardSpec, IndicatorNeed, ParamError, StrategyState
from src.riskmgr.types import BAR_END_EVENTS, ArmEvent, BarContext, EntryTimeSource, HandleView, S0Source

D = Decimal
NO_VECTORS: Final = "no_vectors"
ADAPTER: Final = "vector"
SYMBOL: Final = "SYN.FIVE"
INCREMENT: Final = D("0.00001")
BASE: Final = D("1.20000")
Q: Final = M15.timeframe_ms
B0: Final = 1_773_151_200_000
"""Bar 0's bucket start (a Tuesday, 10:00 New York, far from the 17:00 roll)."""
FILL: Final = B0 + 6 * 60_000


@dataclass(frozen=True)
class StrategyVector:
    """One strategy context and what a conforming version answers. `moves` is the number of handles the proposal
    moves (None: not checked); `at_target` is checked on bar-end events only (None: not checked)."""

    name: str
    row: Mapping[str, object] = field(repr=False)
    ctx: BarContext = field(repr=False)
    state: StrategyState
    trigger_met: bool
    reason: str | None = None
    moves: int | None = None
    at_target: bool | None = None


@dataclass(frozen=True)
class GuardVector:
    name: str
    params: Mapping[str, object] = field(repr=False)
    ctx: BarContext = field(repr=False)
    applies: bool
    status: GuardStatus | None = None
    reason: str = ""


# ---------------------------------------------------------------------------------------------------- contexts
def _series(side: Side = Side.BID) -> SeriesId:
    return SeriesId(ADAPTER, SYMBOL, M15, side, Source.BROKER)


def _bar(bucket: int, o: Decimal, h: Decimal, low: Decimal, c: Decimal, side: Side = Side.BID) -> Bar:
    return Bar(series=_series(side), bucket_start_ms=bucket, open=o, high=h, low=low, close=c, tick_count=12,
               quality=Quality.COMPLETE, final_at_ms=bucket + Q + 30_000)


def strategy_row(**over: object) -> dict[str, object]:
    row: dict[str, object] = {"trigger_basis": "exit_side", "trigger_comparison": "gt", "latch": "off",
                              "be_offset_r": "0.1", "rearm_on_loosen": "off", "bar_spec": "15m",
                              "guards": [{"kind": "first_bar", "version": 1, "params": {}}]}
    row.update(over)
    return row


def _s_ctx(close: str, *, side: str = "BUY", stop: str | None = None, event: ArmEvent = ArmEvent.POST) -> BarContext:
    """Entry 1.20000, S0 one hundred increments on the losing side (1 R = 0.00100), one child stop at `stop` (S0 by
    default), the trigger-side final bar closing at `close` (bar-end events)."""
    long = side == "BUY"
    first = BASE - 100 * INCREMENT if long else BASE + 100 * INCREMENT
    current = D(stop) if stop is not None else first
    bar_end = event in BAR_END_EVENTS
    c = D(close)
    trigger = Side.BID if long else Side.ASK
    return BarContext(
        position_id="V1", adapter_id=ADAPTER, symbol=SYMBOL, side="BUY" if long else "SELL", bar_spec=M15,
        event=event, bar_end_ms=B0 + 4 * Q, bar_index=3, entry=BASE, entry_time_ms=FILL,
        entry_time_source=EntryTimeSource.STREAM, s0=first, s0_source=S0Source.AT_FILL,
        handles=(HandleView(kind="CHILD_ORDER", handle_id="V1-S", level=current),), stop=current,
        stop_at_t=current if bar_end else None, price_increment=INCREMENT,
        bar=_bar(B0 + 3 * Q, c, c, c, c, trigger) if bar_end else None)


GUARD_PARAMS: Final[Mapping[str, object]] = {
    "anchor": "fill_bucket", "measure": "high_low", "comparison": "gt", "atr_bar": "current", "multiplier": "1.318",
    "atr_length": 14, "unknown": "hold_bar"}


def _atr_id() -> str:
    need = IndicatorNeed("ATR", 1, M15, Side.BID, (("length", "14"),))
    return need.resolve(ADAPTER, SYMBOL, INCREMENT).indicator_id


def _guard_ctx(bar0_range: str, *, warm: bool = True, source: EntryTimeSource = EntryTimeSource.STREAM,
               bucket: int = B0, bar_index: int = 0) -> BarContext:
    """Bar-end context of bucket `bucket`: a flat bid history whose ATR(14) is exactly 0.00500 and bar 0 opening at
    the base with a high-low range of `bar0_range`."""
    a = D("0.00500")
    count = window_bars(14) + 1
    hist = [_bar(B0 - (count - k) * Q, BASE, BASE + a, BASE, BASE) for k in range(count)]
    x = D(bar0_range)
    bars = (*hist, _bar(B0, BASE, BASE + x, BASE, BASE))
    iid = _atr_id()
    value = IndicatorValue(indicator_id=iid, output="value", bucket_start_ms=B0, value=D("0.001"),
                           quality=Quality.COMPLETE, warm=warm, final_at_ms=B0 + Q + 30_000,
                           lineage=Lineage(iid, B0 - 140 * Q, "0" * 16, 0, ""))
    stop = BASE - 100 * INCREMENT
    return BarContext(
        position_id="V1", adapter_id=ADAPTER, symbol=SYMBOL, side="BUY", bar_spec=M15, event=ArmEvent.POST,
        bar_end_ms=bucket + Q, bar_index=bar_index, entry=BASE, entry_time_ms=FILL, entry_time_source=source,
        s0=stop, s0_source=S0Source.AT_FILL, handles=(HandleView(kind="CHILD_ORDER", handle_id="V1-S", level=stop),),
        stop=stop, stop_at_t=stop, price_increment=INCREMENT, history=((_series(), bars),),
        indicators=(((iid, "value"), (value,)),))


def _kind_a_vectors() -> tuple[StrategyVector, ...]:
    s0 = StrategyState.of(latched=False)
    row = strategy_row()
    return (
        StrategyVector("above_trigger_long", row, _s_ctx("1.20101"), s0, True, moves=1, at_target=False),
        StrategyVector("equal_is_not_above", row, _s_ctx("1.20100"), s0, False, reason="BELOW_TRIGGER"),
        StrategyVector("not_in_favour", row, _s_ctx("1.19950"), s0, False, reason="NOT_IN_FAVOUR"),
        StrategyVector("above_trigger_short", row, _s_ctx("1.19899", side="SELL"), s0, True, moves=1,
                       at_target=False),
        StrategyVector("stop_at_target", row, _s_ctx("1.20101", stop="1.20010"), s0, True, moves=0,
                       at_target=True),
        StrategyVector("late_below_trigger", row, _s_ctx("1.20050", event=ArmEvent.LATE), s0, False,
                       reason="BELOW_TRIGGER"),
    )


def _first_bar_vectors() -> tuple[GuardVector, ...]:
    p = GUARD_PARAMS
    return (
        GuardVector("calm_bar0_passes", p, _guard_ctx("0.00400"), True, GuardStatus.PASS),
        GuardVector("volatile_bar0_holds", p, _guard_ctx("0.00900"), True, GuardStatus.HOLD, "FIRST_BAR_VOLATILE"),
        GuardVector("atr_not_warm_waits", p, _guard_ctx("0.00400", warm=False), True, GuardStatus.PENDING),
        GuardVector("estimated_entry_waits", p, _guard_ctx("0.00400", source=EntryTimeSource.ESTIMATED), True,
                    GuardStatus.PENDING),
        GuardVector("bar1_not_applicable", p, _guard_ctx("0.00400", bucket=B0 + Q, bar_index=1), False),
    )


#: The vectors per `(strategy | guard, KIND, VERSION)`. Built lazily (the contexts are frozen values).
_BUILDERS: Final[Mapping[tuple[str, str, int], Callable[[], tuple[StrategyVector, ...] | tuple[GuardVector, ...]]]] = {
    ("strategy", "break_even_on_risk_close", 1): _kind_a_vectors,
    ("guard", "first_bar", 1): _first_bar_vectors,
}


def vectors_of(head: str, kind: str, version: int) -> tuple[StrategyVector, ...] | tuple[GuardVector, ...]:
    build = _BUILDERS.get((head, kind, version))
    return () if build is None else build()


# ---------------------------------------------------------------------------------------------------- the run
def _strategy_problems(registry: ArmRegistry, kind: str, version: int, owner: str) -> list[str]:
    strategy = registry.strategy(kind, version)
    if strategy is None:
        return [f"{owner}: not registered"]
    problems: list[str] = []
    vectors = vectors_of("strategy", kind, version)
    if not vectors:
        return [f"{owner}: {NO_VECTORS}"]
    for v in vectors:
        if not isinstance(v, StrategyVector):
            problems.append(f"{owner}: {v.name}: not a strategy vector")
            continue
        try:
            params = registry.parse_params(kind, version, v.row, new_pin=False)
        except ParamError:
            problems.append(f"{owner}: {v.name}: params refused")
            continue
        ev = strategy.propose(params, v.ctx, v.state, v.ctx.event)
        if ev.trigger_met is not v.trigger_met:
            problems.append(f"{owner}: {v.name}: trigger_met")
        if v.reason is not None and ev.reason != v.reason:
            problems.append(f"{owner}: {v.name}: reason")
        if v.moves is not None and len(ev.proposal.targets if ev.proposal is not None else ()) != v.moves:
            problems.append(f"{owner}: {v.name}: moves")
        if v.at_target is not None and v.ctx.event in BAR_END_EVENTS and \
                strategy.at_target(params, v.ctx, v.state) is not v.at_target:
            problems.append(f"{owner}: {v.name}: at_target")
    return problems


def _guard_problems(registry: ArmRegistry, kind: str, version: int, owner: str) -> list[str]:
    guard = registry.guard(kind, version)
    if guard is None:
        return [f"{owner}: not registered"]
    vectors = vectors_of("guard", kind, version)
    if not vectors:
        return [f"{owner}: {NO_VECTORS}"]
    problems: list[str] = []
    for v in vectors:
        if not isinstance(v, GuardVector):
            problems.append(f"{owner}: {v.name}: not a guard vector")
            continue
        try:
            spec = GuardSpec(kind, version, guard.parse_params(v.params))
        except ParamError:
            problems.append(f"{owner}: {v.name}: params refused")
            continue
        applies = guard.applies(spec, v.ctx, StrategyState())
        if applies is not v.applies:
            problems.append(f"{owner}: {v.name}: applies")
            continue
        if not applies or v.status is None:
            continue
        result = guard.evaluate(spec, v.ctx)
        if result.status is not v.status:
            problems.append(f"{owner}: {v.name}: status")
        elif v.reason and result.reason != v.reason:
            problems.append(f"{owner}: {v.name}: reason")
    return problems


def vector_problems(registry: ArmRegistry, head: str, kind: str, version: int) -> list[str]:
    """Run the version's own code against its vectors (inside the probe's job, under the pinned Decimal context the
    job runner sets). A raise propagates to the job runner (`job_raised`: the probe fails), a hang is its
    `job_timeout`. Problems name the vector and the field only."""
    owner = f"{head} {kind} v{version}"
    if head == "strategy":
        return _strategy_problems(registry, kind, version, owner)
    return _guard_problems(registry, kind, version, owner)


__all__ = ["GUARD_PARAMS", "GuardVector", "NO_VECTORS", "StrategyVector", "strategy_row", "vector_problems",
           "vectors_of"]
