"""A toy strategy and a toy guard for the S19 registry and conformance tests (never registered in the engine).

`ToyStrategy` is a small break-even rule in the shape of design §B6.3 (7): `favour = σ·(close − E)`, `risk = σ·(E − S)`;
trigger when `favour > risk` (or `>=`), target `E + σ·offset_r·max(R, 0)` for every handle not yet at it. Synthetic
instrument and levels only.
"""
from __future__ import annotations

from collections.abc import Mapping
from decimal import ROUND_HALF_EVEN, Context, Decimal
from typing import ClassVar

from src.dto import Side as DtoSide
from src.marketdata.ports import HandleRef
from src.marketdata.types import M15, Bar, Quote, SeriesId, Side, Source
from src.riskmgr.guard import GuardResult, UnknownPolicy
from src.riskmgr.outcomes import Category, Outcome
from src.riskmgr.strategy import (
    ArmingRequirements,
    Evaluation,
    GuardSpec,
    HistoryNeed,
    IndicatorNeed,
    LoosenPolicy,
    Proposal,
    Refuse,
    ReleasePolicy,
    ReopenPolicy,
    StrategyParams,
    StrategyState,
    param_pairs,
    read_choice,
    read_decimal,
    read_int,
    reject_unknown,
)
from src.riskmgr.types import (
    BAR_END_EVENTS,
    ArmContext,
    ArmEvent,
    BarContext,
    BarEvent,
    EntryTimeSource,
    HandleView,
    Resource,
    RMultiple,
    S0Source,
)

D = Decimal
ADAPTER = "mock"
SYMBOL = "SYN.A"
BID_SERIES = SeriesId(ADAPTER, SYMBOL, M15, Side.BID, Source.BROKER)
T0 = 1_700_000_100_000 - 1_700_000_100_000 % 900_000   # a 15m bucket start
ZERO = D(0)


class ToyStrategy:
    KIND: ClassVar[str] = "toy_break_even"
    IMPL_VERSION: ClassVar[int] = 1
    RESOURCE: ClassVar[Resource] = Resource.STOP_LEVEL
    DECIMAL_CONTEXT: ClassVar[Context] = Context(prec=28, rounding=ROUND_HALF_EVEN)
    REASONS: ClassVar[Mapping[str, Category]] = {"BELOW_TRIGGER": Category.INFO, "NOT_IN_FAVOUR": Category.INFO}
    DECISION_METRICS: ClassVar[Mapping[str, type[RMultiple]]] = {"risk_r": RMultiple}
    REQUIRED_GUARDS: ClassVar[frozenset[str]] = frozenset({"toy_calm"})
    REOPEN_POLICY: ClassVar[ReopenPolicy] = ReopenPolicy.REUSE_TARGET
    RELEASE_POLICY: ClassVar[ReleasePolicy] = ReleasePolicy.ON_CONFIRMED_TARGET
    STATE_SCHEMA: ClassVar[frozenset[str]] = frozenset({"triggered", "armed_at_ms"})
    KEYS: ClassVar[frozenset[str]] = frozenset({"comparison", "offset_r"})

    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        reject_unknown(raw, self.KEYS, self.KIND)
        return param_pairs(comparison=read_choice(raw, "comparison", ("gt", "ge"), "gt", self.KIND),
                           offset_r=read_decimal(raw, "offset_r", D(0), D("0.5"), D(0), self.KIND))

    def migrate_state(self, from_impl_version: int, old_state: StrategyState) -> StrategyState | Refuse:
        return old_state if from_impl_version == self.IMPL_VERSION else Refuse("unknown_version")

    def triggers(self, params: StrategyParams) -> tuple[BarEvent, ...]:
        return (BarEvent(M15, ArmEvent.PRE), BarEvent(M15, ArmEvent.POST))

    def trigger_side(self, params: StrategyParams, side: DtoSide) -> Side:
        return Side.BID if side == "BUY" else Side.ASK

    def guards(self, params: StrategyParams) -> tuple[GuardSpec, ...]:
        return params.guards

    def loosen_policy(self, params: StrategyParams) -> LoosenPolicy:
        return LoosenPolicy.TELL_ONLY

    def latch(self, params: StrategyParams) -> bool:
        return False

    def required_indicators(self, params: StrategyParams) -> tuple[IndicatorNeed, ...]:
        return (IndicatorNeed("ATR", 1, M15, Side.BID, (("length", "14"),)),)

    def required_history(self, params: StrategyParams) -> tuple[HistoryNeed, ...]:
        return (HistoryNeed(M15, Side.BID, 2),)

    def arming_requirements(self, params: StrategyParams) -> ArmingRequirements:
        return ArmingRequirements()

    def init_state(self, params: StrategyParams, arm_ctx: ArmContext) -> StrategyState:
        return StrategyState.of(triggered=False, armed_at_ms=arm_ctx.armed_at_ms)

    def close_of(self, ctx: BarContext, event: ArmEvent) -> Decimal | None:
        if event in BAR_END_EVENTS:
            return None if ctx.bar is None else ctx.bar.close
        if ctx.quote is None:
            return None
        return ctx.quote.bid if ctx.sigma > 0 else ctx.quote.ask

    def favour(self, c: Context, sig: Decimal, close: Decimal, entry: Decimal) -> Decimal:
        return c.multiply(sig, c.subtract(close, entry))

    def target(self, params: StrategyParams, ctx: BarContext) -> Decimal:
        c = self.DECIMAL_CONTEXT
        r = max(ctx.r, ZERO)
        return c.add(ctx.entry, c.multiply(D(ctx.sigma), c.multiply(D(params.param("offset_r")), r)))

    def propose(self, params: StrategyParams, ctx: BarContext, state: StrategyState,
                event: ArmEvent) -> Evaluation:
        c = self.DECIMAL_CONTEXT
        sig = D(ctx.sigma)
        close = self.close_of(ctx, event)
        stop = ctx.stop_at_t if event in BAR_END_EVENTS and ctx.stop_at_t is not None else ctx.stop
        if close is None or stop is None:
            return Evaluation(False, reason="NOT_IN_FAVOUR")
        favour = self.favour(c, sig, close, ctx.entry)
        risk = c.multiply(sig, c.subtract(ctx.entry, stop))
        metrics: tuple[tuple[str, RMultiple], ...] = ()
        if ctx.r > 0:
            metrics = (("risk_r", RMultiple.of(c.divide(risk, ctx.r))),)
        if favour <= 0:
            return Evaluation(False, reason="NOT_IN_FAVOUR", metrics=metrics)
        met = favour > risk if params.param("comparison") == "gt" else favour >= risk
        if not met:
            return Evaluation(False, reason="BELOW_TRIGGER", metrics=metrics)
        target = self.target(params, ctx)
        moves = tuple((HandleRef(h.kind, h.handle_id), target) for h in ctx.handles
                      if c.multiply(sig, c.subtract(h.level, target)) < 0)
        return Evaluation(True, Proposal(moves), metrics=metrics)

    def commit_state(self, params: StrategyParams, state: StrategyState, evaluation: Evaluation,
                     final_outcome: Outcome) -> StrategyState:
        if not evaluation.trigger_met:
            return state
        return StrategyState.of(triggered=True, armed_at_ms=state.text("armed_at_ms"))

    def at_target(self, params: StrategyParams, ctx: BarContext, state: StrategyState) -> bool:
        c = self.DECIMAL_CONTEXT
        target = self.target(params, ctx)
        return bool(ctx.handles) and all(c.multiply(D(ctx.sigma), c.subtract(h.level, target)) >= 0
                                         for h in ctx.handles)


class ToyGuard:
    KIND: ClassVar[str] = "toy_calm"
    VERSION: ClassVar[int] = 1
    EVENTS: ClassVar[frozenset[ArmEvent]] = frozenset({ArmEvent.POST})
    REASONS: ClassVar[Mapping[str, Category]] = {"TOY_VOLATILE": Category.HOLD, "TOY_UNKNOWN": Category.HOLD}

    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        reject_unknown(raw, frozenset({"limit"}), self.KIND)
        return param_pairs(limit=read_int(raw, "limit", 1, 50, 5, self.KIND))

    def required_indicators(self, params: GuardSpec) -> tuple[IndicatorNeed, ...]:
        return ()

    def required_history(self, params: GuardSpec) -> tuple[HistoryNeed, ...]:
        return ()

    def applies(self, params: GuardSpec, ctx: BarContext, state: StrategyState) -> bool:
        return ctx.bar_index == 0

    def evaluate(self, params: GuardSpec, ctx: BarContext) -> GuardResult:
        if ctx.bar is None:
            return GuardResult.unknown("TOY_UNKNOWN")
        if ctx.bar.tick_count > int(params.param("limit")):
            return GuardResult.hold("TOY_VOLATILE")
        return GuardResult.passed()

    def known_before_bar_end(self, params: GuardSpec, ctx: BarContext) -> GuardResult | None:
        return None

    def on_timeout(self, params: GuardSpec) -> GuardResult:
        return GuardResult.unknown("TOY_UNKNOWN", "timeout")

    def unknown_policy(self, params: GuardSpec) -> UnknownPolicy:
        return UnknownPolicy.HOLD_BAR

    def conditional_label(self, params: GuardSpec) -> str:
        return "toy check pending at bar end"


# ---------------------------------------------------------------------------------------------------- fixtures
def toy_row(**extra: object) -> dict[str, object]:
    return {"comparison": "gt", "offset_r": "0", "guards": [{"kind": "toy_calm", "version": 1, "params": {}}],
            **extra}


def arm_ctx(side: str = "BUY") -> ArmContext:
    long = side == "BUY"
    return ArmContext(position_id="P1", adapter_id=ADAPTER, symbol=SYMBOL, side="BUY" if long else "SELL",
                      bar_spec=M15, entry=D("1.10000"), s0=D("1.09900") if long else D("1.10100"),
                      s0_source=S0Source.AT_FILL, entry_time_ms=T0 + 60_000, entry_time_source=EntryTimeSource.STREAM,
                      armed_at_ms=T0 + 61_000)


def bar(close: str, *, ticks: int = 3, bucket: int = T0) -> Bar:
    c = D(close)
    return Bar(series=BID_SERIES, bucket_start_ms=bucket, open=c, high=c, low=c, close=c, tick_count=ticks,
               final_at_ms=bucket + 960_000)


def ctx(close: str, *, side: str = "BUY", stop: str | None = None, event: ArmEvent = ArmEvent.POST,
        bar_index: int = 1, ticks: int = 3) -> BarContext:
    long = side == "BUY"
    level = D(stop) if stop is not None else (D("1.09900") if long else D("1.10100"))
    handle = HandleView(kind="CHILD_ORDER", handle_id="O1", level=level, read_ms=T0)
    bar_end = event in BAR_END_EVENTS
    q = None if bar_end else Quote(adapter_id=ADAPTER, symbol=SYMBOL, bid=D(close), ask=D(close),
                                   broker_ms=T0 + 850_000, received_ms=T0 + 850_100)
    return BarContext(position_id="P1", adapter_id=ADAPTER, symbol=SYMBOL, side="BUY" if long else "SELL",
                      bar_spec=M15, event=event, bar_end_ms=T0 + 900_000, bar_index=bar_index, entry=D("1.10000"),
                      s0=D("1.09900") if long else D("1.10100"), handles=(handle,), stop=level,
                      stop_at_t=level if bar_end else None, price_increment=D("0.00001"),
                      bar=bar(close, ticks=ticks) if bar_end else None, quote=q)
