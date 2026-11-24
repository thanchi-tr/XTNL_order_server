"""Kind (a) `break_even_on_risk_close`, version 1 (design §B6.3 (7), §B6.7, §20 Q1; plan S20, R11-m).

The rule (Q1 (a)): at a bar end, `favour = σ·(close − E)` and `risk = σ·(E − S)`; the trigger holds when
`favour > risk` (`≥` for `trigger_comparison = ge`), and then every stop handle not yet at the target is proposed
`target = E + σ·be_offset_r·max(R, 0)` (`R = σ·(E − S0)`; an `R ≤ 0` gives no buffer). Otherwise the reason is
`NOT_IN_FAVOUR` (favour ≤ 0) or `BELOW_TRIGGER` (INFO). With `be_offset_r > 0` and S0 not `AT_FILL` (§B7) the
proposal carries the caveat `be_offset_first_seen` ("buffer based on first-seen stop").

* **Close** (`trigger_basis`, Q5): `exit_side` (bid for a long, ask for a short; the default) or always `bid`
  (`trigger_side`). At bar-end events it is the broker's final bar of that side (`BarContext.bar`), at PRE that side
  of the quote frozen at fire time.
* **Stop:** at bar-end events `S_at_T` (`BarContext.stop_at_t`), or, while `pre_basis` is set, whichever of the two
  is farther from entry (§B6.3, R11-M04: a move made on an unconfirmed heads-up never makes the test easier, and a
  looser stop is never tested at a smaller risk); at PRE the current stop, with the same `pre_basis` rule (the
  engine's one stop-basis function, round 12 / S38c, so PRE previews what the bar end will test). The engine guarantees both inputs (pre-guards
  (3) and (5), §B6.4); a missing one is an engine fault and raises (`STRATEGY_ERROR`, S22).
* **`trigger_met`** is this bar's condition only. With `latch = on` a trigger recorded on an earlier final bar
  re-proposes the target, but never counts as this bar's `trigger_met` (§B6.3 (7a)). Default `latch = off` (Q11:
  re-test each bar).
* **`at_target`:** every current handle at or beyond the target (`σ·(level − target) ≥ 0`), which equals "at or beyond
  the target rounded toward profit" for on-grid levels and counts a handle exactly at an off-grid `E` with offset 0
  (§B6.3 (7a)). The proposal holds the unrounded target: rounding toward profit is the envelope's step (9) (S21).
* **REOPEN** (`REOPEN_POLICY = REUSE_TARGET`): the target recorded by `commit_state` on the original bar is reused.
* **Release** (`RELEASE_POLICY = ON_CONFIRMED_TARGET`, S38c): a final bar end with the trigger met and every handle at
  the target releases the position (§B6.3 (7a)).
* **State** (`STATE_SCHEMA`): `latched` (a trigger recorded on a final bar) and `target` (the target proposed with
  it, an order-state level). `commit_state` (final POST / LATE / REOPEN) records `trigger_met` whatever the
  post-guards said; after a FAULT the state is unchanged.
* **Metrics:** `risk_r`, the current risk `σ·(E − S)` in R (order state only; R > 0). `borderline`
  (`|favour / risk − 1| ≤ 0.03`) is a caveat; favour itself is never a metric (it gives the close back, §B8).
* **`loosen_policy`:** `rearm_on_loosen` on → `REARM`, off (Q10 default) → `TELL_ONLY`.
* **`latch`:** the `latch` param (`on` → True), read by the pipeline for SM11 advice withdrawal (§B12).

Pure; arithmetic under `DECIMAL_CONTEXT`. Synthetic levels only in tests; nothing here logs.
"""
from __future__ import annotations

from collections.abc import Mapping
from decimal import ROUND_HALF_EVEN, Context, Decimal
from typing import ClassVar, Final

from src.dto import Side
from src.marketdata.ports import HandleRef
from src.marketdata.timing import TIMING_TABLE
from src.marketdata.types import BarSpec, Source
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.outcomes import ENGINE_CATEGORY, Category, Outcome
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
    reject_unknown,
)
from src.riskmgr.types import (
    BAR_END_EVENTS,
    ArmContext,
    ArmEvent,
    BarContext,
    BarEvent,
    Ratio,
    Resource,
    RMultiple,
    S0Source,
)

NOT_IN_FAVOUR: Final = "NOT_IN_FAVOUR"
BELOW_TRIGGER: Final = "BELOW_TRIGGER"
BE_OFFSET_FIRST_SEEN: Final = "be_offset_first_seen"
BORDERLINE: Final = "borderline"
RISK_R: Final = "risk_r"

BE_OFFSET_FIRST_SEEN_TEXT: Final = "buffer based on first-seen stop"

TRIGGER_BASES: Final = ("exit_side", "bid")
COMPARISONS: Final = ("gt", "ge")
ON_OFF: Final = ("off", "on")
BE_OFFSET_DEFAULT: Final = Decimal("0.0")
BE_OFFSET_MIN: Final = Decimal("0")
BE_OFFSET_MAX: Final = Decimal("0.5")
BAR_SPECS: Final = tuple(sorted(s.name for s in TIMING_TABLE))
"""v1: the bar specs with a timing row (15m only, §B5.2)."""
BORDERLINE_BAND: Final = Decimal("0.03")
PARAM_KEYS: Final = frozenset({"trigger_basis", "trigger_comparison", "latch", "be_offset_r", "rearm_on_loosen",
                               "bar_spec"})
_ZERO: Final = Decimal(0)


class BreakEvenOnRiskClose:
    """Kind (a). See the module docstring."""

    KIND: ClassVar[str] = "break_even_on_risk_close"
    IMPL_VERSION: ClassVar[int] = 1
    RESOURCE: ClassVar[Resource] = Resource.STOP_LEVEL
    DECIMAL_CONTEXT: ClassVar[Context] = Context(prec=40, rounding=ROUND_HALF_EVEN)
    REASONS: ClassVar[Mapping[str, Category]] = {NOT_IN_FAVOUR: Category.INFO, BELOW_TRIGGER: Category.INFO}
    DECISION_METRICS: ClassVar[Mapping[str, type[RMultiple] | type[Ratio]]] = {RISK_R: RMultiple}
    REQUIRED_GUARDS: ClassVar[frozenset[str]] = frozenset({"first_bar"})
    REOPEN_POLICY: ClassVar[ReopenPolicy] = ReopenPolicy.REUSE_TARGET
    RELEASE_POLICY: ClassVar[ReleasePolicy] = ReleasePolicy.ON_CONFIRMED_TARGET
    STATE_SCHEMA: ClassVar[frozenset[str]] = frozenset({"latched", "target"})

    # ------------------------------------------------------------------------------------------------ params
    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        owner = self.KIND
        reject_unknown(raw, PARAM_KEYS, owner)
        return param_pairs(
            trigger_basis=read_choice(raw, "trigger_basis", TRIGGER_BASES, "exit_side", owner),
            trigger_comparison=read_choice(raw, "trigger_comparison", COMPARISONS, "gt", owner),
            latch=read_choice(raw, "latch", ON_OFF, "off", owner),
            be_offset_r=read_decimal(raw, "be_offset_r", BE_OFFSET_MIN, BE_OFFSET_MAX, BE_OFFSET_DEFAULT, owner),
            rearm_on_loosen=read_choice(raw, "rearm_on_loosen", ON_OFF, "off", owner),
            bar_spec=read_choice(raw, "bar_spec", BAR_SPECS, "15m", owner),
        )

    def migrate_state(self, from_impl_version: int, old_state: StrategyState) -> StrategyState | Refuse:
        if from_impl_version != self.IMPL_VERSION:
            return Refuse("unknown_version")
        if not old_state.names() <= self.STATE_SCHEMA:
            return Refuse("state_schema")
        return old_state

    # ------------------------------------------------------------------------------------------------ declarations
    def triggers(self, params: StrategyParams) -> tuple[BarEvent, ...]:
        spec = BarSpec.parse(params.param("bar_spec"))
        return (BarEvent(spec, ArmEvent.PRE), BarEvent(spec, ArmEvent.POST))

    def trigger_side(self, params: StrategyParams, side: Side) -> QuoteSide:
        if params.param("trigger_basis") == "bid" or side == "BUY":
            return QuoteSide.BID
        return QuoteSide.ASK

    def guards(self, params: StrategyParams) -> tuple[GuardSpec, ...]:
        return params.guards

    def loosen_policy(self, params: StrategyParams) -> LoosenPolicy:
        return LoosenPolicy.REARM if params.param("rearm_on_loosen") == "on" else LoosenPolicy.TELL_ONLY

    def latch(self, params: StrategyParams) -> bool:
        return params.param("latch") == "on"

    def target_is_entry(self, params: StrategyParams) -> bool:
        """`be_offset_r = 0` (Q6 default): the target `E + σ·be_offset_r·max(R, 0)` is `E` itself (notice wording
        only, §B8; never read by a decision)."""
        return Decimal(params.param("be_offset_r")) == 0

    def required_indicators(self, params: StrategyParams) -> tuple[IndicatorNeed, ...]:
        return ()

    def required_history(self, params: StrategyParams) -> tuple[HistoryNeed, ...]:
        return ()

    def arming_requirements(self, params: StrategyParams) -> ArmingRequirements:
        return ArmingRequirements()

    def init_state(self, params: StrategyParams, arm_ctx: ArmContext) -> StrategyState:
        return StrategyState.of(latched=False)

    # ------------------------------------------------------------------------------------------------ evaluation
    def target(self, params: StrategyParams, ctx: BarContext) -> Decimal:
        """`E + σ·be_offset_r·max(R, 0)`, unrounded."""
        c = self.DECIMAL_CONTEXT
        offset = c.multiply(Decimal(params.param("be_offset_r")), max(ctx.r, _ZERO))
        return c.add(ctx.entry, c.multiply(Decimal(ctx.sigma), offset))

    def _effective_target(self, params: StrategyParams, ctx: BarContext, state: StrategyState,
                          event: ArmEvent) -> Decimal:
        """The target in force: on REOPEN the recorded one (`REOPEN_POLICY = REUSE_TARGET`) when there is one, else
        `target(params, ctx)`. `propose` and `at_target` both use it, so the release check (§B6.3 (7a)) and the
        proposal never disagree after an S0 or E re-pin."""
        stored = state.text("target")
        if event is ArmEvent.REOPEN and stored is not None:
            return Decimal(stored)
        return self.target(params, ctx)

    def _close(self, params: StrategyParams, ctx: BarContext, event: ArmEvent) -> Decimal:
        side = self.trigger_side(params, ctx.side)
        if event in BAR_END_EVENTS:
            bar = ctx.bar
            if bar is None or bar.series.side is not side or bar.series.source is not Source.BROKER:
                raise ValueError(f"{self.KIND}: the final broker bar of the trigger side is missing")
            return bar.close
        if ctx.quote is None:
            raise ValueError(f"{self.KIND}: the frozen PRE quote is missing")
        return ctx.quote.bid if side is QuoteSide.BID else ctx.quote.ask

    def trigger_stop(self, ctx: BarContext, event: ArmEvent) -> Decimal:
        """The stop the trigger tests against: `S_at_T` at bar-end events, the current stop at PRE, and at both the
        farther of it and `pre_basis` while one is held (the engine's one stop-basis function, S38c)."""
        c = self.DECIMAL_CONTEXT
        if event not in BAR_END_EVENTS:
            if ctx.stop is None:
                raise ValueError(f"{self.KIND}: no current stop at PRE")
            stop = ctx.stop
        else:
            if ctx.stop_at_t is None:
                raise ValueError(f"{self.KIND}: no stop at bar end")
            stop = ctx.stop_at_t
        if ctx.pre_basis is not None:
            sig = Decimal(ctx.sigma)
            if c.multiply(sig, c.subtract(ctx.entry, ctx.pre_basis)) > c.multiply(sig, c.subtract(ctx.entry, stop)):
                stop = ctx.pre_basis
        return stop

    def propose(self, params: StrategyParams, ctx: BarContext, state: StrategyState,
                event: ArmEvent) -> Evaluation:
        c = self.DECIMAL_CONTEXT
        sig = Decimal(ctx.sigma)
        close = self._close(params, ctx, event)
        stop = self.trigger_stop(ctx, event)
        favour = c.multiply(sig, c.subtract(close, ctx.entry))
        risk = c.multiply(sig, c.subtract(ctx.entry, stop))
        met = favour >= risk if params.param("trigger_comparison") == "ge" else favour > risk
        caveats: list[str] = []
        if risk > 0 and c.subtract(favour, risk).copy_abs() <= c.multiply(BORDERLINE_BAND, risk):
            caveats.append(BORDERLINE)
        metrics: tuple[tuple[str, RMultiple], ...] = ()
        r = ctx.r
        current = ctx.stop if ctx.stop is not None else stop
        if r > 0:
            metrics = ((RISK_R, RMultiple.of(c.divide(c.multiply(sig, c.subtract(ctx.entry, current)), r))),)
        latched = params.param("latch") == "on" and state.flag("latched")
        if met or latched:
            target = self._effective_target(params, ctx, state, event)
            moves = tuple((HandleRef(h.kind, h.handle_id), target) for h in ctx.handles
                          if c.multiply(sig, c.subtract(h.level, target)) < 0)
            if Decimal(params.param("be_offset_r")) > 0 and ctx.s0_source is not S0Source.AT_FILL:
                caveats.append(BE_OFFSET_FIRST_SEEN)
            return Evaluation(met, Proposal(moves), metrics=metrics, caveats=tuple(caveats), target=target)
        reason = NOT_IN_FAVOUR if favour <= 0 else BELOW_TRIGGER
        return Evaluation(False, reason=reason, metrics=metrics, caveats=tuple(caveats))

    def commit_state(self, params: StrategyParams, state: StrategyState, evaluation: Evaluation,
                     final_outcome: Outcome) -> StrategyState:
        if ENGINE_CATEGORY.get(final_outcome) is Category.FAULT or final_outcome is Outcome.UNRESOLVED_RANGE:
            return state
        if evaluation.trigger_met:
            target = evaluation.target
            if target is None:                   # an Evaluation built without its target: the first proposed level
                targets = evaluation.proposal.targets if evaluation.proposal is not None else ()
                target = targets[0][1] if targets else (Decimal(t) if (t := state.text("target")) is not None
                                                        else None)
            return StrategyState.of(latched=True, target=target)
        if params.param("latch") == "on":
            return state                         # a recorded trigger stays recorded
        return StrategyState.of(latched=False)

    def at_target(self, params: StrategyParams, ctx: BarContext, state: StrategyState) -> bool:
        """Every handle at or beyond the target `propose` would use for this context (`_effective_target`, so on
        REOPEN the recorded target, never one recomputed from a re-pinned R)."""
        c = self.DECIMAL_CONTEXT
        target = self._effective_target(params, ctx, state, ctx.event)
        sig = Decimal(ctx.sigma)
        return bool(ctx.handles) and all(c.multiply(sig, c.subtract(h.level, target)) >= 0 for h in ctx.handles)


BREAK_EVEN_ON_RISK_CLOSE_V1: Final = BreakEvenOnRiskClose()

__all__ = [
    "BreakEvenOnRiskClose", "BREAK_EVEN_ON_RISK_CLOSE_V1", "NOT_IN_FAVOUR", "BELOW_TRIGGER", "BE_OFFSET_FIRST_SEEN",
    "BE_OFFSET_FIRST_SEEN_TEXT", "BORDERLINE", "RISK_R",
]
