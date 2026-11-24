"""The envelope pipeline: one evaluation of one position at one event (design §B6.2, §B6.3, §B6.4, §B5.2, §B12;
plan S21, R11-M04, IN-10, R11-m).

`run_pipeline(inp)` is pure: every input (the pinned params and state, the `BarContext`, `S_at_T`, the stored
`pre_basis`, this bar's PRE levels, the live quote, the calendar, the clock reading) is handed in, and the answer is a
`PipelineResult` value. The job runner (S22) runs it under the pinned Decimal context with its time cap; the BarClock
(S27) writes what it returns. First match wins:

**Pre-guards** (1) account image fresh, else `DATA_UNAVAILABLE(<first image reason>)`; (2) under PR9 the oldest
handle read no older than 30 s at PRE and 15 s otherwise, else `DATA_UNAVAILABLE(stop_stale)`; (3) an effective stop
(the current handles), else `NO_STOP`; (3b) the price increment known, else `DATA_UNAVAILABLE(increment_unknown)`;
(4) a released position (`STRATEGY_COMPLETE`) gets no new advice: still at target → `COMPLETE` recorded, never shown; a
handle below the target with `TELL_ONLY` → `LOOSENED_AFTER_COMPLETE` (shown once per episode); with `REARM` it
returns to TRACKING (`REARM_ON_LOOSEN`) and goes on. Its FAULTs from (1)-(3b) are recorded, never shown; (5) a
bucket `md-bars` resolved `no_trade` (`bar_no_trade`) is `NO_BROKER_BAR` (INFO, final, recorded only, no trigger, no
state change; S38c; when this bar's PRE proposed a move it is shown with the reason `pre_not_confirmed` and the PRE
levels fold into the stored `pre_basis`, as every other non-confirming bar end does); a bar-end decision without the broker's final bar is PENDING ("awaiting broker bar", no row)
until `decision_cap`, then `UNRESOLVED(broker_bar_not_final)`.

**Guards** (6): each pinned guard, in order, on an event in its `EVENTS` where `applies()` holds (`POST` in `EVENTS`
covers every bar-end event); `final_verdict` maps PENDING to `DATA_UNAVAILABLE(<kind>_pending)` until its `until`
(never later than `decision_cap`), then to `on_timeout` and `unknown_policy`. At PRE a POST-only guard is never run:
`known_before_bar_end` (FirstBarGuard: `PRE_ANCHOR`) or else its `conditional_label` on the item. A HOLD reason
the guard did not declare in `REASONS` is `PipelineFault(guard:<KIND>:<VERSION>)`, never stored (as a
`GUARD_REASON` or a `PRE_NOT_CONFIRMED`); likewise an undeclared strategy reason is
`PipelineFault(strategy:<KIND>:<VERSION>)`.

**Strategy** (7): `propose` on the context, at bar-end events with the trigger stop set by the basis rule below.

**Release check** (7a), bar-end events only, defined on the `Evaluation` (`trigger_met`, the proposal) and
`at_target`, never on one kind's reasons: trigger met and at target → `COMPLETE` (released; needs no open market)
when the strategy declares `RELEASE_POLICY = ON_CONFIRMED_TARGET` (kind (a)); a `NEVER` strategy stays managed there
(`AT_TARGET_UNCONFIRMED`, round 12 / S38c);
at target without the trigger → `AT_TARGET_UNCONFIRMED` (shown only on the first bar of an at-target run); if this
bar's PRE proposed a move and the bar end ends in a guard HOLD, a strategy reason with `trigger_met` false or
`AT_TARGET_UNCONFIRMED`, the outcome is `PRE_NOT_CONFIRMED(reason)` (INFO, shown) and `pre_basis` is stored.

**Post-guards** (8)-(13) (`apply_envelope`, also at PRE on the frozen quote): (8) a target that would loosen a current
handle → `BROKER_AHEAD` for that handle; (9) round toward profit to the increment; (10) market not open now (SM10
`OPEN` / `OPEN_BY_EVIDENCE` through `CalendarPort.state()`, None counting as closed, IN-10) → `MARKET_CLOSED`;
(11) exit quote not live → `DATA_UNAVAILABLE(exit_quote_not_live)`; (12) `σ·(quote − target) < max(min_gap_r·max(R, 0),
min_stop_distance, one increment)` on the exit side (bid for a long, ask for a short) → `PRICE_THROUGH_LEVEL`;
(13) `ADJUST`. The envelope only turns an `ADJUST` into a HOLD, a FAULT or `BROKER_AHEAD`; it never adds a handle and
never moves a target except by rounding it toward profit (the property test, plan S21).

**Stop basis** (§B6.4, R11-M04 option (a), round-11 confirmation C1): the trigger tests against `S_at_T`, or, while a
basis is held (this bar's PRE levels when the PRE proposed a move, and the stored `pre_basis` after a
`PRE_NOT_CONFIRMED`), the one of `S_pre` and `S_at_T` farther from entry (`stop_basis = AT_PRE` when that is
`S_pre`). **A move that answers final advice is never discounted** (round 14, plan S38g): while the position is
ADVICE_OUTSTANDING and `S_at_T` moved toward entry after this bar's PRE but not past the target of the outstanding
final `ADJUST` (`final_advice_target`, the newest final POST / LATE / REOPEN / CORRECTED `ADJUST` of an earlier bar;
for a REOPEN or CORRECTED re-run only an `ADJUST` that is the newest final decision of an earlier bar, so withdrawn
advice never counts: `evaluation.final_advice_target_of`),
this bar's PRE levels are left out: the move was made on final advice, not on the heads-up, so `S_at_T` (Q8's current
stop) is tested, no `PRE_NOT_CONFIRMED` is given, no `pre_basis` is stored, and the decision carries the caveat
`moved_on_final_advice`. The stored basis is cleared when `S_at_T` is at or beyond `S_pre` away from entry (the stop was loosened back
past it), and when a bar end confirms the trigger against it. The bar-end stop is "current" here (Q8): a loosening
made after `T` is seen at the next bar end, so a decision never tests at a stop the bar did not end with.

**PRE** (§B5.2, D1-D3): no item when its valid-until (`fire + 10 s`, S38c) has passed, for a fill
in the last `pre_offset` of the bar, for a released position, in a closed period, or when the position is at its
target with no basis held (checked before the guards, so a guard's known HOLD never gives a row then); while a
basis is held PRE tests the trigger with the same stop-basis function as the bar end (`stopbasis.trigger_basis`:
the farther of the stored `S_pre` and the current stop, caveat `pre_basis_held`), and a position at its target then
gets an `AT_TARGET_UNCONFIRMED` item (round 12, S38c); the quote is the one frozen
at fire time (a quote received after fire is refused, never used); PRE never releases, never changes the SM11 state
(`sm11_events` is empty), never runs FirstBarGuard and never stores a basis (its `pre_levels` are stored on the PRE
row, for the POST of the same bar).

Nothing here logs; levels are order state and stay out of every `repr`.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN, Context, Decimal
from enum import Enum
from typing import Final

from src.marketdata.ports import CalendarPort, HandleRef, calendar_open
from src.marketdata.timing import COLD_SKEW, SkewEstimate, decision_cap_ms, pre_fire_ms, pre_valid_until_ms, timing_for
from src.marketdata.types import BarSpec, Quote
from src.riskmgr.envelope import EnvelopeParams, EnvelopeStamp, stamp
from src.riskmgr.guard import Guard, GuardResult, GuardStatus, final_verdict, guard_scope_id, strategy_scope_id
from src.riskmgr.outcomes import (
    BROKER_BAR_NOT_FINAL,
    EXIT_QUOTE_NOT_LIVE,
    INCREMENT_UNKNOWN,
    STOP_STALE,
    Category,
    Outcome,
    OutcomeCode,
)
from src.riskmgr.registry import REGISTRY, ArmRegistry
from src.riskmgr.sm11 import ArmState, Sm11Event
from src.riskmgr.stopbasis import (
    PreBasis,
    StopAtT,
    StopUnavailable,
    farther_basis,
    farther_from_entry,
    trigger_basis,
)
from src.riskmgr.strategy import (
    Evaluation,
    GuardSpec,
    LoosenPolicy,
    ReleasePolicy,
    SlStrategy,
    StrategyParams,
    StrategyState,
)
from src.riskmgr.types import (
    BAR_END_EVENTS,
    ArmEvent,
    BarContext,
    CloseSource,
    MetricValue,
    StopBasis,
)

ENVELOPE_CONTEXT: Final = Context(prec=40, rounding=ROUND_HALF_EVEN)
"""The envelope's arithmetic (rounding, the gap) runs under this fixed context, never the ambient one."""
PRE_STOP_READ_BOUND_MS: Final = 30_000
STOP_READ_BOUND_MS: Final = 15_000

ACCOUNT_IMAGE_STALE: Final = "account_image_stale"
PRE_QUOTE_UNAVAILABLE: Final = "pre_quote_unavailable"
PRE_QUOTE_AFTER_FIRE: Final = "pre_quote_after_fire"
"""DATA_UNAVAILABLE reasons added by the pipeline (codes only)."""

AWAITING_BROKER_BAR: Final = "awaiting_broker_bar"
PRE_EXPIRED: Final = "pre_expired"
FILL_IN_PRE_WINDOW: Final = "fill_in_pre_window"
MARKET_CLOSED_AT_PRE: Final = "market_closed"
RELEASED: Final = "released"
AT_TARGET: Final = "at_target"
"""Reasons of a PENDING or NO_ITEM result (no row is written)."""

PREVIOUS_POST_PENDING: Final = "previous_post_pending"
"""PRE caveat: the previous bar's POST is still pending (§B5.2)."""
NO_TRADE_PRE_NOT_CONFIRMED: Final = "pre_not_confirmed"
"""`NO_BROKER_BAR` reason: this bar's PRE proposed a move the bar end could not confirm (no broker bar), so the PRE
levels are kept in the stored basis and the item is shown as "not confirmed" (S38c review)."""
MOVED_ON_FINAL_ADVICE: Final = "moved_on_final_advice"
"""Bar-end caveat (round 14, plan S38g): the stop moved after this bar's PRE answers an outstanding final ADJUST, so
the PRE levels are not used (no AT_PRE, no PRE_NOT_CONFIRMED, no stored basis)."""
PRE_BASIS_HELD: Final = "pre_basis_held"
"""PRE caveat: a `pre_basis` is held, so the trigger is tested against the stop as it was before the heads-up move
(the one stop-basis function, S38c)."""


def pending_reason(guard_kind: str) -> str:
    """`DATA_UNAVAILABLE(<kind>_pending)` while a guard is PENDING (FirstBarGuard: `first_bar_pending`, §B6.5)."""
    return f"{guard_kind}_pending"


class ResultKind(str, Enum):
    """`DECISION`: a row to record; `PENDING`: no row yet, decide again later (POST awaiting the broker's final bar);
    `NO_ITEM`: nothing to record or send (PRE only)."""

    DECISION = "DECISION"
    PENDING = "PENDING"
    NO_ITEM = "NO_ITEM"


class PipelineFault(Exception):
    """A strategy or guard raised (or a pinned version is missing): a `STRATEGY_ERROR` attributed to `scope_id`
    (`strategy:<KIND>:<VERSION>` or `guard:<KIND>:<VERSION>`, §B4 Job runner, S22). The original exception text is
    not carried (it could hold a value)."""

    def __init__(self, scope_id: str) -> None:
        super().__init__(scope_id)
        self.scope_id = scope_id


# ---------------------------------------------------------------------------------------------------- PRE window
@dataclass(frozen=True)
class PreWindow:
    """PRE of bar `T`: fires at local `T + p50 − pre_offset`, valid until `fire + 10 s` (S38c)."""

    fire_ms: int
    valid_until_ms: int
    pre_offset_s: int


def pre_window(bar_end_ms: int, spec: BarSpec, skew: SkewEstimate, pre_offset_s: int) -> PreWindow:
    row = timing_for(spec)
    if row is None:
        raise ValueError(f"bar spec {spec.name} has no timing row")
    fire = pre_fire_ms(bar_end_ms, skew, pre_offset_s)
    return PreWindow(fire, pre_valid_until_ms(fire, bar_end_ms, row, skew), pre_offset_s)


# ---------------------------------------------------------------------------------------------------- input, result
@dataclass(frozen=True, kw_only=True)
class PipelineInput:
    """Everything one evaluation reads. `ctx.stop` / `ctx.handles` are the current handles (PR1); at bar-end events
    `stop_at_t` is `S_at_T` (§B6.4) and `live_quote` the exit quote now; at PRE `ctx.quote` is the trigger-side quote
    frozen at fire time and `window` the PRE window. `this_bar_pre` holds the levels this bar's PRE used when it
    proposed a move (an `ADJUST`), `pre_basis` the stored basis. `prev_final_at_target`: the position's previous
    final decision was at target; `loosened_told`: `LOOSENED_AFTER_COMPLETE` was already shown this episode."""

    params: StrategyParams
    ctx: BarContext
    state: StrategyState = field(repr=False)
    arm_state: ArmState
    now_ms: int
    calendar: CalendarPort
    envelope: EnvelopeParams = field(default_factory=EnvelopeParams)
    stop_at_t: StopAtT | StopUnavailable | None = None
    live_quote: Quote | None = None
    window: PreWindow | None = None
    this_bar_pre: PreBasis | None = None
    pre_basis: PreBasis | None = None
    prev_final_at_target: bool = False
    loosened_told: bool = False
    stop_reads: bool = False
    previous_post_pending: bool = False
    final_advice_target: Decimal | None = field(default=None, repr=False)
    """Bar-end events: the proposed stop of the outstanding final `ADJUST` of an earlier bar (order state; round 14;
    `evaluation.final_advice_target_of`). Used only while the position is ADVICE_OUTSTANDING (`answers_final_advice`)."""
    bar_no_trade: bool = False
    """Bar-end events: `md-bars` resolved the trigger bucket `no_trade` (positive evidence, never a missing bar):
    the decision is `NO_BROKER_BAR` (S38c)."""


@dataclass(frozen=True, kw_only=True)
class PipelineResult:
    """One evaluation's answer. `final` = the decision decides the bar (advances `last_decided_bar`; a POST-time
    `DATA_UNAVAILABLE` does not); `shown` False = the `suppressed` sentinel. `targets` are the rounded `ADJUST`
    targets; `state` / `pre_basis` what `arm_positions` holds afterwards; `pre_levels` the levels a PRE `ADJUST` used
    (stored on the PRE row); `sm11_events` the SM11 events of this decision, in order (none at PRE)."""

    event: ArmEvent
    kind: ResultKind
    envelope: EnvelopeStamp
    code: OutcomeCode | None = None
    reason: str = ""
    final: bool = False
    shown: bool = True
    trigger_met: bool | None = None
    at_target: bool | None = None
    targets: tuple[tuple[HandleRef, Decimal], ...] = field(default=(), repr=False)
    broker_ahead: tuple[HandleRef, ...] = ()
    stop_basis: StopBasis | None = None
    close_source: CloseSource | None = None
    metrics: tuple[tuple[str, MetricValue], ...] = ()
    caveats: tuple[str, ...] = ()
    labels: tuple[str, ...] = ()
    guard_verdicts: tuple[tuple[str, GuardResult], ...] = ()
    evaluation: Evaluation | None = None
    state: StrategyState = field(default_factory=StrategyState, repr=False)
    pre_basis: PreBasis | None = None
    pre_levels: PreBasis | None = None
    sm11_events: tuple[Sm11Event, ...] = ()

    @property
    def outcome(self) -> Outcome | None:
        return None if self.code is None else self.code.outcome


# ---------------------------------------------------------------------------------------------------- envelope
@dataclass(frozen=True)
class EnvelopeOutcome:
    """Post-guards (8)-(13) on one proposal: the code, the rounded targets of the handles to move (ADJUST only)
    and the handles the broker is already ahead of."""

    code: OutcomeCode
    targets: tuple[tuple[HandleRef, Decimal], ...] = field(default=(), repr=False)
    broker_ahead: tuple[HandleRef, ...] = ()


def round_toward_profit(level: Decimal, increment: Decimal, sig: int) -> Decimal:
    """Step (9): onto the increment grid, toward profit (up for a long, down for a short)."""
    c = ENVELOPE_CONTEXT
    steps = c.divide(level, increment).to_integral_value(rounding=ROUND_CEILING if sig > 0 else ROUND_FLOOR)
    return c.multiply(steps, increment)


def apply_envelope(ctx: BarContext, evaluation: Evaluation, *, quote: Quote | None, quote_live: bool,
                   market_open: bool, envelope: EnvelopeParams) -> EnvelopeOutcome | None:
    """Steps (8)-(13) on `evaluation.proposal` against the current handles (`ctx.handles`), the price increment and
    `quote`. None when the proposal names no handle to move (the caller decides what that means)."""
    c = ENVELOPE_CONTEXT
    proposal = evaluation.proposal
    increment = ctx.price_increment
    if proposal is None or increment is None:
        return None
    sig = ctx.sigma
    s = Decimal(sig)
    current = {HandleRef(h.kind, h.handle_id): h.level for h in ctx.handles}
    moves: list[tuple[HandleRef, Decimal]] = []
    ahead: list[HandleRef] = []
    for handle, target in proposal.targets:
        level = current.get(handle)
        if level is None or c.multiply(s, c.subtract(target, level)) < 0:
            ahead.append(handle)                                   # (8) the target would loosen it (or it is gone)
        elif c.multiply(s, c.subtract(target, level)) > 0:
            moves.append((handle, round_toward_profit(target, increment, sig)))   # (9)
    if not moves:
        if ahead:
            return EnvelopeOutcome(OutcomeCode.engine(Outcome.BROKER_AHEAD), broker_ahead=tuple(ahead))
        return None
    if not market_open:                                            # (10)
        return EnvelopeOutcome(OutcomeCode.engine(Outcome.MARKET_CLOSED), broker_ahead=tuple(ahead))
    if not quote_live or quote is None:                            # (11)
        return EnvelopeOutcome(OutcomeCode.engine(Outcome.DATA_UNAVAILABLE, EXIT_QUOTE_NOT_LIVE),
                               broker_ahead=tuple(ahead))
    exit_price = quote.bid if sig > 0 else quote.ask
    floor = max(c.multiply(envelope.min_gap_r, max(ctx.r, Decimal(0))),
                ctx.min_stop_distance if ctx.min_stop_distance is not None else Decimal(0), increment)
    if any(c.multiply(s, c.subtract(exit_price, t)) < floor for _, t in moves):   # (12)
        return EnvelopeOutcome(OutcomeCode.engine(Outcome.PRICE_THROUGH_LEVEL), broker_ahead=tuple(ahead))
    return EnvelopeOutcome(OutcomeCode.engine(Outcome.ADJUST), tuple(moves), tuple(ahead))   # (13)


# ---------------------------------------------------------------------------------------------------- helpers
def _dedupe(items: Sequence[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(items))


def _strategy(registry: ArmRegistry, params: StrategyParams) -> SlStrategy:
    strategy = registry.strategy(params.kind, params.impl_version)
    if strategy is None:
        raise PipelineFault(strategy_scope_id(params.kind, params.impl_version))
    return strategy


def _guard(registry: ArmRegistry, spec: GuardSpec) -> Guard:
    guard = registry.guard(spec.kind, spec.version)
    if guard is None:
        raise PipelineFault(guard_scope_id(spec.kind, spec.version))
    return guard


def _cap(ctx: BarContext) -> int:
    if ctx.decision_cap_ms is not None:
        return ctx.decision_cap_ms
    return decision_cap_ms(ctx.bar_end_ms, ctx.bar_spec, COLD_SKEW)


def _pre_guard_fault(inp: PipelineInput, bound_ms: int) -> OutcomeCode | None:
    """Pre-guards (1)-(3b)."""
    ctx = inp.ctx
    if not ctx.account_image_fresh:
        reason = ctx.image_reasons[0] if ctx.image_reasons else ACCOUNT_IMAGE_STALE
        return OutcomeCode.engine(Outcome.DATA_UNAVAILABLE, reason)
    if inp.stop_reads and ctx.handles:
        reads = [h.read_ms for h in ctx.handles]
        if any(r is None for r in reads) or inp.now_ms - min(r for r in reads if r is not None) > bound_ms:
            return OutcomeCode.engine(Outcome.DATA_UNAVAILABLE, STOP_STALE)
    if ctx.stop is None or not ctx.handles:
        return OutcomeCode.engine(Outcome.NO_STOP)
    increment = ctx.price_increment
    if increment is None or not increment.is_finite() or increment <= 0:
        return OutcomeCode.engine(Outcome.DATA_UNAVAILABLE, INCREMENT_UNKNOWN)
    return None


def _call_propose(strategy: SlStrategy, inp: PipelineInput, ctx: BarContext) -> Evaluation:
    try:
        return strategy.propose(inp.params, ctx, inp.state, ctx.event)
    except Exception:
        raise PipelineFault(strategy_scope_id(strategy.KIND, strategy.IMPL_VERSION)) from None


def _call_at_target(strategy: SlStrategy, inp: PipelineInput, ctx: BarContext) -> bool:
    try:
        return bool(strategy.at_target(inp.params, ctx, inp.state))
    except Exception:
        raise PipelineFault(strategy_scope_id(strategy.KIND, strategy.IMPL_VERSION)) from None


def _commit(strategy: SlStrategy, inp: PipelineInput, evaluation: Evaluation | None,
            code: OutcomeCode) -> StrategyState:
    if evaluation is None or code.category is Category.FAULT:
        return inp.state
    try:
        return strategy.commit_state(inp.params, inp.state, evaluation, code.outcome)
    except Exception:
        raise PipelineFault(strategy_scope_id(strategy.KIND, strategy.IMPL_VERSION)) from None


@dataclass
class _Guards:
    verdicts: list[tuple[str, GuardResult]] = field(default_factory=list)
    metrics: list[tuple[str, MetricValue]] = field(default_factory=list)
    caveats: list[str] = field(default_factory=list)
    labels: list[str] = field(default_factory=list)


def _run_guards(inp: PipelineInput, strategy: SlStrategy, registry: ArmRegistry,
                acc: _Guards) -> tuple[Guard, GuardResult] | None:
    """Step (6): the first guard verdict that stops the pipeline (HOLD or PENDING), or None when every applicable
    guard passed (or, at PRE, was shown as a label)."""
    ctx = inp.ctx
    run_as = ArmEvent.POST if ctx.event in BAR_END_EVENTS else ArmEvent.PRE
    try:
        specs = strategy.guards(inp.params)
    except Exception:
        raise PipelineFault(strategy_scope_id(strategy.KIND, strategy.IMPL_VERSION)) from None
    for spec in specs:
        guard = _guard(registry, spec)
        scope = guard_scope_id(guard.KIND, guard.VERSION)
        try:
            if not guard.applies(spec, ctx, inp.state):
                continue
            if run_as not in guard.EVENTS:                    # PRE for a POST-only guard: never run (§B6.1)
                known = guard.known_before_bar_end(spec, ctx)
                if known is not None and known.status is GuardStatus.HOLD:
                    acc.verdicts.append((guard.KIND, known))
                    return guard, known
                acc.labels.append(guard.conditional_label(spec))
                continue
            raw = guard.evaluate(spec, ctx)
            if raw.status is GuardStatus.PENDING and raw.until_ms is not None:
                raw = GuardResult.pending(min(raw.until_ms, _cap(ctx)))
            verdict = final_verdict(guard, spec, raw, inp.now_ms)
        except PipelineFault:
            raise
        except Exception:
            raise PipelineFault(scope) from None
        acc.verdicts.append((guard.KIND, verdict))
        if verdict.status in (GuardStatus.HOLD, GuardStatus.PENDING):
            return guard, verdict
        acc.metrics.extend(verdict.metrics)
        acc.caveats.extend(verdict.caveats)
    return None


def _guard_code(guard: Guard, verdict: GuardResult, *, pre_not_confirmed: bool = False) -> OutcomeCode:
    """The guard's HOLD as `GUARD_REASON(reason)` (or `PRE_NOT_CONFIRMED(reason)` when this bar's PRE proposed a
    move). A reason the guard did not declare in `REASONS` (or a malformed detail) is the guard's fault, attributed
    to its version, never stored."""
    try:
        if verdict.reason not in guard.REASONS:
            raise ValueError("undeclared reason")
        if pre_not_confirmed:
            return OutcomeCode.pre_not_confirmed(guard.KIND, verdict.reason, detail=verdict.detail)
        return OutcomeCode.kind_reason(Outcome.GUARD_REASON, guard.KIND, verdict.reason, guard.REASONS,
                                       detail=verdict.detail)
    except ValueError:
        raise PipelineFault(guard_scope_id(guard.KIND, guard.VERSION)) from None


def _strategy_code(strategy: SlStrategy, evaluation: Evaluation, *, pre_not_confirmed: bool = False) -> OutcomeCode:
    """The strategy's no-proposal reason as `STRATEGY_REASON(reason)` (or `PRE_NOT_CONFIRMED(reason)`). A reason
    not in the strategy's `REASONS` is the strategy's fault (`PipelineFault(strategy:<KIND>:<VERSION>)`)."""
    try:
        if evaluation.reason not in strategy.REASONS:
            raise ValueError("undeclared reason")
        if pre_not_confirmed:
            return OutcomeCode.pre_not_confirmed(strategy.KIND, evaluation.reason)
        return OutcomeCode.kind_reason(Outcome.STRATEGY_REASON, strategy.KIND, evaluation.reason, strategy.REASONS)
    except ValueError:
        raise PipelineFault(strategy_scope_id(strategy.KIND, strategy.IMPL_VERSION)) from None


def _latch(strategy: SlStrategy, inp: PipelineInput) -> bool:
    try:
        return bool(strategy.latch(inp.params))
    except Exception:
        raise PipelineFault(strategy_scope_id(strategy.KIND, strategy.IMPL_VERSION)) from None


def _sm11(arm_state: ArmState, code: OutcomeCode, evaluation: Evaluation | None, *,
          rearmed: bool, latch: bool) -> tuple[Sm11Event, ...]:
    """The SM11 events of one final bar-end decision, in order (`sm11.steps` applies them). §B12: the first POST
    (any outcome, a final FAULT included) leaves ARMED; a POST without `ADJUST` withdraws outstanding advice when
    the strategy's latch is off, and with the latch on only when the strategy itself proposed nothing (a latched
    trigger re-proposes, so its advice stands through a post-guard HOLD or a guard HOLD)."""
    events: list[Sm11Event] = [Sm11Event.REARM_ON_LOOSEN] if rearmed else []
    state = ArmState.TRACKING if rearmed else arm_state
    if code.outcome is Outcome.COMPLETE:
        return (*events, Sm11Event.RELEASED)
    if state is ArmState.ARMED:
        events.append(Sm11Event.FIRST_POST)
        state = ArmState.TRACKING
    if code.outcome is Outcome.ADJUST:
        events.append(Sm11Event.FINAL_ADJUST)
    elif state is ArmState.ADVICE_OUTSTANDING and (
            not latch or (evaluation is not None and evaluation.proposal is None)):
        events.append(Sm11Event.ADVICE_WITHDRAWN)
    return tuple(events)


def answers_final_advice(inp: PipelineInput) -> bool:
    """Round 14 (plan S38g): the stop at bar end answers an outstanding final `ADJUST`, so this bar's PRE levels do
    not apply. True when the position is ADVICE_OUTSTANDING with a final advice target, this bar's PRE proposed a
    move, `S_at_T` is nearer entry than that PRE's `S_pre` (the stop moved after the PRE) and not past the target
    (`S_at_T` at or short of it: a move beyond the advised target is not covered by the advice). A target on the loss
    side of the current E never counts (live review): kind (a) advises break-even or beyond, so such a target was
    advised under an E since re-pinned (partial fill, entry correction) and a move to it answers nothing current; the
    bar then keeps its PRE levels (the conservative basis)."""
    pre, target, s_at_t = inp.this_bar_pre, inp.final_advice_target, inp.stop_at_t
    if (pre is None or target is None or inp.arm_state is not ArmState.ADVICE_OUTSTANDING
            or not isinstance(s_at_t, StopAtT)):
        return False
    side, entry = inp.ctx.side, inp.ctx.entry
    if farther_from_entry(side, entry, target, entry):
        return False
    return (farther_from_entry(side, entry, pre.s_pre, s_at_t.level)
            and not farther_from_entry(side, entry, target, s_at_t.level))


# ---------------------------------------------------------------------------------------------------- the pipeline
def run_pipeline(inp: PipelineInput, registry: ArmRegistry = REGISTRY) -> PipelineResult:
    """One evaluation (see the module docstring). Raises `PipelineFault` when the strategy or a guard raises."""
    strategy = _strategy(registry, inp.params)
    if inp.ctx.event is ArmEvent.PRE:
        return _pre(inp, strategy, registry)
    return _bar_end(inp, strategy, registry)


def _pre(inp: PipelineInput, strategy: SlStrategy, registry: ArmRegistry) -> PipelineResult:
    ctx = inp.ctx
    window = inp.window
    if window is None:
        raise ValueError("a PRE evaluation needs its window")
    stamp_ = stamp(inp.envelope)
    base = PipelineResult(event=ArmEvent.PRE, kind=ResultKind.NO_ITEM, envelope=stamp_, state=inp.state,
                          pre_basis=inp.pre_basis, close_source=CloseSource.QUOTE)

    def no_item(reason: str) -> PipelineResult:
        return replace(base, reason=reason)

    def row(code: OutcomeCode, **kw: object) -> PipelineResult:
        caveats = (PREVIOUS_POST_PENDING,) if inp.previous_post_pending else ()
        out = replace(base, kind=ResultKind.DECISION, code=code, caveats=caveats)
        return replace(out, **kw)   # type: ignore[arg-type]

    if inp.now_ms > window.valid_until_ms:
        return no_item(PRE_EXPIRED)
    if inp.arm_state is ArmState.STRATEGY_COMPLETE:
        return no_item(RELEASED)
    if ctx.entry_time_ms is not None and ctx.entry_time_ms > ctx.bar_end_ms - window.pre_offset_s * 1000:
        return no_item(FILL_IN_PRE_WINDOW)          # D1: a fill in the last pre_offset of the bar gets no PRE
    if not calendar_open(inp.calendar, ctx.adapter_id, ctx.symbol):
        return no_item(MARKET_CLOSED_AT_PRE)
    fault = _pre_guard_fault(inp, PRE_STOP_READ_BOUND_MS)
    if fault is not None:
        return row(fault)
    quote = ctx.quote
    if quote is None:
        return row(OutcomeCode.engine(Outcome.DATA_UNAVAILABLE, PRE_QUOTE_UNAVAILABLE))
    if quote.received_ms > window.fire_ms:          # PRE computes nothing from a quote taken after fire
        return row(OutcomeCode.engine(Outcome.DATA_UNAVAILABLE, PRE_QUOTE_AFTER_FIRE))
    current = ctx.stop
    if current is None:                             # pre-guard (3) answered NO_STOP already
        return row(OutcomeCode.engine(Outcome.NO_STOP))
    tb = trigger_basis(ctx.side, ctx.entry, current, None, inp.pre_basis)   # the one basis (S38c)
    held_caveat = (PRE_BASIS_HELD,) if tb.held is not None else ()
    trigger_ctx = replace(ctx, pre_basis=tb.s_pre, stop_basis=tb.basis)
    at_target = _call_at_target(strategy, inp, trigger_ctx)
    if at_target and tb.held is None:
        return no_item(AT_TARGET)                   # at PRE a position at its target gets no item (before guards)
    acc = _Guards()
    stop = _run_guards(inp, strategy, registry, acc)
    if stop is not None:
        guard, verdict = stop
        if verdict.status is GuardStatus.PENDING:
            return row(OutcomeCode.engine(Outcome.DATA_UNAVAILABLE, pending_reason(guard.KIND)),
                       guard_verdicts=tuple(acc.verdicts))
        return row(_guard_code(guard, verdict), guard_verdicts=tuple(acc.verdicts), metrics=verdict.metrics,
                   caveats=_dedupe([*verdict.caveats, *held_caveat, *((PREVIOUS_POST_PENDING,)
                                                                     if inp.previous_post_pending else ())]))
    evaluation = _call_propose(strategy, inp, trigger_ctx)
    caveats = _dedupe([*ctx.caveats, *acc.caveats, *evaluation.caveats, *held_caveat,
                       *((PREVIOUS_POST_PENDING,) if inp.previous_post_pending else ())])
    common: dict[str, object] = dict(trigger_met=evaluation.trigger_met, evaluation=evaluation,
                                     metrics=(*acc.metrics, *evaluation.metrics), caveats=caveats,
                                     labels=tuple(acc.labels), guard_verdicts=tuple(acc.verdicts),
                                     stop_basis=tb.basis)
    if at_target:
        # Only while a basis is held (C1): the stop is at the target, but the bar end tests the trigger against the
        # pre-move stop, so PRE still previews that test instead of going silent (round 12, S38c).
        return row(OutcomeCode.engine(Outcome.AT_TARGET_UNCONFIRMED), at_target=True, **common)
    if evaluation.proposal is None:
        return row(_strategy_code(strategy, evaluation), **common)
    env = apply_envelope(trigger_ctx, evaluation, quote=quote, quote_live=ctx.exit_quote_live, market_open=True,
                         envelope=inp.envelope)
    if env is None:
        raise PipelineFault(strategy_scope_id(strategy.KIND, strategy.IMPL_VERSION))
    levels = None
    if env.code.outcome is Outcome.ADJUST:
        levels = PreBasis.of(ctx.side, ctx.entry, [(HandleRef(h.kind, h.handle_id), h.level) for h in ctx.handles],
                             ctx.bar_end_ms)
    return row(env.code, targets=env.targets, broker_ahead=env.broker_ahead, pre_levels=levels, **common)


def _bar_end(inp: PipelineInput, strategy: SlStrategy, registry: ArmRegistry) -> PipelineResult:
    if answers_final_advice(inp):
        # round 14: the move answers final advice; the PRE levels are not used anywhere below
        ctx = replace(inp.ctx, caveats=_dedupe([*inp.ctx.caveats, MOVED_ON_FINAL_ADVICE]))
        inp = replace(inp, this_bar_pre=None, ctx=ctx)
    ctx = inp.ctx
    side, entry = ctx.side, ctx.entry
    released = inp.arm_state is ArmState.STRATEGY_COMPLETE
    base = PipelineResult(event=ctx.event, kind=ResultKind.DECISION, envelope=stamp(inp.envelope), state=inp.state,
                          pre_basis=inp.pre_basis, close_source=CloseSource.BROKER)

    def fault_row(code: OutcomeCode, **kw: object) -> PipelineResult:
        """A FAULT: recorded; a DATA_UNAVAILABLE does not decide the bar; a released position's is never shown. A
        final FAULT (a bar-end `NO_STOP`) is a decided POST, so it carries its SM11 events (ARMED -> TRACKING,
        outstanding advice withdrawn) unless the caller passes `sm11_events`."""
        final = code.outcome is not Outcome.DATA_UNAVAILABLE
        events = _sm11(inp.arm_state, code, None, rearmed=False, latch=_latch(strategy, inp)) if final else ()
        out = replace(base, code=code, final=final, shown=not released, sm11_events=events)
        return replace(out, **kw)   # type: ignore[arg-type]

    fault = _pre_guard_fault(inp, STOP_READ_BOUND_MS)          # (1)-(3b)
    if fault is not None:
        return fault_row(fault)
    rearmed = False
    if released:                                                # (4)
        if _call_at_target(strategy, inp, ctx):
            return replace(base, code=OutcomeCode.engine(Outcome.COMPLETE), final=True, shown=False, at_target=True)
        try:
            policy = strategy.loosen_policy(inp.params)
        except Exception:
            raise PipelineFault(strategy_scope_id(strategy.KIND, strategy.IMPL_VERSION)) from None
        if policy is LoosenPolicy.TELL_ONLY:
            return replace(base, code=OutcomeCode.engine(Outcome.LOOSENED_AFTER_COMPLETE), final=True,
                           shown=not inp.loosened_told, at_target=False)
        rearmed = True
        released = False
    bar = ctx.bar                                               # (5)
    if (bar is None or bar.final_at_ms is None) and inp.bar_no_trade:
        # an OPEN bucket the broker did not trade: decided (the cursor moves), no trigger, state and basis kept,
        # recorded only (S38c); a released or ARMED position keeps its SM11 state. A PRE proposed this bar is not
        # confirmed: its levels fold into the stored basis (§B6.4, one stop basis) and the item is shown (S38c review)
        rearm = (Sm11Event.REARM_ON_LOOSEN,) if rearmed else ()
        if inp.this_bar_pre is not None:
            return replace(base, code=OutcomeCode.engine(Outcome.NO_BROKER_BAR, NO_TRADE_PRE_NOT_CONFIRMED),
                           final=True, shown=True, sm11_events=rearm,
                           pre_basis=farther_basis(side, entry, inp.pre_basis, inp.this_bar_pre))
        return replace(base, code=OutcomeCode.engine(Outcome.NO_BROKER_BAR), final=True, shown=False,
                       sm11_events=rearm)
    if bar is None or bar.final_at_ms is None:
        if inp.now_ms < _cap(ctx):
            return replace(base, kind=ResultKind.PENDING, reason=AWAITING_BROKER_BAR,
                           sm11_events=(Sm11Event.REARM_ON_LOOSEN,) if rearmed else ())
        code = OutcomeCode.unresolved(BROKER_BAR_NOT_FINAL)
        return replace(base, code=code, final=True,
                       sm11_events=_sm11(inp.arm_state, code, None, rearmed=rearmed, latch=_latch(strategy, inp)))
    acc = _Guards()
    pre_proposed = inp.this_bar_pre is not None
    stopped = _run_guards(inp, strategy, registry, acc)        # (6)
    if stopped is not None:
        guard, verdict = stopped
        if verdict.status is GuardStatus.PENDING:
            return fault_row(OutcomeCode.engine(Outcome.DATA_UNAVAILABLE, pending_reason(guard.KIND)),
                             guard_verdicts=tuple(acc.verdicts),
                             sm11_events=(Sm11Event.REARM_ON_LOOSEN,) if rearmed else ())
        stored = inp.pre_basis
        code = _guard_code(guard, verdict, pre_not_confirmed=pre_proposed)
        if pre_proposed:
            stored = farther_basis(side, entry, stored, inp.this_bar_pre)
        return replace(base, code=code, final=True, guard_verdicts=tuple(acc.verdicts), metrics=verdict.metrics,
                       caveats=_dedupe([*ctx.caveats, *acc.caveats, *verdict.caveats]), pre_basis=stored,
                       sm11_events=_sm11(inp.arm_state, code, None, rearmed=rearmed, latch=_latch(strategy, inp)))
    s_at_t = inp.stop_at_t                                      # §B6.4
    if not isinstance(s_at_t, StopAtT):
        reason = s_at_t.reason if isinstance(s_at_t, StopUnavailable) else StopUnavailable().reason
        return fault_row(OutcomeCode.engine(Outcome.DATA_UNAVAILABLE, reason), guard_verdicts=tuple(acc.verdicts),
                         sm11_events=(Sm11Event.REARM_ON_LOOSEN,) if rearmed else ())
    tb = trigger_basis(side, entry, s_at_t.level, s_at_t.basis, inp.pre_basis, inp.this_bar_pre)   # the one basis
    stored, basis = tb.stored, tb.basis
    trigger_ctx = replace(ctx, stop_at_t=s_at_t.level, stop_basis=basis, pre_basis=tb.s_pre,
                          caveats=_dedupe([*ctx.caveats, *s_at_t.caveats]))
    evaluation = _call_propose(strategy, inp, trigger_ctx)    # (7)
    at_target = _call_at_target(strategy, inp, trigger_ctx)
    common: dict[str, object] = dict(
        trigger_met=evaluation.trigger_met, at_target=at_target, evaluation=evaluation, stop_basis=basis,
        metrics=(*acc.metrics, *evaluation.metrics), labels=tuple(acc.labels), guard_verdicts=tuple(acc.verdicts),
        caveats=_dedupe([*trigger_ctx.caveats, *acc.caveats, *evaluation.caveats]))
    after_basis = None if evaluation.trigger_met else stored   # a bar end confirmed the trigger: cleared
    shown = True
    if evaluation.trigger_met and at_target:                   # (7a) released, under the strategy's policy
        if strategy.RELEASE_POLICY is ReleasePolicy.ON_CONFIRMED_TARGET:
            code = OutcomeCode.engine(Outcome.COMPLETE)
        else:                                                   # NEVER (S38c): stays managed, TRACKING
            code = OutcomeCode.engine(Outcome.AT_TARGET_UNCONFIRMED)
            shown = not inp.prev_final_at_target
    elif at_target:
        if pre_proposed:
            code = OutcomeCode.pre_not_confirmed("", Outcome.AT_TARGET_UNCONFIRMED.value)
            after_basis = farther_basis(side, entry, after_basis, inp.this_bar_pre)
        else:
            code = OutcomeCode.engine(Outcome.AT_TARGET_UNCONFIRMED)
            shown = not inp.prev_final_at_target
    elif evaluation.proposal is None:
        code = _strategy_code(strategy, evaluation, pre_not_confirmed=pre_proposed)
        if pre_proposed:
            after_basis = farther_basis(side, entry, after_basis, inp.this_bar_pre)
    else:                                                       # (8)-(13)
        env = apply_envelope(trigger_ctx, evaluation, quote=inp.live_quote, quote_live=ctx.exit_quote_live,
                             market_open=calendar_open(inp.calendar, ctx.adapter_id, ctx.symbol),
                             envelope=inp.envelope)
        if env is None:
            raise PipelineFault(strategy_scope_id(strategy.KIND, strategy.IMPL_VERSION))
        code = env.code
        common.update(targets=env.targets, broker_ahead=env.broker_ahead)
    final = code.outcome is not Outcome.DATA_UNAVAILABLE
    return replace(base, code=code, final=final, shown=shown, pre_basis=after_basis if final else inp.pre_basis,
                   state=_commit(strategy, inp, evaluation, code) if final else inp.state,
                   sm11_events=_sm11(inp.arm_state, code, evaluation, rearmed=rearmed,
                                     latch=_latch(strategy, inp)) if final else
                   ((Sm11Event.REARM_ON_LOOSEN,) if rearmed else ()),
                   **common)   # type: ignore[arg-type]


__all__ = [
    "ENVELOPE_CONTEXT", "PRE_STOP_READ_BOUND_MS", "STOP_READ_BOUND_MS", "ACCOUNT_IMAGE_STALE",
    "PRE_QUOTE_UNAVAILABLE", "PRE_QUOTE_AFTER_FIRE", "AWAITING_BROKER_BAR", "PRE_EXPIRED", "FILL_IN_PRE_WINDOW",
    "MARKET_CLOSED_AT_PRE", "RELEASED", "AT_TARGET", "PREVIOUS_POST_PENDING", "PRE_BASIS_HELD",
    "MOVED_ON_FINAL_ADVICE", "answers_final_advice",
    "NO_TRADE_PRE_NOT_CONFIRMED", "ResultKind", "PipelineFault",
    "PreWindow", "pre_window", "PipelineInput", "PipelineResult", "EnvelopeOutcome", "round_toward_profit",
    "apply_envelope", "run_pipeline", "pending_reason",
]
