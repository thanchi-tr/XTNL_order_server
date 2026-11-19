"""The S21 join: the BarClock's `EvaluationPort` over the envelope pipeline (design §B4, §B5.2, §B6.1-§B6.4, §B8;
plan S36b, RT-3).

`PipelineEvaluator.prepare(request)` does every read of one evaluation before any transaction (the BarClock's rule,
§B5.3) and returns the pure job, which runs `src.riskmgr.pipeline.run_pipeline` on the pinned version's executor
(S22 `JobRunner`) and maps its `PipelineResult` to the BarClock's `Evaluated`:

* **Pin and state:** the stored strategy params and guard list re-parsed (`new_pin=False`), the stored strategy
  state, the SM11 state. A pin this node cannot load, or an unreadable state, is `NotReady` (the arm-tracker
  suspends a pin it cannot load; the bar is spanned at its cap meanwhile).
* **Order state:** the account image of the position (`TrackerSnapshotPort`, one snapshot shared by the prepares
  of one pass, `image_ttl_ms`): freshness (pre-guard (1)), the current handles (PR1 `effective_stop_detail`, with
  their PR9 read time when `XTNL_ARM_STOP_READS=1`), the instrument's price increment and minimum stop distance.
  At bar-end events `S_at_T` from the handles' `event_log` versions (`ArmVersionReader.stop_versions` over the
  current children plus every child ever linked to the position that may have been live at `T`, round 13: a
  cancel/replace after `T` decides on the pre-replace level; `src.riskmgr.stopbasis.stop_at_bar_end`).
* **Market data** (`MarketDataPort`): the broker's final bar `T` on the trigger side (bar-end events; a bar not
  final yet is passed as None, so the pipeline answers "awaiting broker bar" until `decision_cap`, unless `md-bars`
  resolved the bucket `no_trade`: then `NO_BROKER_BAR`, S38c), the exit quote
  (its liveness for post-guard (11); at PRE the quote **as it was at fire** (`MarketDataPort.quote_at`, the hook's
  quote ring, round 13), so ticks after fire never turn PRE into `pre_quote_after_fire`), the `required_history` bars (`src.riskmgr.history.read_histories`) and the
  `required_indicators` values of the last two closed buckets.
* **Stop basis memory** (read from PostgreSQL, so a restart or a leader change between PRE and POST decides the
  same): the stored `pre_basis` (`arm_positions.pre_basis_json`, this module's `encode_pre_basis`); at bar-end events
  this bar's PRE levels (`this_bar_pre`), rebuilt from the committed PRE `ADJUST` row's handle rows (`handle_role`,
  `level_at_pre`), the target of the newest final `ADJUST` of an earlier bar (`final_advice_target`, round 14: a
  move that answers it is never discounted by AT_PRE, `pipeline.answers_final_advice`), and whether the previous
  final decision was at target, from the newest committed bar-end decision
  (`ArmDecisionReader.decision_memory`; unreadable is `NotReady(decisions_unreadable)`: no decision on a guessed
  basis). Only "`LOOSENED_AFTER_COMPLETE` already shown this episode" is kept in memory per term, and it is changed
  on the event loop once that decision has committed (`Evaluated.committed`), never by the job thread (a restart
  may show it once more).

The decision row holds ids, codes, order-state levels (the proposed stop, handle levels) and R values from order
state only (`current_risk_r = σ·(E − S) / R`, the first-bar ratio): never a bar, quote or indicator value. Its
inputs (round 13, `decision_inputs`) are the trigger bar `series|T|key_id‖hash` first (the keyed content hash under
the market-data keyring, from which the notice relay re-reads the bar at send time), every history bar the strategy
and its guards read, and every indicator value read as `I|id|output|bar_end|window_start|key_id‖hash` (a keyed hash
of the value's lineage, never the value); `inputs_digest` is the HMAC over all of them. Nothing here logs a
level, a price or a units figure.

Confirmation defaults (plan §9.2, design §0; still to be confirmed by the user, each switchable later): C1 `pre_basis`
kept until a bar end confirms the trigger (the S21 rule); C3 a REOPEN item at the open (the S22 planner); C4 the S07
stop rule (`effective_stop_detail`); C2 (ticks recorded while globally FAILED) is the market-data layer's.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import Context, Decimal, InvalidOperation
from typing import Final

from src.db.arm_ports import (
    ArmDecisionReader,
    ArmPositionRow,
    ArmVersionReader,
    DecisionMemory,
    DecisionHandleRow,
    DecisionRow,
    HandleVersionRow,
    is_lost_fence,
)
from src.dto import PositionDTO, Side
from src.marketdata.hashing import Keyring, content_hash
from src.marketdata.indicators.base import IndicatorValue
from src.marketdata.indicators.registry import REGISTRY as INDICATOR_REGISTRY
from src.marketdata.indicators.registry import IndicatorRegistry
from src.marketdata.ports import BarsRead, CalendarPort, HandleRef, HandleStatus, IndicatorRead, MarketDataPort, QuoteRead
from src.marketdata.types import Bar, BarSpec, DataUnavailable, Liveness, Quote, SeriesId, Source
from src.marketdata.types import Side as QuoteSide
from src.oversee.risk import StopDetail, effective_stop_detail
from src.riskmgr.barclock import EvalRequest, Evaluated, NotReady, Prepared, Verdict
from src.riskmgr.config import ArmSettings
from src.riskmgr.guard import guard_scope_id, strategy_scope_id
from src.riskmgr.history import read_histories
from src.riskmgr.jobs import JobSpec
from src.riskmgr.outcomes import BROKER_BAR_NOT_FINAL, FAILED, RELEASE_UNVERIFIED, Outcome
from src.riskmgr.pipeline import (
    AWAITING_BROKER_BAR,
    PipelineInput,
    PipelineResult,
    PreWindow,
    ResultKind,
    pre_window,
    run_pipeline,
)
from src.riskmgr.registry import REGISTRY, ArmRegistry
from src.riskmgr.render import FIRST_BAR_CONDITIONAL
from src.riskmgr.sm11 import ArmState
from src.riskmgr.stopbasis import (
    CHILD_ROLE_PREFIX,
    POSITION_FIELD_ROLE,
    PreBasis,
    StopAtT,
    StopUnavailable,
    StopVersion,
    stop_at_bar_end,
)
from src.riskmgr.strategy import HistoryNeed, IndicatorNeed, SlStrategy, StrategyParams, StrategyState
from src.riskmgr.tracker import (
    FIRST_BAR_SCAN_BUCKETS,
    FIRST_BAR_SCAN_MS,
    AccountImage,
    BarGridPort,
    TrackerSnapshotPort,
    first_bar_end,
    guards_apply,
)
from src.riskmgr.types import (
    BAR_END_EVENTS,
    ArmEvent,
    BarContext,
    EntryTimeSource,
    HandleView,
    IndicatorInput,
    RMultiple,
    Ratio,
    S0Source,
    SeriesInput,
    inputs_digest,
    lineage_hash,
    sigma,
)

log = logging.getLogger(__name__)

PIN_UNREADABLE: Final = "pin_unreadable"
STATE_UNREADABLE: Final = "state_unreadable"
IMAGE_MISSING: Final = "account_image_missing"
POSITION_NOT_OPEN: Final = "position_not_open"
DECISIONS_UNREADABLE: Final = "decisions_unreadable"
"""`NotReady` reasons (codes): what a bar spanned at its cap records."""
PENDING_REASON: Final = "pending"
FIRST_BAR_RATIO_METRIC: Final = "first_bar_ratio"
BORDERLINE_CAVEAT: Final = "borderline"
_LEVEL_CONTEXT: Final = Context(prec=40)
"""Order-state arithmetic (the proposed stop, `current_risk_r`) under a fixed context, never the ambient one."""


# ---------------------------------------------------------------------------------------------------- pre_basis
def encode_pre_basis(basis: PreBasis | None) -> str | None:
    """`arm_positions.pre_basis_json`: the levels by role (order state) and the bar they were set on."""
    if basis is None:
        return None
    return json.dumps({"bar": basis.bar_end_ms, "levels": [[r, str(lv)] for r, lv in basis.levels]},
                      sort_keys=True, separators=(",", ":"))


def decode_pre_basis(text: str | None) -> PreBasis | None:
    """The stored basis, or None when there is none or it does not decode (never raises: a basis that cannot be
    read is not used, so the trigger tests at `S_at_T` alone)."""
    if not text:
        return None
    try:
        raw = json.loads(text)
        if not isinstance(raw, dict):
            return None
        bar = raw.get("bar")
        levels = raw.get("levels")
        if isinstance(bar, bool) or not isinstance(bar, int) or not isinstance(levels, list):
            return None
        pairs: list[tuple[str, Decimal]] = []
        for item in levels:
            if not isinstance(item, list) or len(item) != 2 or not all(isinstance(x, str) for x in item):
                return None
            pairs.append((item[0], Decimal(item[1])))
        return PreBasis(tuple(pairs), bar)
    except (ValueError, TypeError, InvalidOperation):
        return None


def pre_levels_of(memory: DecisionMemory, bar_end_ms: int) -> PreBasis | None:
    """This bar's committed PRE `ADJUST` levels as `this_bar_pre` (roles in `PreBasis.of` order: the
    `POSITION_FIELD`, then the children by rank), None when no PRE proposed a move this bar. Raises `ValueError` for
    rows a basis cannot hold (the caller does not decide on them)."""
    if not memory.pre_levels:
        return None

    def order(level: tuple[str, Decimal]) -> tuple[int, int]:
        role = level[0]
        if role == POSITION_FIELD_ROLE:
            return (0, 0)
        rank = role[len(CHILD_ROLE_PREFIX):] if role.startswith(CHILD_ROLE_PREFIX) else ""
        return (1, int(rank)) if rank.isdigit() else (2, 0)

    return PreBasis(tuple(sorted(memory.pre_levels, key=order)), bar_end_ms)


def final_at_target(memory: DecisionMemory) -> bool:
    """The newest committed bar-end decision was at target: `AT_TARGET_UNCONFIRMED`, `COMPLETE`, or
    `PRE_NOT_CONFIRMED` carrying the engine's `AT_TARGET_UNCONFIRMED` (a span, a fault or any other outcome is not).
    `NO_BROKER_BAR` rows are skipped by the read (`ArmDecisionReader`), so a no-trade bucket never breaks the run."""
    outcome = memory.last_outcome
    if outcome in (Outcome.AT_TARGET_UNCONFIRMED.value, Outcome.COMPLETE.value):
        return True
    return (outcome == Outcome.PRE_NOT_CONFIRMED.value and memory.last_reason == Outcome.AT_TARGET_UNCONFIRMED.value
            and not memory.last_reason_kind)


def final_advice_target_of(event: ArmEvent, memory: DecisionMemory) -> Decimal | None:
    """The final advice a stop move at this bar end may answer (round 14, plan S38g; review): for a POST or LATE (the
    first final decision of the bar: the row's SM11 state is that of the earlier bars) the newest final ADJUST of an
    earlier bar (`final_advice_target`; the pipeline still requires ADVICE_OUTSTANDING); for a REOPEN or CORRECTED
    re-run (the row's state already includes this bar's own or a later decision) only a final ADJUST that is the
    newest final decision of an earlier bar (`prior_final_adjust`: withdrawn advice never counts)."""
    if event in (ArmEvent.REOPEN, ArmEvent.CORRECTED):
        return memory.prior_final_adjust
    return memory.final_advice_target


# ---------------------------------------------------------------------------------------------------- the pin
def pinned_params(registry: ArmRegistry, row: ArmPositionRow) -> StrategyParams | None:
    """The stored pin re-parsed (`new_pin=False`), or None when this node cannot load it."""
    try:
        raw = json.loads(row.strategy_params_json)
        guards = json.loads(row.guards_json)
        if not isinstance(raw, dict):
            return None
        return registry.parse_params(row.strategy_kind, row.strategy_impl_version, {**raw, "guards": guards},
                                     new_pin=False)
    except (ValueError, TypeError):
        return None


@dataclass(frozen=True)
class PinNeeds:
    """A pin's market-data needs (templates; resolved per position, never stored)."""

    trigger_side: QuoteSide
    history: tuple[HistoryNeed, ...]
    indicators: tuple[IndicatorNeed, ...]
    guard_history: tuple[HistoryNeed, ...] = ()
    """Of `history`, the needs only the pinned guards declare (never the strategy's own): read and recorded only
    while a guard applies (round 13: FirstBarGuard at bar 0)."""
    guard_indicators: tuple[IndicatorNeed, ...] = ()

    def without_guards(self) -> PinNeeds:
        """The needs a decision reads when no pinned guard applies to its bar."""
        return PinNeeds(self.trigger_side, tuple(h for h in self.history if h not in self.guard_history),
                        tuple(n for n in self.indicators if n not in self.guard_indicators))


def needs_of(registry: ArmRegistry, strategy: SlStrategy, params: StrategyParams, side: Side) -> PinNeeds | None:
    """The strategy's and its guards' history and indicator needs and the trigger side; None when a pinned guard
    is not loadable or a method raises."""
    try:
        history = list(strategy.required_history(params))
        indicators = list(strategy.required_indicators(params))
        own_history, own_indicators = set(history), set(indicators)
        g_history: list[HistoryNeed] = []
        g_indicators: list[IndicatorNeed] = []
        for g in params.guards:
            guard = registry.guard(g.kind, g.version)
            if guard is None:
                return None
            g_history += guard.required_history(g)
            g_indicators += guard.required_indicators(g)
        history += g_history
        indicators += g_indicators
        trig = strategy.trigger_side(params, side)
    except Exception:
        return None
    return PinNeeds(trig, tuple(sorted(set(history))), tuple(sorted(set(indicators))),
                    guard_history=tuple(sorted(set(g_history) - own_history)),
                    guard_indicators=tuple(sorted(set(g_indicators) - own_indicators)))


# ---------------------------------------------------------------------------------------------------- helpers
def stop_versions_of(rows: Sequence[HandleVersionRow]) -> list[StopVersion]:
    """`event_log` version rows as `stopbasis` versions (all `APPLIED`; a real broker time is a BROKER one, PR8)."""
    out: list[StopVersion] = []
    for r in rows:
        if r.handle_kind not in ("POSITION_FIELD", "CHILD_ORDER"):
            continue
        out.append(StopVersion(handle=HandleRef(r.handle_kind, r.handle_id), level=r.level,  # type: ignore[arg-type]
                               live=r.live, units=r.units, order_kind=r.order_kind, origin=r.origin,
                               broker_time_source="BROKER" if r.broker_time_ms is not None else None,
                               broker_time_ms=r.broker_time_ms, seq=r.seq_id))
    return out


def handle_role_map(side: Side, entry: Decimal, handles: Sequence[tuple[HandleRef, Decimal]]) -> dict[HandleRef, str]:
    """`POSITION_FIELD`, or `CHILD_ORDER:<rank>` ranked by distance from entry (rank 0 the farthest), per handle:
    the `stopbasis.handle_roles` rule, keeping each handle's id."""
    c = _LEVEL_CONTEXT
    sig = Decimal(sigma(side))
    out: dict[HandleRef, str] = {h: POSITION_FIELD_ROLE for h, _ in handles if h.kind == "POSITION_FIELD"}
    children = sorted(((h, lv) for h, lv in handles if h.kind == "CHILD_ORDER"),
                      key=lambda hl: c.multiply(sig, c.subtract(entry, hl[1])), reverse=True)
    for n, (h, _) in enumerate(children):
        out[h] = f"{CHILD_ROLE_PREFIX}{n}"
    return out


def proposed_level(side: Side, targets: Sequence[tuple[HandleRef, Decimal]]) -> Decimal | None:
    """The effective stop after an `ADJUST`: the `POSITION_FIELD` target, else the child target farthest from entry
    (the §B6.1 definition of `S`)."""
    if not targets:
        return None
    for h, t in targets:
        if h.kind == "POSITION_FIELD":
            return t
    levels = [t for _, t in targets]
    return min(levels) if side == "BUY" else max(levels)


def risk_multiple(side: Side, entry: Decimal, stop: Decimal | None, r: Decimal) -> Decimal | None:
    """`current_risk_r = σ·(E − S) / R` (order state only, 0.01 R), None without a stop or a positive R."""
    if stop is None or r <= 0:
        return None
    c = _LEVEL_CONTEXT
    try:
        return RMultiple.of(c.divide(c.multiply(Decimal(sigma(side)), c.subtract(entry, stop)), r)).value
    except (InvalidOperation, ValueError, ArithmeticError):
        return None


@dataclass(frozen=True, kw_only=True)
class EvalSettings:
    image_ttl_ms: int = 1_000
    """The account snapshot shared by the prepares of one pass (refreshed after this long, or on a miss)."""


@dataclass
class EvalCounters:
    prepares: int = 0
    not_ready: int = 0
    image_reads: int = 0
    image_errors: int = 0
    md_unavailable: int = 0
    decision_reads: int = 0
    decision_read_errors: int = 0
    jobs: int = 0


@dataclass(frozen=True)
class _Reads:
    """What `prepare` read for one evaluation (handed to the job; levels hidden from `repr`)."""

    inp: PipelineInput = field(repr=False)
    trigger: SeriesId | None
    trigger_hash: str | None
    detail: StopDetail | None = field(repr=False)
    indicator_tf: Mapping[str, int] = field(default_factory=dict)
    """Per indicator id read, its bar timeframe (the recorded input's bar end is its bucket start plus this)."""
    awaiting_verification: bool = False
    """The trigger bar's verification re-read has not confirmed it: a release decided on it is held (round 13)."""


# ---------------------------------------------------------------------------------------------------- the port
class PipelineEvaluator:
    """`EvaluationPort` of the BarClock (see the module docstring). One instance per term."""

    def __init__(self, *, md: MarketDataPort, snapshot: TrackerSnapshotPort, versions: ArmVersionReader,
                 decisions: ArmDecisionReader, grid: BarGridPort, calendar: CalendarPort, settings: ArmSettings,
                 keyring: Callable[[], Keyring | None], registry: ArmRegistry = REGISTRY,
                 indicators: IndicatorRegistry = INDICATOR_REGISTRY, evaluation: EvalSettings | None = None,
                 held: Callable[[ArmPositionRow], bool] | None = None,
                 no_trade: Callable[[SeriesId, int], bool] | None = None,
                 verified: Callable[[SeriesId, int], bool] | None = None) -> None:
        """`held(row)`: a FAILED scope covers the position (S30 `FailedScopes.covers_row`), so it is not evaluated
        (`NotReady(failed)`: no row; the bar is spanned `failed` at its cap); a raising predicate holds (fail
        closed). `decisions`: the committed PRE levels and the newest bar-end decision (the ARM state store).
        `no_trade(series, bucket_start_ms)`: `md-bars` resolved the bucket `no_trade` (its coverage map, S38c); the
        bar-end decision is then `NO_BROKER_BAR`. None or a raising predicate means "not known" (the decision waits
        for the broker's bar as before, fail safe). `verified(series, bucket_start_ms)`: `md-bars`' verification
        re-read positively confirmed the trigger bar (round 13 review); a bar-end `COMPLETE` on a bar not confirmed
        so is held (`release_awaiting_verification`) and, if never confirmed, spanned at its cap (no release). A
        raising predicate holds (fail closed); None means no verification gate is wired (unit rigs only: the
        runtime always wires `MdBars.verified`)."""
        self._md = md
        self._snapshot = snapshot
        self._versions = versions
        self._decisions = decisions
        self._grid = grid
        self._calendar = calendar
        self._arm = settings
        self._keyring = keyring
        self._registry = registry
        self._indicators = indicators
        self._cfg = evaluation or EvalSettings()
        self._held = held
        self._no_trade = no_trade
        self._verified = verified
        self.counters = EvalCounters()
        self._lock = asyncio.Lock()
        self._images: tuple[int, dict[tuple[str, str], AccountImage]] | None = None
        self._told: set[str] = set()
        """Positions whose `LOOSENED_AFTER_COMPLETE` was shown this episode (changed on the loop only, after the
        commit; dropped once the position leaves `STRATEGY_COMPLETE`)."""

    # ------------------------------------------------------------------------------------------------ the port
    async def prepare(self, request: EvalRequest) -> Prepared | NotReady:
        self.counters.prepares += 1
        row = request.row
        if self._is_held(row):
            return self._not_ready(FAILED)
        params = pinned_params(self._registry, row)
        strategy = None if params is None else self._registry.strategy(params.kind, params.impl_version)
        needs = None if params is None or strategy is None else needs_of(self._registry, strategy, params, row.side)
        if params is None or strategy is None or needs is None:
            return self._not_ready(PIN_UNREADABLE)
        state = StrategyState.decode(row.strategy_state_json)
        try:
            arm_state = ArmState(row.state)
            entry_source = EntryTimeSource(row.entry_time_source)
            s0_source = S0Source(row.s0_source)
        except ValueError:
            return self._not_ready(STATE_UNREADABLE)
        if state is None:
            return self._not_ready(STATE_UNREADABLE)
        image = await self._image(row.key.adapter_id, row.account_id, request.now_ms)
        if image is None:
            return self._not_ready(IMAGE_MISSING)
        position = _find(image.positions, row.key.position_id)
        if position is None or position.status != "OPEN":
            return self._not_ready(POSITION_NOT_OPEN)
        this_bar_pre: PreBasis | None = None
        prev_at_target = False
        advice_target: Decimal | None = None
        if request.event in BAR_END_EVENTS:
            memory = await self._memory(row, request.bar_end_ms)
            if memory is None:
                return self._not_ready(DECISIONS_UNREADABLE)
            try:
                this_bar_pre = pre_levels_of(memory, request.bar_end_ms)
            except ValueError:
                return self._not_ready(DECISIONS_UNREADABLE)
            # a CORRECTED re-run's newest committed decision is the one it revises: never "previous at target"
            prev_at_target = final_at_target(memory) if request.event is not ArmEvent.CORRECTED else False
            advice_target = final_advice_target_of(request.event, memory)
        reads = await self._read(request, row, image, position, params, needs, state, arm_state, entry_source,
                                 s0_source, this_bar_pre=this_bar_pre, prev_at_target=prev_at_target,
                                 advice_target=advice_target)
        spec = JobSpec(strategy_scope_id(params.kind, params.impl_version),
                       tuple(guard_scope_id(g.kind, g.version) for g in params.guards),
                       decimal_context=strategy.DECIMAL_CONTEXT)
        registry = self._registry
        keyring = self._keyring()
        remember = self._remember_told
        self.counters.jobs += 1

        def job() -> Evaluated:                      # pure: it changes no memory of this evaluator
            return evaluated(request, reads, run_pipeline(reads.inp, registry), keyring=keyring,
                             remember_told=remember)

        return Prepared(spec, job)

    async def _memory(self, row: ArmPositionRow, bar_end_ms: int) -> DecisionMemory | None:
        """This bar's committed PRE levels and the newest committed bar-end decision; None when unreadable."""
        self.counters.decision_reads += 1
        try:
            return await self._decisions.decision_memory(row.key, row.bar_spec, bar_end_ms)
        except Exception as e:
            if is_lost_fence(e):
                raise
            self.counters.decision_read_errors += 1
            log.info("arm evaluation: committed decisions not read (%r)", type(e))
            return None

    def _is_held(self, row: ArmPositionRow) -> bool:
        if self._held is None:
            return False
        try:
            return bool(self._held(row))
        except Exception:
            return True

    def _awaits_verification(self, series: SeriesId, bucket_ms: int) -> bool:
        """`md-bars` has not positively confirmed this final bar by its verification re-read (round 13 review): not
        re-read yet, inconclusive, no time left, final from the lane, or untracked. A raising port holds (fail
        closed: a release waits rather than stand on a bar that may still be revised)."""
        if self._verified is None:
            return False
        try:
            return not bool(self._verified(series, bucket_ms))
        except Exception:
            return True

    def _is_no_trade(self, series: SeriesId, bucket_ms: int) -> bool:
        if self._no_trade is None:
            return False
        try:
            return bool(self._no_trade(series, bucket_ms))
        except Exception:
            return False

    def _not_ready(self, reason: str) -> NotReady:
        self.counters.not_ready += 1
        return NotReady(reason)

    async def _image(self, adapter_id: str, account_id: str, now_ms: int) -> AccountImage | None:
        """The account's image from the snapshot shared by this pass's prepares (re-read when older than
        `image_ttl_ms` or when it lacks the account)."""
        key = (adapter_id, account_id)
        async with self._lock:
            cached = self._images
            if cached is None or now_ms - cached[0] >= self._cfg.image_ttl_ms or key not in cached[1]:
                self.counters.image_reads += 1
                try:
                    images = await self._snapshot.read([key])
                except Exception as e:
                    if is_lost_fence(e):
                        raise
                    self.counters.image_errors += 1
                    log.info("arm evaluation: account image not read (%r)", type(e))
                    return None
                self._images = (now_ms, {(i.adapter_id, i.account_id): i for i in images})
            return self._images[1].get(key) if self._images is not None else None

    # ------------------------------------------------------------------------------------------------ reads
    async def _read(self, request: EvalRequest, row: ArmPositionRow, image: AccountImage, position: PositionDTO,
                    params: StrategyParams, needs: PinNeeds, state: StrategyState, arm_state: ArmState,
                    entry_source: EntryTimeSource, s0_source: S0Source, *, this_bar_pre: PreBasis | None,
                    prev_at_target: bool, advice_target: Decimal | None = None) -> _Reads:
        adapter_id, symbol, spec = row.key.adapter_id, row.symbol, request.spec
        bar_end = request.bar_end_ms
        tf = spec.timeframe_ms
        bar_end_event = request.event in BAR_END_EVENTS
        instrument = image.instrument(symbol)
        increment = None if instrument is None else instrument.price_increment
        min_stop = None if instrument is None else instrument.min_stop_distance
        detail = effective_stop_detail(position, image.positions, image.orders, at_entry=True)
        read_at = self._read_times(request)
        handles = () if detail is None else tuple(
            HandleView(kind=h.kind, handle_id=h.id, level=h.level, read_ms=read_at.get(HandleRef(h.kind, h.id)),
                       order_kind=h.order_kind) for h in detail.handles)
        if request.event is ArmEvent.PRE and request.fire_ms is not None:
            # round 13: the PRE quote is frozen at fire (the hook's quote ring), however many ticks came since
            quote, live = await self._quote_at(adapter_id, symbol, request.fire_ms)
        else:
            quote, live = await self._quote(adapter_id, symbol)
        trigger: SeriesId | None = None
        bar: Bar | None = None
        no_trade = False
        awaiting = False
        if bar_end_event:
            trigger = SeriesId(adapter_id, symbol, spec, needs.trigger_side, Source.BROKER)
            bar = await self._final_bar(trigger, bar_end - tf)
            no_trade = bar is None and self._is_no_trade(trigger, bar_end - tf)
            awaiting = bar is not None and self._awaits_verification(trigger, bar_end - tf)
        history_to = bar_end if bar_end_event else bar_end - tf
        ref = row.ref
        bar_index = self._bar_index(adapter_id, symbol, spec, row.entry_time_ms, bar_end)
        ctx = BarContext(
            position_id=row.key.position_id, adapter_id=adapter_id, symbol=symbol, side=row.side, bar_spec=spec,
            event=request.event, bar_end_ms=bar_end, bar_index=bar_index, entry=row.entry_level,
            entry_time_ms=row.entry_time_ms, entry_time_source=entry_source, decision_cap_ms=request.decision_cap_ms,
            s0=row.s0_level, s0_source=s0_source, handles=handles, stop=None if detail is None else detail.level,
            price_increment=increment, min_stop_distance=min_stop, bar=bar,
            quote=None if bar_end_event else quote, exit_quote_live=live, account_image_fresh=image.fresh,
            image_reasons=image.reasons)
        if not guards_apply(self._registry, params, ctx, state):
            # round 13: past bar 0 FirstBarGuard's bid history and ATR are neither read (their leases are released)
            # nor recorded, so a later decision never depends on them
            needs = needs.without_guards()
        history: tuple[tuple[SeriesId, tuple[Bar, ...]], ...] = ()
        if needs.history:
            got = await read_histories(self._md, needs.history, adapter_id, symbol, history_to,
                                       now_ms=request.now_ms)
            if isinstance(got, DataUnavailable):
                self.counters.md_unavailable += 1
            else:
                history = got
        indicator_tf: dict[str, int] = {}
        indicators = await self._indicator_values(needs.indicators, adapter_id, symbol, increment, history_to, tf,
                                                  indicator_tf)
        s_at_t: StopAtT | StopUnavailable | None = None
        if bar_end_event:
            s_at_t = await self._stop_at(row, position, detail, bar_end)
        ctx = replace(ctx, history=history, indicators=indicators)
        window: PreWindow | None = None
        if request.event is ArmEvent.PRE:
            if request.fire_ms is not None and request.valid_until_ms is not None:
                window = PreWindow(request.fire_ms, request.valid_until_ms, self._arm.pre_offset_s)
            else:
                window = pre_window(bar_end, spec, request.skew, self._arm.pre_offset_s)
        if arm_state is not ArmState.STRATEGY_COMPLETE:
            self._told.discard(ref)
        inp = PipelineInput(
            params=params, ctx=ctx, state=state, arm_state=arm_state, now_ms=request.now_ms,
            calendar=self._calendar, envelope=self._arm.envelope, stop_at_t=s_at_t,
            live_quote=quote if bar_end_event else None, window=window,
            this_bar_pre=this_bar_pre if bar_end_event else None,
            pre_basis=decode_pre_basis(row.pre_basis_json), prev_final_at_target=prev_at_target,
            loosened_told=ref in self._told, stop_reads=self._arm.stop_reads,
            previous_post_pending=request.previous_post_pending, bar_no_trade=no_trade,
            final_advice_target=advice_target if bar_end_event else None)
        trigger_hash = None
        if trigger is not None and bar is not None:
            trigger_hash = self._bar_hash(trigger, bar_end, bar)
        return _Reads(inp, trigger, trigger_hash, detail, indicator_tf, awaiting)

    def _read_times(self, request: EvalRequest) -> dict[HandleRef, int]:
        """PR9: each handle's send time in the newest completed list read (only with `XTNL_ARM_STOP_READS=1`)."""
        if not self._arm.stop_reads:
            return {}
        current = request.stop_reads.current
        if current is None or not current.ok:
            return {}
        return {h.handle: h.sent_ms for h in current.handles
                if h.status is HandleStatus.OK and h.sent_ms is not None}

    async def _quote(self, adapter_id: str, symbol: str) -> tuple[Quote | None, bool]:
        got = await self._md.quote(adapter_id, symbol)
        if not isinstance(got, QuoteRead):
            self.counters.md_unavailable += 1
            return None, False
        return got.quote, got.quote is not None and got.liveness is Liveness.LIVE

    async def _quote_at(self, adapter_id: str, symbol: str, at_ms: int) -> tuple[Quote | None, bool]:
        got = await self._md.quote_at(adapter_id, symbol, at_ms)
        if not isinstance(got, QuoteRead):
            self.counters.md_unavailable += 1
            return None, False
        return got.quote, got.quote is not None and got.liveness is Liveness.LIVE

    async def _final_bar(self, series: SeriesId, bucket_ms: int) -> Bar | None:
        """The broker's final bar of the bucket, or None (not stored, not final, or the series unreadable)."""
        got = await self._md.bars(series, at_bucket_ms=bucket_ms)
        if not isinstance(got, BarsRead):
            self.counters.md_unavailable += 1
            return None
        found = [b for b in got.bars if b.bucket_start_ms == bucket_ms and b.final_at_ms is not None]
        return found[-1] if found else None

    async def _indicator_values(self, needs: Sequence[IndicatorNeed], adapter_id: str, symbol: str,
                                increment: Decimal | None, upto_end_ms: int, tf: int,
                                tf_of: dict[str, int] | None = None,
                                ) -> tuple[tuple[tuple[str, str], tuple[IndicatorValue, ...]], ...]:
        """Each needed indicator's outputs at the last two closed buckets ending at or before `upto_end_ms` (the
        guard checks the bucket it needs; an unavailable read leaves the value out, so the guard waits)."""
        if increment is None:
            return ()
        out: dict[tuple[str, str], list[IndicatorValue]] = {}
        for need in needs:
            spec = need.resolve(adapter_id, symbol, increment)
            ind = self._indicators.get(spec.kind, spec.version)
            if ind is None:
                continue
            if tf_of is not None:
                tf_of[spec.indicator_id] = spec.series.spec.timeframe_ms
            for output in ind.OUTPUTS:
                key = (spec.indicator_id, output.name)
                for bucket in (upto_end_ms - 2 * tf, upto_end_ms - tf):
                    got = await self._md.indicator(spec.indicator_id, output.name, at_bucket_ms=bucket)
                    if not isinstance(got, IndicatorRead):
                        self.counters.md_unavailable += 1
                        continue
                    out.setdefault(key, []).extend(v for v in got.values if v.bucket_start_ms == bucket)
        return tuple((k, tuple(v)) for k, v in sorted(out.items()))

    async def _stop_at(self, row: ArmPositionRow, position: PositionDTO, detail: StopDetail | None,
                       bar_end_ms: int) -> StopAtT | StopUnavailable:
        """`S_at_T` over the versions of the position's `stop_loss` field and **every stop child ever linked to it**
        (round 13): the current children are passed, and the reader adds each linked child that may have been live
        at `T` (cancelled / replaced after `T`), so a cancel/replace after `T` never makes the bar undecidable."""
        children = sorted({h.id for h in detail.handles if h.kind == "CHILD_ORDER"}) if detail is not None else []
        rows = await self._versions.stop_versions(row.key.adapter_id, row.key.position_id, children, bar_end_ms)
        units = position.units if position.units is not None else row.pinned_units
        return stop_at_bar_end(stop_versions_of(rows), side=row.side, entry=row.entry_level, units_to_cover=units,
                               bar_end_ms=bar_end_ms)

    def _bar_index(self, adapter_id: str, symbol: str, spec: BarSpec, entry_ms: int, bar_end_ms: int) -> int | None:
        """0 when `T` is bar 0 (the first open bar end after the fill bucket starts), 1 for a later open bar end
        (FirstBarGuard asks only for bar 0), None when `T` is not an open bar end or bar 0 is not found within
        `FIRST_BAR_SCAN_MS`. Bounded: windows of `FIRST_BAR_SCAN_BUCKETS` from the fill, stopping at the first open
        bar end, never a walk from the fill to `T` (a position held for months costs the same)."""
        tf = spec.timeframe_ms
        try:
            start = spec.bucket_start_ms(entry_ms)
            if bar_end_ms <= start or bar_end_ms not in self._grid.open_bar_ends(adapter_id, symbol, spec,
                                                                                  bar_end_ms - tf, bar_end_ms):
                return None
        except Exception:
            return None
        first = first_bar_end(self._grid, adapter_id, symbol, spec, entry_ms, upto_ms=bar_end_ms)
        if first is None:
            return None
        return 0 if first == bar_end_ms else 1

    def _bar_hash(self, series: SeriesId, bar_end_ms: int, bar: Bar) -> str | None:
        keyring = self._keyring()
        if keyring is None:
            return None
        try:
            return content_hash(keyring, series.canonical, bar_end_ms, bar.open, bar.high, bar.low, bar.close)
        except (ValueError, ArithmeticError):
            return None

    def forget(self, live_refs: set[str]) -> None:
        """Drop the memory of positions no longer live (bounded memory on a 24/7 node). Loop thread only."""
        self._told.intersection_update(live_refs)

    def _remember_told(self, ref: str) -> None:
        """`LOOSENED_AFTER_COMPLETE` was shown and committed (the BarClock's commit hook, on the loop)."""
        self._told.add(ref)


# ---------------------------------------------------------------------------------------------------- inputs
@dataclass(frozen=True)
class RecordedInputs:
    """Every input a bar-end decision read (round 13, plan S38d): the trigger bar first, then each history bar the
    strategy and its guards declared (`required_history`), as series inputs with their keyed content hashes; each
    indicator value read (`required_indicators`) as an indicator input with its keyed lineage hash. Ids and hashes
    only, never a price or a value."""

    series: tuple[SeriesInput, ...]
    indicators: tuple[IndicatorInput, ...]


def decision_inputs(reads: _Reads, bar_end_ms: int, keyring: Keyring) -> RecordedInputs:
    """The `RecordedInputs` of one evaluation (the trigger needs its hash; a history bar that cannot be hashed is
    left out, as the guard could not have used it either)."""
    seen: set[tuple[str, int]] = set()
    series: list[SeriesInput] = []
    if reads.trigger is not None and reads.trigger_hash is not None:
        series.append(SeriesInput(reads.trigger.canonical, bar_end_ms, reads.trigger_hash))
        seen.add((reads.trigger.canonical, bar_end_ms))
    ctx = reads.inp.ctx
    for sid, bars in ctx.history:
        for bar in bars:
            key = (sid.canonical, bar.bucket_end_ms)
            if key in seen or bar.final_at_ms is None:
                continue
            try:
                h = content_hash(keyring, sid.canonical, bar.bucket_end_ms, bar.open, bar.high, bar.low, bar.close)
            except (ValueError, ArithmeticError):
                continue
            seen.add(key)
            series.append(SeriesInput(sid.canonical, bar.bucket_end_ms, h))
    indicators: list[IndicatorInput] = []
    for (iid, output), values in ctx.indicators:
        tf = reads.indicator_tf.get(iid)
        if tf is None:
            continue
        for v in values:
            if v.faulted:
                continue
            start = v.lineage.window_start_ms if v.lineage.window_start_ms is not None else v.bucket_start_ms
            end = v.bucket_start_ms + tf
            try:
                h = lineage_hash(keyring, iid, output, end, start, v.lineage.input_digest, v.lineage.value_revision)
                indicators.append(IndicatorInput(iid, output, end, start, h))
            except ValueError:
                continue
    return RecordedInputs(tuple(series), tuple(sorted(set(indicators))))


# ---------------------------------------------------------------------------------------------------- the job
def evaluated(request: EvalRequest, reads: _Reads, result: PipelineResult, *, keyring: Keyring | None,
              remember_told: Callable[[str], None]) -> Evaluated:
    """The pipeline's answer as the BarClock's `Evaluated` (runs in the job, on the version's executor). Pure: the
    memory it implies (a `LOOSENED_AFTER_COMPLETE` shown) is the `committed` hook, which the BarClock runs on the
    loop only once the row has committed."""
    inp = reads.inp
    row = request.row
    ref = row.ref
    if result.kind is ResultKind.PENDING:
        reason = BROKER_BAR_NOT_FINAL if result.reason == AWAITING_BROKER_BAR else (result.reason or PENDING_REASON)
        return Evaluated(verdict=Verdict.PENDING, reason=reason)
    if result.kind is ResultKind.NO_ITEM or result.code is None:
        return Evaluated(verdict=Verdict.NO_ITEM, reason=result.reason)
    code = result.code
    if (reads.awaiting_verification and request.event in BAR_END_EVENTS and result.final
            and code.outcome is Outcome.COMPLETE):
        # round 13 (D4): a release stands only on a bar whose verification re-read confirmed it; ADJUST goes at once
        return Evaluated(verdict=Verdict.PENDING, reason=RELEASE_UNVERIFIED)
    series_inputs: tuple[str, ...] = ()
    indicator_texts: tuple[str, ...] = ()
    digest = None
    if reads.trigger is not None and reads.trigger_hash is not None and keyring is not None:
        recorded = decision_inputs(reads, request.bar_end_ms, keyring)
        series_inputs = tuple(f"{i.series_id}|{i.bar_end_ms}|{i.content_hash}" for i in recorded.series)
        indicator_texts = tuple(i.text for i in recorded.indicators)
        digest = inputs_digest(keyring, recorded.series, recorded.indicators)
    caveats = list(result.caveats)
    if result.labels:
        caveats.append(FIRST_BAR_CONDITIONAL)
    ratio = next((v for n, v in result.metrics if n == FIRST_BAR_RATIO_METRIC and isinstance(v, Ratio)), None)
    ctx = inp.ctx
    adjust = code.outcome is Outcome.ADJUST
    decision = DecisionRow(
        key=row.key, bar_end_ms=request.bar_end_ms, bar_spec=request.spec.name, event=request.event.value,
        decision_seq=0, decided_at_ms=0, edge_code=row.edge_code, outcome=code.outcome.value,
        category=code.category.value, reason=code.reason or None, reason_kind=code.owner or None,
        trigger_met=result.trigger_met, borderline=BORDERLINE_CAVEAT in caveats, advice=False,
        proposed_stop=proposed_level(row.side, result.targets) if adjust else None,
        current_risk_r=risk_multiple(row.side, row.entry_level, ctx.stop, ctx.r),
        first_bar_ratio=None if ratio is None else ratio.value, caveats=tuple(dict.fromkeys(caveats)),
        close_source=None if result.close_source is None else result.close_source.value,
        stop_basis=None if result.stop_basis is None else result.stop_basis.value, series_inputs=series_inputs,
        indicator_inputs=indicator_texts, inputs_digest=digest, envelope_version=result.envelope.version,
        envelope_params_hash=result.envelope.params_hash, strategy_kind=row.strategy_kind,
        strategy_impl_version=row.strategy_impl_version, guards_hash=row.guards_hash)
    handles = handle_rows(request, inp, result)
    bar_end_event = request.event in BAR_END_EVENTS
    if not bar_end_event:                     # a PRE ADJUST's levels are its handle rows' `level_at_pre`
        return Evaluated(verdict=Verdict.DECISION, decision=decision, handles=handles, final=False,
                         shown=result.shown)
    final = result.final
    committed: Callable[[], None] | None = None
    if final and code.outcome is Outcome.LOOSENED_AFTER_COMPLETE and result.shown:
        committed = lambda: remember_told(ref)            # noqa: E731 (run on the loop after the commit)
    basis_changed = final and result.pre_basis != inp.pre_basis
    return Evaluated(
        verdict=Verdict.DECISION, decision=decision, handles=handles, final=final,
        trigger_hash=reads.trigger_hash if final else None, sm11_events=result.sm11_events,
        strategy_state_json=result.state.encode() if final else None, pre_basis_change=basis_changed,
        pre_basis_json=encode_pre_basis(result.pre_basis) if basis_changed else None,
        pre_basis_bar=(None if result.pre_basis is None else result.pre_basis.bar_end_ms) if basis_changed
        else None,
        reopen_candidate=(final and request.event is ArmEvent.POST and code.outcome is Outcome.MARKET_CLOSED
                          and result.trigger_met is True),
        indicator_tf=tuple(sorted(reads.indicator_tf.items())), shown=result.shown, committed=committed)


def handle_rows(request: EvalRequest, inp: PipelineInput, result: PipelineResult) -> tuple[DecisionHandleRow, ...]:
    """One row per current handle (§B10): its role, the level at bar end (`S_at_T`), the level read, its PR9
    read time, the level at PRE (PRE rows) and the target (an `ADJUST`). Order state only."""
    ctx = inp.ctx
    if not ctx.handles:
        return ()
    current = [(HandleRef(h.kind, h.handle_id), h.level) for h in ctx.handles]
    roles = handle_role_map(ctx.side, ctx.entry, current)
    at_t: Mapping[HandleRef, Decimal] = {}
    if isinstance(inp.stop_at_t, StopAtT):
        at_t = {h.handle: h.level for h in inp.stop_at_t.handles}
    targets = dict(result.targets) if result.code is not None and result.code.outcome is Outcome.ADJUST else {}
    pre = request.event is ArmEvent.PRE
    out: list[DecisionHandleRow] = []
    for h in ctx.handles:
        hr = HandleRef(h.kind, h.handle_id)
        out.append(DecisionHandleRow(
            handle_id=h.handle_id, handle_kind=h.kind, handle_role=roles.get(hr, POSITION_FIELD_ROLE),
            level_at_bar_end=at_t.get(hr), level_read=h.level, read_at_ms=h.read_ms,
            level_at_pre=h.level if pre else None, target_level=targets.get(hr)))
    return tuple(out)


def _find(positions: Sequence[PositionDTO], position_id: str) -> PositionDTO | None:
    for p in positions:
        if p.position_id == position_id:
            return p
    return None


__all__ = [
    "DECISIONS_UNREADABLE", "EvalCounters", "EvalSettings", "FIRST_BAR_SCAN_BUCKETS", "FIRST_BAR_SCAN_MS",
    "IMAGE_MISSING", "PIN_UNREADABLE", "POSITION_NOT_OPEN",
    "PinNeeds", "PipelineEvaluator", "STATE_UNREADABLE", "decode_pre_basis", "encode_pre_basis", "evaluated",
    "final_advice_target_of", "final_at_target", "handle_role_map", "handle_rows", "needs_of", "pinned_params", "pre_levels_of",
    "proposed_level", "risk_multiple", "stop_versions_of",
]
