"""`arm replay --rerun`: re-run the real decision pipeline for a past bar (go-live gate `replay_rerun`, plan S39b;
design §B8 Replay, §A10 `as_of_ms`).

For one stored bar-end decision (`POST`, `LATE`, `REOPEN`, `CORRECTED`; never a PRE row or an `UNRESOLVED_RANGE`
span) the replay builds the BarClock's `EvalRequest` for the same position, event and bar at the decision's own time
(`now_ms = decided_at_ms`) and hands it to the real S21 evaluation (`PipelineEvaluator.prepare`, then its job through
the real `JobRunner` under the version's pinned Decimal context and the 250 ms cap). Its ports are pinned to the
decision's time:

* `AsOfMarketData`: every bar and indicator read is point-in-time (`as_of_ms = decided_at_ms`, §A10); the quote is
  never recorded, so it is not available (`quote_not_recorded`); `demand` / `release` / `refetch` refuse (read-only:
  the replay never leases or re-fetches a series);
* `AsOfDecisions`: the decision memory as it stood before the decision (`PastDecisionReader.decision_memory_before`);
* `AsOfCalendar`: the SM10 state the schedule gives at `decided_at_ms`;
* the order state at `T` is `S_at_T` from the handles' `event_log` versions (already a point-in-time read); the
  account image and the pin are today's;
* `AsOfImages`: the account image's freshness (pre-guard (1)) is not judged from today's stream fences: a fence open
  now says nothing of `T`, and the stored decision was made on a fresh image or it would not be final.

Nothing is written, no SM11 state changes, no notice is queued or sent: the evaluation's result is only compared.
`compare` reports, by the recomputed `inputs_digest` and the outcome:

| Result | When |
|---|---|
| `MATCH` | the re-run read the same inputs (equal `inputs_digest` under the same key id) and gave the same outcome |
| `DIFF(outcome)` | the same inputs and another outcome: the pipeline did not reproduce its own decision |
| `UNVERIFIABLE(reason)` | the re-run could not be compared: the order state moved since (`order_state_changed`: a handle's level as read now is not the level the decision read, from its `arm_decision_handles` rows), not ready (`not_ready:<code>`, e.g. the position is no longer open), a job fault (`job:<code>`), not final, no digest, a digest under another key id (`key_changed`), other inputs (`inputs_differ`: a bar revised since, an indicator recomputed), an outcome that needs the exit quote the decision saw (`exit_quote_not_recorded`), or a `MARKET_CLOSED` on one side only (`calendar_not_recorded`: the decision's SM10 state, an evidence overlay or an older schedule version, is not recorded; the re-run has only the latest schedule) |

Output: ids, bar ends, codes; never a level, a price or a hash value.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal
from enum import Enum
from typing import Final, Protocol

from src.calendar.machine import SessionState
from src.calendar.schedule import SymbolSchedule
from src.calendar.sessions import Phase, phase_at
from src.db.arm_ports import ArmPositionRow, DecisionMemory, PastDecisionReader, PositionKey
from src.marketdata.hashing import HashComparison, compare_hashes
from src.marketdata.indicators.base import IndicatorSpec
from src.marketdata.ports import (
    BarCallback,
    BarsRead,
    EventsRead,
    FormingRead,
    IndicatorCallback,
    IndicatorRead,
    MarketDataPort,
    ProvisionalRead,
    QuoteRead,
    SeriesStatus,
)
from src.marketdata.timing import SkewEstimate, decision_cap_ms
from src.marketdata.types import (
    BarSpec,
    CursorExpired,
    DataUnavailable,
    EventCursor,
    LeaseClass,
    MdGeneration,
    SeriesId,
    Side,
    Unsupported,
)
from src.riskmgr.barclock import EvalRequest, Evaluated, EvaluationPort, NotReady, Verdict
from src.riskmgr.jobs import JobRunner
from src.riskmgr.outcomes import EXIT_QUOTE_NOT_LIVE, Outcome
from src.riskmgr.tracker import AccountImage, TrackerSnapshotPort
from src.riskmgr.types import ArmEvent

QUOTE_NOT_RECORDED: Final = "quote_not_recorded"
READ_ONLY: Final = "replay_read_only"
RERUN_EVENTS: Final = frozenset({ArmEvent.POST, ArmEvent.LATE, ArmEvent.REOPEN, ArmEvent.CORRECTED})
UNRESOLVED_RANGE: Final = "UNRESOLVED_RANGE"
QUOTE_OUTCOMES: Final = frozenset({"ADJUST", "PRICE_THROUGH_LEVEL"})
"""Stored outcomes that were decided on the exit quote (§B6.3 (11)-(13)): the quote is never recorded, so a re-run
that stops at `DATA_UNAVAILABLE(exit_quote_not_live)` cannot be compared with them."""
CALENDAR_NOT_RECORDED: Final = "calendar_not_recorded"
"""A `MARKET_CLOSED` on one side only: the SM10 state at `T` (evidence overlay, schedule version) is not recorded."""


class RerunResult(str, Enum):
    MATCH = "MATCH"
    DIFF = "DIFF"
    UNVERIFIABLE = "UNVERIFIABLE"


@dataclass(frozen=True)
class RerunTarget:
    """What the re-run reads of one stored `arm_decisions` row (ids, codes, times and the keyed digest; no level)."""

    decision_id: int
    key: PositionKey
    bar_spec: str
    event: str
    bar_end_ms: int
    revision: int
    decision_seq: int
    decided_at_ms: int
    outcome: str
    reason: str | None
    inputs_digest: str | None
    handles: tuple[tuple[str, Decimal | None], ...] | None = field(default=None, repr=False)
    """The stored decision's handles as `(handle_id, level_read)` (order state; None: not known, not checked)."""


@dataclass(frozen=True)
class RerunVerdict:
    result: RerunResult
    reason: str | None = None
    outcome: str | None = None
    """The re-run's outcome code (None when it gave none)."""

    @property
    def text(self) -> str:
        return self.result.value + (f"({self.reason})" if self.reason else "")


def eligible(target: RerunTarget) -> str | None:
    """None when the stored row can be re-run, else why not (a PRE row, a span, an unknown event)."""
    try:
        event = ArmEvent(target.event)
    except ValueError:
        return "unknown_event"
    if event not in RERUN_EVENTS:
        return "not_a_bar_end_decision"
    if target.outcome == UNRESOLVED_RANGE:
        return "span"
    return None


def compare(target: RerunTarget, got: Evaluated | NotReady | str) -> RerunVerdict:
    """The verdict of one re-run (module docstring table)."""
    if isinstance(got, NotReady):
        return RerunVerdict(RerunResult.UNVERIFIABLE, f"not_ready:{got.reason}")
    if isinstance(got, str):
        return RerunVerdict(RerunResult.UNVERIFIABLE, f"job:{got}")
    dec = got.decision
    if got.verdict is not Verdict.DECISION or dec is None:
        return RerunVerdict(RerunResult.UNVERIFIABLE, "no_decision")
    if target.handles is not None and \
            sorted(target.handles) != sorted((h.handle_id, h.level_read) for h in got.handles):
        return RerunVerdict(RerunResult.UNVERIFIABLE, "order_state_changed", dec.outcome)
    if dec.reason == EXIT_QUOTE_NOT_LIVE and target.outcome in QUOTE_OUTCOMES:
        return RerunVerdict(RerunResult.UNVERIFIABLE, "exit_quote_not_recorded", dec.outcome)
    if not got.final:
        return RerunVerdict(RerunResult.UNVERIFIABLE, "not_final", dec.outcome)
    if target.inputs_digest is None or dec.inputs_digest is None:
        return RerunVerdict(RerunResult.UNVERIFIABLE, "no_digest", dec.outcome)
    same = compare_hashes(dec.inputs_digest, target.inputs_digest)
    if same is HashComparison.INCOMPARABLE:
        return RerunVerdict(RerunResult.UNVERIFIABLE, "key_changed", dec.outcome)
    if same is HashComparison.CHANGED:
        return RerunVerdict(RerunResult.UNVERIFIABLE, "inputs_differ", dec.outcome)
    closed = Outcome.MARKET_CLOSED.value
    if dec.outcome != target.outcome and closed in (dec.outcome, target.outcome):
        return RerunVerdict(RerunResult.UNVERIFIABLE, CALENDAR_NOT_RECORDED, dec.outcome)
    if dec.outcome != target.outcome or (dec.reason or None) != (target.reason or None):
        return RerunVerdict(RerunResult.DIFF, "outcome", dec.outcome)
    return RerunVerdict(RerunResult.MATCH, None, dec.outcome)


# ---------------------------------------------------------------------------------------------------- as-of ports
class AsOfMarketData:
    """A `MarketDataPort` pinned to `as_of_ms` and read-only (module docstring)."""

    def __init__(self, port: MarketDataPort, as_of_ms: int) -> None:
        self._port = port
        self._as_of = as_of_ms

    async def quote(self, adapter_id: str, symbol: str) -> QuoteRead | DataUnavailable:
        return DataUnavailable(QUOTE_NOT_RECORDED)

    async def quote_at(self, adapter_id: str, symbol: str, at_ms: int) -> QuoteRead | DataUnavailable:
        return DataUnavailable(QUOTE_NOT_RECORDED)

    async def forming_bar(self, series: SeriesId) -> FormingRead | DataUnavailable:
        return DataUnavailable(QUOTE_NOT_RECORDED)

    async def bars(self, series: SeriesId, *, at_bucket_ms: int | None = None, n: int | None = None,
                   from_bucket_ms: int | None = None, to_bucket_ms: int | None = None,
                   as_of_ms: int | None = None) -> BarsRead | DataUnavailable:
        return await self._port.bars(series, at_bucket_ms=at_bucket_ms, n=n, from_bucket_ms=from_bucket_ms,
                                     to_bucket_ms=to_bucket_ms, as_of_ms=self._as_of)

    async def best_bars(self, adapter_id: str, symbol: str, spec_name: str, side: Side,
                        n: int) -> BarsRead | DataUnavailable:
        return DataUnavailable(READ_ONLY)

    async def indicator(self, indicator_id: str, output: str, *, at_bucket_ms: int | None = None,
                        n: int | None = None, as_of_ms: int | None = None) -> IndicatorRead | DataUnavailable:
        return await self._port.indicator(indicator_id, output, at_bucket_ms=at_bucket_ms, n=n, as_of_ms=self._as_of)

    async def provisional(self, indicator_id: str, output: str) -> ProvisionalRead | DataUnavailable:
        return DataUnavailable(QUOTE_NOT_RECORDED)

    def define(self, spec: SeriesId | IndicatorSpec) -> str:
        return self._port.define(spec)

    async def demand(self, spec_id: str, consumer: str, lease_class: LeaseClass) -> bool:
        return False                                  # read-only: the replay never leases

    async def release(self, spec_id: str, consumer: str) -> None:
        return None

    def on_bar_closed(self, callback: BarCallback) -> None:
        return None

    def on_bar_revised(self, callback: BarCallback) -> None:
        return None

    def on_indicator_updated(self, callback: IndicatorCallback) -> None:
        return None

    async def events_since(self, series: SeriesId,
                           cursor: EventCursor) -> EventsRead | CursorExpired | DataUnavailable:
        return DataUnavailable(READ_ONLY)

    async def refetch(self, series: SeriesId, from_bucket_ms: int, to_bucket_ms: int) -> None | Unsupported:
        return Unsupported(READ_ONLY)                 # read-only: the replay never asks md-bars for a re-fetch

    async def md_generation(self) -> MdGeneration | DataUnavailable:
        return await self._port.md_generation()

    async def series_status(self, series: SeriesId) -> SeriesStatus | DataUnavailable:
        return await self._port.series_status(series)


def as_market_data_port(port: AsOfMarketData) -> MarketDataPort:
    """`AsOfMarketData` is a `MarketDataPort` (checked by mypy here)."""
    return port


class AsOfImages:
    """`TrackerSnapshotPort` for the offline re-run: today's account image with its freshness not judged from today's
    stream fences (module docstring). The positions, orders and instruments are today's, as before; a moved order
    state is still caught by the handles check (`order_state_changed`)."""

    def __init__(self, snapshot: TrackerSnapshotPort) -> None:
        self._snapshot = snapshot

    async def read(self, accounts: Sequence[tuple[str, str]]) -> Sequence[AccountImage]:
        return [replace(i, fresh=True, reasons=()) for i in await self._snapshot.read(accounts)]


class AsOfDecisions:
    """`ArmDecisionReader` answering with the memory as it stood before decision `decision_seq`."""

    def __init__(self, reader: PastDecisionReader, decision_seq: int) -> None:
        self._reader = reader
        self._seq = decision_seq

    async def decision_memory(self, key: PositionKey, bar_spec: str, bar_end_ms: int) -> DecisionMemory:
        return await self._reader.decision_memory_before(key, bar_spec, bar_end_ms, self._seq)


class AsOfCalendar:
    """`CalendarPort`: the SM10 state the schedule gives at `as_of_ms` (OPEN, ROLLOVER or CLOSED; no evidence or
    maintenance overlay is known for a past instant). A symbol without a schedule is None (closed)."""

    def __init__(self, schedule_of: Callable[[str, str], SymbolSchedule | None], as_of_ms: int) -> None:
        self._schedule_of = schedule_of
        self._as_of = as_of_ms

    def state(self, adapter_id: str, symbol: str) -> SessionState | None:
        try:
            schedule = self._schedule_of(adapter_id, symbol)
        except Exception:
            return None
        if schedule is None:
            return None
        phase = phase_at(schedule, self._as_of)
        return {Phase.OPEN: SessionState.OPEN, Phase.ROLLOVER: SessionState.ROLLOVER}.get(phase, SessionState.CLOSED)


# ---------------------------------------------------------------------------------------------------- the re-run
class EvaluatorFactory(Protocol):
    def __call__(self, target: RerunTarget) -> EvaluationPort:
        """A fresh evaluation port pinned to the target's time (`AsOfMarketData`, `AsOfDecisions`,
        `AsOfCalendar`)."""
        ...


@dataclass(frozen=True)
class RerunInputs:
    """What one re-run needs besides the evaluator: the position's row, its bar spec and the adapter's skew."""

    row: ArmPositionRow
    spec: BarSpec
    skew: SkewEstimate
    generation: MdGeneration


def rerun_request(target: RerunTarget, inputs: RerunInputs) -> EvalRequest:
    """The BarClock's request for the stored decision's event and bar, at the decision's own time."""
    return EvalRequest(
        row=inputs.row, spec=inputs.spec, event=ArmEvent(target.event), bar_end_ms=target.bar_end_ms,
        now_ms=target.decided_at_ms, skew=inputs.skew,
        decision_cap_ms=decision_cap_ms(target.bar_end_ms, inputs.spec, inputs.skew), generation=inputs.generation)


async def rerun_one(evaluator: EvaluationPort, jobs: JobRunner, target: RerunTarget,
                    inputs: RerunInputs) -> RerunVerdict:
    """Prepare and run one past bar through the real evaluation and job runner, then `compare` (nothing written)."""
    why = eligible(target)
    if why is not None:
        return RerunVerdict(RerunResult.UNVERIFIABLE, why)
    prepared = await evaluator.prepare(rerun_request(target, inputs))
    if isinstance(prepared, NotReady):
        return compare(target, prepared)
    result = await jobs.run(prepared.spec, f"replay:{target.decision_id}", prepared.job)
    if not result.ok or result.value is None:
        return compare(target, result.reason or "no_value")
    return compare(target, result.value)


def summary(verdicts: Mapping[int, RerunVerdict]) -> dict[str, int]:
    """Counts per verdict text (codes only)."""
    out: dict[str, int] = {}
    for v in verdicts.values():
        out[v.text] = out.get(v.text, 0) + 1
    return dict(sorted(out.items()))


__all__ = [
    "AsOfCalendar", "AsOfDecisions", "AsOfImages", "AsOfMarketData", "CALENDAR_NOT_RECORDED", "EvaluatorFactory",
    "QUOTE_NOT_RECORDED", "RERUN_EVENTS", "RerunInputs", "RerunResult", "RerunTarget", "RerunVerdict", "as_market_data_port", "compare", "eligible",
    "rerun_one", "rerun_request", "summary",
]
