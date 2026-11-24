"""The `arm-tracker` task (design §B4 arm-tracker, §B7, §B11, §B12, §B13; plan S26, R11-M18, R11-M20).

Every 2 s (stamp age 10 s, §B5.1) one read-only snapshot of the accounts that hold OPEN positions or live ARM rows
(`TrackerSnapshotPort`: `snapshot_transaction` on the engine pool, implemented in `src/runtime/md_ports.py`, S31).
For each account whose image is **fresh** (no fence, no integrity incident, catch-up finished):

* **Arming** (§B7) of every OPEN position without a live `arm_positions` row: entry known, an effective stop (PR1
  `effective_stop_detail`, children covering only part of the units do not count), an accepted document whose
  assignment applies (`src.armconfig.resolver.resolve` at arming time). No assignment is `UNASSIGNED`; an assignment
  that cannot be served is `UNSUPPORTED` (no broker bars for a needed side, asset class, the pin refused for a new pin,
  `arming_requirements` unmet, the demand ceiling, and with PR9 on the per-adapter armed-handle ceiling,
  `stop_read_budget`). Both are in memory only (no row: there is nothing to pin) and get a one-shot
  `POSITION_NOT_MANAGED` notice keyed by position and reason; `UNSUPPORTED` also pages once. An assigned position not
  armed 5 minutes after it was first seen fresh pages once.
* The pin: the strategy row is re-parsed with `new_pin=True` (a deprecated version or a bar spec with no timing row is
  refused: the S25 registry check, whose first arming caller this is), `edge_code = XTNL_ARM_EDGE`,
  `attribution_source = SINGLE_EDGE_DEFAULT` (§B11; a document enabling a second edge is refused and pages once),
  entry time and S0 with their sources (`src/riskmgr/arming.py`), the indicator and history needs (templates, never a
  resolved spec: the instrument's increment is never written to PostgreSQL), `init_state`, and the cursor as at
  arming (`src.riskmgr.replay.arming_frontier`): bars whose `decision_cap` passed become **one** `armed_late`
  `UNRESOLVED_RANGE` row, written in the same fenced transaction as the position row, so a position armed long after
  its fill decides at most the current bar. A position on a FAILED scope arms straight into `FAILED` (prior ARMED).
* **Changes** of armed positions (§B7): closed or gone (CLOSED; a position replaced by another of the same symbol
  and side ends with a notice), fill progress (a units increase the entry order's fills explain, its final fill
  included: E, units and S0 re-pinned, bar 0 kept), any other units increase (SUSPENDED: notice and page, re-paged
  every 6 h), a units decrease (pinned units lowered, so a later scale-in still suspends), `ROLL_ADJUST` (E kept; noted
  once, durably, by the `roll_note` fingerprint in `state_reason`, so a takeover neither re-pins nor re-notices it)
  and entry corrections (E re-pinned, noted). Every re-pin of E or S0 (fill progress, entry correction, local rearm)
  drops the recorded `target` from the strategy state in the same CAS (`state_after_entry_repin`; `latched` kept), so a
  REOPEN recomputes it from the new E and S0 and never advises, or releases at, a target on the loss side of entry. An `ESTIMATED` entry time is upgraded to `STREAM` / `BROKER` while bar
  0's `decision_cap` has not passed (§B7). A pinned version this node cannot load suspends the position.
* **Ended rows**: an OPEN position whose row is CLOSED or DISARMED (an `arm_disarm`, or a position that came back) is
  never armed again and never counted as "assigned, not armed" (one `ArmStateStore.position` read per position and
  term).
* **Demand leases** (§B4, §A6): the tracker owns every ARM lease: ARMED for the specs of live positions (their BROKER
  series and indicators; TICK 1m and 15m bid and ask for the cross-check) and PREWARM for the pre-warm set, renewed
  every 15 s from the cached accepted document and re-asserted at once when the md generation changes (`NO_BOOT`, a
  reset) and at the start of every term (a takeover builds a new tracker).
* **Requests** (§B13): `arm_disarm`, `arm_repin` and `arm_rearm`, enqueued by the S32 handlers with `submit()` (non
  blocking) and run in this task; each is answered through `RequestAnswerPort` as applied, refused or moot.

Every state change goes through `src.riskmgr.sm11.next_state` and is written by the `row_version` CAS (`must=False`,
§B5.3); a miss is re-read on the next pass (requests retry at once, up to 3 times). Logs, pages and notices carry
ids, codes and counts only: never a level, a units figure, a price or an increment.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal
from enum import Enum
from functools import partial
from typing import Final, Protocol

from src.armconfig.resolver import PositionScope, resolve
from src.armconfig.validate import StrategyEntry, ValidDocument
from src.db.arm_ports import (
    RESOURCE_STOP_LEVEL,
    TERMINAL_ARM_STATES,
    ArmPositionRow,
    ArmStateStore,
    ArmVersionReader,
    EntryTime,
    DecisionRow,
    DecisionWriteStatus,
    PositionCas,
    PositionKey,
    position_ref,
)
from src.dto import InstrumentSpec, OrderDTO, PositionDTO, Side
from src.marketdata.indicators.base import IndicatorSpec
from src.marketdata.ports import HandleRef, MarketDataPort, StampPort
from src.marketdata.timing import SkewEstimate, decision_cap_ms
from src.marketdata.types import M1, M15, BarSpec, LeaseClass, MdGeneration, SeriesId, Source
from src.marketdata.types import Side as QuoteSide
from src.oversee.risk import StopDetail, effective_stop_detail
from src.riskmgr import arming as A
from src.riskmgr.config import ArmSettings
from src.riskmgr.envelope import stamp
from src.riskmgr.ports import Clock, ExecutionTimePort, HistoryFill, Pager
from src.riskmgr.registry import REGISTRY, ArmRegistry, ParamError, StrategyParams, TimingRowError
from src.riskmgr.replay import Frontier, Span, arming_frontier
from src.riskmgr.sm11 import ArmState, Refused, Sm11Event, Transition, next_state
from src.riskmgr.strategy import (
    EntryTimeRequirement,
    HistoryNeed,
    IndicatorNeed,
    Refuse,
    S0Requirement,
    SlStrategy,
    StrategyState,
)
from src.riskmgr.types import ArmContext, ArmEvent, BarContext, EntryTimeSource, S0Source, indicator_scope_id

log = logging.getLogger(__name__)

CONSUMER: Final = "arm"
"""The demand-lease consumer name of every ARM lease."""
TASK: Final = "arm-tracker"
CAS_RETRIES: Final = 3
SPAN_FIRST_BAR_STALE: Final = "first_bar_unknown_stale"
"""The caveat on an arming span that contains bar 0 (§B6.6: `FIRST_BAR_UNKNOWN(stale)`)."""
TICK_SPECS: Final = (M1, M15)
REQUEST_KINDS: Final = frozenset({"arm_disarm", "arm_repin", "arm_rearm"})
_REASON_RE: Final = re.compile(r"[a-z][a-z_]{0,63}")


# ---------------------------------------------------------------------------------------------------- ports
@dataclass(frozen=True, kw_only=True)
class AccountImage:
    """One account in the tracker's snapshot. `positions`: every OPEN position plus the current version of each
    position with a live ARM row (a CLOSED one included); `orders`: the account's non-terminal orders plus the entry
    orders of its positions; `fresh`: §B6.3 pre-guard (1) (no `orders` / `account` fence, no integrity incident,
    consumers running, the latest catch-up finished); `reasons`: why not (codes)."""

    adapter_id: str
    account_id: str
    fresh: bool
    reasons: tuple[str, ...] = ()
    positions: tuple[PositionDTO, ...] = ()
    orders: tuple[OrderDTO, ...] = ()
    instruments: tuple[InstrumentSpec, ...] = ()

    def instrument(self, symbol: str) -> InstrumentSpec | None:
        for i in self.instruments:
            if i.symbol == symbol:
                return i
        return None


class TrackerSnapshotPort(Protocol):
    async def read(self, accounts: Sequence[tuple[str, str]]) -> Sequence[AccountImage]:
        """One read-only snapshot (engine pool, `snapshot_transaction`) of every account holding an OPEN position
        plus each `(adapter_id, account_id)` in `accounts` (those with a live ARM row)."""
        ...


class BarGridPort(Protocol):
    """Bar-end grid facts the BarClock (S27) also uses."""

    def skew(self, adapter_id: str) -> SkewEstimate: ...

    def open_bar_ends(self, adapter_id: str, symbol: str, spec: BarSpec, from_ms: int, to_ms: int) -> Sequence[int]:
        """The ends of open-calendar buckets in `(from_ms, to_ms]`, increasing (a closed period is not a bar)."""
        ...

    def bar_sides(self, adapter_id: str) -> frozenset[QuoteSide]:
        """The quote sides the adapter serves BROKER bars for (empty: not bar-capable)."""
        ...


class AcceptedConfigPort(Protocol):
    def accepted(self) -> ValidDocument | None:
        """The cached accepted document (S33's installer), never the Supabase poll; None while there is none."""
        ...


class FailedScopeView(Protocol):
    def covers(self, adapter_id: str, series: Sequence[str], version_scopes: Sequence[str],
               position: str | None = None) -> bool:
        """A FAILED scope (ARM, the adapter, one of the series, a strategy or guard version, or the position's own
        POSITION scope when `position` = `adapter_id:position_id` is given, S38c) covers a position with these
        pinned specs (S30)."""
        ...


class NoticeKind(str, Enum):
    POSITION_NOT_MANAGED = "POSITION_NOT_MANAGED"
    SUSPENDED = "SUSPENDED"
    REPLACED = "REPLACED"
    REPINNED = "REPINNED"
    ROLL_ADJUST = "ROLL_ADJUST"
    ENTRY_CORRECTED = "ENTRY_CORRECTED"


@dataclass(frozen=True, kw_only=True)
class TrackerNotice:
    """A one-shot tracker notice item for S28 (ids and codes only). `item_key` is the §B8 item key."""

    kind: NoticeKind
    adapter_id: str
    account_id: str
    position_id: str
    symbol: str
    side: str
    reason: str
    at_ms: int

    @property
    def item_key(self) -> str:
        return f"{self.kind.value.lower()}:{position_ref(self.adapter_id, self.position_id)}:{self.reason}"


class TrackerNoticePort(Protocol):
    def notice(self, item: TrackerNotice) -> None: ...


class AnswerStatus(str, Enum):
    APPLIED = "applied"
    REFUSED = "refused"
    MOOT = "moot"


@dataclass(frozen=True)
class RequestAnswer:
    """How the owning task answers an operator request (S32 maps it to `Applied` / `Refused` / `Moot`)."""

    status: AnswerStatus
    reason: str = ""

    def __post_init__(self) -> None:
        if (self.status is AnswerStatus.APPLIED) != (self.reason == ""):
            raise ValueError("only a refused or moot answer carries a reason")
        if self.reason and not _REASON_RE.fullmatch(self.reason):
            raise ValueError("an answer reason is a short lower-case code")


APPLIED: Final = RequestAnswer(AnswerStatus.APPLIED)


def refused(reason: str) -> RequestAnswer:
    return RequestAnswer(AnswerStatus.REFUSED, reason)


def moot(reason: str) -> RequestAnswer:
    return RequestAnswer(AnswerStatus.MOOT, reason)


class RequestAnswerPort(Protocol):
    def complete(self, request_id: int, answer: RequestAnswer) -> None:
        """Record the answer (no I/O; the operator loop marks the request consumed)."""
        ...


@dataclass(frozen=True, kw_only=True)
class TrackerRequest:
    """One operator request for this task. `strategy_code` / `strategy_version` name the repin target; without them
    the assignment in force now is used."""

    request_id: int
    kind: str
    adapter_id: str
    position_id: str
    strategy_code: str | None = None
    strategy_version: int | None = None

    def __post_init__(self) -> None:
        if self.kind not in REQUEST_KINDS:
            raise ValueError("not an arm-tracker request kind")

    @classmethod
    def of(cls, request_id: int, kind: str, target: str, *, strategy_code: str | None = None,
           strategy_version: int | None = None) -> TrackerRequest:
        """From an `operator_requests` row: `target` is `adapter_id:position_id`."""
        adapter_id, sep, position_id = target.partition(":")
        if not sep or not adapter_id or not position_id:
            raise ValueError("an arm request target is adapter_id:position_id")
        return cls(request_id=request_id, kind=kind, adapter_id=adapter_id, position_id=position_id,
                   strategy_code=strategy_code, strategy_version=strategy_version)


# ---------------------------------------------------------------------------------------------------- settings
@dataclass(frozen=True, kw_only=True)
class TrackerSettings:
    snapshot_every_s: float = 2.0
    lease_every_ms: int = 15_000
    not_armed_page_after_ms: int = 300_000
    suspended_repage_ms: int = 6 * 3_600_000
    armed_handle_ceiling: int = 16
    """PR9 only: the armed stop handles one adapter's list reads may cover (R11-M20); one more is UNSUPPORTED."""
    asset_classes: frozenset[str] | None = None
    """Supported `InstrumentSpec.asset_class` values; None accepts any."""
    tick_symbols: tuple[tuple[str, str], ...] = ()
    """`XTNL_TICK_SYMBOLS` as `(adapter_id, symbol)`: pre-warmed under a wildcard assignment."""
    max_requests: int = 200


@dataclass
class TrackerCounters:
    passes: int = 0
    armed: int = 0
    ended: int = 0
    suspended: int = 0
    repinned: int = 0
    cas_miss: int = 0
    errors: int = 0
    lease_renewals: int = 0
    lease_reasserts: int = 0
    lease_refused: int = 0
    config_refused: int = 0
    requests_done: int = 0
    history_lookups: int = 0
    history_errors: int = 0
    history_upgrades: int = 0


class _Lookup(Protocol):
    """A running history lookup (the task `asyncio.create_task` returns)."""

    def done(self) -> bool: ...

    def result(self) -> HistoryFill: ...

    def cancel(self, msg: object = None) -> bool: ...

    def cancelled(self) -> bool: ...

    def exception(self) -> BaseException | None: ...


HISTORY_RETRY_MS: Final = 60_000
"""A history execution-time lookup is retried at most this often per position (plan S39b)."""
HISTORY_MAX_ATTEMPTS: Final = 5
HISTORY_LOOKBACK_MS: Final = 24 * 3_600_000
"""The trade-history window starts this long before the estimated (first-seen) entry time."""
HISTORY_TIMEOUT_S: Final = 30.0


# ---------------------------------------------------------------------------------------------------- needs and specs
@dataclass(frozen=True)
class Needs:
    """A pin's market-data needs as templates on `(bar_spec, side)` (never a resolved spec)."""

    bar_spec: BarSpec
    trigger_side: QuoteSide
    history: tuple[HistoryNeed, ...]
    indicators: tuple[IndicatorNeed, ...]
    guard_history: tuple[HistoryNeed, ...] = ()
    """The needs only the pinned guards declare (not the strategy): read while a guard can still apply (round 13,
    FirstBarGuard until bar 0 is decided or spanned)."""
    guard_indicators: tuple[IndicatorNeed, ...] = ()

    def sides(self) -> frozenset[QuoteSide]:
        return frozenset({self.trigger_side, *(h.side for h in self.history), *(n.side for n in self.indicators)})

    def series_keys(self) -> tuple[tuple[BarSpec, QuoteSide], ...]:
        keys = {(self.bar_spec, self.trigger_side), *((h.bar_spec, h.side) for h in self.history),
                *((n.bar_spec, n.side) for n in self.indicators)}
        return tuple(sorted(keys, key=lambda k: (k[0].timeframe_s, k[1].value)))

    def indicators_json(self) -> str:
        return json.dumps([{"kind": n.kind, "version": n.version, "bar_spec": n.bar_spec.name, "side": n.side.value,
                            "params": dict(n.params)} for n in self.indicators], sort_keys=True,
                          separators=(",", ":"))

    def history_json(self) -> str:
        return json.dumps([{"bar_spec": h.bar_spec.name, "side": h.side.value, "bars": h.bars} for h in self.history],
                          sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class PositionSpecs:
    """The resolved lease specs of one position: decision series and indicators (ARMED) and TICK cross-check
    series."""

    series: tuple[SeriesId, ...]
    indicators: tuple[IndicatorSpec, ...]
    ticks: tuple[SeriesId, ...]
    guard_series: tuple[SeriesId, ...] = ()
    """Of `series`, the ones only a guard reads (never the trigger or the strategy's own history)."""
    guard_indicators: tuple[IndicatorSpec, ...] = ()
    """Of `indicators`, the ones only a guard reads."""

    def all(self, *, past_guards: bool = False) -> tuple[SeriesId | IndicatorSpec, ...]:
        """Every lease spec; `past_guards` (bar 0 decided or spanned, round 13) leaves out the guard-only specs, so
        their leases (FirstBarGuard's ATR and its bid history for a short) are released."""
        if not past_guards:
            return (*self.series, *self.indicators, *self.ticks)
        return (*(s for s in self.series if s not in self.guard_series),
                *(i for i in self.indicators if i not in self.guard_indicators), *self.ticks)

    def scope_series(self, *, past_guards: bool = False) -> tuple[str, ...]:
        """The FAILED scope ids the position depends on (round 13): its series' canonical ids and each indicator's
        own scope id (`indicator_scope_id`) with its input series. `past_guards` drops what only a guard reads: a
        position past bar 0 never depends on FirstBarGuard's ATR or, for a short, on the bid trigger series."""
        series = [s for s in self.series if not (past_guards and s in self.guard_series)]
        indicators = [i for i in self.indicators if not (past_guards and i in self.guard_indicators)]
        return tuple(sorted({s.canonical for s in series} | {i.series.canonical for i in indicators}
                            | {indicator_scope_id(i.series.adapter_id, i.series.symbol, i.indicator_id)
                               for i in indicators}))


FIRST_BAR_SCAN_MS: Final = 14 * 86_400_000
"""Bar 0 (the first open bar end after the fill bucket starts) is looked for this far after the fill."""
FIRST_BAR_SCAN_BUCKETS: Final = 16
"""... in windows of this many buckets, stopping at the first open bar end (never a walk from the fill to `T`)."""
GUARD_SCAN_BARS: Final = 4
"""The first open bar ends after the fill bucket starts that are asked whether a pinned guard applies to them
(FirstBarGuard: the entry bucket and bar 0 under either anchor fall inside them)."""


def open_bar_ends_after(grid: BarGridPort, adapter_id: str, symbol: str, spec: BarSpec, entry_ms: int, count: int,
                        upto_ms: int | None = None) -> list[int]:
    """The first `count` open bar ends after the fill bucket starts (a bucket of a closed period is not a bar), looked
    for in windows of `FIRST_BAR_SCAN_BUCKETS` up to `FIRST_BAR_SCAN_MS` after the fill (and not past `upto_ms`);
    fewer when not found (or the grid raises: what was found so far)."""
    tf = spec.timeframe_ms
    out: list[int] = []
    try:
        lo = spec.bucket_start_ms(entry_ms)
        limit = lo + FIRST_BAR_SCAN_MS if upto_ms is None else min(upto_ms, lo + FIRST_BAR_SCAN_MS)
        while lo < limit and len(out) < count:
            hi = min(limit, lo + FIRST_BAR_SCAN_BUCKETS * tf)
            out.extend(e for e in grid.open_bar_ends(adapter_id, symbol, spec, lo, hi) if lo < e <= hi)
            lo = hi
    except Exception:
        pass
    return out[:count]


def first_bar_end(grid: BarGridPort, adapter_id: str, symbol: str, spec: BarSpec, entry_ms: int,
                  upto_ms: int | None = None) -> int | None:
    """Bar 0's end as the engine counts it (`bar_index` 0): the first open bar end after the fill bucket starts;
    None when not found. The one lookup of the evaluator's `bar_index` (round 13)."""
    got = open_bar_ends_after(grid, adapter_id, symbol, spec, entry_ms, 1, upto_ms)
    return got[0] if got else None


def guards_apply(registry: ArmRegistry, params: StrategyParams, ctx: BarContext, state: StrategyState) -> bool:
    """A pinned guard applies to this bar (`applies`; FirstBarGuard: bar 0 or the entry bucket), so its guard-only
    inputs are read, recorded and leased; a guard that cannot be loaded or raises counts as applying (fail safe: its
    inputs are kept)."""
    for g in params.guards:
        guard = registry.guard(g.kind, g.version)
        if guard is None:
            return True
        try:
            if guard.applies(g, ctx, state):
                return True
        except Exception:
            return True
    return False


def guard_horizon_end(registry: ArmRegistry, row: ArmPositionRow, spec: BarSpec, params: StrategyParams,
                      state: StrategyState, ends: Sequence[int]) -> int | None:
    """The last bar end of `ends` (the first open bar ends after the fill bucket starts) at which a pinned guard
    applies, asked of the guards themselves (FirstBarGuard: bar 0 under its `anchor`, `next_full_bucket` included);
    `ends[0]` when none applies; None when one applies at the last candidate or nothing is known (unbounded: the guard
    inputs are kept)."""
    if not ends:
        return None
    try:
        source = EntryTimeSource(row.entry_time_source)
    except ValueError:
        return None
    horizon = ends[0]
    for n, end in enumerate(ends):
        ctx = BarContext(position_id=row.key.position_id, adapter_id=row.key.adapter_id, symbol=row.symbol,
                         side=row.side, bar_spec=spec, event=ArmEvent.POST, bar_end_ms=end,
                         bar_index=0 if n == 0 else 1, entry=row.entry_level, entry_time_ms=row.entry_time_ms,
                         entry_time_source=source, s0=row.s0_level)
        if guards_apply(registry, params, ctx, state):
            if n == len(ends) - 1:
                return None
            horizon = end
    return horizon


def past_guards(row: ArmPositionRow, guard_end_ms: int | None, *, guard_cap_ms: int | None, now_ms: int) -> bool:
    """No pinned guard can apply any more, so the guard-only inputs (FirstBarGuard's ATR and, for a short, its bid
    history) drop out of the position's FAILED coverage and its leases (round 13 review): the cursor is at or after
    the last bar a guard applies to (`guard_horizon_end`: bar 0 itself, not merely a cursor that is set; arming
    inside the fill bucket sets the cursor to the bucket's start) **and** that bar's `decision_cap` has passed (a
    correction of it can still read the guard's bid bars and ATR until then). An unknown horizon is never past
    (the guard inputs are kept: fail safe)."""
    if guard_end_ms is None or guard_cap_ms is None or row.last_decided_bar is None:
        return False
    return row.last_decided_bar >= guard_end_ms and now_ms >= guard_cap_ms


def version_scopes(params: StrategyParams) -> tuple[str, ...]:
    """The STRATEGY_VERSION scope ids a pin touches (`strategy:<KIND>:<VERSION>`, `guard:<KIND>:<VERSION>`)."""
    return (f"strategy:{params.kind}:{params.impl_version}", *(f"guard:{g.kind}:{g.version}" for g in params.guards))


def params_json(params: StrategyParams) -> str:
    return json.dumps(dict(params.values), sort_keys=True, separators=(",", ":"))


def guards_json(params: StrategyParams) -> str:
    return json.dumps([g.canonical() for g in params.guards], sort_keys=True, separators=(",", ":"))


REPIN_STALE_FIELD: Final = "target"
"""The `STATE_SCHEMA` field a re-pin of E or S0 makes stale (kind (a)'s recorded target, `REOPEN_POLICY =
REUSE_TARGET`): it was computed from the old E and S0 (`E + σ·be_offset_r·max(R, 0)`), so after a re-pin it can sit on
the loss side of the new E."""


def state_after_entry_repin(state_json: str, schema: frozenset[str] | None, *, repinned: bool) -> str:
    """The stored strategy state to write with a re-pin of E or S0 (FILL_PROGRESS, ENTRY_CORRECTION, local rearm):
    `REPIN_STALE_FIELD` dropped when the pinned version's `STATE_SCHEMA` declares it and the state holds it, every
    other field (`latched`) kept. A REOPEN then recomputes the target from the re-pinned E and S0, so it never reuses a
    target recorded under the old E (a stop advised on the loss side of entry, or a release with the stop there).
    Unchanged when nothing was re-pinned, the version is unknown (`schema` None), the schema has no such field or the
    state does not decode."""
    if not repinned or schema is None or REPIN_STALE_FIELD not in schema:
        return state_json
    state = StrategyState.decode(state_json)
    if state is None or state.text(REPIN_STALE_FIELD) is None:
        return state_json
    return StrategyState(tuple((k, v) for k, v in state.fields if k != REPIN_STALE_FIELD)).encode()


@dataclass(frozen=True)
class _Plan:
    entry: StrategyEntry
    params: StrategyParams
    strategy: SlStrategy
    needs: Needs
    specs: PositionSpecs
    entry_ms: int
    entry_source: EntryTimeSource
    s0: A.S0Pick
    state: StrategyState
    handles: int


@dataclass(frozen=True)
class _RequestPlan:
    """What an applied request writes: the new row, an arming span (rearm) and the new pin's lease specs."""

    row: ArmPositionRow
    span: DecisionRow | None = None
    specs: PositionSpecs | None = None


def _prior(row: ArmPositionRow) -> ArmState | None:
    return ArmState(row.prior_state) if row.prior_state else None


def _suspended(row: ArmPositionRow) -> bool:
    return row.state == ArmState.SUSPENDED.value or (row.state == ArmState.FAILED.value
                                                    and row.prior_state == ArmState.SUSPENDED.value)


# ---------------------------------------------------------------------------------------------------- the task
class ArmTracker:
    """The `arm-tracker` task of one term (see the module docstring)."""

    def __init__(self, *, store: ArmStateStore, versions: ArmVersionReader, snapshot: TrackerSnapshotPort,
                 md: MarketDataPort, grid: BarGridPort, config: AcceptedConfigPort, notices: TrackerNoticePort,
                 answers: RequestAnswerPort, pager: Pager, clock: Clock, settings: ArmSettings,
                 failed: FailedScopeView | None = None, tracker: TrackerSettings | None = None,
                 registry: ArmRegistry = REGISTRY, fatal: Callable[[BaseException], bool] | None = None,
                 exec_times: ExecutionTimePort | None = None) -> None:
        """`exec_times` (plan S39b, gate `entry_time_source`): the broker trade-history execution time of an entry
        order; None = no history source (an `ESTIMATED` entry time is upgraded only from `event_log`)."""
        self._store = store
        self._exec_times = exec_times
        self._history: dict[str, _Lookup] = {}
        self._history_tries: dict[str, tuple[int, int]] = {}
        """Per ref: (attempts, time of the last one) of the history lookup this term."""
        self._versions = versions
        self._snapshot = snapshot
        self._md = md
        self._grid = grid
        self._config = config
        self._notices = notices
        self._answers = answers
        self._pager = pager
        self._clock = clock
        self._arm = settings
        self._failed = failed
        self._cfg = tracker or TrackerSettings()
        self._registry = registry
        self._fatal = fatal or (lambda e: False)
        self.counters = TrackerCounters()
        self._requests: list[TrackerRequest] = []
        self._images: dict[tuple[str, str], AccountImage] = {}
        self._first_seen: dict[str, int] = {}
        self._account_of: dict[str, tuple[str, str]] = {}
        self._waiting: dict[str, str] = {}
        self._assigned_since: dict[str, int] = {}
        self._overdue_paged: set[str] = set()
        self._unmanaged: dict[str, tuple[A.Unmanaged, str]] = {}
        self._notified: dict[str, set[str]] = {}
        self._ended: dict[str, tuple[str, str]] = {}
        """Refs whose `arm_positions` row is CLOSED or DISARMED (never armed again), with their account."""
        self._row_checked: dict[str, tuple[str, str]] = {}
        """Refs read once this term with no row (so the ended-row read is not repeated every pass)."""
        self._specs: dict[str, PositionSpecs] = {}
        self._handles: dict[str, int] = {}
        self._handle_refs: dict[str, tuple[HandleRef, ...]] = {}
        """The current stop handles of each live row (PR9's `StopHandlesPort`, S36b), from the last image."""
        self._live_adapter: dict[str, str] = {}
        self._suspend_paged: dict[str, int] = {}
        self._leased: dict[str, LeaseClass] = {}
        self._last_lease_ms: int | None = None
        self._generation: MdGeneration | None = None
        self._edge_refused: str | None = None
        self._rows_last: tuple[ArmPositionRow, ...] = ()
        self.rows_read_ms: int | None = None
        """When the rows of `live_rows()` were read (the start of that pass, before the read: the liveness watch
        confirms an overdue position only on rows read after a BarClock pass it saw complete, round 14 review)."""
        self.last_pass_ms: int | None = None
        """The end of the last completed pass (the liveness watch's stalled-tracker page, plan S38g)."""
        self._horizon: dict[tuple[str, int, str, str, str], tuple[int, int]] = {}
        """The guard horizon's bar end and `decision_cap` per (position ref, entry time, bar spec, guards hash, entry
        time source) (`_past_guards`)."""

    # ------------------------------------------------------------------------------------------------ requests
    def submit(self, request: TrackerRequest) -> bool:
        """Enqueue an operator request (non-blocking; called by the S32 handler). False when the queue is full."""
        if len(self._requests) >= self._cfg.max_requests:
            return False
        self._requests.append(request)
        return True

    # ------------------------------------------------------------------------------------------------ views
    def unmanaged(self) -> tuple[tuple[str, str, str], ...]:
        """`(position ref, UNASSIGNED | UNSUPPORTED, reason)` for the daily test notice, `arm_status` and `arm show`."""
        return tuple(sorted((ref, kind.value, reason) for ref, (kind, reason) in self._unmanaged.items()))

    def leases(self) -> dict[str, LeaseClass]:
        return dict(self._leased)

    def armed_adapters(self) -> tuple[str, ...]:
        """The adapters of the live `arm_positions` rows read by the last pass (S36b: `observe_md`'s markets)."""
        return tuple(sorted(set(self._live_adapter.values())))

    def armed_count(self) -> int:
        """The live rows read by the last pass."""
        return len(self._live_adapter)

    def live_rows(self) -> tuple[ArmPositionRow, ...]:
        """The live `arm_positions` rows read by the last pass (the liveness watch's input, plan S38g)."""
        return self._rows_last

    def unarmed(self, now_ms: int) -> int:
        """Assigned positions not armed `not_armed_page_after_ms` after they were first seen fresh (the `unarmed`
        view; until round 14 this was reported as "overdue", which is now `src.riskmgr.liveness`'s decision age)."""
        return sum(1 for ref, since in self._assigned_since.items()
                   if ref not in self._unmanaged and now_ms - since >= self._cfg.not_armed_page_after_ms)

    def stop_handles(self, adapter_id: str) -> dict[str, tuple[HandleRef, ...]]:
        """PR9's `StopHandlesPort`: the current stop handles of the adapter's live rows, by position ref (memory)."""
        return {ref: refs for ref, refs in self._handle_refs.items()
                if self._live_adapter.get(ref) == adapter_id and refs}

    def handle_count(self, adapter_id: str) -> int:
        """The stop handles of the adapter's live rows (the PR9 armed-handle count)."""
        return sum(n for ref, n in self._handles.items() if self._live_adapter.get(ref) == adapter_id)

    def series_of(self, row: ArmPositionRow) -> tuple[str, ...] | None:
        """The FAILED manager's `PositionSeriesPort`: the series (canonical ids) the position's pinned specs read, as
        resolved this term; None when not known yet (the caller then fails closed)."""
        specs = self._specs.get(row.ref)
        return None if specs is None else specs.scope_series(past_guards=self._past_guards(row,
                                                                                            self._clock.now_ms()))

    def _past_guards(self, row: ArmPositionRow, now: int) -> bool:
        """`past_guards` with the guard horizon asked of the pinned guards over the grid's first open bar ends (cached
        per position, entry time, bar spec and guard set)."""
        key = (row.ref, row.entry_time_ms, row.bar_spec, row.guards_hash, row.entry_time_source)
        horizon = self._horizon.get(key)
        if horizon is None:
            try:
                spec = BarSpec.parse(row.bar_spec)
            except ValueError:
                return False
            params = self._pinned(row)
            state = StrategyState.decode(row.strategy_state_json)
            if params is None or state is None:
                return False
            ends = open_bar_ends_after(self._grid, row.key.adapter_id, row.symbol, spec, row.entry_time_ms,
                                       GUARD_SCAN_BARS)
            end = guard_horizon_end(self._registry, row, spec, params, state, ends)
            if end is None:
                return False
            horizon = (end, decision_cap_ms(end, spec, self._grid.skew(row.key.adapter_id)))
            self._horizon[key] = horizon
        return past_guards(row, horizon[0], guard_cap_ms=horizon[1], now_ms=now)

    # ------------------------------------------------------------------------------------------------ loop
    async def run(self, stop: asyncio.Event, stamp_port: StampPort) -> None:
        try:
            while not stop.is_set():
                stamp_port.stamp()
                try:
                    await self.step()
                except Exception as e:
                    if self._fatal(e):
                        raise
                    self.counters.errors += 1
                    log.warning("%s: pass failed (%r)", TASK, type(e))
                stamp_port.stamp()
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self._cfg.snapshot_every_s)
                except TimeoutError:
                    pass
        finally:
            self.close()

    async def step(self) -> None:
        """One pass: snapshot, arming and changes, requests, leases, the not-armed page."""
        now = self._clock.now_ms()
        self.counters.passes += 1
        rows = {r.key: r for r in await self._store.live_positions()}
        self._rows_last = tuple(rows.values())
        self.rows_read_ms = now
        self._live_adapter = {r.ref: r.key.adapter_id for r in rows.values()}
        self._handle_refs = {r: v for r, v in self._handle_refs.items() if r in self._live_adapter}
        self._horizon = {k: v for k, v in self._horizon.items() if k[0] in self._live_adapter}
        accounts = sorted({(k.adapter_id, r.account_id) for k, r in rows.items()})
        images = list(await self._snapshot.read(accounts))
        self._images = {(i.adapter_id, i.account_id): i for i in images}
        doc = self._usable_doc()
        self._count_handles(images, rows)
        seen: set[str] = set()
        for image in images:
            if image.fresh:
                await self._track_account(image, rows, doc, now, seen)
        self._forget(seen, rows)
        await self._drain_requests(now)
        await self._renew_leases(now, doc)
        self._page_overdue(now)
        self.last_pass_ms = self._clock.now_ms()

    def _count_handles(self, images: Sequence[AccountImage], rows: dict[PositionKey, ArmPositionRow]) -> None:
        """The stop handles of every live row, before any arming of this pass, so the PR9 per-adapter ceiling sees
        every account of the adapter (R11-M20). A stale image counts too (counting arms nothing): after a takeover
        another account of the adapter may still be catching up; a row with no image keeps its last count."""
        for image in images:
            for p in image.positions:
                key = PositionKey(adapter_id=p.adapter_id, position_id=p.position_id)
                if key in rows and p.status == "OPEN":
                    detail = effective_stop_detail(p, image.positions, image.orders, at_entry=True)
                    self._handles[rows[key].ref] = 0 if detail is None else len(detail.handles)
                    self._handle_refs[rows[key].ref] = () if detail is None else tuple(
                        HandleRef(h.kind, h.id) for h in detail.handles)

    async def _guarded(self, ref: str, work: Callable[[], Awaitable[None]]) -> None:
        """One position's work: an error is counted and logged by class, never raised (one bad position never stops
        the pass), except a fatal one (a lost fence: the caller hard-exits)."""
        try:
            await work()
        except Exception as e:
            if self._fatal(e):
                raise
            self.counters.errors += 1
            log.warning("%s: %s failed (%r)", TASK, ref, type(e))

    def _usable_doc(self) -> ValidDocument | None:
        doc = self._config.accepted()
        if doc is None:
            return None
        extra = A.second_edges(doc, self._arm.edge_code)
        if not extra:
            self._edge_refused = None
            return doc
        if self._edge_refused != doc.doc_hash:
            self._edge_refused = doc.doc_hash
            self.counters.config_refused += 1
            self._pager.page("warning", "ARM: configuration enables a second edge; refused for arming",
                             f"doc {doc.doc_hash[:12]}; edges {','.join(extra)}; Phase 1 arms one edge only")
        return None

    def _forget(self, seen: set[str], rows: dict[PositionKey, ArmPositionRow]) -> None:
        """Drop the in-memory books of positions no longer OPEN on a fresh image of their account (bounded memory
        over 24/7 running)."""
        fresh = {k for k, i in self._images.items() if i.fresh}
        for ref in list(self._first_seen):
            if ref not in seen and self._account_of.get(ref) in fresh and ref not in self._live_adapter:
                self._drop_pending(ref)
        open_refs = {position_ref(p.adapter_id, p.position_id) for k in fresh for p in self._images[k].positions
                     if p.status == "OPEN"}
        for book in (self._ended, self._row_checked):
            for ref, account in list(book.items()):
                if account in fresh and ref not in open_refs:
                    book.pop(ref, None)
        live = {r.ref for r in rows.values()}
        for ref in list(self._notified):
            if ref not in self._first_seen and ref not in live:
                self._notified.pop(ref, None)
        for ref in [r for r in {*self._history, *self._history_tries} if r not in live]:
            self._drop_history(ref)                     # closed or disarmed: its lookup is no longer wanted

    def _drop_pending(self, ref: str) -> None:
        """Forget an unarmed position's waiting state (its "assigned, not armed" clock included)."""
        for book in (self._first_seen, self._account_of, self._waiting, self._assigned_since, self._unmanaged):
            book.pop(ref, None)
        self._overdue_paged.discard(ref)

    async def _row_ended(self, p: PositionDTO, account: tuple[str, str]) -> bool:
        """An `arm_positions` row exists for this OPEN position that is not live (CLOSED / DISARMED: never armed
        again), or one appeared since the pass began; read once per position and term."""
        ref = position_ref(p.adapter_id, p.position_id)
        if ref in self._ended:
            return True
        if ref in self._row_checked:
            return False
        row = await self._store.position(PositionKey(adapter_id=p.adapter_id, position_id=p.position_id))
        if row is None:
            self._row_checked[ref] = account
            return False
        if row.state in TERMINAL_ARM_STATES:
            self._ended[ref] = account
        return True

    # ------------------------------------------------------------------------------------------------ accounts
    async def _track_account(self, image: AccountImage, rows: dict[PositionKey, ArmPositionRow],
                             doc: ValidDocument | None, now: int, seen: set[str]) -> None:
        by_id = {p.position_id: p for p in image.positions}
        mine = [r for k, r in rows.items() if k.adapter_id == image.adapter_id and r.account_id == image.account_id]
        armed = {r.key.position_id for r in mine}
        for row in mine:
            p = by_id.get(row.key.position_id)
            await self._guarded(row.ref, partial(self._follow, row, p, image, armed, now))
        for p in image.positions:
            if p.status != "OPEN" or p.position_id in armed:
                continue
            ref = position_ref(p.adapter_id, p.position_id)
            seen.add(ref)
            self._first_seen.setdefault(ref, now)
            self._account_of[ref] = (image.adapter_id, image.account_id)
            await self._guarded(ref, partial(self._try_arm, image, p, doc, now))

    # ------------------------------------------------------------------------------------------------ arming
    async def _try_arm(self, image: AccountImage, p: PositionDTO, doc: ValidDocument | None, now: int) -> None:
        ref = position_ref(p.adapter_id, p.position_id)
        if await self._row_ended(p, (image.adapter_id, image.account_id)):
            self._drop_pending(ref)          # disarmed or ended: no re-plan, no ARMED demand, no 5-minute page
            return
        if p.entry_price is None or p.units is None or p.units <= 0:
            self._waiting[ref] = A.ENTRY_UNKNOWN
            return
        if doc is None:
            self._waiting[ref] = A.NO_CONFIG
            return
        scope = PositionScope(p.adapter_id, p.account_id, p.symbol)
        resolved = resolve(doc, scope, RESOURCE_STOP_LEVEL, now)
        if resolved is None:
            self._not_managed(p, A.Unmanaged.UNASSIGNED, A.NO_ASSIGNMENT, now)
            return
        self._unmanaged.pop(ref, None)
        self._assigned_since.setdefault(ref, now)
        detail = effective_stop_detail(p, image.positions, image.orders, at_entry=True)
        if detail is None:
            self._waiting[ref] = A.NO_STOP
            return
        plan = await self._plan(image, p, p.entry_price, resolved.strategy, detail, now)
        if isinstance(plan, str):
            self._not_managed(p, A.Unmanaged.UNSUPPORTED, plan, now)
            return
        added = await self._demand_all(plan.specs.all())
        if added is None:
            self._not_managed(p, A.Unmanaged.UNSUPPORTED, A.DEMAND_CEILING, now)
            return
        if not await self._write_arming(p, p.entry_price, p.units, doc, plan, now):
            # a row exists after all (ended, or written by another pass): give back what this arming demanded
            await self._release(added)
            self._drop_pending(ref)
            self._row_checked.pop(ref, None)          # read it again next pass

    def _pin(self, entry: StrategyEntry) -> StrategyParams | str:
        """Re-parse the strategy row as a new pin (deprecations and timing rows checked now, S25)."""
        params = entry.params
        raw: dict[str, object] = {**dict(params.values), "guards": [g.canonical() for g in params.guards]}
        try:
            return self._registry.parse_params(params.kind, params.impl_version, raw, new_pin=True)
        except TimingRowError:
            return A.NO_TIMING_ROW
        except ParamError:
            return A.PIN_REFUSED

    def _repinned_state(self, row: ArmPositionRow, entry: Decimal, s0: Decimal) -> str:
        """`row.strategy_state_json` for a row whose E / S0 become `entry` / `s0` (`state_after_entry_repin`, written in
        the same CAS as the new levels)."""
        strategy = self._registry.strategy(row.strategy_kind, row.strategy_impl_version)
        return state_after_entry_repin(row.strategy_state_json, None if strategy is None else strategy.STATE_SCHEMA,
                                       repinned=entry != row.entry_level or s0 != row.s0_level)

    def _pinned(self, row: ArmPositionRow) -> StrategyParams | None:
        """The stored pin re-parsed (`new_pin=False`), or None when this node cannot load it."""
        try:
            raw = json.loads(row.strategy_params_json)
            guards = json.loads(row.guards_json)
            if not isinstance(raw, dict):
                return None
            return self._registry.parse_params(row.strategy_kind, row.strategy_impl_version,
                                               {**raw, "guards": guards}, new_pin=False)
        except (ValueError, TypeError):
            return None

    def _needs(self, strategy: SlStrategy, params: StrategyParams, side: Side) -> Needs | str:
        try:
            specs = {ev.bar_spec for ev in strategy.triggers(params)}
            if len(specs) != 1:
                return A.BAR_SPECS
            history = list(strategy.required_history(params))
            indicators = list(strategy.required_indicators(params))
            own_history, own_indicators = set(history), set(indicators)
            g_history: list[HistoryNeed] = []
            g_indicators: list[IndicatorNeed] = []
            for g in params.guards:
                guard = self._registry.guard(g.kind, g.version)
                if guard is None:
                    return A.PIN_REFUSED
                g_history += guard.required_history(g)
                g_indicators += guard.required_indicators(g)
            history += g_history
            indicators += g_indicators
            trig = strategy.trigger_side(params, side)
        except Exception:
            return A.PIN_REFUSED
        return Needs(next(iter(specs)), trig, tuple(sorted(set(history))), tuple(sorted(set(indicators))),
                     guard_history=tuple(sorted(set(g_history) - own_history)),
                     guard_indicators=tuple(sorted(set(g_indicators) - own_indicators)))

    @staticmethod
    def _resolve(adapter_id: str, symbol: str, needs: Needs, increment: Decimal | None) -> PositionSpecs | str:
        if needs.indicators and increment is None:
            return A.INCREMENT_UNKNOWN
        series = tuple(SeriesId(adapter_id, symbol, spec, side, Source.BROKER) for spec, side in needs.series_keys())
        indicators = tuple(n.resolve(adapter_id, symbol, increment) for n in needs.indicators
                           if increment is not None)
        ticks = tuple(SeriesId(adapter_id, symbol, spec, side, Source.TICK)
                      for spec in TICK_SPECS for side in (QuoteSide.BID, QuoteSide.ASK))
        g_ind = tuple(n.resolve(adapter_id, symbol, increment) for n in needs.guard_indicators
                      if increment is not None)
        core_keys = {(needs.bar_spec, needs.trigger_side),
                     *((h.bar_spec, h.side) for h in needs.history if h not in needs.guard_history),
                     *((n.bar_spec, n.side) for n in needs.indicators if n not in needs.guard_indicators)}
        g_series = tuple(s for s in series if (s.spec, s.side) not in core_keys)
        return PositionSpecs(series, indicators, ticks, guard_series=g_series, guard_indicators=g_ind)

    async def _plan(self, image: AccountImage, p: PositionDTO, entry_level: Decimal, entry: StrategyEntry,
                    detail: StopDetail, now: int) -> _Plan | str:
        """Every arming check but the demand leases; an UNSUPPORTED reason when one fails."""
        params = self._pin(entry)
        if isinstance(params, str):
            return params
        strategy = self._registry.strategy(params.kind, params.impl_version)
        if strategy is None:
            return A.PIN_REFUSED
        instrument = image.instrument(p.symbol)
        allowed = self._cfg.asset_classes
        if allowed is not None and (instrument is None or instrument.asset_class not in allowed):
            return A.ASSET_CLASS
        needs = self._needs(strategy, params, p.side)
        if isinstance(needs, str):
            return needs
        if not needs.sides() <= self._grid.bar_sides(p.adapter_id):
            return A.NO_BROKER_BARS
        specs = self._resolve(p.adapter_id, p.symbol, needs, None if instrument is None else instrument.price_increment)
        if isinstance(specs, str):
            return specs
        if self._arm.stop_reads:
            held = sum(self._handles.get(r, 0) for r, a in self._live_adapter.items() if a == p.adapter_id)
            if held + len(detail.handles) > self._cfg.armed_handle_ceiling:
                return A.STOP_READ_BUDGET
        ref = position_ref(p.adapter_id, p.position_id)
        found = await self._versions.entry_time(p.adapter_id, p.position_id, p.opened_by_client_order_id)
        entry_ms, source = A.entry_choice(found, first_seen_ms=self._first_seen.get(ref, now))
        s0 = await self._s0(p, detail, entry_ms if source is not EntryTimeSource.ESTIMATED else None, now)
        req = strategy.arming_requirements(params)
        if req.s0 is S0Requirement.AT_FILL and s0.source is not S0Source.AT_FILL:
            return A.S0_NOT_AT_FILL
        if req.entry_time is EntryTimeRequirement.EXACT and source is EntryTimeSource.ESTIMATED:
            return A.ENTRY_TIME_NOT_EXACT
        ctx = ArmContext(position_id=p.position_id, adapter_id=p.adapter_id, symbol=p.symbol, side=p.side,
                         bar_spec=needs.bar_spec, entry=entry_level, s0=s0.level, s0_source=s0.source,
                         entry_time_ms=entry_ms, entry_time_source=source, armed_at_ms=now)
        try:
            state = strategy.init_state(params, ctx)
        except Exception:
            return A.INIT_FAILED
        return _Plan(entry, params, strategy, needs, specs, entry_ms, source, s0, state, len(detail.handles))

    async def _s0(self, p: PositionDTO, detail: StopDetail, fill_ms: int | None, now: int) -> A.S0Pick:
        children = sorted(h.id for h in detail.handles if h.kind == "CHILD_ORDER")
        at_fill = [] if fill_ms is None else await self._versions.stop_versions(
            p.adapter_id, p.position_id, children, fill_ms + A.AT_FILL_WINDOW_MS)
        current = await self._versions.stop_versions(p.adapter_id, p.position_id, children, now)
        return A.s0_pick(detail, fill_known=fill_ms is not None, at_fill=at_fill, now=current)

    async def _demand_all(self, specs: Sequence[SeriesId | IndicatorSpec]) -> list[str] | None:
        """ARMED leases for a position's specs: the spec ids this call added (not held before), or None on a
        refusal, after the leases this call added were released again."""
        added: list[str] = []
        for spec in specs:
            sid = self._md.define(spec)
            held = sid in self._leased
            if not await self._md.demand(sid, CONSUMER, LeaseClass.ARMED):
                await self._release(added)
                self.counters.lease_refused += 1
                return None
            if not held:
                added.append(sid)
            self._leased[sid] = LeaseClass.ARMED
        return added

    async def _release(self, added: Sequence[str]) -> None:
        for sid in added:
            await self._md.release(sid, CONSUMER)
            self._leased.pop(sid, None)

    def _frontier(self, p_adapter: str, symbol: str, spec: BarSpec, entry_ms: int, now: int,
                  previous: Frontier | None = None) -> tuple[Frontier, Span | None]:
        fill_bucket = spec.bucket_start_ms(entry_ms)
        start = fill_bucket if previous is None else min(previous.cursor_ms, fill_bucket)
        ends = list(self._grid.open_bar_ends(p_adapter, symbol, spec, start, now))
        got = arming_frontier(fill_bucket_ms=fill_bucket, armed_at_ms=now, spec=spec,
                              skew=self._grid.skew(p_adapter), bar_ends=ends, previous=previous,
                              bar0_end_ms=fill_bucket + spec.timeframe_ms)
        return got.frontier, got.span

    def _scope_failed(self, adapter_id: str, specs: PositionSpecs, params: StrategyParams,
                      position: str | None = None) -> bool:
        return self._failed is not None and self._failed.covers(adapter_id, specs.scope_series(),
                                                                version_scopes(params), position)

    async def _write_arming(self, p: PositionDTO, entry_level: Decimal, units: Decimal, doc: ValidDocument,
                            plan: _Plan, now: int) -> bool:
        """Insert the row (and its arming span); False when a row already exists (nothing written)."""
        ref = position_ref(p.adapter_id, p.position_id)
        frontier, span = self._frontier(p.adapter_id, p.symbol, plan.needs.bar_spec, plan.entry_ms, now)
        t = next_state(ArmState.DISCOVERED, Sm11Event.ARMING_PASSED)
        if isinstance(t, Transition) and self._scope_failed(p.adapter_id, plan.specs, plan.params, ref):
            t = next_state(t.state, Sm11Event.SCOPE_FAILED)
        if isinstance(t, Refused):   # impossible by the SM11 table; never arm around it
            return False
        row = ArmPositionRow(
            key=PositionKey(adapter_id=p.adapter_id, position_id=p.position_id), account_id=p.account_id,
            symbol=p.symbol, side=p.side, state=t.state.value,
            prior_state=None if t.prior_state is None else t.prior_state.value,
            state_reason="scope_failed" if t.state is ArmState.FAILED else None,
            edge_code=self._arm.edge_code, attribution_source=A.ATTRIBUTION_SINGLE_EDGE,
            opened_by_client_order_id=p.opened_by_client_order_id, strategy_code=plan.entry.code,
            strategy_version=plan.entry.version, strategy_kind=plan.params.kind,
            strategy_impl_version=plan.params.impl_version, strategy_params_json=params_json(plan.params),
            guards_json=guards_json(plan.params), guards_hash=plan.params.guards_hash,
            indicator_specs_json=plan.needs.indicators_json(), required_history_json=plan.needs.history_json(),
            bar_spec=plan.needs.bar_spec.name, config_doc_hash=doc.doc_hash, entry_level=entry_level,
            entry_time_ms=plan.entry_ms, entry_time_source=plan.entry_source.value, s0_level=plan.s0.level,
            s0_source=plan.s0.source.value, pinned_units=units, strategy_state_json=plan.state.encode(),
            last_decided_bar=frontier.cursor_ms, last_decided_hash=frontier.last_hash,
            decision_seq=1 if span is not None else 0, armed_at_ms=now, updated_at_ms=now)
        async with self._store.transaction() as tx:
            inserted = await tx.insert_position(row)
            if inserted and span is not None:
                await tx.record_decision(self._span_row(row, span, plan.needs.bar_spec, 1, now))
        if not inserted:
            return False
        self.counters.armed += 1
        self._specs[ref] = plan.specs
        self._handles[ref] = plan.handles
        self._live_adapter[ref] = p.adapter_id
        for book in (self._waiting, self._assigned_since, self._unmanaged):
            book.pop(ref, None)
        self._overdue_paged.discard(ref)
        return True

    def _span_row(self, row: ArmPositionRow, span: Span, spec: BarSpec, seq: int, now: int) -> DecisionRow:
        env = stamp(self._arm.envelope)
        return DecisionRow(
            key=row.key, bar_end_ms=span.bar_end_ms, bar_spec=spec.name, event=ArmEvent.POST.value,
            decision_seq=seq, decided_at_ms=now, edge_code=row.edge_code, outcome=span.code.outcome.value,
            category=span.code.category.value, reason=span.reason, advice=False,
            caveats=(SPAN_FIRST_BAR_STALE,) if span.first_bar_stale else (), envelope_version=env.version,
            envelope_params_hash=env.params_hash, strategy_kind=row.strategy_kind,
            strategy_impl_version=row.strategy_impl_version, guards_hash=row.guards_hash,
            range_from_bucket=span.from_bar_ms - spec.timeframe_ms)

    def _not_managed(self, p: PositionDTO, kind: A.Unmanaged, reason: str, now: int) -> None:
        ref = position_ref(p.adapter_id, p.position_id)
        self._unmanaged[ref] = (kind, reason)
        self._waiting.pop(ref, None)
        if kind is A.Unmanaged.UNASSIGNED:
            self._assigned_since.pop(ref, None)
        item = TrackerNotice(kind=NoticeKind.POSITION_NOT_MANAGED, adapter_id=p.adapter_id, account_id=p.account_id,
                             position_id=p.position_id, symbol=p.symbol, side=p.side, reason=reason, at_ms=now)
        keys = self._notified.setdefault(ref, set())
        if item.item_key in keys:
            return
        keys.add(item.item_key)
        self._notices.notice(item)
        if kind is A.Unmanaged.UNSUPPORTED:
            self._pager.page("warning", "ARM: position not managed (UNSUPPORTED)", f"{ref}; reason {reason}")

    def _page_overdue(self, now: int) -> None:
        for ref, since in self._assigned_since.items():
            if ref in self._overdue_paged or ref in self._unmanaged:
                continue
            if now - since >= self._cfg.not_armed_page_after_ms:
                self._overdue_paged.add(ref)
                self._pager.page("warning", "ARM: assigned position not armed after 5 minutes",
                                 f"{ref}; waiting {self._waiting.get(ref, 'unknown')}")

    # ------------------------------------------------------------------------------------------------ changes
    def _notice(self, kind: NoticeKind, row: ArmPositionRow, reason: str, now: int) -> None:
        self._notices.notice(TrackerNotice(kind=kind, adapter_id=row.key.adapter_id, account_id=row.account_id,
                                           position_id=row.key.position_id, symbol=row.symbol, side=row.side,
                                           reason=reason, at_ms=now))

    async def _cas(self, old: ArmPositionRow, new: ArmPositionRow) -> bool:
        async with self._store.transaction() as tx:
            ok = await tx.update_position(PositionCas(row=new, expected_row_version=old.row_version))
        if not ok:
            self.counters.cas_miss += 1
        return ok

    async def _follow(self, row: ArmPositionRow, p: PositionDTO | None, image: AccountImage, armed: set[str],
                      now: int) -> None:
        ref = row.ref
        if p is None or p.status != "OPEN":
            replaced = p is None and any(q.status == "OPEN" and q.symbol == row.symbol and q.side == row.side
                                         and q.position_id not in armed for q in image.positions)
            reason = "replaced" if replaced else ("position_closed" if p is not None else "position_gone")
            t = next_state(ArmState(row.state), Sm11Event.POSITION_CLOSED, prior_state=_prior(row))
            if isinstance(t, Refused):
                return
            if await self._cas(row, replace(row, state=t.state.value, prior_state=None, state_reason=reason,
                                            ended_at_ms=now, updated_at_ms=now)):
                self.counters.ended += 1
                for book in (self._specs, self._handles, self._suspend_paged, self._handle_refs):
                    book.pop(ref, None)
                self._ended[ref] = (row.key.adapter_id, row.account_id)      # never re-armed if it shows up again
                if replaced:
                    self._notice(NoticeKind.REPLACED, row, reason, now)
            return
        detail = effective_stop_detail(p, image.positions, image.orders, at_entry=True)
        self._handles[ref] = 0 if detail is None else len(detail.handles)
        self._handle_refs[ref] = () if detail is None else tuple(HandleRef(h.kind, h.id) for h in detail.handles)
        if _suspended(row):
            last = self._suspend_paged.setdefault(ref, now)
            if now - last >= self._cfg.suspended_repage_ms:
                self._suspend_paged[ref] = now
                self._pager.page("warning", "ARM: position still SUSPENDED", f"{ref}; re-arm with arm rearm")
            return
        params = self._pinned(row)
        if params is None:
            await self._suspend(row, "pinned_version_missing", now)
            return
        if ref not in self._specs:
            specs = self._row_specs(row, params, image)
            if specs is not None:
                self._specs[ref] = specs
        if p.units is None or p.entry_price is None:
            return
        if row.entry_time_source == EntryTimeSource.ESTIMATED.value and await self._upgrade_entry_time(row, p, now):
            return                                     # written (or a CAS miss): the next pass sees the new row
        noted = row.state_reason == A.roll_note(ref, p.entry_price)      # a ROLL_ADJUST already noted (any term)
        change = A.classify_change(
            pinned_units=row.pinned_units, pinned_entry=p.entry_price if noted else row.entry_level, units=p.units,
            entry=p.entry_price,
            fills_cover=A.entry_fills_cover(row.opened_by_client_order_id, image.orders, p.units),
            changed_at_ms=p.broker_updated_at_ms or now)
        if change is A.Change.FILL_PROGRESS:
            fill_ms = row.entry_time_ms if row.entry_time_source != EntryTimeSource.ESTIMATED.value else None
            s0 = None if detail is None else await self._s0(p, detail, fill_ms, now)
            new = replace(row, entry_level=p.entry_price, pinned_units=p.units, state_reason="fill_progress",
                          updated_at_ms=now)
            if s0 is not None:
                new = replace(new, s0_level=s0.level, s0_source=s0.source.value)
            new = replace(new, strategy_state_json=self._repinned_state(row, new.entry_level, new.s0_level))
            if await self._cas(row, new):
                self.counters.repinned += 1
                self._notice(NoticeKind.REPINNED, row, "fill_progress", now)
        elif change is A.Change.SUSPEND:
            await self._suspend(row, "units_increase", now)
        elif change is A.Change.UNITS_DECREASE:
            # §B7: a decrease leaves the management unchanged; only the pinned units follow it down, so a later
            # scale-in (still below the old pin) is an increase. A roll note stays (E is still the original).
            keep = row.state_reason if (row.state_reason or "").startswith(A.ROLL_NOTE_PREFIX) else "units_decrease"
            await self._cas(row, replace(row, pinned_units=p.units, state_reason=keep, updated_at_ms=now))
        elif change is A.Change.ROLL_ADJUST:
            if await self._cas(row, replace(row, state_reason=A.roll_note(ref, p.entry_price), updated_at_ms=now)):
                self._notice(NoticeKind.ROLL_ADJUST, row, "roll_adjust", now)
        elif change is A.Change.ENTRY_CORRECTION:
            state_json = self._repinned_state(row, p.entry_price, row.s0_level)
            if await self._cas(row, replace(row, entry_level=p.entry_price, pinned_units=p.units,
                                            strategy_state_json=state_json, state_reason="entry_corrected",
                                            updated_at_ms=now)):
                self.counters.repinned += 1
                self._notice(NoticeKind.ENTRY_CORRECTED, row, "entry_corrected", now)

    def _history_entry(self, row: ArmPositionRow, now: int) -> EntryTime | None:
        """Go-live gate `entry_time_source` (plan S39b): the broker's execution time of the entry order from its
        trade history (`ExecutionTimePort`: StoneX `ExecutedDateTimeUtc` through the orchestrator's read-only
        `fetch_history`, reconcile REST priority). The read runs as its own task, never inside this pass: a finished
        lookup with a fill time gives `BROKER` (the earliest opening fill when several are listed); otherwise a new
        lookup starts at most every `HISTORY_RETRY_MS`, `HISTORY_MAX_ATTEMPTS` times per position and term. None
        until then (the entry time stays `ESTIMATED`; FirstBarGuard keeps waiting until bar 0's cap). An adapter the
        port does not support (`ExecutionTimePort.supports`: StoneX only) is never looked up."""
        if self._exec_times is None or not self._exec_times.supports(row.key.adapter_id):
            return None                                 # only StoneX history ids can match an entry order
        ref = row.ref
        task = self._history.get(ref)
        if task is not None:
            if not task.done():
                return None
            del self._history[ref]
            try:
                got = task.result()
            except asyncio.CancelledError:
                got = None
            except Exception as e:
                if self._fatal(e):
                    raise
                self.counters.history_errors += 1
                log.info("arm-tracker: history execution time not read for %s (%r)", ref, type(e))
                got = None
            if got is not None and got.fill_ms is not None:
                self.counters.history_upgrades += 1
                return EntryTime(source=EntryTimeSource.BROKER.value, broker_time_ms=got.fill_ms, seq_id=0,
                                 origin="history")
        tries, last = self._history_tries.get(ref, (0, 0))
        if tries >= HISTORY_MAX_ATTEMPTS or (tries and now - last < HISTORY_RETRY_MS):
            return None
        self._history_tries[ref] = (tries + 1, now)
        self.counters.history_lookups += 1
        order_id = row.opened_by_client_order_id or row.key.position_id
        self._history[ref] = asyncio.create_task(self._history_read(
            row.key.adapter_id, order_id, row.entry_time_ms - HISTORY_LOOKBACK_MS, now))
        return None

    async def _history_read(self, adapter_id: str, order_id: str, since_ms: int, until_ms: int) -> HistoryFill:
        assert self._exec_times is not None
        async with asyncio.timeout(HISTORY_TIMEOUT_S):
            return await self._exec_times.execution_time(adapter_id, order_id, since_ms, until_ms)

    def _drop_history(self, ref: str) -> None:
        """Forget a position's history lookup and its try count (bar 0's cap passed, or the row is no longer live):
        a running lookup is cancelled, a finished one's exception consumed (never "Task exception was never
        retrieved")."""
        self._history_tries.pop(ref, None)
        task = self._history.pop(ref, None)
        if task is None:
            return
        if not task.done():
            task.cancel()
        elif not task.cancelled():
            task.exception()

    def close(self) -> None:
        """Cancel the history lookups still running (the term's stop)."""
        for task in self._history.values():
            task.cancel()
        self._history.clear()

    async def _upgrade_entry_time(self, row: ArmPositionRow, p: PositionDTO, now: int) -> bool:
        """§B7: `ESTIMATED → STREAM / BROKER` while bar 0's `decision_cap` (of the pinned bar 0) has not passed. With
        no decision yet and the fill bucket moved, the cursor is set again as at arming (its `armed_late` span, if
        any, written under the position CAS). True when a write was attempted (the caller stops this pass)."""
        spec = BarSpec.parse(row.bar_spec)
        skew = self._grid.skew(row.key.adapter_id)
        bar0_end = spec.bucket_start_ms(row.entry_time_ms) + spec.timeframe_ms
        if now > decision_cap_ms(bar0_end, spec, skew):
            self._drop_history(row.ref)                 # the entry time stays ESTIMATED: no lookup is wanted
            return False
        found = await self._versions.entry_time(row.key.adapter_id, row.key.position_id,
                                                row.opened_by_client_order_id)
        if not A.exact_entry_time(found):
            found = self._history_entry(row, now)
        if not A.exact_entry_time(found):
            return False
        entry_ms, source = A.entry_choice(found, first_seen_ms=row.entry_time_ms)
        new = replace(row, entry_time_ms=entry_ms, entry_time_source=source.value, updated_at_ms=now)
        span: Span | None = None
        undecided = row.decision_seq == 0 and row.pending_event is None
        if undecided and spec.bucket_start_ms(entry_ms) != spec.bucket_start_ms(row.entry_time_ms):
            frontier, span = self._frontier(row.key.adapter_id, row.symbol, spec, entry_ms, now)
            new = replace(new, last_decided_bar=frontier.cursor_ms, last_decided_hash=frontier.last_hash,
                          decision_seq=1 if span is not None else 0)
        if span is None:
            await self._cas(row, new)
            return True
        async with self._store.transaction() as tx:
            wrote = await tx.record_decision(self._span_row(new, span, spec, 1, now),
                                             position=PositionCas(row=new, expected_row_version=row.row_version))
        if wrote.status is not DecisionWriteStatus.INSERTED:
            self.counters.cas_miss += 1
        return True

    def _row_specs(self, row: ArmPositionRow, params: StrategyParams, image: AccountImage) -> PositionSpecs | None:
        """The lease specs of a live row from its stored pin and the current instrument (after a takeover the cache
        is empty)."""
        strategy = self._registry.strategy(params.kind, params.impl_version)
        if strategy is None:
            return None
        needs = self._needs(strategy, params, row.side)
        if isinstance(needs, str):
            return None
        instrument = image.instrument(row.symbol)
        specs = self._resolve(row.key.adapter_id, row.symbol, needs,
                              None if instrument is None else instrument.price_increment)
        return specs if isinstance(specs, PositionSpecs) else None

    async def _suspend(self, row: ArmPositionRow, reason: str, now: int) -> None:
        t = next_state(ArmState(row.state), Sm11Event.SUSPEND, prior_state=_prior(row))
        if isinstance(t, Refused):
            return
        new = replace(row, state=t.state.value, prior_state=None if t.prior_state is None else t.prior_state.value,
                      state_reason=reason, updated_at_ms=now)
        if await self._cas(row, new):
            self.counters.suspended += 1
            self._suspend_paged[row.ref] = now
            self._notice(NoticeKind.SUSPENDED, row, reason, now)
            self._pager.page("warning", "ARM: position SUSPENDED", f"{row.ref}; reason {reason}")

    # ------------------------------------------------------------------------------------------------ requests
    async def _drain_requests(self, now: int) -> None:
        while self._requests:
            req = self._requests.pop(0)
            self.counters.requests_done += 1
            try:
                answer = await self._apply(req, now)
            except Exception as e:
                if self._fatal(e):
                    raise
                self.counters.errors += 1
                log.warning("%s: request %d failed (%r)", TASK, req.request_id, type(e))
                answer = refused("tracker_error")
            self._answers.complete(req.request_id, answer)

    async def _apply(self, req: TrackerRequest, now: int) -> RequestAnswer:
        key = PositionKey(adapter_id=req.adapter_id, position_id=req.position_id)
        for _ in range(CAS_RETRIES):
            row = await self._store.position(key)
            if row is None or row.state in TERMINAL_ARM_STATES:
                return moot("not_armed")
            plan: _RequestPlan | RequestAnswer
            if req.kind == "arm_disarm":
                plan = self._disarm_plan(row, now)
            elif req.kind == "arm_repin":
                plan = await self._repin_plan(req, row, now)
            else:
                plan = await self._rearm_plan(row, now)
            if isinstance(plan, RequestAnswer):
                return plan
            async with self._store.transaction() as tx:
                if plan.span is None:
                    ok = await tx.update_position(PositionCas(row=plan.row, expected_row_version=row.row_version))
                else:
                    wrote = await tx.record_decision(
                        plan.span, position=PositionCas(row=plan.row, expected_row_version=row.row_version))
                    ok = wrote.status is DecisionWriteStatus.INSERTED
            if ok:
                self._specs.pop(row.ref, None)
                if plan.row.state in TERMINAL_ARM_STATES:
                    self._handles.pop(row.ref, None)
                    self._ended[row.ref] = (row.key.adapter_id, row.account_id)
                elif plan.specs is not None:
                    self._specs[row.ref] = plan.specs
                return APPLIED
            self.counters.cas_miss += 1
        return refused("cas_conflict")

    @staticmethod
    def _disarm_plan(row: ArmPositionRow, now: int) -> _RequestPlan | RequestAnswer:
        t = next_state(ArmState(row.state), Sm11Event.LOCAL_DISARM, prior_state=_prior(row))
        if isinstance(t, Refused):
            return moot("not_armed")
        return _RequestPlan(replace(row, state=t.state.value, prior_state=None, state_reason="local_disarm",
                                    ended_at_ms=now, updated_at_ms=now))

    async def _repin_plan(self, req: TrackerRequest, row: ArmPositionRow, now: int) -> _RequestPlan | RequestAnswer:
        doc = self._usable_doc()
        if doc is None:
            return refused("no_config")
        if req.strategy_code is not None and req.strategy_version is not None:
            entry = doc.strategy(req.strategy_code, req.strategy_version)
        else:
            got = resolve(doc, PositionScope(row.key.adapter_id, row.account_id, row.symbol), RESOURCE_STOP_LEVEL, now)
            entry = None if got is None else got.strategy
        if entry is None:
            return refused("strategy_unknown")
        strategy = self._registry.strategy(entry.kind, entry.impl_version)
        if strategy is None:
            return refused("version_not_registered")
        if entry.kind != row.strategy_kind:
            # migrate_state(from_impl_version, state) has no "from kind": another kind's state is never handed over
            # (a kind change is a disarm and a new arming)
            return refused("kind_changed")
        params = self._pin(entry)
        if isinstance(params, str):
            return refused(params)
        needs = self._needs(strategy, params, row.side)
        if isinstance(needs, str):
            return refused(needs)
        if needs.bar_spec.name != row.bar_spec:
            return refused("bar_spec_changed")
        if (entry.code, entry.version, params_json(params), guards_json(params), params.kind,
                params.impl_version) == (row.strategy_code, row.strategy_version, row.strategy_params_json,
                                         row.guards_json, row.strategy_kind, row.strategy_impl_version):
            return moot("already_pinned")
        old = StrategyState.decode(row.strategy_state_json)
        if old is None:
            return refused("state_unreadable")
        try:
            migrated = strategy.migrate_state(row.strategy_impl_version, old)
        except Exception:
            return refused("migrate_failed")
        if isinstance(migrated, Refuse):
            return moot("migrate_refused")
        image = self._images.get((row.key.adapter_id, row.account_id))
        instrument = None if image is None else image.instrument(row.symbol)
        specs = self._resolve(row.key.adapter_id, row.symbol, needs,
                              None if instrument is None else instrument.price_increment)
        if isinstance(specs, str):
            return refused(specs)
        # the new pin's ARMED leases before the CAS, as at arming (a SUSPENDED row holds none)
        if not _suspended(row) and await self._demand_all(specs.all()) is None:
            return refused(A.DEMAND_CEILING)
        return _RequestPlan(replace(
            row, strategy_code=entry.code, strategy_version=entry.version, strategy_kind=params.kind,
            strategy_impl_version=params.impl_version, strategy_params_json=params_json(params),
            guards_json=guards_json(params), guards_hash=params.guards_hash,
            indicator_specs_json=needs.indicators_json(), required_history_json=needs.history_json(),
            config_doc_hash=doc.doc_hash, strategy_state_json=migrated.encode(), state_reason="repinned",
            updated_at_ms=now), specs=specs)

    async def _rearm_plan(self, row: ArmPositionRow, now: int) -> _RequestPlan | RequestAnswer:
        """SUSPENDED -> ARMED after the arming guard passes again (sm11 note for S26): the strategy's
        `arming_requirements` re-checked, cursor as at arming (§B6.6), E, units and S0 re-pinned from the current
        position, and the ARMED leases demanded before the CAS (refused `demand_ceiling`, deviation 10)."""
        if row.state != ArmState.SUSPENDED.value:
            return moot("not_suspended")
        image = self._images.get((row.key.adapter_id, row.account_id))
        if image is None or not image.fresh:
            return refused(A.IMAGE_STALE)
        p = next((q for q in image.positions if q.position_id == row.key.position_id), None)
        if p is None or p.status != "OPEN":
            return moot("position_gone")
        if p.entry_price is None or p.units is None or p.units <= 0:
            return refused(A.ENTRY_UNKNOWN)
        detail = effective_stop_detail(p, image.positions, image.orders, at_entry=True)
        if detail is None:
            return refused(A.NO_STOP)
        doc = self._usable_doc()
        if doc is None:
            return refused(A.NO_CONFIG)
        if resolve(doc, PositionScope(p.adapter_id, p.account_id, p.symbol), RESOURCE_STOP_LEVEL, now) is None:
            return refused("unassigned")
        params = self._pinned(row)
        strategy = None if params is None else self._registry.strategy(params.kind, params.impl_version)
        if params is None or strategy is None:
            return refused("pinned_version_missing")
        t = next_state(ArmState.SUSPENDED, Sm11Event.LOCAL_REARM)
        if isinstance(t, Refused):
            return moot("not_suspended")
        specs = self._row_specs(row, params, image)
        if specs is None:
            return refused("specs_unresolved")
        if self._scope_failed(p.adapter_id, specs, params, position_ref(p.adapter_id, p.position_id)):
            t = next_state(t.state, Sm11Event.SCOPE_FAILED)
            if isinstance(t, Refused):
                return refused("scope_failed")
        fill_ms = row.entry_time_ms if row.entry_time_source != EntryTimeSource.ESTIMATED.value else None
        s0 = await self._s0(p, detail, fill_ms, now)
        try:
            req = strategy.arming_requirements(params)
        except Exception:
            return refused(A.PIN_REFUSED)
        if req.s0 is S0Requirement.AT_FILL and s0.source is not S0Source.AT_FILL:
            return refused(A.S0_NOT_AT_FILL)
        if req.entry_time is EntryTimeRequirement.EXACT and row.entry_time_source == EntryTimeSource.ESTIMATED.value:
            return refused(A.ENTRY_TIME_NOT_EXACT)
        if await self._demand_all(specs.all()) is None:
            return refused(A.DEMAND_CEILING)
        spec = BarSpec.parse(row.bar_spec)
        previous = Frontier(row.last_decided_bar if row.last_decided_bar is not None
                            else spec.bucket_start_ms(row.entry_time_ms), row.last_decided_hash)
        frontier, span = self._frontier(p.adapter_id, p.symbol, spec, row.entry_time_ms, now, previous)
        seq = row.decision_seq + 1 if span is not None else row.decision_seq
        new = replace(row, state=t.state.value, prior_state=None if t.prior_state is None else t.prior_state.value,
                      state_reason="local_rearm", entry_level=p.entry_price, pinned_units=p.units,
                      s0_level=s0.level, s0_source=s0.source.value,
                      strategy_state_json=self._repinned_state(row, p.entry_price, s0.level),
                      last_decided_bar=frontier.cursor_ms,
                      last_decided_hash=frontier.last_hash, decision_seq=seq, pending_event=None,
                      pending_bar_end_ms=None, updated_at_ms=now)
        span_row = None if span is None else self._span_row(new, span, spec, seq, now)
        return _RequestPlan(new, span_row, specs)

    # ------------------------------------------------------------------------------------------------ leases
    async def _renew_leases(self, now: int, doc: ValidDocument | None) -> None:
        """Renew every 15 s, and at once when the md generation changed (`NO_BOOT`, a reset): the store lost or
        rebuilt its leases, so the whole set is asserted again."""
        gen = await self._md.md_generation()
        changed = isinstance(gen, MdGeneration) and self._generation is not None and gen != self._generation
        if isinstance(gen, MdGeneration):
            self._generation = gen
        due = self._last_lease_ms is None or now - self._last_lease_ms >= self._cfg.lease_every_ms
        if not (due or changed):
            return
        want: dict[str, LeaseClass] = {}
        for row in await self._store.live_positions():
            if _suspended(row):
                continue
            specs = self._specs.get(row.ref)
            image = self._images.get((row.key.adapter_id, row.account_id))
            if specs is None and image is not None:       # e.g. a takeover while the account's image is stale
                params = self._pinned(row)
                specs = None if params is None else self._row_specs(row, params, image)
                if specs is not None:
                    self._specs[row.ref] = specs
            if specs is None:
                continue
            for spec in specs.all(past_guards=self._past_guards(row, now)):
                want[self._md.define(spec)] = LeaseClass.ARMED
        held: dict[str, set[str]] = {}
        bar_adapters: set[str] = set()
        for image in self._images.values():
            if self._grid.bar_sides(image.adapter_id):
                bar_adapters.add(image.adapter_id)
            held.setdefault(image.adapter_id, set()).update(q.symbol for q in image.positions if q.status == "OPEN")
        ticks: dict[str, set[str]] = {}
        for adapter_id, symbol in self._cfg.tick_symbols:
            ticks.setdefault(adapter_id, set()).add(symbol)
            if self._grid.bar_sides(adapter_id):
                bar_adapters.add(adapter_id)
        for adapter_id, symbols in A.prewarm_symbols(doc, now, held=held, tick_symbols=ticks,
                                                     bar_adapters=bar_adapters).items():
            for symbol in symbols:
                for side in (QuoteSide.BID, QuoteSide.ASK):
                    sid = self._md.define(SeriesId(adapter_id, symbol, M15, side, Source.BROKER))
                    want.setdefault(sid, LeaseClass.PREWARM)
        for sid in sorted(want):
            if not await self._md.demand(sid, CONSUMER, want[sid]):
                self.counters.lease_refused += 1
        for sid in sorted(set(self._leased) - set(want)):
            await self._md.release(sid, CONSUMER)
        self._leased = want
        self._last_lease_ms = now
        self.counters.lease_renewals += 1
        if changed:
            self.counters.lease_reasserts += 1


__all__ = [
    "APPLIED", "AcceptedConfigPort", "AccountImage", "AnswerStatus", "ArmTracker", "BarGridPort", "CONSUMER",
    "FailedScopeView", "Needs", "NoticeKind", "PositionSpecs", "REQUEST_KINDS", "RequestAnswer", "RequestAnswerPort",
    "SPAN_FIRST_BAR_STALE", "TASK", "TrackerCounters", "TrackerNotice", "TrackerNoticePort", "TrackerRequest",
    "TrackerSettings", "TrackerSnapshotPort", "FIRST_BAR_SCAN_BUCKETS", "FIRST_BAR_SCAN_MS", "GUARD_SCAN_BARS",
    "first_bar_end", "guard_horizon_end", "guards_apply", "guards_json", "moot", "open_bar_ends_after", "params_json",
    "past_guards", "refused", "version_scopes", "REPIN_STALE_FIELD", "state_after_entry_repin",
]
