"""The stop at bar end (`S_at_T`), handle roles and `pre_basis` (design §B6.3 (7a), §B6.4, PR8; plan S21,
R11-M04, R11-M06).

**`S_at_T`** (`stop_at_bar_end`): the trigger at every bar-end decision applies the definition of `S` (§B6.1: the
`stop_loss` field first, else the most distant of the stop children that together cover the position's units) to each
handle's latest version with broker time at or before `T`. Only these versions **count**: `APPLIED` (never
`DROPPED_STALE`), origin `stream`, `stop_read`, `snapshot` or `dlq`, and `broker_time_source = BROKER` with a real
broker time (`event_log.broker_time_ms`). A `history` version never counts: StoneX history keeps only the last level,
stamped at placement, so it can show a level the stop did not have at `T`. Per handle:

* the newest **candidate** (an `APPLIED` version with a broker time at or before `T`, any origin) counts → its level,
  basis `STREAM` (origin `stream`) or `READ` (a REST read: `stop_read`, `snapshot`, `dlq`);
* the newest candidate does not count (a `history` or watermark-labelled version) → the newest earlier counting
  version, basis `PRIOR_VERSION`; with none, the handle is unresolved. `event_log` stores a watermark-labelled
  version with `broker_time_ms` NULL, so an `APPLIED` version with no broker time that is newer by `seq` than the
  newest counting candidate is taken as such a non-counting newest candidate;
* **direction of the newer version** (round 14, plan S38g): the newer non-counting version may show a level the
  handle held at `T` (a loosened amend seen only through an untimed REST row, or a history row). Its level is order
  state and is read with it; the handle's level is then the **farther from entry** of the prior counting level and
  each newer non-counting level, so a loosened stop is never tested at a smaller risk (the `pre_basis` principle).
  A newer non-counting version that still holds the handle but carries no level (the `stop_loss` field cleared,
  say) cannot be resolved against the prior level: the handle is unresolved (`DATA_UNAVAILABLE(stop_at_bar_end)`).
  One that ended the handle with no level takes no part; history's ended versions carry the last level and compare
  like any other. Without the position's side and entry the direction is unknown, so a differing level is
  unresolved too;
* no candidate at all → the handle did not exist at `T` and takes no part.

An unresolved handle gives `StopUnavailable(stop_at_bar_end)` only when it could change `S` or the cover: a
`POSITION_FIELD` handle always; a child only when no `POSITION_FIELD` resolved and it is not a LIMIT child (its kind
taken from any of its versions).

A version that ends the handle (cancelled, filled, the `stop_loss` field cleared: `live = False` or no level) means
the handle held no stop at `T`. With no handle left at all, or children that do not cover the units at `T` (the same
coverage rule as Oversee's `missing_stop_loss`), the answer is `StopUnavailable(stop_at_bar_end)`. A `stop_read`
version changed after `T` gives the caveat `stop_changed_after_bar_end` (PR9: the handle keeps its newest earlier
version). The decision's `stop_basis` is the weakest basis among the handles used (`PRIOR_VERSION` < `READ` <
`STREAM`).

**Handle roles** (`handle_roles`): `pre_basis` and the same-bar PRE levels are keyed by the handle's role, never by
order id, so a StoneX cancel / replace that gives the stop child a new id keeps the entry: `POSITION_FIELD`, or
`CHILD_ORDER:<rank>` with rank 0 the child farthest from entry.

**`PreBasis`** holds the handle levels a PRE used (order state, by role) and `S_pre`, their effective stop. The
trigger takes, of `S_pre` and `S_at_T`, the one farther from entry (`farther_from_entry`; basis `AT_PRE` when that is
`S_pre`), so a move made on an unconfirmed heads-up never makes the bar-end test easier and a loosened stop is never
tested at a smaller risk.

Pure: no I/O, no clock. Levels are order state; they are kept out of every `repr` all the same.
"""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from decimal import Context, Decimal
from enum import Enum
from typing import Final

from src.dto import BrokerTimeSource, OrderKind, Origin, Side
from src.marketdata.ports import HandleRef
from src.riskmgr.outcomes import STOP_AT_BAR_END
from src.riskmgr.types import StopBasis, sigma

COUNTING_ORIGINS: Final = frozenset({"stream", "stop_read", "snapshot", "dlq"})
"""The PR8 origins whose versions may give `S_at_T` (never `history`)."""
STOP_CHANGED_AFTER_BAR_END: Final = "stop_changed_after_bar_end"
POSITION_FIELD_ROLE: Final = "POSITION_FIELD"
CHILD_ROLE_PREFIX: Final = "CHILD_ORDER:"
STOP_CONTEXT: Final = Context(prec=40)
"""Comparisons of order-state levels run under this fixed context, never the ambient one."""

_BASIS_RANK: Final = {StopBasis.PRIOR_VERSION: 0, StopBasis.READ: 1, StopBasis.STREAM: 2}


class VersionStatus(str, Enum):
    """The `event_log` status of a version: only `APPLIED` versions are candidates."""

    APPLIED = "APPLIED"
    DROPPED_STALE = "DROPPED_STALE"


@dataclass(frozen=True, kw_only=True)
class StopVersion:
    """One version of one stop handle as `event_log` records it (PR8). `level` is the stop the handle held in this
    version (None: no stop, e.g. a cleared `stop_loss` field); `live` is False once the handle has ended (a terminal
    child, a closed position); `units` the remaining units the handle covers; `seq` orders versions with the same
    broker time (the version's `seq_id`)."""

    handle: HandleRef
    level: Decimal | None = field(repr=False)
    live: bool = True
    units: Decimal | None = field(default=None, repr=False)
    order_kind: OrderKind | None = None
    origin: Origin | None = None
    broker_time_source: BrokerTimeSource | None = None
    broker_time_ms: int | None = None
    status: VersionStatus = VersionStatus.APPLIED
    seq: int = 0

    def candidate(self, bar_end_ms: int) -> bool:
        """An `APPLIED` version with a broker time at or before `T` (any origin)."""
        return (self.status is VersionStatus.APPLIED and self.broker_time_ms is not None
                and self.broker_time_ms <= bar_end_ms)

    @property
    def counts(self) -> bool:
        """`APPLIED`, a counting origin and a real broker time (§B6.4, R11-M06)."""
        return (self.status is VersionStatus.APPLIED and self.origin in COUNTING_ORIGINS
                and self.broker_time_source == "BROKER" and self.broker_time_ms is not None)


@dataclass(frozen=True)
class StopUnavailable:
    """`DATA_UNAVAILABLE(reason)` for the stop at bar end (reason a code)."""

    reason: str = STOP_AT_BAR_END


@dataclass(frozen=True)
class HandleAtT:
    """One handle's level at `T` and the basis it came from."""

    handle: HandleRef
    level: Decimal = field(repr=False)
    basis: StopBasis
    units: Decimal | None = field(default=None, repr=False)
    order_kind: OrderKind | None = None


@dataclass(frozen=True)
class StopAtT:
    """`S_at_T`: the level, its basis, the handles it was built from (with their roles) and caveats."""

    level: Decimal = field(repr=False)
    basis: StopBasis
    handles: tuple[HandleAtT, ...] = ()
    caveats: tuple[str, ...] = ()


# ---------------------------------------------------------------------------------------------------- per handle
def _order_key(v: StopVersion) -> tuple[int, int]:
    return (v.broker_time_ms if v.broker_time_ms is not None else -1, v.seq)


def _untimed_newer(versions: Sequence[StopVersion], newest_counting: StopVersion | None) -> list[StopVersion]:
    """The `APPLIED` versions with no broker time (a watermark-labelled read: `event_log.broker_time_ms` is NULL
    unless `broker_time_source` is BROKER) newer by `seq` than the newest counting candidate. Their broker time is
    unknown, so each is taken as a non-counting newest candidate: it may show a level the handle held at `T`."""
    floor = newest_counting.seq if newest_counting is not None else None
    return [v for v in versions if v.status is VersionStatus.APPLIED and v.broker_time_ms is None
            and (floor is None or v.seq > floor)]


def _newer_noncounting(candidates: Sequence[StopVersion], newest_counting: StopVersion | None,
                       untimed: Sequence[StopVersion]) -> list[StopVersion]:
    """Every version newer than the newest counting candidate that does not count: the non-counting candidates
    after it (history, unlabelled) and the untimed ones (watermark)."""
    floor = _order_key(newest_counting) if newest_counting is not None else None
    later = [v for v in candidates if not v.counts and (floor is None or _order_key(v) > floor)]
    return [*later, *untimed]


def handle_at_bar_end(versions: Sequence[StopVersion], bar_end_ms: int, *, side: Side | None = None,
                      entry: Decimal | None = None) -> HandleAtT | StopUnavailable | None:
    """One handle's state at `T` from its versions (any order): its level and basis, None when it held no stop at
    `T` (no candidate, or the counting version ends it), or `StopUnavailable` when its newest candidate does not
    count and no earlier version does, or when a newer non-counting version cannot be resolved against the prior
    level (module docstring). A version with no broker time (watermark) newer by `seq` than the newest counting
    candidate is a non-counting newest candidate (`_untimed_newer`). `side` / `entry`: the position's, for the
    direction rule (round 14): the farthest from entry of the prior counting level and every newer non-counting one
    (the reader returns all of them, bounded: `src/db/arm_versions.py`). A newer non-counting version that **ends**
    the handle with no level takes no part (accepted, design §B6.4): its time is not when the stop ended (StoneX
    history stamps the last version at placement; an untimed REST row is read after the fact), so it cannot show the
    stop was gone at `T`; an ended version that carries a level still takes part in the direction rule."""
    candidates = sorted((v for v in versions if v.candidate(bar_end_ms)), key=_order_key)
    counting = [v for v in candidates if v.counts]
    newest_counting = counting[-1] if counting else None
    untimed = _untimed_newer(versions, newest_counting)
    if not candidates and not untimed:
        return None
    if candidates and candidates[-1].counts and not untimed:
        chosen = candidates[-1]
        basis = StopBasis.STREAM if chosen.origin == "stream" else StopBasis.READ
        if not chosen.live or chosen.level is None:
            return None
        return HandleAtT(chosen.handle, chosen.level, basis, chosen.units, chosen.order_kind)
    if newest_counting is None:
        return StopUnavailable()
    chosen = newest_counting
    if not chosen.live or chosen.level is None:
        return None
    level = chosen.level
    for newer in _newer_noncounting(candidates, newest_counting, untimed):
        if newer.level is None:
            if newer.live:
                return StopUnavailable()       # the handle is held at an unknown level: cannot be resolved
            continue                           # ended with no level: takes no part
        if newer.level == level:
            continue
        if side is None or entry is None:
            return StopUnavailable()           # the direction is unknown: never guess the smaller risk
        if farther_from_entry(side, entry, newer.level, level):
            level = newer.level                # a loosened stop is never tested at a smaller risk
    return HandleAtT(chosen.handle, level, StopBasis.PRIOR_VERSION, chosen.units, chosen.order_kind)


def _is_limit(versions: Sequence[StopVersion]) -> bool:
    """A LIMIT child (kind taken from any of its versions): never part of `S`, never part of the cover."""
    return any(v.order_kind == "LIMIT" for v in versions)


def _child_counts(side: Side, entry: Decimal, h: HandleAtT) -> bool:
    """PR1 kind rules for a filled position: STOP at any price, LIMIT never, None only strictly on the loss side."""
    if h.order_kind == "STOP":
        return True
    if h.order_kind == "LIMIT":
        return False
    return h.level < entry if side == "BUY" else h.level > entry


def stop_at_bar_end(versions: Iterable[StopVersion], *, side: Side, entry: Decimal, units_to_cover: Decimal,
                    bar_end_ms: int) -> StopAtT | StopUnavailable:
    """`S_at_T` over every version of every handle of the position (see the module docstring)."""
    by_handle: dict[HandleRef, list[StopVersion]] = {}
    for v in versions:
        by_handle.setdefault(v.handle, []).append(v)
    at_t: list[HandleAtT] = []
    unresolved: list[HandleRef] = []
    for handle in sorted(by_handle, key=lambda h: (h.kind, h.id)):
        got = handle_at_bar_end(by_handle[handle], bar_end_ms, side=side, entry=entry)
        if isinstance(got, StopUnavailable):
            unresolved.append(handle)
        elif got is not None:
            at_t.append(got)
    # An unresolved handle matters only when it could change `S` or the cover: a POSITION_FIELD always (it would
    # come first); a child only when no POSITION_FIELD resolved and it is not a LIMIT child.
    if any(h.kind == "POSITION_FIELD" for h in unresolved):
        return StopUnavailable()
    fields = [h for h in at_t if h.handle.kind == "POSITION_FIELD"]
    if not fields and any(h.kind == "CHILD_ORDER" and not _is_limit(by_handle[h]) for h in unresolved):
        return StopUnavailable()
    if fields:
        used: list[HandleAtT] = fields[:1]
        level = used[0].level
    else:
        used = [h for h in at_t if h.handle.kind == "CHILD_ORDER" and _child_counts(side, entry, h)]
        covered = sum((h.units for h in used if h.units is not None), Decimal(0))
        if not used or any(h.units is None for h in used) or covered < units_to_cover:
            return StopUnavailable()
        level = min(h.level for h in used) if side == "BUY" else max(h.level for h in used)
    basis = min((h.basis for h in used), key=lambda b: _BASIS_RANK[b])
    changed = any(v.counts and v.origin == "stop_read" and v.broker_time_ms is not None
                  and v.broker_time_ms > bar_end_ms for h in used for v in by_handle[h.handle])
    return StopAtT(level, basis, tuple(used), (STOP_CHANGED_AFTER_BAR_END,) if changed else ())


# ---------------------------------------------------------------------------------------------------- roles
def handle_roles(side: Side, entry: Decimal,
                 handles: Iterable[tuple[HandleRef, Decimal]]) -> tuple[tuple[str, Decimal], ...]:
    """`(role, level)` per handle: `POSITION_FIELD`, or `CHILD_ORDER:<rank>` ranked by distance from entry (rank 0
    the farthest; equal levels in any order, since their levels are equal). Never an order id."""
    c = STOP_CONTEXT
    sig = Decimal(sigma(side))
    out: list[tuple[str, Decimal]] = [(POSITION_FIELD_ROLE, lv) for h, lv in handles if h.kind == "POSITION_FIELD"]
    children = sorted((lv for h, lv in handles if h.kind == "CHILD_ORDER"),
                      key=lambda lv: c.multiply(sig, c.subtract(entry, lv)), reverse=True)
    out.extend((f"{CHILD_ROLE_PREFIX}{n}", lv) for n, lv in enumerate(children))
    return tuple(out)


@dataclass(frozen=True)
class PreBasis:
    """The handle levels a PRE used, keyed by role (order state), and the bar it was set on (`pre_basis_bar`).
    `s_pre` is their effective stop: the `POSITION_FIELD` level, else the child farthest from entry (rank 0)."""

    levels: tuple[tuple[str, Decimal], ...] = field(repr=False)
    bar_end_ms: int

    def __post_init__(self) -> None:
        roles = [r for r, _ in self.levels]
        if not roles or len(set(roles)) != len(roles):
            raise ValueError("a pre_basis names at least one role, each once")
        if not all(r == POSITION_FIELD_ROLE or (r.startswith(CHILD_ROLE_PREFIX)
                                                 and r[len(CHILD_ROLE_PREFIX):].isdigit()) for r in roles):
            raise ValueError("a pre_basis role is POSITION_FIELD or CHILD_ORDER:<rank>")
        if not all(lv.is_finite() for _, lv in self.levels):
            raise ValueError("a pre_basis level is finite")

    @classmethod
    def of(cls, side: Side, entry: Decimal, handles: Iterable[tuple[HandleRef, Decimal]],
           bar_end_ms: int) -> PreBasis:
        return cls(handle_roles(side, entry, handles), bar_end_ms)

    def level(self, role: str) -> Decimal | None:
        for r, lv in self.levels:
            if r == role:
                return lv
        return None

    @property
    def s_pre(self) -> Decimal:
        field_level = self.level(POSITION_FIELD_ROLE)
        if field_level is not None:
            return field_level
        child = self.level(f"{CHILD_ROLE_PREFIX}0")
        if child is None:   # impossible after __post_init__ unless only odd ranks: take any level
            return self.levels[0][1]
        return child


def farther_from_entry(side: Side, entry: Decimal, a: Decimal, b: Decimal) -> bool:
    """`a` is strictly farther from entry than `b` (a larger risk `σ·(E − level)`)."""
    c = STOP_CONTEXT
    sig = Decimal(sigma(side))
    return c.multiply(sig, c.subtract(entry, a)) > c.multiply(sig, c.subtract(entry, b))


@dataclass(frozen=True)
class TriggerBasis:
    """The stop basis of one trigger test (`trigger_basis`): `held` the basis in force (its `S_pre` is farther from
    entry than `S`; None when `S` itself is tested), `stored` the stored `pre_basis` after the loosen-back rule, and
    `basis` the decision's `stop_basis` (`AT_PRE` while held)."""

    held: PreBasis | None
    stored: PreBasis | None
    basis: StopBasis | None

    @property
    def s_pre(self) -> Decimal | None:
        return None if self.held is None else self.held.s_pre


def trigger_basis(side: Side, entry: Decimal, stop: Decimal, stop_basis: StopBasis | None,
                  stored: PreBasis | None, this_bar_pre: PreBasis | None = None) -> TriggerBasis:
    """**The one stop-basis function** of PRE and every bar-end event (round 12, plan S38c): the trigger tests
    against the farther from entry of `S_pre` (while a basis is held: the stored `pre_basis`, and at a bar end this
    bar's PRE levels) and `S` (`S_at_T` at a bar end, the current stop at PRE). A stored basis is dropped once `S` is
    at or beyond its `S_pre` away from entry (the stop was loosened back past it: the larger risk applies)."""
    if stored is not None and not farther_from_entry(side, entry, stored.s_pre, stop):
        stored = None
    held = farther_basis(side, entry, stored, this_bar_pre)
    if held is not None and not farther_from_entry(side, entry, held.s_pre, stop):
        held = None
    return TriggerBasis(held, stored, StopBasis.AT_PRE if held is not None else stop_basis)


def farther_basis(side: Side, entry: Decimal, a: PreBasis | None, b: PreBasis | None) -> PreBasis | None:
    """Of two bases, the one whose `S_pre` is farther from entry (the earlier one on a tie): a newer
    PRE_NOT_CONFIRMED never makes the stored basis easier."""
    if a is None or b is None:
        return a if b is None else b
    return b if farther_from_entry(side, entry, b.s_pre, a.s_pre) else a


__all__ = [
    "COUNTING_ORIGINS", "STOP_CHANGED_AFTER_BAR_END", "POSITION_FIELD_ROLE", "CHILD_ROLE_PREFIX", "VersionStatus",
    "StopVersion", "StopUnavailable", "HandleAtT", "StopAtT", "handle_at_bar_end", "stop_at_bar_end",
    "handle_roles", "PreBasis", "farther_from_entry", "farther_basis", "TriggerBasis", "trigger_basis",
]
