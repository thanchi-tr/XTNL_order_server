"""Pure arming rules of the arm-tracker (design §B7, §B11, §B4 arm-tracker, §B6.6; plan S26).

Everything here is a deterministic function of its arguments: no I/O, no clock, no store. The task that applies these
rules is `src/riskmgr/tracker.py`.

* **Phase 1 single edge** (§B11): every arming writes `edge_code = XTNL_ARM_EDGE` and `attribution_source =
  SINGLE_EDGE_DEFAULT`. An accepted document with an enabled assignment naming an edge other than the local one is
  "a config enabling a second edge" and is refused (`second_edges`): nothing new arms under it (positions already armed
  keep their pins, §B12), and the tracker pages once.
* **Entry time** (§B7, the fill, never the placement): `STREAM` / `BROKER` from the `event_log` reader, else
  `ESTIMATED` at the engine's first sight of the position. The position's broker update time is not used: on StoneX
  REST it is `LastChangedDateTimeUTC`, which may be the placement of a stop entry (§B7, PR3). The tracker upgrades an
  `ESTIMATED` time to `STREAM` / `BROKER` until bar 0's `decision_cap`.
* **S0** (§B7, `s0_pick`): the current effective stop level. `AT_FILL` only when every handle's level comes from a live
  **stream** version with broker time at most fill + 10 s and that version is still the handle's newest counting one
  (no later amendment); a handle whose newest version came from history (StoneX keeps only the last level) gives
  `HISTORY_LAST_LEVEL`; otherwise `FIRST_SEEN`.
* **Position changes** (§B7, `classify_change`): a units increase that the entry order's fills explain is fill
  progress (E and S0 re-pinned, bar 0 kept; `entry_fills_cover`: the order is still working and the units are within
  its size, or it is terminal and its filled units cover them, so the final fill counts too); any other units increase
  suspends; an entry change with units unchanged within ±2 min of the 17:00 New York roll is `ROLL_ADJUST` (original E
  kept, noted durably by `roll_note`); any other entry change is a correction (E re-pinned); a units decrease alone
  lowers the pinned units (nothing else changes), so a later scale-in is still an increase.
* **Pre-warm set** (§B4): explicit symbols of the accepted assignments in force, plus, for wildcard scopes, the held
  symbols and `XTNL_TICK_SYMBOLS` on bar-capable adapters; never "all instruments"; at most 8 per adapter.

Reasons are fixed codes; nothing here holds or returns a value other than order-state levels (entry, stop), which are
kept out of every `repr`.
"""
from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Final
from zoneinfo import ZoneInfo

from src.armconfig.resolver import assignments_in_force
from src.armconfig.validate import ValidDocument
from src.db.arm_ports import EntryTime, HandleVersionRow
from src.dto import OrderDTO
from src.oversee.risk import StopDetail
from src.riskmgr.types import EntryTimeSource, S0Source

ATTRIBUTION_SINGLE_EDGE: Final = "SINGLE_EDGE_DEFAULT"
AT_FILL_WINDOW_MS: Final = 10_000
ROLL_WINDOW_MS: Final = 120_000
NEW_YORK: Final = ZoneInfo("America/New_York")
ROLL_HOUR: Final = 17
PREWARM_CAP: Final = 8
HISTORY_ORIGIN: Final = "history"
STREAM_ORIGIN: Final = "stream"


class Unmanaged(str, Enum):
    """Why an OPEN position is not managed (§B7): no assignment, or one that cannot be served."""

    UNASSIGNED = "UNASSIGNED"
    UNSUPPORTED = "UNSUPPORTED"


# UNASSIGNED reason
NO_ASSIGNMENT: Final = "no_assignment"
# UNSUPPORTED reasons (§B7, R11-M20)
NO_BROKER_BARS: Final = "no_broker_bars"
ASSET_CLASS: Final = "asset_class"
DEMAND_CEILING: Final = "demand_ceiling"
STOP_READ_BUDGET: Final = "stop_read_budget"
S0_NOT_AT_FILL: Final = "s0_not_at_fill"
ENTRY_TIME_NOT_EXACT: Final = "entry_time_not_exact"
PIN_REFUSED: Final = "pin_refused"
NO_TIMING_ROW: Final = "no_timing_row"
INCREMENT_UNKNOWN: Final = "increment_unknown"
BAR_SPECS: Final = "bar_specs"
INIT_FAILED: Final = "init_state_failed"
UNSUPPORTED_REASONS: Final = frozenset({
    NO_BROKER_BARS, ASSET_CLASS, DEMAND_CEILING, STOP_READ_BUDGET, S0_NOT_AT_FILL, ENTRY_TIME_NOT_EXACT, PIN_REFUSED,
    NO_TIMING_ROW, INCREMENT_UNKNOWN, BAR_SPECS, INIT_FAILED,
})
# Waiting reasons: the position stays DISCOVERED (in memory only) and is looked at again on the next snapshot.
IMAGE_STALE: Final = "image_stale"
ENTRY_UNKNOWN: Final = "entry_unknown"
NO_STOP: Final = "no_stop"
NO_CONFIG: Final = "no_config"
SECOND_EDGE: Final = "second_edge"


# ---------------------------------------------------------------------------------------------------- edge (§B11)
def second_edges(doc: ValidDocument, edge_code: str) -> tuple[str, ...]:
    """The edge codes other than the local one that an enabled assignment of the document names (in force or
    future-dated), sorted. Non-empty = the document enables a second edge and is refused for arming (Phase 1)."""
    return tuple(sorted({a.edge_code for a in doc.assignments
                         if a.enabled and a.edge_code is not None and a.edge_code != edge_code}))


# ---------------------------------------------------------------------------------------------------- entry time
def entry_choice(found: EntryTime | None, *, first_seen_ms: int) -> tuple[int, EntryTimeSource]:
    """The pinned entry time and its source: the reader's fill time (`STREAM` / `BROKER`), else `ESTIMATED` at the
    engine's first sight of the position. Never the position's broker update time: on StoneX REST that is
    `LastChangedDateTimeUTC`, which can be a stop entry's placement hours before its fill (§B7, PR3)."""
    if found is not None and found.source in (EntryTimeSource.STREAM.value, EntryTimeSource.BROKER.value):
        return found.broker_time_ms, EntryTimeSource(found.source)
    return first_seen_ms, EntryTimeSource.ESTIMATED


def exact_entry_time(found: EntryTime | None) -> bool:
    """The reader found a fill time (`STREAM` / `BROKER`): an `ESTIMATED` pin may be upgraded to it."""
    return found is not None and found.source in (EntryTimeSource.STREAM.value, EntryTimeSource.BROKER.value)


# ---------------------------------------------------------------------------------------------------- S0
@dataclass(frozen=True)
class S0Pick:
    level: Decimal = field(repr=False)
    source: S0Source


def _handle_rows(rows: Iterable[HandleVersionRow], kind: str, handle_id: str) -> list[HandleVersionRow]:
    return [r for r in rows if r.handle_kind == kind and r.handle_id == handle_id]


def _newest_counting(rows: Sequence[HandleVersionRow]) -> HandleVersionRow | None:
    counting = [r for r in rows if r.counts and r.level is not None]
    return max(counting, key=lambda r: r.seq_id) if counting else None


def s0_pick(detail: StopDetail, *, fill_known: bool, at_fill: Sequence[HandleVersionRow],
            now: Sequence[HandleVersionRow]) -> S0Pick:
    """S0 = the effective stop level now; its source by §B7. `at_fill`: the `S_at_T` reads at fill + 10 s (empty when
    the fill time is not known); `now`: the same reads at arming time."""
    history = False
    at_fill_ok = fill_known and bool(detail.handles)
    for h in detail.handles:
        cur = _handle_rows(now, h.kind, h.id)
        newest_now = _newest_counting(cur)
        newest_any = max(cur, key=lambda r: r.seq_id) if cur else None
        if newest_any is not None and newest_any.origin == HISTORY_ORIGIN \
                and (newest_now is None or newest_any.seq_id > newest_now.seq_id):
            history = True
        then = _newest_counting(_handle_rows(at_fill, h.kind, h.id))
        if then is None or then.origin != STREAM_ORIGIN or then.level != h.level \
                or newest_now is None or newest_now.seq_id != then.seq_id:
            at_fill_ok = False
    if history:
        return S0Pick(detail.level, S0Source.HISTORY_LAST_LEVEL)
    return S0Pick(detail.level, S0Source.AT_FILL if at_fill_ok else S0Source.FIRST_SEEN)


# ---------------------------------------------------------------------------------------------------- changes
class Change(str, Enum):
    NONE = "NONE"
    UNITS_DECREASE = "UNITS_DECREASE"
    FILL_PROGRESS = "FILL_PROGRESS"
    SUSPEND = "SUSPEND"
    ROLL_ADJUST = "ROLL_ADJUST"
    ENTRY_CORRECTION = "ENTRY_CORRECTION"


def near_ny_roll(at_ms: int, window_ms: int = ROLL_WINDOW_MS) -> bool:
    """`at_ms` lies within `window_ms` of a 17:00 America/New_York roll (DST handled by the zone)."""
    local = datetime.fromtimestamp(at_ms // 1000, tz=NEW_YORK)
    for offset in (-1, 0, 1):
        day = (local + timedelta(days=offset)).date()
        roll = datetime(day.year, day.month, day.day, ROLL_HOUR, tzinfo=NEW_YORK)
        if abs(int(roll.timestamp()) * 1000 - at_ms) <= window_ms:
            return True
    return False


def entry_fills_cover(entry_order_id: str | None, orders: Iterable[OrderDTO], units: Decimal) -> bool:
    """The position's entry order (`broker_order_id == entry_order_id`, not reduce-only) explains `units`: while it
    is still working, `units` is within its size; once terminal (FILLED, or a last partial fill with the remainder
    CANCELLED, possibly seen in one stale-then-fresh image), its filled units (`ordered - remaining`) cover `units`.
    No such order in the image (Pepperstone: cTrader positions carry no entry order id) is False."""
    if not entry_order_id:
        return False
    for o in orders:
        if o.broker_order_id != entry_order_id or o.is_reduce_only:
            continue
        if not o.status.terminal:
            return o.ordered_units is None or units <= o.ordered_units
        if o.ordered_units is None or o.remaining_units is None:
            return False
        return o.ordered_units - o.remaining_units >= units
    return False


def classify_change(*, pinned_units: Decimal, pinned_entry: Decimal, units: Decimal, entry: Decimal,
                    fills_cover: bool, changed_at_ms: int) -> Change:
    """§B7 Changes. `fills_cover`: `entry_fills_cover` for the new units; `changed_at_ms`: the broker time of the
    position's newest version (the roll window is judged on it). `pinned_entry` is the entry already accounted for
    (the pinned E, or the broker E a durable `roll_note` names). The entry is compared whatever the units did, so a
    decrease never hides an entry change; a decrease alone is `UNITS_DECREASE` (the tracker lowers the pinned units,
    so a later scale-in is still an increase)."""
    if units > pinned_units:
        return Change.FILL_PROGRESS if fills_cover else Change.SUSPEND
    if entry != pinned_entry:
        if units == pinned_units and near_ny_roll(changed_at_ms):
            return Change.ROLL_ADJUST
        return Change.ENTRY_CORRECTION
    return Change.UNITS_DECREASE if units < pinned_units else Change.NONE


ROLL_NOTE_PREFIX: Final = "roll_adjust_"


def roll_note(ref: str, entry: Decimal) -> str:
    """The durable `state_reason` of a noted `ROLL_ADJUST` (§B7, original E kept): a fingerprint of the position and
    the broker's adjusted entry, so a new term (takeover) knows that this entry was already noted and neither
    re-pins it as a correction nor sends the notice again. Opaque (no level in it); fits the 0017 CHECK."""
    digest = hashlib.sha256(f"{ref}|{entry.normalize()}".encode()).hexdigest()[:16]
    return ROLL_NOTE_PREFIX + digest


# ---------------------------------------------------------------------------------------------------- pre-warm
def prewarm_symbols(doc: ValidDocument | None, at_ms: int, *, held: Mapping[str, Iterable[str]],
                    tick_symbols: Mapping[str, Iterable[str]], bar_adapters: Iterable[str],
                    cap: int = PREWARM_CAP) -> dict[str, tuple[str, ...]]:
    """The pre-warm set per bar-capable adapter: explicit symbols of the assignments in force first, then (only where
    a wildcard-symbol assignment in force covers the adapter) held symbols and tick symbols; sorted within each group,
    at most `cap` per adapter."""
    adapters = sorted(set(bar_adapters))
    if doc is None or not adapters:
        return {}
    explicit: dict[str, set[str]] = {a: set() for a in adapters}
    wildcard: set[str] = set()
    for a in assignments_in_force(doc, at_ms):
        targets = [a.adapter] if a.adapter is not None else adapters
        for adapter in targets:
            if adapter not in explicit:
                continue
            if a.symbol is not None:
                explicit[adapter].add(a.symbol)
            else:
                wildcard.add(adapter)
    out: dict[str, tuple[str, ...]] = {}
    for adapter in adapters:
        chosen = sorted(explicit[adapter])
        if adapter in wildcard:
            more = set(held.get(adapter, ())) | set(tick_symbols.get(adapter, ()))
            chosen += sorted(more - set(chosen))
        if chosen:
            out[adapter] = tuple(chosen[:cap])
    return out


__all__ = [
    "ASSET_CLASS", "ATTRIBUTION_SINGLE_EDGE", "AT_FILL_WINDOW_MS", "BAR_SPECS", "Change", "DEMAND_CEILING",
    "ENTRY_TIME_NOT_EXACT", "ENTRY_UNKNOWN", "IMAGE_STALE", "INCREMENT_UNKNOWN", "INIT_FAILED", "NO_ASSIGNMENT",
    "NO_BROKER_BARS", "NO_CONFIG", "NO_STOP", "NO_TIMING_ROW", "PIN_REFUSED", "PREWARM_CAP", "ROLL_WINDOW_MS",
    "ROLL_NOTE_PREFIX", "S0Pick", "S0_NOT_AT_FILL", "SECOND_EDGE", "STOP_READ_BUDGET", "UNSUPPORTED_REASONS",
    "Unmanaged", "classify_change", "entry_choice", "entry_fills_cover", "exact_entry_time", "near_ny_roll",
    "prewarm_symbols", "roll_note", "s0_pick", "second_edges",
]
