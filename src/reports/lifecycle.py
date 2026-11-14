"""Order life cycles of the past N days, rebuilt from the engine's own PostgreSQL log (read-only).

Read (`read_versions`, one `REPEATABLE READ` transaction with `SET TRANSACTION READ ONLY`; SELECT only):
* every APPLIED ORDER / POSITION version in `event_log` committed in [now - days, now] (the seq_id range of the
  window: seq_id is the HLC `epoch_ms << 20`), downtime catch-up rows included (they are ordinary REST
  versions there);
* the current row (`orders_state` / `positions_state`) of every entity touched in the window, of every
  position a touched order links to (and, StoneX, the position / order sharing the id), of every live row
  (working order, OPEN position, whenever last committed) and of every row committed in the window that
  changed at the broker in it.
DROPPED_STALE rows are left out: they are versions the engine never applied.

Privacy by construction: the SQL extracts only symbol, ids, side, status, price levels (entry / order price,
stop-loss, take-profit), the version and times. Units, quantities, swap, commission and every account amount
never leave the database, so nothing downstream can render them.

Timelines (pure, unit-testable without a database):
* `build_timelines`: versions per entity sorted by the engine's version order (broker_seq, broker time,
  observed_seq, then seq_id), consecutive identical content collapsed, each state stamped with its event time
  (broker time, falling back to the commit time when the broker gave none; never moving backwards).
* `build_lifecycles`: links each order to its position (linked_position_id, then position_id = order id as on
  StoneX, then opened_by_client_order_id, then - Pepperstone opening orders carry no position id - the
  unclaimed position of the same account / symbol / side first seen within FILL_MATCH_MS of the fill) and
  attaches reduce-only orders (StoneX stop / limit children, Pepperstone closing and SL/TP orders) to the
  position they reduce. Each life cycle carries the drawing facts: placement, working order price steps, fill,
  position steps, stop-loss / take-profit steps, close (time, price, favourable or not) and the status trail,
  plus `events`: every placement, working-price change, SL / TP set / move / removal, fill, close and cancel
  with its broker time and price level, oldest first.
  Links the log does not carry are recovered conservatively: an order is reduce-only when any of its versions
  says so (a downtime catch-up REST row drops the flag and the link), a "reduce-only" fill linked to its own id
  is the opening trade (StoneX trade history), and an unlinked order on the other side that was cancelled with a
  position's close is that position's other bracket leg (one-cancels-other). StoneX brackets that never filled
  (entry X, legs X+1 / X+2 on the other side, seen and cancelled together) stay one pending item.
* `build_trades`: a TRADE is one position with every order that belongs to it (opening order, stop-loss /
  take-profit legs and their replacements, closing order), numbered T1..Tn per symbol in entry-time order. It
  carries the entry (time, price and how the time is known), the stop-loss and take-profit steps from the entry
  to the exit, the exit (time, price, and kind: SL hit / TP hit / closed / still open) and its events. Orders
  that never filled are PENDING-ONLY items (`PendingOrder`).

Read-only: nothing here sends, modifies or cancels an order.
"""
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from typing import Final, Literal

from src.db.pg import PgConnection, Row

Kind = Literal["ORDER", "POSITION"]
Side = Literal["BUY", "SELL"]
Leg = Literal["SL", "TP"]
CloseResult = Literal["FAVOURABLE", "UNFAVOURABLE", "FLAT", "UNKNOWN"]
EventKind = Literal["FIRST_SEEN", "STATUS", "PRICE", "STOP_LOSS", "TAKE_PROFIT"]
EntityRef = tuple[str, str, str]   # (kind, adapter_id, entity_id)

DAY_MS: Final = 86_400_000
HLC_SHIFT: Final = 20
MAX_DAYS: Final = 90
#: Safety cap on event_log rows per export (newest kept; the read says when it was hit).
MAX_LOG_ROWS: Final = 200_000
#: A Pepperstone opening order carries no position id: the position of the same account / symbol / side first
#: seen this close to the order's fill is taken as the one it opened.
FILL_MATCH_MS: Final = 5_000

WORKING_STATUSES: Final = frozenset({"PENDING", "WORKING", "PARTIALLY_FILLED"})
FILL_STATUSES: Final = frozenset({"PARTIALLY_FILLED", "FILLED"})
DEAD_STATUSES: Final = frozenset({"CANCELLED", "REJECTED"})
FILLED: Final = frozenset({"FILLED"})
CLOSED: Final = frozenset({"CLOSED"})
ORDER_STATUSES: Final = frozenset({"PENDING", "WORKING", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED"})
POSITION_STATUSES: Final = frozenset({"OPEN", "CLOSED"})


# ------------------------------------------------------------------------------------------------ values
@dataclass(frozen=True)
class StateVersion:
    """One committed version of an order or a position, reduced to the fields a chart may show."""

    kind: Kind
    adapter_id: str
    entity_id: str
    account_id: str
    symbol: str
    side: Side
    status: str
    price: Decimal | None                # order: order / trigger / execution price; position: entry price
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    is_reduce_only: bool = False
    linked_position_id: str | None = None
    client_id: str | None = None         # order client_order_id / position opened_by_client_order_id
    broker_seq: int | None = None
    broker_ms: int = 0
    observed_seq: int = 0
    seq_id: int = 0
    commit_ms: int = 0
    source: str = "STREAM"

    @property
    def ref(self) -> EntityRef:
        return (self.kind, self.adapter_id, self.entity_id)

    @property
    def event_ms(self) -> int:
        """Broker time; the commit time when the broker gave none."""
        return self.broker_ms if self.broker_ms > 0 else self.commit_ms

    @property
    def version_key(self) -> tuple[int, int, int, int]:
        """The engine's order (src.dto.version_key, then seq_id as the last tie-breaker)."""
        return (-1 if self.broker_seq is None else self.broker_seq, self.broker_ms, self.observed_seq, self.seq_id)

    @property
    def content(self) -> tuple[object, ...]:
        return (self.status, self.price, self.stop_loss, self.take_profit, self.is_reduce_only,
                self.linked_position_id, self.symbol, self.side)


@dataclass(frozen=True)
class TimelineEvent:
    t_ms: int
    kind: EventKind
    status: str
    value: Decimal | None = None


@dataclass(frozen=True)
class Timeline:
    """One entity's ordered states; `times[i]` is the (monotonic) event time of `states[i]`."""

    ref: EntityRef
    states: tuple[StateVersion, ...]
    times: tuple[int, ...]
    events: tuple[TimelineEvent, ...]

    @property
    def kind(self) -> Kind:
        return self.states[0].kind

    @property
    def adapter_id(self) -> str:
        return self.ref[1]

    @property
    def entity_id(self) -> str:
        return self.ref[2]

    @property
    def last(self) -> StateVersion:
        return self.states[-1]

    @property
    def first_ms(self) -> int:
        return self.times[0]

    @property
    def last_ms(self) -> int:
        return self.times[-1]

    @property
    def status_trail(self) -> tuple[str, ...]:
        return collapse(s.status for s in self.states)

    def first_with(self, statuses: frozenset[str]) -> tuple[int, StateVersion] | None:
        for t, s in zip(self.times, self.states):
            if s.status in statuses:
                return t, s
        return None

    def first_price(self) -> Decimal | None:
        return next((s.price for s in self.states if s.price is not None), None)

    def last_price(self) -> Decimal | None:
        return next((s.price for s in reversed(self.states) if s.price is not None), None)


@dataclass(frozen=True)
class Step:
    start_ms: int
    end_ms: int
    price: Decimal


@dataclass(frozen=True)
class Marker:
    t_ms: int
    price: Decimal


#: Life-cycle event kinds, in the order they are listed when they share a time.
LC_EVENT_KINDS: Final = ("PLACED", "PRICE_CHANGED", "FILLED", "OPENED", "SL_SET", "SL_MOVED", "SL_REMOVED",
                         "TP_SET", "TP_MOVED", "TP_REMOVED", "CLOSED", "CANCELLED", "REJECTED")
LcEventKind = Literal["PLACED", "PRICE_CHANGED", "FILLED", "OPENED", "SL_SET", "SL_MOVED", "SL_REMOVED",
                      "TP_SET", "TP_MOVED", "TP_REMOVED", "CLOSED", "CANCELLED", "REJECTED"]


@dataclass(frozen=True)
class LcEvent:
    """One thing that happened to a life cycle, at its (broker) time in UNIX ms.

    * PLACED: first seen working at `price`; PRICE_CHANGED: the working order's price moved to `price` (`prev`
      the old one);
    * FILLED: filled, `price` the entry; OPENED: a position first seen OPEN at `price` (no opening order in the
      log, so not a fill time);
    * SL_ / TP_ SET, MOVED (`prev` the old level), REMOVED (`price` the level that went away): every change of
      the stop-loss / take-profit while the order worked or the position was open (the clean-up when the trade
      ends is not an event);
    * CLOSED: `price` the close price (None when not recorded), `result` FAVOURABLE / UNFAVOURABLE / FLAT /
      UNKNOWN; CANCELLED / REJECTED: `price` the last order price (None when unknown).
    """

    t_ms: int
    kind: LcEventKind
    price: Decimal | None
    prev: Decimal | None = None
    result: CloseResult | None = None


@dataclass(frozen=True)
class LegOrder:
    """A reduce-only / bracket leg of a life cycle: its role (by its first price against the entry), last price,
    when it was first seen and when it ended (filled / cancelled / rejected; None while working)."""

    order_id: str
    role: Leg | None
    price: Decimal | None
    first_ms: int
    end_ms: int | None
    outcome: str
    seen_working: bool


@dataclass(frozen=True)
class Lifecycle:
    """One order's life cycle (or a position whose opening order is not in the log), ready to draw."""

    key: str                              # "<adapter>:<order id>" or "<adapter>:<position id>"
    adapter_id: str
    symbol: str
    side: Side
    order_id: str | None
    position_id: str | None
    status_trail: tuple[str, ...]
    outcome: str                          # final order status, or OPEN / CLOSED once a position exists
    placed: Marker | None
    working: tuple[Step, ...]
    fill: Marker | None
    position: tuple[Step, ...]
    stop_loss: tuple[Step, ...]
    take_profit: tuple[Step, ...]
    close: Marker | None                  # close time at the close price (or at the entry when unknown)
    close_result: CloseResult | None
    ended: Marker | None                  # cancelled / rejected before any fill
    first_ms: int
    last_ms: int
    children: tuple[str, ...] = ()        # reduce-only order ids attached to this life cycle
    #: the fill marker is where the position was first seen OPEN (no opening order in the log), not a fill
    fill_seen_only: bool = False
    #: every placement, order price change, SL / TP change, fill, close and cancel, oldest first
    events: tuple[LcEvent, ...] = ()
    #: the children as legs (role, last price, first seen, end), in id order
    legs: tuple[LegOrder, ...] = ()
    #: source (STREAM / REST) of the version that gave the fill time (the position's first version when no fill)
    fill_source: str = ""
    #: the head order is a reduce-only order whose position is not in the log (a closing fill drawn alone)
    reduce_only: bool = False
    account_id: str = ""

    @property
    def label_id(self) -> str:
        return self.order_id or self.position_id or self.key

    @property
    def anchor(self) -> Marker | None:
        """Where the info label goes: placement, else fill, else the position, else the cancel point, else the
        close."""
        if self.placed is not None:
            return self.placed
        if self.fill is not None:
            return self.fill
        if self.position:
            return Marker(self.position[0].start_ms, self.position[0].price)
        if self.ended is not None:
            return self.ended
        return self.close                 # a position first seen CLOSED: only its close is known


# ------------------------------------------------------------------------------------------------ parsing
def collapse(values: Iterable[str]) -> tuple[str, ...]:
    out: list[str] = []
    for v in values:
        if not out or out[-1] != v:
            out.append(v)
    return tuple(out)


def normalise_symbol(symbol: str) -> str:
    """'EUR/USD', 'eurusd', 'EUR_USD' -> 'EURUSD' (upper case, letters and digits only)."""
    return re.sub(r"[^A-Z0-9]", "", symbol.upper())


def _text(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _dec(v: object) -> Decimal | None:
    if v is None:
        return None
    if isinstance(v, Decimal):
        d = v
    else:
        s = _text(v)
        if s is None:
            return None
        try:
            d = Decimal(s)
        except InvalidOperation:
            return None
    return d if d.is_finite() else None


def _int(v: object, default: int = 0) -> int:
    if v is None:
        return default
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    try:
        return int(Decimal(str(v).strip()))
    except (InvalidOperation, ValueError):
        return default


def _opt_int(v: object) -> int | None:
    return None if _text(v) is None else _int(v)


def _bool(v: object) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("true", "t", "1") if v is not None else False


#: Column order shared by both SELECTs below and `version_from_row`.
VERSION_COLUMNS: Final = (
    "kind", "adapter_id", "account_id", "entity_id", "symbol", "side", "status", "price", "stop_loss",
    "take_profit", "is_reduce_only", "linked_position_id", "client_id", "broker_seq", "broker_ms", "observed_seq",
    "seq_id", "commit_ms", "source",
)


def version_from_row(r: Row) -> StateVersion | None:
    """A row in VERSION_COLUMNS order -> StateVersion; None when it cannot be placed (no id, side or status)."""
    if len(r) < len(VERSION_COLUMNS):
        return None
    kind, adapter, entity, side, status = _text(r[0]), _text(r[1]), _text(r[3]), _text(r[5]), _text(r[6])
    if kind not in ("ORDER", "POSITION") or adapter is None or entity is None:
        return None
    if side not in ("BUY", "SELL"):
        return None
    if status not in (ORDER_STATUSES if kind == "ORDER" else POSITION_STATUSES):
        return None
    return StateVersion(
        kind="ORDER" if kind == "ORDER" else "POSITION", adapter_id=adapter, entity_id=entity,
        account_id=_text(r[2]) or "", symbol=_text(r[4]) or "?", side="BUY" if side == "BUY" else "SELL",
        status=status, price=_dec(r[7]), stop_loss=_dec(r[8]),
        take_profit=_dec(r[9]), is_reduce_only=_bool(r[10]), linked_position_id=_text(r[11]), client_id=_text(r[12]),
        broker_seq=_opt_int(r[13]), broker_ms=_int(r[14]), observed_seq=_int(r[15]), seq_id=_int(r[16]),
        commit_ms=_int(r[17]), source=_text(r[18]) or "STREAM",
    )


# ------------------------------------------------------------------------------------------------ SQL
#: Every APPLIED order / position version committed in the window, newest first (capped). Only the
#: chart-safe payload fields are extracted: units and amounts never leave the database.
SQL_LOG_VERSIONS: Final = (
    "SELECT e.entity_type, e.adapter_id, e.account_id, "
    "COALESCE(e.payload->>'broker_order_id', e.payload->>'position_id'), e.payload->>'symbol', "
    "e.payload->>'side', e.payload->>'status', e.payload->>'entry_price', e.payload->>'stop_loss', "
    "e.payload->>'take_profit', e.payload->>'is_reduce_only', e.payload->>'linked_position_id', "
    "COALESCE(e.payload->>'client_order_id', e.payload->>'opened_by_client_order_id'), e.payload->>'broker_seq', "
    "e.payload->>'broker_updated_at_ms', e.payload->>'observed_seq', e.seq_id::text, e.logged_at_ms::text, "
    "e.payload->>'source' "
    "FROM event_log e "
    "WHERE e.entity_type IN ('ORDER', 'POSITION') AND e.action = 'APPLIED' "
    "AND e.seq_id >= $1::bigint AND e.seq_id < $2::bigint AND ($3::text IS NULL OR e.adapter_id = $3::text) "
    "ORDER BY e.seq_id DESC, e.log_id DESC LIMIT $4::int"
)
#: Current order rows named in ($3, $4), every live row (a working order / OPEN position is drawn up to now
#: whenever it was last committed: it may not have changed for longer than the window), and rows committed in
#: the window that changed at the broker in it. A terminal row whose broker time is older than the window is not
#: pulled in by a mere re-commit. Commit time = the version's HLC ms.
SQL_CURRENT_ORDERS: Final = (
    "SELECT 'ORDER', o.adapter_id, o.account_id, o.broker_order_id, o.symbol, o.side, o.status, "
    "o.entry_price::text, o.stop_loss::text, o.take_profit::text, o.is_reduce_only::text, o.linked_position_id, "
    "o.client_order_id, o.broker_seq::text, o.broker_updated_at_ms::text, o.observed_seq::text, o.seq_id::text, "
    "(o.seq_id >> 20)::text, o.source "
    "FROM orders_state o "
    "WHERE ($2::text IS NULL OR o.adapter_id = $2::text) AND (o.status IN ('PENDING', 'WORKING', 'PARTIALLY_FILLED') "
    "OR (o.committed_at_ms >= $1::bigint AND o.broker_updated_at_ms >= $1::bigint) "
    "OR (o.adapter_id, o.broker_order_id) IN (SELECT a, b FROM unnest($3::text[], $4::text[]) AS k (a, b))) "
    "ORDER BY o.adapter_id, o.broker_order_id"
)
SQL_CURRENT_POSITIONS: Final = (
    "SELECT 'POSITION', p.adapter_id, p.account_id, p.position_id, p.symbol, p.side, p.status, "
    "p.entry_price::text, p.stop_loss::text, p.take_profit::text, 'false', NULL::text, "
    "p.opened_by_client_order_id, p.broker_seq::text, p.broker_updated_at_ms::text, p.observed_seq::text, "
    "p.seq_id::text, (p.seq_id >> 20)::text, p.source "
    "FROM positions_state p "
    "WHERE ($2::text IS NULL OR p.adapter_id = $2::text) AND (p.status = 'OPEN' "
    "OR (p.committed_at_ms >= $1::bigint AND p.broker_updated_at_ms >= $1::bigint) "
    "OR (p.adapter_id, p.position_id) IN (SELECT a, b FROM unnest($3::text[], $4::text[]) AS k (a, b))) "
    "ORDER BY p.adapter_id, p.position_id"
)


@dataclass(frozen=True)
class Window:
    since_ms: int
    until_ms: int

    @staticmethod
    def last_days(days: int, now_ms: int) -> Window:
        if not 1 <= days <= MAX_DAYS:
            raise ValueError(f"days must be between 1 and {MAX_DAYS}")
        return Window(now_ms - days * DAY_MS, now_ms)

    @property
    def seq_range(self) -> tuple[int, int]:
        return self.since_ms << HLC_SHIFT, (self.until_ms + 1) << HLC_SHIFT


@dataclass(frozen=True)
class LogRead:
    window: Window
    versions: tuple[StateVersion, ...]
    log_rows: int
    current_rows: int
    skipped_rows: int
    truncated: bool          # MAX_LOG_ROWS hit: the oldest versions of the window were not read


def related_keys(versions: Iterable[StateVersion]) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """(order keys, position keys) whose current rows complete the window: the touched entities, the positions
    their orders link to, and (StoneX) the position named like the order."""
    orders: set[tuple[str, str]] = set()
    positions: set[tuple[str, str]] = set()
    for v in versions:
        # StoneX: position id = opening order id, so each id is looked up on both tables (a position closed in
        # the window brings its opening order, and so its real fill time, even when the order did not change)
        orders.add((v.adapter_id, v.entity_id))
        positions.add((v.adapter_id, v.entity_id))
        if v.kind == "ORDER" and v.linked_position_id:
            positions.add((v.adapter_id, v.linked_position_id))
            orders.add((v.adapter_id, v.linked_position_id))
    return sorted(orders), sorted(positions)


async def read_versions(conn: PgConnection, window: Window, *, adapter: str | None = None,
                        max_rows: int = MAX_LOG_ROWS) -> LogRead:
    """One read-only snapshot of the window's versions and the related current rows. SELECT only."""
    lo, hi = window.seq_range
    await conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
    try:
        await conn.execute("SET TRANSACTION READ ONLY")
        await conn.execute("SET LOCAL statement_timeout = '60s'")
        log_rows = await conn.fetch(SQL_LOG_VERSIONS, lo, hi, adapter, max_rows + 1)
        truncated = len(log_rows) > max_rows
        log_rows = log_rows[:max_rows]
        parsed = [version_from_row(r) for r in log_rows]
        logged = [v for v in parsed if v is not None]
        okeys, pkeys = related_keys(logged)
        cur = await conn.fetch(SQL_CURRENT_ORDERS, window.since_ms, adapter, [k[0] for k in okeys],
                               [k[1] for k in okeys])
        cur += await conn.fetch(SQL_CURRENT_POSITIONS, window.since_ms, adapter, [k[0] for k in pkeys],
                                [k[1] for k in pkeys])
        current = [version_from_row(r) for r in cur]
    except BaseException:
        try:
            await conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    await conn.execute("COMMIT")
    versions = tuple(logged + [v for v in current if v is not None])
    skipped = sum(1 for v in parsed if v is None) + sum(1 for v in current if v is None)
    return LogRead(window, versions, len(log_rows), len(cur), skipped, truncated)


# ------------------------------------------------------------------------------------------------ timelines
def _events(states: Sequence[StateVersion], times: Sequence[int]) -> tuple[TimelineEvent, ...]:
    out: list[TimelineEvent] = []
    prev: StateVersion | None = None
    for t, s in zip(times, states):
        if prev is None:
            out.append(TimelineEvent(t, "FIRST_SEEN", s.status, s.price))
            if s.stop_loss is not None:
                out.append(TimelineEvent(t, "STOP_LOSS", s.status, s.stop_loss))
            if s.take_profit is not None:
                out.append(TimelineEvent(t, "TAKE_PROFIT", s.status, s.take_profit))
        else:
            if s.status != prev.status:
                out.append(TimelineEvent(t, "STATUS", s.status))
            if s.price != prev.price:
                out.append(TimelineEvent(t, "PRICE", s.status, s.price))
            if s.stop_loss != prev.stop_loss:
                out.append(TimelineEvent(t, "STOP_LOSS", s.status, s.stop_loss))
            if s.take_profit != prev.take_profit:
                out.append(TimelineEvent(t, "TAKE_PROFIT", s.status, s.take_profit))
        prev = s
    return tuple(out)


def build_timeline(versions: Iterable[StateVersion]) -> Timeline:
    """One entity's versions -> ordered, de-duplicated states with monotonic event times."""
    ordered = sorted(versions, key=lambda v: (v.version_key, v.commit_ms))
    if not ordered:
        raise ValueError("no versions")
    states: list[StateVersion] = []
    for v in ordered:
        if states and (states[-1].version_key == v.version_key or states[-1].content == v.content):
            # the same version read twice (log row and current row), or a newer version with the same content
            continue
        states.append(v)
    times: list[int] = []
    for s in states:
        times.append(max(s.event_ms, times[-1]) if times else s.event_ms)
    return Timeline(states[0].ref, tuple(states), tuple(times), _events(states, times))


def build_timelines(versions: Iterable[StateVersion]) -> dict[EntityRef, Timeline]:
    groups: dict[EntityRef, list[StateVersion]] = {}
    for v in versions:
        groups.setdefault(v.ref, []).append(v)
    return {ref: build_timeline(groups[ref]) for ref in sorted(groups)}


# ------------------------------------------------------------------------------------------------ steps
def steps_from_points(points: Sequence[tuple[int, Decimal | None]], end_ms: int) -> tuple[Step, ...]:
    """Change points (time, value or None = no level) -> horizontal steps ending at `end_ms`. Zero-length
    steps are dropped and touching steps at the same price merged."""
    out: list[Step] = []
    pts = sorted(points, key=lambda p: p[0])
    for i, (t, value) in enumerate(pts):
        nxt = pts[i + 1][0] if i + 1 < len(pts) else end_ms
        stop = min(nxt, end_ms)
        if value is None or stop <= t:
            continue
        if out and out[-1].price == value and out[-1].end_ms == t:
            out[-1] = Step(out[-1].start_ms, stop, value)
        else:
            out.append(Step(t, stop, value))
    return tuple(out)


def _field_points(tl: Timeline, leg: Leg, until_ms: int | None = None) -> list[tuple[int, Decimal | None]]:
    """A source's own SL or TP field: a point at each change (an initial absence is not a point)."""
    out: list[tuple[int, Decimal | None]] = []
    prev: Decimal | None = None
    for t, s in zip(tl.times, tl.states):
        if until_ms is not None and t > until_ms:
            break
        v = s.stop_loss if leg == "SL" else s.take_profit
        if v != prev:
            out.append((t, v))
        prev = v
    return out


def child_role(child: Timeline, side: Side, reference: Decimal | None) -> Leg | None:
    """A reduce-only order protecting a `side` position: below the reference (entry) for a BUY is the stop-loss,
    above it the take-profit (mirrored for a SELL). The role is fixed at the child's first priced state."""
    price = child.first_price()
    if price is None or reference is None:
        return None
    if side == "BUY":
        return "TP" if price > reference else "SL"
    return "TP" if price < reference else "SL"


def _child_points(child: Timeline) -> list[tuple[int, Decimal | None]]:
    out: list[tuple[int, Decimal | None]] = []
    for t, s in zip(child.times, child.states):
        if s.status in WORKING_STATUSES:
            out.append((t, s.price))
        else:
            out.append((t, None))   # filled (the close) or cancelled / rejected: this level is gone
            break
    return out


def _merge_leg(sources: Sequence[list[tuple[int, Decimal | None]]]) -> list[tuple[int, Decimal | None]]:
    """Points of several sources -> one level per change time. Each source is active from a non-None point
    until its next None (a child filled / cancelled, a field cleared); the level is the value of the active
    source that changed most recently (stable by source order at equal times), None when no source is active.
    So one source ending (an old stop cancelled after its replacement was placed, a terminal protection order)
    never hides a level another source still holds."""
    tagged = [(t, i, j, v) for i, pts in enumerate(sources) for j, (t, v) in enumerate(pts)]
    tagged.sort(key=lambda x: (x[0], x[1], x[2]))
    active: dict[int, tuple[Decimal, int]] = {}     # source -> (value, change stamp)
    stamp = 0
    out: list[tuple[int, Decimal | None]] = []
    k = 0
    while k < len(tagged):
        t = tagged[k][0]
        while k < len(tagged) and tagged[k][0] == t:
            _t, i, _j, v = tagged[k]
            if v is None:
                active.pop(i, None)
            elif i not in active or active[i][0] != v:
                stamp += 1
                active[i] = (v, stamp)
            k += 1
        level = max(active.values(), key=lambda a: a[1])[0] if active else None
        if not out or out[-1][1] != level:
            out.append((t, level))
    return out


def leg_events(points: Sequence[tuple[int, Decimal | None]], leg: Leg, end_ms: int) -> list[LcEvent]:
    """Merged level change points of one leg -> SET / MOVED / REMOVED events before `end_ms` (the close, the
    cancel or now: a level ending with the trade is not an adjustment)."""
    out: list[LcEvent] = []
    prev: Decimal | None = None
    for t, v in points:
        if t >= end_ms:
            break
        if v is not None and prev is None:
            out.append(LcEvent(t, "SL_SET" if leg == "SL" else "TP_SET", v))
        elif v is not None and prev is not None and v != prev:
            out.append(LcEvent(t, "SL_MOVED" if leg == "SL" else "TP_MOVED", v, prev))
        elif v is None and prev is not None:
            out.append(LcEvent(t, "SL_REMOVED" if leg == "SL" else "TP_REMOVED", prev))
        prev = v
    return out


def sort_events(events: Iterable[LcEvent]) -> tuple[LcEvent, ...]:
    rank = {k: i for i, k in enumerate(LC_EVENT_KINDS)}
    return tuple(sorted(events, key=lambda e: (e.t_ms, rank[e.kind])))


def close_result(side: Side, entry: Decimal | None, close: Decimal | None) -> CloseResult:
    if entry is None or close is None:
        return "UNKNOWN"
    if close == entry:
        return "FLAT"
    better = close > entry if side == "BUY" else close < entry
    return "FAVOURABLE" if better else "UNFAVOURABLE"


# ------------------------------------------------------------------------------------------------ lifecycles
def _lifecycle(order: Timeline | None, pos: Timeline | None, children: Sequence[Timeline],
               now_ms: int, reduce_only: bool = False) -> Lifecycle:
    head = order if order is not None else pos
    assert head is not None
    side: Side = head.last.side
    symbol = head.last.symbol

    # --- the order: placement, working price steps, fill, cancel
    placed: Marker | None = None
    working: tuple[Step, ...] = ()
    fill_t: int | None = None
    fill_state: StateVersion | None = None
    dead_t: int | None = None
    events: list[LcEvent] = []
    if order is not None:
        s0 = order.states[0]
        if s0.status in WORKING_STATUSES and s0.price is not None:
            placed = Marker(order.first_ms, s0.price)
        f = order.first_with(FILL_STATUSES)
        if f is not None:
            fill_t, fill_state = f
        d = order.first_with(FILLED | DEAD_STATUSES)
        work_end = d[0] if d is not None else now_ms
        if fill_t is None and d is not None and d[1].status in DEAD_STATUSES:
            dead_t = d[0]
        pts: list[tuple[int, Decimal | None]] = []
        work_price: Decimal | None = None
        for t, s in zip(order.times, order.states):
            if t > work_end:
                break
            pts.append((t, s.price if s.status in WORKING_STATUSES else None))
            if s.status in WORKING_STATUSES and s.price is not None:
                if work_price is not None and s.price != work_price and t < work_end:
                    events.append(LcEvent(t, "PRICE_CHANGED", s.price, work_price))
                work_price = s.price
        working = steps_from_points(pts, work_end)
        if placed is not None:
            events.append(LcEvent(placed.t_ms, "PLACED", placed.price))

    # --- the position: open, entry steps, close
    entry: Decimal | None = pos.first_price() if pos is not None else None
    if entry is None and fill_state is not None:
        entry = fill_state.price
    fill: Marker | None = None
    fill_seen_only = False
    # the position was seen OPEN before the order's fill row (a catch-up read stamped the opening trade with the
    # close time): that row's time is not the fill time
    late_fill = (fill_t is not None and pos is not None and pos.states[0].status == "OPEN"
                 and pos.first_ms + FILL_MATCH_MS < fill_t)
    if fill_t is not None and entry is not None and not late_fill:
        fill = Marker(fill_t, entry)
    elif pos is not None and entry is not None and pos.states[0].status == "OPEN":
        # a position whose opening fill is not in the log: drawn from where it was first seen OPEN (its open
        # may be older). One first seen CLOSED (opened before the window, a catch-up close) gets no fill
        # marker: drawing one at the close time would invent a fill.
        fill = Marker(pos.first_ms, entry)
        fill_seen_only = True
    close_t: int | None = None
    if pos is not None:
        c = pos.first_with(CLOSED)
        close_t = c[0] if c is not None else None
    child_fills: list[tuple[int, Decimal, str]] = []
    for ch in children:
        cf = ch.first_with(FILLED)
        if cf is not None and cf[1].price is not None:
            child_fills.append((cf[0], cf[1].price, ch.entity_id))
    child_fills.sort()
    if close_t is None and pos is None and child_fills:
        close_t = child_fills[0][0]              # a reduce-only fill closed a position we never saw
    close_price: Decimal | None = None
    if close_t is not None:
        # the closing fill: the latest reduce-only fill up to the close (a broker reports them a moment apart)
        before = [cf for cf in child_fills if cf[0] <= close_t + FILL_MATCH_MS]
        if before:
            close_price = before[-1][1]
    position: tuple[Step, ...] = ()
    if pos is not None and entry is not None:
        open_t = fill_t if fill_t is not None and fill_t < pos.first_ms else pos.first_ms
        pend = close_t if close_t is not None else now_ms
        ppts: list[tuple[int, Decimal | None]] = [(open_t, entry)]
        for t, s in zip(pos.times, pos.states):
            if s.status == "OPEN" and s.price is not None and t > open_t:
                ppts.append((t, s.price))
        position = steps_from_points(ppts, pend)

    close: Marker | None = None
    result: CloseResult | None = None
    if close_t is not None:
        result = close_result(side, entry, close_price)
        at = close_price if close_price is not None else entry
        if at is not None:
            close = Marker(close_t, at)

    ended: Marker | None = None
    if dead_t is not None and order is not None:
        last_price = order.last_price()
        if last_price is not None:
            ended = Marker(dead_t, last_price)

    # --- stop-loss / take-profit: the order's fields, the position's fields, the reduce-only children
    if close_t is not None:
        level_end = close_t
    elif dead_t is not None:
        level_end = dead_t
    else:
        level_end = now_ms
    reference = entry if entry is not None else (order.first_price() if order is not None else None)
    legs: dict[Leg, list[list[tuple[int, Decimal | None]]]] = {"SL": [], "TP": []}
    order_done = order.first_with(FILLED | DEAD_STATUSES) if order is not None else None
    for leg in ("SL", "TP"):
        if order is not None:
            opts = _field_points(order, leg, None if order_done is None else order_done[0])
            if pos is not None and order_done is not None:
                # once the order is done and its position exists, the position (and its children) carry the
                # levels: a stale order field must never stand in for a level the position no longer has
                opts.append((max(order_done[0], pos.first_ms), None))
            legs[leg].append(opts)
        if pos is not None:
            legs[leg].append(_field_points(pos, leg))
    for ch in children:
        role = child_role(ch, side, reference)
        if role is not None:
            legs[role].append(_child_points(ch))
    sl_points, tp_points = _merge_leg(legs["SL"]), _merge_leg(legs["TP"])
    sl = steps_from_points(sl_points, level_end)
    tp = steps_from_points(tp_points, level_end)
    events += leg_events(sl_points, "SL", level_end) + leg_events(tp_points, "TP", level_end)
    if fill is not None:
        events.append(LcEvent(fill.t_ms, "OPENED" if fill_seen_only else "FILLED", fill.price))
    if close_t is not None:
        events.append(LcEvent(close_t, "CLOSED", close_price, result=result))
    if dead_t is not None and order is not None:
        dead = order.first_with(DEAD_STATUSES)
        events.append(LcEvent(dead_t, "REJECTED" if dead is not None and dead[1].status == "REJECTED"
                              else "CANCELLED", ended.price if ended is not None else None))

    # --- status trail, outcome, extent
    if order is not None:
        trail = list(order.status_trail)
        if pos is not None:
            trail.append("CLOSED" if pos.last.status == "CLOSED" else "OPEN")
        elif close_t is not None:
            trail.append("CLOSED")
    else:
        assert pos is not None
        trail = list(pos.status_trail)
    trail_t = collapse(trail)
    if pos is not None:
        outcome = pos.last.status
    elif close_t is not None:
        outcome = "CLOSED"
    else:
        assert order is not None
        outcome = order.last.status
    parts = [tl for tl in (order, pos, *children) if tl is not None]
    first_ms = min(tl.first_ms for tl in parts)
    last_ms = max(tl.last_ms for tl in parts)
    key_id = order.entity_id if order is not None else head.entity_id
    leg_orders: list[LegOrder] = []
    for ch in children:
        end = ch.first_with(FILLED | DEAD_STATUSES)
        leg_orders.append(LegOrder(
            order_id=ch.entity_id, role=child_role(ch, side, reference), price=ch.last_price(), first_ms=ch.first_ms,
            end_ms=None if end is None else end[0], outcome=ch.last.status,
            seen_working=any(s.status in WORKING_STATUSES for s in ch.states)))
    if fill_state is not None and not fill_seen_only:
        fill_source = fill_state.source
    elif pos is not None:
        fill_source = pos.states[0].source
    else:
        fill_source = ""
    return Lifecycle(
        key=f"{head.adapter_id}:{key_id}", adapter_id=head.adapter_id, symbol=symbol, side=side,
        order_id=order.entity_id if order is not None else None,
        position_id=pos.entity_id if pos is not None else None, status_trail=trail_t, outcome=outcome,
        placed=placed, working=working, fill=fill, position=position, stop_loss=sl, take_profit=tp, close=close,
        close_result=result, ended=ended, first_ms=first_ms, last_ms=last_ms,
        children=tuple(sorted(ch.entity_id for ch in children)), fill_seen_only=fill_seen_only,
        events=sort_events(events), legs=tuple(sorted(leg_orders, key=lambda g: g.order_id)), fill_source=fill_source,
        reduce_only=reduce_only, account_id=head.last.account_id,
    )


def _fill_time(tl: Timeline) -> int | None:
    f = tl.first_with(FILL_STATUSES)
    return None if f is None else f[0]


def linked_id(tl: Timeline) -> str | None:
    """The newest link any version carries (a catch-up REST row may drop it)."""
    return next((s.linked_position_id for s in reversed(tl.states) if s.linked_position_id), None)


def is_reducer(tl: Timeline) -> bool:
    """A reduce-only order: any version says so (a catch-up REST row may drop the flag). One linked to its own id
    is the opening trade (StoneX trade history marks it as a closing trade of itself): never a reducer."""
    return any(s.is_reduce_only for s in tl.states) and linked_id(tl) != tl.entity_id


def link_orders(timelines: Mapping[EntityRef, Timeline]) -> tuple[dict[EntityRef, EntityRef], list[EntityRef]]:
    """(opening order -> its position, reduce-only orders). Each position is claimed by one order at most."""
    positions = {ref: tl for ref, tl in timelines.items() if ref[0] == "POSITION"}
    openers = sorted((tl for ref, tl in timelines.items() if ref[0] == "ORDER" and not is_reducer(tl)),
                     key=lambda tl: (tl.first_ms, tl.ref))
    reducers = sorted(ref for ref, tl in timelines.items() if ref[0] == "ORDER" and is_reducer(tl))
    claimed: dict[EntityRef, EntityRef] = {}
    owner: dict[EntityRef, EntityRef] = {}

    def same_trade(o: Timeline, p: Timeline) -> bool:
        return o.last.symbol == p.last.symbol and o.last.side == p.last.side

    def claim(o: Timeline, pref: EntityRef) -> bool:
        p = positions.get(pref)
        if p is None or pref in claimed or not same_trade(o, p):
            return False
        claimed[pref] = o.ref
        owner[o.ref] = pref
        return True

    for o in openers:   # explicit links first
        linked = linked_id(o)
        if linked and claim(o, ("POSITION", o.adapter_id, linked)):
            continue
        if claim(o, ("POSITION", o.adapter_id, o.entity_id)):
            continue
        ids = {o.entity_id} | {s.client_id for s in o.states if s.client_id}
        for pref, p in sorted(positions.items()):
            if pref[1] == o.adapter_id and p.last.client_id in ids and claim(o, pref):
                break
    for o in openers:   # then by fill time (Pepperstone opening orders carry no position id)
        ft = _fill_time(o)
        if o.ref in owner or ft is None:
            continue
        cands = sorted((abs(p.first_ms - ft), pref) for pref, p in positions.items()
                       if pref not in claimed and pref[1] == o.adapter_id and p.last.account_id == o.last.account_id
                       and same_trade(o, p) and abs(p.first_ms - ft) <= FILL_MATCH_MS)
        if cands:
            claim(o, cands[0][1])
    return owner, reducers


def _num(entity_id: str) -> int | None:
    return int(entity_id) if entity_id.isdigit() and len(entity_id) < 19 else None


def _id_gap(a: str, b: str) -> int:
    na, nb = _num(a), _num(b)
    return abs(na - nb) if na is not None and nb is not None else 1 << 62


Heads = dict[EntityRef, tuple[Timeline | None, Timeline | None]]


def _never_filled_order(o: Timeline | None, p: Timeline | None) -> bool:
    return o is not None and p is None and o.first_with(FILL_STATUSES) is None and linked_id(o) is None


def _attach_oco_legs(heads: Heads, children: dict[EntityRef, list[Timeline]]) -> None:
    """An unlinked order that never filled, on the other side of a position of the same account / symbol, first
    seen after the trade began and cancelled within FILL_MATCH_MS of that position's close, is the position's other
    bracket leg (one-cancels-other: a downtime catch-up reads it without its parent). The nearest close wins, then
    the nearest id."""
    closes: list[tuple[EntityRef, Timeline, int, int]] = []    # (head, position, close time, trade start)
    for ref, (o, p) in heads.items():
        if p is not None and (c := p.first_with(CLOSED)) is not None:
            start = min(tl.first_ms for tl in (o, p) if tl is not None)
            closes.append((ref, p, c[0], start))
    moved: dict[EntityRef, EntityRef] = {}
    for ref, (o, p) in sorted(heads.items()):
        if o is None or not _never_filled_order(o, p) or children.get(ref):
            continue
        end = o.first_with(DEAD_STATUSES)
        if end is None:
            continue
        cands = sorted((abs(end[0] - close_t), _id_gap(o.entity_id, href[2]), href)
                       for href, pos, close_t, start in closes
                       if href[1] == ref[1] and pos.last.account_id == o.last.account_id
                       and normalise_symbol(pos.last.symbol) == normalise_symbol(o.last.symbol)
                       and pos.last.side != o.last.side and abs(end[0] - close_t) <= FILL_MATCH_MS
                       and o.first_ms >= start - FILL_MATCH_MS)
        if cands:
            moved[ref] = cands[0][2]
    for ref, target in sorted(moved.items()):
        leg, _ = heads.pop(ref)
        assert leg is not None
        children.setdefault(target, []).append(leg)


def _group_pending_brackets(heads: Heads, children: dict[EntityRef, list[Timeline]]) -> None:
    """StoneX brackets that never filled and whose legs carry no link (a catch-up read): entry X with legs X+1 /
    X+2 of the same account / symbol on the other side, first seen and ended within FILL_MATCH_MS of the entry,
    become one pending item (the entry order with its legs)."""
    pend = {ref: o for ref, (o, p) in heads.items() if ref[0] == "ORDER" and o is not None
            and _never_filled_order(o, p) and not children.get(ref) and _num(ref[2]) is not None}
    by_num = {(ref[1], _num(ref[2])): ref for ref in pend}
    used: set[EntityRef] = set()

    def end_ms(tl: Timeline) -> int | None:
        e = tl.first_with(DEAD_STATUSES)
        return None if e is None else e[0]

    for ref in sorted(pend, key=lambda r: (r[1], _num(r[2]) or 0)):
        if ref in used:
            continue
        o = pend[ref]
        n = _num(ref[2])
        assert n is not None
        legs: list[EntityRef] = []
        for k in (1, 2):
            lref = by_num.get((ref[1], n + k))
            if lref is None or lref in used:
                continue
            leg = pend[lref]
            oe, le = end_ms(o), end_ms(leg)
            if (leg.last.account_id == o.last.account_id and leg.last.side != o.last.side
                    and normalise_symbol(leg.last.symbol) == normalise_symbol(o.last.symbol)
                    and abs(leg.first_ms - o.first_ms) <= FILL_MATCH_MS
                    and ((oe is None and le is None) or (oe is not None and le is not None
                                                         and abs(oe - le) <= FILL_MATCH_MS))):
                legs.append(lref)
        if legs:
            used.add(ref)
            used.update(legs)
            for lref in legs:
                heads.pop(lref)
                children.setdefault(ref, []).append(pend[lref])


def build_lifecycles(timelines: Mapping[EntityRef, Timeline], now_ms: int) -> tuple[Lifecycle, ...]:
    """Every order and position of the window as life cycles, newest activity first (then by key)."""
    owner, reducers = link_orders(timelines)
    pos_owner = {p: o for o, p in owner.items()}
    heads: Heads = {}
    for ref, tl in timelines.items():
        if ref[0] == "ORDER" and ref not in reducers:
            p = owner.get(ref)
            heads[ref] = (tl, timelines[p] if p is not None else None)
        elif ref[0] == "POSITION" and ref not in pos_owner:
            heads[ref] = (None, tl)
    children: dict[EntityRef, list[Timeline]] = {}
    alone: set[EntityRef] = set()
    for ref in reducers:
        ch = timelines[ref]
        linked = linked_id(ch)
        target: EntityRef | None = None
        if linked:
            pref: EntityRef = ("POSITION", ref[1], linked)
            oref: EntityRef = ("ORDER", ref[1], linked)
            if pref in pos_owner:
                target = pos_owner[pref]
            elif pref in heads:
                target = pref
            elif oref in heads:
                target = oref
        if target is None:
            # a filled closing trade the broker gave no position link (StoneX: more than one candidate position
            # when it arrived): the position of the same account / symbol, other side, closed within
            # FILL_MATCH_MS of the fill, so its close gets the real close price
            ft = ch.first_with(FILLED)
            if ft is not None:
                cands = sorted(
                    (abs(c[0] - ft[0]), pref) for pref, p in timelines.items()
                    if pref[0] == "POSITION" and pref[1] == ref[1] and p.last.account_id == ch.last.account_id
                    and p.last.symbol == ch.last.symbol and p.last.side != ch.last.side
                    and (c := p.first_with(CLOSED)) is not None and abs(c[0] - ft[0]) <= FILL_MATCH_MS)
                if cands:
                    pref = cands[0][1]
                    target = pos_owner.get(pref, pref if pref in heads else None)
        if target is None:
            heads[ref] = (ch, None)       # a reduce-only order whose position is not in the log: drawn alone
            alone.add(ref)
        else:
            children.setdefault(target, []).append(ch)
    # brackets that never filled first: an unfilled entry with its own legs is never taken for another trade's
    # one-cancels-other leg because it happened to be cancelled with that trade's close
    _group_pending_brackets(heads, children)
    _attach_oco_legs(heads, children)
    out = [_lifecycle(o, p, sorted(children.get(ref, []), key=lambda c: c.ref), now_ms, ref in alone)
           for ref, (o, p) in sorted(heads.items())]
    out.sort(key=lambda lc: (-lc.last_ms, lc.key))
    return tuple(out)


# ------------------------------------------------------------------------------------------------ trades
EntryKind = Literal["FILLED", "SEEN_OPEN", "NOT_RECORDED"]
ExitKind = Literal["SL", "TP", "CLOSED", "OPEN", "UNKNOWN"]
#: An exit is a stop-loss (take-profit) hit when its price is within this fraction of the level active at the
#: exit (2 / 10 000 of the level: room for a stop's slippage).
EXIT_MATCH: Final = Decimal("0.0002")
_SL_EVENTS: Final = frozenset({"SL_SET", "SL_MOVED", "SL_REMOVED"})


@dataclass(frozen=True)
class Trade:
    """One position and every order that belongs to it, ready to draw as a long / short position tool.

    * entry: `entry_ms` / `entry_price`; `entry_kind` FILLED (the fill time), SEEN_OPEN (the opening fill is not
      in the log: where the position was first seen open) or NOT_RECORDED (a catch-up read gave the opening trade
      only the close time, or the position was first seen closed);
    * `stop_loss` / `take_profit`: the level steps from the entry (or from when set) to the exit (or the
      replacement), clipped to [entry, exit]; a trade without a time span (entry time not recorded) carries the
      levels resting at its exit as one step each over [entry, exit];
    * exit: `exit_ms` (the close, or the export time while open), `exit_price` (None when no closing fill was
      recorded or still open) and `exit_kind`: SL / TP (the exit price within EXIT_MATCH of the stop-loss /
      take-profit level resting at the exit), CLOSED (any other close), OPEN (still open at the export), UNKNOWN
      (a filled opening order whose position is not in the log);
    * `events`: the life cycle's events (placement, SL / TP changes, fill, close), oldest first; a trade whose
      entry time is not recorded lists no fill (its fill row carries the close time).
    """

    key: str
    number: int
    adapter_id: str
    symbol: str
    side: Side
    order_id: str | None
    position_id: str | None
    order_ids: tuple[str, ...]
    entry_ms: int
    entry_price: Decimal | None
    entry_kind: EntryKind
    placed: Marker | None
    working: tuple[Step, ...]
    stop_loss: tuple[Step, ...]
    take_profit: tuple[Step, ...]
    exit_ms: int
    exit_price: Decimal | None
    exit_kind: ExitKind
    events: tuple[LcEvent, ...]
    first_ms: int
    last_ms: int

    @property
    def label_id(self) -> str:
        return self.order_id or self.position_id or self.key

    @property
    def name(self) -> str:
        return f"T{self.number}"

    @property
    def sl_moves(self) -> int | None:
        """Stop-loss changes (set, moved, removed) after the entry and before the exit; None when the entry time is
        not recorded (the trade ran while nothing was watching: its stop-loss changes are unknown, not zero)."""
        if self.entry_kind == "NOT_RECORDED":
            return None
        return sum(1 for e in self.events if e.kind in _SL_EVENTS and self.entry_ms < e.t_ms < self.exit_ms)

    def adjustments(self) -> tuple[LcEvent, ...]:
        """SL / TP changes strictly inside the trade (the levels at the entry are the zones themselves)."""
        return tuple(e for e in self.events if e.kind.startswith(("SL_", "TP_"))
                     and self.entry_ms < e.t_ms < self.exit_ms)


@dataclass(frozen=True)
class PendingOrder:
    """An order that never filled (cancelled, rejected or still working) with its bracket legs' levels; or, rarely,
    a closing fill whose position is not in the log (`outcome` FILLED)."""

    key: str
    adapter_id: str
    symbol: str
    side: Side
    order_id: str
    price: Decimal | None
    steps: tuple[Step, ...]
    first_ms: int
    end_ms: int
    placed_seen: bool
    outcome: str
    stop_loss: Decimal | None
    take_profit: Decimal | None
    legs: tuple[str, ...]
    events: tuple[LcEvent, ...]


def _clip(steps: Sequence[Step], lo: int, hi: int) -> tuple[Step, ...]:
    out: list[Step] = []
    for s in steps:
        a, b = max(s.start_ms, lo), min(s.end_ms, hi)
        if b > a:
            out.append(Step(a, b, s.price))
    return tuple(out)


def levels_at_exit(lc: Lifecycle, exit_ms: int) -> dict[Leg, list[Decimal]]:
    """The stop-loss / take-profit levels resting when the trade ended: the step running to the close, a leg the
    log saw only once it was cancelled with the close, and a leg seen only filled at the close while a leg of the
    other role was cancelled with it (one-cancels-other: the filled one was resting at its price)."""
    out: dict[Leg, list[Decimal]] = {"SL": [], "TP": []}
    if lc.stop_loss and lc.stop_loss[-1].end_ms >= exit_ms:
        out["SL"].append(lc.stop_loss[-1].price)
    if lc.take_profit and lc.take_profit[-1].end_ms >= exit_ms:
        out["TP"].append(lc.take_profit[-1].price)

    def at_exit(g: LegOrder) -> bool:
        return g.end_ms is not None and abs(g.end_ms - exit_ms) <= FILL_MATCH_MS and g.price is not None

    cancelled = [g for g in lc.legs if at_exit(g) and g.outcome in DEAD_STATUSES and g.role is not None]
    for g in cancelled:
        if not g.seen_working and g.role is not None and g.price is not None:
            out[g.role].append(g.price)
    for g in lc.legs:
        # a manual close cancels both legs (a level of the fill's own role is still resting): not a leg's fill
        if (at_exit(g) and g.outcome == "FILLED" and not g.seen_working and g.role is not None
                and g.price is not None and not out[g.role] and any(c.role != g.role for c in cancelled)):
            out[g.role].append(g.price)
    return out


def exit_kind(exit_price: Decimal | None, levels: Mapping[Leg, Sequence[Decimal]]) -> ExitKind:
    """SL / TP when the exit price is within EXIT_MATCH of a level of that leg (the nearest wins), else CLOSED."""
    if exit_price is None:
        return "CLOSED"
    best: tuple[Decimal, int, ExitKind] | None = None
    legs: tuple[tuple[int, Leg], ...] = ((0, "SL"), (1, "TP"))
    for i, leg in legs:
        for level in levels.get(leg, []):
            gap = abs(exit_price - level)
            if gap <= abs(level) * EXIT_MATCH and (best is None or (gap, i) < (best[0], best[1])):
                best = (gap, i, leg)
    return "CLOSED" if best is None else best[2]


def _trade(lc: Lifecycle, now_ms: int) -> Trade:
    close_ev = next((e for e in reversed(lc.events) if e.kind == "CLOSED"), None)
    exit_price: Decimal | None = None
    if lc.close is not None:
        exit_ms = lc.close.t_ms
        exit_price = close_ev.price if close_ev is not None else None
    elif lc.outcome == "OPEN":
        exit_ms = now_ms
    else:
        exit_ms = max(lc.last_ms, lc.fill.t_ms if lc.fill is not None else lc.last_ms)
    entry_price: Decimal | None
    kind: EntryKind
    if lc.fill is not None:
        entry_ms, entry_price = lc.fill.t_ms, lc.fill.price
        kind = "SEEN_OPEN" if lc.fill_seen_only else "FILLED"
        if lc.close is not None and entry_ms >= exit_ms and lc.fill_source == "REST":
            kind = "NOT_RECORDED"
    else:
        entry_ms, entry_price, kind = exit_ms, None, "NOT_RECORDED"
        if lc.position:
            entry_price = lc.position[0].price
    entry_ms = min(entry_ms, exit_ms)
    ek: ExitKind
    levels: dict[Leg, list[Decimal]] = {"SL": [], "TP": []}
    if lc.close is not None:
        levels = levels_at_exit(lc, exit_ms)
        ek = exit_kind(exit_price, levels)
    else:
        ek = "OPEN" if lc.outcome == "OPEN" else "UNKNOWN"
    if entry_ms < exit_ms:
        sl, tp = _clip(lc.stop_loss, entry_ms, exit_ms), _clip(lc.take_profit, entry_ms, exit_ms)
    else:
        # no time span (the entry time is not recorded): the levels resting at the exit, one step each
        sl = tuple(Step(entry_ms, exit_ms, p) for p in levels["SL"][-1:])
        tp = tuple(Step(entry_ms, exit_ms, p) for p in levels["TP"][-1:])
    working = _clip(lc.working, lc.working[0].start_ms, entry_ms) if lc.working else ()
    events = lc.events
    if kind == "NOT_RECORDED":
        # the fill row carries the close time (catch-up), not the fill time: listing it as 'filled <time>' would
        # state a fill time nobody recorded (the entry label says 'entry time not recorded @ <price>')
        events = tuple(e for e in events if e.kind not in ("FILLED", "OPENED"))
    ids = {i for i in (lc.order_id, *lc.children) if i}
    return Trade(
        key=lc.key, number=0, adapter_id=lc.adapter_id, symbol=lc.symbol, side=lc.side, order_id=lc.order_id,
        position_id=lc.position_id, order_ids=tuple(sorted(ids)), entry_ms=entry_ms, entry_price=entry_price,
        entry_kind=kind, placed=lc.placed, working=working, stop_loss=sl, take_profit=tp, exit_ms=exit_ms,
        exit_price=exit_price, exit_kind=ek, events=events, first_ms=lc.first_ms, last_ms=lc.last_ms)


def _pending(lc: Lifecycle, now_ms: int) -> PendingOrder:
    price: Decimal | None = lc.working[-1].price if lc.working else None
    if price is None:
        price = next((e.price for e in reversed(lc.events) if e.price is not None and e.kind in
                      ("PLACED", "PRICE_CHANGED", "CANCELLED", "REJECTED", "FILLED")), None)
    if lc.ended is not None:
        end_ms = lc.ended.t_ms
    elif lc.outcome in WORKING_STATUSES:
        end_ms = now_ms
    else:
        end_ms = lc.last_ms
    levels: dict[Leg, Decimal | None] = {"SL": lc.stop_loss[-1].price if lc.stop_loss else None,
                                         "TP": lc.take_profit[-1].price if lc.take_profit else None}
    for g in lc.legs:
        if g.role is not None and levels[g.role] is None and g.price is not None:
            levels[g.role] = g.price
    return PendingOrder(
        key=lc.key, adapter_id=lc.adapter_id, symbol=lc.symbol, side=lc.side, order_id=lc.label_id, price=price,
        steps=lc.working, first_ms=lc.first_ms, end_ms=max(end_ms, lc.first_ms), placed_seen=lc.placed is not None,
        outcome="FILLED" if lc.reduce_only else lc.outcome, stop_loss=levels["SL"], take_profit=levels["TP"],
        legs=lc.children, events=lc.events)


def is_trade(lc: Lifecycle) -> bool:
    """A position (or a filled opening order): not an order that never filled, nor a closing fill alone."""
    if lc.reduce_only:
        return False
    return lc.position_id is not None or lc.fill is not None or lc.close is not None


def build_trades(lifecycles: Iterable[Lifecycle], now_ms: int
                 ) -> tuple[tuple[Trade, ...], tuple[PendingOrder, ...]]:
    """(trades numbered T1..Tn per normalised symbol in entry-time order, pending-only orders). Both newest first
    (trades by entry time, pending orders by their end), then by key."""
    trades: list[Trade] = []
    pending: list[PendingOrder] = []
    for lc in lifecycles:
        if is_trade(lc):
            trades.append(_trade(lc, now_ms))
        else:
            pending.append(_pending(lc, now_ms))
    numbered: list[Trade] = []
    by_symbol: dict[str, list[Trade]] = {}
    for t in trades:
        by_symbol.setdefault(normalise_symbol(t.symbol), []).append(t)
    for sym in sorted(by_symbol):
        for n, t in enumerate(sorted(by_symbol[sym], key=lambda t: (t.entry_ms, t.key)), 1):
            numbered.append(replace(t, number=n))
    numbered.sort(key=lambda t: (-t.entry_ms, t.key))
    pending.sort(key=lambda p: (-p.end_ms, p.key))
    return tuple(numbered), tuple(pending)
