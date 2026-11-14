"""StoneX order / trade history -> OrderDTO / PositionDTO for ``fetch_history`` (the catch-up read after the
engine was off; adapter-isolation.md §2 StoneX, dto-contracts.md §3, §5). Pure mapping, no I/O.

Inputs, all read after one another in this order, so the later reads are the newer truth:
1. ``order/tradehistory`` rows: executed trades. A row whose ``OpeningOrderIds`` name another order (or that
   carries a realised P&L) is a closing trade: a reduce-only FILLED order, and each opening order it names is a
   position it reduced or closed. Any other row is an opening trade: a FILLED order whose position is
   ``position_id = OrderId``. Live finding 2026-11-14 (read-only probe, 5 of 5 trades): StoneX lists an opening
   trade with ``OpeningOrderIds = [its own OrderId]`` and no ``RealisedPnl``; the own id is not a link, so such a
   row is an opening trade (it was taken for a closing trade without P&L before).
2. ``order/stoplimitorderhistory`` rows: stop / limit orders that ended (cancelled, filled), entries and the
   stop-loss / take-profit children of a position or of a pending entry alike.
3. ``order/openpositions`` (and the active stop / limit orders, mapped by the adapter): what is open now.
A position named by the history is OPEN when it is still in (3), and CLOSED only when a closing trade names it
and it is no longer open; an opening trade whose position is neither open nor named by a closing trade proves
nothing and makes the answer incomplete (never closed on absence alone).

Versions: a row is placed in the window by its last change (``LastChangedDateTimeUtc``, else
``ExecutedDateTimeUtc``). The FILLED order a trade row maps to carries its EXECUTION time
(``ExecutedDateTimeUtc``, else the last change): live finding 2026-11-14, an opening trade's last change moves to
the time its position closes, so using it stamped the entry fill with the close time. A closed position carries
its closing trade's time. A row without any time cannot be placed in the window and is unmappable.

A stop / limit history row also yields its placement: when ``CreatedDateTimeUtc`` is known and earlier than the
row's end (its last change, or the trade history's fill time when the order filled), a WORKING version at
``CreatedDateTimeUtc`` precedes the terminal one, so the time a stop-loss / take-profit was placed is kept. Its
level is the row's ``TriggerPrice`` / ``Price``: StoneX keeps no amendment trail, so an order moved before it ended
shows its last level from its placement on. One order can therefore yield several versions (``order_versions``).

Parents (``stop_limit_parents``; live probe 2026-11-14, 36 stop / limit rows, 12 brackets): ``OrderReference`` and
``TradeReference`` are empty on both histories and ``OrderApplicabilityId`` is one constant, so no reference field
links a child to its parent. In order of evidence:
1. ``ParentOrderId`` on the row, when present;
2. the trade history: a child that filled is listed there as a closing trade whose ``OpeningOrderIds`` name the
   position it closed (the opening order id);
3. what the adapter already knows of the order (its stream mapper's parent memo);
4. the bracket rule, a fallback that held for every bracket of the probe: a StoneX bracket is entry X, stop-loss
   X+1 and take-profit X+2, created in the same second. A row C is the child of X = C-1 or C-2 only when the whole
   bracket is there: X, X+1 and X+2 are all rows of the stop / limit history, X is not itself a child, all three
   share the account, market and created whole second, X+1 and X+2 are both on the side opposite X, neither of them
   (nor C) is an opening fill in the trade history, and exactly one such X exists.
Anything else stays unlinked: a stand-alone stop / limit entry has no parent; two entries placed together (an OCO
straddle: opposite sides, same second, adjacent ids) are never parent and child; a bracket with one leg only, or one
leg still working, is not proven; and the children of a market-order entry cannot be told apart from stand-alone
orders (not seen in the probe). Unlinked rows are listed in ``HistoryMapping.unlinked`` (ids only) and never
guessed.

A placement version is WORKING, or PENDING when the order ended in a PENDING status (StoneX 1 / 8 / 10): WORKING ->
PENDING is an illegal transition.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal

from src.adapters.stonex.codes import opposite, parse_side
from src.adapters.stonex.instruments import ADAPTER_ID, to_units, unknown_symbol
from src.adapters.stonex.mapping import SpecLookup, UnmappableError, map_open_position, map_stop_limit_order, parent_of
from src.adapters.stonex.wire import JsonObj, get_ci, parse_broker_time_ms, to_decimal, to_id
from src.dto import OrderDTO, OrderStatus, PositionDTO, Side, is_legal_transition

#: (client account id, raw row) pairs, one per trading-account read.
Rows = Sequence[tuple[str, JsonObj]]
#: (market id, side, entry price) of a position the adapter already knows (its stream mapper's memo), else None.
KnownPosition = Callable[[str], tuple[str | None, Side, Decimal | None] | None]


def _unknown_position(_pid: str) -> None:
    return None


def row_time_ms(row: JsonObj) -> int | None:
    """The broker time of a history row's last change (LastChangedDateTimeUtc, else ExecutedDateTimeUtc)."""
    return (parse_broker_time_ms(get_ci(row, "LastChangedDateTimeUtc", "LastChangedDateTimeUTC"))
            or parse_broker_time_ms(get_ci(row, "ExecutedDateTimeUtc", "ExecutedDateTimeUTC")))


def opening_ids(row: JsonObj) -> list[str]:
    """The orders whose positions this trade reduced or closed. The row's own OrderId is never one: StoneX lists an
    opening trade with ``OpeningOrderIds = [its own id]`` (live 2026-11-14)."""
    raw = get_ci(row, "OpeningOrderIds")
    values = raw if isinstance(raw, list) else [raw] if raw is not None else []
    own = to_id(get_ci(row, "OrderId"))
    out: list[str] = []
    for value in values:
        oid = to_id(value)
        if oid is not None and oid != own and oid not in out:
            out.append(oid)
    return out


def is_closing_trade(row: JsonObj) -> bool:
    return bool(opening_ids(row)) or to_decimal(get_ci(row, "RealisedPnl")) is not None


def row_key(row: JsonObj) -> tuple[str | None, int | None]:
    """Dedupe key across overlapping pages: (OrderId, broker time)."""
    return to_id(get_ci(row, "OrderId")), row_time_ms(row)


@dataclass
class HistoryMapping:
    """The mapped window plus why it may be incomplete. ``unmapped`` reasons are counted by the adapter.
    ``notes``: evidence that did not add up between the reads (a re-read may answer: retryable). ``limits``: a
    property of the broker's rows themselves that every re-read of the window repeats (a broker limit, never
    retryable: holding coverage back for it would re-read the same rows forever).
    ``orders`` holds each order's newest version; ``earlier`` its older ones (a stop / limit order's placement),
    ``order_versions`` all of them. ``unlinked``: ids of stop / limit history rows no rule gave a parent (a
    stand-alone order, or a child whose parent the history cannot prove); informational, never an incompleteness."""

    orders: dict[str, OrderDTO] = field(default_factory=dict)
    positions: dict[str, PositionDTO] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    unmapped: list[str] = field(default_factory=list)
    limits: list[str] = field(default_factory=list)
    earlier: dict[str, list[OrderDTO]] = field(default_factory=dict)
    unlinked: list[str] = field(default_factory=list)

    def add_order(self, dto: OrderDTO) -> None:
        current = self.orders.get(dto.broker_order_id)
        if current is None or current.broker_updated_at_ms <= dto.broker_updated_at_ms:
            self.orders[dto.broker_order_id] = dto

    def add_earlier(self, dto: OrderDTO) -> None:
        """An older version of an order (its placement): kept only while older than the order's newest version."""
        versions = self.earlier.setdefault(dto.broker_order_id, [])
        if all(v.broker_updated_at_ms != dto.broker_updated_at_ms for v in versions):
            versions.append(dto)

    def order_versions(self) -> list[OrderDTO]:
        """Every version of every order, by broker time (then id): the versions of one order oldest first."""
        out = list(self.orders.values())
        for oid, versions in self.earlier.items():
            newest = self.orders.get(oid)
            out += [v for v in versions if newest is None or v.broker_updated_at_ms < newest.broker_updated_at_ms]
        return sorted(out, key=lambda o: (o.broker_updated_at_ms, o.broker_order_id))

    def add_position(self, dto: PositionDTO) -> None:
        """One source decides each position (open now -> the open row; else its latest closing trade), so a
        repeated touch maps the same row again."""
        self.positions[dto.position_id] = dto


@dataclass(frozen=True)
class _Trade:
    client: str
    row: JsonObj
    order_id: str
    side: Side
    ts: int
    market_id: str | None
    openings: tuple[str, ...]
    closing: bool
    executed_ms: int | None = None    # ExecutedDateTimeUtc: when the trade filled (ts may be a later change)

    @property
    def filled_ms(self) -> int:
        return self.executed_ms if self.executed_ms is not None else self.ts


def _parse_trades(rows: Rows, out: HistoryMapping) -> list[_Trade]:
    trades: list[_Trade] = []
    seen: set[tuple[str | None, int | None]] = set()
    for client, row in rows:
        key = row_key(row)
        if key in seen:
            continue
        seen.add(key)
        order_id, ts = key
        side = parse_side(get_ci(row, "Direction"))
        missing = [n for n, bad in (("OrderId", order_id is None), ("Direction", side is None),
                                    ("a broker time", ts is None)) if bad]
        if missing:
            out.unmapped.append(f"trade history row {order_id or '?'} without {', '.join(missing)}")
            continue
        assert order_id is not None and side is not None and ts is not None
        executed = parse_broker_time_ms(get_ci(row, "ExecutedDateTimeUtc", "ExecutedDateTimeUTC"))
        trades.append(_Trade(client, row, order_id, side, ts, to_id(get_ci(row, "MarketId")),
                             tuple(opening_ids(row)), is_closing_trade(row), executed))
    return trades


def _trade_order(t: _Trade, lookup: SpecLookup, *, reduce_only: bool, linked: str | None,
                 adapter_id: str) -> OrderDTO:
    spec = lookup(t.market_id)
    qty = to_decimal(get_ci(t.row, "OriginalQuantity")) or to_decimal(get_ci(t.row, "Quantity", "Qauantity"))
    u = to_units(spec, qty)
    price = to_decimal(get_ci(t.row, "Price", "ExecutionPrice"))
    return OrderDTO(
        client_order_id=t.order_id, broker_order_id=t.order_id, account_id=t.client, adapter_id=adapter_id,
        symbol=spec.symbol if spec else unknown_symbol(t.market_id), side=t.side, status=OrderStatus.FILLED,
        entry_price=price if price else None,
        ordered_units=u.units, remaining_units=None if u.instrument_unknown else Decimal(0),
        instrument_unknown=u.instrument_unknown, raw_qty=u.raw_qty,
        is_reduce_only=reduce_only, linked_position_id=linked if reduce_only else None,
        broker_updated_at_ms=t.filled_ms, source="REST",
    )


def _closed_position(pid: str, closing: _Trade, opening: _Trade | None, lookup: SpecLookup,
                     adapter_id: str, known: KnownPosition = _unknown_position) -> PositionDTO:
    """A position a closing trade names and that is no longer open: CLOSED at the closing trade's broker time.
    Side, market and entry come from its opening trade when the window holds it, else from what the adapter
    already knows of the position (so a close the stream also reported keeps its entry price), else: the
    opposite side of the closing trade, its market, no entry price."""
    held = known(pid) if opening is None else None
    if opening is not None:
        market_id, side = opening.market_id, opening.side
        entry = to_decimal(get_ci(opening.row, "Price", "ExecutionPrice"))
    elif held is not None:
        market_id, side, entry = held[0] or closing.market_id, held[1], held[2]
    else:
        market_id, side, entry = closing.market_id, opposite(closing.side), None
    spec = lookup(market_id)
    u = to_units(spec, Decimal(0))
    return PositionDTO(
        position_id=pid, account_id=closing.client, adapter_id=adapter_id,
        symbol=spec.symbol if spec else unknown_symbol(market_id), side=side,
        units=u.units, instrument_unknown=u.instrument_unknown, raw_qty=Decimal(0),
        entry_price=entry if entry else None, status="CLOSED", opened_by_client_order_id=pid,
        broker_updated_at_ms=closing.filled_ms, source="REST",
    )


def map_trade_history_window(trade_rows: Rows, open_positions: Rows, *, since_ms: int, lookup: SpecLookup,
                             adapter_id: str = ADAPTER_ID, out: HistoryMapping | None = None,
                             known: KnownPosition = _unknown_position) -> HistoryMapping:
    """Trade history rows of the window (+ the open positions read after them) -> orders and positions."""
    out = out if out is not None else HistoryMapping()
    trades = _parse_trades(trade_rows, out)
    open_by_id: dict[str, tuple[str, JsonObj]] = {}
    for client, row in open_positions:
        pid = to_id(get_ci(row, "OrderId"))
        if pid is not None:
            open_by_id[pid] = (client, row)
    openings = {t.order_id: t for t in trades if not t.closing}
    closed_by: dict[str, _Trade] = {}
    for t in trades:
        for pid in t.openings:
            if pid not in closed_by or closed_by[pid].ts <= t.ts:
                closed_by[pid] = t

    def touch_position(pid: str) -> None:
        held = open_by_id.get(pid)
        if held is not None:
            try:
                out.add_position(map_open_position(held[1], account_id=held[0], lookup=lookup,
                                                   adapter_id=adapter_id))
            except UnmappableError as exc:
                out.unmapped.append(f"open position {pid}: {exc}")
            return
        closing = closed_by.get(pid)
        if closing is not None:
            out.add_position(_closed_position(pid, closing, openings.get(pid), lookup, adapter_id, known))
            return
        why = f"position {pid} opened in the window is not open and no closing trade names it"
        if unnamed_closes:
            # A closing trade of the window names no opening order: it may be this position's close, and every
            # re-read returns the same row (a broker data limit, not a race between the reads).
            out.limits.append(why)
        else:
            out.notes.append(why)       # closed after the trade history was read: a re-read names the close

    unnamed_closes = any(t.closing and not t.openings for t in trades if t.ts >= since_ms)
    for t in trades:
        if t.ts < since_ms:
            continue                       # read only to place the window's rows (from= is in whole seconds)
        if t.closing:
            out.add_order(_trade_order(t, lookup, reduce_only=True,
                                       linked=t.openings[0] if len(t.openings) == 1 else None,
                                       adapter_id=adapter_id))
            if not t.openings:
                # The row itself lacks OpeningOrderIds: every re-read returns the same row (a broker limit).
                out.limits.append(f"closing trade {t.order_id} names no opening order: the position it reduced "
                                 "is unknown")
            for pid in t.openings:
                touch_position(pid)
        else:
            out.add_order(_trade_order(t, lookup, reduce_only=False, linked=None, adapter_id=adapter_id))
            touch_position(t.order_id)
    return out


def _created_ms(row: JsonObj) -> int | None:
    return parse_broker_time_ms(get_ci(row, "CreatedDateTimeUtc", "CreatedDateTimeUTC"))


def _num(order_id: str) -> int | None:
    return int(order_id) if order_id.isdigit() and len(order_id) < 19 else None


def stop_limit_parents(rows: Rows, trade_rows: Rows = (), *,
                       known_parent: Callable[[str], str | None] = lambda _oid: None
                       ) -> tuple[dict[str, str], list[str]]:
    """(child order id -> parent order id, ids left unlinked) for the stop / limit history rows, by the rules of the
    module docstring: ParentOrderId, then the trade history's OpeningOrderIds of a child that filled, then the
    adapter's own memo, then the same-second bracket rule (X+1 / X+2 of X), which never links when more than one
    order qualifies."""
    by_id: dict[str, tuple[str, JsonObj]] = {}
    for client, row in rows:
        oid = to_id(get_ci(row, "OrderId"))
        if oid is not None and oid not in by_id:
            by_id[oid] = (client, row)
    parents: dict[str, str] = {}
    for oid, (_client, row) in by_id.items():
        pid = parent_of(row)
        if pid is not None and pid != oid:
            parents[oid] = pid
    for _client, row in trade_rows:
        oid = to_id(get_ci(row, "OrderId"))
        names = opening_ids(row)
        if oid in by_id and oid not in parents and len(names) == 1:
            parents[oid] = names[0]          # a child that filled closed exactly this position
    for oid in by_id:
        if oid not in parents:
            pid = known_parent(oid)
            if pid is not None and pid != oid:
                parents[oid] = pid
    # A trade-history row that is no closing trade is an opening fill: that order is an entry, never a child (an
    # OCO straddle's legs are on opposite sides, placed in the same second with adjacent ids, like a bracket).
    entries = {oid for _client, row in trade_rows
               if (oid := to_id(get_ci(row, "OrderId"))) is not None and not is_closing_trade(row)}

    def shape(oid: str) -> tuple[str, str | None, Side, int] | None:
        held = by_id.get(oid)
        if held is None:
            return None
        side, created = parse_side(get_ci(held[1], "Direction")), _created_ms(held[1])
        if side is None or created is None:
            return None
        return held[0], to_id(get_ci(held[1], "MarketId")), side, created // 1000

    def bracket_head(x: int) -> bool:
        """X, X+1, X+2 all in the stop / limit history, one account, market and created second, X+1 and X+2 on the
        side opposite X and neither an opening fill: the only shape the live probe showed (12 of 12 brackets).
        Anything less (one leg only, a straddle, a leg still working) is not proof and stays unlinked."""
        head, sl, tp = shape(str(x)), shape(str(x + 1)), shape(str(x + 2))
        if head is None or sl is None or tp is None or str(x + 1) in entries or str(x + 2) in entries:
            return False
        want = (head[0], head[1], opposite(head[2]), head[3])
        return sl == want and tp == want

    unlinked: list[str] = []
    for oid in sorted(by_id, key=lambda i: (_num(i) is None, _num(i) or 0, i)):
        if oid in parents:
            continue
        n = _num(oid)
        found: list[str] = []
        if n is not None and oid not in entries:
            for k in (1, 2):
                pid = str(n - k)
                if pid in parents:
                    continue                  # itself a child: never a parent
                if bracket_head(n - k):
                    found.append(pid)
        if len(found) == 1:
            parents[oid] = found[0]
        else:
            unlinked.append(oid)
    heads = set(parents.values())
    return parents, [oid for oid in unlinked if oid not in heads]     # an entry with children is not unlinked


def _placed(dto: OrderDTO, created_ms: int, end: OrderStatus) -> OrderDTO:
    """The version of a stop / limit order at its placement (nothing filled yet): WORKING, or PENDING when the order
    ended in a status WORKING may not move to (StoneX 1 / 8 / 10 map to PENDING; WORKING -> PENDING is an illegal
    transition, so a WORKING placement would park the order's end in the DLQ)."""
    status = OrderStatus.WORKING if is_legal_transition(OrderStatus.WORKING, end) else OrderStatus.PENDING
    return replace(dto, status=status, broker_updated_at_ms=created_ms,
                   remaining_units=None if dto.instrument_unknown else dto.ordered_units)


def map_stop_limit_history(rows: Rows, *, since_ms: int, until_ms: int, lookup: SpecLookup,
                           known_parent: Callable[[str], str | None], adapter_id: str = ADAPTER_ID,
                           out: HistoryMapping | None = None, trade_rows: Rows = ()) -> HistoryMapping:
    """Ended stop / limit orders whose last change lies in the window -> OrderDTO versions: a WORKING one at
    ``CreatedDateTimeUtc`` (when known and earlier than the end) and the terminal one at the last change (status from
    ``StatusId``). A row the trade history already mapped (a stop / limit that filled) keeps the trade history's
    FILLED row (at its execution time) as its terminal version. Children (``stop_limit_parents``, with
    ``trade_rows`` the trade history read) are reduce-only with ``linked_position_id`` = the parent order id."""
    out = out if out is not None else HistoryMapping()
    traded = dict(out.orders)
    parents, unlinked = stop_limit_parents(rows, trade_rows, known_parent=known_parent)
    for client, row in rows:
        order_id = to_id(get_ci(row, "OrderId"))
        ts = row_time_ms(row)
        if order_id is None or ts is None:
            out.unmapped.append(f"stop/limit history row {order_id or '?'} without OrderId / a broker time")
            continue
        if ts < since_ms or ts > until_ms:
            continue
        normalised = {**row,
                      "Status": get_ci(row, "Status", "StatusId", "OrderStatusId"),
                      "Quantity": get_ci(row, "Quantity", "OriginalQuantity", "Qauantity"),
                      "LastChangedDateTimeUtc": ts}
        try:
            dto = map_stop_limit_order(normalised, account_id=client, lookup=lookup,
                                       parent_id=parents.get(order_id), strict_status=True, adapter_id=adapter_id)
        except UnmappableError as exc:
            out.unmapped.append(f"stop/limit history row {order_id}: {exc}")
            continue
        if order_id in unlinked and order_id not in out.unlinked:
            out.unlinked.append(order_id)
        filled = traded.get(order_id)
        end = filled.broker_updated_at_ms if filled is not None else ts
        created = _created_ms(row)
        if created is not None and created < end:
            out.add_earlier(_placed(dto, created, filled.status if filled is not None else dto.status))
        if filled is None:
            out.add_order(dto)
    return out


def market_ids(rows: Iterable[tuple[str, JsonObj]]) -> set[str | None]:
    return {to_id(get_ci(r, "MarketId")) for _, r in rows}
