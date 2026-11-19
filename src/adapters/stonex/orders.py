"""StoneX ORDERS stream -> OrderDTO / PositionDTO (adapter-isolation.md §2 StoneX, dto-contracts.md §5).

- The first item of a non-child order reporting Open(3) emits OrderDTO(FILLED) and PositionDTO(OPEN)
  (position_id = OrderId). Later items for that id map to the PositionDTO only (the order is terminal):
  a reduced Quantity is a partial close; Closed(9) or Quantity 0 closes the position.
- An item reaching Closed(9) with no prior Open is a closing trade: reduce-only, never a position.
- If-done stop / limit children stay separate reduce-only OrderDTOs with linked_position_id = parent
  OrderId. The parent is learned from REST (ParentOrderId, StopOrder / LimitOrder of a position,
  IfDone) or, failing that, from XTNL's proven rule: a Pending(1) stop / limit is a bracket whose
  LastChangedTime equals its parent's first LastChangedTime.
- Versions: ORDER and POSITION carry the item's LastChangedTime (broker-time domain).
- ``order_kind`` (design PR1) comes from the memo's order type: an item without ``Type`` keeps the type last
  seen for that order (from an earlier item or a REST read), so a stop never loses its kind; a Trade has none.
  ``ORDER_FIELDS`` is unchanged (an unknown field name would fail the whole subscription).
- IN-7 (open, decided by the PR3 probe): an item with ``OpenPrice`` empty re-emits the order's last known level.
- K8 (adapter-isolation.md §2 StoneX): the PositionDTO(OPEN) of the first filled item takes its entry price from
  REST (``GET order/{id}/openposition``, else ``GET order/{id}``), read in the background by the adapter
  (``needs_entry_lookup`` / ``learn_entry``) while the item is held; a REST entry is never overwritten by a later
  item's ``OpenPrice``. Without a REST answer (failure, timeout, too many held) the stream price stands in until a
  later REST ``openpositions`` read calls ``learn_position``; the next item of that order then carries the REST entry.
"""
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import TypeVar

from src.adapters.stonex.codes import (
    CHILD_TYPES,
    StoneXOrderStatus,
    StoneXOrderType,
    canonical_status,
    opposite,
    order_kind_of,
    parse_order_type,
    parse_side,
    parse_status,
)
from src.adapters.stonex.instruments import ADAPTER_ID, to_units, unknown_symbol
from src.adapters.stonex.mapping import SpecLookup, UnmappableError
from src.adapters.stonex.wire import parse_broker_time_ms, to_decimal, to_id, to_int
from src.dto import OrderDTO, OrderStatus, PositionDTO, Side

# XTNL's proven ORDERS field list: an unknown field name fails the whole subscription.
ORDER_FIELDS: tuple[str, ...] = (
    "OrderId", "Direction", "Type", "ExecutionPrice", "LastChangedTime", "Status", "Quantity",
    "OpenPrice", "ReasonId", "MarketId", "TradingAccountId", "OriginalQuantity",
)
MEMO_CAP = 20_000

K = TypeVar("K")
V = TypeVar("V")


def _remember(store: OrderedDict[K, V], key: K, value: V, cap: int = MEMO_CAP) -> V:
    store[key] = value
    store.move_to_end(key)
    while len(store) > cap:
        store.popitem(last=False)
    return value


def _nonzero(value: Decimal | None) -> Decimal | None:
    return value if value is not None and value != 0 else None


@dataclass(frozen=True)
class OrderItem:
    """One ORDERS frame, parsed. Absent / malformed fields are None."""

    order_id: str
    market_id: str | None
    trading_account_id: str | None
    side: Side | None
    order_type: StoneXOrderType | None
    status: StoneXOrderStatus | None
    quantity: Decimal | None
    original_quantity: Decimal | None
    open_price: Decimal | None
    execution_price: Decimal | None
    last_changed_ms: int | None
    reason_id: int | None

    @classmethod
    def from_frame(cls, values: Mapping[str, str | None]) -> OrderItem | None:
        order_id = to_id(values.get("OrderId"))
        if order_id is None:
            return None
        return cls(
            order_id=order_id,
            market_id=to_id(values.get("MarketId")),
            trading_account_id=to_id(values.get("TradingAccountId")),
            side=parse_side(values.get("Direction")),
            order_type=parse_order_type(values.get("Type")),
            status=parse_status(values.get("Status")),
            quantity=to_decimal(values.get("Quantity")),
            original_quantity=to_decimal(values.get("OriginalQuantity")),
            open_price=_nonzero(to_decimal(values.get("OpenPrice"))),
            execution_price=_nonzero(to_decimal(values.get("ExecutionPrice"))),
            last_changed_ms=parse_broker_time_ms(values.get("LastChangedTime")),
            reason_id=to_int(values.get("ReasonId")),
        )


@dataclass
class _OrderMemo:
    market_id: str | None = None
    trading_account_id: str | None = None
    side: Side | None = None
    order_type: StoneXOrderType | None = None
    quantity: Decimal | None = None
    original_quantity: Decimal | None = None
    price: Decimal | None = None
    bracket: bool = False      # seen as a Pending stop / limit (if-done child evidence)
    closing: bool = False      # a closing trade (reduce-only)
    terminal: bool = False     # its OrderDTO reached a terminal status


@dataclass
class _PositionMemo:
    market_id: str | None
    side: Side
    entry_price: Decimal | None
    is_open: bool = True
    rest_entry: bool = False   # entry_price came from REST (K8): a later item's OpenPrice never overwrites it


Emitted = OrderDTO | PositionDTO


class OrderStreamMapper:
    """Stateful ORDERS mapper. Not thread-safe: use it from the event loop only."""

    def __init__(self, *, lookup: SpecLookup, account_for: Callable[[str | None], str | None],
                 adapter_id: str = ADAPTER_ID) -> None:
        self._lookup = lookup
        self._account_for = account_for
        self._adapter_id = adapter_id
        self._orders: OrderedDict[str, _OrderMemo] = OrderedDict()
        self._positions: OrderedDict[str, _PositionMemo] = OrderedDict()
        self._parents: OrderedDict[str, str] = OrderedDict()
        self._standalone: OrderedDict[str, bool] = OrderedDict()
        self._looked_up: OrderedDict[str, bool] = OrderedDict()
        self._entry_looked_up: OrderedDict[str, bool] = OrderedDict()
        self._rest_entry: OrderedDict[str, Decimal] = OrderedDict()   # K8: REST entry price per OrderId
        self._first_ts: OrderedDict[tuple[str | None, int], str] = OrderedDict()
        self.watermark_ms: int | None = None

    # --- knowledge from REST snapshots ---------------------------------------------------
    def learn_parent(self, child_id: str, parent_id: str | None) -> None:
        """parent_id None means: known to be a standalone (entry) order."""
        if parent_id is not None and parent_id != child_id:
            _remember(self._parents, child_id, parent_id)
            self._standalone.pop(child_id, None)
        elif child_id not in self._parents:
            _remember(self._standalone, child_id, True)

    def learn_position(self, position: PositionDTO, market_id: str | None) -> None:
        """An open position from REST: later items for it map to the PositionDTO only."""
        if position.status != "OPEN":
            return
        _remember(self._positions, position.position_id,
                  _PositionMemo(market_id=market_id, side=position.side, entry_price=position.entry_price,
                                rest_entry=position.entry_price is not None))
        memo = self._orders.get(position.position_id) or _OrderMemo()
        memo.market_id = memo.market_id or market_id
        memo.side = memo.side or position.side
        memo.quantity = memo.quantity if memo.quantity is not None else position.raw_qty
        memo.terminal = True
        _remember(self._orders, position.position_id, memo)

    def learn_order(self, order: OrderDTO, market_id: str | None) -> None:
        memo = self._orders.get(order.broker_order_id) or _OrderMemo()
        memo.market_id = memo.market_id or market_id
        memo.side = order.side
        if order.raw_qty is not None:
            memo.quantity = order.raw_qty
        memo.price = order.entry_price or memo.price
        if order.order_kind is not None:
            memo.order_type = StoneXOrderType[order.order_kind]
        memo.terminal = memo.terminal or order.status.terminal
        _remember(self._orders, order.broker_order_id, memo)
        self.learn_parent(order.broker_order_id, order.linked_position_id if order.is_reduce_only else None)

    def parent_of(self, order_id: str) -> str | None:
        return self._parents.get(order_id)

    def known_position(self, position_id: str) -> tuple[str | None, Side, Decimal | None] | None:
        """(market id, side, entry price) of a position this process has seen (stream or REST), else None."""
        pos = self._positions.get(position_id)
        return None if pos is None else (pos.market_id, pos.side, pos.entry_price)

    def is_closing(self, order_id: str) -> bool:
        memo = self._orders.get(order_id)
        return memo is not None and memo.closing

    def needs_parent_lookup(self, item: OrderItem) -> bool:
        """A stop / limit order whose parent relation is not known yet (one REST read each)."""
        memo = self._orders.get(item.order_id)
        order_type = item.order_type or (memo.order_type if memo else None)
        return (order_type in CHILD_TYPES
                and item.order_id not in self._parents
                and item.order_id not in self._standalone
                and item.order_id not in self._looked_up
                and item.order_id not in self._positions
                and not (memo is not None and memo.closing))

    def needs_side_lookup(self, item: OrderItem) -> bool:
        """An order seen without Direction whose side is not known yet (live finding 2026-11-13: StoneX can send
        an order's first ORDERS item with Direction empty). One background REST read each, like the parent read."""
        memo = self._orders.get(item.order_id)
        return (item.side is None and (memo is None or memo.side is None)
                and item.order_id not in self._looked_up)

    def side_known(self, order_id: str) -> bool:
        memo = self._orders.get(order_id)
        return memo is not None and memo.side is not None

    def learn_side(self, order_id: str, side: Side | None) -> None:
        """The side from a by-id REST read; never overrides a side the stream already reported."""
        if side is None:
            return
        memo = self._orders.get(order_id) or _OrderMemo()
        memo.side = memo.side or side
        _remember(self._orders, order_id, memo)

    def mark_looked_up(self, order_id: str) -> None:
        _remember(self._looked_up, order_id, True)

    def needs_entry_lookup(self, item: OrderItem) -> bool:
        """K8: an item reporting Open(3) for an order with no position yet (its first fill emits PositionDTO(OPEN)),
        that is not a known if-done child or closing trade: its entry price is read from REST (one background read
        each, like the parent read)."""
        if item.status != StoneXOrderStatus.OPEN:
            return False
        memo = self._orders.get(item.order_id)
        return (item.order_id not in self._positions
                and item.order_id not in self._parents
                and item.order_id not in self._entry_looked_up
                and item.order_id not in self._rest_entry
                and not (memo is not None and (memo.closing or memo.bracket)))

    def mark_entry_looked_up(self, order_id: str) -> None:
        _remember(self._entry_looked_up, order_id, True)

    def learn_entry(self, order_id: str, price: Decimal | None) -> None:
        """The entry price of a filled order read from REST (K8); taken by its first PositionDTO(OPEN), or set on a
        position this mapper already opened from the stream price."""
        if price is None or price <= 0:
            return
        pos = self._positions.get(order_id)
        if pos is not None:
            pos.entry_price, pos.rest_entry = price, True
            return
        _remember(self._rest_entry, order_id, price)

    # --- mapping -----------------------------------------------------------------------------
    def map(self, item: OrderItem) -> list[Emitted]:
        memo = self._absorb(item)
        ts = item.last_changed_ms or 0
        if item.last_changed_ms is not None:
            self.watermark_ms = max(self.watermark_ms or 0, item.last_changed_ms)
        code = item.status
        if code is None:
            raise UnmappableError(f"ORDERS item {item.order_id} without a known Status")
        if memo.side is None:
            raise UnmappableError(f"ORDERS item {item.order_id} without Direction")
        account_id = self._account_for(memo.trading_account_id)
        if account_id is None:
            raise UnmappableError(f"ORDERS item {item.order_id}: unknown trading account")

        is_child, parent = self._child_parent(item, memo, code)
        if is_child:
            status = canonical_status(code)
            return [self._order(item.order_id, memo, account_id, status, ts, reduce_only=True, linked=parent)]

        pos = self._positions.get(item.order_id)
        if code == StoneXOrderStatus.OPEN:
            return self._on_open(item, memo, pos, account_id, ts)
        if code == StoneXOrderStatus.CLOSED:
            return self._on_closed(item, memo, pos, account_id, ts)
        if pos is not None and memo.terminal:
            return []   # position-only once the order is terminal; nothing to update
        status = canonical_status(code)
        out: list[Emitted] = [self._order(item.order_id, memo, account_id, status, ts,
                                          reduce_only=memo.closing, linked=None)]
        memo.terminal = memo.terminal or status.terminal
        return out

    def _absorb(self, item: OrderItem) -> _OrderMemo:
        memo = self._orders.get(item.order_id)
        if memo is None:
            memo = _OrderMemo()
            is_bracket = item.order_type in CHILD_TYPES and item.status == StoneXOrderStatus.PENDING
            if item.last_changed_ms is not None and not is_bracket and item.order_id not in self._parents:
                # candidate parent: its first LastChangedTime is what its brackets carry (XTNL)
                self._first_ts.setdefault((item.market_id, item.last_changed_ms), item.order_id)
                while len(self._first_ts) > MEMO_CAP:
                    self._first_ts.popitem(last=False)
        memo.market_id = item.market_id or memo.market_id
        memo.trading_account_id = item.trading_account_id or memo.trading_account_id
        memo.side = item.side or memo.side
        memo.order_type = item.order_type or memo.order_type
        if item.quantity is not None:
            memo.quantity = item.quantity
        if item.original_quantity is not None:
            memo.original_quantity = item.original_quantity
        memo.price = item.open_price or item.execution_price or memo.price
        return _remember(self._orders, item.order_id, memo)

    def _child_parent(self, item: OrderItem, memo: _OrderMemo,
                      code: StoneXOrderStatus) -> tuple[bool, str | None]:
        oid = item.order_id
        if oid in self._parents:
            return True, self._parents[oid]
        if oid in self._standalone or memo.closing or oid in self._positions:
            return False, None
        if memo.order_type in CHILD_TYPES and (code == StoneXOrderStatus.PENDING or memo.bracket):
            memo.bracket = True
            parent = (self._first_ts.get((memo.market_id, item.last_changed_ms))
                      if item.last_changed_ms is not None else None)
            if parent is not None and parent != oid:
                _remember(self._parents, oid, parent)
                return True, parent
            return True, None
        return False, None

    def _on_open(self, item: OrderItem, memo: _OrderMemo, pos: _PositionMemo | None,
                 account_id: str, ts: int) -> list[Emitted]:
        assert memo.side is not None
        out: list[Emitted] = []
        if pos is None or not pos.is_open:
            if not memo.terminal:
                out.append(self._order(item.order_id, memo, account_id, OrderStatus.FILLED, ts,
                                       reduce_only=False, linked=None))
                memo.terminal = True
            rest_entry = self._rest_entry.pop(item.order_id, None)
            entry = rest_entry or item.open_price or item.execution_price or memo.price
            pos = _remember(self._positions, item.order_id,
                            _PositionMemo(market_id=memo.market_id, side=memo.side, entry_price=entry,
                                          rest_entry=rest_entry is not None))
        elif item.open_price is not None and not pos.rest_entry:
            pos.entry_price = item.open_price
        closed = memo.quantity is not None and memo.quantity == 0
        out.append(self._position(item.order_id, pos, memo, account_id, ts, closed=closed))
        pos.is_open = not closed
        return out

    def _on_closed(self, item: OrderItem, memo: _OrderMemo, pos: _PositionMemo | None,
                   account_id: str, ts: int) -> list[Emitted]:
        out: list[Emitted] = []
        if pos is not None:
            if not memo.terminal:
                out.append(self._order(item.order_id, memo, account_id, OrderStatus.FILLED, ts,
                                       reduce_only=False, linked=None))
                memo.terminal = True
            out.append(self._position(item.order_id, pos, memo, account_id, ts, closed=True))
            pos.is_open = False
            return out
        # Closed with no prior Open: a closing trade (reduce-only, never a position).
        memo.closing = True
        memo.terminal = True
        return [self._order(item.order_id, memo, account_id, OrderStatus.FILLED, ts,
                            reduce_only=True, linked=self._closing_link(item.order_id, memo))]

    def _closing_link(self, order_id: str, memo: _OrderMemo) -> str | None:
        """The one position on the same market with the opposite side, if exactly one."""
        if memo.side is None:
            return None
        want = opposite(memo.side)
        hits = [pid for pid, p in self._positions.items()
                if pid != order_id and p.market_id == memo.market_id and p.side == want]
        open_hits = [pid for pid in hits if self._positions[pid].is_open]
        if len(open_hits) == 1:
            return open_hits[0]
        return hits[-1] if len(hits) == 1 else None

    # --- DTO builders ----------------------------------------------------------------------
    def _qty(self, oid: str, memo: _OrderMemo) -> Decimal | None:
        if memo.quantity is not None:
            return memo.quantity
        parent = self._parents.get(oid)
        parent_memo = self._orders.get(parent) if parent else None
        return parent_memo.quantity if parent_memo else None

    def _order(self, oid: str, memo: _OrderMemo, account_id: str, status: OrderStatus, ts: int, *,
               reduce_only: bool, linked: str | None) -> OrderDTO:
        assert memo.side is not None
        spec = self._lookup(memo.market_id)
        qty = self._qty(oid, memo)
        ordered = to_units(spec, memo.original_quantity if memo.original_quantity else qty)
        remaining = to_units(spec, qty)
        unknown = ordered.instrument_unknown or remaining.instrument_unknown
        rem_units = Decimal(0) if status == OrderStatus.FILLED else remaining.units
        return OrderDTO(
            client_order_id=oid, broker_order_id=oid, account_id=account_id, adapter_id=self._adapter_id,
            symbol=spec.symbol if spec else unknown_symbol(memo.market_id), side=memo.side, status=status,
            entry_price=memo.price,
            ordered_units=None if unknown else ordered.units,
            remaining_units=None if unknown else rem_units,
            instrument_unknown=unknown, raw_qty=qty,
            is_reduce_only=reduce_only, linked_position_id=linked if reduce_only else None,
            broker_updated_at_ms=ts, source="STREAM", order_kind=order_kind_of(memo.order_type),
        )

    def _position(self, pid: str, pos: _PositionMemo, memo: _OrderMemo, account_id: str, ts: int, *,
                  closed: bool) -> PositionDTO:
        spec = self._lookup(pos.market_id)
        u = to_units(spec, Decimal(0) if closed else memo.quantity)
        return PositionDTO(
            position_id=pid, account_id=account_id, adapter_id=self._adapter_id,
            symbol=spec.symbol if spec else unknown_symbol(pos.market_id), side=pos.side,
            units=u.units, instrument_unknown=u.instrument_unknown,
            raw_qty=Decimal(0) if closed else memo.quantity,
            entry_price=pos.entry_price, status="CLOSED" if closed else "OPEN",
            opened_by_client_order_id=pid, broker_updated_at_ms=ts, source="STREAM",
        )
