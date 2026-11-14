"""cTrader deal / order history -> OrderDTO / PositionDTO for ``fetch_history`` (the catch-up read after the
engine was off; adapter-isolation.md §2 Pepperstone, dto-contracts.md §3). Pure helpers, no I/O.

- Orders come from ProtoOAOrderListReq (by time range) and keep the order's own ``utcLastUpdateTimestamp``;
  an order a deal names but the order list lacks is read by id (ProtoOAOrderDetailsReq) by the adapter.
- Positions come from the deals: a position still in the reconcile is OPEN (its reconcile row); one that is no
  longer there is rebuilt from its deals (``position_from_deals``) and is CLOSED only when its deals prove it,
  or, when its opening deals are older than the lookback, when a closing deal (``closePositionDetail``) of the
  window names it. Never closed on absence alone.
- Volumes are hundredths of a unit (``InstrumentSpec.to_notional`` on CENTI_UNITS); deal and position times are
  the broker's own, so a later stream update always compares newer.
"""
from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal

from src.adapters.pepperstone.instruments import ADAPTER_ID
from src.adapters.pepperstone.mapping import (
    DEAL_STATUS,
    DS_FILLED,
    DS_PARTIALLY_FILLED,
    SymbolLookup,
    broker_ms,
    enum_value,
    obj,
    price,
    side_of,
    symbol_of,
    to_units,
)
from src.adapters.pepperstone.protocol import JsonObj
from src.adapters.stonex.codes import opposite
from src.adapters.stonex.wire import to_id, to_int
from src.dto import PositionDTO


def deal_time_ms(deal: JsonObj) -> int:
    """The latest broker time a deal carries (its execution or its last update)."""
    return max(broker_ms(deal, "utcLastUpdateTimestamp"), broker_ms(deal, "executionTimestamp", "createTimestamp"))


def order_time_ms(order: JsonObj) -> int:
    return broker_ms(order, "utcLastUpdateTimestamp")


def latest_by_id(rows: Iterable[JsonObj], id_key: str, *, deal: bool) -> dict[str, JsonObj]:
    """One row per broker id, the one with the latest broker time (pages and split windows overlap)."""
    out: dict[str, JsonObj] = {}
    for row in rows:
        rid = to_id(row.get(id_key))
        if rid is None:
            continue
        ts = deal_time_ms(row) if deal else order_time_ms(row)
        current = out.get(rid)
        if current is None or (deal_time_ms(current) if deal else order_time_ms(current)) <= ts:
            out[rid] = row
    return out


def deal_filled(deal: JsonObj) -> bool | None:
    """True for a FILLED / PARTIALLY_FILLED deal, False for a known other status (REJECTED, INTERNALLY_REJECTED,
    ERROR, MISSED: it moved nothing), None when the status is missing or unknown (the caller decides fail closed)."""
    status = enum_value(deal.get("dealStatus"), DEAL_STATUS)
    if status is None:
        return None
    return status in (DS_FILLED, DS_PARTIALLY_FILLED)


def is_closing_deal(deal: JsonObj) -> bool:
    return bool(obj(deal, "closePositionDetail"))


def closed_from_closing_deal(deal: JsonObj, position_id: str, *, account_id: str, lookup: SymbolLookup,
                             adapter_id: str = ADAPTER_ID) -> PositionDTO | None:
    """A CLOSED position from the last closing deal that names it (its opening deals being out of reach). The
    deal trades against the position, so the position's side is the opposite one; its entry price is the
    detail's ``entryPrice``. None when the deal lacks a side or is not known to have filled (a rejected close
    proves nothing)."""
    deal_side = side_of(deal.get("tradeSide"))
    if deal_side is None or not is_closing_deal(deal) or deal_filled(deal) is not True:
        return None
    symbol_id = to_int(deal.get("symbolId"))
    info = lookup(symbol_id)
    u = to_units(info, Decimal(0))
    return PositionDTO(
        position_id=position_id, account_id=account_id, adapter_id=adapter_id, symbol=symbol_of(info, symbol_id),
        side=opposite(deal_side), units=u.units, entry_price=price(obj(deal, "closePositionDetail").get("entryPrice")),
        status="CLOSED", instrument_unknown=u.instrument_unknown,
        raw_qty=Decimal(0) if u.instrument_unknown else None,
        broker_updated_at_ms=deal_time_ms(deal), broker_seq=None, source="REST")
