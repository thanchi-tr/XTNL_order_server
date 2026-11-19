"""Pure cTrader (ProtoOA JSON) payload -> canonical DTO mapping (adapter-isolation.md §1-§2).

- account_id is the ctidTraderAccountId (as a string).
- Orders and positions carry ``utcLastUpdateTimestamp`` as their broker time (no broker revision).
- The account's broker_seq is ``balanceVersion``; account rows carry no broker time.
- Money fields are int64 scaled by ``moneyDigits``; volumes are cents of units; order / position /
  deal prices are plain decimals; spot prices are 1/100000 integers (see instruments.py).
- Enum fields are accepted as numbers or as names (the JSON encoding is to confirm).
Adapters never stamp observed_seq / seq_id and never invent a broker timestamp.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from src.adapters.pepperstone.instruments import ADAPTER_ID, SymbolInfo, unknown_symbol
from src.adapters.pepperstone.protocol import JsonObj
from src.adapters.stonex.wire import to_bool, to_decimal, to_id, to_int
from src.dto import AccountDTO, CashFlowDTO, OrderDTO, OrderKind, OrderStatus, PositionDTO, Side, Source

SymbolLookup = Callable[[int | None], SymbolInfo | None]


class UnmappableError(ValueError):
    """A payload lacks a field the canonical DTO requires (e.g. the trade side)."""


# --- enums (ProtoOA*; numbers or names) ------------------------------------------------------------
TRADE_SIDE = {"BUY": 1, "SELL": 2}
ORDER_TYPE = {"MARKET": 1, "LIMIT": 2, "STOP": 3, "STOP_LOSS_TAKE_PROFIT": 4, "MARKET_RANGE": 5, "STOP_LIMIT": 6}
ORDER_STATUS = {"ORDER_STATUS_ACCEPTED": 1, "ORDER_STATUS_FILLED": 2, "ORDER_STATUS_REJECTED": 3,
                "ORDER_STATUS_EXPIRED": 4, "ORDER_STATUS_CANCELLED": 5}
POSITION_STATUS = {"POSITION_STATUS_OPEN": 1, "POSITION_STATUS_CLOSED": 2, "POSITION_STATUS_CREATED": 3,
                   "POSITION_STATUS_ERROR": 4}
EXECUTION_TYPE = {"ORDER_ACCEPTED": 2, "ORDER_FILLED": 3, "ORDER_REPLACED": 4, "ORDER_CANCELLED": 5,
                  "ORDER_EXPIRED": 6, "ORDER_REJECTED": 7, "ORDER_CANCEL_REJECTED": 8, "SWAP": 9,
                  "DEPOSIT_WITHDRAW": 10, "ORDER_PARTIAL_FILL": 11, "BONUS_DEPOSIT_WITHDRAW": 12}
DEAL_STATUS = {"FILLED": 2, "PARTIALLY_FILLED": 3, "REJECTED": 4, "INTERNALLY_REJECTED": 5, "ERROR": 6, "MISSED": 7}
BALANCE_OPERATION = {"DEPOSIT": 0, "WITHDRAW": 1}

OT_LIMIT, OT_STOP, OT_STOP_LOSS_TAKE_PROFIT, OT_STOP_LIMIT = 2, 3, 4, 6
OS_ACCEPTED, OS_FILLED, OS_REJECTED, OS_EXPIRED, OS_CANCELLED = 1, 2, 3, 4, 5
PS_OPEN, PS_CLOSED = 1, 2
KNOWN_ORDER_STATUSES = frozenset(ORDER_STATUS.values())
KNOWN_POSITION_STATUSES = frozenset(POSITION_STATUS.values())
ET_PARTIAL_FILL = 11
DS_FILLED, DS_PARTIALLY_FILLED = 2, 3
DEFAULT_MONEY_DIGITS = 2


def enum_value(raw: object, names: Mapping[str, int]) -> int | None:
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    text = str(raw).strip().upper()
    if not text:
        return None
    if text.lstrip("-").isdigit():
        return int(text)
    if text in names:
        return names[text]
    for name, value in names.items():   # tolerate prefixed / unprefixed variants
        if name.endswith(text) or text.endswith(name):
            return value
    return None


def known_status(raw: object, names: Mapping[str, int], known: frozenset[int]) -> int | None:
    """The status enum of a row, None when the field is absent (the proto default). A status that is present but
    not one of ``known`` raises UnmappableError (adapter-isolation.md §1, K7): the adapter never invents a status,
    and the caller counts the row as unmappable and opens the stream's gap."""
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return None
    value = enum_value(raw, names)
    if value is None or value not in known:
        raise UnmappableError(f"unrecognised status {str(raw)[:40]!r}")
    return value


def side_of(raw: object) -> Side | None:
    value = enum_value(raw, TRADE_SIDE)
    return "BUY" if value == 1 else "SELL" if value == 2 else None


def obj(o: JsonObj | None, key: str) -> JsonObj:
    value = (o or {}).get(key)
    return value if isinstance(value, Mapping) else {}


def objs(o: JsonObj | None, key: str) -> list[JsonObj]:
    value = (o or {}).get(key)
    return [v for v in value if isinstance(v, Mapping)] if isinstance(value, list) else []


def money(raw: object, digits: int) -> Decimal | None:
    value = to_decimal(raw)
    return None if value is None else value.scaleb(-digits)


def money_digits(o: JsonObj | None, default: int) -> int:
    value = to_int((o or {}).get("moneyDigits"))
    return value if value is not None and 0 <= value <= 18 else default


def price(raw: object) -> Decimal | None:
    """A ProtoOA double price; 0 / absent mean 'not set'."""
    value = to_decimal(raw)
    return None if value is None or value <= 0 else value


def broker_ms(o: JsonObj | None, *keys: str) -> int:
    for key in keys:
        value = to_int((o or {}).get(key))
        if value is not None and value > 0:
            return value
    return 0


@dataclass(frozen=True)
class Units:
    units: Decimal | None
    instrument_unknown: bool
    raw_qty: Decimal | None


def to_units(info: SymbolInfo | None, volume: Decimal | None) -> Units:
    """cTrader volume (cents of units) -> canonical units via InstrumentSpec.to_notional."""
    if info is None or volume is None:
        return Units(None, True, volume)
    return Units(info.spec.to_notional(volume), False, volume)


def symbol_of(info: SymbolInfo | None, symbol_id: int | None) -> str:
    return info.spec.symbol if info is not None else unknown_symbol(symbol_id)


# --- orders ------------------------------------------------------------------------------------------
def order_status(order: JsonObj, execution_type: int | None = None) -> OrderStatus:
    """Canonical status; an unrecognised ``orderStatus`` raises UnmappableError (K7)."""
    status = known_status(order.get("orderStatus"), ORDER_STATUS, KNOWN_ORDER_STATUSES)
    volume = to_decimal(obj(order, "tradeData").get("volume")) or Decimal(0)
    executed = to_decimal(order.get("executedVolume")) or Decimal(0)
    if status == OS_FILLED:
        return OrderStatus.FILLED
    if status == OS_REJECTED:
        return OrderStatus.REJECTED
    if status in (OS_EXPIRED, OS_CANCELLED):
        return OrderStatus.CANCELLED
    if execution_type == ET_PARTIAL_FILL or (executed > 0 and executed < volume):
        return OrderStatus.PARTIALLY_FILLED
    return OrderStatus.WORKING   # ORDER_STATUS_ACCEPTED (and an absent status, the proto default)


def order_entry_price(order: JsonObj, order_type: int | None) -> Decimal | None:
    """LIMIT: limitPrice; STOP / STOP_LIMIT: stopPrice (the trigger); SL/TP: its trigger price;
    MARKET / MARKET_RANGE (or anything else): executionPrice once filled."""
    entry: Decimal | None = None
    if order_type == 2:
        entry = price(order.get("limitPrice"))
    elif order_type in (3, 6):
        entry = price(order.get("stopPrice"))
    elif order_type == OT_STOP_LOSS_TAKE_PROFIT:
        entry = price(order.get("stopPrice")) or price(order.get("limitPrice"))
    return entry if entry is not None else price(order.get("executionPrice"))


def order_kind(order: JsonObj, order_type: int | None) -> OrderKind | None:
    """cTrader order type -> OrderKind (design PR1): LIMIT -> LIMIT; STOP, STOP_LIMIT -> STOP;
    STOP_LOSS_TAKE_PROFIT -> STOP when its ``stopPrice`` is set (the stop-loss leg), else LIMIT (the take-profit
    leg); MARKET, MARKET_RANGE, unknown or missing -> None."""
    if order_type == OT_LIMIT:
        return "LIMIT"
    if order_type in (OT_STOP, OT_STOP_LIMIT):
        return "STOP"
    if order_type == OT_STOP_LOSS_TAKE_PROFIT:
        return "STOP" if price(order.get("stopPrice")) is not None else "LIMIT"
    return None


def is_protection_order(order: JsonObj) -> bool:
    """A STOP_LOSS_TAKE_PROFIT order: the position's own stopLoss / takeProfit in order form."""
    return enum_value(order.get("orderType"), ORDER_TYPE) == OT_STOP_LOSS_TAKE_PROFIT


def map_order(order: JsonObj, *, account_id: str, lookup: SymbolLookup, source: Source,
              execution_type: int | None = None, adapter_id: str = ADAPTER_ID) -> OrderDTO:
    order_id = to_id(order.get("orderId"))
    trade = obj(order, "tradeData")
    side = side_of(trade.get("tradeSide"))
    if order_id is None or side is None:
        raise UnmappableError(f"order {order_id}: missing orderId or tradeSide")
    symbol_id = to_int(trade.get("symbolId"))
    info = lookup(symbol_id)
    status = order_status(order, execution_type)
    volume = to_decimal(trade.get("volume"))
    executed = to_decimal(order.get("executedVolume")) or Decimal(0)
    ordered = to_units(info, volume)
    remaining_raw = None if volume is None else max(volume - executed, Decimal(0))
    remaining = to_units(info, remaining_raw)
    unknown = ordered.instrument_unknown or remaining.instrument_unknown
    order_type = enum_value(order.get("orderType"), ORDER_TYPE)
    reduce_only = bool(to_bool(order.get("closingOrder"))) or order_type == OT_STOP_LOSS_TAKE_PROFIT
    position_id = to_id(order.get("positionId"))
    entry = order_entry_price(order, order_type)
    client_id = order.get("clientOrderId")
    return OrderDTO(
        client_order_id=str(client_id) if client_id not in (None, "") else None,
        broker_order_id=order_id,
        account_id=account_id,
        adapter_id=adapter_id,
        symbol=symbol_of(info, symbol_id),
        side=side,
        status=status,
        entry_price=entry,
        stop_loss=price(order.get("stopLoss")),
        take_profit=price(order.get("takeProfit")),
        ordered_units=None if unknown else ordered.units,
        remaining_units=None if unknown else (Decimal(0) if status == OrderStatus.FILLED else remaining.units),
        instrument_unknown=unknown,
        raw_qty=volume if unknown else None,
        is_reduce_only=reduce_only,
        linked_position_id=position_id if reduce_only else None,
        broker_updated_at_ms=broker_ms(order, "utcLastUpdateTimestamp"),
        broker_seq=None,
        source=source,
        order_kind=order_kind(order, order_type),
    )


# --- positions -----------------------------------------------------------------------------------
def map_position(position: JsonObj, *, account_id: str, lookup: SymbolLookup, source: Source,
                 default_money_digits: int = DEFAULT_MONEY_DIGITS, adapter_id: str = ADAPTER_ID) -> PositionDTO | None:
    """ProtoOAPosition -> PositionDTO; None for CREATED / ERROR positions (never filled). An unrecognised
    ``positionStatus`` raises UnmappableError (K7: counted as unmappable and the stream's gap opens)."""
    position_id = to_id(position.get("positionId"))
    trade = obj(position, "tradeData")
    side = side_of(trade.get("tradeSide"))
    if position_id is None or side is None:
        raise UnmappableError(f"position {position_id}: missing positionId or tradeSide")
    status_raw = known_status(position.get("positionStatus"), POSITION_STATUS, KNOWN_POSITION_STATUSES)
    if status_raw not in (None, PS_OPEN, PS_CLOSED):
        return None
    closed = status_raw == PS_CLOSED
    symbol_id = to_int(trade.get("symbolId"))
    info = lookup(symbol_id)
    volume = Decimal(0) if closed else to_decimal(trade.get("volume"))
    u = to_units(info, volume)
    md = money_digits(position, default_money_digits)
    return PositionDTO(
        position_id=position_id,
        account_id=account_id,
        adapter_id=adapter_id,
        symbol=symbol_of(info, symbol_id),
        side=side,
        units=u.units,
        entry_price=price(position.get("price")),
        status="CLOSED" if closed else "OPEN",
        instrument_unknown=u.instrument_unknown,
        raw_qty=volume if u.instrument_unknown else None,
        stop_loss=price(position.get("stopLoss")),
        take_profit=price(position.get("takeProfit")),
        accrued_swap=money(position.get("swap"), md) or Decimal(0),
        commission=money(position.get("commission"), md) or Decimal(0),
        broker_updated_at_ms=broker_ms(position, "utcLastUpdateTimestamp"),
        broker_seq=None,
        source=source,
    )


def position_from_deals(deals: Iterable[JsonObj], position_id: str, *, account_id: str, lookup: SymbolLookup,
                        default_money_digits: int = DEFAULT_MONEY_DIGITS,
                        adapter_id: str = ADAPTER_ID) -> PositionDTO | None:
    """Rebuild a position from its deals (ProtoOADealListByPositionIdRes) when it is no longer in the
    reconcile: opening deals add filled volume, closing deals (closePositionDetail) remove closed volume.
    Returns None when the deals do not prove its state (the caller never closes on absence alone)."""
    rows = sorted((d for d in deals if to_id(d.get("positionId")) == position_id
                   and enum_value(d.get("dealStatus"), DEAL_STATUS) in (DS_FILLED, DS_PARTIALLY_FILLED)),
                  key=lambda d: (broker_ms(d, "executionTimestamp", "createTimestamp"), to_int(d.get("dealId")) or 0))
    if not rows:
        return None
    opened = closed = Decimal(0)
    notional = Decimal(0)
    side: Side | None = None
    swap = commission = Decimal(0)
    last_ms = 0
    symbol_id: int | None = None
    for deal in rows:
        filled = to_decimal(deal.get("filledVolume")) or Decimal(0)
        detail = obj(deal, "closePositionDetail")
        symbol_id = to_int(deal.get("symbolId")) if symbol_id is None else symbol_id
        last_ms = max(last_ms, broker_ms(deal, "utcLastUpdateTimestamp", "executionTimestamp"))
        md = money_digits(deal, default_money_digits)
        commission += money(deal.get("commission"), md) or Decimal(0)
        if detail:
            closed += to_decimal(detail.get("closedVolume")) or filled
            swap += money(detail.get("swap"), money_digits(detail, md)) or Decimal(0)
        else:
            side = side or side_of(deal.get("tradeSide"))
            opened += filled
            notional += filled * (price(deal.get("executionPrice")) or Decimal(0))
    if side is None or opened <= 0:
        return None
    open_volume = max(opened - closed, Decimal(0))
    info = lookup(symbol_id)
    u = to_units(info, open_volume)
    return PositionDTO(
        position_id=position_id, account_id=account_id, adapter_id=adapter_id, symbol=symbol_of(info, symbol_id),
        side=side, units=u.units, entry_price=(notional / opened) if notional > 0 else None,
        status="CLOSED" if open_volume == 0 else "OPEN", instrument_unknown=u.instrument_unknown,
        raw_qty=open_volume if u.instrument_unknown else None, accrued_swap=swap, commission=commission,
        broker_updated_at_ms=last_ms, broker_seq=None, source="REST")


# --- account -------------------------------------------------------------------------------------
def account_currency(trader: JsonObj, assets: Mapping[int, str]) -> str | None:
    asset_id = to_int(trader.get("depositAssetId"))
    return assets.get(asset_id) if asset_id is not None else None


def map_trader(trader: JsonObj, *, account_id: str, currency: str, source: Source,
               adapter_id: str = ADAPTER_ID) -> AccountDTO:
    md = money_digits(trader, DEFAULT_MONEY_DIGITS)
    balance = money(trader.get("balance"), md)
    if balance is None:
        raise UnmappableError("trader without balance")
    return AccountDTO(account_id=account_id, adapter_id=adapter_id, currency=currency, balance=balance,
                      realised_pnl_today=None, broker_updated_at_ms=0,
                      broker_seq=to_int(trader.get("balanceVersion")), source=source)


def account_from_balance(holder: JsonObj, *, account_id: str, currency: str, default_money_digits: int,
                         adapter_id: str = ADAPTER_ID) -> AccountDTO | None:
    """Post-event balance carried by ProtoOADepositWithdraw / ProtoOAClosePositionDetail."""
    balance = money(holder.get("balance"), money_digits(holder, default_money_digits))
    version = to_int(holder.get("balanceVersion"))
    if balance is None or version is None:
        return None
    return AccountDTO(account_id=account_id, adapter_id=adapter_id, currency=currency, balance=balance,
                      realised_pnl_today=None, broker_updated_at_ms=0, broker_seq=version, source="STREAM")


# --- cash flows ----------------------------------------------------------------------------------
def map_deposit_withdraw(dw: JsonObj, *, account_id: str, currency: str, default_money_digits: int,
                         adapter_id: str = ADAPTER_ID) -> CashFlowDTO | None:
    """ProtoOADepositWithdraw -> CashFlowDTO(DEPOSIT | WITHDRAWAL), keyed by balanceHistoryId."""
    history_id = to_id(dw.get("balanceHistoryId"))
    delta = money(dw.get("delta"), money_digits(dw, default_money_digits))
    ts = broker_ms(dw, "changeBalanceTimestamp")
    if history_id is None or delta is None or ts == 0:
        return None
    op = enum_value(dw.get("operationType"), BALANCE_OPERATION)
    withdrawal = op == 1 or (op is None and delta < 0)
    return CashFlowDTO(transaction_id=f"pepperstone:balance:{history_id}", account_id=account_id,
                       adapter_id=adapter_id, kind="WITHDRAWAL" if withdrawal else "DEPOSIT",
                       amount=-abs(delta) if withdrawal else abs(delta), currency=currency, broker_ts=ts)


def map_closing_deal(deal: JsonObj, *, account_id: str, currency: str, default_money_digits: int,
                     adapter_id: str = ADAPTER_ID) -> CashFlowDTO | None:
    """A closing deal (closePositionDetail) -> CashFlowDTO(TRADE_PNL), keyed by the deal id. The
    amount is the balance move: grossProfit + swap + commission + pnlConversionFee (to confirm)."""
    detail = obj(deal, "closePositionDetail")
    deal_id = to_id(deal.get("dealId"))
    if not detail or deal_id is None:
        return None
    md = money_digits(detail, money_digits(deal, default_money_digits))
    parts = [money(detail.get(k), md) for k in ("grossProfit", "swap", "commission", "pnlConversionFee")]
    if parts[0] is None:
        return None
    ts = broker_ms(deal, "executionTimestamp", "utcLastUpdateTimestamp", "createTimestamp")
    if ts == 0:
        return None
    return CashFlowDTO(transaction_id=f"pepperstone:deal:{deal_id}", account_id=account_id, adapter_id=adapter_id,
                       kind="TRADE_PNL", amount=sum((p for p in parts if p is not None), Decimal(0)),
                       currency=currency, broker_ts=ts)


def payload_list(payload: JsonObj, key: str) -> list[dict[str, Any]]:
    return [dict(v) for v in objs(payload, key)]
