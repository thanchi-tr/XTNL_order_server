"""Pure StoneX payload -> canonical DTO mapping for REST reads, account frames and price frames
(adapter-isolation.md §1-§2, dto-contracts.md §3).

account_id is the ClientAccountId (the margin / risk account); trading accounts map onto it.
Adapters never stamp observed_seq / seq_id and never invent a broker timestamp: untimed rows
carry broker_updated_at_ms = 0 and the orchestrator labels them.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from src.adapters.stonex.codes import (
    StoneXOrderType,
    canonical_status,
    is_tradeable,
    opposite,
    order_kind_of,
    parse_order_type,
    parse_side,
    parse_status,
)
from src.adapters.stonex.instruments import ADAPTER_ID, to_units, unknown_symbol
from src.adapters.stonex.wire import (
    JsonObj,
    get_ci,
    get_list,
    get_obj,
    parse_broker_time_ms,
    to_decimal,
    to_id,
)
from src.dto import (
    AccountDTO,
    AccountLive,
    CashFlowDTO,
    InstrumentSpec,
    OrderDTO,
    OrderKind,
    OrderStatus,
    PositionDTO,
    PriceDTO,
)

SpecLookup = Callable[[str | None], InstrumentSpec | None]


class UnmappableError(ValueError):
    """A payload lacks a field the canonical DTO requires (e.g. direction)."""


# --- account directory ---------------------------------------------------------------------
@dataclass(frozen=True)
class ClientAccount:
    client_account_id: str
    trading_account_ids: tuple[str, ...]
    currency: str | None


@dataclass(frozen=True)
class AccountDirectory:
    accounts: tuple[ClientAccount, ...]

    @property
    def client_ids(self) -> tuple[str, ...]:
        return tuple(a.client_account_id for a in self.accounts)

    @property
    def trading_ids(self) -> tuple[tuple[str, str], ...]:
        """(trading_account_id, client_account_id) pairs."""
        return tuple((t, a.client_account_id) for a in self.accounts for t in a.trading_account_ids)

    def client_for(self, trading_account_id: str | None) -> str | None:
        for a in self.accounts:
            if trading_account_id in a.trading_account_ids:
                return a.client_account_id
        return self.accounts[0].client_account_id if len(self.accounts) == 1 else None

    def currency_of(self, client_account_id: str) -> str | None:
        for a in self.accounts:
            if a.client_account_id == client_account_id:
                return a.currency
        return None


def parse_account_directory(body: JsonObj) -> AccountDirectory:
    """AccountInformationResponseDTO v2, tolerant of the camelCase variants XTNL observed
    (clientAccounts / clientAccountId / tradingAccountId / clientAccountCurrency)."""
    root = get_obj(body, "AccountResult") or body
    trading: dict[str, list[str]] = {}
    currency: dict[str, str] = {}

    def note(client: str | None, trade: str | None, ccy: object) -> None:
        if client is None:
            return
        ids = trading.setdefault(client, [])
        if trade is not None and trade not in ids:
            ids.append(trade)
        if isinstance(ccy, str) and ccy and client not in currency:
            currency[client] = ccy.upper()

    note(to_id(get_ci(root, "ClientAccountId")), None, get_ci(root, "ClientAccountCurrency", "CurrencyIsoCode"))
    # Only a real tradingAccountId names a trading account. A client account row also carries `contractId`
    # (a legal contract id, e.g. 400000303 on the live account); v1 order/* reads answer 401 for it.
    for row in get_list(root, "ClientAccounts"):
        note(to_id(get_ci(row, "ClientAccountId")), to_id(get_ci(row, "TradingAccountId")),
             get_ci(row, "ClientAccountCurrency", "Currency", "CurrencyIsoCode"))
    default_client = next(iter(trading), None)
    for row in get_list(root, "TradingAccounts"):
        client = to_id(get_ci(row, "ClientAccountId")) or default_client
        note(client, to_id(get_ci(row, "TradingAccountId")), get_ci(row, "Currency", "CurrencyIsoCode"))
    return AccountDirectory(tuple(
        ClientAccount(client_account_id=c, trading_account_ids=tuple(t), currency=currency.get(c))
        for c, t in trading.items()
    ))


# --- account -------------------------------------------------------------------------------
def map_margin(raw: Mapping[str, Any], *, account_id: str, currency_fallback: str | None,
               source: str, received_ms: int, adapter_id: str = ADAPTER_ID) -> tuple[AccountDTO, AccountLive] | None:
    """clientAccountMargin (REST) or a CLIENTACCOUNTMARGIN frame -> (AccountDTO, AccountLive).
    ACCOUNT is in the local clock domain: broker_updated_at_ms stays 0; AccountLive uses receipt time.
    ``realised_pnl_today`` stays None (K9, accepted deviation in adapter-isolation.md §2): neither
    CLIENTACCOUNTMARGIN / clientAccountMargin nor TRADEMARGIN carries a realised-P&L field, an unknown Lightstreamer
    field name would fail the whole subscription, and a figure summed from the trade history would be wrong
    whenever StoneX omits RealisedPnl on a closing trade. Realised P&L reaches the ledger as TRADE_PNL cash flows."""
    cash = to_decimal(get_ci(raw, "Cash"))
    ccy = get_ci(raw, "CurrencyIsoCode", "CurrencyISO") or currency_fallback
    if cash is None or not isinstance(ccy, str) or not ccy:
        return None
    ccy = ccy.upper()
    account = AccountDTO(account_id=account_id, adapter_id=adapter_id, currency=ccy, balance=cash,
                         realised_pnl_today=None, source="REST" if source == "REST" else "STREAM")
    margin = to_decimal(get_ci(raw, "TotalMarginRequirement"))
    live = AccountLive(
        account_id=account_id, adapter_id=adapter_id, currency=ccy,
        equity=to_decimal(get_ci(raw, "NetEquity")),
        margin_used=margin if margin is not None else to_decimal(get_ci(raw, "Margin")),
        free_margin=to_decimal(get_ci(raw, "TradableFunds", "TradeableFunds")),
        unrealised_pnl=to_decimal(get_ci(raw, "OpenTradeEquity")),
        timestamp_ms=received_ms,
    )
    return account, live


# --- prices --------------------------------------------------------------------------------
PRICE_FIELDS: tuple[str, ...] = ("MarketId", "TickDateRaw", "Bid", "Offer", "StatusSummary")


def map_price(frame: Mapping[str, str | None], spec: InstrumentSpec, received_ms: int,
              adapter_id: str = ADAPTER_ID) -> PriceDTO | None:
    """PRICES frame -> PriceDTO. TickDateRaw units vary (s / ms / us): detected by magnitude.
    A missing tick time falls back to receipt time (prices are unversioned). ``received_ms`` is the local receipt
    time stamped on the Lightstreamer thread (design PR2); ``ts_source`` says which of the two ``timestamp_ms`` is:
    BROKER (TickDateRaw) or RECEIPT (the fallback)."""
    bid, ask = to_decimal(frame.get("Bid")), to_decimal(frame.get("Offer"))
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    broker_ts = parse_broker_time_ms(frame.get("TickDateRaw"))
    spread = (ask - bid) / spec.point_size if spec.point_size > 0 else None
    return PriceDTO(adapter_id=adapter_id, symbol=spec.symbol, bid=bid, ask=ask, spread_pips=spread,
                    timestamp_ms=broker_ts or received_ms, broker_market_id=spec.broker_symbol or None,
                    tradeable=is_tradeable(frame.get("StatusSummary")),
                    ts_source="BROKER" if broker_ts else "RECEIPT", received_ms=received_ms)


# --- REST positions / orders ------------------------------------------------------------------
def _price(raw: JsonObj, *names: str) -> Decimal | None:
    value = to_decimal(get_ci(raw, *names))
    return value if value is not None and value != 0 else None


def _rest_ts(raw: JsonObj) -> int:
    """LastChangedDateTimeUTC when present; else 0 (untimed: labelled by the orchestrator)."""
    return parse_broker_time_ms(get_ci(raw, "LastChangedDateTimeUTC", "LastChangedDateTimeUtc")) or 0


def map_open_position(raw: JsonObj, *, account_id: str, lookup: SpecLookup,
                      adapter_id: str = ADAPTER_ID) -> PositionDTO:
    """ApiOpenPositionDTO -> PositionDTO(OPEN). Stops stay on the child OrderDTOs (never folded)."""
    position_id = to_id(get_ci(raw, "OrderId"))
    side = parse_side(get_ci(raw, "Direction"))
    if position_id is None or side is None:
        raise UnmappableError("open position without OrderId / Direction")
    market_id = to_id(get_ci(raw, "MarketId"))
    spec = lookup(market_id)
    u = to_units(spec, to_decimal(get_ci(raw, "Quantity", "Qauantity")))
    return PositionDTO(
        position_id=position_id, account_id=account_id, adapter_id=adapter_id,
        symbol=spec.symbol if spec else unknown_symbol(market_id), side=side,
        units=u.units, instrument_unknown=u.instrument_unknown, raw_qty=u.raw_qty,
        entry_price=_price(raw, "Price", "OpenPrice", "TriggerPrice"), status="OPEN",
        opened_by_client_order_id=position_id,
        broker_updated_at_ms=_rest_ts(raw), source="REST",
    )


def map_stop_limit_order(raw: JsonObj, *, account_id: str, lookup: SpecLookup, parent_id: str | None,
                         default_side: str | None = None, default_qty: Decimal | None = None,
                         default_market_id: str | None = None, strict_status: bool = False,
                         order_kind: OrderKind | None = None, default_status: OrderStatus | None = None,
                         default_ts: int = 0, adapter_id: str = ADAPTER_ID) -> OrderDTO:
    """ApiActiveStopLimitOrderDTO (or a position's StopOrder / LimitOrder) -> OrderDTO.
    A child (parent_id set) is reduce-only with linked_position_id = the parent OrderId.
    ``order_kind`` (design PR1) is the caller's typing when it knows better than the row (a child nested in a
    position or entry is typed by its key, ``attached_orders``); else it comes from the row's own type
    (``stop_limit_type``: 2 -> STOP, 3 -> LIMIT, anything else -> None).
    On the active list (or nested in a position) a missing Status means WORKING. With ``strict_status`` (a
    by-id read, which also returns orders that are no longer active) a missing or unrecognised Status is
    unmappable: the adapter never invents a status (IMPLEMENTATION_PLAN 8.1).
    ``default_status`` / ``default_ts`` are for an if-done child nested in a working entry's row, which carries
    neither: the stream states such a child Pending, and the row it is nested in carries the broker time
    (``AttachedOrder.parent_ts``), so a snapshot agrees with the stream instead of reading WORKING untimed."""
    order_id = to_id(get_ci(raw, "OrderId"))
    side = parse_side(get_ci(raw, "Direction")) or parse_side(default_side)
    if order_id is None or side is None:
        raise UnmappableError("stop/limit order without OrderId / Direction")
    market_id = to_id(get_ci(raw, "MarketId")) or default_market_id
    spec = lookup(market_id)
    raw_status = get_ci(raw, "Status")
    code = parse_status(raw_status)
    if code is None and strict_status:
        raise UnmappableError(f"stop/limit order {order_id} with unrecognised Status {str(raw_status)[:20]!r}")
    status = canonical_status(code) if code is not None else (default_status or OrderStatus.WORKING)
    qty = to_decimal(get_ci(raw, "Quantity", "Qauantity"))
    u = to_units(spec, qty if qty is not None else default_qty)
    remaining = Decimal(0) if status == OrderStatus.FILLED else u.units
    return OrderDTO(
        client_order_id=order_id, broker_order_id=order_id, account_id=account_id, adapter_id=adapter_id,
        symbol=spec.symbol if spec else unknown_symbol(market_id), side=side, status=status,
        entry_price=_price(raw, "TriggerPrice", "Price"),
        ordered_units=u.units, remaining_units=remaining if not u.instrument_unknown else None,
        instrument_unknown=u.instrument_unknown, raw_qty=u.raw_qty,
        is_reduce_only=parent_id is not None, linked_position_id=parent_id,
        broker_updated_at_ms=_rest_ts(raw) or default_ts, source="REST",
        order_kind=order_kind if order_kind is not None else order_kind_of(stop_limit_type(raw)),
    )


def map_trade_order(raw: JsonObj, *, account_id: str, lookup: SpecLookup, reduce_only: bool,
                    linked_position_id: str | None, adapter_id: str = ADAPTER_ID) -> OrderDTO:
    """GET order/{id} TradeOrder -> OrderDTO (Open -> FILLED, remaining 0). A TradeOrder without OrderId,
    Direction or a recognised Status is unmappable (never None, which would read as "the broker has none").
    A trade order has no stop / limit kind: ``order_kind`` stays None (design PR1)."""
    order_id = to_id(get_ci(raw, "OrderId"))
    side = parse_side(get_ci(raw, "Direction"))
    raw_status = get_ci(raw, "Status")
    code = parse_status(raw_status)
    if order_id is None or side is None:
        raise UnmappableError("trade order without OrderId / Direction")
    if code is None:
        raise UnmappableError(f"trade order {order_id} with unrecognised Status {str(raw_status)[:20]!r}")
    market_id = to_id(get_ci(raw, "MarketId"))
    spec = lookup(market_id)
    status = canonical_status(code)
    qty = to_decimal(get_ci(raw, "OriginalQuantity")) or to_decimal(get_ci(raw, "Quantity", "Qauantity"))
    u = to_units(spec, qty)
    remaining = Decimal(0) if status == OrderStatus.FILLED else u.units
    return OrderDTO(
        client_order_id=order_id, broker_order_id=order_id, account_id=account_id, adapter_id=adapter_id,
        symbol=spec.symbol if spec else unknown_symbol(market_id), side=side, status=status,
        entry_price=_price(raw, "Price", "ExecutionPrice", "OpenPrice", "TriggerPrice"),
        ordered_units=u.units, remaining_units=remaining if not u.instrument_unknown else None,
        instrument_unknown=u.instrument_unknown, raw_qty=u.raw_qty,
        is_reduce_only=reduce_only, linked_position_id=linked_position_id,
        broker_updated_at_ms=_rest_ts(raw), source="REST",
    )


@dataclass(frozen=True)
class AttachedOrder:
    """A StopOrder / LimitOrder object nested in an open position or an if-done entry. ``order_type`` comes from
    the key it is nested under (StopOrder / Stop -> STOP, LimitOrder / Limit -> LIMIT), never from the child."""

    raw: JsonObj
    parent_id: str
    order_type: StoneXOrderType
    parent_side: str | None
    parent_qty: Decimal | None
    parent_market_id: str | None
    if_done: bool = False   # nested in the parent's IfDone list (an entry's bracket), not directly in it
    parent_ts: int = 0      # the parent row's LastChangedDateTimeUTC (0 when it has none)


def attached_orders(parent: JsonObj) -> list[AttachedOrder]:
    """Children nested in an open position (StopOrder / LimitOrder) or an entry's IfDone list."""
    parent_id = to_id(get_ci(parent, "OrderId"))
    if parent_id is None:
        return []
    side = get_ci(parent, "Direction")
    qty = to_decimal(get_ci(parent, "Quantity", "Qauantity"))
    market_id = to_id(get_ci(parent, "MarketId"))
    parent_ts = _rest_ts(parent)
    found: list[AttachedOrder] = []
    holders = [parent, *get_list(parent, "IfDone")]
    for holder in holders:
        for key, kind in (("StopOrder", StoneXOrderType.STOP), ("Stop", StoneXOrderType.STOP),
                          ("LimitOrder", StoneXOrderType.LIMIT), ("Limit", StoneXOrderType.LIMIT)):
            child = get_obj(holder, key)
            if child is not None and to_id(get_ci(child, "OrderId")) is not None:
                found.append(AttachedOrder(child, parent_id, kind, str(side) if side is not None else None,
                                           qty, market_id, if_done=holder is not parent, parent_ts=parent_ts))
    return found


def parent_of(raw: JsonObj) -> str | None:
    return to_id(get_ci(raw, "ParentOrderId"))


def stop_limit_type(raw: JsonObj) -> StoneXOrderType | None:
    """The row's own order type: ``Type`` (active orders, ``order/{id}``), ``OrderTypeId``, or ``TypeId`` (the
    stop / limit order history, IN-1)."""
    return parse_order_type(get_ci(raw, "Type", "OrderTypeId", "TypeId"))


def child_side(parent_side: str | None) -> str | None:
    side = parse_side(parent_side)
    return None if side is None else opposite(side)


# --- cash flows ----------------------------------------------------------------------------
def map_trade_history(rows: list[JsonObj], *, account_id: str, since_ms: int,
                      adapter_id: str = ADAPTER_ID) -> list[CashFlowDTO]:
    """Closing deals -> CashFlowDTO(TRADE_PNL). StoneX exposes no deposit / withdrawal history
    in this API, so transfers are unverified on StoneX (dto-contracts.md §3)."""
    return map_trade_history_checked(rows, account_id=account_id, since_ms=since_ms, adapter_id=adapter_id)[0]


def map_trade_history_checked(rows: list[JsonObj], *, account_id: str, since_ms: int,
                              adapter_id: str = ADAPTER_ID) -> tuple[list[CashFlowDTO], list[str]]:
    """``map_trade_history`` plus why closing rows were not mapped (IMPLEMENTATION_PLAN 7.1): a closing row
    (opening links or a realised P&L) without its P&L, order id, timestamp or currency is a booked balance
    move the ledger misses, so the caller must flag the result partial instead of returning it as complete.
    A closing row without a timestamp cannot be placed before or after ``since_ms`` and counts as missing.
    Non-closing rows (openings) and closing rows before ``since_ms`` are not balance moves of the window."""
    flows: list[CashFlowDTO] = []
    skipped: list[str] = []
    for row in rows:
        opening = get_ci(row, "OpeningOrderIds")
        pnl = to_decimal(get_ci(row, "RealisedPnl"))
        order_id = to_id(get_ci(row, "OrderId"))
        # An opening trade lists only its own id (live 2026-11-14) and carries no RealisedPnl: not a closing deal.
        names = {to_id(v) for v in opening} - {None, order_id} if isinstance(opening, list) else set()
        is_closing = bool(names) or pnl is not None
        if not is_closing:
            continue
        ts = parse_broker_time_ms(get_ci(row, "ExecutedDateTimeUtc", "ExecutedDateTimeUTC",
                                         "LastChangedDateTimeUtc", "LastChangedDateTimeUTC"))
        if ts is not None and ts < since_ms:
            continue
        ccy = get_ci(row, "RealisedPnlCurrency", "Currency")
        missing = [name for name, bad in (("RealisedPnl", pnl is None), ("OrderId", order_id is None),
                                          ("ExecutedDateTimeUtc", ts is None),
                                          ("currency", not isinstance(ccy, str) or not ccy)) if bad]
        if missing:
            skipped.append(f"closing trade {order_id or '?'} without {', '.join(missing)}")
            continue
        assert pnl is not None and order_id is not None and ts is not None and isinstance(ccy, str)
        flows.append(CashFlowDTO(transaction_id=f"stonex:trade:{order_id}", account_id=account_id,
                                 adapter_id=adapter_id, kind="TRADE_PNL", amount=pnl,
                                 currency=ccy.upper(), broker_ts=ts))
    return flows, skipped

