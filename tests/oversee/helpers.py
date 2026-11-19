"""Builders for Oversee tests. All times are injected; nothing reads the wall clock."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from types import MappingProxyType
from typing import Any

from src.dto import AccountDTO, AccountLive, CashFlowDTO, CashFlowKind, InstrumentSpec, OrderDTO, OrderStatus
from src.dto import OrderKind, PositionDTO, PriceDTO, Side
from src.oversee.dayroll import NEW_YORK, latest_roll_ms, trading_day_for_roll
from src.oversee.snapshot import AccountSnapshot, CashFlowRow, CleanMark, SodMark, SymbolQuotes, TimedMid

D = Decimal
AD = "stonex"
ACCT = "A1"
# Wednesday 2026-11-11 12:00 New York: well clear of the 17:00 roll.
T0 = int(datetime(2026, 11, 11, 12, 0, tzinfo=NEW_YORK).timestamp() * 1000)
ROLL = latest_roll_ms(T0)


def spec(symbol: str = "EURUSD", base: str = "EUR", quote: str = "USD", pnl: str | None = None,
         adapter: str = AD, swap_booked: bool = True) -> InstrumentSpec:
    return InstrumentSpec(adapter_id=adapter, symbol=symbol, base_ccy=base, quote_ccy=quote,
                          pnl_ccy=pnl or quote, contract_size=D(1), unit_kind="UNITS", point_size=D("0.0001"),
                          min_qty=D(1), swap_booked_to_balance=swap_booked)


def quote(mid: str | Decimal, ts: int, half_spread: str = "0.00005", symbol: str = "EURUSD",
          clean: bool = True, adapter: str = AD) -> SymbolQuotes:
    m, h = D(mid), D(half_spread)
    p = PriceDTO(adapter_id=adapter, symbol=symbol, bid=m - h, ask=m + h, spread_pips=None, timestamp_ms=ts)
    return SymbolQuotes(latest=p, clean_mid=TimedMid(m, ts) if clean else None)


def position(pid: str, side: Side = "BUY", units: str = "100000", entry: str = "1.1000",
             stop: str | None = None, symbol: str = "EURUSD", status: str = "OPEN", **kw: Any) -> PositionDTO:
    return PositionDTO(position_id=pid, account_id=ACCT, adapter_id=kw.pop("adapter", AD), symbol=symbol,
                       side=side, units=D(units), entry_price=D(entry), status=status,  # type: ignore[arg-type]
                       stop_loss=D(stop) if stop else None, **kw)


def order(oid: str, side: Side = "BUY", status: OrderStatus = OrderStatus.WORKING, ordered: str = "100000",
          remaining: str | None = None, entry: str | None = "1.1000", stop: str | None = None,
          reduce_only: bool = False, linked: str | None = None, symbol: str = "EURUSD",
          kind: OrderKind | None = None) -> OrderDTO:
    return OrderDTO(client_order_id=f"c-{oid}", broker_order_id=oid, account_id=ACCT, adapter_id=AD,
                    symbol=symbol, side=side, status=status, entry_price=D(entry) if entry else None,
                    stop_loss=D(stop) if stop else None, ordered_units=D(ordered),
                    remaining_units=D(remaining if remaining is not None else ordered),
                    is_reduce_only=reduce_only, linked_position_id=linked, order_kind=kind)


def account(balance: str = "100000", ccy: str = "USD") -> AccountDTO:
    return AccountDTO(account_id=ACCT, adapter_id=AD, currency=ccy, balance=D(balance))


def live(margin: str | None, sampled: bool = False, equity: str | None = None, ts: int = T0) -> AccountLive:
    return AccountLive(account_id=ACCT, adapter_id=AD, currency="USD", equity=D(equity) if equity else None,
                       margin_used=D(margin) if margin else None, free_margin=None, unrealised_pnl=None,
                       timestamp_ms=ts, sampled=sampled)


def flow(tid: str, kind: CashFlowKind, amount: str, broker_ts: int, committed: int | None = None) -> CashFlowRow:
    cf = CashFlowDTO(transaction_id=tid, account_id=ACCT, adapter_id=AD, kind=kind, amount=D(amount),
                     currency="USD", broker_ts=broker_ts)
    return CashFlowRow(cf, committed if committed is not None else broker_ts)


def sod(balance: str = "100000", equity: str | None = None, uiar: str = "0", partial: bool = False,
        roll: int = ROLL, written: int | None = None) -> SodMark:
    return SodMark(account_id=ACCT, trading_day=trading_day_for_roll(roll), roll_ms=roll, balance=D(balance),
                   sod_equity=D(equity if equity is not None else balance), unexplained_increase_at_roll=D(uiar),
                   partial_session=partial, written_at_ms=written if written is not None else roll + 1000)


def snap(now: int = T0, **kw: Any) -> AccountSnapshot:
    base: dict[str, Any] = dict(
        account_id=ACCT, adapter_id=AD, read_at_ms=now, account=account(),
        instruments=MappingProxyType({(AD, "EURUSD"): spec()}),
        quotes=MappingProxyType({(AD, "EURUSD"): quote("1.1000", now)}),
        sod=sod(),
    )
    for k in ("instruments", "quotes", "stop_watch"):
        if k in kw and isinstance(kw[k], dict):
            kw[k] = MappingProxyType(kw[k])
    base.update(kw)
    return AccountSnapshot(**base)


def clean_mark(mid: str, ts: int) -> CleanMark:
    m = D(mid)
    return CleanMark(m - D("0.00005"), m + D("0.00005"), ts)
