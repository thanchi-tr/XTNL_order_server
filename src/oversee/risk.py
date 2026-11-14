"""Shared pure risk definitions (oversee-rules.md §5; Decimal throughout).

Functions raise `RiskUnknown(reason)` for a missing or unknown input; rules turn it into an
UNKNOWN result. Values carry the stale symbols they used, so callers can cap at SOFT (§2).
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from src.dto import CashFlowDTO, InstrumentSpec, OrderDTO, OrderStatus, PositionDTO, Side
from src.oversee.snapshot import CashFlowRow, SodMark, SymbolQuotes, TimedMid
from src.oversee.types import SymbolKey

ZERO = Decimal(0)
HUNDRED = Decimal(100)
PREFERRED_CROSS = ("USD", "EUR", "GBP", "JPY")


class RiskUnknown(Exception):
    """A missing or unknown input (§2): reason is risk_unknown, mark_unknown or live_unknown."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


class MarkQuality(str, Enum):
    CLEAN = "CLEAN"                      # fresh clean mid
    LIQUIDATION = "LIQUIDATION"          # ticking, no clean quote > stale_mark_ms: bid (long) / ask (short)
    LAST_MID = "LAST_MID"                # no quote at all > stale_mark_ms (or Redis unavailable)
    CLOSED_LAST_CLEAN = "CLOSED_LAST_CLEAN"  # closed / roll break: last clean mid before the break


FLAG_QUALITIES = frozenset({MarkQuality.LAST_MID, MarkQuality.CLOSED_LAST_CLEAN})


@dataclass(frozen=True)
class Mark:
    key: SymbolKey
    price: Decimal
    quality: MarkQuality
    ts_ms: int

    @property
    def stale(self) -> bool:
        return self.quality is not MarkQuality.CLEAN

    @property
    def flags_stale_mark(self) -> bool:
        """`FLAG stale_mark` cases (§5): no quote at all > stale_mark_ms, or closed / in the roll break."""
        return self.quality in FLAG_QUALITIES


@dataclass(frozen=True)
class Valued:
    """A Decimal with the stale symbols that produced it."""

    value: Decimal
    stale: frozenset[SymbolKey] = frozenset()

    def __add__(self, other: Valued) -> Valued:
        return Valued(self.value + other.value, self.stale | other.stale)

    def scale(self, rate: Valued) -> Valued:
        return Valued(self.value * rate.value, self.stale | rate.stale)


def _latest_clean(q: SymbolQuotes) -> TimedMid | None:
    options = [m for m in (q.clean_mid,
                           TimedMid(q.last_clean.mid, q.last_clean.ts_ms) if q.last_clean else None) if m]
    return max(options, key=lambda m: m.ts_ms) if options else None


def select_mark(key: SymbolKey, quotes: SymbolQuotes | None, now_ms: int, stale_mark_ms: int,
                liquidation_side: Side | None = None) -> Mark:
    """Clean-mid selection with the four stale cases (§5). `liquidation_side` is the side of the
    holding being marked (BUY = long, marked at bid)."""
    if quotes is None:
        raise RiskUnknown("mark_unknown", f"no mid ever seen for {key}")
    if quotes.session != "OPEN":
        clean = _latest_clean(quotes)
        if clean is None:
            raise RiskUnknown("mark_unknown", f"no clean mid before the break for {key}")
        return Mark(key, clean.mid, MarkQuality.CLOSED_LAST_CLEAN, clean.ts_ms)
    if quotes.clean_mid is not None and now_ms - quotes.clean_mid.ts_ms <= stale_mark_ms:
        return Mark(key, quotes.clean_mid.mid, MarkQuality.CLEAN, quotes.clean_mid.ts_ms)
    raw = quotes.latest
    if raw is not None and now_ms - raw.timestamp_ms <= stale_mark_ms:
        if liquidation_side == "BUY":
            price = raw.bid
        elif liquidation_side == "SELL":
            price = raw.ask
        else:
            price = raw.mid
        return Mark(key, price, MarkQuality.LIQUIDATION, raw.timestamp_ms)
    options: list[TimedMid] = []
    if raw is not None:
        options.append(TimedMid(raw.mid, raw.timestamp_ms))
    clean = _latest_clean(quotes)
    if clean is not None:
        options.append(clean)
    if not options:
        raise RiskUnknown("mark_unknown", f"no mid ever seen for {key}")
    last = max(options, key=lambda m: m.ts_ms)
    return Mark(key, last.mid, MarkQuality.LAST_MID, last.ts_ms)


class MarkBook:
    """Marks and FX conversion per (adapter_id, symbol) at clean mids (§5). Records stale symbols seen."""

    def __init__(self, quotes: Mapping[SymbolKey, SymbolQuotes],
                 instruments: Mapping[SymbolKey, InstrumentSpec], now_ms: int, stale_mark_ms: int) -> None:
        self.quotes = quotes
        self.instruments = instruments
        self.now_ms = now_ms
        self.stale_mark_ms = stale_mark_ms
        self.stale_seen: set[SymbolKey] = set()
        self.qualities: dict[SymbolKey, MarkQuality] = {}   # every symbol marked, with its mark quality

    def mark(self, adapter_id: str, symbol: str, liquidation_side: Side | None = None) -> Mark:
        key = (adapter_id, symbol)
        m = select_mark(key, self.quotes.get(key), self.now_ms, self.stale_mark_ms, liquidation_side)
        if m.stale:
            self.stale_seen.add(key)
        self.qualities[key] = m.quality
        return m

    def spec(self, adapter_id: str, symbol: str) -> InstrumentSpec:
        spec = self.instruments.get((adapter_id, symbol))
        if spec is None:
            raise RiskUnknown("risk_unknown", f"instrument_unknown {adapter_id}:{symbol}")
        return spec

    def _pair_rate(self, adapter_id: str, frm: str, to: str) -> Valued | None:
        for (adapter, symbol), spec in sorted(self.instruments.items()):
            if adapter != adapter_id:
                continue
            if spec.base_ccy == frm and spec.quote_ccy == to:
                m = self.mark(adapter, symbol)
                return Valued(m.price, frozenset({m.key}) if m.stale else frozenset())
            if spec.base_ccy == to and spec.quote_ccy == frm:
                m = self.mark(adapter, symbol)
                if m.price == ZERO:
                    raise RiskUnknown("mark_unknown", f"zero mid for {symbol}")
                return Valued(1 / m.price, frozenset({m.key}) if m.stale else frozenset())
        return None

    def fx(self, adapter_id: str, frm: str, to: str) -> Valued:
        """Rate converting `frm` into `to` with this adapter's own pairs, direct, inverse or via one cross."""
        if frm == to:
            return Valued(Decimal(1))
        direct = self._pair_rate(adapter_id, frm, to)
        if direct is not None:
            return direct
        ccys = {c for (a, _), s in self.instruments.items() if a == adapter_id for c in (s.base_ccy, s.quote_ccy)}
        ordered = [c for c in PREFERRED_CROSS if c in ccys] + sorted(ccys - set(PREFERRED_CROSS))
        for via in ordered:
            if via in (frm, to):
                continue
            leg1 = self._pair_rate(adapter_id, frm, via)
            leg2 = self._pair_rate(adapter_id, via, to) if leg1 is not None else None
            if leg1 is not None and leg2 is not None:
                return leg1.scale(leg2)
        raise RiskUnknown("mark_unknown", f"no fx path {frm}->{to} on {adapter_id}")


class PreRollMarkBook(MarkBook):
    """Marks from the last clean mids with ts before the roll only (sod_marks, §2 Day roll)."""

    def __init__(self, quotes: Mapping[SymbolKey, SymbolQuotes],
                 instruments: Mapping[SymbolKey, InstrumentSpec], roll_ms: int) -> None:
        super().__init__(quotes, instruments, roll_ms, 0)
        self.roll_ms = roll_ms
        self.used: dict[SymbolKey, TimedMid] = {}

    def mark(self, adapter_id: str, symbol: str, liquidation_side: Side | None = None) -> Mark:
        key = (adapter_id, symbol)
        q = self.quotes.get(key)
        options: list[TimedMid] = []
        if q is not None:
            if q.clean_mid is not None and q.clean_mid.ts_ms < self.roll_ms:
                options.append(q.clean_mid)
            if q.last_clean is not None and q.last_clean.ts_ms < self.roll_ms:
                options.append(TimedMid(q.last_clean.mid, q.last_clean.ts_ms))
        if not options:
            raise RiskUnknown("mark_unknown", f"no pre-roll clean mid for {key}")
        best = max(options, key=lambda m: m.ts_ms)
        self.used[key] = best
        return Mark(key, best.mid, MarkQuality.CLEAN, best.ts_ms)


# ---------------------------------------------------------------- units and stops

AnyEntity = OrderDTO | PositionDTO


def _entity_id(e: AnyEntity) -> str:
    return e.position_id if isinstance(e, PositionDTO) else e.broker_order_id


def _opposite(a: Side, b: Side) -> bool:
    return a != b


def linked_units(link_id: str, symbol: str, child_side: Side,
                 positions: Sequence[PositionDTO], orders: Sequence[OrderDTO]) -> Decimal:
    """Units of the entity a reduce-only order is linked to: the OPEN position (same symbol, opposite
    side) plus the still-pending parent entry order. For a StoneX partially filled parent the
    position and the parent share the id, so both count (§5 reducing_units)."""
    total = ZERO
    for p in positions:
        if p.position_id == link_id and p.status == "OPEN" and p.symbol == symbol and _opposite(p.side, child_side):
            if p.units is None:
                raise RiskUnknown("risk_unknown", f"units unknown for position {p.position_id}")
            total += p.units
    for o in orders:
        if (o.broker_order_id == link_id and not o.is_reduce_only and not o.status.terminal
                and o.symbol == symbol and _opposite(o.side, child_side)):
            if o.remaining_units is None:
                raise RiskUnknown("risk_unknown", f"units unknown for order {o.broker_order_id}")
            total += o.remaining_units
    return total


def reducing_units(order: OrderDTO, positions: Sequence[PositionDTO], orders: Sequence[OrderDTO]) -> Decimal:
    """0 unless is_reduce_only; else min(remaining_units, linked entity units) (§5)."""
    if not order.is_reduce_only:
        return ZERO
    if order.remaining_units is None:
        raise RiskUnknown("risk_unknown", f"units unknown for order {order.broker_order_id}")
    if order.linked_position_id is None:
        return ZERO
    linked = linked_units(order.linked_position_id, order.symbol, order.side, positions, orders)
    return min(order.remaining_units, linked)


def opening_units(order: OrderDTO, positions: Sequence[PositionDTO], orders: Sequence[OrderDTO]) -> Decimal:
    """remaining_units - reducing_units for a non-terminal order; 0 for a terminal order (§5)."""
    if order.status.terminal:
        return ZERO
    if order.instrument_unknown or order.remaining_units is None:
        raise RiskUnknown("risk_unknown", f"units unknown for order {order.broker_order_id}")
    return order.remaining_units - reducing_units(order, positions, orders)


def needs_stop(e: AnyEntity, positions: Sequence[PositionDTO], orders: Sequence[OrderDTO]) -> bool:
    """An OPEN position, or a non-terminal order with opening_units > 0 (§5)."""
    if isinstance(e, PositionDTO):
        return e.status == "OPEN"
    return not e.status.terminal and opening_units(e, positions, orders) > ZERO


def _units_to_cover(e: AnyEntity, positions: Sequence[PositionDTO], orders: Sequence[OrderDTO]) -> Decimal:
    child_side: Side = "SELL" if e.side == "BUY" else "BUY"
    return linked_units(_entity_id(e), e.symbol, child_side, positions, orders)


def effective_stop(e: AnyEntity, positions: Sequence[PositionDTO], orders: Sequence[OrderDTO]) -> Decimal | None:
    """e.stop_loss; else the non-terminal linked reduce-only stop children whose remaining units cover
    e's units (the most distant child stop); else None (§5).

    A child counts as a stop when its trigger price lies on the loss side of e's entry (no order
    type field exists on OrderDTO)."""
    if e.stop_loss is not None:
        return e.stop_loss
    if e.entry_price is None:
        return None
    link = _entity_id(e)
    children = [
        o for o in orders
        if o.is_reduce_only and not o.status.terminal and o.linked_position_id == link
        and o.symbol == e.symbol and o.side != e.side and o.entry_price is not None
        and o.remaining_units is not None
        and ((e.side == "BUY" and o.entry_price < e.entry_price)
             or (e.side == "SELL" and o.entry_price > e.entry_price))
    ]
    if not children:
        return None
    covered = sum((o.remaining_units or ZERO for o in children), ZERO)
    if covered < _units_to_cover(e, positions, orders):
        return None
    prices = [o.entry_price for o in children if o.entry_price is not None]
    return min(prices) if e.side == "BUY" else max(prices)


# ---------------------------------------------------------------- risk


@dataclass(frozen=True)
class RiskFigure:
    """risk(e) in the account currency. `no_stop`: no effective stop (MissingStopLossRule's concern)."""

    value: Decimal
    stale: frozenset[SymbolKey] = frozenset()
    no_stop: bool = False


def risk(e: AnyEntity, units: Decimal, book: MarkBook, account_ccy: str,
         positions: Sequence[PositionDTO], orders: Sequence[OrderDTO]) -> RiskFigure:
    """Directional risk floored at 0, converted at a clean mid (§5).

    BUY (entry - stop) x units, SELL (stop - entry) x units; a stop at or past breakeven is 0.
    UNKNOWN for a missing InstrumentSpec, a PENDING/WORKING order with its stop on the wrong side,
    or no mid ever seen. A missing entry price (market order) uses the current mark."""
    if e.instrument_unknown:
        raise RiskUnknown("risk_unknown", "instrument_unknown")
    spec = book.spec(e.adapter_id, e.symbol)
    mark = book.mark(e.adapter_id, e.symbol, e.side)
    stale: frozenset[SymbolKey] = frozenset({mark.key}) if mark.stale else frozenset()
    stop = effective_stop(e, positions, orders)
    if stop is None:
        return RiskFigure(ZERO, stale, no_stop=True)
    entry = e.entry_price if e.entry_price is not None else mark.price
    per_unit = (entry - stop) if e.side == "BUY" else (stop - entry)
    if (isinstance(e, OrderDTO) and e.status in (OrderStatus.PENDING, OrderStatus.WORKING)
            and per_unit < ZERO):
        raise RiskUnknown("risk_unknown", f"stop on the wrong side for order {e.broker_order_id}")
    per_unit = max(per_unit, ZERO)
    rate = book.fx(e.adapter_id, spec.pnl_ccy, account_ccy)
    return RiskFigure(per_unit * units * rate.value, stale | rate.stale)


def position_pnl(p: PositionDTO, book: MarkBook, account_ccy: str) -> Valued:
    """Open P&L at the mark plus swap / commission not yet booked to the balance, in account ccy.

    Swap and commission are taken as signed amounts in the account currency."""
    if p.instrument_unknown or p.units is None:
        raise RiskUnknown("risk_unknown", f"instrument_unknown {p.position_id}")
    spec = book.spec(p.adapter_id, p.symbol)
    mark = book.mark(p.adapter_id, p.symbol, p.side)
    if p.entry_price is None:
        raise RiskUnknown("risk_unknown", f"no entry price for {p.position_id}")
    diff = (mark.price - p.entry_price) if p.side == "BUY" else (p.entry_price - mark.price)
    rate = book.fx(p.adapter_id, spec.pnl_ccy, account_ccy)
    pnl = Valued(diff * p.units, frozenset({mark.key}) if mark.stale else frozenset()).scale(rate)
    if not spec.swap_booked_to_balance:
        pnl = pnl + Valued(p.accrued_swap + p.commission)
    return pnl


# ---------------------------------------------------------------- cash ledger


def signed_amount(cf: CashFlowDTO) -> Decimal:
    """Balance effect of a ledger row: deposits +, withdrawals -, P&L kinds as signed."""
    if cf.kind == "DEPOSIT":
        return abs(cf.amount)
    if cf.kind == "WITHDRAWAL":
        return -abs(cf.amount)
    return cf.amount


def flow_in_account_ccy(cf: CashFlowDTO, book: MarkBook, account_ccy: str) -> Valued:
    if cf.currency == account_ccy:
        return Valued(signed_amount(cf))
    return Valued(signed_amount(cf)).scale(book.fx(cf.adapter_id, cf.currency, account_ccy))


def flows_since(flows: Iterable[CashFlowRow], roll_ms: int, book: MarkBook, account_ccy: str,
                transfers_only: bool = False) -> Valued:
    total = Valued(ZERO)
    for row in flows:
        if row.flow.broker_ts >= roll_ms and (not transfers_only or row.flow.is_transfer):
            total = total + flow_in_account_ccy(row.flow, book, account_ccy)
    return total


def unexplained_balance_delta(balance: Decimal, sod_balance: Decimal, flows: Iterable[CashFlowRow],
                              roll_ms: int, book: MarkBook, account_ccy: str) -> Valued:
    """(balance - sod balance) - sum(cash_flows since the roll, every kind incl. TRADE_PNL) (§5)."""
    explained = flows_since(flows, roll_ms, book, account_ccy)
    return Valued(balance - sod_balance - explained.value, explained.stale)


def explained_pre_roll_deposits(flows: Iterable[CashFlowRow], sod: SodMark, book: MarkBook,
                                account_ccy: str) -> Valued:
    """Pre-roll DEPOSITs committed after the sod row was written (explained only after the roll)."""
    total = Valued(ZERO)
    for row in flows:
        f = row.flow
        if f.kind == "DEPOSIT" and f.broker_ts < sod.roll_ms and row.committed_at_ms > sod.written_at_ms:
            total = total + flow_in_account_ccy(f, book, account_ccy)
    return total


def remaining_unexplained_at_roll(flows: Iterable[CashFlowRow], sod: SodMark, book: MarkBook,
                                  account_ccy: str) -> Decimal:
    """Part of `unexplained_increase_at_roll` still unexplained (excluded from equity_mark)."""
    explained = explained_pre_roll_deposits(flows, sod, book, account_ccy).value
    return max(ZERO, sod.unexplained_increase_at_roll - explained)


def net_transfers_today(flows: Sequence[CashFlowRow], sod: SodMark, book: MarkBook, account_ccy: str) -> Valued:
    """Transfers whose balance effect is not already in sod_equity (tier-persistence.md §4): every
    DEPOSIT / WITHDRAWAL with broker_ts at or after the roll, plus pre-roll DEPOSITs explained only
    after the roll, up to `unexplained_increase_at_roll`. A pre-roll WITHDRAWAL never counts."""
    today = flows_since(flows, sod.roll_ms, book, account_ccy, transfers_only=True)
    late = explained_pre_roll_deposits(flows, sod, book, account_ccy)
    return Valued(today.value + min(late.value, sod.unexplained_increase_at_roll), today.stale | late.stale)


@dataclass(frozen=True)
class EquityMark:
    value: Decimal
    stale: frozenset[SymbolKey]
    unexplained_delta: Decimal | None     # None without a sod row
    excluded_increase: Decimal            # unexplained increases subtracted from the mark


def equity_mark(balance: Decimal, account_ccy: str, positions: Sequence[PositionDTO], book: MarkBook,
                flows: Sequence[CashFlowRow], sod: SodMark | None) -> EquityMark:
    """balance + sum(OPEN position P&L at the mark + unbooked swap/commission), minus unexplained balance
    increases: today's unexplained increase and the part of `unexplained_increase_at_roll` that is
    still unexplained (so a late pre-roll deposit can't hide a loss)."""
    total = Valued(balance)
    for p in positions:
        if p.status == "OPEN":
            total = total + position_pnl(p, book, account_ccy)
    if sod is None:
        return EquityMark(total.value, total.stale, None, ZERO)
    delta = unexplained_balance_delta(balance, sod.balance, flows, sod.roll_ms, book, account_ccy)
    excluded = max(ZERO, delta.value) + remaining_unexplained_at_roll(flows, sod, book, account_ccy)
    return EquityMark(total.value - excluded, total.stale | delta.stale, delta.value, excluded)


def pct_of(value: Decimal, base: Decimal) -> Decimal:
    if base <= ZERO:
        raise RiskUnknown("risk_unknown", "equity_mark is not positive")
    return value / base * HUNDRED
