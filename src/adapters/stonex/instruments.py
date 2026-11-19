"""StoneX market information -> InstrumentSpec (adapter-isolation.md §1, dto-contracts.md §3).

Several values are marked "to confirm": the API documents them loosely and they must be
checked against a live account before Oversee relies on them.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from decimal import Decimal

from src.adapters.stonex.wire import JsonObj, get_ci, to_decimal, to_id, to_int
from src.dto import InstrumentSpec

log = logging.getLogger(__name__)

ADAPTER_ID = "stonex"
FX_UNDERLYING_TYPE_ID = 4
_PAIR = re.compile(r"\b([A-Z]{3})\s*/\s*([A-Z]{3})\b")
_PER = re.compile(r"\(\s*per\s+([0-9]*\.?[0-9]+)\s*\)", re.IGNORECASE)
_NOISE = re.compile(r"\(.*?\)|\b(CFD|DFT|SPOT|ROLLING)\b", re.IGNORECASE)


@dataclass(frozen=True)
class ParsedName:
    symbol: str
    base: str | None
    quote: str | None
    per: Decimal | None   # "(per 0.0001)" in the market name


def parse_market_name(name: str) -> ParsedName | None:
    """'GBP/USD (per 0.0001) CFD' -> GBPUSD; other names -> upper-case alphanumerics."""
    per_match = _PER.search(name)
    per = to_decimal(per_match.group(1)) if per_match else None
    pair = _PAIR.search(name.upper())
    if pair:
        base, quote = pair.group(1), pair.group(2)
        return ParsedName(symbol=f"{base}{quote}", base=base, quote=quote, per=per)
    cleaned = re.sub(r"[^A-Z0-9]", "", _NOISE.sub("", name).upper())
    return ParsedName(symbol=cleaned, base=None, quote=None, per=per) if cleaned else None


def fx_pip_size(quote: str) -> Decimal:
    return Decimal("0.01") if quote == "JPY" else Decimal("0.0001")


def spec_from_market_info(info: JsonObj, adapter_id: str = ADAPTER_ID) -> InstrumentSpec | None:
    """ApiMarketInformationDTO -> InstrumentSpec, or None when it cannot be mapped safely
    (an unmapped market leaves its orders instrument_unknown, which Oversee treats as risk_unknown)."""
    market_id = to_id(get_ci(info, "MarketId"))
    name = get_ci(info, "Name", "MarketName")
    if market_id is None or not isinstance(name, str):
        return None
    parsed = parse_market_name(name)
    if parsed is None:
        return None
    decimals = to_int(get_ci(info, "PriceDecimalPlaces"))
    underlying = to_int(get_ci(info, "MarketUnderlyingTypeId", "MarketUnderlyingType"))
    is_fx = parsed.base is not None and underlying in (None, FX_UNDERLYING_TYPE_ID)

    if parsed.base is not None and parsed.quote is not None:
        base, quote = parsed.base, parsed.quote
        if parsed.per is not None:
            point = parsed.per
        elif is_fx:
            point = fx_pip_size(quote)           # 1 pip = 0.0001 (0.01 for JPY quotes)
        elif decimals is not None:
            point = Decimal(1).scaleb(-decimals)
        else:
            return None
    else:
        ccy = get_ci(info, "MarketCurrencyIsoCode", "MarketCurrency", "CurrencyIsoCode", "CurrencyISO", "Currency")
        if not isinstance(ccy, str) or len(ccy) != 3:
            log.warning("stonex market %s (%s): no ISO currency, left unmapped", market_id, name)
            return None
        base = quote = ccy.upper()
        if parsed.per is not None:
            point = parsed.per
        elif decimals is not None:
            point = Decimal(1).scaleb(-decimals)
        else:
            return None

    factor = to_decimal(get_ci(info, "QuantityConversionFactor"))
    min_qty = to_decimal(get_ci(info, "WebMinSize")) or Decimal(0)
    return InstrumentSpec(
        adapter_id=adapter_id,
        symbol=parsed.symbol,
        base_ccy=base,
        quote_ccy=quote,
        pnl_ccy=quote,
        # To confirm: StoneX FX CFD quantities are base-currency units; QuantityConversionFactor
        # is kept as contract_size for reference only (UNITS ignores it in to_notional).
        contract_size=factor if factor is not None and factor > 0 else Decimal(1),
        unit_kind="UNITS",
        point_size=point,
        min_qty=min_qty,
        swap_booked_to_balance=True,   # to confirm: StoneX books financing to cash
        broker_symbol=market_id,
        price_increment=price_increment(decimals),
    )


def price_increment(decimals: int | None) -> Decimal | None:
    """The quote's smallest step, 10^-PriceDecimalPlaces (design PR7; to confirm by the PR3 probe). None when the
    market states no decimal places: ``point_size`` (a pip, or the name's "per" size) is never a fallback.
    ``asset_class`` stays None: no StoneX field read here states it (an ARM position is then UNSUPPORTED)."""
    if decimals is None or decimals < 0:
        return None
    return Decimal(1).scaleb(-decimals)


def unknown_symbol(market_id: str | None) -> str:
    """Placeholder symbol for a market missing from the instrument map."""
    return f"UNKNOWN:{market_id or '?'}"


@dataclass(frozen=True)
class Units:
    units: Decimal | None
    instrument_unknown: bool
    raw_qty: Decimal | None


def to_units(spec: InstrumentSpec | None, raw_qty: Decimal | None) -> Units:
    """Broker quantity -> canonical units via InstrumentSpec.to_notional (adapter-isolation.md §1).
    Unknown instrument or quantity -> instrument_unknown with the raw quantity kept."""
    if spec is None or raw_qty is None:
        return Units(units=None, instrument_unknown=True, raw_qty=raw_qty)
    return Units(units=spec.to_notional(raw_qty), instrument_unknown=False, raw_qty=raw_qty)
