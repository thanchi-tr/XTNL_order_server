"""cTrader symbols -> InstrumentSpec (adapter-isolation.md §1-§2, dto-contracts.md §3).

ProtoOA volumes (order / position / deal ``volume``, the symbol's ``lotSize`` / ``minVolume``) are in
hundredths of a unit, so ``unit_kind = CENTI_UNITS`` and ``to_notional(volume) = volume / 100``.
Spot prices are integers in 1/100000 of a price unit and are rounded to the symbol's ``digits``.
Values marked "to confirm" must be checked against a live account before Oversee relies on them.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from src.adapters.pepperstone.protocol import JsonObj
from src.adapters.stonex.wire import to_decimal, to_int
from src.dto import InstrumentSpec

ADAPTER_ID = "pepperstone"
PRICE_SCALE = Decimal(100_000)
_NON_ALNUM = re.compile(r"[^A-Z0-9]")
_FX = re.compile(r"^[A-Z]{6}$")


@dataclass(frozen=True)
class SymbolInfo:
    """One resolved cTrader symbol: the canonical spec plus the wire facts the mapper needs."""

    symbol_id: int
    spec: InstrumentSpec
    digits: int


def canonical_symbol(name: str) -> str:
    """'EURUSD' -> 'EURUSD', 'EUR/USD' -> 'EURUSD', 'US500.cash' -> 'US500CASH'."""
    return _NON_ALNUM.sub("", name.upper())


def unknown_symbol(symbol_id: object) -> str:
    """Placeholder symbol for a symbolId missing from the instrument map."""
    return f"UNKNOWN:{symbol_id if symbol_id is not None else '?'}"


def parse_assets(rows: list[JsonObj]) -> dict[int, str]:
    """ProtoOAAsset rows -> {assetId: name} (asset names are ISO codes for currencies)."""
    out: dict[int, str] = {}
    for row in rows:
        asset_id, name = to_int(row.get("assetId")), row.get("name") or row.get("displayName")
        if asset_id is not None and isinstance(name, str) and name.strip():
            out[asset_id] = name.strip().upper()
    return out


def spec_from_symbol(light: JsonObj | None, full: JsonObj, assets: Mapping[int, str],
                     adapter_id: str = ADAPTER_ID) -> SymbolInfo | None:
    """ProtoOALightSymbol (name, assets) + ProtoOASymbol (digits, volumes) -> SymbolInfo.

    None when the symbol cannot be mapped safely (its entities then stay instrument_unknown)."""
    symbol_id = to_int(full.get("symbolId"))
    if symbol_id is None and light is not None:
        symbol_id = to_int(light.get("symbolId"))
    name = (light or {}).get("symbolName") or full.get("symbolName")
    if symbol_id is None or not isinstance(name, str):
        return None
    symbol = canonical_symbol(name)
    if not symbol:
        return None
    base = assets.get(to_int((light or {}).get("baseAssetId")) or -1)
    quote = assets.get(to_int((light or {}).get("quoteAssetId")) or -1)
    if (base is None or quote is None) and _FX.match(symbol):
        base, quote = base or symbol[:3], quote or symbol[3:]
    if quote is None:
        return None
    base = base or quote
    digits = to_int(full.get("digits"))
    pip_position = to_int(full.get("pipPosition"))
    if digits is None or digits < 0:
        return None
    point = Decimal(1).scaleb(-(pip_position if pip_position is not None and pip_position >= 0 else digits))
    lot_size = to_decimal(full.get("lotSize"))
    min_volume = to_decimal(full.get("minVolume"))
    spec = InstrumentSpec(
        adapter_id=adapter_id,
        symbol=symbol,
        base_ccy=base,
        quote_ccy=quote,
        pnl_ccy=quote,
        # lotSize is in cents of units (to confirm): kept as units per lot, for reference only;
        # CENTI_UNITS ignores contract_size in to_notional.
        contract_size=lot_size / 100 if lot_size is not None and lot_size > 0 else Decimal(1),
        unit_kind="CENTI_UNITS",
        point_size=point,
        min_qty=min_volume / 100 if min_volume is not None and min_volume > 0 else Decimal(0),
        # cTrader accrues swap and commission on the open position and books them to the balance
        # with the closing deal (closePositionDetail), so they are not yet in the balance (to confirm).
        swap_booked_to_balance=False,
        broker_symbol=str(symbol_id),
        # PR7: the quote's smallest step from the symbol's digits; pipPosition (point_size) is never a fallback.
        # asset_class stays None: no field read here states it.
        price_increment=Decimal(1).scaleb(-digits),
    )
    return SymbolInfo(symbol_id=symbol_id, spec=spec, digits=digits)


def relative_price(raw: object, digits: int) -> Decimal | None:
    """Spot integer price (1/100000 of a unit) -> Decimal rounded to the symbol's digits."""
    value = to_decimal(raw)
    if value is None or value <= 0:
        return None
    return (value / PRICE_SCALE).quantize(Decimal(1).scaleb(-digits)) if digits >= 0 else value / PRICE_SCALE
