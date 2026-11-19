"""K9, accepted deviation (adapter-isolation.md §2 StoneX): CLIENTACCOUNTMARGIN / clientAccountMargin and TRADEMARGIN
carry no realised-P&L field, so StoneX AccountDTO.realised_pnl_today stays None (never invented, never summed from a
trade history that can omit RealisedPnl). The subscribed field list stays XTNL's proven one: an unknown field name
would fail the whole CLIENTACCOUNTMARGIN subscription."""
from __future__ import annotations

from src.adapters.stonex.adapter import ACCOUNT_FIELDS
from src.adapters.stonex.mapping import map_margin

CLIENT = "100"


def test_k9_account_fields_are_the_proven_list() -> None:
    assert ACCOUNT_FIELDS == ("Cash", "Margin", "MarginIndicator", "NetEquity", "OpenTradeEquity", "TradeableFunds",
                              "PendingFunds", "TradingResource", "TotalMarginRequirement", "CurrencyId", "CurrencyISO")
    assert not any("realis" in f.lower() for f in ACCOUNT_FIELDS)


def test_k9_realised_pnl_today_is_none_from_rest_and_stream() -> None:
    raw = {"Cash": "1", "Margin": "0", "NetEquity": "1", "OpenTradeEquity": "0", "TradeableFunds": "1",
           "TotalMarginRequirement": "0", "CurrencyISO": "AUD"}
    for source in ("REST", "STREAM"):
        mapped = map_margin(raw, account_id=CLIENT, currency_fallback=None, source=source, received_ms=1)
        assert mapped is not None
        account, _live = mapped
        assert account.realised_pnl_today is None
