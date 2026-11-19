"""StoneX REST client (httpx.MockTransport) and pure REST / account / price mapping."""
from __future__ import annotations

import json
import logging
from decimal import Decimal

import httpx
import pytest

from src.adapters.base import AuthFailedError, BrokerError, SessionExpiredError, TransientBrokerError
from src.adapters.stonex.instruments import spec_from_market_info
from src.adapters.stonex.mapping import (
    PRICE_FIELDS,
    attached_orders,
    map_margin,
    map_open_position,
    map_price,
    map_stop_limit_order,
    map_trade_history,
    map_trade_order,
    parse_account_directory,
    stop_limit_type,
)
from src.adapters.stonex.codes import StoneXOrderType
from src.adapters.stonex.rest import StoneXRestClient
from src.adapters.transport import ReadOnlyTransport
from src.config import ExecutionLockedError
from src.dto import InstrumentSpec, OrderStatus

from .conftest import CLIENT, EURUSD_ID, MARKETS, SESSION, TRADING, USDJPY_ID, V1, V2, FakeStoneX, make_rest, wcf

T0 = 1_767_225_600_000


def eur() -> InstrumentSpec:
    spec = spec_from_market_info(MARKETS[EURUSD_ID])
    assert spec is not None
    return spec


def lookup(market_id: str | None) -> InstrumentSpec | None:
    return eur() if market_id == EURUSD_ID else None


# --- REST client -----------------------------------------------------------------------------------
async def test_log_on_sends_credentials_then_session_headers(fake_broker: FakeStoneX) -> None:
    rest = make_rest(fake_broker)
    await rest.log_on()
    assert rest.has_session and rest.session_token == SESSION
    await rest.account_information()
    logon, info = fake_broker.requests
    assert logon.url.path == "/v2/Session" and logon.method == "POST"
    import json
    body = json.loads(logon.content)
    assert body == {"UserName": "probe-user", "Password": "pw-SECRET", "AppKey": "key-SECRET",
                    "AppVersion": "1", "AppComments": ""}
    assert "Session" not in logon.headers
    assert info.headers["UserName"] == "probe-user" and info.headers["Session"] == SESSION
    await rest.aclose()


async def test_lowercase_session_key_is_accepted(fake_broker: FakeStoneX) -> None:
    fake_broker.logon_body = {"session": SESSION, "passwordChangeRequired": False}
    rest = make_rest(fake_broker)
    await rest.log_on()
    assert rest.session_token == SESSION


async def test_password_change_required_is_auth_failed(fake_broker: FakeStoneX) -> None:
    fake_broker.logon_body = {"Session": SESSION, "PasswordChangeRequired": True}
    with pytest.raises(AuthFailedError):
        await make_rest(fake_broker).log_on()


async def test_invalid_credentials_4010_is_auth_failed(fake_broker: FakeStoneX) -> None:
    fake_broker.one_shot["/v2/Session"] = httpx.Response(
        401, json={"HttpStatus": 401, "ErrorMessage": "Invalid Username or Password", "ErrorCode": 4010})
    with pytest.raises(AuthFailedError) as err:
        await make_rest(fake_broker).log_on()
    assert "pw-SECRET" not in str(err.value)


async def test_logon_without_session_is_auth_failed(fake_broker: FakeStoneX) -> None:
    fake_broker.logon_body = {"Session": None, "Is2FAEnabled": True}
    with pytest.raises(AuthFailedError):
        await make_rest(fake_broker).log_on()


async def test_401_on_a_read_is_session_expired(fake_broker: FakeStoneX) -> None:
    rest = make_rest(fake_broker)
    await rest.log_on()
    fake_broker.one_shot["/TradingAPI/order/openpositions"] = httpx.Response(401, json={"ErrorCode": 4011})
    with pytest.raises(SessionExpiredError):
        await rest.open_positions(TRADING)
    assert not rest.has_session
    with pytest.raises(SessionExpiredError):   # no silent re-login
        await rest.open_positions(TRADING)


async def test_error_code_4011_in_a_200_body_is_session_expired(fake_broker: FakeStoneX) -> None:
    rest = make_rest(fake_broker)
    await rest.log_on()
    fake_broker.one_shot["/v2/margin/clientAccountMargin"] = httpx.Response(
        200, json={"HttpStatus": 401, "ErrorMessage": "Session is not valid", "ErrorCode": 4011})
    with pytest.raises(SessionExpiredError):
        await rest.client_account_margin(CLIENT)


@pytest.mark.parametrize("response", [
    httpx.Response(429, text=""),
    httpx.Response(400, json={"HttpStatus": 400, "ErrorMessage": "throttled", "ErrorCode": 5002}),
    httpx.Response(503, text="maintenance"),
])
async def test_throttling_and_5xx_are_transient(fake_broker: FakeStoneX, response: httpx.Response) -> None:
    rest = make_rest(fake_broker)
    await rest.log_on()
    fake_broker.one_shot["/TradingAPI/order/activestoplimitorders"] = response
    with pytest.raises(TransientBrokerError):
        await rest.active_stop_limit_orders(TRADING)


async def test_network_error_is_transient() -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    from .conftest import make_settings
    rest = StoneXRestClient.create(make_settings(), http=httpx.AsyncClient(transport=httpx.MockTransport(boom)))
    with pytest.raises(TransientBrokerError):
        await rest.log_on()


async def test_other_4xx_is_a_plain_broker_error(fake_broker: FakeStoneX) -> None:
    rest = make_rest(fake_broker)
    await rest.log_on()
    fake_broker.one_shot["/TradingAPI/order/tradehistory"] = httpx.Response(400, json={"ErrorCode": 4000})
    with pytest.raises(BrokerError) as err:
        await rest.trade_history(TRADING, 0)
    assert not isinstance(err.value, (TransientBrokerError, SessionExpiredError, AuthFailedError))


async def test_read_endpoints_and_parameters(fake_broker: FakeStoneX) -> None:
    rest = make_rest(fake_broker)
    await rest.log_on()
    await rest.client_account_margin(CLIENT)
    await rest.open_positions(TRADING)
    await rest.trade_history(TRADING, 1_767_225_600, 200)
    infos = await rest.market_information(CLIENT, [EURUSD_ID, USDJPY_ID])
    assert {str(i["MarketId"]) for i in infos} == {EURUSD_ID, USDJPY_ID}
    margin, positions, history, market = fake_broker.requests[1:5]
    assert margin.url.params["clientAccountId"] == CLIENT
    assert positions.url.params["TradingAccountId"] == TRADING
    assert history.url.params["from"] == "1767225600" and history.url.params["maxResults"] == "200"
    assert market.method == "POST" and market.url.path == "/v2/market/information"
    assert await rest.validate() is True
    await rest.log_off()
    assert fake_broker.requests[-1].url.path == "/TradingAPI/session/deleteSession" and not rest.has_session


async def test_order_ids_must_be_numeric(fake_broker: FakeStoneX) -> None:
    rest = make_rest(fake_broker)
    await rest.log_on()
    with pytest.raises(ValueError):
        await rest.order("1/../../x")


@pytest.mark.parametrize("path", ["/order/newtradeorder", "/order/newstoplimitorder", "/order/updatetradeorder",
                                  "/order/updatestoplimitorder", "/order/cancel", "/ORDER/NewTradeOrder"])
async def test_read_only_transport_refuses_order_changing_paths(path: str) -> None:
    calls: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={})

    transport = ReadOnlyTransport(httpx.AsyncClient(transport=httpx.MockTransport(record)), env="sandbox")
    with pytest.raises(ExecutionLockedError):
        await transport.request("POST", f"{V1}{path}", json={})
    assert calls == []   # refused before reaching the wire
    await transport.request("GET", f"{V1}/order/openpositions")
    await transport.request("GET", f"{V1}/order/tradehistory")
    await transport.request("POST", f"{V2}/market/information", json={})
    assert len(calls) == 3


# --- pure mapping ----------------------------------------------------------------------------------
def test_account_directory_tolerates_casing_variants() -> None:
    xtnl_style = {"clientAccounts": [{"clientAccountId": 1001, "tradingAccountId": 2001,
                                      "clientAccountCurrency": "aud"}],
                  "TradingAccounts": [{"ClientAccountId": 1001, "TradingAccountId": 2001}]}
    docs_style = {"AccountResult": {"ClientAccountId": 1001, "ClientAccountCurrency": "AUD",
                                    "TradingAccounts": [{"ClientAccountId": 1001, "TradingAccountId": 2001},
                                                        {"ClientAccountId": 1001, "TradingAccountId": 2002}]}}
    a = parse_account_directory(xtnl_style)
    b = parse_account_directory(docs_style)
    assert a.client_ids == ("1001",) and a.currency_of("1001") == "AUD"
    assert a.trading_ids == (("2001", "1001"),)
    assert b.trading_ids == (("2001", "1001"), ("2002", "1001"))
    assert b.client_for("2002") == "1001" and b.client_for(None) == "1001"
    assert parse_account_directory({}).accounts == ()


def test_margin_maps_to_account_and_live() -> None:
    raw = {"cash": 1000.5, "netEquity": 1100.25, "openTradeEquity": 99.75, "tradableFunds": 800,
           "Margin": 300, "TotalMarginRequirement": 250, "currencyIsoCode": "aud"}
    mapped = map_margin(raw, account_id=CLIENT, currency_fallback=None, source="REST", received_ms=T0)
    assert mapped is not None
    account, live = mapped
    assert (account.balance, account.currency, account.source, account.broker_updated_at_ms) == (
        Decimal("1000.5"), "AUD", "REST", 0)
    assert account.realised_pnl_today is None and account.seq_id == 0
    assert (live.equity, live.margin_used, live.free_margin, live.unrealised_pnl, live.timestamp_ms) == (
        Decimal("1100.25"), Decimal(250), Decimal(800), Decimal("99.75"), T0)
    stream = {"Cash": "10", "Margin": "3", "TradeableFunds": "7", "CurrencyISO": None}
    mapped2 = map_margin(stream, account_id=CLIENT, currency_fallback="AUD", source="STREAM", received_ms=T0)
    assert mapped2 is not None
    assert mapped2[1].margin_used == Decimal(3) and mapped2[1].free_margin == Decimal(7)
    assert mapped2[0].source == "STREAM" and mapped2[0].currency == "AUD"
    assert map_margin({"NetEquity": 1}, account_id=CLIENT, currency_fallback="AUD", source="REST",
                      received_ms=T0) is None


@pytest.mark.parametrize(("tick", "expected"), [
    (str(T0 // 1000), (T0 // 1000) * 1000), (str(T0), T0), (str(T0 * 1000), T0), (wcf(T0), T0), (None, 42),
])
def test_price_mapping_tick_units(tick: str | None, expected: int) -> None:
    price = map_price({"MarketId": EURUSD_ID, "TickDateRaw": tick, "Bid": "1.08600", "Offer": "1.08612",
                       "StatusSummary": "0"}, eur(), received_ms=42)
    assert price is not None
    assert price.timestamp_ms == expected
    assert (price.symbol, price.bid, price.ask, price.adapter_id) == ("EURUSD", Decimal("1.08600"),
                                                                       Decimal("1.08612"), "stonex")
    assert price.spread_pips == Decimal("1.2") and price.broker_market_id == EURUSD_ID and price.tradeable


def test_price_status_and_malformed_prices() -> None:
    base = {"Bid": "1.1", "Offer": "1.1001", "TickDateRaw": str(T0)}
    closed = map_price(base | {"StatusSummary": "4"}, eur(), T0)
    assert closed is not None and not closed.tradeable
    assert map_price(base | {"Offer": None}, eur(), T0) is None
    assert map_price(base | {"Bid": "0"}, eur(), T0) is None
    assert PRICE_FIELDS == ("MarketId", "TickDateRaw", "Bid", "Offer", "StatusSummary")


def test_open_position_mapping() -> None:
    raw = {"OrderId": 555, "MarketId": int(EURUSD_ID), "Direction": "buy", "Qauantity": 25000, "Price": 1.0855,
           "TradingAccountId": 2001, "LastChangedDateTimeUTC": wcf(T0),
           "StopOrder": {"OrderId": 556, "TriggerPrice": 1.08}}
    pos = map_open_position(raw, account_id=CLIENT, lookup=lookup)
    assert (pos.position_id, pos.side, pos.units, pos.status, pos.source) == ("555", "BUY", Decimal(25000), "OPEN", "REST")
    assert pos.entry_price == Decimal("1.0855") and pos.broker_updated_at_ms == T0
    assert pos.stop_loss is None and pos.take_profit is None   # stops stay on the child orders
    (child,) = attached_orders(raw)
    assert child.parent_id == "555" and child.parent_market_id == EURUSD_ID


def test_stop_limit_order_mapping_child_and_entry() -> None:
    raw = {"OrderId": 777, "ParentOrderId": 555, "MarketId": int(EURUSD_ID), "Direction": "Sell",
           "Quantity": 25000, "TriggerPrice": 1.08, "Type": "Stop", "Status": 2, "CreatedDateTimeUTC": wcf(T0)}
    child = map_stop_limit_order(raw, account_id=CLIENT, lookup=lookup, parent_id="555")
    assert child.is_reduce_only and child.linked_position_id == "555" and child.status == OrderStatus.WORKING
    assert child.entry_price == Decimal("1.08") and child.broker_updated_at_ms == 0 and child.source == "REST"
    entry = map_stop_limit_order(raw | {"ParentOrderId": None}, account_id=CLIENT, lookup=lookup, parent_id=None)
    assert not entry.is_reduce_only and entry.linked_position_id is None
    unknown = map_stop_limit_order(raw | {"MarketId": 1}, account_id=CLIENT, lookup=lookup, parent_id=None)
    assert unknown.instrument_unknown and unknown.ordered_units is None and unknown.raw_qty == Decimal(25000)


@pytest.mark.parametrize(("type_field", "kind"), [
    ({"Type": "Stop"}, "STOP"), ({"Type": "Limit"}, "LIMIT"), ({"Type": 2}, "STOP"), ({"OrderTypeId": 3}, "LIMIT"),
    ({"TypeId": 2}, "STOP"), ({"TypeId": "3"}, "LIMIT"), ({"Type": 1}, None), ({}, None),
])
def test_stop_limit_order_kind_comes_from_the_row_type(type_field: dict[str, object], kind: str | None) -> None:
    """Design PR1: activeorders / order/{id} rows carry Type, the history TypeId (IN-1)."""
    raw = {"OrderId": 778, "MarketId": int(EURUSD_ID), "Direction": "Sell", "Quantity": 25000,
           "TriggerPrice": 1.08, "Status": 2, **type_field}
    assert map_stop_limit_order(raw, account_id=CLIENT, lookup=lookup, parent_id="555").order_kind == kind


def test_stop_and_limit_children_nested_in_a_position_are_typed_by_their_key() -> None:
    raw = {"OrderId": 555, "MarketId": int(EURUSD_ID), "Direction": "Buy", "Quantity": 25000,
           "StopOrder": {"OrderId": 556, "TriggerPrice": 1.08, "Type": "Limit"},      # the key wins over the child
           "IfDone": [{"Limit": {"OrderId": 557, "TriggerPrice": 1.09}}]}
    found = {c.raw["OrderId"]: c.order_type for c in attached_orders(raw)}
    assert found == {556: StoneXOrderType.STOP, 557: StoneXOrderType.LIMIT}
    kinds = {}
    for child in attached_orders(raw):
        kind = "STOP" if child.order_type == StoneXOrderType.STOP else "LIMIT"
        dto = map_stop_limit_order(child.raw, account_id=CLIENT, lookup=lookup, parent_id=child.parent_id,
                                   default_side="Sell", default_qty=child.parent_qty,
                                   default_market_id=child.parent_market_id, order_kind=kind)
        kinds[dto.broker_order_id] = dto.order_kind
    assert kinds == {"556": "STOP", "557": "LIMIT"}
    assert stop_limit_type(raw["StopOrder"]) == StoneXOrderType.LIMIT       # what the row alone would say


def test_a_trade_order_has_no_order_kind() -> None:
    raw = {"OrderId": 900, "MarketId": int(EURUSD_ID), "Direction": "Buy", "Quantity": 10000, "Status": 3,
           "Type": 1, "Price": 1.0855}
    dto = map_trade_order(raw, account_id=CLIENT, lookup=lookup, reduce_only=False, linked_position_id=None)
    assert dto.order_kind is None


def test_order_kind_changes_the_checksum_but_none_keeps_it() -> None:
    raw = {"OrderId": 779, "MarketId": int(EURUSD_ID), "Direction": "Sell", "Quantity": 25000,
           "TriggerPrice": 1.08, "Status": 2}
    untyped = map_stop_limit_order(raw, account_id=CLIENT, lookup=lookup, parent_id="555")
    typed = map_stop_limit_order(raw | {"Type": "Stop"}, account_id=CLIENT, lookup=lookup, parent_id="555")
    assert untyped.order_kind is None and typed.order_kind == "STOP"
    assert typed.checksum != untyped.checksum
    assert untyped.checksum == map_stop_limit_order(raw | {"Type": "Trade"}, account_id=CLIENT, lookup=lookup,
                                                    parent_id="555").checksum


def test_trade_history_maps_closing_deals_only() -> None:
    rows = [
        {"OrderId": 1, "OpeningOrderIds": [], "RealisedPnl": None, "RealisedPnlCurrency": "AUD",
         "ExecutedDateTimeUtc": wcf(T0)},                                                      # opening deal
        {"OrderId": 2, "OpeningOrderIds": [1], "RealisedPnl": -12.5, "RealisedPnlCurrency": "aud",
         "ExecutedDateTimeUtc": wcf(T0 + 1000)},
        {"OrderId": 3, "OpeningOrderIds": [9], "RealisedPnl": 40, "RealisedPnlCurrency": "AUD",
         "ExecutedDateTimeUtc": wcf(T0 - 5000)},                                               # before since
    ]
    flows = map_trade_history(rows, account_id=CLIENT, since_ms=T0)
    assert len(flows) == 1
    (flow,) = flows
    assert (flow.transaction_id, flow.kind, flow.amount, flow.currency, flow.broker_ts) == (
        "stonex:trade:2", "TRADE_PNL", Decimal("-12.5"), "AUD", T0 + 1000)
    assert not flow.is_transfer


async def test_log_off_never_puts_the_session_token_in_the_url(
        fake_broker: FakeStoneX, caplog: pytest.LogCaptureFixture) -> None:
    """Review finding httpx-info-log-leaks-secret-urls: httpx logs the full request URL at INFO."""
    rest = make_rest(fake_broker)
    await rest.log_on()
    with caplog.at_level(logging.DEBUG):
        await rest.log_off()
    req = fake_broker.requests[-1]
    assert req.url.path == "/TradingAPI/session/deleteSession"
    assert SESSION not in str(req.url) and "session" not in {k.lower() for k in req.url.params}
    assert req.headers["Session"] == SESSION and json.loads(req.content)["Session"] == SESSION
    assert SESSION not in caplog.text


def test_account_directory_never_treats_a_contract_id_as_a_trading_account() -> None:
    # Shape of the live v2 ClientAndTradingAccount answer (ids only): the client account row carries a
    # contractId, the trading account sits in the top-level tradingAccounts list.
    live_shape = {"clientAccounts": [{"clientAccountId": 400000101, "contractId": 400000303,
                                      "clientAccountCurrency": "AUD"}],
                  "tradingAccounts": [{"clientAccountId": 400000101, "tradingAccountId": 400000202,
                                       "tradingAccountStatus": "Open", "tradingAccountType": "CFD"}],
                  "contracts": [{"contractId": 400000303}]}
    d = parse_account_directory(live_shape)
    assert d.trading_ids == (("400000202", "400000101"),)
    assert d.currency_of("400000101") == "AUD"
