"""Plan item 6.1: broker socket reset options, the per-adapter REST token bucket, stream mapping that never
waits on REST, and the credential age. No network I/O: sockets are created but never connected."""
from __future__ import annotations

import asyncio
import socket
import struct
import sys
import time
from collections.abc import AsyncIterator
from typing import Any, TypeVar

import httpx
import pytest

from src.adapters.base import StreamEvent, StreamEventKind, TransientBrokerError
from src.adapters.mock import MockBrokerAdapter
from src.adapters.pepperstone.tokens import InMemoryTokenStore, TokenManager, TokenPair
from src.adapters.ratelimit import (
    RestBudget,
    RestPriority,
    current_rest_priority,
    parse_retry_after,
    rest_priority,
)
from src.adapters.sockets import (
    TCP_USER_TIMEOUT_MS,
    apply_reset_options,
    install_lightstreamer_socket_options,
    linger_reset,
    reset_http_client,
    reset_socket_factory,
    reset_socket_options,
)
from src.adapters.stonex.adapter import StoneXAdapter
from src.adapters.stonex.rest import StoneXRestClient
from src.adapters.stonex.streaming import LightstreamerTransport
from src.config import (
    CTRADER_REST_BURST,
    STONEX_REST_BURST,
    STONEX_REST_RATE_PER_S,
    PepperstoneSettings,
    StoneXSettings,
)
from src.dto import AccountDTO, OrderDTO

from .conftest import EURUSD_ID, TRADING, FakeStoneX, FakeStreamTransport, make_adapter, make_settings, wait_until, wcf
from .pepperstone.conftest import EURUSD as CT_EURUSD
from .pepperstone.conftest import FakeCTrader, fake  # noqa: F401 - the cTrader fixture
from .pepperstone.conftest import make_adapter as make_ctrader

T0 = 1_767_225_600_000
T = TypeVar("T")


async def take(gen: AsyncIterator[T], n: int, timeout: float = 2.0) -> list[T]:
    return [await asyncio.wait_for(gen.__anext__(), timeout) for _ in range(n)]


def order_frame(order_id: str, status: str, **kw: str | None) -> dict[str, str | None]:
    values: dict[str, str | None] = {
        "OrderId": order_id, "Direction": "1", "Type": "1", "ExecutionPrice": "0", "LastChangedTime": wcf(T0),
        "Status": status, "Quantity": "10000", "OpenPrice": "1.08500", "ReasonId": "1", "MarketId": EURUSD_ID,
        "TradingAccountId": "2001", "OriginalQuantity": "10000"}
    values.update(kw)
    return values


def _linger(sock: socket.socket) -> tuple[int, int]:
    raw = sock.getsockopt(socket.SOL_SOCKET, socket.SO_LINGER, 8)
    fmt = "HH" if sys.platform == "win32" else "ii"
    on, secs = struct.unpack(fmt, raw[:struct.calcsize(fmt)])
    return int(on), int(secs)


# --- sockets: SO_LINGER(on, 0) + TCP_USER_TIMEOUT --------------------------------------------------------
def test_reset_options_set_linger_zero_and_user_timeout_on_a_real_socket() -> None:
    opts = reset_socket_options()
    assert (socket.SOL_SOCKET, socket.SO_LINGER, linger_reset()) in opts
    if hasattr(socket, "TCP_USER_TIMEOUT"):
        assert (socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT, TCP_USER_TIMEOUT_MS) in opts
    assert TCP_USER_TIMEOUT_MS <= 2000
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        assert apply_reset_options(sock)
        assert _linger(sock) == (1, 0)
        if hasattr(socket, "TCP_USER_TIMEOUT"):
            assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT) == TCP_USER_TIMEOUT_MS
    sock = reset_socket_factory((socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 0)))
    try:
        assert _linger(sock) == (1, 0)
    finally:
        sock.close()


async def test_stonex_rest_default_client_carries_the_reset_options() -> None:
    client = reset_http_client(5.0)
    try:
        pool: Any = client._transport._pool  # type: ignore[attr-defined]
        assert list(pool._socket_options) == reset_socket_options()
    finally:
        await client.aclose()
    rest = StoneXRestClient.create(make_settings())
    try:
        inner: Any = rest._t._client._transport._pool  # type: ignore[attr-defined]
        assert (socket.SOL_SOCKET, socket.SO_LINGER, linger_reset()) in list(inner._socket_options)
    finally:
        await rest.aclose()


def test_lightstreamer_session_opens_sockets_through_the_reset_factory() -> None:
    from lightstreamer.client import com_lightstreamer_net as net

    assert install_lightstreamer_socket_options()
    assert install_lightstreamer_socket_options()          # idempotent
    assert net.SessionPy.session.connector._socket_factory is reset_socket_factory

    async def recreate() -> Any:
        old = net.SessionPy.session
        net.SessionPy.session = None
        await old.close()
        return net.SessionPy.getInstance()                 # what the library calls on its IO thread

    fresh = net.ls_io_thread.submit_coro(recreate()).result(timeout=5)
    assert fresh.connector._socket_factory is reset_socket_factory


def test_lightstreamer_transport_installs_options_before_the_client_connects() -> None:
    order: list[str] = []

    class Stop(Exception):
        pass

    def install() -> bool:
        order.append("install")
        raise Stop

    with pytest.raises(Stop):
        LightstreamerTransport("https://push.sx.test", install_socket_options=install).connect(
            "u", "token", lambda s: None, lambda c, m: None)
    assert order == ["install"]


async def test_ctrader_connector_applies_the_reset_options(monkeypatch: pytest.MonkeyPatch) -> None:
    import websockets.asyncio.client as ws_client

    from src.adapters.pepperstone.session import websockets_connector

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    class FakeTransport:
        def get_extra_info(self, name: str) -> Any:
            return sock if name == "socket" else None

    class FakeWs:
        transport = FakeTransport()

    async def fake_connect(url: str, **kw: Any) -> FakeWs:
        return FakeWs()

    monkeypatch.setattr(ws_client, "connect", fake_connect)
    try:
        await websockets_connector("wss://fake.ctrader.test:5036")
        assert _linger(sock) == (1, 0)
    finally:
        sock.close()


# --- RestBudget -----------------------------------------------------------------------------------------
async def test_budget_serves_the_burst_then_paces_at_the_rate() -> None:
    budget = RestBudget("x", rate_per_s=50.0, burst=2)
    start = time.monotonic()
    for _ in range(3):
        await budget.acquire()
    assert time.monotonic() - start >= 0.015            # the third token needed ~20 ms of refill
    assert budget.stats().requests["reconcile"] == 3


async def test_waiters_are_served_by_priority_not_arrival() -> None:
    budget = RestBudget("x", rate_per_s=40.0, burst=1)
    await budget.acquire()                               # bucket empty
    served: list[str] = []

    async def call(p: RestPriority) -> None:
        with rest_priority(p):
            await budget.acquire()
        served.append(p.label)

    tasks = [asyncio.create_task(call(RestPriority.POLL))]
    await asyncio.sleep(0)
    tasks.append(asyncio.create_task(call(RestPriority.DLQ)))
    await asyncio.sleep(0)
    tasks.append(asyncio.create_task(call(RestPriority.CASH_FLOWS)))
    await asyncio.sleep(0)
    tasks.append(asyncio.create_task(call(RestPriority.RECONCILE)))
    await asyncio.wait_for(asyncio.gather(*tasks), 2)
    assert served == ["reconcile", "cash_flows", "dlq", "poll"]
    assert budget.stats().requests == {"reconcile": 2, "cash_flows": 1, "dlq": 1, "poll": 1}


async def test_background_classes_leave_a_reserve_for_reconcile() -> None:
    budget = RestBudget("x", rate_per_s=0.001, burst=4,
                        max_wait_s={p: 0.05 for p in RestPriority})
    await budget.acquire(RestPriority.POLL)
    await budget.acquire(RestPriority.POLL)
    with pytest.raises(TransientBrokerError, match="poll"):
        await budget.acquire(RestPriority.POLL)          # 2 left = POLL's reserve (50 % of 4)
    await budget.acquire(RestPriority.RECONCILE)
    await budget.acquire(RestPriority.RECONCILE)
    stats = budget.stats()
    assert stats.refused == 1 and stats.waiting == 0 and stats.tokens < 1


async def test_untagged_calls_count_as_reconcile_and_tags_follow_the_task() -> None:
    assert current_rest_priority() is RestPriority.RECONCILE
    budget = RestBudget("x", rate_per_s=100.0, burst=10)
    with rest_priority(RestPriority.DLQ):
        assert current_rest_priority() is RestPriority.DLQ
        await asyncio.create_task(budget.acquire())      # a child task inherits the tag
    await budget.acquire()
    assert current_rest_priority() is RestPriority.RECONCILE
    assert budget.stats().requests["dlq"] == 1 and budget.stats().requests["reconcile"] == 1


async def test_throttle_answer_empties_and_blocks_the_bucket() -> None:
    budget = RestBudget("x", rate_per_s=1000.0, burst=10)
    budget.throttled(0.06)
    start = time.monotonic()
    await budget.acquire()
    assert time.monotonic() - start >= 0.05
    assert budget.stats().throttled == 1
    assert parse_retry_after("2") == 2.0 and parse_retry_after("Wed, 21 Oct 2026") is None
    assert parse_retry_after(None) is None


async def test_spend_never_waits_and_cancelled_waiters_leave_the_queue() -> None:
    budget = RestBudget("x", rate_per_s=0.001, burst=1)
    budget.throttled(30)
    budget.spend()                                       # lease-checked call: no wait even when blocked
    assert budget.tokens() < 0
    waiter = asyncio.create_task(budget.acquire())
    await asyncio.sleep(0.01)
    assert budget.stats().waiting == 1
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert budget.stats().waiting == 0
    with pytest.raises(ValueError):
        RestBudget("x", rate_per_s=0, burst=1)


# --- StoneX: one bucket for every REST caller -------------------------------------------------------------
async def test_stonex_rest_calls_share_the_adapter_bucket_by_priority(fake_broker: FakeStoneX) -> None:
    adapter = make_adapter(fake_broker)
    budget = adapter.rest_budget
    assert (budget.rate_per_s, budget.capacity) == (STONEX_REST_RATE_PER_S, STONEX_REST_BURST)
    await adapter.connect()                               # LogOn (spent) + account information
    await adapter.fetch_positions()
    with rest_priority(RestPriority.DLQ):
        await adapter.fetch_order_status("999")
    with rest_priority(RestPriority.POLL):
        await adapter.fetch_account_snapshot()
    with rest_priority(RestPriority.CASH_FLOWS):
        await adapter.fetch_cash_flows(T0)
    requests = budget.stats().requests
    assert requests["dlq"] == 1 and requests["poll"] == 1 and requests["cash_flows"] == 1
    assert sum(requests.values()) == len(fake_broker.requests)
    await adapter.close()
    await adapter.connect()                               # a re-created REST client keeps the same bucket
    assert adapter.rest_budget is budget
    await adapter.close()


async def test_stonex_logon_does_not_wait_and_a_429_throttles_the_bucket(fake_broker: FakeStoneX) -> None:
    adapter = make_adapter(fake_broker)
    adapter.rest_budget.throttled(30)                     # bucket blocked for 30 s
    await asyncio.wait_for(adapter._rest.log_on(), 1)      # LogOn behind the lease check never waits
    fresh = make_adapter(fake_broker)
    await fresh.connect()
    fake_broker.one_shot["/TradingAPI/order/openpositions"] = httpx.Response(
        429, headers={"Retry-After": "0.05"}, json={})
    with pytest.raises(TransientBrokerError, match="throttled"):
        await fresh.fetch_positions()
    assert fresh.rest_budget.stats().throttled == 1
    start = time.monotonic()
    await fresh.fetch_positions()                          # waited for Retry-After first
    assert time.monotonic() - start >= 0.04
    await fresh.close()
    await adapter.close()


def test_rest_limits_come_from_the_environment() -> None:
    env = {"STONE_X_USERNAME": "u", "STONE_X_PASS": "p", "STONE_X_APP_KEY": "k",
           "STONE_X_REST_RATE_PER_S": "2.5", "STONE_X_REST_BURST": "4"}
    s = StoneXSettings.from_env(env)
    assert (s.rest_rate_per_s, s.rest_burst) == (2.5, 4)
    assert StoneXAdapter(s).rest_budget.capacity == 4
    with pytest.raises(ValueError):
        StoneXSettings.from_env(env | {"STONE_X_REST_BURST": "0"})
    ct = {"CTRADER_CLIENT_ID": "c", "CTRADER_CLIENT_SECRET": "s", "CTRADER_ACCESS_TOKEN": "a",
          "CTRADER_REFRESH_TOKEN": "r", "CTRADER_ACCOUNT_ID": "9"}
    assert PepperstoneSettings.from_env(ct).rest_burst == CTRADER_REST_BURST
    assert PepperstoneSettings.from_env(ct | {"CTRADER_REST_RATE_PER_S": "7"}).rest_rate_per_s == 7.0
    with pytest.raises(ValueError):
        PepperstoneSettings.from_env(ct | {"CTRADER_REST_RATE_PER_S": "fast"})


# --- cTrader: requests metered per adapter, heartbeats and auth never wait --------------------------------
async def test_ctrader_requests_draw_from_one_adapter_bucket(fake: FakeCTrader) -> None:
    adapter = make_ctrader(fake)
    await adapter.connect()
    try:
        budget = adapter.rest_budget
        assert sum(budget.stats().requests.values()) == len([t for t in fake.types_sent() if t != 51])
        with rest_priority(RestPriority.DLQ):
            await adapter.fetch_positions()
        assert budget.stats().requests["dlq"] >= 1
        fake.errors[2124] = ["REQUEST_FREQUENCY_EXCEEDED"]            # reconcile request throttled
        with pytest.raises(Exception, match="REQUEST_FREQUENCY_EXCEEDED"):
            await adapter.fetch_positions()
        assert budget.stats().throttled == 1
        await adapter.close()
        await adapter.connect()                                       # a new session, the same bucket
        assert adapter.rest_budget is budget and adapter.session is not None
        assert adapter.session._budget is budget
    finally:
        await adapter.close()


async def test_ctrader_auth_requests_do_not_wait_for_tokens(fake: FakeCTrader) -> None:
    adapter = make_ctrader(fake)
    adapter.rest_budget.throttled(30)
    sent: list[int] = []
    fake.on_request = lambda sock, frame: sent.append(int(frame["payloadType"]))
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(adapter.connect(), 0.3)    # scope check waits; application auth did not
    assert sent[:1] == [2100]
    await adapter.close()


# --- the instrument map is refreshed at reconcile priority, whoever met the unknown symbol ----------------
async def test_stonex_instrument_refresh_from_a_dlq_read_runs_at_reconcile_priority(fake_broker: FakeStoneX) -> None:
    """The ORDERS stream waits on the same refresh lock: a DLQ / poll caller must not hold it while queued
    behind its own reserve (priority inversion that would stall stream mapping)."""
    fake_broker.orders["888"] = {"TradeOrder": {"OrderId": 888, "MarketId": int(EURUSD_ID), "Direction": "Buy",
                                                "Quantity": 1000, "Price": 1.09, "Status": 3,
                                                "TradingAccountId": int(TRADING)}}
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    before = dict(adapter.rest_budget.stats().requests)
    with rest_priority(RestPriority.DLQ):
        trade = await adapter.fetch_order_status("888")
    assert trade is not None and trade.symbol == "EURUSD"
    after = adapter.rest_budget.stats().requests
    assert "/v2/market/information" in fake_broker.paths()
    assert after["dlq"] - before["dlq"] == 1                      # only the order read itself
    assert after["reconcile"] - before["reconcile"] == 1          # the market-information refresh
    await adapter.close()


async def test_ctrader_instrument_refresh_from_a_poll_read_runs_at_reconcile_priority(fake: FakeCTrader) -> None:
    adapter = make_ctrader(fake)
    await adapter.connect()
    try:
        before = dict(adapter.rest_budget.stats().requests)
        with rest_priority(RestPriority.POLL):
            await adapter._ensure_symbols([CT_EURUSD])
        after = adapter.rest_budget.stats().requests
        assert adapter.instrument_for_symbol_id(CT_EURUSD) is not None
        assert after["poll"] == before["poll"]
        assert after["reconcile"] > before["reconcile"]
    finally:
        await adapter.close()


def test_rest_limits_refuse_a_non_finite_rate() -> None:
    env = {"STONE_X_USERNAME": "u", "STONE_X_PASS": "p", "STONE_X_APP_KEY": "k"}
    for bad in ("inf", "nan", "-1"):
        with pytest.raises(ValueError):
            StoneXSettings.from_env(env | {"STONE_X_REST_RATE_PER_S": bad})
    with pytest.raises(ValueError):
        RestBudget("x", rate_per_s=float("inf"), burst=1)


# --- stream mapping never waits on REST -----------------------------------------------------------------
def _gated_adapter(fake: FakeStoneX, stream: FakeStreamTransport, gate: asyncio.Event,
                   **kw: Any) -> StoneXAdapter:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/activestoplimitorder"):
            await gate.wait()
        return fake.handler(request)

    rest = StoneXRestClient.create(make_settings(), http=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    return StoneXAdapter(make_settings(), rest=rest, stream_transport_factory=lambda: stream, heartbeat_s=60.0, **kw)


async def test_parent_read_runs_beside_the_stream_and_holds_only_that_order(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    fake_broker.order_active["701"] = {"OrderId": 701, "ParentOrderId": 700}
    gate = asyncio.Event()
    adapter = _gated_adapter(fake_broker, fake_stream, gate)
    await adapter.connect()
    gen = adapter.stream_order_updates()
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    fake_stream.emit("ORDERS", order_frame("700", "3", LastChangedTime=wcf(T0 + 1)))
    await asyncio.wait_for(first, 2)
    await take(gen, 1)                                         # FILLED order + OPEN position
    fake_stream.emit("ORDERS", order_frame("701", "2", Type="2", Direction="0", LastChangedTime=wcf(T0 + 2)))
    fake_stream.emit("ORDERS", order_frame("702", "2", LastChangedTime=wcf(T0 + 3)))
    fake_stream.emit("ORDERS", order_frame("701", "2", Type="2", Direction="0", Quantity="5000",
                                           LastChangedTime=wcf(T0 + 4)))
    (other,) = await take(gen, 1)                              # the parent read is still blocked
    assert isinstance(other, OrderDTO) and other.broker_order_id == "702"
    assert not gate.is_set()
    gate.set()
    held = await take(gen, 2)
    assert [(o.broker_order_id, o.broker_updated_at_ms) for o in held if isinstance(o, OrderDTO)] == [
        ("701", T0 + 2), ("701", T0 + 4)]
    assert all(isinstance(o, OrderDTO) and o.is_reduce_only and o.linked_position_id == "700" for o in held)
    await gen.aclose()
    await adapter.close()


async def test_a_parent_read_that_times_out_releases_the_held_frames(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    gate = asyncio.Event()                                     # never set: the read hangs
    adapter = _gated_adapter(fake_broker, fake_stream, gate, parent_lookup_timeout_s=0.05)
    await adapter.connect()
    gen = adapter.stream_order_updates()
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    fake_stream.emit("ORDERS", order_frame("801", "2", Type="2", Direction="0", LastChangedTime=wcf(T0 + 2)))
    stop = await asyncio.wait_for(first, 2)
    assert isinstance(stop, OrderDTO) and stop.broker_order_id == "801" and not stop.is_reduce_only
    await gen.aclose()
    await adapter.close()


async def test_unmappable_stream_frames_open_a_gap_instead_of_vanishing(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    adapter = make_adapter(fake_broker, fake_stream)
    await adapter.connect()
    orders = adapter.stream_order_updates()
    first = asyncio.ensure_future(orders.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    fake_stream.emit("ORDERS", order_frame("", "2") | {"OrderId": None})
    gap = await asyncio.wait_for(first, 2)
    assert isinstance(gap, StreamEvent) and gap.kind == StreamEventKind.GAP and gap.stream == "orders"
    await orders.aclose()
    account = adapter.stream_account()
    first_acct = asyncio.ensure_future(account.__anext__())
    await wait_until(lambda: any(s.data_adapter == "CLIENTACCOUNTMARGIN" and s.active for s in fake_stream.subs))
    fake_stream.emit("CLIENTACCOUNTMARGIN", {"Margin": "50"})          # no Cash
    gap2 = await asyncio.wait_for(first_acct, 2)
    assert isinstance(gap2, StreamEvent) and gap2.kind == StreamEventKind.GAP and gap2.stream == "account"
    fake_stream.emit("CLIENTACCOUNTMARGIN", {"Cash": "1000", "CurrencyISO": "AUD"})
    (acct,) = await take(account, 1)
    assert isinstance(acct, AccountDTO)
    await account.aclose()
    await adapter.close()


# --- credential age ------------------------------------------------------------------------------------
async def test_stonex_token_age_is_the_session_since_logon(fake_broker: FakeStoneX) -> None:
    now = [T0]
    adapter = make_adapter(fake_broker, wall_ms=lambda: now[0])
    assert adapter.token_issued_at_ms is None and adapter.token_expires_at_ms is None
    await adapter.connect()
    assert adapter.token_issued_at_ms == T0
    now[0] += 5_000
    await adapter.connect()                                     # re-LogOn: a new session, a new age
    assert adapter.token_issued_at_ms == T0 + 5_000
    await adapter.close()
    assert adapter.token_issued_at_ms is None


async def test_ctrader_token_age_is_the_pair_since_its_save(fake: FakeCTrader) -> None:
    fake.valid_access = {"ACCESS-TOKEN-2-SECRET"}               # forces a refresh + save at connect
    store = InMemoryTokenStore(wall_ms=lambda: T0 + 7)
    adapter = make_ctrader(fake, token_store=store)
    assert adapter.token_issued_at_ms is None                   # the environment's initial pair
    await adapter.connect()
    try:
        assert adapter.token_issued_at_ms == T0 + 60_000            # the adapter's clock at the save
        assert store.pair is not None and store.pair.saved_at_ms == T0 + 7
        assert adapter.token_expires_at_ms == adapter._tokens.pair.expires_at_ms is not None
    finally:
        await adapter.close()


async def test_token_manager_keeps_its_save_time_when_the_store_reports_none() -> None:
    class NoTimeStore(InMemoryTokenStore):
        async def load(self) -> TokenPair | None:
            p = self.pair
            return None if p is None else TokenPair(p.access_token, p.refresh_token, p.expires_at_ms, p.generation)

    store = NoTimeStore(TokenPair("a1", "r1", None, 1))

    async def refresher(token: str) -> Any:
        from src.adapters.pepperstone.tokens import RefreshedTokens
        return RefreshedTokens("a2", "r2", None)

    manager = TokenManager(TokenPair("a0", "r0", None, 0), store, wall_ms=lambda: T0 + 9)
    await manager.load()
    assert manager.issued_at_ms is None                          # stored pair, no save time known
    await manager.refresh(refresher)
    assert manager.generation == 2 and manager.issued_at_ms == T0 + 9
    await manager.load()                                         # same generation reloaded without a time
    assert manager.issued_at_ms == T0 + 9


# --- mock: records the caller's priority, optional bucket --------------------------------------------------
async def test_mock_adapter_records_priorities_and_uses_an_attached_bucket() -> None:
    budget = RestBudget("mock", rate_per_s=100.0, burst=5)
    mock = MockBrokerAdapter(rest_budget=budget)
    with rest_priority(RestPriority.POLL):
        await mock.fetch_account_snapshot()
    await mock.fetch_positions()
    assert mock.rest_calls == [("fetch_account_snapshot", RestPriority.POLL),
                               ("fetch_positions", RestPriority.RECONCILE)]
    assert mock.rest_budget is budget and budget.stats().requests["poll"] == 1
    assert MockBrokerAdapter().rest_budget is None
