"""cTrader allow-list, framing, settings and the token pair (no network)."""
from __future__ import annotations

import json

import pytest

from src.adapters.base import AuthFailedError, TransientBrokerError
from src.adapters.pepperstone.protocol import (
    OUTBOUND_ALLOW_LIST,
    PT_HEARTBEAT_EVENT,
    PT_RECONCILE_REQ,
    assert_allowed,
    decode_frame,
    encode_request,
    scope_name,
)
from src.adapters.pepperstone.session import OpenApiSession
from src.adapters.pepperstone.tokens import InMemoryTokenStore, RefreshedTokens, TokenManager, TokenPair
from src.config import ExecutionLockedError, PepperstoneSettings

from .conftest import CLIENT_SECRET, FakeCTrader

# Order-mutating ProtoOA payload types (new / cancel / amend / amend SL-TP / close position) and
# other non-read messages (trendbar / depth subscriptions, margin-call settings, account logout).
REFUSED = [2106, 2108, 2109, 2110, 2111, 2135, 2137, 2139, 2145, 2156, 2162, 2169, 2187, 0, -1, 99999]


@pytest.mark.parametrize("payload_type", REFUSED)
def test_non_allow_listed_payload_types_are_refused(payload_type: int) -> None:
    with pytest.raises(ExecutionLockedError):
        assert_allowed(payload_type)
    with pytest.raises(ExecutionLockedError):
        encode_request(payload_type, {"ctidTraderAccountId": 1}, "m1")


def test_allow_list_is_the_read_only_set() -> None:
    assert OUTBOUND_ALLOW_LIST == frozenset({2100, 2102, 2149, 2173, 2104, 2124, 2181, 2175, 2133, 2179, 2143, 2121,
                                             2114, 2116, 2112, 2127, 2129, 51})
    assert not OUTBOUND_ALLOW_LIST & {2106, 2108, 2109, 2110, 2111}
    with pytest.raises(ExecutionLockedError):
        assert_allowed(True)   # bool is never a payload type


def test_encode_and_decode_frames() -> None:
    text = encode_request(PT_RECONCILE_REQ, {"ctidTraderAccountId": 7}, "xtnl-1")
    assert json.loads(text) == {"payloadType": 2124, "payload": {"ctidTraderAccountId": 7}, "clientMsgId": "xtnl-1"}
    frame = decode_frame('{"payloadType": "2131", "payload": {"symbolId": 1}}')
    assert frame is not None and frame.payload_type == 2131 and frame.client_msg_id is None
    assert decode_frame("not json") is None
    assert decode_frame('{"payload": {}}') is None
    assert decode_frame("[1, 2]") is None


def test_scope_names() -> None:
    assert scope_name(0) == "SCOPE_VIEW"
    assert scope_name("SCOPE_TRADE") == "SCOPE_TRADE"
    assert scope_name("1") == "SCOPE_TRADE"
    assert scope_name(None) is None


async def test_refused_payload_never_reaches_the_socket(fake: FakeCTrader) -> None:
    closed: list[str] = []
    session = OpenApiSession("wss://fake.ctrader.test:5036", fake.connect, on_event=lambda f: None,
                             on_closed=closed.append, heartbeat_s=60.0)
    await session.open()
    try:
        with pytest.raises(ExecutionLockedError):
            await session.send(2106, {"ctidTraderAccountId": 1})
        with pytest.raises(ExecutionLockedError):
            await session.request(2111, {"positionId": 1})
        assert fake.sent == []
        await session.send(PT_HEARTBEAT_EVENT, {})
        assert fake.types_sent() == [51]
    finally:
        await session.close()
    assert closed == ["session closed"]


# --- settings ---------------------------------------------------------------------------------------
ENV = {"CTRADER_CLIENT_ID": "cid", "CTRADER_CLIENT_SECRET": "csecret-XYZ", "CTRADER_ACCESS_TOKEN": "atok-XYZ",
       "CTRADER_REFRESH_TOKEN": "rtok-XYZ", "CTRADER_ACCOUNT_ID": "12345"}


def test_settings_from_env_hosts_and_repr() -> None:
    demo = PepperstoneSettings.from_env({**ENV, "BROKER_ENV": "sandbox"})
    assert demo.ws_url == "wss://demo.ctraderapi.com:5036" and demo.account_id == 12345
    live = PepperstoneSettings.from_env({**ENV, "BROKER_ENV": "live_readonly"})
    assert live.ws_url == "wss://live.ctraderapi.com:5036" and live.env == "live_readonly"
    custom = PepperstoneSettings.from_env({**ENV, "CTRADER_HOST": "wss://other.host:6000/"})
    assert custom.ws_url == "wss://other.host:6000"
    text = repr(live)
    for secret in ("csecret-XYZ", "atok-XYZ", "rtok-XYZ"):
        assert secret not in text and secret not in str(live)


def test_settings_refuse_bad_env_and_missing_vars() -> None:
    with pytest.raises(ExecutionLockedError):
        PepperstoneSettings.from_env({**ENV, "BROKER_ENV": "live"})
    with pytest.raises(KeyError):
        PepperstoneSettings.from_env({k: v for k, v in ENV.items() if k != "CTRADER_REFRESH_TOKEN"})
    with pytest.raises(ValueError):
        PepperstoneSettings.from_env({**ENV, "CTRADER_ACCOUNT_ID": "abc"})


# --- token pair -------------------------------------------------------------------------------------
def test_token_repr_hides_material() -> None:
    pair = TokenPair("ACCESS-SECRET", "REFRESH-SECRET", 5, 3)
    assert "SECRET" not in repr(pair) and "SECRET" not in repr(RefreshedTokens("A-SECRET", "R-SECRET", None))
    assert CLIENT_SECRET not in repr(pair)


async def test_refresh_saves_before_use_and_bumps_generation() -> None:
    store = InMemoryTokenStore(TokenPair("a1", "r1", 1000, 4))
    seen_store_gen: list[int] = []

    async def refresher(refresh_token: str) -> RefreshedTokens:
        assert refresh_token == "r1"
        return RefreshedTokens("a2", "r2", 9000)

    manager = TokenManager(TokenPair("env-a", "env-r", None, 0), store)
    assert (await manager.load()).generation == 4          # the stored pair wins over the environment's
    pair = await manager.refresh(refresher)
    seen_store_gen.append(store.pair.generation if store.pair else -1)
    assert pair.access_token == "a2" and pair.generation == 5 and seen_store_gen == [5]
    assert store.saves == 1
    # a caller that saw generation 4 fail adopts 5 instead of refreshing twice
    again = await manager.refresh(refresher, used_generation=4)
    assert again.generation == 5 and store.saves == 1


async def test_refresh_adopts_a_newer_stored_generation() -> None:
    store = InMemoryTokenStore(TokenPair("a1", "r1", None, 1))
    manager = TokenManager(TokenPair("a1", "r1", None, 1), store)
    store.pair = TokenPair("a9", "r9", None, 7)             # another leader refreshed meanwhile
    calls: list[str] = []

    async def refresher(refresh_token: str) -> RefreshedTokens:
        calls.append(refresh_token)
        return RefreshedTokens("x", "y", None)

    pair = await manager.refresh(refresher)
    assert pair.generation == 7 and pair.access_token == "a9" and calls == []


async def test_refresh_in_flight_elsewhere_is_transient() -> None:
    store = InMemoryTokenStore(TokenPair("a1", "r1", None, 2))
    store.in_flight = 2
    manager = TokenManager(TokenPair("a1", "r1", None, 2), store)

    async def refresher(refresh_token: str) -> RefreshedTokens:
        raise AssertionError("must not refresh generation 2 twice")

    with pytest.raises(TransientBrokerError):
        await manager.refresh(refresher)


async def test_failed_save_keeps_the_pair_unused_then_finishes_the_save() -> None:
    class FlakyStore(InMemoryTokenStore):
        fail = 3

        async def save(self, expected_generation: int, pair: TokenPair) -> int | None:
            if self.fail:
                self.fail -= 1
                raise ConnectionError("pg down")
            return await super().save(expected_generation, pair)

    store = FlakyStore(TokenPair("a1", "r1", None, 1))
    manager = TokenManager(TokenPair("a1", "r1", None, 1), store, save_attempts=2, save_retry_s=0.0)
    refreshes: list[str] = []

    async def refresher(refresh_token: str) -> RefreshedTokens:
        refreshes.append(refresh_token)
        return RefreshedTokens("a2", "r2", None)

    with pytest.raises(TransientBrokerError):
        await manager.refresh(refresher)
    assert manager.pair.access_token == "a1"               # never used before it is saved
    pair = await manager.refresh(refresher)                 # resumes the save, no second refresh
    assert pair.access_token == "a2" and pair.generation == 2 and refreshes == ["r1"]


async def test_cas_miss_without_newer_pair_is_auth_failed() -> None:
    class MissStore(InMemoryTokenStore):
        async def save(self, expected_generation: int, pair: TokenPair) -> int | None:
            return None

    store = MissStore(TokenPair("a1", "r1", None, 1))
    manager = TokenManager(TokenPair("a1", "r1", None, 1), store)

    async def refresher(refresh_token: str) -> RefreshedTokens:
        return RefreshedTokens("a2", "r2", None)

    with pytest.raises(AuthFailedError):
        await manager.refresh(refresher)
