"""RedisFeedStream (XREADGROUP / XAUTOCLAIM / XACK), live-key readers and the halt:state mirror (fakeredis)."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from src.cache import keys as K
from src.cache.client import create_redis
from src.cache.feed_stream import RedisFeedStream
from src.cache.halt_mirror import RedisHaltMirror
from src.cache.live_reader import (
    account_live_from_hash,
    price_from_hash,
    read_account_live,
    read_gate_keys,
    read_quotes,
)
from src.cache.scripts import RedisState
from src.dto import AccountLive, PriceDTO
from src.oversee.feed import FeedObserver, RecordingQuarantine
from src.oversee.types import HaltLevel

from tests.cache.conftest import BOOT


async def run_id(_: Any) -> str | None:
    return "run123"


class Sweep:
    def __init__(self) -> None:
        self.woken: list[str] = []

    def sweep_now(self, account_id: str) -> None:
        self.woken.append(account_id)


async def test_feed_group_read_ack_and_claim(redis: Any) -> None:
    feed = RedisFeedStream(redis, "node-a")
    await feed.ensure_group()
    await feed.ensure_group()   # BUSYGROUP counts as success
    for acct in ("A1", "A2"):
        await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": acct})
    entries = await feed.read_group(10, 10)
    assert [(e.account_id, e.deliveries) for e in entries] == [("A1", 1), ("A2", 1)]
    await feed.ack([entries[0].entry_id])
    await feed.ack([])
    claimed = await RedisFeedStream(redis, "node-b").autoclaim(0, 10)
    assert [(e.entry_id, e.deliveries) for e in claimed] == [(entries[1].entry_id, 2)]
    assert await feed.read_group(10, 10) == []
    assert await RedisFeedStream(redis, "node-b").autoclaim(60_000, 10) == []   # not idle long enough


async def test_feed_observer_end_to_end(redis: Any, state: RedisState) -> None:
    feed = RedisFeedStream(redis, "node-a")
    await feed.ensure_group()
    await redis.xadd(K.FEED, {"entity_type": "ACCOUNT", "account_id": "ACC"})
    sweep, quarantine = Sweep(), RecordingQuarantine()
    obs = FeedObserver(feed, sweep, lambda: BOOT, quarantine, clock=lambda: 0)
    assert await feed.boot_id() == BOOT
    assert await obs.poll_once() == 1 and sweep.woken == ["ACC"]
    assert (await redis.xpending(K.FEED, "oversee"))["pending"] == 0   # XACKed after the wake-up
    await redis.delete(K.BOOT_ID)
    await redis.xadd(K.FEED, {"entity_type": "ORDER", "account_id": "ACC"})
    assert await obs.poll_once() == 0   # SM8 not READY: nothing consumed


async def test_read_quotes_and_account_live(redis: Any, state: RedisState) -> None:
    await state.write_price(PriceDTO(adapter_id="stonex", symbol="EURUSD", bid=Decimal("1.1"), ask=Decimal("1.1002"),
                                     spread_pips=Decimal("2"), timestamp_ms=1_000), Decimal("1"))   # wide: no clean
    await state.write_price(PriceDTO(adapter_id="stonex", symbol="GBPUSD", bid=Decimal("1.3"), ask=Decimal("1.3001"),
                                     spread_pips=Decimal("0.5"), timestamp_ms=2_000), Decimal("1"))
    quotes = await read_quotes(redis, [("stonex", "EURUSD"), ("stonex", "GBPUSD"), ("stonex", "USDJPY")])
    assert set(quotes) == {("stonex", "EURUSD"), ("stonex", "GBPUSD")}
    eur = quotes[("stonex", "EURUSD")]
    assert eur.latest is not None and eur.latest.ask == Decimal("1.1002") and eur.clean_mid is None
    assert quotes[("stonex", "GBPUSD")].clean_mid == (Decimal("1.30005"), 2_000)
    assert await read_quotes(redis, []) == {}
    await state.write_account_live(AccountLive(account_id="ACC", adapter_id="stonex", currency="USD", equity=None,
                                               margin_used=Decimal(5), free_margin=Decimal(1),
                                               unrealised_pnl=Decimal(0), timestamp_ms=3_000, sampled=True))
    live = await read_account_live(redis, "ACC")
    assert live is not None and live.equity is None and live.sampled and live.margin_used == Decimal(5)
    assert await read_account_live(redis, "NOPE") is None


def test_malformed_hashes_are_absent() -> None:
    assert price_from_hash("a", "s", {"bid": "x", "ask": "1", "ts": "1"}) is None
    assert price_from_hash("a", "s", {"bid": "1", "ask": "1"}) is None
    assert account_live_from_hash("A", {"ts": "nope"}) is None


async def test_gate_keys_and_halt_mirror(redis: Any, state: RedisState) -> None:
    keys = await read_gate_keys(redis, run_id)
    assert (keys.boot_id, keys.run_id, keys.halt_state) == (BOOT, "run123", None)
    mirror = RedisHaltMirror(state)
    assert await mirror.read() is None
    assert await mirror.cas_set(HaltLevel.SOFT, 5) is True
    assert await mirror.cas_set(HaltLevel.NONE, 4) is False
    assert await mirror.read() == (HaltLevel.SOFT, 5)
    assert (await read_gate_keys(redis, run_id)).halt_state == ("SOFT", 5)


def test_client_factory_builds_without_connecting() -> None:
    r = create_redis("redis://127.0.0.1:6399/0")
    assert r.connection_pool.connection_kwargs["socket_timeout"] == 2.0
