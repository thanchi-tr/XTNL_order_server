"""light.lua, halt_cas.lua, conn: keys, feed trim and the guarded flush."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from src.cache import keys as K
from src.cache.scripts import (
    RedisState,
    RedisStreamHealth,
    boot_id_value,
    flush_local_redis,
    read_boot_id,
    trim_feed,
)
from src.dto import AccountLive, CashFlowDTO, PriceDTO


def price(ts: int, bid: str = "1.1000", ask: str = "1.1002", spread: str | None = "0.2") -> PriceDTO:
    return PriceDTO(adapter_id="stonex", symbol="EURUSD", bid=Decimal(bid), ask=Decimal(ask),
                    spread_pips=None if spread is None else Decimal(spread), timestamp_ms=ts)


async def test_price_and_clean_mid_for_tight_spread(state: RedisState, redis: Any) -> None:
    assert await state.write_price(price(1000), Decimal("1.0")) == (True, True)
    p = await redis.hgetall(K.price_key("stonex", "EURUSD"))
    assert p[b"bid"] == b"1.1000" and p[b"ts"] == b"0000000000001000"
    c = await redis.hgetall(K.clean_mid_key("stonex", "EURUSD"))
    assert c[b"mid"] == b"1.1001"


async def test_wide_spread_updates_price_only(state: RedisState, redis: Any) -> None:
    await state.write_price(price(1000), Decimal("1.0"))
    assert await state.write_price(price(2000, "1.2000", "1.2050", "5.0"), Decimal("1.0")) == (True, False)
    assert (await redis.hget(K.price_key("stonex", "EURUSD"), "bid")) == b"1.2000"
    assert (await redis.hget(K.clean_mid_key("stonex", "EURUSD"), "mid")) == b"1.1001"


async def test_unknown_spread_or_threshold_is_never_clean(state: RedisState, redis: Any) -> None:
    assert await state.write_price(price(1000, spread=None), Decimal("1.0")) == (True, False)
    assert await state.write_price(price(2000), None) == (True, False)
    assert not await redis.exists(K.clean_mid_key("stonex", "EURUSD"))


async def test_latest_live_value_wins_older_tick_ignored(state: RedisState, redis: Any) -> None:
    await state.write_price(price(2000, "1.3"), Decimal("1.0"))
    assert await state.write_price(price(1000, "1.1"), Decimal("1.0")) == (False, False)
    assert (await redis.hget(K.price_key("stonex", "EURUSD"), "bid")) == b"1.3"


async def test_seed_never_overwrites_fresher_value(state: RedisState, redis: Any) -> None:
    await state.write_price(price(5000, "1.3", "1.3002"), Decimal("1.0"))
    assert await state.seed_clean_mark("stonex", "EURUSD", Decimal("1.1"), Decimal("1.1002"), 4000) == (False, False)
    assert (await redis.hget(K.price_key("stonex", "EURUSD"), "bid")) == b"1.3"
    # equal timestamp: a seed still does not overwrite
    assert await state.seed_clean_mark("stonex", "EURUSD", Decimal("1.1"), Decimal("1.1002"), 5000) == (False, False)


async def test_seed_fills_absent_and_older_keys(state: RedisState, redis: Any) -> None:
    assert await state.seed_clean_mark("stonex", "GBPUSD", Decimal("1.25"), Decimal("1.2502"), 4000) == (True, True)
    c = await redis.hgetall(K.clean_mid_key("stonex", "GBPUSD"))
    assert c[b"seeded"] == b"1" and c[b"mid"] == b"1.2501"
    assert await state.seed_clean_mark("stonex", "GBPUSD", Decimal("1.26"), Decimal("1.2602"), 4500) == (True, True)


async def test_account_live_seed_and_live(state: RedisState, redis: Any) -> None:
    live = AccountLive(account_id="ACC", adapter_id="stonex", currency="USD", equity=Decimal("1000"),
                       margin_used=Decimal("10"), free_margin=Decimal("990"), unrealised_pnl=Decimal("0"),
                       timestamp_ms=10_000)
    assert await state.write_account_live(live) is True
    sampled = AccountLive(account_id="ACC", adapter_id="stonex", currency="USD", equity=Decimal("1"),
                          margin_used=None, free_margin=None, unrealised_pnl=None, timestamp_ms=9_000, sampled=True)
    assert await state.write_account_live(sampled, seed=True) is False
    h = await redis.hgetall(K.account_live_key("ACC"))
    assert h[b"equity"] == b"1000" and h[b"sampled"] == b"0"
    assert not await redis.exists(K.FEED)


async def test_conn_keys_expire_and_delete(state: RedisState, redis: Any) -> None:
    health = RedisStreamHealth(state, clock_ms=lambda: 1_700_000_000_000)
    await health.heartbeat("stonex", "orders")
    key = K.conn_key("stonex", "orders")
    ttl = await redis.pttl(key)
    assert 0 < ttl <= 30_000
    await health.mark_lost("stonex", "orders")
    assert not await redis.exists(key)
    await health.mark_reconciled("stonex", "orders")
    assert await redis.exists(key)


async def test_halt_cas_only_increases(state: RedisState, redis: Any) -> None:
    assert await state.set_halt_state("SOFT", 5) is True
    assert await state.set_halt_state("NONE", 4) is False
    assert await state.set_halt_state("HARD", 5) is False
    assert await state.read_halt_state() == ("SOFT", 5)
    # 9 -> 10 digits and beyond 2^53: fixed width keeps the order
    assert await state.set_halt_state("HARD", 10) is True
    assert await state.set_halt_state("NONE", 2**53 + 1) is True
    assert await state.set_halt_state("SOFT", 2**53) is False
    assert await state.read_halt_state() == ("NONE", 2**53 + 1)
    await state.delete_halt_state()
    assert await state.read_halt_state() is None
    assert await state.set_halt_state("SOFT", 1) is True


async def test_halt_cas_rejects_bad_level(state: RedisState) -> None:
    from redis.exceptions import ResponseError
    with pytest.raises(ResponseError):
        await state.set_halt_state("MAYBE", 1)


async def test_cash_flow_feed_entry(state: RedisState, redis: Any) -> None:
    cf = CashFlowDTO(transaction_id="T1", account_id="ACC", adapter_id="stonex", kind="DEPOSIT",
                     amount=Decimal("100"), currency="USD", broker_ts=1, seq_id=77)
    await state.feed_cash_flow(cf)
    entries = await redis.xrange(K.FEED)
    assert entries[0][1] == {b"entity_type": b"ACCOUNT", b"entity_id": b"ACC", b"account_id": b"ACC",
                             b"seq_id": b"77"}


async def test_trim_feed_to_oldest_unacked(state: RedisState, redis: Any) -> None:
    ids = [await redis.xadd(K.FEED, {"n": str(i)}) for i in range(5)]
    await redis.xgroup_create(K.FEED, "oversee", id="0")
    got = await redis.xreadgroup("oversee", "c1", {K.FEED: ">"}, count=5)
    delivered = [eid for eid, _ in got[0][1]]
    await redis.xack(K.FEED, "oversee", *delivered[:3])
    res = await trim_feed(redis, maxlen=8)
    assert res.min_id == ids[3].decode()
    assert res.length == 2
    assert res.lag_alert is False
    res2 = await trim_feed(redis, maxlen=4)
    assert res2.lag_alert is True


async def test_trim_feed_without_stream(redis: Any) -> None:
    res = await trim_feed(redis)
    assert res.min_id is None and res.length == 0


async def test_flush_guard(redis: Any) -> None:
    await redis.set(K.BOOT_ID, boot_id_value("run", "c:1"))
    with pytest.raises(PermissionError):
        await flush_local_redis(redis, "RECONCILE")  # type: ignore[arg-type]
    assert await read_boot_id(redis) == "run:c:1"
    await flush_local_redis(redis, "LEADER_INIT")
    assert await read_boot_id(redis) is None
