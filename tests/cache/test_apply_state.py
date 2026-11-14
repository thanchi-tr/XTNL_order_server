"""apply_state.lua through fakeredis (real Lua 5.1 via lupa)."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any

from src.cache import keys as K
from src.cache.digest import ZERO_DIGEST, entity_hash, xor_hex
from src.cache.scripts import ApplyResult, EntityWrite, RedisState, entity_write, instrument_write, pad
from src.dto import InstrumentSpec, PositionDTO

from tests.cache.conftest import BOOT, Boot, order


async def _digest(redis: Any) -> str:
    raw = await redis.get(K.STATE_DIGEST)
    return ZERO_DIGEST if raw is None else raw.decode()


async def test_new_entity_applies_and_feeds_once(state: RedisState, redis: Any) -> None:
    dto = order()
    w = entity_write(dto)
    assert await state.apply(w) is ApplyResult.APPLIED
    h = await redis.hgetall(w.key)
    assert h[b"v"].decode() == w.version and h[b"cs"].decode() == dto.checksum
    assert h[b"seq"].decode() == pad(dto.seq_id, 20)
    entries = await redis.xrange(K.FEED)
    assert len(entries) == 1
    fields = entries[0][1]
    assert fields == {b"entity_type": b"ORDER", b"entity_id": b"B1", b"account_id": b"ACC", b"seq_id": b"1000"}
    assert await _digest(redis) == w.digest_hash


async def test_newer_version_applies_with_one_more_feed_entry(state: RedisState, redis: Any) -> None:
    await state.apply(entity_write(order()))
    newer = order(stop_loss=Decimal("1.0960"), observed_seq=101, seq_id=1001)
    assert await state.apply(entity_write(newer)) is ApplyResult.APPLIED
    assert await redis.xlen(K.FEED) == 2


async def test_same_checksum_advances_version_without_feed(state: RedisState, redis: Any) -> None:
    first = entity_write(order())
    await state.apply(first)
    same = entity_write(order(observed_seq=150, seq_id=1500))
    assert same.checksum == first.checksum and same.version > first.version
    assert await state.apply(same) is ApplyResult.NOOP_ADVANCED
    h = await redis.hgetall(first.key)
    assert h[b"v"].decode() == same.version
    assert await redis.xlen(K.FEED) == 1
    # digest moved from the old hash to the new one
    assert await _digest(redis) == same.digest_hash


async def test_identical_write_changes_nothing(state: RedisState, redis: Any) -> None:
    w = entity_write(order())
    await state.apply(w)
    before = await _digest(redis)
    assert await state.apply(w) is ApplyResult.NOOP_ADVANCED
    assert await redis.xlen(K.FEED) == 1
    assert await _digest(redis) == before


async def test_stale_version_ignored(state: RedisState, redis: Any) -> None:
    await state.apply(entity_write(order(observed_seq=200, seq_id=2000)))
    older = order(stop_loss=Decimal("1.0"), observed_seq=150, seq_id=2500)
    before = await _digest(redis)
    assert await state.apply(entity_write(older)) is ApplyResult.STALE
    assert await redis.xlen(K.FEED) == 1
    assert await _digest(redis) == before
    data = (await redis.hget(K.order_key("stonex", "B1"), "data")).decode()
    assert '"stop_loss":"1.0950"' in data


async def test_equal_key_different_content_tie_broken_by_seq_id(state: RedisState, redis: Any) -> None:
    await state.apply(entity_write(order(seq_id=5000)))
    lower_seq = order(stop_loss=Decimal("1.0900"), seq_id=4000)
    assert await state.apply(entity_write(lower_seq)) is ApplyResult.STALE
    higher_seq = order(stop_loss=Decimal("1.0900"), seq_id=6000)
    assert await state.apply(entity_write(higher_seq)) is ApplyResult.APPLIED


async def test_fixed_width_compare_beats_numeric_pitfalls(state: RedisState, redis: Any) -> None:
    # 9-digit vs 10-digit broker_seq: lexical compare of unpadded strings would say "999999999" > "1000000000".
    await state.apply(entity_write(order(broker_seq=999_999_999)))
    assert await state.apply(entity_write(order(broker_seq=1_000_000_000, stop_loss=Decimal("1.08")))) \
        is ApplyResult.APPLIED
    # Values beyond 2^53 differ only in the last digit; a float (tonumber) compare would call them equal.
    big = 2**53
    await state.apply(entity_write(order(broker_order_id="B2", broker_seq=big + 1)))
    assert await state.apply(entity_write(order(broker_order_id="B2", broker_seq=big + 2,
                                                stop_loss=Decimal("1.07")))) is ApplyResult.APPLIED
    assert await state.apply(entity_write(order(broker_order_id="B2", broker_seq=big + 1,
                                                stop_loss=Decimal("1.06")))) is ApplyResult.STALE


async def test_no_boot_when_boot_id_missing(state: RedisState, redis: Any) -> None:
    await redis.delete(K.BOOT_ID)
    w = entity_write(order())
    assert await state.apply(w) is ApplyResult.NO_BOOT
    assert not await redis.exists(w.key)
    assert await redis.xlen(K.FEED) == 0
    assert await redis.get(K.STATE_DIGEST) is None


async def test_no_boot_when_boot_id_from_other_term(state: RedisState, redis: Any, boot: Boot) -> None:
    boot.value = "run123:cluster-a:8"
    assert await state.apply(entity_write(order())) is ApplyResult.NO_BOOT


async def test_lock_holder_applies_without_boot_id(state: RedisState, redis: Any) -> None:
    await redis.delete(K.BOOT_ID)
    await redis.set(K.BOOT_LOCK, "owner-1")
    w = entity_write(order())
    assert await state.apply(w, lock_token="wrong") is ApplyResult.NO_BOOT
    assert await state.apply(w, lock_token="owner-1") is ApplyResult.APPLIED
    assert await redis.exists(w.key)


async def test_apply_after_commit_queues_no_boot_and_replays(redis: Any, boot: Boot) -> None:
    signals: list[int] = []
    from src.cache.scripts import NoBootQueue
    st = RedisState(redis, lambda: boot.value, no_boot=NoBootQueue(lambda: signals.append(1)))
    w1 = entity_write(order(observed_seq=1))
    w2 = entity_write(order(observed_seq=2, stop_loss=Decimal("1.09")))
    assert await st.apply_after_commit(w1) is ApplyResult.NO_BOOT
    assert await st.apply_after_commit(w2) is ApplyResult.NO_BOOT
    assert len(st.no_boot) == 1 and signals == [1, 1]   # newest version per key kept
    await redis.set(K.BOOT_LOCK, "sm8")
    assert await st.replay_no_boot("sm8") == 1
    assert len(st.no_boot) == 0
    assert (await redis.hget(w2.key, "v")).decode() == w2.version


async def test_instrument_keys_never_feed(state: RedisState, redis: Any) -> None:
    spec = InstrumentSpec(adapter_id="stonex", symbol="EURUSD", base_ccy="EUR", quote_ccy="USD", pnl_ccy="USD",
                          contract_size=Decimal(1), unit_kind="UNITS", point_size=Decimal("0.0001"),
                          min_qty=Decimal(1000), swap_booked_to_balance=True)
    w = instrument_write(spec, seq_id=10)
    assert await state.apply(w) is ApplyResult.APPLIED
    assert await redis.exists(K.instrument_key("stonex", "EURUSD"))
    assert await redis.xlen(K.FEED) == 0
    changed = instrument_write(replace(spec, min_qty=Decimal(1)), seq_id=11)
    assert await state.apply(changed) is ApplyResult.APPLIED
    assert await redis.xlen(K.FEED) == 0


async def test_position_and_account_keys_feed(state: RedisState, redis: Any) -> None:
    pos = PositionDTO(position_id="P1", account_id="ACC", adapter_id="ctrader", symbol="EURUSD", side="SELL",
                      units=Decimal(1000), entry_price=Decimal("1.1"), status="OPEN", seq_id=7)
    assert await state.apply(entity_write(pos)) is ApplyResult.APPLIED
    entries = await redis.xrange(K.FEED)
    assert entries[0][1][b"entity_type"] == b"POSITION"
    assert entries[0][1][b"entity_id"] == b"P1"


async def test_digest_xor_round_trip(state: RedisState, redis: Any) -> None:
    a = entity_write(order(broker_order_id="A"))
    b = entity_write(order(broker_order_id="B"))
    await state.apply(a)
    await state.apply(b)
    assert await _digest(redis) == xor_hex(a.digest_hash, b.digest_hash)
    a2 = entity_write(order(broker_order_id="A", stop_loss=Decimal("1.0"), observed_seq=500))
    await state.apply(a2)
    assert await _digest(redis) == xor_hex(a2.digest_hash, b.digest_hash)
    # XOR-ing the remaining hashes back out returns to zero
    assert xor_hex(xor_hex(await _digest(redis), a2.digest_hash), b.digest_hash) == ZERO_DIGEST


async def test_lua_xor_matches_python_for_many_values(state: RedisState, redis: Any) -> None:
    expected = ZERO_DIGEST
    for i in range(20):
        w = entity_write(order(broker_order_id=f"X{i}"))
        await state.apply(w)
        expected = xor_hex(expected, entity_hash(w.key, w.version, w.checksum))
    assert await _digest(redis) == expected


async def test_bad_hash_rejected(state: RedisState, redis: Any) -> None:
    import pytest
    from redis.exceptions import ResponseError
    w = entity_write(order())
    bad = EntityWrite(w.key, w.version, w.seq_id, w.checksum, w.payload, w.entity_type, w.entity_id, w.account_id)
    await state.apply(bad)  # valid
    with pytest.raises(ResponseError):
        await state._runner.run("apply_state", [w.key, K.STATE_DIGEST, K.FEED, K.BOOT_ID, K.BOOT_LOCK],
                                [BOOT, "", w.version, pad(2, 20), "x", "ZZ", "{}", "ORDER",
                                 "B1", "ACC", "2", "100"])


async def test_noscript_fallback_reloads(state: RedisState, redis: Any) -> None:
    await state.apply(entity_write(order()))
    await redis.script_flush()
    assert await state.apply(entity_write(order(observed_seq=999, stop_loss=Decimal("1.0")))) \
        is ApplyResult.APPLIED
