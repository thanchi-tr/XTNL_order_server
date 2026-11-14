"""redis_ops: BootIdentity, drift classification, digest-preserving DEL, boot-lock primitives."""
from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from src.cache import keys as K
from src.cache.digest import ZERO_DIGEST, fold
from src.cache.scripts import ApplyResult, RedisState
from src.integrity.ports import committed_row
from src.integrity.redis_ops import (
    BootIdentity,
    Drift,
    RedisMeta,
    classify,
    drop_entity_key,
    finish_boot,
    last_feed_entry_ms,
    read_digest,
    read_meta,
    release_lock,
    renew_lock,
    scan_entity_keys,
    set_digest_if_unchanged,
    try_lock,
)
from tests.integrity.conftest import EPOCH, RunId, boot
from tests.integrity.fakes import order


async def test_identity_fails_closed_until_refreshed(run_id: RunId) -> None:
    ident = BootIdentity(lambda: EPOCH, run_id)
    assert ident.expected() == ""
    assert await ident.refresh() == f"run-1:{EPOCH}"
    run_id.value = "run-2"
    assert ident.expected() == f"run-1:{EPOCH}"          # cached until the next refresh
    assert await ident.refresh() == f"run-2:{EPOCH}"


async def test_identity_rejects_empty_run_id() -> None:
    async def empty() -> str:
        return ""
    with pytest.raises(RuntimeError):
        await BootIdentity(lambda: EPOCH, empty).refresh()


def test_classify_cases() -> None:
    row = committed_row(order(observed=200, seq=2000), 0)
    seq = f"{row.seq_id:020d}"
    assert classify(None, None) is Drift.MATCH
    assert classify(None, RedisMeta(row.version, seq, row.checksum, "")) is Drift.ORPHAN
    assert classify(row, None) is Drift.BEHIND
    assert classify(row, RedisMeta(row.version, seq, row.checksum, "")) is Drift.MATCH
    older = committed_row(order(observed=100, seq=1000), 0)
    assert classify(row, RedisMeta(older.version, f"{1000:020d}", older.checksum, "")) is Drift.BEHIND
    newer = committed_row(order(observed=300, seq=3000), 0)
    assert classify(row, RedisMeta(newer.version, f"{3000:020d}", newer.checksum, "")) is Drift.AHEAD
    # same version, different content: the lower seq is behind, the higher is ahead
    assert classify(row, RedisMeta(row.version, f"{1:020d}", "x" * 64, "")) is Drift.BEHIND
    assert classify(row, RedisMeta(row.version, f"{9999:020d}", "x" * 64, "")) is Drift.AHEAD
    # malformed widths can never be fixed by a CAS
    assert classify(row, RedisMeta("123", seq, "x", "")) is Drift.AHEAD


async def test_drop_entity_key_keeps_digest_consistent(redis: Any, cache: RedisState, identity: BootIdentity) -> None:
    await boot(redis, identity)
    a = committed_row(order("A"), 0)
    b = committed_row(order("B"), 0)
    assert await cache.apply(a.write()) is ApplyResult.APPLIED
    assert await cache.apply(b.write()) is ApplyResult.APPLIED
    assert await read_digest(redis) == fold([a.digest_hash, b.digest_hash])
    assert await drop_entity_key(redis, a.key) is True
    assert await redis.exists(a.key) == 0
    assert await read_digest(redis) == b.digest_hash
    assert await drop_entity_key(redis, a.key) is False
    assert await cache.apply(a.write()) is ApplyResult.APPLIED
    assert await read_digest(redis) == fold([a.digest_hash, b.digest_hash])


async def test_drop_key_without_hash_only_deletes(redis: Any) -> None:
    await redis.hset("order:x:1", mapping={"v": "1"})
    await redis.set(K.STATE_DIGEST, ZERO_DIGEST)
    assert await drop_entity_key(redis, "order:x:1")
    assert await read_digest(redis) == ZERO_DIGEST


async def test_read_meta_pipelined(redis: Any, cache: RedisState, identity: BootIdentity) -> None:
    await boot(redis, identity)
    a = committed_row(order("A"), 0)
    await cache.apply(a.write())
    metas = await read_meta(redis, [a.key, "order:none:1"])
    assert metas["order:none:1"] is None
    m = metas[a.key]
    assert m is not None and m.version == a.version and m.checksum == a.checksum and m.hash == a.digest_hash
    assert await read_meta(redis, []) == {}


async def test_scan_and_feed_helpers(redis: Any, cache: RedisState, identity: BootIdentity) -> None:
    await boot(redis, identity)
    assert await last_feed_entry_ms(redis) is None
    for oid in ("A", "B"):
        await cache.apply(committed_row(order(oid), 0).write())
    keys = [k async for batch in scan_entity_keys(redis) for k in batch]
    assert sorted(keys) == ["order:stonex:A", "order:stonex:B"]
    assert await last_feed_entry_ms(redis) is not None


async def test_set_digest_if_unchanged(redis: Any) -> None:
    assert await set_digest_if_unchanged(redis, None, "1" * 64)
    assert not await set_digest_if_unchanged(redis, None, "2" * 64)
    assert await set_digest_if_unchanged(redis, "1" * 64, "2" * 64)
    assert await read_digest(redis) == "2" * 64


async def test_boot_lock_primitives(redis: Any) -> None:
    assert await try_lock(redis, "me", 10_000)
    assert not await try_lock(redis, "other", 10_000)
    assert await renew_lock(redis, "me", 20_000)
    assert not await renew_lock(redis, "other", 20_000)
    assert not await release_lock(redis, "other")
    assert not await finish_boot(redis, "other", "boot")
    assert await redis.get(K.BOOT_ID) is None
    assert await finish_boot(redis, "me", "boot")
    assert await redis.get(K.BOOT_ID) == b"boot"
    assert await redis.get(K.BOOT_LOCK) is None
    assert await try_lock(redis, "x", 10_000)
    assert await release_lock(redis, "x")


def test_committed_row_write_roundtrip() -> None:
    row = committed_row(order(), 5)
    w = row.write()
    assert (w.key, w.version, w.checksum, w.digest_hash) == (row.key, row.version, row.checksum, row.digest_hash)
    assert replace(row, entity=None).write() == w
