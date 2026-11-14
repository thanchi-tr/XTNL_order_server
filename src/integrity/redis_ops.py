"""Redis helpers shared by SM3 and SM8 (tier-persistence.md §1-§3).

* BootIdentity: the expected redis:boot_id = run_id:epoch (INFO run_id refreshed by SM3 / SM8).
* read_meta / classify: pipelined (v, seq, cs, h) reads of entity keys and the drift verdict.
* drop_entity_key: the "Redis ahead" DEL, which XORs the key's hash out of state:digest in the same
  MULTI (WATCH-guarded), so the commutative digest stays equal to PostgreSQL's after the re-apply.
* Boot-lock primitives for SM8 (SET NX PX, compare-and-renew, boot_id-then-release).
"""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final

import redis.asyncio as aioredis
from redis.exceptions import WatchError

from src.cache import keys as K
from src.cache.digest import DIGEST_HEX_LEN, ZERO_DIGEST, xor_hex
from src.cache.scripts import SEQ_WIDTH, boot_id_value, pad, redis_run_id
from src.integrity.ports import CommittedRow

log = logging.getLogger(__name__)

ENTITY_PATTERNS: Final = ("order:*", "position:*", "account:*", "instrument:*")
_DROP_RETRIES: Final = 16

# Compare-and-renew / compare-and-finish for redis:boot_lock (lock bookkeeping, not entity data).
RENEW_LOCK_LUA: Final = (
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) end return 0"
)
RELEASE_LOCK_LUA: Final = (
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0"
)
# SM8 step 6: write redis:boot_id LAST and release the lock, only while still the lock holder.
FINISH_BOOT_LUA: Final = (
    "if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('SET', KEYS[2], ARGV[2]); "
    "redis.call('DEL', KEYS[1]); return 1 end return 0"
)


def _s(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


class BootIdentity:
    """Expected redis:boot_id. `expected()` is synchronous (RedisState / FeedObserver call it per write);
    until the first refresh it returns '' so every apply_state call answers NO_BOOT (fail closed)."""

    def __init__(self, epoch_label: Callable[[], str], run_id_reader: Callable[[], Awaitable[str]]) -> None:
        self._epoch_label = epoch_label
        self._read_run_id = run_id_reader
        self.run_id: str | None = None

    @classmethod
    def for_redis(cls, redis: aioredis.Redis, epoch_label: Callable[[], str]) -> BootIdentity:
        async def reader() -> str:
            return await redis_run_id(redis)
        return cls(epoch_label, reader)

    async def refresh(self) -> str:
        rid = await self._read_run_id()
        if not rid:
            raise RuntimeError("Redis INFO returned no run_id")
        self.run_id = rid
        return self.expected()

    def expected(self) -> str:
        if self.run_id is None:
            return ""
        return boot_id_value(self.run_id, self._epoch_label())


# ---- entity metadata & drift -----------------------------------------------------------------------

@dataclass(frozen=True)
class RedisMeta:
    version: str
    seq: str
    checksum: str
    hash: str


class Drift(str, Enum):
    MATCH = "MATCH"
    BEHIND = "BEHIND"      # missing or older in Redis: a version CAS heals it
    AHEAD = "AHEAD"        # newer than PostgreSQL (or same key, different content): DEL + re-apply
    ORPHAN = "ORPHAN"      # in Redis, not in PostgreSQL: DEL


async def read_meta(redis: aioredis.Redis, keys: Sequence[str]) -> dict[str, RedisMeta | None]:
    """One pipelined round trip: HMGET v seq cs h per key."""
    if not keys:
        return {}
    pipe = redis.pipeline(transaction=False)
    for k in keys:
        pipe.hmget(k, "v", "seq", "cs", "h")
    raw = await pipe.execute()
    out: dict[str, RedisMeta | None] = {}
    for k, vals in zip(keys, raw):
        v, seq, cs, h = vals
        out[k] = None if v is None else RedisMeta(_s(v), "" if seq is None else _s(seq),
                                                  "" if cs is None else _s(cs), "" if h is None else _s(h))
    return out


def classify(pg: CommittedRow | None, meta: RedisMeta | None) -> Drift:
    """Redis vs PostgreSQL's current row, compared like apply_state.lua (fixed-width strings)."""
    if pg is None:
        return Drift.MATCH if meta is None else Drift.ORPHAN
    if meta is None:
        return Drift.BEHIND
    if meta.version == pg.version and meta.checksum == pg.checksum:
        return Drift.MATCH
    pg_seq = pad(pg.seq_id, SEQ_WIDTH)
    if len(meta.version) != len(pg.version) or (meta.seq and len(meta.seq) != len(pg_seq)):
        return Drift.AHEAD          # a CAS cannot fix a malformed key: DEL + re-apply
    if (meta.version, meta.seq) < (pg.version, pg_seq):
        return Drift.BEHIND
    return Drift.AHEAD


async def drop_entity_key(redis: aioredis.Redis, key: str) -> bool:
    """DEL an entity key and XOR its stored hash out of state:digest atomically (WATCH/MULTI).
    Returns False if the key did not exist."""
    for _ in range(_DROP_RETRIES):
        async with redis.pipeline(transaction=True) as raw_pipe:
            pipe: Any = raw_pipe   # untyped MULTI/UNWATCH in redis-py stubs
            try:
                await pipe.watch(key, K.STATE_DIGEST)
                h_raw = await pipe.hget(key, "h")
                exists = bool(await pipe.exists(key))
                d_raw = await pipe.get(K.STATE_DIGEST)
                if not exists:
                    await pipe.unwatch()
                    return False
                pipe.multi()
                h = "" if h_raw is None else _s(h_raw)
                if len(h) == DIGEST_HEX_LEN:
                    digest = ZERO_DIGEST if d_raw is None else _s(d_raw)
                    pipe.set(K.STATE_DIGEST, xor_hex(digest, h))
                pipe.delete(key)
                await pipe.execute()
                return True
            except WatchError:
                continue
    raise RuntimeError(f"drop_entity_key: {key} kept changing")


async def set_digest_if_unchanged(redis: aioredis.Redis, expected_old: str | None, new: str) -> bool:
    """Repair state:digest when every key matches PostgreSQL but the digest value itself drifted."""
    async with redis.pipeline(transaction=True) as raw_pipe:
        pipe: Any = raw_pipe   # untyped MULTI/UNWATCH in redis-py stubs
        try:
            await pipe.watch(K.STATE_DIGEST)
            cur = await pipe.get(K.STATE_DIGEST)
            if (None if cur is None else _s(cur)) != expected_old:
                await pipe.unwatch()
                return False
            pipe.multi()
            pipe.set(K.STATE_DIGEST, new)
            await pipe.execute()
            return True
        except WatchError:
            return False


async def scan_entity_keys(redis: aioredis.Redis, batch: int = 500) -> AsyncIterator[list[str]]:
    """Batches of entity keys (order:/position:/account:/instrument:) via SCAN (non-blocking)."""
    for pattern in ENTITY_PATTERNS:
        cursor: int = 0
        while True:
            cursor, raw = await redis.scan(cursor=cursor, match=pattern, count=batch)
            keys = [_s(k) for k in raw]
            if keys:
                yield keys
            if int(cursor) == 0:
                break


async def read_digest(redis: aioredis.Redis) -> str | None:
    raw = await redis.get(K.STATE_DIGEST)
    return None if raw is None else _s(raw)


async def last_feed_entry_ms(redis: aioredis.Redis) -> int | None:
    """Time of the newest oversee:feed entry (the last fed entity write), from its stream id."""
    entries = await redis.xrevrange(K.FEED, count=1)
    if not entries:
        return None
    entry_id = _s(entries[0][0])
    return int(entry_id.partition("-")[0])


# ---- boot lock ----------------------------------------------------------------------------------------

async def try_lock(redis: aioredis.Redis, token: str, ttl_ms: int) -> bool:
    return bool(await redis.set(K.BOOT_LOCK, token, nx=True, px=ttl_ms))


async def renew_lock(redis: aioredis.Redis, token: str, ttl_ms: int) -> bool:
    return bool(int(await redis.eval(RENEW_LOCK_LUA, 1, K.BOOT_LOCK, token, str(ttl_ms))))


async def release_lock(redis: aioredis.Redis, token: str) -> bool:
    return bool(int(await redis.eval(RELEASE_LOCK_LUA, 1, K.BOOT_LOCK, token)))


async def finish_boot(redis: aioredis.Redis, token: str, boot_id: str) -> bool:
    """SET redis:boot_id and DEL the lock, atomically, only if `token` still holds the lock."""
    return bool(int(await redis.eval(FINISH_BOOT_LUA, 2, K.BOOT_LOCK, K.BOOT_ID, token, boot_id)))
