"""Reads of `redis-md` for the bootstrap, the read port, the pull cursor and the re-hash (design §A7, §A10, §A13;
plan S16).

`StoreReader` wraps one redis-py client: the `md_reader` pool for the `MarketDataPort` (500 ms timeouts) or the
writer connection for the bootstrap and the re-hash (which run inside `md-writer`). It uses only commands both ACLs
grant (`GET`, `HGETALL`, `ZRANGE`, `ZRANGEBYSCORE`, `EXISTS`; never `KEYS` / `SCAN`), so the same code serves both.
Every method raises `StoreUnavailable` (class name and allow-listed code only) on a timeout, a connection error or an
error reply, and returns raw members as text: decoding and the hash check are the callers' (`codec`, `validate`).

The parse helpers split the stored member forms written by the scripts (`scripts.py`):

* rollback entry `<write_seq:20>|<bar_end_ms:13>|<bar member>` (score = `write_seq`);
* superseded revision `<bar member>|<superseded_at_ms:13>` (score = bucket start).

Nothing here logs or formats a member (members carry prices).
"""
from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Final

from redis.asyncio import Redis
from redis.exceptions import RedisError

from src.marketdata.store import keys as K
from src.marketdata.store.runner import StoreUnavailable, unavailable
from src.marketdata.types import SeriesId

_RB_RE: Final = re.compile(r"([0-9]{20})\|([0-9]{13})\|(.+)")
_REV_RE: Final = re.compile(r"(.+)\|([0-9]{13})")
BAR_MEMBER_RE: Final = re.compile(r"[0-9]{17}\|[0-9CPDNB]{87}\|[0-9a-f]{8}\|[0-9a-f]{34}")
"""The shape the scripts accept as a bar member (`QUARANTINE` refuses another as `remove_member`)."""


def _text(value: object) -> str:
    return value.decode("ascii", "replace") if isinstance(value, bytes) else str(value)


@dataclass(frozen=True)
class RollbackEntry:
    """One rollback-set entry as stored: `write_seq`, `bar_end_ms` and the bar member (hidden: it carries prices)."""

    write_seq: int
    bar_end_ms: int
    member: str = field(repr=False)

    @property
    def stored(self) -> str:
        return f"{self.write_seq:020d}|{self.bar_end_ms:013d}|{self.member}"


def parse_rollback(raw: str | bytes) -> RollbackEntry | None:
    """A rollback member, or None when it is not of the written form."""
    m = _RB_RE.fullmatch(_text(raw))
    if m is None:
        return None
    return RollbackEntry(int(m.group(1)), int(m.group(2)), m.group(3))


def parse_superseded(raw: str | bytes) -> tuple[str, int] | None:
    """`(bar member, superseded_at_ms)` of an `md:bars_rev` member, or None."""
    m = _REV_RE.fullmatch(_text(raw))
    if m is None:
        return None
    return m.group(1), int(m.group(2))


def removable_member(member: str) -> bool:
    """True when the scripts would accept `member` as a stored bar member (so `QUARANTINE` may remove it)."""
    return BAR_MEMBER_RE.fullmatch(member) is not None


@dataclass(frozen=True)
class SessionHead:
    """`md:boot` and `md:writer` read in one round trip at the start (and end) of a read session."""

    boot: K.BootSentinel | None
    writer: str | None


class StoreReader:
    """Read helpers over one redis-py client (never `KEYS` / `SCAN`)."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def _call(self, coro: Any) -> Any:
        try:
            return await coro
        except (RedisError, TimeoutError, OSError) as e:
            raise unavailable(e) from None

    async def head(self) -> SessionHead:
        pipe = self._redis.pipeline(transaction=False)
        pipe.get(K.BOOT)
        pipe.get(K.WRITER)
        try:
            boot, writer = await pipe.execute()
        except (RedisError, TimeoutError, OSError) as e:
            raise unavailable(e) from None
        return SessionHead(K.BootSentinel.parse(boot), None if writer is None else _text(writer))

    async def boot(self) -> K.BootSentinel | None:
        return K.BootSentinel.parse(await self._call(self._redis.get(K.BOOT)))

    async def writer_token(self) -> str | None:
        raw = await self._call(self._redis.get(K.WRITER))
        return None if raw is None else _text(raw)

    async def hash(self, key: str) -> dict[str, str]:
        raw = await self._call(self._redis.hgetall(key))
        return {_text(k): _text(v) for k, v in dict(raw).items()}

    async def meta(self, series: SeriesId) -> dict[str, str]:
        return await self.hash(K.meta(series))

    async def metas(self, series: Sequence[SeriesId]) -> dict[SeriesId, dict[str, str]]:
        """`md:meta` of every series in one pipelined round trip."""
        if not series:
            return {}
        pipe = self._redis.pipeline(transaction=False)
        for s in series:
            pipe.hgetall(K.meta(s))
        try:
            raw = await pipe.execute()
        except (RedisError, TimeoutError, OSError) as e:
            raise unavailable(e) from None
        return {s: {_text(k): _text(v) for k, v in dict(r).items()} for s, r in zip(series, raw)}

    async def rollback(self, series: SeriesId) -> list[RollbackEntry]:
        """The rollback set oldest `write_seq` first; members not of the written form are left out."""
        raw = await self._call(self._redis.zrange(K.rollback(series), 0, -1))
        out = [parse_rollback(m) for m in raw]
        return [e for e in out if e is not None]

    async def rollback_seqs(self, series: SeriesId) -> list[int]:
        """The score (`write_seq`) of every rollback-set member, one that does not parse included (the bootstrap
        must not send an entry at a score the store already holds: `BOOT_WRITE` would refuse `STALE_SEQ`)."""
        raw = await self._call(self._redis.zrange(K.rollback(series), 0, -1, withscores=True))
        return [int(score) for _, score in raw]

    async def bars_range(self, series: SeriesId, from_start_ms: int, to_start_ms: int) -> list[tuple[int, str]]:
        """`(bucket start, member)` of `md:bars:{series}` with a bucket start in `[from, to]`, oldest first."""
        raw = await self._call(self._redis.zrangebyscore(K.bars(series), from_start_ms, to_start_ms,
                                                         withscores=True))
        return [(int(score), _text(m)) for m, score in raw]

    async def bars_all(self, series: SeriesId) -> list[tuple[int, str]]:
        raw = await self._call(self._redis.zrange(K.bars(series), 0, -1, withscores=True))
        return [(int(score), _text(m)) for m, score in raw]

    async def bars_page(self, series: SeriesId, from_start_ms: int, count: int) -> list[tuple[int, str]]:
        """Up to `count` members with a bucket start at or after `from_start_ms`, oldest first (the re-hash pages)."""
        raw = await self._call(self._redis.zrangebyscore(K.bars(series), from_start_ms, "+inf", start=0, num=count,
                                                         withscores=True))
        return [(int(score), _text(m)) for m, score in raw]

    async def bars_newest(self, series: SeriesId, n: int) -> list[tuple[int, str]]:
        """The newest `n` members, oldest first (`ZRANGE -n -1`: `md_reader` has no `ZREVRANGE`)."""
        if n < 1:
            return []
        raw = await self._call(self._redis.zrange(K.bars(series), -n, -1, withscores=True))
        return [(int(score), _text(m)) for m, score in raw]

    async def superseded_range(self, series: SeriesId, from_start_ms: int,
                               to_start_ms: int) -> list[tuple[int, str, int]]:
        """`(bucket start, member, superseded_at_ms)` of `md:bars_rev:{series}` in `[from, to]`."""
        raw = await self._call(self._redis.zrangebyscore(K.bars_rev(series), from_start_ms, to_start_ms,
                                                         withscores=True))
        out: list[tuple[int, str, int]] = []
        for m, score in raw:
            parsed = parse_superseded(m)
            if parsed is not None:
                out.append((int(score), parsed[0], parsed[1]))
        return out

    async def indicator_range(self, indicator_id: str, output: str, from_start_ms: int,
                              to_start_ms: int) -> list[tuple[int, str]]:
        """`(bucket start, member)` of `md:ind:{id}:{output}` in `[from, to]`, oldest first (S18)."""
        raw = await self._call(self._redis.zrangebyscore(K.indicator(indicator_id, output), from_start_ms,
                                                         to_start_ms, withscores=True))
        return [(int(score), _text(m)) for m, score in raw]

    async def indicator_newest(self, indicator_id: str, output: str, n: int) -> list[tuple[int, str]]:
        """The newest `n` value members of one output, oldest first."""
        if n < 1:
            return []
        raw = await self._call(self._redis.zrange(K.indicator(indicator_id, output), -n, -1, withscores=True))
        return [(int(score), _text(m)) for m, score in raw]

    async def checkpoint_newest(self, indicator_id: str) -> str | None:
        """The newest `md:ind_ckpt:{id}` member, or None."""
        raw = await self._call(self._redis.zrange(K.indicator_checkpoints(indicator_id), -1, -1))
        return _text(raw[0]) if raw else None

    async def registry_ind(self) -> dict[str, str]:
        return await self.hash(K.REGISTRY_IND)

    async def quarantine_count(self, series: SeriesId) -> int:
        return len(await self._call(self._redis.zrange(K.quarantine(series), 0, -1)))

    async def registry_series(self) -> dict[str, str]:
        return await self.hash(K.REGISTRY_SERIES)

    async def stored_index(self) -> dict[str, str]:
        return await self.hash(K.STORED)

    async def live_leases(self) -> list[tuple[str, str, str]]:
        """`(consumer, spec_id, lease value)` of every lease in the lease index whose `md:demand:*` key still exists
        (expired leases left in the index are skipped)."""
        live, _ = await self.lease_index()
        return live

    async def lease_index(self) -> tuple[list[tuple[str, str, str]], list[tuple[str, str]]]:
        """The lease index split into live leases `(consumer, spec_id, value)` and expired entries `(consumer,
        spec_id)` whose `md:demand:*` key is gone (the bootstrap prunes those). One `HGETALL`, then one pipelined
        `EXISTS` per entry."""
        index = await self.hash(K.LEASE_INDEX)
        entries: list[tuple[str, str, str]] = []
        for name, value in sorted(index.items()):
            consumer, _, spec_id = name.partition(":")
            if not consumer or not spec_id or ":" in spec_id:
                continue
            try:
                K.demand(consumer, spec_id)
            except ValueError:
                continue
            entries.append((consumer, spec_id, value))
        if not entries:
            return [], []
        pipe = self._redis.pipeline(transaction=False)
        for consumer, spec_id, _ in entries:
            pipe.exists(K.demand(consumer, spec_id))
        try:
            alive = await pipe.execute()
        except (RedisError, TimeoutError, OSError) as e:
            raise unavailable(e) from None
        live = [e for e, a in zip(entries, alive) if int(a) == 1]
        expired = [(c, sid) for (c, sid, _), a in zip(entries, alive) if int(a) != 1]
        return live, expired


__all__ = ["StoreReader", "StoreUnavailable", "RollbackEntry", "SessionHead", "parse_rollback", "parse_superseded",
           "removable_member", "BAR_MEMBER_RE"]
