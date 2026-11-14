"""FeedStream (src/oversee/feed.py) over the Redis Stream `oversee:feed` (oversee-rules.md §1, §3).

The single Redis reader of the feed. The entry record (`FeedEntry`) and the consumer-group name live here
so src.oversee.feed can import them without a cycle (src.oversee.feed re-exports all three).

* `read_group`: XREADGROUP > (new entries for this consumer; first delivery).
* `autoclaim`: XAUTOCLAIM entries idle >= min_idle_ms, then ONE XPENDING range over the claimed ids for
  their delivery counts (the claim itself counts as a delivery), so a batch costs two round trips.
* `ack`: XACK. `boot_id`: GET redis:boot_id (the observer consumes nothing until it equals run_id:epoch).
* `ensure_group`: XGROUP CREATE ... 0 MKSTREAM; BUSYGROUP counts as success (SM8 step 2).
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final, cast

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from src.cache import keys as K

FEED_GROUP: Final = "oversee"


@dataclass(frozen=True)
class FeedEntry:
    """One oversee:feed entry as delivered to this consumer (`deliveries` = times delivered so far)."""

    entry_id: str
    fields: dict[str, str]
    deliveries: int = 1

    @property
    def account_id(self) -> str | None:
        return self.fields.get("account_id") or None


def _s(v: object) -> str:
    return v.decode("utf-8") if isinstance(v, bytes) else str(v)


def _id_key(entry_id: str) -> tuple[int, int]:
    ms, _, seq = entry_id.partition("-")
    return int(ms), int(seq or 0)


def _fields(raw: object) -> dict[str, str]:
    return {_s(k): _s(v) for k, v in cast(dict[object, object], raw or {}).items()}


class RedisFeedStream:
    def __init__(self, redis: aioredis.Redis, consumer: str, *, stream: str = K.FEED,
                 group: str = FEED_GROUP) -> None:
        self._r = redis
        self._consumer = consumer
        self._stream = stream
        self._group = group

    async def boot_id(self) -> str | None:
        v = await self._r.get(K.BOOT_ID)
        return None if v is None else _s(v)

    async def ensure_group(self) -> None:
        try:
            await self._r.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read_group(self, count: int, block_ms: int) -> list[FeedEntry]:
        resp: Any = await self._r.xreadgroup(self._group, self._consumer, {self._stream: ">"}, count=count,
                                             block=block_ms)
        out: list[FeedEntry] = []
        for _name, items in cast(list[tuple[object, list[tuple[object, object]]]], resp or []):
            out += [FeedEntry(_s(i), _fields(f), 1) for i, f in items if f is not None]
        return out

    async def autoclaim(self, min_idle_ms: int, count: int) -> list[FeedEntry]:
        resp: list[Any] = await self._r.xautoclaim(self._stream, self._group, self._consumer,
                                                   min_idle_time=min_idle_ms, start_id="0-0", count=count)
        items = [(_s(i), f) for i, f in cast(list[tuple[object, object]], resp[1] if len(resp) > 1 else [])
                 if f is not None]   # a trimmed entry comes back without fields: nothing to wake
        if not items:
            return []
        ids = sorted((i for i, _ in items), key=_id_key)
        pending = cast(list[dict[str, Any]], await self._r.xpending_range(
            self._stream, self._group, min=ids[0], max=ids[-1], count=max(count, len(ids)),
            consumername=self._consumer))
        times = {_s(p["message_id"]): int(p["times_delivered"]) for p in pending}
        return [FeedEntry(i, _fields(f), times.get(i, 1)) for i, f in items]

    async def ack(self, entry_ids: Sequence[str]) -> None:
        if entry_ids:
            await self._r.xack(self._stream, self._group, *entry_ids)
