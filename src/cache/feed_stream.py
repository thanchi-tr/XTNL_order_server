"""FeedStream (src/oversee/feed.py) over the Redis Stream `oversee:feed` (oversee-rules.md §1, §3).

The single Redis reader of the feed. The entry record (`FeedEntry`) and the consumer-group name live here
so src.oversee.feed can import them without a cycle (src.oversee.feed re-exports all three).

* `read_group`: XREADGROUP > (new entries for this consumer; first delivery). A client-side socket timeout
  on the BLOCK read (redis.exceptions.TimeoutError, e.g. after an event-loop stall) reads as empty: redis-py
  has already dropped that connection, and anything Redis put in the PEL is recovered by `read_own_pending`
  (the observer's drain after an empty read) or, failing that, XAUTOCLAIM. Only
  timeouts that persist without a single good `>` read for `max_timeout_streak_s` re-raise, so a Redis that
  keeps timing out still ends the feed task and reaches the supervisor. Connection errors are never swallowed.
* `autoclaim`: XAUTOCLAIM entries idle >= min_idle_ms, then ONE XPENDING range over the claimed ids for
  their delivery counts (the claim itself counts as a delivery), so a batch costs two round trips.
* `read_own_pending`: XREADGROUP 0 (this consumer's own PEL, e.g. entries of a timed-out read), with counts.
  A client-side socket timeout anywhere in the drain (the XREADGROUP 0, the trimmed-entry XACK or the XPENDING
  range) reads as empty, like `read_group`, and counts toward the same continuous-timeout bound (2026-11-25
  live review 3). Nothing is lost: an id-0 read moves no entry, so they all stay in this consumer's PEL. The
  drain stays owed, and the next `read_group` drains the own PEL before its `>` read (oversee-rules.md §3:
  drained after a read error, before the next `>` read), so a busy feed that never reads empty does not leave
  them waiting for XAUTOCLAIM.
* `ack`: XACK. `boot_id`: GET redis:boot_id (the observer consumes nothing until it equals run_id:epoch).
* `ensure_group`: XGROUP CREATE ... 0 MKSTREAM; BUSYGROUP counts as success (SM8 step 2).
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Final, cast

import redis.asyncio as aioredis
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from src.cache import keys as K

FEED_GROUP: Final = "oversee"
# Below the feed-observer task's 30 s max stamp age: a streak of read timeouts this long re-raises.
MAX_TIMEOUT_STREAK_S: Final = 20.0
_WARN_EVERY_S: Final = 60.0

log = logging.getLogger(__name__)


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
                 group: str = FEED_GROUP, max_timeout_streak_s: float = MAX_TIMEOUT_STREAK_S,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        self._r = redis
        self._consumer = consumer
        self._stream = stream
        self._group = group
        self._max_streak_s = max_timeout_streak_s
        self._mono = monotonic
        self._timeout_since: float | None = None   # start of the current run of read timeouts
        self._last_warn: float | None = None
        self._own_pending_owed = False             # a drain read timed out: drain before the next '>' read
        self.read_timeouts = 0                     # total timed-out reads, '>' and id 0 (diagnostics)

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
        if self._own_pending_owed:
            # The last own-pending drain timed out: finish it before reading '>' (oversee-rules.md §3).
            owed = await self.read_own_pending(count)
            if owed or self._own_pending_owed:
                return owed
        try:
            resp: Any = await self._r.xreadgroup(self._group, self._consumer, {self._stream: ">"}, count=count,
                                                 block=block_ms)
        except RedisTimeoutError:
            return self._on_read_timeout("XREADGROUP >")
        self._timeout_since = None
        out: list[FeedEntry] = []
        for _name, items in cast(list[tuple[object, list[tuple[object, object]]]], resp or []):
            out += [FeedEntry(_s(i), _fields(f), 1) for i, f in items if f is not None]
        return out

    def _on_read_timeout(self, what: str) -> list[FeedEntry]:
        """A timed-out read means the same as an empty one, unless the timeouts have lasted too long."""
        now = self._mono()
        self.read_timeouts += 1
        if self._timeout_since is None:
            self._timeout_since = now
        elif now - self._timeout_since >= self._max_streak_s:
            raise RedisTimeoutError(f"oversee:feed reads timed out continuously for "
                                    f"{now - self._timeout_since:.0f} s (last: {what})")
        if self._last_warn is None or now - self._last_warn >= _WARN_EVERY_S:
            self._last_warn = now
            log.warning("oversee:feed %s read timed out (TimeoutError); treated as empty "
                        "(%d timed-out reads so far)", what, self.read_timeouts)
        return []

    async def autoclaim(self, min_idle_ms: int, count: int) -> list[FeedEntry]:
        resp: list[Any] = await self._r.xautoclaim(self._stream, self._group, self._consumer,
                                                   min_idle_time=min_idle_ms, start_id="0-0", count=count)
        items = [(_s(i), f) for i, f in cast(list[tuple[object, object]], resp[1] if len(resp) > 1 else [])
                 if f is not None]   # a trimmed entry comes back without fields: nothing to wake
        return await self._with_deliveries(items, count)

    async def read_own_pending(self, count: int) -> list[FeedEntry]:
        """This consumer's own pending entries (XREADGROUP id 0, no BLOCK): entries Redis delivered to a read
        whose reply was lost (a timed-out BLOCK read, a crash). Redis counts each such read as a delivery; the
        counts come from one XPENDING range. A trimmed entry (no fields: nothing to wake) is XACKed here.

        A redis TimeoutError in any of its round trips reads as empty and leaves the drain owed to the next
        `read_group` (module docstring); it counts toward the same continuous-timeout bound as `read_group`.
        A good drain does not end that streak: only a good `>` read does."""
        try:
            got, rows_read = await self._read_own_pending(count)
        except RedisTimeoutError:
            self._own_pending_owed = True
            return self._on_read_timeout("XREADGROUP 0 (own pending)")
        self._own_pending_owed = rows_read >= count    # a full batch may leave more in the own PEL
        return got

    async def _read_own_pending(self, count: int) -> tuple[list[FeedEntry], int]:
        resp: Any = await self._r.xreadgroup(self._group, self._consumer, {self._stream: "0"}, count=count)
        items: list[tuple[str, object]] = []
        trimmed: list[str] = []
        rows_read = 0
        for _name, rows in cast(list[tuple[object, list[tuple[object, object]]]], resp or []):
            for i, f in rows:
                rows_read += 1
                if f is None or f == {} or f == []:
                    trimmed.append(_s(i))
                else:
                    items.append((_s(i), f))
        if trimmed:
            await self._r.xack(self._stream, self._group, *trimmed)
        return await self._with_deliveries(items, count), rows_read

    async def _with_deliveries(self, items: list[tuple[str, object]], count: int) -> list[FeedEntry]:
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
