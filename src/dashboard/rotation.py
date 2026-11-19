"""Dashboard credential rotation without restart (operations.md §13; K10).

The dashboard's credentials (`XTNL_DASHBOARD_DSN`: the read-only `xtnl_dashboard` role's password;
`XTNL_DASHBOARD_REDIS_URLS`: the dashboard ACL user's password per node) follow its secrets source
(`XTNL_SECRETS_SOURCE` = env | aws-sm, `src.secrets.source`, like the engine and the dead-man receiver). With
aws-sm, `CredentialWatcher` re-reads the cached secret every `REFRESH_S` (no I/O on the event loop: the source's
own thread refreshes it) and, when a value changed:

* the DSN: `RotatingPgSource.replace` builds a new lazily-connected read-only pool for the new DSN (its every
  connection still passes `verify_read_only_connection`) and closes the old one after `CLOSE_GRACE_S`, so requests
  in flight finish on it;
* the Redis URLs: new read-only clients replace the old ones in `DashboardViews` (`replace_redis`), which are closed.

Settings that do not parse keep the current ones (logged by variable name only, never a value). The listen address
and `XTNL_DASHBOARD_FRESH_MS` still take a restart. With the env source nothing changes until a restart (as the
receiver).
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Any, Final, Protocol

import redis.asyncio as aioredis

from src.dashboard.server import DashboardConfigError, DashboardSettings

log = logging.getLogger(__name__)

REFRESH_S: Final = 60.0
CLOSE_GRACE_S: Final = 10.0


class ClosableSource(Protocol):
    def acquire(self) -> AbstractAsyncContextManager[Any]: ...

    async def close(self) -> None: ...


class RotatingPgSource:
    """A ConnectionSource whose underlying pool is swapped when the DSN changes (no restart)."""

    def __init__(self, dsn: str, make: Callable[[str], ClosableSource]) -> None:
        self._make = make
        self._dsn = dsn
        self._cur = make(dsn)
        self._retiring: set[asyncio.Task[None]] = set()
        self.swaps = 0

    def acquire(self) -> AbstractAsyncContextManager[Any]:
        return self._cur.acquire()

    def replace(self, dsn: str) -> bool:
        """Switch to a pool for `dsn` (False when unchanged); the old pool closes after CLOSE_GRACE_S."""
        if dsn == self._dsn:
            return False
        old, self._cur, self._dsn = self._cur, self._make(dsn), dsn
        self.swaps += 1
        task = asyncio.get_running_loop().create_task(self._close_later(old))
        self._retiring.add(task)
        task.add_done_callback(self._retiring.discard)
        return True

    @staticmethod
    async def _close_later(old: ClosableSource) -> None:
        await asyncio.sleep(CLOSE_GRACE_S)
        try:
            await old.close()
        except Exception as exc:
            log.warning("dashboard: closing the replaced pool failed (%s)", type(exc).__name__)

    async def close(self) -> None:
        for t in list(self._retiring):
            t.cancel()
        await self._cur.close()


class RedisHolder(Protocol):
    def replace_redis(self, clients: Mapping[str, aioredis.Redis]) -> dict[str, aioredis.Redis]: ...


class CredentialWatcher:
    """Applies a rotated DSN / Redis URL set from the secrets source (K10)."""

    def __init__(self, load: Callable[[], Mapping[str, str]], current: DashboardSettings, pg: RotatingPgSource,
                 views: RedisHolder, make_clients: Callable[[Mapping[str, str]], dict[str, aioredis.Redis]]) -> None:
        self._load = load
        self.current = current
        self._pg = pg
        self._views = views
        self._make_clients = make_clients
        self.issue: str | None = None

    async def check_once(self) -> None:
        try:
            fresh = DashboardSettings.from_env(self._load())
        except DashboardConfigError as exc:
            self._set_issue(f"dashboard credentials not re-read: {exc}")
            return
        except Exception as exc:                      # the source itself (e.g. the secret not read yet)
            self._set_issue(f"dashboard secrets source unreadable ({type(exc).__name__})")
            return
        if self.issue is not None:
            log.warning("dashboard secrets source valid again")
        self.issue = None
        if self._pg.replace(fresh.dsn):
            log.warning("dashboard PostgreSQL credentials rotated: new read-only pool (no restart)")
        if dict(fresh.redis_urls) != dict(self.current.redis_urls):
            old = self._views.replace_redis(self._make_clients(fresh.redis_urls))
            for c in old.values():
                try:
                    await c.aclose()
                except Exception as exc:
                    log.warning("dashboard: closing a replaced Redis client failed (%s)", type(exc).__name__)
            log.warning("dashboard Redis credentials rotated: %d client(s) rebuilt (no restart)",
                        len(fresh.redis_urls))
        self.current = fresh

    def _set_issue(self, msg: str) -> None:
        if self.issue != msg:
            log.error("%s; keeping the current credentials", msg)
        self.issue = msg

    async def run(self, stop: asyncio.Event, every_s: float = REFRESH_S) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), every_s)
            except TimeoutError:
                pass
            if stop.is_set():
                return
            try:
                await self.check_once()
            except Exception as exc:
                log.error("dashboard credential check failed (%s)", type(exc).__name__)


__all__ = ["CLOSE_GRACE_S", "REFRESH_S", "CredentialWatcher", "RotatingPgSource"]
