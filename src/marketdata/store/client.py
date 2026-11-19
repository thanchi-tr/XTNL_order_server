"""The `redis-md` clients (design §A2, §A8; plan S13, ST-10).

* **Read client:** a small pool (4 connections) with 500 ms socket and connect timeouts. A timeout is
  `DATA_UNAVAILABLE` for that use (S16), never a task death.
* **Write client:** one dedicated writer connection (a blocking pool of exactly one connection, 2 s timeouts), so
  every script call and the `WAITAOF` after it run on the same connection: `WAITAOF` covers only that connection's
  earlier writes (design §A8 release rule). redis-py's connect callback marks the script runner stale, so every
  (re)connect re-runs `SCRIPT LOAD` and the `INFO server run_id` read before the next call. redis-py reconnects
  silently on checkout when the pooled connection hit EOF, so `write_and_wait` compares the runner's connect counter
  around `WAITAOF`: a reconnect in between is reported as not confirmed (`aof_error` = `reconnected`), since a fresh
  connection has no pending writes and its `WAITAOF` would confirm nothing at once.
* redis-py's own retries are off (`Retry(NoBackoff(), 0)`) on both clients: a reconnect never happens silently inside a
  call, it is seen as an error and the next call re-runs the connect hook.
* **Memory:** `MdWriteClient.memory_ratio()` reads `INFO memory` (granted to `md_writer` by name) for the S15 shedding
  thresholds; `maxmemory` 0 reads as None (no threshold applies).
* **AofWaiter:** `RedisAofWaiter` sends the real `WAITAOF 1 0 1500` and returns the reply's first element (local
  fsyncs confirmed; 0 = not confirmed in time). fakeredis has no `WAITAOF` (ST-10), so tests inject a fake. With
  Q17 = off the write client has no waiter and `WAITAOF` is never sent (Redis refuses it without `appendonly`).
* **Closing:** `aclose()` closes both pools in parallel inside the caller's budget (the 0.5 s `drain_aclose_s`,
  design §A2 DRAINING); whatever has not closed by then is dropped. Nothing is flushed. redis-py never sends `QUIT`.

The URL carries a password: it is never logged, never put in an exception and never kept beyond the pool.
"""
from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final, Protocol

from redis.asyncio import BlockingConnectionPool, ConnectionPool, Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError

from src.marketdata.errors import safe_redis_error
from src.marketdata.store import keys as K
from src.marketdata.store.runner import RunIdReader, ScriptReply, ScriptRunner, StoreUnavailable, unavailable
from src.marketdata.store.scripts import ScriptCall

READ_TIMEOUT_S: Final = 0.5
READ_CONNECT_TIMEOUT_S: Final = 0.5
READ_POOL_SIZE: Final = 4
WRITE_TIMEOUT_S: Final = 2.0
WRITE_CONNECT_TIMEOUT_S: Final = 2.0
WAITAOF_TIMEOUT_MS: Final = 1_500
CLOSE_BUDGET_S: Final = 0.5
RECONNECTED: Final = "reconnected"


class AofWaiter(Protocol):
    async def wait(self) -> int:
        """Local AOF fsyncs confirmed for this connection's earlier writes (0 = not in time). May raise
        `StoreUnavailable`."""
        ...


class RedisAofWaiter:
    """`WAITAOF 1 0 1500` on the writer connection."""

    def __init__(self, redis: Redis, *, timeout_ms: int = WAITAOF_TIMEOUT_MS) -> None:
        self._redis = redis
        self._timeout_ms = timeout_ms

    async def wait(self) -> int:
        try:
            reply = await self._redis.execute_command(  # type: ignore[no-untyped-call]
                "WAITAOF", 1, 0, self._timeout_ms)
        except (RedisError, TimeoutError, OSError) as e:
            raise unavailable(e) from None
        if not isinstance(reply, (list, tuple)) or not reply:
            raise StoreUnavailable("error", "WAITAOF gave an unexpected reply")
        return int(reply[0])


def _no_retry() -> Retry:
    return Retry(NoBackoff(), 0)


def build_reader(url: str) -> Redis:
    """The read client: up to 4 connections, 500 ms timeouts, no silent retries, bytes replies."""
    pool = ConnectionPool.from_url(url, max_connections=READ_POOL_SIZE, socket_timeout=READ_TIMEOUT_S,
                                   socket_connect_timeout=READ_CONNECT_TIMEOUT_S, retry=_no_retry(),
                                   decode_responses=False)
    return Redis(connection_pool=pool)


def build_writer(url: str, runner_ref: list[ScriptRunner]) -> Redis:
    """The writer: exactly one connection (blocking pool), 2 s timeouts, no silent retries; each (re)connect marks
    every runner in `runner_ref` stale so it re-runs its connect hook."""

    async def connected(conn: Any) -> None:
        await conn.on_connect()
        for runner in runner_ref:
            runner.mark_reconnected()

    pool = BlockingConnectionPool.from_url(url, max_connections=1, timeout=WRITE_TIMEOUT_S,
                                           socket_timeout=WRITE_TIMEOUT_S,
                                           socket_connect_timeout=WRITE_CONNECT_TIMEOUT_S, retry=_no_retry(),
                                           decode_responses=False, redis_connect_func=connected)
    return Redis(connection_pool=pool)


async def _close(redis: Redis) -> None:
    try:
        await redis.aclose(close_connection_pool=True)
    except (RedisError, TimeoutError, OSError):
        pass


async def close_within(clients: Sequence[Redis], budget_s: float = CLOSE_BUDGET_S) -> bool:
    """Close `clients` in parallel; True when all closed inside `budget_s`, False when the rest was dropped."""
    try:
        async with asyncio.timeout(budget_s):
            await asyncio.gather(*(_close(c) for c in clients))
    except TimeoutError:
        return False
    return True


@dataclass(frozen=True)
class WriteResult:
    """Script replies plus the AOF confirmation: `aof_local` None = AOF off (no `WAITAOF` sent); 0 = not confirmed
    (timeout, `aof_error`, or `reconnected`: the connection changed between the writes and `WAITAOF`); ≥ 1 = the
    writes are on disk and closed bars may leave the queue."""

    replies: tuple[ScriptReply, ...]
    aof_local: int | None
    aof_error: str | None = None

    @property
    def aof_confirmed(self) -> bool:
        return self.aof_local is None or self.aof_local >= 1


class MdWriteClient:
    """The writer connection: the token SET, the scripts, `WAITAOF`."""

    def __init__(self, redis: Redis, runner: ScriptRunner, aof: AofWaiter | None) -> None:
        self._redis = redis
        self._runner = runner
        self._aof = aof

    @classmethod
    def connect(cls, url: str, *, aof_enabled: bool = True, run_id_reader: RunIdReader | None = None) -> MdWriteClient:
        """Build (no I/O yet) the writer for `url`: one connection, the connect hook wired to the runner."""
        ref: list[ScriptRunner] = []
        redis = build_writer(url, ref)
        runner = ScriptRunner(redis, run_id_reader=run_id_reader)
        ref.append(runner)
        return cls(redis, runner, RedisAofWaiter(redis) if aof_enabled else None)

    @property
    def redis(self) -> Redis:
        return self._redis

    @property
    def runner(self) -> ScriptRunner:
        return self._runner

    async def install_token(self, token: K.WriterToken) -> None:
        """SET `md:writer` unconditionally (the caller has checked its lease and `engine_epoch`, design §A7)."""
        try:
            await self._redis.set(K.WRITER, token.text)
        except (RedisError, TimeoutError, OSError) as e:
            self._runner.mark_reconnected()
            raise unavailable(e) from None

    async def read_boot(self) -> K.BootSentinel | None:
        """The current `md:boot` (None when missing or malformed)."""
        try:
            raw = await self._redis.get(K.BOOT)
        except (RedisError, TimeoutError, OSError) as e:
            self._runner.mark_reconnected()
            raise unavailable(e) from None
        return K.BootSentinel.parse(raw)

    async def current_run_id(self) -> str:
        """The connection's `run_id`, running the connect hook first when the runner is stale (S16: the bootstrap
        builds its sentinel with it; `FINISH` re-checks it at send time). Raises `StoreUnavailable`."""
        if not self._runner.ready or self._runner.run_id is None:
            await self._runner.on_connect()
        run_id = self._runner.run_id
        if run_id is None:
            raise StoreUnavailable("error", "no run_id after the connect hook")
        return run_id

    async def run(self, calls: Sequence[ScriptCall]) -> list[ScriptReply]:
        return await self._runner.run(calls)

    async def run_one(self, call: ScriptCall) -> ScriptReply:
        return await self._runner.run_one(call)

    async def run_sequential(self, calls: Sequence[ScriptCall]) -> list[ScriptReply]:
        """Dependent calls (bootstrap steps): one at a time, stopping at the first non-`ScriptOk` reply."""
        return await self._runner.run_sequential(calls)

    async def write_and_wait(self, calls: Sequence[ScriptCall]) -> WriteResult:
        """Pipeline `calls`, then (AOF on) `WAITAOF` on the same connection. A `WAITAOF` timeout or error leaves
        `aof_local` 0, so the caller keeps its bars queued and re-writes them (the bucket replace is idempotent)."""
        replies = tuple(await self._runner.run(calls))
        if self._aof is None:
            return WriteResult(replies, None)
        seen = self._runner.connects
        try:
            local = await self._aof.wait()
        except StoreUnavailable as e:
            self._runner.mark_reconnected()
            return WriteResult(replies, 0, e.detail)
        if self._runner.connects != seen:   # WAITAOF ran on a new connection: it says nothing about these writes
            return WriteResult(replies, 0, RECONNECTED)
        return WriteResult(replies, local)

    async def memory_ratio(self) -> float | None:
        """`used_memory / maxmemory` from `INFO memory` (the `md-writer` shedding thresholds, design §A8); None when
        `maxmemory` is 0 (unbounded) or the reply has no usable numbers. Raises `StoreUnavailable`."""
        try:
            info = await self._redis.info("memory")
        except (RedisError, TimeoutError, OSError) as e:
            self._runner.mark_reconnected()
            raise unavailable(e) from None
        if not isinstance(info, dict):
            return None
        try:
            used, limit = int(info.get("used_memory", -1)), int(info.get("maxmemory", 0))
        except (TypeError, ValueError):
            return None
        if used < 0 or limit <= 0:
            return None
        return used / limit

    async def aclose(self, budget_s: float = CLOSE_BUDGET_S) -> bool:
        return await close_within([self._redis], budget_s)


class MdReadClient:
    """The read connection pool (S16 builds the `MarketDataPort` reads on it)."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    @classmethod
    def connect(cls, url: str) -> MdReadClient:
        return cls(build_reader(url))

    @property
    def redis(self) -> Redis:
        return self._redis

    async def read_boot(self) -> K.BootSentinel | None:
        try:
            raw = await self._redis.get(K.BOOT)
        except (RedisError, TimeoutError, OSError) as e:
            raise unavailable(e) from None
        return K.BootSentinel.parse(raw)

    async def aclose(self, budget_s: float = CLOSE_BUDGET_S) -> bool:
        return await close_within([self._redis], budget_s)


@dataclass(frozen=True)
class MdClients:
    """Both clients of one term; `aclose` closes them in parallel inside one budget."""

    reader: MdReadClient
    writer: MdWriteClient

    @classmethod
    def connect(cls, url: str, *, aof_enabled: bool = True) -> MdClients:
        return cls(MdReadClient.connect(url), MdWriteClient.connect(url, aof_enabled=aof_enabled))

    async def aclose(self, budget_s: float = CLOSE_BUDGET_S) -> bool:
        return await close_within([self.reader.redis, self.writer.redis], budget_s)


def describe(exc: BaseException) -> str:
    """Loggable text for any store-side exception (class name and allow-listed code only)."""
    if isinstance(exc, StoreUnavailable):
        return f"{exc.kind}: {exc.detail}"
    return safe_redis_error(exc)
