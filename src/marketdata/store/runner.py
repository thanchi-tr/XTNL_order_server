"""`EVALSHA` runner for the `redis-md` write scripts (design §A2, §A7, §A8; plan S13, ST-6, R11-m).

* **Connect hook.** On the first use and after every (re)connect, `on_connect()` runs `SCRIPT LOAD` for every script
  (each SHA checked against the one computed here) and reads `INFO server run_id`, in that one hook. The real writer
  client calls `mark_reconnected()` from redis-py's connect callback, and any connection error or timeout marks the
  runner stale too, so the next call re-runs the hook before it sends anything. The connection's `run_id` is passed
  in the ARGV of every ordinary write (the `RUN_ID` placeholder), and the scripts compare it with the `run_id` in
  `md:boot`: a Redis restart that reloaded the old sentinel from the AOF still gives `NO_BOOT`.
* **EVALSHA only**, through a non-transactional pipeline (`transaction=False`: no `MULTI` / `EXEC`, which the
  `md_writer` ACL does not grant and which atomicity does not need, each script being atomic). Never `EVAL`.
* **NOSCRIPT** (a restarted Redis): the hook runs again (scripts and `run_id`) and only the calls that got NOSCRIPT are
  sent again; a NOSCRIPT call never ran, so nothing runs twice.
* Replies are values: `ScriptOk`, `ScriptRefused(code)` for the scripts' own codes (`NO_BOOT`, `NO_WRITER`, ...),
  `ScriptFailed(safe text)` for any other error reply (`NOPERM`, `OOM`, a Lua runtime error, ...). A connection error
  or timeout raises `StoreUnavailable`, whose text is `safe_redis_error` output only (never a message, key, URL or
  value). A `ScriptRefused` wrote nothing; a `ScriptFailed` from a Lua runtime error means **unknown, possibly
  partial** state (Lua does not roll back), never "nothing written".
* The pipeline does not stop at a refusal: every call in it runs, whatever an earlier one returned. Calls that depend
  on an earlier one succeeding (bootstrap `BOOT_WRITE` then `FINISH`) go through `run_sequential` or separate
  `run_one` calls, never one `run`.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Final

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import NoScriptError, RedisError, ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from src.marketdata.errors import redis_error_code, safe_redis_error
from src.marketdata.store.keys import valid_run_id
from src.marketdata.store.scripts import RUN_ID, SCRIPT_CODES, SCRIPTS, Script, ScriptCall

log = logging.getLogger(__name__)

RunIdReader = Callable[[], Awaitable[str]]

TIMEOUT: Final = "timeout"
CONNECTION: Final = "connection"
ERROR: Final = "error"


class StoreUnavailable(Exception):
    """`redis-md` timed out, could not be reached or answered something unusable. `kind` is `timeout`,
    `connection` or `error`; the text is `safe_redis_error` output only."""

    def __init__(self, kind: str, detail: str) -> None:
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


@dataclass(frozen=True)
class ScriptOk:
    value: Any = field(repr=False)


@dataclass(frozen=True)
class ScriptRefused:
    """A script refusal: `NO_BOOT`, `NO_WRITER`, `NO_LOCK`, `BAD_ARGS` or `STALE_SEQ` (nothing was written)."""

    code: str


@dataclass(frozen=True)
class ScriptFailed:
    """Any other error reply (`NOPERM`, `OOM`, ...): `detail` is `safe_redis_error` text."""

    detail: str


ScriptReply = ScriptOk | ScriptRefused | ScriptFailed


def _text(value: object) -> str:
    return value.decode("ascii", "replace") if isinstance(value, bytes) else str(value)


def unavailable(exc: BaseException) -> StoreUnavailable:
    """The `StoreUnavailable` for a redis-py exception (timeouts first: redis-py's TimeoutError is no
    ConnectionError, and the builtin one can escape a socket read)."""
    if isinstance(exc, (RedisTimeoutError, TimeoutError)):
        return StoreUnavailable(TIMEOUT, safe_redis_error(exc))
    if isinstance(exc, (RedisConnectionError, OSError)):
        return StoreUnavailable(CONNECTION, safe_redis_error(exc))
    return StoreUnavailable(ERROR, safe_redis_error(exc))


class ScriptRunner:
    """`SCRIPT LOAD` on (re)connect, `EVALSHA` through a non-transactional pipeline, NOSCRIPT reload."""

    def __init__(self, redis: Redis, *, run_id_reader: RunIdReader | None = None,
                 scripts: Sequence[Script] = SCRIPTS) -> None:
        self._redis = redis
        self._read_run_id: RunIdReader = run_id_reader or self._info_run_id
        self._scripts = tuple(scripts)
        self._connects = 0
        self._ready_at: int | None = None
        self._run_id: str | None = None
        self._hooks = 0
        self._lock = asyncio.Lock()

    # ---------------------------------------------------------------- connect hook
    @property
    def run_id(self) -> str | None:
        """The `run_id` read by the last connect hook (None before the first)."""
        return self._run_id

    @property
    def hooks_run(self) -> int:
        return self._hooks

    @property
    def connects(self) -> int:
        """How many (re)connects or connection failures were seen. A change between two commands means they may
        not have run on the same connection (`write_and_wait` compares it around `WAITAOF`)."""
        return self._connects

    @property
    def ready(self) -> bool:
        return self._ready_at == self._connects

    def mark_reconnected(self) -> None:
        """A new connection was made (redis-py connect callback) or the connection failed: re-run the hook."""
        self._connects += 1

    async def _info_run_id(self) -> str:
        info = await self._redis.info("server")
        value = info.get("run_id") if isinstance(info, dict) else None
        return "" if value is None else _text(value)

    async def on_connect(self) -> None:
        """SCRIPT LOAD every script and read `INFO server run_id` (one hook). Raises `StoreUnavailable`."""
        async with self._lock:
            seen = self._connects
            try:
                for script in self._scripts:
                    sha = _text(await self._redis.script_load(script.source))
                    if sha != script.sha:
                        raise StoreUnavailable(ERROR, f"SCRIPT LOAD returned another SHA for {script.name}")
                run_id = await self._read_run_id()
            except RedisError as e:
                self.mark_reconnected()
                raise unavailable(e) from None
            except (TimeoutError, OSError) as e:
                self.mark_reconnected()
                raise unavailable(e) from None
            if not valid_run_id(run_id):
                raise StoreUnavailable(ERROR, "INFO server gave no usable run_id")
            self._run_id = run_id
            self._ready_at = seen
            self._hooks += 1

    # ---------------------------------------------------------------- calls
    def _argv(self, call: ScriptCall) -> list[str]:
        run_id = self._run_id or ""
        return [run_id if a == RUN_ID else a for a in call.args]

    async def _send(self, calls: Sequence[ScriptCall]) -> list[Any]:
        pipe = self._redis.pipeline(transaction=False)
        for call in calls:
            pipe.evalsha(call.script.sha, len(call.keys), *call.keys, *self._argv(call))
        try:
            return list(await pipe.execute(raise_on_error=False))
        except RedisError as e:
            self.mark_reconnected()
            raise unavailable(e) from None
        except (TimeoutError, OSError) as e:
            self.mark_reconnected()
            raise unavailable(e) from None

    async def run(self, calls: Sequence[ScriptCall]) -> list[ScriptReply]:
        """Run `calls` in order over one pipeline. Raises `StoreUnavailable` on a connection error or timeout.

        Every call runs whatever an earlier one returned (no `MULTI`): send dependent calls with `run_sequential`."""
        if not calls:
            return []
        if not self.ready:
            await self.on_connect()
        raw = await self._send(calls)
        missing = [i for i, r in enumerate(raw) if isinstance(r, NoScriptError)]
        if missing:
            log.info("redis-md scripts missing (NOSCRIPT); reloading")
            self.mark_reconnected()
            await self.on_connect()
            again = await self._send([calls[i] for i in missing])
            for i, r in zip(missing, again):
                raw[i] = r
        return [self._reply(r) for r in raw]

    async def run_one(self, call: ScriptCall) -> ScriptReply:
        return (await self.run([call]))[0]

    async def run_sequential(self, calls: Sequence[ScriptCall]) -> list[ScriptReply]:
        """Run `calls` one at a time and stop at the first reply that is not `ScriptOk`: the result is shorter than
        `calls` when one was refused or failed, and the later calls were never sent."""
        replies: list[ScriptReply] = []
        for call in calls:
            reply = await self.run_one(call)
            replies.append(reply)
            if not isinstance(reply, ScriptOk):
                break
        return replies

    @staticmethod
    def _reply(raw: Any) -> ScriptReply:
        if isinstance(raw, ResponseError):
            code = redis_error_code(raw)
            if code in SCRIPT_CODES:
                return ScriptRefused(code)
            return ScriptFailed(safe_redis_error(raw))
        if isinstance(raw, BaseException):
            return ScriptFailed(safe_redis_error(raw))
        return ScriptOk(raw)
