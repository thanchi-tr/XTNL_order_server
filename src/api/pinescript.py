"""This week's Pine Script order history: compile on request, cache in Redis only, report the state.

* Week: the current ISO week in UTC, label 'YYYY-Www'; the window runs from its Monday 00:00 UTC to the moment
  the compile starts.
* Single flight: one compile per week in this process; a request while it runs reuses it. force=False with a
  ready cache entry answers "ready" without compiling; force=True always starts a new compile (or reuses the one
  already running).
* Cache (Redis database XTNL_API_REDIS_DB only, never disk or PostgreSQL):
    pinescript:week:<week>    the compiled document (JSON), TTL 8 days
    pinescript:status:<week>  compiling (TTL 3 min, so a crash never sticks) / ready / failed (TTL 15 min)
* Read: PostgreSQL in one READ ONLY, REPEATABLE READ transaction through src.reports (plan_pinescript): symbols,
  sides, times, status names and price levels only; never a size, amount, balance or spread. Phase 1 never
  sends orders.
* Role: the connection must be role xtnl_api (migration 0014) or as weak: before anything is read, a role that is
  a superuser, can write any table, or whose session is not default_transaction_read_only is refused
  ("db_role_refused"), so the API never compiles over the engine's writable role.

A failure is reported as a short code ("db_unavailable", "db_query_failed", "timeout", "compile_failed",
"interrupted"); exception text never leaves the process (the log carries the exception type only).
"""
from __future__ import annotations

import asyncio
import importlib
import json
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Protocol

from src.db.pg import AsyncpgConnection, PgConnection, init_connection, is_connection_error
from src.reports.export import plan_pinescript, plan_texts
from src.reports.lifecycle import Window

log = logging.getLogger("xtnl.api")

WEEK_KEY: Final = "pinescript:week:{week}"
STATUS_KEY: Final = "pinescript:status:{week}"
CACHE_TTL_S: Final = 8 * 86_400
COMPILING_TTL_S: Final = 180
FAILED_TTL_S: Final = 900
COMPILE_TIMEOUT_S: Final = 150.0
APPLICATION_NAME: Final = "xtnl-api"
#: (role, superuser?, session default_transaction_read_only, number of tables the role could write).
SQL_ROLE_CHECK: Final = (
    "SELECT current_user::text, r.rolsuper, current_setting('default_transaction_read_only'), "
    "(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE c.relkind IN ('r', 'p') AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\\_%' "
    "AND (has_table_privilege(c.oid, 'INSERT') OR has_table_privilege(c.oid, 'UPDATE') "
    "OR has_table_privilege(c.oid, 'DELETE') OR has_table_privilege(c.oid, 'TRUNCATE'))) "
    "FROM pg_roles r WHERE r.rolname = current_user"
)

Clock = Callable[[], int]
#: (window, now_ms) -> ((symbol, Pine Script source), ...)
Compiler = Callable[[Window, int], Awaitable[Sequence[tuple[str, str]]]]
Closer = Callable[[], Awaitable[None]]
Connect = Callable[[str], Awaitable[tuple[PgConnection, Closer]]]


class CacheClient(Protocol):
    """The two Redis commands used (redis.asyncio.Redis, fakeredis)."""

    def get(self, name: str) -> Awaitable[Any]: ...

    def set(self, name: str, value: str, ex: int | None = None) -> Awaitable[Any]: ...


class CompileError(Exception):
    """A compile failure with a short, non-sensitive code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def iso_week(at_ms: int) -> tuple[str, int]:
    """('YYYY-Www', Monday 00:00 UTC of that ISO week in ms) for the instant `at_ms`."""
    t = datetime.fromtimestamp(at_ms / 1000, tz=UTC)
    year, week, weekday = t.isocalendar()
    monday = (t - timedelta(days=weekday - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return f"{year:04d}-W{week:02d}", int(monday.timestamp() * 1000)


def week_window(at_ms: int) -> tuple[str, Window]:
    week, monday = iso_week(at_ms)
    return week, Window(monday, at_ms)


async def connect_readonly(dsn: str) -> tuple[PgConnection, Closer]:   # pragma: no cover - needs a server
    asyncpg = importlib.import_module("asyncpg")
    raw = await asyncpg.connect(dsn, timeout=5, server_settings={
        "application_name": APPLICATION_NAME, "statement_timeout": "60000",
        "default_transaction_read_only": "on", "idle_in_transaction_session_timeout": "10000"})
    await init_connection(raw)

    async def close() -> None:
        await raw.close()

    return AsyncpgConnection(raw), close


def role_problems(row: Sequence[object] | None) -> list[str]:
    """Why the session of SQL_ROLE_CHECK's row may not serve the API (empty = a read-only role)."""
    if row is None:
        return ["role not found in pg_roles"]
    problems: list[str] = []
    if bool(row[1]):
        problems.append("is a superuser")
    writable = row[3]
    if int(writable) if isinstance(writable, (int, str)) else 1:
        problems.append("can write tables")
    if str(row[2]).lower() != "on":
        problems.append("session is not default_transaction_read_only")
    return problems


async def verify_read_only_role(conn: PgConnection) -> None:
    """Raise CompileError("db_role_refused") unless the session is a read-only role (role name logged only)."""
    try:
        row = await conn.fetchrow(SQL_ROLE_CHECK)
    except Exception as exc:
        raise CompileError("db_unavailable" if is_connection_error(exc) else "db_query_failed") from exc
    problems = role_problems(row)
    if problems:
        role = str(row[0]) if row is not None else "?"
        log.critical("PostgreSQL role refused for the API", extra={"event": "db_role_refused", "role": role[:64],
                                                                   "problems": ", ".join(problems)})
        raise CompileError("db_role_refused")


def pg_compiler(dsn: str, connect: Connect = connect_readonly, *, verify_role: bool = True) -> Compiler:
    """The real compiler: one read-only connection per compile, the report code renders in memory. The role is
    checked first (verify_read_only_role); verify_role=False only for tests against a throwaway database."""

    async def compile_week(window: Window, at_ms: int) -> Sequence[tuple[str, str]]:
        try:
            conn, close = await connect(dsn)
        except Exception as exc:
            raise CompileError("db_unavailable") from exc
        try:
            if verify_role:
                await verify_read_only_role(conn)
            plan = await plan_pinescript(conn, window, now_ms=at_ms)
        except CompileError:
            raise
        except Exception as exc:
            raise CompileError("db_unavailable" if is_connection_error(exc) else "db_query_failed") from exc
        finally:
            try:
                await close()
            except Exception:
                pass
        return plan_texts(plan)

    return compile_week


def _loads(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


class PinescriptService:
    def __init__(self, cache: CacheClient, compiler: Compiler, *, clock: Clock = now_ms,
                 timeout_s: float = COMPILE_TIMEOUT_S) -> None:
        self._cache = cache
        self._compiler = compiler
        self._clock = clock
        self._timeout_s = timeout_s
        self._tasks: dict[str, asyncio.Task[None]] = {}

    def running(self, week: str) -> bool:
        task = self._tasks.get(week)
        return task is not None and not task.done()

    async def start(self, *, force: bool) -> dict[str, Any]:
        """POST /v1/pinescript/compile. Redis errors propagate (the server answers 503)."""
        at = self._clock()
        week, window = week_window(at)
        if self.running(week):
            return {"week": week, "status": "compiling"}
        if not force and _loads(await self._cache.get(WEEK_KEY.format(week=week))) is not None:
            return {"week": week, "status": "ready"}
        await self._cache.set(STATUS_KEY.format(week=week),
                              json.dumps({"status": "compiling", "started_ms": at}), ex=COMPILING_TTL_S)
        for old in [w for w, t in self._tasks.items() if t.done()]:
            del self._tasks[old]
        self._tasks[week] = asyncio.create_task(self._run(week, window, at), name=f"pinescript-{week}")
        log.info("compile started", extra={"event": "compile_started", "week": week, "force": force})
        return {"week": week, "status": "compiling"}

    async def _run(self, week: str, window: Window, at: int) -> None:
        started = time.monotonic()
        try:
            scripts = await asyncio.wait_for(self._compiler(window, at), self._timeout_s)
            doc = {"week": week, "status": "ready", "generated_at": iso_ms(at), "generated_ms": at,
                   "window": {"from": iso_ms(window.since_ms), "to": iso_ms(window.until_ms)},
                   "scripts": [{"symbol": s, "text": t} for s, t in scripts]}
            await self._cache.set(WEEK_KEY.format(week=week), json.dumps(doc, separators=(",", ":")),
                                  ex=CACHE_TTL_S)
            await self._cache.set(STATUS_KEY.format(week=week),
                                  json.dumps({"status": "ready", "finished_ms": self._clock()}), ex=CACHE_TTL_S)
            log.info("compile ready", extra={"event": "compile_ready", "week": week, "scripts": len(scripts),
                                             "latency_ms": int((time.monotonic() - started) * 1000)})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if isinstance(exc, CompileError):
                code = exc.code
            elif isinstance(exc, TimeoutError):
                code = "timeout"
            else:
                code = "compile_failed"
            cause = exc.__cause__ if isinstance(exc, CompileError) and exc.__cause__ is not None else exc
            log.warning("compile failed", extra={"event": "compile_failed", "week": week, "error": code,
                                                 "exc_type": type(cause).__name__})
            try:
                await self._cache.set(STATUS_KEY.format(week=week),
                                      json.dumps({"status": "failed", "error": code, "failed_ms": self._clock()}),
                                      ex=FAILED_TTL_S)
            except Exception as cache_exc:
                log.warning("compile state not recorded", extra={"event": "cache_write_failed", "week": week,
                                                                 "exc_type": type(cache_exc).__name__})

    async def latest(self) -> dict[str, Any]:
        """GET /v1/pinescript/latest. Redis errors propagate (the server answers 503)."""
        at = self._clock()
        week, window = week_window(at)
        current = {"from": iso_ms(window.since_ms), "to": iso_ms(window.until_ms)}
        if self.running(week):
            return {"week": week, "status": "compiling", "generated_at": None, "window": current}
        doc = _loads(await self._cache.get(WEEK_KEY.format(week=week)))
        state = _loads(await self._cache.get(STATUS_KEY.format(week=week))) or {}
        if state.get("status") == "failed":
            failed_ms = state.get("failed_ms")
            generated_ms = doc.get("generated_ms") if doc else None
            newer = isinstance(failed_ms, int) and isinstance(generated_ms, int) and failed_ms > generated_ms
            if doc is None or not isinstance(generated_ms, int) or newer:
                error = state.get("error")
                return {"week": week, "status": "failed", "generated_at": None, "window": current,
                        "error": error if isinstance(error, str) else "compile_failed"}
        if doc is not None and isinstance(doc.get("scripts"), list):
            return {"week": week, "status": "ready", "generated_at": doc.get("generated_at"),
                    "window": doc.get("window") or current,
                    "scripts": [{"symbol": str(s.get("symbol", "")), "text": str(s.get("text", ""))}
                                for s in doc["scripts"] if isinstance(s, dict)]}
        if state.get("status") == "compiling":   # recorded, but no compile runs here: the process restarted
            return {"week": week, "status": "failed", "generated_at": None, "window": current,
                    "error": "interrupted"}
        return {"week": week, "status": "missing", "generated_at": None, "window": current}

    async def close(self) -> None:
        tasks = [t for t in self._tasks.values() if not t.done()]
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
