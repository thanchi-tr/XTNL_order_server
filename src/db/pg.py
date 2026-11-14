"""Driver-neutral PostgreSQL connection protocol plus the asyncpg adapter (skill §1 stack standard:
asyncpg binary protocol).

SQL in src/db uses `$n` placeholders (asyncpg) and lives in module constants. Rows are returned as plain
tuples so the engine logic can be tested with a fake connection. Engine connections: connect timeout
3 s, statement_timeout 5 s (operations.md §4); COMMIT runs with statement_timeout 0 (operations.md §2).

TCP keepalives ~5 s (operations.md §4, IMPLEMENTATION_PLAN 6.4): asyncpg has no libpq `keepalives_*`
options, so `init_connection` sets them on every pooled connection's socket (`configure_tcp_keepalive`):
SO_KEEPALIVE with 5 s idle, 1 s interval, 3 probes, plus TCP_USER_TIMEOUT 8 s on Linux, so a primary that
vanished without a RST is detected in ~8 s instead of the kernel defaults (2 h keepalive, ~15 min
retransmission). Keepalive probes are answered by the peer's kernel, so a long but healthy COMMIT (sync
replication wait, statement_timeout 0) is never cut; TCP_USER_TIMEOUT only fires when data sent to the server
stays unacknowledged. Platforms without these options (Windows dev boxes) get what they support.

Codecs (installed on every pooled connection by `init_connection`):
* NUMERIC <-> `decimal.Decimal`: asyncpg's native binary codec already maps NUMERIC to Decimal; the
  engine never passes floats for money (dto-contracts.md §3).
* jsonb / json <-> Python objects, text format, Decimal kept exact: `json_dumps` writes Decimal as a JSON
  string, `json_loads` parses JSON numbers as Decimal. Callers may still pass a pre-encoded JSON string
  with an explicit `$n::text::jsonb` cast where the payload is already canonical (event_log, outbox).
"""
from __future__ import annotations

import importlib
import json
import logging
import socket
import sys
from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from decimal import Decimal
from typing import Any, Final, Protocol

log = logging.getLogger(__name__)

Row = tuple[Any, ...]

#: Client-side TCP keepalive (operations.md §4 "TCP keepalives ~5 s").
KEEPALIVE_IDLE_S: Final = 5
KEEPALIVE_INTERVAL_S: Final = 1
KEEPALIVE_COUNT: Final = 3
#: Unacknowledged data older than this closes the socket (Linux TCP_USER_TIMEOUT): idle + count x interval.
TCP_USER_TIMEOUT_MS: Final = (KEEPALIVE_IDLE_S + KEEPALIVE_COUNT * KEEPALIVE_INTERVAL_S) * 1000

# SQLSTATE classes/codes that mean "the connection or server is gone" (not a statement failure).
_CONNECTION_SQLSTATE_PREFIXES = ("08", "57P")
_CONNECTION_ERROR_NAMES = frozenset({
    "InterfaceError", "ConnectionDoesNotExistError", "ConnectionFailureError", "PostgresConnectionError",
    "CannotConnectNowError", "TooManyConnectionsError", "OperationalError",
})


class PgConnection(Protocol):
    async def execute(self, sql: str, *args: object) -> str:
        """Run one statement; returns the command status tag (e.g. 'UPDATE 1')."""
        ...

    async def fetchrow(self, sql: str, *args: object) -> Row | None: ...

    async def fetch(self, sql: str, *args: object) -> list[Row]: ...


class ConnectionSource(Protocol):
    def acquire(self) -> AbstractAsyncContextManager[PgConnection]: ...


def rowcount(status: str) -> int:
    """Rows affected from a command tag ('INSERT 0 3' -> 3, 'UPDATE 1' -> 1)."""
    try:
        return int(status.rsplit(" ", 1)[-1])
    except (ValueError, IndexError):
        return 0


def sqlstate(exc: BaseException) -> str | None:
    code = getattr(exc, "sqlstate", None)
    return code if isinstance(code, str) else None


def is_connection_error(exc: BaseException) -> bool:
    """True when PostgreSQL is unreachable / the connection dropped (vs. a statement error)."""
    if isinstance(exc, (OSError, TimeoutError)):
        return True
    code = sqlstate(exc)
    if code is not None and code.startswith(_CONNECTION_SQLSTATE_PREFIXES):
        return True
    return any(cls.__name__ in _CONNECTION_ERROR_NAMES for cls in type(exc).__mro__)


# ---- JSON codec (exact decimals) -------------------------------------------------------------------

def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    raise TypeError(f"not JSON serialisable: {type(value).__name__}")


def json_dumps(value: object) -> str:
    """Canonical compact JSON with Decimal written as an exact string."""
    if isinstance(value, str):
        return value  # already encoded by the caller (canonical DTO JSON)
    return json.dumps(value, default=_json_default, sort_keys=True, separators=(",", ":"))


def json_loads(text: str | bytes) -> Any:
    return json.loads(text, parse_float=Decimal)


def configure_tcp_keepalive(sock: Any) -> list[str]:
    """Turn on client TCP keepalives (~5 s) and, where available, TCP_USER_TIMEOUT on `sock`. Returns the
    options set (for the log / tests). A unix-domain socket (local PostgreSQL) needs none: returns []."""
    family = getattr(sock, "family", None)
    if family is not None and family not in (socket.AF_INET, socket.AF_INET6):
        return []
    done: list[str] = []
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    done.append("SO_KEEPALIVE")
    tcp = socket.IPPROTO_TCP
    for name, value in (("TCP_KEEPIDLE", KEEPALIVE_IDLE_S), ("TCP_KEEPALIVE", KEEPALIVE_IDLE_S),   # macOS idle
                        ("TCP_KEEPINTVL", KEEPALIVE_INTERVAL_S), ("TCP_KEEPCNT", KEEPALIVE_COUNT),
                        ("TCP_USER_TIMEOUT", TCP_USER_TIMEOUT_MS)):
        opt = getattr(socket, name, None)
        if opt is None or (name == "TCP_KEEPALIVE" and "TCP_KEEPIDLE" in done):
            continue
        try:
            sock.setsockopt(tcp, opt, value)
            done.append(name)
        except OSError as exc:      # an option this kernel lacks: keep the others
            log.debug("TCP option %s not set: %s", name, exc)
    sio = getattr(socket, "SIO_KEEPALIVE_VALS", None)
    if sio is not None and sys.platform == "win32" and "TCP_KEEPIDLE" not in done:
        try:
            sock.ioctl(sio, (1, KEEPALIVE_IDLE_S * 1000, KEEPALIVE_INTERVAL_S * 1000))
            done.append("SIO_KEEPALIVE_VALS")
        except (OSError, AttributeError) as exc:
            log.debug("SIO_KEEPALIVE_VALS not set: %s", exc)
    return done


def _connection_socket(raw: Any) -> Any:
    """The socket under an asyncpg connection (asyncpg exposes no public accessor; None if unavailable)."""
    transport = getattr(raw, "_transport", None)
    get = getattr(transport, "get_extra_info", None)
    return get("socket") if callable(get) else None


async def init_connection(raw: Any) -> None:
    """Pool `init` hook: client TCP keepalives on the socket, then jsonb / json codecs (text format) with exact
    decimals."""
    sock = _connection_socket(raw)
    if sock is None:
        log.warning("PostgreSQL connection socket not reachable: TCP keepalives not set (kernel defaults apply)")
    else:
        try:
            configure_tcp_keepalive(sock)
        except OSError as exc:
            log.warning("PostgreSQL TCP keepalives not set: %s", exc)
    for typename in ("jsonb", "json"):
        await raw.set_type_codec(typename, schema="pg_catalog", encoder=json_dumps, decoder=json_loads,
                                 format="text")


# ---- asyncpg adapter ---------------------------------------------------------------------------------

class AsyncpgConnection:
    """Adapts an asyncpg Connection to PgConnection (Record -> tuple)."""

    def __init__(self, raw: Any) -> None:
        self._raw = raw

    async def execute(self, sql: str, *args: object) -> str:
        return str(await self._raw.execute(sql, *args))

    async def fetchrow(self, sql: str, *args: object) -> Row | None:
        rec = await self._raw.fetchrow(sql, *args)
        return None if rec is None else tuple(rec)

    async def fetch(self, sql: str, *args: object) -> list[Row]:
        return [tuple(r) for r in await self._raw.fetch(sql, *args)]


class AsyncpgSource:
    """ConnectionSource over an asyncpg pool."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @asynccontextmanager
    async def _acquire(self) -> AsyncIterator[PgConnection]:
        async with self._pool.acquire() as raw:
            yield AsyncpgConnection(raw)

    def acquire(self) -> AbstractAsyncContextManager[PgConnection]:
        return self._acquire()

    async def close(self) -> None:
        await self._pool.close()


def pool_kwargs(
    *,
    application_name: str,
    statement_timeout_ms: int = 5_000,
    connect_timeout_s: float = 3.0,
    min_size: int = 1,
    max_size: int = 10,
) -> dict[str, Any]:
    """Keyword arguments for asyncpg.create_pool (separated so they can be tested without a server)."""
    return {
        "min_size": min_size,
        "max_size": max_size,
        "timeout": connect_timeout_s,
        "init": init_connection,
        "server_settings": {"application_name": application_name,
                            "statement_timeout": str(statement_timeout_ms)},
    }


async def create_asyncpg_source(
    dsn: str,
    *,
    application_name: str,
    statement_timeout_ms: int = 5_000,
    connect_timeout_s: float = 3.0,
    min_size: int = 1,
    max_size: int = 10,
) -> AsyncpgSource:
    """Engine pool (asyncpg, binary protocol). application_name carries the epoch (see
    epoch.application_name) so a newer leader can terminate an older epoch's backends after the bump
    (operations.md §2). TCP keepalives (~5 s) are set on every pooled connection by `init_connection`."""
    asyncpg = importlib.import_module("asyncpg")  # untyped library: imported dynamically
    pool = await asyncpg.create_pool(dsn, **pool_kwargs(
        application_name=application_name, statement_timeout_ms=statement_timeout_ms,
        connect_timeout_s=connect_timeout_s, min_size=min_size, max_size=max_size))
    return AsyncpgSource(pool)


def text_array(values: Sequence[object]) -> list[str | None]:
    """Values for a `$n::text[]` parameter."""
    return [None if v is None else str(v) for v in values]
