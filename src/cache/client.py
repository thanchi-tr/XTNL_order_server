"""Redis client factory (redis.asyncio). The URL comes from REDIS_URL (src/db/settings.py) and is never
logged: it may carry a password."""
from __future__ import annotations

from typing import Any, Final

import redis.asyncio as aioredis

# The feed observer's XREADGROUP BLOCK read must not time out on an ordinary event-loop stall: its client's
# socket timeout is at least block_ms + this margin (the 2 s engine client leaves only ~1 s over BLOCK 1000).
FEED_SOCKET_MARGIN_S: Final = 5.0
# Wiring guard: the smallest acceptable margin of a feed client's socket timeout over the BLOCK time.
FEED_MIN_MARGIN_S: Final = 2.0


def create_redis(url: str, *, socket_timeout_s: float = 2.0, connect_timeout_s: float = 2.0) -> aioredis.Redis:
    """Bytes responses (the Lua scripts and readers decode explicitly); bounded timeouts so a stalled Redis
    never blocks the loop beyond the gate / sweep budgets."""
    return aioredis.Redis.from_url(url, decode_responses=False, socket_timeout=socket_timeout_s,
                                   socket_connect_timeout=connect_timeout_s, health_check_interval=15)


def socket_timeout_of(client: aioredis.Redis) -> float | None:
    """The socket timeout (s) the client's connections use for every read; None = no timeout."""
    v = client.connection_pool.connection_kwargs.get("socket_timeout")
    return None if v is None else float(v)


def feed_timeout_ok(client: aioredis.Redis, block_ms: int, *, min_margin_s: float = FEED_MIN_MARGIN_S) -> bool:
    """True when a BLOCK `block_ms` read on `client` has at least `min_margin_s` of socket-timeout headroom."""
    t = socket_timeout_of(client)
    return t is None or t >= block_ms / 1000 + min_margin_s


def feed_redis_from(base: aioredis.Redis, block_ms: int, *, margin_s: float = FEED_SOCKET_MARGIN_S) -> aioredis.Redis:
    """A dedicated client for the feed observer (XREADGROUP BLOCK), built from `base`'s connection settings
    (same server, credentials and db) with socket timeout block_ms/1000 + margin_s. The gate, sweep and SM8
    keep `base` and its 2 s budget. The caller owns the new client and must `aclose()` it."""
    pool = base.connection_pool
    kwargs: dict[str, Any] = {k: v for k, v in pool.connection_kwargs.items()
                              if k not in ("maint_notifications_pool_handler", "orig_socket_timeout")}
    kwargs["socket_timeout"] = block_ms / 1000 + margin_s
    return aioredis.Redis(connection_pool=aioredis.ConnectionPool(connection_class=pool.connection_class, **kwargs))
