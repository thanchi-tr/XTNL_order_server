"""Redis client factory (redis.asyncio). The URL comes from REDIS_URL (src/db/settings.py) and is never
logged: it may carry a password."""
from __future__ import annotations

import redis.asyncio as aioredis


def create_redis(url: str, *, socket_timeout_s: float = 2.0, connect_timeout_s: float = 2.0) -> aioredis.Redis:
    """Bytes responses (the Lua scripts and readers decode explicitly); bounded timeouts so a stalled Redis
    never blocks the loop beyond the gate / sweep budgets."""
    return aioredis.Redis.from_url(url, decode_responses=False, socket_timeout=socket_timeout_s,
                                   socket_connect_timeout=connect_timeout_s, health_check_interval=15)
