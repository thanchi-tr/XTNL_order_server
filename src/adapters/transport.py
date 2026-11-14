"""ReadOnlyTransport (lockout layer 4, adapter-isolation.md §4).

REST: order-changing endpoints are refused by **path**, never by HTTP verb (StoneX
`/order/activeorders` is a read-only POST). This module is the one place allowed to name
the blocked paths (see .claude/hooks/pre_tool_guard.py).
"""
from __future__ import annotations

import re
from typing import Any

import httpx

from src.config import BrokerEnv, ExecutionLockedError, broker_env

# Lower-cased path fragments that change orders on StoneX (CIAPI).
DENY_PATH_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p) for p in (
        r"/order/newtradeorder\b",
        r"/order/newstoplimitorder\b",
        r"/order/updatetradeorder\b",
        r"/order/updatestoplimitorder\b",
        r"/order/cancel\b",
        r"/order/fixedmargin",
        r"/order/simulate",
        r"/order/order\b",
        r"/order/trade\b",
    )
)


def is_denied_path(path: str) -> bool:
    p = path.lower().split("?", 1)[0]
    return any(rx.search(p) for rx in DENY_PATH_PATTERNS)


class ReadOnlyTransport:
    """Wraps an httpx.AsyncClient; refuses order-changing paths and unknown environments."""

    def __init__(self, client: httpx.AsyncClient, env: BrokerEnv | None = None) -> None:
        self._client = client
        self.env: BrokerEnv = env if env is not None else broker_env()

    async def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        if is_denied_path(path):
            raise ExecutionLockedError(f"order-changing path refused in Phase 1: {method} {path}")
        broker_env({"BROKER_ENV": self.env})  # re-validate (raises on anything unexpected)
        return await self._client.request(method, path, **kwargs)

    async def aclose(self) -> None:
        await self._client.aclose()
