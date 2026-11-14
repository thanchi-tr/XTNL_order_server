"""Storage configuration from the environment: ORDER_SERVER_DSN (PostgreSQL) and REDIS_URL.

Nothing here reads .env files, and secrets are never printed: `repr()` and `redact()` drop the password
(and any query-string credentials) from both URLs.
"""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final
from urllib.parse import urlsplit, urlunsplit

DSN_VAR: Final = "ORDER_SERVER_DSN"
REDIS_VAR: Final = "REDIS_URL"
DEFAULT_REDIS_URL: Final = "redis://127.0.0.1:6379/0"


def redact(url: str) -> str:
    """URL without password or query string: 'postgresql://user:***@host:5432/db'.

    Never raises: a multi-host libpq DSN ('postgresql://u:p@h1:5432,h2:5432/db') keeps its host list
    verbatim, and anything unparseable becomes '<unparseable url>'."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return "<unparseable url>"
    userinfo, sep, hosts = parts.netloc.rpartition("@")
    netloc = hosts
    if sep:
        user, colon, _password = userinfo.partition(":")
        netloc = f"{user}{':***' if colon else ''}@{hosts}"
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))


@dataclass(frozen=True)
class StorageSettings:
    dsn: str
    redis_url: str

    @staticmethod
    def from_env(environ: Mapping[str, str] | None = None) -> StorageSettings:
        e = environ if environ is not None else os.environ
        dsn = e.get(DSN_VAR, "").strip()
        if not dsn:
            raise KeyError(f"missing environment variable {DSN_VAR}")
        if urlsplit(dsn).scheme not in ("postgres", "postgresql"):
            raise ValueError(f"{DSN_VAR} must be a postgresql:// URL")
        redis_url = e.get(REDIS_VAR, "").strip() or DEFAULT_REDIS_URL
        if urlsplit(redis_url).scheme not in ("redis", "rediss", "unix"):
            raise ValueError(f"{REDIS_VAR} must be a redis://, rediss:// or unix:// URL")
        return StorageSettings(dsn, redis_url)

    def __repr__(self) -> str:
        return f"StorageSettings(dsn={redact(self.dsn)!r}, redis_url={redact(self.redis_url)!r})"

    __str__ = __repr__
