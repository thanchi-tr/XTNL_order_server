"""Order-server API settings from the environment. Nothing here reads .env files; the secret is never printed.

    XTNL_API_SECRET        required: the shared HMAC secret, hex or base64 of >= 32 random bytes. Set in the
                           Windows User environment by deploy/local/new-api-credentials.ps1, never in a file.
                           The HMAC key is the configured text itself (UTF-8), exactly as the website holds it.
    XTNL_API_PORT          TCP port (default 47443)
    XTNL_API_BIND          bind address (default 0.0.0.0: this is the one network-facing listener)
    XTNL_API_TLS_CERT      PEM certificate (default var/api-tls/server.crt)
    XTNL_API_TLS_KEY       PEM private key (default var/api-tls/server.key)
    XTNL_API_REDIS_DB      Redis database index of the Pine Script cache (default 2)
    XTNL_API_RATE_PER_MIN  requests per minute per client IP (default 30)
    XTNL_API_DSN           required: PostgreSQL as role xtnl_api (migration 0014: SELECT on the three tables the
                           compile reads, read-only sessions). Set in the Windows User environment by
                           deploy/local/new-api-credentials.ps1 -DbRole. Never the engine's ORDER_SERVER_DSN.
    XTNL_API_REDIS_URL     the API's OWN Redis (default redis://127.0.0.1:56381, deploy/local/docker-compose.local.yml
                           service redis-api), never the engine's; the database index is replaced by XTNL_API_REDIS_DB

Least privilege: the API is the only network-facing process, so it must not hold anything of the engine's.
from_env refuses to start (exit 78) while the environment carries a broker credential (STONE_X_*, CTRADER_*),
the engine's storage (ORDER_SERVER_DSN, REDIS_URL), BROKER_ENV, a market-data / ARM setting or secret (XTNL_MD_*,
XTNL_ARM_*), an alert channel setting or credential (XTNL_SMTP_*, XTNL_SES_*, XTNL_PUSH_*) or the dead-man token
(XTNL_DEADMAN_TOKEN); deploy/local/start-xtnl.ps1 removes them from the API child's environment (names only are
ever reported, never values).
"""
from __future__ import annotations

import base64
import binascii
import ipaddress
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from src.db.settings import redact

SECRET_VAR: Final = "XTNL_API_SECRET"
PORT_VAR: Final = "XTNL_API_PORT"
BIND_VAR: Final = "XTNL_API_BIND"
CERT_VAR: Final = "XTNL_API_TLS_CERT"
KEY_VAR: Final = "XTNL_API_TLS_KEY"
REDIS_DB_VAR: Final = "XTNL_API_REDIS_DB"
RATE_VAR: Final = "XTNL_API_RATE_PER_MIN"
DSN_VAR: Final = "XTNL_API_DSN"
REDIS_URL_VAR: Final = "XTNL_API_REDIS_URL"
#: Never in the API's environment: broker credentials, the engine's own storage (writable role, engine Redis), the
#: market-data / ARM settings and secrets (redis-md URLs, the content-hash key, the config DSN, the ARM channels), the
#: Oversee alert channels, the dead-man token and the broker environment (design §C2 Secrets). Equal to
#: deploy/local/start-xtnl.ps1 `$ApiDropEnv` (tests/deploy/test_order_server_api_assets.py).
FORBIDDEN_PREFIXES: Final = ("STONE_X_", "CTRADER_", "XTNL_MD_", "XTNL_ARM_", "XTNL_SMTP_", "XTNL_SES_", "XTNL_PUSH_")
FORBIDDEN_NAMES: Final = frozenset({"ORDER_SERVER_DSN", "REDIS_URL", "XTNL_DEADMAN_TOKEN", "BROKER_ENV"})

DEFAULT_PORT: Final = 47443
DEFAULT_BIND: Final = "0.0.0.0"
DEFAULT_CERT: Final = "var/api-tls/server.crt"
DEFAULT_KEY: Final = "var/api-tls/server.key"
DEFAULT_REDIS_DB: Final = 2
DEFAULT_API_REDIS_URL: Final = "redis://127.0.0.1:56381"
DEFAULT_RATE_PER_MIN: Final = 30
MIN_SECRET_BYTES: Final = 32

_HEX = re.compile(r"[0-9a-fA-F]+")
_B64 = re.compile(r"[A-Za-z0-9+/_-]+={0,2}")


class ApiConfigError(ValueError):
    """A setting is missing or invalid. The message names the variable, never its value."""


def secret_strength(text: str) -> int:
    """Random bytes the secret text encodes (hex first, then base64 / base64url); 0 when it is neither."""
    t = text.strip()
    if len(t) % 2 == 0 and _HEX.fullmatch(t):
        return len(t) // 2
    if _B64.fullmatch(t):
        padded = t.replace("-", "+").replace("_", "/")
        padded += "=" * (-len(padded) % 4)
        try:
            return len(base64.b64decode(padded, validate=True))
        except (binascii.Error, ValueError):
            return 0
    return 0


def redis_url_with_db(url: str, db: int) -> str:
    """The same Redis with database `db`: the path of redis:// / rediss://, the `db` query of unix://."""
    parts = urlsplit(url)
    if parts.scheme == "unix":
        query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != "db"]
        query.append(("db", str(db)))
        return f"unix://{parts.netloc}{parts.path}?{urlencode(query)}"
    return urlunsplit((parts.scheme, parts.netloc, f"/{db}", parts.query, ""))


def forbidden_env_names(environ: Mapping[str, str]) -> list[str]:
    """Names (never values) of variables the API process must not hold: broker credentials, engine storage."""
    return sorted(n for n, v in environ.items()
                  if v and (n.upper() in FORBIDDEN_NAMES or n.upper().startswith(FORBIDDEN_PREFIXES)))


def _int(e: Mapping[str, str], name: str, default: int, lo: int, hi: int) -> int:
    raw = e.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ApiConfigError(f"{name} must be an integer") from None
    if not lo <= value <= hi:
        raise ApiConfigError(f"{name} must be between {lo} and {hi}")
    return value


def cert_path_from_env(environ: Mapping[str, str] | None = None, *, root: Path | None = None) -> Path:
    e = environ if environ is not None else os.environ
    cert = Path(e.get(CERT_VAR, "").strip() or DEFAULT_CERT)
    return cert if cert.is_absolute() else (root if root is not None else Path.cwd()) / cert


@dataclass(frozen=True)
class ApiSettings:
    secret: bytes = field(repr=False)
    port: int = DEFAULT_PORT
    bind: str = DEFAULT_BIND
    cert_path: Path = Path(DEFAULT_CERT)
    key_path: Path = Path(DEFAULT_KEY)
    redis_db: int = DEFAULT_REDIS_DB
    rate_per_min: int = DEFAULT_RATE_PER_MIN
    dsn: str = field(default="", repr=False)
    redis_url: str = field(default="", repr=False)

    @staticmethod
    def secret_from_env(environ: Mapping[str, str] | None = None) -> bytes:
        e = environ if environ is not None else os.environ
        text = e.get(SECRET_VAR, "").strip()
        if not text:
            raise ApiConfigError(f"missing environment variable {SECRET_VAR} (deploy/local/new-api-credentials.ps1)")
        if secret_strength(text) < MIN_SECRET_BYTES:
            raise ApiConfigError(f"{SECRET_VAR} must be hex or base64 of at least {MIN_SECRET_BYTES} random bytes")
        return text.encode("utf-8")

    @staticmethod
    def from_env(environ: Mapping[str, str] | None = None, *, root: Path | None = None,
                 need_storage: bool = True) -> ApiSettings:
        e = environ if environ is not None else os.environ
        base = root if root is not None else Path.cwd()
        secret = ApiSettings.secret_from_env(e)
        bind = e.get(BIND_VAR, "").strip() or DEFAULT_BIND
        try:
            ipaddress.ip_address(bind)
        except ValueError:
            raise ApiConfigError(f"{BIND_VAR} must be an IP address") from None
        key = Path(e.get(KEY_VAR, "").strip() or DEFAULT_KEY)
        redis_db = _int(e, REDIS_DB_VAR, DEFAULT_REDIS_DB, 1, 15)
        dsn = redis_url = ""
        if need_storage:
            leaked = forbidden_env_names(e)
            if leaked:
                raise ApiConfigError("refusing to start: the network-facing API must not hold broker credentials, "
                                     "the engine's storage settings or its other settings and secrets; remove "
                                     f"{', '.join(leaked)} from its environment "
                                     "(deploy/local/start-xtnl.ps1 does)")
            dsn = e.get(DSN_VAR, "").strip()
            if not dsn:
                raise ApiConfigError(f"missing environment variable {DSN_VAR} (role xtnl_api; "
                                     "deploy/local/new-api-credentials.ps1 -DbRole)")
            if urlsplit(dsn).scheme not in ("postgres", "postgresql"):
                raise ApiConfigError(f"{DSN_VAR} must be a postgresql:// URL")
            raw_redis = e.get(REDIS_URL_VAR, "").strip() or DEFAULT_API_REDIS_URL
            if urlsplit(raw_redis).scheme not in ("redis", "rediss", "unix"):
                raise ApiConfigError(f"{REDIS_URL_VAR} must be a redis://, rediss:// or unix:// URL")
            redis_url = redis_url_with_db(raw_redis, redis_db)
        return ApiSettings(
            secret=secret,
            port=_int(e, PORT_VAR, DEFAULT_PORT, 1, 65535),
            bind=bind,
            cert_path=cert_path_from_env(e, root=base),
            key_path=key if key.is_absolute() else base / key,
            redis_db=redis_db,
            rate_per_min=_int(e, RATE_VAR, DEFAULT_RATE_PER_MIN, 1, 600),
            dsn=dsn,
            redis_url=redis_url,
        )

    def describe(self) -> str:
        """Safe one-line summary for the start-up log: no secret, URLs redacted."""
        return (f"bind={self.bind} port={self.port} cert={self.cert_path} redis_db={self.redis_db} "
                f"rate_per_min={self.rate_per_min} dsn={redact(self.dsn) if self.dsn else '-'} "
                f"redis={redact(self.redis_url) if self.redis_url else '-'}")
