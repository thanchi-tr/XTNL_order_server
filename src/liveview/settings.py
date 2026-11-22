"""Live view settings from the environment (docs/design/live-view.md s4.1). The only module of src/liveview that
reads `os.environ`. Nothing here reads .env files; the DSN is never printed or logged.

    XTNL_LIVEVIEW_DSN             required: PostgreSQL as role xtnl_liveview (migration 0021), set in the Windows
                                  User environment by deploy/local/new-liveview-credentials.ps1
    XTNL_LIVEVIEW_ADDR            listen address, a literal loopback address (default 127.0.0.1:8090)
    XTNL_LIVEVIEW_POLL_MS         poll interval, 250..10000 (default 1000)
    XTNL_LIVEVIEW_HISTORY         ring size in events, 200..20000 (default 2000)
    XTNL_LIVEVIEW_BACKFILL_H      hours shown at start, 1..168 (default 24)
    XTNL_LIVEVIEW_MAX_CLIENTS     concurrent streams, 1..16 (default 4)
    XTNL_LIVEVIEW_ENGINE_METRICS  the engine exporter, literal loopback (default 127.0.0.1:9464), or `off`

Least privilege: the live view refuses to start (exit 78, naming the variable, never a value) while its environment
holds a broker credential, the engine's storage, an alert channel, an API / market-data / ARM setting, the heartbeat
copy URL, a backup credential (XTNL_BACKUP_*, RCLONE_*, B2_*) or any other XTNL secret; deploy/local/start-xtnl.ps1 removes them from the child's environment ($LiveViewDropEnv).
"""
from __future__ import annotations

import ipaddress
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

DSN_VAR: Final = "XTNL_LIVEVIEW_DSN"
ADDR_VAR: Final = "XTNL_LIVEVIEW_ADDR"
POLL_VAR: Final = "XTNL_LIVEVIEW_POLL_MS"
HISTORY_VAR: Final = "XTNL_LIVEVIEW_HISTORY"
BACKFILL_VAR: Final = "XTNL_LIVEVIEW_BACKFILL_H"
CLIENTS_VAR: Final = "XTNL_LIVEVIEW_MAX_CLIENTS"
METRICS_VAR: Final = "XTNL_LIVEVIEW_ENGINE_METRICS"

DEFAULT_ADDR: Final = "127.0.0.1:8090"
DEFAULT_METRICS: Final = "127.0.0.1:9464"

#: Refused name prefixes (each ends in `_`, so no entry looks like a variable name to the deploy scanner). The last
#: three are the backup credentials of the separate backup Windows account (docs/design/home-server-plan.md s3.2);
#: none of them can match XTNL_LIVEVIEW_DSN.
REFUSED_PREFIXES: Final = ("STONE_X_", "CTRADER_", "XTNL_MD_", "XTNL_ARM_", "XTNL_SMTP_", "XTNL_SES_", "XTNL_PUSH_",
                           "XTNL_API_", "XTNL_BACKUP_", "RCLONE_", "B2_")
#: Refused names: a set literal (never a tuple), like src/api/settings.py FORBIDDEN_NAMES. XTNL_HEARTBEAT_COPY_URL
#: is a capability URL (home server: a Healthchecks.io ping URL) that REFUSED_PATTERN does not catch.
REFUSED_NAMES: Final = frozenset({"ORDER_SERVER_DSN", "REDIS_URL", "BROKER_ENV", "XTNL_DEADMAN_TOKEN",
                                  "XTNL_HEARTBEAT_COPY_TOKEN", "XTNL_DASHBOARD_DSN", "XTNL_HEARTBEAT_COPY_URL"})
#: Any other XTNL secret-looking name, except the live view's own DSN.
REFUSED_PATTERN: Final = re.compile(r"^XTNL_\w*(PASS|SECRET|TOKEN|DSN|KEY)")


class LiveViewConfigError(ValueError):
    """A setting is missing, invalid or refused. The message names the variable, never its value."""


def refused_names(environ: Mapping[str, str]) -> list[str]:
    """Names in `environ` the live view must not hold (sorted; names only)."""
    out: list[str] = []
    for name in environ:
        upper = name.upper()
        if upper == DSN_VAR:
            continue
        if upper in REFUSED_NAMES or upper.startswith(REFUSED_PREFIXES) or REFUSED_PATTERN.match(upper):
            out.append(name)
    return sorted(out)


def _bounded(environ: Mapping[str, str], var: str, default: int, low: int, high: int) -> int:
    raw = (environ.get(var) or "").strip()
    if not raw:
        return default
    if not re.fullmatch(r"[0-9]{1,6}", raw):
        raise LiveViewConfigError(f"{var} must be a whole number from {low} to {high}")
    value = int(raw)
    if not low <= value <= high:
        raise LiveViewConfigError(f"{var} must be from {low} to {high}")
    return value


def parse_loopback(var: str, raw: str) -> tuple[str, int]:
    """`host:port` (or `[::1]:port`) where host is a literal loopback address; a name, `localhost` included, is
    refused so a hosts-file entry can never move the address off loopback."""
    m = re.fullmatch(r"\[([0-9A-Fa-f:]+)\]:([0-9]{1,5})|([0-9.]+):([0-9]{1,5})", raw.strip())
    if not m:
        raise LiveViewConfigError(f"{var} must be a literal loopback address and port, e.g. 127.0.0.1:8090")
    host = m.group(1) or m.group(3) or ""
    port = int(m.group(2) or m.group(4) or "0")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        raise LiveViewConfigError(f"{var} must be a literal loopback address and port") from None
    if not ip.is_loopback:
        raise LiveViewConfigError(f"{var} must be a loopback address (127.0.0.0/8 or [::1])")
    if not 1 <= port <= 65535:
        raise LiveViewConfigError(f"{var} port must be from 1 to 65535")
    return str(ip), port


@dataclass(frozen=True)
class LiveViewSettings:
    dsn: str
    host: str
    port: int
    poll_ms: int = 1000
    history: int = 2000
    backfill_h: int = 24
    max_clients: int = 4
    metrics: tuple[str, int] | None = None

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> LiveViewSettings:
        env = os.environ if environ is None else environ
        refused = refused_names(env)
        if refused:
            raise LiveViewConfigError("refusing to start: the environment holds " + ", ".join(refused)
                                      + " (the live view must hold nothing of the engine's; start it through "
                                        "deploy/local/start-xtnl.ps1)")
        dsn = (env.get(DSN_VAR) or "").strip()
        if not dsn:
            raise LiveViewConfigError(f"{DSN_VAR} is not set (run deploy/local/new-liveview-credentials.ps1)")
        if not dsn.startswith(("postgresql://", "postgres://")):
            raise LiveViewConfigError(f"{DSN_VAR} must be a postgresql:// address")
        host, port = parse_loopback(ADDR_VAR, env.get(ADDR_VAR) or DEFAULT_ADDR)
        raw_metrics = (env.get(METRICS_VAR) or DEFAULT_METRICS).strip()
        metrics = None if raw_metrics.lower() == "off" else parse_loopback(METRICS_VAR, raw_metrics)
        return cls(dsn=dsn, host=host, port=port,
                   poll_ms=_bounded(env, POLL_VAR, 1000, 250, 10000),
                   history=_bounded(env, HISTORY_VAR, 2000, 200, 20000),
                   backfill_h=_bounded(env, BACKFILL_VAR, 24, 1, 168),
                   max_clients=_bounded(env, CLIENTS_VAR, 4, 1, 16),
                   metrics=metrics)

    def describe(self) -> str:
        probe = "off" if self.metrics is None else f"{self.metrics[0]}:{self.metrics[1]}"
        return (f"addr={self.host}:{self.port} poll_ms={self.poll_ms} history={self.history} "
                f"backfill_h={self.backfill_h} max_clients={self.max_clients} engine_metrics={probe}")


__all__ = ["ADDR_VAR", "BACKFILL_VAR", "CLIENTS_VAR", "DSN_VAR", "HISTORY_VAR", "LiveViewConfigError",
           "LiveViewSettings", "METRICS_VAR", "POLL_VAR", "REFUSED_NAMES", "REFUSED_PATTERN", "REFUSED_PREFIXES",
           "parse_loopback", "refused_names"]
