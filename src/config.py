"""Process-wide configuration and the environment gate (adapter-isolation.md §5).

Credentials are read from a mapping of environment-variable names only: the process environment, or the
environment with the AWS Secrets Manager secret laid over it (src/secrets, XTNL_SECRETS_SOURCE). Nothing
here reads .env files.
"""
from __future__ import annotations

import logging
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

BrokerEnv = Literal["sandbox", "live_readonly"]
ALLOWED_BROKER_ENVS: tuple[BrokerEnv, ...] = ("sandbox", "live_readonly")

_log = logging.getLogger(__name__)
_FLAG_ON = ("1", "true", "yes", "on")
_FLAG_OFF = ("0", "false", "no", "off")


class ExecutionLockedError(RuntimeError):
    """Raised whenever something would reach an order-changing capability in Phase 1."""


# One REST token bucket per adapter (operations.md §15; src/adapters/ratelimit.py). The broker limits are
# still to be confirmed (CLAUDE.md "Known gaps"): StoneX defaults follow the skill's 5 requests/s; cTrader
# documents 50 non-historical requests/s per connection, the default stays well below it.
STONEX_REST_RATE_PER_S = 5.0
STONEX_REST_BURST = 10
CTRADER_REST_RATE_PER_S = 20.0
CTRADER_REST_BURST = 40


def _rest_limits(e: Mapping[str, str], prefix: str, rate: float, burst: int) -> tuple[float, int]:
    """``{prefix}_REST_RATE_PER_S`` / ``{prefix}_REST_BURST`` overrides; anything not positive is refused."""
    try:
        r = float(e.get(f"{prefix}_REST_RATE_PER_S") or rate)
        b = int(e.get(f"{prefix}_REST_BURST") or burst)
    except ValueError:
        raise ValueError(f"{prefix}_REST_RATE_PER_S / {prefix}_REST_BURST must be numbers") from None
    if not (r > 0 and math.isfinite(r)) or b < 1:
        raise ValueError(f"{prefix}_REST_RATE_PER_S must be finite and > 0, {prefix}_REST_BURST >= 1")
    return r, b


def _bars_cap(e: Mapping[str, str], prefix: str) -> int | None:
    """``{prefix}_REST_BARS_PER_MIN``: the per-adapter cap on ``BARS`` REST reads per minute (design PR11). None
    (unset) means the default, a quarter of the bucket's rate (``src.adapters.ratelimit.default_bars_per_min``).
    Only the optional market-data / ARM services read at ``BARS``, so a bad value never stops start-up: it logs a
    warning (the name only) and the default applies."""
    name = f"{prefix}_REST_BARS_PER_MIN"
    raw = (e.get(name) or "").strip()
    if not raw:
        return None
    try:
        cap = int(raw)
    except ValueError:
        cap = 0
    if cap < 1:
        _log.warning("%s must be a whole number >= 1; the default (a quarter of the REST rate) applies", name)
        return None
    return cap


# The BARS cap rule (design PR11) for readers outside the adapters (the ops CLI's admission check, which may not
# import src.adapters). It is the same rule as `src/adapters/ratelimit.py` default / max / effective_bars_per_min;
# tests/ops/test_arm_ops.py checks the two give the same caps.
BARS_WINDOW_S = 60.0
MAX_BARS_SHARE = 0.5
#: The REST defaults (rate per second, burst) per settings prefix.
REST_DEFAULTS: dict[str, tuple[float, int]] = {
    "STONE_X": (STONEX_REST_RATE_PER_S, STONEX_REST_BURST),
    "CTRADER": (CTRADER_REST_RATE_PER_S, CTRADER_REST_BURST),
}


def default_bars_per_min(rate_per_s: float) -> int:
    """The default ``BARS`` cap: a quarter of the bucket's rate, per minute (at least 1)."""
    return max(1, int(rate_per_s * BARS_WINDOW_S / 4))


def max_bars_per_min(rate_per_s: float) -> int:
    """The highest ``BARS`` cap allowed: half the bucket's rate, per minute (at least 1)."""
    return max(1, int(rate_per_s * BARS_WINDOW_S * MAX_BARS_SHARE))


def effective_bars_per_min(rate_per_s: float, bars_per_min: int | None) -> int:
    """The cap a bucket uses for a configured value (None: the default), clamped to :func:`max_bars_per_min`."""
    cap = default_bars_per_min(rate_per_s) if bars_per_min is None else int(bars_per_min)
    return min(cap, max_bars_per_min(rate_per_s))


def rest_bars_per_min(e: Mapping[str, str], prefix: str) -> int:
    """The ``BARS`` cap per minute the adapter with settings prefix ``prefix`` (a :data:`REST_DEFAULTS` key) uses,
    read exactly as the adapter's settings read it (``_rest_limits`` and ``_bars_cap``). A bad
    ``{prefix}_REST_RATE_PER_S`` / ``{prefix}_REST_BURST`` raises ValueError, as the adapter's start-up does."""
    rate, burst = REST_DEFAULTS[prefix]
    r, _ = _rest_limits(e, prefix, rate, burst)
    return effective_bars_per_min(r, _bars_cap(e, prefix))


def broker_env(environ: Mapping[str, str] | None = None) -> BrokerEnv:
    """Return BROKER_ENV, refusing anything but sandbox / live_readonly (default sandbox)."""
    env = (environ if environ is not None else os.environ).get("BROKER_ENV", "sandbox")
    if env == "sandbox":
        return "sandbox"
    if env == "live_readonly":
        return "live_readonly"
    raise ExecutionLockedError(f"BROKER_ENV={env!r} is refused; allowed: {ALLOWED_BROKER_ENVS}")


def env_flag(e: Mapping[str, str], name: str, default: bool = False) -> bool:
    """An on/off switch: unset or '' = ``default``; 0/false/no/off = off; 1/true/yes/on = on; anything else is
    refused (names only, never the value)."""
    raw = (e.get(name) or "").strip().lower()
    if raw == "":
        return default
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    raise ValueError(f"{name} must be 0 or 1")


def tolerant_flag(e: Mapping[str, str], name: str) -> bool:
    """An optional feature switch that must never stop start-up (design §C2 local flags): on only for
    1/true/yes/on; unset, off values and anything unreadable are off. An unreadable value logs a warning (the
    name only, never the value) and never raises."""
    raw = (e.get(name) or "").strip().lower()
    if raw and raw not in _FLAG_ON and raw not in _FLAG_OFF:
        _log.warning("%s is not an on/off value (1/true/yes/on or 0/false/no/off); treated as off", name)
    return raw in _FLAG_ON


@dataclass(frozen=True)
class StoneXSettings:
    """StoneX (CIAPI) settings. Variable names match the XTNL project so both can share secrets."""

    username: str
    password: str
    app_key: str
    base_url_v2: str   # e.g. https://ciapi.cityindex.com/v2  (Session, userAccount, margin, market)
    base_url_v1: str   # e.g. https://ciapi.cityindex.com/TradingAPI  (order/*, market/search)
    stream_url: str    # Lightstreamer, adapter set STREAMINGALL
    env: BrokerEnv = "sandbox"
    app_version: str = "1"
    rest_rate_per_s: float = STONEX_REST_RATE_PER_S
    rest_burst: int = STONEX_REST_BURST
    rest_bars_per_min: int | None = None     # STONE_X_REST_BARS_PER_MIN; None = a quarter of the rate (PR11)
    # Downtime catch-up (StoneXAdapter.fetch_history). Both off until a live read-only probe confirmed the
    # behaviour; while off every StoneX history answer is reported incomplete (fail closed):
    # STONE_X_STOP_LIMIT_HISTORY=1 reads order/stoplimitorderhistory (stop / limit orders placed and finished
    # while off); STONE_X_HISTORY_PAGING_CONFIRMED=1 trusts order/tradehistory paging across pages.
    stop_limit_history: bool = True          # confirmed live 2026-11-14 (see the comment above)
    history_paging_confirmed: bool = True
    # XTNL_STONEX_PRICE_UNFILTERED=1: setRequestedMaxFrequency("unfiltered") on PRICES subscriptions (design PR3,
    # §C2). Off by default; turned on only after the PR3 probe. Parsed tolerantly (anything but an "on" value is
    # off), so a typo never stops ingestion.
    price_unfiltered: bool = False

    @staticmethod
    def from_env(environ: Mapping[str, str] | None = None) -> "StoneXSettings":
        e: Mapping[str, str] = environ if environ is not None else os.environ
        env = broker_env(e)
        if env == "sandbox":
            host, v1, push = ("https://ciapipreprod.cityindextest9.co.uk", "TradingApi",
                              "https://pushpreprod.cityindextest9.co.uk")
        else:
            host, v1, push = ("https://ciapi.cityindex.com", "TradingAPI", "https://push.cityindex.com")
        missing = [k for k in ("STONE_X_USERNAME", "STONE_X_PASS", "STONE_X_APP_KEY") if not e.get(k)]
        if missing:
            raise KeyError(f"missing StoneX environment variables: {', '.join(missing)}")
        rate, burst = _rest_limits(e, "STONE_X", STONEX_REST_RATE_PER_S, STONEX_REST_BURST)
        return StoneXSettings(
            username=e["STONE_X_USERNAME"],
            password=e["STONE_X_PASS"],
            app_key=e["STONE_X_APP_KEY"],
            base_url_v2=e.get("STONE_X_BASE_URL", f"{host}/v2").rstrip("/"),
            base_url_v1=e.get("STONE_X_BASE_URL_DEPRECATED", f"{host}/{v1}").rstrip("/"),
            stream_url=e.get("STONE_X_STREAM_URL", push).rstrip("/"),
            env=env,
            rest_rate_per_s=rate,
            rest_burst=burst,
            rest_bars_per_min=_bars_cap(e, "STONE_X"),
            stop_limit_history=env_flag(e, "STONE_X_STOP_LIMIT_HISTORY", default=True),
            history_paging_confirmed=env_flag(e, "STONE_X_HISTORY_PAGING_CONFIRMED", default=True),
            price_unfiltered=tolerant_flag(e, "XTNL_STONEX_PRICE_UNFILTERED"),
        )

    def __repr__(self) -> str:  # never print secrets
        return (f"StoneXSettings(username={self.username!r}, env={self.env!r}, base_url_v2={self.base_url_v2!r}, "
                f"base_url_v1={self.base_url_v1!r}, stream_url={self.stream_url!r})")


CTRADER_HOSTS: dict[BrokerEnv, str] = {"sandbox": "demo.ctraderapi.com", "live_readonly": "live.ctraderapi.com"}
CTRADER_JSON_PORT = 5036   # JSON over WebSocket (5035 is the protobuf TCP port)


@dataclass(frozen=True)
class PepperstoneSettings:
    """Pepperstone via the cTrader Open API (JSON over WebSocket, 'accounts' OAuth scope only).

    The access / refresh tokens here are only the initial pair: once a TokenStore holds a saved
    pair (operations.md §13), that pair and its token_generation win.
    """

    client_id: str
    client_secret: str
    access_token: str
    refresh_token: str
    account_id: int        # ctidTraderAccountId
    host: str
    port: int = CTRADER_JSON_PORT
    env: BrokerEnv = "sandbox"
    rest_rate_per_s: float = CTRADER_REST_RATE_PER_S
    rest_burst: int = CTRADER_REST_BURST
    rest_bars_per_min: int | None = None     # CTRADER_REST_BARS_PER_MIN; None = a quarter of the rate (PR11)

    @property
    def ws_url(self) -> str:
        return f"wss://{self.host}:{self.port}"

    @staticmethod
    def from_env(environ: Mapping[str, str] | None = None) -> "PepperstoneSettings":
        e: Mapping[str, str] = environ if environ is not None else os.environ
        env = broker_env(e)
        required = ("CTRADER_CLIENT_ID", "CTRADER_CLIENT_SECRET", "CTRADER_ACCESS_TOKEN", "CTRADER_REFRESH_TOKEN",
                    "CTRADER_ACCOUNT_ID")
        missing = [k for k in required if not e.get(k)]
        if missing:
            raise KeyError(f"missing cTrader environment variables: {', '.join(missing)}")
        try:
            account_id = int(e["CTRADER_ACCOUNT_ID"].strip())
        except ValueError:
            raise ValueError("CTRADER_ACCOUNT_ID must be the numeric ctidTraderAccountId") from None
        host = (e.get("CTRADER_HOST") or CTRADER_HOSTS[env]).strip()
        for scheme in ("wss://", "ws://", "https://"):
            host = host.removeprefix(scheme)
        host = host.rstrip("/")
        port = CTRADER_JSON_PORT
        if ":" in host:
            host, _, port_text = host.rpartition(":")
            port = int(port_text)
        rate, burst = _rest_limits(e, "CTRADER", CTRADER_REST_RATE_PER_S, CTRADER_REST_BURST)
        return PepperstoneSettings(
            client_id=e["CTRADER_CLIENT_ID"],
            client_secret=e["CTRADER_CLIENT_SECRET"],
            access_token=e["CTRADER_ACCESS_TOKEN"],
            refresh_token=e["CTRADER_REFRESH_TOKEN"],
            account_id=account_id,
            host=host,
            port=port,
            env=env,
            rest_rate_per_s=rate,
            rest_burst=burst,
            rest_bars_per_min=_bars_cap(e, "CTRADER"),
        )

    def __repr__(self) -> str:  # never print secrets
        return (f"PepperstoneSettings(client_id={self.client_id[:6] + '...' if self.client_id else ''!r}, "
                f"account_id={self.account_id}, env={self.env!r}, ws_url={self.ws_url!r})")
