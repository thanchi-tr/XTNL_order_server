"""Receiver settings from environment variables (XTNL_RECEIVER_*; values are never logged).

Service:
* XTNL_RECEIVER_LISTEN       host:port to listen on (default 127.0.0.1:8443; the VPN address on the host).
* XTNL_RECEIVER_TOKEN        the bearer token the engine sends (its XTNL_DEADMAN_TOKEN); required, >= 16 chars.
* XTNL_RECEIVER_TOKEN_NEXT   optional next token, accepted beside it (rotation; src/deadman_receiver/tokens.py).
* XTNL_RECEIVER_TOKEN_OVERLAP_S  how long a replaced token stays accepted (default 1800, 600..86400).
* XTNL_RECEIVER_TLS_CERT / XTNL_RECEIVER_TLS_KEY   PEM files; required unless the listen host is loopback
                             (then a TLS-terminating proxy on the same host fronts it).
* XTNL_RECEIVER_PATH         the heartbeat path (default /xtnl/heartbeat, the path of XTNL_DEADMAN_URL).
* XTNL_RECEIVER_TEST_PAGE_UTC  HH:MM of the daily test page (default 22:00 UTC = morning in Melbourne).
* XTNL_RECEIVER_REPAGE_S     re-page interval while a condition stays active (default 900, 60..86400).
* XTNL_RECEIVER_CERTS        K5: name=path list of the certificates on this Melbourne host the receiver watches
                             (e.g. dashboard-redis-client=/etc/xtnl/dashboard/redis-client.crt): each pages
                             `cert:<name>` when unreadable or within 30 days of expiry. Its own TLS certificate is
                             always watched (`deadman-receiver`).

Rotation without restart (K10): with aws-sm, the bearer tokens (every tick) and the paging channel credentials
(every `CHANNEL_REFRESH_S`) follow the cached secret; the TLS certificate / key files are re-loaded in place when
they change (checked every `CERT_CHECK_S`). The listen address, path and channel kinds still need a restart.

Secrets source (operations.md §13 "rotation without restart"; IMPLEMENTATION_PLAN 8.2): XTNL_SECRETS_SOURCE
= env (default) | aws-sm with XTNL_SECRETS_ID / XTNL_SECRETS_REGION (`src.secrets.source`). With aws-sm the secret's
values are laid over the env file at start, and the tokens are re-read from the cached secret (refreshed every
5 min, and soon after a request with an unknown token) on every tick, so a rotation needs no restart. The secret's
region must be outside eu-west-2 (like the paging channel: a London outage must not stop the receiver).

Paging channel (operations.md §9: **outside eu-west-2**): the existing `src.notifications` transports, configured
with the engine's variable names prefixed `XTNL_RECEIVER_` (XTNL_RECEIVER_SES_REGION, XTNL_RECEIVER_SMTP_HOST,
XTNL_RECEIVER_PUSH_PROVIDER, XTNL_RECEIVER_ALERT_EMAIL_TO, ...: the full list is `CHANNEL_VARS`). A channel that
names eu-west-2 (SES region or endpoint, an SMTP host, a push URL) is refused: a London region outage must not
silence the page that reports it. At least one channel must remain, else the configuration is refused (exit 78).
"""
from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Final

from src.deadman_receiver.tokens import (
    TOKEN_VAR,
    TokenError,
    overlap_s,
    token_set,
)
from src.notifications.settings import NotificationSettings, PushSettings, SesSettings, SmtpSettings, is_loopback
from src.observability.certs import parse_name_list
from src.secrets.source import REGION_VAR, SOURCE_VAR, SecretsError, SecretsSource, secrets_source_from_env

PREFIX: Final = "XTNL_RECEIVER_"
LISTEN_VAR: Final = "XTNL_RECEIVER_LISTEN"
TLS_CERT_VAR: Final = "XTNL_RECEIVER_TLS_CERT"
TLS_KEY_VAR: Final = "XTNL_RECEIVER_TLS_KEY"
PATH_VAR: Final = "XTNL_RECEIVER_PATH"
TEST_PAGE_VAR: Final = "XTNL_RECEIVER_TEST_PAGE_UTC"
REPAGE_VAR: Final = "XTNL_RECEIVER_REPAGE_S"
CERTS_VAR: Final = "XTNL_RECEIVER_CERTS"
#: The channel variables: XTNL_RECEIVER_<X> is read as the engine's XTNL_<X> by src.notifications.settings.
CHANNEL_VARS: Final = (
    "XTNL_RECEIVER_ALERT_EMAIL_TO",
    "XTNL_RECEIVER_SES_REGION", "XTNL_RECEIVER_SES_FROM", "XTNL_RECEIVER_SES_ACCESS_KEY_ID",
    "XTNL_RECEIVER_SES_SECRET_ACCESS_KEY", "XTNL_RECEIVER_SES_SESSION_TOKEN", "XTNL_RECEIVER_SES_ENDPOINT",
    "XTNL_RECEIVER_SES_CONFIGURATION_SET", "XTNL_RECEIVER_SES_TIMEOUT_S",
    "XTNL_RECEIVER_SMTP_HOST", "XTNL_RECEIVER_SMTP_PORT", "XTNL_RECEIVER_SMTP_FROM", "XTNL_RECEIVER_SMTP_SECURITY",
    "XTNL_RECEIVER_SMTP_USERNAME", "XTNL_RECEIVER_SMTP_PASSWORD", "XTNL_RECEIVER_SMTP_TIMEOUT_S",
    "XTNL_RECEIVER_PUSH_PROVIDER", "XTNL_RECEIVER_PUSH_URL", "XTNL_RECEIVER_PUSH_TOKEN", "XTNL_RECEIVER_PUSH_USER",
    "XTNL_RECEIVER_PUSH_TIMEOUT_S",
)
FORBIDDEN_REGION: Final = "eu-west-2"
DEFAULT_LISTEN: Final = "127.0.0.1:8443"
DEFAULT_PATH: Final = "/xtnl/heartbeat"
DEFAULT_TEST_PAGE: Final = (22, 0)
DEFAULT_REPAGE_S: Final = 900
MIN_TOKEN_CHARS: Final = 16
_PATH: Final = re.compile(r"^/[A-Za-z0-9._~/-]{0,200}$")


class ReceiverConfigError(ValueError):
    """The configuration is refused (the message names variables only, never values)."""


@dataclass(frozen=True)
class ReceiverSettings:
    host: str
    port: int
    token: str = field(repr=False)
    channels: NotificationSettings = field(repr=False)
    tls_cert: str | None = None
    tls_key: str | None = None
    path: str = DEFAULT_PATH
    test_page_utc: tuple[int, int] = DEFAULT_TEST_PAGE
    repage_s: int = DEFAULT_REPAGE_S
    issues: tuple[str, ...] = ()
    token_next: str | None = field(default=None, repr=False)
    token_overlap_s: int = 1_800
    certs: tuple[tuple[str, str], ...] = ()     # K5: (name, path) of the watched Melbourne certificates

    @property
    def tls(self) -> bool:
        return self.tls_cert is not None

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> ReceiverSettings:
        e = os.environ if env is None else env
        errors: list[str] = []
        host, port = _listen(_get(e, LISTEN_VAR) or DEFAULT_LISTEN, errors)
        token: str | None = None
        token_next: str | None = None
        try:
            tokens = token_set(e)
            token, token_next = tokens.current, tokens.next
        except TokenError as exc:
            errors.append(str(exc))
        try:
            overlap = overlap_s(e)
        except TokenError as exc:
            errors.append(str(exc))
            overlap = 1_800
        cert, key = _get(e, TLS_CERT_VAR), _get(e, TLS_KEY_VAR)
        if (cert is None) != (key is None):
            errors.append(f"{TLS_CERT_VAR} and {TLS_KEY_VAR} must be set together")
        elif cert is None and not is_loopback(host):
            errors.append(f"{TLS_CERT_VAR} / {TLS_KEY_VAR} are required unless {LISTEN_VAR} is a loopback address")
        path = _get(e, PATH_VAR) or DEFAULT_PATH
        if not _PATH.match(path) or path == "/healthz":
            errors.append(f"{PATH_VAR} must be an absolute URL path (not /healthz)")
        test_page = _hhmm(_get(e, TEST_PAGE_VAR), errors)
        repage = _int(_get(e, REPAGE_VAR), DEFAULT_REPAGE_S, 60, 86_400, REPAGE_VAR, errors)
        channels = channel_settings(e)
        if channels.email is None and channels.push is None:
            errors.append("no paging channel outside eu-west-2 is configured (XTNL_RECEIVER_SES_* / "
                          "XTNL_RECEIVER_SMTP_* / XTNL_RECEIVER_PUSH_*)")
        certs = dict(parse_name_list(_get(e, CERTS_VAR)))
        if cert is not None:
            certs.setdefault("deadman-receiver", cert)
        if errors:
            raise ReceiverConfigError("; ".join(errors))
        assert token is not None
        return cls(host, port, token, channels, cert, key, path, test_page, repage, channels.issues, token_next,
                   overlap, tuple(sorted(certs.items())))


def receiver_secrets_source(env: Mapping[str, str] | None = None) -> SecretsSource:
    """The receiver's secrets source (connects nothing). ReceiverConfigError for a bad selection or a secret in
    eu-west-2."""
    e = os.environ if env is None else env
    region = (e.get(REGION_VAR) or "").strip().lower()
    if (e.get(SOURCE_VAR) or "env").strip().lower() == "aws-sm" and region == FORBIDDEN_REGION:
        raise ReceiverConfigError(f"{REGION_VAR} must be outside {FORBIDDEN_REGION} for the receiver")
    try:
        return secrets_source_from_env(e)
    except SecretsError as exc:
        raise ReceiverConfigError(str(exc)) from None


def _get(e: Mapping[str, str], name: str) -> str | None:
    v = e.get(name)
    return v.strip() if v is not None and v.strip() else None


def _listen(raw: str, errors: list[str]) -> tuple[str, int]:
    host, sep, port_s = raw.rpartition(":")
    host = host.strip("[]")
    try:
        port = int(port_s)
    except ValueError:
        port = 0
    if not sep or not host or not 0 < port < 65536:
        errors.append(f"{LISTEN_VAR} must be host:port")
        return "127.0.0.1", 0
    return host, port


def _hhmm(raw: str | None, errors: list[str]) -> tuple[int, int]:
    if raw is None:
        return DEFAULT_TEST_PAGE
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", raw)
    if m is None or int(m.group(1)) > 23 or int(m.group(2)) > 59:
        errors.append(f"{TEST_PAGE_VAR} must be HH:MM (UTC)")
        return DEFAULT_TEST_PAGE
    return int(m.group(1)), int(m.group(2))


def _int(raw: str | None, default: int, lo: int, hi: int, name: str, errors: list[str]) -> int:
    if raw is None:
        return default
    try:
        v = int(raw)
    except ValueError:
        v = lo - 1
    if not lo <= v <= hi:
        errors.append(f"{name} must be an integer in [{lo}, {hi}]")
        return default
    return v


def _names_region(text: str | None) -> bool:
    return text is not None and FORBIDDEN_REGION in text.lower()


def channel_settings(env: Mapping[str, str]) -> NotificationSettings:
    """The notification settings read from the XTNL_RECEIVER_* channel variables, with every channel that names
    eu-west-2 removed (and reported in `issues`)."""
    mapped = {"XTNL_" + name[len(PREFIX):]: env[name] for name in CHANNEL_VARS if name in env}
    s = NotificationSettings.from_env(mapped)
    issues = [i.replace("XTNL_", PREFIX) for i in s.issues]
    email: SmtpSettings | SesSettings | None = s.email
    if isinstance(email, SesSettings) and (email.region.lower() == FORBIDDEN_REGION or _names_region(email.endpoint)):
        issues.append(f"email disabled: XTNL_RECEIVER_SES_* must page from outside {FORBIDDEN_REGION}")
        email = None
    elif isinstance(email, SmtpSettings) and _names_region(email.host):
        issues.append(f"email disabled: XTNL_RECEIVER_SMTP_HOST must be outside {FORBIDDEN_REGION}")
        email = None
    push: PushSettings | None = s.push
    if push is not None and _names_region(push.host):
        issues.append(f"mobile push disabled: XTNL_RECEIVER_PUSH_URL must be outside {FORBIDDEN_REGION}")
        push = None
    return NotificationSettings(email, push, tuple(issues))
