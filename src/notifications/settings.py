"""Channel settings from environment variables (never from files; values are never logged).

`from_env` takes any mapping: the engine passes the environment with the AWS Secrets Manager secret laid
over it (src/secrets, XTNL_SECRETS_SOURCE=aws-sm), so the channel credentials below come from Secrets
Manager under the same names (oversee-rules.md §10).

Email (recipients: XTNL_ALERT_EMAIL_TO, comma separated). The backend is chosen by which settings are
present: XTNL_SES_REGION set -> Amazon SES v2; else XTNL_SMTP_HOST set -> SMTP; else email is disabled.

* SMTP: XTNL_SMTP_HOST, XTNL_SMTP_PORT (587), XTNL_SMTP_FROM, XTNL_SMTP_SECURITY (starttls | ssl | none;
  default starttls, `none` only to a loopback relay), XTNL_SMTP_USERNAME + XTNL_SMTP_PASSWORD (optional,
  together), XTNL_SMTP_TIMEOUT_S (10).
* SES: XTNL_SES_REGION, XTNL_SES_FROM, XTNL_SES_ACCESS_KEY_ID, XTNL_SES_SECRET_ACCESS_KEY,
  XTNL_SES_SESSION_TOKEN (optional), XTNL_SES_ENDPOINT (optional https override),
  XTNL_SES_CONFIGURATION_SET (optional), XTNL_SES_TIMEOUT_S (10).

Mobile push: XTNL_PUSH_PROVIDER (ntfy | pushover), XTNL_PUSH_URL (ntfy: the topic URL; pushover:
optional, default the Pushover messages API), XTNL_PUSH_TOKEN (ntfy: optional bearer token; pushover:
the application token, required), XTNL_PUSH_USER (pushover user / group key, required),
XTNL_PUSH_TIMEOUT_S (10).

Dead-man receiver: XTNL_DEADMAN_URL (runtime config) and XTNL_DEADMAN_TOKEN (optional bearer token).

Missing or invalid settings disable that channel and add a line to `issues` (names only, no values).

**ARM channels** (design PR4, §C2; plan S29): the same settings under the prefix `XTNL_ARM_` instead of `XTNL_`
(`XTNL_ARM_SMTP_HOST`, `XTNL_ARM_SES_REGION`, `XTNL_ARM_PUSH_PROVIDER`, `XTNL_ARM_ALERT_EMAIL_TO`, ...):
`NotificationSettings.from_env(env, prefix=ARM_PREFIX)`. They feed the `arm_email` / `arm_push` channels of the
ARM relay (a separate SMTP or SES account, ntfy with a token, or Pushover). The Oversee settings (`prefix` left at
`XTNL_`) parse exactly as before, and neither prefix reads the other's variables.
"""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import ClassVar, Final, Literal
from urllib.parse import urlsplit

SmtpSecurity = Literal["starttls", "ssl", "none"]
PushProvider = Literal["ntfy", "pushover"]

OVERSEE_PREFIX: Final = "XTNL_"
ARM_PREFIX: Final = "XTNL_ARM_"
PREFIXES: Final = (OVERSEE_PREFIX, ARM_PREFIX)
#: Every channel variable, under the Oversee prefix (the ARM reads the same names under `XTNL_ARM_`). The one list:
#: `_var` refuses a name not in it, and the dead-man receiver's `CHANNEL_VARS` must cover it (its own test).
CHANNEL_VARS: Final = frozenset({
    "XTNL_ALERT_EMAIL_TO",
    "XTNL_SES_REGION", "XTNL_SES_FROM", "XTNL_SES_ACCESS_KEY_ID", "XTNL_SES_SECRET_ACCESS_KEY",
    "XTNL_SES_SESSION_TOKEN", "XTNL_SES_ENDPOINT", "XTNL_SES_CONFIGURATION_SET", "XTNL_SES_TIMEOUT_S",
    "XTNL_SMTP_HOST", "XTNL_SMTP_PORT", "XTNL_SMTP_FROM", "XTNL_SMTP_SECURITY", "XTNL_SMTP_USERNAME",
    "XTNL_SMTP_PASSWORD", "XTNL_SMTP_TIMEOUT_S",
    "XTNL_PUSH_PROVIDER", "XTNL_PUSH_URL", "XTNL_PUSH_TOKEN", "XTNL_PUSH_USER", "XTNL_PUSH_TIMEOUT_S",
})
_SUFFIXES: Final = frozenset(v[len(OVERSEE_PREFIX):] for v in CHANNEL_VARS)


def _var(prefix: str, suffix: str) -> str:
    """The variable `suffix` under `prefix` (`XTNL_` or `XTNL_ARM_`); only the names of `CHANNEL_VARS`."""
    if suffix not in _SUFFIXES:
        raise KeyError(suffix)
    return prefix + suffix
PUSHOVER_DEFAULT_URL: Final = "https://api.pushover.net/1/messages.json"
DEFAULT_TIMEOUT_S: Final = 10.0
_LOOPBACK: Final = frozenset({"localhost", "127.0.0.1", "::1"})


def is_loopback(host: str | None) -> bool:
    return host is not None and host.lower() in _LOOPBACK


def check_https_url(url: str) -> str | None:
    """None when `url` is https (or http to a loopback host, for a local relay), else the reason."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return "is not a valid URL"
    if not parts.hostname:
        return "has no host"
    if parts.scheme == "https" or (parts.scheme == "http" and is_loopback(parts.hostname)):
        return None
    return "must be https"


def url_host(url: str) -> str:
    """The host of a URL, safe to log (never the path, which may carry a topic or token)."""
    try:
        return urlsplit(url).hostname or "?"
    except ValueError:
        return "?"


def _valid_address(a: str) -> bool:
    return "@" in a and not any(c in a for c in "\r\n,;<> \t")


@dataclass(frozen=True)
class SmtpSettings:
    host: str
    port: int
    sender: str
    recipients: tuple[str, ...]
    security: SmtpSecurity = "starttls"
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    timeout_s: float = DEFAULT_TIMEOUT_S

    backend: ClassVar[str] = "smtp"


@dataclass(frozen=True)
class SesSettings:
    region: str
    sender: str
    recipients: tuple[str, ...]
    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)
    session_token: str | None = field(default=None, repr=False)
    endpoint: str | None = None
    configuration_set: str | None = None
    timeout_s: float = DEFAULT_TIMEOUT_S

    backend: ClassVar[str] = "ses"

    @property
    def url(self) -> str:
        base = self.endpoint or f"https://email.{self.region}.amazonaws.com"
        return base.rstrip("/") + "/v2/email/outbound-emails"


@dataclass(frozen=True)
class PushSettings:
    provider: PushProvider
    url: str = field(repr=False)          # a ntfy topic URL is itself a capability: never logged
    token: str | None = field(default=None, repr=False)
    user: str | None = field(default=None, repr=False)
    timeout_s: float = DEFAULT_TIMEOUT_S

    @property
    def host(self) -> str:
        return url_host(self.url)


@dataclass(frozen=True)
class DeadmanSettings:
    url: str | None = field(default=None, repr=False)
    token: str | None = field(default=None, repr=False)

    @property
    def host(self) -> str | None:
        return None if self.url is None else url_host(self.url)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> DeadmanSettings:
        e = os.environ if env is None else env
        return cls(_get(e, "XTNL_DEADMAN_URL"), _get(e, "XTNL_DEADMAN_TOKEN"))


EmailSettings = SmtpSettings | SesSettings


@dataclass(frozen=True)
class NotificationSettings:
    email: EmailSettings | None = None
    push: PushSettings | None = None
    issues: tuple[str, ...] = ()

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, *,
                 prefix: str = OVERSEE_PREFIX) -> NotificationSettings:
        """The channel settings under `prefix`: `XTNL_` (Oversee, the default) or `XTNL_ARM_` (the ARM channels)."""
        if prefix not in PREFIXES:
            raise ValueError(f"prefix must be one of {PREFIXES}")
        e = os.environ if env is None else env
        issues: list[str] = []
        email = _email(e, issues, prefix)
        push = _push(e, issues, prefix)
        return cls(email, push, tuple(issues))

    @property
    def email_enabled(self) -> bool:
        return self.email is not None

    @property
    def push_enabled(self) -> bool:
        return self.push is not None


def _get(e: Mapping[str, str], name: str) -> str | None:
    v = e.get(name)
    return v.strip() if v is not None and v.strip() else None


def _float(e: Mapping[str, str], name: str, default: float, issues: list[str]) -> float:
    raw = _get(e, name)
    if raw is None:
        return default
    try:
        v = float(raw)
    except ValueError:
        v = -1.0
    if not 0 < v <= 120:
        issues.append(f"{name} is not a number of seconds in (0, 120]; using {default}")
        return default
    return v


def _recipients(e: Mapping[str, str], issues: list[str], p: str = OVERSEE_PREFIX) -> tuple[str, ...] | None:
    raw = _get(e, _var(p, "ALERT_EMAIL_TO"))
    if raw is None:
        issues.append(f"email disabled: {p}ALERT_EMAIL_TO is not set")
        return None
    addrs = tuple(a.strip() for a in raw.split(",") if a.strip())
    if not addrs or not all(_valid_address(a) for a in addrs):
        issues.append(f"email disabled: {p}ALERT_EMAIL_TO holds an invalid address")
        return None
    return addrs


def _sender(e: Mapping[str, str], name: str, issues: list[str]) -> str | None:
    v = _get(e, name)
    if v is None or not _valid_address(v):
        issues.append(f"email disabled: {name} is {'not set' if v is None else 'not a valid address'}")
        return None
    return v


def _email(e: Mapping[str, str], issues: list[str], p: str = OVERSEE_PREFIX) -> EmailSettings | None:
    ses, smtp = _get(e, _var(p, "SES_REGION")), _get(e, _var(p, "SMTP_HOST"))
    if ses is None and smtp is None:
        issues.append(f"email disabled: neither {p}SES_REGION nor {p}SMTP_HOST is set")
        return None
    if ses is not None and smtp is not None:
        issues.append(f"both {p}SES_REGION and {p}SMTP_HOST are set: using SES, SMTP settings ignored")
    to = _recipients(e, issues, p)
    if ses is not None:
        return _ses(e, ses, to, issues, p)
    assert smtp is not None
    return _smtp(e, smtp, to, issues, p)


def _ses(e: Mapping[str, str], region: str, to: tuple[str, ...] | None,
         issues: list[str], p: str = OVERSEE_PREFIX) -> SesSettings | None:
    sender = _sender(e, _var(p, "SES_FROM"), issues)
    key, secret = _get(e, _var(p, "SES_ACCESS_KEY_ID")), _get(e, _var(p, "SES_SECRET_ACCESS_KEY"))
    if key is None or secret is None:
        issues.append(f"email disabled: {p}SES_ACCESS_KEY_ID and {p}SES_SECRET_ACCESS_KEY are required")
    endpoint = _get(e, _var(p, "SES_ENDPOINT"))
    bad = None if endpoint is None else check_https_url(endpoint)
    if bad is not None:
        issues.append(f"email disabled: {p}SES_ENDPOINT {bad}")
    if not region.replace("-", "").isalnum():
        issues.append(f"email disabled: {p}SES_REGION is not a region name")
        return None
    timeout = _float(e, _var(p, "SES_TIMEOUT_S"), DEFAULT_TIMEOUT_S, issues)
    if to is None or sender is None or key is None or secret is None or bad is not None:
        return None
    return SesSettings(region, sender, to, key, secret, _get(e, _var(p, "SES_SESSION_TOKEN")), endpoint,
                       _get(e, _var(p, "SES_CONFIGURATION_SET")), timeout)


def _smtp(e: Mapping[str, str], host: str, to: tuple[str, ...] | None,
          issues: list[str], p: str = OVERSEE_PREFIX) -> SmtpSettings | None:
    sender = _sender(e, _var(p, "SMTP_FROM"), issues)
    sec_raw = (_get(e, _var(p, "SMTP_SECURITY")) or "starttls").lower()
    ok = True
    if sec_raw not in ("starttls", "ssl", "none"):
        issues.append(f"email disabled: {p}SMTP_SECURITY must be starttls, ssl or none")
        ok = False
    security: SmtpSecurity = "ssl" if sec_raw == "ssl" else "none" if sec_raw == "none" else "starttls"
    port_raw = _get(e, _var(p, "SMTP_PORT"))
    port = 465 if security == "ssl" else 587
    if port_raw is not None:
        try:
            port = int(port_raw)
        except ValueError:
            port = 0
        if not 0 < port < 65536:
            issues.append(f"email disabled: {p}SMTP_PORT is not a port number")
            ok = False
    user, password = _get(e, _var(p, "SMTP_USERNAME")), _get(e, _var(p, "SMTP_PASSWORD"))
    if (user is None) != (password is None):
        issues.append(f"email disabled: {p}SMTP_USERNAME and {p}SMTP_PASSWORD must be set together")
        ok = False
    if security == "none" and not is_loopback(host):
        issues.append(f"email disabled: {p}SMTP_SECURITY=none is allowed only to a loopback relay")
        ok = False
    if security == "none" and user is not None:
        issues.append("email disabled: refusing to send SMTP credentials without TLS")
        ok = False
    timeout = _float(e, _var(p, "SMTP_TIMEOUT_S"), DEFAULT_TIMEOUT_S, issues)
    if not ok or to is None or sender is None:
        return None
    return SmtpSettings(host, port, sender, to, security, user, password, timeout)


def _push(e: Mapping[str, str], issues: list[str], p: str = OVERSEE_PREFIX) -> PushSettings | None:
    provider = (_get(e, _var(p, "PUSH_PROVIDER")) or "").lower()
    if not provider:
        issues.append(f"mobile push disabled: {p}PUSH_PROVIDER is not set")
        return None
    if provider not in ("ntfy", "pushover"):
        issues.append(f"mobile push disabled: {p}PUSH_PROVIDER must be ntfy or pushover")
        return None
    url = _get(e, _var(p, "PUSH_URL"))
    token, user = _get(e, _var(p, "PUSH_TOKEN")), _get(e, _var(p, "PUSH_USER"))
    timeout = _float(e, _var(p, "PUSH_TIMEOUT_S"), DEFAULT_TIMEOUT_S, issues)
    if provider == "pushover":
        url = url or PUSHOVER_DEFAULT_URL
        if token is None or user is None:
            issues.append(f"mobile push disabled: pushover needs {p}PUSH_TOKEN and {p}PUSH_USER")
            return None
    elif url is None:
        issues.append(f"mobile push disabled: ntfy needs {p}PUSH_URL (the topic URL)")
        return None
    bad = check_https_url(url)
    if bad is not None:
        issues.append(f"mobile push disabled: {p}PUSH_URL {bad}")
        return None
    return PushSettings("pushover" if provider == "pushover" else "ntfy", url, token, user, timeout)
