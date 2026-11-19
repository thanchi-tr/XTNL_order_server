"""Where credentials come from (operations.md §13; oversee-rules.md §10): the process environment, or one
AWS Secrets Manager secret mapped onto the same variable names.

    XTNL_SECRETS_SOURCE = env (default) | aws-sm
    XTNL_SECRETS_ID     = the secret's name or ARN          (aws-sm only)
    XTNL_SECRETS_REGION = its region, e.g. eu-west-2        (aws-sm only)

The secret's `SecretString` is a JSON object whose keys are environment-variable names
(`{"STONE_X_PASS": "...", "XTNL_SMTP_PASSWORD": "...", ...}`). `resolve(base)` returns `base` (the
process environment) with the secret's values laid over it, so `StoneXSettings.from_env`,
`PepperstoneSettings.from_env`, `NotificationSettings.from_env`, the Patroni REST credentials and the
storage DSN all read through one mapping and keep their variable names.

Keys that steer the process rather than authenticate it (`PROTECTED_KEYS`: BROKER_ENV, the mock switch,
the node id, the adapter list, the secrets selector itself) are never taken from the secret, so the
secret can never change the environment gate (adapter-isolation.md §5); they are ignored with a warning
naming the key.

Rotation without restart: `AwsSecretsManager` caches the secret, a background thread re-reads it every
`refresh_s` (and at once after `invalidate()`, e.g. on a broker credential error), and an AWS
authentication error refreshes the instance-role credentials and retries once (src/secrets/aws.py). The
current `version_id` is the Secrets Manager part of the credential generation (adapter_auth_state).

No value is ever logged, printed or put into an exception message: only variable names.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from collections.abc import Callable, Mapping
from typing import Final, Literal, Protocol

import httpx

from src.secrets.aws import AwsError, AwsJsonClient, CredentialProvider, Imdsv2Credentials

log = logging.getLogger(__name__)

SOURCE_VAR: Final = "XTNL_SECRETS_SOURCE"
SECRET_ID_VAR: Final = "XTNL_SECRETS_ID"
REGION_VAR: Final = "XTNL_SECRETS_REGION"
SourceKind = Literal["env", "aws-sm"]
SOURCES: Final[tuple[SourceKind, ...]] = ("env", "aws-sm")
PROTECTED_KEYS: Final = frozenset({
    "BROKER_ENV", "XTNL_USE_MOCK_BROKER", "XTNL_NODE_ID", "XTNL_ADAPTERS", SOURCE_VAR, SECRET_ID_VAR, REGION_VAR,
})
_ENV_NAME: Final = re.compile(r"^[A-Z][A-Z0-9_]*$")
DEFAULT_REFRESH_S: Final = 300.0


class SecretsError(RuntimeError):
    """The secrets source is misconfigured or could not be read (names only, never values)."""


class SecretsUnavailable(SecretsError):
    """The secret could not be read just now (an AWS, IMDS or network error): retrying may succeed, unlike the
    configuration errors above (a missing variable, a malformed secret). Names only, never values."""


class SecretsSource(Protocol):
    @property
    def kind(self) -> SourceKind: ...

    @property
    def version_id(self) -> str | None:
        """The secret's current version (credential generation), None for the environment."""
        ...

    def resolve(self, base: Mapping[str, str]) -> dict[str, str]:
        """`base` with the secret's values laid over it. May block on first use (start-up only)."""
        ...

    def resolve_cached(self, base: Mapping[str, str]) -> dict[str, str]:
        """Like `resolve` but never does I/O: uses the last value read (safe on the event loop)."""
        ...

    def invalidate(self) -> None:
        """Re-read the secret soon (the refresher thread wakes up); never blocks."""
        ...

    def start(self) -> None:
        """Start periodic refresh (no-op for the environment)."""
        ...

    def close(self) -> None: ...


class EnvSecrets:
    """The process environment as it is (the behaviour before 4.7)."""

    kind: SourceKind = "env"
    version_id: str | None = None

    def resolve(self, base: Mapping[str, str]) -> dict[str, str]:
        return dict(base)

    def resolve_cached(self, base: Mapping[str, str]) -> dict[str, str]:
        return dict(base)

    def invalidate(self) -> None:
        return None

    def start(self) -> None:
        return None

    def close(self) -> None:
        return None

    def __repr__(self) -> str:
        return "EnvSecrets()"


def parse_secret_string(text: str) -> dict[str, str]:
    """SecretString JSON object -> {VARIABLE: value}. Numbers / booleans become strings; anything else is
    refused, naming the key. Keys that are not environment-variable names are refused too."""
    try:
        obj = json.loads(text)
    except ValueError:
        raise SecretsError("secret is not a JSON object of environment variables") from None
    if not isinstance(obj, dict):
        raise SecretsError("secret is not a JSON object of environment variables")
    out: dict[str, str] = {}
    for k, v in obj.items():
        if not isinstance(k, str) or not _ENV_NAME.match(k):
            raise SecretsError(f"secret key {str(k)[:64]!r} is not an environment variable name")
        if isinstance(v, bool):
            out[k] = "1" if v else "0"
        elif isinstance(v, (str, int, float)):
            out[k] = str(v)
        else:
            raise SecretsError(f"secret key {k} must hold a string")
    return out


def overlay(base: Mapping[str, str], values: Mapping[str, str]) -> tuple[dict[str, str], list[str]]:
    """(base with `values` laid over it, the protected keys that were ignored)."""
    out = dict(base)
    ignored: list[str] = []
    for k, v in values.items():
        if k in PROTECTED_KEYS:
            ignored.append(k)
            continue
        out[k] = v
    return out, sorted(ignored)


class AwsSecretsManager:
    """One Secrets Manager secret (GetSecretValue, AWSCURRENT) over httpx + SigV4, cached and refreshed."""

    kind: SourceKind = "aws-sm"

    def __init__(self, secret_id: str, region: str, *, credentials: CredentialProvider,
                 http: httpx.Client | None = None, endpoint: str | None = None, refresh_s: float = DEFAULT_REFRESH_S,
                 timeout_s: float = 5.0, monotonic: Callable[[], float] = time.monotonic) -> None:
        if not secret_id:
            raise SecretsError(f"{SECRET_ID_VAR} is required when {SOURCE_VAR}=aws-sm")
        try:
            self._client = AwsJsonClient("secretsmanager", region, "secretsmanager", credentials, http=http,
                                         endpoint=endpoint, timeout_s=timeout_s)
        except ValueError:
            raise SecretsError(f"{REGION_VAR} is not a region name") from None
        self._secret_id = secret_id
        self._refresh_s = refresh_s
        self._mono = monotonic
        self._lock = threading.Lock()
        self._values: dict[str, str] | None = None
        self._version: str | None = None
        self._read_at: float | None = None
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.reads = 0
        self._warned: set[str] = set()

    def __repr__(self) -> str:
        return f"AwsSecretsManager(region={self._client.region!r}, version_id={self._version!r})"

    @property
    def version_id(self) -> str | None:
        with self._lock:
            return self._version

    # --- reading -----------------------------------------------------------------------------------
    def refresh(self) -> bool:
        """Read the secret now (blocking). True when the version changed. Raises SecretsError."""
        try:
            reply = self._client.call("GetSecretValue", {"SecretId": self._secret_id})
        except AwsError as exc:
            raise SecretsUnavailable(f"cannot read the secret: {exc}") from None
        text = reply.get("SecretString")
        if not isinstance(text, str):
            raise SecretsError("secret has no SecretString (binary secrets are not supported)")
        values = parse_secret_string(text)
        version = reply.get("VersionId")
        with self._lock:
            changed = self._version is not None and version != self._version
            self._values = values
            self._version = str(version) if version is not None else None
            self._read_at = self._mono()
            self.reads += 1
        if changed:
            log.warning("secrets: Secrets Manager secret rotated (new version id)")
        return changed

    def _current(self, *, allow_io: bool) -> dict[str, str]:
        with self._lock:
            values, read_at = self._values, self._read_at
        stale = read_at is None or self._mono() - read_at >= self._refresh_s
        if allow_io and (values is None or stale):
            self.refresh()
            with self._lock:
                values = self._values
        if values is None:
            raise SecretsError("the secret has not been read yet")
        return values

    def _overlay(self, base: Mapping[str, str], values: Mapping[str, str]) -> dict[str, str]:
        out, ignored = overlay(base, values)
        for k in ignored:
            if k not in self._warned:
                self._warned.add(k)
                log.warning("secrets: key %s in the secret is ignored (it is taken from the environment only)", k)
        return out

    def resolve(self, base: Mapping[str, str]) -> dict[str, str]:
        return self._overlay(base, self._current(allow_io=True))

    def resolve_cached(self, base: Mapping[str, str]) -> dict[str, str]:
        return self._overlay(base, self._current(allow_io=False))

    # --- background refresh ------------------------------------------------------------------------
    def invalidate(self) -> None:
        self._wake.set()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="xtnl-secrets-refresh", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(self._refresh_s)
            if self._stop.is_set():
                return
            self._wake.clear()
            try:
                self.refresh()
            except SecretsError as exc:
                log.warning("secrets: periodic refresh failed (keeping the cached secret): %s", exc)

    def close(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        self._client.close()


def secrets_source_from_env(environ: Mapping[str, str] | None = None, *,
                            credentials: CredentialProvider | None = None,
                            http: httpx.Client | None = None) -> SecretsSource:
    """Select the source from XTNL_SECRETS_SOURCE (default env). Connects nothing. Raises SecretsError."""
    e = os.environ if environ is None else environ
    kind = (e.get(SOURCE_VAR) or "env").strip().lower()
    if kind == "env":
        return EnvSecrets()
    if kind != "aws-sm":
        raise SecretsError(f"{SOURCE_VAR} must be one of {', '.join(SOURCES)}")
    secret_id = (e.get(SECRET_ID_VAR) or "").strip()
    region = (e.get(REGION_VAR) or "").strip()
    missing = [n for n, v in ((SECRET_ID_VAR, secret_id), (REGION_VAR, region)) if not v]
    if missing:
        raise SecretsError(f"{SOURCE_VAR}=aws-sm needs {', '.join(missing)}")
    return AwsSecretsManager(secret_id, region, credentials=credentials or Imdsv2Credentials(), http=http)
