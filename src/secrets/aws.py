"""AWS JSON-protocol calls over httpx with SigV4 (operations.md §13, §16), shared by Secrets Manager and KMS.

* Credentials come from the EC2 instance role through IMDSv2 (`Imdsv2Credentials`): no static AWS keys
  in the environment (IAM least privilege, §16). They are cached until 5 min before their expiry.
* `AwsJsonClient.call(action, body)` signs one `POST /` with `src.notifications.sigv4` and returns the
  JSON reply. On an authentication error (expired / unrecognised credentials, bad signature) it fetches
  fresh credentials and retries once.

Everything is **synchronous** (one `httpx.Client`): async callers run it in `asyncio.to_thread`
(operations.md §3, blocking SDK calls off the event loop). Errors carry the service, the action, the
HTTP status and the AWS error type only: never a credential, a request body or a reply body.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, Protocol

import httpx

from src.notifications.sigv4 import sign_headers

log = logging.getLogger(__name__)

IMDS_BASE: Final = "http://169.254.169.254"
IMDS_TOKEN_TTL_S: Final = 21_600
#: AWS error types that mean "these credentials are stale or wrong": refresh them and retry once.
AUTH_ERROR_TYPES: Final = frozenset({
    "ExpiredTokenException", "ExpiredToken", "UnrecognizedClientException", "InvalidSignatureException",
    "IncompleteSignature", "InvalidClientTokenId", "RequestExpired",
})


class AwsError(RuntimeError):
    """An AWS call failed. The message names the service, action, status and error type only."""

    def __init__(self, service: str, action: str, error_type: str, status: int | None = None) -> None:
        self.service = service
        self.action = action
        self.error_type = error_type
        self.status = status
        where = f"HTTP {status} " if status is not None else ""
        super().__init__(f"{service} {action} failed: {where}{error_type}")

    @property
    def is_auth_error(self) -> bool:
        return self.error_type in AUTH_ERROR_TYPES


@dataclass(frozen=True)
class AwsCredentials:
    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)
    session_token: str | None = field(default=None, repr=False)
    expires_at: float | None = None          # epoch seconds; None = does not expire


class CredentialProvider(Protocol):
    def get(self, *, force_refresh: bool = False) -> AwsCredentials: ...


class StaticCredentials:
    """Fixed credentials (tests, or an injected role session). `force_refresh` is a no-op."""

    def __init__(self, creds: AwsCredentials) -> None:
        self._creds = creds

    def get(self, *, force_refresh: bool = False) -> AwsCredentials:
        return self._creds

    def __repr__(self) -> str:
        return "StaticCredentials()"


def _parse_expiry(text: object) -> float | None:
    if not isinstance(text, str) or not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


class Imdsv2Credentials:
    """The instance role's temporary credentials from the EC2 metadata service (IMDSv2, token first)."""

    def __init__(self, *, http: httpx.Client | None = None, base_url: str = IMDS_BASE, timeout_s: float = 2.0,
                 refresh_margin_s: float = 300.0, clock: Callable[[], float] = time.time) -> None:
        self._http = http
        self._owned: httpx.Client | None = None
        self._base = base_url.rstrip("/")
        self._timeout = timeout_s
        self._margin = refresh_margin_s
        self._clock = clock
        self._lock = threading.Lock()
        self._cached: AwsCredentials | None = None
        self.fetches = 0

    def __repr__(self) -> str:
        return "Imdsv2Credentials()"

    def _client(self) -> httpx.Client:
        if self._http is not None:
            return self._http
        if self._owned is None:
            self._owned = httpx.Client(timeout=self._timeout)
        return self._owned

    def close(self) -> None:
        if self._owned is not None:
            self._owned.close()
            self._owned = None

    def _fresh(self, creds: AwsCredentials) -> bool:
        return creds.expires_at is None or creds.expires_at - self._margin > self._clock()

    def get(self, *, force_refresh: bool = False) -> AwsCredentials:
        with self._lock:
            if self._cached is not None and not force_refresh and self._fresh(self._cached):
                return self._cached
            self._cached = self._fetch()
            return self._cached

    def _fetch(self) -> AwsCredentials:
        http = self._client()
        try:
            tok = http.put(f"{self._base}/latest/api/token", timeout=self._timeout,
                           headers={"X-aws-ec2-metadata-token-ttl-seconds": str(IMDS_TOKEN_TTL_S)})
            if tok.status_code != 200:
                raise AwsError("imds", "token", "MetadataTokenRefused", tok.status_code)
            hdr = {"X-aws-ec2-metadata-token": tok.text.strip()}
            path = f"{self._base}/latest/meta-data/iam/security-credentials/"
            roles = http.get(path, headers=hdr, timeout=self._timeout)
            if roles.status_code != 200 or not roles.text.strip():
                raise AwsError("imds", "role", "NoInstanceRole", roles.status_code)
            role = roles.text.strip().splitlines()[0].strip()
            doc_resp = http.get(path + role, headers=hdr, timeout=self._timeout)
            if doc_resp.status_code != 200:
                raise AwsError("imds", "credentials", "CredentialsUnavailable", doc_resp.status_code)
            doc = doc_resp.json()
        except httpx.HTTPError as exc:
            raise AwsError("imds", "credentials", type(exc).__name__) from None
        except ValueError:
            raise AwsError("imds", "credentials", "MalformedCredentials") from None
        if not isinstance(doc, dict) or doc.get("Code", "Success") != "Success":
            raise AwsError("imds", "credentials", "CredentialsNotReady")
        key, secret = doc.get("AccessKeyId"), doc.get("SecretAccessKey")
        if not isinstance(key, str) or not isinstance(secret, str) or not key or not secret:
            raise AwsError("imds", "credentials", "MalformedCredentials")
        token = doc.get("Token")
        self.fetches += 1
        return AwsCredentials(key, secret, token if isinstance(token, str) and token else None,
                              _parse_expiry(doc.get("Expiration")))


def _error_type(resp: httpx.Response) -> str:
    raw = ""
    try:
        body = resp.json()
        if isinstance(body, dict):
            raw = str(body.get("__type") or body.get("code") or "")
    except ValueError:
        pass
    if not raw:
        raw = resp.headers.get("x-amzn-errortype", "")
    raw = raw.split(":", 1)[0].rsplit("#", 1)[-1].strip()
    return raw or "UnknownError"


class AwsJsonClient:
    """One AWS service endpoint speaking the JSON 1.1 protocol (`X-Amz-Target: <prefix>.<Action>`)."""

    def __init__(self, service: str, region: str, target_prefix: str, credentials: CredentialProvider, *,
                 http: httpx.Client | None = None, endpoint: str | None = None, timeout_s: float = 5.0,
                 now: Callable[[], datetime] | None = None) -> None:
        if not region or not region.replace("-", "").isalnum():
            raise ValueError(f"{service}: region is not a region name")
        self.service = service
        self.region = region
        self._prefix = target_prefix
        self._creds = credentials
        self._http = http
        self._owned: httpx.Client | None = None
        self._timeout = timeout_s
        self._now = now
        self.url = (endpoint or f"https://{service}.{region}.amazonaws.com").rstrip("/") + "/"

    def __repr__(self) -> str:
        return f"AwsJsonClient(service={self.service!r}, region={self.region!r})"

    def _client(self) -> httpx.Client:
        if self._http is not None:
            return self._http
        if self._owned is None:
            self._owned = httpx.Client(timeout=self._timeout)
        return self._owned

    def close(self) -> None:
        if self._owned is not None:
            self._owned.close()
            self._owned = None

    def call(self, action: str, body: Mapping[str, Any]) -> dict[str, Any]:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
        for attempt in (0, 1):
            creds = self._creds.get(force_refresh=attempt > 0)
            headers = sign_headers(
                "POST", self.url,
                {"content-type": "application/x-amz-json-1.1", "x-amz-target": f"{self._prefix}.{action}"},
                data, access_key=creds.access_key_id, secret_key=creds.secret_access_key, region=self.region,
                service=self.service, session_token=creds.session_token,
                now=self._now() if self._now is not None else None)
            try:
                resp = self._client().post(self.url, content=data, headers=headers, timeout=self._timeout)
            except httpx.HTTPError as exc:
                raise AwsError(self.service, action, type(exc).__name__) from None
            if resp.status_code == 200:
                try:
                    out = resp.json()
                except ValueError:
                    raise AwsError(self.service, action, "MalformedResponse", 200) from None
                if not isinstance(out, dict):
                    raise AwsError(self.service, action, "MalformedResponse", 200)
                return out
            err = AwsError(self.service, action, _error_type(resp), resp.status_code)
            if attempt == 0 and err.is_auth_error:
                log.warning("%s %s: %s; refreshing AWS credentials and retrying once", self.service, action,
                            err.error_type)
                continue
            raise err
        raise AssertionError("unreachable")  # pragma: no cover
