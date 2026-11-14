"""etcd v3 access for leadership and health records (operations.md §1, §2, §4).

`EtcdClient` is a **synchronous** protocol: it is called from the lease-keeper thread (never from the
event loop; the loop uses `asyncio.to_thread`). `HttpEtcdClient` speaks the etcd v3 JSON / gRPC
gateway over httpx:

    POST /v3/lease/grant      {"TTL": 15}
    POST /v3/lease/keepalive  {"ID": "<id>"}
    POST /v3/lease/revoke     {"ID": "<id>"}
    POST /v3/kv/put           {"key": b64, "value": b64, "lease": "<id>"}
    POST /v3/kv/range         {"key": b64[, "range_end": b64]}
    POST /v3/kv/txn           compare create_revision == 0, then put with lease  (campaign)

All three endpoints are configured; a failed request is retried at once on the next endpoint,
inside the caller's total time budget (§2 "retrying at once on another endpoint").

TLS (IMPLEMENTATION_PLAN 4.7): `verify=` takes the internal CA bundle (XTNL_ETCD_CA) and `cert=` the
client certificate and key (XTNL_ETCD_CERT, XTNL_ETCD_KEY) for etcd `client-cert-auth`; `EtcdTls.from_env`
reads them (paths only, never key material).
"""
from __future__ import annotations

import base64
import json
import logging
import os
import ssl
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final, Protocol

import httpx

log = logging.getLogger(__name__)

LEADER_KEY = "/xtnl/leader"
HEALTH_PREFIX = "/xtnl/health/"
SAFE_HOLD_KEY = "/xtnl/safe_hold"
ETCD_CA_VAR: Final = "XTNL_ETCD_CA"
ETCD_CERT_VAR: Final = "XTNL_ETCD_CERT"
ETCD_KEY_VAR: Final = "XTNL_ETCD_KEY"


class EtcdError(RuntimeError):
    """etcd could not be reached on any endpoint within the budget, or answered with an error."""


@dataclass(frozen=True)
class LeaseGrant:
    lease_id: int
    ttl_s: int
    cluster_id: str
    revision: int


@dataclass(frozen=True)
class TxnResult:
    succeeded: bool
    cluster_id: str
    revision: int          # header revision: the create revision of the key when succeeded


@dataclass(frozen=True)
class KeyValue:
    key: str
    value: str
    create_revision: int
    mod_revision: int
    lease_id: int


class EtcdClient(Protocol):
    def lease_grant(self, ttl_s: int, *, timeout_s: float) -> LeaseGrant: ...

    def lease_keepalive(self, lease_id: int, *, timeout_s: float) -> int:
        """Returns the remaining TTL in seconds; 0 means the lease no longer exists."""
        ...

    def lease_revoke(self, lease_id: int, *, timeout_s: float) -> None: ...

    def put(self, key: str, value: str, *, lease_id: int = 0, timeout_s: float) -> int: ...

    def get(self, key: str, *, timeout_s: float) -> KeyValue | None: ...

    def get_prefix(self, prefix: str, *, timeout_s: float) -> list[KeyValue]: ...

    def create_if_absent(self, key: str, value: str, *, lease_id: int, timeout_s: float) -> TxnResult:
        """Txn: compare create_revision(key) == 0, then put(key, value, lease)."""
        ...


def _b64(s: str | bytes) -> str:
    raw = s.encode() if isinstance(s, str) else s
    return base64.b64encode(raw).decode("ascii")


def _unb64(s: str) -> str:
    return base64.b64decode(s).decode()


def prefix_range_end(prefix: str) -> bytes:
    """etcd range_end for a prefix: the prefix with its last byte incremented."""
    b = bytearray(prefix.encode())
    for i in range(len(b) - 1, -1, -1):
        if b[i] < 0xFF:
            b[i] += 1
            return bytes(b[: i + 1])
    return b"\x00"


def _header(body: dict[str, Any]) -> tuple[str, int]:
    h = body.get("header") or {}
    return str(h.get("cluster_id", "")), int(h.get("revision", 0))


def _kv(obj: dict[str, Any]) -> KeyValue:
    return KeyValue(_unb64(obj["key"]), _unb64(obj.get("value", "")), int(obj.get("create_revision", 0)),
                    int(obj.get("mod_revision", 0)), int(obj.get("lease", 0)))


@dataclass(frozen=True)
class EtcdTls:
    """etcd client TLS: CA bundle path (None = the default trust store) and client cert / key paths."""

    ca_file: str | None = None
    cert_file: str | None = None
    key_file: str | None = None

    @property
    def verify(self) -> str | bool:
        return self.ca_file if self.ca_file else True

    @property
    def cert(self) -> tuple[str, str] | None:
        return (self.cert_file, self.key_file) if self.cert_file and self.key_file else None

    @staticmethod
    def from_env(environ: Mapping[str, str] | None = None) -> EtcdTls:
        """ValueError (names only) when only one of XTNL_ETCD_CERT / XTNL_ETCD_KEY is set."""
        e = os.environ if environ is None else environ
        ca, cert, key = ((e.get(n) or "").strip() or None for n in (ETCD_CA_VAR, ETCD_CERT_VAR, ETCD_KEY_VAR))
        if (cert is None) != (key is None):
            raise ValueError(f"{ETCD_CERT_VAR} and {ETCD_KEY_VAR} must be set together")
        return EtcdTls(ca, cert, key)


def tls_context(verify: str | bool | ssl.SSLContext = True,
                cert: tuple[str, str] | None = None) -> ssl.SSLContext | bool:
    """The `verify=` value for httpx: an SSLContext with the CA bundle and client cert when either is
    given, otherwise `verify` itself. ValueError names the file that could not be loaded."""
    if isinstance(verify, ssl.SSLContext):
        ctx = verify
    elif isinstance(verify, str):
        try:
            ctx = ssl.create_default_context(cafile=verify)
        except (OSError, ssl.SSLError) as exc:
            raise ValueError(f"etcd CA bundle {verify!r} cannot be loaded: {type(exc).__name__}") from None
    elif cert is None:
        return verify
    elif verify:
        ctx = ssl.create_default_context()
    else:
        raise ValueError("etcd client certificate given with verify=False: refusing unverified TLS")
    if cert is not None:
        try:
            ctx.load_cert_chain(cert[0], cert[1])
        except (OSError, ssl.SSLError) as exc:
            raise ValueError(f"etcd client certificate {cert[0]!r} / key cannot be loaded: "
                             f"{type(exc).__name__}") from None
    return ctx


class HttpEtcdClient:
    """EtcdClient over the v3 JSON gateway with endpoint failover. Thread-safe enough for one keeper
    thread plus occasional `to_thread` callers (httpx.Client is safe to share).

    `verify`: True (default trust store), a CA bundle path, or an SSLContext; `cert`: (cert file, key
    file) presented for etcd client-cert auth."""

    def __init__(self, endpoints: Sequence[str], *, per_request_timeout_s: float = 0.9,
                 transport: httpx.BaseTransport | None = None, verify: str | bool | ssl.SSLContext = True,
                 cert: tuple[str, str] | None = None) -> None:
        if not endpoints:
            raise ValueError("at least one etcd endpoint is required")
        self._endpoints = [e.rstrip("/") for e in endpoints]
        self._current = 0
        self._per_request = per_request_timeout_s
        self.tls = tls_context(verify, cert)
        self.client_cert = cert is not None
        self._http = httpx.Client(transport=transport, verify=self.tls)

    @property
    def current_endpoint(self) -> str:
        return self._endpoints[self._current]

    def close(self) -> None:
        self._http.close()

    def _post(self, path: str, body: dict[str, Any], timeout_s: float) -> dict[str, Any]:
        deadline = time.monotonic() + max(timeout_s, 0.05)
        errors: list[str] = []
        for i in range(len(self._endpoints)):
            left = deadline - time.monotonic()
            if left <= 0:
                break
            idx = (self._current + i) % len(self._endpoints)
            url = self._endpoints[idx] + path
            try:
                resp = self._http.post(url, json=body, timeout=min(self._per_request, left))
                if resp.status_code >= 500:
                    raise httpx.HTTPStatusError(f"{resp.status_code}", request=resp.request, response=resp)
                if resp.status_code >= 400:
                    raise EtcdError(f"{path}: HTTP {resp.status_code} {resp.text[:200]}")
                self._current = idx
                text = resp.text.strip()
                first = text.splitlines()[0] if text else "{}"   # keepalive streams one JSON per line
                parsed: dict[str, Any] = json.loads(first)
                if "error" in parsed and "result" not in parsed:
                    raise EtcdError(f"{path}: {parsed['error']}")
                return parsed
            except (httpx.TransportError, httpx.HTTPStatusError, json.JSONDecodeError) as exc:
                errors.append(f"{url}: {exc!r}")
                log.warning("etcd %s failed on %s; trying next endpoint", path, url)
        self._current = (self._current + 1) % len(self._endpoints)
        raise EtcdError(f"{path}: no endpoint answered ({'; '.join(errors) or 'budget exhausted'})")

    def lease_grant(self, ttl_s: int, *, timeout_s: float) -> LeaseGrant:
        body = self._post("/v3/lease/grant", {"TTL": ttl_s}, timeout_s)
        cluster_id, revision = _header(body)
        return LeaseGrant(int(body["ID"]), int(body.get("TTL", ttl_s)), cluster_id, revision)

    def lease_keepalive(self, lease_id: int, *, timeout_s: float) -> int:
        body = self._post("/v3/lease/keepalive", {"ID": str(lease_id)}, timeout_s)
        result = body.get("result", body)
        return int(result.get("TTL", 0))

    def lease_revoke(self, lease_id: int, *, timeout_s: float) -> None:
        self._post("/v3/lease/revoke", {"ID": str(lease_id)}, timeout_s)

    def put(self, key: str, value: str, *, lease_id: int = 0, timeout_s: float) -> int:
        req: dict[str, Any] = {"key": _b64(key), "value": _b64(value)}
        if lease_id:
            req["lease"] = str(lease_id)
        return _header(self._post("/v3/kv/put", req, timeout_s))[1]

    def get(self, key: str, *, timeout_s: float) -> KeyValue | None:
        body = self._post("/v3/kv/range", {"key": _b64(key)}, timeout_s)
        kvs = body.get("kvs") or []
        return _kv(kvs[0]) if kvs else None

    def get_prefix(self, prefix: str, *, timeout_s: float) -> list[KeyValue]:
        body = self._post("/v3/kv/range", {"key": _b64(prefix), "range_end": _b64(prefix_range_end(prefix))},
                          timeout_s)
        return [_kv(o) for o in body.get("kvs") or []]

    def create_if_absent(self, key: str, value: str, *, lease_id: int, timeout_s: float) -> TxnResult:
        req = {
            "compare": [{"key": _b64(key), "target": "CREATE", "result": "EQUAL", "create_revision": "0"}],
            "success": [{"request_put": {"key": _b64(key), "value": _b64(value), "lease": str(lease_id)}}],
        }
        body = self._post("/v3/kv/txn", req, timeout_s)
        cluster_id, revision = _header(body)
        return TxnResult(bool(body.get("succeeded", False)), cluster_id, revision)

    # --- read-only cluster health (metrics, IMPLEMENTATION_PLAN 5.6) ---------------------------------
    def _post_one(self, endpoint: str, path: str, body: dict[str, Any], timeout_s: float) -> dict[str, Any]:
        resp = self._http.post(endpoint + path, json=body, timeout=timeout_s)
        if resp.status_code >= 400:
            raise EtcdError(f"{path}: HTTP {resp.status_code}")
        parsed: dict[str, Any] = json.loads(resp.text.strip() or "{}")
        return parsed

    def cluster_status(self, *, timeout_s: float = 1.0) -> EtcdClusterStatus:
        """Maintenance status of every configured endpoint (each asked directly, `timeout_s` each), the member
        names (member list) and the active alarm count. Read-only; blocking (call via to_thread)."""
        names: dict[str, str] = {}       # endpoint -> member name
        try:
            members = self._post("/v3/cluster/member/list", {}, timeout_s).get("members") or []
            for m in members:
                for url in m.get("clientURLs") or []:
                    names[str(url).rstrip("/")] = str(m.get("name") or m.get("ID") or "")
        except EtcdError:
            pass
        alarms: int | None
        try:
            alarms = len(self._post("/v3/maintenance/alarm", {"action": "GET"}, timeout_s).get("alarms") or [])
        except EtcdError:
            alarms = None
        out: list[EtcdMemberStatus] = []
        for ep in self._endpoints:
            name = names.get(ep) or urlsplit_host(ep)
            try:
                st = self._post_one(ep, "/v3/maintenance/status", {}, timeout_s)
            except (httpx.HTTPError, EtcdError, ValueError):
                out.append(EtcdMemberStatus(name, ep, False, None, None))
                continue
            leader = str(st.get("leader") or "0")
            db = st.get("dbSize")
            out.append(EtcdMemberStatus(name, ep, True, None if db is None else int(db), leader not in ("", "0")))
        return EtcdClusterStatus(tuple(out), alarms)


def urlsplit_host(endpoint: str) -> str:
    rest = endpoint.split("://", 1)[-1]
    return rest.split("/", 1)[0].rsplit(":", 1)[0]


@dataclass(frozen=True)
class EtcdMemberStatus:
    member: str              # etcd member name (node-a, node-b, witness), else the endpoint host
    endpoint: str
    healthy: bool            # answered a maintenance status call in time
    db_size: int | None
    has_leader: bool | None


@dataclass(frozen=True)
class EtcdClusterStatus:
    members: tuple[EtcdMemberStatus, ...]
    alarms_active: int | None
