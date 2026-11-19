"""Request authentication and abuse limits of the order-server API (pure: no I/O, injectable clock).

Signature (shared contract with the thesis website):

    X-XTNL-Signature = lowercase hex HMAC-SHA256(secret,
        METHOD + "\n" + PATH (no query) + "\n" + TIMESTAMP + "\n" + NONCE + "\n" + sha256_hex(raw body))

with X-XTNL-Timestamp in unix milliseconds (accepted within +-60 s of this server's clock) and X-XTNL-Nonce
16-64 hex characters, random per request, accepted once within 120 s (bounded replay cache). The signature is
compared in constant time, and a nonce is remembered only once its signature checked out, so unsigned traffic
cannot fill the cache. A nonce is kept for at least 120 s AND until its own timestamp has left the clock window
(timestamp + 60 s), so a request stamped at the far edge of the window can never be replayed after its nonce
was forgotten. The rate limit is a token bucket per client IP (30 per minute by default), bounded in the
number of addresses it tracks.
"""
from __future__ import annotations

import hashlib
import hmac
import re
import heapq
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Final

TS_HEADER: Final = "x-xtnl-timestamp"
NONCE_HEADER: Final = "x-xtnl-nonce"
SIG_HEADER: Final = "x-xtnl-signature"

CLOCK_WINDOW_MS: Final = 60_000
NONCE_TTL_MS: Final = 120_000
MAX_NONCES: Final = 20_000
MAX_TRACKED_IPS: Final = 4096

_NONCE = re.compile(r"[0-9a-fA-F]{16,64}")
_SIG = re.compile(r"[0-9a-f]{64}")
_TS = re.compile(r"[0-9]{1,15}")


def body_hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def canonical(method: str, path: str, timestamp: str, nonce: str, body: bytes) -> bytes:
    return "\n".join((method.upper(), path, timestamp, nonce, body_hash(body))).encode("utf-8")


def sign(secret: bytes, method: str, path: str, timestamp: str, nonce: str, body: bytes = b"") -> str:
    return hmac.new(secret, canonical(method, path, timestamp, nonce, body), hashlib.sha256).hexdigest()


def signed_headers(secret: bytes, method: str, path: str, body: bytes, *, now_ms: int, nonce: str) -> dict[str, str]:
    """Headers of one signed request (selfcheck and tests)."""
    ts = str(now_ms)
    return {"X-XTNL-Timestamp": ts, "X-XTNL-Nonce": nonce,
            "X-XTNL-Signature": sign(secret, method, path, ts, nonce, body)}


class AuthResult(Enum):
    OK = "ok"
    MISSING = "missing_headers"
    MALFORMED = "malformed_headers"
    EXPIRED = "timestamp_outside_window"
    BAD_SIGNATURE = "bad_signature"
    REPLAYED = "nonce_replayed"
    BUSY = "replay_cache_full"


class ReplayCache:
    """Nonces still inside their replay horizon, at most `max_entries`.

    Each nonce is kept until max(first seen + `ttl_ms`, `accept_until_ms` + 1), where `accept_until_ms` is the
    last instant its timestamp is still inside the clock window: the nonce outlives every moment the request
    could be accepted again. Expiry times differ per nonce, so they are kept in a heap (ordered eviction)."""

    def __init__(self, ttl_ms: int = NONCE_TTL_MS, max_entries: int = MAX_NONCES) -> None:
        self.ttl_ms = ttl_ms
        self.max_entries = max_entries
        self._seen: dict[str, int] = {}
        self._expiry: list[tuple[int, str]] = []

    def __len__(self) -> int:
        return len(self._seen)

    def _purge(self, now_ms: int) -> None:
        while self._expiry and self._expiry[0][0] <= now_ms:
            expires, nonce = heapq.heappop(self._expiry)
            if self._seen.get(nonce) == expires:
                del self._seen[nonce]

    def add(self, nonce: str, now_ms: int, *, accept_until_ms: int | None = None) -> AuthResult:
        """OK and remembered; REPLAYED when still remembered; BUSY (fail closed) when the cache is full."""
        self._purge(now_ms)
        if nonce in self._seen:
            return AuthResult.REPLAYED
        if len(self._seen) >= self.max_entries:
            return AuthResult.BUSY
        expires = now_ms + self.ttl_ms
        if accept_until_ms is not None:
            expires = max(expires, accept_until_ms + 1)
        self._seen[nonce] = expires
        heapq.heappush(self._expiry, (expires, nonce))
        return AuthResult.OK


class Authenticator:
    def __init__(self, secret: bytes, *, window_ms: int = CLOCK_WINDOW_MS, replay: ReplayCache | None = None) -> None:
        if not secret:
            raise ValueError("empty secret")
        self._secret = secret
        self.window_ms = window_ms
        self.replay = replay if replay is not None else ReplayCache()

    def __repr__(self) -> str:   # never the secret
        return f"Authenticator(window_ms={self.window_ms}, nonces={len(self.replay)})"

    def check(self, method: str, path: str, headers: Mapping[str, str], body: bytes, now_ms: int) -> AuthResult:
        ts = headers.get(TS_HEADER)
        nonce = headers.get(NONCE_HEADER)
        sig = headers.get(SIG_HEADER)
        if ts is None or nonce is None or sig is None:
            return AuthResult.MISSING
        if not (_TS.fullmatch(ts) and _NONCE.fullmatch(nonce) and _SIG.fullmatch(sig)):
            return AuthResult.MALFORMED
        if abs(now_ms - int(ts)) > self.window_ms:
            return AuthResult.EXPIRED
        expected = sign(self._secret, method, path, ts, nonce, body)
        if not hmac.compare_digest(expected.encode("ascii"), sig.encode("ascii")):
            return AuthResult.BAD_SIGNATURE
        # remembered until its timestamp can no longer pass the window check above
        return self.replay.add(nonce.lower(), now_ms, accept_until_ms=int(ts) + self.window_ms)


@dataclass
class _Bucket:
    tokens: float
    at_ms: int


class RateLimiter:
    """Token bucket per client IP: `per_min` requests a minute sustained, bursts up to `per_min`."""

    def __init__(self, per_min: int, *, max_ips: int = MAX_TRACKED_IPS) -> None:
        if per_min < 1:
            raise ValueError("per_min must be >= 1")
        self.capacity = float(per_min)
        self.refill_per_ms = per_min / 60_000.0
        self.max_ips = max_ips
        self._buckets: OrderedDict[str, _Bucket] = OrderedDict()

    def __len__(self) -> int:
        return len(self._buckets)

    def allow(self, ip: str, now_ms: int) -> bool:
        b = self._buckets.get(ip)
        if b is None:
            if len(self._buckets) >= self.max_ips:
                self._buckets.popitem(last=False)    # least recently seen address
            b = _Bucket(self.capacity, now_ms)
            self._buckets[ip] = b
        else:
            self._buckets.move_to_end(ip)
            elapsed = max(0, now_ms - b.at_ms)
            b.tokens = min(self.capacity, b.tokens + elapsed * self.refill_per_ms)
            b.at_ms = now_ms
        if b.tokens >= 1.0:
            b.tokens -= 1.0
            return True
        return False

    def retry_after_s(self, ip: str) -> int:
        b = self._buckets.get(ip)
        if b is None or b.tokens >= 1.0:
            return 1
        return max(1, int((1.0 - b.tokens) / self.refill_per_ms / 1000) + 1)
