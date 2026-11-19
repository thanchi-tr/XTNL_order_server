"""HMAC request signature, clock window, replay cache and per-IP rate limit (pure; the shared contract)."""
from __future__ import annotations

import hashlib
import hmac

import pytest

from src.api.auth import (CLOCK_WINDOW_MS, NONCE_TTL_MS, Authenticator, AuthResult, RateLimiter, ReplayCache,
                          canonical, sign, signed_headers)

from tests.api.conftest import NOW, SECRET, SECRET_TEXT

NONCE = "0123456789abcdef0123456789abcdef"


def headers(method: str = "GET", path: str = "/v1/health", body: bytes = b"", *, now: int = NOW,
            nonce: str = NONCE, secret: bytes = SECRET) -> dict[str, str]:
    return {k.lower(): v for k, v in signed_headers(secret, method, path, body, now_ms=now, nonce=nonce).items()}


def test_signature_matches_an_independent_implementation_of_the_contract() -> None:
    body = b'{"force":false}'
    message = "\n".join(["POST", "/v1/pinescript/compile", str(NOW), NONCE, hashlib.sha256(body).hexdigest()])
    expected = hmac.new(SECRET_TEXT.encode("utf-8"), message.encode(), hashlib.sha256).hexdigest()
    assert sign(SECRET, "POST", "/v1/pinescript/compile", str(NOW), NONCE, body) == expected
    assert canonical("get", "/v1/health", "1", NONCE, b"").endswith(hashlib.sha256(b"").hexdigest().encode())
    assert canonical("get", "/v1/health", "1", NONCE, b"").startswith(b"GET\n/v1/health\n1\n")
    assert expected == expected.lower() and len(expected) == 64


def test_valid_request_passes_once() -> None:
    auth = Authenticator(SECRET)
    assert auth.check("GET", "/v1/health", headers(), b"", NOW) is AuthResult.OK
    assert auth.check("GET", "/v1/health", headers(), b"", NOW + 1000) is AuthResult.REPLAYED


@pytest.mark.parametrize("change", ["secret", "method", "path", "body", "timestamp", "nonce"])
def test_any_change_breaks_the_signature(change: str) -> None:
    auth = Authenticator(SECRET)
    h = headers("POST", "/v1/pinescript/compile", b'{"force":false}')
    method, path, body = "POST", "/v1/pinescript/compile", b'{"force":false}'
    if change == "secret":
        h = headers("POST", path, body, secret=b"6a" * 32)
    elif change == "method":
        method = "GET"
    elif change == "path":
        path = "/v1/pinescript/latest"
    elif change == "body":
        body = b'{"force":true}'
    elif change == "timestamp":
        h["x-xtnl-timestamp"] = str(NOW + 1)
    else:
        h["x-xtnl-nonce"] = NONCE[::-1]
    assert auth.check(method, path, h, body, NOW) is AuthResult.BAD_SIGNATURE


def test_uppercase_signature_is_refused() -> None:
    h = headers()
    h["x-xtnl-signature"] = h["x-xtnl-signature"].upper()
    assert Authenticator(SECRET).check("GET", "/v1/health", h, b"", NOW) is AuthResult.MALFORMED


@pytest.mark.parametrize("skew", [CLOCK_WINDOW_MS + 1, -(CLOCK_WINDOW_MS + 1), 3_600_000])
def test_timestamp_outside_the_window_is_refused(skew: int) -> None:
    h = headers(now=NOW - skew)
    assert Authenticator(SECRET).check("GET", "/v1/health", h, b"", NOW) is AuthResult.EXPIRED


@pytest.mark.parametrize("skew", [0, CLOCK_WINDOW_MS, -CLOCK_WINDOW_MS])
def test_timestamp_inside_the_window_passes(skew: int) -> None:
    h = headers(now=NOW - skew)
    assert Authenticator(SECRET).check("GET", "/v1/health", h, b"", NOW) is AuthResult.OK


@pytest.mark.parametrize("missing", ["x-xtnl-timestamp", "x-xtnl-nonce", "x-xtnl-signature"])
def test_missing_header_is_refused(missing: str) -> None:
    h = headers()
    del h[missing]
    assert Authenticator(SECRET).check("GET", "/v1/health", h, b"", NOW) is AuthResult.MISSING


@pytest.mark.parametrize("field,value", [("x-xtnl-nonce", "abc"), ("x-xtnl-nonce", "g" * 32),
                                         ("x-xtnl-nonce", "a" * 65), ("x-xtnl-timestamp", "12.5"),
                                         ("x-xtnl-timestamp", "-1"), ("x-xtnl-signature", "zz")])
def test_malformed_header_is_refused(field: str, value: str) -> None:
    h = headers()
    h[field] = value
    assert Authenticator(SECRET).check("GET", "/v1/health", h, b"", NOW) is AuthResult.MALFORMED


def test_replay_cache_forgets_after_its_ttl_and_ignores_case() -> None:
    auth = Authenticator(SECRET)
    assert auth.check("GET", "/v1/health", headers(), b"", NOW) is AuthResult.OK
    upper = headers(nonce=NONCE.upper())
    assert auth.check("GET", "/v1/health", upper, b"", NOW) is AuthResult.REPLAYED
    later = NOW + NONCE_TTL_MS + 1
    assert auth.check("GET", "/v1/health", headers(now=later), b"", later) is AuthResult.OK


def test_bad_signatures_never_fill_the_replay_cache() -> None:
    auth = Authenticator(SECRET)
    for i in range(50):
        h = headers(nonce=f"{i:032x}")
        h["x-xtnl-signature"] = "0" * 64
        assert auth.check("GET", "/v1/health", h, b"", NOW) is AuthResult.BAD_SIGNATURE
    assert len(auth.replay) == 0


def test_full_replay_cache_fails_closed() -> None:
    cache = ReplayCache(max_entries=2)
    assert cache.add("a" * 16, NOW) is AuthResult.OK
    assert cache.add("b" * 16, NOW) is AuthResult.OK
    assert cache.add("c" * 16, NOW) is AuthResult.BUSY
    assert cache.add("c" * 16, NOW + NONCE_TTL_MS) is AuthResult.OK     # expired entries make room


def test_authenticator_repr_hides_the_secret() -> None:
    assert SECRET_TEXT not in repr(Authenticator(SECRET))


def test_rate_limit_is_30_a_minute_per_ip_with_refill() -> None:
    rl = RateLimiter(30)
    assert all(rl.allow("203.0.113.5", NOW) for _ in range(30))
    assert not rl.allow("203.0.113.5", NOW)
    assert rl.allow("198.51.100.7", NOW)                    # another address has its own bucket
    assert rl.retry_after_s("203.0.113.5") >= 1
    assert not rl.allow("203.0.113.5", NOW + 1000)          # half a token
    assert rl.allow("203.0.113.5", NOW + 2000)              # one token per 2 s
    assert not rl.allow("203.0.113.5", NOW + 2000)


def test_rate_limiter_tracks_a_bounded_number_of_addresses() -> None:
    rl = RateLimiter(1, max_ips=3)
    for i in range(10):
        rl.allow(f"192.0.2.{i}", NOW)
    assert len(rl) == 3


@pytest.mark.parametrize("skew", [CLOCK_WINDOW_MS, 0, -CLOCK_WINDOW_MS])
def test_a_nonce_outlives_every_instant_its_timestamp_is_accepted(skew: int) -> None:
    """Regression: a request stamped 60 s in the future used to be forgotten at first-seen + 120 000 ms, while its
    timestamp was still inside the window, so it could be replayed once at exactly +120 s."""
    auth = Authenticator(SECRET)
    ts = NOW + skew
    h = headers(now=ts)
    assert auth.check("GET", "/v1/health", h, b"", NOW) is AuthResult.OK
    last_accepted = ts + CLOCK_WINDOW_MS            # the last instant the timestamp passes the window check
    for at in (NOW + 1, NOW + NONCE_TTL_MS - 1, NOW + NONCE_TTL_MS, last_accepted):
        result = auth.check("GET", "/v1/health", h, b"", at)
        assert result is (AuthResult.REPLAYED if at <= last_accepted else AuthResult.EXPIRED), at
    assert auth.check("GET", "/v1/health", h, b"", last_accepted + 1) is AuthResult.EXPIRED


def test_replay_cache_keeps_each_nonce_until_its_own_horizon() -> None:
    cache = ReplayCache(ttl_ms=100)
    assert cache.add("a" * 16, 0, accept_until_ms=500) is AuthResult.OK        # kept until 501
    assert cache.add("b" * 16, 0) is AuthResult.OK                             # kept until 100
    assert cache.add("b" * 16, 99) is AuthResult.REPLAYED
    assert cache.add("b" * 16, 100) is AuthResult.OK and len(cache) == 2       # b expired and is new again
    assert cache.add("a" * 16, 500) is AuthResult.REPLAYED
    assert cache.add("a" * 16, 501) is AuthResult.OK
