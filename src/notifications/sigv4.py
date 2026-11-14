"""AWS Signature Version 4 request signing (stdlib hmac / hashlib only).

Used for Amazon SES v2 `SendEmail`. Implements the documented algorithm: canonical request ->
string to sign -> HMAC-SHA256 signing-key chain -> `Authorization` header. Secrets never leave this
function except as the signature.
"""
from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Final
from urllib.parse import parse_qsl, quote, urlsplit

ALGORITHM: Final = "AWS4-HMAC-SHA256"


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def signing_key(secret_key: str, date: str, region: str, service: str) -> bytes:
    k = _hmac(("AWS4" + secret_key).encode("utf-8"), date)
    k = _hmac(k, region)
    k = _hmac(k, service)
    return _hmac(k, "aws4_request")


def _uri_encode(s: str) -> str:
    return quote(s, safe="-_.~")


def canonical_uri(path: str) -> str:
    if not path:
        return "/"
    return "/".join(_uri_encode(seg) for seg in path.split("/"))


def canonical_query(query: str) -> str:
    pairs = parse_qsl(query, keep_blank_values=True)
    return "&".join(f"{_uri_encode(k)}={_uri_encode(v)}" for k, v in sorted(pairs))


def _norm_value(v: str) -> str:
    return " ".join(v.strip().split())


def sign_headers(method: str, url: str, headers: Mapping[str, str], body: bytes, *, access_key: str,
                 secret_key: str, region: str, service: str, now: datetime | None = None,
                 session_token: str | None = None) -> dict[str, str]:
    """Return `headers` plus `host`, `x-amz-date`, `x-amz-security-token` (when given) and
    `authorization`. Every header passed in is signed."""
    t = (now or datetime.now(UTC)).astimezone(UTC)
    amz_date = t.strftime("%Y%m%dT%H%M%SZ")
    date = t.strftime("%Y%m%d")
    parts = urlsplit(url)
    host = parts.netloc
    out: dict[str, str] = {k.lower(): v for k, v in headers.items()}
    out.setdefault("host", host)
    out["x-amz-date"] = amz_date
    if session_token:
        out["x-amz-security-token"] = session_token
    names = sorted(out)
    canon_headers = "".join(f"{n}:{_norm_value(out[n])}\n" for n in names)
    signed = ";".join(names)
    creq = "\n".join([method.upper(), canonical_uri(parts.path), canonical_query(parts.query), canon_headers,
                      signed, _sha256_hex(body)])
    scope = f"{date}/{region}/{service}/aws4_request"
    sts = "\n".join([ALGORITHM, amz_date, scope, _sha256_hex(creq.encode("utf-8"))])
    sig = hmac.new(signing_key(secret_key, date, region, service), sts.encode("utf-8"), hashlib.sha256).hexdigest()
    out["authorization"] = f"{ALGORITHM} Credential={access_key}/{scope}, SignedHeaders={signed}, Signature={sig}"
    return out
