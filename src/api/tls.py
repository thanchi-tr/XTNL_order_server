"""TLS for the order-server API: the server context (TLS 1.2+, AEAD ciphers only) and certificate pinning.

The certificate is self-signed (deploy/local/new-api-credentials.ps1, files under var/api-tls/, git-ignored).
The website trusts no CA for it: it pins the SHA-256 fingerprint of the certificate's DER encoding
(ORDER_SERVER_TLS_FINGERPRINT), which `fingerprint()` prints in the same form OpenSSL does (AB:CD:...).
"""
from __future__ import annotations

import hashlib
import hmac
import re
import ssl
from pathlib import Path
from typing import Final

#: TLS 1.2 suites: forward secret AEAD only (TLS 1.3 suites are all AEAD and stay at OpenSSL's defaults).
TLS12_CIPHERS: Final = "ECDHE+AESGCM:ECDHE+CHACHA20:!aNULL:!eNULL:!MD5:!SHA1:!RC4:!3DES:!DSS:!PSK"


def server_context(cert: Path, key: Path) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.set_ciphers(TLS12_CIPHERS)
    ctx.options |= ssl.OP_NO_COMPRESSION | ssl.OP_CIPHER_SERVER_PREFERENCE | ssl.OP_NO_RENEGOTIATION
    ctx.options |= ssl.OP_SINGLE_ECDH_USE | ssl.OP_SINGLE_DH_USE
    ctx.load_cert_chain(str(cert), str(key))
    return ctx


def pinned_client_context() -> ssl.SSLContext:
    """No CA trust and no hostname check: the caller compares the peer certificate with its pin itself."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def der_of(pem_path: Path) -> bytes:
    text = pem_path.read_text(encoding="ascii")
    m = re.search(r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", text, re.S)
    if m is None:
        raise ValueError(f"no PEM certificate in {pem_path}")
    return ssl.PEM_cert_to_DER_cert(m.group(0))


def fingerprint_of_der(der: bytes) -> str:
    digest = hashlib.sha256(der).hexdigest().upper()
    return ":".join(digest[i:i + 2] for i in range(0, len(digest), 2))


def fingerprint(pem_path: Path) -> str:
    return fingerprint_of_der(der_of(pem_path))


def normalise_fingerprint(text: str) -> str:
    """'ab:cd:...' / 'ABCD...' / with spaces -> 64 lowercase hex characters (ValueError otherwise)."""
    hexed = re.sub(r"[\s:]", "", text).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", hexed):
        raise ValueError("a SHA-256 fingerprint is 64 hex characters")
    return hexed


def matches_pin(der: bytes, pin: str) -> bool:
    return hmac.compare_digest(hashlib.sha256(der).hexdigest(), normalise_fingerprint(pin))
