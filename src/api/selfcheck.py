"""`python -m src.api.selfcheck`: signed GET /v1/health against the local API, pinned to the local certificate.

    python -m src.api.selfcheck                 health over https://127.0.0.1:<XTNL_API_PORT>
    python -m src.api.selfcheck --fingerprint   only print the certificate's SHA-256 fingerprint (not secret)

The peer certificate must be byte-identical to XTNL_API_TLS_CERT (SHA-256 of the DER compared in constant time)
before anything is sent: the same pin the website applies. Needs XTNL_API_SECRET in the environment (never
printed). Prints one line. Exit codes: 0 healthy; 1 not reachable / not healthy; 3 certificate mismatch;
78 configuration (no secret, no certificate). Phase 1 never sends orders.
"""
from __future__ import annotations

import argparse
import http.client
import json
import os
import secrets
import ssl
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final, TextIO

from src.api.auth import signed_headers
from src.api.pinescript import now_ms
from src.api.settings import ApiConfigError, ApiSettings, cert_path_from_env
from src.api.tls import der_of, fingerprint, fingerprint_of_der, matches_pin, pinned_client_context

ROOT: Final = Path(__file__).resolve().parents[2]
PATH: Final = "/v1/health"


def check(settings: ApiSettings, *, host: str = "127.0.0.1", timeout_s: float = 5.0) -> tuple[int, str]:
    try:
        pin = fingerprint_of_der(der_of(settings.cert_path))
    except (OSError, ValueError) as exc:
        return 78, f"certificate unreadable: {settings.cert_path} ({type(exc).__name__})"
    conn = http.client.HTTPSConnection(host, settings.port, timeout=timeout_s, context=pinned_client_context())
    try:
        try:
            conn.connect()
        except (OSError, ssl.SSLError) as exc:
            return 1, f"not reachable: https://{host}:{settings.port} ({type(exc).__name__})"
        sock = conn.sock
        der = sock.getpeercert(binary_form=True) if isinstance(sock, ssl.SSLSocket) else None
        if not der or not matches_pin(der, pin):
            return 3, "the listener's certificate does not match the local certificate file (pin mismatch)"
        tls = sock.version() if isinstance(sock, ssl.SSLSocket) else None
        headers = signed_headers(settings.secret, "GET", PATH, b"", now_ms=now_ms(), nonce=secrets.token_hex(16))
        try:
            conn.request("GET", PATH, headers=headers)
            resp = conn.getresponse()
            raw = resp.read(4096)
        except (OSError, ssl.SSLError, http.client.HTTPException) as exc:
            return 1, f"request failed ({type(exc).__name__})"
        try:
            body = json.loads(raw)
        except ValueError:
            body = None
        if resp.status == 200 and isinstance(body, dict) and body.get("ok") is True:
            return 0, (f"healthy: https://{host}:{settings.port}{PATH} -> 200 service={body.get('service')} "
                       f"api={body.get('api')} tls={tls or '?'}")
        return 1, f"unhealthy: {PATH} -> HTTP {resp.status}"
    finally:
        conn.close()


def main(argv: Sequence[str] | None = None, *, environ: Mapping[str, str] | None = None,
         out: TextIO | None = None) -> int:
    out = out if out is not None else sys.stdout
    p = argparse.ArgumentParser(prog="python -m src.api.selfcheck")
    p.add_argument("--fingerprint", action="store_true", help="print the certificate fingerprint and exit")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--timeout", type=float, default=5.0)
    ns = p.parse_args(list(argv) if argv is not None else None)
    env = environ if environ is not None else os.environ
    if ns.fingerprint:   # needs no secret
        cert = cert_path_from_env(env, root=ROOT)
        try:
            out.write(f"sha256 fingerprint: {fingerprint(cert)}\n")
        except (OSError, ValueError) as exc:
            out.write(f"certificate unreadable: {cert} ({type(exc).__name__})\n")
            return 78
        return 0
    try:
        settings = ApiSettings.from_env(env, root=ROOT, need_storage=False)
    except ApiConfigError as exc:
        out.write(f"config: {exc}\n")
        return 78
    code, line = check(settings, host=ns.host, timeout_s=ns.timeout)
    out.write(line + "\n")
    return code


if __name__ == "__main__":
    sys.exit(main())
