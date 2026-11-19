"""TLS certificate expiry and readability (operations.md §13 "Certificate expiry alert at 30 days"; K5) and
in-place server certificate reload (operations.md §13 "rotation without restart"; K10). Standard library only, so
the Melbourne receiver and dashboard can use it without the engine's dependencies.

Two ways to read a certificate's notAfter:

* `cert_not_after_s(path)`: a PEM file on this host (a certificate this host serves or presents, or a CA);
* `endpoint_not_after_s(host, port)`: the certificate a TLS server presents, read by a verifying handshake (the
  default trust store, which honours `SSL_CERT_FILE`; optionally presenting a client certificate for a mutual-TLS
  endpoint such as an etcd peer port). This covers certificates served on hosts that export no metrics of their
  own (the witness's etcd member, the Melbourne receiver and dashboard listeners).

Either returns None when the certificate cannot be read **or** (endpoint) does not verify, which includes an
expired or untrusted certificate: an unreadable certificate is alerted on in its own right (`xtnl_cert_readable`
= 0, alert `XtnlCertUnreadable`; the receiver's `cert:<name>` condition), never silently skipped.

`ReloadingServerTls` keeps one server `SSLContext` and loads a replaced certificate / key pair into it when the
files change, so new handshakes use the new certificate without a restart (a pair that fails to load keeps the
current one and is reported). Nothing here logs a path's contents or a key.
"""
from __future__ import annotations

import logging
import os
import socket
import ssl
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Final

log = logging.getLogger(__name__)

EXPIRY_WARN_S: Final = 30 * 86_400            # operations.md §13: alert at 30 days
ENDPOINT_TIMEOUT_S: Final = 3.0


def cert_not_after_s(path: str) -> float | None:
    """notAfter of a PEM certificate file (epoch seconds), via the stdlib's certificate decoder; None if the file is
    missing, unreadable or not a certificate."""
    decode = getattr(getattr(ssl, "_ssl", None), "_test_decode_cert", None)
    if decode is None or not os.path.isfile(path):
        return None
    try:
        return float(ssl.cert_time_to_seconds(str(decode(path)["notAfter"])))
    except Exception:
        return None


def _client_context(cafile: str | None, client_cert: tuple[str, str] | None) -> ssl.SSLContext:
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cafile)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    if client_cert is not None:
        ctx.load_cert_chain(client_cert[0], client_cert[1])
    return ctx


def endpoint_not_after_s(host: str, port: int, *, timeout_s: float = ENDPOINT_TIMEOUT_S, cafile: str | None = None,
                         client_cert: tuple[str, str] | None = None, server_name: str | None = None) -> float | None:
    """notAfter of the certificate `host:port` presents, read by a verifying TLS handshake (blocking: run it in a
    thread). None when the connection, the handshake or the verification fails (an expired, untrusted or
    misnamed certificate included), so the caller reports it unreadable."""
    try:
        ctx = _client_context(cafile, client_cert)
        with socket.create_connection((host, port), timeout=timeout_s) as raw:
            with ctx.wrap_socket(raw, server_hostname=server_name or host) as tls:
                peer = tls.getpeercert()
        if not peer or "notAfter" not in peer:
            return None
        return float(ssl.cert_time_to_seconds(str(peer["notAfter"])))
    except Exception:
        return None


@dataclass(frozen=True)
class CertReading:
    """One certificate's state: `not_after_s` None = unreadable (or, for an endpoint, not verifiable)."""

    name: str
    not_after_s: float | None

    @property
    def readable(self) -> bool:
        return self.not_after_s is not None

    def expires_in_s(self, now_s: float) -> float | None:
        return None if self.not_after_s is None else self.not_after_s - now_s

    def problem(self, now_s: float, warn_s: float = EXPIRY_WARN_S) -> str | None:
        """None while readable and valid for more than `warn_s`; else a short description (no path, no contents)."""
        left = self.expires_in_s(now_s)
        if left is None:
            return f"certificate {self.name} is unreadable or does not verify"
        if left < warn_s:
            return (f"certificate {self.name} expired {int(-left // 86_400)} day(s) ago" if left <= 0
                    else f"certificate {self.name} expires in {int(left // 86_400)} day(s)")
        return None


def parse_name_list(raw: str | None) -> dict[str, str]:
    """'name=value,name=value' -> {name: value} (blank parts and parts without a name or value are skipped)."""
    out: dict[str, str] = {}
    for part in (raw or "").split(","):
        name, sep, value = part.partition("=")
        if sep and name.strip() and value.strip():
            out[name.strip()] = value.strip()
    return out


def parse_endpoint(value: str) -> tuple[str, int] | None:
    """'host:port' or '[v6]:port' -> (host, port); None when malformed."""
    host, sep, port_s = value.strip().rpartition(":")
    host = host.strip("[]")
    try:
        port = int(port_s)
    except ValueError:
        return None
    if not sep or not host or not 0 < port < 65_536:
        return None
    return host, port


def read_file_certs(certs: Mapping[str, str]) -> list[CertReading]:
    return [CertReading(name, cert_not_after_s(path)) for name, path in sorted(certs.items())]


class ReloadingServerTls:
    """A server TLS context (TLS 1.2+) whose certificate / key are re-loaded in place when their files change.

    `context` is created once (passed to `asyncio.start_server`); `reload_if_changed()` compares the files'
    (mtime, size) with the last loaded pair and, when they differ, loads the new pair into the same context, so the
    next handshake presents the new certificate (no restart, no dropped listener). A pair that fails to load (a
    half-written file, a key that does not match) keeps the current certificate, sets `issue` and is retried on the
    next call. Raises at construction when the initial pair cannot be loaded (a configuration error)."""

    def __init__(self, cert: str, key: str, *, stat: Callable[[str], os.stat_result] = os.stat) -> None:
        self.cert = cert
        self.key = key
        self._stat = stat
        self.context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.context.minimum_version = ssl.TLSVersion.TLSv1_2
        self.context.load_cert_chain(cert, key)
        self._seen = self._fingerprint()
        self.reloads = 0
        self.issue: str | None = None

    def _fingerprint(self) -> tuple[tuple[int, int], ...] | None:
        try:
            out = []
            for p in (self.cert, self.key):
                st = self._stat(p)
                out.append((st.st_mtime_ns, st.st_size))
            return tuple(out)
        except OSError:
            return None

    def reload_if_changed(self) -> bool:
        """True when a new pair was loaded."""
        fp = self._fingerprint()
        if fp is None:
            self.issue = "server certificate / key files unreadable (keeping the loaded pair)"
            return False
        if fp == self._seen and self.issue is None:
            return False
        try:
            # Validate the pair on a scratch context first: a failed load into the live context could leave it
            # holding the new certificate with the old key, and every handshake would fail.
            probe = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            probe.load_cert_chain(self.cert, self.key)
            self.context.load_cert_chain(self.cert, self.key)
        except (OSError, ssl.SSLError, ValueError) as exc:
            msg = f"server certificate / key not reloaded ({type(exc).__name__}); keeping the loaded pair"
            if self.issue != msg:
                log.error("%s", msg)
            self.issue = msg
            self._seen = fp
            return False
        changed = fp != self._seen or self.issue is not None
        self._seen = fp
        self.issue = None
        if changed:
            self.reloads += 1
            log.warning("server TLS certificate reloaded from its files (no restart)")
        return changed


__all__ = ["ENDPOINT_TIMEOUT_S", "EXPIRY_WARN_S", "CertReading", "ReloadingServerTls", "cert_not_after_s",
           "endpoint_not_after_s", "parse_endpoint", "parse_name_list", "read_file_certs"]
