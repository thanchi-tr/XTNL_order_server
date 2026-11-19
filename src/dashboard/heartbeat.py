"""The dashboard's copy of the leader heartbeat (operations.md §9 Dashboard; IMPLEMENTATION_PLAN 7.4).

"Redis live values are shown only when the Redis it reads has a `redis:boot_id` epoch equal to the epoch in the
latest leader heartbeat, otherwise they are marked stale." The leader POSTs a copy of every dead-man heartbeat
(`src.runtime.deadman.DeadmanSender(copies=...)`, engine variables XTNL_HEARTBEAT_COPY_URL / _TOKEN) to this
listener; `HeartbeatBoard` keeps the latest one, and `DashboardViews.live` compares each Redis's boot_id epoch
with its epoch. A heartbeat older than `max_age_ms` (default 60 s, the receiver's silence threshold) is no
heartbeat: a dead leader's Redis is marked stale once its beats stop, even while its term is still open in
PostgreSQL. No listener configured means no heartbeat, so every live value is stale (fail closed).

Settings come from one JSON file (default /etc/xtnl/dashboard/heartbeat.json, 0640 root:xtnl, beside the
dashboard's TLS files; `python -m src.dashboard --heartbeat-file`), absent = no listener:

    {"listen": "10.0.9.10:8444", "token": "<bearer token>", "tls_cert": "/etc/xtnl/dashboard/heartbeat.crt",
     "tls_key": "/etc/xtnl/dashboard/heartbeat.key", "path": "/xtnl/heartbeat", "max_age_ms": 60000}

HTTP (its own listener, so the page server stays GET / HEAD only): `POST <path>` with `Authorization: Bearer`
(constant-time compare, checked before the body is read) and a JSON heartbeat of at most 64 KiB with a
Content-Length (validated by the dead-man receiver's own parser): 204 accepted, 400 malformed, 401 bad token,
409 older than the beat already held, 411 / 413 no or too large a body. Plain http only on a loopback address.
Nothing here writes PostgreSQL or Redis; the token is never logged.

Rotation without restart (operations.md §13; K10): the listener re-reads the file every `RELOAD_S` (and soon after a
request with an unknown token) and accepts `token`, an optional `token_next`, and a replaced token for
`token_overlap_s` (default 1800 s) after it first saw it replaced (the receiver's `TokenRing`), so the engine's
XTNL_HEARTBEAT_COPY_TOKEN (re-read from its secret every 5 min, at once after a 401) can be rotated without a refused
copy: put the new token in `token_next`, switch the engine's secret, then promote it to `token`. A file that becomes
unreadable or invalid keeps the last valid tokens (logged). A renewed `tls_cert` / `tls_key` pair is loaded into the
listener in place (`ReloadingServerTls`). A changed `token_overlap_s` applies from the next re-read (to tokens replaced
from then on). `listen`, `path`, `max_age_ms` and the `tls_cert` / `tls_key` paths take a restart: a re-read that
finds one of them changed logs a warning naming the key(s), once per change.
"""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from src.deadman_receiver.evaluate import MAX_BODY_BYTES, BeatError, parse_beat
from src.deadman_receiver.tokens import (
    DEFAULT_OVERLAP_S,
    MAX_OVERLAP_S,
    MIN_OVERLAP_S,
    TokenError,
    TokenRing,
    TokenSet,
)
from src.observability.certs import ReloadingServerTls
from src.observability.http import parse_addr

log = logging.getLogger(__name__)

DEFAULT_HEARTBEAT_FILE: Final = "/etc/xtnl/dashboard/heartbeat.json"
DEFAULT_HEARTBEAT_PATH: Final = "/xtnl/heartbeat"
DEFAULT_HEARTBEAT_PORT: Final = 8444
DEFAULT_MAX_AGE_MS: Final = 60_000
MIN_TOKEN_CHARS: Final = 16
HEADER_TIMEOUT_S: Final = 10.0
BODY_TIMEOUT_S: Final = 10.0
REQUEST_TIMEOUT_S: Final = 20.0           # the whole request (request line, headers, body)
MAX_HEADER_BYTES: Final = 16_384
MAX_HEADERS: Final = 64
RELOAD_S: Final = 30.0                    # K10: heartbeat.json (tokens) and the TLS files are re-read this often
_LOOPBACK: Final = frozenset({"localhost", "127.0.0.1", "::1"})
_REASONS: Final = {204: "No Content", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found",
                   405: "Method Not Allowed", 409: "Conflict", 411: "Length Required", 413: "Payload Too Large",
                   431: "Request Header Fields Too Large"}


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


class HeartbeatConfigError(ValueError):
    """The heartbeat file is invalid (the message names the key, never a value)."""


def server_tls_context(cert: str, key: str) -> ssl.SSLContext:
    """The listener's server TLS context (TLS 1.2+). Raises OSError / ssl.SSLError on a bad or unreadable file."""
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(cert, key)
    return ctx


@dataclass(frozen=True)
class HeartbeatListenSettings:
    host: str
    port: int
    token: str = field(repr=False)
    path: str = DEFAULT_HEARTBEAT_PATH
    tls_cert: str | None = None
    tls_key: str | None = None
    max_age_ms: int = DEFAULT_MAX_AGE_MS
    token_next: str | None = field(default=None, repr=False)
    token_overlap_s: int = DEFAULT_OVERLAP_S

    @property
    def tls(self) -> bool:
        return self.tls_cert is not None and self.tls_key is not None

    @staticmethod
    def parse(doc: object) -> HeartbeatListenSettings:
        if not isinstance(doc, dict):
            raise HeartbeatConfigError("heartbeat file must hold a JSON object")
        unknown = set(doc) - {"listen", "token", "path", "tls_cert", "tls_key", "max_age_ms", "token_next",
                              "token_overlap_s"}
        if unknown:
            raise HeartbeatConfigError(f"unknown heartbeat key(s): {', '.join(sorted(map(str, unknown)))}")

        def text(key: str, required: bool = False) -> str | None:
            v = doc.get(key)
            if v is None and not required:
                return None
            if not isinstance(v, str) or not v.strip():
                raise HeartbeatConfigError(f"heartbeat key {key} must be a non-empty string")
            return v.strip()

        listen = text("listen", True)
        token = text("token", True)
        assert listen is not None and token is not None
        if len(token) < MIN_TOKEN_CHARS:
            raise HeartbeatConfigError(f"heartbeat key token must have at least {MIN_TOKEN_CHARS} characters")
        try:
            host, port = parse_addr(listen, DEFAULT_HEARTBEAT_PORT)
        except ValueError:
            raise HeartbeatConfigError("heartbeat key listen must be host:port") from None
        if not 0 < port < 65536:
            raise HeartbeatConfigError("heartbeat key listen has a bad port")
        path = text("path") or DEFAULT_HEARTBEAT_PATH
        if not path.startswith("/"):
            raise HeartbeatConfigError("heartbeat key path must start with /")
        cert, key = text("tls_cert"), text("tls_key")
        if (cert is None) != (key is None):
            raise HeartbeatConfigError("heartbeat keys tls_cert and tls_key go together")
        if cert is None and host.lower() not in _LOOPBACK:
            raise HeartbeatConfigError("heartbeat listener needs tls_cert / tls_key (plain http only on loopback)")
        max_age = doc.get("max_age_ms", DEFAULT_MAX_AGE_MS)
        if not isinstance(max_age, int) or isinstance(max_age, bool) or not 15_000 <= max_age <= 600_000:
            raise HeartbeatConfigError("heartbeat key max_age_ms must be an integer from 15000 to 600000")
        token_next = text("token_next")
        if token_next is not None and len(token_next) < MIN_TOKEN_CHARS:
            raise HeartbeatConfigError(f"heartbeat key token_next must have at least {MIN_TOKEN_CHARS} characters")
        overlap = doc.get("token_overlap_s", DEFAULT_OVERLAP_S)
        if (not isinstance(overlap, int) or isinstance(overlap, bool)
                or not MIN_OVERLAP_S <= overlap <= MAX_OVERLAP_S):
            raise HeartbeatConfigError(f"heartbeat key token_overlap_s must be an integer from {MIN_OVERLAP_S} to "
                                       f"{MAX_OVERLAP_S}")
        return HeartbeatListenSettings(host, port, token, path, cert, key, max_age,
                                       None if token_next == token else token_next, overlap)

    def check_files(self) -> None:
        """Load the listener's TLS certificate and key now (the unit's `--check-config`, IMPLEMENTATION_PLAN 8.2),
        so a file the `xtnl` user cannot read, or a key that does not match, is a configuration error (exit 78)
        rather than a crash loop at start. HeartbeatConfigError names the key, never a path's contents."""
        if not self.tls:
            return
        assert self.tls_cert is not None and self.tls_key is not None
        try:
            server_tls_context(self.tls_cert, self.tls_key)
        except (OSError, ssl.SSLError, ValueError) as exc:
            raise HeartbeatConfigError(f"heartbeat keys tls_cert / tls_key: files unreadable or invalid "
                                       f"({type(exc).__name__})") from None

    @staticmethod
    def from_file(path: str | Path) -> HeartbeatListenSettings | None:
        """The settings, or None when the file does not exist (no listener). HeartbeatConfigError otherwise."""
        p = Path(path)
        try:
            raw = p.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise HeartbeatConfigError(f"heartbeat file unreadable ({type(exc).__name__})") from None
        try:
            doc: Any = json.loads(raw)
        except ValueError:
            raise HeartbeatConfigError("heartbeat file is not JSON") from None
        return HeartbeatListenSettings.parse(doc)


@dataclass(frozen=True)
class LeaderBeat:
    node: str
    epoch: str | None
    sent_at_ms: int
    received_ms: int

    @property
    def epoch_revision(self) -> int | None:
        """The fencing token (`Epoch.label` is the lease revision); None when absent or not an integer."""
        if self.epoch is None:
            return None
        try:
            return int(self.epoch)
        except ValueError:
            return None


class HeartbeatBoard:
    """The latest leader heartbeat this dashboard received (in memory; one per process)."""

    def __init__(self, *, max_age_ms: int = DEFAULT_MAX_AGE_MS, clock_ms: Callable[[], int] = _wall_ms,
                 configured: bool = True) -> None:
        self.max_age_ms = max_age_ms
        self.configured = configured
        self._clock = clock_ms
        self._latest: LeaderBeat | None = None
        self.accepted = 0
        self.refused = 0

    def offer(self, node: str, epoch: str | None, sent_at_ms: int) -> bool:
        """Keep the beat unless it is older (by its send time) than a still-fresh beat already held."""
        now = self._clock()
        cur = self._latest
        if cur is not None and now - cur.received_ms <= self.max_age_ms and sent_at_ms < cur.sent_at_ms:
            self.refused += 1
            return False
        self._latest = LeaderBeat(node, epoch, sent_at_ms, now)
        self.accepted += 1
        return True

    def latest(self) -> LeaderBeat | None:
        return self._latest

    def current(self, now_ms: int) -> tuple[LeaderBeat | None, str]:
        """(the latest beat when it is fresh, else None; why there is none)."""
        if not self.configured:
            return None, "no leader heartbeat listener configured"
        beat = self._latest
        if beat is None:
            return None, "no leader heartbeat received yet"
        age = now_ms - beat.received_ms
        if age > self.max_age_ms:
            return None, f"latest leader heartbeat ({beat.node}, epoch {beat.epoch}) is {age // 1000} s old"
        return beat, ""


#: Keys of heartbeat.json read only at start (`HeartbeatListenSettings` field names; `listen` is host + port).
RESTART_ONLY_FIELDS: Final = ("host", "port", "path", "max_age_ms", "tls_cert", "tls_key")
_FIELD_KEY: Final = {"host": "listen", "port": "listen"}


def restart_only_changes(running: HeartbeatListenSettings, fresh: HeartbeatListenSettings) -> list[str]:
    """The heartbeat.json keys whose re-read value differs from the running one but needs a restart (sorted)."""
    return sorted({_FIELD_KEY.get(f, f) for f in RESTART_ONLY_FIELDS if getattr(running, f) != getattr(fresh, f)})


def _file_tokens(doc: Mapping[str, str]) -> TokenSet:
    """The ring's parser for the re-read heartbeat file (`watch` hands it the parsed tokens)."""
    token, nxt = doc.get("token"), doc.get("token_next") or None
    if not token or len(token) < MIN_TOKEN_CHARS:
        raise TokenError(f"heartbeat key token must have at least {MIN_TOKEN_CHARS} characters")
    return TokenSet(token, None if nxt == token else nxt)


class HeartbeatListener:
    """The POST endpoint feeding a HeartbeatBoard. With `settings_file`, `watch()` keeps the tokens and the TLS
    files current without a restart (K10)."""

    def __init__(self, settings: HeartbeatListenSettings, board: HeartbeatBoard, *,
                 settings_file: str | Path | None = None, clock_ms: Callable[[], int] = _wall_ms) -> None:
        self._s = settings
        self.board = board
        self._file = None if settings_file is None else Path(settings_file)
        self._clock = clock_ms
        self._latest: Mapping[str, str] | Exception = self._doc(settings)
        self._reread = asyncio.Event()
        self.tokens = TokenRing(TokenSet(settings.token, settings.token_next), load=self._load,
                                invalidate=self._reread.set, overlap_ms=settings.token_overlap_s * 1000,
                                parse=_file_tokens, label="dashboard heartbeat")
        self._tls: ReloadingServerTls | None = None
        self._restart_pending: list[str] = []    # restart-only keys changed in the file (warned once per change)

    @staticmethod
    def _doc(s: HeartbeatListenSettings) -> dict[str, str]:
        return {"token": s.token, **({"token_next": s.token_next} if s.token_next else {})}

    def _load(self) -> Mapping[str, str]:
        if isinstance(self._latest, Exception):
            raise self._latest
        return self._latest

    def authorized(self, header: str | None) -> bool:
        """Constant-time compare against every accepted token (current, next, a replaced one in its overlap)."""
        return self.tokens.authorized(header, self._clock())

    async def reload_once(self) -> None:
        """Re-read the settings file (in a thread) into the token ring, and re-load changed TLS files."""
        if self._file is not None:
            path = self._file
            try:
                fresh = await asyncio.to_thread(HeartbeatListenSettings.from_file, path)
                self._latest = (HeartbeatConfigError("heartbeat file missing") if fresh is None
                                else self._doc(fresh))
            except HeartbeatConfigError as exc:
                self._latest = exc
            else:
                if fresh is not None:
                    self._apply_live_keys(fresh)
        self.tokens.refresh(self._clock())
        if self._tls is not None:
            self._tls.reload_if_changed()

    def _apply_live_keys(self, fresh: HeartbeatListenSettings) -> None:
        """K10: `token_overlap_s` follows the file; a changed restart-only key is logged (key names only)."""
        overlap_ms = fresh.token_overlap_s * 1000
        if overlap_ms != self.tokens.overlap_ms:
            log.info("dashboard heartbeat token_overlap_s now %d s", fresh.token_overlap_s)
            self.tokens.overlap_ms = overlap_ms
        changed = restart_only_changes(self._s, fresh)
        if changed and changed != self._restart_pending:
            log.warning("dashboard heartbeat key(s) %s changed in the settings file; they take effect only after a "
                        "restart of the dashboard", ", ".join(changed))
        self._restart_pending = changed

    @property
    def restart_pending(self) -> list[str]:
        """The restart-only keys the last re-read found changed (empty when the file matches the running ones)."""
        return list(self._restart_pending)

    async def watch(self, stop: asyncio.Event, every_s: float = RELOAD_S) -> None:
        """Every `every_s`, and soon after a request with an unknown token (the ring throttles that to once a
        minute), re-read the file. Never raises (a failed read keeps the last valid tokens)."""
        while not stop.is_set():
            try:
                await self.reload_once()
            except Exception as exc:
                log.error("dashboard heartbeat settings not re-read: %s", type(exc).__name__)
            self._reread.clear()
            waiters = [asyncio.ensure_future(stop.wait()), asyncio.ensure_future(self._reread.wait())]
            try:
                await asyncio.wait(waiters, timeout=every_s, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for w in waiters:
                    w.cancel()

    def on_body(self, body: bytes) -> tuple[int, str]:
        try:
            beat = parse_beat(body)
        except BeatError as exc:
            log.warning("leader heartbeat copy rejected: %s", exc)
            return 400, str(exc)
        if not self.board.offer(beat.node, beat.epoch, beat.sent_at_ms):
            return 409, "older than the heartbeat held"
        return 204, ""

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        status, text = 400, "bad request"
        try:
            # One deadline for the whole request: a client trickling header lines never holds a connection for
            # MAX_HEADERS x HEADER_TIMEOUT_S.
            status, text = await asyncio.wait_for(self._request(reader), REQUEST_TIMEOUT_S)
        except (TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, OSError, ValueError):
            # OSError covers ConnectionError and ssl.SSLError: the connection is still answered and closed below.
            status, text = 400, "bad request"
        try:
            payload = b"" if status == 204 else text.encode("utf-8")
            head = (f"HTTP/1.1 {status} {_REASONS.get(status, 'Error')}\r\nContent-Type: text/plain; charset=utf-8\r\n"
                    f"Content-Length: {len(payload)}\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n")
            writer.write(head.encode("ascii") + payload)
            await asyncio.wait_for(writer.drain(), BODY_TIMEOUT_S)
        except (TimeoutError, ConnectionError, OSError):
            pass
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), 2.0)
            except (TimeoutError, ConnectionError, OSError, ssl.SSLError):
                pass

    async def _request(self, reader: asyncio.StreamReader) -> tuple[int, str]:
        line = await asyncio.wait_for(reader.readline(), HEADER_TIMEOUT_S)
        parts = line.decode("latin-1").split()
        if len(parts) != 3 or not parts[2].startswith("HTTP/1."):
            return 400, "bad request line"
        method, target = parts[0], parts[1].split("?", 1)[0]
        headers: dict[str, str] = {}
        total = len(line)
        while True:
            raw = await asyncio.wait_for(reader.readline(), HEADER_TIMEOUT_S)
            total += len(raw)
            if total > MAX_HEADER_BYTES or len(headers) > MAX_HEADERS:
                return 431, "headers too large"
            if raw in (b"\r\n", b"\n", b""):
                break
            name, sep, value = raw.decode("latin-1").partition(":")
            if not sep:
                return 400, "bad header"
            headers[name.strip().lower()] = value.strip()
        if target != self._s.path:
            return 404, "not found"
        if method != "POST":
            return 405, "method not allowed"
        if not self.authorized(headers.get("authorization")):
            log.warning("leader heartbeat copy with a missing or wrong bearer token refused")
            return 401, "unauthorized"
        if "transfer-encoding" in headers:
            return 411, "content-length required"
        try:
            length = int(headers.get("content-length", ""))
        except ValueError:
            return 411, "content-length required"
        if length < 0:
            return 400, "bad content-length"
        if length > MAX_BODY_BYTES:
            return 413, "body too large"
        body = await asyncio.wait_for(reader.readexactly(length), BODY_TIMEOUT_S)
        return self.on_body(body)

    def ssl_context(self) -> ssl.SSLContext | None:
        """The listener's TLS context, re-loaded in place by `reload_once` when its files change (K10)."""
        if not self._s.tls:
            return None
        assert self._s.tls_cert is not None and self._s.tls_key is not None
        if self._tls is None:
            self._tls = ReloadingServerTls(self._s.tls_cert, self._s.tls_key)
        return self._tls.context

    async def start(self) -> asyncio.Server:
        server = await asyncio.start_server(self.handle, self._s.host, self._s.port, ssl=self.ssl_context(),
                                            limit=MAX_HEADER_BYTES)
        log.info("leader heartbeat listener on %s:%d%s (%s)", self._s.host, self._s.port, self._s.path,
                 "https" if self._s.tls else "http, loopback only")
        return server


__all__ = ["DEFAULT_HEARTBEAT_FILE", "DEFAULT_MAX_AGE_MS", "HeartbeatBoard", "HeartbeatConfigError",
           "HeartbeatListenSettings", "HeartbeatListener", "LeaderBeat"]
