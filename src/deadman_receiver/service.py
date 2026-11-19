"""The receiver service: HTTP(S) endpoint for the engine heartbeat, the 1 s evaluation tick and the paging.

HTTP (stdlib asyncio streams, HTTP/1.1, one request per connection, `Connection: close`):

* `POST <path>` with `Authorization: Bearer <token>` (constant-time compare against every accepted token: the
  current one, an optional next one and, for the overlap window, a replaced one; `TokenRing`) and a JSON body of at most 64 KiB
  with a Content-Length: 204 accepted; 400 malformed (the reason names fields only); 401 bad token (checked
  before the body is read); 409 not newer than the last accepted beat; 411 / 413 for a missing / large body.
* `GET /healthz`: 200 `ok` (reachability only; no state without the token). Anything else: 404 / 405.

Each tick (1 s) first re-reads the tokens from the secrets source (no I/O; a rotation needs no restart, and an
invalid secret keeps the last valid tokens and pages `token_source`), then compares the active conditions (`Evaluator.active`) with the previous set: a new condition is
paged (critical), a condition still active is re-paged every `repage_s`, a cleared one sends a resolved notice
(info). A daily test page at `test_page_utc` proves the path (operations.md §9), and a start-up notice shows
receiver restarts. Nothing here reads or writes PostgreSQL or Redis, and nothing reaches a broker.

K5 / K10 (operations.md §13): every `CERT_CHECK_S` the tick re-loads a changed TLS certificate / key in place
(`ReloadingServerTls`; a pair that fails to load keeps the current one and pages `tls_reload`) and reads the watched
certificates (`settings.certs`, its own included): one unreadable or within 30 days of expiry pages `cert:<name>`.
With a `channels_source`, every `CHANNEL_REFRESH_S` the paging channel settings are re-read from the cached secret
and changed ones rebuild the transports (`Pager.replace_transports`); a source with no usable channel keeps the
current transports and pages `channel_source`.
"""
from __future__ import annotations

import asyncio
import logging
import ssl
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Final

from src.deadman_receiver.evaluate import MAX_BODY_BYTES, BeatError, Condition, Evaluator, Thresholds, parse_beat
from src.deadman_receiver.pager import Pager
from src.deadman_receiver.settings import ReceiverSettings
from src.deadman_receiver.tokens import TokenRing, TokenSet
from src.notifications.settings import NotificationSettings
from src.observability.certs import CertReading, ReloadingServerTls, read_file_certs
from src.oversee.notify import AlertKind, AlertMessage, AlertTransport, Severity

log = logging.getLogger(__name__)

TICK_S: Final = 1.0
HEADER_TIMEOUT_S: Final = 10.0
BODY_TIMEOUT_S: Final = 10.0
MAX_HEADER_BYTES: Final = 16_384
MAX_HEADERS: Final = 64
_REASONS: Final = {200: "OK", 204: "No Content", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found",
                   405: "Method Not Allowed", 409: "Conflict", 411: "Length Required", 413: "Payload Too Large",
                   431: "Request Header Fields Too Large"}
SUBJECT_TAG: Final = "[XTNL DEADMAN]"
CERT_CHECK_S: Final = 60            # K5 / K10: certificate expiry check and TLS file reload
CHANNEL_REFRESH_S: Final = 60       # K10: paging channel credentials re-read from the cached secret


def wall_ms() -> int:
    return time.time_ns() // 1_000_000


def next_daily_ms(now_ms: int, hhmm: tuple[int, int]) -> int:
    """The next HH:MM UTC strictly after `now_ms`."""
    now = datetime.fromtimestamp(now_ms / 1000, tz=UTC)
    at = now.replace(hour=hhmm[0], minute=hhmm[1], second=0, microsecond=0)
    if at <= now:
        at += timedelta(days=1)
    return int(at.timestamp() * 1000)


class ReceiverService:
    def __init__(self, settings: ReceiverSettings, pager: Pager, *, clock_ms: Callable[[], int] = wall_ms,
                 thresholds: Thresholds | None = None, tokens: TokenRing | None = None,
                 channels_source: Callable[[], NotificationSettings] | None = None,
                 transports_for: Callable[[NotificationSettings], list[AlertTransport]] | None = None,
                 read_certs: Callable[[], list[CertReading]] | None = None) -> None:
        self._s = settings
        self._pager = pager
        self._clock = clock_ms
        # K10: rotation without restart of the paging channel credentials and of the server certificate.
        self._channels_source = channels_source
        self._transports_for = transports_for
        self._channels = (settings.channels.email, settings.channels.push)
        self._channel_issue: str | None = None
        self._next_channel_ms = 0
        self._tls: ReloadingServerTls | None = None
        # K5: the watched certificates (its own TLS certificate included).
        certs = dict(settings.certs)
        self._read_certs = read_certs or (lambda: read_file_certs(certs))
        self._cert_problems: dict[str, str] = {}
        self._next_cert_ms = 0
        now = clock_ms()
        self.evaluator = Evaluator(started_ms=now, thresholds=thresholds or Thresholds())
        self.tokens = tokens or TokenRing(TokenSet(settings.token, settings.token_next),
                                          overlap_ms=settings.token_overlap_s * 1000)
        self._active: dict[str, Condition] = {}
        self._last_paged: dict[str, int] = {}
        self._next_test_ms = next_daily_ms(now, settings.test_page_utc)
        self.pages: list[AlertMessage] = []          # what this process paged (bounded; status / tests)
        self._started_notice = False

    # ------------------------------------------------------------------ heartbeat input
    def on_beat(self, body: bytes) -> tuple[int, str]:
        """Handle one authenticated POST body. Returns (HTTP status, short text)."""
        try:
            beat = parse_beat(body)
        except BeatError as exc:
            self.evaluator.reject(str(exc))
            log.warning("dead-man heartbeat rejected: %s", exc)
            return 400, str(exc)
        try:
            self.evaluator.accept(beat, self._clock())
        except BeatError as exc:
            log.warning("dead-man heartbeat from %s rejected: %s", beat.node, exc)
            return (409 if "not newer" in str(exc) else 400), str(exc)
        return 204, ""

    def authorized(self, header: str | None) -> bool:
        return self.tokens.authorized(header, self._clock())

    # ------------------------------------------------------------------ paging
    def _msg(self, subject: str, body: str, severity: Severity, kind: AlertKind) -> AlertMessage:
        msg = AlertMessage(f"{SUBJECT_TAG} {subject}", body, severity, kind)
        self.pages.append(msg)
        del self.pages[:-200]
        return msg

    def _body(self, lines: list[str], now_ms: int) -> str:
        stamp = datetime.fromtimestamp(now_ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        return "\n".join([*lines, "", f"receiver time: {stamp}", self.evaluator.status_line(now_ms),
                          "runbook: docs/runbooks/deadman-alert.md",
                          "Phase 1 never sends orders: this means oversight may be blind, not that an order is at risk."])

    async def tick(self) -> None:
        """Evaluate, then send this tick's pages concurrently (each delivery is bounded by the Pager's timeout),
        so several conditions changing at once cost one delivery round, not one per condition."""
        now = self._clock()
        out: list[AlertMessage] = []
        if not self._started_notice:
            self._started_notice = True
            out.append(self._msg("receiver started", self._body(
                [f"dead-man receiver started; paging via {', '.join(self._pager.backends)}"], now), "info", "info"))
        self.tokens.refresh(now)
        self._refresh_channels(now)
        self._check_certs(now)
        active = self.evaluator.active(now)
        if self.tokens.issue is not None:
            active["token_source"] = Condition(
                "token_source", "dead-man: receiver token source invalid",
                f"{self.tokens.issue}; the last valid tokens stay accepted (deadman-alert.md, token rotation)")
        if self._channel_issue is not None:
            active["channel_source"] = Condition(
                "channel_source", "dead-man: receiver paging channel source invalid",
                f"{self._channel_issue}; the current channels stay in use (deadman-alert.md)")
        if self._tls is not None and self._tls.issue is not None:
            active["tls_reload"] = Condition(
                "tls_reload", "dead-man: receiver TLS certificate not reloaded", self._tls.issue)
        for name, problem in self._cert_problems.items():
            active[f"cert:{name}"] = Condition(
                f"cert:{name}", f"dead-man: {problem}",
                f"{problem}; renew it from the internal CA (off-node-alerts.md XtnlCertExpiry)")
        for key, cond in active.items():
            last = self._last_paged.get(key)
            if key not in self._active:
                log.error("dead-man condition active: %s (%s)", cond.summary, cond.detail)
                out.append(self._msg(cond.summary, self._body([cond.detail], now), "critical", "episode"))
                self._last_paged[key] = now
            elif last is not None and now - last >= self._s.repage_s * 1000:
                out.append(self._msg(f"still active: {cond.summary}", self._body([cond.detail], now),
                                     "critical", "repage"))
                self._last_paged[key] = now
        for key, cond in self._active.items():
            if key not in active:
                log.warning("dead-man condition resolved: %s", cond.summary)
                out.append(self._msg(f"resolved: {cond.summary}", self._body([cond.detail], now), "info", "info"))
                self._last_paged.pop(key, None)
        self._active = active
        if now >= self._next_test_ms:
            self._next_test_ms = next_daily_ms(now, self._s.test_page_utc)
            out.append(self._msg("daily test page", self._body(
                ["daily test page: the Melbourne dead-man paging path works",
                 f"active conditions: {', '.join(sorted(active)) or 'none'}"], now), "high", "info"))
        if out:
            await asyncio.gather(*(self._pager.page(m) for m in out))
        await self._pager.retry_due()

    def _refresh_channels(self, now: int) -> None:
        """K10: rebuild the paging transports when the channel settings in the cached secret change."""
        if self._channels_source is None or self._transports_for is None or now < self._next_channel_ms:
            return
        self._next_channel_ms = now + CHANNEL_REFRESH_S * 1000
        try:
            channels = self._channels_source()
        except Exception as exc:                 # the source itself: keep the current channels
            self._channel_issue = f"channel source unreadable ({type(exc).__name__})"
            return
        if channels.email is None and channels.push is None:
            self._channel_issue = "no paging channel outside eu-west-2 in the source"
            return
        self._channel_issue = None
        key = (channels.email, channels.push)
        if key == self._channels:
            return
        try:
            self._pager.replace_transports(self._transports_for(channels))
        except Exception as exc:
            self._channel_issue = f"paging channels not rebuilt ({type(exc).__name__})"
            return
        self._channels = key
        log.warning("dead-man receiver paging channels reloaded (rotated credentials): %s",
                    ", ".join(self._pager.backends))

    def _check_certs(self, now: int) -> None:
        """K5 / K10: reload a changed TLS certificate in place, and read the watched certificates."""
        if now < self._next_cert_ms:
            return
        self._next_cert_ms = now + CERT_CHECK_S * 1000
        if self._tls is not None:
            self._tls.reload_if_changed()
        try:
            readings = self._read_certs()
        except Exception as exc:
            log.error("dead-man receiver certificate check failed: %s", type(exc).__name__)
            return
        problems: dict[str, str] = {}
        for r in readings:
            problem = r.problem(now / 1000.0)
            if problem is not None:
                problems[r.name] = problem
        self._cert_problems = problems

    @property
    def active(self) -> dict[str, Condition]:
        return dict(self._active)

    # ------------------------------------------------------------------ HTTP
    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        status, text = 400, "bad request"
        try:
            status, text = await self._request(reader)
        except (TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, ConnectionError, ValueError):
            status, text = 400, "bad request"
        try:
            payload = text.encode("utf-8")
            head = (f"HTTP/1.1 {status} {_REASONS.get(status, 'Error')}\r\nContent-Type: text/plain; charset=utf-8\r\n"
                    f"Content-Length: {len(payload)}\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n")
            writer.write(head.encode("ascii") + (b"" if status == 204 else payload))
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
        if target == "/healthz":
            return (200, "ok\n") if method in ("GET", "HEAD") else (405, "method not allowed")
        if target != self._s.path:
            return 404, "not found"
        if method != "POST":
            return 405, "method not allowed"
        if not self.authorized(headers.get("authorization")):
            log.warning("dead-man heartbeat with a missing or wrong bearer token refused")
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
        return self.on_beat(body)

    def ssl_context(self) -> ssl.SSLContext | None:
        """The listener's TLS context, re-loaded in place when the certificate / key files change (K10)."""
        if self._s.tls_cert is None or self._s.tls_key is None:
            return None
        if self._tls is None:
            self._tls = ReloadingServerTls(self._s.tls_cert, self._s.tls_key)
        return self._tls.context

    async def run(self, stop: asyncio.Event) -> None:
        server = await asyncio.start_server(self.handle, self._s.host, self._s.port, ssl=self.ssl_context(),
                                            limit=MAX_HEADER_BYTES)
        log.info("dead-man receiver listening on %s:%d%s (%s), paging via %s", self._s.host, self._s.port,
                 self._s.path, "https" if self._s.tls else "http, loopback only", ", ".join(self._pager.backends))
        try:
            while not stop.is_set():
                try:
                    await self.tick()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("dead-man receiver tick failed")
                try:
                    await asyncio.wait_for(stop.wait(), TICK_S)
                except TimeoutError:
                    pass
        finally:
            server.close()
            await server.wait_closed()
