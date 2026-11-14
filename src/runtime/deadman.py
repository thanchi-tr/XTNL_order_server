"""Push dead-man's switch (operations.md §9).

Every 15 s the leader POSTs a heartbeat to the Melbourne receiver, which pages (through a channel
outside eu-west-2) after 60 s of silence, when a field is missing, when an adapter has not been
STREAMING for 2 min while its `market_open` is true and it is not in maintenance, when an age passes
its threshold, or when any task circuit is open. Missing values are sent as JSON null, never
omitted, so the receiver can page on them. Sending is an externally visible action: it first checks
the lease synchronously (§2) and only the leader sends.

Transport: `HttpsDeadmanTransport` POSTs the JSON payload over HTTPS (plain http only to a loopback
receiver) with an optional bearer token (XTNL_DEADMAN_TOKEN), one persistent httpx client and a 5 s
timeout. Any 2xx is success. Logs name the receiver host and the failure class only, never the URL
path (which may carry a check id) or the token.

Token rotation without restart (operations.md §13, IMPLEMENTATION_PLAN 7.4): with `token_source` the bearer token
is read for every POST from the secrets source's cached secret (no I/O); a source that raises keeps the last good
token. A 401 / 403 asks the secrets source to re-read the secret (`on_auth_error`, at most once a minute), so a
token rotated at the receiver first is picked up within one refresh instead of after an engine restart.

Heartbeat copy (IMPLEMENTATION_PLAN 7.4, operations.md §9 Dashboard): `DeadmanSender(copies=...)` POSTs the same
payload to further transports (the Melbourne dashboard's heartbeat listener, src/dashboard/heartbeat.py), after the
same lease checks. A copy is best effort: its failure is logged (throttled) and never counts as a dead-man failure.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from urllib.parse import urlsplit
from dataclasses import asdict, dataclass
from typing import Any, Final, Protocol

import httpx

log = logging.getLogger(__name__)

DEADMAN_INTERVAL_S: Final = 15.0
DEADMAN_TIMEOUT_S: Final = 5.0


@dataclass(frozen=True)
class AdapterBeat:
    sm4_state: str | None
    market_open: bool | None            # SM10
    maintenance_until_ms: int | None    # end of the active BROKER_MAINTENANCE window, else null


@dataclass(frozen=True)
class DeadmanPayload:
    node: str
    epoch: str | None
    sent_at_ms: int
    gate_state: str | None
    halt_level: str | None
    adapters: Mapping[str, AdapterBeat]
    last_committed_broker_event_age_ms: int | None
    oldest_unacked_feed_age_ms: int | None
    oldest_risksweep_pass_age_ms: int | None   # the oldest per-account RiskSweep pass age
    calendar_version: int | None
    oldest_undelivered_notification_age_ms: int | None
    sm8_state: str | None
    sm5_state: str | None
    spill_bytes: int | None
    degraded: Sequence[str]

    def to_json(self) -> str:
        d: dict[str, Any] = asdict(self)
        d["adapters"] = {k: asdict(v) for k, v in self.adapters.items()}
        d["degraded"] = list(self.degraded)
        return json.dumps(d, separators=(",", ":"), sort_keys=True)


class DeadmanSource(Protocol):
    """Read-only view of the engine for the heartbeat (wired by the runtime)."""

    def node_id(self) -> str: ...

    def epoch_label(self) -> str | None: ...

    def gate_state(self) -> str | None: ...

    def halt_level(self) -> str | None: ...

    def adapter_ids(self) -> Sequence[str]: ...

    def sm4_state(self, adapter_id: str) -> str | None: ...

    def market_open(self, adapter_id: str) -> bool | None: ...

    def maintenance_until_ms(self, adapter_id: str) -> int | None: ...

    def calendar_version(self) -> int | None: ...

    def last_committed_broker_event_ms(self) -> int | None: ...

    def oldest_unacked_feed_ms(self) -> int | None: ...

    def risksweep_pass_ms(self) -> Mapping[str, int]:
        """account_id -> end time of its last complete pass."""
        ...

    def oldest_undelivered_notification_ms(self) -> int | None: ...

    def sm8_state(self) -> str | None: ...

    def sm5_state(self) -> str | None: ...

    def spill_bytes(self) -> int | None: ...

    def degraded(self) -> Sequence[str]: ...


def _age(now_ms: int, ts: int | None) -> int | None:
    return None if ts is None else max(0, now_ms - ts)


def build_payload(src: DeadmanSource, now_ms: int) -> DeadmanPayload:
    passes = src.risksweep_pass_ms()
    oldest_pass = min(passes.values()) if passes else None
    return DeadmanPayload(
        node=src.node_id(), epoch=src.epoch_label(), sent_at_ms=now_ms, gate_state=src.gate_state(),
        halt_level=src.halt_level(),
        adapters={a: AdapterBeat(src.sm4_state(a), src.market_open(a), src.maintenance_until_ms(a))
                  for a in src.adapter_ids()},
        last_committed_broker_event_age_ms=_age(now_ms, src.last_committed_broker_event_ms()),
        oldest_unacked_feed_age_ms=_age(now_ms, src.oldest_unacked_feed_ms()),
        oldest_risksweep_pass_age_ms=_age(now_ms, oldest_pass),
        calendar_version=src.calendar_version(),
        oldest_undelivered_notification_age_ms=_age(now_ms, src.oldest_undelivered_notification_ms()),
        sm8_state=src.sm8_state(), sm5_state=src.sm5_state(), spill_bytes=src.spill_bytes(),
        degraded=list(src.degraded()),
    )


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


_LOOPBACK: Final = frozenset({"localhost", "127.0.0.1", "::1"})


def check_deadman_url(url: str) -> str:
    """The URL's host; ValueError unless https (or http to a loopback receiver)."""
    try:
        parts = urlsplit(url)
    except ValueError:
        raise ValueError("dead-man URL is not a valid URL") from None
    host = parts.hostname
    if not host or parts.scheme not in ("https", "http"):
        raise ValueError("dead-man URL must be https")
    if parts.scheme == "http" and host.lower() not in _LOOPBACK:
        raise ValueError("dead-man URL must be https (plain http only to a loopback receiver)")
    return host


class DeadmanError(Exception):
    """One heartbeat POST failed; the message is safe to log."""


class DeadmanTransport(Protocol):
    @property
    def host(self) -> str: ...

    async def post(self, payload: DeadmanPayload) -> None: ...

    async def aclose(self) -> None: ...


#: A rejected token re-reads the secret at most this often (a receiver that keeps refusing never hammers it).
AUTH_REFRESH_MIN_INTERVAL_S: Final = 60.0


class HttpsDeadmanTransport:
    """HTTPS POST of the heartbeat JSON. `client`: shared / test client (the caller closes it);
    otherwise one persistent client is created on first use and closed by `aclose()`.

    `token_source`: the current bearer token, read for every POST (None result: no Authorization header); it
    replaces the fixed `token` and must not do I/O. `on_auth_error`: called (throttled) on HTTP 401 / 403, e.g.
    `SecretsSource.invalidate` so a rotated token is read at once."""

    def __init__(self, url: str, *, token: str | None = None, client: httpx.AsyncClient | None = None,
                 timeout_s: float = DEADMAN_TIMEOUT_S, token_source: Callable[[], str | None] | None = None,
                 on_auth_error: Callable[[], None] | None = None,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        self.host = check_deadman_url(url)
        self._url = url
        self._token = token
        self._token_source = token_source
        self._on_auth_error = on_auth_error
        self._mono = monotonic
        self._last_auth_refresh: float | None = None
        self._client = client
        self._owned: httpx.AsyncClient | None = None
        self._timeout = timeout_s
        self.auth_refreshes = 0

    def _current_token(self) -> str | None:
        """The token for this POST: the source's current value (remembered as the last good one), or the last
        good one when the source fails (only the error class is logged)."""
        if self._token_source is None:
            return self._token
        try:
            self._token = self._token_source()
        except Exception as exc:
            log.warning("dead-man token for %s not re-read (keeping the last one): %s", self.host,
                        type(exc).__name__)
        return self._token

    def _auth_rejected(self) -> None:
        if self._on_auth_error is None:
            return
        now = self._mono()
        if self._last_auth_refresh is not None and now - self._last_auth_refresh < AUTH_REFRESH_MIN_INTERVAL_S:
            return
        self._last_auth_refresh = now
        self.auth_refreshes += 1
        log.error("%s refused the heartbeat bearer token; re-reading the secret", self.host)
        try:
            self._on_auth_error()
        except Exception as exc:
            log.warning("heartbeat token refresh request failed: %s", type(exc).__name__)

    def __repr__(self) -> str:
        return f"HttpsDeadmanTransport(host={self.host!r})"

    def _http(self) -> httpx.AsyncClient:
        if self._client is not None:
            return self._client
        if self._owned is None:
            self._owned = httpx.AsyncClient(timeout=self._timeout)
        return self._owned

    async def post(self, payload: DeadmanPayload) -> None:
        headers = {"content-type": "application/json", "user-agent": "xtnl-order-server-deadman"}
        token = self._current_token()
        if token:
            headers["authorization"] = f"Bearer {token}"
        try:
            resp = await self._http().post(self._url, content=payload.to_json().encode("utf-8"), headers=headers,
                                           timeout=self._timeout)
        except httpx.HTTPError as exc:
            raise DeadmanError(f"{type(exc).__name__}") from None
        if resp.status_code in (401, 403):
            self._auth_rejected()
        if not 200 <= resp.status_code < 300:
            raise DeadmanError(f"HTTP {resp.status_code}")

    async def aclose(self) -> None:
        if self._owned is not None:
            await self._owned.aclose()
            self._owned = None


class DeadmanSender:
    """Decides when to beat (every `interval_s`, leader only, lease checked right before the POST) and
    hands the payload to a `DeadmanTransport` (HTTPS by default, built from `url`)."""

    def __init__(self, url: str | None, source: DeadmanSource, *, lease_valid: Callable[[], bool],
                 is_leader: Callable[[], bool], client: httpx.AsyncClient | None = None,
                 interval_s: float = DEADMAN_INTERVAL_S, timeout_s: float = DEADMAN_TIMEOUT_S,
                 clock_ms: Callable[[], int] = _wall_ms, token: str | None = None,
                 transport: DeadmanTransport | None = None,
                 token_source: Callable[[], str | None] | None = None,
                 on_auth_error: Callable[[], None] | None = None,
                 copies: Sequence[DeadmanTransport] = ()) -> None:
        if transport is None:
            if url is None:
                raise ValueError("dead-man sender needs a URL or a transport")
            transport = HttpsDeadmanTransport(url, token=token, client=client, timeout_s=timeout_s,
                                              token_source=token_source, on_auth_error=on_auth_error)
        self._transport = transport
        self._copies = tuple(copies)
        # A copy never holds up the dead-man beat: each copy POST is cut off after this (httpx's timeout is per
        # phase, so connect + write + read to a hung dashboard could otherwise stretch every beat by ~3x it).
        self._copy_timeout_s = timeout_s
        self._copy_failures: dict[str, int] = {}
        self.copies_sent = 0
        self._src = source
        self._lease_valid = lease_valid
        self._is_leader = is_leader
        self._interval = interval_s
        self._clock_ms = clock_ms
        self.sent = 0
        self.failures = 0
        self.skipped = 0
        self.consecutive_failures = 0
        self.last_ok_ms: int | None = None

    async def send_once(self) -> bool:
        """Build and POST one heartbeat. False when skipped (not leader / lease invalid) or failed."""
        if not self._is_leader() or not self._lease_valid():
            self.skipped += 1
            return False
        payload = build_payload(self._src, self._clock_ms())
        if not self._lease_valid():       # synchronous check right before the externally visible send
            self.skipped += 1
            return False
        if not self._copies:
            return await self._post_main(payload)
        results = await asyncio.gather(self._post_main(payload), *(self._post_copy(c, payload) for c in self._copies))
        return results[0]

    async def _post_copy(self, copy: DeadmanTransport, payload: DeadmanPayload) -> bool:
        """Best effort: a failed copy is logged (its first failure, then every 20th) and never counts as a
        dead-man failure. Bounded by the sender's timeout, so a hung copy never delays the next beat."""
        try:
            await asyncio.wait_for(copy.post(payload), self._copy_timeout_s)
        except Exception as exc:
            n = self._copy_failures.get(copy.host, 0) + 1
            self._copy_failures[copy.host] = n
            if n == 1 or n % 20 == 0:
                log.warning("heartbeat copy to %s failed (%d in a row): %s", copy.host, n,
                            exc if isinstance(exc, DeadmanError) else type(exc).__name__)
            return False
        if self._copy_failures.pop(copy.host, 0):
            log.info("heartbeat copy to %s recovered", copy.host)
        self.copies_sent += 1
        return True

    async def _post_main(self, payload: DeadmanPayload) -> bool:
        try:
            await self._transport.post(payload)
        except DeadmanError as exc:
            self.failures += 1
            self.consecutive_failures += 1
            # the receiver pages after 60 s of silence: 4 missed beats is an error, not a warning
            level = logging.ERROR if self.consecutive_failures >= 4 else logging.WARNING
            log.log(level, "dead-man heartbeat to %s failed (%d in a row): %s", self._transport.host,
                    self.consecutive_failures, exc)
            return False
        if self.consecutive_failures:
            log.info("dead-man heartbeat to %s recovered after %d failures", self._transport.host,
                     self.consecutive_failures)
        self.consecutive_failures = 0
        self.sent += 1
        self.last_ok_ms = payload.sent_at_ms
        return True

    async def run(self, stop: asyncio.Event, stamp: Callable[[], None] | None = None) -> None:
        try:
            while not stop.is_set():
                await self.send_once()
                if stamp is not None:
                    stamp()
                try:
                    await asyncio.wait_for(stop.wait(), self._interval)
                except TimeoutError:
                    pass
        finally:
            await self._transport.aclose()
            for c in self._copies:
                with contextlib.suppress(Exception):
                    await c.aclose()
