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

Optional ARM fields (design PR4, §B14, §C2; plan S08): `arm_oldest_undelivered_notification_age_ms`,
`arm_overdue_positions` and `arm_failed` (the number of FAILED scopes and whether one is global, from memory so it is
still sent while PostgreSQL is down). They are None until the ARM runs (S32) and are **left out of the JSON while
None**, so the payload stays byte-identical with the ARM off; the receiver treats an absent key as None (it is
deployed first and already ignores unknown keys).

Receiver-down alert (home-server plan W1): the receiver cannot page about itself, so the engine watches its own
main-beat streak. While this node beats (leader, lease valid) and the main POST has failed continuously for
`alert_after_s` (XTNL_DEADMAN_ALERT_AFTER_S, 60..3600 s, default 300), one PAGE[high] "dead-man receiver not
acknowledging" goes through `page` (the session's outbox page: leader only, lease checked again there), once per
episode; the first good beat after it sends one PAGE[info] "dead-man receiver acknowledging again". The text carries
the receiver host, the failure count, the duration and the runbook section only. The streak is timed on the
monotonic clock (a wall-clock step never moves the page). The episode is durable (operations.md §9): the high and
info pages record their times in `notification_dedupe` (`ack_episode_keys`), and a new leader term reads them before
its first POST, so a takeover or restart mid-episode neither pages the same outage again nor leaves it without its
info page. The streak itself restarts with the term (a new term needs a full `alert_after_s` of its own failures
before it pages a new episode). The page is marked sent only once it was queued, so a page path that raises or has
no valid lease is tried again on the next failed beat. The session's page path (`queue_page_durable`) awaits the
outbox insert, so "queued" means the row committed: a failed insert leaves the episode unpaged and unrecorded and
the next failed beat pages again (round-2 review). The counters are exported as gauges (`deadman_samples`,
deploy/observability/metrics.md).

Closing the durable episode (round-4 review): a stale 'open' episode would make the next term inherit it and stay
silent through a new, unrelated outage. So the 'back' row is written after the 'lost' row it closes, at
max(beat time, wall clock, lost time + 1 ms), which survives a wall-clock step back between the two; a 'back' row
that cannot be written (PostgreSQL slow or down) is retried on every following beat until it lands, as the page
itself is; and an inherited 'lost' row older than `INHERITED_EPISODE_MAX_AGE_MS` (6 h) no longer silences this
term: if this term's own beats then fail for `alert_after_s`, it pages high as a new episode (a repeat page for a
very long outage is better than a missed page for a new one).

Opening the durable episode (round-5 review): the 'lost' row is handled the same way. It is written at
max(wall clock, last 'back' time + 1 ms), where the last 'back' time is the one this sender wrote or the one its
term read from the store, so a wall-clock step back between two episodes never leaves the new 'lost' row older than
the old 'back' row (which would read as closed). A 'lost' row that cannot be written is retried on every following
beat, before the POST, until it lands. When the episode closes first, 'lost' is still written before 'back'.
"""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import os
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from urllib.parse import urlsplit
from dataclasses import asdict, dataclass
from typing import Any, Final, Protocol

import httpx

from src.db.fence import EpochMismatch
from src.observability.metrics import Sample, gauge
from src.oversee.notify import DedupeStore, EpisodeKey

log = logging.getLogger(__name__)

DEADMAN_INTERVAL_S: Final = 15.0
DEADMAN_TIMEOUT_S: Final = 5.0

#: W1: how long the main beat must fail without a break before the engine pages that the receiver is not
#: acknowledging. 60 s is the floor (4 beats: the receiver's own silence threshold, so a shorter value would page
#: on every receiver restart); 3600 s the ceiling (an hour without an off-site watcher is the most worth waiting).
ALERT_AFTER_VAR: Final = "XTNL_DEADMAN_ALERT_AFTER_S"
DEFAULT_ALERT_AFTER_S: Final = 300
MIN_ALERT_AFTER_S: Final = 60
MAX_ALERT_AFTER_S: Final = 3600
ACK_LOST_TITLE: Final = "dead-man receiver not acknowledging"
ACK_BACK_TITLE: Final = "dead-man receiver acknowledging again"
#: Where the operator looks (no "/" and no secret: the text goes to email and push).
ACK_RUNBOOK: Final = "deadman-alert.md, receiver not acknowledging"
#: notification_dedupe episode keys of W1 (rule, subject type, subject = receiver host, activation 0, level): the
#: last high page and the last info page per receiver host. Not a rule id, so no Oversee episode can collide.
ACK_EPISODE_RULE: Final = "deadman_ack"
ACK_LOST_LEVEL: Final = 2
ACK_BACK_LEVEL: Final = 0
#: Round-4 review: an inherited open episode older than this (by its stored 'lost' time) no longer stops this term
#: from paging its own outage. Long enough that a restart in the middle of an evening outage does not page it twice,
#: short enough that a stale 'open' row (its 'back' record lost with the process) cannot hide the next day's outage.
INHERITED_EPISODE_MAX_AGE_MS: Final = 6 * 3_600_000


def ack_episode_keys(host: str) -> tuple[EpisodeKey, EpisodeKey]:
    """(lost, back): the dedupe keys of the receiver-down high page and its closing info page."""
    return ((ACK_EPISODE_RULE, "receiver", host, 0, ACK_LOST_LEVEL),
            (ACK_EPISODE_RULE, "receiver", host, 0, ACK_BACK_LEVEL))


def alert_after_s_from_env(environ: Mapping[str, str] | None = None) -> int:
    """XTNL_DEADMAN_ALERT_AFTER_S: whole seconds in 60..3600 (default 300). Raises ValueError (name only), which the
    wiring turns into a configuration stop like the other bounded settings (XTNL_UPTIME_PING_S)."""
    e = os.environ if environ is None else environ
    raw = (e.get(ALERT_AFTER_VAR) or "").strip()
    if not raw:
        return DEFAULT_ALERT_AFTER_S
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{ALERT_AFTER_VAR} must be a whole number of seconds") from None
    if not MIN_ALERT_AFTER_S <= value <= MAX_ALERT_AFTER_S:
        raise ValueError(f"{ALERT_AFTER_VAR} must be between {MIN_ALERT_AFTER_S} and {MAX_ALERT_AFTER_S} s")
    return value


def deadman_token_problem(url: str | None, token: str | None) -> str | None:
    """The start-up error for a URL without a bearer token (the receiver answers 401 to every beat), else None. Names
    the variables only, never a value."""
    if url is not None and not (token or "").strip():
        return ("XTNL_DEADMAN_URL is set but XTNL_DEADMAN_TOKEN is empty: the dead-man receiver refuses every "
                "heartbeat (HTTP 401) and nothing off-site watches this engine")
    return None


@dataclass(frozen=True)
class AdapterBeat:
    sm4_state: str | None
    market_open: bool | None            # SM10
    maintenance_until_ms: int | None    # end of the active BROKER_MAINTENANCE window, else null


@dataclass(frozen=True)
class ArmFailedBeat:
    """The ARM's FAILED scopes (§B14): how many are FAILED, whether one of them is the global ARM scope, and how many
    episodes PostgreSQL has not recorded (held in this node's memory and spool only; the receiver pages on it and
    latches it across a leader change, S38c review)."""

    count: int
    global_failed: bool
    unrecorded: int = 0


#: DeadmanPayload fields sent only when set (src.deadman_receiver.evaluate.OPTIONAL_FIELDS; a test keeps them equal).
OPTIONAL_PAYLOAD_FIELDS: Final = ("arm_oldest_undelivered_notification_age_ms", "arm_overdue_positions", "arm_failed")


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
    # optional ARM fields (S08): None until the ARM runs, then sent
    arm_oldest_undelivered_notification_age_ms: int | None = None
    arm_overdue_positions: int | None = None
    arm_failed: ArmFailedBeat | None = None

    def to_json(self) -> str:
        d: dict[str, Any] = asdict(self)
        d["adapters"] = {k: asdict(v) for k, v in self.adapters.items()}
        d["degraded"] = list(self.degraded)
        for name in OPTIONAL_PAYLOAD_FIELDS:
            if d[name] is None:
                del d[name]
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

    def arm_oldest_undelivered_notification_ms(self) -> int | None:
        """Creation time of the oldest unsent ARM notice (None: the ARM is not running or nothing is unsent)."""
        ...

    def arm_overdue_positions(self) -> int | None:
        """Positions with no final decision for more than 2 OPEN bars (None: the ARM is not running)."""
        ...

    def arm_failed(self) -> ArmFailedBeat | None:
        """The ARM's FAILED scopes, from memory (None: the ARM is not running)."""
        ...


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
        arm_oldest_undelivered_notification_age_ms=_age(now_ms, src.arm_oldest_undelivered_notification_ms()),
        arm_overdue_positions=src.arm_overdue_positions(), arm_failed=src.arm_failed(),
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
                 copies: Sequence[DeadmanTransport] = (),
                 prepare: Callable[[], Awaitable[None]] | None = None,
                 page: Callable[[str, str, str], object | Awaitable[object]] | None = None,
                 alert_after_s: float = DEFAULT_ALERT_AFTER_S,
                 episodes: DedupeStore | None = None,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
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
        # K14: refreshes the source's inputs right before each payload is built (EngineSession.refresh_beat_view:
        # the halt / epoch fields of gate_state). It bounds and fails closed itself; a raise never stops the beat.
        self._prepare = prepare
        self._interval = interval_s
        self._clock_ms = clock_ms
        self.sent = 0
        self.failures = 0
        self.skipped = 0
        self.consecutive_failures = 0
        self.last_ok_ms: int | None = None
        # W1 receiver-down episode: `page(severity, title, body)` (None: no page, the log lines stay; a False result
        # means the page was not queued). The streak is timed on the monotonic clock from the first failed beat, so a
        # wall-clock step (w32time on the single PC) neither delays nor hastens the page; `failing_since_ms` (wall)
        # is kept for the logs and tests only.
        self._page = page
        self._alert_after_ms = int(alert_after_s * 1000)
        self._mono = monotonic
        self._failing_since_mono: float | None = None
        self.failing_since_ms: int | None = None
        self.ack_paged = False
        self.ack_inherited = False
        self.ack_episodes = 0
        # The episode is durable (operations.md §9: dedupe state in PostgreSQL): the high page records
        # `ack_episode_keys(host)[0]` and the info page `[1]` in notification_dedupe. A new term reads both before
        # its first POST: an open episode (lost newer than back) is not paged again, and its first acknowledged beat
        # sends the info page even though this term never paged. A read that fails is retried on the next beat.
        self._episodes = episodes
        self._episode_loaded = episodes is None
        self._episode_timeout_s = timeout_s
        # The info page's body is fixed at its first attempt (round-3 review): the failure counters are reset by the
        # acknowledged beat whether or not the page could be queued, so a retry built from the live counters would
        # report '0 failed beats over 0 s'. `_ack_lost_at_ms` is the stored 'lost' row of an inherited episode.
        self._ack_back_body: str | None = None
        self._ack_lost_at_ms: int | None = None
        # Round-4 review: the time of the 'back' row still to be written (its first attempt failed); retried each
        # beat. The 'lost' time is kept for an episode paged in this term too, so 'back' is always written after it.
        self._back_record_pending_ms: int | None = None
        # Round-5 review: the time of the 'lost' row still to be written (retried each beat, always before a pending
        # 'back'), and the last 'back' time this sender wrote or read, so a new 'lost' row always sorts after it.
        self._lost_record_pending_ms: int | None = None
        self._last_back_ms: int | None = None

    async def send_once(self) -> bool:
        """Build and POST one heartbeat. False when skipped (not leader / lease invalid) or failed."""
        if not self._is_leader() or not self._lease_valid():
            self.skipped += 1
            return False
        if self._prepare is not None:
            try:
                await self._prepare()
            except Exception as exc:          # the source then reports what it last read (fail-closed fields)
                log.warning("dead-man view refresh raised: %s", type(exc).__name__)
        await self._load_episode()
        await self._retry_episode_records()
        payload = build_payload(self._src, self._clock_ms())
        if not self._lease_valid():       # synchronous check right before the externally visible send
            self.skipped += 1
            return False
        if not self._copies:
            return await self._post_main(payload)
        results = await asyncio.gather(self._post_main(payload), *(self._post_copy(c, payload) for c in self._copies))
        return results[0]

    async def _load_episode(self) -> None:
        """Once per term (retried each beat until it succeeds): is a receiver-down episode from an earlier term
        still open? A lost fence propagates (the supervisor hard-exits); any other error only logs."""
        if self._episode_loaded or self._episodes is None:
            return
        lost, back = ack_episode_keys(self._transport.host)
        try:
            got = await asyncio.wait_for(self._episodes.last_sent_of([lost, back]), self._episode_timeout_s)
        except EpochMismatch:
            raise
        except Exception as exc:
            log.warning("dead-man receiver episode not read (retrying next beat): %s", type(exc).__name__)
            return
        self._episode_loaded = True
        lost_ms, back_ms = got.get(lost), got.get(back)
        if back_ms is not None:
            self._note_back(back_ms)
            # this term paged before the store could be read: its 'lost' row must still sort after the stored 'back'
            if (self.ack_paged and not self.ack_inherited and self._ack_lost_at_ms is not None
                    and self._ack_lost_at_ms <= back_ms):
                self._ack_lost_at_ms = back_ms + 1
                self._lost_record_pending_ms = back_ms + 1
        if lost_ms is not None and (back_ms is None or lost_ms > back_ms) and not self.ack_paged:
            self.ack_paged = True
            self.ack_inherited = True
            self._ack_lost_at_ms = lost_ms
            log.warning("dead-man receiver %s: an earlier leader term paged it as not acknowledging; the info page "
                        "follows the first acknowledged beat", self._transport.host)

    async def _record_episode(self, key: EpisodeKey, at_ms: int) -> bool:
        """True when the row was written (or there is no store). A lost fence propagates; any other error logs."""
        if self._episodes is None:
            return True
        try:
            await asyncio.wait_for(self._episodes.record(key, at_ms), self._episode_timeout_s)
        except EpochMismatch:
            raise
        except Exception as exc:          # the page is queued; only the takeover dedupe is at stake
            log.warning("dead-man receiver episode not recorded: %s", type(exc).__name__)
            return False
        return True

    def _note_back(self, back_ms: int) -> None:
        self._last_back_ms = back_ms if self._last_back_ms is None else max(self._last_back_ms, back_ms)

    async def _retry_episode_records(self) -> None:
        """Write the episode rows whose first attempt failed: 'lost' first (round-5 review: until it lands a new
        term would not inherit the paged episode), then 'back' (round-4 review: until it lands the durable episode
        reads as open, and a new term would inherit it). 'back' waits for a pending 'lost', so it is never written
        before the row it closes."""
        host = self._transport.host
        lost_key, back_key = ack_episode_keys(host)
        if self._lost_record_pending_ms is not None:
            if not await self._record_episode(lost_key, self._lost_record_pending_ms):
                return
            self._lost_record_pending_ms = None
            log.info("dead-man receiver %s: the paged episode is now recorded", host)
        if self._back_record_pending_ms is not None:
            if await self._record_episode(back_key, self._back_record_pending_ms):
                self._back_record_pending_ms = None
                log.info("dead-man receiver %s: the closed episode is now recorded", host)

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
            if self._failing_since_mono is None:
                self._failing_since_mono = self._mono()
                self.failing_since_ms = payload.sent_at_ms
            # the receiver pages after 60 s of silence: 4 missed beats is an error, not a warning
            level = logging.ERROR if self.consecutive_failures >= 4 else logging.WARNING
            log.log(level, "dead-man heartbeat to %s failed (%d in a row): %s", self._transport.host,
                    self.consecutive_failures, exc)
            await self._maybe_page_lost()
            return False
        if self.consecutive_failures:
            log.info("dead-man heartbeat to %s recovered after %d failures", self._transport.host,
                     self.consecutive_failures)
        if self.ack_paged:
            await self._maybe_page_back(payload.sent_at_ms)
        self.consecutive_failures = 0
        self._failing_since_mono = None
        self.failing_since_ms = None
        self.sent += 1
        self.last_ok_ms = payload.sent_at_ms
        return True

    def _streak_ms(self) -> int:
        if self._failing_since_mono is None:
            return 0
        return max(0, int((self._mono() - self._failing_since_mono) * 1000))

    async def _maybe_page_lost(self) -> None:
        """One PAGE[high] per episode once the streak has lasted `alert_after_s`. The leader / lease test is repeated
        here (the POST may have taken the whole timeout); when it fails, or the page cannot be queued, nothing is
        marked, so the next failed beat of a still-valid term pages instead."""
        if self._page is None or self._failing_since_mono is None:
            return
        if self.ack_paged and not self._inherited_too_old():
            return
        streak = self._streak_ms()
        if streak < self._alert_after_ms:
            return
        if not self._is_leader() or not self._lease_valid():
            return
        if not await self._send_page("high", ACK_LOST_TITLE,
                               f"receiver {self._transport.host}: {self.consecutive_failures} failed beats in a row "
                               f"over {streak // 1000} s ({ACK_RUNBOOK})"):
            return
        if self.ack_inherited:
            log.warning("dead-man receiver %s: the inherited episode is older than %d h; this term's outage was paged "
                        "as a new episode", self._transport.host, INHERITED_EPISODE_MAX_AGE_MS // 3_600_000)
        self.ack_paged = True
        self.ack_inherited = False
        self._ack_back_body = None
        self.ack_episodes += 1
        # a new 'lost' row supersedes an unwritten 'back' of the previous episode (it is open again either way)
        self._back_record_pending_ms = None
        # round-5 review: 'lost' must sort after the last 'back' (the store keeps the last write, and a wall-clock
        # step back between two episodes would otherwise leave 'lost' < 'back': a closed episode to the next term)
        lost_ms = self._clock_ms()
        if self._last_back_ms is not None:
            lost_ms = max(lost_ms, self._last_back_ms + 1)
        self._ack_lost_at_ms = lost_ms
        self._lost_record_pending_ms = lost_ms
        if await self._record_episode(ack_episode_keys(self._transport.host)[0], lost_ms):
            self._lost_record_pending_ms = None    # otherwise retried on every following beat, before the POST

    def _inherited_too_old(self) -> bool:
        """An inherited open episode whose stored 'lost' row is older than INHERITED_EPISODE_MAX_AGE_MS."""
        return (self.ack_inherited and self._ack_lost_at_ms is not None
                and self._clock_ms() - self._ack_lost_at_ms > INHERITED_EPISODE_MAX_AGE_MS)

    async def _maybe_page_back(self, ok_ms: int) -> None:
        """The info page that closes an episode whose high page went out (in this term or, per the durable episode,
        an earlier one). A page that cannot be queued keeps the episode open for the next acknowledged beat."""
        if self._page is None or not self.ack_paged:
            return
        host = self._transport.host
        if self._ack_back_body is None:
            self._ack_back_body = self._back_body(host, ok_ms)
        if not await self._send_page("info", ACK_BACK_TITLE, self._ack_back_body):
            return
        # 'back' must sort after the 'lost' row it closes (the store keeps the last write, and a wall-clock step back
        # between the two would otherwise leave 'lost' > 'back': an open episode the next term would inherit)
        back_ms = max(ok_ms, self._clock_ms())
        if self._ack_lost_at_ms is not None:
            back_ms = max(back_ms, self._ack_lost_at_ms + 1)
        self.ack_paged = False
        self.ack_inherited = False
        self._ack_back_body = None
        self._ack_lost_at_ms = None
        self._note_back(back_ms)
        # a 'lost' row still pending is written first; whatever fails is retried on every following beat
        self._back_record_pending_ms = back_ms
        await self._retry_episode_records()

    def _back_body(self, host: str, ok_ms: int) -> str:
        """Built once per episode, from the counters of the beat that first came back (the caller resets them
        right after, whether or not the page is queued). An inherited episode's earlier failures were counted by
        another term, so their number is unknown; the time since the stored 'lost' row is given instead."""
        n, streak_s = self.consecutive_failures, self._streak_ms() // 1000
        if not self.ack_inherited:
            return f"receiver {host}: acknowledged again after {n} failed beats over {streak_s} s"
        this_term = (f"{n} failed beats over {streak_s} s in this leader term" if n
                     else "the first beat of this leader term was acknowledged")
        since = ""
        if self._ack_lost_at_ms is not None:
            since = f" {max(0, ok_ms - self._ack_lost_at_ms) // 1000} s before this beat"
        return (f"receiver {host}: acknowledged again; {this_term}; the episode was paged in an earlier term{since} "
                f"(failed beats of earlier terms not known)")

    async def _send_page(self, severity: str, title: str, body: str) -> bool:
        """True when the page was handed to the page path (it did not raise and did not answer False). A page path
        that returns an awaitable (`EngineSession.queue_page_durable`, round-2 review) is awaited, so True then
        means the outbox row committed. A lost fence propagates (the supervisor hard-exits)."""
        assert self._page is not None
        try:
            result = self._page(severity, title, body)
            if inspect.isawaitable(result):
                result = await result
        except EpochMismatch:
            raise
        except Exception as exc:          # a page path error never stops the beat
            log.warning("dead-man receiver page not queued: %s", type(exc).__name__)
            return False
        if result is False:
            # queue_page_durable answers False both without a valid lease and when the outbox insert raised or
            # timed out (session.py), so the log names both causes
            log.warning("dead-man receiver page not queued (no valid lease or outbox insert failed)")
            return False
        return True

    def samples(self, now_ms: int) -> list[Sample]:
        """xtnl_deadman_consecutive_failures and xtnl_deadman_last_ok_age_seconds (absent until this term's first
        acknowledged beat: absence, never a fake 0, deploy/observability/metrics.md)."""
        out = gauge("xtnl_deadman_consecutive_failures", self.consecutive_failures)
        if self.last_ok_ms is not None:
            out += gauge("xtnl_deadman_last_ok_age_seconds", max(0, now_ms - self.last_ok_ms) / 1000.0)
        return out

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


def deadman_samples(sender: DeadmanSender | None, *, configured: bool, now_ms: int) -> list[Sample]:
    """The W1 series of one leader term: `xtnl_deadman_configured` (1 when XTNL_DEADMAN_URL is set) and, once the
    sender runs, its failure streak and the age of its last acknowledged beat. Counts and ages only."""
    out = gauge("xtnl_deadman_configured", configured)
    if sender is not None:
        out += sender.samples(now_ms)
    return out
