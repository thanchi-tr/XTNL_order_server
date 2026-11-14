"""OutboxRelay: the supervised task that delivers `notification_outbox` rows (oversee-rules.md §10,
operations.md §3 / §9, SM6 on diagram page 3).

Per tick (`tick()`, every `interval_s` from `run(stop)`):

1. Only the leader sends: `lease_valid()` is checked synchronously before every externally visible send.
2. Channel credentials: with a `channel_source` (`src.notifications.rotation.RotatingChannels`) the channels
   are rebuilt whenever the settings read through the secrets source change (polled every
   `credentials_poll_ms`, and every tick for a while after an auth error, which also asks the source to
   re-read the secret), so a rotated SMTP / SES / push credential is used without a restart.
3. Due rows (`sent_at_ms IS NULL AND next_attempt_at_ms <= now`, oldest first) are read in a batch.
4. **Episode dedupe** on a row's first attempt (SM6 QUEUED -> [duplicate]): an `episode` alert is sent once
   per episode key; a `repage` only when the re-page cadence has passed since the last send (the cadence is
   the installed ruleset's `escalate_after_ms` once `follow_cadence()` binds it); a `trip` (level change)
   and `info` rows (no episode) always go. A suppressed row is finished with `delivered = false`.
   The first attempted row of an episode claims it (`notification_dedupe` is written after the
   attempt, even a failed one), so a duplicate row queued later is suppressed while the claimant retries.
5. **Routing:** the row's `channels` (the outbox INSERT sets them by severity: FLAG / info -> email,
   SOFT / HARD -> email + mobile push). A routed channel that is not configured is reported and skipped;
   when none of the routed channels is configured the row is escalated at once (step 7).
6. **Per-channel independence (SM6 SENDING / RETRY_WAIT):** each channel is tried independently, so a
   dead SMTP server never stops the push (or the reverse). Each channel's failures are counted per row
   (n, stored with the row's remaining channels) and per channel (status). A failed channel is `down`
   for min(60, 2^n) s (`RelayPolicy.backoff_ms`); the row is rescheduled for the earliest moment one of
   its remaining channels may be tried again, and only its remaining channels are re-sent.
7. **Escalation (SM6 ESCALATED):** once one channel has failed `escalate_after_failures` (5) attempts for
   a row (a non-retryable provider rejection counts as all 5), or a row finds that channel backing off after
   5+ consecutive failures on any rows (only the oldest due row probes a down channel, so the rows behind it
   would otherwise wait hours for their own 5), the alert also goes to the other configured
   channel(s) (those not in its severity route), with a note naming the failing route; this happens once
   per row. The escalation is the dashboard alert: it is logged as an error, counted in the channel status
   and `recent_escalations()`, passed to `on_escalation`, and marked in the row's `channels` column
   (`+escalated`). The failing channel keeps retrying at the 60 s cap.
8. **Delivered flag:** `delivered = true` once every configured routed channel (and any escalation channel)
   accepted the alert. A row still undelivered after `max_attempts` attempts (~4 h at the cap) is finished
   with `delivered = false` and logged as an error.

Nothing here logs credentials or alert bodies: errors are reduced by `safe_error()`.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Final, Literal, Protocol, runtime_checkable

from src.notifications.errors import ChannelError, safe_error
from src.oversee.notify import Channel, DedupeStore, OutboxRow, OutboxStore, channels_for
from src.oversee.snapshot import DependencyUnavailable

if TYPE_CHECKING:
    from src.notifications.factory import ChannelSet

log = logging.getLogger(__name__)

RELAY_INTERVAL_S: Final = 1.0
RELAY_MAX_STAMP_AGE_S: Final = 60.0   # operations.md §3 (outbox relay)
AUTH_FAST_POLL_MS: Final = 60_000     # after an auth error, look for rotated credentials every tick this long

ChannelState = Literal["unknown", "up", "down", "disabled"]


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


@runtime_checkable
class RelayOutboxStore(OutboxStore, Protocol):
    """The durable retry bookkeeping the relay needs (PgOutboxStore implements it)."""

    async def due(self, now_ms: int, limit: int) -> list[OutboxRow]:
        """Unsent rows whose next attempt is due, oldest first, with `channels` = channels still to
        deliver, `channel_failures` / `escalated` as last rescheduled and `attempts` = attempts so far."""
        ...

    async def reschedule(self, outbox_id: int, *, remaining: Sequence[str], next_attempt_ms: int,
                         error: str | None, attempted: bool, failures: Mapping[str, int] | None = None,
                         escalated: bool = False) -> None: ...

    async def finish(self, outbox_id: int, at_ms: int, *, delivered: bool, error: str | None) -> None: ...

    async def oldest_undelivered_ms(self) -> int | None: ...


class ChannelSource(Protocol):
    """Rebuilds the channels when their credentials change (src.notifications.rotation.RotatingChannels)."""

    def poll(self) -> ChannelSet | None:
        """A new ChannelSet when the settings changed since the last poll, else None. No network I/O."""
        ...

    def invalidate(self) -> None:
        """A channel's credentials were rejected: ask the secrets source to re-read them soon."""
        ...


@dataclass
class _Retry:
    attempts: int
    remaining: tuple[str, ...]
    next_ms: int
    failures: tuple[tuple[str, int], ...] = ()
    escalated: bool = False


class VolatileRetryOutbox:
    """Adapts a plain OutboxStore (e.g. the in-memory Oversee db) to RelayOutboxStore, keeping the
    retry state in memory. A restart forgets it: the row is still pending and is sent again in full."""

    def __init__(self, store: OutboxStore) -> None:
        self._store = store
        self._retry: dict[int, _Retry] = {}

    async def add(self, rows: Sequence[OutboxRow]) -> None:
        await self._store.add(rows)

    async def pending(self, limit: int) -> list[OutboxRow]:
        return await self._store.pending(limit)

    async def mark_sent(self, outbox_id: int, at_ms: int, delivered: bool) -> None:
        self._retry.pop(outbox_id, None)
        await self._store.mark_sent(outbox_id, at_ms, delivered)

    async def due(self, now_ms: int, limit: int) -> list[OutboxRow]:
        out: list[OutboxRow] = []
        for row in await self._store.pending(limit + len(self._retry)):
            st = None if row.outbox_id is None else self._retry.get(row.outbox_id)
            if st is not None:
                if st.next_ms > now_ms:
                    continue
                row = replace(row, attempts=st.attempts, channels=st.remaining, channel_failures=st.failures,
                              escalated=st.escalated)
            out.append(row)
            if len(out) >= limit:
                break
        return out

    async def reschedule(self, outbox_id: int, *, remaining: Sequence[str], next_attempt_ms: int,
                         error: str | None, attempted: bool, failures: Mapping[str, int] | None = None,
                         escalated: bool = False) -> None:
        prev = self._retry.get(outbox_id)
        attempts = (prev.attempts if prev else 0) + (1 if attempted else 0)
        fails = tuple((c, n) for c, n in (failures or {}).items() if n > 0 and c in remaining)
        self._retry[outbox_id] = _Retry(attempts, tuple(remaining), next_attempt_ms, fails, escalated)

    async def finish(self, outbox_id: int, at_ms: int, *, delivered: bool, error: str | None) -> None:
        await self.mark_sent(outbox_id, at_ms, delivered)

    async def oldest_undelivered_ms(self) -> int | None:
        rows = await self._store.pending(1)
        return rows[0].created_ms if rows else None


@dataclass(frozen=True)
class RelayPolicy:
    """SM6: back-off min(60, 2^n) s after the n-th failure; escalation after 5 failures on one channel."""

    interval_s: float = RELAY_INTERVAL_S
    batch: int = 50
    escalate_after_ms: int = 900_000      # default re-page cadence until follow_cadence() binds the ruleset's
    backoff_base_ms: int = 1_000          # 2^n s
    backoff_max_ms: int = 60_000          # min(60, ...) s
    escalate_after_failures: int = 5      # SM6 attempts >= 5 -> ESCALATED
    max_attempts: int = 240               # ~4 h at the back-off cap, then finished undelivered
    credentials_poll_ms: int = 5_000      # how often the channel source is polled for rotated credentials

    def backoff_ms(self, failures: int) -> int:
        if failures <= 0:
            return 0
        return min(self.backoff_max_ms, self.backoff_base_ms << min(failures, 30))


@dataclass(frozen=True)
class ChannelStatus:
    name: str
    backend: str
    state: ChannelState
    consecutive_failures: int = 0
    retry_at_ms: int = 0
    last_ok_ms: int | None = None
    last_failure_ms: int | None = None
    last_error: str | None = None
    delivered: int = 0
    failed: int = 0
    escalations: int = 0      # rows escalated to another channel because this one kept failing


@dataclass(frozen=True)
class Escalation:
    """SM6 ESCALATED (the dashboard alert): `failed` channels did not take the alert, `to` now also get it."""

    outbox_id: int
    kind: str
    severity: str
    title: str
    account_id: str | None
    failed: tuple[str, ...]
    to: tuple[str, ...]
    reason: str
    at_ms: int


@dataclass(frozen=True)
class TickResult:
    due: int = 0
    delivered: int = 0       # rows finished with every configured channel delivered
    partial: int = 0         # rows with at least one channel still to deliver (rescheduled)
    suppressed: int = 0      # rows dropped by episode dedupe
    gave_up: int = 0         # rows finished undelivered (attempts exhausted / no channel)
    lease_lost: bool = False
    escalated: int = 0       # rows escalated to the other channel during this tick


@dataclass
class _Tally:
    due: int = 0
    delivered: int = 0
    partial: int = 0
    suppressed: int = 0
    gave_up: int = 0
    lease_lost: bool = False
    escalated: int = 0

    def freeze(self) -> TickResult:
        return TickResult(self.due, self.delivered, self.partial, self.suppressed, self.gave_up, self.lease_lost,
                          self.escalated)


@dataclass
class _Pass:
    """One row's delivery pass."""

    failures: dict[str, int]
    remaining: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    permanent: set[str] = field(default_factory=set)
    tried: set[str] = field(default_factory=set)
    down: set[str] = field(default_factory=set)      # skipped: the channel is backing off after failures
    attempted: bool = False


def _backend_of(ch: Channel) -> str:
    b = getattr(ch, "backend", None)
    return b if isinstance(b, str) else ch.name


def _as_cadence(value: int | Callable[[], int]) -> Callable[[], int]:
    if callable(value):
        return value
    fixed = int(value)
    return lambda: fixed


class OutboxRelay:
    """Supervised loop: `run(stop, stamp)`; `tick()` is one pass (tests drive it directly).

    Wiring: `OutboxRelay(PgOutboxStore(db), PgDedupeStore(db), channel_set.channels,
    disabled=channel_set.disabled, issues=channel_set.issues, lease_valid=keeper.lease_valid,
    channel_source=rotating_channels, escalate_after_ms=lambda: dispatcher.escalate_after_ms,
    on_escalation=...)`.
    """

    def __init__(self, outbox: OutboxStore, dedupe: DedupeStore, channels: Sequence[Channel], *,
                 disabled: Sequence[str] = (), issues: Sequence[str] = (), policy: RelayPolicy = RelayPolicy(),
                 lease_valid: Callable[[], bool] = lambda: True, clock_ms: Callable[[], int] = _wall_ms,
                 on_auth_error: Callable[[str], None] | None = None,
                 channel_source: ChannelSource | None = None,
                 escalate_after_ms: int | Callable[[], int] | None = None,
                 on_escalation: Callable[[Escalation], None] | None = None) -> None:
        self._outbox: RelayOutboxStore = (outbox if isinstance(outbox, RelayOutboxStore)
                                          else VolatileRetryOutbox(outbox))
        self._dedupe = dedupe
        self._policy = policy
        self._lease_valid = lease_valid
        self._clock_ms = clock_ms
        self._on_auth_error = on_auth_error
        self._on_escalation = on_escalation
        self._source = channel_source
        self._cadence = _as_cadence(policy.escalate_after_ms if escalate_after_ms is None else escalate_after_ms)
        self._channels: dict[str, Channel] = {}
        self._status: dict[str, ChannelStatus] = {}
        self._issues: tuple[str, ...] = ()
        self._install(channels, disabled, issues)
        self._next_poll_ms = 0
        self._fast_poll_until_ms = 0
        self._escalations: deque[Escalation] = deque(maxlen=50)
        self._oldest_undelivered: int | None = None
        self._reported = False
        self.last_tick: TickResult = TickResult()

    # ---- channels ----------------------------------------------------------------------------------
    def _install(self, channels: Sequence[Channel], disabled: Sequence[str], issues: Sequence[str]) -> None:
        new: dict[str, Channel] = {}
        for ch in channels:
            if ch.name in new:
                raise ValueError(f"duplicate alert channel {ch.name!r}")
            new[ch.name] = ch
        old = self._status
        status: dict[str, ChannelStatus] = {}
        for name, ch in new.items():
            prev = old.get(name)
            fresh = ChannelStatus(name, _backend_of(ch), "unknown")
            # new credentials: the back-off earned by the old ones does not apply; the counters are kept
            status[name] = fresh if prev is None else replace(
                fresh, delivered=prev.delivered, failed=prev.failed, escalations=prev.escalations,
                last_ok_ms=prev.last_ok_ms, last_failure_ms=prev.last_failure_ms, last_error=prev.last_error)
        for name in disabled:
            if name not in status:
                status[name] = ChannelStatus(name, "none", "disabled")
        self._channels, self._status, self._issues = new, status, tuple(issues)

    def replace_channels(self, channel_set: ChannelSet) -> None:
        """Use these channels from now on (rotated credentials). The rows' retry state is kept."""
        self._install(channel_set.channels, channel_set.disabled, channel_set.issues)
        log.info("notifications: alert channels rebuilt (%s)", ", ".join(
            f"{n}={s.backend}" for n, s in sorted(self._status.items())) or "none")

    def _poll_source(self, now: int) -> None:
        if self._source is None or (now < self._next_poll_ms and now >= self._fast_poll_until_ms):
            return
        self._next_poll_ms = now + self._policy.credentials_poll_ms
        try:
            cs = self._source.poll()
            if cs is not None:
                self.replace_channels(cs)
        except Exception as exc:  # a settings problem never stops delivery with the current channels
            log.warning("notifications: cannot reload channel credentials (%s); keeping the current channels",
                        safe_error(exc))

    def _auth_failed(self, name: str, now: int) -> None:
        if self._on_auth_error is not None:
            try:
                self._on_auth_error(name)
            except Exception as exc:
                log.warning("notifications: auth-error hook failed (%s)", safe_error(exc))
        if self._source is not None:
            try:
                self._source.invalidate()
            except Exception as exc:
                log.warning("notifications: cannot ask for fresh credentials (%s)", safe_error(exc))
            self._fast_poll_until_ms = now + AUTH_FAST_POLL_MS

    # ---- re-page cadence ---------------------------------------------------------------------------
    def follow_cadence(self, escalate_after_ms: int | Callable[[], int]) -> None:
        """Re-page cadence from now on: a fixed value, or a callable re-read on every use (bind it to the
        installed ruleset, e.g. `lambda: dispatcher.escalate_after_ms`)."""
        self._cadence = _as_cadence(escalate_after_ms)

    @property
    def escalate_after_ms(self) -> int:
        """The re-page cadence in force now."""
        return self._cadence()

    # ---- read-only views ---------------------------------------------------------------------------
    def status(self) -> Mapping[str, ChannelStatus]:
        return dict(self._status)

    def recent_escalations(self) -> tuple[Escalation, ...]:
        """The latest SM6 escalations (newest last; bounded), for the dashboard and metrics."""
        return tuple(self._escalations)

    @property
    def warnings(self) -> tuple[str, ...]:
        disabled = tuple(f"alert channel {n} is disabled (not configured)"
                         for n, s in sorted(self._status.items()) if s.state == "disabled")
        none = ("no alert channel is configured: alerts will not be delivered",) if not self._channels else ()
        return none + disabled + self._issues

    def oldest_undelivered_ms(self) -> int | None:
        """Created time of the oldest unsent row at the last tick (dead-man payload field)."""
        return self._oldest_undelivered

    def report_startup(self) -> tuple[str, ...]:
        """Log the configuration problems once (start-up warning; never fatal)."""
        w = self.warnings
        if not self._reported:
            self._reported = True
            for line in w:
                (log.error if line.startswith("no alert channel") else log.warning)("notifications: %s", line)
            if self._channels:
                log.info("notifications: channels %s", ", ".join(
                    f"{n}={s.backend}" for n, s in sorted(self._status.items()) if s.state != "disabled"))
        return w

    # ---- loop --------------------------------------------------------------------------------------
    async def run(self, stop: asyncio.Event, stamp: Callable[[], None] | None = None) -> None:
        self.report_startup()
        db_down = False
        while not stop.is_set():
            try:
                await self.tick(stamp=stamp)
                if db_down:
                    log.info("notification relay: PostgreSQL reachable again")
                db_down = False
            except DependencyUnavailable as exc:
                if not db_down:
                    log.warning("notification relay: PostgreSQL unavailable (%s); retrying", type(exc).__name__)
                db_down = True
            if stamp is not None:
                stamp()
            try:
                await asyncio.wait_for(stop.wait(), self._policy.interval_s)
            except TimeoutError:
                pass

    async def tick(self, *, stamp: Callable[[], None] | None = None) -> TickResult:
        t = _Tally()
        now = self._clock_ms()
        self._poll_source(now)
        if not self._lease_valid():
            t.lease_lost = True
            self.last_tick = t.freeze()
            return self.last_tick
        rows = await self._outbox.due(now, self._policy.batch)
        t.due = len(rows)
        for row in rows:
            if not self._lease_valid():
                t.lease_lost = True
                break
            await self._process(row, self._clock_ms(), t)
            if stamp is not None:
                stamp()
        self._oldest_undelivered = await self._outbox.oldest_undelivered_ms()
        self.last_tick = t.freeze()
        return self.last_tick

    # ---- one row -----------------------------------------------------------------------------------
    async def _should_send(self, row: OutboxRow, now_ms: int) -> bool:
        if row.bypass_dedupe or row.episode is None:
            return True
        sent = await self._dedupe.last_sent(row.episode)
        if row.kind == "episode":
            return sent is None
        return sent is None or now_ms - sent >= self._cadence()

    def _fallbacks(self, row: OutboxRow, exclude: Sequence[str]) -> list[str]:
        """Configured channels outside the row's severity route: SM6's 'other channel'."""
        routed = channels_for(row.severity)
        return [c for c in self._channels if c not in routed and c not in exclude]

    @staticmethod
    def _escalated_copy(row: OutboxRow) -> OutboxRow:
        failing = "/".join(channels_for(row.severity))
        return replace(row, body=f"{row.body}\n\nescalated: not delivered by {failing}")

    async def _try(self, name: str, row: OutboxRow, now: int, p: _Pass, t: _Tally) -> None:
        st = self._status[name]
        if st.retry_at_ms > now:
            p.remaining.append(name)
            p.errors.append(f"{name}: down")
            p.down.add(name)
            return
        if not self._lease_valid():          # synchronous check right before the externally visible send
            p.remaining.append(name)
            t.lease_lost = True
            return
        p.attempted = True
        p.tried.add(name)
        escalation_leg = row.escalated and name not in channels_for(row.severity)
        try:
            await self._channels[name].send(self._escalated_copy(row) if escalation_leg else row)
        except Exception as exc:  # one channel's failure never stops the others
            err = safe_error(exc)
            p.remaining.append(name)
            p.errors.append(f"{name}: {err}")
            n = p.failures.get(name, 0) + 1
            if isinstance(exc, ChannelError):
                if not exc.retryable:
                    p.permanent.add(name)
                    n = max(n, self._policy.escalate_after_failures)
                if exc.auth:
                    self._auth_failed(name, now)
            p.failures[name] = n
            self._channel_failed(name, now, err)
        else:
            p.failures.pop(name, None)
            self._channel_ok(name, now)

    async def _process(self, row: OutboxRow, now: int, t: _Tally) -> None:
        oid = row.outbox_id
        if oid is None:
            return
        first = row.attempts == 0
        if first and not await self._should_send(row, now):
            await self._outbox.finish(oid, now, delivered=False, error="deduplicated")
            t.suppressed += 1
            return
        route = row.route
        targets = [c for c in route if c in self._channels]
        unconfigured = [c for c in route if c not in self._channels]
        p = _Pass(failures={c: n for c, n in row.channel_failures if c in route})
        if not targets:
            fallback = [] if row.escalated else self._fallbacks(row, route)
            if not fallback:
                err = "no configured channel for route " + ",".join(route)
                await self._outbox.finish(oid, now, delivered=False, error=err)
                log.error("alert %d (%s/%s) not delivered: %s", oid, row.kind, row.severity, err)
                t.gave_up += 1
                return
            row = replace(row, escalated=True)
            self._escalate(row, unconfigured, fallback, "not configured", now, t)
            targets = fallback
        for name in targets:
            await self._try(name, row, now, p, t)
        if p.attempted and first and row.episode is not None and not row.bypass_dedupe:
            await self._dedupe.record(row.episode, now)   # this row now owns the episode
        limit = self._policy.escalate_after_failures
        failing = [c for c in p.remaining if p.failures.get(c, 0) >= limit or self._known_dead(c, p)]
        if failing and not row.escalated and not t.lease_lost:
            row = replace(row, escalated=True)
            fallback = self._fallbacks(row, p.remaining)
            reason = ("rejected by the provider" if any(c in p.permanent for c in failing)
                      else f"{limit} failed attempts" if any(p.failures.get(c, 0) >= limit for c in failing)
                      else f"channel down after {limit}+ consecutive failures")
            self._escalate(row, failing, fallback, reason, now, t)
            for name in fallback:
                await self._try(name, row, now, p, t)
        notes = (["not configured: " + ",".join(unconfigured)] if unconfigured else []) + (
            ["escalated"] if row.escalated else [])
        if not p.remaining:
            await self._outbox.finish(oid, now, delivered=True, error="; ".join(notes) or None)
            t.delivered += 1
            return
        error = "; ".join(p.errors + notes)[:500]
        attempts = row.attempts + (1 if p.attempted else 0)
        if attempts >= self._policy.max_attempts:
            await self._outbox.finish(oid, now, delivered=False, error=error)
            log.error("alert %d (%s/%s) undeliverable after %d attempts: %s", oid, row.kind, row.severity,
                      attempts, error)
            t.gave_up += 1
            return
        await self._outbox.reschedule(oid, remaining=p.remaining, next_attempt_ms=self._next_attempt(p, now),
                                      error=error, attempted=p.attempted, failures=p.failures,
                                      escalated=row.escalated)
        t.partial += 1

    def _known_dead(self, name: str, p: _Pass) -> bool:
        """A channel this row skipped because it is backing off after at least `escalate_after_failures`
        consecutive failures (on any rows). Only one row probes a down channel per back-off slot (the oldest
        due), so without this the rows queued behind it would wait hours for their own 5 attempts and an
        email-only alert would never reach the other channel while the email backend is dead."""
        st = self._status.get(name)
        return (name in p.down and st is not None and st.state == "down"
                and st.consecutive_failures >= self._policy.escalate_after_failures)

    def _next_attempt(self, p: _Pass, now: int) -> int:
        """The earliest moment one of the remaining channels may be tried again (per-channel independence):
        a channel that failed in this pass waits its own min(60, 2^n) s for this row (60 s after a provider
        rejection); a channel skipped as down waits only until its status lets it (its back-off runs from
        its own last failure, not from this pass)."""
        def ready(c: str) -> int:
            status_ready = self._status[c].retry_at_ms if c in self._status else 0
            if c not in p.tried:
                return max(now, status_ready)
            own = (self._policy.backoff_max_ms if c in p.permanent
                   else self._policy.backoff_ms(p.failures.get(c, 0)))
            return max(now + own, status_ready)
        return min(ready(c) for c in p.remaining)

    def _escalate(self, row: OutboxRow, failed: Sequence[str], to: Sequence[str], reason: str, now: int,
                  t: _Tally) -> None:
        assert row.outbox_id is not None
        ev = Escalation(row.outbox_id, row.kind, row.severity, row.title, row.account_id, tuple(failed), tuple(to),
                        reason, now)
        t.escalated += 1
        self._escalations.append(ev)
        for c in failed:
            st = self._status.get(c)
            if st is not None:
                self._status[c] = replace(st, escalations=st.escalations + 1)
        log.error("alert %d (%s/%s) escalated: %s %s; %s", row.outbox_id, row.kind, row.severity,
                  ",".join(failed) or "-", reason,
                  ("also sent to " + ",".join(to)) if to else "no other channel is configured")
        if self._on_escalation is not None:
            try:
                self._on_escalation(ev)
            except Exception as exc:
                log.warning("notifications: escalation hook failed (%s)", safe_error(exc))

    def _channel_ok(self, name: str, now: int) -> None:
        st = self._status[name]
        if st.state == "down":
            log.info("alert channel %s (%s) recovered after %d failures", name, st.backend, st.consecutive_failures)
        self._status[name] = replace(st, state="up", consecutive_failures=0, retry_at_ms=0, last_ok_ms=now,
                                     delivered=st.delivered + 1)

    def _channel_failed(self, name: str, now: int, err: str) -> None:
        st = self._status[name]
        n = st.consecutive_failures + 1
        if st.state != "down":
            log.warning("alert channel %s (%s) down: %s", name, st.backend, err)
        self._status[name] = replace(st, state="down", consecutive_failures=n,
                                     retry_at_ms=now + self._policy.backoff_ms(n), last_failure_ms=now,
                                     last_error=err, failed=st.failed + 1)
