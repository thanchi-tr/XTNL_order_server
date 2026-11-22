"""NotificationDispatcher: outbox-based, episode-deduplicated alerts (oversee-rules.md §10).

Alert rows are written to `notification_outbox` in the same transaction as the transition or halt
row. One alert per state episode (rule_id, subject_type, subject_id, activation id, level), a
re-page every escalate_after_ms while active unless acknowledged; only the level-raising trip()
alert bypasses dedupe. Dedupe state lives in `notification_dedupe`.

Routing (`channels_for`): info / FLAG -> email; high / critical (SOFT / HARD) -> email + mobile push,
the same rule the outbox INSERT applies to its `channels` column.

The production sender is `src.notifications.relay.OutboxRelay` (SM6: per-channel status, retries
with min(60, 2^n) s back-off, escalation to the other channel after 5 failures, delivered flag). Its
per-row, per-channel retry state is kept in the outbox `channels` column (`encode_channels` /
`decode_channels`). `NotificationDispatcher.drain()` is the simple in-process sender used by
tests and the in-memory engine; `queue_repages()` is what the RiskSweep calls after each pass.
`EmailChannel` / `MobilePushChannel` record what they send and, when given an `AlertTransport`
(SMTP / SES / ntfy / Pushover, built by `src.notifications`), also deliver it.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Final, Literal, Protocol

from src.oversee.snapshot import BreachAck
from src.oversee.types import BreachState, Level, Transition

AlertKind = Literal["episode", "repage", "trip", "info"]
Severity = Literal["info", "high", "critical"]
EpisodeKey = tuple[str, str, str, int, int]   # rule_id, subject_type, subject_id, activation id, level

EMAIL: Final = "email"
MOBILE_PUSH: Final = "mobile_push"
KNOWN_CHANNELS: Final = (EMAIL, MOBILE_PUSH)
ESCALATED_TOKEN: Final = "+escalated"   # marker in the outbox `channels` column (SM6 ESCALATED)
#: The outbox kind of ARM notices (PR4, migration 0016). They share notification_outbox but are delivered by the
#: ARM relay's own store; every Oversee outbox read skips them. Not an AlertKind: Oversee never writes one.
ARM_OUTBOX_KIND: Final[str] = "arm"


def channels_for(severity: Severity) -> tuple[str, ...]:
    """FLAG / info -> email; SOFT / HARD (high / critical) -> email + mobile push."""
    return (EMAIL, MOBILE_PUSH) if severity in ("high", "critical") else (EMAIL,)


@dataclass(frozen=True)
class OutboxRow:
    kind: AlertKind
    severity: Severity
    title: str
    body: str
    created_ms: int
    episode: EpisodeKey | None = None
    account_id: str | None = None
    outbox_id: int | None = None
    channels: tuple[str, ...] = ()   # channels still to deliver (from the outbox row); () = by severity
    attempts: int = 0                # delivery attempts so far (relay bookkeeping)
    channel_failures: tuple[tuple[str, int], ...] = ()   # failed attempts per remaining channel (SM6 n)
    escalated: bool = False          # SM6 ESCALATED: the alert also goes to the other channel(s)
    html: str | None = field(default=None, compare=False, repr=False)
    """A prebuilt HTML email part (the ARM relay's send-time render): never persisted, None on every outbox read."""

    @property
    def bypass_dedupe(self) -> bool:
        return self.kind == "trip"

    @property
    def route(self) -> tuple[str, ...]:
        """The channels this row goes to: its stored remaining channels, else the severity rule."""
        return self.channels if self.channels else channels_for(self.severity)

    def failures_of(self, channel: str) -> int:
        return next((n for c, n in self.channel_failures if c == channel), 0)


def encode_channels(remaining: Sequence[str], failures: Mapping[str, int], escalated: bool) -> list[str]:
    """The outbox `channels` value for a rescheduled row: each remaining channel, as `name:n` once it has
    failed n times for this row, plus `ESCALATED_TOKEN` once the row was escalated (readable as is by the
    dashboard). A plain `{email}` (the INSERT default) decodes as email with 0 failures."""
    out = [f"{c}:{failures[c]}" if failures.get(c, 0) > 0 else c for c in remaining]
    if escalated:
        out.append(ESCALATED_TOKEN)
    return out


def decode_channels(tokens: Iterable[str]) -> tuple[tuple[str, ...], tuple[tuple[str, int], ...], bool]:
    """(remaining channels, per-channel failures, escalated) from an outbox `channels` value."""
    names: list[str] = []
    fails: list[tuple[str, int]] = []
    escalated = False
    for raw in tokens:
        tok = str(raw)
        if tok == ESCALATED_TOKEN:
            escalated = True
            continue
        name, sep, n = tok.rpartition(":")
        if sep and name and n.isdigit():
            names.append(name)
            if int(n) > 0:
                fails.append((name, int(n)))
        else:
            names.append(tok)
    return tuple(names), tuple(fails), escalated


@dataclass(frozen=True)
class AlertMessage:
    """A rendered alert: what a transport sends. Built from an OutboxRow by `render_message`."""

    subject: str
    body: str
    severity: Severity
    kind: AlertKind
    outbox_id: int | None = None
    account_id: str | None = None
    # Presentation only (the email's HTML part, src/notifications/email_html.py): not compared, never persisted.
    created_ms: int = field(default=0, compare=False)
    episode: EpisodeKey | None = field(default=None, compare=False)
    html: str | None = field(default=None, compare=False, repr=False)


def render_message(row: OutboxRow) -> AlertMessage:
    tag = {"info": "INFO", "high": "HIGH", "critical": "CRITICAL"}[row.severity]
    lines = [row.body, "", f"kind: {row.kind}", f"severity: {tag}", f"created_ms: {row.created_ms}"]
    if row.account_id is not None:
        lines.append(f"account: {row.account_id}")
    if row.outbox_id is not None:
        lines.append(f"alert id: {row.outbox_id}")
    return AlertMessage(f"[XTNL {tag}] {row.title}", "\n".join(lines), row.severity, row.kind,
                        row.outbox_id, row.account_id, created_ms=row.created_ms, episode=row.episode, html=row.html)


class AlertTransport(Protocol):
    """One delivery backend (SMTP, SES, ntfy, Pushover). Raises on failure (a ChannelError from
    src.notifications, with a message that carries no credentials and no alert body)."""

    @property
    def backend(self) -> str: ...

    async def deliver(self, msg: AlertMessage) -> None: ...


def _severity(level: Level) -> Severity:
    if level >= Level.HARD_HALT:
        return "critical"
    if level >= Level.SOFT_HALT:
        return "high"
    return "info"


def episode_key(rule_id: str, subject_type: str, subject_id: str, activation_id: int | None,
                level: Level) -> EpisodeKey:
    return (rule_id, subject_type, subject_id, activation_id or 0, int(level))


def alerts_for_transitions(transitions: Iterable[Transition]) -> list[OutboxRow]:
    """Activated / raised -> one episode alert; lowered / resolved / capped / uncapped -> info."""
    rows: list[OutboxRow] = []
    for t in transitions:
        subject = f"{t.rule_id} {t.subject_type}:{t.subject_id}"
        if t.transition in ("activated", "raised"):
            rows.append(OutboxRow("episode", _severity(t.level), f"{t.level.name} {subject}",
                                  f"{t.reason} (cap {t.cap.value}, sweep {t.sweep_id})", t.at_ms,
                                  episode_key(t.rule_id, t.subject_type, t.subject_id, t.activation_id, t.level),
                                  t.account_id))
        else:
            rows.append(OutboxRow("info", "info", f"{t.transition} {subject}",
                                  f"{t.reason} (level {t.level.name}, cap {t.cap.value})", t.at_ms,
                                  None, t.account_id))
    return rows


def is_acknowledged(state: BreachState, acks: Iterable[BreachAck]) -> bool:
    """An ack silences re-pages until the state's level, cap or reason changes (§4 acknowledge)."""
    return any(a.rule_id == state.rule_id and a.subject_type == state.subject_type
               and a.subject_id == state.subject_id and a.level == state.level and a.cap == state.cap
               and a.reason == state.reason and (state.active_since is None or a.at_ms >= state.active_since)
               for a in acks)


def repage_candidate(state: BreachState, acks: Sequence[BreachAck]) -> EpisodeKey | None:
    """The episode whose last delivery decides whether `state` re-pages (None: it cannot re-page now)."""
    if not state.active or is_acknowledged(state, acks):
        return None
    return episode_key(state.rule_id, state.subject_type, state.subject_id, state.activation_id, state.level)


def repages_due(states: Iterable[BreachState], acks: Sequence[BreachAck], last_sent: Mapping[EpisodeKey, int],
                now_ms: int, escalate_after_ms: int,
                held: Callable[[BreachState], bool] | None = None) -> list[OutboxRow]:
    """Re-page **every** active rule state (FLAG included: info severity, so email only) every
    escalate_after_ms unless acknowledged (oversee-rules.md §10, §2 "capped states ... re-page").

    The cadence runs from the episode's last delivery (`notification_dedupe`); an episode whose alert was never
    delivered (held back by the AlertInhibitor, or written by an earlier leader whose relay never sent it) runs
    from the state's `active_since`, so it is not silent for ever. `held(state)`: the state's re-page is held back
    now (its adapter's dependent alerts are suppressed: maintenance window or epoch-change settle). The RiskSweep
    passes no state at all for a flat account (oversee-rules.md §10 accepted deviation, `sweep._flat_account`)."""
    rows: list[OutboxRow] = []
    for s in states:
        if not s.active or is_acknowledged(s, acks):
            continue
        if held is not None and held(s):
            continue
        key = episode_key(s.rule_id, s.subject_type, s.subject_id, s.activation_id, s.level)
        sent = last_sent.get(key)
        if sent is None:
            sent = s.active_since
        if sent is not None and now_ms - sent >= escalate_after_ms:
            rows.append(OutboxRow("repage", _severity(s.level), f"still {s.level.name} {s.rule_id} "
                                  f"{s.subject_type}:{s.subject_id}", f"{s.reason} (cap {s.cap.value})",
                                  now_ms, key, s.account_id))
    return rows


class Channel(Protocol):
    name: str

    async def send(self, row: OutboxRow) -> None: ...


@dataclass
class RecordingChannel:
    """Without a `transport`: a pure recorder (`sent` holds every row). With one: delivers the rendered
    alert through it and only counts deliveries (`delivered`), so a 24/7 process does not grow."""

    name: str
    sent: list[OutboxRow] = field(default_factory=list)
    transport: AlertTransport | None = field(default=None, repr=False)
    delivered: int = 0

    async def send(self, row: OutboxRow) -> None:
        if self.transport is None:
            self.sent.append(row)
        else:
            await self.transport.deliver(render_message(row))
        self.delivered += 1

    @property
    def backend(self) -> str:
        return "recording" if self.transport is None else self.transport.backend


@dataclass
class EmailChannel(RecordingChannel):
    """Email strategy: SMTP or Amazon SES v2 transport (src.notifications.mail)."""

    name: str = EMAIL


@dataclass
class MobilePushChannel(RecordingChannel):
    """Mobile push strategy: ntfy or Pushover transport (src.notifications.push)."""

    name: str = MOBILE_PUSH


class OutboxStore(Protocol):
    async def add(self, rows: Sequence[OutboxRow]) -> None: ...

    async def pending(self, limit: int) -> list[OutboxRow]: ...

    async def mark_sent(self, outbox_id: int, at_ms: int, delivered: bool) -> None: ...


class DedupeStore(Protocol):
    async def last_sent(self, key: EpisodeKey) -> int | None: ...

    async def record(self, key: EpisodeKey, at_ms: int) -> None: ...

    async def snapshot(self) -> Mapping[EpisodeKey, int]: ...

    async def last_sent_of(self, keys: Sequence[EpisodeKey]) -> Mapping[EpisodeKey, int]:
        """The last-sent time of each of `keys` that has one (one read of only those rows)."""
        ...


class NotificationDispatcher:
    """Sends outbox rows to every channel. Only the leader sends: `lease_valid()` is checked first.

    `escalate_after_ms` (the re-page cadence) is an int or a callable that is re-read on every use, so a
    ruleset reload changes the cadence without rebuilding the dispatcher (`follow_cadence`; the RiskSweep
    binds it to its installed ruleset in `RiskSweep.install_ruleset`)."""

    def __init__(self, outbox: OutboxStore, dedupe: DedupeStore, channels: Sequence[Channel],
                 escalate_after_ms: int | Callable[[], int], lease_valid: Callable[[], bool] = lambda: True) -> None:
        self._outbox = outbox
        self._dedupe = dedupe
        self._channels = list(channels)
        self._cadence: Callable[[], int] = _as_cadence(escalate_after_ms)
        self._lease_valid = lease_valid
        self._queued: dict[EpisodeKey, int] = {}   # re-page rows queued, by episode (pruned after a cadence)

    def follow_cadence(self, escalate_after_ms: int | Callable[[], int]) -> None:
        """Re-page cadence from now on: a fixed value, or a callable re-read on every use."""
        self._cadence = _as_cadence(escalate_after_ms)

    @property
    def escalate_after_ms(self) -> int:
        """The re-page cadence in force now."""
        return self._cadence()

    async def _should_send(self, row: OutboxRow, now_ms: int) -> bool:
        if row.bypass_dedupe or row.episode is None:
            return True
        sent = await self._dedupe.last_sent(row.episode)
        if row.kind == "episode":
            return sent is None
        return sent is None or now_ms - sent >= self.escalate_after_ms

    async def drain(self, now_ms: int, limit: int = 100) -> int:
        """Send pending rows to the channels in their route; a channel error leaves the row pending
        for the next drain (no back-off: use OutboxRelay in production)."""
        delivered = 0
        for row in await self._outbox.pending(limit):
            if not self._lease_valid():
                break
            if row.outbox_id is None:
                continue
            if not await self._should_send(row, now_ms):
                await self._outbox.mark_sent(row.outbox_id, now_ms, delivered=False)
                continue
            route = row.route
            try:
                for ch in self._channels:
                    if ch.name in KNOWN_CHANNELS and ch.name not in route:
                        continue   # FLAG / info rows do not page the phone
                    await ch.send(row)
            except Exception:
                continue
            if row.episode is not None:
                await self._dedupe.record(row.episode, now_ms)
            await self._outbox.mark_sent(row.outbox_id, now_ms, delivered=True)
            delivered += 1
        return delivered

    def mark_alerted(self, episodes: Iterable[EpisodeKey], now_ms: int) -> None:
        """Episodes whose alert was just written to the outbox (activated / raised): no re-page is queued for them
        within one cadence, so the rise of a long-active state is not re-paged on top of its own alert while the
        sender has not recorded it yet. In memory, no I/O."""
        for k in episodes:
            self._queued[k] = now_ms

    async def queue_repages(self, states: Iterable[BreachState], acks: Sequence[BreachAck], now_ms: int) -> int:
        """Queue the re-pages due (`repages_due`: every active state, FLAG included). One row per episode per
        cadence at most: a row the sender has not recorded in `notification_dedupe` yet (still pending, or
        suppressed by a sender whose own cadence is longer, or just alerted: `mark_alerted`) is not queued again
        on every pass. A state whose re-page is held back is simply not passed in (the RiskSweep filters the
        stale / FENCE states of a suppressed adapter, and every state of a flat account: `sweep._flat_account`)."""
        cadence = self.escalate_after_ms
        self._queued = {k: at for k, at in self._queued.items() if now_ms - at < cadence}
        states = list(states)
        wanted = sorted({k for k in (repage_candidate(s, acks) for s in states)
                         if k is not None and k not in self._queued}, key=repr)
        # Only the `notification_dedupe` rows of this pass's candidates are read (IMPLEMENTATION_PLAN 8.2): the
        # table grows with every episode ever paged, so a full read per account pass would grow without bound.
        last_sent = await self._dedupe.last_sent_of(wanted) if wanted else {}
        rows = [r for r in repages_due(states, acks, last_sent, now_ms, cadence)
                if r.episode is None or r.episode not in self._queued]
        if rows:
            await self._outbox.add(rows)
            self._queued.update((r.episode, now_ms) for r in rows if r.episode is not None)
        return len(rows)


def _as_cadence(value: int | Callable[[], int]) -> Callable[[], int]:
    if callable(value):
        return value
    fixed = int(value)
    return lambda: fixed
