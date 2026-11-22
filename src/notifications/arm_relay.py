"""`arm-relay`: the supervised task that delivers the kind `arm` rows of `notification_outbox` (design §B4, §B5.1,
§B8, §B13, §B14; plan S29, RT-M4, R11-M14).

Its own class, **not** `OutboxRelay` (RT-M4): no episode dedupe, no escalation, no fallback to another channel, no
emergency priority (a notice goes out at `info`, the kind `arm` copy of the immediate ping at `high`, never
`critical`, which Pushover would send as an emergency), and no `+escalated` marker (the 0016 CHECK refuses it on an
ARM row). It reads and writes only kind `arm` rows (`src.db.arm_outbox_store.PgArmOutboxStore`, on the engine (owner)
pool like the Oversee relay); the Oversee relay never reads one, so an ARM channel that is down never delays an
Oversee alert, and the reverse.

Per tick (`tick()`, every `interval_s` from `run(stop, stamp)`):

1. Only the leader sends: `lease_valid()` is checked before the batch and synchronously before every send.
2. Channel credentials follow the secrets source (`channel_source`, the `XTNL_ARM_*` settings through
   `RotatingChannels(..., names=ARM_CHANNEL_NAMES)`), as in the Oversee relay.
3. Due rows: unsent kind `arm` rows whose next attempt is due or whose expiry has passed.
4. **Expiry:** a row past `expires_at_ms` is finished undelivered (`expired`), its body replaced
   (`finished_body`); an expired POST or LATE notice with an `ADJUST` item pages once (§B8 Pages). The immediate
   ping has no expiry.
5. **`--drain-notices`** (`drain_notices()`, the `arm_controls` flag): every queued, unsent notice is finished as
   `suppressed`, the due ones and those still in back-off (`pending`), so a resume never sends one; an expired one
   takes the expiry path. An immediate ping (the CRITICAL kind `arm` row, §B14) is never touched by it, nor by a
   pause.
6. **Supersede** (`src.riskmgr.notices.supersedes`): an unsent notice is finished as `superseded` (with
   `superseded_by`) once a newer kind `arm` row supersedes it (a PRE once its POST exists; POST parts and LATE,
   CORRECTED and REOPEN items once the route's next POST part 0 exists); a part-5 notice only after one channel has
   accepted it once (`delivered_at_ms`).
7. **Send-time render** (R11-M14): the row's body holds ids, outcome codes and order state only. The injected
   renderer (`SendTimeRenderer`) re-reads each input bar through the market-data read port by `(series,
   bar_end_ms)` (`src.riskmgr.render.bar_reads`) and passes them to the pure `render`, which shows favour R and
   favour / risk only when the bar's keyed hash verifies; an unavailable bar, a failing read or a failing renderer
   gives the text without them (never at PRE, C5). A body that decodes but cannot be rendered even without them is
   finished `bad_body`, never sent. The rendered text exists only in memory and in the message sent: it is never
   written back, logged or paged. The email's HTML part is built from the structured body
   (`src.notifications.email_html_arm.render_arm_html`: the SL advice block with the order-state entry, current stop
   and proposed stop; never favour R or favour / risk) and travels on the in-memory `OutboxRow.html` only; the
   immediate ping gets none (the email channel classifies it as a critical ping).
8. **Per-channel delivery:** each of the row's remaining channels (`arm_email`, `arm_push`) is tried on its own; a
   failed channel backs off min(60, 2^n) s; a channel that is not configured is retried at the cap (both channels
   are required before the ARM starts, §B14, so this only bridges a credential gap). A row with channels left is
   rescheduled (`name:n` encoded, always passing the 0016 CHECK); `delivered_at_ms` records the first acceptance.
   Five consecutive failures of one channel page "ARM channel failing" once per down episode.
9. **Finish:** when every channel accepted, the row is finished `delivered = true` and its body replaced with the
   notice id, the key, the decision ids and the outcome codes.

A notice keeps retrying until it is delivered, superseded or expired; a ping until it is delivered. A row whose
processing raises anything but a lost fence or a PostgreSQL connection error is logged by class name and counted
(`row_errors`), and the batch goes on, so one bad row never blocks the rows or the ping behind it. Counters and the
expiry page follow the store's answer: a row another pass or housekeeping finished first counts nothing. Errors are
reduced by `safe_error()` / `safe_db_error()`; nothing here logs or pages a body, a credential or a price.
"""
from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Final, Literal, Protocol

from src.db.arm_outbox_store import ArmOutboxRow, ArmRecentRow
from src.db.fence import EpochMismatch
from src.db.pg import is_connection_error
from src.db.safe_errors import safe_db_error
from src.marketdata.hashing import Keyring
from src.marketdata.ports import BarsRead
from src.marketdata.types import Bar, SeriesId
from src.notifications.email_html import FooterSource
from src.notifications.email_html_arm import render_arm_html
from src.notifications.email_theme import PipUnit, pip_unit_from
from src.notifications.errors import ChannelError, safe_error
from src.notifications.factory import ARM_CHANNEL_NAMES, ChannelSet
from src.notifications.relay import AUTH_FAST_POLL_MS, ChannelSource
from src.oversee.notify import Channel, OutboxRow
from src.riskmgr.notices import POST, NoticeMeta, supersedes
from src.riskmgr.outcomes import Outcome
from src.riskmgr.render import BodyError, NoticeBody, Rendered, RenderSettings, bar_reads, decode_body, render

if TYPE_CHECKING:
    from src.db.arm_outbox_store import PgArmOutboxInserter, PgArmOutboxStore
    from src.marketdata.ports import MarketDataPort
    from src.riskmgr.notices import NoticeOutboxPort

log = logging.getLogger(__name__)

TASK: Final = "arm-relay"
ARM_RELAY_INTERVAL_S: Final = 1.0
ARM_RELAY_MAX_STAMP_AGE_S: Final = 60.0     # design §B5.1
NOTICE_SEVERITY: Final = "info"
PING_SEVERITY: Final = "high"
"""The ARM relay never sends at `critical` (no emergency priority, RT-M4); the ping's critical copy is the Oversee
relay's (§B8 Immediate ping)."""
LATE: Final = "LATE"

ChannelState = Literal["unknown", "up", "down", "disabled"]
NoticeRenderer = Callable[[NoticeBody], Awaitable[Rendered]]
PageFn = Callable[[str, str, str], None]


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


# ---------------------------------------------------------------------------------------------------- ports
class ArmRelayStore(Protocol):
    """The relay's bookkeeping (`src.db.arm_outbox_store.PgArmOutboxStore` implements it)."""

    async def due(self, now_ms: int, limit: int) -> list[ArmOutboxRow]: ...

    async def pending(self, limit: int) -> list[ArmOutboxRow]: ...

    async def recent(self, since_ms: int, limit: int) -> list[ArmRecentRow]: ...

    async def oldest_undelivered_ms(self) -> int | None: ...

    async def reschedule(self, row: ArmOutboxRow, *, remaining: Sequence[str], failures: Mapping[str, int],
                         next_attempt_ms: int, error: str | None, attempted: bool,
                         delivered_at_ms: int | None) -> bool: ...

    async def finish(self, row: ArmOutboxRow, at_ms: int, *, delivered: bool, status: str, error: str | None,
                     attempted: bool, superseded_by: int | None = None) -> bool: ...


PointSizeSource = Callable[[Sequence[tuple[str, str]]], Awaitable[Mapping[tuple[str, str], object]]]
"""`(adapter_id, symbol)` pairs -> each instrument's `point_size` (`src.db.instrument_reads.PgPointSizes`)."""


class BarReadPort(Protocol):
    """The one read of `src.marketdata.ports.MarketDataPort` the renderer makes (a bucket-addressed bar read)."""

    async def bars(self, series: SeriesId, *, at_bucket_ms: int | None = None) -> object: ...


# ---------------------------------------------------------------------------------------------------- renderer
class SendTimeRenderer:
    """The injected renderer (R11-M14): re-reads each input bar of a body by `(series, bar_end_ms)` and renders.

    A bar that is unavailable (no port, `DataUnavailable`, a read error or timeout, a different bucket) is left out,
    so `render` omits the favour figures for its item; `render` itself shows them only when the bar's keyed hash
    verifies against the decision's stored hash (`keyring()` None: never).

    A body's reads run concurrently under one overall `read_timeout_s`, so a hung `redis-md` costs one timeout per
    notice, not one per series. After a read fails or times out, the port counts as down for the rest of the relay
    tick (`new_tick()` resets it): later notices of that tick skip their reads and go out without favour figures.

    `settings` (Q13 `show_levels`; the Q10 text follows each item's pinned `loosen_policy`) live here only: the relay's fallback text uses
    `fallback(body)`, so the normal and the fallback text never differ in them."""

    def __init__(self, md: BarReadPort | None, *, keyring: Callable[[], Keyring | None],
                 settings: RenderSettings | None = None, read_timeout_s: float = 2.0) -> None:
        self._md = md
        self._keyring = keyring
        self._settings = settings or RenderSettings()
        self._timeout = read_timeout_s
        self._md_down = False
        self.reads = 0
        self.unavailable = 0
        self.skipped = 0

    @property
    def settings(self) -> RenderSettings:
        return self._settings

    def new_tick(self) -> None:
        """A relay tick starts: try the market-data port again."""
        self._md_down = False

    def fallback(self, body: NoticeBody) -> Rendered:
        """The text without bar figures, with this renderer's settings (the relay's fallback)."""
        return render(body, settings=self._settings)

    async def _read(self, md: BarReadPort, series: SeriesId, bar_end_ms: int) -> Bar | None:
        """One bar; raises on a read error (the caller marks the port down). None: not held (a normal answer)."""
        got = await md.bars(series, at_bucket_ms=bar_end_ms - series.spec.timeframe_ms)
        if not isinstance(got, BarsRead):
            return None
        for bar in got.bars:
            if bar.series.canonical == series.canonical and bar.bucket_end_ms == bar_end_ms:
                return bar
        return None

    async def _read_all(self, wanted: Sequence[tuple[str, int]]) -> dict[tuple[str, int], Bar]:
        bars: dict[tuple[str, int], Bar] = {}
        md = self._md
        if md is None or not wanted:
            self.unavailable += len(wanted)
            return bars
        if self._md_down:
            self.unavailable += len(wanted)
            self.skipped += len(wanted)
            return bars
        tasks: dict[asyncio.Task[Bar | None], tuple[str, int]] = {}
        for series_text, bar_end_ms in wanted:
            try:
                series = SeriesId.parse(series_text)
            except Exception:   # a body problem, not the port: only this bar is left out
                self.unavailable += 1
                continue
            tasks[asyncio.ensure_future(self._read(md, series, bar_end_ms))] = (series_text, bar_end_ms)
        if not tasks:
            return bars
        try:
            done, pending = await asyncio.wait(tasks, timeout=self._timeout)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
            self._md_down = True
            self.unavailable += len(pending)
            log.debug("arm-relay: %d bar read(s) timed out; market data skipped for this tick", len(pending))
        for task in done:
            exc = task.exception()
            got = None if exc is not None else task.result()
            if exc is not None:   # an unavailable bar only omits the favour figures
                self._md_down = True
                log.debug("arm-relay: bar read failed (%s); market data skipped for this tick", type(exc).__name__)
            if got is None:
                self.unavailable += 1
            else:
                bars[tasks[task]] = got
        return bars

    async def __call__(self, body: NoticeBody) -> Rendered:
        wanted = list(dict.fromkeys(bar_reads(body)))
        self.reads += len(wanted)
        bars = await self._read_all(wanted)
        try:
            keyring = self._keyring()
        except Exception:
            keyring = None
        return render(body, bars=bars or None, keyring=keyring, settings=self._settings)


# ---------------------------------------------------------------------------------------------------- policy, status
@dataclass(frozen=True)
class ArmRelayPolicy:
    interval_s: float = ARM_RELAY_INTERVAL_S
    batch: int = 50
    backoff_base_ms: int = 1_000          # 2^n s
    backoff_max_ms: int = 60_000          # min(60, ...) s
    failing_after: int = 5                # consecutive failures of one channel -> "ARM channel failing" page
    recent_limit: int = 500               # rows read for the supersede check
    credentials_poll_ms: int = 5_000

    def backoff_ms(self, failures: int) -> int:
        if failures <= 0:
            return 0
        return min(self.backoff_max_ms, self.backoff_base_ms << min(failures, 30))


@dataclass(frozen=True)
class ArmChannelStatus:
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


@dataclass(frozen=True)
class ArmTickResult:
    due: int = 0
    delivered: int = 0       # rows finished with every channel delivered
    partial: int = 0         # rows rescheduled with channels left
    expired: int = 0
    superseded: int = 0
    suppressed: int = 0      # --drain-notices
    bad_body: int = 0        # a notice body that does not decode or render (finished, never sent)
    render_fallbacks: int = 0
    lease_lost: bool = False
    row_errors: int = 0      # rows whose processing raised (logged by class name; the batch went on)


@dataclass
class _Tally:
    due: int = 0
    delivered: int = 0
    partial: int = 0
    expired: int = 0
    superseded: int = 0
    suppressed: int = 0
    bad_body: int = 0
    render_fallbacks: int = 0
    lease_lost: bool = False
    row_errors: int = 0

    def freeze(self) -> ArmTickResult:
        return ArmTickResult(self.due, self.delivered, self.partial, self.expired, self.superseded, self.suppressed,
                             self.bad_body, self.render_fallbacks, self.lease_lost, self.row_errors)


@dataclass
class _Pass:
    failures: dict[str, int]
    remaining: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    tried: set[str] = field(default_factory=set)
    unconfigured: set[str] = field(default_factory=set)
    attempted: bool = False
    accepted: bool = False


def _backend_of(ch: Channel) -> str:
    b = getattr(ch, "backend", None)
    return b if isinstance(b, str) else ch.name


def expired_adjust(body: NoticeBody) -> bool:
    """§B8 Pages: a POST or LATE notice with an `ADJUST` item that expires undelivered pages."""
    return (body.event in (POST, LATE) or any(i.event == LATE for i in body.items)) and any(
        i.outcome == Outcome.ADJUST.value for i in body.items)


def meta_of(body: NoticeBody) -> NoticeMeta:
    return NoticeMeta(route=body.route, bar_spec=body.bar_spec, event=body.event, bar_end_ms=body.bar_end_ms,
                      part=body.part, item_key=body.item_key)


def superseded_by(row: ArmOutboxRow, body: NoticeBody, newer: Sequence[ArmRecentRow]) -> int | None:
    """The outbox id of the newest kind `arm` row that supersedes `row` (None: still current)."""
    old = meta_of(body)
    sent = row.delivered_at_ms is not None
    for r in newer:
        if r.outbox_id == row.outbox_id or r.is_ping or r.key is None:
            continue
        k = r.key
        new = NoticeMeta(route=k.route, bar_spec=k.bar_spec, event=k.event, bar_end_ms=k.bar_end_ms, part=k.part,
                         item_key=k.item_key)
        if supersedes(new, old, old_sent=sent):
            return r.outbox_id
    return None


# ---------------------------------------------------------------------------------------------------- the relay
class ArmRelay:
    """Supervised loop: `run(stop, stamp)`; `tick()` is one pass (tests drive it directly).

    Wiring (S31 / S32): `ArmRelay(PgArmOutboxStore(engine_db), cs.channels, renderer=SendTimeRenderer(md_port,
    keyring=...), disabled=cs.disabled, issues=cs.issues, lease_valid=keeper.lease_valid, channel_source=rc,
    drain_notices=controls.drain_notices, page=pager.page)` with `cs = rc.current` and `rc = RotatingChannels(
    settings_from_secrets(source, environ, prefix=ARM_PREFIX), client, names=ARM_CHANNEL_NAMES)`."""

    def __init__(self, store: ArmRelayStore, channels: Sequence[Channel], *, renderer: NoticeRenderer,
                 disabled: Sequence[str] = (), issues: Sequence[str] = (), policy: ArmRelayPolicy = ArmRelayPolicy(),
                 lease_valid: Callable[[], bool] = lambda: True, clock_ms: Callable[[], int] = _wall_ms,
                 drain_notices: Callable[[], bool] = lambda: False, channel_source: ChannelSource | None = None,
                 page: PageFn | None = None, render_settings: RenderSettings | None = None,
                 footer: FooterSource | None = None, email_theme: str = "classic",
                 point_sizes: PointSizeSource | None = None, point_size_timeout_s: float = 2.0) -> None:
        self._store = store
        self._footer = footer
        # The HTML part's look (XTNL_EMAIL_THEME) and, for the xtnl theme, the instruments' point sizes read at send
        # time so an ADJUST item's levels show pips from the entry (never the size itself; a failed read only drops
        # the pips).
        self._theme = email_theme
        self._point_sizes = point_sizes
        self._point_size_timeout_s = point_size_timeout_s
        self._renderer = renderer
        self._policy = policy
        self._lease_valid = lease_valid
        self._clock_ms = clock_ms
        self._drain = drain_notices
        self._source = channel_source
        self._page = page
        if isinstance(renderer, SendTimeRenderer):
            # One place for Q13 / Q10: the renderer's settings also make the fallback text (`fallback(body)`).
            if render_settings is not None and render_settings != renderer.settings:
                raise ValueError("render_settings differ from the SendTimeRenderer's settings; set them on the "
                                 "renderer only")
            render_settings = renderer.settings
        self._fallback_settings = render_settings or RenderSettings()
        self._channels: dict[str, Channel] = {}
        self._status: dict[str, ArmChannelStatus] = {}
        self._issues: tuple[str, ...] = ()
        self._failing_paged: set[str] = set()
        self._install(channels, disabled, issues)
        self._next_poll_ms = 0
        self._fast_poll_until_ms = 0
        self._oldest_undelivered: int | None = None
        self._reported = False
        self._db_down = False
        self.last_tick = ArmTickResult()

    # ---- channels ----------------------------------------------------------------------------------
    def _install(self, channels: Sequence[Channel], disabled: Sequence[str], issues: Sequence[str]) -> None:
        new: dict[str, Channel] = {}
        for ch in channels:
            if ch.name in new:
                raise ValueError(f"duplicate ARM channel {ch.name!r}")
            if ch.name not in ARM_CHANNEL_NAMES:
                raise ValueError(f"the ARM relay sends on {ARM_CHANNEL_NAMES} only, not {ch.name!r}")
            new[ch.name] = ch
        old = self._status
        status: dict[str, ArmChannelStatus] = {}
        for name, ch in new.items():
            prev = old.get(name)
            fresh = ArmChannelStatus(name, _backend_of(ch), "unknown")
            status[name] = fresh if prev is None else replace(
                fresh, delivered=prev.delivered, failed=prev.failed, last_ok_ms=prev.last_ok_ms,
                last_failure_ms=prev.last_failure_ms, last_error=prev.last_error)
        for name in disabled:
            if name not in status:
                status[name] = ArmChannelStatus(name, "none", "disabled")
        self._channels, self._status, self._issues = new, status, tuple(issues)
        self._failing_paged &= set(new)

    def replace_channels(self, channel_set: ChannelSet) -> None:
        """Use these channels from now on (rotated credentials). The rows' retry state is kept."""
        self._install(channel_set.channels, channel_set.disabled, channel_set.issues)
        log.info("arm-relay: channels rebuilt (%s)", ", ".join(
            f"{n}={s.backend}" for n, s in sorted(self._status.items())) or "none")

    def _poll_source(self, now: int) -> None:
        if self._source is None or (now < self._next_poll_ms and now >= self._fast_poll_until_ms):
            return
        self._next_poll_ms = now + self._policy.credentials_poll_ms
        try:
            cs = self._source.poll()
            if cs is not None:
                self.replace_channels(cs)
        except Exception as exc:
            log.warning("arm-relay: cannot reload channel credentials (%s); keeping the current channels",
                        safe_error(exc))

    def _auth_failed(self, now: int) -> None:
        if self._source is not None:
            try:
                self._source.invalidate()
            except Exception as exc:
                log.warning("arm-relay: cannot ask for fresh credentials (%s)", safe_error(exc))
            self._fast_poll_until_ms = now + AUTH_FAST_POLL_MS

    # ---- read-only views ---------------------------------------------------------------------------
    def status(self) -> Mapping[str, ArmChannelStatus]:
        return dict(self._status)

    def configured(self, channels: Sequence[str]) -> bool:
        """Every one of `channels` has a configured transport (the notices' 60 s page condition, §B8)."""
        return all(c in self._channels for c in channels)

    @property
    def warnings(self) -> tuple[str, ...]:
        disabled = tuple(f"ARM channel {n} is disabled (not configured)"
                         for n, s in sorted(self._status.items()) if s.state == "disabled")
        none = ("no ARM channel is configured: ARM notices will not be delivered",) if not self._channels else ()
        return none + disabled + self._issues

    def oldest_undelivered_ms(self) -> int | None:
        """Created time of the oldest unsent kind `arm` row at the last tick (dead-man field
        `arm_oldest_undelivered_notification_age_ms`)."""
        return self._oldest_undelivered

    def report_startup(self) -> tuple[str, ...]:
        w = self.warnings
        if not self._reported:
            self._reported = True
            for line in w:
                log.warning("arm-relay: %s", line)
        return w

    # ---- loop --------------------------------------------------------------------------------------
    async def run(self, stop: asyncio.Event, stamp: Callable[[], None] | None = None) -> None:
        self.report_startup()
        while not stop.is_set():
            try:
                await self.tick(stamp=stamp)
                if self._db_down:
                    log.info("arm-relay: PostgreSQL reachable again")
                self._db_down = False
            except EpochMismatch:
                raise
            except Exception as exc:
                if not is_connection_error(exc):
                    raise
                if not self._db_down:
                    log.warning("arm-relay: PostgreSQL unavailable (%s); retrying", safe_db_error(exc))
                self._db_down = True
            if stamp is not None:
                stamp()
            try:
                await asyncio.wait_for(stop.wait(), self._policy.interval_s)
            except TimeoutError:
                pass

    async def tick(self, *, stamp: Callable[[], None] | None = None) -> ArmTickResult:
        t = _Tally()
        now = self._clock_ms()
        self._poll_source(now)
        if not self._lease_valid():
            t.lease_lost = True
            self.last_tick = t.freeze()
            return self.last_tick
        if isinstance(self._renderer, SendTimeRenderer):
            self._renderer.new_tick()
        rows = await self._store.due(now, self._policy.batch)
        t.due = len(rows)
        live = [r for r in rows if not r.is_ping and (r.expires_at_ms is None or r.expires_at_ms > now)]
        newer: list[ArmRecentRow] = []
        if live:
            newer = await self._store.recent(min(r.created_ms for r in live), self._policy.recent_limit)
        drain = self._drain_now()
        for row in rows:
            if not self._lease_valid():
                t.lease_lost = True
                break
            await self._isolated(row, self._process(row, self._clock_ms(), newer, drain, t), t)
            if stamp is not None:
                stamp()
        if drain and not t.lease_lost:
            await self._drain_queued({r.outbox_id for r in rows}, t, stamp)
        self._oldest_undelivered = await self._store.oldest_undelivered_ms()
        self.last_tick = t.freeze()
        return self.last_tick

    async def _isolated(self, row: ArmOutboxRow, step: Awaitable[None], t: _Tally) -> None:
        """One row's work: a lost fence or a PostgreSQL connection error stops the tick (`run` handles both); any
        other error is logged by class name and counted, and the batch goes on, so one bad row never holds back the
        rows (and the ping) queued behind it."""
        try:
            await step
        except EpochMismatch:
            raise
        except Exception as exc:
            if is_connection_error(exc):
                raise
            t.row_errors += 1
            log.error("arm-relay: outbox row %d failed (%s); continuing with the next row", row.outbox_id,
                      type(exc).__name__)

    async def _drain_queued(self, seen: set[int], t: _Tally, stamp: Callable[[], None] | None) -> None:
        """`--drain-notices` (§B13): every queued, unsent notice is finished `suppressed`, also one still in back-off
        (not due yet), so a resume never sends it. Never a ping. Due rows were handled by `_process` already."""
        for row in await self._store.pending(self._policy.batch):
            now = self._clock_ms()
            if row.is_ping or row.outbox_id in seen or (row.expires_at_ms is not None and row.expires_at_ms <= now):
                continue        # a ping is never drained; an expired row takes the expiry path (and its page)
            if not self._lease_valid():
                t.lease_lost = True
                return
            await self._isolated(row, self._suppress(row, now, t), t)
            if stamp is not None:
                stamp()

    async def _suppress(self, row: ArmOutboxRow, now: int, t: _Tally) -> None:
        if await self._store.finish(row, now, delivered=False, status="suppressed", error="suppressed",
                                    attempted=False):
            t.suppressed += 1

    def _drain_now(self) -> bool:
        try:
            return bool(self._drain())
        except Exception as exc:   # unreadable controls are not a drain (§B13)
            log.warning("arm-relay: drain flag unreadable (%s); not draining", type(exc).__name__)
            return False

    # ---- one row -----------------------------------------------------------------------------------
    async def _process(self, row: ArmOutboxRow, now: int, newer: Sequence[ArmRecentRow], drain: bool,
                       t: _Tally) -> None:
        body: NoticeBody | None = None
        if not row.is_ping:
            try:
                body = decode_body(row.body)
            except (BodyError, ValueError):
                body = None
        if row.expires_at_ms is not None and row.expires_at_ms <= now:
            # Counted and paged only when this call finished the row (housekeeping or an earlier pass may have).
            if await self._store.finish(row, now, delivered=False, status="expired", error="expired",
                                        attempted=False):
                t.expired += 1
                if body is not None and expired_adjust(body):
                    self._page_once("warning", f"ARM notice #{body.notice_id} expired undelivered",
                                    f"route {body.route}, event {body.event}, part {body.part}: an ADJUST notice "
                                    "expired before any delivery completed (see arm decisions)")
            return
        if row.is_ping:
            out = OutboxRow("info", PING_SEVERITY, row.subject, row.body, row.created_ms, outbox_id=row.outbox_id)
        else:
            if drain:
                await self._suppress(row, now, t)
                return
            if body is None:
                await self._bad_body(row, now, t, "holds no notice body")
                return
            by = superseded_by(row, body, newer)
            if by is not None:
                if await self._store.finish(row, now, delivered=False, status="superseded", error="superseded",
                                            attempted=False, superseded_by=by):
                    t.superseded += 1
                return
            rendered = await self._render(body, t)
            if rendered is None:
                await self._bad_body(row, now, t, "cannot be rendered")
                return
            units = await self._pip_units(body)
            out = OutboxRow("info", NOTICE_SEVERITY, rendered.subject, rendered.text, row.created_ms,
                            outbox_id=row.outbox_id, html=self._html(body, row.outbox_id, rendered.subject, units))
        await self._deliver(row, out, now, t)

    async def _pip_units(self, body: NoticeBody) -> dict[tuple[str, str], PipUnit]:
        """The pip unit of every ADJUST item's instrument (xtnl theme only); {} when there is no source, nothing to
        read, or the read fails or times out (the email then shows the levels without pips)."""
        if self._theme != "xtnl" or self._point_sizes is None:
            return {}
        keys = sorted({(i.adapter_id, i.symbol) for i in body.items if i.outcome == "ADJUST" and i.symbol})
        if not keys:
            return {}
        try:
            sizes = await asyncio.wait_for(self._point_sizes(keys), self._point_size_timeout_s)
        except Exception as exc:   # the pips never cost a notice
            log.debug("arm-relay: point sizes not read (%s); levels without pips", type(exc).__name__)
            return {}
        out: dict[tuple[str, str], PipUnit] = {}
        for key, size in sizes.items():
            try:
                unit = pip_unit_from(Decimal(str(size)), key[1])
            except (ArithmeticError, ValueError):
                unit = None
            if unit is not None:
                out[key] = unit
        return out

    def _html(self, body: NoticeBody, outbox_id: int, subject: str,
              units: Mapping[tuple[str, str], PipUnit] | None = None) -> str | None:
        """The email's HTML part from the structured body (order-state levels only, never favour figures); None (the
        email channel then renders a generic page from the text, or sends text only) when it cannot be built."""
        try:
            return render_arm_html(body, footer=self._footer, settings=self._fallback_settings, outbox_id=outbox_id,
                                   title=subject, theme=self._theme, pip_units=units)
        except Exception as exc:   # the HTML part never costs a notice
            log.warning("arm-relay: HTML part of notice #%d not built (%s)", body.notice_id, type(exc).__name__)
            return None

    async def _bad_body(self, row: ArmOutboxRow, now: int, t: _Tally, why: str) -> None:
        if await self._store.finish(row, now, delivered=False, status="bad_body", error="bad_body", attempted=False):
            log.error("arm-relay: outbox row %d %s; finished unsent", row.outbox_id, why)
            t.bad_body += 1

    def _fallback(self, body: NoticeBody) -> Rendered:
        if isinstance(self._renderer, SendTimeRenderer):
            return self._renderer.fallback(body)
        return render(body, settings=self._fallback_settings)

    async def _render(self, body: NoticeBody, t: _Tally) -> Rendered | None:
        """The send-time text; on a renderer error the text without bar figures; None when even that cannot be
        made (a body that decodes but does not render, e.g. an out-of-range time): the row is then `bad_body`."""
        try:
            return await self._renderer(body)
        except Exception as exc:   # the text without favour figures, never no notice
            log.warning("arm-relay: renderer failed (%s); sending notice #%d without bar figures",
                        type(exc).__name__, body.notice_id)
        try:
            fallback = self._fallback(body)
        except Exception as exc:
            log.error("arm-relay: notice #%d cannot be rendered (%s)", body.notice_id, type(exc).__name__)
            return None
        t.render_fallbacks += 1
        return fallback

    async def _try(self, name: str, out: OutboxRow, now: int, p: _Pass, t: _Tally) -> None:
        if name not in self._channels:
            p.remaining.append(name)
            p.unconfigured.add(name)
            p.errors.append(f"{name}: not configured")
            return
        st = self._status[name]
        if st.retry_at_ms > now:
            p.remaining.append(name)
            p.errors.append(f"{name}: down")
            return
        if not self._lease_valid():           # synchronous check right before the externally visible send
            p.remaining.append(name)
            t.lease_lost = True
            return
        p.attempted = True
        p.tried.add(name)
        try:
            await self._channels[name].send(out)
        except Exception as exc:   # one channel's failure never stops the other
            err = safe_error(exc)
            p.remaining.append(name)
            p.errors.append(f"{name}: {err}")
            p.failures[name] = p.failures.get(name, 0) + 1
            if isinstance(exc, ChannelError) and exc.auth:
                self._auth_failed(now)
            self._channel_failed(name, now, err)
        else:
            p.failures.pop(name, None)
            p.accepted = True
            self._channel_ok(name, now)

    async def _deliver(self, row: ArmOutboxRow, out: OutboxRow, now: int, t: _Tally) -> None:
        p = _Pass(failures={c: n for c, n in row.failures if c in row.channels})
        for name in row.channels:
            await self._try(name, out, now, p, t)
        if not p.remaining:
            if await self._store.finish(row, now, delivered=True, status="sent", error=None, attempted=p.attempted):
                t.delivered += 1
            return
        if await self._store.reschedule(row, remaining=p.remaining, failures=p.failures,
                                        next_attempt_ms=self._next_attempt(p, now),
                                        error="; ".join(p.errors)[:500], attempted=p.attempted,
                                        delivered_at_ms=now if p.accepted else None):
            t.partial += 1

    def _next_attempt(self, p: _Pass, now: int) -> int:
        def ready(c: str) -> int:
            if c in p.unconfigured:
                return now + self._policy.backoff_max_ms
            status_ready = self._status[c].retry_at_ms if c in self._status else 0
            if c not in p.tried:
                return max(now, status_ready)
            return max(now + self._policy.backoff_ms(p.failures.get(c, 0)), status_ready)
        return min(ready(c) for c in p.remaining)

    def _channel_ok(self, name: str, now: int) -> None:
        st = self._status[name]
        if st.state == "down":
            log.info("ARM channel %s (%s) recovered after %d failures", name, st.backend, st.consecutive_failures)
        self._failing_paged.discard(name)
        self._status[name] = replace(st, state="up", consecutive_failures=0, retry_at_ms=0, last_ok_ms=now,
                                     delivered=st.delivered + 1)

    def _channel_failed(self, name: str, now: int, err: str) -> None:
        st = self._status[name]
        n = st.consecutive_failures + 1
        if st.state != "down":
            log.warning("ARM channel %s (%s) down: %s", name, st.backend, err)
        self._status[name] = replace(st, state="down", consecutive_failures=n,
                                     retry_at_ms=now + self._policy.backoff_ms(n), last_failure_ms=now,
                                     last_error=err, failed=st.failed + 1)
        if n >= self._policy.failing_after and name not in self._failing_paged:
            self._failing_paged.add(name)
            self._page_once("warning", f"ARM channel {name} failing",
                            f"{name} ({st.backend}): {n} consecutive delivery failures; ARM notices are queued and "
                            "retried until they expire")

    def _page_once(self, severity: str, title: str, body: str) -> None:
        if self._page is None:
            log.warning("arm-relay: %s", title)
            return
        try:
            self._page(severity, title, body)
        except Exception as exc:
            log.warning("arm-relay: page failed (%s)", type(exc).__name__)



if TYPE_CHECKING:
    def _ports_fit(inserter: PgArmOutboxInserter, store: PgArmOutboxStore,
                   md: MarketDataPort) -> tuple[NoticeOutboxPort, ArmRelayStore, BarReadPort]:
        """mypy proves the S29 stores fill the S28 `NoticeOutboxPort` and this relay's ports."""
        return inserter, store, md


__all__ = [
    "ARM_RELAY_INTERVAL_S", "ARM_RELAY_MAX_STAMP_AGE_S", "ArmChannelStatus", "ArmRelay", "ArmRelayPolicy",
    "ArmRelayStore", "ArmTickResult", "BarReadPort", "NOTICE_SEVERITY", "NoticeRenderer",
    "PING_SEVERITY", "SendTimeRenderer", "TASK", "expired_adjust", "meta_of", "superseded_by",
]
