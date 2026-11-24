"""Downtime catch-up (leader only): reconstruct what the broker changed while the engine was off.

The engine runs on a PC that is not on 24/7. Each leader term, and again whenever the UptimeTracker sees that the
process was suspended (PC sleep), every adapter goes through one catch-up:

1. **Fence first.** `IngestionOrchestrator.hold_fence(adapter, "catchup")` before SM4 starts (or at the resume):
   no reconcile clears the adapter's `stream_health` fence until the catch-up has finished (complete, or
   incomplete and paged), so ActionGate denies risk-increasing actions and Oversee caps every result as for a gap:
   nothing is released on a pre-catch-up image.
2. **Wait** for the adapter's first SNAPSHOTTING (at a resume: the forced RESYNCING) to commit, with SM4 STREAMING:
   the streams are SUBSCRIBED from then on, so nothing between the snapshot and live is missed.
3. **Window.** gap_start = `adapter_coverage.last_streaming_ms` - XTNL_CATCHUP_OVERLAP_S (default 900 s, at least
   the uptime heartbeat interval, so the heartbeat's interval of uncertainty is always re-read). Coverage ahead of
   the wall clock (the clock was stepped back) never yields an empty window: it proves nothing about when the
   engine stopped, so gap_start is that of a window without coverage (below; at most now - overlap), the
   catch-up is recorded incomplete and `clock moved back` is paged. Without a coverage row: the oldest of now -
   XTNL_CATCHUP_FIRST_RUN_DAYS (default 7), the earliest locally stored non-terminal order / OPEN position time,
   and the engine's last known active time before this term (`CatchupStore.prior_activity_ms`: the earliest
   last-seen / stop time of any earlier `engine_runs` row, and the newest `event_log` row written before this
   term); a window that would reach back further than MAX_WINDOW_DAYS is cut there, recorded incomplete and
   paged with the part not read. gap_end = now.
4. **Read** `adapter.fetch_history(gap_start, gap_end)` (shared contract `BrokerHistory`; an adapter without it
   gives an incomplete catch-up) and `fetch_cash_flows(gap_start)`, then submit every row through the
   orchestrator's normal REST path (`catchup_submit`: version compare, event_log, outbox, Redis apply in the
   consumers; never a direct write) and wait until it committed. A history may hold several versions of one
   order (StoneX: a stop / limit order WORKING at its placement, then its end): each is submitted, oldest first per
   entity, so each lands in event_log as its own APPLIED version; one older than the stored row is a normal stale
   drop, and re-reading the same window only advances the stored versions (same checksum: no new APPLIED row).
5. **Completeness check.** Every order still stored as working / position stored OPEN that is in neither the
   latest snapshot nor the history is re-read by id; a broker "not found" is an anomaly (never a close).
6. **Record** one fenced `catchup_runs` row. Counts new / updated / unchanged come from the stored rows before and
   after (checksum and status only), per entity: an order with two history versions counts once.
7. **Pages** (counts, ids and times only, never an amount): always PAGE[info] "engine was offline from X to Y
   (duration); catch-up: N orders new, M updated, K positions closed while offline"; PAGE[high] "downtime
   catch-up incomplete" when the history says incomplete, the broker's retention starts after gap_start
   (covered_from_ms > gap_start), a by-id re-read found nothing or failed, a row could not be versioned / mapped /
   committed, or the adapter has no history at all.
8. **Release** the fence hold (the newest committed reconcile clears the gaps; anything still open resyncs).

A read that raises (or runs past `fetch_timeout_s`) is retried with backoff (the fence stays held); after
`max_attempts` the catch-up is recorded incomplete, paged, and the hold released (the page names the uncovered
window).

**Coverage is only vouched for what was really read** (`vouches()`, which gates the UptimeTracker's
`adapter_coverage` heartbeat):
* An incomplete catch-up whose cause is on this side or passing (every read attempt failed, a history answer
  whose `BrokerHistory.retryable` is not False: a failed or timed-out page or request, a refused budget, a row
  the adapter could not map, evidence that did not add up; a by-id re-read failed or unmappable; rows not
  committed in time or parked on a full topic; a history that does not say what it covers) is *retryable*:
  coverage stays where it was, so the next start-up re-reads the same window, and a `retry` catch-up runs again
  after `retry_after_s` (doubling, capped at `retry_after_max_s`) until one is not retryable. Fail closed: an
  incomplete history whose cause is unknown is retryable. Only what the broker itself cannot answer better
  (retention, its own truncation / paging limit, the unconfirmed paging rule, no history facility, a "not found"
  by id, a row without a version) and this side's fixed limits (the window cap, a clock stepped back) are paged
  once without holding coverage back: re-reading would give the same answer.
* An interruption of the adapter's orders / account path after the catch-up read (a gap, a lost socket or auth,
  a live row the adapter could not map: `AdapterRuntime.last_interruption_seq`) also stops coverage, because
  orders placed and finished during it are in no reconcile snapshot; a `reconnect` catch-up then re-reads the
  history from the unchanged coverage once a reconcile pass sent after the interruption has committed. Rows the
  adapter cannot map during the catch-up's own reads count on their own channel (`history_read`), so they make
  the catch-up incomplete without ever counting as, or hiding, a live-stream interruption.
Read-only: nothing here sends, modifies or cancels an order.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Final, Literal, Protocol

from src.db.uptime_store import CatchupRecord, Coverage, StoredRow
from src.dto import CashFlowDTO, OrderDTO, PositionDTO
from src.observability.metrics import CounterVec, Sample, gauge
from src.pubsub.connection import ConnState
from src.pubsub.orchestrator import CatchupFetch, CatchupPass, IngestionOrchestrator
from src.runtime.leader_init import is_fence_loss

log = logging.getLogger(__name__)

#: 'reconnect' and 'retry' need migration 0013 (catchup_runs.reason CHECK).
CatchupReason = Literal["startup", "resume_after_sleep", "operator", "reconnect", "retry"]
#: Reasons that follow an engine downtime (the PAGE[info] summary is always sent and the offline gap is measured).
DOWNTIME_REASONS: Final = frozenset({"startup", "resume_after_sleep", "operator"})
HOLD_REASON: Final = "catchup"
OVERLAP_VAR: Final = "XTNL_CATCHUP_OVERLAP_S"
FIRST_RUN_DAYS_VAR: Final = "XTNL_CATCHUP_FIRST_RUN_DAYS"
DEFAULT_OVERLAP_S: Final = 900
#: Floor of the overlap whatever the heartbeat: a sleep shorter than the 120 s detection threshold goes unnoticed
#: until the broker socket's 30 s silence check drops it, and a coverage heartbeat may land in between.
MIN_OVERLAP_S: Final = 300
DEFAULT_FIRST_RUN_DAYS: Final = 7
MAX_FIRST_RUN_DAYS: Final = 365
#: Without a coverage row the window never reaches back further than this (cut, recorded incomplete, paged).
MAX_WINDOW_DAYS: Final = 365
#: Coverage this far ahead of the wall clock means the clock was stepped back (coverage is wall-clock time).
CLOCK_BACK_TOLERANCE_MS: Final = 1_000
DAY_MS: Final = 86_400_000
#: xtnl_catchup_rows_total{kind}
ROW_KINDS: Final = ("order_new", "order_updated", "position_new", "position_closed", "cash_flow_new", "anomaly")


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


def iso_utc(ms: int | None) -> str:
    if ms is None:
        return "?"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def duration_text(ms: int) -> str:
    s = max(0, ms) // 1000
    d, s = divmod(s, 86_400)
    h, s = divmod(s, 3_600)
    m, s = divmod(s, 60)
    if d:
        return f"{d}d {h}h {m}m"
    if h:
        return f"{h}h {m}m"
    return f"{m}m {s}s"


@dataclass(frozen=True)
class CatchupSettings:
    overlap_ms: int = DEFAULT_OVERLAP_S * 1000
    first_run_days: int = DEFAULT_FIRST_RUN_DAYS
    commit_timeout_s: float = 60.0      # the consumers commit the submitted rows within this
    max_attempts: int = 5               # a read that raises is retried; then recorded incomplete and paged
    retry_s: float = 5.0                # first retry delay (doubles, capped at 60 s)
    poll_s: float = 0.25                # waiting for the first committed snapshot
    fetch_timeout_s: float = 600.0      # one history + cash-flow read (a hung read is a failed attempt)
    reread_timeout_s: float = 30.0      # one by-id re-read
    retry_after_s: float = 300.0        # a retryable incomplete catch-up runs again after this (doubling) ...
    retry_after_max_s: float = 3600.0   # ... capped here
    max_window_days: int = MAX_WINDOW_DAYS  # a first-run window never reaches back further (cut and paged)

    @staticmethod
    def from_env(environ: Mapping[str, str] | None = None, *, ping_s: int | None = None) -> CatchupSettings:
        """XTNL_CATCHUP_OVERLAP_S (seconds, >= the uptime heartbeat interval so its uncertainty is always
        covered, and >= MIN_OVERLAP_S; default 900) and XTNL_CATCHUP_FIRST_RUN_DAYS (1..365, default 7). Raises ValueError (names only)."""
        e = os.environ if environ is None else environ
        overlap = _int_var(e, OVERLAP_VAR, DEFAULT_OVERLAP_S)
        days = _int_var(e, FIRST_RUN_DAYS_VAR, DEFAULT_FIRST_RUN_DAYS)
        floor = max(MIN_OVERLAP_S, ping_s if ping_s is not None else 0)
        if overlap < floor:
            raise ValueError(f"{OVERLAP_VAR} must be at least {floor} s (the uptime heartbeat interval, and "
                             f"{MIN_OVERLAP_S} s for an undetected short sleep plus the stream-silence detection)")
        if overlap > 7 * 86_400:
            raise ValueError(f"{OVERLAP_VAR} must be at most 604800 s")
        if not 1 <= days <= MAX_FIRST_RUN_DAYS:
            raise ValueError(f"{FIRST_RUN_DAYS_VAR} must be between 1 and {MAX_FIRST_RUN_DAYS}")
        return CatchupSettings(overlap_ms=overlap * 1000, first_run_days=days)


def _int_var(e: Mapping[str, str], name: str, default: int) -> int:
    raw = (e.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name} must be a whole number of seconds / days") from None


class CatchupStore(Protocol):
    """src/db/uptime_store.py PgCatchupStore."""

    async def coverage(self, adapter_id: str) -> Coverage | None: ...

    async def stored_orders(self, adapter_id: str, broker_order_ids: Collection[str]) -> Mapping[str, StoredRow]: ...

    async def stored_positions(self, adapter_id: str, position_ids: Collection[str]) -> Mapping[str, StoredRow]: ...

    async def known_cash_flows(self, adapter_id: str, transaction_ids: Collection[str]) -> set[str]: ...

    async def record(self, epoch_token: int, rec: CatchupRecord) -> int: ...

    async def prior_activity_ms(self, adapter_id: str, run_id: str | None, before_ms: int) -> int | None:
        """The engine's last known active time before `before_ms` (this term's start), for a window without a
        coverage row: the earliest last-seen / stop time of any earlier engine run (not `run_id`), and the newest
        event the engine logged before `before_ms`; the older of the two. None when nothing is recorded."""
        ...


class OpenEntities(Protocol):
    async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]: ...


class CatchupMetrics:
    """Process-wide catch-up series (deploy/observability/metrics.md): cumulative across leader terms."""

    def __init__(self) -> None:
        self.runs = CounterVec("xtnl_catchup_runs_total")
        self.rows = CounterVec("xtnl_catchup_rows_total")
        self.offline_gap_s: float | None = None

    def samples(self) -> list[Sample]:
        out = self.runs.samples() + self.rows.samples()
        if self.offline_gap_s is not None:
            out += gauge("xtnl_offline_gap_seconds", self.offline_gap_s)
        return out


@dataclass
class CatchupResult:
    """One finished catch-up (what was recorded and paged)."""

    record: CatchupRecord
    anomalies: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    uncovered: tuple[int, int] | None = None
    #: Entity keys (ids only) of history rows skipped for want of a broker revision / time; named in their own
    #: clause of the incomplete page, so the 600-character cut of the notes can never drop them.
    unversioned: list[str] = field(default_factory=list)
    #: Incomplete for a cause on this side (failed reads / re-reads, rows not committed or parked on a full
    #: topic): coverage is held back and the catch-up runs again (module doc).
    retryable: bool = False


@dataclass
class _Request:
    reason: CatchupReason
    baseline: int          # passes_committed when requested: wait for a newer committed pass
    at_ms: int             # the end of the offline span (term start / resume detection)


Pager = Callable[[str, str, str], None]


@dataclass
class _Window:
    start: int
    offline_from: int | None
    notes: list[str] = field(default_factory=list)       # why the window itself cannot prove the gap (incomplete)
    uncovered: tuple[int, int] | None = None             # the part of the wanted window that was cut
    clock_back: bool = False


class CatchupCoordinator:
    """Per-term, leader only. `begin_startup()` before SM4 starts; `run()` is the supervised task."""

    def __init__(self, orchestrator: IngestionOrchestrator, store: CatchupStore, local_state: OpenEntities, *,
                 adapter_ids: Callable[[], Sequence[str]], epoch_token: Callable[[], int],
                 run_id: Callable[[], str | None], pager: Pager, settings: CatchupSettings | None = None,
                 metrics: CatchupMetrics | None = None, clock_ms: Callable[[], int] = _wall_ms) -> None:
        self._orch = orchestrator
        self._store = store
        self._local = local_state
        self._ids = adapter_ids
        self._epoch = epoch_token
        self._run_id = run_id
        self._page = pager
        self.settings = settings or CatchupSettings()
        self.metrics = metrics or CatchupMetrics()
        self._clock = clock_ms
        self._pending: dict[str, _Request] = {}
        self._jobs: dict[str, asyncio.Task[CatchupResult | None]] = {}
        self._running: dict[str, _Request] = {}   # the request each job serves (re-queued if the task restarts)
        self._finished: set[str] = set()          # adapters whose latest requested catch-up has finished
        #: adapter -> AdapterRuntime.last_interruption_seq when its newest catch-up read started (an interruption
        #: after it is not covered by that read).
        self._marks: dict[str, int] = {}
        self._retry_due: dict[str, int] = {}      # adapter -> wall ms when a retryable catch-up runs again
        self._retry_delay_s: dict[str, float] = {}
        self._last_complete: dict[str, bool] = {}  # adapter -> whether its newest finished catch-up was complete
        self.results: list[CatchupResult] = []    # this term's finished catch-ups (newest last)
        self.liveness_ms: int | None = None
        self._term_start_ms = clock_ms()           # events logged before it are the engine's earlier activity
        self._clock_back_paged: set[str] = set()
        self._window_starts: dict[str, int] = {}  # adapter -> start of the window its newest attempt computed

    # ------------------------------------------------------------------ requests
    def begin_startup(self) -> None:
        """LEADER_INIT step 6, before SM4 starts: hold every adapter's fence and queue its start-up catch-up."""
        now = self._clock()
        for aid in self._ids():
            self.request(aid, "startup", at_ms=now, baseline=0)

    def request(self, adapter_id: str, reason: CatchupReason, *, at_ms: int | None = None,
                baseline: int | None = None) -> None:
        """Hold the adapter's fence and queue a catch-up that runs once a reconcile pass newer than now has
        committed. A request while one is running runs again after it (the hold stays until the last one)."""
        rt = self._orch.runtime(adapter_id)
        self._orch.hold_fence(adapter_id, HOLD_REASON)
        self._finished.discard(adapter_id)
        self._pending[adapter_id] = _Request(reason, rt.passes_committed if baseline is None else baseline,
                                             self._clock() if at_ms is None else at_ms)
        log.warning("%s: downtime catch-up requested (%s)", adapter_id, reason)

    def resume_after_sleep(self, suspended_ms: int | None = None) -> None:
        """The process was suspended: every adapter re-subscribes (forced RESYNCING) and catches up again."""
        now = self._clock()
        for aid in self._ids():
            self.request(aid, "resume_after_sleep", at_ms=now)
            try:
                self._orch.force_resync(aid, "process was suspended (sleep)")
            except Exception:
                log.exception("%s: forced RESYNCING after sleep failed", aid)

    def finished(self, adapter_id: str) -> bool:
        """This term's catch-up of the adapter has finished (complete or paged) and none is pending."""
        return adapter_id in self._finished and adapter_id not in self._pending and adapter_id not in self._jobs

    def vouches(self, adapter_id: str) -> bool:
        """Coverage may advance for the adapter (the UptimeTracker heartbeat): its catch-up finished, it was not
        retryable-incomplete, and the orders / account path was not interrupted since that catch-up's read."""
        if not self.finished(adapter_id) or adapter_id in self._retry_due:
            return False
        mark = self._marks.get(adapter_id)
        return mark is not None and self._orch.runtime(adapter_id).last_interruption_seq <= mark

    def verified(self, adapter_id: str) -> bool:
        """ActionGate input (`catchup_verified`, oversee-rules.md §8): the adapter's book is proven to hold what the
        broker changed while the engine was off. True only when `vouches()` and this term's newest finished
        catch-up of the adapter was complete. Fail closed: an incomplete catch-up (broker retention, the
        history's own incomplete flag, no history facility, a "not found" by id, a row that could not be
        versioned) keeps risk-increasing actions denied for the adapter until a later catch-up of this term is
        complete; a new term starts unverified."""
        return self._last_complete.get(adapter_id, False) and self.vouches(adapter_id)

    def _follow_ups(self) -> None:
        """Queue a `retry` whose time has come, and a `reconnect` for an adapter interrupted after its read."""
        now = self._clock()
        for aid in self._ids():
            if aid in self._pending or aid in self._jobs or aid not in self._finished:
                continue
            due = self._retry_due.get(aid)
            if due is not None:
                if now >= due:
                    del self._retry_due[aid]
                    self.request(aid, "retry", baseline=0)
                continue
            mark = self._marks.get(aid)
            if mark is not None and self._orch.runtime(aid).last_interruption_seq > mark:
                self.request(aid, "reconnect", baseline=0)

    def busy(self) -> bool:
        return bool(self._pending or self._jobs)

    # ------------------------------------------------------------------ task
    async def run(self, stop: asyncio.Event, stamp: Callable[[], None] | None = None) -> None:
        """Supervised: starts each queued catch-up (adapters in parallel) and reaps finished ones. A lost fence
        raised by a job propagates (the runtime hard-exits); any other job error is handled inside the job."""
        try:
            while not stop.is_set():
                self.liveness_ms = self._clock()
                if stamp is not None:
                    stamp()
                for aid, job in list(self._jobs.items()):
                    if job.done():
                        del self._jobs[aid]
                        self._running.pop(aid, None)
                        exc = None if job.cancelled() else job.exception()
                        if exc is not None:
                            raise exc
                self._follow_ups()
                for aid in list(self._pending):
                    if aid not in self._jobs:
                        req = self._pending.pop(aid)
                        self._running[aid] = req
                        self._jobs[aid] = asyncio.create_task(self._job(aid, req), name=f"catchup:{aid}")
                try:
                    await asyncio.wait_for(stop.wait(), self.settings.poll_s)
                except TimeoutError:
                    pass
        finally:
            # A job cut short (this task restarted by the supervisor) is queued again, so its fence hold is
            # released only by a catch-up that really finished.
            for aid, job in self._jobs.items():
                if not job.done():
                    job.cancel()
                    cut = self._running.get(aid)
                    if cut is not None:
                        self._pending.setdefault(aid, cut)
            await asyncio.gather(*self._jobs.values(), return_exceptions=True)
            self._jobs.clear()
            self._running.clear()

    async def _job(self, aid: str, req: _Request) -> CatchupResult | None:
        delay = self.settings.retry_s
        attempt = 0
        result: CatchupResult | None = None
        self._window_starts.pop(aid, None)         # only this catch-up's own attempts name its window
        started = self._clock()
        while True:
            try:
                await self._wait_for_pass(aid, req.baseline)
                started = self._clock()
                result = await self._attempt(aid, req, started)
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_fence_loss(exc):
                    raise
                attempt += 1
                log.warning("%s: downtime catch-up attempt %d failed: %s", aid, attempt, type(exc).__name__)
                if attempt >= self.settings.max_attempts:
                    result = await self._give_up(aid, req, started, f"{type(exc).__name__} after {attempt} attempts")
                    break
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)
        if aid not in self._pending:
            self._finished.add(aid)
            self._orch.release_fence(aid, HOLD_REASON)
        return result

    async def _wait_for_pass(self, aid: str, baseline: int) -> None:
        """STREAMING, with a committed reconcile pass newer than the request and sent after the newest
        interruption (so the streams were SUBSCRIBED again before it: nothing between it and live is missed)."""
        while True:
            rt = self._orch.runtime(aid)
            if (rt.passes_committed > baseline and rt.last_committed_send_seq > rt.last_interruption_seq
                    and rt.conn.state is ConnState.STREAMING):
                return
            await asyncio.sleep(self.settings.poll_s)

    # ------------------------------------------------------------------ one catch-up
    async def _window(self, aid: str, now: int) -> _Window:
        """gap_start and offline_from: from adapter_coverage (clamped when it is ahead of the wall clock), else the
        oldest of the first-run floor, the stored working entities and the engine's last known active time."""
        overlap = self.settings.overlap_ms
        cov = await self._store.coverage(aid)
        if cov is not None:
            if cov.last_streaming_ms > now + CLOCK_BACK_TOLERANCE_MS:
                # Coverage (wall-clock time) is in the future: it proves nothing about when the engine stopped.
                # Read as if there were no coverage (never less than the overlap), recorded incomplete and paged.
                first = await self._first_run_window(aid, now)
                start = min(now - overlap, first.start)
                return _Window(start, cov.last_streaming_ms, [
                    f"clock moved back: adapter_coverage {iso_utc(cov.last_streaming_ms)} is ahead of the wall "
                    f"clock {iso_utc(now)}; the offline span is unknown, read as without coverage from "
                    f"{iso_utc(start)}", *first.notes], uncovered=first.uncovered, clock_back=True)
            return _Window(min(now, cov.last_streaming_ms - overlap), cov.last_streaming_ms)
        return await self._first_run_window(aid, now)

    async def _first_run_window(self, aid: str, now: int) -> _Window:
        """Without usable coverage: the oldest of the first-run floor, the stored working entities' broker times and
        the engine's last known active time before this term; cut (incomplete, paged) at `max_window_days`."""
        orders, positions = await self._local.open_entities(aid)
        times = [o.broker_updated_at_ms for o in orders if o.broker_updated_at_ms > 0]
        times += [p.broker_updated_at_ms for p in positions if p.broker_updated_at_ms > 0]
        prior = await self._store.prior_activity_ms(aid, self._run_id(), self._term_start_ms)
        if prior is not None:
            times.append(prior)
        wanted = min([now - self.settings.first_run_days * DAY_MS, *times])
        cap = now - self.settings.max_window_days * DAY_MS
        if wanted < cap:
            return _Window(cap, None, [
                f"no coverage recorded and the window would start at {iso_utc(wanted)}: cut at "
                f"{self.settings.max_window_days} days"], uncovered=(wanted, cap))
        return _Window(wanted, None)

    async def _attempt(self, aid: str, req: _Request, started: int) -> CatchupResult:
        self._marks[aid] = self._orch.runtime(aid).last_interruption_seq
        win = await self._window(aid, started)
        self._window_starts[aid] = win.start       # `_give_up` names at least this window if every attempt fails
        gap_start, offline_from = win.start, win.offline_from
        gap_end = started
        if win.clock_back and aid not in self._clock_back_paged:
            self._clock_back_paged.add(aid)
            self._page("high", f"clock moved back: {aid}",
                       f"{aid}: clock_moved_back; {win.notes[0]}. The catch-up is recorded incomplete; check the "
                       "PC clock and the broker's order history")
        fetch: CatchupFetch = await asyncio.wait_for(self._orch.catchup_fetch(aid, gap_start, gap_end),
                                                     self.settings.fetch_timeout_s)
        history = fetch.history
        h_orders = [o for o in tuple(getattr(history, "orders", ()) or ()) if isinstance(o, OrderDTO)]
        h_positions = [p for p in tuple(getattr(history, "positions", ()) or ()) if isinstance(p, PositionDTO)]
        order_ids = {o.broker_order_id for o in h_orders}      # entities: several versions of one order count once
        position_ids = {p.position_id for p in h_positions}
        flow_ids = {cf.transaction_id for cf in fetch.cash_flows if isinstance(cf, CashFlowDTO)}

        # completeness: stored working orders / OPEN positions neither in the latest snapshot nor in the history
        rt = self._orch.runtime(aid)
        local_orders, local_positions = await self._local.open_entities(aid)
        recheck_o = sorted({o.broker_order_id for o in local_orders
                            if o.entity_key not in rt.last_seen_orders and o.broker_order_id not in order_ids})
        recheck_p = sorted({p.position_id for p in local_positions
                            if p.entity_key not in rt.last_seen_positions and p.position_id not in position_ids})

        all_o = order_ids | set(recheck_o)
        all_p = position_ids | set(recheck_p)
        before_o = await self._store.stored_orders(aid, all_o)
        before_p = await self._store.stored_positions(aid, all_p)
        before_cf = await self._store.known_cash_flows(aid, flow_ids)
        res: CatchupPass = await self._orch.catchup_submit(fetch, recheck_orders=recheck_o,
                                                           recheck_positions=recheck_p,
                                                           timeout_s=self.settings.commit_timeout_s,
                                                           reread_timeout_s=self.settings.reread_timeout_s)
        after_o = await self._store.stored_orders(aid, all_o)
        after_p = await self._store.stored_positions(aid, all_p)
        after_cf = await self._store.known_cash_flows(aid, flow_ids)

        o_new = sum(1 for i in all_o if i not in before_o and i in after_o)
        o_upd = sum(1 for i in all_o if i in before_o and i in after_o
                    and (before_o[i].checksum != after_o[i].checksum or before_o[i].status != after_o[i].status))
        p_new = sum(1 for i in all_p if i not in before_p and i in after_p)
        p_closed = sum(1 for i in all_p if i in after_p and after_p[i].status == "CLOSED"
                       and (i not in before_p or before_p[i].status == "OPEN"))
        cf_new = len((after_cf - before_cf) & flow_ids)

        notes: list[str] = list(win.notes)
        anomalies = list(res.not_found) + list(res.reread_errors)
        covered_from: int | None = None
        uncovered: tuple[int, int] | None = None       # of the read window (the cut part is added below)
        history_retryable = False
        if history is None:
            notes.append("adapter has no fetch_history: the window could not be read")
            uncovered = (gap_start, gap_end)
        else:
            complete_flag = getattr(history, "complete", False) is True
            raw_cov = getattr(history, "covered_from_ms", None)
            covered_from = int(raw_cov) if isinstance(raw_cov, int) and not isinstance(raw_cov, bool) else None
            if covered_from is None:
                notes.append("history did not say which time it covers")
                uncovered = (gap_start, gap_end)
                history_retryable = True                  # unknown cause: fail closed
            elif covered_from > gap_start:
                notes.append("broker history retention starts after the gap start")
                uncovered = (gap_start, min(covered_from, gap_end))
            if not complete_flag:
                # Only an answer that says every cause is a broker limit (retryable is False) moves on.
                history_retryable = history_retryable or getattr(history, "retryable", True) is not False
                notes.append("history incomplete" + (" (retryable)" if history_retryable else " (broker limit)"))
                for n in tuple(getattr(history, "notes", ()) or ())[:5]:
                    notes.append(str(n)[:200])
                if uncovered is None:
                    uncovered = (gap_start, gap_end)
        if fetch.history_unmapped:
            notes.append(f"{fetch.history_unmapped} rows the adapter could not map during the history read")
            if uncovered is None:
                uncovered = (gap_start, gap_end)
        if win.uncovered is not None:
            uncovered = _span(uncovered, win.uncovered) if uncovered is not None else win.uncovered
        # Cash-flow completeness is reported but does NOT decide `complete`: `complete` means no order / position
        # can be missing (the user's requirement), and a partial cash-flow ledger is a StoneX data property (closing
        # trades without RealisedPnl, live 2026-11-14). It still pages on its own ("cash-flow ledger incomplete")
        # and keeps MaxDailyLossRule at pnl_unverified (oversee-rules.md §5).
        cash_note = ("cash-flow ledger partial (P&L unverified): " + "; ".join(fetch.cash_flows_partial)[:300]
                     if fetch.cash_flows_partial else None)
        if res.not_found:
            notes.append(f"{len(res.not_found)} stored working entities not found by id at the broker")
        if res.reread_errors:
            notes.append(f"{len(res.reread_errors)} by-id re-reads failed")
        unversioned = sorted(set(res.unversioned))     # entity keys (ids only); one entity may skip several rows
        if unversioned:
            notes.append(f"{len(res.unversioned)} history rows without a broker revision / time skipped: "
                         + _id_list(unversioned))
            # No broker time: nothing narrows when they changed, so the whole read window is unverified for them.
            uncovered = _span(uncovered, (gap_start, gap_end))
        if res.skipped:
            notes.append(f"{len(res.skipped)} history rows of another adapter or type skipped")
        if res.unmapped:
            notes.append(f"{res.unmapped} by-id re-read rows the adapter could not map")
        if res.parked:
            notes.append(f"{res.parked} rows parked in events_dlq")
        if res.timed_out:
            notes.append("rows not committed within the commit timeout")
        elif not res.committed:
            notes.append("some rows were not committed: " + ",".join(sorted(res.failed_streams)))
        complete = not notes and not anomalies
        if cash_note is not None:
            notes.append(cash_note)
        retryable = bool(res.timed_out or not res.committed or res.reread_errors or res.parked_queue_full
                         or history_retryable or fetch.history_unmapped or res.unmapped)
        finished = self._clock()
        rec = CatchupRecord(
            run_id=self._run_id(), adapter_id=aid, reason=req.reason, gap_start_ms=gap_start, gap_end_ms=gap_end,
            covered_from_ms=covered_from, offline_from_ms=offline_from, started_at_ms=started,
            finished_at_ms=max(finished, started), orders_seen=len(all_o), orders_new=o_new, orders_updated=o_upd,
            positions_seen=len(all_p), positions_new=p_new, positions_closed=p_closed, cash_flows_new=cf_new,
            anomalies=len(anomalies), complete=complete, notes="; ".join(notes))
        result = CatchupResult(rec, anomalies, notes, uncovered, unversioned, retryable=retryable and not complete)
        await self._store.record(self._epoch(), rec)
        self._finish(result, req)
        return result

    async def _give_up(self, aid: str, req: _Request, started: int, why: str) -> CatchupResult:
        """Every attempt failed: record (best effort) and page incomplete; the caller releases the hold."""
        now = self._clock()
        cov = None
        try:
            cov = await self._store.coverage(aid)
        except Exception as exc:
            if is_fence_loss(exc):
                raise
        offline_from = None if cov is None else cov.last_streaming_ms
        if cov is None:
            gap_start = now - self.settings.first_run_days * DAY_MS
        elif cov.last_streaming_ms > now + CLOCK_BACK_TOLERANCE_MS:     # clock stepped back: never an empty window
            gap_start = now - self.settings.overlap_ms
        else:
            gap_start = cov.last_streaming_ms - self.settings.overlap_ms
        # An attempt that got as far as its window (the engine's last known active time, stored working entities,
        # a clock stepped back) may have wanted more than the floor above: the page names the whole of it.
        gap_start = min(gap_start, self._window_starts.get(aid, gap_start))
        rec = CatchupRecord(run_id=self._run_id(), adapter_id=aid, reason=req.reason, gap_start_ms=min(gap_start, now),
                            gap_end_ms=now, covered_from_ms=None, offline_from_ms=offline_from,
                            started_at_ms=min(started, now), finished_at_ms=now, complete=False,
                            notes=f"catch-up failed: {why}")
        result = CatchupResult(rec, [], [f"catch-up failed: {why}"], (rec.gap_start_ms, now), retryable=True)
        try:
            await self._store.record(self._epoch(), rec)
        except Exception as exc:
            if is_fence_loss(exc):
                raise
            log.error("%s: catchup_runs row not written: %s", aid, type(exc).__name__)
        self._finish(result, req)
        return result

    def _finish(self, result: CatchupResult, req: _Request) -> None:
        rec = result.record
        aid = rec.adapter_id
        self._last_complete[aid] = rec.complete
        self.results.append(result)
        del self.results[:-50]
        retry_note = ""
        if result.retryable:
            delay = self._retry_delay_s.get(aid, self.settings.retry_after_s)
            self._retry_due[aid] = self._clock() + int(delay * 1000)
            self._retry_delay_s[aid] = min(delay * 2, self.settings.retry_after_max_s)
            retry_note = (f"; adapter_coverage held back (the next start-up re-reads this window); retry in "
                          f"{duration_text(int(delay * 1000))}")
        else:
            self._retry_due.pop(aid, None)
            self._retry_delay_s.pop(aid, None)
        m = self.metrics
        m.runs.inc(adapter=aid, complete="true" if rec.complete else "false")
        for kind, n in (("order_new", rec.orders_new), ("order_updated", rec.orders_updated),
                        ("position_new", rec.positions_new), ("position_closed", rec.positions_closed),
                        ("cash_flow_new", rec.cash_flows_new), ("anomaly", rec.anomalies)):
            if n:
                m.rows.inc(n, adapter=aid, kind=kind)
        downtime = rec.reason in DOWNTIME_REASONS
        if rec.offline_from_ms is not None and downtime:
            m.offline_gap_s = max(0, req.at_ms - rec.offline_from_ms) / 1000.0
        # pages: counts, ids and times only (never an amount)
        if rec.offline_from_ms is not None and not downtime:
            what = "stream interrupted" if rec.reason == "reconnect" else "retry"
            span = (f"{what}; re-read from the last coverage {iso_utc(rec.offline_from_ms)} to "
                    f"{iso_utc(rec.gap_end_ms)}")
        elif rec.offline_from_ms is not None:
            span = (f"engine was offline from {iso_utc(rec.offline_from_ms)} to {iso_utc(req.at_ms)} "
                    f"({duration_text(req.at_ms - rec.offline_from_ms)})")
        else:
            span = (f"no earlier coverage recorded (first run); window from {iso_utc(rec.gap_start_ms)} to "
                    f"{iso_utc(rec.gap_end_ms)}")
        summary = (f"{aid}: {span}; catch-up ({rec.reason}): {rec.orders_new} orders new, {rec.orders_updated} "
                   f"updated, {rec.positions_closed} positions closed while offline; {rec.positions_new} positions "
                   f"new, {rec.cash_flows_new} cash flows new; {'complete' if rec.complete else 'INCOMPLETE'}")
        changed = rec.orders_new + rec.orders_updated + rec.positions_new + rec.positions_closed + rec.cash_flows_new
        if downtime or changed or not rec.complete:
            # a reconnect / retry that found nothing new and is complete stays in the log and catchup_runs only
            self._page("info", f"downtime catch-up: {aid}", summary)
        log.warning("downtime catch-up %s", summary)
        if not rec.complete:
            window = "" if result.uncovered is None else (
                f"; uncovered window {iso_utc(result.uncovered[0])} .. {iso_utc(result.uncovered[1])}")
            ids = "" if not result.anomalies else "; anomalies: " + ", ".join(result.anomalies[:20])
            if result.unversioned:
                ids += "; skipped (no version): " + _id_list(result.unversioned)
            self._page("high", f"downtime catch-up incomplete: {aid}",
                       f"{aid} ({rec.reason}): {rec.orders_seen} orders / {rec.positions_seen} positions seen, "
                       f"{rec.anomalies} anomalies{window}{ids}; {'; '.join(result.notes)[:600]}{retry_note}. The "
                       "adapter's fence was held until now; check the broker's order history for that window")


def _span(cur: tuple[int, int] | None, add: tuple[int, int]) -> tuple[int, int]:
    """The smallest window holding both (the uncovered part named in the page)."""
    return add if cur is None else (min(cur[0], add[0]), max(cur[1], add[1]))


def _id_list(keys: Sequence[str], limit: int = 10) -> str:
    """At most `limit` ids, then how many more (ids only, never an amount)."""
    more = len(keys) - limit
    return ", ".join(keys[:limit]) + (f" (+{more} more)" if more > 0 else "")


__all__ = ["CLOCK_BACK_TOLERANCE_MS", "CatchupCoordinator", "CatchupMetrics", "CatchupReason", "CatchupResult",
           "CatchupSettings", "CatchupStore", "DEFAULT_FIRST_RUN_DAYS", "DEFAULT_OVERLAP_S", "DOWNTIME_REASONS",
           "FIRST_RUN_DAYS_VAR", "HOLD_REASON", "MAX_WINDOW_DAYS", "MIN_OVERLAP_S", "OVERLAP_VAR", "ROW_KINDS",
           "duration_text", "iso_utc"]
