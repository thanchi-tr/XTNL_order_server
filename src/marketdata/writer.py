"""`md-writer`: the one writer of `redis-md` (design §A8, §A5, §A7, §A13, §A4 Quality; plan S15).

A supervised, non-critical task (stamp 5 s) added by `optional-bootstrap` (S31) only when `XTNL_MD_ENABLED=1`. It
never touches ingestion or Oversee: it reads the tick hook (`MdHook`, pure and in process) and writes `redis-md`
only, over the one dedicated writer connection (`MdWriteClient`). It holds no broker port and never awaits broker
I/O; BROKER bars reach it from `md-bars` (S17) through `queue_bar`, already validated.

**Every 250 ms** (`cycle`):

1. its own wall-versus-monotonic detector (§A7): a step goes to `MdHook.clock_step`;
2. `MdHook.advance(now)` (finality at `T + p50 + settle`, receipt gaps);
3. every ring is drained: ticks into a bounded per-symbol pending buffer (the newest are kept, the oldest dropped and
   counted, so a Redis outage never grows memory), closed TICK bars validated (§A4 checks 1-4; a failing bar goes to
   `md:quarantine` and its bucket stays `blind`) and moved into the **closed-bar queue**;
4. one `FLUSH` script per symbol (ticks, forming bars, the queued closed bars of every series of that symbol),
   pipelined over the writer connection, then `WAITAOF 1 0 1500` on that connection (`write_and_wait`) **only when
   the flush carries closed bars**; a flush of ticks, forming bars or quarantine entries alone is pipelined without
   `WAITAOF` (§A8: only closed bars wait for the disk).

**One deadline per cycle:** the flush, the memory read and `RETAIN` share one deadline (`cycle_budget_s`, default the
5 s stamp budget less 1 s), each also capped by its own 4 s budget, so one cycle never outlives the supervisor's stamp
(a stale stamp would restart the task and lose the in-memory queue). A flush that hit a store error or the deadline
skips the cycle's retention and memory read. Memory reads and retention are scheduled on the monotonic clock, so a
backward wall step never pauses them.

**Bars the codec cannot hold** (more than 12 decimal places, too many digits) are found when they are queued: such a
closed bar is counted (`unencodable`) and dropped (its bucket stays `blind`), a forming bar that cannot be encoded is
skipped (its stored forming record cleared), and neither ever stops the cycle or holds back the other series and the
ticks of its symbol.

**Closed-bar queue** (TICK and BROKER): per series, keyed by bucket start (a newer revision of a queued bucket
replaces it), bounded at 360 bars = 6 h of 1-minute bars. Tick shedding never touches it. At the bound the oldest
bar is dropped: a TICK bucket stays absent, which reads as `blind` (§A4 "absent means blind"); a BROKER bucket also
gets a re-fetch request to `md-bars`. By then §B14 (S30) has declared the store unrecoverable.

**Release rule** (§A8): bars leave the queue only when their script returned `ScriptOk` **and** the `WAITAOF` reply's
first element is ≥ 1 (or AOF is off, Q17: no `WAITAOF`, release on the script reply). A `WAITAOF` timeout or error
keeps them queued (`aof_wait_timeouts`, exported as `md_aof_wait_timeouts_total` by S32) and the next flush re-writes
them; the bucket replace is idempotent (an identical member is a no-op with no event). Ticks leave the pending buffer
on the script reply (a tick id is not idempotent, so ticks are never re-sent after a reply that may mean they were
written): they go back only after a connection error or timeout, a script refusal (`ScriptRefused` writes nothing) or
an `OOM` (Redis refuses a script's first write only); a `ScriptOk` whose reply cannot be parsed, any other
`ScriptFailed` (unknown, possibly partial) or a missing reply drops them (`ticks_unconfirmed`). Bars stay queued in
every failure case.

**After the release** the released bars that produced a `bar_closed` / `bar_revised` event are handed to the
listeners (`on_released`): the `md-indicators` recompute queue (S18) and the in-process wake-ups (S16).

**Redis errors** never reach ingestion: a connection error or timeout puts the drained ticks back (newest kept), keeps
the queue and marks the involved series GAPPED until a write for them succeeds. `NO_BOOT` (and `NO_WRITER`) stop the
ordinary writes and request a bootstrap (`request_bootstrap`, S16); the writer resumes when `install_boot` hands it the
new sentinel. Any other refusal or error reply keeps everything queued and marks the series GAPPED.

**Retention** (`RETAIN`, every 60 s): ticks `XTRIM MINID` at 2 h from the engine clock plus the 200 000 cap; TICK 1m
bars 7 days, TICK 5m / 15m / 1h 90 days. **BROKER retention** (`RETAIN_BROKER`, the same pass, plan S38e): every
stored BROKER series, demanded or not, keeps 1 year of bars (or its widest declared backfill when longer,
`broker_keep_ms`) and 7 days of superseded members and quarantine entries, so the §A5 sizing is a bound. **Memory shedding** (`INFO memory` every 10 s), each step paged once when
entered, left 5 points below its threshold: 75 % ticks to 30 minutes, 80 % TICK 1m beyond 2 days, 85 % TICK 5m
beyond 30 days, 90 % stop storing undemanded TICK specs (no demand port yet: every TICK spec counts as undemanded).
BROKER series, their indicators and the rollback sets are never shed. A memory refusal (`OOM`) after the last step,
or while the memory level is unknown (no memory port, or its last read failed), is paged once; the page re-arms only
when shedding drops below step 4 or after 240 flushes (60 s) without an `OOM`. An `OOM` below step 4 makes the next
memory read due at once (the §B14 FAILED is S30). Memory at 70 % pages once (cleared below 65 %).

**Retention coverage:** the writer trims the series and tick streams it knows: those written in this term plus those
handed to `track_retention` (S16's bootstrap seeds it from `md:registry:series` / the stored-series index, so a symbol
no longer subscribed is still trimmed).

**S16 additions.** The writer keeps the in-memory copy of each series' rollback set (the last N entries its flushes
added: `write_seq`, `bar_end_ms`, member; bounded at N whatever the consumers do) and the last final bucket it wrote
per series; the bootstrap reconciles both with the store (§A7 step 3). Every series and tick stream it writes is
entered once per sentinel in `md:registry:stored` (`INDEX`, token only, pipelined with the flush), so a later term's
bootstrap finds them without `KEYS` / `SCAN`. `quarantine_stored` takes a stored bar the read port found failing (its
member is removed from `md:bars` by `QUARANTINE`). `use_token` installs the fresh token of an `md_writer_reset`.

**S17 addition.** `set_meta` holds `md:meta` fields of a series (the `md-bars` coverage counts and SM12 mirror) for
its next flush; a flush of meta fields alone carries no closed bar and so never waits for `WAITAOF`.

**S17b addition.** `on_closed_tick` listeners see every closed TICK bar the writer drains from the hook that passes
the hard checks (`check_tick_bar`; with its cross-check flags and median spread, memory only), before shedding: the D6
cross-check's TICK book (`crosscheck.TickRefBook`). A bar the writer quarantines never reaches a listener (zero
trust: the reference that judges a BROKER bar must itself be valid). A listener's fault is counted and never stops
the capture.

**Stop flag:** once the session's stop flag is set (DRAINING), no new write starts; nothing is flushed at the end
(closed bars not yet released die with the term, §A2).

Nothing here logs, pages or formats a price: series ids, bucket starts, counts, check names and percentages only.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Final, Protocol

from src.marketdata.clockstep import SUSPEND, ClockStep, WallMonoDetector
from src.marketdata.codec import encode_member, encode_record
from src.marketdata.errors import safe_redis_error
from src.marketdata.hashing import Keyring, canonical_decimal, content_hash
from src.marketdata.ports import PagePort, StampPort
from src.marketdata.store import keys as K
from src.marketdata.store import scripts as S
from src.marketdata.store.client import WriteResult
from src.marketdata.store.reads import RollbackEntry, removable_member
from src.marketdata.store.runner import ScriptOk, ScriptRefused, ScriptReply, StoreUnavailable
from src.marketdata.tickbars import TickBar
from src.marketdata.ticks import MdHook, RingTick
from src.marketdata.timing import TIMING_TABLE
from src.marketdata.types import M1, M5, M15, Bar, BarEndEvent, SeriesId, Source
from src.marketdata.validate import check_tick_bar

log = logging.getLogger(__name__)

FLUSH_INTERVAL_S: Final = 0.25
STAMP_BUDGET_S: Final = 5.0
"""The supervisor's stamp budget for `md-writer` (design §B5.1); one cycle stays well inside it."""
CYCLE_BUDGET_S: Final = STAMP_BUDGET_S - 1.0
"""One deadline shared by the flush, the memory read and `RETAIN` of one cycle (1 s left for the in-memory work)."""
FLUSH_BUDGET_S: Final = 4.0
"""Hard cap on one flush (scripts plus `WAITAOF`, 2 s timeouts and 1.5 s wait), inside the cycle deadline."""
QUEUE_BARS_PER_SERIES: Final = 360
"""6 h of 1-minute bars per series (design §A8)."""
PENDING_TICKS_PER_SYMBOL: Final = 4_096
MAX_TICKS_PER_CALL: Final = 2_048
MAX_CLOSED_PER_CALL: Final = 128
MAX_QUARANTINE_PENDING: Final = 256
OOM_REARM_FLUSHES: Final = 240
"""Flushes without an `OOM` (60 s at 250 ms) after which the `OOM` page re-arms."""

HOUR_MS: Final = 3_600_000
DAY_MS: Final = 24 * HOUR_MS
TICK_RETENTION_MS: Final = 2 * HOUR_MS
TICK_SHED_RETENTION_MS: Final = 30 * 60_000
TICK_1M_RETENTION_MS: Final = 7 * DAY_MS
TICK_ROLLUP_RETENTION_MS: Final = 90 * DAY_MS
TICK_1M_SHED_RETENTION_MS: Final = 2 * DAY_MS
TICK_5M_SHED_RETENTION_MS: Final = 30 * DAY_MS
RETAIN_EVERY_MS: Final = 60_000
BROKER_RETENTION_MS: Final = 365 * DAY_MS
"""BROKER 15m retention (design §A5, plan S38e): `RETAIN_BROKER` trims every stored BROKER series below it (or below
the widest declared backfill when that is longer)."""
BROKER_RETAIN_PER_CALL: Final = 32
"""BROKER series per `RETAIN_BROKER` call (one bounded script each, pipelined)."""
MEMORY_EVERY_MS: Final = 10_000

MEMORY_PAGE_RATIO: Final = 0.70
SHED_THRESHOLDS: Final = (0.75, 0.80, 0.85, 0.90)
SHED_HYSTERESIS: Final = 0.05
SHED_STEPS: Final = (
    "ticks kept 30 minutes",
    "TICK 1m bars kept 2 days",
    "TICK 5m bars kept 30 days",
    "undemanded TICK specs no longer stored",
)
"""The §A8 shedding order; step k (1-based) is entered at `SHED_THRESHOLDS[k - 1]` of `maxmemory`."""
SHED_TICKS: Final = 1
SHED_TICK_1M: Final = 2
SHED_TICK_5M: Final = 3
SHED_UNDEMANDED: Final = 4

NO_BOOT: Final = "NO_BOOT"
NO_WRITER: Final = "NO_WRITER"

Market = tuple[str, str]
ReleaseListener = Callable[[BarEndEvent], None]


# ---------------------------------------------------------------------------------------------------- ports
class WritePort(Protocol):
    """The writer connection (`MdWriteClient`)."""

    async def run(self, calls: Sequence[S.ScriptCall]) -> list[ScriptReply]: ...

    async def write_and_wait(self, calls: Sequence[S.ScriptCall]) -> WriteResult: ...


class MemoryPort(Protocol):
    async def memory_ratio(self) -> float | None:
        """`used_memory / maxmemory` (None: unbounded or unknown). May raise `StoreUnavailable`."""
        ...


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


def _mono_ms() -> int:
    return time.monotonic_ns() // 1_000_000


# ---------------------------------------------------------------------------------------------------- state
@dataclass
class WriterCounters:
    cycles: int = 0
    flushes: int = 0
    flush_errors: int = 0
    aof_wait_timeouts: int = 0          # md_aof_wait_timeouts_total
    ticks_written: int = 0
    ticks_dropped: int = 0              # pending buffer full (writer-side tick shedding)
    ticks_refused: int = 0              # a tick the script would refuse (dropped alone)
    ticks_unconfirmed: int = 0          # a reply that may mean written (not re-sent: a tick id is not idempotent)
    ring_dropped: int = 0               # the hook's ring overflowed before a drain
    backward_steps: int = 0
    bars_queued: int = 0
    bars_released: int = 0
    bars_requeued: int = 0              # written but not released (AOF not confirmed)
    queue_dropped: int = 0              # the 6 h bound: bucket now blind
    refetch_requests: int = 0
    shed_bars: int = 0                  # undemanded TICK bars not stored at shedding step 4
    quarantined: int = 0
    no_boot: int = 0
    no_writer: int = 0
    script_failures: int = 0
    oom_refusals: int = 0
    build_errors: int = 0
    unencodable: int = 0                # a bar the codec cannot hold: closed dropped (blind), forming skipped
    deadline_skips: int = 0             # retention / memory read left for the next cycle (deadline reached)
    clock_steps: int = 0
    retain_runs: int = 0
    retain_errors: int = 0
    ticks_trimmed: int = 0
    bars_trimmed: int = 0
    broker_retain_runs: int = 0         # RETAIN_BROKER calls (S38e: BROKER retention, design §A5)
    broker_bars_trimmed: int = 0        # BROKER buckets older than their floor (1 year or the widest backfill)
    revisions_trimmed: int = 0          # md:bars_rev / md:quarantine entries older than 7 days (every stored series)
    memory_errors: int = 0
    tick_listener_errors: int = 0       # an `on_closed_tick` listener raised (S17b)
    listener_errors: int = 0
    index_errors: int = 0


@dataclass
class _Queued:
    """One closed bar waiting for its release (the bar's prices stay out of `repr`, `Bar` hides them)."""

    bar: Bar
    content_hash: str
    emitted: bool = False
    revised: bool = False
    write_seq: int = 0
    attempts: int = 0


@dataclass
class _Call:
    """One script of a flush and what to do with its reply."""

    call: S.ScriptCall
    market: Market
    series: tuple[SeriesId, ...] = ()
    ticks: tuple[RingTick, ...] = ()
    closed: tuple[tuple[SeriesId, int, _Queued], ...] = ()
    forming: dict[SeriesId, tuple[int, str] | None] = field(default_factory=dict)
    quarantine: tuple[S.QuarantineEntry, ...] = ()
    index: tuple[str, ...] = ()
    meta: dict[SeriesId, tuple[tuple[str, str], ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class WriterStatus:
    """What `md show` / `arm_status` (S32 / S35) report: counts and states only."""

    boot: str | None
    bootstrap_wanted: str | None
    shed_level: int
    memory_ratio: float | None
    queued_bars: int
    pending_ticks: int
    gapped: tuple[str, ...]
    #: False while the last memory read failed or gave no ratio: `memory_ratio` is then the last good read, stale
    #: (the exporter drops `xtnl_md_redis_used_memory_ratio` rather than repeat it; plan S37 review).
    memory_ok: bool = False


# ---------------------------------------------------------------------------------------------------- writer
class MdWriter:
    """The `md-writer` task body. One instance per term (S31), sharing the term's `MdHook`."""

    def __init__(self, hook: MdHook, write: WritePort, *, token: K.WriterToken, keyring: Keyring,
                 boot: K.BootSentinel | None = None, memory: MemoryPort | None = None, pager: PagePort | None = None,
                 rollback_n: int = TIMING_TABLE[M15].rollback_n_default,
                 request_bootstrap: Callable[[str], None] | None = None,
                 refetch: Callable[[SeriesId, int, int], None] | None = None,
                 is_demanded: Callable[[SeriesId], bool] | None = None,
                 clock_ms: Callable[[], int] = _wall_ms, mono_ms: Callable[[], int] = _mono_ms,
                 interval_s: float = FLUSH_INTERVAL_S, flush_budget_s: float = FLUSH_BUDGET_S,
                 cycle_budget_s: float = CYCLE_BUDGET_S,
                 queue_bars: int = QUEUE_BARS_PER_SERIES, pending_ticks: int = PENDING_TICKS_PER_SYMBOL,
                 max_ticks_per_call: int = MAX_TICKS_PER_CALL, max_closed_per_call: int = MAX_CLOSED_PER_CALL,
                 retain_every_ms: int = RETAIN_EVERY_MS, memory_every_ms: int = MEMORY_EVERY_MS,
                 broker_keep_ms: Callable[[SeriesId], int] | None = None,
                 suspend_after_ms: int | None = None) -> None:
        """`suspend_after_ms`: the clock detector's suspend trigger (round 14; the runtime passes
        `clockstep.SUSPEND_AFTER_MS`); None = steps only (unit rigs that drive cycles with sparse clock jumps)."""
        if queue_bars < 1 or pending_ticks < 1 or max_ticks_per_call < 1 or max_closed_per_call < 1:
            raise ValueError("md-writer bounds must be positive")
        if not 0 < cycle_budget_s < STAMP_BUDGET_S or flush_budget_s <= 0:
            raise ValueError("md-writer budgets must be positive and the cycle inside the stamp budget")
        self._hook = hook
        self._write = write
        self._token = token
        self._keyring = keyring
        self._boot = boot
        self._memory = memory
        self._pager = pager
        self._rollback_n = rollback_n
        self._request_bootstrap = request_bootstrap
        self._refetch = refetch
        self._is_demanded = is_demanded
        self._clock_ms = clock_ms
        self._mono_ms = mono_ms
        self._interval_s = interval_s
        self._budget_s = flush_budget_s
        self._cycle_budget_s = cycle_budget_s
        self._queue_bars = queue_bars
        self._pending_cap = pending_ticks
        self._max_ticks = max_ticks_per_call
        self._max_closed = max_closed_per_call
        self._retain_every = retain_every_ms
        self._memory_every = memory_every_ms
        self._detector = WallMonoDetector(suspend_after_ms=suspend_after_ms)
        self._suspended: tuple[int, int] | None = None
        """A suspend the uptime tracker reported (`note_suspended`), applied at the next cycle (round 14)."""
        self._pending: dict[Market, deque[RingTick]] = {}
        self._queue: dict[SeriesId, dict[int, _Queued]] = {}
        self._quarantine: dict[SeriesId, list[S.QuarantineEntry]] = {}
        self._forming_sig: dict[SeriesId, tuple[int, str]] = {}
        self._tick_series: dict[Market, set[SeriesId]] = {}
        self._broker_series: set[SeriesId] = set()
        self._broker_keep = broker_keep_ms
        self._gapped: set[SeriesId] = set()
        self._dropped_logged: set[SeriesId] = set()
        self._unencodable_logged: set[SeriesId] = set()
        self._bad_forming: dict[SeriesId, int] = {}
        self._listeners: list[ReleaseListener] = []
        self._tick_listeners: list[Callable[[TickBar], None]] = []
        self._rollback: dict[SeriesId, deque[RollbackEntry]] = {}
        self._last_final: dict[SeriesId, int] = {}
        self._indexed: set[str] = set()
        self._meta: dict[SeriesId, dict[str, str]] = {}
        self._last_write_ms = 0
        self._bootstrap_wanted: str | None = None if boot is not None else "start"
        self._store_down = False
        self._shed = 0
        self._ratio: float | None = None
        self._memory_high = False
        self._oom_paged = False
        self._clean_flushes = 0
        self._memory_ok = memory is not None    # False once a read fails or gives no ratio: an OOM then pages
        self._mono_now = 0
        self._now_ms = 0
        self._next_memory = 0                   # monotonic ms
        self._next_retain = 0                   # monotonic ms
        self.counters = WriterCounters()

    # -------------------------------------------------------------------------------------------- control
    @property
    def boot(self) -> K.BootSentinel | None:
        return self._boot

    @property
    def bootstrap_wanted(self) -> str | None:
        """Why the writer waits for a bootstrap (`start`, `no_boot`, `no_writer`), or None while writing."""
        return self._bootstrap_wanted

    def install_boot(self, sentinel: K.BootSentinel) -> None:
        """The bootstrap (S16) wrote `sentinel` into `md:boot`: ordinary writes resume with it."""
        self._boot = sentinel
        self._bootstrap_wanted = None
        self._forming_sig.clear()           # the bootstrap cleared md:forming:*
        self._indexed.clear()               # the stored-series index may be gone (a flush): enter them again

    @property
    def token(self) -> K.WriterToken:
        return self._token

    def use_token(self, token: K.WriterToken) -> None:
        """A fresh writer token (`md_writer_reset`, S16): later scripts carry it. The caller SETs it in the store."""
        self._token = token

    @property
    def rollback_n(self) -> int:
        return self._rollback_n

    @property
    def last_write_ms(self) -> int:
        """The engine time of the newest flush the store accepted (0 before the first): this node's record (§A7)."""
        return self._last_write_ms

    def known_series(self) -> frozenset[SeriesId]:
        """Every series this writer holds anything of: queued, forming, rollback memory, written or tracked."""
        out: set[SeriesId] = set(self._queue) | set(self._forming_sig) | set(self._rollback) | set(self._last_final)
        out.update(s for group in self._tick_series.values() for s in group)
        out.update(self._quarantine)
        return frozenset(out)

    def known_markets(self) -> frozenset[Market]:
        return frozenset(set(self._tick_series) | set(self._pending) | set(self._hook.markets()))

    def rollback_memory(self, series: SeriesId) -> tuple[RollbackEntry, ...]:
        """The in-memory rollback set of `series`, oldest `write_seq` first (at most N entries)."""
        return tuple(self._rollback.get(series, ()))

    def restore_rollback(self, series: SeriesId, entries: Iterable[RollbackEntry]) -> None:
        """Merge hash-checked store entries into the memory copy (bootstrap, store to memory); keeps the newest N."""
        merged = {e.write_seq: e for e in self._rollback.get(series, ())}
        for e in entries:
            merged.setdefault(e.write_seq, e)
        ring: deque[RollbackEntry] = deque(maxlen=self._rollback_n)
        ring.extend(merged[k] for k in sorted(merged))
        self._rollback[series] = ring

    def rehash_rollback(self, series: SeriesId, members: Mapping[str, str]) -> int:
        """`md_rehash` (S16): memory entries holding an old bar member (a key in `members`) carry the new one, with
        the same `write_seq` and bar end, as the silent `FLUSH` did in the store. Returns the count rewritten."""
        ring = self._rollback.get(series)
        if not ring or not members:
            return 0
        done = 0
        out: deque[RollbackEntry] = deque(maxlen=self._rollback_n)
        for e in ring:
            new = members.get(e.member)
            if new is not None:
                e = RollbackEntry(e.write_seq, e.bar_end_ms, new)
                done += 1
            out.append(e)
        self._rollback[series] = out
        return done

    def note_lost_boot(self, code: str) -> None:
        """A script sent outside the writer's own flush (`md_rehash`) was refused `NO_BOOT` / `NO_WRITER`: the
        writer stops and asks for a bootstrap, as if its own flush had been refused (an idle writer would not)."""
        if code in (NO_BOOT, NO_WRITER):
            self._lost_boot(code)

    def last_final_bucket(self, series: SeriesId) -> int | None:
        """The newest closed bucket start this writer wrote for `series` (None: none in this process)."""
        return self._last_final.get(series)

    def is_queued(self, series: SeriesId, bucket_start_ms: int) -> bool:
        return bucket_start_ms in self._queue.get(series, {})

    def quarantine_stored(self, bar: Bar, check: str, member: str = "") -> bool:
        """A stored bar the read port found failing (§A10): quarantine it and remove `member` from `md:bars` on the
        next flush. False when it cannot be held (bound reached, already held, or a record the codec cannot hold)."""
        remove = member if removable_member(member) else ""
        try:
            encode_record(bar)
            entry = S.QuarantineEntry(bar, check, remove)
            S.quarantine_call(self._token, "-", bar.series, 1, [entry])     # the builder's own checks, now
        except ValueError:
            return False
        held = self._quarantine.setdefault(bar.series, [])
        if len(held) >= MAX_QUARANTINE_PENDING or (remove and any(h.remove_member == remove for h in held)):
            return False
        held.append(entry)
        return True

    def track_retention(self, *, series: Iterable[SeriesId] = (), markets: Iterable[Market] = ()) -> None:
        """Series and tick streams stored by earlier terms (S16 bootstrap: `md:registry:series` / the stored-series
        index), so `RETAIN` also trims a symbol this term never writes, and (S38e) `RETAIN_BROKER` every stored BROKER
        series, demanded or not."""
        for market in markets:
            self._tick_series.setdefault(market, set())
        for s in series:
            if s.source is Source.BROKER:
                self._broker_series.add(s)
                continue
            group = self._tick_series.setdefault((s.adapter_id, s.symbol), set())
            group.add(s)

    def broker_series(self) -> frozenset[SeriesId]:
        """Every BROKER series `RETAIN_BROKER` trims: those handed to `track_retention` plus those this writer holds
        anything of (S38e)."""
        return frozenset(self._broker_series | {s for s in self.known_series() if s.source is Source.BROKER})

    def stored_broker_markets(self) -> frozenset[Market]:
        """`(adapter, symbol)` of every stored BROKER series (the demand book's memory ceiling, S38e)."""
        return frozenset((s.adapter_id, s.symbol) for s in self.broker_series())

    @property
    def memory_ratio(self) -> float | None:
        """The last `used_memory / maxmemory` read; None while unknown (no port, or the last read failed)."""
        return self._ratio if self._memory_ok else None

    def broker_floor(self, series: SeriesId, now: int) -> int:
        """The bucket a BROKER series is kept from: 1 year back, or the widest declared backfill when longer."""
        keep = BROKER_RETENTION_MS
        if self._broker_keep is not None:
            try:
                keep = max(keep, int(self._broker_keep(series)))
            except Exception as e:      # a demand-source fault keeps the 1-year floor
                log.warning("md-writer: backfill span of %s unknown (%s)", series.canonical, safe_redis_error(e))
        return max(series.spec.bucket_start_ms(now - keep), 0)

    def on_released(self, listener: ReleaseListener) -> None:
        """Called with each released bar that produced a `bar_closed` / `bar_revised` event (indicator jobs)."""
        self._listeners.append(listener)

    def on_closed_tick(self, listener: Callable[[TickBar], None]) -> None:
        """Called with every closed TICK bar drained from the hook that passes `check_tick_bar`, before shedding (the
        S17b cross-check book); a quarantined bar is never handed to a listener."""
        self._tick_listeners.append(listener)

    @property
    def shed_level(self) -> int:
        return self._shed

    @property
    def gapped(self) -> frozenset[SeriesId]:
        return frozenset(self._gapped)

    def queue_depth(self, series: SeriesId | None = None) -> int:
        if series is not None:
            return len(self._queue.get(series, {}))
        return sum(len(q) for q in self._queue.values())

    def pending_ticks(self, market: Market | None = None) -> int:
        if market is not None:
            return len(self._pending.get(market, ()))
        return sum(len(d) for d in self._pending.values())

    def status(self) -> WriterStatus:
        return WriterStatus(None if self._boot is None else str(self._boot.generation), self._bootstrap_wanted,
                            self._shed, self._ratio, self.queue_depth(), self.pending_ticks(),
                            tuple(sorted(s.canonical for s in self._gapped)), self._memory_ok)

    # -------------------------------------------------------------------------------------------- input
    def queue_bar(self, bar: Bar) -> None:
        """A validated, final closed bar (BROKER from `md-bars`, S17) for the closed-bar queue. Never awaits."""
        self._enqueue(bar)

    def set_meta(self, series: SeriesId, pairs: Sequence[tuple[str, str]]) -> bool:
        """`md:meta` fields of `series` for its next flush (S17: the `md-bars` coverage counts and SM12 mirror; ids,
        states and counts only, never a price). A newer value of a field replaces a pending one; a field is dropped
        from the pending set once a flush carrying it is accepted. False (nothing held) when the `FLUSH` builder
        refuses a field (a name that is not a word, a field the script writes itself, a bad numeric field), so a bad
        field can never stop the flush of its symbol."""
        if not pairs:
            return True
        try:
            S.flush_call(self._token, "-", series.adapter_id, series.symbol, 1, (),
                         [S.SeriesFlush(series, meta=tuple(pairs))], rollback_n=self._rollback_n)
        except ValueError:
            return False
        self._meta.setdefault(series, {}).update(pairs)
        return True

    # -------------------------------------------------------------------------------------------- task body
    async def run(self, stop: asyncio.Event, stamp: StampPort) -> None:
        """The supervised loop: a cycle every 250 ms until the stop flag is set."""
        while not stop.is_set():
            stamp.stamp()
            await self.cycle(stop)
            stamp.stamp()
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval_s)
            except TimeoutError:
                pass

    def note_suspended(self, gap_ms: int) -> None:
        """The process was suspended for about `gap_ms` (the uptime tracker's signal through
        `EngineSession.add_suspend_listener`, round 14): the next cycle blinds `[now - gap, now]` unless its own
        detector already reported the suspend. Synchronous, no I/O."""
        now = self._clock_ms()
        self._suspended = (now - max(gap_ms, 0), now)

    async def cycle(self, stop: asyncio.Event | None = None) -> None:
        """One writer pass: clock-step check, finality, drain, flush, then retention and shedding when due. The awaits
        share one deadline (`cycle_budget_s`), so a hung connection never outlives the stamp budget."""
        self.counters.cycles += 1
        deadline = time.monotonic() + self._cycle_budget_s
        now = self._now_ms = self._clock_ms()
        mono = self._mono_now = self._mono_ms()
        step = self._detector.observe(now, mono)
        if step is not None:
            self.counters.clock_steps += 1
            self._hook.clock_step(step)
        suspended, self._suspended = self._suspended, None
        if suspended is not None and step is None:
            # round 14: the uptime tracker saw a suspend this writer's own detector did not (a sleep shorter than
            # its threshold, or one that ended between two cycles): blind the TICK buckets it covered
            self.counters.clock_steps += 1
            frm, to = suspended
            self._hook.clock_step(ClockStep(frm, to, 0, frm, max(to - frm, 0), SUSPEND))
        self._hook.advance(now)
        self._capture(now)
        if stop is not None and stop.is_set():
            return
        if not await self._flush(now, deadline):
            return                          # a store error or the deadline: retention and memory wait a cycle
        if stop is not None and stop.is_set():
            return
        await self._maintain(now, mono, deadline)

    @staticmethod
    def _cap(deadline: float, budget_s: float) -> float:
        """Seconds left for one await: `budget_s`, cut to what is left of the cycle deadline (`time.monotonic`)."""
        return max(min(budget_s, deadline - time.monotonic()), 0.0)

    # -------------------------------------------------------------------------------------------- capture
    def _capture(self, now: int) -> None:
        for adapter_id, symbol in self._hook.markets():
            drained = self._hook.drain(adapter_id, symbol)
            self.counters.ring_dropped += drained.ring_dropped
            if drained.ticks:
                buf = self._buffer((adapter_id, symbol))
                for tick in drained.ticks:
                    if len(buf) == self._pending_cap:
                        self.counters.ticks_dropped += 1      # deque(maxlen) drops the oldest
                    buf.append(tick)
            for tb in drained.closed:
                bar = tb.bar
                result = check_tick_bar(bar, built_bucket_start_ms=bar.bucket_start_ms, built_side=bar.series.side,
                                        received_ms=now)
                if result.ok:                            # zero trust (D6): only a bar that passed is a reference
                    for listener in self._tick_listeners:
                        try:
                            listener(tb)
                        except Exception:                # a listener's fault never stops the capture
                            self.counters.tick_listener_errors += 1
                if self._undemanded(bar.series):         # after the tap: an undemanded TICK bar still feeds D6
                    self.counters.shed_bars += 1
                    continue
                if result.ok:
                    self._enqueue(bar)
                    continue
                try:
                    encode_record(bar)              # QUARANTINE stores the record: one it cannot hold is dropped
                except ValueError as e:
                    self._unencodable(bar.series, "quarantined", e)
                    continue
                held = self._quarantine.setdefault(bar.series, [])
                if len(held) < MAX_QUARANTINE_PENDING:
                    held.append(S.QuarantineEntry(bar, result.failed[0].value))

    def _buffer(self, market: Market) -> deque[RingTick]:
        buf = self._pending.get(market)
        if buf is None:
            buf = deque(maxlen=self._pending_cap)
            self._pending[market] = buf
        return buf

    def _undemanded(self, series: SeriesId) -> bool:
        """At shedding step 4 a TICK spec nobody demands is not stored (without a demand port: every TICK spec)."""
        if self._shed < SHED_UNDEMANDED or series.source is not Source.TICK:
            return False
        return self._is_demanded is None or not self._is_demanded(series)

    def _enqueue(self, bar: Bar) -> None:
        series = bar.series
        try:
            digest = content_hash(self._keyring, series.canonical, bar.bucket_end_ms, bar.open, bar.high, bar.low,
                                  bar.close)
            encode_member(bar, digest)          # found here, not on every flush: the bar never blocks its symbol
        except ValueError as e:                 # CodecError included
            self._unencodable(series, "closed", e)
            return
        q = self._queue.setdefault(series, {})
        if bar.bucket_start_ms not in q and len(q) >= self._queue_bars:
            self._drop_oldest(series, q)
        q[bar.bucket_start_ms] = _Queued(bar, digest)
        self.counters.bars_queued += 1

    def _unencodable(self, series: SeriesId, what: str, e: Exception) -> None:
        """A bar the codec (or the content hash) cannot hold: counted, logged once per series, never stored."""
        self.counters.unencodable += 1
        if series not in self._unencodable_logged:
            self._unencodable_logged.add(series)
            log.warning("md-writer: a %s bar of %s cannot be encoded (%s); not stored", what, series.canonical,
                        safe_redis_error(e))

    def _drop_oldest(self, series: SeriesId, q: dict[int, _Queued]) -> None:
        start = min(q)
        del q[start]
        self.counters.queue_dropped += 1
        if series not in self._dropped_logged:
            self._dropped_logged.add(series)
            log.warning("md-writer: closed-bar queue full for %s; oldest bucket %d dropped (blind)",
                        series.canonical, start)
        if series.source is Source.BROKER and self._refetch is not None:
            self.counters.refetch_requests += 1
            try:
                self._refetch(series, start, start)
            except Exception as e:   # a consumer's fault never stops the writer
                log.warning("md-writer: re-fetch request for %s failed (%s)", series.canonical, safe_redis_error(e))

    # -------------------------------------------------------------------------------------------- flush
    def _markets(self) -> list[Market]:
        markets = {m for m, buf in self._pending.items() if buf}
        markets.update((s.adapter_id, s.symbol) for s, q in self._queue.items() if q)
        markets.update((s.adapter_id, s.symbol) for s, held in self._quarantine.items() if held)
        markets.update(self._hook.markets())
        markets.update((s.adapter_id, s.symbol) for s in self._forming_sig)
        markets.update((s.adapter_id, s.symbol) for s, m in self._meta.items() if m)
        return sorted(markets)

    def _build(self, boot: K.BootSentinel, now: int) -> list[_Call]:
        out: list[_Call] = []
        for market in self._markets():
            taken: list[RingTick] = []
            try:
                call = self._build_flush(boot, market, now, taken)
            except ValueError as e:     # the token, sentinel or bounds: nothing of this symbol is lost
                self.counters.build_errors += 1
                self._requeue_ticks(market, taken)
                log.warning("md-writer: flush for %s:%s not built (%s)", market[0], market[1], safe_redis_error(e))
            else:
                if call is not None:
                    out.append(call)
            out += self._build_quarantine(boot, market, now)
        return out

    def _build_flush(self, boot: K.BootSentinel, market: Market, now: int, taken: list[RingTick]) -> _Call | None:
        """The `FLUSH` of one symbol; the ticks it pops are appended to `taken` (so the caller can put them back)."""
        adapter_id, symbol = market
        buf = self._pending.get(market)
        writes: list[S.TickWrite] = []
        while buf and len(taken) < self._max_ticks:
            tick = buf.popleft()
            try:
                writes.append(S.TickWrite(tick.received_ms, canonical_decimal(tick.bid),
                                          canonical_decimal(tick.ask), tick.broker_ms, tick.ts_source,
                                          tick.status))
            except ValueError:
                self.counters.ticks_refused += 1
                continue
            taken.append(tick)
        forming_now = {b.series: b for b in self._hook.forming(adapter_id, symbol)
                       if not self._undemanded(b.series)}
        names = set(forming_now)
        names.update(s for s in self._queue if (s.adapter_id, s.symbol) == market and self._queue[s])
        names.update(s for s in self._forming_sig if (s.adapter_id, s.symbol) == market)
        names.update(s for s, m in self._meta.items() if m and (s.adapter_id, s.symbol) == market)
        flushes: list[S.SeriesFlush] = []
        closed: list[tuple[SeriesId, int, _Queued]] = []
        forming: dict[SeriesId, tuple[int, str] | None] = {}
        metas: dict[SeriesId, tuple[tuple[str, str], ...]] = {}
        budget = self._max_closed
        for series in sorted(names):
            fw = self._forming_change(series, forming_now.get(series), forming)
            picked = self._pick_closed(series, budget)
            budget -= len(picked)
            meta = tuple(sorted(self._meta.get(series, {}).items()))
            if fw == S.KEEP_FORMING and not picked and not meta:
                continue
            closed += picked
            if meta:
                metas[series] = meta
            flushes.append(S.SeriesFlush(series, forming=fw,
                                         closed=tuple(S.ClosedBarWrite(e.bar, e.content_hash) for _, _, e in picked),
                                         meta=meta))
        if not writes and not flushes:
            return None
        call = S.flush_call(self._token, boot, adapter_id, symbol, now, writes, flushes, rollback_n=self._rollback_n)
        return _Call(call, market, tuple(f.series for f in flushes), tuple(taken), tuple(closed), forming,
                     meta=metas)

    def _forming_change(self, series: SeriesId, bar: Bar | None,
                        forming: dict[SeriesId, tuple[int, str] | None]) -> S.FormingWrite | str:
        """What this flush does with the forming record of `series` (written only on change, cleared once final). A
        forming bar the codec cannot hold is skipped, and a stored older record of the series is cleared."""
        if bar is not None:
            try:
                w = S.forming_write(bar)
            except ValueError as e:
                if self._bad_forming.get(series) != bar.bucket_start_ms:
                    self._bad_forming[series] = bar.bucket_start_ms     # counted once per bucket
                    self._unencodable(series, "forming", e)
            else:
                self._bad_forming.pop(series, None)
                if self._forming_sig.get(series) == (w.bucket_start_ms, w.record):
                    return S.KEEP_FORMING
                forming[series] = (w.bucket_start_ms, w.record)
                return w
        if series in self._forming_sig:
            forming[series] = None
            return S.CLEAR_FORMING
        return S.KEEP_FORMING

    def _pick_closed(self, series: SeriesId, budget: int) -> list[tuple[SeriesId, int, _Queued]]:
        """Up to `budget` queued closed bars of `series`, oldest first. A bar that does not encode (checked when it
        was queued, so this is a safety net) leaves the queue alone instead of failing the whole symbol."""
        q = self._queue.get(series)
        if not q:
            return []
        picked: list[tuple[SeriesId, int, _Queued]] = []
        for start in sorted(q):
            if len(picked) >= budget:
                break
            entry = q[start]
            try:
                encode_member(entry.bar, entry.content_hash)
            except ValueError as e:
                del q[start]
                self._unencodable(series, "closed", e)
                continue
            picked.append((series, start, entry))
        if not q:
            del self._queue[series]
        return picked

    def _build_quarantine(self, boot: K.BootSentinel, market: Market, now: int) -> list[_Call]:
        out: list[_Call] = []
        for series in sorted(s for s in self._quarantine if (s.adapter_id, s.symbol) == market):
            entries = tuple(self._quarantine.pop(series))
            if not entries:
                continue
            try:
                call = S.quarantine_call(self._token, boot, series, now, entries)
            except ValueError as e:     # checked when held, so a safety net: these entries are dropped
                self.counters.build_errors += 1
                log.warning("md-writer: quarantine for %s not built (%s); %d entries dropped", series.canonical,
                            safe_redis_error(e), len(entries))
                continue
            out.append(_Call(call, market, (series,), quarantine=entries))
        return out

    def _index_call(self, calls: Sequence[_Call]) -> _Call | None:
        """`INDEX` for the series and tick streams of this flush not yet entered under the current sentinel."""
        fields: set[str] = set()
        for c in calls:
            if c.ticks:
                fields.add(K.market_field(*c.market))
            fields.update(s.canonical for s in c.series)
        new = sorted(fields - self._indexed)
        if not new:
            return None
        try:
            call = S.index_call(self._token, new)
        except ValueError:
            self.counters.index_errors += 1
            return None
        return _Call(call, ("", ""), index=tuple(new))

    async def _flush(self, now: int, deadline: float) -> bool:
        """One pipelined flush; False after a store error or the deadline (the cycle then skips its maintenance).
        `WAITAOF` is sent only when the flush carries closed bars (design §A8)."""
        boot = self._boot
        if boot is None:
            return True
        calls = self._build(boot, now)
        if not calls:
            return True
        index = self._index_call(calls)
        if index is not None:
            calls.insert(0, index)
        self.counters.flushes += 1
        scripts = [c.call for c in calls]
        try:
            async with asyncio.timeout(self._cap(deadline, self._budget_s)):
                if any(c.closed for c in calls):
                    result = await self._write.write_and_wait(scripts)
                else:                   # nothing waits for the disk: no WAITAOF
                    result = WriteResult(tuple(await self._write.run(scripts)), None)
        except StoreUnavailable as e:
            self._store_failed(calls, e.kind)
            return False
        except TimeoutError:
            self._store_failed(calls, "timeout")
            return False
        self._apply(calls, result)
        return True

    def _apply(self, calls: Sequence[_Call], result: WriteResult) -> None:
        released = result.aof_confirmed
        if not released:                # only a flush with closed bars sends WAITAOF
            self.counters.aof_wait_timeouts += 1
            log.info("md-writer: WAITAOF not confirmed (%s); closed bars stay queued",
                     result.aof_error or "timeout")
        replies: list[ScriptReply | None] = list(result.replies)
        replies += [None] * (len(calls) - len(replies))
        ok_any = False
        oom_seen = False
        for c, reply in zip(calls, replies):
            if c.index:
                if isinstance(reply, ScriptOk):
                    self._indexed.update(c.index)
                else:
                    self.counters.index_errors += 1
                    if isinstance(reply, ScriptRefused) and reply.code in (NO_BOOT, NO_WRITER):
                        self._lost_boot(reply.code)
                continue
            if isinstance(reply, ScriptOk):
                if self._applied(c, reply, released):
                    ok_any = True
                    continue
                self._failed(c, "reply", requeue_ticks=False)      # the script ran: its ticks are written
            elif isinstance(reply, ScriptRefused):
                if reply.code in (NO_BOOT, NO_WRITER):
                    self._lost_boot(reply.code)
                self._failed(c, reply.code, requeue_ticks=True)    # a refusal writes nothing
            elif reply is None:
                self._failed(c, "no_reply", requeue_ticks=False)   # unknown: never risk a duplicate tick
            else:
                oom = "(OOM)" in reply.detail                       # refused at the script's first write
                if oom:
                    oom_seen = True
                    self._oom()
                self._failed(c, reply.detail, requeue_ticks=oom)   # otherwise unknown, possibly partial
        if oom_seen:
            self._clean_flushes = 0
        else:
            self._clean_flushes += 1
            if self._clean_flushes >= OOM_REARM_FLUSHES:
                self._oom_paged = False
        if ok_any:
            self._last_write_ms = max(self._last_write_ms, self._now_ms)
            if self._store_down:
                log.info("md-writer: redis-md writes succeed again")
            self._store_down = False

    def _applied(self, c: _Call, reply: ScriptOk, released: bool) -> bool:
        """A `ScriptOk` reply; False when its value is unusable (handled as a failure)."""
        if c.quarantine:
            self.counters.quarantined += len(c.quarantine)
            self._gapped.difference_update(c.series)
            return True
        try:
            parsed = S.parse_flush_reply(reply.value)
        except (ValueError, TypeError):
            return False
        if len(parsed.bars) != len(c.closed):
            return False
        self.counters.ticks_written += parsed.ticks_added
        self.counters.backward_steps += parsed.backward_steps
        self._gapped.difference_update(c.series)
        tick_series = self._tick_series.setdefault(c.market, set())
        tick_series.update(s for s in c.series if s.source is Source.TICK)
        for series, sig in c.forming.items():
            if sig is None:
                self._forming_sig.pop(series, None)
            else:
                self._forming_sig[series] = sig
        for series, sent in c.meta.items():
            held = self._meta.get(series)
            if held is None:
                continue
            for name, value in sent:
                if held.get(name) == value:     # a newer value set meanwhile stays pending
                    del held[name]
            if not held:
                del self._meta[series]
        for (series, start, entry), (outcome, write_seq) in zip(c.closed, parsed.bars):
            entry.attempts += 1
            last = self._last_final.get(series)
            if last is None or start > last:
                self._last_final[series] = start
            if outcome in (S.CLOSED, S.REVISED):
                entry.emitted = True
                entry.revised = outcome == S.REVISED
                entry.write_seq = write_seq
                ring = self._rollback.get(series)
                if ring is None:
                    ring = self._rollback[series] = deque(maxlen=self._rollback_n)
                ring.append(RollbackEntry(write_seq, entry.bar.bucket_end_ms,
                                          encode_member(entry.bar, entry.content_hash)))
            if released:
                self._release(series, start, entry)
            else:
                self.counters.bars_requeued += 1
        return True

    def _release(self, series: SeriesId, start: int, entry: _Queued) -> None:
        q = self._queue.get(series)
        if q is not None and q.get(start) is entry:   # a newer revision queued meanwhile stays queued
            del q[start]
            if not q:
                del self._queue[series]
        self.counters.bars_released += 1
        if not entry.emitted:
            return
        event = BarEndEvent(series, entry.bar.bucket_end_ms, entry.content_hash, entry.write_seq,
                            entry.bar.revision, entry.revised)
        for listener in self._listeners:
            try:
                listener(event)
            except Exception as e:   # a consumer's fault never stops the writer
                self.counters.listener_errors += 1
                log.warning("md-writer: release listener failed for %s (%s)", series.canonical, safe_redis_error(e))

    def _failed(self, c: _Call, why: str, *, requeue_ticks: bool) -> None:
        """The call did not succeed: its bars stay queued (their re-write is idempotent), its quarantine entries are
        kept, its series are GAPPED. Its ticks go back (newest kept) only when nothing was written (`requeue_ticks`);
        after a reply that may mean they were written they are dropped and counted, since a re-sent tick gets a new
        id and would be stored twice."""
        if c.ticks:
            if requeue_ticks:
                self._requeue_ticks(c.market, c.ticks)
            else:
                self.counters.ticks_unconfirmed += len(c.ticks)
        if c.quarantine:
            held = self._quarantine.setdefault(c.series[0], [])
            room = MAX_QUARANTINE_PENDING - len(held)
            held[:0] = list(c.quarantine[:max(room, 0)])
        if why not in (NO_BOOT, NO_WRITER):
            self.counters.script_failures += 1
            log.warning("md-writer: write for %s:%s refused (%s)", c.market[0], c.market[1], why)
        self._gapped.update(c.series)
        self._gapped.update(s for s in self._queue if (s.adapter_id, s.symbol) == c.market)

    def _requeue_ticks(self, market: Market, ticks: Sequence[RingTick]) -> None:
        """Put taken ticks back in front of the newer ones; when the buffer is full the oldest are dropped."""
        buf = self._buffer(market)
        room = self._pending_cap - len(buf)
        keep = list(ticks[len(ticks) - room:]) if room < len(ticks) else list(ticks)
        self.counters.ticks_dropped += len(ticks) - len(keep)
        buf.extendleft(reversed(keep))

    def _store_failed(self, calls: Sequence[_Call], kind: str) -> None:
        self.counters.flush_errors += 1
        if not self._store_down:
            self._store_down = True
            log.warning("md-writer: redis-md unavailable (%s); ticks and closed bars held, series GAPPED", kind)
        for c in calls:
            if c.ticks:
                self._requeue_ticks(c.market, c.ticks)
            if c.quarantine:
                self._quarantine.setdefault(c.series[0], [])[:0] = list(c.quarantine)
            self._gapped.update(c.series)
            self._gapped.update(s for s in self._queue if (s.adapter_id, s.symbol) == c.market)

    def _lost_boot(self, code: str) -> None:
        """`NO_BOOT` / `NO_WRITER`: stop the ordinary writes and ask for a bootstrap (S16) once."""
        if code == NO_BOOT:
            self.counters.no_boot += 1
        else:
            self.counters.no_writer += 1
        if self._boot is None:
            return
        self._boot = None
        self._bootstrap_wanted = code.lower()
        log.warning("md-writer: %s; ordinary writes paused until a bootstrap", code)
        if self._request_bootstrap is not None:
            try:
                self._request_bootstrap(code.lower())
            except Exception as e:
                log.warning("md-writer: bootstrap request failed (%s)", safe_redis_error(e))

    def _oom(self) -> None:
        """An `OOM` refusal: below step 4 the memory read (and shedding) runs in this cycle; after the last step, or
        while the memory level is unknown, it pages once (re-armed by `_apply` / `_check_memory`)."""
        self.counters.oom_refusals += 1
        last_step = self._shed >= SHED_UNDEMANDED
        if not last_step:
            self._next_memory = min(self._next_memory, self._mono_now)
        if (last_step or not self._memory_ok) and not self._oom_paged:
            self._oom_paged = True
            why = "after the last shedding step" if last_step else "while its memory level is unknown"
            self._page("high", "redis-md refuses writes for memory", f"redis-md refused a write for memory {why}")

    # -------------------------------------------------------------------------------------------- retention, shedding
    async def _maintain(self, now: int, mono: int, deadline: float) -> None:
        """The memory read and `RETAIN` when due (monotonic schedule), each inside what is left of the cycle deadline;
        one that finds the deadline reached stays due for the next cycle."""
        if self._memory is not None and mono >= self._next_memory:
            if time.monotonic() >= deadline:
                self.counters.deadline_skips += 1
            else:
                self._next_memory = mono + self._memory_every
                await self._check_memory(mono, deadline)
        if mono >= self._next_retain and self._boot is not None:
            if time.monotonic() >= deadline:
                self.counters.deadline_skips += 1
            else:
                self._next_retain = mono + self._retain_every
                await self._retain(now, deadline)

    async def _check_memory(self, mono: int, deadline: float) -> None:
        if self._memory is None:
            return
        try:
            async with asyncio.timeout(self._cap(deadline, self._budget_s)):
                ratio = await self._memory.memory_ratio()
        except (StoreUnavailable, TimeoutError):
            self.counters.memory_errors += 1
            self._memory_ok = False
            return
        self._ratio = ratio
        self._memory_ok = ratio is not None
        if ratio is None:
            return
        pct = round(ratio * 100)
        if ratio >= MEMORY_PAGE_RATIO and not self._memory_high:
            self._memory_high = True
            self._page("high", "redis-md memory high", f"redis-md memory at {pct} % of maxmemory")
        elif ratio < MEMORY_PAGE_RATIO - SHED_HYSTERESIS:
            self._memory_high = False
        level = self._shed
        while level < len(SHED_THRESHOLDS) and ratio >= SHED_THRESHOLDS[level]:
            level += 1
        while level > 0 and ratio < SHED_THRESHOLDS[level - 1] - SHED_HYSTERESIS:
            level -= 1
        old, self._shed = self._shed, level
        for k in range(old + 1, level + 1):
            self._page("high", f"redis-md memory shedding step {k}",
                       f"redis-md memory at {pct} % of maxmemory; step {k}: {SHED_STEPS[k - 1]}")
        if level > old:
            self._next_retain = mono                   # trim at once
        elif level < old:
            log.info("md-writer: memory shedding back to step %d (%d %%)", level, pct)
            if level < SHED_UNDEMANDED <= old:
                self._oom_paged = False                # a later OOM after the last step pages again

    def retention_floors(self, now: int) -> tuple[int, dict[SeriesId, int]]:
        """The tick `MINID` and the per-TICK-series bucket floor for the current shedding level."""
        tick_keep = TICK_SHED_RETENTION_MS if self._shed >= SHED_TICKS else TICK_RETENTION_MS
        floors: dict[SeriesId, int] = {}
        for series in (s for group in self._tick_series.values() for s in group):
            if series.spec == M1:
                keep = TICK_1M_SHED_RETENTION_MS if self._shed >= SHED_TICK_1M else TICK_1M_RETENTION_MS
            elif series.spec == M5:
                keep = TICK_5M_SHED_RETENTION_MS if self._shed >= SHED_TICK_5M else TICK_ROLLUP_RETENTION_MS
            else:
                keep = TICK_ROLLUP_RETENTION_MS
            floors[series] = max(series.spec.bucket_start_ms(now - keep), 0)
        return max(now - tick_keep, 0), floors

    async def _retain(self, now: int, deadline: float) -> None:
        await self._retain_ticks(now, deadline)
        await self._retain_broker(now, deadline)

    async def _retain_ticks(self, now: int, deadline: float) -> None:
        boot = self._boot
        if boot is None:
            return
        minid, floors = self.retention_floors(now)
        markets = sorted(set(self._tick_series) | set(self._pending))
        calls: list[S.ScriptCall] = []
        for adapter_id, symbol in markets:
            mine = sorted((s, f) for s, f in floors.items() if (s.adapter_id, s.symbol) == (adapter_id, symbol))
            calls.append(S.retain_call(self._token, boot, adapter_id, symbol, tick_minid_ms=minid, floors=mine))
        if not calls:
            return
        self.counters.retain_runs += 1
        try:
            async with asyncio.timeout(self._cap(deadline, self._budget_s)):
                replies = await self._write.run(calls)
        except (StoreUnavailable, TimeoutError):
            self.counters.retain_errors += 1
            return
        for reply in replies:
            if isinstance(reply, ScriptOk):
                try:
                    parsed = S.parse_retain_reply(reply.value)
                except (ValueError, TypeError):
                    self.counters.retain_errors += 1
                    continue
                self.counters.ticks_trimmed += parsed.ticks_trimmed
                self.counters.bars_trimmed += sum(parsed.bars_removed)
            else:
                self.counters.retain_errors += 1
                if isinstance(reply, ScriptRefused) and reply.code in (NO_BOOT, NO_WRITER):
                    self._lost_boot(reply.code)

    async def _retain_broker(self, now: int, deadline: float) -> None:
        """`RETAIN_BROKER` (S38e, design §A5): every stored BROKER series, demanded or not, keeps 1 year (or its
        widest declared backfill) of bars and 7 days of superseded members and quarantine entries. This is retention,
        not shedding: the shedding steps never touch BROKER series."""
        boot = self._boot
        if boot is None or time.monotonic() >= deadline:
            return
        floors = sorted((s, self.broker_floor(s, now)) for s in self.broker_series())
        if not floors:
            return
        calls = [S.retain_broker_call(self._token, boot, now, floors[i:i + BROKER_RETAIN_PER_CALL])
                 for i in range(0, len(floors), BROKER_RETAIN_PER_CALL)]
        self.counters.broker_retain_runs += 1
        try:
            async with asyncio.timeout(self._cap(deadline, self._budget_s)):
                replies = await self._write.run(calls)
        except (StoreUnavailable, TimeoutError):
            self.counters.retain_errors += 1
            return
        for reply in replies:
            if isinstance(reply, ScriptOk):
                try:
                    parsed = S.parse_retain_broker_reply(reply.value)
                except (ValueError, TypeError):
                    self.counters.retain_errors += 1
                    continue
                for bars_removed, rev, quarantine in parsed.removed:
                    self.counters.broker_bars_trimmed += bars_removed
                    self.counters.revisions_trimmed += rev + quarantine
            else:
                self.counters.retain_errors += 1
                if isinstance(reply, ScriptRefused) and reply.code in (NO_BOOT, NO_WRITER):
                    self._lost_boot(reply.code)

    def _page(self, severity: str, title: str, body: str) -> None:
        log.warning("md-writer: %s: %s", title, body)
        if self._pager is None:
            return
        try:
            self._pager.page(severity, title, body)
        except Exception as e:
            log.warning("md-writer: page failed (%s)", safe_redis_error(e))

