"""`md-bars`: BROKER bars, finality, the error unit, the backfill lane and the broker-outage pause (design §A4, §A8,
§A7 SM12, §B5.2, §B14; plan S17a, R11-M03, R11-M08, R11-M10, R11-M15, R11-m).

A supervised, non-critical task (stamp 30 s) added by `optional-bootstrap` (S31) only when `XTNL_MD_ENABLED=1`. It
reads broker bars through `BrokerBarsPort` (REST priority `BARS`, a port-level value on every failure, never a broker
exception), validates them (§A4, `validate.split_response`) and hands final bars to `md-writer` (`BarSink.queue_bar`).
It never writes `redis-md` itself, never touches ingestion or Oversee, and holds no PostgreSQL port.

**Live path, per demanded BROKER series** (the one schedule of §B5.2, on the adapter's skew, §A9):

* the first read of bar `T` (bucket end, broker time) is at `T + p50 + settle` (`timing.first_read_ms`, the one rule);
  `XTNL_MD_BROKER_BAR_LAG_MS` only starts the re-read back-off (`timing.next_read_ms`), R11-m;
* while the bar is not final it is re-read every 5 s until `T + post_grace`, every 30 s until `T + 5 min`, then every
  60 s until `decision_cap` (one range read per series covers every due bucket and reaches the bucket in progress, so
  a later bucket in the same answer is finality evidence);
* **finality on broker-side evidence only:** two reads at least `broker_bar_settle_s` apart return the same OHLC,
  or the broker's partial flag is clear on an adapter whose flag PR3 confirmed (`trusts_partial_flag`). A later
  bucket in the same answer (closed or forming; evidence only, never stored by this read) makes the bar final only
  on an adapter whose PR3 probe showed the broker never amends bar `T` once `T + 1` appears
  (`trusts_next_bucket`, False until then: round 13, plan S38d). The same rule holds
  at the 17:00 New York break and at Friday's close (two agreeing reads, no later bucket). An empty bucket in a
  complete answer (read succeeded, not truncated) is `no_trade` (evidence `broker_history`) when the same answer
  holds a later **closed** bar (the broker has finished past it; a forming bar alone may precede a lagging bar), when
  the calendar is closed, or when it is still empty in a complete answer at its cap; a truncated answer says nothing
  about an empty bucket (`blind`). A `no_trade` bucket neither adds to nor resets the error unit;
* one **verification re-read** at `min(final + 60 s, cap − 30 s)`; a changed bar replaces the bucket with
  `revision + 1` (`bar_revised`); when no such time remains the bar carries the caveat `unverified_final` (kept per
  series, `unverified`), R11-m. `verified(series, bucket)` is True only once that re-read **positively** confirmed the
  bucket (the same hash, or a revision that was stored; the last `VERIFIED_KEEP` buckets per series): the ARM holds a
  release (`COMPLETE`) on any bar not confirmed so (round 13 review: an inconclusive re-read, a failed store read, no
  verification time left, a bucket final from the lane, or an untracked series never lets a release through; an
  `ADJUST` is never held). `awaiting_verification` (final, re-read not answered yet) is kept for status;
* **validation before queueing**: a bar failing a hard check is quarantined at once (`BarSink.quarantine_stored`,
  check name only) and its bucket stays `blind`; a forming bar (bucket end more than 5 s after receipt) is never
  validated, stored or counted (R11-M15);
* a final bar is compared with the stored one (`StoredBarsPort`, keyed hash verified under any retained key id, plus
  this task's own memory of what it queued): an identical bar is not written again, a changed one is queued as
  `revision + 1`, a new one as revision 0.

**Error unit (R11-M08, design §B14 "one counting unit").** A bucket adds at most 1, and only at its `decision_cap`:
still invalid there adds 1 to the series' `K` streak (`invalid_streak`); still not final (missing, or reads that
failed) adds 1 to `unresolved_streak`. A clean final bar that reached the store resets both, and a bucket older than
the newest clean one never adds afterwards (its cap may fall after the clean bucket's when the skew drifts). A
`no_trade` bucket neither adds nor resets. A bar final on broker evidence whose store comparison failed (`redis-md`
unreadable, the writer refused it) is decided again from the store every `DEFER_MS` without a broker re-read, and at
its cap goes to the lane uncounted: a store outage never feeds `unresolved_streak`. Decode errors,
undecodable answers and failed reads are retries inside the bucket's healing: a metric and one `info` page per
series episode (the episode ends at the next successful read), never the counter. A bad bar is quarantined once per
bucket until the bucket resolves, however often it is re-read. `K` invalid buckets in a row call
`on_fault(series, "invalid_bar")` once (the §B14 SERIES fault; S30 records FAILED and pings).

**Counting pauses** (nothing counts) for a bucket whose cap passed while the task was not running (a PC sleep), while
the market is not OPEN (when a calendar is wired), while the adapter is
not STREAMING or its REST reads fail at every priority (`AdapterHealthPort`), while the health port itself fails
(`is_streaming` raises: unknown, logged once, no page, no GAP, reads go on), and failed reads count only while
other REST priorities on the same adapter succeed (`rest_failing_everywhere` False; unknown counts nothing).

**Broker outage (R11-M10).** While the adapter is not STREAMING or REST fails at every priority, its series' reads
and cap counters pause, one `broker_unavailable` page goes out per adapter episode and every series of the adapter is
GAPPED (SM12), never FAILED. Buckets whose cap passes meanwhile go to the backfill lane uncounted. When the outage
ends the series backfill again and return to LIVE.

**SM12 BACKFILLING** (the `backfill_bars` its indicators declare, from the demand source): a series that is EMPTY or GAPPED (a
bootstrap, a writer error, an outage) or BACKFILLING after an operator clear gets a backfill job, read newest first
in pages through the range form; each page reaches one bucket past its newest (evidence only). A bar in a page is
final when a later bucket is in the answer or its cap has passed (broker history); an empty bucket of a complete page
is `no_trade` when a later closed bar is in the answer or its cap has passed; a bucket whose cap has not passed and has no evidence joins the live path. Each page
extends the backfilled range (`note_complete`); the whole window done is SM12 LIVE.

**Backfill lane (R11-M03).** After its cap a `blind` BROKER bucket moves to a low-priority lane inside the BARS budget
(at most `lane_reads_per_pass` reads per pass, after every live and backfill read): re-fetched with back-off until it
resolves (a bar, or `no_trade`) or broker history no longer reaches it (`unfillable` in the coverage map: older than
`history_reach_ms`, or `lane_max_misses` answers in a row that leave it unknown). Each resolution or `unfillable`
calls the `on_resolved` listeners (`bucket_resolved`: S18 recomputes ATR). A capped empty bucket of a complete lane
or backfill answer is `no_trade` only when the same answer holds a closed bar later or earlier than it (broker history
reaches it); an empty answer beyond the broker's real reach is a miss, so it ends `unfillable`, never `no_trade`. Each
lane read reaches one bucket before its oldest and one past its newest (evidence only). The lane also takes the consumer re-fetches
(`RefetchQueue`, §A13), the `md_backfill` work item (`request_backfill`, completed as `applied` / `refused`; refused
`series_failed` for a FAILED series, also for pending ones when the series enters FAILED) and the
**gap-heal** pass, which walks the stored buckets of each LIVE series inside retention, oldest first, a chunk at a low
rate, and puts every hole that is neither `no_trade` nor `unfillable` in the lane.

**Coverage map** per series (in memory, mirrored as counts in `md:meta` through `BarSink.set_meta`: `bars_phase`,
`cov_blind`, `cov_no_trade`, `cov_unfillable`, `bars_k`, `sm12`): `no_trade` and `unfillable` buckets; the lane is the
known `blind` set. Absent means blind: a stored bucket missing from `md:bars` is never read as closed.

**Fault containment.** Every series' fetch, decode and validation runs in its own try/except and under its own
deadline (the pass budget), and the reads of one pass run concurrently, so one series' failed, malformed or slow answer
never delays or cancels another; a series whose live read used more than half the budget skips its backfill page that
pass, and the lane and gap heal run with what remains of the budget.

**D6 cross-check (S17b, `crosscheck.py`).** When a `CrossChecker` is wired, every final bar this task commits
(queued, or identical to the stored one) is handed to it: live-path bars (first finality and the verification
re-read) as demanded buckets, backfill and lane bars as history (checked only when the TICK bar is still in memory).
`poll` runs once per pass (bars waiting for their TICK bar), a series leaving demand is forgotten (its state kept for
one timeframe, so a re-demand inside it resumes the streak), the D6 status and the cross-check counts join the
`md:meta` mirror, and `status` reports the D6 status and the mismatch streak. The
checker's own fault (`source_mismatch` at `K`) and skipped-ratio page go through the sinks it was built with.

**D7 feeds (plan S38e).** Each bucket counted unresolved at its cap is also handed to the error budget's
`note_unresolved` (`BucketNotes`; the budget's own `fail_unresolved_n` streak and pauses decide the SERIES fault
`unresolved`), a clean final bar to `note_final` and a `no_trade` bucket to `note_no_trade`. A consumer re-fetch
(`RefetchQueue`: a rollback needed beyond the rollback set, or a bar the writer's queue dropped) that the broker can no
longer serve (the bucket ends `unfillable`, or is already beyond `history_reach_ms`) calls
`on_fault(series, "refetch_failed")` once per series until its next clean bar (the budget drops it during a broker
outage).

**Clear probes (plan S38e).** `probe_reread(series)` re-reads the series' rollback set (the newest `rollback_n` closed
buckets) from the broker: it passes when every closed bar passes every hard check and none mismatches its TICK bar
where the D6 book holds one (`CrossChecker.check_only`, nothing recorded). `probe_fresh_bar(series)` waits for the next bar end
`T'` and reads that bar until two equal reads `settle` apart (or a trusted clear partial flag) make it final by
local `T' + post_grace` (a bar already closed when the probe starts never passes it). Both read through the same `BrokerBarsPort` (REST priority `BARS`), also while the series is FAILED
(md-bars does not read a FAILED series otherwise), and answer a check code or None (passed); they never write.

Nothing here logs, pages or formats a price: series ids, bucket starts, counts, check names and reason codes only;
no value reaches PostgreSQL (this task holds no PostgreSQL port).
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Final, Protocol

from src.dto.bars import BrokerBar, PriceBarsResponse
from src.marketdata.bootstrap import Sm12Event
from src.marketdata.codec import DecodedMember, decode_member
from src.marketdata.config import MdSettings
from src.marketdata.crosscheck import CrossChecker, D6Status, Verdict
from src.marketdata.errors import safe_redis_error
from src.marketdata.events import RefetchQueue
from src.marketdata.hashing import HashStatus, Keyring, content_hash, verify
from src.marketdata.ports import AdapterHealthPort, BrokerBarsPort, CalendarPort, PagePort, StampPort, calendar_open
from src.marketdata.store.reads import StoreReader
from src.marketdata.store.runner import StoreUnavailable
from src.marketdata.timing import (
    COLD_SKEW,
    SkewEstimate,
    TimingRow,
    decision_cap_ms,
    first_read_ms,
    next_read_ms,
    post_grace_end_ms,
    timing_for,
    verification_read_ms,
)
from src.marketdata.types import Bar, Quality, SeriesId, Sm12State, Source, Transient, Unsupported
from src.marketdata.validate import ResponseSplit, split_response

log = logging.getLogger(__name__)

DAY_MS: Final = 86_400_000
BROKER_RETENTION_MS: Final = 365 * DAY_MS
"""BROKER 15m retention (design §A5): the gap-heal pass walks holes inside it."""
STAMP_BUDGET_S: Final = 30.0
"""The supervisor's stamp budget for `md-bars` (design §A8)."""
PASS_BUDGET_S: Final = 20.0
READ_TIMEOUT_S: Final = 18.0
"""One broker read (the BARS wait is at most 15 s, PR11), inside the pass budget."""
INTERVAL_S: Final = 1.0
PAGE_BARS: Final = 200
LANE_READS_PER_PASS: Final = 2
LANE_RETRY_MIN_MS: Final = 60_000
LANE_RETRY_MAX_MS: Final = 30 * 60_000
LANE_MAX_MISSES: Final = 12
LANE_MAX_PER_SERIES: Final = 2_048
HEAL_EVERY_MS: Final = 10 * 60_000
HEAL_CHUNK: Final = 480
BACKFILL_RETRY_MIN_MS: Final = 5_000
BACKFILL_RETRY_MAX_MS: Final = 60_000
CAP_SLACK_MS: Final = 60_000
"""A bucket whose cap passed more than this before the pass that saw it (the engine was not running: a PC sleep, a
stalled loop) never counts (§B5.2: such bars go through replay)."""
DEFER_MS: Final = 5_000
"""A final bar that could not be compared with the store (store unreachable) is decided again this much later."""
MD_BACKFILL_MAX: Final = 2_880
"""Buckets one `md_backfill` request or one consumer re-fetch may name (30 days of 15m)."""
UNVERIFIED_KEEP: Final = 64
VERIFIED_KEEP: Final = 256
"""Buckets per series whose verification re-read confirmed them (`verified`): 2.6 days of 15m, a weekend's REOPEN
re-check included."""
KNOWN_KEEP: Final = 4_096
REVISION_MAX: Final = 9_999

APPLIED: Final = "applied"
REFUSED: Final = "refused"

_REASON_RE: Final = re.compile(r"[a-z][a-z0-9_]{0,31}")


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


def _code(text: str, default: str = "transient") -> str:
    """A port reason as a loggable code: a short lower-case word, else `default` (a reason is never echoed raw)."""
    text = text.strip().lower()
    return text if _REASON_RE.fullmatch(text) else default


# ---------------------------------------------------------------------------------------------------- values
class Coverage(str, Enum):
    """Coverage-map entries beyond stored bars (§A4): `blind` is the lane itself."""

    NO_TRADE = "no_trade"
    UNFILLABLE = "unfillable"


class Resolution(str, Enum):
    """How a lane bucket left the lane (`bucket_resolved`)."""

    BAR = "bar"
    NO_TRADE = "no_trade"
    UNFILLABLE = "unfillable"


class Phase(str, Enum):
    IDLE = "idle"
    BACKFILLING = "backfilling"
    LIVE = "live"
    PAUSED = "paused"            # broker outage (adapter not STREAMING / REST failing everywhere)
    FAILED = "failed"            # SM12 FAILED (S30): no reads until the operator clear
    UNSUPPORTED = "unsupported"  # no broker bars for this adapter / side, or no timing row for the spec


@dataclass(frozen=True)
class StoredBar:
    """What `md:bars` holds for a bucket: the keyed content hash and the node-local revision (never a price)."""

    content_hash: str
    revision: int


@dataclass(frozen=True, kw_only=True)
class BrokerBarsSettings:
    """The `md-bars` knobs. `settle_ms`, `lag_ms`, `mismatch_k` and `rollback_n` come from `MdSettings` (`from_md`);
    the rest are engine constants with test overrides."""

    settle_ms: int = 10_000
    lag_ms: int = 5_000
    mismatch_k: int = 3
    rollback_n: int = 8
    """The rollback set's size (`XTNL_MD_ROLLBACK_N`): the buckets the `series_reread` clear probe re-reads."""
    page_bars: int = PAGE_BARS
    lane_reads_per_pass: int = LANE_READS_PER_PASS
    lane_retry_min_ms: int = LANE_RETRY_MIN_MS
    lane_retry_max_ms: int = LANE_RETRY_MAX_MS
    lane_max_misses: int = LANE_MAX_MISSES
    lane_max_per_series: int = LANE_MAX_PER_SERIES
    heal_every_ms: int = HEAL_EVERY_MS
    heal_chunk: int = HEAL_CHUNK
    history_reach_ms: int = BROKER_RETENTION_MS
    md_backfill_max: int = MD_BACKFILL_MAX
    read_timeout_s: float = READ_TIMEOUT_S
    pass_budget_s: float = PASS_BUDGET_S
    interval_s: float = INTERVAL_S

    def __post_init__(self) -> None:
        if min(self.settle_ms, self.mismatch_k, self.rollback_n, self.page_bars, self.lane_reads_per_pass, self.lane_max_misses,
               self.lane_max_per_series, self.heal_chunk, self.md_backfill_max) < 1 or self.lag_ms < 0:
            raise ValueError("md-bars settings must be positive")
        if not 0 < self.read_timeout_s <= self.pass_budget_s < STAMP_BUDGET_S:
            raise ValueError("md-bars budgets must nest: read <= pass < the 30 s stamp budget")

    @classmethod
    def from_md(cls, md: MdSettings, **overrides: Any) -> BrokerBarsSettings:
        base = cls(settle_ms=md.broker_bar_settle_s * 1000, lag_ms=md.broker_bar_lag_ms, mismatch_k=md.mismatch_k,
                   rollback_n=md.rollback_n)
        return replace(base, **overrides) if overrides else base


@dataclass
class BarsCounters:
    passes: int = 0
    pass_timeouts: int = 0
    series_timeouts: int = 0         # one series' pass reached the pass budget (only that series is cut)
    backfill_skipped: int = 0        # a backfill page left for the next pass (the live read used half the budget)
    reads: int = 0
    failed_reads: int = 0            # Transient answers, timeouts and raising ports (retries, never counted)
    decode_errors: int = 0           # a Transient whose reason names a decode failure
    invalid_bars: int = 0            # closed bars failing a hard check (quarantined, bucket blind)
    quarantine_refused: int = 0      # the writer could not hold the quarantine entry
    forming_seen: int = 0            # forming bars (evidence only)
    bars_queued: int = 0
    bars_unchanged: int = 0          # final bars identical to the stored one (not written again)
    revisions: int = 0               # changed re-reads queued as revision + 1
    unencodable: int = 0
    no_trade: int = 0
    deferred: int = 0                # final but not comparable with the store this pass
    unverified_final: int = 0
    verified: int = 0
    verify_inconclusive: int = 0
    capped_invalid: int = 0          # buckets invalid at their cap (K)
    capped_unresolved: int = 0       # buckets not final at their cap (counted)
    capped_uncounted: int = 0        # capped while counting paused, store-deferred, or older than a clean bucket
    lane_added: int = 0
    lane_dropped: int = 0
    lane_reads: int = 0
    lane_resolved: int = 0
    unfillable: int = 0
    heal_runs: int = 0
    heal_holes: int = 0
    backfill_pages: int = 0
    backfill_done: int = 0
    refetch_drained: int = 0
    series_errors: int = 0
    listener_errors: int = 0
    faults: int = 0
    outages: int = 0
    failed_passes: int = 0           # passes while the ARM is globally FAILED (only md_backfill repairs run)
    refetch_failed: int = 0          # consumer re-fetches the broker can no longer serve (S38e, D7)
    note_errors: int = 0             # the error budget's bucket notes raised (counted, never stops md-bars)
    probe_reads: int = 0             # clear-probe reads (S38e)


@dataclass(frozen=True)
class BrokerSeriesStatus:
    """What `md show` / `arm_status` report for one BROKER series: states and counts only."""

    series: str
    phase: Phase
    live_pending: int
    lane: int
    no_trade: int
    unfillable: int
    unverified: int
    invalid_streak: int
    unresolved_streak: int
    backfilled: bool
    d6: str = D6Status.UNAVAILABLE.value
    mismatch_streak: int = 0


# ---------------------------------------------------------------------------------------------------- ports
class BarSink(Protocol):
    """`md-writer` (`MdWriter`): the closed-bar queue, quarantine and `md:meta` fields. None of it awaits."""

    def queue_bar(self, bar: Bar) -> None: ...

    def quarantine_stored(self, bar: Bar, check: str, member: str = "") -> bool: ...

    def set_meta(self, series: SeriesId, pairs: Sequence[tuple[str, str]]) -> bool: ...

    def is_queued(self, series: SeriesId, bucket_start_ms: int) -> bool: ...


class StoredBarsPort(Protocol):
    """What `md:bars` holds (hashes and revisions only). None: the store cannot be read now."""

    async def stored(self, series: SeriesId, from_ms: int, to_ms: int) -> Mapping[int, StoredBar] | None: ...

    async def span(self, series: SeriesId) -> tuple[int, int] | None:
        """`(first_bucket, last_bucket)` from `md:meta`, or None (nothing stored, or the store cannot be read)."""
        ...


class BookPort(Protocol):
    """The SM12 book (`bootstrap.SeriesStateBook`): `md-bars` applies BACKFILL, LIVE and GAP to BROKER series."""

    def state(self, series: SeriesId) -> Sm12State: ...

    def apply(self, series: SeriesId, event: Sm12Event) -> bool: ...

    def note_complete(self, series: SeriesId, from_ms: int, to_ms: int) -> None: ...


class BucketNotes(Protocol):
    """The error budget's bucket counter (`ErrorBudget.note_unresolved` / `note_final` / `note_no_trade`, S38e)."""

    def note_unresolved(self, series: SeriesId, bucket_ms: int, cap_ms: int, now_ms: int) -> None: ...

    def note_final(self, series: SeriesId, bucket_ms: int) -> None: ...

    def note_no_trade(self, series: SeriesId, bucket_ms: int) -> None: ...


DemandSource = Callable[[], Mapping[SeriesId, int]]
"""The demanded BROKER series, each with the bars its backfill needs (the widest declared `backfill_bars`, S18 demand)."""
ResolvedListener = Callable[[SeriesId, int, Resolution], None]
FaultSink = Callable[[SeriesId, str], None]
Completion = Callable[[int, str, str], None]
"""`complete(request_id, outcome, reason)`: outcome `applied` or `refused`; reason a short code."""


class StoreBarsReader:
    """`StoredBarsPort` over a `StoreReader` (the `md_writer` client in the engine). A member that does not decode is
    left out (absent: the next valid read replaces it). Store errors and timeouts are None, never exceptions."""

    __slots__ = ("_reader", "_timeout_s")

    def __init__(self, reader: StoreReader, *, timeout_s: float = 2.0) -> None:
        self._reader = reader
        self._timeout_s = timeout_s

    async def stored(self, series: SeriesId, from_ms: int, to_ms: int) -> Mapping[int, StoredBar] | None:
        try:
            async with asyncio.timeout(self._timeout_s):
                rows = await self._reader.bars_range(series, from_ms, to_ms)
        except (StoreUnavailable, TimeoutError):
            return None
        out: dict[int, StoredBar] = {}
        for start, member in rows:
            decoded = decode_member(series, member)
            if isinstance(decoded, DecodedMember) and decoded.bar.bucket_start_ms == start:
                out[start] = StoredBar(decoded.content_hash, decoded.bar.revision)
        return out

    async def span(self, series: SeriesId) -> tuple[int, int] | None:
        try:
            async with asyncio.timeout(self._timeout_s):
                meta = await self._reader.meta(series)
        except (StoreUnavailable, TimeoutError):
            return None
        first, last = meta.get("first_bucket", ""), meta.get("last_bucket", "")
        if not (first.isdigit() and last.isdigit()):
            return None
        return int(first), int(last)


# ---------------------------------------------------------------------------------------------------- state
@dataclass
class _Obs:
    """One read's bar for a bucket (its OHLC, memory only) and the receipt time: two equal ones `settle` apart make
    the bar final."""

    at_ms: int
    bar: BrokerBar = field(repr=False)

    def same(self, other: _Obs) -> bool:
        a, b = self.bar, other.bar
        return (a.open, a.high, a.low, a.close) == (b.open, b.high, b.low, b.close)


@dataclass
class _Live:
    """A bucket on the live schedule: not final yet, or final and waiting for its verification re-read."""

    start: int
    end: int
    cap_ms: int
    next_at: int | None
    reads: int = 0
    obs: _Obs | None = None
    invalid: bool = False
    failed: bool = False
    empty: bool = False          # the last read was a complete answer without this bucket
    deferred: bool = False       # final on broker evidence (`obs`), waiting for the store comparison
    final_at: int | None = None


@dataclass
class _Lane:
    next_at: int
    tries: int = 0
    misses: int = 0
    request_ids: set[int] = field(default_factory=set)
    refetch: bool = False        # a consumer re-fetch (S38e: unfillable is `refetch_failed`)


@dataclass
class _Job:
    """A BACKFILLING window `[lo, hi]` read newest first: `cursor` is the newest bucket of the next page."""

    lo: int
    hi: int
    cursor: int
    next_at: int = 0
    failures: int = 0
    done: bool = False


@dataclass
class _Request:
    request_id: int
    series: SeriesId
    remaining: set[int]
    unfillable: int = 0


@dataclass
class _Series:
    series: SeriesId
    row: TimingRow | None
    need: int
    next_end: int | None = None
    live: dict[int, _Live] = field(default_factory=dict)
    lane: dict[int, _Lane] = field(default_factory=dict)
    coverage: dict[int, Coverage] = field(default_factory=dict)
    known: dict[int, StoredBar] = field(default_factory=dict)
    unverified: list[int] = field(default_factory=list)
    verified: dict[int, None] = field(default_factory=dict)
    """Bucket starts whose verification re-read confirmed the stored bar (insertion order, the newest last)."""
    quarantined: set[int] = field(default_factory=set)
    job: _Job | None = None
    backfilled: bool = False
    invalid_streak: int = 0
    unresolved_streak: int = 0
    last_clean_start: int | None = None
    fault_sent: bool = False
    refetch_fault_sent: bool = False
    read_episode: bool = False
    unsupported: str | None = None
    heal_cursor: int | None = None
    next_heal_at: int = 0
    meta_sent: tuple[tuple[str, str], ...] = ()


@dataclass
class _Commit:
    """A final bar decided by a read, waiting for the comparison with the store."""

    start: int
    bar: BrokerBar = field(repr=False)


# ---------------------------------------------------------------------------------------------------- md-bars
class MdBars:
    """The `md-bars` task body. One instance per term (S31)."""

    def __init__(self, port: BrokerBarsPort, sink: BarSink, book: BookPort, *, keyring: Keyring,
                 demand: DemandSource, settings: BrokerBarsSettings | None = None,
                 stored: StoredBarsPort | None = None, health: AdapterHealthPort | None = None,
                 calendar: CalendarPort | None = None, pager: PagePort | None = None,
                 skew: Callable[[str], SkewEstimate] | None = None,
                 trusts_partial_flag: Callable[[str], bool] | None = None,
                 trusts_next_bucket: Callable[[str], bool] | None = None,
                 refetch: RefetchQueue | None = None, on_fault: FaultSink | None = None,
                 complete: Completion | None = None, global_failed: Callable[[], bool] | None = None,
                 crosscheck: CrossChecker | None = None, buckets: BucketNotes | None = None,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                 clock_ms: Callable[[], int] = _wall_ms) -> None:
        self._port = port
        self._sink = sink
        self._book = book
        self._keyring = keyring
        self._demand = demand
        self._cfg = settings or BrokerBarsSettings()
        self._stored = stored
        self._health = health
        self._calendar = calendar
        self._pager = pager
        self._skew = skew
        self._trusts_partial = trusts_partial_flag
        self._trusts_next = trusts_next_bucket
        self._refetch = refetch
        self._on_fault = on_fault
        self._complete = complete
        self._global_failed = global_failed
        self._crosscheck = crosscheck
        self._buckets = buckets
        self._sleep = sleep
        self._clock_ms = clock_ms
        self._series: dict[SeriesId, _Series] = {}
        self._listeners: list[ResolvedListener] = []
        self._requests: dict[int, _Request] = {}
        self._outage: set[str] = set()
        self._health_unknown: set[str] = set()
        self._lane_turn = 0
        self.counters = BarsCounters()

    # -------------------------------------------------------------------------------------------- control
    def on_resolved(self, listener: ResolvedListener) -> None:
        """`bucket_resolved`: a lane bucket became a bar or `no_trade`, or was given up as `unfillable` (S18)."""
        self._listeners.append(listener)

    def tracked(self) -> tuple[SeriesId, ...]:
        return tuple(sorted(self._series))

    def awaiting_verification(self, series: SeriesId, bucket_start_ms: int) -> bool:
        """The bar of this bucket is final and its verification re-read has not answered yet (round 13): the ARM
        holds a release decided on it until then. False once verified (same or revised), once the bucket has left
        the live path (verified, capped, or `unverified_final`) and for an untracked series."""
        st = self._series.get(series)
        if st is None:
            return False
        lb = st.live.get(bucket_start_ms)
        return lb is not None and lb.final_at is not None and not lb.deferred

    def verified(self, series: SeriesId, bucket_start_ms: int) -> bool:
        """The verification re-read of this bucket answered and confirmed it: the same hash as stored, or a revision
        that the store took (round 13 review). False for anything else: not re-read yet, an inconclusive re-read (the
        bucket missing from the answer, a failed read or store read), no verification time left
        (`unverified_final`), a bucket made final by the lane or a backfill, an untracked series (after a restart:
        design deviation 4). The ARM releases only on a verified bar."""
        st = self._series.get(series)
        return st is not None and bucket_start_ms in st.verified

    def unverified(self, series: SeriesId) -> frozenset[int]:
        """Bucket starts of final bars that carry the caveat `unverified_final` (no verification time remained)."""
        st = self._series.get(series)
        return frozenset(st.unverified) if st is not None else frozenset()

    def coverage(self, series: SeriesId) -> Mapping[int, Coverage]:
        st = self._series.get(series)
        return dict(st.coverage) if st is not None else {}

    def coverage_of(self, series: SeriesId, bucket_ms: int) -> Coverage | None:
        """One bucket's coverage (a point lookup, no copy of the map; S38c review), None when unknown."""
        st = self._series.get(series)
        return None if st is None else st.coverage.get(bucket_ms)

    def lane(self, series: SeriesId) -> frozenset[int]:
        st = self._series.get(series)
        return frozenset(st.lane) if st is not None else frozenset()

    def status(self, series: SeriesId) -> BrokerSeriesStatus | None:
        st = self._series.get(series)
        if st is None:
            return None
        xc = self._crosscheck
        return BrokerSeriesStatus(series.canonical, self._phase(st), sum(1 for b in st.live.values()
                                                                         if b.final_at is None),
                                  len(st.lane), self._count(st, Coverage.NO_TRADE),
                                  self._count(st, Coverage.UNFILLABLE), len(st.unverified), st.invalid_streak,
                                  st.unresolved_streak, st.backfilled,
                                  (xc.d6(series) if xc is not None else D6Status.UNAVAILABLE).value,
                                  xc.mismatch_streak(series) if xc is not None else 0)

    def request_backfill(self, request_id: int, series: SeriesId, from_bucket_ms: int, to_bucket_ms: int) -> str | None:
        """The `md_backfill` work item (from the S32 handler; never run in the operator loop): re-fetch
        `[from, to]` in the backfill lane. None when accepted (completed later as `applied`), else the refusal code
        (the handler answers `refused` with it)."""
        st = self._series.get(series)
        if series.source is not Source.BROKER:
            return "tick_series"
        if st is None:
            return "not_demanded"
        if st.unsupported is not None:
            return "unsupported"
        if self._book.state(series) is Sm12State.FAILED:
            return "series_failed"
        spec = series.spec
        if to_bucket_ms < from_bucket_ms or not (spec.on_grid(from_bucket_ms) and spec.on_grid(to_bucket_ms)):
            return "bucket_range"
        if (to_bucket_ms - from_bucket_ms) // spec.timeframe_ms + 1 > self._cfg.md_backfill_max:
            return "range_too_large"
        if request_id in self._requests:
            return "duplicate"
        n = (to_bucket_ms - from_bucket_ms) // spec.timeframe_ms + 1
        if len(st.lane) + n > self._cfg.lane_max_per_series:
            return "lane_full"
        now = self._clock_ms()
        req = _Request(request_id, series, set())
        self._requests[request_id] = req
        for start in range(from_bucket_ms, to_bucket_ms + 1, spec.timeframe_ms):
            if self._lane_add(st, start, now, force=True, request=request_id):
                req.remaining.add(start)
            elif st.coverage.get(start) is Coverage.UNFILLABLE:
                req.unfillable += 1
        self._finish_request(req)
        return None

    # -------------------------------------------------------------------------------------------- clear probes
    async def probe_reread(self, series: SeriesId) -> str | None:
        """The `series_reread` clear probe (§B14 Clearing, plan S38e): re-read the rollback set (the newest
        `rollback_n` closed buckets) from the broker. None when every closed bar passes every hard check and none
        mismatches its TICK bar (where the D6 book holds one); else a check code. Never writes, never counts."""
        row = timing_for(series.spec)
        early = self._probe_precheck(series, row)
        if early is not None or row is None:
            return early or "no_timing_row"
        skew = self._skew_for(series.adapter_id)
        tf = series.spec.timeframe_ms
        newest = series.spec.bucket_start_ms(self._clock_ms() - skew.p50_ms - skew.settle_ms) - tf
        lo = newest - (max(self._cfg.rollback_n, row.rollback_n_min) - 1) * tf
        resp = await self._probe_fetch(series, lo, newest + tf)
        if isinstance(resp, str):
            return resp
        try:
            split = split_response(resp, series)
        except Exception:
            return "read_failed"
        if any(lo <= b.bucket_start_ms <= newest for b, _ in split.invalid):
            return "invalid_bar"
        closed = [b for b in split.valid if lo <= b.bucket_start_ms <= newest]
        if not closed:
            return "no_bars"
        if self._crosscheck is not None:
            for b in closed:
                bar = Bar(series=series, bucket_start_ms=b.bucket_start_ms, open=b.open, high=b.high, low=b.low,
                          close=b.close, quality=Quality.COMPLETE, final_at_ms=resp.received_ms)
                try:
                    verdict = self._crosscheck.check_only(bar).verdict
                except Exception:
                    return "crosscheck_error"
                if verdict is Verdict.MISMATCH:
                    return "source_mismatch"
        return None

    async def probe_fresh_bar(self, series: SeriesId) -> str | None:
        """The `fresh_bar` clear probe (§B14 Clearing, plan S38e): one fresh bar read reaches finality within the
        row's `post_grace` of its bar end. The probe takes the next bar end `T'` after it starts (a bucket already
        closed proves nothing about the cause recorded, `unresolved`), sleeps until `T' + p50 + settle` (the live
        path's first read), then reads bucket `T' - timeframe` until two equal reads at least `settle` apart (or a
        clear partial flag on an adapter whose flag PR3 confirmed) make it final. None when that happens by local
        `T' + post_grace`; else a check code (`not_final`, `no_bar`, `market_closed`, a read code)."""
        row = timing_for(series.spec)
        early = self._probe_precheck(series, row)
        if early is not None or row is None:
            return early or "no_timing_row"
        if self._probe_closed(series):
            return "market_closed"
        skew = self._skew_for(series.adapter_id)
        tf = series.spec.timeframe_ms
        end = series.spec.bucket_start_ms(self._clock_ms() - skew.p50_ms) + tf     # T': the next bar end
        start = end - tf
        deadline = post_grace_end_ms(end, row, skew)
        wait = first_read_ms(end, skew) - self._clock_ms()
        if wait > 0:
            await self._sleep(wait / 1000)
            if self._probe_closed(series):
                return "market_closed"
        trusts = False
        if self._trusts_partial is not None:
            try:
                trusts = bool(self._trusts_partial(series.adapter_id))
            except Exception:
                trusts = False
        first: _Obs | None = None
        while True:
            resp = await self._probe_fetch(series, start, end)
            if isinstance(resp, str):
                return resp
            try:
                split = split_response(resp, series)
            except Exception:
                return "read_failed"
            if any(b.bucket_start_ms == start for b, _ in split.invalid):
                return "invalid_bar"
            bar = next((b for b in split.valid if b.bucket_start_ms == start), None)
            if bar is not None:
                obs = _Obs(resp.received_ms, bar)
                final = (trusts and not bar.partial) or (
                    first is not None and first.same(obs) and obs.at_ms - first.at_ms >= self._cfg.settle_ms)
                if final:
                    return None if self._clock_ms() <= deadline else "not_final"
                if first is None or not first.same(obs):
                    first = obs
            if self._clock_ms() + self._cfg.settle_ms > deadline:
                return "not_final" if first is not None else "no_bar"
            await self._sleep(self._cfg.settle_ms / 1000)

    def _probe_closed(self, series: SeriesId) -> bool:
        """The market is not OPEN (a calendar that cannot answer refuses too: a clear probe fails closed)."""
        if self._calendar is None:
            return False
        try:
            return not calendar_open(self._calendar, series.adapter_id, series.symbol)
        except Exception:
            return True

    def _probe_precheck(self, series: SeriesId, row: TimingRow | None) -> str | None:
        if series.source is not Source.BROKER:
            return "not_broker"
        if row is None:
            return "no_timing_row"
        streaming = self._streaming(series.adapter_id)
        if streaming is False or self._rest_status(series.adapter_id) is True:
            return "broker_unavailable"
        return None

    async def _probe_fetch(self, series: SeriesId, from_ms: int, to_ms: int) -> PriceBarsResponse | str:
        """One probe read (no counter of the live path, no page): the response, or a check code."""
        self.counters.probe_reads += 1
        try:
            async with asyncio.timeout(self._cfg.read_timeout_s):
                result = await self._port.fetch_price_bars(series.adapter_id, series.symbol, series.side,
                                                           series.spec.timeframe_s, from_ms=from_ms, to_ms=to_ms)
        except Exception:   # a timeout or a raising port: a failed probe read, never an exception to the caller
            return "read_failed"
        if isinstance(result, Unsupported) or (isinstance(result, PriceBarsResponse) and not result.supported):
            return "unsupported"
        if not isinstance(result, PriceBarsResponse):
            return "read_failed"
        return result

    # -------------------------------------------------------------------------------------------- task body
    async def run(self, stop: asyncio.Event, stamp: StampPort) -> None:
        while not stop.is_set():
            stamp.stamp()
            await self.step(stop)
            stamp.stamp()
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._cfg.interval_s)
            except TimeoutError:
                pass

    async def step(self, stop: asyncio.Event | None = None) -> None:
        """One pass: demand, re-fetch requests, outages, then every series' live and backfill reads concurrently,
        then the lane and the gap-heal pass, all inside one pass budget."""
        self.counters.passes += 1
        if self._global_failed is not None and self._global_failed():
            await self._repair_pass()       # ARM global FAILED: md-bars stops writing (§B14), repairs still run
            return
        now = self._clock_ms()
        self._sync_demand(now)
        self._poll_crosscheck(now)
        self._drain_refetch(now)
        self._check_outages(now)
        began = time.monotonic()
        deadline = began + self._cfg.pass_budget_s
        try:
            # each series runs under its own deadline: a slow one is cut alone, never the others' work
            await asyncio.gather(*(self._series_pass(st, now, began, deadline) for st in list(self._series.values())))
            if stop is not None and stop.is_set():
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            async with asyncio.timeout(remaining):
                await self._lane_pass(self._clock_ms())
                await self._heal_pass(self._clock_ms())
        except TimeoutError:
            self.counters.pass_timeouts += 1
            log.warning("md-bars: pass budget reached; the rest waits for the next pass")
        finally:
            for st in self._series.values():
                self._mirror(st)

    async def _repair_pass(self) -> None:
        """A pass while the ARM is globally FAILED (§B14): no demand change, live read, re-fetch or gap heal; only
        the lane buckets an operator `md_backfill` asked for (a repair, applied during FAILED, §B13) are read and
        written, inside the pass budget."""
        self.counters.failed_passes += 1
        if not self._requests:
            return
        try:
            async with asyncio.timeout(self._cfg.pass_budget_s):
                await self._lane_pass(self._clock_ms(), requests_only=True)
        except TimeoutError:
            self.counters.pass_timeouts += 1
            log.warning("md-bars: pass budget reached during FAILED; the backfill waits for the next pass")
        finally:
            for st in self._series.values():
                self._mirror(st)

    # -------------------------------------------------------------------------------------------- demand
    def _sync_demand(self, now: int) -> None:
        try:
            wanted = {s: max(int(n), 1) for s, n in self._demand().items() if s.source is Source.BROKER}
        except Exception as e:   # a demand-source fault keeps the current set
            log.warning("md-bars: demand source failed (%s)", safe_redis_error(e))
            return
        for series in [s for s in self._series if s not in wanted]:
            gone = self._series.pop(series)
            if self._crosscheck is not None:
                self._crosscheck.forget(series, now_ms=now)
            for req_id in {r for e in gone.lane.values() for r in e.request_ids}:
                req = self._requests.pop(req_id, None)
                if req is not None:
                    self._done(req.request_id, REFUSED, "not_demanded")
        for series, need in wanted.items():
            st = self._series.get(series)
            if st is None:
                if self._crosscheck is not None:
                    self._crosscheck.resume(series)     # a brief lapse of demand keeps the cross-check streak
                row = timing_for(series.spec)
                st = self._series[series] = _Series(series, row, need)
                if row is None:
                    st.unsupported = "timing"
                    log.warning("md-bars: %s has no timing row; not read", series.canonical)
            else:
                st.need = need

    def _poll_crosscheck(self, now: int) -> None:
        if self._crosscheck is None:
            return
        try:
            self._crosscheck.poll(now)
        except Exception as e:   # the cross-check never stops md-bars
            log.warning("md-bars: cross-check poll failed (%s)", safe_redis_error(e))

    def _cross(self, st: _Series, bar: Bar, live: bool) -> None:
        """Hand a committed final bar to the D6 cross-check (live: a demanded bucket of the live schedule)."""
        if self._crosscheck is None:
            return
        try:
            self._crosscheck.submit(bar, live=live, now_ms=self._clock_ms())
        except Exception as e:   # the cross-check never stops md-bars
            log.warning("md-bars: cross-check of %s failed (%s)", st.series.canonical, safe_redis_error(e))

    def _drain_refetch(self, now: int) -> None:
        if self._refetch is None:
            return
        for req in self._refetch.drain():
            st = self._series.get(req.series)
            if st is None or st.unsupported is not None:
                continue
            tf = req.series.spec.timeframe_ms
            lo = max(req.from_bucket_ms, req.to_bucket_ms - (self._cfg.md_backfill_max - 1) * tf)
            self.counters.refetch_drained += 1
            for start in range(lo, req.to_bucket_ms + 1, tf):
                if not self._lane_add(st, start, now, force=True, refetch=True) and \
                        st.coverage.get(start) is Coverage.UNFILLABLE:
                    self._refetch_failed(st, start)     # beyond broker reach already (S38e)

    # -------------------------------------------------------------------------------------------- outage
    def _rest_status(self, adapter_id: str) -> bool | None:
        if self._health is None:
            return False
        try:
            return self._health.rest_failing_everywhere(adapter_id)
        except Exception:
            return None

    def _streaming(self, adapter_id: str) -> bool | None:
        """None: unknown (the health port raised). A health-wiring fault is never read as a broker outage."""
        if self._health is None:
            return True
        try:
            return bool(self._health.is_streaming(adapter_id))
        except Exception:
            return None

    def _check_outages(self, now: int) -> None:
        for adapter_id in sorted({s.adapter_id for s in self._series}):
            streaming = self._streaming(adapter_id)
            down = streaming is False or self._rest_status(adapter_id) is True
            if streaming is None and not down:
                # unknown: counting pauses (no page, no GAP, reads go on); an outage already open stays open
                if adapter_id not in self._health_unknown:
                    self._health_unknown.add(adapter_id)
                    log.warning("md-bars: %s adapter health unknown (the health port failed); counting paused",
                                adapter_id)
                continue
            if adapter_id in self._health_unknown:
                self._health_unknown.discard(adapter_id)
                log.info("md-bars: %s adapter health known again", adapter_id)
            if down and adapter_id not in self._outage:
                self._outage.add(adapter_id)
                self.counters.outages += 1
                self._page("high", "broker bars unavailable",
                           f"{adapter_id}: broker_unavailable; its BROKER series are GAPPED and nothing counts "
                           "toward FAILED until it recovers")
            elif not down and adapter_id in self._outage:
                self._outage.discard(adapter_id)
                log.info("md-bars: %s broker reads available again; BROKER series backfill", adapter_id)
            if down:
                for st in self._series.values():
                    if st.series.adapter_id == adapter_id and \
                            self._book.state(st.series) in (Sm12State.LIVE, Sm12State.BACKFILLING):
                        self._book.apply(st.series, Sm12Event.GAP)

    def _paused(self, st: _Series) -> bool:
        return st.series.adapter_id in self._outage

    def _counting(self, st: _Series) -> bool:
        """False while counting pauses: adapter outage, or the market not OPEN (when a calendar is wired)."""
        if self._paused(st) or st.series.adapter_id in self._health_unknown:
            return False
        if self._calendar is None:
            return True
        try:
            return calendar_open(self._calendar, st.series.adapter_id, st.series.symbol)
        except Exception:
            return False

    def _calendar_closed(self, st: _Series) -> bool:
        if self._calendar is None:
            return False
        try:
            return not calendar_open(self._calendar, st.series.adapter_id, st.series.symbol)
        except Exception:
            return False

    # -------------------------------------------------------------------------------------------- one series
    def _skew_of(self, st: _Series) -> SkewEstimate:
        return self._skew_for(st.series.adapter_id)

    def _skew_for(self, adapter_id: str) -> SkewEstimate:
        if self._skew is None:
            return COLD_SKEW
        try:
            return self._skew(adapter_id)
        except Exception:
            return COLD_SKEW

    def _phase(self, st: _Series) -> Phase:
        if st.unsupported is not None:
            return Phase.UNSUPPORTED
        if self._book.state(st.series) is Sm12State.FAILED:
            return Phase.FAILED
        if self._paused(st):
            return Phase.PAUSED
        if st.job is not None and not st.job.done:
            return Phase.BACKFILLING
        return Phase.LIVE if st.next_end is not None else Phase.IDLE

    async def _series_pass(self, st: _Series, now: int, began: float, deadline: float) -> None:
        try:
            async with asyncio.timeout(max(deadline - time.monotonic(), 0.0)):
                await self._series_work(st, now, began, deadline)
        except TimeoutError:     # this series alone is cut; the rest waits for its next pass
            self.counters.series_timeouts += 1
            log.warning("md-bars: pass for %s reached the pass budget", st.series.canonical)
        except Exception as e:   # one series' fault never stops another
            self.counters.series_errors += 1
            log.warning("md-bars: pass for %s failed (%s)", st.series.canonical, safe_redis_error(e))

    async def _series_work(self, st: _Series, now: int, began: float, deadline: float) -> None:
        if st.unsupported is not None or st.row is None:
            return
        if self._book.state(st.series) is Sm12State.FAILED:
            self._refuse_requests(st, "series_failed")
            return                          # no reads until the operator clear (then BACKFILLING)
        skew = self._skew_of(st)
        self._schedule(st, st.row, skew, now)
        self._expire(st, now)
        if self._paused(st):
            return
        self._ensure_job(st, now)
        await self._retry_deferred(st, st.row, now)
        await self._live_read(st, st.row, skew, now)
        if st.unsupported is not None:
            return
        if time.monotonic() - began > (deadline - began) / 2:
            self.counters.backfill_skipped += 1     # a slow live read: the page waits for the next pass
            return
        await self._backfill_page(st, now)

    def _refuse_requests(self, st: _Series, reason: str) -> None:
        """Complete every pending `md_backfill` request of the series as `refused` (it will not be served)."""
        gone = [r for r in self._requests.values() if r.series == st.series]
        if not gone:
            return
        ids = {r.request_id for r in gone}
        for entry in st.lane.values():
            entry.request_ids -= ids
        for req in gone:
            del self._requests[req.request_id]
            self._done(req.request_id, REFUSED, reason)

    def _schedule(self, st: _Series, row: TimingRow, skew: SkewEstimate, now: int) -> None:
        """Put every bar end whose first read is due on the live schedule (a bucket whose cap already passed goes to
        the lane uncounted: the engine was not reading it)."""
        tf = st.series.spec.timeframe_ms
        newest_end = st.series.spec.bucket_start_ms(now - skew.p50_ms - skew.settle_ms)
        if st.next_end is None or newest_end - st.next_end > st.need * tf:
            if st.next_end is not None:
                st.job = None               # a long gap (a sleep): a fresh backfill of the newest window covers it
                self._mark_gapped(st)
            st.next_end = newest_end
        while first_read_ms(st.next_end, skew) <= now:
            end = st.next_end
            start = end - tf
            cap = decision_cap_ms(end, st.series.spec, skew)
            if start not in st.live:
                if cap <= now:
                    self._lane_add(st, start, now)
                else:
                    st.live[start] = _Live(start, end, cap, first_read_ms(end, skew))
            st.next_end = end + tf

    def _mark_gapped(self, st: _Series) -> None:
        if self._book.state(st.series) in (Sm12State.LIVE, Sm12State.BACKFILLING):
            self._book.apply(st.series, Sm12Event.GAP)

    def _expire(self, st: _Series, now: int) -> None:
        """Buckets at their `decision_cap`: live re-reading stops. A final bar never verified is `unverified_final`;
        a bucket not final adds to the error unit (when counting) and moves to the lane."""
        for start in sorted(st.live):
            lb = st.live[start]
            if now < lb.cap_ms:
                continue
            del st.live[start]
            if lb.final_at is not None:
                self._note_unverified(st, start)
                continue
            if lb.deferred:                             # final on broker evidence; only the store compare failed
                self.counters.capped_uncounted += 1
                self._lane_add(st, start, now)
                continue
            if lb.empty:                                # still empty in a complete answer at its cap
                self._no_trade(st, start)
                continue
            counting = self._counting(st) and now - lb.cap_ms <= CAP_SLACK_MS
            superseded = st.last_clean_start is not None and start < st.last_clean_start
            if not counting or superseded:              # a later bucket was already clean: the streak moved on
                self.counters.capped_uncounted += 1
            elif lb.invalid:
                self.counters.capped_invalid += 1
                st.invalid_streak += 1
            elif lb.failed and self._rest_status(st.series.adapter_id) is not False:
                self.counters.capped_uncounted += 1     # failed reads count only while other priorities succeed
            else:
                self.counters.capped_unresolved += 1
                st.unresolved_streak += 1
                self._note("note_unresolved", st.series, start, lb.cap_ms, now)
            self._lane_add(st, start, now)
            if st.invalid_streak >= self._cfg.mismatch_k and not st.fault_sent:
                st.fault_sent = True
                self.counters.faults += 1
                log.warning("md-bars: %s has %d invalid buckets in a row (invalid_bar)", st.series.canonical,
                            st.invalid_streak)
                if self._on_fault is not None:
                    try:
                        self._on_fault(st.series, "invalid_bar")
                    except Exception as e:
                        log.warning("md-bars: fault sink failed (%s)", safe_redis_error(e))

    def _note_verified(self, st: _Series, start: int) -> None:
        self.counters.verified += 1
        st.verified.pop(start, None)
        st.verified[start] = None
        while len(st.verified) > VERIFIED_KEEP:
            del st.verified[next(iter(st.verified))]

    def _note_unverified(self, st: _Series, start: int) -> None:
        self.counters.unverified_final += 1
        st.unverified.append(start)
        del st.unverified[:-UNVERIFIED_KEEP]

    def _clean(self, st: _Series, start: int) -> None:
        st.invalid_streak = st.unresolved_streak = 0
        st.fault_sent = st.refetch_fault_sent = False
        if st.last_clean_start is None or start > st.last_clean_start:
            st.last_clean_start = start
        self._note("note_final", st.series, start)

    def _note(self, method: str, series: SeriesId, *args: int) -> None:
        """One bucket note to the error budget (S38e); a raising budget is counted, never stops md-bars."""
        b = self._buckets
        if b is None:
            return
        try:
            if method == "note_unresolved":
                b.note_unresolved(series, args[0], args[1], args[2])
            elif method == "note_final":
                b.note_final(series, args[0])
            else:
                b.note_no_trade(series, args[0])
        except Exception as e:
            self.counters.note_errors += 1
            log.warning("md-bars: error budget note failed (%s)", safe_redis_error(e))

    def _refetch_failed(self, st: _Series, start: int) -> None:
        """A consumer re-fetch the broker can no longer serve (D7, plan S38e): `refetch_failed` once per series
        until its next clean bar."""
        self.counters.refetch_failed += 1
        if st.refetch_fault_sent:
            return
        st.refetch_fault_sent = True
        self.counters.faults += 1
        log.warning("md-bars: %s re-fetch of bucket %d failed (refetch_failed)", st.series.canonical, start)
        if self._on_fault is not None:
            try:
                self._on_fault(st.series, "refetch_failed")
            except Exception as e:
                log.warning("md-bars: fault sink failed (%s)", safe_redis_error(e))

    # -------------------------------------------------------------------------------------------- reads
    async def _fetch(self, st: _Series, from_ms: int, to_ms: int) -> PriceBarsResponse | Unsupported | Transient:
        s = st.series
        self.counters.reads += 1
        try:
            async with asyncio.timeout(self._cfg.read_timeout_s):
                result = await self._port.fetch_price_bars(s.adapter_id, s.symbol, s.side, s.spec.timeframe_s,
                                                           from_ms=from_ms, to_ms=to_ms)
        except TimeoutError:
            result = Transient("timeout")
        except Exception as e:   # a raising port is a failed read of this series only
            log.warning("md-bars: bar read for %s raised (%s)", s.canonical, safe_redis_error(e))
            result = Transient("error")
        if isinstance(result, PriceBarsResponse) and not result.supported:
            result = Unsupported("not_supported")
        if not isinstance(result, (PriceBarsResponse, Unsupported, Transient)):
            return Transient("malformed")
        if isinstance(result, Unsupported):
            self._unsupported(st)
        elif isinstance(result, Transient):
            self._read_failed(st, result.reason)
        elif st.read_episode:
            st.read_episode = False
            log.info("md-bars: bar reads of %s succeed again", s.canonical)
        return result

    def _unsupported(self, st: _Series) -> None:
        if st.unsupported is None:
            st.unsupported = "broker"
            log.warning("md-bars: %s has no broker bars (UNSUPPORTED); not read", st.series.canonical)

    def _read_failed(self, st: _Series, reason: str) -> None:
        code = _code(reason)
        self.counters.failed_reads += 1
        if "decode" in code:
            self.counters.decode_errors += 1
        if not st.read_episode:
            st.read_episode = True
            self._page("info", "broker bar reads failing",
                       f"{st.series.canonical}: bar reads fail ({code}); retried on the schedule, never counted")

    def _split(self, st: _Series, resp: PriceBarsResponse) -> ResponseSplit | None:
        try:
            split = split_response(resp, st.series)
        except Exception as e:   # a malformed answer is a failed read of this series
            log.warning("md-bars: answer for %s not usable (%s)", st.series.canonical, safe_redis_error(e))
            self._read_failed(st, "malformed")
            return None
        self.counters.forming_seen += len(split.forming)
        return split

    def _quarantine(self, st: _Series, bad: BrokerBar, check: str) -> None:
        self.counters.invalid_bars += 1
        if bad.bucket_start_ms in st.quarantined:
            return                                  # once per bucket until it resolves
        if len(st.quarantined) >= self._cfg.lane_max_per_series:
            st.quarantined.clear()
        st.quarantined.add(bad.bucket_start_ms)
        bar = Bar(series=st.series, bucket_start_ms=bad.bucket_start_ms, open=bad.open, high=bad.high, low=bad.low,
                  close=bad.close, quality=Quality.BLIND)
        try:
            held = self._sink.quarantine_stored(bar, check)
        except Exception:
            held = False
        if not held:
            self.counters.quarantine_refused += 1

    @staticmethod
    def _evidence(split: ResponseSplit) -> tuple[int | None, int | None, int | None]:
        """`(newest bucket of any valid or forming bar, newest and oldest bucket of a valid closed bar)`: a later
        bucket of either kind finalises a bar; only a later closed bar finalises an empty bucket as `no_trade`, and a
        capped empty bucket needs a closed bar on either side (the broker's history reaches it)."""
        closed = [b.bucket_start_ms for b in split.valid]
        starts = closed + [b.bucket_start_ms for b in split.forming]
        return (max(starts) if starts else None), (max(closed) if closed else None), (min(closed) if closed else None)

    @staticmethod
    def _reached(start: int, capped: bool, closed_after: int | None, closed_before: int | None) -> bool:
        """A missing bucket of a complete answer is `no_trade`: a later closed bar, or (capped) an earlier one."""
        if closed_after is not None and closed_after > start:
            return True
        return capped and closed_before is not None and closed_before < start

    # -------------------------------------------------------------------------------------------- live path
    async def _live_read(self, st: _Series, row: TimingRow, skew: SkewEstimate, now: int) -> None:
        due = [lb for lb in st.live.values() if lb.next_at is not None and lb.next_at <= now and not lb.deferred]
        if not due:
            return
        due_starts = {lb.start for lb in due}
        spec = st.series.spec
        lo = min(lb.start for lb in due)
        hi = max(spec.bucket_start_ms(now - skew.p50_ms), max(st.live))
        result = await self._fetch(st, lo, hi)
        read_at = self._clock_ms()
        if not isinstance(result, PriceBarsResponse):
            for lb in due:
                self._after_failed_read(st, lb, row, skew, read_at)
            return
        split = self._split(st, result)
        if split is None:
            for lb in due:
                self._after_failed_read(st, lb, row, skew, read_at)
            return
        valid = {b.bucket_start_ms: b for b in split.valid}
        invalid = {b.bucket_start_ms: r for b, r in split.invalid}
        bad = {b.bucket_start_ms: b for b, _ in split.invalid}
        evidence, closed_after, _ = self._evidence(split)
        complete = not result.truncated
        trusts = self._trusts(st)
        commits: list[_Commit] = []
        verifies: list[_Commit] = []
        for start in sorted(st.live):
            lb = st.live[start]
            if not lo <= start <= hi or lb.deferred:   # a deferred bar is decided from the store, not re-read
                continue
            if lb.final_at is not None:                 # waiting for the verification re-read
                if start not in due_starts:
                    continue
                if start in valid:
                    verifies.append(_Commit(start, valid[start]))
                else:
                    self.counters.verify_inconclusive += 1
                del st.live[start]
                continue
            lb.reads += 1
            lb.failed = lb.empty = False
            if start in invalid:
                lb.invalid, lb.obs = True, None
                self._quarantine(st, bad[start], invalid[start].failed[0].value)
            elif start in valid:
                lb.invalid = False
                bar = valid[start]
                obs = _Obs(result.received_ms, bar)
                later = evidence is not None and evidence > start and self._trusts_next_bucket(st)
                agreed = lb.obs is not None and lb.obs.same(obs) and \
                    result.received_ms - lb.obs.at_ms >= self._cfg.settle_ms
                if trusts and bar.partial:          # the broker says forming: not final on any rule
                    later = agreed = False
                flag = trusts and not bar.partial
                if later or agreed or flag:
                    commits.append(_Commit(start, bar))
                    continue
                if lb.obs is None or not lb.obs.same(obs):
                    lb.obs = obs
            elif complete:
                lb.invalid, lb.empty, lb.obs = False, True, None
                if (closed_after is not None and closed_after > start) or self._calendar_closed(st):
                    self._no_trade(st, start)
                    del st.live[start]
                    continue
            else:
                lb.invalid = False                      # truncated: nothing learned about an empty bucket
            lb.next_at = next_read_ms(lb.end, read_at, row, skew, self._cfg.lag_ms)
        if not commits and not verifies:
            return
        stored = await self._stored_for(st, [c.start for c in commits + verifies])
        final_at = self._clock_ms()
        for c in commits:
            held = st.live.get(c.start)
            if held is None:
                continue
            if stored is None:
                self._defer(held, c.bar, result.received_ms)
                continue
            self._finalize(st, held, c, stored, final_at, row)
        for c in verifies:
            if stored is None:
                self.counters.verify_inconclusive += 1
                continue
            if self._commit(st, c, stored, final_at, live=True):
                self._note_verified(st, c.start)
            else:
                self.counters.verify_inconclusive += 1

    def _finalize(self, st: _Series, lb: _Live, c: _Commit, stored: Mapping[int, StoredBar], final_at: int,
                  row: TimingRow) -> None:
        """A bar final on broker evidence: stored (queued, or identical to the stored one) it is clean and waits for
        its verification re-read; not stored (unencodable, the writer refused it) it is deferred."""
        if not self._commit(st, c, stored, final_at, live=True):
            self._defer(lb, c.bar, final_at)
            return
        lb.deferred = False
        self._clean(st, c.start)
        st.coverage.pop(c.start, None)
        lb.final_at = final_at
        at = verification_read_ms(final_at, lb.cap_ms, row)
        if at is None:
            del st.live[c.start]
            self._note_unverified(st, c.start)
        else:
            lb.next_at = at
        if self._book.state(st.series) is Sm12State.BACKFILLING:
            self._book.note_complete(st.series, c.start, c.start)

    def _defer(self, lb: _Live, bar: BrokerBar, received_ms: int) -> None:
        """Final on broker evidence but not compared with (or not taken by) the store: decided again from the store
        every `DEFER_MS`, no broker re-read; at its cap the bucket goes to the lane uncounted (`_expire`)."""
        self.counters.deferred += 1
        lb.deferred = True
        lb.invalid = lb.failed = lb.empty = False
        lb.obs = _Obs(received_ms, bar)
        lb.next_at = self._clock_ms() + DEFER_MS

    async def _retry_deferred(self, st: _Series, row: TimingRow, now: int) -> None:
        due = [lb for lb in st.live.values()
               if lb.deferred and lb.obs is not None and lb.next_at is not None and lb.next_at <= now]
        if not due:
            return
        stored = await self._stored_for(st, [lb.start for lb in due])
        final_at = self._clock_ms()
        for lb in due:
            if st.live.get(lb.start) is not lb or lb.obs is None:
                continue
            if stored is None:
                self._defer(lb, lb.obs.bar, lb.obs.at_ms)
                continue
            self._finalize(st, lb, _Commit(lb.start, lb.obs.bar), stored, final_at, row)

    def _after_failed_read(self, st: _Series, lb: _Live, row: TimingRow, skew: SkewEstimate, read_at: int) -> None:
        if lb.final_at is not None:
            self.counters.verify_inconclusive += 1      # one verification re-read only
            st.live.pop(lb.start, None)
            return
        lb.reads += 1
        lb.failed = True
        lb.empty = False                                # an older empty answer is not "still empty" at the cap
        lb.next_at = next_read_ms(lb.end, read_at, row, skew, self._cfg.lag_ms)

    def _trusts_next_bucket(self, st: _Series) -> bool:
        """`T + 1` in the same answer makes bar `T` final only where PR3 showed no amendment after it (round 13)."""
        if self._trusts_next is None:
            return False
        try:
            return bool(self._trusts_next(st.series.adapter_id))
        except Exception:
            return False

    def _trusts(self, st: _Series) -> bool:
        if self._trusts_partial is None:
            return False
        try:
            return bool(self._trusts_partial(st.series.adapter_id))
        except Exception:
            return False

    def _no_trade(self, st: _Series, start: int) -> None:
        """Positive `no_trade` evidence: neither adds to nor resets the error unit."""
        self.counters.no_trade += 1
        st.coverage[start] = Coverage.NO_TRADE
        st.quarantined.discard(start)
        self._note("note_no_trade", st.series, start)

    # -------------------------------------------------------------------------------------------- store compare
    async def _stored_for(self, st: _Series, starts: Sequence[int]) -> Mapping[int, StoredBar] | None:
        """The stored hash and revision of each bucket. `md:bars` is the truth; this task's memory of what it queued
        stands in only for a bucket the store lacks while the writer still holds it queued (not flushed yet), or
        when no store port is wired. None: the store cannot be read now."""
        if not starts:
            return {}
        if self._stored is None:
            return {s: st.known[s] for s in starts if s in st.known}
        try:
            got = await self._stored.stored(st.series, min(starts), max(starts))
        except Exception:
            got = None
        if got is None:
            return None
        held = dict(got)
        for s in starts:
            mine = st.known.get(s)
            if mine is not None and s not in held and self._queued(st, s):
                held[s] = mine
        return held

    def _queued(self, st: _Series, start: int) -> bool:
        try:
            return bool(self._sink.is_queued(st.series, start))
        except Exception:
            return False

    def _commit(self, st: _Series, c: _Commit, stored: Mapping[int, StoredBar], final_at: int, *,
                live: bool = False) -> bool:
        """Queue a final bar unless the store already holds the identical one; a changed bar is `revision + 1`.
        True when the bucket is now stored (queued, or identical to the stored bar); False when it is not (the bar
        does not encode, or the writer refused it). A stored bar goes to the D6 cross-check (`live`: from the live
        schedule)."""
        series, b = st.series, c.bar
        end = series.spec.bucket_end_ms(c.start)
        prev = stored.get(c.start)
        try:
            if prev is not None and verify(self._keyring, prev.content_hash, series.canonical, end, b.open, b.high,
                                           b.low, b.close) is HashStatus.VALID:
                self.counters.bars_unchanged += 1
                self._cross(st, Bar(series=series, bucket_start_ms=c.start, open=b.open, high=b.high, low=b.low,
                                    close=b.close, quality=Quality.COMPLETE, revision=prev.revision,
                                    final_at_ms=final_at), live)
                return True
            digest = content_hash(self._keyring, series.canonical, end, b.open, b.high, b.low, b.close)
        except ValueError:
            self.counters.unencodable += 1
            return False
        revision = 0 if prev is None else min(prev.revision + 1, REVISION_MAX)
        bar = Bar(series=series, bucket_start_ms=c.start, open=b.open, high=b.high, low=b.low, close=b.close,
                  tick_count=0, quality=Quality.COMPLETE, revision=revision, final_at_ms=final_at)
        try:
            self._sink.queue_bar(bar)
        except Exception as e:
            log.warning("md-bars: queueing a bar of %s failed (%s)", series.canonical, safe_redis_error(e))
            return False
        self.counters.bars_queued += 1
        st.quarantined.discard(c.start)
        if prev is not None:
            self.counters.revisions += 1
        st.known[c.start] = StoredBar(digest, revision)
        if len(st.known) > KNOWN_KEEP:
            for old in sorted(st.known)[:len(st.known) - KNOWN_KEEP]:
                del st.known[old]
        self._cross(st, bar, live)
        return True

    # -------------------------------------------------------------------------------------------- backfill
    def _ensure_job(self, st: _Series, now: int) -> None:
        state = self._book.state(st.series)
        running = st.job is not None and not st.job.done
        if state in (Sm12State.EMPTY, Sm12State.GAPPED) or (state is Sm12State.BACKFILLING and not running):
            if not running:
                assert st.next_end is not None
                tf = st.series.spec.timeframe_ms
                live_starts = [s for s in st.live]
                hi = (min(live_starts) if live_starts else st.next_end - tf) - tf
                lo = hi - (st.need - 1) * tf
                st.job = _Job(lo, hi, hi, now)
                st.backfilled = False
            if state is not Sm12State.BACKFILLING:
                self._book.apply(st.series, Sm12Event.BACKFILL)
                job = st.job
                if job is not None and job.cursor < job.hi:      # a resumed job: its pages stay complete
                    self._book.note_complete(st.series, job.cursor + st.series.spec.timeframe_ms, job.hi)

    async def _backfill_page(self, st: _Series, now: int) -> None:
        job = st.job
        if job is None or job.done or now < job.next_at:
            return
        spec = st.series.spec
        tf = spec.timeframe_ms
        page_lo = max(job.lo, job.cursor - (self._cfg.page_bars - 1) * tf)
        result = await self._fetch(st, page_lo, job.cursor + tf)    # one bucket past the page: evidence only
        if not isinstance(result, PriceBarsResponse):
            self._job_retry(job, now)
            return
        split = self._split(st, result)
        if split is None:
            self._job_retry(job, now)
            return
        read_at = self._clock_ms()
        skew = self._skew_of(st)
        valid = {b.bucket_start_ms: b for b in split.valid}
        invalid = {b.bucket_start_ms: (b, r) for b, r in split.invalid}
        evidence, closed_after, closed_before = self._evidence(split)
        complete = not result.truncated
        commits: list[int] = []
        empties: list[int] = []
        blind: list[int] = []
        handoff: list[int] = []
        for start in range(page_lo, job.cursor + 1, tf):
            if start in st.live:
                continue                                # the live path owns it
            capped = decision_cap_ms(spec.bucket_end_ms(start), spec, skew) <= read_at
            if start in invalid:
                bad, res = invalid[start]
                self._quarantine(st, bad, res.failed[0].value)
                blind.append(start)
            elif start in valid:
                (commits if capped or (evidence is not None and evidence > start) else handoff).append(start)
            elif complete and self._reached(start, capped, closed_after, closed_before):
                empties.append(start)
            elif complete and not capped:
                handoff.append(start)
            else:
                blind.append(start)                     # truncated, or capped with no evidence history reaches it
        stored = await self._stored_for(st, commits)
        if stored is None:
            self.counters.deferred += 1
            self._job_retry(job, now)
            return
        final_at = self._clock_ms()
        for start in commits:
            if self._commit(st, _Commit(start, valid[start]), stored, final_at):
                st.coverage.pop(start, None)
                self._lane_resolved(st, start, Resolution.BAR)
            else:
                self._lane_add(st, start, now)
        for start in empties:
            self._no_trade(st, start)
            self._lane_resolved(st, start, Resolution.NO_TRADE)
        for start in blind:
            self._lane_add(st, start, now)
        for start in handoff:
            end = spec.bucket_end_ms(start)
            st.live[start] = _Live(start, end, decision_cap_ms(end, spec, skew), read_at)
        self.counters.backfill_pages += 1
        job.failures = 0
        if self._book.state(st.series) is Sm12State.BACKFILLING:
            self._book.note_complete(st.series, page_lo, job.hi)
        job.cursor = page_lo - tf
        job.next_at = now
        if job.cursor < job.lo:
            job.done = True
            st.backfilled = True
            self.counters.backfill_done += 1
            if self._book.state(st.series) is Sm12State.BACKFILLING:
                self._book.apply(st.series, Sm12Event.LIVE)
            if st.heal_cursor is None:
                st.next_heal_at = now + self._cfg.heal_every_ms

    def _job_retry(self, job: _Job, now: int) -> None:
        job.failures += 1
        job.next_at = now + min(BACKFILL_RETRY_MIN_MS * 2 ** (job.failures - 1), BACKFILL_RETRY_MAX_MS)

    # -------------------------------------------------------------------------------------------- lane
    def _lane_add(self, st: _Series, start: int, now: int, *, force: bool = False, request: int | None = None,
                  refetch: bool = False) -> bool:
        """A `blind` bucket into the lane (due at once). False when not added: already known `no_trade` /
        `unfillable` (unless `force`), beyond broker reach (`unfillable`), or the series' lane is full."""
        spec = st.series.spec
        if not spec.on_grid(start):
            return False
        if start in st.coverage:
            if not force:
                return False
            del st.coverage[start]
        entry = st.lane.get(start)
        if entry is None:
            if now - spec.bucket_end_ms(start) > self._cfg.history_reach_ms:
                st.coverage[start] = Coverage.UNFILLABLE
                self.counters.unfillable += 1
                return False
            if len(st.lane) >= self._cfg.lane_max_per_series:
                self.counters.lane_dropped += 1
                return False
            entry = st.lane[start] = _Lane(now)
            self.counters.lane_added += 1
        elif force:
            entry.next_at = min(entry.next_at, now)
        if request is not None:
            entry.request_ids.add(request)
        if refetch:
            entry.refetch = True
        return True

    async def _lane_pass(self, now: int, *, requests_only: bool = False) -> None:
        """Up to `lane_reads_per_pass` lane reads, one series each, round robin, after every live read.
        `requests_only` (ARM global FAILED): only the buckets an `md_backfill` request is waiting for."""
        ready = [st for st in sorted(self._series.values(), key=lambda s: s.series)
                 if st.unsupported is None and not self._paused(st)
                 and self._book.state(st.series) is not Sm12State.FAILED
                 and any(e.next_at <= now and (e.request_ids or not requests_only) for e in st.lane.values())]
        if not ready:
            return
        k = self._lane_turn % len(ready)
        picked = (ready[k:] + ready[:k])[:self._cfg.lane_reads_per_pass]
        self._lane_turn += len(picked)
        await asyncio.gather(*(self._lane_read(st, now, requests_only) for st in picked))

    async def _lane_read(self, st: _Series, now: int, requests_only: bool = False) -> None:
        try:
            await self._lane_read_inner(st, now, requests_only)
        except Exception as e:
            self.counters.series_errors += 1
            log.warning("md-bars: lane read for %s failed (%s)", st.series.canonical, safe_redis_error(e))

    async def _lane_read_inner(self, st: _Series, now: int, requests_only: bool = False) -> None:
        spec = st.series.spec
        tf = spec.timeframe_ms

        def wanted(s: int) -> bool:
            return not requests_only or bool(st.lane[s].request_ids)

        due = sorted(s for s, e in st.lane.items() if e.next_at <= now and wanted(s))
        newest = due[-1]
        lo = max(due[0], newest - (self._cfg.page_bars - 1) * tf)
        self.counters.lane_reads += 1
        result = await self._fetch(st, lo - tf, newest + tf)        # one bucket either side: evidence only
        in_range = [s for s in sorted(st.lane) if lo <= s <= newest and wanted(s)]
        if not isinstance(result, PriceBarsResponse):
            for s in in_range:
                self._lane_retry(st, s, now, miss=False)
            return
        split = self._split(st, result)
        if split is None:
            for s in in_range:
                self._lane_retry(st, s, now, miss=False)
            return
        read_at = self._clock_ms()
        skew = self._skew_of(st)
        valid = {b.bucket_start_ms: b for b in split.valid}
        invalid = {b.bucket_start_ms: (b, r) for b, r in split.invalid}
        evidence, closed_after, closed_before = self._evidence(split)
        complete = not result.truncated
        commits: list[int] = []
        for s in in_range:
            capped = decision_cap_ms(spec.bucket_end_ms(s), spec, skew) <= read_at
            if s in invalid:
                bad, res = invalid[s]
                self._quarantine(st, bad, res.failed[0].value)
                self._lane_retry(st, s, now, miss=True)
            elif s in valid and (capped or (evidence is not None and evidence > s)):
                commits.append(s)
            elif s not in valid and complete and self._reached(s, capped, closed_after, closed_before):
                self._no_trade(st, s)
                self._lane_resolved(st, s, Resolution.NO_TRADE)
            else:
                self._lane_retry(st, s, now, miss=s not in valid)
        if not commits:
            return
        stored = await self._stored_for(st, commits)
        if stored is None:
            self.counters.deferred += 1
            for s in commits:
                self._lane_retry(st, s, now, miss=False)
            return
        final_at = self._clock_ms()
        for s in commits:
            if self._commit(st, _Commit(s, valid[s]), stored, final_at):
                st.coverage.pop(s, None)
                self._lane_resolved(st, s, Resolution.BAR)
            else:
                self._lane_retry(st, s, now, miss=False)

    def _lane_retry(self, st: _Series, start: int, now: int, *, miss: bool) -> None:
        entry = st.lane.get(start)
        if entry is None:
            return
        entry.tries += 1
        if miss:
            entry.misses += 1
            if entry.misses >= self._cfg.lane_max_misses:
                self.counters.unfillable += 1
                st.coverage[start] = Coverage.UNFILLABLE
                self._lane_resolved(st, start, Resolution.UNFILLABLE)
                return
        entry.next_at = now + min(self._cfg.lane_retry_min_ms * 2 ** (entry.tries - 1), self._cfg.lane_retry_max_ms)

    def _lane_resolved(self, st: _Series, start: int, how: Resolution) -> None:
        entry = st.lane.pop(start, None)
        if entry is None:
            return
        if how is not Resolution.UNFILLABLE:
            self.counters.lane_resolved += 1
        elif entry.refetch:
            self._refetch_failed(st, start)
        for listener in self._listeners:
            try:
                listener(st.series, start, how)
            except Exception as e:   # a consumer's fault never stops md-bars
                self.counters.listener_errors += 1
                log.warning("md-bars: resolved listener failed for %s (%s)", st.series.canonical,
                            safe_redis_error(e))
        for req_id in sorted(entry.request_ids):
            req = self._requests.get(req_id)
            if req is None:
                continue
            req.remaining.discard(start)
            if how is Resolution.UNFILLABLE:
                req.unfillable += 1
            self._finish_request(req)

    def _finish_request(self, req: _Request) -> None:
        if req.remaining:
            return
        self._requests.pop(req.request_id, None)
        self._done(req.request_id, APPLIED, f"unfillable_{req.unfillable}" if req.unfillable else "resolved")

    def _done(self, request_id: int, outcome: str, reason: str) -> None:
        if self._complete is None:
            return
        try:
            self._complete(request_id, outcome, reason)
        except Exception as e:
            log.warning("md-bars: request %d completion failed (%s)", request_id, safe_redis_error(e))

    # -------------------------------------------------------------------------------------------- gap heal
    async def _heal_pass(self, now: int) -> None:
        """Walk the stored buckets of each LIVE series inside retention, oldest first, one chunk per series per
        `heal_every_ms`; every hole that is neither `no_trade` nor `unfillable` goes to the lane."""
        if self._stored is None:
            return
        for st in sorted(self._series.values(), key=lambda s: s.series):
            if not st.backfilled or st.unsupported is not None or now < st.next_heal_at or self._paused(st):
                continue
            if self._book.state(st.series) is not Sm12State.LIVE:
                continue
            st.next_heal_at = now + self._cfg.heal_every_ms
            try:
                await self._heal(st, now)
            except Exception as e:
                self.counters.series_errors += 1
                log.warning("md-bars: gap heal for %s failed (%s)", st.series.canonical, safe_redis_error(e))

    async def _heal(self, st: _Series, now: int) -> None:
        assert self._stored is not None
        span = await self._stored.span(st.series)
        if span is None:
            return
        spec = st.series.spec
        tf = spec.timeframe_ms
        first, last = span
        floor = max(first, spec.bucket_start_ms(now - BROKER_RETENTION_MS))
        cursor = st.heal_cursor if st.heal_cursor is not None and st.heal_cursor >= floor else floor
        if cursor > last:
            cursor = floor                              # one sweep done: start again from the oldest
        hi = min(cursor + (self._cfg.heal_chunk - 1) * tf, last)
        held = await self._stored.stored(st.series, cursor, hi)
        if held is None:
            return
        self.counters.heal_runs += 1
        for start in range(cursor, hi + 1, tf):
            if start in held or start in st.coverage or start in st.lane or start in st.live:
                continue
            if self._lane_add(st, start, now):
                self.counters.heal_holes += 1
        st.heal_cursor = hi + tf

    # -------------------------------------------------------------------------------------------- mirror, pages
    @staticmethod
    def _count(st: _Series, kind: Coverage) -> int:
        return sum(1 for v in st.coverage.values() if v is kind)

    def _mirror(self, st: _Series) -> None:
        """`md:meta` mirror of the coverage counts and SM12 state, sent only when changed."""
        pairs: tuple[tuple[str, str], ...] = (
            ("bars_k", str(st.invalid_streak)),
            ("bars_phase", self._phase(st).value),
            ("cov_blind", str(len(st.lane))),
            ("cov_no_trade", str(self._count(st, Coverage.NO_TRADE))),
            ("cov_unfillable", str(self._count(st, Coverage.UNFILLABLE))),
            ("sm12", self._book.state(st.series).value),
        )
        if self._crosscheck is not None:
            try:
                pairs += self._crosscheck.meta_pairs(st.series)
            except Exception as e:
                log.warning("md-bars: cross-check status of %s failed (%s)", st.series.canonical,
                            safe_redis_error(e))
        if pairs == st.meta_sent:
            return
        try:
            ok = self._sink.set_meta(st.series, pairs)
        except Exception:
            ok = False
        if ok:
            st.meta_sent = pairs

    def _page(self, severity: str, title: str, body: str) -> None:
        log.warning("md-bars: %s: %s", title, body)
        if self._pager is None:
            return
        try:
            self._pager.page(severity, title, body)
        except Exception as e:
            log.warning("md-bars: page failed (%s)", safe_redis_error(e))


def demanded(entries: Iterable[tuple[SeriesId, int]]) -> DemandSource:
    """A fixed demand source (tests and the S31 wiring before the S18 demand port)."""
    fixed = dict(entries)
    return lambda: fixed


__all__ = [
    "MdBars", "BrokerBarsSettings", "BarsCounters", "BrokerSeriesStatus", "BarSink", "BucketNotes", "StoredBarsPort",
    "BookPort",
    "StoreBarsReader", "StoredBar", "Coverage", "Resolution", "Phase", "demanded", "APPLIED", "REFUSED",
]
