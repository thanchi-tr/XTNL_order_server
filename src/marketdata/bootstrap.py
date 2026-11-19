"""The `redis-md` bootstrap, SM12, the reset flag and the `md-writer` service loop (design §A7, §A13, §B14 Clearing;
plan S16, R11-M09, R11-m).

**Bootstrap** (`MdBootstrap.bootstrap`, owned by `md-writer` alone and run inside its task, so no flush interleaves):

1. SET `md:writer` again when it is missing, at a term start and on `md_writer_reset` (a fresh token), each only after
   the leader check (lease held and `engine_epoch` this node's, `LeaderCheckPort`). Another writer's token is never
   replaced in-term: that is a fenced ex-leader (`no_writer`).
2. Take `md:boot_lock` (`BOOT_LOCK`, `SET NX PX 10000`), renewed every 2 s between the steps (`RENEW`).
3. Classify the store against **this node's** record (`NodeRecordPort`, `classify`), mark every series the writer
   knows (memory, `md:registry:series`, the stored-series index; never `KEYS` / `SCAN`) GAPPED from
   `min(last final bucket in md:meta, the writer's in-memory last final bucket)` (EMPTY when neither exists), clear
   their `md:forming:*`, and reconcile the rollback set both ways: store to memory for every hash-checked entry the
   process lost, memory to store for every entry the store lost (an entry whose `write_seq` the store holds with
   other content is left alone: `STALE_SEQ` would refuse the whole step).
4. Take the demand set synchronously from the in-process lease set (`LeaseSourcePort`) plus every live lease already
   in the store (the lease index, each checked with `EXISTS`), rewrite the registry entries, prune the lease-index
   entries whose `md:demand:*` key has expired, and re-assert the in-process leases (`BOOT_WRITE`, then `LEASE`,
   sequential).
5. `FINISH`: the `md_reset(generation, class)` event, then `md:boot` with the new generation (written last), then the
   lock released. The writer then resumes with the new sentinel (`MdWriter.install_boot`), every stored TICK series
   and tick stream is handed to `MdWriter.track_retention`, and this node's record is saved (best effort).

No broker I/O; meanwhile the rings and the closed-bar queue hold the data.

**Generation** `(epoch revision, n)`: the token's epoch revision and the number of bootstrap attempts in this term
(from 1), so it only rises. **Reset event:** every successful bootstrap emits one `MdReset` to the in-process listeners
(the ARM's notice source, S28) and `md_reset` on `md:events`. `notice` is True only for class `store_lost` /
`store_foreign`, or an in-term `run_id` change, `NO_BOOT` or `NO_WRITER`; a clear probe's trial bootstrap and the
bootstrap re-run after a global clear never send one, and the routine term-start bootstrap of an intact store sends
none. **Reset budget (D7):** 3 counted resets within one hour are the §B14 ARM-global fault (`on_fault("md_resets")`);
only in-term `NO_BOOT` / `NO_WRITER` / `run_id` resets count: never a term start, an operator request
(`md_writer_reset`), a trial bootstrap or the post-clear bootstrap. An accepted clear (`clear_budget`) resets the
counters and the one-hour window. 3 failed bootstraps in a row are the other ARM-global fault
(`on_fault("bootstrap_failed")`; a trial's failure, `not_leader`, `no_writer` and `lock_busy` never count). **Only a
store that answered counts** (plan S38e): a refused script, an inconsistent state or a restore error. A store that is
unreachable, times out, or replies `LOADING` / `BUSY` (`UNREACHABLE_FAILURES`) is retried with back-off (1 s doubling
to 30 s) and never counted; it falls under the §B14 `fail_md_down_s` time counter instead. Bootstraps run while
the ARM is globally FAILED: they repair the store and never advise (R11-M09).

**SM12** (per series, §A7): `EMPTY → BACKFILLING → LIVE`; `LIVE → GAPPED`; `GAPPED → BACKFILLING / LIVE`; any state
→ `FAILED` → `BACKFILLING` after an operator clear (`SM12_TRANSITIONS`, one pure table). `SeriesStateBook` holds the
in-process state the read port gates on, with the backfilled range of a BACKFILLING series; `md:meta` mirrors it
(`sm12`, `gap_from`) for `md show`.

**`MdWriterService`** is the `md-writer` task body around `MdWriter`: each step runs one writer cycle, then (in this
order) a requested trial bootstrap, a due bootstrap (term start, `NO_BOOT` / `NO_WRITER`, operator reset or post-clear,
with back-off after a failure), or one step of a queued work item (`md_writer_reset`, `md_rehash`, filed by the S32
handlers and never run in the operator loop), each completed as `applied`, `refused` or `moot`.

Nothing here logs, pages or formats a price: series ids, buckets, counts, classes and codes only.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Final, Protocol

from src.marketdata.codec import DecodedMember, decode_member
from src.marketdata.errors import safe_redis_error
from src.marketdata.hashing import HashStatus, Keyring
from src.marketdata.ports import NodeMdRecord, NodeRecordPort, StampPort
from src.marketdata.store import keys as K
from src.marketdata.store import scripts as S
from src.marketdata.store.reads import RollbackEntry, StoreReader
from src.marketdata.store.runner import ScriptFailed, ScriptOk, ScriptRefused, ScriptReply, StoreUnavailable
from src.marketdata.types import BarEndEvent, MdGeneration, SeriesId, Sm12State, Source
from src.marketdata.validate import check_stored
from src.marketdata.writer import MdWriter

log = logging.getLogger(__name__)

HOUR_MS: Final = 3_600_000
RESET_WINDOW_MS: Final = HOUR_MS
RESET_LIMIT: Final = 3
"""D7: 3 counted resets within one hour are the ARM-global fault (§B14)."""
BOOT_FAIL_LIMIT: Final = 3
"""3 failed bootstraps in a row make the store unrecoverable (§A7, §B14)."""
UNCOUNTED_FAILURES: Final = frozenset({"not_leader", "no_writer", "lock_busy"})
"""Failures that never count toward `BOOT_FAIL_LIMIT`: not this node's to repair, or a boot lock that expires on its
own (PX 10 s)."""
UNREACHABLE_FAILURES: Final = frozenset({"store_timeout", "store_connection", "timeout", "store_loading",
                                         "store_busy"})
"""`redis-md` unreachable, timing out, still loading its AOF (`LOADING`) or busy (`BUSY`): the store did not answer
the bootstrap, so the bootstrap itself did not fail (plan S38e). `store_error` is not one: a store that answered with
a permanent error (`NOPERM` from a wrong ACL, another script SHA, no usable `run_id`, an unknown `WAITAOF`) counts
toward `BOOT_FAIL_LIMIT` like a refused script. Never counted toward `BOOT_FAIL_LIMIT`; retried with
back-off (`unreachable_streak`); an unusable store is the §B14 `fail_md_down_s` time counter (900 s, only while an
armed market is OPEN), never an ARM-global FAILED within seconds of a start, a Docker restart or a PC wake."""
LEASE_PRUNE_MAX: Final = 256
"""Expired lease-index entries removed per bootstrap (the rest go on the next one)."""
INTACT_LAG_MS: Final = 2_000
"""`md:meta` at most 2 s behind this node's record is `intact` (the `everysec` AOF window, §A7)."""
LOCK_RENEW_MS: Final = 2_000
BOOT_BUDGET_S: Final = 4.0
"""One bootstrap attempt stays inside the 5 s `md-writer` stamp budget."""
RECORD_BUDGET_S: Final = 1.0
BACKOFF_MIN_MS: Final = 1_000
BACKOFF_MAX_MS: Final = 30_000
WORK_QUEUE_MAX: Final = 16

APPLIED: Final = "applied"
REFUSED: Final = "refused"
MOOT: Final = "moot"


# ---------------------------------------------------------------------------------------------------- values
class BootReason(str, Enum):
    TERM_START = "term_start"
    NO_BOOT = "no_boot"
    NO_WRITER = "no_writer"
    RUN_ID_CHANGED = "run_id_changed"
    OPERATOR_RESET = "md_writer_reset"
    TRIAL = "trial"
    POST_CLEAR = "post_clear"


COUNTED_REASONS: Final = frozenset({BootReason.NO_BOOT, BootReason.NO_WRITER, BootReason.RUN_ID_CHANGED})
"""In-term resets that count toward the D7 budget (§A13)."""
SILENT_REASONS: Final = frozenset({BootReason.TRIAL, BootReason.POST_CLEAR})
"""Bootstraps that never send an MD_RESET notice (§A13, §B14 Clearing)."""


class ResetClass(str, Enum):
    """§A7 classification against this node's record. `fresh`: no record and an empty store (a first start)."""

    INTACT = "intact"
    STORE_FOREIGN = "store_foreign"
    STORE_LOST = "store_lost"
    FRESH = "fresh"


@dataclass(frozen=True)
class MdReset:
    """The in-process `md_reset` event of one bootstrap (ids, class and flags only)."""

    generation: MdGeneration
    reset_class: ResetClass
    reason: BootReason
    notice: bool
    counted: bool


@dataclass(frozen=True)
class BootResult:
    ok: bool
    reason: BootReason
    generation: MdGeneration
    failure: str | None = None
    reset: MdReset | None = None
    series: int = 0
    restored_to_store: int = 0
    restored_to_memory: int = 0
    rejected_entries: int = 0


@dataclass(frozen=True)
class HeldLease:
    """One lease of the in-process lease set (arm-tracker, S26; the S18 demand book): `definition` is the registry
    value (the series id, or an indicator spec for `registry_key` = `md:registry:ind`)."""

    consumer: str
    spec_id: str
    value: str
    ttl_ms: int
    definition: str = ""
    registry_key: str = K.REGISTRY_SERIES


# ---------------------------------------------------------------------------------------------------- ports
class BootWritePort(Protocol):
    """The writer connection (`MdWriteClient`)."""

    async def install_token(self, token: K.WriterToken) -> None: ...

    async def current_run_id(self) -> str: ...

    async def run_one(self, call: S.ScriptCall) -> ScriptReply: ...

    async def run_sequential(self, calls: Sequence[S.ScriptCall]) -> list[ScriptReply]: ...


class LeaderCheckPort(Protocol):
    async def confirm_leader(self) -> bool:
        """The lease is held and `engine_epoch` in PostgreSQL is this node's (cluster id adoption included)."""
        ...


class LeaseSourcePort(Protocol):
    def held_leases(self) -> Sequence[HeldLease]:
        """The in-process lease set, read synchronously (empty at a fresh start)."""
        ...


FaultSink = Callable[[str], None]
ResetListener = Callable[[MdReset], None]


# ---------------------------------------------------------------------------------------------------- classification
def classify(record: NodeMdRecord | None, stored: K.BootSentinel | None, newest_write_ms: int | None, *, node: str,
             current_epoch: int, previous_term_epoch: int | None = None) -> ResetClass:
    """§A7: compare the store with **this node's** record. `newest_write_ms` is the newest `last_write_ms` in the
    `md:meta` of the known series (None: none). A newer sentinel this node wrote itself (its record save failed)
    counts as the record's."""
    if record is None:
        if stored is None:
            return ResetClass.FRESH if newest_write_ms is None else ResetClass.STORE_LOST
        return ResetClass.STORE_FOREIGN
    if stored is None:
        return ResetClass.STORE_LOST
    mine = K.BootSentinel.parse(record.sentinel)
    same = stored.text == record.sentinel or (
        mine is not None and stored.node == mine.node == node and stored.cluster_id == mine.cluster_id
        and stored.generation > mine.generation)
    if not same:
        if mine is not None and stored.generation < mine.generation:
            return ResetClass.STORE_LOST
        return ResetClass.STORE_FOREIGN
    if (newest_write_ms or 0) < record.last_write_ms - INTACT_LAG_MS:     # nothing written yet: nothing lost
        return ResetClass.STORE_LOST
    if mine is not None and mine.generation.epoch_rev not in (current_epoch, previous_term_epoch) \
            and previous_term_epoch is not None:
        return ResetClass.STORE_FOREIGN          # other nodes have led since this node last wrote
    return ResetClass.INTACT


def wants_notice(reason: BootReason, reset_class: ResetClass) -> bool:
    if reason in SILENT_REASONS:
        return False
    return reset_class in (ResetClass.STORE_LOST, ResetClass.STORE_FOREIGN) or reason in COUNTED_REASONS


# ---------------------------------------------------------------------------------------------------- reset budget
class ResetBudget:
    """D7: counted resets within the last hour; `clear()` is an accepted clear (counters and window reset)."""

    def __init__(self, *, limit: int = RESET_LIMIT, window_ms: int = RESET_WINDOW_MS) -> None:
        self._limit = limit
        self._window = window_ms
        self._times: deque[int] = deque()

    def count(self, at_ms: int) -> bool:
        """Count one reset; True when the budget is now exceeded (`limit` within the window)."""
        self._times.append(at_ms)
        self._trim(at_ms)
        return len(self._times) >= self._limit

    def in_window(self, now_ms: int) -> int:
        self._trim(now_ms)
        return len(self._times)

    def clear(self) -> None:
        self._times.clear()

    def _trim(self, now_ms: int) -> None:
        while self._times and self._times[0] <= now_ms - self._window:
            self._times.popleft()


# ---------------------------------------------------------------------------------------------------- SM12
class Sm12Event(str, Enum):
    BACKFILL = "backfill"           # md-bars starts BACKFILLING (each indicator's backfill_bars)
    LIVE = "live"                   # backfill complete (BROKER) or writes resumed (TICK)
    GAP = "gap"                     # engine down, subscription lost, suspension, outage, write errors
    FAIL = "fail"                   # §B14
    CLEAR = "clear"                 # an operator clear


_S = Sm12State
SM12_TRANSITIONS: Final[dict[tuple[Sm12State, Sm12Event], Sm12State]] = {
    (_S.EMPTY, Sm12Event.BACKFILL): _S.BACKFILLING,
    (_S.GAPPED, Sm12Event.BACKFILL): _S.BACKFILLING,
    (_S.BACKFILLING, Sm12Event.LIVE): _S.LIVE,
    (_S.GAPPED, Sm12Event.LIVE): _S.LIVE,
    (_S.EMPTY, Sm12Event.LIVE): _S.LIVE,
    (_S.LIVE, Sm12Event.GAP): _S.GAPPED,
    (_S.BACKFILLING, Sm12Event.GAP): _S.GAPPED,
    (_S.EMPTY, Sm12Event.FAIL): _S.FAILED,
    (_S.BACKFILLING, Sm12Event.FAIL): _S.FAILED,
    (_S.LIVE, Sm12Event.FAIL): _S.FAILED,
    (_S.GAPPED, Sm12Event.FAIL): _S.FAILED,
    (_S.FAILED, Sm12Event.CLEAR): _S.BACKFILLING,
}
"""SM12 (§A7) as one table; a pair not listed is refused (the state stays). The bootstrap itself sets GAPPED / EMPTY
for every non-FAILED series (`SeriesStateBook.mark_bootstrapped`)."""


def sm12_next(state: Sm12State, event: Sm12Event) -> Sm12State | None:
    return SM12_TRANSITIONS.get((state, event))


@dataclass
class SeriesState:
    state: Sm12State = Sm12State.EMPTY
    gap_from_ms: int | None = None
    gaps: int = 0
    complete_from_ms: int | None = None
    complete_to_ms: int | None = None


class SeriesStateBook:
    """The in-process SM12 state per series (the read port's gate)."""

    def __init__(self) -> None:
        self._states: dict[SeriesId, SeriesState] = {}

    def state(self, series: SeriesId) -> Sm12State:
        entry = self._states.get(series)
        return Sm12State.EMPTY if entry is None else entry.state

    def entry(self, series: SeriesId) -> SeriesState:
        return self._states.setdefault(series, SeriesState())

    def series(self) -> tuple[SeriesId, ...]:
        return tuple(sorted(self._states))

    def apply(self, series: SeriesId, event: Sm12Event) -> bool:
        """Apply one SM12 event; False when the table refuses it."""
        entry = self.entry(series)
        nxt = sm12_next(entry.state, event)
        if nxt is None:
            return False
        if nxt is Sm12State.GAPPED:
            entry.gaps += 1
        if nxt is not Sm12State.BACKFILLING:
            entry.complete_from_ms = entry.complete_to_ms = None
        entry.state = nxt
        return True

    def mark_bootstrapped(self, series: SeriesId, gap_from_ms: int | None) -> Sm12State:
        """Bootstrap step 3: GAPPED from `gap_from_ms` (EMPTY when the series holds nothing); FAILED stays FAILED."""
        entry = self.entry(series)
        if entry.state is Sm12State.FAILED:
            return entry.state
        new = Sm12State.EMPTY if gap_from_ms is None else Sm12State.GAPPED
        if new is Sm12State.GAPPED and entry.state is not Sm12State.GAPPED:
            entry.gaps += 1
        entry.state, entry.gap_from_ms = new, gap_from_ms
        entry.complete_from_ms = entry.complete_to_ms = None
        return new

    def note_complete(self, series: SeriesId, from_ms: int, to_ms: int) -> None:
        """md-bars (S17): buckets `[from, to]` of a BACKFILLING series are backfilled (newest first, so ranges grow)."""
        entry = self.entry(series)
        if entry.state is not Sm12State.BACKFILLING:
            return
        lo = from_ms if entry.complete_from_ms is None else min(from_ms, entry.complete_from_ms)
        hi = to_ms if entry.complete_to_ms is None else max(to_ms, entry.complete_to_ms)
        entry.complete_from_ms, entry.complete_to_ms = lo, hi

    def gaps(self, series: SeriesId) -> int:
        entry = self._states.get(series)
        return 0 if entry is None else entry.gaps

    def readable(self, series: SeriesId, from_ms: int | None, to_ms: int | None) -> bool:
        """Decision reads: LIVE, or BACKFILLING with `[from, to]` inside the backfilled range."""
        entry = self._states.get(series)
        if entry is None:
            return False
        if entry.state is Sm12State.LIVE:
            return True
        if entry.state is not Sm12State.BACKFILLING or from_ms is None or to_ms is None:
            return False
        lo, hi = entry.complete_from_ms, entry.complete_to_ms
        return lo is not None and hi is not None and lo <= from_ms and to_ms <= hi


# ---------------------------------------------------------------------------------------------------- bootstrap
class BootFailure(Exception):
    """One bootstrap step refused or failed; `code` is a script code or a short reason (never a value)."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


_LOCK_WORD: Final = re.compile(r"[^A-Za-z0-9_]")


def _digits(meta: dict[str, str], name: str) -> int | None:
    value = meta.get(name, "")
    return int(value) if value.isascii() and value.isdigit() and len(value) <= 13 else None


def _mono_ms() -> int:
    return time.monotonic_ns() // 1_000_000


class MdBootstrap:
    """Bootstrap steps 1-5, the generation, the reset event and budget, and the failure streak."""

    def __init__(self, writer: MdWriter, client: BootWritePort, reader: StoreReader, *, keyring: Keyring,
                 node: str, leader: LeaderCheckPort, book: SeriesStateBook | None = None,
                 records: NodeRecordPort | None = None, leases: LeaseSourcePort | None = None,
                 on_fault: FaultSink | None = None, previous_term_epoch: int | None = None,
                 clock_ms: Callable[[], int] | None = None, mono_ms: Callable[[], int] = _mono_ms,
                 budget_s: float = BOOT_BUDGET_S, budget: ResetBudget | None = None) -> None:
        self._writer = writer
        self._client = client
        self._reader = reader
        self._keyring = keyring
        self._node = node
        self._leader = leader
        self.book = book or SeriesStateBook()
        self._records = records
        self._leases = leases
        self._on_fault = on_fault
        self._previous_term_epoch = previous_term_epoch
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._mono_ms = mono_ms
        self._budget_s = budget_s
        self.budget = budget or ResetBudget()
        self._n = 0
        self._started = False
        self._fail_streak = 0
        self._unreachable_streak = 0
        self._streak_tripped = False
        self._budget_tripped = False
        self._listeners: list[ResetListener] = []
        self._last_sentinel: K.BootSentinel | None = writer.boot
        self._renewed_at = 0
        self.last_result: BootResult | None = None

    # -------------------------------------------------------------------------------------------- control
    def on_reset(self, listener: ResetListener) -> None:
        self._listeners.append(listener)

    @property
    def generation(self) -> MdGeneration | None:
        return None if self._last_sentinel is None else self._last_sentinel.generation

    @property
    def term_started(self) -> bool:
        """True once a bootstrap of this term has succeeded (later `start` requests are in-term)."""
        return self._started

    @property
    def fail_streak(self) -> int:
        return self._fail_streak

    @property
    def unreachable_streak(self) -> int:
        """Bootstraps in a row that found `redis-md` unreachable or still loading (back-off only, never a fault)."""
        return self._unreachable_streak

    def clear_budget(self) -> None:
        """An accepted clear (§B14 Clearing): the reset counters and the one-hour window start again."""
        self.budget.clear()
        self._budget_tripped = False
        self._fail_streak = 0
        self._streak_tripped = False

    def node_record(self) -> NodeMdRecord | None:
        """This node's current record (S32's `arm_status` writer saves it every 5 s): the sentinel it wrote and the
        newest `last_write_ms` its writer wrote."""
        if self._last_sentinel is None:
            return None
        return NodeMdRecord(self._node, self._last_sentinel.text, self._writer.last_write_ms)

    async def save_record(self) -> bool:
        record = self.node_record()
        if record is None or self._records is None:
            return False
        try:
            async with asyncio.timeout(RECORD_BUDGET_S):
                await self._records.save_record(record)
        except Exception as e:   # PostgreSQL down or slow: the next save carries the newer record
            log.warning("md bootstrap: node record not saved (%s)", safe_redis_error(e))
            return False
        return True

    # -------------------------------------------------------------------------------------------- run
    async def bootstrap(self, reason: BootReason, *, fresh_token: K.WriterToken | None = None) -> BootResult:
        """One attempt. Never raises: a refusal or a store error is a failed `BootResult`."""
        token = fresh_token or self._writer.token
        self._n += 1
        generation = MdGeneration(token.epoch_rev, self._n)
        # stable per node and term (no attempt number): a lock left by this writer's previous attempt (its release
        # timed out) is re-taken by BOOT_LOCK's re-entry path instead of reading `lock_busy` until it expires
        lock_value = _LOCK_WORD.sub("_", f"boot_{self._node}_{token.term_nonce}_{generation.epoch_rev}")
        result: BootResult
        try:
            async with asyncio.timeout(self._budget_s):
                result = await self._steps(reason, token, generation, lock_value, fresh_token is not None)
        except BootFailure as e:
            result = BootResult(False, reason, generation, e.code)
        except StoreUnavailable as e:
            result = BootResult(False, reason, generation, self._unavailable_code(e))
        except TimeoutError:
            result = BootResult(False, reason, generation, "timeout")
        except ValueError:                       # a call the builders refuse (an unusable id): nothing was sent
            result = BootResult(False, reason, generation, "build_error")
        except Exception as e:                   # a port's own fault (lease source, writer): never kills md-writer
            log.warning("md bootstrap: internal error (%s)", safe_redis_error(e))
            result = BootResult(False, reason, generation, "internal")
        if not result.ok:
            await self._release_quietly(lock_value)
        self._account(result)
        self.last_result = result
        return result

    async def _steps(self, reason: BootReason, token: K.WriterToken, generation: MdGeneration, lock_value: str,
                     fresh: bool) -> BootResult:
        run_id = await self._client.current_run_id()
        if reason is BootReason.NO_BOOT and self._last_sentinel is not None and self._last_sentinel.run_id != run_id:
            reason = BootReason.RUN_ID_CHANGED
        # (1) the token
        current = await self._reader.writer_token()
        if current is None or reason in (BootReason.TERM_START, BootReason.OPERATOR_RESET) or fresh:
            if not await self._leader.confirm_leader():
                raise BootFailure("not_leader")
            await self._client.install_token(token)
            if fresh:
                self._writer.use_token(token)
        elif current != token.text:
            raise BootFailure("no_writer")
        # (2) the boot lock
        reply = await self._client.run_one(S.boot_lock_call(token, lock_value))
        if not isinstance(reply, ScriptOk):
            raise BootFailure(self._code(reply))
        if int(reply.value) != 1:
            raise BootFailure("lock_busy")
        self._renewed_at = self._mono_ms()
        # (3) classify, mark GAPPED, clear forming, reconcile the rollback set
        stored = await self._reader.boot()
        registry = await self._reader.registry_series()
        index = await self._reader.stored_index()
        series, markets = self._known(registry, index)
        metas = await self._reader.metas(sorted(series))
        writes = [_digits(m, "last_write_ms") for m in metas.values()]
        newest = max((w for w in writes if w is not None), default=None)
        record = await self._load_record()
        reset_class = classify(record, stored, newest, node=self._node, current_epoch=token.epoch_rev,
                               previous_term_epoch=self._previous_term_epoch)
        boots: list[S.SeriesBoot] = []
        to_store = to_memory = rejected = 0
        now = self._clock_ms()
        for s in sorted(series):
            meta = metas.get(s, {})
            entries, restored, bad = await self._reconcile(s, now)
            to_store += len(entries)
            to_memory += restored
            rejected += bad
            last = [b for b in (_digits(meta, "last_bucket"), self._writer.last_final_bucket(s)) if b is not None]
            gap_from = min(last) + s.spec.timeframe_ms if last else None
            state = self.book.mark_bootstrapped(s, gap_from)
            pairs: list[tuple[str, str]] = [("sm12", state.value)]
            if gap_from is not None:
                pairs.append(("gap_from", str(gap_from)))
            boots.append(S.SeriesBoot(s, clear_forming=True, meta=tuple(pairs), entries=tuple(entries)))
        # (4) demand: the in-process lease set plus every live lease in the store
        held = list(self._leases.held_leases()) if self._leases is not None else []
        live, expired = await self._reader.lease_index()
        pairs_reg: dict[str, str] = {}
        for _, spec_id, _ in live:
            if spec_id in registry:
                pairs_reg[spec_id] = registry[spec_id]
        for lease in held:
            if lease.definition and lease.registry_key == K.REGISTRY_SERIES:
                pairs_reg[lease.spec_id] = lease.definition
        calls = [S.boot_write_call(token, lock_value, boots, rollback_n=self._writer.rollback_n,
                                   registry=sorted(pairs_reg.items()), prune=expired[:LEASE_PRUNE_MAX])]
        for h in held:
            if h.ttl_ms <= 0:
                continue
            try:
                calls.append(S.lease_call(token, h.consumer, h.spec_id, h.ttl_ms, h.value,
                                          registry_key=h.registry_key,
                                          registry_field=h.spec_id if h.definition else "",
                                          registry_value=h.definition))
            except ValueError:
                log.warning("md bootstrap: a held lease with an unusable id was skipped")
        await self._renew(token, lock_value)
        replies = await self._client.run_sequential(calls)
        if len(replies) != len(calls) or not all(isinstance(r, ScriptOk) for r in replies):
            first = next((r for r in replies if not isinstance(r, ScriptOk)), None)
            raise BootFailure("no_reply" if first is None else self._code(first))
        # (5) md:boot last
        await self._renew(token, lock_value)
        sentinel = K.BootSentinel(run_id, token.cluster_id, self._node, generation)
        reply = await self._client.run_one(S.finish_call(token, lock_value, sentinel, reset_class.value))
        if not isinstance(reply, ScriptOk):
            raise BootFailure(self._code(reply))
        self._writer.install_boot(sentinel)
        self._last_sentinel = sentinel
        self._writer.track_retention(series=series, markets=markets)
        notice = wants_notice(reason, reset_class)
        counted = reason in COUNTED_REASONS
        reset = MdReset(generation, reset_class, reason, notice, counted)
        return BootResult(True, reason, generation, None, reset, len(series), to_store, to_memory, rejected)

    def _known(self, registry: dict[str, str],
               index: dict[str, str]) -> tuple[set[SeriesId], set[tuple[str, str]]]:
        series: set[SeriesId] = set(self._writer.known_series())
        markets: set[tuple[str, str]] = set(self._writer.known_markets())
        for text in (*registry.values(), *index):
            parts = text.split(":")
            if len(parts) == 2 and text in index:
                markets.add((parts[0], parts[1]))
                continue
            try:
                series.add(SeriesId.parse(text))
            except ValueError:
                continue
        markets.update((s.adapter_id, s.symbol) for s in series if s.source is Source.TICK)
        return series, markets

    def _entry_ok(self, series: SeriesId, entry: RollbackEntry, now_ms: int) -> bool:
        """A store rollback entry adopted into memory only when it decodes, names its own bar end and its hash
        verifies under a retained key id."""
        decoded = decode_member(series, entry.member)
        if not isinstance(decoded, DecodedMember) or decoded.bar.bucket_end_ms != entry.bar_end_ms:
            return False
        checked = check_stored(decoded.bar, decoded.content_hash, self._keyring, now_ms=now_ms)
        return checked.result.ok and checked.hash_status is HashStatus.VALID

    async def _reconcile(self, series: SeriesId, now_ms: int) -> tuple[list[tuple[int, str]], int, int]:
        """Both directions of §A7 step 3 for one series: returns the memory entries to write to the store, the count
        restored to memory and the count of store entries refused by the hash check."""
        stored = await self._reader.rollback(series)
        seqs = await self._reader.rollback_seqs(series)
        good = [e for e in stored if self._entry_ok(series, e, now_ms)]
        memory = self._writer.rollback_memory(series)
        known = {e.write_seq for e in memory}
        restored = sum(1 for e in good if e.write_seq not in known)
        self._writer.restore_rollback(series, good)
        held = set(seqs)        # every stored score, a member that does not parse included (else STALE_SEQ)
        entries = [(e.write_seq, e.stored) for e in memory if e.write_seq not in held]
        return entries, restored, max(len(seqs) - len(good), 0)

    async def _renew(self, token: K.WriterToken, lock_value: str) -> None:
        if self._mono_ms() - self._renewed_at < LOCK_RENEW_MS:
            return
        reply = await self._client.run_one(S.renew_call(token, lock_value))
        if not isinstance(reply, ScriptOk):
            raise BootFailure(self._code(reply))
        self._renewed_at = self._mono_ms()

    async def _release_quietly(self, lock_value: str) -> None:
        try:
            async with asyncio.timeout(0.5):
                await self._client.run_one(S.release_call(lock_value))
        except (StoreUnavailable, TimeoutError):
            pass                                 # the lock expires on its own (PX 10 s)

    async def _load_record(self) -> NodeMdRecord | None:
        """The stored record, or this process's own when that is fresher: the stored one is saved at bootstrap
        time (its `last_write_ms` from before the term's flushes) and only every 5 s later (S32), so an older AOF
        in-term would otherwise read `intact`."""
        stored: NodeMdRecord | None = None
        if self._records is not None:
            try:
                async with asyncio.timeout(RECORD_BUDGET_S):
                    stored = await self._records.load_record()
            except Exception as e:   # unreadable record: classified as without one (info only, §A7)
                log.warning("md bootstrap: node record unreadable (%s)", safe_redis_error(e))
                stored = None
        mine = self.node_record()
        if mine is not None and (stored is None or (stored.sentinel == mine.sentinel
                                                    and mine.last_write_ms > stored.last_write_ms)):
            return mine
        return stored

    @staticmethod
    def _unavailable_code(exc: StoreUnavailable) -> str:
        """`store_<kind>`, except an error reply naming `LOADING` / `BUSY` (the store is not ready yet: uncounted)."""
        if exc.kind == "error":
            words = set(re.findall(r"[A-Z]+", exc.detail))
            if "LOADING" in words or "BusyLoadingError" in exc.detail:
                return "store_loading"
            if "BUSY" in words:
                return "store_busy"
        return f"store_{exc.kind}"

    @staticmethod
    def _code(reply: ScriptReply) -> str:
        if isinstance(reply, ScriptRefused):
            return reply.code.lower()
        if isinstance(reply, ScriptFailed):
            words = set(re.findall(r"[A-Z]+", reply.detail))
            if "LOADING" in words or "BusyLoadingError" in reply.detail:
                return "store_loading"      # the AOF is still loading: unreachable, never a counted failure
            if "BUSY" in words:
                return "store_busy"
        return "script_failed"

    def _account(self, result: BootResult) -> None:
        now = self._clock_ms()
        if result.ok and result.reset is not None:
            self._started = True
            self._fail_streak = 0
            self._unreachable_streak = 0
            self._streak_tripped = False
            reset = result.reset
            log.info("md bootstrap: generation %s (%s, %s)", reset.generation, reset.reset_class.value,
                     reset.reason.value)
            if reset.counted and self.budget.count(now) and not self._budget_tripped:
                self._budget_tripped = True
                self._fault("md_resets")
            for listener in self._listeners:
                try:
                    listener(reset)
                except Exception as e:   # a consumer's fault never stops the writer
                    log.warning("md bootstrap: reset listener failed (%s)", safe_redis_error(e))
            return
        log.warning("md bootstrap failed (%s, %s)", result.reason.value, result.failure)
        if result.reason is BootReason.TRIAL or result.failure in UNCOUNTED_FAILURES:
            return
        if result.failure in UNREACHABLE_FAILURES:
            self._unreachable_streak += 1    # the store did not answer: back-off, the md_down time counter (S38e)
            return
        self._fail_streak += 1
        if self._fail_streak >= BOOT_FAIL_LIMIT and not self._streak_tripped:
            self._streak_tripped = True
            self._fault("bootstrap_failed")

    def _fault(self, reason: str) -> None:
        log.warning("md bootstrap: store unrecoverable (%s)", reason)
        if self._on_fault is None:
            return
        try:
            self._on_fault(reason)
        except Exception as e:
            log.warning("md bootstrap: fault sink failed (%s)", safe_redis_error(e))


# ---------------------------------------------------------------------------------------------------- service
class WorkKind(str, Enum):
    MD_WRITER_RESET = "md_writer_reset"
    MD_REHASH = "md_rehash"


@dataclass(frozen=True)
class WorkItem:
    """A request work item filed by an S32 handler: `request_id` is the `operator_requests` id."""

    kind: WorkKind
    request_id: int
    fresh_token: K.WriterToken | None = None


Completion = Callable[[int, str, str], None]
"""`complete(request_id, outcome, reason)`: outcome `applied`, `refused` or `moot`; reason a short code."""


class RehashPort(Protocol):
    """The re-hash runner (`src/marketdata/rehash.py`)."""

    def needed(self) -> bool: ...

    @property
    def active(self) -> bool: ...

    def start(self) -> None: ...

    async def step(self) -> str | None:
        """One chunk; None while running, else `applied` or `refused:<code>`."""
        ...


@dataclass
class ServiceCounters:
    bootstraps: int = 0
    bootstrap_failures: int = 0
    trials: int = 0
    work_items: int = 0
    work_refused: int = 0
    completion_errors: int = 0


class MdWriterService:
    """The `md-writer` task body: writer cycles, bootstraps and request work items, one at a time."""

    def __init__(self, writer: MdWriter, bootstrap: MdBootstrap, *, rehash: RehashPort | None = None,
                 complete: Completion | None = None, mono_ms: Callable[[], int] = _mono_ms,
                 interval_s: float = 0.25) -> None:
        self._writer = writer
        self.bootstrap = bootstrap
        self._rehash = rehash
        self._complete = complete
        self._mono_ms = mono_ms
        self._interval_s = interval_s
        self._reason: BootReason | None = None if writer.boot is not None else BootReason.TERM_START
        self._fresh_token: K.WriterToken | None = None
        self._next_boot_at = 0
        self._resets: list[WorkItem] = []
        self._work: deque[WorkItem] = deque()
        self._rehash_item: WorkItem | None = None
        self._trials: list[Callable[[BootResult], None]] = []
        self.counters = ServiceCounters()
        writer.on_released(self._released)

    # -------------------------------------------------------------------------------------------- requests
    @property
    def pending_reason(self) -> BootReason | None:
        return self._reason

    def submit(self, item: WorkItem) -> bool:
        """Enqueue a work item (non-blocking; the S32 handler then answers `Accepted`). False when the queue is
        full (the handler answers refused)."""
        if len(self._work) >= WORK_QUEUE_MAX:
            return False
        self._work.append(item)
        return True

    def request_trial(self, done: Callable[[BootResult], None]) -> None:
        """The ARM-scope clear probe's trial bootstrap (§B14 Clearing): run on the next step, under the writer
        token and boot lock, allowed while globally FAILED; never counted, never a notice."""
        self._trials.append(done)

    def request_post_clear(self) -> None:
        """After an accepted global clear: re-run the bootstrap (not counted, no notice) and reset the budget."""
        self.bootstrap.clear_budget()
        self._reason = BootReason.POST_CLEAR
        self._next_boot_at = 0

    # -------------------------------------------------------------------------------------------- loop
    async def run(self, stop: asyncio.Event, stamp: StampPort) -> None:
        while not stop.is_set():
            stamp.stamp()
            await self.step(stop, stamp)
            stamp.stamp()
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval_s)
            except TimeoutError:
                pass

    async def step(self, stop: asyncio.Event | None = None, stamp: StampPort | None = None) -> None:
        """One writer cycle (its own 4 s deadline), a stamp, then at most one bootstrap, trial or work-item step
        (each inside its own budget under the 5 s stamp budget)."""
        await self._writer.cycle(stop)
        if stop is not None and stop.is_set():
            return
        if stamp is not None:
            stamp.stamp()
        self._sync_book()
        if self._trials:
            await self._trial(stamp)
        elif self._boot_due():
            await self._boot(stamp)
        elif self._work or self._rehash_item is not None:
            await self._work_step()

    def _sync_book(self) -> None:
        for s in self._writer.gapped:
            self.bootstrap.book.apply(s, Sm12Event.GAP)
        wanted = self._writer.bootstrap_wanted
        if wanted is not None and self._reason is None:
            if wanted == "no_writer":
                self._reason = BootReason.NO_WRITER
            elif wanted == "no_boot" or self.bootstrap.term_started:
                self._reason = BootReason.NO_BOOT
            else:
                self._reason = BootReason.TERM_START

    def _released(self, event: BarEndEvent) -> None:
        if event.series.source is Source.TICK:
            self.bootstrap.book.apply(event.series, Sm12Event.LIVE)   # TICK writes resumed (TICK gaps stay blind)

    def _boot_due(self) -> bool:
        return self._reason is not None and self._mono_ms() >= self._next_boot_at

    async def _boot(self, stamp: StampPort | None = None) -> None:
        reason = self._reason
        if reason is None:
            return
        fresh, self._fresh_token = self._fresh_token, None
        self.counters.bootstraps += 1
        result = await self.bootstrap.bootstrap(reason, fresh_token=fresh)
        if stamp is not None:
            stamp.stamp()                        # up to 4 s spent: stamp before the record save (up to 1 s)
        resets, self._resets = self._resets, []
        if result.ok:
            self._reason = None
            self._next_boot_at = 0
            for item in resets:
                self._done(item, APPLIED, result.reset.reset_class.value if result.reset else "")
            await self.bootstrap.save_record()
            return
        self.counters.bootstrap_failures += 1
        for item in resets:
            self._done(item, REFUSED, result.failure or "failed")
        if reason is BootReason.OPERATOR_RESET and self._writer.boot is not None:
            self._reason = None                  # the old sentinel still holds: nothing to retry
            return
        streak = max(self.bootstrap.fail_streak, self.bootstrap.unreachable_streak, 1)
        delay = min(BACKOFF_MIN_MS * 2 ** (streak - 1), BACKOFF_MAX_MS)
        self._next_boot_at = self._mono_ms() + delay

    async def _trial(self, stamp: StampPort | None = None) -> None:
        dones, self._trials = self._trials, []
        self.counters.trials += 1
        result = await self.bootstrap.bootstrap(BootReason.TRIAL)
        if stamp is not None:
            stamp.stamp()
        if result.ok and self._reason in (BootReason.NO_BOOT, BootReason.NO_WRITER, BootReason.TERM_START):
            self._reason = None                  # the trial wrote md:boot: the writer resumed with it
        for done in dones:
            try:
                done(result)
            except Exception as e:
                log.warning("md-writer: trial callback failed (%s)", safe_redis_error(e))

    async def _work_step(self) -> None:
        """Take the next queued item (a second `md_rehash` while one runs is moot at once), then run one re-hash
        page when one is active."""
        if self._work:
            self._take(self._work.popleft())
        if self._rehash_item is not None:
            await self._rehash_step()

    def _take(self, item: WorkItem) -> None:
        self.counters.work_items += 1
        if item.kind is WorkKind.MD_WRITER_RESET:
            self._resets.append(item)
            self._fresh_token = item.fresh_token or self._fresh_token
            self._reason = BootReason.OPERATOR_RESET
            self._next_boot_at = 0
            return
        if self._rehash is None or not self._rehash.needed():
            self._done(item, MOOT, "no_retained_key")
            return
        if self._rehash.active:
            self._done(item, MOOT, "already_running")
            return
        self._rehash.start()
        self._rehash_item = item

    async def _rehash_step(self) -> None:
        item, rehash = self._rehash_item, self._rehash
        if item is None or rehash is None:
            return
        if self._writer.boot is None:
            return                               # wait for the bootstrap: the re-hash writes through FLUSH
        outcome = await rehash.step()
        if outcome is None:
            return
        self._rehash_item = None
        if outcome == APPLIED:
            self._done(item, APPLIED, "rehash_complete")
        else:
            self._done(item, REFUSED, outcome.partition(":")[2] or "failed")

    def _done(self, item: WorkItem, outcome: str, reason: str) -> None:
        if outcome == REFUSED:
            self.counters.work_refused += 1
        if self._complete is None:
            return
        try:
            self._complete(item.request_id, outcome, reason)
        except Exception as e:
            self.counters.completion_errors += 1
            log.warning("md-writer: request %d completion failed (%s)", item.request_id, safe_redis_error(e))


__all__ = [
    "MdBootstrap", "MdWriterService", "SeriesStateBook", "ResetBudget", "BootReason", "ResetClass", "MdReset",
    "BootResult", "HeldLease", "WorkItem", "WorkKind", "Sm12Event", "SM12_TRANSITIONS", "classify", "sm12_next",
    "wants_notice",
]
