"""Bootstrap, SM12, the reset flag and budget, the node-record classification and the `md-writer` service loop
(plan S16; design §A7, §A13, §B14 Clearing). fakeredis + lupa; synthetic instrument `mock:SYN1`, synthetic prices,
no price in an assertion message."""
from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

import pytest

from src.marketdata.bootstrap import (
    BOOT_FAIL_LIMIT,
    UNREACHABLE_FAILURES,
    BootReason,
    HeldLease,
    ResetBudget,
    ResetClass,
    SeriesStateBook,
    Sm12Event,
    WorkItem,
    WorkKind,
    classify,
    sm12_next,
    wants_notice,
)
from src.marketdata.codec import encode_member
from src.marketdata.events import events_after
from src.marketdata.ports import BarsRead, EventsRead, NodeMdRecord
from src.marketdata.store import keys as K
from src.marketdata.store import scripts as S
from src.marketdata.store.reads import RollbackEntry, StoreReader
from src.marketdata.store.runner import (
    ScriptFailed,
    ScriptOk,
    ScriptRefused,
    ScriptReply,
    ScriptRunner,
    StoreUnavailable,
)
from src.marketdata.types import (
    M1,
    M15,
    CursorExpired,
    EventCursor,
    MdGeneration,
    SeriesId,
    Side,
    Sm12State,
    Source,
)

from .bootrig import Leases, Records, Rig, make_rig, restore, snapshot
from .conftest import KEYRING, RUN_ID, SERIES, T0, TOKEN, RunIds, bar, hash_of

Q = 900_000
TICK_1M = SeriesId("mock", "SYN1", M1, Side.BID, Source.TICK)
OTHER_TICK = SeriesId("mock", "SYN2", M1, Side.BID, Source.TICK)


def end_of(k: int) -> int:
    """The engine time just after bucket k (from T0) has closed (+60 s: inside the hash check's future rule)."""
    return T0 + (k + 1) * Q + 60_000


async def started(md_redis: Any, run_ids: RunIds, **kw: Any) -> Rig:
    rig = await make_rig(md_redis, run_ids, start=T0, **kw)
    await rig.at(T0)                                             # the term-start bootstrap
    assert rig.writer.boot is not None and rig.svc.pending_reason is None
    return rig


async def write_bar(rig: Rig, k: int, close: str = "1.2345", revision: int = 0) -> None:
    rig.writer.queue_bar(bar(T0 + k * Q, close, revision=revision))
    await rig.at(end_of(k))
    assert rig.writer.queue_depth() == 0


class Consumer:
    """A fake pull consumer: durable acknowledgement `(bar_end, hash)` per id, re-applies in bar order."""

    def __init__(self, rig: Rig) -> None:
        self.rig = rig
        self.acked: dict[int, str] = {}
        self.applied: list[tuple[int, str]] = []
        self.cursor = EventCursor(T0 + Q, 0)
        self.expired = 0

    async def pull(self) -> None:
        read = await self.rig.port.events_since(SERIES, self.cursor)
        if isinstance(read, CursorExpired):
            self.expired += 1
            last = max(self.acked, default=T0 + Q)
            self.cursor = EventCursor(last, 0)                    # restart from the durable acknowledgement
            read = await self.rig.port.events_since(SERIES, self.cursor)
        assert isinstance(read, EventsRead), read
        for e in sorted(read.events, key=lambda e: (e.bar_end_ms, e.write_seq)):
            if self.acked.get(e.bar_end_ms) == e.content_hash:
                continue                                          # same id and hash: a no-op
            self.acked[e.bar_end_ms] = e.content_hash
            self.applied.append((e.bar_end_ms, e.content_hash))
        self.cursor = read.next_cursor(self.cursor)


# ---------------------------------------------------------------------------------------------------- the plan's drills
async def test_redis_restart_between_bars_raises_the_generation_one_notice_and_no_duplicate(
        md_redis: Any, run_ids: RunIds, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    rig = await started(md_redis, run_ids)
    await rig.boot.save_record()
    consumer = Consumer(rig)
    await write_bar(rig, 0)
    await write_bar(rig, 1)
    await consumer.pull()
    assert [end for end, _ in consumer.applied] == [T0 + Q, T0 + 2 * Q]
    first_gen = rig.writer.boot.generation if rig.writer.boot else None
    await rig.boot.save_record()

    rig.restart_redis("f00dbabe01")                              # AOF reloaded: same data, new run_id
    rig.writer.queue_bar(bar(T0 + 2 * Q))
    await rig.at(end_of(2))                                       # NO_BOOT, then the bootstrap in the same step
    assert rig.writer.boot is not None and first_gen is not None
    assert rig.writer.boot.generation > first_gen and rig.writer.boot.run_id == "f00dbabe01"
    reset = rig.resets[-1]
    assert (reset.reason, reset.reset_class, reset.notice, reset.counted) == (
        BootReason.RUN_ID_CHANGED, ResetClass.INTACT, True, True)
    assert [r.notice for r in rig.resets] == [False, True]       # the term start sent none: one notice
    await rig.at(end_of(2) + 250)                                 # the queued bar is written after the bootstrap
    assert rig.writer.queue_depth() == 0
    await consumer.pull()                                         # old generation: CURSOR_EXPIRED, restart, re-apply
    assert consumer.expired == 1
    ends = [end for end, _ in consumer.applied]
    assert ends == [T0 + Q, T0 + 2 * Q, T0 + 3 * Q] and len(set(ends)) == len(ends)   # no duplicate
    assert all(value not in caplog.text for value in ("1.2345", "1.23", "1.24", "1.22"))


async def test_flushall_while_running_gives_a_bootstrap_that_restores_the_rollback_set(md_redis: Any,
                                                                                      run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids)
    for k in range(3):
        await write_bar(rig, k)
    memory = [e.stored for e in rig.writer.rollback_memory(SERIES)]
    await md_redis.flushall()
    rig.writer.queue_bar(bar(T0 + 3 * Q))
    await rig.at(end_of(3))
    reset = rig.resets[-1]
    assert reset.reason is BootReason.NO_WRITER and reset.reset_class is ResetClass.STORE_LOST and reset.notice
    assert await md_redis.get(K.WRITER) == TOKEN.text.encode() and rig.leader.checks == 2
    assert await md_redis.get(K.BOOT) is not None
    rb = [m.decode() for m in await md_redis.zrange(K.rollback(SERIES), 0, -1)]
    assert rb == memory                                           # memory to store where the store lost it
    await rig.at(end_of(3) + 250)
    assert rig.writer.queue_depth() == 0
    seqs = [int(s) for _, s in await md_redis.zrange(K.rollback(SERIES), 0, -1, withscores=True)]
    assert seqs == [1, 2, 3, 4]                                   # write_seq continues above the restored set
    assert rig.book.state(SERIES) is Sm12State.GAPPED


async def test_an_older_aof_is_store_lost_and_the_memory_entries_go_back(md_redis: Any, run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids)
    await write_bar(rig, 0)
    image = await snapshot(md_redis)                              # the AOF as of bar 0
    await write_bar(rig, 1)
    await write_bar(rig, 2)
    # no save since the term-start bootstrap: the stored record predates every flush, so only this process's
    # in-memory record (newest last_write_ms) tells the older AOF apart
    assert rig.records.record is not None and rig.records.record.last_write_ms < end_of(0)
    await restore(md_redis, image)
    rig.restart_redis("0ldaof0001")
    rig.writer.queue_bar(bar(T0 + 3 * Q))
    await rig.at(end_of(3))
    reset = rig.resets[-1]
    assert reset.reset_class is ResetClass.STORE_LOST and reset.notice    # meta more than 2 s behind the record
    seqs = [int(s) for _, s in await md_redis.zrange(K.rollback(SERIES), 0, -1, withscores=True)]
    assert seqs == [1, 2, 3]
    meta = await md_redis.hgetall(K.meta(SERIES))
    assert meta[b"write_seq"] == b"3" and meta[b"sm12"] == b"GAPPED"
    assert meta[b"gap_from"] == str(T0 + Q).encode()              # min(meta last bucket, memory last bucket) + 15m
    assert rig.book.state(SERIES) is Sm12State.GAPPED


async def test_q17_off_a_restart_loses_the_store_and_the_bootstrap_heals_it(md_redis: Any, run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids, aof_off=True)
    await write_bar(rig, 0)
    assert rig.aof.calls == []                                    # no WAITAOF with AOF off
    await rig.boot.save_record()
    await md_redis.flushall()                                     # Q17 off: a restart is an empty store
    rig.restart_redis("n0a0f00001")
    rig.writer.queue_bar(bar(T0 + Q))
    await rig.at(end_of(1))
    assert rig.resets[-1].reset_class is ResetClass.STORE_LOST
    await rig.at(end_of(1) + 250)
    assert rig.writer.queue_depth() == 0 and rig.aof.calls == []
    assert await md_redis.zcard(K.rollback(SERIES)) == 2


async def test_a_term_start_sends_no_notice_counts_nothing_and_quarantines_nothing(md_redis: Any,
                                                                                   run_ids: RunIds) -> None:
    records = Records()
    term1 = await started(md_redis, run_ids, records=records)
    for k in range(3):
        await write_bar(term1, k)
    await term1.boot.save_record()
    # a new term on the same node (new epoch, new process: empty memory), store intact
    token2 = K.WriterToken(TOKEN.cluster_id, 8, TOKEN.node, "n0nce2")
    term2 = await make_rig(md_redis, run_ids, start=end_of(2) + 1_000, token=token2, records=records,
                           previous_term_epoch=7)
    await term2.at(end_of(2) + 1_000)
    reset = term2.resets[-1]
    assert (reset.reason, reset.reset_class, reset.notice, reset.counted) == (
        BootReason.TERM_START, ResetClass.INTACT, False, False)
    assert reset.generation == MdGeneration(8, 1) and term2.boot.budget.in_window(end_of(2)) == 0
    assert len(term2.writer.rollback_memory(SERIES)) == 3         # store to memory, hash-checked
    assert term2.boot.last_result is not None and term2.boot.last_result.restored_to_memory == 3
    term2.book.apply(SERIES, Sm12Event.LIVE)
    read = await term2.port.bars(SERIES, from_bucket_ms=T0, to_bucket_ms=T0 + 2 * Q)
    assert isinstance(read, BarsRead) and [b.quality.value for b in read.bars] == ["complete"] * 3
    assert await md_redis.zcard(K.quarantine(SERIES)) == 0        # a restart quarantines no stored bar
    assert term2.port.quarantine_requests == 0


async def test_a_trial_bootstrap_on_an_empty_store_writes_md_boot(md_redis: Any, run_ids: RunIds) -> None:
    rig = await make_rig(md_redis, run_ids, start=T0)
    results: list[Any] = []
    rig.svc.request_trial(results.append)
    await rig.at(T0)
    (result,) = results
    assert result.ok and result.reason is BootReason.TRIAL
    assert result.reset.notice is False and result.reset.counted is False
    assert await md_redis.get(K.BOOT) is not None and rig.writer.boot is not None
    assert rig.svc.pending_reason is None                         # the trial's sentinel serves the writer


async def test_global_failed_from_resets_then_refused_and_accepted_clear_do_not_retrip(md_redis: Any,
                                                                                    run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids)
    for minute in range(3):
        rig.clock.now = T0 + (minute + 1) * 60_000
        result = await rig.boot.bootstrap(BootReason.NO_BOOT)
        assert result.ok and result.reset is not None and result.reset.counted
    assert rig.faults == ["md_resets"]                            # 3 in one hour: the ARM-global fault
    trials: list[Any] = []
    rig.svc.request_trial(trials.append)                          # a refused clear's probe
    await rig.at(T0 + 4 * 60_000)
    assert trials[0].ok and rig.boot.budget.in_window(rig.clock.now) == 3 and rig.faults == ["md_resets"]
    rig.svc.request_post_clear()                                  # an accepted clear
    await rig.at(T0 + 5 * 60_000)
    assert rig.resets[-1].reason is BootReason.POST_CLEAR and not rig.resets[-1].notice
    assert rig.boot.budget.in_window(rig.clock.now) == 0 and rig.faults == ["md_resets"]
    rig.clock.now = T0 + 6 * 60_000
    await rig.boot.bootstrap(BootReason.NO_BOOT)                  # within the same hour: counts 1, no re-trip
    assert rig.boot.budget.in_window(rig.clock.now) == 1 and rig.faults == ["md_resets"]


async def test_a_revision_of_an_acknowledged_bar_is_pulled_by_write_seq_without_a_wake_up(md_redis: Any,
                                                                                        run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids)
    woken: list[Any] = []
    await write_bar(rig, 0)
    consumer = Consumer(rig)
    rig.port.on_bar_closed(woken.append)
    rig.port.on_bar_revised(woken.append)
    await consumer.pull()
    assert consumer.applied == [(T0 + Q, hash_of(bar(T0)))]
    revised = bar(T0, "1.2399", revision=1)
    await write_bar(rig, 0, "1.2399", revision=1)
    read = await rig.port.events_since(SERIES, consumer.cursor)
    assert isinstance(read, EventsRead)
    (event,) = read.events
    assert (event.bar_end_ms, event.content_hash, event.write_seq, event.revised) == (
        T0 + Q, hash_of(revised), 2, True)                        # same id, new hash, higher write_seq
    assert len(woken) == 1                                        # the wake-up fired, but the pull needed none


async def test_a_consumer_stopped_for_three_hours_leaves_writer_memory_unchanged(md_redis: Any,
                                                                                 run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids)
    await write_bar(rig, 0)
    gen = rig.writer.boot.generation if rig.writer.boot else None
    stale = EventCursor(T0 + Q, 1, gen)
    for k in range(1, 12):                                        # 3 hours of 15m bars, nobody pulls
        await write_bar(rig, k)
    assert len(rig.writer.rollback_memory(SERIES)) == 8 and rig.writer.queue_depth() == 0
    assert await md_redis.zcard(K.rollback(SERIES)) == 8
    assert isinstance(await rig.port.events_since(SERIES, stale), CursorExpired)


# ---------------------------------------------------------------------------------------------------- steps 1, 4 and the record
async def test_the_registry_is_rebuilt_from_held_and_live_store_leases(md_redis: Any, run_ids: RunIds) -> None:
    await md_redis.set(K.WRITER, TOKEN.text)
    runner_calls = [
        S.lease_call(TOKEN, "dash", "specA", 60_000, "PREWARM", registry_field="specA",
                     registry_value=SERIES.canonical),
        S.lease_call(TOKEN, "old", "specB", 60_000, "PREWARM", registry_field="specB",
                     registry_value=TICK_1M.canonical),
    ]
    runner = ScriptRunner(md_redis, run_id_reader=run_ids)
    assert all(isinstance(r, ScriptOk) for r in await runner.run(runner_calls))
    await md_redis.delete(K.demand("old", "specB"))               # expired: still in the index
    await md_redis.delete(K.REGISTRY_SERIES)                      # registry lost, leases alive
    leases = Leases([HeldLease("arm", "specC", "ARMED", 60_000, OTHER_TICK.canonical)])
    rig = await make_rig(md_redis, run_ids, start=T0, leases=leases)
    await md_redis.hset(K.REGISTRY_SERIES, "specA", SERIES.canonical)
    await rig.at(T0)
    registry = {k.decode(): v.decode() for k, v in (await md_redis.hgetall(K.REGISTRY_SERIES)).items()}
    assert registry == {"specA": SERIES.canonical, "specC": OTHER_TICK.canonical}
    assert await md_redis.exists(K.demand("arm", "specC")) == 1   # the in-process lease re-asserted
    assert await StoreReader(md_redis).live_leases() == [("arm", "specC", "ARMED"), ("dash", "specA", "PREWARM")]
    index = {k.decode() for k in await md_redis.hkeys(K.LEASE_INDEX)}
    assert index == {"arm:specC", "dash:specA"}                   # the expired entry was pruned


async def test_stored_series_are_handed_to_retention_after_the_bootstrap(md_redis: Any, run_ids: RunIds) -> None:
    await md_redis.set(K.WRITER, TOKEN.text)
    runner = ScriptRunner(md_redis, run_id_reader=run_ids)
    reply = await runner.run_one(S.index_call(TOKEN, [OTHER_TICK.canonical, K.market_field("mock", "SYN3")]))
    assert reply == ScriptOk(2)
    rig = await make_rig(md_redis, run_ids, start=T0)
    await rig.at(T0)
    _, floors = rig.writer.retention_floors(T0)
    assert OTHER_TICK in floors                                   # a symbol this term never writes is trimmed
    assert ("mock", "SYN3") in rig.writer.known_markets()


async def test_the_writer_enters_what_it_writes_in_the_stored_index(md_redis: Any, run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids)
    await write_bar(rig, 0)
    index = {k.decode() for k in await md_redis.hkeys(K.STORED)}
    assert SERIES.canonical in index


async def test_a_record_save_failure_never_fails_the_bootstrap(md_redis: Any, run_ids: RunIds) -> None:
    rig = await make_rig(md_redis, run_ids, start=T0, records=Records(fail=True))
    await rig.at(T0)
    assert rig.writer.boot is not None and rig.resets[-1].reset_class is ResetClass.FRESH


async def test_md_writer_reset_installs_a_fresh_token_and_is_not_counted(md_redis: Any, run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids)
    fresh = K.WriterToken(TOKEN.cluster_id, TOKEN.epoch_rev, TOKEN.node, "fresh1")
    assert rig.svc.submit(WorkItem(WorkKind.MD_WRITER_RESET, 41, fresh))
    await rig.at(T0 + 1_000)                                      # the item is taken
    await rig.at(T0 + 2_000)                                      # the bootstrap runs
    assert rig.completed == [(41, "applied", "intact")]
    assert await md_redis.get(K.WRITER) == fresh.text.encode() and rig.writer.token == fresh
    reset = rig.resets[-1]
    assert reset.reason is BootReason.OPERATOR_RESET and not reset.counted and not reset.notice
    await write_bar(rig, 0)                                       # writes go on with the fresh token


async def test_md_rehash_is_moot_without_a_retained_key(md_redis: Any, run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids)
    rig.svc.submit(WorkItem(WorkKind.MD_REHASH, 7))
    await rig.at(T0 + 1_000)
    assert rig.completed == [(7, "moot", "no_retained_key")]


# ---------------------------------------------------------------------------------------------------- failures
class DownClient:
    """A writer connection whose `run_id` read fails like an unreachable `redis-md`."""

    async def install_token(self, token: K.WriterToken) -> None:
        raise StoreUnavailable("connection", "ConnectionError")

    async def current_run_id(self) -> str:
        raise StoreUnavailable("connection", "ConnectionError")

    async def run_one(self, call: S.ScriptCall) -> ScriptReply:
        raise StoreUnavailable("connection", "ConnectionError")

    async def run_sequential(self, calls: Sequence[S.ScriptCall]) -> list[ScriptReply]:
        raise StoreUnavailable("connection", "ConnectionError")


class AnsweringFailClient:
    """A writer connection whose store answers but refuses the bootstrap's scripts (an error reply): the bootstrap
    itself fails, which counts toward `BOOT_FAIL_LIMIT` (S38e)."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id

    async def install_token(self, token: K.WriterToken) -> None:
        return None

    async def current_run_id(self) -> str:
        return self.run_id

    async def run_one(self, call: S.ScriptCall) -> ScriptReply:
        return ScriptFailed("ResponseError(ERR)")

    async def run_sequential(self, calls: Sequence[S.ScriptCall]) -> list[ScriptReply]:
        return [ScriptFailed("ResponseError(ERR)")]


class LoadingClient(AnsweringFailClient):
    """`redis-md` replaying its AOF: every script answers `LOADING` (S38e: unreachable, never counted)."""

    async def run_one(self, call: S.ScriptCall) -> ScriptReply:
        return ScriptFailed("BusyLoadingError(LOADING)")


async def test_three_failed_bootstraps_in_a_row_are_the_global_fault_once(md_redis: Any, run_ids: RunIds) -> None:
    """Only a store that answered counts (S38e): three refused bootstraps in a row are `bootstrap_failed`, once."""
    rig = await make_rig(md_redis, run_ids, start=T0)
    rig.boot._client = AnsweringFailClient(RUN_ID)                # type: ignore[assignment]
    for k in range(BOOT_FAIL_LIMIT + 2):
        result = await rig.boot.bootstrap(BootReason.NO_BOOT)
        assert not result.ok and result.failure == "script_failed"
    assert rig.faults == ["bootstrap_failed"]
    assert rig.writer.boot is None


class NoPermClient(AnsweringFailClient):
    """A store that answers every `run_id` read with a permanent error (a wrong ACL: `NOPERM`)."""

    async def current_run_id(self) -> str:
        raise StoreUnavailable("error", "ResponseError(NOPERM)")


class BusyRaiseClient(AnsweringFailClient):
    """A store whose `run_id` read raises an error reply naming `BUSY` (a script still running)."""

    async def current_run_id(self) -> str:
        raise StoreUnavailable("error", "ResponseError(BUSY)")


async def test_a_store_answering_a_permanent_error_counts_toward_bootstrap_failed(
        md_redis: Any, run_ids: RunIds) -> None:
    """Review S38e: `store_error` (the store answered: NOPERM, another script SHA, no usable run_id) is not
    unreachable; three in a row trip `bootstrap_failed` as before S38e."""
    rig = await make_rig(md_redis, run_ids, start=T0)
    rig.boot._client = NoPermClient(RUN_ID)                       # type: ignore[assignment]
    for _ in range(BOOT_FAIL_LIMIT):
        result = await rig.boot.bootstrap(BootReason.NO_BOOT)
        assert not result.ok and result.failure == "store_error"
        assert result.failure not in UNREACHABLE_FAILURES
    assert rig.faults == ["bootstrap_failed"] and rig.boot.unreachable_streak == 0


async def test_a_raised_busy_reply_is_never_counted(md_redis: Any, run_ids: RunIds) -> None:
    rig = await make_rig(md_redis, run_ids, start=T0)
    rig.boot._client = BusyRaiseClient(RUN_ID)                    # type: ignore[assignment]
    for _ in range(BOOT_FAIL_LIMIT + 1):
        result = await rig.boot.bootstrap(BootReason.NO_BOOT)
        assert not result.ok and result.failure == "store_busy"
    assert rig.faults == [] and rig.boot.fail_streak == 0


@pytest.mark.parametrize("client", ["down", "loading"])
async def test_an_unreachable_or_loading_store_is_never_a_counted_bootstrap_failure(
        md_redis: Any, run_ids: RunIds, client: str) -> None:
    """S38e: a store that is down, times out or replies LOADING (AOF replay at start-up) never trips
    `bootstrap_failed`; the failures only grow the back-off (`unreachable_streak`), the md_down time counter
    applies; the first bootstrap after the store is back succeeds and resets the streak."""
    rig = await make_rig(md_redis, run_ids, start=T0)
    real = rig.boot._client
    rig.boot._client = DownClient() if client == "down" else LoadingClient(RUN_ID)   # type: ignore[assignment]
    for _ in range(BOOT_FAIL_LIMIT * 4):
        result = await rig.boot.bootstrap(BootReason.TERM_START)
        assert not result.ok and result.failure in UNREACHABLE_FAILURES
    assert rig.faults == [] and rig.boot.fail_streak == 0
    assert rig.boot.unreachable_streak == BOOT_FAIL_LIMIT * 4
    rig.boot._client = real
    result = await rig.boot.bootstrap(BootReason.TERM_START)
    assert result.ok and rig.boot.unreachable_streak == 0 and rig.faults == []


async def test_the_service_backs_off_while_the_store_is_unreachable(md_redis: Any, run_ids: RunIds) -> None:
    """S38e: the md-writer service retries an unreachable store with back-off (1 s doubling, at most 30 s) and
    no fault; the writer starts once the store answers (a redis-md started after the engine ends LIVE)."""
    rig = await make_rig(md_redis, run_ids, start=T0)
    real = rig.boot._client
    rig.boot._client = DownClient()                               # type: ignore[assignment]
    mono = [0]
    rig.svc._mono_ms = lambda: mono[0]                            # type: ignore[method-assign]
    delays = []
    for _ in range(8):
        await rig.svc._boot()
        delays.append(rig.svc._next_boot_at - mono[0])
        mono[0] = rig.svc._next_boot_at
    assert delays == [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000]
    assert rig.faults == [] and rig.writer.boot is None
    rig.boot._client = real
    await rig.svc._boot()
    assert rig.writer.boot is not None and rig.faults == [] and rig.svc.pending_reason is None


async def test_a_trial_failure_and_not_leader_never_count(md_redis: Any, run_ids: RunIds) -> None:
    rig = await make_rig(md_redis, run_ids, start=T0)
    rig.leader.ok = False
    for _ in range(4):
        result = await rig.boot.bootstrap(BootReason.TERM_START)
        assert result.failure == "not_leader"
    rig.boot._client = DownClient()                               # type: ignore[assignment]
    for _ in range(4):
        await rig.boot.bootstrap(BootReason.TRIAL)
    assert rig.faults == [] and rig.boot.fail_streak == 0


async def test_a_fenced_ex_leader_cannot_take_the_token_back_in_term(md_redis: Any, run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids)
    await md_redis.set(K.WRITER, K.WriterToken(TOKEN.cluster_id, 9, "node-b", "other").text)
    result = await rig.boot.bootstrap(BootReason.NO_BOOT)
    assert not result.ok and result.failure == "no_writer"
    assert (await md_redis.get(K.WRITER)).decode().split(":")[2] == "node-b"


def own_lock(token: K.WriterToken = TOKEN) -> str:
    return f"boot_{token.node}_{token.term_nonce}_{token.epoch_rev}".replace("-", "_")


async def test_a_boot_lock_left_by_this_writers_previous_attempt_is_re_taken(md_redis: Any,
                                                                             run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids)
    await md_redis.set(K.BOOT_LOCK, own_lock(), px=10_000)       # an earlier attempt's lock: its release timed out
    for _ in range(BOOT_FAIL_LIMIT):
        result = await rig.boot.bootstrap(BootReason.NO_BOOT)
        assert result.ok, result.failure
    assert "bootstrap_failed" not in rig.faults and rig.boot.fail_streak == 0   # (3 resets: the budget's fault)
    assert await md_redis.get(K.BOOT_LOCK) is None


async def test_a_busy_boot_lock_never_counts_toward_the_failure_streak(md_redis: Any, run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids)
    await md_redis.set(K.BOOT_LOCK, "boot_other_holder", px=10_000)   # expires on its own
    for _ in range(BOOT_FAIL_LIMIT + 1):
        result = await rig.boot.bootstrap(BootReason.NO_BOOT)
        assert not result.ok and result.failure == "lock_busy"
    assert rig.faults == [] and rig.boot.fail_streak == 0
    assert await md_redis.get(K.BOOT_LOCK) == b"boot_other_holder"   # never released by another value
    await md_redis.delete(K.BOOT_LOCK)
    assert (await rig.boot.bootstrap(BootReason.NO_BOOT)).ok


async def test_a_corrupt_rollback_member_does_not_fail_the_bootstrap(md_redis: Any, run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids)
    for k in range(3):
        await write_bar(rig, k)
    (top,) = await md_redis.zrange(K.rollback(SERIES), -1, -1)
    await md_redis.zrem(K.rollback(SERIES), top)
    await md_redis.zadd(K.rollback(SERIES), {"garbled": 3})       # same write_seq as the memory entry
    for _ in range(BOOT_FAIL_LIMIT):
        result = await rig.boot.bootstrap(BootReason.NO_BOOT)
        assert result.ok, result.failure
        assert result.rejected_entries == 1 and result.restored_to_store == 0
    assert "bootstrap_failed" not in rig.faults and rig.boot.fail_streak == 0


class FaultyLeases:
    def held_leases(self) -> Sequence[HeldLease]:
        raise RuntimeError("arm-tracker fault")


async def test_an_unexpected_port_error_is_a_failed_attempt_and_releases_the_lock(md_redis: Any,
                                                                                  run_ids: RunIds) -> None:
    rig = await started(md_redis, run_ids)
    rig.boot._leases = FaultyLeases()
    result = await rig.boot.bootstrap(BootReason.NO_BOOT)
    assert not result.ok and result.failure == "internal"
    assert await md_redis.get(K.BOOT_LOCK) is None and rig.boot.fail_streak == 1


class FirstRefused:
    """Wraps the writer connection: `BOOT_WRITE` refused, the LEASE replies after it succeed."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    async def run_sequential(self, calls: Sequence[S.ScriptCall]) -> list[ScriptReply]:
        return [ScriptRefused("NO_LOCK"), *(ScriptOk(1) for _ in calls[1:])]


async def test_the_first_failing_reply_names_the_failure(md_redis: Any, run_ids: RunIds) -> None:
    leases = Leases([HeldLease("arm", "specC", "ARMED", 60_000, OTHER_TICK.canonical)])
    rig = await started(md_redis, run_ids, leases=leases)
    rig.boot._client = FirstRefused(rig.client)                   # type: ignore[assignment]
    result = await rig.boot.bootstrap(BootReason.NO_BOOT)
    assert not result.ok and result.failure == "no_lock"


async def test_the_service_backs_off_after_a_failure(md_redis: Any, run_ids: RunIds) -> None:
    rig = await make_rig(md_redis, run_ids, start=T0)
    rig.leader.ok = False
    await rig.at(T0)
    assert rig.leader.checks == 1 and rig.svc.pending_reason is BootReason.TERM_START
    await rig.at(T0 + 500)                                        # back-off: not yet
    assert rig.leader.checks == 1
    rig.leader.ok = True
    await rig.at(T0 + 1_000)
    assert rig.leader.checks == 2 and rig.writer.boot is not None


# ---------------------------------------------------------------------------------------------------- pure parts
def _record(sentinel: K.BootSentinel, last_write_ms: int) -> NodeMdRecord:
    return NodeMdRecord(sentinel.node, sentinel.text, last_write_ms)


def test_classification_against_this_nodes_record() -> None:
    mine = K.BootSentinel(RUN_ID, "c1", "node-a", MdGeneration(7, 2))
    rec = _record(mine, 100_000)
    kw = {"node": "node-a", "current_epoch": 7}
    assert classify(None, None, None, **kw) is ResetClass.FRESH
    assert classify(None, None, 5, **kw) is ResetClass.STORE_LOST
    assert classify(None, mine, 5, **kw) is ResetClass.STORE_FOREIGN
    assert classify(rec, None, 100_000, **kw) is ResetClass.STORE_LOST
    assert classify(rec, mine, 98_000, **kw) is ResetClass.INTACT          # within the 2 s AOF window
    assert classify(rec, mine, 97_999, **kw) is ResetClass.STORE_LOST      # more than 2 s behind
    older = K.BootSentinel(RUN_ID, "c1", "node-a", MdGeneration(7, 1))
    assert classify(rec, older, 100_000, **kw) is ResetClass.STORE_LOST
    newer_mine = K.BootSentinel("ffff", "c1", "node-a", MdGeneration(7, 3))
    assert classify(rec, newer_mine, 100_000, **kw) is ResetClass.INTACT   # its own newer sentinel (save failed)
    peer = K.BootSentinel(RUN_ID, "c1", "node-b", MdGeneration(8, 1))
    assert classify(rec, peer, 100_000, **kw) is ResetClass.STORE_FOREIGN
    assert classify(rec, mine, 100_000, node="node-a", current_epoch=12,
                    previous_term_epoch=11) is ResetClass.STORE_FOREIGN   # other nodes have led since
    assert classify(rec, mine, 100_000, node="node-a", current_epoch=8,
                    previous_term_epoch=7) is ResetClass.INTACT


def test_notice_rule() -> None:
    assert not wants_notice(BootReason.TERM_START, ResetClass.INTACT)
    assert not wants_notice(BootReason.TERM_START, ResetClass.FRESH)
    assert wants_notice(BootReason.TERM_START, ResetClass.STORE_LOST)
    assert wants_notice(BootReason.NO_BOOT, ResetClass.INTACT)
    assert wants_notice(BootReason.RUN_ID_CHANGED, ResetClass.INTACT)
    assert not wants_notice(BootReason.OPERATOR_RESET, ResetClass.INTACT)
    assert not wants_notice(BootReason.TRIAL, ResetClass.STORE_LOST)
    assert not wants_notice(BootReason.POST_CLEAR, ResetClass.STORE_FOREIGN)


def test_reset_budget_window() -> None:
    budget = ResetBudget()
    assert not budget.count(0) and not budget.count(1_000)
    assert not budget.count(3_600_000 + 1)                         # the first one left the window
    assert budget.count(3_600_000 + 2)
    budget.clear()
    assert budget.in_window(3_600_000 + 3) == 0


def test_sm12_table() -> None:
    assert sm12_next(Sm12State.EMPTY, Sm12Event.BACKFILL) is Sm12State.BACKFILLING
    assert sm12_next(Sm12State.BACKFILLING, Sm12Event.LIVE) is Sm12State.LIVE
    assert sm12_next(Sm12State.LIVE, Sm12Event.GAP) is Sm12State.GAPPED
    assert sm12_next(Sm12State.GAPPED, Sm12Event.LIVE) is Sm12State.LIVE
    for state in (Sm12State.EMPTY, Sm12State.BACKFILLING, Sm12State.LIVE, Sm12State.GAPPED):
        assert sm12_next(state, Sm12Event.FAIL) is Sm12State.FAILED
    assert sm12_next(Sm12State.FAILED, Sm12Event.CLEAR) is Sm12State.BACKFILLING
    assert sm12_next(Sm12State.FAILED, Sm12Event.LIVE) is None     # only an operator clear leaves FAILED
    assert sm12_next(Sm12State.LIVE, Sm12Event.CLEAR) is None


def test_book_gates_decision_reads() -> None:
    book = SeriesStateBook()
    assert not book.readable(SERIES, T0, T0)
    assert book.mark_bootstrapped(SERIES, T0) is Sm12State.GAPPED and book.gaps(SERIES) == 1
    assert book.apply(SERIES, Sm12Event.BACKFILL)
    book.note_complete(SERIES, T0 + 4 * Q, T0 + 8 * Q)
    book.note_complete(SERIES, T0 + 2 * Q, T0 + 4 * Q)
    assert book.readable(SERIES, T0 + 2 * Q, T0 + 8 * Q)
    assert not book.readable(SERIES, T0 + Q, T0 + 3 * Q)
    assert not book.readable(SERIES, None, None)                  # an n-form read needs LIVE
    assert book.apply(SERIES, Sm12Event.FAIL) and not book.readable(SERIES, T0 + 2 * Q, T0 + 2 * Q)
    assert book.mark_bootstrapped(SERIES, T0) is Sm12State.FAILED   # a bootstrap never hides FAILED
    assert not book.apply(SERIES, Sm12Event.LIVE)
    assert book.apply(SERIES, Sm12Event.CLEAR) and book.state(SERIES) is Sm12State.BACKFILLING
    assert book.mark_bootstrapped(TICK_1M, None) is Sm12State.EMPTY


def test_events_after_restart_form_needs_the_set_to_reach_back() -> None:
    gen = MdGeneration(7, 1)
    entries = [RollbackEntry(ws, T0 + k * Q + Q, encode_member(bar(T0 + k * Q), hash_of(bar(T0 + k * Q))))
               for ws, k in ((5, 2), (6, 3), (7, 4))]
    assert isinstance(events_after(SERIES, entries, EventCursor(T0 + 2 * Q, 0), gen), CursorExpired)
    got = events_after(SERIES, entries, EventCursor(T0 + 3 * Q, 0), gen)
    assert not isinstance(got, CursorExpired) and [e.write_seq for e in got] == [5, 6, 7]
    assert KEYRING.current_id == 3


def test_events_after_restart_form_expires_on_a_hole_in_a_backfill_ordered_set() -> None:
    """The set is the newest N by write_seq: a backfill wrote buckets 5..0 newest first, then live bars 20, 21. The
    low bar ends survived the trim while 6..19 were never in the set: a restart from bucket 3 must not skip them."""
    gen = MdGeneration(7, 1)
    order = (5, 4, 3, 2, 1, 0, 20, 21)
    entries = [RollbackEntry(15 + i, T0 + k * Q + Q, encode_member(bar(T0 + k * Q), hash_of(bar(T0 + k * Q))))
               for i, k in enumerate(order)]
    assert isinstance(events_after(SERIES, entries, EventCursor(T0 + 4 * Q, 0), gen), CursorExpired)
    assert isinstance(events_after(SERIES, entries, EventCursor(T0 + Q, 0), gen), CursorExpired)
    got = events_after(SERIES, entries, EventCursor(T0 + 21 * Q, 0), gen)          # from bucket 20: no hole
    assert not isinstance(got, CursorExpired) and [e.write_seq for e in got] == [21, 22]
    got = events_after(SERIES, entries[:6], EventCursor(T0 + 4 * Q, 0), gen)        # contiguous 0..5
    assert not isinstance(got, CursorExpired) and sorted(e.bar_end_ms for e in got) == [T0 + 4 * Q, T0 + 5 * Q,
                                                                                        T0 + 6 * Q]


class Stamp:
    def __init__(self) -> None:
        self.stamps = 0

    def stamp(self) -> None:
        self.stamps += 1

    def set_exempt(self, reason: str | None) -> None:
        pass


async def test_the_service_stamps_between_the_cycle_and_the_bootstrap(md_redis: Any, run_ids: RunIds) -> None:
    rig = await make_rig(md_redis, run_ids, start=T0)
    stamp = Stamp()
    await rig.svc.step(None, stamp)
    assert stamp.stamps == 2 and rig.writer.boot is not None      # after the cycle and after the bootstrap


async def test_a_stopped_service_starts_no_bootstrap(md_redis: Any, run_ids: RunIds) -> None:
    import asyncio
    rig = await make_rig(md_redis, run_ids, start=T0)
    stop = asyncio.Event()
    stop.set()
    await rig.svc.step(stop)
    assert rig.writer.boot is None and await md_redis.get(K.BOOT) is None
    await rig.svc.run(stop, Stamp())                              # returns at once
