"""S38 (plan E2E; design §C3 "Failure drills", §A7 SM12, §B5.3, §B14): failure drills on the end-to-end rig of
`tests/e2e/arm_rig.py` (real md store on fakeredis + lupa, real ARM stores on the SQL emulator, the real tracker,
BarClock, S21 pipeline and notices, driven the way the runtime's `arm` / `arm-tracker` loops drive them), plus two
on the real `EngineSession` (the in-memory replay scenario) and the two relays on one `notification_outbox`.

* a 30 s PostgreSQL outage of the ARM pool: every pass fails and is counted, nothing is written, nothing pages;
  the POST is decided once, on time, when PostgreSQL returns;
* `redis-md` restarted at "10:16" (one minute after the 10:15 bar, decided `ADJUST`) while the backfill lane is
  filling (design §B6.6): the AOF store is re-bootstrapped in place with a new generation, the queued backfill bars
  (older buckets and the 10:15 bar again with the same OHLC) are written after it and read back under the new
  generation; the same-content 10:15 bar is a no-op (no `CORRECTED`, no revision, no new outbox item, the row
  unchanged) and the next POST is decided normally;
* the engine and `redis-md` restarting together: a new term over the same ARM tables and store, no re-arm, no
  second row for a decided bar, the next POST decided once with a notice;
* a takeover: the new leader boots its md writer with the epoch-8 token; the old leader's next md flush is refused
  (`NO_WRITER`) and its next PostgreSQL write by the fence (`EpochMismatch`, fatal); the new leader decides the bar
  once;
* PC sleep: the clock jumps two hours; the missed bars are one `downtime` span and the bar after the wake is
  decided normally;
* the market-data hook raising on every tick: the real session's scenario (ingestion, Oversee, the halt and the
  gate) runs unchanged, one page, the calendar still sees every tick;
* the Oversee relay never touches an `arm` row and the ARM relay never an Oversee row (an unsent one included).

Synthetic instrument, ids, levels and times only; no assertion message carries a value.
"""
from __future__ import annotations

import copy
import json
from collections.abc import AsyncIterator
from decimal import Decimal
from json import dumps as _dumps
from pathlib import Path
from typing import Any

import fakeredis
import pytest

from src.db.arm_outbox_store import PgArmOutboxStore
from src.db.fence import EpochMismatch
from src.db.notify_store import PgDedupeStore, PgOutboxStore
from src.marketdata.bootstrap import BootReason, Sm12Event
from src.marketdata.ports import BarsRead
from src.marketdata.store import keys as K
from src.notifications.arm_relay import ArmRelay, SendTimeRenderer
from src.notifications.relay import OutboxRelay
from src.oversee.notify import OutboxRow, RecordingChannel
from src.riskmgr.sm11 import ArmState
from tests.db.arm_outbox_emulator import ArmOutboxEmulator
from tests.e2e.arm_rig import (
    CLOSE_ABOVE,
    CLOSE_BELOW,
    POST_AFTER_MS,
    SERIES_ASK,
    SERIES_BID,
    SPREAD,
    TF,
    E2e,
    broker_bar,
    e2e_rig,
    engine_db,
    md_term,
    run_main_scenario,
)
from tests.marketdata.conftest import KEYRING


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


@pytest.fixture
async def redis() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.flushall()
    await r.aclose()


async def armed(redis: Any) -> tuple[E2e, int]:
    """P1 armed (late, one `armed_late` span); returns the first bar end the ARM decides."""
    e = await e2e_rig(redis=redis)
    await e.tracker_round()
    await e.notices.step()
    assert e.row()["state"] == ArmState.ARMED.value
    return e, int(e.row()["last_decided_bar"]) + TF


async def post(e: E2e, bar_end: int, close: str, *, at: int | None = None) -> None:
    """The bar's final broker bars, the live exit quote, then one `arm` round at `at` (default: on time)."""
    now = at if at is not None else bar_end + POST_AFTER_MS
    await e.final_bars(bar_end, close)
    e.tick(close, now - 500)
    await e.at(now)


def bar_rows(e: E2e, bar_end: int, event: str = "POST") -> list[dict[str, Any]]:
    return [d for d in e.decisions(event) if d["bar_end_ms"] == bar_end]


def no_duplicates(e: E2e) -> bool:
    keys = [(d["position_id"], d["bar_spec"], d["event"], d["bar_end_ms"], d["revision"]) for d in e.decisions()]
    return len(keys) == len(set(keys))


# ---------------------------------------------------------------------------------------------------- PostgreSQL
async def test_a_30_s_postgresql_outage_writes_nothing_and_the_post_lands_once_when_it_returns(redis: Any) -> None:
    e, b1 = await armed(redis)
    try:
        await e.final_bars(b1, CLOSE_BELOW)
        down = [True]
        e.emu.fail_when = lambda sql, args: ConnectionResetError("down") if down[0] else None
        before = len(e.decisions())
        for k in range(4):                                     # 30 s: one round every 10 s
            now = b1 + POST_AFTER_MS + k * 10_000
            e.tick(CLOSE_BELOW, now - 500)
            await e.at(now)
            await e.tracker_round()
        assert e.barclock.counters.errors == 4 and e.notice_errors == 4 and e.tracker_errors == 4
        assert len(e.decisions()) == before and e.rec.pages == []
        # the md layer never depended on PostgreSQL: the bar is stored and readable through the hash check
        assert e.md.writer.queue_depth() == 0
        down[0] = False
        await post(e, b1, CLOSE_BELOW, at=b1 + POST_AFTER_MS + 36_000)
        [row] = bar_rows(e, b1)
        assert (row["outcome"], row["caveats"]) == ("STRATEGY_REASON", [])     # still on time
        await e.at(b1 + POST_AFTER_MS + 40_000)
        assert row["decision_id"] in {d["decision_id"] for d in e.decisions() if d["notice_id"] is not None}
        assert no_duplicates(e) and e.rec.pages == []
    finally:
        e.barclock.close()


# ---------------------------------------------------------------------------------------------------- redis-md
def b1_outbox(e: E2e, bar_end: int) -> list[str]:
    """The ARM outbox bodies that carry an item of the bar ending at `bar_end`."""
    return [r["body"] for r in e.emu.arm_rows()
            if any(i.get("bar_end_ms") == bar_end for i in json.loads(r["body"]).get("items", []))
            or json.loads(r["body"]).get("bar_end_ms") == bar_end]


async def test_redis_md_restart_at_10_16_during_backfill_rebootstraps_and_decides_nothing_twice(redis: Any) -> None:
    """Design §B6.6: `redis-md` restarts at 10:16 while BROKER 15m backfills; the 10:15 bar was decided `ADJUST`.
    The backfill lane re-delivers the 10:15 bar with the same OHLC (and older buckets): the store is re-bootstrapped
    in place, every queued bar is written under the new generation, the same-content 10:15 bar is a no-op (no
    `CORRECTED` item, no revision, no new outbox item, the 10:15 row unchanged) and the next POST is decided once."""
    e, b1 = await armed(redis)
    try:
        await post(e, b1, CLOSE_ABOVE)                        # 10:15 bar decided ADJUST on time
        assert [(d["outcome"], d["revision"]) for d in bar_rows(e, b1)] == [("ADJUST", 0)]
        await e.at(b1 + 30_000)                               # its notice queued before the restart
        row_10_15 = copy.deepcopy(bar_rows(e, b1))
        outbox_10_15 = b1_outbox(e, b1)
        decisions_before = len(e.decisions())
        assert outbox_10_15                                    # the ADJUST notice is in the ARM outbox
        generation = e.md.writer.boot.generation if e.md.writer.boot else None
        # the backfill lane is filling both series when redis-md restarts (AOF kept): older buckets plus the 10:15
        # bar again with the same OHLC, all still in the writer's queue at the restart
        for s in (SERIES_BID, SERIES_ASK):
            e.md.book.apply(s, Sm12Event.GAP)
            e.md.book.apply(s, Sm12Event.BACKFILL)
        older = [b1 - k * TF for k in range(4, 8)]
        for end in older:
            e.md.writer.queue_bar(broker_bar(SERIES_BID, end, CLOSE_BELOW))
        e.md.writer.queue_bar(broker_bar(SERIES_BID, b1, CLOSE_ABOVE))
        e.md.writer.queue_bar(broker_bar(SERIES_ASK, b1, str(Decimal(CLOSE_ABOVE) + SPREAD)))
        assert e.md.writer.queue_depth() == len(older) + 2
        e.md.restart_redis("r3st4rt1016")
        e.tick(CLOSE_ABOVE, b1 + 60_000 - 500)
        await e.at(b1 + 60_000)                               # 10:16: NO_BOOT, bootstrap in the same step
        await e.at(b1 + 60_250)                               # the queued backfill bars written after it
        assert e.md.writer.boot is not None and generation is not None
        new_generation = e.md.writer.boot.generation
        assert new_generation > generation and e.md.writer.boot.run_id == "r3st4rt1016"
        assert [r.reason for r in e.md.resets][-1] is BootReason.RUN_ID_CHANGED
        assert e.md.writer.queue_depth() == 0
        for s in (SERIES_BID, SERIES_ASK):
            e.md.book.apply(s, Sm12Event.LIVE)               # backfill complete
        # the backfilled buckets read back through the hash-checked read path, under the new generation
        for end in older + [b1]:
            read = await e.md.port.bars(SERIES_BID, at_bucket_ms=end - TF)
            assert isinstance(read, BarsRead) and read.generation == new_generation
            assert [b.bucket_start_ms for b in read.bars] == [end - TF]
        # the same-content 10:15 bar is a no-op for the ARM: no CORRECTED, no revision, no new outbox item
        assert len(e.decisions()) == decisions_before
        assert bar_rows(e, b1) == row_10_15
        assert all(d["outcome"] != "CORRECTED" and d["revision"] == 0 for d in bar_rows(e, b1))
        assert b1_outbox(e, b1) == outbox_10_15
        b2 = b1 + TF
        await post(e, b2, CLOSE_ABOVE)
        await e.at(b2 + 120_000)
        assert [d["outcome"] for d in bar_rows(e, b2)] == ["ADJUST"]
        assert bar_rows(e, b1) == row_10_15 and b1_outbox(e, b1) == outbox_10_15
        assert not [d for d in e.decisions() if d["outcome"] == "CORRECTED"]
        assert no_duplicates(e) and e.rec.pages == [] and e.barclock.counters.errors == 0
    finally:
        e.barclock.close()


async def test_engine_and_redis_md_restarting_together_resume_without_rearming_or_duplicates(redis: Any) -> None:
    e, b1 = await armed(redis)
    try:
        await post(e, b1, CLOSE_BELOW)
        e.barclock.close()                                   # the engine stops: this term's graph is gone
        await e.md.boot.save_record()
        b2 = b1 + TF
        e.md.run_ids.value = "r3st4rt0002"                   # redis-md restarted with its AOF at the same time
        e.emu.epoch = 8                                      # the engine comes back as leader of a new term
        md2 = await md_term(redis, e.md.run_ids, start=b2 - 60_000,
                            token=K.WriterToken("1234567891", 8, "node-a", "n0nce2"), previous_term_epoch=7,
                            records=e.md.records)
        n = e.new_term(md=md2, epoch=8)
        await n.tracker_round()
        assert len(n.emu.t.positions) == 1 and n.row()["state"] == ArmState.TRACKING.value
        assert n.row()["last_decided_bar"] == b1                # the cursor survived: nothing re-armed
        await post(n, b2, CLOSE_ABOVE)
        await n.at(b2 + 120_000)
        assert [d["outcome"] for d in bar_rows(n, b2)] == ["ADJUST"]
        assert [d["outcome"] for d in bar_rows(n, b1)] == ["STRATEGY_REASON"]
        assert no_duplicates(n) and n.rec.pages == []
        assert all(d["notice_id"] is not None for d in n.decisions())
        assert len({r["body"] for r in n.emu.arm_rows()}) == len(n.emu.arm_rows())   # no notice queued twice
        n.barclock.close()
    finally:
        e.barclock.close()


# ---------------------------------------------------------------------------------------------------- takeover
async def test_a_takeover_fences_the_old_leader_and_the_new_one_decides_the_bar_once(redis: Any) -> None:
    """Both sides of the old term are fenced: its PostgreSQL write is `EpochMismatch` (fatal) and its market-data
    writer's next flush is refused (`NO_WRITER`) once the new leader's md writer has booted with the epoch-8 token
    over the same store; the new leader decides the bar once."""
    e, b1 = await armed(redis)
    md2: Any = None
    new: E2e | None = None
    try:
        await e.final_bars(b1, CLOSE_BELOW)
        e.emu.epoch = 8                                      # the new leader's epoch is committed first
        old_writer = e.md.writer
        old_generation = old_writer.boot.generation if old_writer.boot else None
        # the new leader (another node) boots its own md writer over the same store with the epoch-8 token
        md2 = await md_term(redis, e.md.run_ids, start=b1 + 1_500,
                            token=K.WriterToken("1234567890", 8, "node-b", "n0nce8"), previous_term_epoch=7)
        assert md2.writer.boot is not None and old_generation is not None
        assert md2.writer.boot.generation > old_generation
        new = e.new_term(md=md2, epoch=8)
        # the old md writer's next flush is refused: md:writer now names the new term's token
        before = old_writer.counters.no_writer + old_writer.counters.no_boot
        old_writer.queue_bar(broker_bar(SERIES_BID, b1 - 4 * TF, CLOSE_BELOW))
        e.tick(CLOSE_BELOW, b1 + POST_AFTER_MS - 500)
        with pytest.raises(EpochMismatch):
            await e.at(b1 + POST_AFTER_MS)                   # fatal: the old process exits (operations.md §3)
        assert old_writer.counters.no_writer + old_writer.counters.no_boot > before
        assert old_writer.queue_depth() == 1                 # nothing of the old term reached the store
        assert bar_rows(e, b1) == []
        await new.tracker_round()
        new.tick(CLOSE_BELOW, b1 + POST_AFTER_MS + 250 - 500)
        await new.at(b1 + POST_AFTER_MS + 250)
        assert [d["outcome"] for d in bar_rows(new, b1)] == ["STRATEGY_REASON"]
        assert all(d["epoch"] == 8 for d in bar_rows(new, b1))
        assert no_duplicates(new)
        assert md2.writer.counters.no_writer == 0 and md2.writer.counters.no_boot == 0
    finally:
        e.barclock.close()
        if new is not None:
            new.barclock.close()


# ---------------------------------------------------------------------------------------------------- PC sleep
async def test_pc_sleep_spans_the_missed_bars_once_and_decides_the_bar_after_the_wake(redis: Any) -> None:
    e, b1 = await armed(redis)
    try:
        await post(e, b1, CLOSE_BELOW)
        for k in range(1, 9):                                 # the broker's bars exist; the PC slept through them
            await e.final_bars(b1 + k * TF, CLOSE_BELOW)
        wake = b1 + 8 * TF + POST_AFTER_MS
        e.tick(CLOSE_BELOW, wake - 500)
        await e.at(wake)
        await e.at(wake + 250)
        spans = [d for d in e.decisions("POST") if d["reason"] == "downtime"]
        assert len(spans) == 1 and spans[0]["outcome"] == "UNRESOLVED_RANGE"
        assert spans[0]["range_from_bucket"] is not None and spans[0]["advice"] is False
        last = b1 + 8 * TF
        assert [d["outcome"] for d in bar_rows(e, last)] == ["STRATEGY_REASON"]
        assert e.row()["last_decided_bar"] == last and no_duplicates(e)
        assert e.row()["state"] != ArmState.FAILED.value and e.barclock.counters.errors == 0
    finally:
        e.barclock.close()


# ---------------------------------------------------------------------------------------------------- the session
async def test_market_data_hook_raising_on_every_tick_leaves_the_engine_scenario_unchanged(tmp_path: Path,
                                                                                         redis: Any) -> None:
    """The real `EngineSession` (in-memory backend, MockBrokerAdapter) with a market-data hook that raises on every
    tick: the replay scenario (STREAMING, SM8 READY, stream-beats-REST, the Oversee breach, the halt and the gate)
    runs exactly as with no hook; the hook's guard trips once and pages once; the calendar is still fed."""
    from src.db.epoch import Epoch
    from src.runtime.session import EngineSession, SessionContext
    from tests.e2e.memory_backend import MemoryBackend
    from src.cache.keys import price_key
    from src.dto import PriceDTO
    from src.runtime.session import MD_HOOK_TRIP_AFTER
    from tests.e2e.scenario import (FAST_TIMINGS, NODE, FakeLease, eventually, fixture, mock_adapter, run_scenario,
                                    wall_ms)

    epoch = Epoch("cluster-e2e", 7)

    async def run_id(_: Any) -> str:
        return "run-e2e"

    calls = [0]

    def broken(tick: Any, snapshot: bool) -> None:
        calls[0] += 1
        raise RuntimeError("ring full")

    backend = MemoryBackend(redis, run_id, epoch.token)
    fx = fixture(wall_ms())
    adapter = mock_adapter(fx)
    exits: list[str] = []
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
                         adapters=lambda: [adapter], lease=FakeLease(), run_id_reader=run_id, exit_fn=exits.append,
                         timings=FAST_TIMINGS)
    session = EngineSession(ctx, epoch)
    session.set_md_hook(broken)
    leader = await run_scenario(session, adapter, fx, epoch, backend)
    try:
        # SM10 evidence: every tick the session hands the calendar after the hook (the real `on_tick` still runs)
        fed: list[tuple[str, str, int]] = []
        real_on_tick = session.calendar.on_tick

        def on_tick(adapter_id: str, symbol: str, bid: Any, ask: Any, broker_ts_ms: int, now_ms: int, *,
                    snapshot: bool = False) -> None:
            fed.append((adapter_id, symbol, broker_ts_ms))
            real_on_tick(adapter_id, symbol, bid, ask, broker_ts_ms, now_ms, snapshot=snapshot)

        session.calendar.on_tick = on_tick  # type: ignore[method-assign]
        assert session.calendar.version is not None              # a schedule is loaded: the calendar path is live
        # then more ticks than the guard's trip count through the mock broker's price stream (the real price path)
        first = calls[0]
        n = MD_HOOK_TRIP_AFTER + 5
        t0 = wall_ms()
        for i in range(n):
            adapter.push_price(PriceDTO(adapter_id="mock", symbol="EURUSD", bid=Decimal("1.08") + Decimal(i) / 10_000,
                                        ask=Decimal("1.0802") + Decimal(i) / 10_000, spread_pips=None,
                                        timestamp_ms=t0 + i, ts_source="BROKER", received_ms=t0 + i))

        async def tripped() -> bool:
            return session._md_guard.trips == 1 and session._md_guard.skipped > 0

        await eventually(tripped, "the md hook guard tripped")
        assert first >= 1 and calls[0] == MD_HOOK_TRIP_AFTER          # then skipped for its window
        live = json_dump(await redis.hgetall(price_key("mock", "EURUSD")))
        assert live is not None and str(t0 + n - 1).encode() in live  # the price path kept running (Oversee's key)

        async def calendar_fed() -> bool:
            return [ts for a, s, ts in fed if (a, s) == ("mock", "EURUSD") and ts >= t0] == [t0 + i for i in range(n)]

        await eventually(calendar_fed, "the calendar saw every pushed tick")   # raising hook, tripped or skipped

        async def paged() -> bool:
            return "market-data hook failing" in [t for _, t in await backend.outbox()]

        await eventually(paged, "one page")
        titles = [t for _, t in await backend.outbox()]
        assert titles.count("market-data hook failing") == 1
        assert not exits and ("reached_leader", epoch.label) in backend.tables.terms
    finally:
        await leader.shutdown()
    assert not leader.exits and not exits


# ---------------------------------------------------------------------------------------------------- relays
async def test_the_oversee_relay_never_touches_the_runs_arm_rows(redis: Any) -> None:
    """The run's ARM notices and a HARD Oversee alert on one `notification_outbox`: the Oversee relay delivers its
    alert and leaves every `arm` row untouched; the ARM relay then delivers the ARM rows only."""
    e = await e2e_rig(redis=redis)
    try:
        await run_main_scenario(e)
    finally:
        e.barclock.close()
    emu = ArmOutboxEmulator(epoch=7)
    for a in e.emu.inserts:
        emu.add_outbox_row(kind="arm", channels=list(a[2]), created=a[1], title=a[3], body=a[4], next_attempt=a[1],
                           expires=a[5])
    db = engine_db(emu)
    now = max(a[1] for a in e.emu.inserts) + 1
    ov_email, ov_push = RecordingChannel(name="email"), RecordingChannel(name="mobile_push")
    oversee = OutboxRelay(PgOutboxStore(db), PgDedupeStore(db), [ov_email, ov_push], clock_ms=lambda: now)
    hard = OutboxRow("trip", "critical", "HARD MaxPositionRisk position:P9", "reason", now - 1)
    await PgOutboxStore(db).add([hard])
    res = await oversee.tick()
    assert res.delivered == 1 and [m.title for m in ov_email.sent] == [hard.title]
    assert all(r["sent_at"] is None and r["attempts"] == 0 for r in emu.arm_rows())
    # a second Oversee alert, still unsent when the ARM relay runs: the ARM relay must leave it alone
    pending = OutboxRow("trip", "critical", "HARD MaxPositionRisk position:P8", "reason", now - 1)
    await PgOutboxStore(db).add([pending])
    [waiting] = [r for r in emu.t.outbox if r["kind"] != "arm" and r["title"] == pending.title]
    assert waiting["sent_at"] is None and waiting["attempts"] == 0
    arm_email, arm_push = RecordingChannel(name="arm_email"), RecordingChannel(name="arm_push")
    relay = ArmRelay(PgArmOutboxStore(db), [arm_email, arm_push],
                     renderer=SendTimeRenderer(e.md.port, keyring=lambda: KEYRING), clock_ms=lambda: now)
    await relay.tick()
    assert arm_email.sent and all(m.title.startswith("ARM ") for m in arm_email.sent)
    assert pending.title not in [m.title for m in arm_email.sent + arm_push.sent]
    assert len(ov_email.sent) == 1 and all(r["sent_at"] is not None for r in emu.arm_rows())
    assert waiting["sent_at"] is None and waiting["attempts"] == 0           # the ARM relay never touched it


def json_dump(value: Any) -> bytes:
    """A Redis hash as one byte string (keys and values), for a containment check."""
    return _dumps({str(k): str(v) for k, v in dict(value or {}).items()}).encode()
