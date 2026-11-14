"""Protocol conformance: the PostgreSQL Oversee stores behave like src/oversee/memory.py.

Every scenario runs against InMemoryOverseeDb (the reference) and against the PostgreSQL stores over
`PgEmulator` (the stores' exact SQL with emulated table semantics); the observable results must match.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal
from typing import Any

import fakeredis
import pytest

from src.cache.halt_mirror import RedisHaltMirror
from src.cache.scripts import RedisState
from src.db.epoch import EngineDb, Epoch
from src.db.halt_store import PgHaltStore
from src.db.notify_store import PgDedupeStore, PgOutboxStore
from src.db.quarantine import PgQuarantineSink
from src.db.rows import breach_from_row
from src.db.sweep_writer import PgSweepWriter
from src.oversee.feed import FeedEntry
from src.oversee.halt import HaltMirror, HaltStore, HaltSwitch
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.notify import DedupeStore, OutboxRow, OutboxStore, episode_key
from src.oversee.snapshot import DependencyUnavailable, SodMark, TimedMid
from src.oversee.sweep import AccountWrite, CasConflict, EpochMismatch, StateWrite, SweepWriter
from src.oversee.types import BreachState, Cap, HaltLevel, Level, Transition

from tests.db.pg_emulator import COLS, PgEmulator

ACC = "ACC"


@dataclass
class Backend:
    name: str
    writer: SweepWriter
    halt: HaltStore
    mirror: HaltMirror
    outbox: OutboxStore
    dedupe: DedupeStore
    mem: InMemoryOverseeDb | None = None
    emu: PgEmulator | None = None

    def states(self) -> dict[tuple[str, str, str], BreachState]:
        if self.mem is not None:
            return dict(self.mem.t.breaches)
        assert self.emu is not None
        rows = [breach_from_row(tuple(r[c] for c in COLS)) for r in self.emu.t.breaches.values()]
        return {s.key: s for s in rows}

    def stop_watch(self) -> dict[tuple[str, str], int]:
        if self.mem is not None:
            return dict(self.mem.t.stop_watch)
        assert self.emu is not None
        return {(k[0].lower(), k[1]): v for k, v in self.emu.t.stop_watch.items()}

    def n_events(self) -> int:
        if self.mem is not None:
            return len(self.mem.t.events)
        assert self.emu is not None
        return len(self.emu.t.events)

    def n_outbox(self) -> int:
        if self.mem is not None:
            return len(self.mem.t.outbox)
        assert self.emu is not None
        return len(self.emu.t.outbox)

    def go_down(self) -> None:
        if self.mem is not None:
            self.mem.dependency_down = True
        else:
            assert self.emu is not None
            self.emu.down = True


@pytest.fixture(params=["memory", "pg"])
async def backend(request: pytest.FixtureRequest) -> AsyncIterator[Backend]:
    if request.param == "memory":
        mem = InMemoryOverseeDb(engine_epoch=7)
        yield Backend("memory", mem, mem, mem, mem, mem, mem=mem)
        return
    emu = PgEmulator(epoch=7)
    db = EngineDb(emu, lambda: Epoch("c", 7))
    redis = fakeredis.FakeAsyncRedis()
    yield Backend("pg", PgSweepWriter(db), PgHaltStore(db), RedisHaltMirror(RedisState(redis, lambda: "b")),
                  PgOutboxStore(db), PgDedupeStore(db), emu=emu)
    await redis.aclose()


def state(**kw: Any) -> BreachState:
    base: dict[str, Any] = dict(rule_id="max_position_risk", subject_type="order", subject_id="stonex:B1",
                                account_id=ACC, level=Level.SOFT_HALT, cap=Cap.NONE, reason="risk 1.6%",
                                active_since=1_000, activation_id=10, last_pass_ms=1_000, last_sweep_id=10)
    base.update(kw)
    return BreachState(**base)


def batch(*writes: StateWrite, epoch: int = 7, sweep_id: int = 11, **kw: Any) -> AccountWrite:
    return AccountWrite(account_id=ACC, epoch=epoch, sweep_id=sweep_id, states=writes, **kw)


def transition(s: BreachState, kind: str = "activated", sweep_id: int = 10) -> Transition:
    return Transition(s.rule_id, s.subject_type, s.subject_id, s.account_id, kind,  # type: ignore[arg-type]
                      sweep_id, 1_000, s.level, s.cap, s.reason, s.activation_id)


async def test_insert_then_compare_and_set(backend: Backend) -> None:
    s1 = state()
    await backend.writer.write_account(batch(StateWrite(s1, None, None)))
    assert backend.states()[s1.key] == s1
    s2 = replace(s1, level=Level.HARD_HALT, last_sweep_id=11, uncapped_ms=500)
    await backend.writer.write_account(batch(StateWrite(s2, 10, 0)))
    assert backend.states()[s1.key] == s2
    with pytest.raises(CasConflict):   # built from the older row
        await backend.writer.write_account(batch(StateWrite(replace(s2, reason="x"), 10, 0)))
    with pytest.raises(CasConflict):   # "must not exist yet"
        await backend.writer.write_account(batch(StateWrite(s1, None, None)))
    with pytest.raises(CasConflict):   # missing row with an expectation
        await backend.writer.write_account(batch(StateWrite(state(subject_id="stonex:B9"), 3, 0)))
    assert backend.states()[s1.key] == s2


async def test_operator_generation_bump_fails_cas(backend: Backend) -> None:
    s1 = state()
    await backend.writer.write_account(batch(StateWrite(s1, None, None)))
    async with backend.halt.transaction() as tx:   # operator clear bumps clear_generation
        await tx.save_states([replace(s1, clear_generation=1, uncapped_ms=0)])
    with pytest.raises(CasConflict):
        await backend.writer.write_account(batch(StateWrite(replace(s1, last_sweep_id=11), 10, 0)))
    await backend.writer.write_account(batch(StateWrite(replace(s1, last_sweep_id=11, clear_generation=1), 10, 1)))
    assert backend.states()[s1.key].last_sweep_id == 11


async def test_batch_is_atomic(backend: Backend) -> None:
    good = state(subject_id="stonex:B2")
    bad = StateWrite(state(), 99, 0)   # row does not exist with that expectation
    with pytest.raises(CasConflict):
        await backend.writer.write_account(batch(StateWrite(good, None, None), bad,
                                                 events=(transition(good),),
                                                 outbox=(OutboxRow("info", "info", "t", "b", 1),)))
    assert backend.states() == {} and backend.n_events() == 0 and backend.n_outbox() == 0


async def test_fenced_writer_raises_epoch_mismatch(backend: Backend) -> None:
    with pytest.raises(EpochMismatch):
        await backend.writer.write_account(batch(StateWrite(state(), None, None), epoch=6))
    with pytest.raises(EpochMismatch):
        await backend.writer.write_account(batch(epoch=6))   # even an empty batch confirms the fence
    assert backend.states() == {}


async def test_a_lost_fence_on_an_empty_batch_reaches_the_guarded_engine_db() -> None:
    """IMPLEMENTATION_PLAN 5.3: the runtime hard exit (GuardedEngineDb `on_fence_lost`) sees a lost fence
    found by the empty-batch check too (a quiet account writes nothing but still confirms the fence)."""
    from src.runtime.pg_ports import GuardedEngineDb
    emu = PgEmulator(epoch=7)
    fences: list[BaseException] = []
    writer = PgSweepWriter(GuardedEngineDb(emu, lambda: Epoch("c", 7), fences.append))
    await writer.write_account(batch(epoch=7))          # fence held: nothing reported
    assert fences == []
    with pytest.raises(EpochMismatch):
        await writer.write_account(batch(epoch=6))
    assert len(fences) == 1
    with pytest.raises(EpochMismatch):
        await writer.write_account(batch(StateWrite(state(), None, None), epoch=6))
    assert len(fences) == 2


async def test_stop_watch_keeps_first_deadline(backend: Backend) -> None:
    key = ("order", "stonex:B1")
    await backend.writer.write_account(batch(stop_watch_set=((key, 100),)))
    await backend.writer.write_account(batch(stop_watch_set=((key, 200),)))
    assert backend.stop_watch() == {key: 100}
    await backend.writer.write_account(batch(stop_watch_clear=(key,)))
    assert backend.stop_watch() == {}


async def test_events_are_idempotent(backend: Backend) -> None:
    s = state()
    await backend.writer.write_account(batch(events=(transition(s),)))
    await backend.writer.write_account(batch(events=(transition(s),)))
    await backend.writer.write_account(batch(events=(transition(s, "raised"),)))
    assert backend.n_events() == 2


async def test_sod_mark_is_written_once(backend: Backend) -> None:
    sod = SodMark(ACC, date(2026, 11, 13), 1_000, Decimal("100"), Decimal("101.5"), Decimal(0), False, 1_001,
                  marks={("stonex", "EURUSD"): TimedMid(Decimal("1.1"), 999)})
    await backend.writer.write_account(batch(sod_mark=sod))
    await backend.writer.write_account(batch(sod_mark=replace(sod, sod_equity=Decimal(5))))
    if backend.mem is not None:
        assert backend.mem.t.sod_marks[(ACC, sod.trading_day)].sod_equity == Decimal("101.5")
    else:
        assert backend.emu is not None
        (row,) = backend.emu.t.sod.values()
        assert row[4] == Decimal("101.5") and '"symbol":"EURUSD"' in row[8]


async def test_dependency_down(backend: Backend) -> None:
    backend.go_down()
    with pytest.raises(DependencyUnavailable):
        await backend.writer.write_account(batch(StateWrite(state(), None, None)))
    with pytest.raises(DependencyUnavailable):
        async with backend.halt.transaction():
            pass


async def test_quarantine_set_and_release(backend: Backend) -> None:
    await backend.writer.set_quarantine(ACC, 5_000, "consecutive pass failures")
    held = backend.mem.t.quarantine if backend.mem is not None else backend.emu.t.quarantine  # type: ignore[union-attr]
    assert ACC in held
    await backend.writer.set_quarantine(ACC, None, "")
    assert ACC not in held


async def test_halt_switch_sequence(backend: Backend) -> None:
    hs = HaltSwitch(backend.halt, backend.mirror)
    soft = await hs.trip(HaltLevel.SOFT, rule_id="r", reason="soft", now_ms=1_000)
    assert soft is not None and (soft.id, soft.effective_level, soft.hard_latch) == (1, HaltLevel.SOFT, False)
    assert await hs.trip(HaltLevel.SOFT, rule_id="r", reason="again", now_ms=1_100) is None   # no rise, no row
    hard = await hs.trip(HaltLevel.HARD, rule_id="r", reason="hard", now_ms=2_000)
    assert hard is not None and hard.hard_latch and hard.id == 2
    assert await hs.system_clear(reason="cool_down", now_ms=3_000) is None                  # latch holds
    st = await hs.status()
    assert (st.effective_level, st.hard_latch, st.event_id, st.rose_at_ms) == (HaltLevel.HARD, True, 2, 2_000)
    # an active HARD state blocks the operator clear unless forced and capped / unknown
    hard_state = state(level=Level.HARD_HALT, cap=Cap.SOFT, subject_id="stonex:B7")
    await backend.writer.write_account(batch(StateWrite(hard_state, None, None)))
    refused = await hs.operator_clear("op", "reviewed", 2, now_ms=4_000)
    assert refused.status == "refused" and refused.reason.startswith("active_hard_state")
    assert (await hs.operator_clear("op", "x", 1, now_ms=4_000)).reason == "newer_halt_event:2"
    ack = await hs.acknowledge("op", "seen", hard_state.key, now_ms=4_100)
    assert (ack.rule_id, ack.level, ack.operator) == (hard_state.rule_id, Level.HARD_HALT, "op")
    with pytest.raises(LookupError):
        await hs.acknowledge("op", "seen", ("nope", "order", "x"), now_ms=4_100)
    cleared = await hs.operator_clear("op", "capped, reviewed", 2, force=True, now_ms=5_000)
    assert cleared.status == "cleared" and cleared.event is not None
    assert (cleared.event.id, cleared.event.effective_level, cleared.event.hard_latch, cleared.event.by) == (
        3, HaltLevel.NONE, False, "op")
    resolved = backend.states()[hard_state.key]
    assert resolved.resolved_by == "operator" and resolved.clear_generation == 1 and resolved.level is Level.PASS
    st = await hs.status()
    assert (st.effective_level, st.hard_latch, st.event_id) == (HaltLevel.NONE, False, 3)
    assert await hs.trip(HaltLevel.SOFT, rule_id="r", reason="old basis", now_ms=6_000, basis_ms=4_999) is None


async def test_halt_mirror_is_cas(backend: Backend) -> None:
    assert await backend.mirror.cas_set(HaltLevel.SOFT, 2) is True
    assert await backend.mirror.cas_set(HaltLevel.HARD, 1) is False
    assert await backend.mirror.cas_set(HaltLevel.NONE, 3) is True


async def test_outbox_and_dedupe(backend: Backend) -> None:
    key = episode_key("r", "order", "stonex:B1", 10, Level.SOFT_HALT)
    await backend.outbox.add([OutboxRow("episode", "high", "SOFT r", "b", 1_000, key, ACC),
                              OutboxRow("trip", "critical", "HALT HARD", "b", 1_001)])
    await backend.outbox.add([])
    rows = await backend.outbox.pending(10)
    assert [(r.kind, r.severity, r.title, r.episode, r.account_id) for r in rows] == [
        ("episode", "high", "SOFT r", key, ACC), ("trip", "critical", "HALT HARD", None, None)]
    assert rows[1].bypass_dedupe and all(r.outbox_id is not None for r in rows)
    assert len(await backend.outbox.pending(1)) == 1
    await backend.outbox.mark_sent(rows[0].outbox_id or 0, 2_000, delivered=True)
    assert [r.title for r in await backend.outbox.pending(10)] == ["HALT HARD"]
    assert await backend.dedupe.last_sent(key) is None
    await backend.dedupe.record(key, 2_000)
    await backend.dedupe.record(key, 3_000)
    assert await backend.dedupe.last_sent(key) == 3_000
    assert dict(await backend.dedupe.snapshot()) == {key: 3_000}


async def test_pg_quarantine_sink() -> None:
    emu = PgEmulator(epoch=7)
    sink = PgQuarantineSink(EngineDb(emu, lambda: Epoch("c", 7)))
    await sink.quarantine(FeedEntry("1-0", {"account_id": ACC, "entity_type": "ORDER"}, 5), "max_deliveries")
    (row,) = emu.t.feed_quarantine
    assert row[0] == "1-0" and row[1] == "ORDER" and row[3] == ACC and row[5] == "max_deliveries" and row[6] == 5
    fenced = PgQuarantineSink(EngineDb(emu, lambda: Epoch("c", 6)))
    with pytest.raises(EpochMismatch):
        await fenced.quarantine(FeedEntry("2-0", {"account_id": ACC}, 5), "max_deliveries")


async def test_resolve_unlisted_resolves_only_unlisted_active_states(backend: Backend) -> None:
    gone = state(subject_id="stonex:G1", account_id="GONE")
    kept = state()
    await backend.writer.write_account(batch(StateWrite(gone, None, None)))
    await backend.writer.write_account(batch(StateWrite(kept, None, None)))
    switch = HaltSwitch(backend.halt, backend.mirror)
    events = backend.n_events()
    resolved = await switch.resolve_unlisted({ACC}, now_ms=5_000, sweep_id=20)
    assert [s.key for s in resolved] == [gone.key]
    after = backend.states()
    assert after[gone.key].resolved_by == "entity_end" and after[gone.key].below_soft_since == 5_000
    assert after[gone.key].clear_generation == 1 and not after[gone.key].active
    assert after[kept.key] == kept
    assert backend.n_events() == events + 1
    assert await switch.resolve_unlisted({ACC}, now_ms=6_000, sweep_id=21) == ()


async def test_upsert_state_writes_over_the_row_read_under_halt_lock(backend: Backend) -> None:
    """4.9: the RiskSweep's retry of a lost evaluation_failed CAS (HaltSwitch.upsert_state)."""
    failed = state(rule_id="evaluation", subject_type="account", subject_id=ACC, cap=Cap.SOFT,
                   reason="evaluation_failed", last_sweep_id=30)
    switch = HaltSwitch(backend.halt, backend.mirror)
    seen: list[BreachState | None] = []

    def build(current: BreachState | None) -> tuple[BreachState, list[Transition]] | None:
        seen.append(current)
        return None if current is not None and current.active else (failed, [transition(failed, sweep_id=30)])

    written = await switch.upsert_state(failed.key, build)                 # no row yet: inserted
    assert written == replace(failed, clear_generation=1) and backend.states()[failed.key] == written
    assert seen == [None] and backend.n_events() == 1 and backend.n_outbox() == 0
    assert await switch.upsert_state(failed.key, build) == written         # stored active: kept as is
    assert seen[-1] == written and backend.states()[failed.key] == written
    async with backend.halt.transaction() as tx:                           # bumped elsewhere: still written over
        await tx.save_states([replace(written, level=Level.PASS, resolved_at=2_000, clear_generation=4)])
    again = await switch.upsert_state(failed.key, build)
    assert again is not None and again.active and backend.states()[failed.key] == again
    await backend.writer.set_quarantine(ACC, None, "first good pass")       # releasing a missing row is a no-op


@pytest.mark.parametrize("name",["SQL_BREACH_INSERT", "SQL_BREACH_UPSERT", "SQL_EVENTS", "SQL_STOP_WATCH_SET",
                                  "SQL_QUARANTINE_SET"])
def test_adapter_id_falls_back_when_the_account_has_no_account_state_row(name: str) -> None:
    """The NOT NULL adapter_id of an account listed only through its positions or orders, or no longer listed
    (HaltSwitch.resolve_unlisted re-saves its rows), comes from its entities or its existing halt_breaches
    rows: an account_state-only lookup made every write for such an account a not-null violation."""
    from src.db import sweep_writer as W

    sql: str = getattr(W, name)
    assert "COALESCE((SELECT adapter_id FROM account_state" in sql
    for table in ("positions_state", "orders_state", "halt_breaches WHERE account_id"):
        assert f"SELECT adapter_id FROM {table}" in sql
    assert sql.count("(") == sql.count(")")


async def test_pg_halt_txn_reads_the_acks_of_unresolved_states_by_key() -> None:
    """6.3: the RiskSweep reads the acks of a failing account's evaluation_failed state outside a snapshot."""
    from src.db.halt_store import SQL_ACKS_OF, PgHaltTxn

    class Tx:
        calls: list[tuple[str, tuple[Any, ...]]] = []

        async def fetch(self, sql: str, *args: Any) -> list[tuple[Any, ...]]:
            self.calls.append((sql, args))
            return [(4, "evaluation", "ACCOUNT", ACC, "SOFT", "SOFT", "evaluation_failed", "op", "known", 9_000)]

    tx = Tx()
    htx = PgHaltTxn(tx)  # type: ignore[arg-type]
    assert await htx.acks([]) == []
    (ack,) = await htx.acks([("evaluation", "account", ACC)])
    assert (ack.ack_id, ack.subject_type, ack.level, ack.cap, ack.reason) == (
        4, "account", Level.SOFT_HALT, Cap.SOFT, "evaluation_failed")
    ((sql, args),) = tx.calls
    assert sql == SQL_ACKS_OF and "resolved_at IS NULL" in sql and not sql.lstrip().upper().startswith("INSERT")
    assert args == (["evaluation"], ["ACCOUNT"], [ACC])


async def test_memory_halt_txn_acks_match_the_pg_contract() -> None:
    mem = InMemoryOverseeDb(engine_epoch=7)
    s = state(rule_id="evaluation", subject_type="account", subject_id=ACC, cap=Cap.SOFT, reason="evaluation_failed")
    await mem.write_account(batch(StateWrite(s, None, None)))
    hs = HaltSwitch(mem, mem)
    await hs.acknowledge("op", "known", s.key, 2_000)
    assert [a.operator for a in await hs.acks_for([s.key])] == ["op"]
    assert await hs.acks_for([("other", "account", ACC)]) == [] and await hs.acks_for([]) == []
    mem.t.breaches[s.key] = replace(s, resolved_at=3_000)
    assert await hs.acks_for([s.key]) == []                       # a resolved state's acks are not returned


async def test_acks_for_matches_on_both_backends(backend: Backend) -> None:
    """7.2: HaltSwitch.acks_for (PgHaltTxn.acks -> SQL_ACKS_OF) against the emulator, with the same observable
    results as the in-memory store: only the listed keys, only unresolved states, oldest ack first."""
    failing = state(rule_id="evaluation", subject_type="account", subject_id=ACC, cap=Cap.SOFT,
                    reason="evaluation_failed")
    other = state(subject_id="stonex:B2")
    await backend.writer.write_account(batch(StateWrite(failing, None, None)))
    await backend.writer.write_account(batch(StateWrite(other, None, None)))
    hs = HaltSwitch(backend.halt, backend.mirror)
    await hs.acknowledge("op1", "known", failing.key, now_ms=2_000)
    await hs.acknowledge("op2", "still known", failing.key, now_ms=2_500)
    await hs.acknowledge("op3", "other", other.key, now_ms=2_600)
    acks = await hs.acks_for([failing.key])
    assert [(a.operator, a.subject_type, a.level, a.cap, a.reason, a.at_ms) for a in acks] == [
        ("op1", "account", Level.SOFT_HALT, Cap.SOFT, "evaluation_failed", 2_000),
        ("op2", "account", Level.SOFT_HALT, Cap.SOFT, "evaluation_failed", 2_500)]
    assert [a.operator for a in await hs.acks_for([failing.key, other.key])] == ["op1", "op2", "op3"]
    assert await hs.acks_for([("nope", "order", "x")]) == [] and await hs.acks_for([]) == []
    async with backend.halt.transaction() as tx:                       # resolved: its acks no longer count
        await tx.save_states([replace(failing, level=Level.PASS, resolved_at=3_000)])
    assert [a.operator for a in await hs.acks_for([failing.key, other.key])] == ["op3"]
