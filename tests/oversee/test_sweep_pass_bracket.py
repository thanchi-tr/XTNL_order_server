"""IMPLEMENTATION_PLAN 5.3: the RiskSweep tells the ruleset when a pass is running (`begin_pass` / `end_pass`), so
a staged ruleset installs only between passes; an `EpochMismatch` from any sweep write propagates (after
`on_fence_lost`, the runtime hard exit) and is never an account failure (oversee-rules.md §2, operations.md §2-§3).
Clock injected; in-memory store; no network."""
from __future__ import annotations

import asyncio
from collections.abc import Iterable, Sequence
from typing import Any

import pytest

from src.db.fence import EpochMismatch as DbEpochMismatch
from src.db.oversee_errors import SweepFenced
from src.oversee.config import OverseeConfig
from src.oversee.halt import HaltEvent, HaltSwitch
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.notify import NotificationDispatcher
from src.oversee.rules import RuleRegistry
from src.oversee.ruleset import ActiveRuleset, parse_ruleset
from src.oversee.snapshot import AccountSnapshot, BreachAck
from src.oversee.sweep import AccountWrite, EpochMismatch, RiskSweep, is_epoch_mismatch
from src.oversee.types import BreachState, HaltLevel
from tests.oversee.helpers import ACCT, AD, T0, live, position, quote, snap
from tests.oversee.test_sweep import CFG, Clock, Env

POS_KEY = ("max_position_risk", "position", f"{AD}:P")
SOFT_POS = (position("P", stop="1.0840"),)            # 1.6 % of 1000: max_position_risk SOFT at once


class Fenced(Env):
    """Env whose sweep reports a lost fence to `fences` (the runtime's hard exit in production)."""

    def __init__(self, cfg: OverseeConfig = CFG, db: InMemoryOverseeDb | None = None,
                 halt: HaltSwitch | None = None, dispatcher: NotificationDispatcher | None = None,
                 **kw: Any) -> None:
        super().__init__(cfg=cfg, db=db, **kw)
        self.fences: list[BaseException] = []
        self.halt = halt or self.halt
        self.sweep = RiskSweep(reader=self.db, writer=self.db, registry=RuleRegistry.from_config(cfg),
                               halt=self.halt, config=cfg, clock=self.clock, epoch=lambda: self.db.engine_epoch,
                               pager=self.pages.append, dispatcher=dispatcher, on_fence_lost=self.fences.append)


# ================================================================ EpochMismatch propagates
def test_is_epoch_mismatch_matches_every_variant_by_class_name_and_through_the_cause_chain() -> None:
    assert is_epoch_mismatch(EpochMismatch("x")) and is_epoch_mismatch(DbEpochMismatch("x"))
    assert is_epoch_mismatch(SweepFenced("x"))

    class EpochMismatchElsewhere(Exception):
        pass
    EpochMismatchElsewhere.__name__ = "EpochMismatch"
    assert is_epoch_mismatch(EpochMismatchElsewhere())
    try:
        try:
            raise DbEpochMismatch("lost")
        except DbEpochMismatch as inner:
            raise RuntimeError("wrapped") from inner
    except RuntimeError as outer:
        assert is_epoch_mismatch(outer)
    assert not is_epoch_mismatch(RuntimeError("boom")) and not is_epoch_mismatch(ConnectionError())


async def test_epoch_mismatch_on_the_account_write_propagates_and_is_not_an_account_failure() -> None:
    env = Fenced(positions=(position("P"),))
    await env.at(T0)
    env.db.engine_epoch = 2
    env.sweep._epoch = lambda: 1     # a paused ex-leader still on epoch 1
    with pytest.raises(EpochMismatch):
        await env.at(T0 + 1000)
    assert len(env.fences) == 1 and isinstance(env.fences[0], EpochMismatch)
    h = env.sweep.health(ACCT)
    assert h.first_failure_ms is None and h.last_error == "" and h.last_success_ms == T0
    assert "evaluation_failed" not in [p.code for p in env.pages]


async def test_db_epoch_mismatch_from_the_writer_propagates_even_when_not_the_oversee_class() -> None:
    class DbFenced(InMemoryOverseeDb):
        lost = False

        async def write_account(self, batch: AccountWrite) -> None:
            if self.lost:
                raise DbEpochMismatch("fenced write affected 0 rows")
            await super().write_account(batch)

    db = DbFenced()
    env = Fenced(db=db)
    await env.at(T0)
    db.lost = True
    with pytest.raises(DbEpochMismatch):
        await env.at(T0 + 1000)
    assert len(env.fences) == 1 and env.sweep.health(ACCT).first_failure_ms is None


async def test_epoch_mismatch_on_the_quarantine_release_propagates() -> None:
    class FencedQuarantine(InMemoryOverseeDb):
        async def set_quarantine(self, account_id: str, at_ms: int | None, reason: str) -> None:
            raise SweepFenced("lost")

    env = Fenced(db=FencedQuarantine())
    with pytest.raises(SweepFenced):
        await env.at(T0)                  # the first good pass releases the account's sweep_quarantine row
    assert env.fences and "quarantine_release_failed" not in [p.code for p in env.pages]


async def test_epoch_mismatch_on_the_quarantine_write_propagates() -> None:
    class FencedQuarantine(InMemoryOverseeDb):
        async def set_quarantine(self, account_id: str, at_ms: int | None, reason: str) -> None:
            if at_ms is not None:
                raise SweepFenced("lost")
            await super().set_quarantine(account_id, at_ms, reason)

    db = FencedQuarantine()
    env = Fenced(db=db)
    await env.at(T0)
    db.failing_accounts.add(ACCT)
    await env.at(T0 + 1000)
    with pytest.raises(SweepFenced):
        await env.at(T0 + CFG.sweep.quarantine_after_ms + 1000)
    assert env.fences


async def test_epoch_mismatch_on_the_evaluation_failed_write_propagates() -> None:
    class FencedFailureRow(InMemoryOverseeDb):
        async def write_account(self, batch: AccountWrite) -> None:
            if any(w.state.reason == "evaluation_failed" for w in batch.states):
                raise DbEpochMismatch("lost")
            await super().write_account(batch)

    db = FencedFailureRow()
    env = Fenced(db=db)
    await env.at(T0)
    db.failing_accounts.add(ACCT)
    await env.at(T0 + 1000)                      # an account fault: counted, nothing written yet
    assert env.sweep.health(ACCT).first_failure_ms == T0 + 1000
    with pytest.raises(DbEpochMismatch):
        await env.at(T0 + 1000 + CFG.sweep.evaluation_failed_after_ms)
    assert len(env.fences) == 1


async def test_epoch_mismatch_on_the_evaluation_failed_cas_retry_propagates() -> None:
    class FencedUpsert(HaltSwitch):
        async def upsert_state(self, key: Any, build: Any) -> BreachState | None:
            raise SweepFenced("lost")

    class CasMiss(InMemoryOverseeDb):
        async def write_account(self, batch: AccountWrite) -> None:
            if any(w.state.reason == "evaluation_failed" for w in batch.states):
                from src.oversee.sweep import CasConflict
                raise CasConflict("moved")
            await super().write_account(batch)

    db = CasMiss()
    env = Fenced(db=db, halt=FencedUpsert(db, db))
    await env.at(T0)
    db.failing_accounts.add(ACCT)
    await env.at(T0 + 1000)
    with pytest.raises(SweepFenced):
        await env.at(T0 + 1000 + CFG.sweep.evaluation_failed_after_ms)
    assert "evaluation_failed_unwritten" not in [p.code for p in env.pages]


async def test_epoch_mismatch_on_the_halt_trip_propagates_instead_of_paging_halt_decision_failed() -> None:
    class FencedTrip(HaltSwitch):
        async def trip(self, level: HaltLevel, **kw: Any) -> HaltEvent | None:
            raise DbEpochMismatch("lost")

    db = InMemoryOverseeDb()
    env = Fenced(cfg=OverseeConfig(), db=db, halt=FencedTrip(db, db), positions=SOFT_POS)
    with pytest.raises(DbEpochMismatch):
        await env.at(T0)
    assert env.fences and "halt_decision_failed" not in [p.code for p in env.pages]


async def test_epoch_mismatch_on_unlisted_state_resolution_propagates() -> None:
    class FencedResolve(HaltSwitch):
        async def resolve_unlisted(self, listed: Any, *, now_ms: int, sweep_id: int) -> tuple[BreachState, ...]:
            raise SweepFenced("lost")

    db = InMemoryOverseeDb()
    env = Fenced(db=db, halt=FencedResolve(db, db))
    with pytest.raises(SweepFenced):
        await env.at(T0)
    assert env.fences and "unlisted_states_unresolved" not in [p.code for p in env.pages]


async def test_epoch_mismatch_on_repage_queueing_propagates() -> None:
    class FencedDispatcher(NotificationDispatcher):
        async def queue_repages(self, states: Iterable[BreachState], acks: Sequence[BreachAck], now_ms: int) -> int:
            raise DbEpochMismatch("lost")

    db = InMemoryOverseeDb()
    env = Fenced(db=db, dispatcher=FencedDispatcher(db, db, (), escalate_after_ms=900_000))
    with pytest.raises(DbEpochMismatch):
        await env.at(T0)
    assert env.fences and "repage_failed" not in [p.code for p in env.pages]


async def test_other_write_errors_stay_account_failures() -> None:
    class Broken(InMemoryOverseeDb):
        broken = False

        async def write_account(self, batch: AccountWrite) -> None:
            if self.broken:
                raise RuntimeError("constraint violated")
            await super().write_account(batch)

    db = Broken()
    env = Fenced(db=db)
    await env.at(T0)
    db.broken = True
    r = await env.at(T0 + 1000)
    assert r.failed == [ACCT] and not env.fences


async def test_run_loop_ends_on_a_lost_fence() -> None:
    env = Fenced()
    env.db.set_snapshot(env.snapshot(T0))
    env.db.engine_epoch = 2
    env.sweep._epoch = lambda: 1
    with pytest.raises(EpochMismatch):
        await asyncio.wait_for(env.sweep.run(), 2.0)
    assert len(env.fences) == 1


# ================================================================ ruleset installs only between passes
def test_install_if_sweep_idle_waits_while_a_pass_is_running() -> None:
    clock = Clock(T0)
    active = ActiveRuleset(OverseeConfig(), clock_ms=clock, stale_install_ms=60_000)
    active.begin_pass()                               # a pass starts (stuck on PostgreSQL, say)
    clock.t += 1
    active.stage(parse_ruleset({}, 2))
    clock.t += 120_000
    assert active.pass_running and not active.install_if_sweep_idle() and active.version == 1
    active.end_pass()
    assert not active.pass_running and active.install_if_sweep_idle() and active.version == 2


def test_a_boundary_inside_a_pass_installs_nothing() -> None:
    clock = Clock(T0)
    active = ActiveRuleset(OverseeConfig(), clock_ms=clock)
    assert not active.begin_pass()
    active.stage(parse_ruleset({}, 2))
    ids = active.wrap_sweep_ids(lambda: 7)            # the compatibility wrapper, drawn mid-pass
    assert ids() == 7 and not active.at_boundary() and active.version == 1 and active.pending is not None
    active.end_pass()
    assert active.begin_pass() and active.version == 2 and active.pending is None
    active.end_pass()


def test_direct_install_during_a_pass_is_staged_for_the_next_boundary() -> None:
    clock = Clock(T0)
    active = ActiveRuleset(OverseeConfig(), clock_ms=clock)
    active.begin_pass()
    assert active.install(parse_ruleset({}, 3)) is False and active.version == 1 and active.pending is not None
    active.end_pass()
    assert active.install(parse_ruleset({}, 4)) is True and active.version == 4


def test_a_raising_install_at_begin_pass_still_counts_the_pass() -> None:
    class Broken(ActiveRuleset):
        def at_boundary(self) -> bool:
            raise RuntimeError("install failed")

    active = Broken(OverseeConfig(), clock_ms=Clock(T0))
    with pytest.raises(RuntimeError):
        active.begin_pass()
    assert active.pass_running
    active.end_pass()
    assert not active.pass_running


class _GatedReader:
    """SnapshotReader that holds `read_account` until `release` is set (a pass stuck on PostgreSQL)."""

    def __init__(self, db: InMemoryOverseeDb) -> None:
        self.db = db
        self.release = asyncio.Event()
        self.entered = asyncio.Event()

    async def list_accounts(self) -> Sequence[str]:
        return await self.db.list_accounts()

    async def read_account(self, account_id: str, now_ms: int) -> AccountSnapshot:
        self.entered.set()
        await self.release.wait()
        return await self.db.read_account(account_id, now_ms)


def _set(db: InMemoryOverseeDb, t: int) -> None:
    db.set_snapshot(snap(now=t, account_live=live("1000"), quotes={(AD, "EURUSD"): quote("1.1000", t)},
                         positions=SOFT_POS))


async def test_a_slow_pass_runs_on_one_ruleset_and_the_staged_one_installs_after_it() -> None:
    clock = Clock(T0)
    db = InMemoryOverseeDb()
    reader = _GatedReader(db)
    active = ActiveRuleset(OverseeConfig(), clock_ms=clock, stale_install_ms=60_000)
    sweep = RiskSweep(reader=reader, writer=db, registry=active.registry, halt=HaltSwitch(db, db),
                      config=OverseeConfig(), clock=clock, epoch=lambda: db.engine_epoch)
    sweep.install_ruleset(active)
    _set(db, T0)
    reader.release.set()
    await sweep.run_pass()
    assert db.t.breaches[POS_KEY].active and not active.pass_running
    reader.release.clear()
    reader.entered.clear()
    task = asyncio.create_task(sweep.run_pass())
    await reader.entered.wait()                       # the pass is mid-way
    assert active.pass_running and sweep.pass_running
    active.stage(parse_ruleset({"max_position_risk": {"enabled": False}}, 2))
    clock.t += 120_000                                # far past stale_install_ms: still no install mid-pass
    assert not active.install_if_sweep_idle() and active.version == 1
    reader.release.set()
    await task
    assert active.version == 1 and db.t.breaches[POS_KEY].resolved_by is None   # the pass kept v1's rules
    assert not active.pass_running
    clock.t += 1000
    _set(db, clock.t)
    await sweep.run_pass()                            # the next boundary installs v2
    assert active.version == 2 and db.t.breaches[POS_KEY].resolved_by == "reload"


async def test_end_pass_runs_when_the_pass_raises() -> None:
    clock = Clock(T0)
    db = InMemoryOverseeDb()
    active = ActiveRuleset(OverseeConfig(), clock_ms=clock)
    fences: list[BaseException] = []
    sweep = RiskSweep(reader=db, writer=db, registry=active.registry, halt=HaltSwitch(db, db),
                      config=OverseeConfig(), clock=clock, epoch=lambda: 1, on_fence_lost=fences.append)
    sweep.install_ruleset(active)
    _set(db, T0)
    db.engine_epoch = 2
    with pytest.raises(EpochMismatch):
        await sweep.run_pass()
    assert not active.pass_running and not sweep.pass_running and len(fences) == 1


async def test_end_pass_runs_when_the_pass_is_cancelled() -> None:
    clock = Clock(T0)
    db = InMemoryOverseeDb()
    reader = _GatedReader(db)
    active = ActiveRuleset(OverseeConfig(), clock_ms=clock)
    sweep = RiskSweep(reader=reader, writer=db, registry=active.registry, halt=HaltSwitch(db, db),
                      config=OverseeConfig(), clock=clock, epoch=lambda: db.engine_epoch)
    sweep.install_ruleset(active)
    _set(db, T0)
    task = asyncio.create_task(sweep.run_pass())
    await reader.entered.wait()
    assert active.pass_running
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not active.pass_running and not sweep.pass_running


async def test_install_ruleset_during_a_pass_takes_effect_from_the_next_pass() -> None:
    clock = Clock(T0)
    db = InMemoryOverseeDb()
    reader = _GatedReader(db)
    old = ActiveRuleset(OverseeConfig(), clock_ms=clock)
    sweep = RiskSweep(reader=reader, writer=db, registry=old.registry, halt=HaltSwitch(db, db),
                      config=OverseeConfig(), clock=clock, epoch=lambda: db.engine_epoch)
    sweep.install_ruleset(old)
    _set(db, T0)
    task = asyncio.create_task(sweep.run_pass())
    await reader.entered.wait()
    new = ActiveRuleset(parse_ruleset({"max_position_risk": {"enabled": False}}, 2), clock_ms=clock)
    sweep.install_ruleset(new)                        # mid-pass: the running pass keeps the old one
    reader.release.set()
    await task
    assert db.t.breaches[POS_KEY].active               # evaluated with the old ruleset's rules
    assert not old.pass_running and not new.pass_running
    clock.t += 1000
    _set(db, clock.t)
    await sweep.run_pass()
    assert db.t.breaches[POS_KEY].resolved_by == "reload"


async def test_a_raising_pager_in_the_bracket_never_leaves_the_pass_counted_as_running() -> None:
    """A failing install pages `ruleset_install_failed`; a pager that raises there must not escape the bracket
    (the pass would stay counted as running and no ruleset could ever install again)."""
    class Broken(ActiveRuleset):
        broken = True

        def at_boundary(self) -> bool:
            if self.broken:
                raise RuntimeError("install failed")
            return super().at_boundary()

    def pager(page: Any) -> None:
        raise RuntimeError("pager down")

    clock = Clock(T0)
    db = InMemoryOverseeDb()
    active = Broken(OverseeConfig(), clock_ms=clock)
    sweep = RiskSweep(reader=db, writer=db, registry=active.registry, halt=HaltSwitch(db, db),
                      config=OverseeConfig(), clock=clock, epoch=lambda: db.engine_epoch, pager=pager)
    sweep.install_ruleset(active)
    _set(db, T0)
    report = await sweep.run_pass()
    assert report.succeeded == [ACCT]
    assert not active.pass_running and not sweep.pass_running
    active.stage(parse_ruleset({}, 2))
    active.broken = False
    clock.t += 1000
    _set(db, clock.t)
    await sweep.run_pass()
    assert active.version == 2
