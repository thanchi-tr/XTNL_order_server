"""SM3 incident path (tier-persistence.md §3 step 2, operations.md §7): every transition."""
from __future__ import annotations

from typing import Any

import pytest

from src.cache import keys as K
from src.integrity.dbcheck import DbFinding
from src.integrity.incident import IncidentMachine
from src.integrity.ports import IntegrityState as S
from tests.integrity.fakes import FakeBoot, FakeIngestion, FakePatroni, FakeReconciler, ManualClock, Pages

DAMAGE = [DbFinding("checksum", "order:stonex:A", "stored != computed")]
FAILOVER = [DbFinding("failover_regression", "seq_id", "5000 -> 4000")]


class Env:
    def __init__(self, redis: Any) -> None:
        self.redis = redis
        self.ingestion = FakeIngestion()
        self.patroni = FakePatroni()
        self.reconciler = FakeReconciler()
        self.boot = FakeBoot()
        self.pages = Pages()
        self.ready = False
        self.changes: list[tuple[S | None, S | None]] = []
        self.resets = 0

        async def boot_ready() -> bool:
            return self.ready

        def on_resolved() -> None:
            self.resets += 1

        self.m = IncidentMachine(redis=redis, ingestion=self.ingestion, patroni=self.patroni,
                                 reconciler=self.reconciler, boot=self.boot, boot_ready=boot_ready,
                                 clock_ms=ManualClock(), pager=self.pages,
                                 on_change=lambda a, b, _w: self.changes.append((a, b)), on_resolved=on_resolved)


async def test_primary_only_damage_switches_over_and_recovers(redis: Any) -> None:
    e = Env(redis)
    await redis.set(K.BOOT_ID, "x")
    await e.m.open(DAMAGE)
    assert e.m.state is S.HALTED and e.ingestion.calls == ["halt"]
    assert e.pages.alerts[0].severity == "critical" and e.pages.alerts[0].code == "integrity_halted"
    await e.m.open(DAMAGE)                       # already open: no second halt
    assert e.ingestion.calls == ["halt"]
    await e.m.advance()
    assert e.m.state is S.SWITCHING_OVER and e.m.damaged_member == "pg-a"
    await e.m.advance()
    assert e.m.state is S.RECONCILING
    assert e.patroni.calls == ["primary", "standby_check", "switchover", "reinit:pg-a"]
    assert e.resets == 1
    await e.m.advance()                          # prices first, reconcile, FLUSHDB, wait for SM8
    assert e.ingestion.calls == ["halt", "resume_prices"]
    assert e.reconciler.calls == [None]
    assert await redis.get(K.BOOT_ID) is None and e.boot.signals
    assert e.m.state is S.RECONCILING and e.m.phase == "await_sm8"
    await e.m.advance()
    assert e.m.state is S.RECONCILING            # SM8 not READY yet: ingestion stays partial
    e.ready = True
    await e.m.advance()
    assert e.m.state is None and not e.m.active
    assert e.ingestion.calls == ["halt", "resume_prices", "resume_all"]
    assert e.pages.codes()[-1] == "integrity_resolved"
    assert [b for _a, b in e.changes] == [S.HALTED, S.SWITCHING_OVER, S.RECONCILING, None]


async def test_standby_damaged_needs_operator(redis: Any) -> None:
    e = Env(redis)
    e.patroni.standby_ok = False
    await e.m.open(DAMAGE)
    await e.m.advance()
    assert e.m.state is S.MANUAL_INTERVENTION
    assert e.pages.codes()[-1] == "integrity_manual_intervention"
    await e.m.advance()                          # waits for the operator; nothing happens
    assert e.m.state is S.MANUAL_INTERVENTION and e.reconciler.calls == []
    await e.m.operator_restored(1_699_000_000_000)
    assert e.m.state is S.RECONCILING
    e.ready = True
    await e.m.advance()
    assert e.reconciler.calls == [1_699_000_000_000]   # cash flows since the restore target
    assert e.m.state is None


async def test_switchover_failure_needs_operator(redis: Any) -> None:
    e = Env(redis)
    e.patroni.switchover_error = RuntimeError("no sync standby")
    await e.m.open(DAMAGE)
    await e.m.advance()
    await e.m.advance()
    assert e.m.state is S.MANUAL_INTERVENTION


async def test_reinit_failure_pages_but_continues(redis: Any) -> None:
    e = Env(redis)
    e.patroni.reinit_error = RuntimeError("reinit failed")
    await e.m.open(DAMAGE)
    await e.m.advance()
    await e.m.advance()
    assert e.m.state is S.RECONCILING
    assert "patroni_reinit_failed" in e.pages.codes()


async def test_patroni_unreachable_three_times_needs_operator(redis: Any) -> None:
    e = Env(redis)
    e.patroni.check_error = ConnectionError("patroni api down")
    await e.m.open(DAMAGE)
    await e.m.advance()
    await e.m.advance()
    assert e.m.state is S.HALTED
    await e.m.advance()
    assert e.m.state is S.MANUAL_INTERVENTION


async def test_divergence_persists_needs_operator(redis: Any) -> None:
    e = Env(redis)
    e.reconciler.results = [False, RuntimeError("broker 500"), False]
    await e.m.open(FAILOVER)
    await e.m.advance()
    assert e.m.state is S.RECONCILING and e.patroni.calls == []
    await e.m.advance()
    await e.m.advance()
    assert e.m.state is S.RECONCILING
    await e.m.advance()
    assert e.m.state is S.MANUAL_INTERVENTION
    assert "divergence persists" in e.pages.alerts[-1].detail


async def test_reconcile_retry_succeeds(redis: Any) -> None:
    e = Env(redis)
    e.ready = True
    e.reconciler.results = [False]
    await e.m.open(FAILOVER)
    await e.m.advance()
    await e.m.advance()
    assert e.m.state is S.RECONCILING and e.m.phase == "reconcile"
    await e.m.advance()
    assert e.m.state is None


async def test_operator_restored_only_from_manual(redis: Any) -> None:
    e = Env(redis)
    with pytest.raises(RuntimeError):
        await e.m.operator_restored(None)
    await e.m.open(DAMAGE)
    with pytest.raises(RuntimeError):
        await e.m.operator_restored(None)
