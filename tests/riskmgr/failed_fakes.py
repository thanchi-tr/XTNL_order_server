"""Fakes for the error-budget and FAILED tests (S30a). Synthetic adapters, symbols, ids and times only."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field, replace

from src.calendar.machine import SessionState
from src.db.arm_failures_store import PgArmFailuresStore
from src.db.arm_ports import ArmFailuresStore, ArmPositionRow, FailedPing, FailureRecord, OpenFailure
from src.db.epoch import EngineDb, Epoch
from src.marketdata.types import M15, SeriesId, Source
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.budget import ErrorBudget
from src.riskmgr.config import FailSettings
from src.riskmgr.failed import FailedManager, FailedSettings, FailedStores
from src.riskmgr.ports import FailedSpoolRecord, Pager
from tests.db.arm_emulator import ArmEmulator
from tests.riskmgr.barclock_fakes import arm_row
from tests.riskmgr.tracker_fakes import ADAPTER, SYMBOL, FakeClock, FakeStore

T0 = 1_700_000_100_000 - 1_700_000_100_000 % 900_000
EPOCH = 7


def series(symbol: str = SYMBOL, *, adapter: str = ADAPTER, side: QuoteSide = QuoteSide.BID,
           source: Source = Source.BROKER) -> SeriesId:
    return SeriesId(adapter, symbol, M15, side, source)


@dataclass
class FakeCalendar:
    closed: set[tuple[str, str]] = field(default_factory=set)

    def state(self, adapter_id: str, symbol: str) -> SessionState | None:
        return SessionState.CLOSED if (adapter_id, symbol) in self.closed else SessionState.OPEN


@dataclass
class FakeHealth:
    down: set[str] = field(default_factory=set)
    rest_failing: dict[str, bool | None] = field(default_factory=dict)

    def is_streaming(self, adapter_id: str) -> bool:
        return adapter_id not in self.down

    def rest_failing_everywhere(self, adapter_id: str) -> bool | None:
        return self.rest_failing.get(adapter_id, False)


@dataclass
class FakePg:
    clock: FakeClock
    down: bool = False
    unreachable: bool = False

    def last_fenced_commit_ms(self) -> int | None:
        return self.clock.now - 60_000 if self.down else self.clock.now - 1_000

    def sweep_pg_unreachable(self) -> bool:
        return self.unreachable


@dataclass
class FakeMaintenance:
    adapters: set[str] = field(default_factory=set)

    def in_maintenance(self, adapter_id: str, now_ms: int) -> bool:
        return adapter_id in self.adapters


@dataclass
class FakeSpool:
    records: list[FailedSpoolRecord] = field(default_factory=list)
    acked: list[str] = field(default_factory=list)
    broken: bool = False

    async def append(self, record: FailedSpoolRecord) -> None:
        if self.broken:
            raise OSError("disk")
        self.records.append(record)

    async def replay(self) -> Sequence[FailedSpoolRecord]:
        return [r for r in self.records if r.failure_id not in self.acked]

    async def acknowledge(self, failure_ids: Sequence[str]) -> None:
        self.acked.extend(failure_ids)


class Flaky:
    """An `ArmFailuresStore` whose pool can be taken down."""

    def __init__(self, inner: ArmFailuresStore) -> None:
        self.inner = inner
        self.down = False
        self.calls = 0

    def _check(self) -> None:
        self.calls += 1
        if self.down:
            raise ConnectionRefusedError("pool down")

    async def record(self, failure: FailureRecord, ping: FailedPing, *, queue_ping: bool = True) -> bool:
        self._check()
        return await self.inner.record(failure, ping, queue_ping=queue_ping)

    async def reping(self, failure_id: str, ping_id: str, ping: FailedPing) -> bool:
        self._check()
        return await self.inner.reping(failure_id, ping_id, ping)

    async def note_probe(self, failure_id: str, at_ms: int, result: str) -> bool:
        self._check()
        return await self.inner.note_probe(failure_id, at_ms, result)

    async def open_episodes(self) -> list[OpenFailure]:
        self._check()
        return await self.inner.open_episodes()


@dataclass
class Mirror:
    pages: list[tuple[str, str, str]] = field(default_factory=list)
    mirrored: list[tuple[str, str]] = field(default_factory=list)

    def pager(self) -> Pager:
        return Pager(lambda s, t, b: self.pages.append((s, t, b)), lambda t, b: self.mirrored.append((t, b)))


@dataclass
class Answers:
    got: list[tuple[int, str, str]] = field(default_factory=list)

    def complete(self, request_id: int, answer: object) -> None:
        self.got.append((request_id, getattr(answer, "status").value, getattr(answer, "reason")))


@dataclass
class FailedRig:
    clock: FakeClock
    calendar: FakeCalendar
    health: FakeHealth
    pg: FakePg
    budget: ErrorBudget
    emu: ArmEmulator
    arm: Flaky
    engine: Flaky
    positions: FakeStore
    spool: FakeSpool
    mirror: Mirror
    answers: Answers
    manager: FailedManager

    def add(self, *rows: ArmPositionRow) -> None:
        for r in rows:
            self.positions.rows[r.key] = r

    def advance(self, ms: int) -> None:
        self.clock.now += ms

    def critical_rows(self) -> list[dict[str, object]]:
        return [r for r in self.emu.t.outbox if r["kind"] == "info" and r["severity"] == "CRITICAL"]

    def arm_rows(self) -> list[dict[str, object]]:
        return [r for r in self.emu.t.outbox if r["kind"] == "arm"]

    def restart(self, *, spool: FakeSpool | None = None) -> FailedManager:
        """A new term (restart or takeover): new budget and manager over the same PostgreSQL and spool."""
        self.budget = ErrorBudget(self.budget.settings, calendar=self.calendar, health=self.health, pg=self.pg)
        self.mirror = Mirror()
        self.manager = make_manager(self, spool=spool or self.spool)
        return self.manager


class DownPositions(FakeStore):
    """A `FakeStore` whose live-position read can fail (its pool down)."""

    def __init__(self, rows: FakeStore | None = None) -> None:
        super().__init__()
        if rows is not None:
            self.rows = rows.rows       # the same rows (one PostgreSQL), read through another pool
        self.down = False
        self.reads = 0

    async def live_positions(self) -> list[ArmPositionRow]:
        self.reads += 1
        if self.down:
            raise ConnectionRefusedError("pool down")
        return await super().live_positions()


def make_manager(r: FailedRig, *, spool: FakeSpool, engine_positions: FakeStore | None = None) -> FailedManager:
    return FailedManager(budget=r.budget, stores=FailedStores(arm=r.arm, engine=r.engine), positions=r.positions,
                         spool=spool, pager=r.mirror.pager(), clock=r.clock, epoch=lambda: EPOCH,
                         fail=r.budget.settings, settings=FailedSettings(max_positions=3), answers=r.answers,
                         engine_positions=engine_positions)


def failed_rig(*, now: int = T0, fail: FailSettings | None = None, positions: FakeStore | None = None) -> FailedRig:
    clock = FakeClock(now)
    calendar, health, pg = FakeCalendar(), FakeHealth(), FakePg(clock)
    budget = ErrorBudget(fail or FailSettings(), calendar=calendar, health=health, pg=pg)
    emu = ArmEmulator(epoch=EPOCH)
    db = EngineDb(emu, lambda: Epoch("c", EPOCH))
    arm, engine = Flaky(PgArmFailuresStore(db)), Flaky(PgArmFailuresStore(db))
    rig = FailedRig(clock=clock, calendar=calendar, health=health, pg=pg, budget=budget, emu=emu, arm=arm,
                    engine=engine, positions=FakeStore() if positions is None else positions, spool=FakeSpool(),
                    mirror=Mirror(), answers=Answers(), manager=None)  # type: ignore[arg-type]
    rig.manager = make_manager(rig, spool=rig.spool)
    return rig


def row(pid: str = "P1", *, symbol: str = SYMBOL, state: str = "TRACKING", adapter: str = ADAPTER,
        guards_json: str = "[]") -> ArmPositionRow:
    return replace(arm_row(pid, state=state, adapter=adapter), symbol=symbol, guards_json=guards_json)
