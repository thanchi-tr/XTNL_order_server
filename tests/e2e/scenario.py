"""The end-to-end replay scenario (IMPLEMENTATION_PLAN 4.3), shared by the PostgreSQL + Redis integration
test and the in-memory smoke test. Both run the real `EngineSession` composition (src/runtime/session.py)
through LEADER_INIT; only the storage ports differ.

    MockBrokerAdapter fixture -> IngestionOrchestrator -> consumers -> PostgreSQL (ports) -> Redis apply_state
    -> oversee:feed -> FeedObserver -> RiskSweep -> halt_breaches / halt_events / halt:state / outbox

Checks:
1. committed rows, Redis entity keys and state:digest agree;
2. a stale REST read never overwrites a newer stream update;
3. a MaxPositionRiskRule breach produces a rule state, a halt_events row, halt:state in Redis and an
   outbox row;
4. the ActionGate denies once redis:boot_id is deleted and allows again after RedisBootstrap (SM8).
"""
from __future__ import annotations

import asyncio
import dataclasses
import time
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Protocol

from src.adapters.mock import MockBrokerAdapter, MockFixture, build_fx_fixture
from src.cache import keys as K
from src.integrity.ports import CommittedRow, IntegrityDb
from src.integrity.redis_ops import read_digest, read_meta
from src.dto import PositionDTO
from src.db.epoch import Epoch
from src.oversee.gate import Action
from src.pubsub.connection import ConnState
from src.runtime.leader_init import InitOutcome, LeaderInit
from src.runtime.session import EngineSession, SessionTimings
from src.runtime.supervisor import TaskSupervisor

ACCOUNT = "MOCK-AUD-1"
ADAPTER = "mock"
NODE = "node-e2e"

FAST_TIMINGS = SessionTimings(sm8_deadline_s=30.0, sm8_price_wait_ms=500, drain_close_s=2.0, housekeeping_s=1.0,
                              monitor_poll_s=0.2, consumer_poll_s=0.2)


class Views(Protocol):
    async def breaches(self) -> list[tuple[str, str, str]]: ...

    async def halt_events(self) -> list[tuple[int, str]]: ...

    async def outbox(self) -> list[tuple[str, str]]: ...


class FakeLease:
    def __init__(self) -> None:
        self.valid = True

    def lease_valid(self, min_remaining_s: float = 0.0) -> bool:
        return self.valid

    def holds_lease(self) -> bool:
        return self.valid


def wall_ms() -> int:
    return time.time_ns() // 1_000_000


def fixture(now_ms: int) -> MockFixture:
    """The FX fixture, timestamped a few seconds in the past (stream updates 1 s later)."""
    return build_fx_fixture(adapter_id=ADAPTER, account_id=ACCOUNT, ts_ms=now_ms - 5_000)


def mock_adapter(fx: MockFixture) -> MockBrokerAdapter:
    return MockBrokerAdapter.from_fixture(fx, finite_streams=False)


async def eventually(check: Callable[[], Awaitable[bool]], what: str, timeout_s: float = 20.0,
                     poll_s: float = 0.05) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        if await check():
            return
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out waiting for: {what}")
        await asyncio.sleep(poll_s)


async def all_rows(db: IntegrityDb) -> list[CommittedRow]:
    out: list[CommittedRow] = []
    after = ""
    while True:
        page = await db.rows_after_key(after, 500)
        if not page:
            return out
        out.extend(page)
        after = page[-1].key


async def row(db: IntegrityDb, key: str) -> CommittedRow | None:
    return (await db.rows_by_keys([key])).get(key)


@dataclasses.dataclass
class Leader:
    session: EngineSession
    supervisor: TaskSupervisor
    stop: asyncio.Event
    task: asyncio.Task[None]
    exits: list[str]

    async def shutdown(self) -> None:
        await self.session.drain("switchover")
        self.stop.set()
        await self.task


async def become_leader(session: EngineSession, epoch: Epoch) -> Leader:
    exits: list[str] = []
    sup = TaskSupervisor([], exit_fn=exits.append, poll_interval_s=0.1)
    stop = asyncio.Event()
    task = asyncio.create_task(sup.run(stop))
    report = await LeaderInit(session.init_deps(sup), exit_fn=exits.append, deadline_s=60.0,
                              clock_ms=wall_ms).run(epoch, NODE)
    assert report is not None, f"LEADER_INIT failed: {exits}"
    assert report.outcome is InitOutcome.LEADER, report
    assert report.steps[-1] == "reached_leader", report.steps
    return Leader(session, sup, stop, task, exits)


def _consumers_idle(s: EngineSession) -> bool:
    return s._ingestion_idle()


async def check_redis_agrees(session: EngineSession) -> list[CommittedRow]:
    """1. every committed current row is in Redis with the same version and checksum, and state:digest
    equals the committed digest."""
    db = session.ports.integrity_db
    redis = session.redis

    async def agree() -> bool:
        if not _consumers_idle(session) or not await session.sm8.is_booted():
            return False
        rows = await all_rows(db)
        if not rows:
            return False
        meta = await read_meta(redis, [r.key for r in rows])
        for r in rows:
            m = meta[r.key]
            if m is None or m.version != r.version or m.checksum != r.checksum:
                return False
        return await db.digest() == await read_digest(redis)

    await eventually(agree, "Redis entity keys and state:digest equal the committed rows")
    return await all_rows(db)


async def run_scenario(session: EngineSession, adapter: MockBrokerAdapter, fx: MockFixture, epoch: Epoch,
                       views: Views) -> Leader:
    leader = await become_leader(session, epoch)
    db = session.ports.integrity_db
    eu_key = K.position_key(ADAPTER, "P-EU-1")

    # --- ingestion reaches STREAMING; the fixture is committed; SM8 is READY -------------------------
    await eventually(lambda: _true(session.orchestrator.runtime(ADAPTER).conn.state is ConnState.STREAMING),
                     "mock adapter STREAMING")
    await eventually(session.sm8.is_booted, "SM8 READY (redis:boot_id = run_id:epoch)")
    rows = await check_redis_agrees(session)
    keys = {r.key for r in rows}
    for k in (K.position_key(ADAPTER, "P-EU-1"), K.position_key(ADAPTER, "PF-1"), K.order_key(ADAPTER, "PF-1"),
              K.order_key(ADAPTER, "PF-1-SL"), K.account_key(ADAPTER, ACCOUNT), K.instrument_key(ADAPTER, "EURUSD")):
        assert k in keys, f"{k} not committed: {sorted(keys)}"

    # --- 2. the stream update (units 10 000, 1 s newer) wins; a stale REST read never overwrites it ----
    async def eu_units_is_stream() -> bool:
        r = await row(db, eu_key)
        return r is not None and isinstance(r.entity, PositionDTO) and r.entity.units == Decimal(10000)

    await eventually(eu_units_is_stream, "P-EU-1 at the stream version (10 000 units)")
    before = await row(db, eu_key)
    assert before is not None
    stale = next(p for p in fx.positions if p.position_id == "P-EU-1")       # REST row, 1 s older
    assert stale.units == Decimal(20000)
    adapter.set_position(stale)
    fresh = await session.orchestrator.reread_entity(ADAPTER, "POSITION", "P-EU-1")
    assert isinstance(fresh, PositionDTO) and fresh.units == Decimal(20000)
    await eventually(lambda: _true(_consumers_idle(session)), "consumers idle after the stale REST read")
    after = await row(db, eu_key)
    assert after is not None and isinstance(after.entity, PositionDTO)
    assert after.entity.units == Decimal(10000) and after.version == before.version
    meta = (await read_meta(session.redis, [eu_key]))[eu_key]
    assert meta is not None and meta.version == before.version
    await check_redis_agrees(session)

    # --- 3. a MaxPositionRiskRule breach: rule state, halt_events, halt:state, outbox --------------------
    big = PositionDTO(position_id="P-BIG", account_id=ACCOUNT, adapter_id=ADAPTER, symbol="EURUSD", side="BUY",
                      units=Decimal(200000), raw_qty=Decimal(200000), entry_price=Decimal("1.0850"),
                      stop_loss=Decimal("1.0750"), status="OPEN", opened_by_client_order_id="P-BIG",
                      broker_updated_at_ms=wall_ms(), source="STREAM")
    adapter.push_order_update(big)

    async def breached() -> bool:
        states = await views.breaches()
        return any(rule == "max_position_risk" and "P-BIG" in subject and level in ("SOFT_HALT", "HARD_HALT")
                   for rule, subject, level in states)

    await eventually(breached, "max_position_risk SOFT state for P-BIG")

    async def halted() -> bool:
        evs = await views.halt_events()
        if not evs or evs[-1][1] not in ("SOFT", "HARD"):
            return False
        mirrored = await session.cache.read_halt_state()
        return mirrored is not None and mirrored == (evs[-1][1], evs[-1][0])

    await eventually(halted, "halt_events row and halt:state mirrored in Redis")
    outbox = await views.outbox()
    assert any(kind == "trip" for kind, _ in outbox), outbox
    assert any(kind in ("episode", "trip") and title for kind, title in outbox), outbox

    # --- 4. ActionGate: allow, deny without boot_id, allow again after SM8 ---------------------------
    action = Action(ACCOUNT, ADAPTER, risk_increasing=False, symbol="EURUSD", description="e2e reduce")

    async def allowed() -> bool:
        return (await session.gate.check(action)).allowed

    await eventually(allowed, "ActionGate allows a risk-reducing action under SOFT")
    await session.redis.delete(K.BOOT_ID)
    verdict = await session.gate.check(action)
    assert not verdict.allowed and verdict.reason == "boot_id_missing", verdict
    assert await session.sm8.run_once(in_place=True)
    await eventually(allowed, "ActionGate allows again after RedisBootstrap")
    assert not leader.exits, leader.exits
    return leader


async def _true(v: bool) -> bool:
    return v
