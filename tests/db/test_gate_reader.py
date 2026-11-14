"""TieredGateContextReader feeding the canonical ActionGate list (oversee-rules.md §8)."""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import fakeredis
import pytest

from src.cache import keys as K
from src.cache.scripts import RedisState
from src.db import gate_reader as G
from src.db.epoch import EngineDb
from src.oversee.gate import Action, ActionGate, evaluate_gate
from src.oversee.sweep import AccountView

from tests.db.conftest import EPOCH, FakeConn, FakeSource

NOW = 100_000
BUY = Action(account_id="ACC", adapter_id="stonex", risk_increasing=True, symbol="EURUSD")
CLOSE = Action(account_id="ACC", adapter_id="stonex", risk_increasing=False)


@pytest.fixture
async def redis() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.aclose()


async def run_id(_: Any) -> str | None:
    return "run1"


def reader(conn: FakeConn, redis: Any, view: AccountView | None = None, lease: bool = True,
           settle: int = 0, verified: str = "stonex") -> G.TieredGateContextReader:
    inputs = G.GateInputs(node_epoch=lambda: 7, lease_valid=lambda: lease, settle_until_ms=lambda: settle,
                          sweep_view=lambda a: view, clock_ms=lambda: NOW, latest_open_ms=lambda a: 50_000,
                          catchup_verified=lambda a: a == verified)
    return G.TieredGateContextReader(EngineDb(FakeSource(conn), lambda: EPOCH), redis, inputs, run_id)


async def ready(redis: Any, level: str = "NONE", event_id: int = 4) -> None:
    await redis.set(K.BOOT_ID, "run1:7")
    await RedisState(redis, lambda: "run1:7").set_halt_state(level, event_id)


def script(conn: FakeConn, *, max_id: int = 4, streams: list[Any] | None = None) -> None:
    conn.returns(G.SQL_GATE_PG, (7, max_id))
    conn.returns_all(G.SQL_GATE_STREAMS, streams if streams is not None else
                     [("orders", None, 60_000, 7), ("account", None, 70_000, 7), ("prices", None, 65_000, 7)])


FRESH = AccountView(last_pass_ms=NOW - 500, stale_symbols=frozenset(), has_unknown=False)


async def test_all_conditions_hold(conn: FakeConn, redis: Any) -> None:
    await ready(redis)
    script(conn)
    ctx = await reader(conn, redis, FRESH).read(BUY)
    assert (ctx.redis_boot_id, ctx.redis_run_id, ctx.node_epoch, ctx.engine_epoch) == ("run1:7", "run1", 7, 7)
    assert ctx.halt_state is not None and ctx.halt_state[1] == 4 and ctx.max_halt_event_id == 4
    assert (ctx.reconciled_at_ms, ctx.reconciled_epoch, ctx.open_stream_health) == (60_000, 7, False)
    assert evaluate_gate(ctx, BUY).allowed and ctx.catchup_verified


async def test_an_unverified_downtime_catchup_denies_risk_increasing_only(conn: FakeConn, redis: Any) -> None:
    """The adapter's catch-up was incomplete (or never wired): risk-increasing actions are denied, closes are not."""
    await ready(redis)
    script(conn)
    script(conn)
    gate = ActionGate(reader(conn, redis, FRESH, verified="other-adapter"))
    verdict = await gate.check(BUY)
    assert not verdict.allowed and verdict.reason == "catchup_unverified"
    assert await gate.allow(CLOSE)
    inputs = G.GateInputs(node_epoch=lambda: 7, lease_valid=lambda: True, settle_until_ms=lambda: 0,
                          sweep_view=lambda a: FRESH, clock_ms=lambda: NOW, latest_open_ms=lambda a: 50_000)
    script(conn)
    ctx = await G.TieredGateContextReader(EngineDb(FakeSource(conn), lambda: EPOCH), redis, inputs, run_id).read(BUY)
    assert ctx.catchup_verified is False                   # not wired: fail closed


@pytest.mark.parametrize("case,reason", [
    ("no_boot", "boot_id_missing"), ("old_halt", "halt_state_not_current"), ("open_stream", "stream_health_open"),
    ("other_epoch", "not_reconciled"), ("stale_sweep", "sweep_not_current"), ("no_view", "unknown_result"),
    ("lease", "lease_invalid"), ("settle", "settle_window"), ("stale_mark", "stale_mark"),
    ("before_open", "not_reconciled"),
])
async def test_denials(conn: FakeConn, redis: Any, case: str, reason: str) -> None:
    if case != "no_boot":
        await ready(redis, event_id=3 if case == "old_halt" else 4)
    streams = [("orders", None, 60_000, 7), ("account", 9 if case == "open_stream" else None, 70_000,
                                             6 if case == "other_epoch" else 7)]
    if case == "before_open":
        streams = [("orders", None, 40_000, 7)]
    script(conn, streams=streams)
    view: AccountView | None = FRESH
    if case == "stale_sweep":
        view = AccountView(NOW - 3_000, frozenset(), False)
    elif case == "no_view":
        view = None
    elif case == "stale_mark":
        view = AccountView(NOW - 100, frozenset({("stonex", "EURUSD")}), False)
    r = reader(conn, redis, view, lease=case != "lease", settle=NOW + 1 if case == "settle" else 0)
    verdict = evaluate_gate(await r.read(BUY), BUY)
    assert not verdict.allowed and verdict.reason == reason


async def test_soft_halt_allows_only_risk_reducing(conn: FakeConn, redis: Any) -> None:
    await ready(redis, "SOFT")
    script(conn)
    assert not evaluate_gate(await reader(conn, redis, FRESH).read(BUY), BUY).allowed
    script(conn)
    assert evaluate_gate(await reader(conn, redis, FRESH).read(CLOSE), CLOSE).allowed


def test_reconcile_state() -> None:
    assert G.reconcile_state([], 7) == (False, None, None)
    assert G.reconcile_state([("o", None, None, None)], 7) == (False, None, None)
    assert G.reconcile_state([("o", 5, 10, 7), ("a", None, 20, 7)], 7) == (True, 10, 7)
    assert G.reconcile_state([("o", None, 10, 7), ("a", None, 20, 6)], 7) == (False, 10, 6)


async def test_errors_fail_closed(conn: FakeConn, redis: Any) -> None:
    conn.fail(G.SQL_GATE_PG, ConnectionRefusedError("down"))
    conn.fail(G.SQL_GATE_PG, ConnectionRefusedError("down"))
    gate = ActionGate(reader(conn, redis, FRESH))
    assert not await gate.allow(BUY)
    verdict = await gate.check(CLOSE)
    assert not verdict.allowed and verdict.reason == "error:ConnectionRefusedError"


async def test_unknown_latest_open_denies_risk_increasing(conn: FakeConn, redis: Any) -> None:
    """gate-open-without-calendar: no SM10 calendar (or no session for the adapter) is not a pass."""
    await ready(redis)
    script(conn)
    script(conn)
    inputs = G.GateInputs(node_epoch=lambda: 7, lease_valid=lambda: True, settle_until_ms=lambda: 0,
                          sweep_view=lambda a: FRESH, clock_ms=lambda: NOW)          # latest_open_ms not wired
    r = G.TieredGateContextReader(EngineDb(FakeSource(conn), lambda: EPOCH), redis, inputs, run_id)
    verdict = await ActionGate(r).check(BUY)
    assert not verdict.allowed and verdict.reason == "latest_open_unknown"
    assert await ActionGate(r).allow(CLOSE)               # risk-reducing actions do not need the calendar
