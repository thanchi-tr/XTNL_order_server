"""4.8 SM4 / orchestrator hooks: on_known_bad(), the PRE_OPEN back-off reset, SM10 price evidence."""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from src.pubsub import connection as connmod
from src.pubsub.connection import ConnectionStateMachine, ConnState, IllegalConnTransition
from src.dto import PriceDTO
from tests.pubsub.conftest import ManualClock, ev, price
from tests.pubsub.test_orchestrator import K, make, to_streaming, wait_for
from tests.pubsub.test_orchestrator_ports import FakeAuthStore

S = ConnState


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


def sm() -> ConnectionStateMachine:
    return ConnectionStateMachine("mock", jitter=lambda: 0.0, env_check=lambda: "sandbox")


# ===================================================================== SM4 events
def test_on_known_bad_goes_straight_to_auth_failed() -> None:
    m = sm()
    m.start()
    assert m.on_known_bad("generation v1") is S.AUTH_FAILED
    assert m.history[-1] == (S.AUTHENTICATING, S.AUTH_FAILED, "credential generation known bad: generation v1")
    assert len(m.history) == 2                              # one transition, no credential-error retries
    with pytest.raises(IllegalConnTransition):
        m.on_known_bad()                                    # only from AUTHENTICATING
    assert m.on_rearmed() is S.AUTHENTICATING and m.credential_errors == 0


def test_reset_backoff_clears_the_count_and_never_leaves_auth_failed() -> None:
    m = sm()
    m.start()
    m.on_transient_error("x")
    m.on_backoff_elapsed()
    m.on_transient_error("y")
    assert m.state is S.BACKOFF and m.attempt == 2 and m.backoff_s > 0
    assert m.reset_backoff() is True and m.attempt == 0 and m.backoff_s == 0.0 and m.state is S.BACKOFF
    m.on_backoff_elapsed()
    assert m.reset_backoff() is False                       # not in BACKOFF: only the count is cleared
    for _ in range(3):
        m.on_credential_error("bad")
    assert m.state is S.AUTH_FAILED
    assert m.reset_backoff() is False and m.state is S.AUTH_FAILED


# ===================================================================== orchestrator
async def test_known_bad_generation_uses_on_known_bad(tmp_path: Path, clock: ManualClock) -> None:
    auth = FakeAuthStore()
    auth.bad.add(("mock", "v1"))
    env = make(tmp_path, clock, auth_store=auth, credential_generation=lambda a: "v1", auth_poll_s=10.0)
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.conn.state is ConnState.AUTH_FAILED)
    reasons = [why for _, _, why in rt.conn.history]
    assert reasons == ["start", "credential generation known bad: generation v1"]   # no fake credential errors
    assert env.adapter.called("connect") == []
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_readiness_reset_cuts_a_long_backoff(tmp_path: Path, clock: ManualClock,
                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(connmod, "backoff_delay", lambda attempt, jitter: 60.0)
    env = make(tmp_path, clock)
    env.adapter.connect_errors = [OSError("network down")]
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.conn.state is ConnState.BACKOFF)
    await asyncio.sleep(0.05)
    assert rt.conn.state is ConnState.BACKOFF and len(env.adapter.called("connect")) == 1   # waiting 60 s
    assert env.orch.reset_backoff("mock") is True
    await wait_for(lambda: rt.conn.state is ConnState.STREAMING, timeout=3.0)
    assert len(env.adapter.called("connect")) == 2 and rt.conn.attempt == 0
    assert env.orch.reset_backoff("mock") is False          # STREAMING: nothing to cut
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_price_ticks_feed_sm10_evidence_with_snapshot_items_marked(tmp_path: Path,
                                                                        clock: ManualClock) -> None:
    seen: list[tuple[str, bool]] = []

    def hook(tick: PriceDTO, snapshot: bool) -> None:
        seen.append((tick.symbol, snapshot))

    env = make(tmp_path, clock, on_price_tick=hook)
    to_streaming(env)
    env.item("prices", ev(K.SUBSCRIBED, "prices"))
    env.item("prices", price("EURUSD", ts=1))
    env.item("prices", price("EURUSD", ts=2))
    env.item("prices", price("GBPUSD", ts=3))
    env.item("prices", ev(K.SUBSCRIBED, "prices"))          # re-subscribed: the next tick is a snapshot again
    env.item("prices", price("EURUSD", ts=4))
    env.item("orders", ev(K.DISCONNECTED, "orders"))        # socket lost: every subscription restarts
    env.item("prices", price("EURUSD", ts=5))
    assert seen == [("EURUSD", True), ("EURUSD", False), ("GBPUSD", True), ("EURUSD", True), ("EURUSD", True)]


async def test_a_failing_evidence_hook_never_blocks_the_tick(tmp_path: Path, clock: ManualClock) -> None:
    def hook(tick: PriceDTO, snapshot: bool) -> None:
        raise RuntimeError("calendar bug")

    env = make(tmp_path, clock, on_price_tick=hook)
    to_streaming(env)
    env.item("prices", price("EURUSD", ts=1))
    assert env.bus.prices.qsize() == 1


# ===================================================================== SM10 execution evidence (4.9)
async def test_stream_fills_feed_sm10_execution_evidence(tmp_path: Path, clock: ManualClock) -> None:
    """operations.md §10: an execution event on a symbol is SM10 evidence. Only stream rows that show a
    fill against a known row count; first rows (possible replay / snapshot items), amends, cancels and REST
    rows never do. Called synchronously with (adapter_id, symbol, local receipt ms)."""
    from src.dto import OrderStatus as O
    from tests.pubsub.conftest import order, position

    seen: list[tuple[str, str, int]] = []
    c0 = clock.ms
    env = make(tmp_path, clock, on_execution=lambda a, s, t: seen.append((a, s, t)))
    to_streaming(env)
    env.item("orders", order("O1", O.FILLED, t=clock.ms - 10))        # first row: may be a replay item
    env.item("orders", order("O2", O.WORKING, t=clock.ms - 9))
    env.item("orders", order("O2", O.WORKING, t=clock.ms - 8, stop="1.05"))   # amend
    assert seen == []
    clock.ms += 1
    env.item("orders", order("O2", O.PARTIALLY_FILLED, t=clock.ms - 7, remaining="400"))
    clock.ms += 1
    env.item("orders", order("O2", O.PARTIALLY_FILLED, t=clock.ms - 6, remaining="100"))
    env.item("orders", order("O2", O.FILLED, t=clock.ms - 5, remaining="0"))
    env.item("orders", order("O3", O.WORKING, t=clock.ms - 5, symbol="GBPUSD"))
    env.item("orders", order("O3", O.CANCELLED, t=clock.ms - 4, symbol="GBPUSD"))   # cancel: no execution
    env.item("orders", position("P1", t=clock.ms - 4))                               # first row
    env.item("orders", position("P1", t=clock.ms - 3, units="2000"))                 # grown
    env.item("orders", position("P1", "CLOSED", t=clock.ms - 2))                     # closed
    assert seen == [("mock", "EURUSD", c0 + 1)] + [("mock", "EURUSD", c0 + 2)] * 4
    seen.clear()
    env.item("orders", order("O4", O.PENDING, t=clock.ms - 20))
    env.adapter.open_orders = [order("O4", O.PARTIALLY_FILLED, t=clock.ms - 1, remaining="500")]
    await env.orch.run_snapshot("mock")                                          # REST rows never count
    assert seen == []


async def test_a_failing_execution_hook_never_blocks_the_row(tmp_path: Path, clock: ManualClock) -> None:
    from src.dto import OrderStatus as O
    from tests.pubsub.conftest import order

    def hook(adapter_id: str, symbol: str, ts_ms: int) -> None:
        raise RuntimeError("calendar bug")

    env = make(tmp_path, clock, on_execution=hook)
    to_streaming(env)
    env.item("orders", order("O1", O.WORKING, t=clock.ms - 2))
    env.item("orders", order("O1", O.FILLED, t=clock.ms - 1, remaining="0"))
    assert env.bus.orders.qsize() == 2
    (stored,) = env.orch.runtime("mock").orders.values()
    assert stored.status is O.FILLED


async def test_unknown_instrument_position_growth_is_execution_evidence(tmp_path: Path,
                                                                        clock: ManualClock) -> None:
    """dto-contracts.md §3: with the instrument unknown a position's units are null and `raw_qty` carries
    the broker quantity; a change of it is a deal on that symbol, so it is SM10 execution evidence too."""
    from dataclasses import replace as dc_replace
    from decimal import Decimal

    from src.dto import PositionDTO
    from tests.pubsub.conftest import position

    def unknown(t: int, qty: str) -> PositionDTO:
        return dc_replace(position("P1", t=t), units=None, instrument_unknown=True, raw_qty=Decimal(qty))

    seen: list[tuple[str, str, int]] = []
    env = make(tmp_path, clock, on_execution=lambda a, s, t: seen.append((a, s, t)))
    to_streaming(env)
    env.item("orders", unknown(clock.ms - 3, "5"))       # first row
    env.item("orders", unknown(clock.ms - 2, "5"))       # same quantity
    assert seen == []
    env.item("orders", unknown(clock.ms - 1, "7"))       # grown
    assert seen == [("mock", "EURUSD", clock.ms)]


def test_connection_history_is_bounded() -> None:
    from src.pubsub.connection import ConnectionStateMachine
    m = ConnectionStateMachine("mock")
    for i in range(2_000):
        m._go(m.state, f"t{i}")
    assert len(m.history) == 500 and m.history[-1][2] == "t1999"
