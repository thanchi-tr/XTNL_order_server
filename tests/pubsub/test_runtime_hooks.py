"""4.8 SM4 / orchestrator hooks: on_known_bad(), the PRE_OPEN back-off reset, SM10 price evidence; S05 tick-only
symbols and per-market price subscriptions (design PR5, PR6)."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal
from pathlib import Path

import pytest

from src.adapters.base import SessionExpiredError, StreamEvent
from src.pubsub import connection as connmod
from src.pubsub.connection import ConnectionStateMachine, ConnState, IllegalConnTransition
from src.dto import InstrumentSpec, PriceDTO
from tests.pubsub.conftest import FakeAdapter, ManualClock, account, ev, order, price
from tests.pubsub.test_orchestrator import Env, K, make, to_streaming, wait_for
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


# ===================================================================== tick-only symbols, per-market subscriptions (S05)
def _spec(symbol: str) -> InstrumentSpec:
    return InstrumentSpec(adapter_id="mock", symbol=symbol, base_ccy=symbol[:3], quote_ccy=symbol[3:],
                          pnl_ccy=symbol[3:], contract_size=Decimal(1), unit_kind="UNITS",
                          point_size=Decimal("0.0001"), min_qty=Decimal(1), swap_booked_to_balance=True)


def mev(kind: K, market: str | None, at_ms: int = 0) -> StreamEvent:
    return StreamEvent(kind, "prices", "", at_ms, market)


async def _start_streams(env: Env) -> None:
    """Start the stream pumps (as SM4 does) and wait until each fake stream reported SUBSCRIBED."""
    env.orch.start_missing_streams("mock")
    rt = env.orch.runtime("mock")
    await wait_for(lambda: rt.all_subscribed)


async def _stop_streams(env: Env) -> None:
    rt = env.orch.runtime("mock")
    tasks = list(rt.tasks.values())
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    rt.tasks.clear()


def _drain_prices(env: Env) -> list[PriceDTO]:
    out: list[PriceDTO] = []
    while not env.bus.prices.empty():
        out.append(env.bus.prices.get_nowait())
    return out


class _FailingMdEnsure(FakeAdapter):
    """ensure_ticks() refuses the symbols in `fail_on` (an unknown market, a refused subscribe)."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_on: set[str] = set()

    async def ensure_ticks(self, symbols: Sequence[str]) -> None:
        self.calls.append(("ensure_ticks", tuple(symbols)))
        if self.fail_on & set(symbols):
            raise RuntimeError("market not found")


async def test_tick_only_symbols_ride_the_price_stream_but_never_the_held_sets(tmp_path: Path,
                                                                            clock: ManualClock) -> None:
    """PR5 / IN-8: the tick-only set is subscribed with the required symbols at stream start, but never joins
    rt.tick_symbols / pending_symbols (SM10's wanted symbols, the stale-price and clean-mid watches); its ticks reach
    the price hook only, never market.price.dto (so never the Redis price keys or last_clean_marks)."""
    seen: list[str] = []
    env = make(tmp_path, clock, tick_symbols={"mock": ["EURUSD"]},
               on_price_tick=lambda tick, snap: seen.append(tick.symbol))
    env.orch.set_md_tick_symbols("mock", ["XAUUSD", " US500 ", ""])
    assert env.orch.md_tick_symbols("mock") == frozenset({"XAUUSD", "US500"})
    await _start_streams(env)
    try:
        rt = env.orch.runtime("mock")
        assert env.adapter.called("stream_ticks") == [("EURUSD", "US500", "XAUUSD")]
        assert rt.tick_symbols == {"EURUSD"} and not rt.pending_symbols
        assert env.orch.required_tick_symbols("mock") == ["EURUSD"]
        assert env.orch.is_md_only("mock", "XAUUSD") and not env.orch.is_md_only("mock", "EURUSD")
        assert not env.orch.is_md_only("other", "XAUUSD")
        env.item("prices", price("XAUUSD", ts=1))
        assert env.bus.prices.qsize() == 0                         # hook only, never the topic
        env.item("prices", price("EURUSD", ts=2))
        assert env.bus.prices.qsize() == 1
        assert seen == ["XAUUSD", "EURUSD"]
        # a tick-only symbol that becomes held is an ordinary symbol from then on
        env.adapter.instruments = [_spec("XAUUSD")]
        env.item("orders", order("O1", t=clock.ms, symbol="XAUUSD"))
        assert "XAUUSD" in rt.pending_symbols and not env.orch.is_md_only("mock", "XAUUSD")
        env.item("prices", price("XAUUSD", ts=3))
        assert env.bus.prices.qsize() == 2
    finally:
        await _stop_streams(env)


async def test_without_tick_only_symbols_the_stream_asks_for_the_required_set_only(tmp_path: Path,
                                                                                 clock: ManualClock) -> None:
    """Flags off (empty set): the prices stream, ensure_ticks and the price topic behave exactly as before."""
    env = make(tmp_path, clock, tick_symbols={"mock": ["EURUSD"]})
    await _start_streams(env)
    try:
        to_streaming(env)
        assert env.adapter.called("stream_ticks") == [("EURUSD",)]
        assert await env.orch.flush_md_symbols("mock") == []
        assert env.adapter.called("ensure_ticks") == []
        assert not env.orch.is_md_only("mock", "EURUSD")
        env.item("prices", price("GBPUSD"))                        # any other symbol: published as before
        assert env.bus.prices.qsize() == 1
    finally:
        await _stop_streams(env)


async def test_tick_only_symbols_added_later_get_their_own_ensure_ticks_call(tmp_path: Path,
                                                                           clock: ManualClock) -> None:
    adapter = _FailingMdEnsure()
    env = make(tmp_path, clock, adapter=adapter, tick_symbols={"mock": ["EURUSD"]})
    adapter.instruments = [_spec("EURUSD"), _spec("GBPUSD")]
    adapter.accounts = [account()]
    await env.orch.run_snapshot("mock")
    env.orders(), env.account_items()
    await _start_streams(env)
    try:
        to_streaming(env)
        rt = env.orch.runtime("mock")
        env.orch.set_md_tick_symbols("mock", ["US500"])
        assert await env.orch.flush_md_symbols("mock") == ["US500"]
        assert adapter.called("ensure_ticks") == [("US500",)]
        assert rt.md_subscribed == {"US500"} and "US500" not in rt.tick_symbols | rt.pending_symbols
        # a held symbol is ensured in its own call, never together with tick-only symbols
        env.item("orders", order("O1", t=clock.ms, symbol="GBPUSD"))
        clock.ms += 1_000
        assert await env.orch.flush_pending_symbols("mock") == ["GBPUSD"]
        assert adapter.called("ensure_ticks")[-1] == ("GBPUSD",)
        # a failing tick-only ensure is logged and retried later; it never raises, opens a gap or asks for a resync
        adapter.fail_on = {"XAUUSD"}
        env.orch.set_md_tick_symbols("mock", ["US500", "XAUUSD"])
        clock.ms += 1_000
        await env.health_ops()
        assert await env.orch.flush_md_symbols("mock") == []
        assert rt.md_pending == {"XAUUSD"} and not rt.resync_event.is_set() and not env.resyncs
        assert await env.health_ops() == [] and rt.conn.state is ConnState.STREAMING
        clock.ms += 30_000
        n = len(adapter.called("ensure_ticks"))
        assert await env.orch.flush_md_symbols("mock") == []       # inside the retry interval: not tried
        assert len(adapter.called("ensure_ticks")) == n
        adapter.fail_on = set()
        clock.ms += 30_000
        assert await env.orch.flush_md_symbols("mock") == ["XAUUSD"]
        assert rt.tick_symbols == {"EURUSD", "GBPUSD"} and rt.md_subscribed == {"US500", "XAUUSD"}
    finally:
        await _stop_streams(env)


async def test_a_tick_only_symbol_is_ignored_by_the_stale_price_watch(tmp_path: Path, clock: ManualClock) -> None:
    pages: list[tuple[str, str, str]] = []
    env = make(tmp_path, clock, tick_symbols={"mock": ["EURUSD"]},
               pager=lambda s, t, b: pages.append((s, t, b)))
    env.orch.set_md_tick_symbols("mock", ["XAUUSD"])
    await _start_streams(env)
    try:
        to_streaming(env)
        for _ in range(5):
            clock.ms += 20_000
            for stream in ("account", "orders", "prices"):
                env.item(stream, ev(K.HEARTBEAT, stream))
            env.item("prices", price("EURUSD"))
            env.orch.check_liveness("mock")
        assert not [p for p in pages if "stale price" in p[1] or "clean mid" in p[1]]
        env.orch.set_md_tick_symbols("mock", [])
        env.item("prices", price("XAUUSD"))                        # still subscribed on this stream: not published
        assert all(p.symbol == "EURUSD" for p in _drain_prices(env))
    finally:
        await _stop_streams(env)


async def test_a_tick_only_markets_error_never_opens_the_prices_gap(tmp_path: Path, clock: ManualClock) -> None:
    """A per-market UNSUBSCRIBED / ERROR on a tick-only market closes only its interval; a required market's
    still opens the prices gap (as before). A tick-only market's own SUBSCRIBED leaves the stream's order alone."""
    env = make(tmp_path, clock, tick_symbols={"mock": ["EURUSD"]})
    env.orch.set_md_tick_symbols("mock", ["XAUUSD"])
    await _start_streams(env)
    try:
        rt = env.orch.runtime("mock")
        sub = rt.subs["prices"]
        await env.orch.run_snapshot("mock")                        # every stream reconciled: no gap open
        await env.health_ops()
        rt.resync_event.clear()
        assert not sub.lost
        seq = sub.subscribed_seq
        env.item("prices", mev(K.SUBSCRIBED, "XAUUSD"))
        assert sub.subscribed_seq == seq and "XAUUSD" in env.orch.price_subscriptions("mock")
        env.item("prices", mev(K.ERROR, "XAUUSD"))
        assert sub.subscribed and not sub.lost and not env.resyncs and await env.health_ops() == []
        assert "XAUUSD" not in env.orch.price_subscriptions("mock")
        env.item("prices", mev(K.UNSUBSCRIBED, "XAUUSD"))
        assert sub.subscribed and not sub.lost and not env.resyncs
        env.item("prices", mev(K.ERROR, "EURUSD"))
        assert not sub.subscribed and sub.lost and env.resyncs == ["mock"]
    finally:
        await _stop_streams(env)


async def test_one_markets_subscribed_does_not_reset_anothers_snapshot_tracking(tmp_path: Path,
                                                                               clock: ManualClock) -> None:
    """IN-12: a per-market SUBSCRIBED marks only that market's next tick as the subscription snapshot."""
    seen: list[tuple[str, bool]] = []
    env = make(tmp_path, clock, on_price_tick=lambda tick, snap: seen.append((tick.symbol, snap)))
    to_streaming(env)
    env.item("prices", ev(K.SUBSCRIBED, "prices"))
    env.item("prices", price("EURUSD", ts=1))
    env.item("prices", price("GBPUSD", ts=2))
    env.item("prices", mev(K.SUBSCRIBED, "GBPUSD"))
    env.item("prices", price("EURUSD", ts=3))
    env.item("prices", price("GBPUSD", ts=4))
    assert seen == [("EURUSD", True), ("GBPUSD", True), ("EURUSD", False), ("GBPUSD", True)]


async def test_per_market_subscription_intervals_reach_the_hook(tmp_path: Path, clock: ManualClock) -> None:
    calls: list[tuple[str, str, bool, int]] = []
    env = make(tmp_path, clock, tick_symbols={"mock": ["EURUSD"]},
               on_price_subscription=lambda a, m, s, ts: calls.append((a, m, s, ts)))
    env.orch.set_md_tick_symbols("mock", ["XAUUSD"])
    await _start_streams(env)
    try:
        # the fake's whole-stream SUBSCRIBED opened every market the stream was asked for, at receipt time
        assert env.orch.price_subscriptions("mock") == {"EURUSD": clock.ms, "XAUUSD": clock.ms}
        assert sorted(calls) == [("mock", "EURUSD", True, clock.ms), ("mock", "XAUUSD", True, clock.ms)]
        calls.clear()
        # every interval timestamp is the orchestrator's clock, never the adapter's at_ms (one clock: an interval
        # can never close before it opened)
        t0 = clock.ms
        clock.ms += 5
        env.item("prices", mev(K.SUBSCRIBED, "XAUUSD", at_ms=t0 + 500))         # re-subscribed: restarts
        clock.ms += 4
        env.item("prices", mev(K.ERROR, "XAUUSD", at_ms=t0 - 500))
        env.item("prices", mev(K.ERROR, "XAUUSD", at_ms=t0 + 10))               # already closed: no second call
        assert calls == [("mock", "XAUUSD", True, t0 + 5), ("mock", "XAUUSD", False, t0 + 9)]
        calls.clear()
        clock.ms += 7
        env.item("orders", ev(K.DISCONNECTED, "orders"))                         # the socket is really lost
        assert calls == [("mock", "EURUSD", False, clock.ms)] and env.orch.price_subscriptions("mock") == {}
        calls.clear()
        env.item("orders", ev(K.DISCONNECTED, "orders"))
        assert calls == []
    finally:
        await _stop_streams(env)


async def test_restarting_the_prices_pump_closes_its_intervals(tmp_path: Path, clock: ManualClock) -> None:
    calls: list[tuple[str, bool]] = []
    env = make(tmp_path, clock, tick_symbols={"mock": ["EURUSD", "GBPUSD"]},
               on_price_subscription=lambda a, m, s, ts: calls.append((m, s)))
    await _start_streams(env)
    try:
        calls.clear()
        env.item("prices", mev(K.ERROR, "EURUSD"))      # a required market: the prices stream is re-subscribed
        assert calls == [("EURUSD", False)]
        env.orch.start_missing_streams("mock")          # RESYNCING restarts the prices pump: GBPUSD ends too
        assert calls == [("EURUSD", False), ("GBPUSD", False)]
        await wait_for(lambda: env.orch.runtime("mock").subs["prices"].subscribed)
        assert set(env.orch.price_subscriptions("mock")) == {"EURUSD", "GBPUSD"}
    finally:
        await _stop_streams(env)


async def test_the_subscription_hook_is_not_fired_from_the_catchup_hold(tmp_path: Path, clock: ManualClock) -> None:
    calls: list[tuple[str, bool]] = []
    env = make(tmp_path, clock, tick_symbols={"mock": ["EURUSD"]},
               on_price_subscription=lambda a, m, s, ts: calls.append((m, s)))
    await _start_streams(env)
    try:
        calls.clear()
        env.orch.hold_fence("mock", "startup")
        assert all(sub.lost for sub in env.orch.runtime("mock").subs.values())    # the fence is held ...
        assert calls == [] and set(env.orch.price_subscriptions("mock")) == {"EURUSD"}   # ... intervals untouched
        env.orch.release_fence("mock", "startup")
        assert calls == [] and set(env.orch.price_subscriptions("mock")) == {"EURUSD"}
    finally:
        await _stop_streams(env)


async def test_a_raising_subscription_hook_never_blocks(tmp_path: Path, clock: ManualClock,
                                                        caplog: pytest.LogCaptureFixture) -> None:
    def hook(adapter_id: str, market: str, subscribed: bool, ts: int) -> None:
        raise RuntimeError("market-data bug")

    env = make(tmp_path, clock, on_price_subscription=hook)
    to_streaming(env)
    with caplog.at_level("ERROR"):
        for sym in ("EURUSD", "GBPUSD", "USDJPY"):
            env.item("prices", mev(K.SUBSCRIBED, sym))
            env.item("prices", price(sym))
        env.item("prices", mev(K.ERROR, "EURUSD"))
    rt = env.orch.runtime("mock")
    assert env.bus.prices.qsize() == 3 and rt.subs["prices"].lost and env.resyncs == ["mock"]
    assert set(env.orch.price_subscriptions("mock")) == {"GBPUSD", "USDJPY"}
    assert len([r for r in caplog.records if "price subscription hook failed" in r.getMessage()]) == 1   # throttled
    assert not env.orch.frames_lost


async def test_dropped_prices_are_counted_per_market(tmp_path: Path, clock: ManualClock) -> None:
    class _Counting(FakeAdapter):
        dropped_price_frames = {"EURUSD": 2, "999": 1}

    env = make(tmp_path, clock, adapter=_Counting())
    env.orch.backpressure_overflow = True
    env.orch._check_backpressure = lambda: None  # type: ignore[method-assign]   # keep the overflow on
    env.item("prices", price("EURUSD"))
    env.item("prices", price("GBPUSD"))
    env.item("prices", price("EURUSD"))
    rt = env.orch.runtime("mock")
    assert rt.dropped_prices == 3 and rt.dropped_prices_by_symbol == {"EURUSD": 2, "GBPUSD": 1}
    assert env.orch.dropped_prices_by_market("mock") == {"EURUSD": 4, "GBPUSD": 1, "999": 1}


# ===================================================================== S05 review fixes
class _StoneXLikePrices(FakeAdapter):
    """StoneX's price stream shape: one subscription per market, every market ever asked for kept for the adapter's
    life (subscribed again at each stream start); a whole-stream SUBSCRIBED only for an empty subscription; each
    market acks with its own SUBSCRIBED, fails with its own ERROR (`bad`) or never acks (`silent`)."""

    def __init__(self) -> None:
        super().__init__()
        self.wanted: set[str] = set()
        self.bad: set[str] = set()
        self.silent: set[str] = set()

    def _ack(self, market: str) -> StreamEvent | None:
        if market in self.silent:
            return None
        return mev(K.ERROR if market in self.bad else K.SUBSCRIBED, market)

    def stream_ticks(self, symbols: Sequence[str]) -> AsyncIterator[PriceDTO | StreamEvent]:
        self.calls.append(("stream_ticks", tuple(symbols)))
        return self._prices(list(symbols))

    async def _prices(self, symbols: list[str]) -> AsyncIterator[PriceDTO | StreamEvent]:
        self.calls.append(("subscribe", "prices"))
        q: asyncio.Queue[object] = asyncio.Queue()
        self.queues["prices"] = q
        markets = sorted(set(symbols) | self.wanted)
        self.wanted.update(symbols)
        if not markets:
            yield StreamEvent(K.SUBSCRIBED, "prices", "no markets yet")
        for market in markets:
            event = self._ack(market)
            if event is not None:
                yield event
        while True:
            item = await q.get()
            if isinstance(item, BaseException):
                raise item
            if isinstance(item, (PriceDTO, StreamEvent)):
                yield item

    async def ensure_ticks(self, symbols: Sequence[str]) -> None:
        self.calls.append(("ensure_ticks", tuple(symbols)))
        self.wanted.update(symbols)
        q = self.queues.get("prices")
        for market in symbols:
            event = self._ack(market)
            if q is not None and event is not None:
                q.put_nowait(event)


@pytest.mark.parametrize("failure", ["bad", "silent"])
async def test_a_flat_account_with_a_bad_tick_only_market_still_reaches_streaming(
        tmp_path: Path, clock: ManualClock, monkeypatch: pytest.MonkeyPatch, failure: str) -> None:
    """Review S05 #1: no required symbol (flat, none configured) and one tick-only market that fails or never acks.
    The stream starts with the empty required set (so StoneX reports its empty subscription), the tick-only symbol
    gets its own ensure_ticks(), and after a reconnect (the adapter kept the market) the stream is trivially
    subscribed: SNAPSHOTTING reaches STREAMING both times, never BACKOFF from the subscribe timeout."""
    monkeypatch.setattr(connmod, "backoff_delay", lambda attempt, jitter: 0.0)
    adapter = _StoneXLikePrices()
    getattr(adapter, failure).add("XAUUSD")
    env = make(tmp_path, clock, adapter=adapter)
    env.orch.set_md_tick_symbols("mock", ["XAUUSD"])
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    try:
        await wait_for(lambda: rt.conn.state is ConnState.STREAMING)
        assert adapter.called("stream_ticks") == [()]           # the tick-only symbol is not part of the start
        await wait_for(lambda: ("XAUUSD",) in adapter.called("ensure_ticks"))
        assert rt.md_subscribed == {"XAUUSD"} and rt.subs["prices"].subscribed
        adapter.push("orders", ev(K.DISCONNECTED, "orders"))     # reconnect: the adapter kept XAUUSD
        await wait_for(lambda: len(adapter.called("connect")) == 2 and rt.conn.state is ConnState.STREAMING)
        assert adapter.called("stream_ticks") == [(), ()]
        states = [new for _, new, _ in rt.conn.history]
        assert states.count(ConnState.BACKOFF) == 1             # the disconnect only, never a subscribe timeout
        assert rt.subs["prices"].subscribed and not env.orch.frames_lost
    finally:
        stop.set()
        await asyncio.wait_for(task, 3)


async def test_without_tick_only_symbols_a_flat_stream_waits_for_the_adapters_own_subscribed(
        tmp_path: Path, clock: ManualClock) -> None:
    """Flags off: an empty required set is not marked subscribed by the orchestrator (the adapter decides, as
    before); with a tick-only symbol in play it is."""
    adapter = _StoneXLikePrices()
    adapter.silent.add("GBPUSD")
    adapter.wanted.add("GBPUSD")                                # a market kept from an earlier holding
    env = make(tmp_path, clock, adapter=adapter)
    rt = env.orch.runtime("mock")
    env.orch.start_missing_streams("mock")
    await asyncio.sleep(0.05)
    assert not rt.subs["prices"].subscribed
    await _stop_streams(env)
    env.orch.set_md_tick_symbols("mock", ["XAUUSD"])
    env.orch.start_missing_streams("mock")
    try:
        assert rt.subs["prices"].subscribed and rt.md_pending == {"XAUUSD"} and not rt.md_subscribed
    finally:
        await _stop_streams(env)


async def test_a_removed_tick_only_symbol_stays_tick_only_after_a_prices_restart(tmp_path: Path,
                                                                               clock: ManualClock) -> None:
    """Review S05 #2: the adapters keep every market and subscribe it again at each start, so a symbol removed from
    the set stays tick-only: after the prices pump restarts its tick is not published (no Redis price key, no
    last_clean_marks row) and its ERROR opens no gap. Held or wanted again, it is ordinary / tick-only as usual."""
    env = make(tmp_path, clock, tick_symbols={"mock": ["EURUSD"]})
    env.orch.set_md_tick_symbols("mock", ["XAUUSD"])
    await _start_streams(env)
    try:
        rt = env.orch.runtime("mock")
        sub = rt.subs["prices"]
        env.orch.set_md_tick_symbols("mock", [])
        assert rt.md_retired == {"XAUUSD"} and env.orch.is_md_only("mock", "XAUUSD")
        env.item("prices", mev(K.ERROR, "EURUSD"))              # a required market: the prices pump restarts
        env.orch.start_missing_streams("mock")
        await wait_for(lambda: sub.subscribed)
        assert env.adapter.called("stream_ticks")[-1] == ("EURUSD",)
        assert not rt.md_subscribed and rt.md_retired == {"XAUUSD"} and env.orch.is_md_only("mock", "XAUUSD")
        await env.orch.run_snapshot("mock")
        await env.health_ops()
        rt.resync_event.clear()
        env.resyncs.clear()
        assert not sub.lost
        env.item("prices", price("XAUUSD"))                      # still streamed by the adapter
        assert env.bus.prices.qsize() == 0
        env.item("prices", mev(K.ERROR, "XAUUSD"))
        assert sub.subscribed and not sub.lost and not env.resyncs and await env.health_ops() == []
        env.orch.set_md_tick_symbols("mock", ["XAUUSD"])         # wanted again: tick-only, no longer retired
        assert not rt.md_retired and env.orch.is_md_only("mock", "XAUUSD")
        env.adapter.instruments = [_spec("XAUUSD")]
        env.item("orders", order("O1", t=clock.ms, symbol="XAUUSD"))   # held: an ordinary symbol
        assert not env.orch.is_md_only("mock", "XAUUSD")
        env.item("prices", price("XAUUSD"))
        assert env.bus.prices.qsize() == 1
    finally:
        await _stop_streams(env)


async def test_a_tick_only_subscribed_after_a_required_error_leaves_the_gap_open(tmp_path: Path,
                                                                               clock: ManualClock) -> None:
    """Review S05 #3: a tick-only market's SUBSCRIBED after a required market's ERROR never marks the stream
    subscribed, so a REST pass cannot clear the prices gap while the required market is still unsubscribed."""
    env = make(tmp_path, clock, tick_symbols={"mock": ["EURUSD"]})
    env.orch.set_md_tick_symbols("mock", ["XAUUSD"])
    await _start_streams(env)
    try:
        rt = env.orch.runtime("mock")
        sub = rt.subs["prices"]
        await env.orch.run_snapshot("mock")
        await env.health_ops()
        env.item("prices", mev(K.ERROR, "EURUSD"))
        assert not sub.subscribed and sub.lost
        env.item("prices", mev(K.SUBSCRIBED, "XAUUSD"))
        assert not sub.subscribed and "XAUUSD" in env.orch.price_subscriptions("mock")
        await env.orch.run_snapshot("mock")                      # a REST pass sent after the tick-only ack
        assert sub.lost and ("reconciled", "prices") not in await env.health_ops()
        env.item("prices", mev(K.SUBSCRIBED, "EURUSD"))          # the required market is back
        assert sub.subscribed
        await env.orch.run_snapshot("mock")
        assert not sub.lost and ("reconciled", "prices") in await env.health_ops()
    finally:
        await _stop_streams(env)


async def test_a_session_expiry_in_the_tick_only_ensure_loses_the_session(tmp_path: Path,
                                                                         clock: ManualClock) -> None:
    """Review S05 #6: SessionExpiredError from the tick-only ensure_ticks() is not a bad symbol: it is raised and
    the STREAMING step loses every subscription and goes to AUTHENTICATING, as on the required path."""
    class _Expiring(FakeAdapter):
        async def ensure_ticks(self, symbols: Sequence[str]) -> None:
            self.calls.append(("ensure_ticks", tuple(symbols)))
            raise SessionExpiredError("session gone")

    env = make(tmp_path, clock, adapter=_Expiring(), tick_symbols={"mock": ["EURUSD"]})
    await _start_streams(env)
    try:
        to_streaming(env)
        rt = env.orch.runtime("mock")
        env.orch.set_md_tick_symbols("mock", ["XAUUSD"])
        with pytest.raises(SessionExpiredError):
            await env.orch.flush_md_symbols("mock")
        assert rt.md_ensure_failed_ms == 0                      # not counted as a bad symbol
        clock.ms += 1_000
        await env.orch._step_streaming(rt, asyncio.Event())
        assert rt.auth_lost and not rt.subs["prices"].subscribed
    finally:
        await _stop_streams(env)


async def test_a_whole_stream_subscribed_opens_only_the_markets_the_adapter_subscribed(tmp_path: Path,
                                                                                     clock: ManualClock) -> None:
    """Review S05 #5: an adapter that reports `subscribed_price_markets` (a cTrader single-symbol subscribe failed,
    a StoneX symbol not resolved) gets intervals for those markets only."""
    class _Reporting(FakeAdapter):
        subscribed_price_markets = frozenset({"EURUSD"})

    env = make(tmp_path, clock, adapter=_Reporting(), tick_symbols={"mock": ["EURUSD", "GBPUSD"]})
    await _start_streams(env)
    try:
        assert env.orch.price_subscriptions("mock") == {"EURUSD": clock.ms}
    finally:
        await _stop_streams(env)
