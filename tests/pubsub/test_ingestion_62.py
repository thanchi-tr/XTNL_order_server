"""6.2 ingestion: lost frames / adapter-skipped rows open a gap, the clean-mid watch for ensure_ticks symbols,
a later market open is never lost while one is pending, and the SM4 driver restarts after a non-fence error."""
from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from src.adapters.base import BrokerError
from src.dto import InstrumentSpec, OrderDTO, PositionDTO
from src.pubsub import connection as connmod
from src.pubsub import orchestrator as orchmod
from src.pubsub.connection import ConnState
from tests.pubsub.conftest import FakeAdapter, ManualClock, account, ev, order, price
from tests.pubsub.test_orchestrator import Env, K, make, ready, to_streaming, wait_for

Page = tuple[str, str, str]


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


@pytest.fixture
def no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(connmod, "backoff_delay", lambda attempt, jitter=None: 0.0)
    monkeypatch.setattr(orchmod, "backoff_delay", lambda attempt, jitter=None: 0.0)


def paged(tmp_path: Path, clock: ManualClock, **kw: object) -> tuple[Env, list[Page]]:
    pages: list[Page] = []
    env = make(tmp_path, clock, pager=lambda s, t, b: pages.append((s, t, b)), **kw)
    return env, pages


def spec(symbol: str) -> InstrumentSpec:
    return InstrumentSpec(adapter_id="mock", symbol=symbol, base_ccy=symbol[:3], quote_ccy=symbol[3:],
                          pnl_ccy=symbol[3:], contract_size=Decimal(1), unit_kind="UNITS",
                          point_size=Decimal("0.0001"), min_qty=Decimal(1), swap_booked_to_balance=True)


def titles(pages: list[Page]) -> list[str]:
    return [t for _, t, _ in pages]


async def start(env: Env) -> tuple[asyncio.Event, asyncio.Task[None]]:
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: env.orch.runtime("mock").conn.state is ConnState.STREAMING)
    return stop, task


async def finish(stop: asyncio.Event, task: asyncio.Task[None]) -> None:
    stop.set()
    await asyncio.wait_for(task, 2)


class SkippingAdapter(FakeAdapter):
    """Counts broker rows it could not map in `unmapped_rows` (the orchestrator's UNMAPPED_ATTR contract)."""

    def __init__(self) -> None:
        super().__init__()
        self.unmapped_rows: dict[str, int] = {"orders": 0, "account": 0}

    def skip(self, stream: str = "orders") -> None:
        self.unmapped_rows[stream] += 1


# ===================================================================== frames never silently dropped
async def test_an_unexpected_stream_item_opens_the_gap_of_its_stream(tmp_path: Path, clock: ManualClock) -> None:
    env, pages = paged(tmp_path, clock)
    await ready(env)
    rt = env.orch.runtime("mock")
    env.item("orders", object())
    assert rt.subs["orders"].lost and rt.resync_event.is_set()
    assert await env.health_ops() == [("lost", "orders")]
    assert titles(pages) == ["ingestion frame lost: mock:orders"]
    assert env.orch.frames_lost == {"orders": 1}
    rt.resync_event.clear()
    env.item("prices", "garbage")                                 # a lost price frame is counted, no gap
    assert not rt.subs["prices"].lost and not rt.resync_event.is_set() and rt.dropped_prices == 1


async def test_a_row_whose_handling_raises_opens_the_gap_instead_of_killing_the_socket(
        tmp_path: Path, clock: ManualClock, monkeypatch: pytest.MonkeyPatch) -> None:
    env, pages = paged(tmp_path, clock)
    await ready(env)
    rt = env.orch.runtime("mock")

    def boom(*a: object, **k: object) -> OrderDTO:
        raise ValueError("odd row")

    monkeypatch.setattr(env.orch, "_label", boom)
    env.item("account", account(t=clock.ms))                      # no exception reaches the pump
    assert rt.subs["account"].lost and not rt.socket_lost
    assert "ingestion frame lost: mock:account" in titles(pages)


async def test_the_pump_hands_unexpected_items_over_instead_of_dropping_them(tmp_path: Path,
                                                                            clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    stop, task = await start(env)
    rt = env.orch.runtime("mock")
    await env.health_ops()
    env.adapter.push("orders", 42)
    await wait_for(lambda: env.orch.frames_lost.get("orders") == 1)
    await wait_for(lambda: any(new is ConnState.RESYNCING for _, new, _ in rt.conn.history))
    await finish(stop, task)


async def test_a_rest_row_the_adapter_skipped_keeps_the_gap_open(tmp_path: Path, clock: ManualClock) -> None:
    adapter = SkippingAdapter()
    env, pages = paged(tmp_path, clock, adapter=adapter)
    rt = env.orch.runtime("mock")
    for s in ("orders", "account", "prices"):
        env.item(s, ev(K.SUBSCRIBED, s))
    adapter.on_fetch_open_orders = adapter.skip                   # one open order could not be mapped
    rep = await env.orch.run_snapshot("mock")
    assert rep.dropped_after_watermark                            # the pass cannot clear the orders gap
    assert rt.subs["orders"].lost and not rt.subs["account"].lost and not rt.subs["prices"].lost
    assert ("reconciled", "orders") not in await env.health_ops()
    assert "ingestion frame lost: mock:orders" in titles(pages)
    adapter.on_fetch_open_orders = None                           # mapped again: the next pass clears it
    rep = await env.orch.run_snapshot("mock")
    assert not rep.dropped_after_watermark and not rt.subs["orders"].lost


async def test_a_stream_row_the_adapter_skipped_opens_a_gap_while_streaming(tmp_path: Path,
                                                                           clock: ManualClock) -> None:
    adapter = SkippingAdapter()
    env = make(tmp_path, clock, adapter=adapter)
    stop, task = await start(env)
    rt = env.orch.runtime("mock")
    await env.health_ops()
    adapter.skip("account")
    rt.wake.set()
    await wait_for(lambda: any(new is ConnState.RESYNCING for _, new, _ in rt.conn.history))
    assert ("lost", "account") in await env.health_ops()
    await finish(stop, task)


def test_unmapped_counters_are_normalised_and_start_from_the_adapter_baseline(tmp_path: Path,
                                                                              clock: ManualClock) -> None:
    norm = orchmod.IngestionOrchestrator._unmapped_counts

    class A(FakeAdapter):
        unmapped_rows: object = None

    a = A()
    assert norm(a) == {}
    a.unmapped_rows = 3
    assert norm(a) == {"orders": 3, "account": 3}
    a.unmapped_rows = {"positions": 2, "orders": 1, "account": 4, "prices": 5, "other": 9, "bad": "x"}
    assert norm(a) == {"orders": 3, "account": 4, "prices": 5}
    adapter = SkippingAdapter()
    adapter.unmapped_rows["orders"] = 7                           # skipped before registration: not ours
    env = make(tmp_path, clock, adapter=adapter)
    assert env.orch.check_unmapped("mock") == set()
    adapter.skip("orders")
    assert env.orch.check_unmapped("mock") == {"orders"}
    assert env.orch.check_unmapped("mock") == set()               # acted on once


# ===================================================================== clean-mid watch (adapter-isolation §5)
async def held(env: Env, *symbols: str) -> None:
    env.adapter.instruments = [spec(s) for s in symbols]
    env.adapter.accounts = [account()]
    await ready(env)
    to_streaming(env)
    for sym in symbols:
        env.item("orders", order(f"O-{sym}", t=env.clock.ms, symbol=sym))


async def test_a_symbol_whose_ensure_ticks_keeps_failing_pages_no_clean_mid(tmp_path: Path,
                                                                           clock: ManualClock) -> None:
    env, pages = paged(tmp_path, clock)

    async def failing(symbols: Sequence[str]) -> None:
        raise BrokerError("subscribe refused")

    env.adapter.ensure_ticks = failing                            # type: ignore[method-assign]
    await held(env, "EURUSD")
    with pytest.raises(BrokerError):
        await env.orch.flush_pending_symbols("mock")
    clock.ms += 30_000
    env.orch.check_liveness("mock")
    assert pages == []                                            # 30 s grace not exceeded yet
    clock.ms += 1
    env.orch.check_liveness("mock")
    env.orch.check_liveness("mock")
    assert titles(pages) == ["no clean mid: mock:EURUSD"]
    assert "has not succeeded" in pages[0][2]


async def test_ticks_only_at_a_wide_spread_page_and_a_clean_tick_ends_the_watch(tmp_path: Path,
                                                                               clock: ManualClock) -> None:
    env, pages = paged(tmp_path, clock, clean_spread=lambda a, s: Decimal("2") if s == "EURUSD" else None)
    await held(env, "EURUSD", "GBPUSD")
    assert await env.orch.flush_pending_symbols("mock") == ["EURUSD", "GBPUSD"]
    rt = env.orch.runtime("mock")
    wide = replace(price("EURUSD"), spread_pips=Decimal(3))
    clock.ms += 10_000
    env.item("prices", wide)
    env.item("prices", price("GBPUSD"))                           # spread 1, but no threshold: never clean
    clock.ms += 21_000
    env.item("prices", wide)
    env.item("prices", price("GBPUSD"))
    env.orch.check_liveness("mock")
    assert sorted(titles(pages)) == ["no clean mid: mock:EURUSD", "no clean mid: mock:GBPUSD"]
    assert "last spread 3 pips" in next(b for _, t, b in pages if t.endswith("EURUSD"))
    env.item("prices", price("EURUSD"))                           # spread 1 <= 2: clean
    assert "EURUSD" not in rt.clean_wanted_ms and "GBPUSD" in rt.clean_wanted_ms
    clock.ms += 60_000
    env.item("prices", wide)
    env.item("prices", price("GBPUSD"))
    env.orch.check_liveness("mock")
    clean_pages = [t for t in titles(pages) if t.startswith("no clean mid")]
    assert len(clean_pages) == 2                                  # once per episode; EURUSD no longer watched


async def test_a_clean_tick_within_30_s_never_pages_and_muting_restarts_the_grace(tmp_path: Path,
                                                                                 clock: ManualClock) -> None:
    muted: set[str] = set()
    env, pages = paged(tmp_path, clock, price_staleness_suppressed=lambda a, s: s in muted)
    await held(env, "EURUSD", "GBPUSD")
    await env.orch.flush_pending_symbols("mock")
    rt = env.orch.runtime("mock")
    clock.ms += 5_000
    env.item("prices", price("EURUSD"))                           # clean within the grace
    env.item("prices", replace(price("GBPUSD"), spread_pips=Decimal(50)))
    muted.add("GBPUSD")
    clock.ms += 40_000
    env.item("prices", price("EURUSD"))
    env.item("prices", replace(price("GBPUSD"), spread_pips=Decimal(50)))
    env.orch.check_liveness("mock")                               # GBPUSD muted (e.g. closed): grace restarts
    muted.clear()
    clock.ms += 29_000
    env.item("prices", price("EURUSD"))
    env.item("prices", replace(price("GBPUSD"), spread_pips=Decimal(50)))
    env.orch.check_liveness("mock")
    assert pages == [] and set(rt.clean_wanted_ms) == {"GBPUSD"}
    clock.ms += 2_000
    env.item("prices", price("EURUSD"))
    env.item("prices", replace(price("GBPUSD"), spread_pips=Decimal(50)))
    env.orch.check_liveness("mock")
    assert titles(pages) == ["no clean mid: mock:GBPUSD"]


async def test_a_subscribed_symbol_without_any_tick_is_paged_once_as_stale_price(tmp_path: Path,
                                                                                clock: ManualClock) -> None:
    env, pages = paged(tmp_path, clock)
    await held(env, "EURUSD")
    await env.orch.flush_pending_symbols("mock")
    env.item("account", ev(K.HEARTBEAT, "account"))
    clock.ms += 31_000
    env.orch.check_liveness("mock")
    assert titles(pages) == ["stale price: mock:EURUSD"]          # not also "no clean mid"


# ===================================================================== a later open is never lost
async def test_a_later_open_while_one_is_pending_gets_its_own_resync(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock, open_resync_jitter=lambda: 0.0)
    rt = env.orch.runtime("mock")
    t0 = clock.ms
    assert env.orch.market_open_resync("mock", open_ms=t0) == t0 + 30_000
    assert env.orch.market_open_resync("mock", open_ms=t0) == t0 + 30_000      # same open: one pass
    assert rt.open_resync_next_ms is None
    assert env.orch.market_open_resync("mock", open_ms=t0 + 20_000) == t0 + 30_000
    assert rt.open_resync_next_ms == t0 + 50_000                               # kept, not dropped
    assert env.orch.market_open_resync("mock", open_ms=t0 + 15_000) == t0 + 30_000   # covered by t0 + 50 s
    assert rt.open_resync_next_ms == t0 + 50_000
    clock.ms = t0 + 30_000
    await env.orch.run_snapshot("mock")                                        # satisfies the first open only
    assert rt.open_resync_due_ms == t0 + 50_000 and rt.open_resync_next_ms is None
    clock.ms = t0 + 50_000
    await env.orch.run_snapshot("mock")
    assert rt.open_resync_due_ms is None


async def test_an_open_arriving_while_the_open_pass_is_in_flight_is_not_lost(tmp_path: Path,
                                                                            clock: ManualClock) -> None:
    env = make(tmp_path, clock, open_resync_jitter=lambda: 0.0)
    rt = env.orch.runtime("mock")
    t0 = clock.ms
    due = env.orch.market_open_resync("mock", open_ms=t0)
    clock.ms = due
    later_open = due + 1_000

    def open_during_pass() -> None:
        env.orch.market_open_resync("mock", open_ms=later_open)

    env.adapter.on_fetch_open_orders = open_during_pass
    await env.orch.run_snapshot("mock")
    assert rt.open_resync_due_ms == later_open + 30_000                        # the in-flight pass predates it


async def test_a_pass_after_both_due_times_satisfies_both(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock, open_resync_jitter=lambda: 0.0)
    rt = env.orch.runtime("mock")
    t0 = clock.ms
    env.orch.market_open_resync("mock", open_ms=t0)
    env.orch.market_open_resync("mock", open_ms=t0 + 40_000)
    clock.ms = t0 + 80_000
    await env.orch.run_snapshot("mock")
    assert rt.open_resync_due_ms is None and rt.open_resync_next_ms is None


# ===================================================================== SM4 driver supervision
class EpochMismatch(Exception):
    """Stands in for src.db's fence loss (matched by class name)."""


async def test_a_non_fence_driver_error_restarts_sm4_instead_of_ending_it(tmp_path: Path, clock: ManualClock,
                                                                         no_backoff: None) -> None:
    calls: list[int] = []

    class Reader:
        async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("asyncpg: connection was closed in the middle of operation")
            return [], []

    env, pages = paged(tmp_path, clock, local_state=Reader())
    rt = env.orch.runtime("mock")
    stop, task = await start(env)
    assert not task.done() and len(calls) == 2
    states = [new for _, new, _ in rt.conn.history]
    assert states.index(ConnState.BACKOFF) < len(states) - 1 and states[-1] is ConnState.STREAMING
    assert any("driver error" in why for _, _, why in rt.conn.history)
    assert titles(pages) == ["ingestion driver error: mock"]
    assert "RuntimeError" in pages[0][2] and len(env.adapter.called("connect")) == 2
    assert rt.driver_errors in (0, 1)
    await finish(stop, task)


async def test_a_driver_error_outside_a_session_state_waits_and_retries(tmp_path: Path, clock: ManualClock,
                                                                       no_backoff: None,
                                                                       monkeypatch: pytest.MonkeyPatch) -> None:
    env, pages = paged(tmp_path, clock)
    rt = env.orch.runtime("mock")
    real = env.orch._wait_backoff
    failures: list[int] = []

    async def flaky(r: object, stop: asyncio.Event) -> None:
        if not failures:
            failures.append(1)
            raise OSError("odd")
        await real(rt, stop)

    monkeypatch.setattr(env.orch, "_wait_backoff", flaky)
    env.adapter.connect_errors = [BrokerError("down")]           # first connect -> BACKOFF
    stop, task = await start(env)
    assert failures == [1] and titles(pages) == ["ingestion driver error: mock"]
    await finish(stop, task)


async def test_a_lost_fence_still_ends_the_driver(tmp_path: Path, clock: ManualClock) -> None:
    class Reader:
        async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
            raise EpochMismatch("epoch moved")

    env = make(tmp_path, clock, local_state=Reader())
    stop = asyncio.Event()
    with pytest.raises(EpochMismatch):
        await asyncio.wait_for(env.orch.run_adapter("mock", stop), 2)
    assert env.orch.runtime("mock").conn.state is ConnState.DISCONNECTED


async def test_repeated_driver_errors_back_off_exponentially(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    rt = env.orch.runtime("mock")
    rt.conn.start()
    rt.conn.on_authenticated()
    stop = asyncio.Event()
    for _ in range(3):
        await env.orch._driver_error(rt, stop, RuntimeError("x"))
        assert rt.conn.state is ConnState.BACKOFF
        rt.conn.on_backoff_elapsed()
        rt.conn.on_authenticated()
        rt.conn.on_snapshot_published()                           # each reconnect resets SM4's own count
    into_backoff = [why for _, new, why in rt.conn.history if new is ConnState.BACKOFF]
    assert rt.driver_errors == 3 and len(into_backoff) == 3
    assert all(why.startswith("transient: driver error") for why in into_backoff)
    assert rt.conn.backoff_s >= 4.0                               # 2 ** (3 - 1), not SM4's reset count


async def test_a_resync_retry_after_a_skipped_row_waits_between_rest_passes(tmp_path: Path,
                                                                           clock: ManualClock) -> None:
    adapter = SkippingAdapter()
    env = make(tmp_path, clock, adapter=adapter)
    env.orch._resync_retry_s = 0.2
    stop, task = await start(env)
    rt = env.orch.runtime("mock")
    adapter.on_fetch_open_orders = adapter.skip                   # every pass skips a row again: gap stays
    adapter.skip()
    rt.wake.set()
    await wait_for(lambda: rt.conn.state is ConnState.RESYNCING)
    passes = len(adapter.called("fetch_open_orders"))
    await asyncio.sleep(0.3)
    assert len(adapter.called("fetch_open_orders")) - passes <= 2  # ~1 pass per retry interval, no spin
    assert rt.subs["orders"].lost
    await finish(stop, task)


# ===================================================================== skeptic review fixes
async def test_a_driver_error_after_every_reconnect_keeps_the_streak_until_streaming_is_stable(
        tmp_path: Path, clock: ManualClock, no_backoff: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """An error that recurs in every RESYNCING (right after a STREAMING step) must keep growing the backoff
    exponent; it used to reset after any STREAMING step, so the adapter reconnected and re-snapshotted ~1/s."""
    env, pages = paged(tmp_path, clock)
    rt = env.orch.runtime("mock")
    real_stream, real_snap = env.orch._step_streaming, env.orch._step_snapshot
    failing = [True]

    async def streaming(r: object, stop: asyncio.Event) -> None:
        if failing[0]:
            rt.conn.on_market_open()                              # every STREAMING step leads to a RESYNCING ...
            return
        await real_stream(rt, stop)

    async def snapshot(r: object) -> None:
        if failing[0] and rt.conn.state is ConnState.RESYNCING:
            raise RuntimeError("resync bug")                      # ... that fails
        await real_snap(rt)

    monkeypatch.setattr(env.orch, "_step_streaming", streaming)
    monkeypatch.setattr(env.orch, "_step_snapshot", snapshot)
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.driver_errors >= 4)                 # the streak grows across reconnects
    assert titles(pages) == ["ingestion driver error: mock"]      # throttled: one page per 5 min
    failing[0] = False
    await wait_for(lambda: rt.conn.state is ConnState.STREAMING)
    await asyncio.sleep(0.05)
    assert rt.driver_errors >= 4                                  # STREAMING, but not for 60 s yet
    clock.ms += orchmod.DRIVER_ERROR_STREAK_RESET_MS
    rt.wake.set()
    await wait_for(lambda: rt.driver_errors == 0)                 # a stable STREAMING period ends the streak
    await finish(stop, task)


async def test_pages_built_from_exception_text_redact_urls(tmp_path: Path, clock: ManualClock) -> None:
    env, pages = paged(tmp_path, clock)
    await ready(env)
    rt = env.orch.runtime("mock")
    rt.conn.start()
    rt.conn.on_authenticated()
    secret_url = "https://user:pw@broker.example/session?token=abc123"
    await env.orch._driver_error(rt, asyncio.Event(), RuntimeError(f"failed GET {secret_url}"))
    env.orch._frame_lost(rt, "orders", f"row not handled: {secret_url}")
    bodies = " ".join(b for _, _, b in pages)
    assert len(pages) == 2 and "broker.example" in bodies
    assert "abc123" not in bodies and "pw@" not in bodies and "token=" not in bodies


async def test_a_frame_failing_on_every_tick_logs_its_traceback_once_per_interval(
        tmp_path: Path, clock: ManualClock, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture) -> None:
    env = make(tmp_path, clock)
    await ready(env)
    rt = env.orch.runtime("mock")

    def boom(*a: object, **k: object) -> None:
        raise ValueError("bad tick")

    monkeypatch.setattr(env.orch, "_on_price", boom)
    with caplog.at_level("ERROR", logger=orchmod.__name__):
        for _ in range(200):
            env.item("prices", price("EURUSD"))
        clock.ms += orchmod.FRAME_LOST_LOG_INTERVAL_MS
        env.item("prices", price("EURUSD"))
    lost = [r for r in caplog.records if "frame lost" in r.getMessage()]
    assert len(lost) == 2 and lost[0].exc_info is not None        # one per interval, with the traceback
    assert env.orch.frames_lost["prices"] == 201 and rt.dropped_prices == 201
    assert not rt.subs["prices"].lost                             # a lost price frame opens no gap


async def test_driver_steps_that_never_suspend_still_yield_the_event_loop(tmp_path: Path, clock: ManualClock,
                                                                         monkeypatch: pytest.MonkeyPatch) -> None:
    """An early-return STREAMING step (e.g. a skipped-row gap) followed by a REST pass served without I/O used to
    loop without ever suspending, starving every other task (the socket pumps, the watchdog heartbeat)."""
    env = make(tmp_path, clock)
    stop = asyncio.Event()
    steps = [0]

    async def instant_step(rt: object, s: asyncio.Event) -> bool:
        steps[0] += 1
        if steps[0] >= 1_000:
            stop.set()
        return True

    monkeypatch.setattr(env.orch, "_drive_step", instant_step)
    seen: list[int] = []

    async def probe() -> None:
        seen.append(steps[0])

    probe_task = asyncio.create_task(probe())
    await asyncio.wait_for(env.orch.run_adapter("mock", stop), 5)
    await probe_task
    assert seen and seen[0] < 1_000                               # the probe ran while the driver was looping
