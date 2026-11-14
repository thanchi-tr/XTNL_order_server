"""5.2 ingestion completeness: open RESYNCING, AUTH_FAILED page, schema / checksum parks, violation pages,
flat-account poll, conn-key absence and stale-price pages, FX crosses, transfer source, credential rotation."""
from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from src.adapters.base import AuthFailedError, BrokerError
from src.dto import AccountDTO, AccountLive, InstrumentSpec, OrderDTO, OrderStatus, PositionDTO
from src.pubsub.connection import ConnState
from src.pubsub.validation import checksum_valid, schema_problem
from tests.pubsub.conftest import (
    FakeAdapter,
    ManualClock,
    account,
    cash_flow,
    ev,
    live,
    order,
    position,
    price,
)
from tests.pubsub.test_orchestrator import Env, K, make, ready, to_streaming, wait_for

Page = tuple[str, str, str]


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


def paged(tmp_path: Path, clock: ManualClock, **kw: object) -> tuple[Env, list[Page]]:
    pages: list[Page] = []
    env = make(tmp_path, clock, pager=lambda s, t, b: pages.append((s, t, b)), **kw)
    return env, pages


def spec(symbol: str) -> InstrumentSpec:
    return InstrumentSpec(adapter_id="mock", symbol=symbol, base_ccy=symbol[:3], quote_ccy=symbol[3:],
                          pnl_ccy=symbol[3:], contract_size=Decimal(1), unit_kind="UNITS",
                          point_size=Decimal("0.0001"), min_qty=Decimal(1), swap_booked_to_balance=True)


async def start(env: Env) -> tuple[asyncio.Event, asyncio.Task[None]]:
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: env.orch.runtime("mock").conn.state is ConnState.STREAMING)
    return stop, task


async def finish(stop: asyncio.Event, task: asyncio.Task[None]) -> None:
    stop.set()
    await asyncio.wait_for(task, 2)


def advance(env: Env, ms: int) -> None:
    """Move the clock and keep the socket alive (a heartbeat counts as inbound traffic), then wake the driver."""
    env.clock.ms += ms
    env.item("orders", ev(K.HEARTBEAT, "orders"))
    env.orch.runtime("mock").wake.set()


# ===================================================================== SM10 open: REST-only RESYNCING
async def test_market_open_resync_is_jittered_into_open_plus_30_to_60_s(tmp_path: Path,
                                                                       clock: ManualClock) -> None:
    for jitter, expect in ((0.0, 30_000), (0.5, 45_000), (1.0, 60_000), (7.0, 60_000)):
        env = make(tmp_path, clock, open_resync_jitter=lambda j=jitter: j)
        assert env.orch.market_open_resync("mock", open_ms=1_000) == 1_000 + expect
        assert env.orch.market_open_resync("mock", open_ms=9_999) == 1_000 + expect   # idempotent while due


async def test_market_open_runs_a_rest_only_resync_when_due(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock, open_resync_jitter=lambda: 0.5)
    rt = env.orch.runtime("mock")
    stop, task = await start(env)
    subs = list(env.adapter.called("subscribe"))
    rest_reads = len(env.adapter.called("fetch_open_orders"))
    due = env.orch.market_open_resync("mock", open_ms=clock.ms)
    advance(env, 44_000)
    await asyncio.sleep(0.05)
    assert rt.conn.state is ConnState.STREAMING and rt.open_resync_due_ms == due   # not yet due
    advance(env, 1_000)
    await wait_for(lambda: rt.open_resync_due_ms is None and rt.conn.state is ConnState.STREAMING)
    assert ("market open" in [why for _, _, why in rt.conn.history])
    assert len(env.adapter.called("fetch_open_orders")) == rest_reads + 1
    assert env.adapter.called("subscribe") == subs              # REST only: no stream re-subscribed
    assert env.adapter.called("fetch_instruments") == [None]    # and no instrument refresh
    await finish(stop, task)


async def test_a_snapshot_sent_after_the_due_time_satisfies_the_open(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock, open_resync_jitter=lambda: 0.0)
    rt = env.orch.runtime("mock")
    due = env.orch.market_open_resync("mock", open_ms=clock.ms)
    await env.orch.run_snapshot("mock")                          # sent before open + 30 s: does not count
    assert rt.open_resync_due_ms == due
    clock.ms = due
    await env.orch.run_snapshot("mock")                          # SNAPSHOTTING after the open
    assert rt.open_resync_due_ms is None


# ===================================================================== AUTH_FAILED page
async def test_auth_failed_pages_critical_after_three_attempts(tmp_path: Path, clock: ManualClock) -> None:
    env, pages = paged(tmp_path, clock)
    env.adapter.connect_errors = [AuthFailedError("bad password")] * 3
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.conn.state is ConnState.AUTH_FAILED)
    await asyncio.sleep(0.05)
    assert len(env.adapter.called("connect")) == 3
    crit = [p for p in pages if p[0] == "critical"]
    assert len(crit) == 1 and crit[0][1] == "broker AUTH_FAILED: mock" and "bad password" in crit[0][2]
    await finish(stop, task)


# ===================================================================== schema / checksum parks
def test_schema_problem_and_checksum_checks() -> None:
    assert schema_problem(order(), "mock") is None
    assert schema_problem(order(), "other") == "adapter_id 'mock' is not 'other'"
    assert schema_problem(replace(order(), broker_order_id=" "), "mock") == "broker_order_id empty"
    assert schema_problem(replace(position(), side="LONG"), "mock") == "side invalid"   # type: ignore[arg-type]
    assert schema_problem(replace(account(), balance=Decimal("NaN")), "mock") == "balance invalid"
    assert schema_problem(replace(account(), currency=""), "mock") == "currency empty"
    assert schema_problem(replace(cash_flow(), kind="GIFT"), "mock") == "kind invalid"   # type: ignore[arg-type]
    assert schema_problem(replace(position(), units=Decimal(-1)), "mock") == "units invalid"
    o = order()
    assert checksum_valid(o)
    object.__setattr__(o, "stop_loss", Decimal("1.0"))          # mutated after it was sealed
    assert not checksum_valid(o)


async def test_schema_invalid_stream_rows_are_parked_and_paged(tmp_path: Path, clock: ManualClock) -> None:
    env, pages = paged(tmp_path, clock)
    env.item("orders", replace(order("O1", t=clock.ms), symbol=""))
    env.item("account", replace(cash_flow("T1"), currency=""))
    env.item("orders", replace(position("P9", t=clock.ms), adapter_id="other"))
    assert env.orders() == [] and env.account_items() == [] and env.positions() == []
    assert env.ring() == [("O1", "schema_invalid"), ("T1", "schema_invalid"), ("P9", "schema_invalid")]
    assert [r.adapter_id for r in env.spooler._ring] == ["mock", "mock", "mock"]   # re-read where it came from
    assert [t for _, t, _ in pages] == ["ingestion schema_invalid: mock"]          # throttled per reason
    assert "symbol empty" in pages[0][2] and env.orch.parked == {"schema_invalid": 3}


async def test_checksum_invalid_row_is_parked(tmp_path: Path, clock: ManualClock) -> None:
    env, pages = paged(tmp_path, clock)
    bad = account(t=1)
    object.__setattr__(bad, "checksum", "0" * 64)
    env.item("account", bad)
    assert env.account_items() == [] and env.ring() == [("A1", "checksum")]
    assert pages[0][1] == "ingestion checksum: mock"


async def test_schema_invalid_rest_row_keeps_the_gap_open(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    await ready(env)
    env.item("orders", ev(K.GAP, "orders"))
    await env.health_ops()
    env.adapter.open_orders = [replace(order("O1", t=clock.ms), account_id="")]
    await env.orch.run_snapshot("mock")
    env.orders()
    assert env.orch.runtime("mock").subs["orders"].lost and env.ring() == [("O1", "schema_invalid")]


# ===================================================================== violation pages
async def test_illegal_transition_and_future_timestamp_are_parked_and_paged(tmp_path: Path,
                                                                           clock: ManualClock) -> None:
    env, pages = paged(tmp_path, clock)
    env.item("orders", order("O1", OrderStatus.FILLED, t=clock.ms - 10, remaining="0"))
    env.item("orders", order("O1", OrderStatus.WORKING, t=clock.ms - 5))
    env.item("orders", order("O2", t=clock.ms + 6_000))
    env.orders()
    assert env.ring() == [("O1", "illegal_transition"), ("O2", "future_timestamp")]
    assert [(s, t) for s, t, _ in pages] == [("high", "ingestion illegal_transition: mock"),
                                             ("high", "ingestion future_timestamp: mock")]
    assert "FILLED -> WORKING" in pages[0][2] and "6000 ms ahead" in pages[1][2]


async def test_violation_pages_are_throttled_and_count_what_they_held_back(tmp_path: Path,
                                                                          clock: ManualClock) -> None:
    env, pages = paged(tmp_path, clock)
    for i in range(3):
        env.item("orders", order(f"F{i}", t=clock.ms + 10_000))
    assert len(pages) == 1
    clock.ms += 60_000
    env.item("orders", order("F9", t=clock.ms + 10_000))
    assert len(pages) == 2 and "(+2 more since the last page)" in pages[1][2]
    assert len(env.ring()) == 4                                   # every row parked regardless


async def test_a_failing_pager_never_breaks_the_parser_path(tmp_path: Path, clock: ManualClock) -> None:
    def boom(*_: str) -> None:
        raise RuntimeError("pager down")
    env = make(tmp_path, clock, pager=boom)
    env.item("orders", order("O1", t=clock.ms + 10_000))
    env.item("orders", order("O2", t=clock.ms))
    assert env.ring() == [("O1", "future_timestamp")] and [o.broker_order_id for o in env.orders()] == ["O2"]


async def test_stored_rows_are_loaded_before_any_stream_starts(tmp_path: Path, clock: ManualClock) -> None:
    """A stream row arriving before the first REST pass is still checked against the stored row."""
    adapter = FakeAdapter()

    class Reader:
        async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
            adapter.calls.append(("open_entities", adapter_id))
            return [order("DB1", OrderStatus.PARTIALLY_FILLED, t=1, remaining="500")], []

    env = make(tmp_path, clock, adapter=adapter, local_state=Reader())
    stop, task = await start(env)
    names = [n for n, _ in adapter.calls]
    assert names.index("open_entities") < names.index("subscribe")
    assert names.count("open_entities") == 1
    await finish(stop, task)


# ===================================================================== flat-account poll
async def test_flat_account_is_polled_every_60_s(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.adapter.accounts = [account()]
    stop, task = await start(env)
    polls = len(env.adapter.called("fetch_account_snapshot"))
    advance(env, 30_000)
    await asyncio.sleep(0.05)
    assert len(env.adapter.called("fetch_account_snapshot")) == polls           # the snapshot proved it
    advance(env, 31_000)
    await wait_for(lambda: len(env.adapter.called("fetch_account_snapshot")) == polls + 1)
    rows = [a for a in env.account_items() if isinstance(a, AccountDTO)]
    assert rows[-1].source == "REST"
    await finish(stop, task)


async def test_an_account_with_an_open_position_is_not_polled(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.adapter.accounts = [account()]
    env.adapter.positions = [position("P1", t=clock.ms)]
    stop, task = await start(env)
    polls = len(env.adapter.called("fetch_account_snapshot"))
    advance(env, 61_000)
    await asyncio.sleep(0.1)
    assert len(env.adapter.called("fetch_account_snapshot")) == polls
    await finish(stop, task)


async def test_failing_account_polls_page_once_and_clear_on_success(tmp_path: Path, clock: ManualClock) -> None:
    class Flaky(FakeAdapter):
        fail = True

        async def fetch_account_snapshot(self) -> tuple[list[AccountDTO], list[AccountLive]]:
            self.calls.append(("fetch_account_snapshot", None))
            if self.fail:
                raise BrokerError("503")
            return list(self.accounts), list(self.lives)

    adapter = Flaky()
    env, pages = paged(tmp_path, clock, adapter=adapter)
    to_streaming(env)
    for _ in range(4):
        assert await env.orch.poll_account("mock") is False
    assert [t for _, t, _ in pages] == ["account path unproven: mock"]
    adapter.fail = False
    adapter.accounts = [account()]
    assert await env.orch.poll_account("mock") is True
    adapter.fail = True
    for _ in range(3):
        await env.orch.poll_account("mock")
    assert len(pages) == 2                                        # re-armed by the success


# ===================================================================== conn-key absence
async def test_conn_key_absent_over_60_s_while_streaming_and_open_pages(tmp_path: Path,
                                                                        clock: ManualClock) -> None:
    is_open = {"v": False}
    env, pages = paged(tmp_path, clock, market_open=lambda a: is_open["v"])
    await ready(env)
    to_streaming(env)
    clock.ms += 30_000 + 61_000                                   # TTL ran out 61 s ago
    env.orch.check_liveness("mock")
    assert pages == []                                            # market closed: no page
    is_open["v"] = True
    env.orch.check_liveness("mock")
    env.orch.check_liveness("mock")
    assert sorted(t for _, t, _ in pages) == ["conn key absent: mock:account", "conn key absent: mock:orders",
                                              "conn key absent: mock:prices"]
    env.item("orders", ev(K.HEARTBEAT, "orders"))                 # the key is written again: re-armed
    env.orch.check_liveness("mock")
    env.item("orders", ev(K.ERROR, "orders"))                     # a gap deletes it
    clock.ms += 60_000
    env.orch.check_liveness("mock")
    assert len(pages) == 3
    clock.ms += 1_001
    env.orch.check_liveness("mock")
    assert pages[-1][1] == "conn key absent: mock:orders" and len(pages) == 4


async def test_no_conn_alert_unless_streaming(tmp_path: Path, clock: ManualClock) -> None:
    env, pages = paged(tmp_path, clock)
    clock.ms += 1_000_000
    env.orch.check_liveness("mock")
    assert pages == []


# ===================================================================== stale-price page
async def test_stale_price_pages_per_open_symbol_and_is_muted_while_closed(tmp_path: Path,
                                                                          clock: ManualClock) -> None:
    muted: set[str] = set()
    env, pages = paged(tmp_path, clock, price_staleness_suppressed=lambda a, s: s in muted)
    env.adapter.instruments = [spec("EURUSD"), spec("GBPUSD")]
    env.adapter.accounts = [account()]
    await ready(env)
    to_streaming(env)
    for sym in ("EURUSD", "GBPUSD"):
        env.item("orders", order(f"O-{sym}", t=clock.ms, symbol=sym))
    assert await env.orch.flush_pending_symbols("mock") == ["EURUSD", "GBPUSD"]
    env.item("account", ev(K.HEARTBEAT, "account"))
    clock.ms += 20_000
    env.item("prices", price("EURUSD"))
    clock.ms += 11_000
    muted.add("GBPUSD")
    env.orch.check_liveness("mock")
    assert pages == []                                            # EURUSD ticked 11 s ago; GBPUSD muted
    muted.clear()
    clock.ms += 25_000
    env.orch.check_liveness("mock")                               # GBPUSD's grace restarted at the unmute
    assert [t for _, t, _ in pages] == ["stale price: mock:EURUSD"]
    clock.ms += 6_000
    env.orch.check_liveness("mock")
    env.orch.check_liveness("mock")
    assert [t for _, t, _ in pages] == ["stale price: mock:EURUSD", "stale price: mock:GBPUSD"]
    assert pages[1][2] == "no tick yet"
    env.item("prices", price("EURUSD"))                           # a tick re-arms the episode
    env.orch.check_liveness("mock")
    clock.ms += 31_000
    env.orch.check_liveness("mock")
    assert [t for _, t, _ in pages].count("stale price: mock:EURUSD") == 2


# ===================================================================== FX crosses
async def test_crosses_are_added_once_the_account_currency_is_known(tmp_path: Path, clock: ManualClock) -> None:
    """Gap: on the first SNAPSHOTTING the prices pump starts before the account (and its currency) is known."""
    adapter = FakeAdapter()
    adapter.instruments = [spec("EURUSD"), spec("GBPJPY"), spec("USDJPY")]
    adapter.accounts = [account()]                                 # USD, known only from the REST pass

    class Reader:
        async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
            return [], [replace(position("P1", t=1), symbol="GBPJPY")]

    env = make(tmp_path, clock, adapter=adapter, local_state=Reader())
    stop, task = await start(env)
    assert adapter.called("stream_ticks") == [("GBPJPY",)]          # no account currency yet
    await wait_for(lambda: ("USDJPY",) in adapter.called("ensure_ticks"))
    assert "USDJPY" in env.orch.runtime("mock").tick_symbols
    await finish(stop, task)


async def test_a_live_row_currency_also_adds_crosses_for_subscribed_symbols(tmp_path: Path,
                                                                           clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.adapter.instruments = [spec("GBPJPY"), spec("USDJPY")]
    await env.orch.run_snapshot("mock")
    to_streaming(env)
    rt = env.orch.runtime("mock")
    env.item("orders", order("O1", t=clock.ms, symbol="GBPJPY"))
    assert await env.orch.flush_pending_symbols("mock") == ["GBPJPY"]   # no account currency known
    rt.tasks["prices"] = asyncio.get_running_loop().create_future()      # type: ignore[assignment]
    env.item("account", live())                                          # USD account seen on a live row
    clock.ms += 1_000
    assert await env.orch.flush_pending_symbols("mock") == ["USDJPY"]
    env.item("orders", order("O1", t=clock.ms + 1, symbol="GBPJPY"))     # already subscribed: nothing new
    assert rt.pending_symbols == set()


# ===================================================================== transfer source
async def test_transfer_source_is_declared_or_unverified(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    assert env.orch.transfer_source("mock") is False               # undeclared: transfers_unverified
    env.adapter.HAS_TRANSFER_SOURCE = True                          # type: ignore[attr-defined]
    assert env.orch.transfer_source("mock") is True
    env2 = make(tmp_path, clock, transfer_sources={"mock": False})
    env2.adapter.HAS_TRANSFER_SOURCE = True                         # type: ignore[attr-defined]
    assert env2.orch.transfer_source("mock") is False              # the wiring's declaration wins
    assert env.orch.transfer_source("nope") is False


# ===================================================================== credential rotation
async def test_rotated_credentials_leave_auth_failed_within_the_term(tmp_path: Path, clock: ManualClock) -> None:
    old = FakeAdapter()
    old.connect_errors = [AuthFailedError("password changed")] * 3
    new = FakeAdapter()
    rotated = {"ready": False}

    def refresh(adapter_id: str, current: object) -> FakeAdapter | None:
        return new if rotated["ready"] and current is not new else None

    env = make(tmp_path, clock, adapter=old, refresh_adapter=refresh, auth_poll_s=0.01)
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.conn.state is ConnState.AUTH_FAILED)
    await asyncio.sleep(0.05)
    assert rt.conn.state is ConnState.AUTH_FAILED and len(old.called("connect")) == 3
    rotated["ready"] = True                                          # the secret was rotated
    await wait_for(lambda: rt.conn.state is ConnState.STREAMING)
    assert env.orch.adapter("mock") is new and new.called("connect") == [None]
    assert len(old.called("connect")) == 3 and old.called("close")
    await finish(stop, task)


async def test_a_rotation_is_used_at_the_next_connect(tmp_path: Path, clock: ManualClock) -> None:
    old, new = FakeAdapter(), FakeAdapter()
    env = make(tmp_path, clock, adapter=old, refresh_adapter=lambda a, cur: new if cur is old else None)
    stop, task = await start(env)
    assert old.called("connect") == [] and new.called("connect") == [None]
    await finish(stop, task)


async def test_a_refresh_with_another_adapter_id_is_ignored(tmp_path: Path, clock: ManualClock) -> None:
    old = FakeAdapter()
    env = make(tmp_path, clock, adapter=old, refresh_adapter=lambda a, cur: FakeAdapter("other"))
    stop, task = await start(env)
    assert env.orch.adapter("mock") is old and old.called("connect") == [None]
    await finish(stop, task)


# ===================================================================== skeptic review (5.2 follow-ups)
async def test_two_leg_fx_path_subscribes_both_legs_oversee_will_mark(tmp_path: Path, clock: ManualClock) -> None:
    """GBPJPY (pnl JPY) on an AUD account with no AUDJPY: Oversee converts JPY -> USD -> AUD, so both
    USDJPY and AUDUSD must be ticked. With a direct AUDJPY only that pair is needed."""
    env = make(tmp_path, clock)
    env.adapter.instruments = [spec("GBPJPY"), spec("USDJPY"), spec("AUDUSD"), spec("EURCHF")]
    env.adapter.accounts = [replace(account(), currency="AUD")]
    env.adapter.positions = [replace(position("P1", t=clock.ms), symbol="GBPJPY")]
    await env.orch.run_snapshot("mock")
    assert env.orch.required_tick_symbols("mock") == ["AUDUSD", "GBPJPY", "USDJPY"]
    env.adapter.instruments = env.adapter.instruments + [spec("AUDJPY")]
    await env.orch.run_snapshot("mock")
    assert env.orch.required_tick_symbols("mock") == ["AUDJPY", "GBPJPY"]


async def test_an_unserialisable_parked_row_opens_a_gap_but_not_backpressure(tmp_path: Path, clock: ManualClock,
                                                                             monkeypatch: pytest.MonkeyPatch) -> None:
    env = make(tmp_path, clock)
    await ready(env)
    import src.pubsub.orchestrator as orch_mod

    def boom(*_: object) -> None:
        raise TypeError("not JSON")
    monkeypatch.setattr(orch_mod, "dlq_row", boom)
    env.item("orders", order("O1", t=clock.ms + 10_000))              # future_timestamp, cannot be parked
    rt = env.orch.runtime("mock")
    assert rt.subs["orders"].lost and rt.resync_event.is_set()      # fail closed: the gap is open
    assert env.orch.backpressure_overflow is False                 # the ring has room: prices still flow
    env.item("prices", price("EURUSD"))
    assert rt.dropped_prices == 0


async def test_an_account_with_only_working_orders_is_flat_and_polled(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.adapter.accounts = [account()]
    env.adapter.open_orders = [order("O1", t=clock.ms)]
    stop, task = await start(env)
    polls = len(env.adapter.called("fetch_account_snapshot"))
    advance(env, 61_000)
    await wait_for(lambda: len(env.adapter.called("fetch_account_snapshot")) == polls + 1)
    await finish(stop, task)


async def test_a_hung_or_odd_account_poll_counts_as_a_failure(tmp_path: Path, clock: ManualClock) -> None:
    class Odd(FakeAdapter):
        mode = "hang"

        async def fetch_account_snapshot(self) -> tuple[list[AccountDTO], list[AccountLive]]:
            if self.mode == "hang":
                await asyncio.Event().wait()
            raise ValueError("unparseable account frame")

    env, pages = paged(tmp_path, clock, adapter=Odd(), account_poll_timeout_s=0.01)
    to_streaming(env)
    assert await env.orch.poll_account("mock") is False             # timed out, never hangs the driver
    env.adapter.mode = "raise"                                       # type: ignore[attr-defined]
    assert await env.orch.poll_account("mock") is False             # an unexpected error never kills it
    assert await env.orch.poll_account("mock") is False
    assert env.orch.runtime("mock").account_poll_failures == 3
    assert [t for _, t, _ in pages] == ["account path unproven: mock"]


async def test_rotated_credentials_get_their_own_three_attempts(tmp_path: Path, clock: ManualClock) -> None:
    old, new = FakeAdapter(), FakeAdapter()
    old.connect_errors = [AuthFailedError("password changed")] * 3
    new.connect_errors = [AuthFailedError("not yet propagated")]
    env = make(tmp_path, clock, adapter=old,
               refresh_adapter=lambda a, cur: new if cur is old and len(old.called("connect")) >= 2 else None)
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.conn.state in (ConnState.STREAMING, ConnState.AUTH_FAILED))
    assert rt.conn.state is ConnState.STREAMING                      # 2 old failures + 1 new one: not 3
    assert len(old.called("connect")) == 2 and len(new.called("connect")) == 2
    await finish(stop, task)


def test_fx_via_order_matches_oversee() -> None:
    from src.oversee.risk import PREFERRED_CROSS
    from src.pubsub.orchestrator import FX_VIA_ORDER
    assert FX_VIA_ORDER == PREFERRED_CROSS
