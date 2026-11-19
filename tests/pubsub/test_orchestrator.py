from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from dataclasses import replace as dc_replace
from decimal import Decimal
from pathlib import Path

import pytest

from src.adapters.base import AuthFailedError, StreamEventKind, VersionDecl
from src.adapters.mock import MockBrokerAdapter
from src.adapters.ratelimit import RestPriority, mark_rest_sent
from src.dto import AccountDTO, InstrumentSpec, OrderDTO, OrderStatus, PositionDTO, version_str
from src.pubsub import connection as connmod
from src.pubsub.bus import EventBus
from src.pubsub.connection import ConnState
from src.pubsub.hlc import LOGICAL_BITS, HybridLogicalClock
from src.pubsub.orchestrator import IngestionOrchestrator, RereadStatus
from src.pubsub.spooler import DlqSpooler
from tests.pubsub.conftest import (
    FakeAdapter,
    FakeDlqSink,
    FakeHealth,
    ManualClock,
    account,
    cash_flow,
    ev,
    live,
    order,
    position,
    price,
)

K = StreamEventKind
ROLL = 123


@dataclass
class Env:
    orch: IngestionOrchestrator
    adapter: FakeAdapter
    bus: EventBus
    spooler: DlqSpooler
    sink: FakeDlqSink
    health: FakeHealth
    clock: ManualClock
    resyncs: list[str] = field(default_factory=list)

    def item(self, stream: str, x: object) -> None:
        self.orch.handle_stream_item("mock", stream, x)  # type: ignore[arg-type]

    def _consumed(self, item: object, ok: bool = True) -> None:
        """Draining a topic here plays the consumer: the row is committed (settled on the barrier)."""
        seq = getattr(item, "seq_id", None)
        if isinstance(seq, int):
            self.bus.commit_barrier.settle(seq, ok)

    def orders(self, commit: bool = True) -> list[OrderDTO]:
        out = []
        while not self.bus.orders.empty():
            out.append(self.bus.orders.get_nowait())
        if commit:
            for o in out:
                self._consumed(o)
        return out

    def positions(self, commit: bool = True) -> list[PositionDTO]:
        out = []
        while not self.bus.positions.empty():
            out.append(self.bus.positions.get_nowait())
        if commit:
            for p in out:
                self._consumed(p)
        return out

    def account_items(self, commit: bool = True) -> list[object]:
        out: list[object] = []
        while not self.bus.account.empty():
            out.append(self.bus.account.get_nowait())
        if commit:
            for a in out:
                self._consumed(a)
        return out

    def ring(self) -> list[tuple[str, str]]:
        return [(r.entity_id, r.error_reason) for r in self.spooler._ring]

    async def health_ops(self) -> list[tuple[str, str]]:
        await self.orch.drain_health()
        ops = [(op, stream) for op, _, stream in self.health.ops]
        self.health.ops.clear()
        return ops


def make(tmp_path: Path, clock: ManualClock, *, adapter: FakeAdapter | None = None, bus: EventBus | None = None,
         ring: int = 100, local_state: object = None, **kw: object) -> Env:
    adapter = adapter or FakeAdapter()
    bus = bus or EventBus()
    sink = FakeDlqSink()
    sp = DlqSpooler(sink, capacity=ring, spill_path=tmp_path / "s.bin")
    health = FakeHealth()
    resyncs: list[str] = []
    orch = IngestionOrchestrator(bus, sp, health, HybridLogicalClock(clock), clock_ms=clock,
                                 roll_ms=lambda now: ROLL, on_resync_needed=resyncs.append,
                                 jitter=lambda: 0.0, local_state=local_state,  # type: ignore[arg-type]
                                 subscribe_timeout_s=1.0, resync_retry_s=0.01, **kw)  # type: ignore[arg-type]
    orch.add_adapter(adapter)
    return Env(orch, adapter, bus, sp, sink, health, clock, resyncs)


async def ready(env: Env) -> None:
    """All three streams SUBSCRIBED and reconciled by a snapshot; topics and health emptied."""
    for s in ("orders", "account", "prices"):
        env.item(s, ev(K.SUBSCRIBED, s))
    await env.orch.run_snapshot("mock")
    env.orders(), env.positions(), env.account_items()
    await env.health_ops()
    env.orch.runtime("mock").resync_event.clear()


def to_streaming(env: Env) -> None:
    conn = env.orch.runtime("mock").conn
    conn.start()
    conn.on_authenticated()
    conn.on_snapshot_published()


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


# ===================================================================== labelling
async def test_stream_row_labelled_at_arrival(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.item("orders", order(t=clock.ms - 100))
    (o,) = env.orders()
    assert o.source == "STREAM"
    assert o.observed_seq == clock.ms << LOGICAL_BITS
    assert o.seq_id > o.observed_seq
    assert o.broker_updated_at_ms == clock.ms - 100
    assert env.orch.runtime("mock").watermark_ms == clock.ms - 100


async def test_seq_id_strictly_monotonic(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    for i in range(20):
        env.item("orders", order(f"O{i}", t=clock.ms))
        env.item("prices", price(f"S{i}"))
        env.item("account", cash_flow(f"T{i}"))
    seqs = sorted([o.seq_id for o in env.orders()])
    prices = [env.bus.prices.get_nowait().seq_id for _ in range(20)]
    flows = [x.seq_id for x in env.account_items()]  # type: ignore[attr-defined]
    allseq = seqs + prices + flows
    assert len(set(allseq)) == 60 and all(s > 0 for s in allseq)
    assert prices == sorted(prices) and flows == sorted(flows)


async def test_rest_rows_observed_at_send_time(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    a = env.adapter
    a.open_orders = [order("O1", t=clock.ms - 10)]
    a.positions = [position("P1", t=clock.ms - 10)]
    a.accounts = [account()]
    send_ms = clock.ms
    a.on_fetch_open_orders = lambda: setattr(clock, "ms", clock.ms + 3000)   # response arrives later
    rep = await env.orch.run_snapshot("mock")
    (o,) = env.orders()
    (p,) = env.positions()
    (acc,) = env.account_items()
    assert rep.send_ms == send_ms
    for row in (o, p, acc):
        assert row.source == "REST"                                # type: ignore[attr-defined]
        assert row.observed_seq == rep.send_seq                    # type: ignore[attr-defined]
        assert row.seq_id > rep.send_seq                           # type: ignore[attr-defined]
    assert acc.broker_updated_at_ms == send_ms                     # type: ignore[attr-defined]  # local-time domain
    assert a.called("fetch_cash_flows") == [ROLL]


async def test_local_time_domain_stream_uses_receipt_time(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.item("account", account(t=42))          # StoneX account data has no broker time
    (acc,) = env.account_items()
    assert acc.broker_updated_at_ms == clock.ms  # type: ignore[attr-defined]
    assert env.orch.runtime("mock").watermark_ms is None


async def test_untimed_rest_row_gets_stream_watermark(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.item("orders", order("S1", t=clock.ms - 500))
    env.orders()
    env.adapter.open_orders = [order("R1", t=0), order("R2", t=clock.ms - 900)]
    rep = await env.orch.run_snapshot("mock")
    got = {o.broker_order_id: o for o in env.orders() if o.source == "REST"}
    assert rep.watermark_ms == clock.ms - 500
    assert got["R1"].broker_updated_at_ms == clock.ms - 500    # watermark, never a local clock
    assert got["R2"].broker_updated_at_ms == clock.ms - 900    # its own broker time


async def test_untimed_rest_row_without_watermark_gets_zero(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.adapter.open_orders = [order("R1", t=0)]
    await env.orch.run_snapshot("mock")
    assert env.orders()[0].broker_updated_at_ms == 0


async def test_missing_broker_seq_inherits_last_seen(tmp_path: Path, clock: ManualClock) -> None:
    ad = FakeAdapter(versioning={"ORDER": VersionDecl(True, True), "POSITION": VersionDecl(False, True),
                                 "ACCOUNT": VersionDecl(True, False)})
    env = make(tmp_path, clock, adapter=ad)
    env.item("orders", order(t=clock.ms, bseq=7))
    env.item("orders", order(t=clock.ms + 1, bseq=None, stop="1.05"))
    env.item("account", account(bseq=3))
    env.item("account", account("9000", bseq=None))
    assert [o.broker_seq for o in env.orders()] == [7, 7]
    assert [a.broker_seq for a in env.account_items()] == [3, 3]  # type: ignore[attr-defined]


async def test_undeclared_broker_seq_is_not_inherited(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.item("orders", order(t=clock.ms, bseq=7))
    env.item("orders", order(t=clock.ms + 1, bseq=None, stop="1.05"))
    assert [o.broker_seq for o in env.orders()] == [7, None]


# ===================================================================== validation -> DLQ
async def test_future_timestamp_goes_to_dlq(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.item("orders", order("F1", t=clock.ms + 5_001))
    env.item("orders", order("OK", t=clock.ms + 5_000))
    assert [o.broker_order_id for o in env.orders()] == ["OK"]
    assert env.ring() == [("F1", "future_timestamp")]
    assert env.orch.runtime("mock").watermark_ms == clock.ms + 5_000


async def test_illegal_transition_goes_to_dlq(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.item("orders", order("O1", OrderStatus.FILLED, t=clock.ms - 10))   # first observation: any status
    env.item("orders", order("O1", OrderStatus.WORKING, t=clock.ms))
    assert [o.status for o in env.orders()] == [OrderStatus.FILLED]
    assert env.ring() == [("O1", "illegal_transition")]


async def test_partial_fill_while_pending_is_implied_ack(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.item("orders", order("O1", OrderStatus.PENDING, t=clock.ms - 10))
    env.item("orders", order("O1", OrderStatus.PARTIALLY_FILLED, t=clock.ms, remaining="400"))
    assert [o.status for o in env.orders()] == [OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]
    assert env.ring() == []


async def test_stale_rest_row_is_published_not_parked(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.item("orders", order("O1", OrderStatus.FILLED, t=clock.ms))
    env.orders()
    env.adapter.open_orders = [order("O1", OrderStatus.WORKING, t=clock.ms - 1000)]   # older version
    await env.orch.run_snapshot("mock")
    assert [o.status for o in env.orders()] == [OrderStatus.WORKING]   # committer drops it as STALE
    assert env.ring() == []
    assert env.orch.runtime("mock").orders["mock:O1"].status is OrderStatus.FILLED


async def test_closed_position_cannot_reopen(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.item("orders", position("P1", "CLOSED", t=clock.ms - 10))
    env.item("orders", position("P1", "OPEN", t=clock.ms))
    assert [p.status for p in env.positions()] == ["CLOSED"]
    assert env.ring() == [("P1", "illegal_transition")]


async def test_missing_client_order_id_optional_violation(tmp_path: Path, clock: ManualClock) -> None:
    from dataclasses import replace
    env = make(tmp_path, clock, require_client_order_id=True)
    env.item("orders", replace(order("O1", t=clock.ms), client_order_id=None))
    assert env.ring() == [("O1", "missing_client_order_id")]


# ===================================================================== backpressure
async def test_queue_full_goes_to_dlq(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock, bus=EventBus(orders_maxsize=1, account_maxsize=1))
    env.item("orders", order("O1", t=clock.ms))
    env.item("orders", order("O2", t=clock.ms))
    env.item("account", account())
    env.item("account", cash_flow("T1"))
    env.item("account", live())                     # live marks are dropped, never parked
    assert env.ring() == [("O2", "QueueFull"), ("T1", "QueueFull")]
    assert not env.orch.backpressure_overflow
    assert env.orch.runtime("mock").dropped_live == 1


async def test_double_full_opens_gap_and_requests_resync(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock, bus=EventBus(orders_maxsize=1), ring=2)
    await ready(env)
    rt = env.orch.runtime("mock")
    for i in range(4):
        env.item("orders", order(f"O{i}", t=clock.ms))
    assert env.bus.orders.qsize() == 1 and len(env.spooler) == 2
    assert env.orch.backpressure_overflow
    assert rt.resync_event.is_set() and env.resyncs == ["mock"]
    assert await env.health_ops() == [("lost", "orders")]
    assert rt.subs["orders"].lost and rt.subs["orders"].dropped_seq > 0
    env.item("prices", price("EURUSD"))                 # price frames dropped at the parser
    assert env.bus.prices.empty() and rt.dropped_prices == 1
    await env.spooler.flush()                           # ring drains below 50 %
    env.item("prices", price("EURUSD"))
    assert not env.orch.backpressure_overflow and env.bus.prices.qsize() == 1


async def test_overflow_after_watermark_blocks_reconcile(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock, bus=EventBus(orders_maxsize=1), ring=1)
    await ready(env)
    env.adapter.open_orders = [order("A", t=clock.ms), order("B", t=clock.ms), order("C", t=clock.ms)]
    rep = await env.orch.run_snapshot("mock")
    assert rep.dropped_after_watermark
    assert await env.health_ops() == [("lost", "orders")]   # not reconciled by the same pass


# ===================================================================== subscription health
async def test_health_sequencing(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    rt = env.orch.runtime("mock")
    for s in ("orders", "account", "prices"):
        env.item(s, ev(K.SUBSCRIBED, s))
    env.item("orders", ev(K.HEARTBEAT, "orders"))     # still lost (no reconcile yet): no heartbeat
    assert await env.health_ops() == []
    await env.orch.run_snapshot("mock")
    assert sorted(await env.health_ops()) == [("reconciled", "account"), ("reconciled", "orders"),
                                              ("reconciled", "prices")]
    env.item("orders", ev(K.HEARTBEAT, "orders"))
    assert await env.health_ops() == [("heartbeat", "orders")]

    env.item("account", ev(K.ERROR, "account"))
    env.item("account", ev(K.HEARTBEAT, "account"))   # not SUBSCRIBED: ignored
    assert await env.health_ops() == [("lost", "account")]
    assert rt.resync_event.is_set()
    env.item("account", ev(K.SUBSCRIBED, "account"))
    assert await env.health_ops() == []               # SUBSCRIBED alone never clears the gap
    await env.orch.run_snapshot("mock")
    assert await env.health_ops() == [("reconciled", "account")]


async def test_subscription_acked_after_send_needs_another_pass(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.item("orders", ev(K.SUBSCRIBED, "orders"))
    env.item("account", ev(K.SUBSCRIBED, "account"))
    env.adapter.on_fetch_open_orders = lambda: env.item("prices", ev(K.SUBSCRIBED, "prices"))
    rep = await env.orch.run_snapshot("mock")
    assert rep.all_subscribed
    assert sorted(await env.health_ops()) == [("reconciled", "account"), ("reconciled", "orders")]
    env.adapter.on_fetch_open_orders = None
    await env.orch.run_snapshot("mock")
    assert await env.health_ops() == [("reconciled", "prices")]


async def test_gap_frame_and_disconnect(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    await ready(env)
    rt = env.orch.runtime("mock")
    env.item("orders", ev(K.GAP, "orders"))
    assert rt.subs["orders"].subscribed and rt.subs["orders"].lost
    assert await env.health_ops() == [("lost", "orders")]
    env.item("prices", ev(K.DISCONNECTED, "prices"))
    assert sorted(await env.health_ops()) == [("lost", "account"), ("lost", "prices")]
    assert rt.socket_lost and not any(s.subscribed for s in rt.subs.values())


async def test_auth_lost_resets_subscriptions(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    await ready(env)
    env.item("account", ev(K.AUTH_LOST, "account"))
    rt = env.orch.runtime("mock")
    assert rt.auth_lost and not rt.all_subscribed
    assert len(await env.health_ops()) == 3


# ===================================================================== reconciliation
async def test_first_snapshot_rereads_every_absent_entity_and_never_closes(tmp_path: Path,
                                                                         clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.item("orders", order("O1", t=clock.ms + 100))          # newer than any watermark
    env.item("orders", position("P1", t=clock.ms + 100))
    env.orders(), env.positions()
    env.adapter.orders_by_id["O1"] = None                      # broker has no row
    env.adapter.positions_by_id["P1"] = None
    rep = await env.orch.run_snapshot("mock")
    assert rep.first and rep.reread_orders == ["O1"] and rep.reread_positions == ["P1"]
    assert env.orders() == [] and env.positions() == []        # nothing closed on absence
    rt = env.orch.runtime("mock")
    assert rt.orders["mock:O1"].status is OrderStatus.WORKING and rt.positions["mock:P1"].status == "OPEN"


async def test_later_snapshot_rereads_only_absent_at_or_below_watermark(tmp_path: Path,
                                                                       clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    await env.orch.run_snapshot("mock")                        # first pass done
    t = clock.ms
    env.item("orders", order("OLD", t=t - 5000))
    env.item("orders", order("EDGE", t=t - 1000))              # == watermark
    env.item("orders", order("DONE", OrderStatus.FILLED, t=t - 2000))
    env.item("orders", position("PC", "CLOSED", t=t - 2000))
    env.item("orders", position("PO", "OPEN", t=t - 2000))
    env.item("orders", order("LISTED", t=t - 3000))
    env.orders(), env.positions()
    # a frame that arrives after the REST request was sent is newer than the watermark
    env.adapter.on_fetch_open_orders = lambda: env.item("orders", order("NEW", t=t + 1000))
    env.adapter.open_orders = [order("LISTED", t=t - 3000)]
    fresh = order("OLD", OrderStatus.CANCELLED, t=t - 4000)
    env.adapter.orders_by_id = {"OLD": fresh, "EDGE": None}
    env.adapter.positions_by_id = {"PO": position("PO", "OPEN", t=t - 2000, units="500")}
    rep = await env.orch.run_snapshot("mock")
    assert rep.watermark_ms == t - 1000
    assert sorted(rep.reread_orders) == ["EDGE", "OLD"]
    assert rep.reread_positions == ["PO"]
    by_id = {o.broker_order_id: o for o in env.orders()}
    assert by_id["OLD"].status is OrderStatus.CANCELLED and by_id["OLD"].source == "REST"
    assert by_id["OLD"].observed_seq > rep.send_seq            # versioned at its own read
    assert "EDGE" not in by_id                                  # None: kept as is, never closed
    assert [p.units for p in env.positions()] == [Decimal(500)]


async def test_local_state_reader_seeds_first_snapshot(tmp_path: Path, clock: ManualClock) -> None:
    class Reader:
        async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
            return [order("DB1", t=1)], [position("DBP", t=1)]

    env = make(tmp_path, clock, local_state=Reader())
    env.adapter.orders_by_id["DB1"] = order("DB1", OrderStatus.FILLED, t=2, remaining="0")
    rep = await env.orch.run_snapshot("mock")
    assert rep.reread_orders == ["DB1"] and rep.reread_positions == ["DBP"]
    assert [o.status for o in env.orders()] == [OrderStatus.FILLED]


async def test_reread_entity_for_dlq_worker(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.adapter.orders_by_id["O9"] = order("O9", t=clock.ms)
    env.adapter.accounts = [account("500")]
    assert (await env.orch.reread_entity("mock", "ORDER", "O9")).found
    assert (await env.orch.reread_entity("mock", "ACCOUNT", "A1")).found
    assert not (await env.orch.reread_entity("mock", "POSITION", "missing")).found
    assert env.orders()[0].source == "REST"
    assert len(env.account_items()) == 1


# ===================================================================== relays
async def test_request_cash_flows_rate_limited_per_account(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    assert not await env.orch.request_cash_flows("A1")         # unknown account
    env.item("account", account())
    assert not await env.orch.request_cash_flows("A1")         # not STREAMING
    to_streaming(env)
    env.adapter.flows = [cash_flow("T1")]
    assert await env.orch.request_cash_flows("A1")
    assert env.adapter.called("fetch_cash_flows") == [ROLL]
    clock.ms += 59_000
    assert not await env.orch.request_cash_flows("A1")
    clock.ms += 1_000
    assert await env.orch.request_cash_flows("A1")
    assert len(env.adapter.called("fetch_cash_flows")) == 2


async def test_ensure_ticks_for_new_symbols_and_fx_crosses(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock, tick_symbols={"mock": ["EURUSD"]})
    env.adapter.instruments = [
        InstrumentSpec(adapter_id="mock", symbol=s, base_ccy=s[:3], quote_ccy=s[3:], pnl_ccy=s[3:],
                       contract_size=Decimal(1), unit_kind="UNITS", point_size=Decimal("0.0001"),
                       min_qty=Decimal(1), swap_booked_to_balance=True)
        for s in ("EURUSD", "GBPJPY", "USDJPY")]
    env.adapter.accounts = [account()]
    await env.orch.run_snapshot("mock")
    to_streaming(env)
    env.item("orders", order("O1", t=clock.ms, symbol="GBPJPY"))
    assert await env.orch.flush_pending_symbols("mock") == ["GBPJPY", "USDJPY"]
    env.item("orders", order("O2", t=clock.ms, symbol="AUDUSD"))
    assert await env.orch.flush_pending_symbols("mock") == []           # rate-limited
    clock.ms += 1_000
    assert await env.orch.flush_pending_symbols("mock") == ["AUDUSD"]
    assert env.orch.required_tick_symbols("mock") == ["AUDUSD", "EURUSD", "GBPJPY", "USDJPY"]


# ===================================================================== SM4 driver
async def wait_for(pred: Callable[[], bool], timeout: float = 3.0) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not pred():
        if loop.time() > end:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.005)


async def test_driver_reaches_streaming_and_recovers_from_disconnect(tmp_path: Path, clock: ManualClock,
                                                                     monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(connmod, "backoff_delay", lambda attempt, jitter: 0.0)
    env = make(tmp_path, clock)
    env.adapter.open_orders = [order("O1", t=clock.ms)]
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.conn.state is ConnState.STREAMING)
    names = [n for n, _ in env.adapter.calls]
    assert names.index("fetch_instruments") < names.index("subscribe")   # instrument map before streams
    assert [o.broker_order_id for o in env.orders()] == ["O1"]
    assert sorted(await env.health_ops()) == [("reconciled", "account"), ("reconciled", "orders"),
                                              ("reconciled", "prices")]

    env.adapter.push("orders", ev(K.DISCONNECTED, "orders"))
    await wait_for(lambda: len(env.adapter.called("connect")) == 2 and rt.conn.state is ConnState.STREAMING)
    states = [new for _, new, _ in rt.conn.history]
    assert ConnState.BACKOFF in states
    assert env.adapter.called("close")
    stop.set()
    await asyncio.wait_for(task, 2)
    assert rt.conn.state is ConnState.DISCONNECTED


async def test_driver_resync_resubscribes_only_lost_stream(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.conn.state is ConnState.STREAMING)
    await env.health_ops()
    subs_before = env.adapter.called("subscribe")
    env.adapter.push("account", ev(K.ERROR, "account"))
    env.adapter.end("account")
    await wait_for(lambda: ConnState.RESYNCING in [n for _, n, _ in rt.conn.history]
                   and rt.conn.state is ConnState.STREAMING and not rt.subs["account"].lost)
    assert env.adapter.called("subscribe")[len(subs_before):] == ["account"]
    ops = await env.health_ops()
    assert ("lost", "account") in ops and ops[-1] == ("reconciled", "account")
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_driver_auth_failed_after_three_credential_errors(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.adapter.connect_errors = [AuthFailedError("bad password")] * 3
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.conn.state is ConnState.AUTH_FAILED)
    await asyncio.sleep(0.05)
    assert len(env.adapter.called("connect")) == 3                 # no loop
    env.orch.rearm("mock")
    await wait_for(lambda: rt.conn.state is ConnState.STREAMING)
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_driver_refused_on_unknown_env(tmp_path: Path, clock: ManualClock,
                                             monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "live")
    env = make(tmp_path, clock)
    await asyncio.wait_for(env.orch.run_adapter("mock", asyncio.Event()), 1)
    assert env.orch.runtime("mock").conn.state is ConnState.REFUSED
    assert env.adapter.called("connect") == []


# ===================================================================== reconciled-before-commit
def subscribe_all(env: Env) -> None:
    for s in ("orders", "account", "prices"):
        env.item(s, ev(K.SUBSCRIBED, s))


async def test_gap_cleared_only_after_the_pass_rows_commit(tmp_path: Path, clock: ManualClock) -> None:
    """oversee-rules.md §2: stream_health is cleared once the reconcile *has committed*, not when its rows
    were put on the bus."""
    env = make(tmp_path, clock)
    subscribe_all(env)
    env.adapter.open_orders = [order("O1", t=clock.ms)]
    env.adapter.positions = [position("P1", t=clock.ms)]
    env.adapter.accounts = [account()]
    await env.orch.run_snapshot("mock")
    assert await env.health_ops() == []                     # published, nothing committed yet
    assert env.bus.commit_barrier.pending("mock") == 3
    env.orders()                                            # orders committed, positions / account not yet
    assert await env.health_ops() == []
    env.positions()
    env.account_items()
    assert sorted(await env.health_ops()) == [("reconciled", "account"), ("reconciled", "orders"),
                                              ("reconciled", "prices")]


async def test_queue_full_rest_row_keeps_the_gap_open(tmp_path: Path, clock: ManualClock) -> None:
    """A REST row parked QueueFull (ring accepted it, so no overflow) is a dropped frame for the pass."""
    env = make(tmp_path, clock, bus=EventBus(positions_maxsize=1))
    subscribe_all(env)
    env.adapter.positions = [position("P1", t=clock.ms), position("P2", t=clock.ms)]
    rt = env.orch.runtime("mock")
    rep = await env.orch.run_snapshot("mock")
    assert env.ring() == [("P2", "QueueFull")] and not env.orch.backpressure_overflow
    assert rep.dropped_after_watermark                      # SM4 stays in / returns to RESYNCING
    env.positions()
    ops = await env.health_ops()
    assert ("reconciled", "orders") not in ops               # P2 is not in positions_state: gap stays
    assert rt.subs["orders"].lost and rt.resync_event.is_set()


async def test_consumer_failure_keeps_the_gap_open(tmp_path: Path, clock: ManualClock) -> None:
    """A pass row the consumer parks (timeout / error) never committed: no clear, another RESYNCING."""
    env = make(tmp_path, clock)
    subscribe_all(env)
    env.adapter.open_orders = [order("O1", t=clock.ms)]
    env.adapter.accounts = [account()]
    rt = env.orch.runtime("mock")
    await env.orch.run_snapshot("mock")
    (o,) = env.orders(commit=False)
    env._consumed(o, ok=False)                              # consumer timed out and parked it
    env.account_items()
    ops = sorted(await env.health_ops())
    assert ("reconciled", "orders") not in ops and ("reconciled", "account") in ops
    assert rt.subs["orders"].lost and rt.resync_event.is_set()


async def test_newer_pass_supersedes_a_pending_one(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    subscribe_all(env)
    env.adapter.open_orders = [order("O1", t=clock.ms)]
    await env.orch.run_snapshot("mock")
    await env.orch.run_snapshot("mock")
    first, second = env.orders(commit=False)
    env._consumed(first)
    assert await env.health_ops() == []                     # the old pass's callback is gone
    env._consumed(second)
    assert sorted(await env.health_ops()) == [("reconciled", "account"), ("reconciled", "orders"),
                                              ("reconciled", "prices")]


async def test_rows_committed_while_the_pass_is_still_rereading_still_clear_the_gap(
        tmp_path: Path, clock: ManualClock) -> None:
    """The consumers run while run_snapshot awaits its by-id re-reads. A pass row committed during that
    await must still count: the pass is registered before its first row is published, not at the end
    (otherwise the early settle is ignored, the pass never completes and the fence never clears)."""
    env = make(tmp_path, clock)
    subscribe_all(env)
    env.item("orders", order("O1", t=clock.ms + 100))          # stored, absent from REST: re-read by id
    env.orders()
    env.adapter.open_orders = [order("O2", t=clock.ms)]
    env.adapter.accounts = [account()]
    env.adapter.orders_by_id["O1"] = None
    real = env.adapter.fetch_order_status

    async def reread_while_consumers_commit(broker_order_id: str) -> OrderDTO | None:
        env.orders(), env.account_items()                      # the consumers commit O2 and the account
        return await real(broker_order_id)

    env.adapter.fetch_order_status = reread_while_consumers_commit  # type: ignore[method-assign]
    rep = await env.orch.run_snapshot("mock")
    assert rep.reread_orders == ["O1"]
    assert env.bus.commit_barrier.pending("mock") == 0
    assert sorted(await env.health_ops()) == [("reconciled", "account"), ("reconciled", "orders"),
                                              ("reconciled", "prices")]


async def test_failed_pass_does_not_cancel_the_pending_one(tmp_path: Path, clock: ManualClock) -> None:
    """A pass that raises (broker error) never seals; the earlier pass still clears once its rows commit."""
    env = make(tmp_path, clock)
    subscribe_all(env)
    env.adapter.open_orders = [order("O1", t=clock.ms)]
    await env.orch.run_snapshot("mock")

    async def boom() -> list[OrderDTO]:
        raise RuntimeError("broker 503")

    env.adapter.fetch_open_orders = boom  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        await env.orch.run_snapshot("mock")
    env.orders(), env.positions(), env.account_items()
    assert sorted(await env.health_ops()) == [("reconciled", "account"), ("reconciled", "orders"),
                                              ("reconciled", "prices")]


# ===================================================================== 4.9: any parked reconcile row keeps the gap
@pytest.mark.parametrize("reason", ["illegal_transition", "missing_client_order_id", "future_timestamp"])
async def test_rest_row_parked_for_any_reason_keeps_the_gap_open(tmp_path: Path, clock: ManualClock,
                                                                 reason: str) -> None:
    """4.9 decision (fail closed): a REST row of a reconcile pass parked for ANY reason never reached the
    store, so it is a dropped frame for the pass: the orders gap stays open and RESYNCING retries."""
    from dataclasses import replace as dc_replace
    env = make(tmp_path, clock, require_client_order_id=(reason == "missing_client_order_id"))
    rt = env.orch.runtime("mock")
    subscribe_all(env)
    if reason == "illegal_transition":
        env.item("orders", order("O1", OrderStatus.FILLED, t=clock.ms - 1_000))
        env.orders()
        env.adapter.open_orders = [order("O1", OrderStatus.WORKING, t=clock.ms)]   # FILLED -> WORKING
    elif reason == "missing_client_order_id":
        env.adapter.open_orders = [dc_replace(order("O1", t=clock.ms), client_order_id="")]
    else:
        env.adapter.open_orders = [order("O1", t=clock.ms + 60_000)]                # > 5 s ahead
    env.adapter.accounts = [account()]
    rt.resync_event.clear()
    rep = await env.orch.run_snapshot("mock")
    assert env.ring() == [("O1", reason)] and rep.parked == 1
    assert rep.dropped_after_watermark                      # SM4 stays in / returns to RESYNCING
    env.orders(), env.positions(), env.account_items()
    ops = await env.health_ops()
    assert ("reconciled", "orders") not in ops and ("reconciled", "account") in ops
    assert rt.subs["orders"].lost and rt.resync_event.is_set()


async def test_reconcile_without_parked_rows_still_clears_every_gap(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    subscribe_all(env)
    env.adapter.open_orders = [order("O1", t=clock.ms)]
    rep = await env.orch.run_snapshot("mock")
    assert rep.parked == 0 and not rep.dropped_after_watermark
    env.orders(), env.positions(), env.account_items()
    assert sorted(await env.health_ops()) == [("reconciled", "account"), ("reconciled", "orders"),
                                              ("reconciled", "prices")]


# ===================================================================== ARM prerequisites (plan S04: PR1, PR8, PR9)
def _kind(o: OrderDTO, kind: str | None) -> OrderDTO:
    return dc_replace(o, order_kind=kind)


async def test_stream_then_rest_keeps_the_stop_kind(tmp_path: Path, clock: ManualClock) -> None:
    """One StoneX child: the stream says STOP, the REST resync row states no type -> still STOP, same content."""
    env = make(tmp_path, clock)
    env.item("orders", _kind(order("C1", t=clock.ms - 100, stop="1.0"), "STOP"))
    [s] = env.orders()
    env.adapter.open_orders = [order("C1", t=clock.ms - 100, stop="1.0")]
    await env.orch.run_snapshot("mock")
    [r] = env.orders()
    assert s.order_kind == r.order_kind == "STOP" and r.checksum == s.checksum      # a no-op advance, no flip
    assert (s.origin, r.origin) == ("stream", "snapshot")


async def test_rest_then_stream_keeps_the_stop_kind(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.adapter.open_orders = [_kind(order("C1", t=clock.ms - 100), "STOP")]
    await env.orch.run_snapshot("mock")
    env.orders()
    env.item("orders", order("C1", OrderStatus.FILLED, t=clock.ms, remaining="0"))  # a fill row without a type
    [f] = env.orders()
    assert f.order_kind == "STOP" and f.status is OrderStatus.FILLED
    assert f.checksum == _kind(order("C1", OrderStatus.FILLED, t=clock.ms, remaining="0"), "STOP").checksum


async def test_two_known_kinds_page_once_and_count_as_unknown(tmp_path: Path, clock: ManualClock) -> None:
    pages: list[tuple[str, str, str]] = []
    env = make(tmp_path, clock, pager=lambda s, t, b: pages.append((s, t, b)))
    env.item("orders", _kind(order("C1", t=clock.ms - 300), "STOP"))
    env.item("orders", _kind(order("C1", t=clock.ms - 200), "LIMIT"))
    env.item("orders", _kind(order("C1", t=clock.ms - 100), "STOP"))
    env.item("orders", order("C1", t=clock.ms))
    kinds = [o.order_kind for o in env.orders()]
    assert kinds == ["STOP", None, None, None]
    conflict = [p for p in pages if "order_kind_conflict" in p[1]]
    assert len(conflict) == 1 and "C1" in conflict[0][2]
    assert env.orch.runtime("mock").order_kind == {"mock:C1": None}


async def test_origin_labels_every_path(tmp_path: Path, clock: ManualClock) -> None:
    class Stored:
        async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
            return [order("ABSENT", t=1)], []

    env = make(tmp_path, clock, local_state=Stored())
    env.item("orders", order("S1", t=clock.ms - 10))
    assert [o.origin for o in env.orders()] == ["stream"]
    env.adapter.open_orders = [order("O1", t=clock.ms)]
    env.adapter.positions = [position("P1", t=clock.ms)]
    env.adapter.accounts = [account()]
    env.adapter.orders_by_id["ABSENT"] = order("ABSENT", t=2)                     # the first pass re-reads by id
    rep = await env.orch.run_snapshot("mock")
    assert "ABSENT" in rep.reread_orders                                          # (S1 too: none by id)
    assert {o.broker_order_id: o.origin for o in env.orders()} == {"O1": "snapshot", "ABSENT": "snapshot"}
    assert [p.origin for p in env.positions()] == ["snapshot"]
    assert [a.origin for a in env.account_items() if isinstance(a, AccountDTO)] == ["snapshot"]
    assert await env.orch.poll_account("mock")
    assert [a.origin for a in env.account_items() if isinstance(a, AccountDTO)] == ["snapshot"]
    env.adapter.orders_by_id["D1"] = order("D1", t=clock.ms)
    dlq = await env.orch.reread_entity("mock", "ORDER", "D1")
    assert dlq.dto is not None and dlq.dto.origin == "dlq"                         # the default origin
    env.adapter.accounts = [account("10001")]
    res = await env.orch.reread_entity("mock", "ACCOUNT", "A1", origin="snapshot")
    assert res.dto is not None and res.dto.origin == "snapshot"


async def test_catchup_history_rows_are_history_and_rechecks_snapshot(tmp_path: Path, clock: ManualClock) -> None:
    mock = MockBrokerAdapter(orders=[order("R1", t=clock.ms - 50)])
    mock.list_in_history(order("H1", OrderStatus.CANCELLED, t=clock.ms - 1_000))
    env = make(tmp_path, clock, adapter=mock)  # type: ignore[arg-type]
    fetch = await env.orch.catchup_fetch("mock", clock.ms - 10_000, clock.ms)
    res = await env.orch.catchup_submit(fetch, recheck_orders=["R1"])
    assert res.orders_published == 1 and [o.broker_order_id for o in res.rechecked_orders] == ["R1"]
    assert {o.broker_order_id: o.origin for o in env.orders()} == {"H1": "history", "R1": "snapshot"}


async def test_broker_time_source_and_event_log_broker_time(tmp_path: Path, clock: ManualClock) -> None:
    """IN-3 / R11-M06: only the broker's own record time is BROKER and reaches event_log.broker_time_ms; an untimed
    REST row labelled with the stream watermark, and the local-time domain (accounts here), are WATERMARK / NULL."""
    from src.db.repository import version_labels

    env = make(tmp_path, clock)
    env.item("orders", order("S1", t=clock.ms - 500))
    [s] = env.orders()
    assert s.broker_time_source == "BROKER" and version_labels(s) == ("stream", clock.ms - 500)
    env.adapter.open_orders = [order("U1", t=0), order("T1", t=clock.ms - 400)]
    env.adapter.accounts = [account(t=clock.ms - 300)]
    await env.orch.run_snapshot("mock")
    got = {o.broker_order_id: o for o in env.orders()}
    untimed, timed = got["U1"], got["T1"]
    assert untimed.broker_updated_at_ms == clock.ms - 500                       # the stream watermark
    assert untimed.broker_time_source == "WATERMARK" and version_labels(untimed) == ("snapshot", None)
    assert timed.broker_time_source == "BROKER" and version_labels(timed) == ("snapshot", clock.ms - 400)
    [a] = [x for x in env.account_items() if isinstance(x, AccountDTO)]
    assert a.broker_time_source == "WATERMARK" and version_labels(a) == ("snapshot", None)
    assert untimed.checksum == order("U1", t=0).checksum                         # labels never touch the checksum


async def test_reread_entity_statuses(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    env.adapter.orders_by_id["O1"] = order("O1", t=clock.ms - 100)
    first = await env.orch.reread_entity("mock", "ORDER", "O1")
    assert first.status is RereadStatus.APPLIED and first.found and first.dto is not None
    assert first.version == version_str(first.dto) and first.dto.source == "REST"
    assert (await env.orch.reread_entity("mock", "ORDER", "O1")).status is RereadStatus.NOOP
    env.adapter.orders_by_id["O1"] = order("O1", t=clock.ms - 200, stop="1.0")       # an older broker version
    assert (await env.orch.reread_entity("mock", "ORDER", "O1")).status is RereadStatus.DROPPED_STALE
    env.adapter.orders_by_id["O1"] = order("O1", t=clock.ms - 50, stop="1.0")
    assert (await env.orch.reread_entity("mock", "ORDER", "O1")).status is RereadStatus.APPLIED
    missing = await env.orch.reread_entity("mock", "POSITION", "nope")
    assert missing.status is RereadStatus.NOT_FOUND and not missing.found and missing.dto is None
    bad = order("O2", t=clock.ms)
    object.__setattr__(bad, "checksum", "0" * 64)                                    # parked: never handed over
    env.adapter.orders_by_id["O2"] = bad
    transient = await env.orch.reread_entity("mock", "ORDER", "O2")
    assert transient.status is RereadStatus.TRANSIENT and transient.found and transient.dto is bad
    assert transient.version is None and env.ring() == [("O2", "checksum")]
    assert len(env.orders()) == 4


async def test_reread_entity_reports_the_send_time_of_its_own_read(tmp_path: Path, clock: ManualClock) -> None:
    mock = MockBrokerAdapter(orders=[order("O1", t=clock.ms - 10)], wall_ms=lambda: clock.ms + 7)
    env = make(tmp_path, clock, adapter=mock)  # type: ignore[arg-type]
    mark_rest_sent(1)                                   # an older stamp in this context is never reported
    res = await env.orch.reread_entity("mock", "ORDER", "O1")
    assert res.sent_ms == clock.ms + 7


def _stop_mock(clock: ManualClock) -> MockBrokerAdapter:
    return MockBrokerAdapter(orders=[_kind(order("S1", t=clock.ms - 100, stop="1.0"), "STOP"),
                                     _kind(order("S2", t=clock.ms - 100, stop="1.0"), "STOP")],
                             positions=[position("P1", t=clock.ms - 100)], wall_ms=lambda: clock.ms + 3)


async def test_reread_stop_orders_one_list_read_never_closes_an_absent_order(tmp_path: Path,
                                                                             clock: ManualClock) -> None:
    mock = _stop_mock(clock)
    env = make(tmp_path, clock, adapter=mock)  # type: ignore[arg-type]
    await env.orch.run_snapshot("mock")
    env.orders(), env.positions(), env.account_items()
    rt = env.orch.runtime("mock")
    mock.set_order(_kind(order("S1", t=clock.ms - 10, stop="1.05"), "STOP"))                    # stop moved
    mock.set_order(_kind(order("S2", OrderStatus.CANCELLED, t=clock.ms - 10), "STOP"))          # not listed
    mock.calls.clear()
    mock.rest_calls.clear()
    res = await env.orch.reread_stop_orders("mock")
    assert mock.rest_calls == [("fetch_active_stop_orders", RestPriority.BARS)] and mock.stop_order_reads == 1
    assert "fetch_order_status" not in mock.calls                              # nothing re-read by id
    assert res.supported and res.sent_ms == clock.ms + 3 and res.skipped == 0
    [row] = res.rows
    assert row.status is RereadStatus.APPLIED and row.sent_ms == clock.ms + 3
    assert row.dto is not None and row.dto.origin == "stop_read" and row.dto.stop_loss == Decimal("1.05")
    [published] = env.orders()
    assert published.broker_order_id == "S1" and published.order_kind == "STOP"
    assert rt.orders["mock:S2"].status is OrderStatus.WORKING                   # absent: never closed
    assert env.positions() == []
    again = await env.orch.reread_stop_orders("mock")
    assert [r.status for r in again.rows] == [RereadStatus.NOOP]


async def test_reread_stop_orders_position_handles_and_unsupported(tmp_path: Path, clock: ManualClock) -> None:
    mock = _stop_mock(clock)
    env = make(tmp_path, clock, adapter=mock)  # type: ignore[arg-type]
    mock.set_stop_orders_supported(True, positions=True)
    res = await env.orch.reread_stop_orders("mock")
    assert [(r.status, r.dto.origin if r.dto else None) for r in res.rows] == [(RereadStatus.APPLIED, "stop_read")]
    assert [p.position_id for p in env.positions()] == ["P1"] and env.orders() == []
    mock.set_stop_orders_supported(False)
    res = await env.orch.reread_stop_orders("mock")
    assert not res.supported and res.rows == ()
    assert env.orders() == [] and env.positions() == []


async def test_reread_stop_orders_ignores_another_adapters_rows(tmp_path: Path, clock: ManualClock) -> None:
    mock = MockBrokerAdapter(orders=[_kind(order("X1", t=clock.ms - 100, adapter_id="other"), "STOP")])
    env = make(tmp_path, clock, adapter=mock)  # type: ignore[arg-type]
    res = await env.orch.reread_stop_orders("mock")
    assert res.skipped == 1 and res.rows == () and env.orders() == []
