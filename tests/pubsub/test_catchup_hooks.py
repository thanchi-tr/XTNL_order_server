"""Downtime catch-up hooks of the IngestionOrchestrator (src/runtime/catchup.py): the stream_health fence hold, the
forced RESYNCING after sleep, and the catch-up submit through the normal REST path. No network."""
from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from src.adapters.base import (
    BrokerError,
    BrokerHistory,
    CashFlowBatch,
    VersionDecl,
    new_unmapped_rows,
    note_unmapped,
)
from src.dto import CashFlowDTO, OrderDTO, OrderStatus, PositionDTO
from src.pubsub.connection import ConnState
from tests.pubsub.conftest import FakeAdapter, ManualClock, cash_flow, ev, order, position
from tests.pubsub.test_orchestrator import Env, K, make, ready, to_streaming


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


class HistoryAdapter(FakeAdapter):
    """FakeAdapter with the shared-contract `fetch_history` (scripted)."""

    def __init__(self, **kw: object) -> None:
        super().__init__(**kw)  # type: ignore[arg-type]
        self.history: BrokerHistory | None = None
        self.order_errors: dict[str, BaseException] = {}
        self.flow_batch: CashFlowBatch | None = None
        self.flows_only_since: int | None = None     # flows are returned only to a read from exactly this time

    async def fetch_cash_flows(self, since_ms: int) -> list[CashFlowDTO]:
        if self.flow_batch is not None:
            self.calls.append(("fetch_cash_flows", since_ms))
            return self.flow_batch
        if self.flows_only_since is not None and since_ms != self.flows_only_since:
            self.calls.append(("fetch_cash_flows", since_ms))
            return []
        return await super().fetch_cash_flows(since_ms)

    async def fetch_history(self, since_ms: int, until_ms: int) -> BrokerHistory:
        self.calls.append(("fetch_history", (since_ms, until_ms)))
        if self.history is None:
            return BrokerHistory.unsupported(self.adapter_id, since_ms, until_ms)
        return self.history

    async def fetch_order_status(self, broker_order_id: str) -> OrderDTO | None:
        if broker_order_id in self.order_errors:
            raise self.order_errors[broker_order_id]
        return await super().fetch_order_status(broker_order_id)


def history(orders: Sequence[OrderDTO] = (), positions: Sequence[PositionDTO] = (), *, since: int = 0,
            until: int = 10, complete: bool = True) -> BrokerHistory:
    return BrokerHistory(adapter_id="mock", since_ms=since, until_ms=until, covered_from_ms=since,
                         orders=tuple(orders), positions=tuple(positions), complete=complete, notes=())


def subscribe_all(env: Env) -> None:
    for s in ("orders", "account", "prices"):
        env.item(s, ev(K.SUBSCRIBED, s))


@dataclass
class Committer:
    """Plays the consumers: drains the topics and settles every row on the barrier (committed)."""

    env: Env
    seen: list[object] = field(default_factory=list)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self.seen += self.env.orders() + self.env.positions() + self.env.account_items()
            await asyncio.sleep(0.001)


async def submit_with_consumers(env: Env, coro: object) -> object:
    stop = asyncio.Event()
    c = Committer(env)
    task = asyncio.create_task(c.run(stop))
    try:
        return await coro  # type: ignore[misc]
    finally:
        stop.set()
        await task


# ===================================================================== fence hold
async def test_held_fence_survives_a_committed_snapshot_and_release_clears_it(tmp_path: Path,
                                                                              clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    rt = env.orch.runtime("mock")
    env.orch.hold_fence("mock", "catchup")
    subscribe_all(env)
    await env.orch.run_snapshot("mock")
    env.orders(), env.positions(), env.account_items()        # committed: the pass's barrier fires
    assert rt.passes_committed == 1 and rt.last_committed_send_seq > 0
    assert all(s.lost for s in rt.subs.values())               # the fence stays open while held
    assert ("reconciled", "orders") not in await env.health_ops()
    assert env.orch.release_fence("mock", "catchup") is True
    assert not any(s.lost for s in rt.subs.values())
    assert sorted(op for op in await env.health_ops() if op[0] == "reconciled") == [
        ("reconciled", "account"), ("reconciled", "orders"), ("reconciled", "prices")]


async def test_release_keeps_a_stream_that_dropped_after_the_last_pass_and_resyncs(tmp_path: Path,
                                                                                   clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    rt = env.orch.runtime("mock")
    env.orch.hold_fence("mock", "catchup")
    subscribe_all(env)
    await env.orch.run_snapshot("mock")
    env.orders(), env.positions(), env.account_items()
    rt.resync_event.clear()
    env.item("orders", ev(K.GAP, "orders"))                    # a frame lost after the pass was sent
    rt.resync_event.clear()
    env.orch.release_fence("mock", "catchup")
    assert rt.subs["orders"].lost and not rt.subs["account"].lost and not rt.subs["prices"].lost
    assert rt.resync_event.is_set()                            # the next pass clears it


async def test_two_holds_release_only_with_the_last(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    rt = env.orch.runtime("mock")
    env.orch.hold_fence("mock", "a")
    env.orch.hold_fence("mock", "b")
    subscribe_all(env)
    await env.orch.run_snapshot("mock")
    env.orders(), env.positions(), env.account_items()
    assert env.orch.release_fence("mock", "a") is False and rt.subs["orders"].lost
    assert env.orch.release_fence("mock", "b") is True and not rt.subs["orders"].lost


async def test_hold_reopens_a_reconciled_fence(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    await ready(env)
    env.orch.hold_fence("mock", "catchup")
    assert env.orch.fence_held("mock")
    assert sorted(await env.health_ops()) == [("lost", "account"), ("lost", "orders"), ("lost", "prices")]


async def test_no_conn_key_page_while_the_catchup_holds_the_fence(tmp_path: Path, clock: ManualClock) -> None:
    pages: list[tuple[str, str, str]] = []
    env = make(tmp_path, clock, pager=lambda s, t, b: pages.append((s, t, b)))
    await ready(env)
    to_streaming(env)
    env.orch.hold_fence("mock", "catchup")
    clock.ms += 300_000
    env.orch.check_liveness("mock")
    assert not [p for p in pages if p[1].startswith("conn key absent")]
    env.orch.release_fence("mock", "catchup")                # the key is now reconciled again: no page either
    env.orch.check_liveness("mock")
    assert not [p for p in pages if p[1].startswith("conn key absent")]


async def test_force_resync_loses_every_subscription_and_asks_for_resyncing(tmp_path: Path,
                                                                            clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    await ready(env)
    to_streaming(env)
    rt = env.orch.runtime("mock")
    env.orch.force_resync("mock", "process was suspended (sleep)")
    assert all(not s.subscribed and s.lost for s in rt.subs.values())
    assert rt.resync_event.is_set() and env.resyncs == ["mock"]
    assert rt.conn.on_gap(True) is ConnState.RESYNCING       # SM4 re-subscribes every stream in RESYNCING


# ===================================================================== fetch + submit
async def test_catchup_fetch_reads_history_and_cash_flows_of_the_window(tmp_path: Path, clock: ManualClock) -> None:
    ad = HistoryAdapter()
    env = make(tmp_path, clock, adapter=ad)
    ad.flow_batch = CashFlowBatch([cash_flow("T1")], partial_reasons=("capped page",))
    f = await env.orch.catchup_fetch("mock", 100, 200)
    assert ad.called("fetch_history") == [(100, 200)] and ad.called("fetch_cash_flows") == [100]
    assert isinstance(f.history, BrokerHistory) and f.history.complete is False   # default: unsupported
    assert f.cash_flows_partial == ("capped page",) and not env.orch.pnl_source("mock")
    assert f.send_ms == clock.ms and f.send_seq > 0


async def test_submit_publishes_history_rows_through_the_rest_path_and_waits_for_commit(
        tmp_path: Path, clock: ManualClock) -> None:
    ad = HistoryAdapter()
    env = make(tmp_path, clock, adapter=ad)
    await ready(env)
    rt = env.orch.runtime("mock")
    rt.pending_symbols.clear()
    ad.history = history([order("H1", OrderStatus.FILLED, t=50, symbol="GBPUSD")],
                         [position("HP1", "CLOSED", t=60)])
    ad.flows = [cash_flow("T9")]
    f = await env.orch.catchup_fetch("mock", 0, clock.ms)
    res = await submit_with_consumers(env, env.orch.catchup_submit(f, timeout_s=2.0))
    assert res.committed and not res.timed_out  # type: ignore[attr-defined]
    assert (res.orders_published, res.positions_published, res.cash_flows_published) == (1, 1, 1)  # type: ignore[attr-defined]
    assert rt.orders["mock:H1"].source == "REST" and rt.orders["mock:H1"].observed_seq == f.send_seq
    assert "GBPUSD" not in rt.pending_symbols                   # a finished order subscribes no price
    assert not env.orch.parked


async def test_submit_rechecks_stored_working_rows_by_id_and_reports_not_found(tmp_path: Path,
                                                                               clock: ManualClock) -> None:
    ad = HistoryAdapter()
    env = make(tmp_path, clock, adapter=ad)
    await ready(env)
    ad.history = history()
    ad.orders_by_id = {"GONE": None, "LIVE": order("LIVE", OrderStatus.CANCELLED, t=70)}
    ad.order_errors = {"ERR": BrokerError("boom")}
    ad.positions_by_id = {"P-GONE": None}
    f = await env.orch.catchup_fetch("mock", 0, clock.ms)
    res = await submit_with_consumers(env, env.orch.catchup_submit(
        f, recheck_orders=["GONE", "LIVE", "ERR"], recheck_positions=["P-GONE"], timeout_s=2.0))
    assert res.not_found == ["order GONE", "position P-GONE"]  # type: ignore[attr-defined]
    assert res.reread_errors == ["order ERR: BrokerError"]  # type: ignore[attr-defined]
    assert [o.broker_order_id for o in res.rechecked_orders] == ["LIVE"]  # type: ignore[attr-defined]
    assert res.committed  # type: ignore[attr-defined]


async def test_a_history_row_without_a_revision_is_skipped_without_opening_a_gap(tmp_path: Path,
                                                                                 clock: ManualClock) -> None:
    ad = HistoryAdapter(versioning={"ORDER": VersionDecl(True, True), "POSITION": VersionDecl(False, True),
                                    "ACCOUNT": VersionDecl(False, False)})
    env = make(tmp_path, clock, adapter=ad)
    await ready(env)
    rt = env.orch.runtime("mock")
    ad.history = history([order("NOSEQ", OrderStatus.FILLED, t=5, bseq=None), order("SEQ", OrderStatus.FILLED,
                                                                                    t=5, bseq=3)])
    f = await env.orch.catchup_fetch("mock", 0, clock.ms)
    res = await submit_with_consumers(env, env.orch.catchup_submit(f, timeout_s=2.0))
    assert res.unversioned == ["mock:NOSEQ"] and res.orders_published == 1  # type: ignore[attr-defined]
    assert not rt.subs["orders"].lost and not env.orch.frames_lost      # the streams are fine


async def test_submit_reports_uncommitted_rows_on_timeout(tmp_path: Path, clock: ManualClock) -> None:
    ad = HistoryAdapter()
    env = make(tmp_path, clock, adapter=ad)
    await ready(env)
    ad.history = history([order("H2", OrderStatus.FILLED, t=50)])
    f = await env.orch.catchup_fetch("mock", 0, clock.ms)
    res = await env.orch.catchup_submit(f, timeout_s=0.05)     # nobody consumes: never committed
    assert res.timed_out and not res.committed


async def test_a_long_history_is_paced_by_the_topic_fill_and_never_parks(tmp_path: Path, clock: ManualClock) -> None:
    from src.pubsub.bus import EventBus
    ad = HistoryAdapter()
    env = make(tmp_path, clock, adapter=ad, bus=EventBus(orders_maxsize=4))
    await ready(env)
    ad.history = history([order(f"H{i}", OrderStatus.FILLED, t=50 + i) for i in range(25)])
    f = await env.orch.catchup_fetch("mock", 0, clock.ms)
    res = await submit_with_consumers(env, env.orch.catchup_submit(f, timeout_s=2.0))
    assert res.orders_published == 25 and res.parked == 0 and res.committed  # type: ignore[attr-defined]
    assert not env.orch.parked


# ===================================================================== skeptic review: versioning, interruptions
async def test_a_history_row_without_a_broker_time_is_skipped_never_labelled_with_the_watermark(
        tmp_path: Path, clock: ManualClock) -> None:
    """An untimed snapshot row describes the present and takes the stream watermark; an untimed *history* row
    does not: labelled with the watermark (and a later observed_seq) it would outrank the newer stream row of
    the same entity and roll its stop back."""
    ad = HistoryAdapter()
    env = make(tmp_path, clock, adapter=ad)
    await ready(env)
    rt = env.orch.runtime("mock")
    env.item("orders", order("AMD", OrderStatus.WORKING, t=200, stop="1.05"))      # the stream: stop moved at 200
    env.orders()
    ad.history = history([order("AMD", OrderStatus.WORKING, t=0, stop="1.00"),    # untimed, older content
                          order("TIMED", OrderStatus.FILLED, t=150)])
    f = await env.orch.catchup_fetch("mock", 0, clock.ms)
    res = await submit_with_consumers(env, env.orch.catchup_submit(f, timeout_s=2.0))
    assert res.unversioned == ["mock:AMD"] and res.orders_published == 1  # type: ignore[attr-defined]
    assert rt.orders["mock:AMD"].stop_loss == order("AMD", stop="1.05").stop_loss    # the stream row stands
    assert rt.orders["mock:AMD"].broker_updated_at_ms == 200
    assert not rt.subs["orders"].lost                                               # no gap: the streams are fine


async def test_a_by_id_reread_that_hangs_is_reported_not_awaited_forever(tmp_path: Path, clock: ManualClock) -> None:
    ad = HistoryAdapter()
    env = make(tmp_path, clock, adapter=ad)
    await ready(env)
    hang = asyncio.Event()

    async def stuck(broker_order_id: str) -> OrderDTO | None:
        await hang.wait()
        return None

    ad.fetch_order_status = stuck  # type: ignore[method-assign]
    ad.history = history()
    f = await env.orch.catchup_fetch("mock", 0, clock.ms)
    res = await submit_with_consumers(env, env.orch.catchup_submit(f, recheck_orders=["SLOW"], timeout_s=2.0,
                                                                   reread_timeout_s=0.05))
    assert res.reread_errors == ["order SLOW: TimeoutError"]  # type: ignore[attr-defined]


async def test_interruptions_of_the_order_or_account_path_are_counted_but_a_fence_hold_is_not(
        tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    await ready(env)
    rt = env.orch.runtime("mock")
    assert rt.last_interruption_seq == 0
    env.orch.hold_fence("mock", "catchup")                     # held on purpose: not an interruption
    env.orch.release_fence("mock", "catchup")
    assert rt.last_interruption_seq == 0
    env.item("prices", ev(K.GAP, "prices"))                    # a price gap loses no order
    assert rt.last_interruption_seq == 0
    env.item("orders", ev(K.GAP, "orders"))
    first = rt.last_interruption_seq
    assert first > 0
    env.item("account", ev(K.DISCONNECTED, "account"))         # the socket
    assert rt.last_interruption_seq > first


async def test_rows_skipped_before_a_catchup_read_still_count_as_an_interruption(tmp_path: Path,
                                                                                 clock: ManualClock) -> None:
    """A live row skipped before the read is a real gap; so is one a stream task skipped while the catch-up ran:
    `catchup_submit` registers it and never reverts `last_interruption_seq` (skeptic review: a live interruption
    during a catch-up must survive and stop coverage)."""
    ad = HistoryAdapter()
    env = make(tmp_path, clock, adapter=ad)
    await ready(env)
    rt = env.orch.runtime("mock")
    ad.unmapped_rows = {"orders": 1}  # type: ignore[attr-defined]       # a stream row skipped earlier
    ad.history = history()
    f = await env.orch.catchup_fetch("mock", 0, clock.ms)
    counted = rt.last_interruption_seq
    assert counted > 0 and rt.subs["orders"].lost
    ad.unmapped_rows = {"orders": 2}  # type: ignore[attr-defined]       # a live row skipped by a stream task meanwhile
    res = await submit_with_consumers(env, env.orch.catchup_submit(f, timeout_s=2.0))
    assert res.unmapped == 0  # type: ignore[attr-defined]               # not the catch-up's own row
    assert rt.last_interruption_seq > counted                            # the live interruption survives


def _unmappable_history(ad: HistoryAdapter, rows: int) -> None:
    real = ad.fetch_history

    async def fetch_history(since_ms: int, until_ms: int) -> BrokerHistory:
        for _ in range(rows):
            note_unmapped(ad.unmapped_rows, "mock", "orders", "history row without an id")  # type: ignore[attr-defined]
        await asyncio.sleep(0)                                           # the SM4 driver may run in between
        return await real(since_ms, until_ms)

    ad.fetch_history = fetch_history  # type: ignore[method-assign]


async def test_unmappable_history_rows_count_on_the_read_never_as_a_stream_interruption(
        tmp_path: Path, clock: ManualClock) -> None:
    """Skeptic review: history rows the adapter cannot map used to land on the live `unmapped_rows` counter. The
    1 s SM4 driver step then took them as an interruption before `catchup_submit` could excuse them, queueing a
    `reconnect` catch-up that hit the same row, forever. They now count on the read's own channel."""
    ad = HistoryAdapter()
    ad.unmapped_rows = new_unmapped_rows()  # type: ignore[attr-defined]
    env = make(tmp_path, clock, adapter=ad)
    await ready(env)
    rt = env.orch.runtime("mock")
    before = rt.last_interruption_seq
    _unmappable_history(ad, 2)
    ad.history = history()
    f = await env.orch.catchup_fetch("mock", 0, clock.ms)
    assert f.history_unmapped == 2 and ad.unmapped_rows["orders"] == 0  # type: ignore[attr-defined]
    assert env.orch.check_unmapped("mock") == set()                     # the driver's step finds nothing
    res = await submit_with_consumers(env, env.orch.catchup_submit(f, timeout_s=2.0))
    assert res.unmapped == 0  # type: ignore[attr-defined]
    assert rt.last_interruption_seq == before and not rt.subs["orders"].lost


async def test_an_unmappable_by_id_reread_counts_on_the_reread_never_on_the_stream(
        tmp_path: Path, clock: ManualClock) -> None:
    ad = HistoryAdapter()
    ad.unmapped_rows = new_unmapped_rows()  # type: ignore[attr-defined]
    env = make(tmp_path, clock, adapter=ad)
    await ready(env)
    rt = env.orch.runtime("mock")
    before = rt.last_interruption_seq
    real = ad.fetch_order_status

    async def unmappable(broker_order_id: str) -> OrderDTO | None:
        if broker_order_id == "BAD":
            note_unmapped(ad.unmapped_rows, "mock", "orders", "by-id answer unmappable")  # type: ignore[attr-defined]
            raise BrokerError("order BAD unmappable")
        return await real(broker_order_id)

    ad.fetch_order_status = unmappable  # type: ignore[method-assign]
    ad.history = history()
    f = await env.orch.catchup_fetch("mock", 0, clock.ms)
    res = await submit_with_consumers(env, env.orch.catchup_submit(f, recheck_orders=["BAD"], timeout_s=2.0))
    assert res.unmapped == 1 and res.reread_errors == ["order BAD: BrokerError"]  # type: ignore[attr-defined]
    assert ad.unmapped_rows["orders"] == 0 and rt.last_interruption_seq == before  # type: ignore[attr-defined]
