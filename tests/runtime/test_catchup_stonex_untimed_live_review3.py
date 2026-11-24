"""Live review round 3 (2026-11-25): the real StoneX adapter's REST rows, run through the real IngestionOrchestrator,
after round 1's history filter and its widening of the accepted deviation were reverted.

- ``adapter-isolation.md`` §4: fetch_history offers every working row, untimed or not. ``operations.md`` §17 step 3:
  an untimed history row in a broker-time domain is skipped and reported, never labelled with the watermark, so a
  catch-up over an open position's untimed children (and a working entry's own-row children, which keep the plain
  untimed WORKING mapping) is **incomplete** (``oversee-rules.md`` §8 ``catchup_unverified``); the HIGH page names
  the skipped ids and the uncovered window.
- The 2026-11-21 accepted deviation (IfDone shape only) still holds end to end: a snapshot after the stream stated
  the IfDone children PENDING agrees with it (PENDING at BROKER time), and a later stream re-send is no illegal
  transition."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from typing import Any, TypeVar

import pytest

from src.adapters.base import BrokerHistory
from src.adapters.stonex.adapter import StoneXAdapter
from src.db.uptime_store import Coverage
from src.dto import OrderDTO, OrderStatus, PositionDTO
from src.runtime.catchup import CatchupCoordinator, CatchupMetrics
from tests.adapters.conftest import EURUSD_ID, TRADING, FakeStoneX, make_adapter, wcf
from tests.adapters.test_stonex_history import trade
from tests.pubsub.conftest import ManualClock, ev
from tests.pubsub.test_catchup_hooks import HistoryAdapter
from tests.pubsub.test_orchestrator import K, make, wait_for
from tests.runtime.test_catchup import FAST, Db, Rig, start

T0 = 1_767_225_600_000
NOW = T0 + 3_600_000
_D = TypeVar("_D", OrderDTO, PositionDTO)
IF_DONE = [{"Stop": {"OrderId": 971, "TriggerPrice": 1.083}}, {"Limit": {"OrderId": 972, "TriggerPrice": 1.075}}]


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(NOW)


def as_mock(dto: _D) -> _D:
    return replace(dto, adapter_id="mock", account_id="A1")


def entry_row(oid: int, ts: int, **extra: Any) -> dict[str, Any]:
    return {"OrderId": oid, "MarketId": int(EURUSD_ID), "Direction": "Sell", "Quantity": 10000, "TriggerPrice": 1.08,
            "Type": 2, "ParentOrderId": None, "TradingAccountId": int(TRADING), "LastChangedDateTimeUTC": wcf(ts),
            **extra}


def child_row(oid: int, type_: int, parent: int) -> dict[str, Any]:
    return {"OrderId": oid, "MarketId": int(EURUSD_ID), "Direction": "Buy", "Quantity": 10000,
            "TriggerPrice": 1.083 if type_ == 2 else 1.075, "Type": type_, "ParentOrderId": parent,
            "TradingAccountId": int(TRADING)}


def live_book() -> FakeStoneX:
    fake = FakeStoneX()
    fake.history = [trade(900, T0 + 60_000)]                     # opened in the window, still open
    fake.positions = [{"OrderId": 900, "MarketId": int(EURUSD_ID), "Direction": "Buy", "Quantity": 10000,
                       "Price": 1.087, "TradingAccountId": int(TRADING), "LastChangedDateTimeUTC": wcf(T0 + 60_000),
                       "StopOrder": {"OrderId": 901, "TriggerPrice": 1.08},          # untimed, WORKING
                       "LimitOrder": {"OrderId": 902, "TriggerPrice": 1.09}}]
    fake.actives = [entry_row(970, T0 + 120_000), child_row(971, 2, 970), child_row(972, 3, 970)]
    return fake


async def connected(fake: FakeStoneX) -> StoneXAdapter:
    adapter = make_adapter(fake, stop_limit_history=True)
    await adapter.connect()
    return adapter


class StoneXBacked(HistoryAdapter):
    """The mock runtime, answering history with the real StoneX adapter's fetch_history (rows re-labelled 'mock')."""

    def __init__(self, stonex: StoneXAdapter) -> None:
        super().__init__()
        self.stonex = stonex

    async def fetch_history(self, since_ms: int, until_ms: int) -> BrokerHistory:
        self.calls.append(("fetch_history", (since_ms, until_ms)))
        h = await self.stonex.fetch_history(since_ms, until_ms)
        return replace(h, adapter_id="mock", orders=tuple(as_mock(o) for o in h.orders),
                       positions=tuple(as_mock(p) for p in h.positions))


async def stonex_rig(tmp_path: Path, clock: ManualClock, ad: HistoryAdapter, db: Db) -> Rig:
    """tests.runtime.test_catchup.rig with the given adapter."""
    env = make(tmp_path, clock, adapter=ad, local_state=db)
    pages: list[tuple[str, str, str]] = []
    coord = CatchupCoordinator(env.orch, db, db, adapter_ids=lambda: ["mock"], epoch_token=lambda: 7,
                               run_id=lambda: "run-1", pager=lambda s, t, b: pages.append((s, t, b)),
                               settings=FAST, metrics=CatchupMetrics(), clock_ms=clock)
    stop = asyncio.Event()

    async def committer() -> None:
        while not stop.is_set():
            for item in env.orders() + env.positions() + env.account_items():
                db.apply(item)
            await asyncio.sleep(0.001)

    return Rig(env, ad, db, coord, pages, stop, [asyncio.create_task(committer())])


async def test_a_catchup_over_untimed_working_rows_is_incomplete_and_pages_the_ids_and_window(
        tmp_path: Path, clock: ManualClock) -> None:
    stonex = await connected(live_book())
    snapshot_orders = [as_mock(o) for o in await stonex.fetch_open_orders()]
    untimed = {o.broker_order_id for o in snapshot_orders if o.broker_updated_at_ms == 0}
    assert untimed == {"901", "902", "971", "972"}            # own-row children keep the plain untimed WORKING
    ad = StoneXBacked(stonex)
    ad.open_orders = snapshot_orders
    ad.positions = [as_mock(p) for p in await stonex.fetch_positions()]
    r = await stonex_rig(tmp_path, clock, ad, Db(cov=Coverage("mock", T0, None)))
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
        await stonex.close()
    ((_epoch, rec),) = r.db.records
    assert not rec.complete
    (note,) = [n for n in rec.notes.split("; ") if "without a broker revision" in n]
    assert all(oid in note for oid in untimed), note
    high = [(t, b) for sev, t, b in r.pages if sev == "high" and "catch-up incomplete" in t]
    assert len(high) == 1, r.pages
    body = high[0][1]
    assert "uncovered window" in body and "skipped (no version)" in body
    assert all(oid in body for oid in untimed), body


async def test_a_snapshot_after_the_streams_pending_if_done_children_keeps_them_pending_at_broker_time(
        tmp_path: Path, clock: ManualClock) -> None:
    fake = FakeStoneX()
    fake.actives = [entry_row(970, T0, IfDone=IF_DONE)]
    stonex = await connected(fake)
    try:
        rest = {o.broker_order_id: as_mock(o) for o in await stonex.fetch_open_orders()}
    finally:
        await stonex.close()
    assert all((rest[c].status, rest[c].broker_updated_at_ms) == (OrderStatus.PENDING, T0) for c in ("971", "972"))
    env = make(tmp_path, clock)
    for s in ("orders", "account", "prices"):
        env.item(s, ev(K.SUBSCRIBED, s))
    for oid in ("970", "971", "972"):                       # the stream states the children Pending at T0
        env.item("orders", rest[oid])
    env.orders()
    env.adapter.open_orders = list(rest.values())
    await env.orch.run_snapshot("mock")
    env.orders()
    for cid in ("971", "972"):
        stored = env.orch.runtime("mock").orders.get(f"mock:{cid}")
        assert stored is not None and stored.status is OrderStatus.PENDING and stored.broker_time_source == "BROKER"
    # an entry amendment re-sends the children Pending: no illegal transition
    for cid in ("971", "972"):
        env.item("orders", replace(rest[cid], broker_updated_at_ms=T0 + 60_000))
    assert env.ring() == []
    assert all(o.status is OrderStatus.PENDING for o in env.orders() if o.broker_order_id in ("971", "972"))
