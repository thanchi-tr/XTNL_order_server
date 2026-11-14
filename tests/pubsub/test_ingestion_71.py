"""IMPLEMENTATION_PLAN 7.1 (ingestion): adapter-skipped rows before / during a pass, broker_seq inheritance across a
restart, partial cash-flow fetches flagged through `pnl_source`, consumers mapping the store's
IllegalTransitionParked to `illegal_transition`, and a refused status never kept in orchestrator memory."""
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest

from src.adapters.base import CashFlowBatch, VersionDecl
from src.dto import AccountDTO, CashFlowDTO, OrderDTO, OrderStatus, PositionDTO
from src.pubsub.bus import EventBus
from src.pubsub.consumers import AccountImageProjector, OrderStateConsumer, PositionStateConsumer, store_refusal
from src.pubsub.spooler import DlqSpooler
from tests.pubsub.conftest import (
    FakeAdapter,
    FakeCommitter,
    FakeDlqSink,
    ManualClock,
    account,
    cash_flow,
    ev,
    order,
    position,
)
from tests.pubsub.test_ingestion_62 import SkippingAdapter
from tests.pubsub.test_orchestrator import ROLL, K, make, ready

Page = tuple[str, str, str]
SEQ_ACCOUNTS = {"ORDER": VersionDecl(False, True), "POSITION": VersionDecl(False, True),
                "ACCOUNT": VersionDecl(True, False)}


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


def subscribe_all(env: object) -> None:
    for s in ("orders", "account", "prices"):
        env.item(s, ev(K.SUBSCRIBED, s))  # type: ignore[attr-defined]


# ===================================================================== unmapped rows around a REST pass
async def test_a_row_skipped_before_the_pass_is_covered_by_it(tmp_path: Path, clock: ManualClock) -> None:
    adapter = SkippingAdapter()
    env = make(tmp_path, clock, adapter=adapter)
    rt = env.orch.runtime("mock")
    subscribe_all(env)
    adapter.skip("orders")                       # a stream row skipped (its GAP / count) before the pass is sent
    rep = await env.orch.run_snapshot("mock")
    assert not rep.dropped_after_watermark       # the pass read the broker after the skip: it may clear the gap
    assert not rt.subs["orders"].lost and not rt.resync_event.is_set()   # no second pass needed for it
    assert ("reconciled", "orders") in await env.health_ops()
    assert env.orch.frames_lost == {"orders": 1}  # still counted, never silent


async def test_a_row_skipped_during_the_pass_still_keeps_the_gap(tmp_path: Path, clock: ManualClock) -> None:
    adapter = SkippingAdapter()
    env = make(tmp_path, clock, adapter=adapter)
    rt = env.orch.runtime("mock")
    subscribe_all(env)
    adapter.on_fetch_open_orders = adapter.skip
    rep = await env.orch.run_snapshot("mock")
    assert rep.dropped_after_watermark and rt.subs["orders"].lost and rt.resync_event.is_set()


# ===================================================================== broker_seq inheritance
class StoredReader:
    """LocalStateReader with the optional account_states method (src/db wiring: IMPLEMENTATION_PLAN 7.4)."""

    def __init__(self, accounts: Sequence[AccountDTO] = (), orders: Sequence[OrderDTO] = ()) -> None:
        self.accounts = list(accounts)
        self.orders = list(orders)

    async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
        return self.orders, []

    async def account_states(self, adapter_id: str) -> Sequence[AccountDTO]:
        return self.accounts


async def test_after_a_restart_a_row_without_broker_seq_inherits_the_stored_one(tmp_path: Path,
                                                                                clock: ManualClock) -> None:
    ad = FakeAdapter(versioning={**SEQ_ACCOUNTS, "ORDER": VersionDecl(True, True)})   # type: ignore[dict-item]
    reader = StoredReader(accounts=[account("9000", bseq=41)], orders=[order("O1", t=1, bseq=9)])
    env = make(tmp_path, clock, adapter=ad, local_state=reader)
    ad.accounts = [account("9100", bseq=None)]        # the first REST row of the new term omits balanceVersion
    ad.orders_by_id["O1"] = order("O1", t=2, bseq=None, stop="1.0")
    subscribe_all(env)
    await env.orch.run_snapshot("mock")
    [acct] = [a for a in env.account_items() if isinstance(a, AccountDTO)]
    assert acct.broker_seq == 41                       # inherited from the stored account_state row
    [o] = env.orders()
    assert o.broker_seq == 9                           # inherited from the stored order row
    rt = env.orch.runtime("mock")
    assert not rt.subs["account"].lost


async def test_a_reader_without_account_states_still_seeds_orders(tmp_path: Path, clock: ManualClock) -> None:
    class OrdersOnly:
        async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
            return [order("O1", t=1, bseq=5)], []

    ad = FakeAdapter(versioning={**SEQ_ACCOUNTS, "ORDER": VersionDecl(True, True)})   # type: ignore[dict-item]
    env = make(tmp_path, clock, adapter=ad, local_state=OrdersOnly())
    await env.orch.run_snapshot("mock")
    assert env.orch.runtime("mock").broker_seq == {"mock:O1": 5}


async def test_a_row_without_broker_seq_and_nothing_to_inherit_opens_the_gap(tmp_path: Path,
                                                                            clock: ManualClock) -> None:
    pages: list[Page] = []
    env = make(tmp_path, clock, adapter=FakeAdapter(versioning=SEQ_ACCOUNTS),  # type: ignore[arg-type]
               pager=lambda s, t, b: pages.append((s, t, b)))
    await ready(env)
    rt = env.orch.runtime("mock")
    env.item("account", account("9000", bseq=None))    # never published with a null broker_seq
    assert [a for a in env.account_items() if isinstance(a, AccountDTO)] == []
    assert rt.subs["account"].lost and rt.resync_event.is_set()
    assert "ingestion frame lost: mock:account" in [t for _, t, _ in pages]
    # A REST pass whose row also lacks it cannot clear the gap ...
    env.adapter.accounts = [account("9000", bseq=None)]
    rep = await env.orch.run_snapshot("mock")
    assert rep.dropped_after_watermark and rt.subs["account"].lost
    # ... one that carries it can, and later rows inherit it.
    env.adapter.accounts = [account("9000", bseq=12)]
    rep = await env.orch.run_snapshot("mock")
    assert not rep.dropped_after_watermark
    env.account_items()
    await env.health_ops()
    assert not rt.subs["account"].lost
    env.item("account", account("9050", bseq=None))
    assert [a.broker_seq for a in env.account_items() if isinstance(a, AccountDTO)] == [12]


# ===================================================================== partial cash flows
class BatchAdapter(FakeAdapter):
    """Returns `flows` as is (a CashFlowBatch keeps its partial marker)."""

    async def fetch_cash_flows(self, since_ms: int) -> list[CashFlowDTO]:
        self.calls.append(("fetch_cash_flows", since_ms))
        return self.flows


async def test_a_partial_cash_flow_fetch_turns_pnl_source_off_until_a_complete_one(tmp_path: Path,
                                                                                    clock: ManualClock) -> None:
    pages: list[Page] = []
    env = make(tmp_path, clock, adapter=BatchAdapter(), pager=lambda s, t, b: pages.append((s, t, b)))
    assert env.orch.pnl_source("mock") and not env.orch.pnl_source("unknown")    # unknown adapter: fail closed
    env.adapter.flows = CashFlowBatch([cash_flow("T1")], ["trade history hit maxResults=200"])   # type: ignore[assignment]
    await env.orch.run_snapshot("mock")
    assert not env.orch.pnl_source("mock")
    assert env.orch.runtime("mock").cash_flows_partial == ("trade history hit maxResults=200",)
    assert [t for _, t, _ in pages] == ["cash-flow ledger incomplete: mock"]
    assert [x for x in env.account_items() if isinstance(x, CashFlowDTO)]      # what was read is still used

    # a complete fetch that does not cover the whole day (since after the roll) leaves the flag set
    assert env.orch.note_cash_flows("mock", [cash_flow("T1")], ROLL + 1) == ()
    assert not env.orch.pnl_source("mock")
    env.adapter.flows = [cash_flow("T1")]
    await env.orch.run_snapshot("mock")                # since = the roll: complete again
    assert env.orch.pnl_source("mock") and env.orch.runtime("mock").cash_flows_partial == ()


async def test_request_cash_flows_notes_a_partial_result(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock, adapter=BatchAdapter())
    await ready(env)
    from tests.pubsub.test_orchestrator import to_streaming
    to_streaming(env)
    env.item("account", account("100"))
    env.account_items()
    env.adapter.flows = CashFlowBatch([], ["deal list truncated"])   # type: ignore[assignment]
    assert await env.orch.request_cash_flows("A1")
    assert not env.orch.pnl_source("mock")


# ===================================================================== store refusals
class IllegalTransitionParked(Exception):
    """Stand-in with the shape of src.db.repository.IllegalTransitionParked (duck-typed by name + reason)."""

    reason = "illegal_transition"

    def __init__(self, entity_key: str, stored_status: str, new_status: str) -> None:
        super().__init__(f"illegal transition {stored_status} -> {new_status} for {entity_key}: parked")
        self.entity_key, self.stored_status, self.new_status = entity_key, stored_status, new_status


def test_store_refusal_recognises_only_the_store_refusal() -> None:
    assert store_refusal(IllegalTransitionParked("k", "FILLED", "WORKING")) == "FILLED"
    assert store_refusal(RuntimeError("illegal_transition")) is None

    class Other(Exception):
        reason = "illegal_transition"

    assert store_refusal(Other()) is None                 # same reason attr, not the store's exception


def test_the_real_repository_exception_is_recognised() -> None:
    from src.db.repository import IllegalTransitionParked as Real

    assert store_refusal(Real("mock:O1", "FILLED", "WORKING")) == "FILLED"


async def test_a_store_refusal_is_parked_and_paged_as_illegal_transition(tmp_path: Path) -> None:
    bus = EventBus()
    com = FakeCommitter()
    sp = DlqSpooler(FakeDlqSink(), spill_path=tmp_path / "s.bin")
    pages: list[Page] = []
    refused: list[tuple[object, str | None]] = []
    bus.commit_barrier.add_refusal_listener(lambda item, st: refused.append((item, st)))
    com.fail = IllegalTransitionParked("mock:O7", "FILLED", "WORKING")
    o = order("O7")
    bus.publish_order_nowait(o)
    c = OrderStateConsumer(bus.orders, com, sp, pager=lambda s, t, b: pages.append((s, t, b)))
    settled: list[frozenset[str]] = []
    bus.commit_barrier.track("mock", [(o.seq_id, "orders")], lambda failed: settled.append(failed))  # type: ignore[arg-type]
    assert not await c.process(bus.orders.get_nowait())
    assert c.failed == 1 and c.illegal_transitions == 1
    # the same row the store parked (same payload_hash: the idempotent insert adds nothing), never consumer_error
    [row] = sp._ring
    assert (row.entity_id, row.error_reason) == ("O7", "illegal_transition")
    from src.pubsub.dlq_rows import dlq_row
    assert row.payload_hash == dlq_row(o, "illegal_transition").payload_hash
    assert refused == [(o, "FILLED")]
    assert settled == [frozenset({"orders"})]              # a reconcile pass that carried it keeps its gap
    assert [t for _, t, _ in pages] == ["consumer commit failed: orders"]
    assert "illegal_transition" in pages[0][2] and "refused by the store; stored FILLED" in pages[0][2]
    assert "consumer_error" not in pages[0][2]


async def test_other_consumer_failures_still_park_consumer_error(tmp_path: Path) -> None:
    bus, com = EventBus(), FakeCommitter()
    sp = DlqSpooler(FakeDlqSink(), spill_path=tmp_path / "s.bin")
    com.fail = RuntimeError("pg constraint")
    bus.publish_position_nowait(position("P7"))
    c = PositionStateConsumer(bus.positions, com, sp)
    await c.process_available()
    assert c.illegal_transitions == 0 and sp._ring[0].error_reason == "consumer_error"


async def test_a_refused_order_status_is_not_kept_in_orchestrator_memory(tmp_path: Path,
                                                                        clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    rt = env.orch.runtime("mock")
    env.item("orders", order("O1", OrderStatus.WORKING, t=clock.ms - 10))
    [o] = env.orders()
    assert rt.orders["mock:O1"].status is OrderStatus.WORKING
    # The store already holds O1 FILLED (unknown to memory) and refuses the WORKING row.
    com = FakeCommitter()
    com.fail = IllegalTransitionParked("mock:O1", "FILLED", "WORKING")
    env.bus.publish_order_nowait(o)
    await OrderStateConsumer(env.bus.orders, com, env.spooler).process_available()
    assert rt.orders["mock:O1"].status is OrderStatus.FILLED      # the stored status, not the refused one
    assert env.ring() == [("O1", "illegal_transition")]           # the consumer's (idempotent) park
    # A later WORKING row is now caught in memory as illegal, like the store would.
    env.item("orders", order("O1", OrderStatus.WORKING, t=clock.ms, stop="1.0"))
    assert env.orders() == [] and env.ring() == [("O1", "illegal_transition")] * 2


async def test_a_refused_position_reopen_is_not_kept_and_not_reread_as_open(tmp_path: Path,
                                                                           clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    rt = env.orch.runtime("mock")
    env.item("orders", position("P1", "OPEN", t=clock.ms - 10))
    [p] = env.positions()
    env.orch.on_store_refused(p, "CLOSED")
    assert rt.positions["mock:P1"].status == "CLOSED"
    rep = await env.orch.run_snapshot("mock")
    assert rep.reread_positions == []                  # a CLOSED entity is not re-read as an absent OPEN one


async def test_only_the_refused_row_itself_is_restored(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    rt = env.orch.runtime("mock")
    env.item("orders", order("O1", OrderStatus.WORKING, t=clock.ms - 10))
    [first] = env.orders()
    env.item("orders", order("O1", OrderStatus.PARTIALLY_FILLED, t=clock.ms, remaining="500"))
    env.orch.on_store_refused(first, "FILLED")             # an older refusal: memory moved on, left alone
    assert rt.orders["mock:O1"].status is OrderStatus.PARTIALLY_FILLED
    [second] = env.orders()
    env.orch.on_store_refused(second, "NOT-A-STATUS")      # unknown stored status: forgotten, store guards
    assert "mock:O1" not in rt.orders
    env.orch.on_store_refused(account(), "X")              # accounts have no lifecycle: ignored
    env.orch.on_store_refused(order("O9", adapter_id="other"), "FILLED")   # unknown adapter: ignored


async def test_account_projector_maps_the_refusal_too(tmp_path: Path) -> None:
    bus, com = EventBus(), FakeCommitter()
    sp = DlqSpooler(FakeDlqSink(), spill_path=tmp_path / "s.bin")
    com.fail = IllegalTransitionParked("mock:A1", "X", "Y")
    bus.publish_account_nowait(account())
    c = AccountImageProjector(bus.account, com, sp)
    await c.process_available()
    assert c.illegal_transitions == 1 and [r.error_reason for r in sp._ring] == ["illegal_transition"]
