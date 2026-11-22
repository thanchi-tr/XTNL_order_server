"""Known facts are never blanked (dto-contracts.md §2, fix 2026-11-21).

Live finding: after an engine restart the start-up catch-up re-read a closed StoneX sell position from the trade
history, which states no entry price, and its take-profit child, which the history could not link. Both rows had
the same broker time as the stored rows and a later observation, so they won the version tie and replaced the
stored entry price and the child's link (and reduce-only flag) with nothing. The orchestrator now keeps those facts
when a row does not state them, and loads the stored rows a catch-up touches before applying it."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from src.dto import OrderDTO, OrderStatus, PositionDTO
from src.pubsub.orchestrator import _keep_known_facts
from tests.pubsub.conftest import ManualClock, ev, order, position
from tests.pubsub.test_catchup_hooks import HistoryAdapter, history, submit_with_consumers
from tests.pubsub.test_orchestrator import K, make, ready


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


def child(oid: str = "C1", parent: str | None = "P1", status: OrderStatus = OrderStatus.WORKING,
          t: int = 50) -> OrderDTO:
    return replace(order(oid, status, t=t), side="SELL", is_reduce_only=parent is not None,
                   linked_position_id=parent)


# ---- the rule -------------------------------------------------------------------------------------------------
def test_an_order_row_without_a_link_keeps_the_known_link_and_reduce_only() -> None:
    known = child()
    unlinked = replace(child(status=OrderStatus.CANCELLED, t=60), is_reduce_only=False, linked_position_id=None)
    kept = _keep_known_facts(unlinked, known)
    assert (kept.is_reduce_only, kept.linked_position_id, kept.status) == (True, "P1", OrderStatus.CANCELLED)
    assert kept.checksum != unlinked.checksum            # re-sealed with the kept link


def test_a_stated_link_always_wins_and_a_standalone_order_is_never_made_a_child() -> None:
    corrected = child(parent="P2", t=60)
    assert _keep_known_facts(corrected, child()) is corrected
    entry = order("E1")
    assert _keep_known_facts(replace(entry, entry_price=None), entry).entry_price == Decimal("1.1")
    assert not _keep_known_facts(order("E1", t=5), entry).is_reduce_only


def test_a_position_row_without_an_entry_keeps_the_known_entry_and_opening_order() -> None:
    known = replace(position("P1", "OPEN", t=10), opened_by_client_order_id="c-P1")
    closed = replace(position("P1", "CLOSED", t=60, units="0"), entry_price=None, opened_by_client_order_id=None)
    kept = _keep_known_facts(closed, known)
    assert (kept.entry_price, kept.opened_by_client_order_id, kept.status) == (Decimal("1.1"), "c-P1", "CLOSED")
    moved = replace(position("P1", "OPEN", t=70), entry_price=Decimal("1.2"))
    assert _keep_known_facts(moved, known).entry_price == Decimal("1.2")      # a stated entry is never changed


def test_stops_units_and_status_are_never_inherited() -> None:
    known = order("O1", stop="1.05")
    removed = order("O1", t=9)                           # its stop removed at the broker
    assert _keep_known_facts(removed, known).stop_loss is None
    assert _keep_known_facts(replace(position("P1", "CLOSED", units="0"), entry_price=None),
                             position("P1")).units == 0


def test_nothing_known_or_another_entity_changes_nothing() -> None:
    row = replace(child(), linked_position_id=None, is_reduce_only=False)
    assert _keep_known_facts(row, None) is row
    assert _keep_known_facts(row, child("C2")) is row
    assert _keep_known_facts(row, replace(child(), adapter_id="other")) is row


# ---- the live incident, end to end --------------------------------------------------------------------------
class Reader:
    """The stored rows after a restart: nothing open (the trade closed before it), so only `stored_entities` has
    the closed position and its finished children."""

    def __init__(self, orders: Sequence[OrderDTO], positions: Sequence[PositionDTO]) -> None:
        self.orders, self.positions = list(orders), list(positions)
        self.asked: list[tuple[list[str], list[str]]] = []

    async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
        return [], []

    async def stored_entities(self, adapter_id: str, order_ids: Sequence[str],
                              position_ids: Sequence[str]) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
        self.asked.append((list(order_ids), list(position_ids)))
        return ([o for o in self.orders if o.broker_order_id in order_ids],
                [p for p in self.positions if p.position_id in position_ids])


async def test_a_catchup_after_a_restart_never_blanks_the_closed_trade(tmp_path: Path, clock: ManualClock) -> None:
    stored_pos = replace(position("P1", "CLOSED", t=60, units="0"), side="SELL", entry_price=Decimal("1.09903"))
    stored_tp = child("TP1", status=OrderStatus.CANCELLED, t=60)
    stored_sl = child("SL1", status=OrderStatus.FILLED, t=60)
    reader = Reader([stored_tp, stored_sl], [stored_pos])
    ad = HistoryAdapter()
    env = make(tmp_path, clock, adapter=ad, local_state=reader)
    await ready(env)
    rt = env.orch.runtime("mock")
    assert "mock:P1" not in rt.positions and "mock:TP1" not in rt.orders    # a restart holds no finished entity
    # what the history states: the close without an entry, the take-profit it could not link
    ad.history = history([replace(stored_tp, is_reduce_only=False, linked_position_id=None), stored_sl],
                         [replace(stored_pos, entry_price=None)])
    f = await env.orch.catchup_fetch("mock", 0, clock.ms)
    res = await submit_with_consumers(env, env.orch.catchup_submit(f, timeout_s=2.0))
    assert res.committed  # type: ignore[attr-defined]
    assert reader.asked == [(["SL1", "TP1"], ["P1"])]
    assert rt.positions["mock:P1"].entry_price == Decimal("1.09903")
    tp = rt.orders["mock:TP1"]
    assert (tp.is_reduce_only, tp.linked_position_id) == (True, "P1")
    # same content as stored: the store sees a no-op version advance, never a blanked row
    assert tp.checksum == stored_tp.checksum and rt.positions["mock:P1"].checksum == stored_pos.checksum
    assert not env.orch.parked


async def test_a_stream_row_without_its_link_keeps_the_stored_link(tmp_path: Path, clock: ManualClock) -> None:
    """After a restart the stream mapper may not know a working child's parent yet: its row states none."""
    stored = child("C9", t=40)

    class Open:
        async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
            return [stored], []

    env = make(tmp_path, clock, local_state=Open())
    await env.orch._load_local(env.orch.runtime("mock"))
    await ready(env)
    env.item("orders", replace(stored, broker_updated_at_ms=70, is_reduce_only=False, linked_position_id=None,
                               source="STREAM"))
    (row,) = [o for o in env.orders() if o.broker_order_id == "C9"]
    assert (row.is_reduce_only, row.linked_position_id, row.broker_updated_at_ms) == (True, "P1", 70)


async def test_a_reader_without_stored_entities_still_works(tmp_path: Path, clock: ManualClock) -> None:
    class Old:
        async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
            return [], []

    ad = HistoryAdapter()
    env = make(tmp_path, clock, adapter=ad, local_state=Old())
    await ready(env)
    ad.history = history([order("H1", OrderStatus.FILLED, t=5)])
    f = await env.orch.catchup_fetch("mock", 0, clock.ms)
    res = await submit_with_consumers(env, env.orch.catchup_submit(f, timeout_s=2.0))
    assert res.committed and res.orders_published == 1  # type: ignore[attr-defined]
    assert ev(K.SUBSCRIBED, "orders") is not None
