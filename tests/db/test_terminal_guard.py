"""Terminal-state guard in the repository (dto-contracts.md §5; IMPLEMENTATION_PLAN 6.4).

An illegal transition with a newer version is parked in events_dlq inside the store's own fenced transaction
and never committed, even when the orchestrator no longer holds the entity in memory (restart, LRU eviction):
the check reads the stored row's status, not an in-memory copy.
"""
from __future__ import annotations

import hashlib
from decimal import Decimal
from typing import Any

import pytest

from src.db import repository as R
from src.db.entity_sql import ACCOUNTS, ORDERS, POSITIONS
from src.db.epoch import EngineDb
from src.db.fence import FENCE, EpochMismatch
from src.dto import AccountDTO, OrderDTO, OrderStatus, PositionDTO
from src.pubsub.dlq_rows import ILLEGAL_TRANSITION, dlq_row

from tests.db.conftest import FakeConn


def order(**kw: Any) -> OrderDTO:
    base: dict[str, Any] = dict(
        client_order_id=None, broker_order_id="B1", account_id="ACC", adapter_id="stonex", symbol="EURUSD",
        side="BUY", status=OrderStatus.WORKING, entry_price=Decimal("1.1"), stop_loss=Decimal("1.09"),
        ordered_units=Decimal(1000), remaining_units=Decimal(1000), broker_updated_at_ms=1_000, observed_seq=10,
        seq_id=100)
    base.update(kw)
    return OrderDTO(**base)


def position(**kw: Any) -> PositionDTO:
    base: dict[str, Any] = dict(position_id="P1", account_id="ACC", adapter_id="ctrader", symbol="EURUSD",
                                side="BUY", units=Decimal(1000), entry_price=Decimal("1.1"), status="OPEN",
                                broker_updated_at_ms=1_000, observed_seq=10, seq_id=100)
    base.update(kw)
    return PositionDTO(**base)


def stored_row(dto: OrderDTO | PositionDTO, status: str) -> tuple[Any, ...]:
    return (dto.broker_seq, dto.broker_updated_at_ms, dto.observed_seq, dto.seq_id, dto.checksum, status)


def test_select_reads_the_status_for_tables_with_a_lifecycle() -> None:
    assert ", status FROM orders_state" in ORDERS.select_for_update
    assert ", status FROM positions_state" in POSITIONS.select_for_update
    assert "status" not in ACCOUNTS.select_for_update
    assert FENCE in R.SQL_PARK_ILLEGAL and "ON CONFLICT (payload_hash) DO NOTHING" in R.SQL_PARK_ILLEGAL


@pytest.mark.parametrize(("old", "new", "illegal"), [
    ("FILLED", OrderStatus.WORKING, True), ("CANCELLED", OrderStatus.PARTIALLY_FILLED, True),
    ("REJECTED", OrderStatus.PENDING, True), ("PARTIALLY_FILLED", OrderStatus.WORKING, True),
    ("WORKING", OrderStatus.FILLED, False), ("PENDING", OrderStatus.PARTIALLY_FILLED, False),
    ("FILLED", OrderStatus.FILLED, False), ("NOT_A_STATUS", OrderStatus.WORKING, False),
])
def test_illegal_transition_table(old: str, new: OrderStatus, illegal: bool) -> None:
    assert (R.illegal_transition(order(status=new), old) is not None) is illegal


def test_position_closed_to_open_is_illegal_only_that_way() -> None:
    assert R.illegal_transition(position(status="OPEN"), "CLOSED") == ("CLOSED", "OPEN")
    assert R.illegal_transition(position(status="CLOSED"), "OPEN") is None
    assert R.illegal_transition(position(status="OPEN"), None) is None
    assert R.illegal_transition(AccountDTO(account_id="A", adapter_id="x", currency="USD", balance=Decimal(1)),
                                "CLOSED") is None


async def test_filled_order_not_in_memory_is_never_reopened(db: EngineDb, conn: FakeConn) -> None:
    """Regression: the stored row is FILLED (the orchestrator's LRU no longer has it); a newer WORKING row
    must be parked, not committed: no UPDATE, no event_log APPLIED row, no outbox row."""
    filled = order(status=OrderStatus.FILLED, remaining_units=Decimal(0))
    conn.returns(ORDERS.select_for_update, stored_row(filled, "FILLED"))
    newer = order(status=OrderStatus.WORKING, observed_seq=11, seq_id=101)
    with pytest.raises(R.IllegalTransitionParked) as info:
        await R.PgStateStore(db).commit_entity(newer)
    assert info.value.reason == ILLEGAL_TRANSITION
    assert (info.value.stored_status, info.value.new_status) == ("FILLED", "WORKING")
    assert conn.sql[1:-2] == [ORDERS.select_for_update, R.SQL_PARK_ILLEGAL]   # committed: the park survives
    (args,) = conn.args_of(R.SQL_PARK_ILLEGAL)
    expected = dlq_row(newer, ILLEGAL_TRANSITION)
    assert args == (7, "ORDER", "B1", "stonex", expected.raw_payload, expected.payload_hash, ILLEGAL_TRANSITION)
    assert args[5] == hashlib.sha256(args[4].encode("utf-8")).hexdigest()
    assert ORDERS.update_apply not in conn.sql and R.SQL_EVENT_LOG not in conn.sql and R.SQL_OUTBOX not in conn.sql


async def test_closed_position_not_in_memory_is_never_reopened(db: EngineDb, conn: FakeConn) -> None:
    closed = position(status="CLOSED", units=Decimal(0))
    conn.returns(POSITIONS.select_for_update, stored_row(closed, "CLOSED"))
    with pytest.raises(R.IllegalTransitionParked):
        await R.PgStateStore(db).commit_entity(position(status="OPEN", observed_seq=12, seq_id=102))
    assert conn.sql[1:-2] == [POSITIONS.select_for_update, R.SQL_PARK_ILLEGAL]


async def test_legal_newer_transition_still_commits(db: EngineDb, conn: FakeConn) -> None:
    working = order()
    conn.returns(ORDERS.select_for_update, stored_row(working, "WORKING"))
    res = await R.PgStateStore(db).commit_entity(order(status=OrderStatus.FILLED, remaining_units=Decimal(0),
                                                       observed_seq=11, seq_id=101))
    assert res.decision.name == "APPLY"
    assert R.SQL_PARK_ILLEGAL not in conn.sql and ORDERS.update_apply in conn.sql


async def test_stale_illegal_row_is_dropped_not_parked(db: EngineDb, conn: FakeConn) -> None:
    """A lower version is never parked (tier-persistence.md §5): it is dropped as before."""
    filled = order(status=OrderStatus.FILLED, observed_seq=99, remaining_units=Decimal(0))
    conn.returns(ORDERS.select_for_update, stored_row(filled, "FILLED"))
    res = await R.PgStateStore(db).commit_entity(order(status=OrderStatus.WORKING, observed_seq=5))
    assert res.decision.name == "DROP_STALE"
    assert R.SQL_PARK_ILLEGAL not in conn.sql


async def test_insert_race_winner_is_checked_too(db: EngineDb, conn: FakeConn) -> None:
    filled = order(status=OrderStatus.FILLED, observed_seq=1, remaining_units=Decimal(0))
    conn.returns(ORDERS.select_for_update, None, stored_row(filled, "FILLED"))   # insert lost to a FILLED winner
    with pytest.raises(R.IllegalTransitionParked):
        await R.PgStateStore(db).commit_entity(order(status=OrderStatus.WORKING, observed_seq=11))
    assert ORDERS.update_apply not in conn.sql


async def test_lost_fence_while_parking_is_an_epoch_mismatch(db: EngineDb, conn: FakeConn) -> None:
    conn.returns(ORDERS.select_for_update, stored_row(order(status=OrderStatus.FILLED), "FILLED"))
    conn.stored_epoch = 8          # another leader bumped the epoch: the park affects 0 rows
    with pytest.raises(EpochMismatch):
        await R.PgStateStore(db).commit_entity(order(status=OrderStatus.WORKING, observed_seq=11))


async def test_tiered_committer_propagates_the_park_without_touching_redis(db: EngineDb, conn: FakeConn) -> None:
    class NoRedis:
        no_boot: Any = None

        async def apply_after_commit(self, w: Any) -> Any:   # pragma: no cover - must not be reached
            raise AssertionError("Redis must not see a parked row")

    conn.returns(ORDERS.select_for_update, stored_row(order(status=OrderStatus.CANCELLED), "CANCELLED"))
    committer = R.TieredStateCommitter(R.PgStateStore(db), NoRedis(), lambda a, s: None)  # type: ignore[arg-type]
    with pytest.raises(R.IllegalTransitionParked):
        await committer.commit_entity(order(status=OrderStatus.WORKING, observed_seq=11))


async def test_consumer_counts_the_park_as_a_failure_and_its_repark_dedupes(db: EngineDb, conn: FakeConn,
                                                                           tmp_path: Any) -> None:
    """Contract with src/pubsub/consumers.py: the raised park is a failed commit (the reconcile barrier keeps
    the gap open) and the consumer's own re-park carries the same payload_hash, so the idempotent events_dlq
    insert adds no second row."""
    from src.pubsub.bus import EventBus
    from src.pubsub.consumers import OrderStateConsumer
    from src.pubsub.spooler import DlqSpooler
    from tests.pubsub.conftest import FakeDlqSink

    conn.returns(ORDERS.select_for_update, stored_row(order(status=OrderStatus.FILLED), "FILLED"))
    store = R.PgStateStore(db)

    class StoreOnly:
        async def commit_entity(self, dto: Any) -> Any:
            return (await store.commit_entity(dto)).decision

    bus = EventBus()
    spooler = DlqSpooler(FakeDlqSink(), spill_path=tmp_path / "spill.bin")
    newer = order(status=OrderStatus.WORKING, observed_seq=11)
    bus.publish_order_nowait(newer)
    consumer = OrderStateConsumer(bus.orders, StoreOnly(), spooler)  # type: ignore[arg-type]
    assert await consumer.process(await bus.orders.get()) is False
    (parked_args,) = conn.args_of(R.SQL_PARK_ILLEGAL)
    assert spooler._ring[0].payload_hash == parked_args[5]
