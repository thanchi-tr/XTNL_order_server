"""S01: the 0015 columns through the stores. order_kind and the instrument metadata round-trip through the row
decoders and the integrity recompute (dbcheck); the event_log insert writes origin / broker_time_ms as NULL from
every caller until S04 labels versions. Synthetic instruments only."""
from __future__ import annotations

import re
from dataclasses import replace
from decimal import Decimal

from src.cache.scripts import entity_write
from src.dashboard import views as V
from src.db import integrity_store as I
from src.db import repository as R
from src.db.entity_sql import ORDERS
from src.db.epoch import EngineDb
from src.db.rows import INSTRUMENT_COLS, ORDER_COLS, instrument_from_row, order_from_row
from src.dto import CashFlowDTO, Decision, InstrumentSpec, OrderDTO, OrderStatus
from src.integrity.dbcheck import check_row
from tests.db.conftest import FakeConn
from tests.db.integrity_emulator import IntegrityEmulator
from tests.integrity.fakes import T0, ManualClock, order


def spec(**kw: object) -> InstrumentSpec:
    base: dict[str, object] = dict(adapter_id="mock", symbol="SYN.A", base_ccy="AAA", quote_ccy="BBB",
                                   pnl_ccy="BBB", contract_size=Decimal(1), unit_kind="UNITS",
                                   point_size=Decimal("0.5"), min_qty=Decimal(1), swap_booked_to_balance=False)
    base.update(kw)
    return InstrumentSpec(**base)  # type: ignore[arg-type]


def _cols(cols: str) -> list[str]:
    return [c.strip() for c in cols.split(",")]


def _row(dto: object, cols: str) -> tuple[object, ...]:
    return tuple(getattr(dto, c).value if isinstance(getattr(dto, c), OrderStatus) else getattr(dto, c)
                 for c in _cols(cols))


def test_order_cols_append_order_kind_at_index_21() -> None:
    assert _cols(ORDER_COLS).index("order_kind") == 21 == len(_cols(ORDER_COLS)) - 1
    assert _cols(INSTRUMENT_COLS)[11:] == ["price_increment", "min_stop_distance", "asset_class"]
    assert ORDERS.payload_cols[-1] == "order_kind"


def test_order_row_round_trip_keeps_order_kind_and_checksum() -> None:
    for kind in (None, "STOP", "LIMIT"):
        o: OrderDTO = replace(order("B1"), order_kind=kind)
        back = order_from_row(_row(o, ORDER_COLS))
        assert back == o and back.order_kind == kind and back.checksum == o.checksum


def test_instrument_row_round_trip() -> None:
    for s in (spec(), spec(price_increment=Decimal("0.25"), min_stop_distance=Decimal(2), asset_class="SYNTHETIC")):
        assert instrument_from_row(_row(s, INSTRUMENT_COLS)) == s


async def test_integrity_recompute_with_order_kind_stop() -> None:
    emu = IntegrityEmulator(ManualClock())
    plain, stop = order("B1"), replace(order("B2"), order_kind="STOP")
    emu.put(plain, T0)
    emu.put(stop, T0)
    emu.put(spec(price_increment=Decimal("0.25")), T0, seq_id=900)
    db = I.PgIntegrityDb(emu)
    rows = {r.key: r for r in await db.committed_rows(T0, T0, 10)}
    good = rows["order:stonex:B2"]
    assert check_row(good) == [] and check_row(rows["order:stonex:B1"]) == []
    assert isinstance(good.entity, OrderDTO) and good.entity.order_kind == "STOP"
    assert good.checksum == stop.checksum != plain.checksum
    assert good.write().payload == good.payload and '"order_kind":"STOP"' in good.payload
    assert '"order_kind"' not in rows["order:stonex:B1"].payload
    inst = rows["instrument:mock:SYN.A"]
    assert check_row(inst) == [] and isinstance(inst.entity, InstrumentSpec)
    assert inst.entity.price_increment == Decimal("0.25")


def test_order_kind_is_a_payload_column_of_the_current_row() -> None:
    o = replace(order("B1"), order_kind="STOP")
    w = entity_write(o)
    params = ORDERS.insert_params(o, w)
    assert params[len(ORDERS.key_cols) + ORDERS.payload_cols.index("order_kind")] == "STOP"
    assert "order_kind = $" in ORDERS.update_apply and "order_kind" not in ORDERS.update_advance


def test_event_log_sql_writes_origin_and_broker_time_ms() -> None:
    assert "origin, broker_time_ms)" in R.SQL_EVENT_LOG
    assert "$11::text, $12::bigint WHERE" in R.SQL_EVENT_LOG
    assert {int(n) for n in re.findall(r"\$(\d+)", R.SQL_EVENT_LOG)} == set(range(1, 13))
    assert {int(n) for n in re.findall(r"\$(\d+)", R.SQL_INSTRUMENT_UPSERT)} == set(range(1, 20))
    for col in ("price_increment", "min_stop_distance", "asset_class"):
        assert f"{col} = EXCLUDED.{col}" in R.SQL_INSTRUMENT_UPSERT


async def test_event_log_labels_come_only_from_labelled_entity_versions(db: EngineDb, conn: FakeConn) -> None:
    """S04: an entity version writes its origin, and its broker time only when that is a real broker time; cash
    flows and instruments (unlabelled) write NULL."""
    store = R.PgStateStore(db)
    conn.returns(ORDERS.insert_new, (1,))
    labelled = replace(order("B1"), order_kind="STOP", origin="stream", broker_time_source="BROKER")
    assert (await store.commit_entity(labelled)).decision is Decision.APPLY
    conn.returns(R.SQL_CASH_FLOW, (1,))
    assert await store.commit_cash_flow(CashFlowDTO(transaction_id="T1", account_id="ACC", adapter_id="mock",
                                                    kind="ADJUSTMENT", amount=Decimal(1), currency="XXX",
                                                    broker_ts=1, seq_id=5))
    conn.returns(R.SQL_INSTRUMENT_UPSERT, (1,))
    assert await store.commit_instrument(spec(asset_class="SYNTHETIC"), 10) is not None
    logs = conn.args_of(R.SQL_EVENT_LOG)
    assert len(logs) == 3
    assert all(len(args) == 12 for args in logs)
    assert logs[0][-2:] == ("stream", labelled.broker_updated_at_ms)
    assert logs[1][-2:] == (None, None) and logs[2][-2:] == (None, None)
    (ins,) = conn.args_of(R.SQL_INSTRUMENT_UPSERT)
    assert len(ins) == 19 and ins[-3:] == (None, None, "SYNTHETIC")


def test_dashboard_orders_view_shows_order_kind() -> None:
    assert "order_kind" in V.ORDER_COLS
    m = re.match(r"SELECT (.*?) FROM orders_state", V.SQL_ORDERS)
    assert m and _cols(m.group(1)) == list(V.ORDER_COLS)
