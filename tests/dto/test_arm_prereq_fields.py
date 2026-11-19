"""S01 (ARM prerequisites PR1, PR2, PR7, PR8): the new DTO fields are checksum- and encoding-neutral while None.

The golden checksums below were computed before the fields existed. If one changes, every stored row would fail
its integrity recompute and be healed, so they must never move (arm-implementation-plan.md S01, §10).
Fixtures use synthetic instruments only.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import fields, replace
from decimal import Decimal as D
from typing import Any

import pytest

from src.cache.codec import _plain, dto_json
from src.dto import (NON_PAYLOAD_FIELDS, OMITTED_WHEN_NONE, OPTIONAL_PAYLOAD_FIELDS, AccountDTO, CashFlowDTO,
                     InstrumentSpec, OrderDTO, OrderStatus, PositionDTO, PriceDTO, payload_checksum)
from src.pubsub import dlq_rows

NEW_FIELDS = frozenset({"order_kind", "price_increment", "min_stop_distance", "asset_class", "origin",
                        "broker_time_source", "ts_source", "received_ms"})


def order(**kw: Any) -> OrderDTO:
    base: dict[str, Any] = dict(
        client_order_id="C1", broker_order_id="B1", account_id="ACC1", adapter_id="mock", symbol="SYN.A",
        side="SELL", status=OrderStatus.WORKING, entry_price=D("101.5"), stop_loss=D("103.25"),
        ordered_units=D("10"), remaining_units=D("10"), is_reduce_only=True, linked_position_id="P1",
        broker_updated_at_ms=1_700_000_000_000, broker_seq=7, source="REST", observed_seq=11, seq_id=12)
    base.update(kw)
    return OrderDTO(**base)


def position(**kw: Any) -> PositionDTO:
    base: dict[str, Any] = dict(
        position_id="P1", account_id="ACC1", adapter_id="mock", symbol="SYN.A", side="BUY", units=D("10"),
        entry_price=D("101.5"), status="OPEN", stop_loss=D("99.75"), opened_by_client_order_id="C0",
        broker_updated_at_ms=1_700_000_000_000, broker_seq=3, observed_seq=4, seq_id=5)
    base.update(kw)
    return PositionDTO(**base)


def account(**kw: Any) -> AccountDTO:
    base: dict[str, Any] = dict(account_id="ACC1", adapter_id="mock", currency="XXX", balance=D("1"),
                                broker_updated_at_ms=1)
    base.update(kw)
    return AccountDTO(**base)


def instrument(**kw: Any) -> InstrumentSpec:
    base: dict[str, Any] = dict(
        adapter_id="mock", symbol="SYN.A", base_ccy="AAA", quote_ccy="BBB", pnl_ccy="BBB", contract_size=D("1"),
        unit_kind="UNITS", point_size=D("0.5"), min_qty=D("1"), swap_booked_to_balance=False, broker_symbol="M1")
    base.update(kw)
    return InstrumentSpec(**base)


def cash_flow() -> CashFlowDTO:
    return CashFlowDTO(transaction_id="T1", account_id="ACC1", adapter_id="mock", kind="ADJUSTMENT", amount=D("1"),
                       currency="XXX", broker_ts=1)


GOLDEN = {
    "order": "f1f1a0b05b0a561e2c7598b45107223409f59d9c1fd9f871c06d71fc5539c4e5",
    "position": "950f47f62134b8542ecaa0835e8de4e6967a1b85d9dd23e76004409105db5c48",
    "account": "d7dfe270ca74ea6a40eed11aaf7a7e612808bb68c939ac3d570a658757fc59c8",
    "instrument": "d311e55a706f6bb697850088d4d883816e6cc400b7c322518723606ed6bfa84e",
    "cash_flow": "03a4844785fb49072b29ce990d975e149995adc7273369ecae30c140612c1da4",
}
BUILDERS: dict[str, Any] = {"order": order, "position": position, "account": account, "instrument": instrument,
                            "cash_flow": cash_flow}


@pytest.mark.parametrize("name", sorted(GOLDEN))
def test_golden_checksums_are_unchanged_with_the_new_fields_none(name: str) -> None:
    dto = BUILDERS[name]()
    assert payload_checksum(dto) == GOLDEN[name]
    if hasattr(dto, "checksum"):
        assert dto.checksum == GOLDEN[name]


def test_new_fields_default_to_none() -> None:
    o, p, a, i = order(), position(), account(), instrument()
    assert o.order_kind is None and i.price_increment is None and i.min_stop_distance is None
    assert i.asset_class is None
    for e in (o, p, a):
        assert e.origin is None and e.broker_time_source is None
    px = PriceDTO(adapter_id="mock", symbol="SYN.A", bid=D("1"), ask=D("2"), spread_pips=None, timestamp_ms=1)
    assert px.ts_source is None and px.received_ms is None


def test_field_sets() -> None:
    assert OPTIONAL_PAYLOAD_FIELDS == {"order_kind", "price_increment", "min_stop_distance", "asset_class"}
    assert {"origin", "broker_time_source", "source", "broker_seq"} <= NON_PAYLOAD_FIELDS
    assert not OPTIONAL_PAYLOAD_FIELDS & NON_PAYLOAD_FIELDS
    assert OMITTED_WHEN_NONE == NEW_FIELDS


@pytest.mark.parametrize("kind", ["STOP", "LIMIT"])
def test_a_set_order_kind_changes_the_checksum(kind: str) -> None:
    o = order(order_kind=kind)
    assert o.checksum != GOLDEN["order"] and o.checksum == payload_checksum(o)
    assert order(order_kind="STOP").checksum != order(order_kind="LIMIT").checksum
    # replace() re-seals: back to None gives the golden checksum again
    assert replace(o, order_kind=None).checksum == GOLDEN["order"]


@pytest.mark.parametrize("field,value", [("price_increment", D("0.25")), ("min_stop_distance", D("2")),
                                         ("asset_class", "SYNTHETIC")])
def test_a_set_instrument_field_changes_the_checksum(field: str, value: object) -> None:
    assert payload_checksum(instrument(**{field: value})) != GOLDEN["instrument"]


@pytest.mark.parametrize("name", ["order", "position", "account"])
def test_version_labels_never_change_the_checksum(name: str) -> None:
    dto = BUILDERS[name]()
    for origin in ("stream", "snapshot", "history", "dlq", "stop_read"):
        for bts in ("BROKER", "WATERMARK"):
            labelled = replace(dto, origin=origin, broker_time_source=bts)
            assert labelled.checksum == GOLDEN[name] == payload_checksum(labelled)


def _old_encoding(obj: Any) -> dict[str, Any]:
    """Every field as before S01 (the new fields did not exist)."""
    return {f.name: _plain(getattr(obj, f.name)) for f in fields(obj) if f.name not in NEW_FIELDS}


@pytest.mark.parametrize("name", sorted(GOLDEN))
def test_json_encodings_are_byte_identical_while_none(name: str) -> None:
    dto = BUILDERS[name]()
    assert dto_json(dto) == json.dumps(_old_encoding(dto), sort_keys=True, separators=(",", ":"))
    if name != "instrument":
        old = _old_encoding(dto) | {"_type": type(dto).__name__}
        raw = json.dumps(old, sort_keys=True, separators=(",", ":"))
        assert dlq_rows.dto_json(dto) == raw   # so the DLQ payload_hash is unchanged
        row = dlq_rows.dlq_row(dto, dlq_rows.SCHEMA_INVALID)
        assert row.payload_hash == hashlib.sha256(raw.encode("utf-8")).hexdigest()


def test_set_fields_are_encoded() -> None:
    o = order(order_kind="STOP", origin="stream", broker_time_source="BROKER")
    body = json.loads(dto_json(o))
    assert body["order_kind"] == "STOP" and body["origin"] == "stream" and body["broker_time_source"] == "BROKER"
    assert json.loads(dlq_rows.dto_json(o))["order_kind"] == "STOP"
    spec = json.loads(dto_json(instrument(price_increment=D("0.25"), asset_class="SYNTHETIC")))
    assert spec["price_increment"] == "0.25" and spec["asset_class"] == "SYNTHETIC"
    assert "min_stop_distance" not in spec
