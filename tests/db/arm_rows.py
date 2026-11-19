"""Synthetic ARM rows for the S24 store tests (synthetic instruments and levels; no account values)."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any

from src.db.arm_ports import (
    ArmPositionRow,
    DecisionHandleRow,
    DecisionRow,
    FailedPing,
    FailureRecord,
    HandleVersionRow,
    PositionKey,
)
from src.marketdata.ports import HandleRef
from src.riskmgr.stopbasis import StopVersion

KEY = PositionKey(adapter_id="syn", position_id="P1")
HASH16 = "0123456789abcdef"
DOC_HASH = "a" * 64


def position_row(**kw: Any) -> ArmPositionRow:
    base = ArmPositionRow(
        key=KEY, account_id="ACC1", symbol="SYN1", side="BUY", state="ARMED", edge_code="edge_a",
        attribution_source="SINGLE_EDGE_DEFAULT", strategy_code="be_a", strategy_version=1,
        strategy_kind="break_even_on_risk_close", strategy_impl_version=1, strategy_params_json='{"k": "1"}',
        guards_hash=HASH16, bar_spec="15m", config_doc_hash=DOC_HASH, entry_level=Decimal("10.0"),
        entry_time_ms=1_000, entry_time_source="STREAM", s0_level=Decimal("9.0"), s0_source="AT_FILL",
        pinned_units=Decimal("2"), armed_at_ms=1_000, updated_at_ms=1_000)
    return replace(base, **kw)


def decision_row(**kw: Any) -> DecisionRow:
    base = DecisionRow(
        key=KEY, bar_end_ms=900_000, bar_spec="15m", event="POST", decision_seq=1, decided_at_ms=901_000,
        edge_code="edge_a", outcome="ADJUST", category="ACTIONABLE", trigger_met=True, advice=True,
        proposed_stop=Decimal("10.0"), current_risk_r=Decimal("1.00"), close_source="BROKER", stop_basis="STREAM",
        series_inputs=("s1|900000|01" + "f" * 32,), inputs_digest="01" + "e" * 32, envelope_version=1,
        envelope_params_hash=HASH16, strategy_kind="break_even_on_risk_close", strategy_impl_version=1,
        guards_hash=HASH16)
    return replace(base, **kw)


def handle_row(**kw: Any) -> DecisionHandleRow:
    base = DecisionHandleRow(handle_id="P1", handle_kind="POSITION_FIELD", handle_role="POSITION_FIELD",
                             level_at_bar_end=Decimal("9.0"), target_level=Decimal("10.0"))
    return replace(base, **kw)


def failure(**kw: Any) -> FailureRecord:
    base = FailureRecord(failure_id="f-1", scope_kind="SERIES", scope_id="syn:SYN1:15m:bid", reason="invalid_bar",
                         probe_kind="series_reread", ping_id="ping-1", first_fault_at_ms=1_000, entered_at_ms=2_000,
                         counters=(("invalid_buckets", 3),), position_ids=("syn:P1",))
    return replace(base, **kw)


def ping(ping_id: str = "ping-1", body: str = "scope SERIES syn:SYN1:15m:bid; reason invalid_bar; positions syn:P1",
         created_ms: int = 2_000) -> FailedPing:
    return FailedPing(title=f"ARM FAILED [{ping_id}] SERIES syn:SYN1:15m:bid", body=body, created_ms=created_ms)


def to_stop_version(v: HandleVersionRow) -> StopVersion:
    """The riskmgr's `StopVersion` of a stored version (the S27 mapping, kept here for the golden tests)."""
    return StopVersion(handle=HandleRef(kind=v.handle_kind, id=v.handle_id),  # type: ignore[arg-type]
                       level=v.level, live=v.live, units=v.units, order_kind=v.order_kind, origin=v.origin,
                       broker_time_source="BROKER" if v.broker_time_ms is not None else None,
                       broker_time_ms=v.broker_time_ms, seq=v.seq_id)
