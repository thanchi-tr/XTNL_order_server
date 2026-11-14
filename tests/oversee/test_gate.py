"""ActionGate: the canonical fail-closed list (oversee-rules.md §8)."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

import pytest

from src.oversee.gate import Action, ActionGate, GateContext, evaluate_gate
from src.oversee.types import HaltLevel

NOW = 1_000_000
GOOD = GateContext(
    now_ms=NOW, redis_boot_id="run1:7", redis_run_id="run1", node_epoch=7, engine_epoch=7, lease_valid=True,
    halt_state=(HaltLevel.NONE, 3), max_halt_event_id=3, settle_until_ms=NOW - 1, reconciled_at_ms=NOW - 500,
    reconciled_epoch=7, latest_open_ms=NOW - 1000, open_stream_health=False, stale_mark_symbols=frozenset(),
    has_unknown_result=False, last_pass_ms=NOW - 1000, catchup_verified=True,
)
INC = Action("A1", "stonex", risk_increasing=True, symbol="EURUSD")
RED = Action("A1", "stonex", risk_increasing=False, symbol="EURUSD")


class Reader:
    def __init__(self, ctx: GateContext | Exception, delay: float = 0) -> None:
        self.ctx, self.delay = ctx, delay

    async def read(self, action: Action) -> GateContext:
        if self.delay:
            await asyncio.sleep(self.delay)
        if isinstance(self.ctx, Exception):
            raise self.ctx
        return self.ctx


async def test_all_conditions_hold_allows() -> None:
    assert await ActionGate(Reader(GOOD)).allow(INC)
    assert await ActionGate(Reader(GOOD)).allow(RED)


DENIALS: list[tuple[dict[str, Any], str]] = [
    (dict(redis_boot_id=None), "boot_id_missing"),
    (dict(redis_boot_id="run0:7"), "boot_id_mismatch"),
    (dict(redis_boot_id="run1:6"), "boot_id_mismatch"),
    (dict(engine_epoch=8, redis_boot_id="run1:7"), "epoch_mismatch"),
    (dict(lease_valid=False), "lease_invalid"),
    (dict(halt_state=None), "halt_state_missing"),
    (dict(max_halt_event_id=4), "halt_state_not_current"),
    (dict(max_halt_event_id=None), "halt_state_not_current"),
    (dict(halt_state=(HaltLevel.HARD, 3)), "hard_halt"),
    (dict(halt_state=(HaltLevel.SOFT, 3)), "soft_halt_risk_increasing"),
    (dict(settle_until_ms=NOW + 1), "settle_window"),
    (dict(reconciled_at_ms=None), "not_reconciled"),
    (dict(reconciled_epoch=6), "not_reconciled"),
    (dict(latest_open_ms=NOW - 100), "not_reconciled"),
    (dict(latest_open_ms=None), "latest_open_unknown"),     # no SM10 calendar / no session: fail closed
    (dict(open_stream_health=True), "stream_health_open"),
    (dict(catchup_verified=False), "catchup_unverified"),
    (dict(stale_mark_symbols=frozenset({"EURUSD"})), "stale_mark"),
    (dict(has_unknown_result=True), "unknown_result"),
    (dict(last_pass_ms=None), "sweep_not_current"),
    (dict(last_pass_ms=NOW - 3000), "sweep_not_current"),
]


@pytest.mark.parametrize(("change", "reason"), DENIALS)
async def test_each_canonical_condition_denies(change: dict[str, Any], reason: str) -> None:
    verdict = await ActionGate(Reader(replace(GOOD, **change))).check(INC)
    assert not verdict.allowed and verdict.reason == reason


def test_soft_allows_risk_reducing_only_and_hard_denies_everything() -> None:
    soft = replace(GOOD, halt_state=(HaltLevel.SOFT, 3), open_stream_health=True, last_pass_ms=None)
    assert evaluate_gate(soft, RED).allowed and not evaluate_gate(soft, INC).allowed
    hard = replace(GOOD, halt_state=(HaltLevel.HARD, 3))
    assert not evaluate_gate(hard, RED).allowed


async def test_reader_error_timeout_or_no_reader_denies() -> None:
    assert not await ActionGate(Reader(ConnectionError("redis down"))).allow(RED)
    assert not await ActionGate(Reader(GOOD, delay=0.2), timeout_s=0.01).allow(RED)
    assert not await ActionGate(None).allow(RED)
