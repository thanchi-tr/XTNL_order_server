"""S39 review (major): the runtime arm-tracker's `bar_sides` and admission / `arm config accept` read one bar-capable
set, `src.riskmgr.golive.bar_capable_adapters()`. While a gate S39 waits for is OPEN in the gate table it is empty,
so an accepted assignment with **no adapter** (a wildcard) never arms a StoneX position, even though StoneX
`price_bars_supported` is now True and every open gate is accepted in `XTNL_ARM_GOLIVE_ACCEPT` (an env acceptance is
not an S39 acceptance). No network, no values."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from src.ops import arm as ops_arm
from src.riskmgr import arming as A
from src.riskmgr import golive
from src.runtime.arm_ports import CalendarGrid
from src.runtime.optional_plan import OptionalRuntime
from tests.riskmgr.tracker_fakes import assignment, document, image, instrument, position, rig

STONEX = "stonex"


def runtime_with_bar_adapters() -> Any:
    """A stand-in for `OptionalRuntime` whose every adapter reports `price_bars_supported` True (StoneX since S39)."""
    adapter = SimpleNamespace(price_bars_supported=True)
    orchestrator = SimpleNamespace(runtime=lambda _adapter_id: SimpleNamespace(adapter=adapter))
    return SimpleNamespace(session=SimpleNamespace(orchestrator=orchestrator))


def bars_capable(adapter_id: str) -> bool:
    return OptionalRuntime._bars_capable(runtime_with_bar_adapters(), adapter_id)  # noqa: SLF001 (the unit under test)


def accept_every_open_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    ids = ",".join(g.gate_id for g in golive.GATES if g.state is golive.GateState.OPEN)
    monkeypatch.setenv(golive.ACCEPT_VAR, ids)
    monkeypatch.setenv(golive.STOP_AT_ENTRY_VAR, "1")


def stonex_wildcard_rig(grid: CalendarGrid) -> Any:
    r = rig(doc=document(assignments=(assignment(adapter=None),)))          # no adapter: matches every adapter
    r.grid.bar_sides = grid.bar_sides  # type: ignore[method-assign]
    pos = replace(position("P1"), adapter_id=STONEX)
    img = image(pos, instruments=(replace(instrument(), adapter_id=STONEX),))
    r.snapshot.images = [replace(img, adapter_id=STONEX)]
    return r


def test_wildcard_assignment_never_arms_stonex_while_s39_gates_are_open(monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    accept_every_open_gate(monkeypatch)
    assert golive.verdict(os.environ).allowed                     # XTNL_ARM_ENABLED=1 would start the ARM
    assert golive.s39_blockers()                                  # but S39's gates are open in the table
    assert golive.bar_capable_adapters() == frozenset() == ops_arm.BAR_CAPABLE_ADAPTERS
    assert not bars_capable(STONEX)
    grid = CalendarGrid(schedule_of=lambda _a, _s: None, bars_capable=bars_capable)
    assert grid.bar_sides(STONEX) == frozenset()
    r = stonex_wildcard_rig(grid)
    asyncio.run(r.tracker.step())
    assert not r.store.rows
    assert r.tracker.unmanaged() == ((f"{STONEX}:P1", "UNSUPPORTED", A.NO_BROKER_BARS),)


def test_the_same_position_arms_once_no_s39_gate_is_open(monkeypatch: pytest.MonkeyPatch) -> None:
    """The control: with the table's S39 blockers closed, StoneX joins the one set and the position arms."""
    monkeypatch.setattr(golive, "s39_blockers", lambda gates=golive.GATES: ())
    assert golive.bar_capable_adapters() == golive.BROKER_BAR_ADAPTERS
    assert bars_capable(STONEX) and not bars_capable("pepperstone")
    grid = CalendarGrid(schedule_of=lambda _a, _s: None, bars_capable=bars_capable)
    r = stonex_wildcard_rig(grid)
    asyncio.run(r.tracker.step())
    assert {k.position_id for k in r.store.rows} == {"P1"}
    assert not r.tracker.unmanaged()


def test_ops_reuses_the_golive_derivation() -> None:
    assert ops_arm.bar_capable_adapters is golive.bar_capable_adapters
    assert ops_arm.BROKER_BAR_ADAPTERS is golive.BROKER_BAR_ADAPTERS
