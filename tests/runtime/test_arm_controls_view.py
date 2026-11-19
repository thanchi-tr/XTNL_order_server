"""The engine side of the operator controls (plan S34; the S33 hand-off): `ControlsView` reads `arm_controls` on the
ARM pool, its operator pause is ORed into the `arm_status` row and its drain flag (only while paused) is the
arm-relay's `drain_notices`; unreadable controls are never a pause."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from src.db.arm_ports import ControlsRow
from src.db.fence import EpochMismatch
from src.runtime.optional_plan import DRAIN_STALE_S, OPERATOR_PAUSE, ControlsView, OptionalRuntime


def controls(*, paused: bool, drain: bool = False, reason: str | None = "operator") -> ControlsRow:
    return ControlsRow(paused=paused, pause_reason=reason if paused else None, paused_at_ms=1 if paused else None,
                       drain_notices=drain, notify_test_at_ms=None, updated_at_ms=1)


class Store:
    def __init__(self) -> None:
        self.row: ControlsRow | None = controls(paused=False)
        self.error: BaseException | None = None

    async def controls(self) -> ControlsRow | None:
        if self.error is not None:
            raise self.error
        return self.row


async def test_the_view_follows_the_row_and_keeps_the_last_read_when_unreadable() -> None:
    view, store = ControlsView(), Store()
    assert view.pause_reason() is None and not view.drain()              # before any read: nothing paused
    store.row = controls(paused=True, drain=True)
    await view.refresh(store, 1.0)  # type: ignore[arg-type]
    assert view.pause_reason() == "operator" and view.drain() and view.reads == 1
    store.error = ConnectionResetError("pool down")
    await view.refresh(store, 1.0)  # type: ignore[arg-type]
    assert view.read_errors == 1 and view.pause_reason() == "operator"   # unreadable: last read kept
    store.error, store.row = None, None
    await view.refresh(store, 1.0)  # type: ignore[arg-type]
    assert view.read_errors == 2 and view.pause_reason() == "operator"   # a missing row is unreadable, not a resume
    store.row = controls(paused=False, drain=True)
    await view.refresh(store, 1.0)  # type: ignore[arg-type]
    assert view.pause_reason() is None and not view.drain()              # drain only while paused
    store.row = controls(paused=True, reason=None)
    await view.refresh(store, 1.0)  # type: ignore[arg-type]
    assert view.pause_reason() == OPERATOR_PAUSE
    store.error = EpochMismatch("fenced")
    with pytest.raises(EpochMismatch):
        await view.refresh(store, 1.0)  # type: ignore[arg-type]


async def test_a_stale_drain_is_no_drain_but_the_pause_holds() -> None:
    """Draining is destructive (a `suppressed` notice is never sent): after a failed read the drain lapses once the
    last good read is older than DRAIN_STALE_S, while the pause keeps the last read."""
    now = [100.0]
    view, store = ControlsView(clock=lambda: now[0]), Store()
    store.row = controls(paused=True, drain=True)
    await view.refresh(store, 1.0)  # type: ignore[arg-type]
    assert view.drain()
    store.error = ConnectionResetError("pool down")
    now[0] += 5.0
    await view.refresh(store, 1.0)  # type: ignore[arg-type]
    assert view.drain() and view.read_errors == 1                        # within the bound: the last read holds
    now[0] = 100.0 + DRAIN_STALE_S + 0.1
    await view.refresh(store, 1.0)  # type: ignore[arg-type]
    assert not view.drain() and view.pause_reason() == "operator"        # stale: no drain, still paused
    store.error = None
    await view.refresh(store, 1.0)  # type: ignore[arg-type]
    assert view.drain()                                                  # a fresh read restores it


def _rt(view: ControlsView, config: Any = None) -> Any:
    graph = None if config is None else SimpleNamespace(
        failed=SimpleNamespace(summary=lambda: SimpleNamespace(count=0, global_failed=False), pending=lambda: ()),
        budget=SimpleNamespace(snapshot=lambda: ()), tick_errors=0, config=config, decide_holds=0, notice_errors=0,
        clock=None, tracker=None, notices=None, evaluator=None, started=False)
    session = SimpleNamespace(ctx=SimpleNamespace(clock_ms=lambda: 1_000), node_id="node-a",
                              epoch=SimpleNamespace(token=7))
    rt = SimpleNamespace(session=session, graph=graph, controls=view, liveness=SimpleNamespace(pages=0),
                         liveness_errors=0, suspend_latch=SimpleNamespace(notes=0))
    rt.pause_reason = lambda: OptionalRuntime.pause_reason(rt)  # type: ignore[arg-type]
    return rt


async def test_the_status_row_ors_the_operator_pause_with_the_config_pause() -> None:
    view, store = ControlsView(), Store()
    config = SimpleNamespace(pause_reason=lambda: "config_hash", snapshot=lambda: (), installed_hash=lambda: None)
    idle = SimpleNamespace(pause_reason=lambda: None, snapshot=lambda: (), installed_hash=lambda: None)
    row = OptionalRuntime.status_row(_rt(view, idle))
    assert not row.paused and row.pause_reason is None
    row = OptionalRuntime.status_row(_rt(view, config))
    assert row.paused and row.pause_reason == "config_hash"
    store.row = controls(paused=True, drain=True)
    await view.refresh(store, 1.0)  # type: ignore[arg-type]
    row = OptionalRuntime.status_row(_rt(view, config))
    assert row.paused and row.pause_reason == "operator"                 # the operator's reason first
    assert ("controls_drain", 1) in row.counters and ("controls_reads", 1) in row.counters
    row = OptionalRuntime.status_row(_rt(view, idle))
    assert row.paused and row.pause_reason == "operator"
    broken = SimpleNamespace(pause_reason=boom, snapshot=lambda: (), installed_hash=lambda: None)
    store.row = controls(paused=False, drain=False)
    await view.refresh(store, 1.0)  # type: ignore[arg-type]
    row = OptionalRuntime.status_row(_rt(view, broken))                  # S36b: a raising installer is a pause
    assert row.paused and row.pause_reason == "config_unreadable" and not row.running


def boom() -> str | None:
    raise RuntimeError("boom")
