"""Live review round 4: the pre-guards (1)-(3b) at PRE, never run by the suite before.

Design §B6.3 pre-guards: (1) account image fresh, else `DATA_UNAVAILABLE(<first image reason>)`; (2) under PR9 the
oldest handle read no older than 30 s at PRE, else `DATA_UNAVAILABLE(stop_stale)`; (3) an effective stop, else
`NO_STOP`; (3b) the price increment known, else `DATA_UNAVAILABLE(increment_unknown)`. At PRE each is a recorded,
non-final row with no targets and the state unchanged. Tests only; synthetic levels.
"""
from __future__ import annotations

from dataclasses import replace

import pytest

from src.marketdata.types import M15
from src.riskmgr.outcomes import Outcome
from src.riskmgr.outcomes import STOP_STALE
from src.riskmgr.pipeline import PipelineInput, PipelineResult, ResultKind, pre_window, run_pipeline
from src.riskmgr.registry import REGISTRY
from src.riskmgr.sm11 import ArmState
from src.riskmgr.types import ArmEvent, BarContext
from tests.riskmgr.kind_a import s_ctx
from tests.riskmgr.test_pipeline import FRESH, OPEN, SKEW, lv, params, quote


def _pre(ctx: BarContext, *, stop_reads: bool = False, now_delta: int = 0) -> PipelineResult:
    win = pre_window(ctx.bar_end_ms, M15, SKEW, 40)
    c = replace(ctx, quote=quote(lv(1, "0.0150"), win.fire_ms - 100), exit_quote_live=True)
    inp = PipelineInput(params=params(), ctx=c, state=FRESH, arm_state=ArmState.TRACKING,
                        now_ms=win.fire_ms + now_delta, calendar=OPEN, window=win, stop_reads=stop_reads)
    return run_pipeline(inp, REGISTRY)


@pytest.mark.parametrize("mut,outcome,reason", [
    (dict(account_image_fresh=False, image_reasons=("orders_fence",)), Outcome.DATA_UNAVAILABLE, "orders_fence"),
    (dict(price_increment=None), Outcome.DATA_UNAVAILABLE, "increment_unknown"),
    (dict(handles=(), stop=None), Outcome.NO_STOP, ""),
])
def test_pre_pre_guard_faults_are_rows(mut: dict[str, object], outcome: Outcome, reason: str) -> None:
    ctx = replace(s_ctx(lv(1, "0.0150"), event=ArmEvent.PRE), **mut)  # type: ignore[arg-type]
    r = _pre(ctx)
    assert r.kind is ResultKind.DECISION and r.outcome is outcome
    assert r.code is not None and (r.code.reason or "") == reason
    assert r.targets == () and r.state == FRESH and not r.final


def test_pre_stop_read_older_than_30s_is_stop_stale() -> None:
    ctx = s_ctx(lv(1, "0.0150"), event=ArmEvent.PRE)
    win = pre_window(ctx.bar_end_ms, M15, SKEW, 40)
    old = tuple(replace(h, read_ms=win.fire_ms - 31_000) for h in ctx.handles)
    fresh = tuple(replace(h, read_ms=win.fire_ms - 29_000) for h in ctx.handles)
    r = _pre(replace(ctx, handles=old), stop_reads=True)
    assert r.outcome is Outcome.DATA_UNAVAILABLE and r.code is not None and r.code.reason == STOP_STALE
    assert r.targets == () and not r.final
    assert _pre(replace(ctx, handles=fresh), stop_reads=True).outcome is Outcome.ADJUST
