"""Live review 2026-11-25: `slow_pass` pages on every full pass longer than the interval, latched until a fast pass
(oversee-rules.md §2), and full passes are scheduled and timed on the monotonic clock."""
from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from dataclasses import replace

from src.oversee.config import OverseeConfig, SweepConfig
from src.oversee.halt import HaltSwitch
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.rules import RuleRegistry
from src.oversee.sweep import PassReport, RiskSweep
from src.oversee.types import Page

T0 = 1_763_000_000_000


class Wall:
    def __init__(self, t: int = T0) -> None:
        self.t = t

    def __call__(self) -> int:
        return self.t


def make(cfg: OverseeConfig | None = None, wall: Wall | None = None,
         mono: object = time.monotonic) -> tuple[RiskSweep, list[Page]]:
    cfg = cfg or OverseeConfig()
    db = InMemoryOverseeDb()
    pages: list[Page] = []
    sweep = RiskSweep(reader=db, writer=db, registry=RuleRegistry.from_config(cfg), halt=HaltSwitch(db, db),
                      config=cfg, clock=wall or Wall(), epoch=lambda: 1, pager=pages.append,
                      monotonic=mono)  # type: ignore[arg-type]
    return sweep, pages


def slow_pages(pages: list[Page]) -> list[Page]:
    return [p for p in pages if p.code == "slow_pass"]


def feed(sweep: RiskSweep, passes: Sequence[tuple[int, int]]) -> None:
    """(monotonic end ms, duration ms) of full passes."""
    for end, dur in passes:
        sweep._check_pass_time(dur, end)


# ---------------------------------------------------------------- slow_pass paging (oversee-rules.md §2)

def test_any_pass_longer_than_the_interval_pages() -> None:
    for dur in (1001, 1500, 2391, 3200):
        sweep, pages = make()
        feed(sweep, [(1000, 30), (5000, dur)])
        (page,) = slow_pages(pages)
        assert page.severity == "high" and page.detail == f"{dur} ms" and page.at_ms == T0


def test_a_pass_exactly_at_the_interval_does_not_page() -> None:
    sweep, pages = make()
    feed(sweep, [(1000, 1000)])
    assert slow_pages(pages) == []


def test_slow_pass_page_is_latched_until_a_fast_pass() -> None:
    sweep, pages = make()
    feed(sweep, [(1100, 1100), (2200, 1100), (3300, 1100)])
    assert len(slow_pages(pages)) == 1
    feed(sweep, [(3400, 30), (4600, 1200)])          # a fast pass re-arms it
    assert len(slow_pages(pages)) == 2


def test_a_lasting_condition_re_pages_every_escalate_after_ms() -> None:
    wall = Wall()
    cfg = replace(OverseeConfig(), sweep=replace(SweepConfig(), escalate_after_ms=10_000))
    sweep, pages = make(cfg, wall)
    for i in range(1, 25):     # a 1.1 s pass every 1.1 s for 26 s
        wall.t = T0 + 1100 * i
        feed(sweep, [(1100 * i, 1100)])
    pages_at = [p.at_ms for p in slow_pages(pages)]
    assert pages_at[0] == T0 + 1100 and len(pages_at) == 3


# ---------------------------------------------------------------- finding 3: monotonic timing

class Mono:
    """time.monotonic plus a test-controlled offset (seconds)."""

    def __init__(self) -> None:
        self.offset = 0.0

    def __call__(self) -> float:
        return time.monotonic() + self.offset


def fake_passes(sweep: RiskSweep, step: list[tuple[int, float]], stop_after: int,
                starts: list[float]) -> None:
    """Replace run_pass: each full pass moves the wall clock by step[i][0] ms and the monotonic clock by
    step[i][1] s, then the loop stops after `stop_after` full passes."""
    wall = sweep._clock
    mono = sweep._mono
    assert isinstance(wall, Wall) and isinstance(mono, Mono)

    async def run_pass(accounts: Sequence[str] | None = None) -> PassReport:
        if accounts is None:
            starts.append(time.monotonic())
            i = min(len(starts) - 1, len(step) - 1)
            wall.t += step[i][0]
            mono.offset += step[i][1]
            if len(starts) >= stop_after:
                sweep.stop()
        await asyncio.sleep(0)
        return PassReport(1, [], [], [])

    sweep.run_pass = run_pass  # type: ignore[method-assign]


def fast_cfg() -> OverseeConfig:
    return replace(OverseeConfig(), sweep=replace(SweepConfig(), interval_ms=50))


async def test_a_wall_clock_jump_during_a_fast_pass_does_not_page() -> None:
    wall, mono = Wall(), Mono()
    sweep, pages = make(fast_cfg(), wall, mono)
    starts: list[float] = []
    fake_passes(sweep, [(5000, 0.010)], 3, starts)
    await asyncio.wait_for(sweep.run(), 2.0)
    assert len(starts) == 3 and slow_pages(pages) == []


async def test_a_slow_pass_on_the_monotonic_clock_pages_with_wall_time() -> None:
    wall, mono = Wall(), Mono()
    sweep, pages = make(fast_cfg(), wall, mono)
    starts: list[float] = []
    fake_passes(sweep, [(-60_000, 0.200)], 1, starts)   # wall steps back; the pass takes 4 x interval
    await asyncio.wait_for(sweep.run(), 2.0)
    (page,) = slow_pages(pages)
    assert page.at_ms == T0 - 60_000 and page.detail.startswith("200 ms")


async def test_a_wall_clock_step_back_does_not_delay_the_next_full_pass() -> None:
    wall, mono = Wall(), Mono()
    sweep, _ = make(fast_cfg(), wall, mono)
    starts: list[float] = []
    fake_passes(sweep, [(-3_600_000, 0.001), (0, 0.001)], 3, starts)
    await asyncio.wait_for(sweep.run(), 2.0)
    assert len(starts) == 3 and starts[-1] - starts[0] < 1.0
