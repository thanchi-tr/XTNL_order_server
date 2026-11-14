from __future__ import annotations

from src.pubsub.hlc import LOGICAL_BITS, HybridLogicalClock
from tests.pubsub.conftest import ManualClock


def test_monotonic_with_frozen_clock() -> None:
    clk = ManualClock(1000)
    hlc = HybridLogicalClock(clk)
    a, b, c = hlc.next(), hlc.next(), hlc.next()
    assert a == 1000 << LOGICAL_BITS
    assert (b, c) == (a + 1, a + 2)
    assert HybridLogicalClock.physical_ms(c) == 1000
    assert HybridLogicalClock.logical(c) == 2


def test_jumps_to_physical_time_when_clock_advances() -> None:
    clk = ManualClock(1000)
    hlc = HybridLogicalClock(clk)
    hlc.next()
    clk.ms = 2000
    assert hlc.next() == 2000 << LOGICAL_BITS


def test_clock_going_backwards_stays_monotonic() -> None:
    clk = ManualClock(5000)
    hlc = HybridLogicalClock(clk)
    a = hlc.next()
    clk.ms = 1000
    assert hlc.next() == a + 1


def test_seed_raises_floor_and_never_lowers() -> None:
    clk = ManualClock(1000)
    hlc = HybridLogicalClock(clk)
    db_max = (9000 << LOGICAL_BITS) + 7
    hlc.seed(db_max)
    assert hlc.next() == db_max + 1
    hlc.seed(5)
    assert hlc.next() == db_max + 2
    hlc.reseed(db_max + 100)          # after an epoch bump
    assert hlc.next() == db_max + 101
    assert hlc.last == db_max + 101


def test_constructor_seed() -> None:
    hlc = HybridLogicalClock(ManualClock(0), seed=42)
    assert hlc.next() == 43


def test_many_values_strictly_increasing() -> None:
    hlc = HybridLogicalClock()
    vals = [hlc.next() for _ in range(10_000)]
    assert all(b > a for a, b in zip(vals, vals[1:]))
