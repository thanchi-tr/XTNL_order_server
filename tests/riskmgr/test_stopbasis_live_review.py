"""Live review, finding 3: an untimed (watermark-labelled) APPLIED version takes part in `S_at_T` only while it is older
by `seq` than the first counting version timed after `T`. One newer than such a version was read after it (a stale
REST read never overwrites a newer stream update), so it shows state from after `T`: a stop loosened after `T` must
not withhold a bar that met the trigger. The round-14 rule (an untimed loosened amend with no post-`T` counting
version still takes the farther level) is unchanged. Synthetic levels only.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.marketdata.ports import HandleRef
from src.riskmgr.stopbasis import StopAtT, StopUnavailable, StopVersion, stop_at_bar_end
from src.riskmgr.types import StopBasis

D = Decimal
E = D("1.20000")
T = 1_773_152_100_000
O1 = HandleRef("CHILD_ORDER", "O1")


def v(level: str, at: int | None, *, seq: int, origin: str = "stream") -> StopVersion:
    return StopVersion(handle=O1, level=D(level), units=D("1000"), order_kind="STOP",
                       origin=origin, broker_time_source="BROKER" if at is not None else None,  # type: ignore[arg-type]
                       broker_time_ms=at, seq=seq)


def at_t(versions: list[StopVersion], side: str) -> StopAtT | StopUnavailable:
    return stop_at_bar_end(versions, side=side, entry=E, units_to_cover=D("1000"), bar_end_ms=T)  # type: ignore[arg-type]


def lv(sign: int, x: str) -> str:
    return str(E + sign * D(x))


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_an_untimed_row_after_a_post_t_stream_version_takes_no_part(sign: int, side: str) -> None:
    """v1 stream at T-5min (-0.00050), v2 stream at T+60s loosened (-0.00200), v3 an untimed snapshot after v2
    (an even farther level, so it is a real APPLIED version, not a no-op). `S_at_T` is v1's level, basis STREAM."""
    v1 = v(lv(sign, "-0.00050"), T - 300_000, seq=1)
    v2 = v(lv(sign, "-0.00200"), T + 60_000, seq=2)
    v3 = v(lv(sign, "-0.00250"), None, seq=3, origin="snapshot")
    before = at_t([v1, v2], side)
    assert isinstance(before, StopAtT) and (before.level, before.basis) == (D(lv(sign, "-0.00050")), StopBasis.STREAM)
    got = at_t([v1, v2, v3], side)
    assert isinstance(got, StopAtT)
    assert (got.level, got.basis) == (D(lv(sign, "-0.00050")), StopBasis.STREAM)


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_an_untimed_row_between_t_and_the_post_t_version_still_takes_part(sign: int, side: str) -> None:
    """An untimed row older by seq than the first post-T counting version may show the level at T: the farther
    level is still used, basis PRIOR_VERSION (round 14)."""
    v1 = v(lv(sign, "-0.00050"), T - 300_000, seq=1)
    mid = v(lv(sign, "-0.00150"), None, seq=2, origin="snapshot")
    v2 = v(lv(sign, "-0.00200"), T + 60_000, seq=3)
    got = at_t([v1, mid, v2], side)
    assert isinstance(got, StopAtT)
    assert (got.level, got.basis) == (D(lv(sign, "-0.00150")), StopBasis.PRIOR_VERSION)


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_round_14_untimed_loosen_without_a_post_t_version_is_unchanged(sign: int, side: str) -> None:
    v1 = v(lv(sign, "-0.00100"), T - 300_000, seq=1)
    v2 = v(lv(sign, "-0.00200"), None, seq=2, origin="snapshot")
    got = at_t([v1, v2], side)
    assert isinstance(got, StopAtT)
    assert (got.level, got.basis) == (D(lv(sign, "-0.00200")), StopBasis.PRIOR_VERSION)


@pytest.mark.parametrize("sign,side", [(1, "BUY"), (-1, "SELL")])
def test_a_post_t_stop_read_also_bounds_the_untimed_rows(sign: int, side: str) -> None:
    v1 = v(lv(sign, "-0.00050"), T - 300_000, seq=1)
    read = v(lv(sign, "-0.00200"), T + 30_000, seq=2, origin="stop_read")
    late = v(lv(sign, "-0.00250"), None, seq=3, origin="snapshot")
    got = at_t([v1, read, late], side)
    assert isinstance(got, StopAtT) and got.level == D(lv(sign, "-0.00050"))
