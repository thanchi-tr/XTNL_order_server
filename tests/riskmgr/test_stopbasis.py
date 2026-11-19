"""`S_at_T`, handle roles and `PreBasis` (plan S21; design §B6.4, §B6.3 (7a), PR8, R11-M04, R11-M06).

Golden: a stop amended and then cancelled while the engine was off. StoneX history keeps only the last level, stamped
at placement, so its version would show at `T` a level the stop did not have then; only counting versions (APPLIED,
origin stream / stop_read / snapshot / dlq, real broker time) give `S_at_T`. Synthetic levels only.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.marketdata.ports import HandleRef
from src.riskmgr.outcomes import STOP_AT_BAR_END
from src.riskmgr.stopbasis import (
    STOP_CHANGED_AFTER_BAR_END,
    PreBasis,
    StopAtT,
    StopUnavailable,
    StopVersion,
    VersionStatus,
    farther_basis,
    farther_from_entry,
    handle_at_bar_end,
    handle_roles,
    stop_at_bar_end,
)
from src.riskmgr.types import StopBasis

D = Decimal
E = D("1.20000")
T = 1_773_152_100_000   # a 15m bar end
O1 = HandleRef("CHILD_ORDER", "O1")
O2 = HandleRef("CHILD_ORDER", "O2")
O7 = HandleRef("CHILD_ORDER", "O7")
PF = HandleRef("POSITION_FIELD", "P1")


def v(handle: HandleRef, level: str | None, at: int | None, *, origin: str = "stream", seq: int = 0,
      live: bool = True, units: str = "1000", source: str = "BROKER", kind: str | None = "STOP",
      status: VersionStatus = VersionStatus.APPLIED) -> StopVersion:
    return StopVersion(handle=handle, level=None if level is None else D(level), live=live, units=D(units),
                       order_kind=kind, origin=origin, broker_time_source=source,   # type: ignore[arg-type]
                       broker_time_ms=at, status=status, seq=seq)


def s_at_t(versions: list[StopVersion], side: str = "BUY", units: str = "1000") -> StopAtT | StopUnavailable:
    return stop_at_bar_end(versions, side=side, entry=E, units_to_cover=D(units), bar_end_ms=T)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------- golden R11-M06
def test_golden_amended_then_cancelled_while_off() -> None:
    """Placed at t0 with 1.19900 (stream), engine off before `T`; while off the stop was amended to 1.19950 after `T`
    and then cancelled. On restart history reports the cancelled order with the last level stamped at placement.
    Counting history would give 1.19950 (or no stop) at `T`; the history version is excluded, so the newest earlier
    counting version (the stream placement) applies: 1.19900, basis PRIOR_VERSION."""
    t0 = T - 40 * 60_000
    versions = [
        v(O1, "1.19900", t0, origin="stream", seq=10),
        v(O1, "1.19950", t0, origin="history", seq=50, live=False),   # last level, stamped at placement, cancelled
    ]
    got = s_at_t(versions)
    assert isinstance(got, StopAtT)
    assert got.level == D("1.19900") and got.basis is StopBasis.PRIOR_VERSION
    assert [h.handle for h in got.handles] == [O1]


def test_only_a_history_candidate_is_unavailable() -> None:
    """A stop placed, amended and cancelled entirely while off: only the history version exists."""
    got = s_at_t([v(O1, "1.19950", T - 60_000, origin="history", live=False)])
    assert got == StopUnavailable(STOP_AT_BAR_END)


@pytest.mark.parametrize("origin,basis", [("stream", StopBasis.STREAM), ("stop_read", StopBasis.READ),
                                          ("snapshot", StopBasis.READ), ("dlq", StopBasis.READ)])
def test_counting_origins(origin: str, basis: StopBasis) -> None:
    got = s_at_t([v(O1, "1.19900", T - 1, origin=origin)])
    assert isinstance(got, StopAtT) and got.basis is basis and got.level == D("1.19900")


def test_watermark_dropped_stale_and_after_t_never_count() -> None:
    """`event_log` stores a watermark-labelled version with `broker_time_ms` NULL (repository `version_labels`): newer
    by seq than the newest counting version, it is a non-counting newest candidate (PRIOR_VERSION)."""
    base = v(O1, "1.19900", T - 600_000, seq=1)
    wm = v(O1, "1.19990", None, origin="snapshot", source="WATERMARK", seq=2)
    dropped = v(O1, "1.19980", T - 500, seq=3, status=VersionStatus.DROPPED_STALE)
    later = v(O1, "1.20000", T + 1, seq=4)
    got = s_at_t([base, wm, dropped, later])
    assert isinstance(got, StopAtT)
    assert got.level == D("1.19900") and got.basis is StopBasis.PRIOR_VERSION   # the watermark row was newest
    got2 = s_at_t([base, dropped, later])
    assert isinstance(got2, StopAtT) and got2.basis is StopBasis.STREAM and got2.level == D("1.19900")


def test_watermark_older_than_the_counting_version_is_superseded() -> None:
    wm = v(O1, "1.19990", None, origin="snapshot", source="WATERMARK", seq=1)
    newer = v(O1, "1.19900", T - 60_000, seq=2)
    got = s_at_t([wm, newer])
    assert isinstance(got, StopAtT) and got.basis is StopBasis.STREAM and got.level == D("1.19900")


def test_watermark_only_handle_is_unresolved_not_absent() -> None:
    """A STOP child known only through a watermark read could be the most distant child at `T`: dropping it would
    give a smaller risk, so `S_at_T` is unavailable."""
    near = v(O1, "1.19950", T - 60_000)
    far_wm = v(O2, "1.19700", None, origin="snapshot", source="WATERMARK", seq=5)
    assert handle_at_bar_end([far_wm], T) == StopUnavailable()
    assert s_at_t([near, far_wm]) == StopUnavailable()


def test_an_unresolved_handle_that_cannot_change_s_is_ignored() -> None:
    """A LIMIT take-profit child known only from history, or a history-only STOP child beside a resolved
    POSITION_FIELD stop, never changes `S` or the cover."""
    stop = v(O1, "1.19900", T - 60_000)
    tp = v(O2, "1.20300", T - 60_000, origin="history", kind="LIMIT")
    got = s_at_t([stop, tp])
    assert isinstance(got, StopAtT) and got.level == D("1.19900") and [h.handle for h in got.handles] == [O1]
    field_v = v(PF, "1.19850", T - 30_000, kind=None)
    hist_child = v(O2, "1.19700", T - 60_000, origin="history")
    got2 = s_at_t([field_v, hist_child])
    assert isinstance(got2, StopAtT) and got2.level == D("1.19850") and got2.handles[0].handle == PF
    assert s_at_t([stop, hist_child]) == StopUnavailable()             # a STOP child could be the most distant
    assert s_at_t([stop, v(PF, "1.19850", T - 30_000, origin="history", kind=None)]) == StopUnavailable()


def test_version_exactly_at_t_counts() -> None:
    got = s_at_t([v(O1, "1.19900", T - 60_000, seq=1), v(O1, "1.19950", T, seq=2)])
    assert isinstance(got, StopAtT) and got.level == D("1.19950")


def test_stop_read_changed_after_bar_end_caveat() -> None:
    versions = [v(O1, "1.19900", T - 60_000, origin="stop_read", seq=1),
                v(O1, "1.20000", T + 20_000, origin="stop_read", seq=2)]
    got = s_at_t(versions)
    assert isinstance(got, StopAtT)
    assert got.level == D("1.19900") and got.caveats == (STOP_CHANGED_AFTER_BAR_END,)


def test_handle_that_did_not_exist_at_t_takes_no_part() -> None:
    versions = [v(O1, "1.19900", T - 60_000), v(O2, "1.19800", T + 60_000, units="1000")]
    got = s_at_t(versions)
    assert isinstance(got, StopAtT) and [h.handle for h in got.handles] == [O1]
    assert handle_at_bar_end([versions[1]], T) is None


def test_no_handle_or_partial_cover_is_unavailable() -> None:
    assert s_at_t([]) == StopUnavailable()
    assert s_at_t([v(O1, "1.19900", T - 60_000, units="400")]) == StopUnavailable()
    assert s_at_t([v(O1, "1.19900", T - 60_000, live=False)]) == StopUnavailable()


def test_two_children_cover_and_the_most_distant_is_s() -> None:
    versions = [v(O1, "1.19900", T - 60_000, units="500"), v(O2, "1.19800", T - 60_000, units="500")]
    got = s_at_t(versions)
    assert isinstance(got, StopAtT) and got.level == D("1.19800")
    short = [v(O1, "1.20100", T - 60_000, units="500"), v(O2, "1.20200", T - 60_000, units="500")]
    got_s = s_at_t(short, side="SELL")
    assert isinstance(got_s, StopAtT) and got_s.level == D("1.20200")


def test_kind_rules_and_position_field_first() -> None:
    limit = v(O2, "1.19800", T - 60_000, kind="LIMIT")
    assert s_at_t([limit]) == StopUnavailable()
    unknown_beyond = v(O2, "1.20100", T - 60_000, kind=None)
    assert s_at_t([unknown_beyond]) == StopUnavailable()
    be_stop = v(O1, "1.20000", T - 60_000, kind="STOP")          # a STOP at break-even still counts
    got = s_at_t([be_stop])
    assert isinstance(got, StopAtT) and got.level == D("1.20000")
    field_v = v(PF, "1.19850", T - 30_000, kind=None)
    got2 = s_at_t([be_stop, field_v])
    assert isinstance(got2, StopAtT) and got2.level == D("1.19850") and got2.handles[0].handle == PF


def test_weakest_basis_is_recorded() -> None:
    versions = [v(O1, "1.19900", T - 60_000, units="500", origin="stream"),
                v(O2, "1.19800", T - 60_000, units="500", origin="stop_read")]
    got = s_at_t(versions)
    assert isinstance(got, StopAtT) and got.basis is StopBasis.READ


# ---------------------------------------------------------------------------------------------------- roles, PreBasis
def test_roles_rank_children_by_distance_never_by_id() -> None:
    roles = handle_roles("BUY", E, [(O1, D("1.19950")), (O2, D("1.19800"))])
    assert roles == (("CHILD_ORDER:0", D("1.19800")), ("CHILD_ORDER:1", D("1.19950")))
    replaced = handle_roles("BUY", E, [(O7, D("1.19800")), (O1, D("1.19950"))])   # O2 cancelled / replaced as O7
    assert replaced == roles
    short = handle_roles("SELL", E, [(O1, D("1.20050")), (O2, D("1.20200"))])
    assert short[0] == ("CHILD_ORDER:0", D("1.20200"))
    assert handle_roles("BUY", E, [(PF, D("1.19900"))]) == (("POSITION_FIELD", D("1.19900")),)


def test_pre_basis_s_pre_and_validation() -> None:
    b = PreBasis.of("BUY", E, [(O1, D("1.19950")), (O2, D("1.19800"))], T)
    assert b.s_pre == D("1.19800") and b.bar_end_ms == T
    assert PreBasis.of("BUY", E, [(PF, D("1.19900")), (O1, D("1.19700"))], T).s_pre == D("1.19900")
    assert "1.19" not in repr(b)
    with pytest.raises(ValueError):
        PreBasis((), T)
    with pytest.raises(ValueError):
        PreBasis((("O1", D("1.19900")),), T)


def test_farther_rules() -> None:
    assert farther_from_entry("BUY", E, D("1.19900"), D("1.20000"))
    assert not farther_from_entry("BUY", E, D("1.20000"), D("1.20000"))
    assert farther_from_entry("SELL", E, D("1.20100"), D("1.20000"))
    a = PreBasis.of("BUY", E, [(O1, D("1.19900"))], T)
    b = PreBasis.of("BUY", E, [(O1, D("1.19800"))], T + 900_000)
    assert farther_basis("BUY", E, a, b) == b and farther_basis("BUY", E, b, a) == b
    assert farther_basis("BUY", E, None, a) == a and farther_basis("BUY", E, a, None) == a
    assert farther_basis("BUY", E, None, None) is None


# ---------------------------------------------------------------------------------------------------- round 14 (S38g)
@pytest.mark.parametrize("side,prior,newer,expected", [
    ("BUY", "1.19900", "1.19800", "1.19800"),     # long, loosened through an untimed read: the farther level
    ("BUY", "1.19900", "1.19950", "1.19900"),     # long, tightened: the prior (farther) level stays
    ("SELL", "1.20100", "1.20200", "1.20200"),    # short, loosened
    ("SELL", "1.20100", "1.20050", "1.20100"),    # short, tightened
])
def test_prior_version_takes_the_farther_of_the_prior_and_the_newer_noncounting_level(
        side: str, prior: str, newer: str, expected: str) -> None:
    """Round 14 (plan S38g): a newer version that does not count (watermark or history) may show the level the
    handle held at `T`; the trigger uses the farther from entry of it and the prior counting level, so a loosened
    stop is never tested at a smaller risk. The basis stays PRIOR_VERSION."""
    for wm in (v(PF, newer, None, origin="snapshot", source="WATERMARK", seq=2, kind=None),
               v(PF, newer, T - 1_000, origin="history", seq=2, kind=None)):
        got = s_at_t([v(PF, prior, T - 600_000, seq=1, kind=None), wm], side=side)
        assert isinstance(got, StopAtT) and got.level == D(expected) and got.basis is StopBasis.PRIOR_VERSION


def test_a_newer_noncounting_version_without_a_level_is_unresolved_unless_it_ended() -> None:
    held = v(PF, None, None, origin="snapshot", source="WATERMARK", seq=2, kind=None)
    assert s_at_t([v(PF, "1.19900", T - 600_000, seq=1, kind=None), held]) == StopUnavailable()
    ended = v(O1, None, T - 1_000, origin="history", seq=2, live=False)
    got = s_at_t([v(O1, "1.19900", T - 600_000, seq=1), ended])
    assert isinstance(got, StopAtT) and got.level == D("1.19900") and got.basis is StopBasis.PRIOR_VERSION


@pytest.mark.parametrize("side,prior,loosened,retightened", [
    ("BUY", "1.19900", "1.19700", "1.19950"),
    ("SELL", "1.20100", "1.20300", "1.20050"),
])
def test_every_newer_noncounting_version_takes_part_not_only_the_newest(
        side: str, prior: str, loosened: str, retightened: str) -> None:
    """Round 14 review: a non-counting loosen followed by a non-counting re-tighten, both newer than the newest
    counting version: the handle may have held the loosened level at `T`, so that farthest level is tested, never
    the newest (tighter) one."""
    for later in (v(PF, retightened, T - 1_000, origin="history", seq=3, kind=None),
                  v(PF, retightened, None, origin="snapshot", source="WATERMARK", seq=3, kind=None)):
        got = s_at_t([v(PF, prior, T - 600_000, seq=1, kind=None),
                      v(PF, loosened, T - 2_000, origin="history", seq=2, kind=None), later], side=side)
        assert isinstance(got, StopAtT) and got.level == D(loosened) and got.basis is StopBasis.PRIOR_VERSION


def test_without_side_and_entry_a_differing_newer_level_is_unresolved() -> None:
    versions = [v(O1, "1.19900", T - 600_000, seq=1), v(O1, "1.19800", None, origin="snapshot", seq=2)]
    assert handle_at_bar_end(versions, T) == StopUnavailable()
    got = handle_at_bar_end(versions, T, side="BUY", entry=E)
    assert not isinstance(got, StopUnavailable) and got is not None and got.level == D("1.19800")
