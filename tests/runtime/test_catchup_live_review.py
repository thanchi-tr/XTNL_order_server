"""Live review 2026-11-25 (src/runtime/catchup.py): an incomplete catch-up whose only cause is a history row
without a broker revision / time must name that row's entity id and the uncovered window in its PAGE[high] and
in catchup_runs.notes (ids and times only, never an amount), and stays a broker limit: paged once, not retried,
coverage moves on (operations.md section 17 step 6)."""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from src.adapters.base import BrokerHistory
from src.db.uptime_store import Coverage
from src.dto import OrderStatus
from src.runtime.catchup import iso_utc
from tests.pubsub.conftest import ManualClock, order
from tests.runtime.test_catchup import AMOUNT, NOW, Db, rig, start
from tests.pubsub.test_orchestrator import wait_for


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(NOW)


async def test_an_untimed_history_row_is_named_with_its_window_in_the_page(tmp_path: Path,
                                                                          clock: ManualClock) -> None:
    db = Db(cov=Coverage("mock", NOW - 3_600_000, None))
    r = await rig(tmp_path, clock, db=db)
    gap_start, gap_end = NOW - 3_600_000 - 900_000, NOW
    # Orders are in the broker-time domain on the mock: a history row without a broker time is skipped (twice,
    # two versions of one entity) and the timed one is applied. Full coverage, complete flag set.
    r.ad.history = BrokerHistory("mock", gap_start, gap_end, covered_from_ms=gap_start,
                                 orders=(order("UNTIMED", OrderStatus.WORKING, t=0),
                                         order("UNTIMED", OrderStatus.FILLED, t=0),
                                         order("TIMED", OrderStatus.FILLED, t=NOW - 1_000_000)),
                                 positions=(), complete=True, notes=())
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
        clock.ms += 7_200_000                     # long past any retry delay: no retry may be scheduled
        await asyncio.sleep(0.05)
    finally:
        await r.close()
    (_, rec), = r.db.records
    assert not rec.complete and rec.anomalies == 0
    assert "2 history rows without a broker revision / time skipped: mock:UNTIMED" in rec.notes
    result = r.coord.results[-1]
    assert result.uncovered == (gap_start, gap_end) and result.unversioned == ["mock:UNTIMED"]
    assert not result.retryable and r.coord.vouches("mock") and not r.coord.verified("mock")
    high = [b for s, _t, b in r.pages if s == "high"]
    assert len(high) == 1
    body = high[0]
    assert "mock:UNTIMED" in body and "skipped (no version): mock:UNTIMED" in body
    assert f"uncovered window {iso_utc(gap_start)} .. {iso_utc(gap_end)}" in body
    assert "retry in" not in body and "held back" not in body
    assert AMOUNT not in body and "1.1" not in body                 # no amount, no price
    assert all(AMOUNT not in b for _s, _t, b in r.pages)


async def test_the_skipped_ids_survive_the_note_cut_and_are_capped(tmp_path: Path, clock: ManualClock) -> None:
    db = Db(cov=Coverage("mock", NOW - 3_600_000, None))
    r = await rig(tmp_path, clock, db=db)
    gap_start, gap_end = NOW - 3_600_000 - 900_000, NOW
    untimed = tuple(order(f"U{i:02d}", OrderStatus.FILLED, t=0) for i in range(12))
    r.ad.history = BrokerHistory("mock", gap_start, gap_end, covered_from_ms=gap_start, orders=untimed,
                                 positions=(), complete=False, retryable=False,
                                 notes=tuple("x" * 200 for _ in range(5)))   # long notes push the 600-char cut
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    (_, rec), = r.db.records
    assert "mock:U00" in rec.notes and "(+2 more)" in rec.notes and "mock:U11" not in rec.notes
    body = [b for s, _t, b in r.pages if s == "high"][0]
    assert "skipped (no version): mock:U00, mock:U01" in body and "(+2 more)" in body
    assert f"uncovered window {iso_utc(gap_start)} .. {iso_utc(gap_end)}" in body
