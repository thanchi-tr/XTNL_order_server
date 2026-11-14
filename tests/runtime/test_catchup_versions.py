"""Downtime catch-up with several versions of one order (a StoneX stop / limit order WORKING at its placement, then
its end): every version goes through the normal path oldest first per entity and lands as its own APPLIED version,
the counts stay per entity, an older version behind a newer stored row is a plain stale drop (no anomaly), and
re-reading the same window creates no APPLIED row. The store plays PgStateStore's version compare (`decide`), its
log is the event_log outcome per row. No network."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from src.db.uptime_store import Coverage
from src.dto import Decision, OrderDTO, OrderStatus, decide
from tests.pubsub.conftest import ManualClock, order
from tests.pubsub.test_catchup_hooks import history
from tests.runtime.test_catchup import NOW, Db, rig, start, wait_for

OFF = NOW - 3_600_000          # coverage: the engine went off here
PLACED, ENDED = NOW - 1_800_000, NOW - 1_200_000


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(NOW)


@dataclass
class VersionDb(Db):
    """Db whose order commits follow PgStateStore: `decide()` against the stored row; `log` is what event_log
    records per published order row (APPLIED / NOOP_ADVANCE (no history row) / DROPPED_STALE)."""

    log: list[tuple[str, str, str]] = field(default_factory=list)      # (decision, order id, status)

    def apply(self, item: object) -> None:
        if not isinstance(item, OrderDTO):
            super().apply(item)
            return
        cur = self.orders.get(item.broker_order_id)
        decision = decide(cur, item)
        if decision in (Decision.APPLY, Decision.NOOP_ADVANCE):
            self.orders[item.broker_order_id] = item
        self.log.append((decision.value, item.broker_order_id, item.status.value))

    def applied(self, oid: str) -> list[str]:
        return [status for d, o, status in self.log if o == oid and d == Decision.APPLY.value]


def two_versions(oid: str) -> list[OrderDTO]:
    """Newest first on purpose: the submit must still apply the placement before the end."""
    return [order(oid, OrderStatus.CANCELLED, t=ENDED), order(oid, OrderStatus.WORKING, t=PLACED)]


async def run_catchups(tmp_path: Path, clock: ManualClock, db: VersionDb, orders: list[OrderDTO],
                       times: int = 1) -> VersionDb:
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = history(orders, since=OFF - 900_000, until=NOW)
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: len(db.records) == 1 and r.coord.finished("mock"))
        for n in range(2, times + 1):
            r.coord.request("mock", "operator")             # the same window read again
            await r.snapshot()
            await wait_for(lambda n=n: len(db.records) == n and r.coord.finished("mock"))  # type: ignore[misc]
    finally:
        await r.close()
    return db


async def test_every_version_is_applied_oldest_first_and_counted_once(tmp_path: Path, clock: ManualClock) -> None:
    db = await run_catchups(tmp_path, clock, VersionDb(cov=Coverage("mock", OFF, "run-0")),
                            [*two_versions("SL1"), order("E1", OrderStatus.FILLED, t=PLACED + 1)])
    assert db.applied("SL1") == ["WORKING", "CANCELLED"]            # both versions, placement first
    assert db.orders["SL1"].status is OrderStatus.CANCELLED and db.orders["SL1"].broker_updated_at_ms == ENDED
    (_epoch, rec), = db.records
    assert rec.complete and rec.anomalies == 0
    assert (rec.orders_seen, rec.orders_new, rec.orders_updated) == (2, 2, 0)    # entities, not versions


async def test_an_older_version_behind_a_newer_stored_row_is_a_plain_stale_drop(tmp_path: Path,
                                                                               clock: ManualClock) -> None:
    db = VersionDb(cov=Coverage("mock", OFF, "run-0"))
    db.orders["SL1"] = order("SL1", OrderStatus.CANCELLED, t=ENDED)             # stored by an earlier catch-up
    db = await run_catchups(tmp_path, clock, db, two_versions("SL1"))
    assert [(d, s) for d, o, s in db.log if o == "SL1"] == [("DROP_STALE", "WORKING"), ("NOOP_ADVANCE", "CANCELLED")]
    (_epoch, rec), = db.records
    assert rec.complete and rec.anomalies == 0 and "parked" not in rec.notes
    assert (rec.orders_seen, rec.orders_new, rec.orders_updated) == (1, 0, 0)


async def test_re_reading_the_same_window_creates_no_applied_row(tmp_path: Path, clock: ManualClock) -> None:
    db = await run_catchups(tmp_path, clock, VersionDb(cov=Coverage("mock", OFF, "run-0")), two_versions("SL1"),
                            times=2)
    assert db.applied("SL1") == ["WORKING", "CANCELLED"]                         # from the first read only
    second = [(d, s) for d, o, s in db.log if o == "SL1"][2:]
    assert second == [("DROP_STALE", "WORKING"), ("NOOP_ADVANCE", "CANCELLED")]
    (_e1, first), (_e2, again) = db.records
    assert (first.orders_new, first.orders_updated) == (1, 0)
    assert again.complete and (again.orders_seen, again.orders_new, again.orders_updated) == (1, 0, 0)
