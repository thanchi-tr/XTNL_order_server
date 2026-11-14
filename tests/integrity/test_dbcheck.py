"""Step 2 checks (tier-persistence.md §3 step 2): checksum / digest / version label, monotonicity."""
from __future__ import annotations

from dataclasses import replace

from src.integrity.dbcheck import DbFinding, DbIntegrityChecker, check_row
from src.integrity.ports import DbHighWater, committed_row
from tests.integrity.fakes import T0, account, order, position


def test_clean_rows_pass() -> None:
    for item in (order(), position(), account()):
        assert check_row(committed_row(item, T0)) == []


def test_row_without_entity_checks_digest_only() -> None:
    row = committed_row(order(), T0, with_entity=False)
    assert check_row(row) == []
    assert [f.kind for f in check_row(replace(row, checksum="0" * 64))] == ["digest_hash"]


def test_checksum_mismatch() -> None:
    row = committed_row(order(), T0)
    bad = replace(row, entity=order(stop="1.0"))
    assert [f.kind for f in check_row(bad)] == ["checksum"]


def test_version_regression_tracked_and_bounded() -> None:
    c = DbIntegrityChecker(max_tracked=2)
    new = committed_row(order("A", observed=500, seq=5000), T0)
    old = committed_row(order("A", observed=100, seq=1000), T0)
    assert c.check_rows([new]) == []
    assert [f.kind for f in c.check_rows([old])] == ["version_regression"]
    assert [f.kind for f in c.check_rows([old])] == ["version_regression"]   # baseline kept
    c.check_rows([committed_row(order(x), T0) for x in ("B", "C")])            # evicts A
    assert c.check_rows([old]) == []
    c.reset()
    assert c.baseline is None


def test_high_water_timeline_and_epoch_aware() -> None:
    c = DbIntegrityChecker()
    assert c.check_high_water(DbHighWater(1, 7, 100)) == []
    assert c.check_high_water(DbHighWater(1, 8, 150)) == []      # new leader: HLC reseeded upwards
    [f] = c.check_high_water(DbHighWater(1, 8, 120))
    assert f.kind == "seq_regression" and f.is_damage
    [g] = c.check_high_water(DbHighWater(2, 8, 120))
    assert g.kind == "failover_regression" and not g.is_damage
    assert c.baseline == DbHighWater(1, 8, 150)


def test_finding_damage_flag() -> None:
    assert DbFinding("checksum", "k", "").is_damage
    assert not DbFinding("failover_regression", "k", "").is_damage
