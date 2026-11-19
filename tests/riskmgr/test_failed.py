"""S30a: FAILED in memory first, the record with the immediate ping, SM11 FAILED, re-ping, collapsed pings, the spool,
restart and takeover, and the clear refused while the S30b probes are not wired (design §B12, §B14, PR4; R11-M05, R11-M10, R11-M11, R11-M17).

The record goes through the real `PgArmFailuresStore` over the `ArmEmulator` (one `kind='info'` critical row and one
kind `arm` row per ping); positions through the arm-tracker's `FakeStore`. Synthetic ids, symbols and levels only.
"""
from __future__ import annotations

import ast
import inspect
import json
from pathlib import Path

import pytest

from src.db.arm_ports import TERMINAL_ARM_STATES
from src.db.fence import EpochMismatch
from src.marketdata.types import INVALID_BAR
from src.riskmgr import budget as B
from src.riskmgr import failed as F
from src.riskmgr.config import FailSettings
from src.riskmgr.types import ScopeKind
from tests.riskmgr.failed_fakes import (
    EPOCH,
    DownPositions,
    FailedRig,
    FakeSpool,
    T0,
    failed_rig,
    make_manager,
    row,
    series,
)
from tests.riskmgr.tracker_fakes import ADAPTER

S_A, S_B, S_C = series("SYN.A"), series("SYN.B"), series("SYN.C")
HOUR = 3_600_000


@pytest.fixture
async def r() -> FailedRig:
    rig = failed_rig()
    rig.add(row("P1", symbol="SYN.A"), row("P2", symbol="SYN.A"), row("P3", symbol="SYN.B"))
    assert await rig.manager.start() is True
    await rig.manager.tick()
    return rig


def states(r: FailedRig) -> dict[str, tuple[str, str | None]]:
    return {k.position_id: (v.state, v.prior_state) for k, v in r.positions.rows.items()}


# ---------------------------------------------------------------------------------------------------- SERIES
async def test_a_series_fault_fails_that_series_in_memory_first_then_records_and_pings(r: FailedRig) -> None:
    sql_before = len(r.emu.sql)
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now, counters=(("invalid_buckets", 3),))
    # memory first: no I/O on the caller's path, the scope already holds its positions
    assert len(r.emu.sql) == sql_before
    m = r.manager
    assert m.covers(ADAPTER, [S_A.canonical], []) and not m.covers(ADAPTER, [S_B.canonical], [])
    assert m.scopes.series_failed(S_A.canonical) and not m.scopes.series_failed(S_B.canonical)
    assert m.summary().payload() == {"count": 1, "global_failed": False, "unrecorded": 0}
    assert len(r.mirror.mirrored) == 1 and r.mirror.pages == []      # log mirror only, never page()
    await m.tick()
    (crit,), (arm,) = r.critical_rows(), r.arm_rows()
    (ep,) = r.emu.t.failures.values()
    assert ep["ping_id"] in str(crit["title"]) and crit["title"] == arm["title"] and crit["body"] == arm["body"]
    assert ep["probe_kind"] == "series_reread" and ep["position_ids"] == ["mock:P1", "mock:P2"]
    assert states(r) == {"P1": ("FAILED", "TRACKING"), "P2": ("FAILED", "TRACKING"), "P3": ("TRACKING", None)}
    assert r.positions.row("P1").state_reason == F.SCOPE_FAILED_REASON
    await m.tick()
    assert len(r.critical_rows()) == 1                                 # one ping per episode


async def test_other_series_keep_advising(r: FailedRig) -> None:
    for _ in range(3):
        r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)          # a repeat signal is the same episode
    await r.manager.tick()
    assert r.manager.counters.duplicates == 2
    assert not r.manager.covers(ADAPTER, [S_B.canonical], ["strategy:break_even_on_risk_close:1"])
    assert r.positions.row("P3").state == "TRACKING"


async def test_the_ping_is_bounded_and_carries_no_value() -> None:
    rig = failed_rig()
    rig.add(*(row(f"P{i}", symbol="SYN.A") for i in range(1, 7)))
    assert await rig.manager.start()
    await rig.manager.tick()
    rig.budget.series_fault(S_A, INVALID_BAR, rig.clock.now)
    await rig.manager.tick()
    (crit,) = rig.critical_rows()
    body = str(crit["body"])
    assert "and 3 more; see arm failed show" in body and body.count("mock:P") == 3
    assert "no advice for these positions until FAILED is cleared" in body and "issued since" in body
    assert f"python -m src.ops arm failed clear SERIES:{S_A.canonical} --reason <text>" in body
    assert "SYN.A BUY" in body
    for level in ("1.2000", "1.1900", "1.2", "1.19"):                       # entry and stop levels never appear
        assert level not in body and level not in str(crit["title"])


# ---------------------------------------------------------------------------------------------------- PostgreSQL down
async def test_postgresql_down_reaches_the_dead_man_and_the_ping_is_written_on_recovery(r: FailedRig) -> None:
    r.arm.down = r.engine.down = True
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    for _ in range(3):
        await r.manager.tick()
        r.clock.now += 5_000
    assert r.manager.summary().payload() == {"count": 1, "global_failed": False,   # from memory
                                              "unrecorded": 1}                    # held on this node only
    assert r.emu.t.failures == {} and r.critical_rows() == []
    assert len(r.spool.records) == 1                                    # spooled once, on the first failure
    assert r.manager.covers(ADAPTER, [S_A.canonical], [])
    r.arm.down = r.engine.down = False
    await r.manager.tick()
    assert len(r.emu.t.failures) == 1 and len(r.critical_rows()) == 1
    assert r.spool.acked == [r.spool.records[0].failure_id]
    assert r.positions.row("P1").state == "FAILED"


async def test_an_engine_stopping_before_postgresql_returns_replays_the_spool_before_any_advice() -> None:
    rig = failed_rig()
    rig.add(row("P1", symbol="SYN.A"))
    assert await rig.manager.start()
    rig.arm.down = rig.engine.down = True
    rig.budget.series_fault(S_A, INVALID_BAR, rig.clock.now)
    await rig.manager.drain()                                           # stop: the episode goes to the spool
    assert len(rig.spool.records) == 1 and rig.emu.t.failures == {}
    rig.arm.down = rig.engine.down = False
    m = rig.restart()
    assert m.covers(ADAPTER, [S_B.canonical], [])                       # not loaded yet: advises nothing
    assert await m.start() is True
    assert m.covers(ADAPTER, [S_A.canonical], []) and not m.covers(ADAPTER, [S_B.canonical], [])
    await m.tick()
    assert len(rig.emu.t.failures) == 1 and len(rig.critical_rows()) == 1
    assert rig.spool.acked == [rig.spool.records[0].failure_id]
    m2 = rig.restart()
    assert await m2.start()
    await m2.tick()
    assert len(rig.critical_rows()) == 1                                # a replayed spool never pings twice


async def test_failed_survives_a_restart_and_a_takeover(r: FailedRig) -> None:
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await r.manager.tick()
    m = r.restart(spool=FakeSpool())                                    # another node: an empty spool
    assert await m.start()
    assert m.covers(ADAPTER, [S_A.canonical], []) and m.summary().count == 1
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)                # the cause is still there
    await m.tick()
    assert len(r.critical_rows()) == 1 and len(r.emu.t.failures) == 1


async def test_unreadable_open_episodes_advise_nothing_until_read() -> None:
    rig = failed_rig()
    rig.engine.down = True
    assert await rig.manager.start() is False
    assert rig.manager.covers(ADAPTER, [], []) and rig.manager.scopes.series_failed(S_A.canonical)
    # every position held, the open set unknown: the dead-man must not read "nothing FAILED"
    assert rig.manager.summary().payload() == {"count": 1, "global_failed": True, "unrecorded": 0}
    await rig.manager.tick()
    assert not rig.manager.scopes.ready and rig.manager.summary().global_failed
    rig.engine.down = False
    await rig.manager.tick()
    assert rig.manager.scopes.ready and not rig.manager.covers(ADAPTER, [S_A.canonical], [])
    assert rig.manager.summary().payload() == {"count": 0, "global_failed": False, "unrecorded": 0}


async def test_a_fault_before_start_is_adopted_by_the_stored_episode() -> None:
    """A new term sees the fault again before it has read the open rows: the stored episode wins, no second ping."""
    rig = failed_rig()
    assert await rig.manager.start()
    rig.budget.series_fault(S_A, INVALID_BAR, rig.clock.now)
    await rig.manager.tick()
    rig.clock.now += 60_000
    m = rig.restart()
    rig.budget.series_fault(S_A, INVALID_BAR, rig.clock.now)
    assert await m.start()
    await m.tick()
    assert len(rig.emu.t.failures) == 1 and len(rig.critical_rows()) == 1


# ---------------------------------------------------------------------------------------------------- ARM, collapse
async def test_the_arm_pool_down_gives_one_arm_scope_fault_and_one_ping(r: FailedRig) -> None:
    r.arm.down = True
    for _ in range(int(900 / 5) + 2):
        r.budget.observe_pool(arm_ok=False, now_ms=r.clock.now)
        await r.manager.tick()
        r.clock.now += 5_000
    (ep,) = r.emu.t.failures.values()
    assert ep["scope"] == ("ARM", "arm") and ep["reason"] == B.POOL_DOWN and ep["probe_kind"] == "arm_global"
    assert len(r.critical_rows()) == 1 and len(r.arm_rows()) == 1     # recorded through the engine pool
    assert r.manager.summary().payload() == {"count": 1, "global_failed": True, "unrecorded": 0}
    assert r.manager.covers(ADAPTER, [S_B.canonical], [])
    assert S_B.canonical not in str(r.critical_rows()[0]["body"])
    # a series fault under the open global episode is recorded, folded under the ARM ping id, and named by one
    # re-ping of the ARM episode: no series storm of pings, and never a FAILED scope nobody was told about
    r.budget.series_fault(S_B, INVALID_BAR, r.clock.now)
    assert S_B.canonical in r.mirror.mirrored[-1][1]                    # the log mirror names it at once
    await r.manager.tick()
    assert len(r.emu.t.failures) == 2 and len(r.critical_rows()) == 2
    folded = next(f for f in r.emu.t.failures.values() if f["scope"][0] == "SERIES")
    assert folded["ping_id"] == ep["ping_id"]
    second = r.critical_rows()[1]
    assert ep["ping_id"] in str(second["title"]) and "re-ping 1" in str(second["title"])
    assert f"also FAILED with it: SERIES {S_B.canonical} (invalid_bar)" in str(second["body"])
    await r.manager.tick()
    r.clock.now += 60_000
    await r.manager.tick()
    assert len(r.critical_rows()) == 2                                  # named once; the 4 h cycle resumes


async def test_a_global_failed_following_series_gives_one_collapsed_ping(r: FailedRig) -> None:
    r.arm.down = r.engine.down = True                                   # PostgreSQL down: nothing pinged yet
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    r.budget.series_fault(S_B, INVALID_BAR, r.clock.now)
    await r.manager.tick()
    r.budget.store_fault(B.BOOTSTRAP_FAILED, r.clock.now)
    assert len(r.mirror.mirrored) == 3
    r.arm.down = r.engine.down = False
    r.clock.now += 10_000
    await r.manager.tick()
    assert len(r.emu.t.failures) == 3
    (crit,) = r.critical_rows()
    assert "also FAILED with it" in str(crit["body"])
    assert S_A.canonical in str(crit["body"]) and S_B.canonical in str(crit["body"])
    assert len({f["ping_id"] for f in r.emu.t.failures.values()}) == 1


async def test_n_series_of_one_adapter_fail_the_adapter_with_one_ping() -> None:
    rig = failed_rig(fail=FailSettings(global_series_n=3))
    rig.add(row("P1", symbol="SYN.A"), row("Q1", symbol="SYN.A", adapter="other"))
    assert await rig.manager.start()
    rig.arm.down = rig.engine.down = True
    for s in (S_A, S_B, S_C):
        rig.budget.series_fault(s, INVALID_BAR, rig.clock.now)
    rig.budget.series_fault(series("SYN.A", adapter="other"), INVALID_BAR, rig.clock.now)
    rig.arm.down = rig.engine.down = False
    rig.clock.now += 5_000
    await rig.manager.tick()
    scopes = sorted(f["scope"] for f in rig.emu.t.failures.values())
    assert ("ADAPTER", ADAPTER) in scopes and ("ADAPTER", "other") not in scopes and len(scopes) == 5
    assert len(rig.critical_rows()) == 2                                # the ADAPTER ping and the other series'
    adapter = next(f for f in rig.emu.t.failures.values() if f["scope"] == ("ADAPTER", ADAPTER))
    assert adapter["probe_kind"] == "adapter_series" and json.loads(adapter["counters"]) == {"failed_series": 3}
    assert rig.manager.covers(ADAPTER, [series("SYN.Z").canonical], [])
    assert rig.positions.row("P1").state == "FAILED"


# ---------------------------------------------------------------------------------------------------- STRATEGY
async def test_a_strategy_version_fault_fails_only_its_positions() -> None:
    guard = json.dumps([{"kind": "first_bar", "version": 2, "params": {}}])
    rig = failed_rig()
    rig.add(row("P1", guards_json=guard), row("P2"))
    assert await rig.manager.start()
    await rig.manager.tick()
    rig.budget.version_fault("guard:first_bar:2", 3, ("mock:P1",), rig.clock.now, edge_code="default")
    await rig.manager.tick()
    assert rig.positions.row("P1").state == "FAILED" and rig.positions.row("P2").state == "TRACKING"
    (ep,) = rig.emu.t.failures.values()
    assert ep["edge_code"] == "default" and ep["probe_kind"] == "strategy_conformance"
    assert rig.manager.covers(ADAPTER, [], ["guard:first_bar:2"])
    assert not rig.manager.covers(ADAPTER, [], ["strategy:break_even_on_risk_close:1"])


def test_row_version_scopes() -> None:
    assert F.row_version_scopes(row()) == ("strategy:break_even_on_risk_close:1",)
    g = row(guards_json='[{"kind": "first_bar", "version": 1, "params": {}}]')
    assert F.row_version_scopes(g) == ("strategy:break_even_on_risk_close:1", "guard:first_bar:1")
    bad = row(guards_json="not json")
    assert F.row_version_scopes(bad) is None
    scope = B.ScopeRef(ScopeKind.STRATEGY_VERSION, "guard:any:9")
    assert F.scope_covers_row(scope, bad, None)                         # unknown guards: fail closed


# ---------------------------------------------------------------------------------------------------- SM11
async def test_terminal_and_failed_positions_are_left_alone_and_a_cas_miss_retries(r: FailedRig) -> None:
    r.add(row("P4", symbol="SYN.A", state="CLOSED"), row("P5", symbol="SYN.A", state="SUSPENDED"))
    r.positions.miss_next = 1
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await r.manager.tick()
    await r.manager.tick()
    s = states(r)
    assert s["P4"] == ("CLOSED", None) and s["P5"] == ("FAILED", "SUSPENDED")
    assert s["P1"][0] == s["P2"][0] == "FAILED"
    assert all(v.state in TERMINAL_ARM_STATES or v.state in ("FAILED", "TRACKING") for v in r.positions.rows.values())


# ---------------------------------------------------------------------------------------------------- re-ping
async def test_each_ping_and_re_ping_is_exactly_one_critical_outbox_row(r: FailedRig) -> None:
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await r.manager.tick()
    r.clock.now += 4 * HOUR - 1
    await r.manager.tick()
    assert len(r.critical_rows()) == 1
    r.clock.now += 1
    await r.manager.tick()
    assert len(r.critical_rows()) == 2 and len(r.arm_rows()) == 2
    assert "re-ping 1" in str(r.critical_rows()[1]["title"])
    (ep,) = r.emu.t.failures.values()
    assert ep["ping_count"] == 2 and ep["ping_id"] in str(r.critical_rows()[1]["title"])
    r.clock.now += 4 * HOUR
    await r.manager.tick()
    assert len(r.critical_rows()) == 3 and len(r.mirror.mirrored) == 3


async def test_a_folded_episode_is_re_pinged_by_its_collapsing_episode(r: FailedRig) -> None:
    r.budget.store_fault(B.MD_RESETS, r.clock.now)
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)                # folded while the ARM ping is unsent
    await r.manager.tick()
    (first,) = r.critical_rows()
    assert f"also FAILED with it: SERIES {S_A.canonical}" in str(first["body"])
    r.clock.now += 4 * HOUR
    await r.manager.tick()
    assert len(r.critical_rows()) == 2
    assert f"also FAILED with it: SERIES {S_A.canonical}" in str(r.critical_rows()[1]["body"])


async def test_a_scope_folded_after_the_collapsing_ping_is_named_by_one_re_ping(r: FailedRig) -> None:
    r.budget.store_fault(B.MD_RESETS, r.clock.now)
    await r.manager.tick()
    assert len(r.critical_rows()) == 1
    r.clock.now += 60_000
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)                # two scopes fold in one tick
    r.budget.series_fault(S_B, INVALID_BAR, r.clock.now)
    await r.manager.tick()
    assert len(r.critical_rows()) == 2                                  # one re-ping names both
    body = str(r.critical_rows()[1]["body"])
    assert S_A.canonical in body and S_B.canonical in body
    assert len({f["ping_id"] for f in r.emu.t.failures.values()}) == 1
    r.clock.now += 4 * HOUR - 1
    await r.manager.tick()
    assert len(r.critical_rows()) == 2


async def test_a_folded_scope_whose_collapser_is_gone_pings_on_its_own_if_never_named(r: FailedRig) -> None:
    r.budget.store_fault(B.MD_RESETS, r.clock.now)
    await r.manager.tick()
    r.clock.now += 60_000
    r.budget.series_fault(S_B, INVALID_BAR, r.clock.now)
    del r.manager.scopes.episodes[B.ARM_SCOPE]                          # the ARM episode closed before it re-pinged
    await r.manager.tick()
    assert len(r.critical_rows()) == 2
    title = str(r.critical_rows()[1]["title"])
    assert S_B.canonical in title and "re-ping 1" in title
    await r.manager.tick()
    assert len(r.critical_rows()) == 2


# ---------------------------------------------------------------------------------------------------- mode, pause, clear
def test_the_ping_reads_no_mode_pause_or_route_policy() -> None:
    """The FAILED path has no notice mode, controls or Supabase route input: the ping bypasses `actionable`
    mode, digests, quiet settings and a pause."""
    params = set(inspect.signature(F.FailedManager).parameters)
    assert not params & {"mode", "controls", "notices", "policies", "routes", "paused"}


async def test_the_ping_is_written_while_the_arm_is_paused_and_the_route_is_actionable() -> None:
    """Behaviour: with the ARM paused (`arm_controls`) and the route's policy in `actionable` mode, the FAILED ping is
    still one `kind='info'` CRITICAL row plus its kind `arm` copy, and the FAILED path never reads the controls or
    the route policies."""
    from src.armconfig.validate import RoutePolicy
    from src.db import arm_config_store as C
    rig = failed_rig()
    rig.add(row("P1", symbol="SYN.A"))
    rig.emu.t.controls = (True, "operator", T0 - 60_000, False, None, 2)
    policy = RoutePolicy(route=f"{ADAPTER}:acct", mode="actionable")
    assert policy.mode == "actionable"
    assert await rig.manager.start()
    rig.budget.series_fault(S_A, INVALID_BAR, rig.clock.now)
    await rig.manager.tick()
    (crit,), (arm,) = rig.critical_rows(), rig.arm_rows()
    assert crit["kind"] == "info" and crit["severity"] == "CRITICAL" and arm["title"] == crit["title"]
    assert S_A.canonical in str(crit["title"])
    config_sql = {v for k, v in vars(C).items() if k.startswith("SQL_") and isinstance(v, str)}
    assert not config_sql & set(rig.emu.sql)


async def test_every_clear_is_refused_while_the_probes_are_not_wired(r: FailedRig) -> None:
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await r.manager.tick()
    assert r.manager.submit_clear(11, f"SERIES:{S_A.canonical}") is True      # non-blocking: queued only
    assert r.answers.got == []
    await r.manager.tick()
    assert r.answers.got == [(11, "refused", "probe_not_wired")]            # fail closed (S30b probes unwired)
    assert r.manager.covers(ADAPTER, [S_A.canonical], []) and r.positions.row("P1").state == "FAILED"


# ---------------------------------------------------------------------------------------------------- spool, positions
async def test_a_spooled_episode_keeps_its_edge_and_a_later_fold_replays_as_one_ping() -> None:
    guard = json.dumps([{"kind": "first_bar", "version": 2, "params": {}}])
    rig = failed_rig()
    rig.add(row("P1", symbol="SYN.A", guards_json=guard))
    assert await rig.manager.start()
    await rig.manager.tick()
    rig.arm.down = rig.engine.down = True
    rig.budget.version_fault("guard:first_bar:2", 3, ("mock:P1",), rig.clock.now, edge_code="default")
    rig.budget.series_fault(S_A, INVALID_BAR, rig.clock.now)
    await rig.manager.tick()                                            # both spooled under their own ping ids
    assert len(rig.spool.records) == 2 and rig.spool.records[0].edge_code == "default"
    rig.budget.store_fault(B.BOOTSTRAP_FAILED, rig.clock.now)           # folds both
    await rig.manager.tick()
    arm_ping = rig.manager.scopes.episodes[B.ARM_SCOPE].record.ping_id
    assert {r.ping_id for r in rig.spool.records[-3:]} == {arm_ping}    # the fold re-spooled
    await rig.manager.drain()
    rig.arm.down = rig.engine.down = False
    m = rig.restart()
    assert await m.start()
    await m.tick()
    (crit,) = rig.critical_rows()                                       # one ping, naming both folded scopes
    body = str(crit["body"])
    assert arm_ping in str(crit["title"]) and S_A.canonical in body and "guard:first_bar:2" in body
    assert {f["ping_id"] for f in rig.emu.t.failures.values()} == {arm_ping}
    version = next(f for f in rig.emu.t.failures.values() if f["scope"][0] == "STRATEGY_VERSION")
    assert version["edge_code"] == "default"


async def test_a_fault_before_the_first_read_pings_the_positions_read_after_it() -> None:
    rig = failed_rig()
    rig.add(row("P1", symbol="SYN.A"), row("P2", symbol="SYN.A"))
    rig.budget.series_fault(S_A, INVALID_BAR, rig.clock.now)            # no positions read yet
    assert await rig.manager.start()
    await rig.manager.tick()
    (crit,) = rig.critical_rows()
    assert "positions (2):" in str(crit["body"]) and "mock:P1" in str(crit["body"])
    (ep,) = rig.emu.t.failures.values()
    assert ep["position_ids"] == ["mock:P1", "mock:P2"]


async def test_the_pool_down_ping_lists_positions_read_through_the_engine_pool() -> None:
    arm_positions = DownPositions()
    rig = failed_rig(positions=arm_positions)
    engine_positions = DownPositions(arm_positions)
    rig.manager = make_manager(rig, spool=rig.spool, engine_positions=engine_positions)
    rig.add(row("P1", symbol="SYN.A"), row("P2", symbol="SYN.B"))
    assert await rig.manager.start()
    arm_positions.down = rig.arm.down = True
    for _ in range(int(900 / 5) + 2):
        rig.budget.observe_pool(arm_ok=False, now_ms=rig.clock.now)
        await rig.manager.tick()
        rig.clock.now += 5_000
    (crit,) = rig.critical_rows()
    assert "positions (2):" in str(crit["body"]) and engine_positions.reads > 0


async def test_a_record_waits_one_rows_period_for_a_fresh_read_then_goes_out() -> None:
    positions = DownPositions()
    rig = failed_rig(positions=positions)
    rig.add(row("P1", symbol="SYN.A"))
    assert await rig.manager.start()
    positions.down = True
    rig.budget.series_fault(S_A, INVALID_BAR, rig.clock.now)
    await rig.manager.tick()
    assert rig.critical_rows() == []                                    # waiting for a read after the entry
    rig.clock.now += 5_000
    await rig.manager.tick()
    assert len(rig.critical_rows()) == 1                                # never held back longer than that


# ---------------------------------------------------------------------------------------------------- isolation
async def test_a_lost_fence_raises() -> None:
    rig = failed_rig()
    assert await rig.manager.start()
    rig.budget.series_fault(S_A, INVALID_BAR, rig.clock.now)
    rig.emu.epoch = EPOCH + 1
    with pytest.raises(EpochMismatch):
        await rig.manager.tick()


def test_failed_never_reaches_halts_ingestion_or_oversee() -> None:
    """FAILED writes no halt row and stops nothing outside the ARM: the modules import nothing from Oversee, the
    bus, the runtime or the cache, and no code string names a halt table or key."""
    root = Path(__file__).resolve().parents[2] / "src" / "riskmgr"
    for name in ("failed.py", "budget.py"):
        tree = ast.parse((root / name).read_text(encoding="utf-8"))
        docstrings = {id(n.body[0].value) for n in ast.walk(tree)
                      if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                      and n.body and isinstance(n.body[0], ast.Expr)}
        imported = {n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
        imported |= {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        assert not {m for m in imported if m.startswith(("src.oversee", "src.pubsub", "src.runtime", "src.cache"))}
        strings = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)
                   and id(n) not in docstrings]
        assert not [t for t in strings if "halt" in t or "EngineSession" in t], name


async def test_only_info_and_arm_outbox_rows_are_written(r: FailedRig) -> None:
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    r.budget.store_fault(B.MD_OOM, r.clock.now)
    await r.manager.tick()
    assert {row["kind"] for row in r.emu.t.outbox} == {"info", "arm"}


def test_ids_fit_the_migration_patterns() -> None:
    import re
    fid, pid = F.episode_ids(B.series_scope(S_A), INVALID_BAR, 2**62, T0)
    pattern = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}")
    assert pattern.fullmatch(fid) and pattern.fullmatch(pid) and pid == F.own_ping_id(fid)
    assert F.episode_ids(B.series_scope(S_B), INVALID_BAR, 1, T0) != F.episode_ids(B.series_scope(S_A),
                                                                                    INVALID_BAR, 1, T0)


def test_failed_settings_bounds() -> None:
    with pytest.raises(ValueError):
        F.FailedSettings(max_positions=0)


def test_the_summary_is_the_dead_man_arm_failed_shape() -> None:
    from src.deadman_receiver.evaluate import ARM_FAILED_FIELDS, ARM_FAILED_OPTIONAL
    assert tuple(F.FailedSummary(count=2, global_failed=True).payload()) == ARM_FAILED_FIELDS + ARM_FAILED_OPTIONAL
