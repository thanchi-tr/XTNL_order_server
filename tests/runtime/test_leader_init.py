"""LEADER_INIT ordering and failure -> hard exit (operations.md §4)."""
from __future__ import annotations

from dataclasses import replace

import pytest

from src.db.epoch import Epoch
from src.runtime.leader_init import InitOutcome, LeaderInit
from tests.runtime.fakes import Exits
from tests.runtime.initfakes import InitRecorder

EPOCH = Epoch("c1", 101)


async def test_documented_order() -> None:
    rec, exits = InitRecorder(), Exits()
    report = await LeaderInit(rec.deps(), exit_fn=exits, clock_ms=lambda: 1).run(EPOCH, "n1")
    assert report is not None and report.outcome is InitOutcome.LEADER and exits.reasons == []
    # FLUSHDB (the engine's database), never FLUSHALL; the ruleset is installed before the sod pass
    assert rec.log[:7] == ["epoch_bump", "max_seq", "hlc_reseed", "flushdb", "ruleset", "sod", "calendar"]
    assert "flushall" not in rec.log
    assert set(rec.log[7:10]) == {"sm4", "sm8", "risksweep"}          # step 6 in parallel
    assert rec.log.index("sm4") < rec.log.index("sm8")                 # SM8 completed after SM4 started
    assert rec.log[10:] == ["dlq_barrier", "reached_leader"]
    assert rec.reseeded == 12345 and report.calendar_version == 7 and report.ruleset_version == 3
    assert report.steps[-1] == "reached_leader" and rec.flags == []


async def test_partial_session_raises_a_flag_and_continues() -> None:
    rec = InitRecorder(partial=True)
    report = await LeaderInit(rec.deps(), exit_fn=Exits()).run(EPOCH, "n1")
    assert report is not None and report.outcome is InitOutcome.LEADER
    assert rec.flags and rec.flags[0][0] == "partial_session"


async def test_safe_hold_due_aborts_the_term_without_touching_anything_else() -> None:
    rec, exits = InitRecorder(safe_hold=True), Exits()
    report = await LeaderInit(rec.deps(), exit_fn=exits).run(EPOCH, "n1")
    assert report is not None and report.outcome is InitOutcome.SAFE_HOLD
    assert rec.log == ["epoch_bump"] and exits.reasons == []


@pytest.mark.parametrize("step", ["epoch_bump", "max_seq", "flushdb", "ruleset", "sod", "calendar", "sm4", "sm8",
                                  "risksweep", "dlq_barrier", "reached_leader"])
async def test_any_failure_is_a_hard_exit(step: str) -> None:
    rec, exits = InitRecorder(fail_at=step), Exits()
    report = await LeaderInit(rec.deps(), exit_fn=exits).run(EPOCH, "n1")
    assert report is None
    assert len(exits.reasons) == 1 and exits.reasons[0].startswith("init_failed")
    assert "reached_leader" not in rec.log[:-1]
    if step != "reached_leader":
        assert "reached_leader" not in rec.log


async def test_failure_before_reseed_stops_later_steps() -> None:
    rec = InitRecorder(fail_at="epoch_bump")
    await LeaderInit(rec.deps(), exit_fn=Exits()).run(EPOCH, "n1")
    assert rec.log == ["epoch_bump"]


async def test_deadline_is_a_hard_exit() -> None:
    rec, exits = InitRecorder(hang_at="dlq_barrier"), Exits()
    report = await LeaderInit(rec.deps(), exit_fn=exits, deadline_s=0.05).run(EPOCH, "n1")
    assert report is None and exits.reasons == ["init_failed: deadline"]


async def test_without_a_ruleset_step_the_order_is_unchanged() -> None:
    rec = InitRecorder()
    deps = rec.deps()

    report = await LeaderInit(replace(deps, rules=None), exit_fn=Exits()).run(EPOCH, "n1")
    assert report is not None and report.ruleset_version is None
    assert rec.log[:6] == ["epoch_bump", "max_seq", "hlc_reseed", "flushdb", "sod", "calendar"]
