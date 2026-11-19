"""S39b gate `replay_rerun` (operator side): `arm replay --rerun` adds each decision's re-run verdict (codes only) to
the replay report; with unparseable local settings or a re-run that cannot be set up it says so and still reports
the inputs; the CLI flag reaches the view. The re-run itself is tested in `tests/riskmgr/test_s39b_rerun.py`.
fakeredis; synthetic values that never reach the output."""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import fakeredis
import pytest

from src.ops import arm_view
from src.ops.arm_rerun import target_of
from src.riskmgr.rerun import RerunResult, RerunTarget, RerunVerdict
from tests.ops.arm_view_fakes import ArmViewPg, bar, decision_for, store_bar
from tests.ops.test_arm_replay import views


@pytest.fixture
async def md() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.flushall()
    await r.aclose()


class FakeRerunner:
    built = 0
    closed = 0
    seen: list[RerunTarget] = []

    @classmethod
    async def build(cls, conn: Any, reader: Any, keyring: Any, settings: Any) -> FakeRerunner:
        cls.built += 1
        return cls()

    async def run(self, target: RerunTarget) -> RerunVerdict:
        FakeRerunner.seen.append(target)
        return RerunVerdict(RerunResult.MATCH, None, target.outcome)

    def close(self) -> None:
        FakeRerunner.closed += 1


async def test_rerun_adds_the_verdict_per_decision(md: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(arm_view, "Rerunner", FakeRerunner)
    pg = ArmViewPg()
    await store_bar(md, bar())
    pg.decisions.append(decision_for(bar()))
    v = views(pg, md)
    rep = await v.replay(None, from_ms=None, to_ms=None, limit=5, request_backfill=False, actor=None, wait_s=0,
                         rerun=True)
    assert rep.fields["reruns"] == "MATCH=1" and rep.rows[0]["rerun"] == "MATCH"
    assert FakeRerunner.built == 1 and FakeRerunner.closed == 1
    t = FakeRerunner.seen[-1]
    assert (t.decision_id, t.key.position_id, t.event) == (1, "P1", "POST")
    plain = await v.replay(None, from_ms=None, to_ms=None, limit=5, request_backfill=False, actor=None, wait_s=0)
    assert "reruns" not in plain.fields and "rerun" not in plain.rows[0]
    assert any("--rerun" in w for w in plain.warnings)


async def test_rerun_without_settings_or_setup_says_so(md: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    pg = ArmViewPg()
    await store_bar(md, bar())
    pg.decisions.append(decision_for(bar()))
    rep = await views(pg, md, arm_settings=None).replay(None, from_ms=None, to_ms=None, limit=5,
                                                        request_backfill=False, actor=None, wait_s=0, rerun=True)
    assert any("do not parse" in w for w in rep.warnings) and rep.fields["replayed"] == 1

    class Broken(FakeRerunner):
        @classmethod
        async def build(cls, conn: Any, reader: Any, keyring: Any, settings: Any) -> FakeRerunner:
            raise ConnectionError("planted")

    monkeypatch.setattr(arm_view, "Rerunner", Broken)
    rep = await views(pg, md).replay(None, from_ms=None, to_ms=None, limit=5, request_backfill=False, actor=None,
                                     wait_s=0, rerun=True)
    assert any("ConnectionError" in w for w in rep.warnings) and rep.fields["replayed"] == 1
    assert rep.fields["reruns"] is None


def test_target_of_reads_the_view_row() -> None:
    d = decision_for(bar())
    t = target_of(d.row())
    assert (t.decision_id, t.bar_end_ms, t.event, t.decision_seq, t.inputs_digest) == (
        1, d.bar_end_ms, d.event, d.decision_seq, d.inputs_digest)
    assert t.handles is None                                     # read from arm_decision_handles by the re-run
