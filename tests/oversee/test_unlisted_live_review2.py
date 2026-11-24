"""Live review 2026-11-25, round 2: rows of an account that is no longer listed are all resolved, but only the
ones that were active (a 'resolved' transition) are paged as `unlisted_states_resolved` or counted as a drop below
SOFT. A pending or UNKNOWN episode that never activated is stored resolved with no transition and no alert
(oversee-rules.md §2 Confirm, §10 alerts per active state episode)."""
from __future__ import annotations

from src.oversee.halt import HaltSwitch
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.types import BreachState, Level
from tests.oversee.helpers import T0
from tests.oversee.test_sweep import Env

PENDING = BreachState("max_aggregate_open_risk", "account", "GONE", "GONE", pending_level=Level.SOFT_HALT,
                      pending_since=T0 - 500, last_pass_ms=T0 - 500, last_sweep_id=10)
UNKNOWN_IN_GRACE = BreachState("missing_stop_loss", "position", "stonex:G1", "GONE", unknown_since=T0 - 5_000,
                               last_pass_ms=T0 - 1_000, last_sweep_id=10)
ACTIVE = BreachState("missing_stop_loss", "position", "stonex:G2", "GONE", Level.SOFT_HALT, active_since=T0 - 1,
                     activation_id=9)


def unlisted_pages(env: Env) -> list[str]:
    return [p.detail for p in env.pages if p.code == "unlisted_states_resolved"]


async def test_unlisted_account_with_only_never_activated_rows_sends_no_page() -> None:
    db = InMemoryOverseeDb()
    for s in (PENDING, UNKNOWN_IN_GRACE):
        db.t.breaches[s.key] = s
    env = Env(db=db)
    await env.at(T0)
    for s in (PENDING, UNKNOWN_IN_GRACE):
        row = db.t.breaches[s.key]
        assert row.resolved_at == T0 and row.resolved_by == "entity_end" and not row.active
        assert row.pending_since is None and row.unknown_since is None
    assert unlisted_pages(env) == []
    assert not [k for k in db.t.events if k[2] in ("GONE", "stonex:G1")]
    assert not [r for r in db.t.outbox if "GONE" in r.title or "stonex:G1" in r.title]
    assert env.sweep._unlisted_drop is None


async def test_page_names_only_the_rows_that_were_active() -> None:
    db = InMemoryOverseeDb()
    for s in (PENDING, UNKNOWN_IN_GRACE, ACTIVE):
        db.t.breaches[s.key] = s
    env = Env(db=db)
    await env.at(T0)
    assert unlisted_pages(env) == ["/".join(ACTIVE.key)]
    assert all(db.t.breaches[s.key].resolved_at == T0 for s in (PENDING, UNKNOWN_IN_GRACE, ACTIVE))
    drop = env.sweep._unlisted_drop
    assert drop is not None and drop.key == ACTIVE.key


async def test_resolve_unlisted_returns_only_the_previously_active_rows() -> None:
    db = InMemoryOverseeDb(engine_epoch=7)
    for s in (PENDING, ACTIVE):
        db.t.breaches[s.key] = s
    switch = HaltSwitch(db, db)
    resolved = await switch.resolve_unlisted({"A1"}, now_ms=T0, sweep_id=20)
    assert [s.key for s in resolved] == [ACTIVE.key]
    assert db.t.breaches[PENDING.key].resolved_at == T0 and db.t.breaches[PENDING.key].clear_generation == 1
    assert [k[:4] for k in db.t.events] == [(*ACTIVE.key, "resolved")]
