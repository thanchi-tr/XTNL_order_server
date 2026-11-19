"""Plan S38f (round 14, fix 3): the D7 error budget survives restarts and takeovers, and unclean restarts count.

* The budget's own counters (the `unresolved` streaks, the `md_down` / `pool_down` time counters, the unclean-restart
  watermark) are written fenced to `arm_budget_counters` only on change and restored by the FAILED manager at start:
  K-1 unresolved buckets, a takeover, one more bucket -> SERIES FAILED with the immediate ping.
* A late restore (the first read failed at start) merges with what the term counted meanwhile; a clean result seen
  first supersedes the stored counter; a stored counter at its threshold is filed only while its scope is open; an
  accepted clear writes its counter reset in the clear's own transaction (R14 review).
* Unclean restarts (leader terms ended `lost` / `init_failed`) within the window while positions are armed: a fault
  that crashes the engine every 2 minutes meets the SAFE_HOLD brake first (driven through the real
  `leader_init_fence`); after the operator's resume the ARM goes FAILED `unclean_restarts` on the crashes, never on
  the term SAFE_HOLD itself aborted; a clean restart never counts.
Synthetic adapters, symbols, ids and times only.
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from dataclasses import dataclass, field

import pytest

from src.db import leader as L
from src.db.arm_budget_store import SQL_UNCLEAN_ENDS
from src.db.arm_ports import BudgetCounterRow
from src.db.epoch import Epoch
from src.riskmgr import budget as B
from src.riskmgr.budget import ARM_SCOPE, ErrorBudget, series_scope
from src.riskmgr.config import FailSettings, parse_arm_settings
from src.riskmgr.failed import FailedManager, FailedSettings, FailedStores
from tests.riskmgr.failed_fakes import FailedRig, Mirror, failed_rig, row, series
from tests.riskmgr.test_failed_clear import clearing

MIN = 60_000
S_A = series("SYN.A")
ABNORMAL = ("lost", "init_failed")


@dataclass
class MemoryBudgetStore:
    """`ArmBudgetStore` as one PostgreSQL table shared by every term."""

    rows: dict[tuple[str, str, str], BudgetCounterRow] = field(default_factory=dict)
    saves: list[list[BudgetCounterRow]] = field(default_factory=list)
    down: bool = False

    async def load(self) -> list[BudgetCounterRow]:
        if self.down:
            raise ConnectionRefusedError("pool down")
        return list(self.rows.values())

    async def save(self, rows: Sequence[BudgetCounterRow], now_ms: int) -> None:
        if self.down:
            raise ConnectionRefusedError("pool down")
        self.saves.append(list(rows))
        for r in rows:
            self.rows[r.key] = r


@dataclass
class Terms:
    """`leader_terms` (end time, end reason); the port returns what `SQL_UNCLEAN_ENDS` selects."""

    ended: list[tuple[int, str]] = field(default_factory=list)

    async def unclean_ends_since(self, since_ms: int) -> list[int]:
        return sorted(t for t, why in self.ended if why in ABNORMAL and t >= since_ms)


@dataclass
class _Term:
    cluster_id: str
    revision: int
    started_at: int
    reached_leader_at: int | None = None
    ended_at: int | None = None
    end_reason: str | None = None


@dataclass
class LeaderTerms:
    """`leader_terms` and `cluster_state` for the exact statements `src.db.leader.leader_init_fence` issues, plus the
    restart port (`SQL_UNCLEAN_ENDS`): each handler applies its statement's documented semantics (not a SQL engine)."""

    terms: list[_Term] = field(default_factory=list)
    safe_hold: bool = False
    safe_hold_since: int | None = None
    last_resume_at: int | None = None

    async def execute(self, sql: str, *a: object) -> str:
        if sql == L.SQL_FENCE_ALL_STREAMS:
            return "UPDATE 0"
        if sql == L.SQL_CLOSE_PREDECESSORS:
            now, cid, rev = a
            n = 0
            for t in self.terms:
                if (t.ended_at is None or t.end_reason is None) and not (t.cluster_id == cid and t.revision == rev):
                    t.ended_at = t.ended_at if t.ended_at is not None else int(str(now))
                    t.end_reason = t.end_reason or ("init_failed" if t.reached_leader_at is None else "lost")
                    n += 1
            return f"UPDATE {n}"
        if sql == L.SQL_INSERT_TERM:
            cid, rev, _node, now = a
            self.terms.append(_Term(str(cid), int(str(rev)), int(str(now))))
            return "INSERT 0 1"
        if sql == L.SQL_SET_SAFE_HOLD:
            self.safe_hold, self.safe_hold_since = True, int(str(a[0]))
            return "INSERT 0 1"
        if sql == L.SQL_END_TERM:
            cid, rev, ended, why = a
            (t,) = [t for t in self.terms if t.cluster_id == cid and t.revision == rev]
            t.ended_at, t.end_reason = int(str(ended)), str(why)
            return "UPDATE 1"
        raise AssertionError(f"unexpected SQL: {sql[:80]}")

    async def fetchrow(self, sql: str, *a: object) -> tuple[object, ...] | None:
        if sql == L.SQL_READ_CLUSTER_STATE:
            return (self.safe_hold, self.last_resume_at)
        since, resume = int(str(a[0])), int(str(a[1] or 0))
        if sql == L.SQL_COUNT_ABNORMAL:
            return (sum(1 for t in self.terms if t.end_reason in ABNORMAL and t.ended_at is not None
                        and t.ended_at >= since and t.started_at >= since and t.ended_at > resume),)
        if sql == L.SQL_COUNT_EPOCH_CHANGES:
            return (sum(1 for t in self.terms if t.started_at >= since and t.started_at > resume),)
        raise AssertionError(f"unexpected SQL: {sql[:80]}")

    async def fetch(self, sql: str, *a: object) -> list[tuple[object, ...]]:
        raise AssertionError(f"unexpected SQL: {sql[:80]}")

    async def unclean_ends_since(self, since_ms: int) -> list[int]:
        """`SQL_UNCLEAN_ENDS`: lost / init_failed ends since `since_ms`, without a term SAFE_HOLD aborted itself."""
        return sorted(t.ended_at for t in self.terms
                      if t.end_reason in ABNORMAL and t.ended_at is not None and t.ended_at >= since_ms
                      and t.started_at >= since_ms
                      and not (t.end_reason == "init_failed" and t.reached_leader_at is None
                               and t.ended_at == t.started_at))

    def reach_leader(self, revision: int, now_ms: int) -> None:
        (t,) = [t for t in self.terms if t.revision == revision]
        t.reached_leader_at = now_ms

    def resume(self, now_ms: int) -> None:
        """The operator's audited `resume()`: SAFE_HOLD cleared, the resume time recorded."""
        self.safe_hold, self.last_resume_at = False, now_ms


def term(r: FailedRig, store: MemoryBudgetStore | None, terms: Terms | LeaderTerms | None) -> FailedManager:
    """A new term (restart or takeover) over the same PostgreSQL: a new budget and manager."""
    r.budget = ErrorBudget(r.budget.settings, calendar=r.calendar, health=r.health, pg=r.pg)
    r.mirror = Mirror()
    r.manager = FailedManager(budget=r.budget, stores=FailedStores(arm=r.arm, engine=r.engine),
                              positions=r.positions, spool=r.spool, pager=r.mirror.pager(), clock=r.clock,
                              epoch=lambda: 7, fail=r.budget.settings, settings=FailedSettings(max_positions=3),
                              answers=r.answers, budget_store=store, restarts=terms)
    return r.manager


async def unresolved_bucket(r: FailedRig, bucket_ms: int) -> None:
    r.clock.now = bucket_ms + 15 * MIN + 30_000                # a live cap just passed
    r.budget.note_unresolved(S_A, bucket_ms, bucket_ms + 15 * MIN, r.clock.now)
    await r.manager.tick()


# ---------------------------------------------------------------------------------------------------- streaks
async def test_k_minus_1_unresolved_buckets_a_takeover_and_one_more_fail_the_series() -> None:
    r = failed_rig(fail=FailSettings(unresolved_n=2))
    r.add(row("P1", symbol="SYN.A"))
    store, terms = MemoryBudgetStore(), Terms()
    m = term(r, store, terms)
    assert await m.start() is True
    b0 = r.clock.now - r.clock.now % (15 * MIN)
    await unresolved_bucket(r, b0)
    assert r.budget.unresolved_streak(S_A) == 1 and not m.scopes.is_open(series_scope(S_A))
    (saved,) = store.rows.values()
    assert (saved.scope_kind, saved.scope_id, saved.counter, saved.count) == ("SERIES", S_A.canonical,
                                                                              B.COUNTER_UNRESOLVED, 1)
    # takeover: a new leader, a new budget, the same PostgreSQL
    m = term(r, store, terms)
    assert await m.start() is True and r.budget.unresolved_streak(S_A) == 1
    await unresolved_bucket(r, b0 + 15 * MIN)
    assert m.scopes.is_open(series_scope(S_A))
    await m.tick()
    assert len(r.critical_rows()) == 1 and len(r.arm_rows()) == 1   # the immediate ping, once
    (ep,) = r.emu.t.failures.values()
    assert ep["reason"] == "unresolved" and ep["scope"] == ("SERIES", S_A.canonical)


async def test_without_the_durable_counters_the_takeover_would_reset_the_streak() -> None:
    """The round-14 failure mode, kept as the control: in memory only, a restart resets the streak."""
    r = failed_rig(fail=FailSettings(unresolved_n=2))
    r.add(row("P1", symbol="SYN.A"))
    m = term(r, None, None)
    assert await m.start() is True
    b0 = r.clock.now - r.clock.now % (15 * MIN)
    await unresolved_bucket(r, b0)
    m = term(r, None, None)
    await m.start()
    await unresolved_bucket(r, b0 + 15 * MIN)
    assert not m.scopes.is_open(series_scope(S_A)) and r.budget.unresolved_streak(S_A) == 1


async def test_counters_are_written_only_on_change_and_never_before_the_restore() -> None:
    r = failed_rig(fail=FailSettings(unresolved_n=3))
    store = MemoryBudgetStore(down=True)
    m = term(r, store, Terms())
    await m.start()
    b0 = r.clock.now - r.clock.now % (15 * MIN)
    await unresolved_bucket(r, b0)                 # counted in memory; the store is unreadable: nothing written
    assert store.saves == [] and m.counters.budget_errors >= 2
    store.rows[(("SERIES", S_A.canonical, B.COUNTER_UNRESOLVED))] = BudgetCounterRow(
        scope_kind="SERIES", scope_id=S_A.canonical, counter=B.COUNTER_UNRESOLVED, count=1, streak_start_ms=b0 - 1,
        last_event_ms=b0 - 15 * MIN)
    store.down = False
    await m.tick()                                  # restored (memory's own streak kept), then written once
    assert len(store.saves) == 1 and store.rows[("SERIES", S_A.canonical, "unresolved")].last_event_ms == b0
    await m.tick()
    await m.tick()
    assert len(store.saves) == 1                    # unchanged: no write
    r.budget.note_final(S_A, b0 + 15 * MIN)         # the clean result: the counter goes back to rest
    await m.tick()
    assert store.rows[("SERIES", S_A.canonical, "unresolved")].count == 0 and len(store.saves) == 2


async def test_a_late_restore_merges_this_terms_count_and_fails_the_series_at_k() -> None:
    """R14 review: the store is down at start(), one unresolved bucket is counted in memory, the store returns and
    the next tick restores: the stored K-1 and this term's 1 are one streak of K -> SERIES FAILED."""
    r = failed_rig(fail=FailSettings(unresolved_n=3))
    r.add(row("P1", symbol="SYN.A"))
    b0 = r.clock.now - r.clock.now % (15 * MIN)
    key = ("SERIES", S_A.canonical, B.COUNTER_UNRESOLVED)
    store = MemoryBudgetStore(rows={key: BudgetCounterRow(
        scope_kind="SERIES", scope_id=S_A.canonical, counter=B.COUNTER_UNRESOLVED, count=2,
        streak_start_ms=b0 - 25 * MIN, last_event_ms=b0 - 15 * MIN)}, down=True)
    m = term(r, store, Terms())
    assert await m.start() is True                    # the open episodes are readable; the counters are not
    await unresolved_bucket(r, b0)
    assert r.budget.unresolved_streak(S_A) == 1 and not m.scopes.is_open(series_scope(S_A))
    assert store.saves == []
    store.down = False
    await m.tick()
    assert r.budget.unresolved_streak(S_A) == 3 and m.scopes.is_open(series_scope(S_A))
    (ep,) = r.emu.t.failures.values()
    assert ep["reason"] == "unresolved" and json.loads(str(ep["counters"]))["unresolved_buckets"] == 3
    assert ep["first_fault_at_ms"] == b0 - 25 * MIN    # the streak started in the earlier term
    assert store.rows[key].count == 3


async def test_a_clean_result_before_a_late_restore_supersedes_the_stored_streak() -> None:
    r = failed_rig(fail=FailSettings(unresolved_n=3))
    r.add(row("P1", symbol="SYN.A"))
    b0 = r.clock.now - r.clock.now % (15 * MIN)
    key = ("SERIES", S_A.canonical, B.COUNTER_UNRESOLVED)
    store = MemoryBudgetStore(rows={key: BudgetCounterRow(
        scope_kind="SERIES", scope_id=S_A.canonical, counter=B.COUNTER_UNRESOLVED, count=2,
        streak_start_ms=b0 - 25 * MIN, last_event_ms=b0 - 15 * MIN)}, down=True)
    m = term(r, store, Terms())
    await m.start()
    r.budget.note_final(S_A, b0)                       # a final bar this term: the streak is over
    store.down = False
    await m.tick()
    assert r.budget.unresolved_streak(S_A) == 0 and store.rows[key].count == 0
    await unresolved_bucket(r, b0 + 15 * MIN)
    assert r.budget.unresolved_streak(S_A) == 1 and not m.scopes.is_open(series_scope(S_A))


def test_a_late_restore_adds_the_stored_time_and_files_at_the_limit() -> None:
    r = failed_rig(fail=FailSettings(md_down_s=300))
    t = r.clock.now
    b = ErrorBudget(r.budget.settings, calendar=r.calendar, health=r.health, pg=r.pg)
    for i in range(0, 120_001, 10_000):               # 120 s this term, before the restore
        b.observe_md(usable=False, market_open=True, now_ms=t + i)
    assert b.take() == []
    stored = BudgetCounterRow(scope_kind="ARM", scope_id="arm", counter=B.COUNTER_MD_DOWN, count=200_000,
                              streak_start_ms=t - 3_600_000)
    assert b.restore([stored]) == 1
    (fault,) = b.take()                                # 200 s stored + 120 s now >= 300 s
    assert fault.reason == B.MD_DOWN and fault.first_fault_at_ms == t - 3_600_000
    usable = ErrorBudget(r.budget.settings, calendar=r.calendar, health=r.health, pg=r.pg)
    usable.observe_md(usable=True, market_open=True, now_ms=t)    # redis-md usable this term: the outage is over
    assert usable.restore([stored]) == 0 and usable.durable_rows() == {}


def test_a_restored_counter_at_its_threshold_files_again_unless_its_scope_is_open() -> None:
    r = failed_rig(fail=FailSettings(unresolved_n=2))
    b0 = r.clock.now - r.clock.now % (15 * MIN)
    at_n = BudgetCounterRow(scope_kind="SERIES", scope_id=S_A.canonical, counter=B.COUNTER_UNRESOLVED, count=2,
                            streak_start_ms=b0 - 20 * MIN, last_event_ms=b0 - 15 * MIN)
    for open_scopes, files in (((), True), ((series_scope(S_A),), False)):
        b = ErrorBudget(r.budget.settings, calendar=r.calendar, health=r.health, pg=r.pg)
        b.restore([at_n], open_scopes)
        assert b.take() == []                          # nothing filed by the restore itself
        r.clock.now = now = b0 + 15 * MIN + 30_000
        b.note_unresolved(S_A, b0, b0 + 15 * MIN, now)
        assert [f.reason for f in b.take()] == (["unresolved"] if files else []), open_scopes


def test_reset_rows_equal_what_the_manager_would_write_after_the_reset() -> None:
    r = failed_rig()
    now = r.clock.now
    b = ErrorBudget(r.budget.settings, calendar=r.calendar, health=r.health, pg=r.pg)
    b.observe_md(usable=False, market_open=True, now_ms=now)
    b.note_restarts([now - MIN], now, armed=True)
    rows = b.reset_rows(ARM_SCOPE, now + 5)
    assert {(x.counter, x.count, x.streak_start_ms, x.last_event_ms) for x in rows} == {
        (B.COUNTER_MD_DOWN, 0, None, None), (B.COUNTER_POOL_DOWN, 0, None, None),
        (B.COUNTER_RESTARTS, 0, None, now + 5)}
    b.reset(ARM_SCOPE, now + 5)
    (after,) = b.durable_rows().values()
    assert after in rows                               # the watermark row the clear already wrote: no rewrite
    (s_row,) = b.reset_rows(series_scope(S_A), now)
    assert (s_row.scope_kind, s_row.counter, s_row.count) == ("SERIES", B.COUNTER_UNRESOLVED, 0)
    assert b.reset_rows(B.position_scope("mock:P1"), now) == []


async def test_an_accepted_clear_resets_the_stored_streak_in_the_clear_transaction() -> None:
    """R14 review: the reset is not left to the next tick's ARM-pool write; it commits with the clear."""
    from src.db.arm_budget_store import SQL_BUDGET_UPSERT
    from src.db.arm_failures_store import SQL_FAILURE_CLEAR
    from src.db.epoch import SQL_COMMIT
    c = await clearing(fail=FailSettings(unresolved_n=2), rows=(row("P1", symbol="SYN.A"),), budget_store=True)
    r = c.rig
    b0 = r.clock.now - r.clock.now % (15 * MIN) + 15 * MIN
    for b in (b0, b0 + 15 * MIN):
        r.clock.now = b + 15 * MIN + 30_000
        r.budget.note_unresolved(S_A, b, b + 15 * MIN, r.clock.now)
        await c.m.tick()
    key = ("SERIES", S_A.canonical, B.COUNTER_UNRESOLVED)
    assert c.m.scopes.is_open(series_scope(S_A)) and r.emu.t.budget[key][0] == 2
    r.advance(MIN)
    rid = c.request(f"SERIES:{S_A.canonical}")
    mark = len(r.emu.sql)
    await c.m.tick()
    for _ in range(5):
        await asyncio.sleep(0)
    await c.m.tick()
    assert c.answer(rid) == ("applied", "")
    sql = r.emu.sql[mark:]
    clear_at = sql.index(SQL_FAILURE_CLEAR)
    upsert_at = sql.index(SQL_BUDGET_UPSERT, clear_at)
    assert upsert_at < sql.index(SQL_COMMIT, clear_at)  # inside the clear's transaction
    assert SQL_BUDGET_UPSERT not in sql[upsert_at + 1:]  # and not written again by the next tick
    assert r.emu.t.budget[key][0] == 0


def test_time_counters_restore_their_total_and_never_add_the_downtime() -> None:
    r = failed_rig(fail=FailSettings(md_down_s=300))
    b = r.budget
    t = r.clock.now
    for i in range(0, 200_001, 10_000):            # 200 s unusable while a market is OPEN
        b.observe_md(usable=False, market_open=True, now_ms=t + i)
    rows = list(b.durable_rows().values())
    (md,) = [x for x in rows if x.counter == B.COUNTER_MD_DOWN]
    assert md.count == 200_000 and md.last_event_ms is None
    fresh = ErrorBudget(b.settings, calendar=r.calendar, health=r.health, pg=r.pg)
    assert fresh.restore(rows) == 1
    later = t + 3_600_000                           # an hour later (the engine was down): adds nothing
    fresh.observe_md(usable=False, market_open=True, now_ms=later)
    assert fresh.take() == []
    for i in range(10_000, 100_001, 10_000):        # 100 s more: 300 s in all
        fresh.observe_md(usable=False, market_open=True, now_ms=later + i)
    (fault,) = fresh.take()
    assert fault.scope == ARM_SCOPE and fault.reason == B.MD_DOWN


# ---------------------------------------------------------------------------------------------------- restarts
async def test_a_crash_loop_meets_safe_hold_first_and_the_arm_fails_after_the_resume() -> None:
    """The real path (R14 review): crashes 2 minutes apart, each restart through `leader_init_fence`'s own counting.
    Terms 2 and 3 see 1 and 2 unclean ends (below N=3); the 4th LEADER_INIT finds 3 abnormal ends within 15 min, sets
    SAFE_HOLD and ends its own term `init_failed`: no node leads, no ARM runs (the brake preempts). The operator
    resumes; the next term's ARM counts the 3 crashes, never the term SAFE_HOLD aborted, and goes FAILED."""
    r = failed_rig()
    r.add(row("P1", symbol="SYN.A", state="TRACKING"))
    store, lt = MemoryBudgetStore(), LeaderTerms()
    revision = 0

    async def leader_init() -> L.LeaderInitFence:
        nonlocal revision
        revision += 1
        return await L.leader_init_fence(lt, Epoch("c", revision), "node-a", r.clock.now)

    async def lead() -> FailedManager:
        lt.reach_leader(revision, r.clock.now)
        m = term(r, store, lt)
        assert await m.start() is True
        await m.tick()
        return m

    assert not (await leader_init()).safe_hold
    m = await lead()
    assert not m.scopes.is_open(ARM_SCOPE)
    held = None
    for crash in range(1, 6):
        r.advance(2 * MIN)                             # the fault kills the engine; it restarts and campaigns
        fence = await leader_init()
        if fence.safe_hold:
            held = crash
            break
        m = await lead()
        assert not m.scopes.is_open(ARM_SCOPE), crash  # 1, then 2 unclean ends: below N
    assert held == 3 and lt.safe_hold and r.emu.t.failures == {}
    aborted = lt.terms[-1]
    assert (aborted.end_reason, aborted.reached_leader_at, aborted.ended_at) == ("init_failed", None,
                                                                                 aborted.started_at)
    r.advance(10 * MIN)
    lt.resume(r.clock.now)                             # the operator, after fixing the cause
    r.advance(MIN)
    assert not (await leader_init()).safe_hold         # SAFE_HOLD counts only ends after the resume
    m = await lead()
    assert m.scopes.is_open(ARM_SCOPE)                 # the ARM's own budget is not cleared by the resume
    await m.tick()
    (ep,) = r.emu.t.failures.values()
    assert ep["reason"] == B.UNCLEAN_RESTARTS and ep["scope"] == ("ARM", "arm") and ep["probe_kind"] == "arm_global"
    assert len(r.critical_rows()) == 1 and len(r.arm_rows()) == 1
    assert json.loads(str(ep["counters"]))["unclean_restarts"] == 3      # the aborted term is not a crash
    assert "reached_leader_at IS NULL AND ended_at = started_at" in SQL_UNCLEAN_ENDS


async def test_a_resume_after_the_window_leaves_the_arm_running() -> None:
    r = failed_rig()
    r.add(row("P1", symbol="SYN.A", state="TRACKING"))
    lt = LeaderTerms()
    for rev in range(1, 5):
        fence = await L.leader_init_fence(lt, Epoch("c", rev), "node-a", r.clock.now)
        if not fence.safe_hold:
            lt.reach_leader(rev, r.clock.now)
        r.advance(2 * MIN)
    assert lt.safe_hold
    r.advance(40 * MIN)
    lt.resume(r.clock.now)
    assert not (await L.leader_init_fence(lt, Epoch("c", 5), "node-a", r.clock.now)).safe_hold
    lt.reach_leader(5, r.clock.now)
    m = term(r, MemoryBudgetStore(), lt)
    await m.start()
    await m.tick()
    assert not m.scopes.is_open(ARM_SCOPE)             # the crashes are older than the 30 min window


@pytest.mark.parametrize("reason", ["switchover", "abdicate"])
async def test_a_clean_restart_does_not_count(reason: str) -> None:
    r = failed_rig()
    r.add(row("P1", symbol="SYN.A", state="TRACKING"))
    terms = Terms([(r.clock.now - 10 * MIN, "lost"), (r.clock.now - 6 * MIN, "lost")])
    for _ in range(5):
        r.advance(MIN)
        terms.ended.append((r.clock.now, reason))
        m = term(r, MemoryBudgetStore(), terms)
        await m.start()
        await m.tick()
        assert not m.scopes.is_open(ARM_SCOPE)


async def test_restarts_count_only_while_positions_are_armed_and_inside_the_window() -> None:
    r = failed_rig()
    terms = Terms([(r.clock.now - 3 * MIN, "lost"), (r.clock.now - 2 * MIN, "init_failed"),
                   (r.clock.now - MIN, "lost")])
    r.add(row("P1", symbol="SYN.A", state="DISCOVERED"))
    m = term(r, MemoryBudgetStore(), terms)
    await m.start()
    await m.tick()
    assert not m.scopes.is_open(ARM_SCOPE)         # nothing armed: no advice to protect
    r2 = failed_rig()
    r2.add(row("P1", symbol="SYN.A", state="TRACKING"))
    old = Terms([(r2.clock.now - 45 * MIN, "lost"), (r2.clock.now - 40 * MIN, "lost"), (r2.clock.now - MIN, "lost")])
    m2 = term(r2, MemoryBudgetStore(), old)
    await m2.start()
    await m2.tick()
    assert not m2.scopes.is_open(ARM_SCOPE)        # two of three are older than the 30 min window


def test_a_cleared_episode_never_re_trips_on_the_same_restarts() -> None:
    r = failed_rig()
    now = r.clock.now
    b = ErrorBudget(r.budget.settings, calendar=r.calendar, health=r.health, pg=r.pg)   # no listener: faults queue
    ends = [now - 5 * MIN, now - 3 * MIN, now - MIN]
    b.note_restarts(ends, now, armed=True)
    (fault,) = b.take()
    assert fault.reason == B.UNCLEAN_RESTARTS and fault.first_fault_at_ms == ends[0]
    b.reset(ARM_SCOPE, now + 1)                      # the accepted clear
    b.note_restarts(ends, now + 2, armed=True)
    assert b.take() == []
    rows = list(b.durable_rows().values())          # the watermark survives the next restart too
    fresh = ErrorBudget(b.settings, calendar=r.calendar, health=r.health, pg=r.pg)
    fresh.restore(rows)
    fresh.note_restarts(ends, now + 3, armed=True)
    assert fresh.take() == []
    fresh.note_restarts([*ends, now + 10, now + 20, now + 30], now + 40, armed=True)
    (again,) = fresh.take()
    assert dict(again.counters)["unclean_restarts"] == 3


def test_restart_settings_parse_with_their_bounds() -> None:
    s = parse_arm_settings({})
    assert not isinstance(s, list) and (s.fail.restarts_n, s.fail.restart_window_s) == (3, 1800)
    bad = parse_arm_settings({"XTNL_ARM_FAIL_RESTARTS_N": "1", "XTNL_ARM_FAIL_RESTART_WINDOW_S": "99999"})
    assert isinstance(bad, list) and len(bad) == 2 and all("XTNL_ARM_FAIL_RESTART" in p for p in bad)
    assert B.UNCLEAN_RESTARTS in B.ARM_REASONS and B.probe_kind(ARM_SCOPE.kind, B.UNCLEAN_RESTARTS) == "arm_global"


def test_the_arm_graph_wires_the_durable_store_and_the_restart_history() -> None:
    from pathlib import Path
    text = (Path(__file__).resolve().parents[2] / "src/runtime/optional_plan.py").read_text(encoding="utf-8")
    assert "budget_store=PgArmBudgetStore(arm.db)," in text
    # S38g: the restart history is read through the sleep filter (a PC sleep's own restart is not unclean)
    assert "restarts=SleepAwareRestarts(PgRestartHistory(engine_db.source), self._sleep_fences)" in text
