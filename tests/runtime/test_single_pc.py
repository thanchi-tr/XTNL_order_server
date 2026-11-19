"""Single-PC SAFE_HOLD auto-resume and the not-recording page (src/runtime/single_pc.py; operations.md §4, §17).

Flag off: SAFE_HOLD stays operator-only (no store call, ever). Flag on: the node resumes the hold itself once its hard
checks pass and the cool-down has run (5 min doubling per earlier auto-resume within 24 h, capped at 60 min), with
the operator CLI's fenced resume + one operator_audit row; after 6 within 24 h it stays held and pages critical (straight
to the alert channels: there is no leader then); a person's resume starts a fresh budget; a hold left by an earlier
run counts the same; failing hard checks never resume; only the hold that was judged is cleared.
"""
from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pytest

from src.db import cluster_state as C
from src.notifications.settings import NotificationSettings
from src.runtime.etcd import LEADER_KEY
from src.runtime.lease import LeaseKeeper
from src.runtime.health import HealthBoard
from src.runtime.node import NodeController
from src.runtime.roles import ClusterState, NodeRole
from src.runtime.single_pc import (
    AUDIT_COMMAND,
    AUDIT_OPERATOR,
    AUTO_RESUME_ACTOR,
    EXHAUSTED_TITLE,
    IDLE_TITLE,
    RECORDING_TITLE,
    RESUMED_TITLE,
    DirectChannelPager,
    NotRecordingWatch,
    PgAutoResumeStore,
    ResumeRecord,
    SafeHoldAutoResume,
    SinglePcSettings,
    single_pc_settings_from_env,
)
from tests.db.conftest import FakeConn, FakeSource
from tests.runtime.fakes import record
from tests.runtime.test_node import World

R = NodeRole
DAY_MS = 86_400_000
HOLD_SINCE_MS = 500_000          # the hold's safe_hold_since (World's clock starts at 1 000 000 ms)


# ============================================================================================ fakes
@dataclass
class FakeStore:
    """`prior` stands for the audit rows the count sees; the real count SQL is checked in test_single_pc_pg.py.
    `resume` behaves as SQL_SAFE_HOLD_AUTO_RESUME: only the judged hold (since, reason) is cleared."""
    world: World
    prior: int = 0
    counted_since: list[int] = field(default_factory=list)
    resumes: list[tuple[str, dict[str, object]]] = field(default_factory=list)
    fail: bool = False
    before_resume: Callable[[], None] | None = None

    async def count_auto_resumes(self, since_ms: int) -> int:
        self.counted_since.append(since_ms)
        return self.prior

    async def resume(self, reason: str, detail: Mapping[str, object], held_since: int | None,
                     held_reason: str | None) -> ResumeRecord | None:
        if self.fail:
            raise ConnectionError("postgres down")
        if self.before_resume is not None:
            self.before_resume()
        self.resumes.append((reason, dict(detail)))
        cs = self.world.cluster
        if not cs.safe_hold or (cs.safe_hold_since, cs.safe_hold_reason) != (held_since, held_reason):
            return None
        now = self.world.now_ms()
        self.world.cluster = ClusterState(False, cs.safe_hold_since, cs.safe_hold_reason, now)
        return ResumeRecord(7, len(self.resumes), now, cs.safe_hold_since, cs.safe_hold_reason)


@dataclass
class FakePager:
    pages: list[tuple[str, str, str]] = field(default_factory=list)

    def page(self, severity: str, title: str, body: str) -> None:
        self.pages.append((severity, title, body))

    def titles(self) -> list[str]:
        return [t for _, t, _ in self.pages]


def held() -> ClusterState:
    return ClusterState(safe_hold=True, safe_hold_since=HOLD_SINCE_MS, safe_hold_reason="3 abnormal term ends")


def controller(w: World, *, settings: SinglePcSettings | None, store: FakeStore | None = None,
               pager: FakePager | None = None, idle: bool = False,
               direct: FakePager | None = None) -> NodeController:
    keeper = LeaseKeeper(w.etcd, clock=w.mono, fence=w.exits)
    auto = None if settings is None or store is None else SafeHoldAutoResume(settings, store)
    watch = NotRecordingWatch(settings.idle_page_s * 1000) if idle and settings is not None else None
    return NodeController("n1", keeper=keeper, board=HealthBoard(w.etcd, "n1"), probe=w, terms=w, cluster=w,
                          local_redis=w, session_factory=w.factory, pager=pager, exit_fn=w.exits,
                          clock_ms=w.now_ms, monotonic=w.mono, step_interval_s=0.001, require_keeper_thread=False,
                          auto_resume=auto, idle_watch=watch, direct_pager=direct)


ON = SinglePcSettings(enabled=True)


async def run_for(c: NodeController, w: World, seconds: float, *, step_s: float = 10.0) -> None:
    """Step the controller while the fake clock moves `seconds` forward."""
    elapsed = 0.0
    while elapsed < seconds:
        w.mono.advance(step_s)
        elapsed += step_s
        await c.step()


# ============================================================================================ settings
def test_flag_is_off_unless_exactly_1_and_refused_under_systemd() -> None:
    assert not single_pc_settings_from_env({}).enabled
    assert not single_pc_settings_from_env({"XTNL_SINGLE_PC": "0"}).enabled
    assert single_pc_settings_from_env({"XTNL_SINGLE_PC": " 1 "}).enabled
    bad = single_pc_settings_from_env({"XTNL_SINGLE_PC": "true"})
    assert not bad.enabled and any("neither 0 nor 1" in p for p in bad.problems)
    aws = single_pc_settings_from_env({"XTNL_SINGLE_PC": "1", "NOTIFY_SOCKET": "/run/systemd/notify"})
    assert not aws.enabled and any("refused under systemd" in p for p in aws.problems)


def test_defaults_and_tolerant_numbers() -> None:
    s = single_pc_settings_from_env({"XTNL_SINGLE_PC": "1"})
    assert (s.resume_first_s, s.resume_max_s, s.resume_limit, s.resume_window_s, s.idle_page_s) == (
        300, 3600, 6, 86_400, 600)
    assert not s.problems
    odd = single_pc_settings_from_env({"XTNL_SINGLE_PC": "1", "XTNL_SINGLE_PC_RESUME_FIRST_S": "abc",
                                       "XTNL_SINGLE_PC_RESUME_LIMIT": "0", "XTNL_SINGLE_PC_RESUME_MAX_S": "10"})
    assert odd.enabled and odd.resume_first_s == 300 and odd.resume_limit == 6 and odd.resume_max_s == 300
    assert len(odd.problems) == 3


def test_cooldown_doubles_from_5_min_and_caps_at_60() -> None:
    assert [ON.cooldown_s(n) for n in range(7)] == [300, 600, 1200, 2400, 3600, 3600, 3600]


# ============================================================================================ flag off
async def test_flag_off_never_auto_resumes() -> None:
    w = World(cluster=held())
    store = FakeStore(w)
    c = controller(w, settings=None)
    await c.startup()
    assert c.role is R.SAFE_HOLD
    await run_for(c, w, 3 * 3600, step_s=60)
    assert c.role is R.SAFE_HOLD and w.cluster.safe_hold
    assert store.counted_since == [] and store.resumes == [] and LEADER_KEY not in w.etcd.kv


# ============================================================================================ flag on
async def test_hold_from_an_earlier_run_is_resumed_after_the_cooldown_and_the_node_records() -> None:
    w = World(cluster=held())                         # set before this process started
    store = FakeStore(w)
    pager = FakePager()
    c = controller(w, settings=ON, store=store, pager=pager)
    await c.startup()
    assert c.role is R.SAFE_HOLD
    await c.step()                                    # hard checks pass: the cool-down starts
    await run_for(c, w, 290)
    assert c.role is R.SAFE_HOLD and store.resumes == []
    w.mono.advance(10)
    await c.step()                                    # 300 s since the cool-down started
    assert c.role is R.STANDBY and len(store.resumes) == 1
    assert store.counted_since == [HOLD_SINCE_MS - DAY_MS]      # the budget is anchored at the hold
    reason, detail = store.resumes[0]
    assert "auto-resume 1 of 6" in reason and "3 abnormal term ends" in reason and len(reason) >= 8
    assert detail["cooldown_s"] == 300 and detail["auto_resume"] == 1
    assert RESUMED_TITLE not in pager.titles()        # only the leader sends: held for the next LEADER entry
    await c.step()                                    # campaigns normally
    assert c.role is R.LEADER
    assert pager.pages[-1][:2] == ("info", RESUMED_TITLE)
    assert "operator_audit 1" in pager.pages[-1][2]


async def test_hold_seen_while_running_is_resumed_too() -> None:
    w = World()
    store = FakeStore(w)
    c = controller(w, settings=ON, store=store)
    await c.startup()
    assert c.role is R.STANDBY
    w.cluster = held()
    await c.step()
    assert c.role is R.SAFE_HOLD
    await run_for(c, w, 320)
    assert len(store.resumes) == 1 and c.role in (R.STANDBY, R.LEADER)


@pytest.mark.parametrize("prior, cooldown_s", [(0, 300), (1, 600), (2, 1200), (3, 2400), (4, 3600), (5, 3600)])
async def test_back_off_doubles_per_earlier_auto_resume(prior: int, cooldown_s: int) -> None:
    w = World(cluster=held())
    store = FakeStore(w, prior=prior)
    c = controller(w, settings=ON, store=store)
    await c.startup()
    await c.step()
    await run_for(c, w, cooldown_s - 20)
    assert store.resumes == [] and c.role is R.SAFE_HOLD
    await run_for(c, w, 30)
    assert len(store.resumes) == 1
    assert store.resumes[0][1]["cooldown_s"] == cooldown_s and store.resumes[0][1]["auto_resume"] == prior + 1


async def test_stops_after_6_within_24h_and_pages_critical_once() -> None:
    w = World(cluster=held())
    store = FakeStore(w, prior=6)
    pager = FakePager()
    direct = FakePager()
    c = controller(w, settings=ON, store=store, pager=pager, direct=direct)
    await c.startup()
    await run_for(c, w, 2 * 86_400, step_s=600)       # time passing never changes the decision for this hold
    assert c.role is R.SAFE_HOLD and store.resumes == [] and w.cluster.safe_hold
    assert pager.titles().count(EXHAUSTED_TITLE) == 1
    sev, _, body = next(p for p in pager.pages if p[1] == EXHAUSTED_TITLE)
    assert sev == "critical" and "safe-hold resume" in body
    assert store.counted_since == [HOLD_SINCE_MS - DAY_MS]
    # no leader session: the critical page also goes straight to the alert channels, once
    assert direct.titles() == [EXHAUSTED_TITLE] and direct.pages[0][0] == "critical"


async def test_after_an_operator_resume_a_new_hold_gets_a_fresh_budget() -> None:
    """Budget used up -> a person resumes -> a new hold in the same 24 h is auto-resumed again. The count it reads
    then is 0 (SQL_COUNT_AUTO_RESUMES counts only auto-resumes after the last resume by anyone else; proven against
    PostgreSQL in test_single_pc_pg.py), so the new hold is auto-resume 1 with the first cool-down."""
    w = World(cluster=held())
    store = FakeStore(w, prior=6)
    c = controller(w, settings=ON, store=store)
    await c.startup()
    await run_for(c, w, 60)
    assert c.role is R.SAFE_HOLD and store.resumes == []
    w.cluster = ClusterState(last_resume_at=w.now_ms())          # the operator's two-person resume
    store.prior = 0                                              # the 6 earlier auto-resumes precede it
    await c.step()
    assert c.role in (R.STANDBY, R.LEADER)
    w.cluster = ClusterState(True, w.now_ms(), "7 epoch changes within 30 min")
    if c.role is R.LEADER:
        c.request_drain("switchover", exit_after=False)
        await c.step()
    await c.step()
    assert c.role is R.SAFE_HOLD
    await run_for(c, w, 300 + 30)
    assert len(store.resumes) == 1 and store.resumes[0][1]["auto_resume"] == 1
    assert store.resumes[0][1]["cooldown_s"] == 300
    assert len(store.counted_since) == 2                         # each hold is counted once, afresh


async def test_a_newer_hold_set_during_the_resume_is_not_cleared_but_judged_afresh() -> None:
    w = World(cluster=held())
    newer = ClusterState(True, HOLD_SINCE_MS + 1_000_000, "7 epoch changes within 30 min")

    def replace_hold() -> None:
        if w.cluster.safe_hold_since == HOLD_SINCE_MS:
            w.cluster = newer

    store = FakeStore(w, before_resume=replace_hold)
    c = controller(w, settings=ON, store=store)
    await c.startup()
    await run_for(c, w, 320)
    assert len(store.resumes) == 1 and w.cluster == newer and c.role is R.SAFE_HOLD   # the newer hold stays set
    await run_for(c, w, 320)
    assert len(store.resumes) == 2 and not w.cluster.safe_hold                       # after its own cool-down
    assert store.counted_since == [HOLD_SINCE_MS - DAY_MS, newer.safe_hold_since - DAY_MS]


async def test_failing_hard_checks_never_resume_and_restart_the_cooldown() -> None:
    w = World(cluster=held(), probe_kw={"pg_writable": False})
    store = FakeStore(w)
    c = controller(w, settings=ON, store=store)
    await c.startup()
    await run_for(c, w, 3600, step_s=60)
    assert store.resumes == [] and c.role is R.SAFE_HOLD
    for failing in ({"etcd_reachable": False}, {"redis_writable": False}, {"chrony_offset_ms": 80.0},
                    {"chrony_offset_ms": None}):
        w.probe_kw = failing
        await run_for(c, w, 900, step_s=60)
        assert store.resumes == [], failing
    w.probe_kw = {}
    await run_for(c, w, 200)
    w.probe_kw = {"pg_writable": False}                # a failure inside the cool-down restarts it
    await run_for(c, w, 30)
    w.probe_kw = {}
    await run_for(c, w, 200)
    assert store.resumes == []
    await run_for(c, w, 120)
    assert len(store.resumes) == 1


async def test_a_failed_resume_is_retried_and_never_raises() -> None:
    w = World(cluster=held())
    store = FakeStore(w, fail=True)
    c = controller(w, settings=ON, store=store)
    await c.startup()
    await run_for(c, w, 320)
    assert c.role is R.SAFE_HOLD
    store.fail = False
    await run_for(c, w, 40)
    assert len(store.resumes) == 1 and c.role in (R.STANDBY, R.LEADER)


# ============================================================================================ not-recording page
def test_not_recording_watch_pages_once_and_reports_recovery() -> None:
    watch = NotRecordingWatch(600_000)
    assert watch.observe(0, R.STANDBY) is None
    assert watch.observe(599_000, R.SAFE_HOLD) is None
    page = watch.observe(600_000, R.SAFE_HOLD, "SAFE_HOLD set")
    assert page is not None and page.severity == "critical" and page.title == IDLE_TITLE
    assert "SAFE_HOLD set" in page.body and "10 min" in page.body
    assert watch.observe(5_000_000, R.STANDBY) is None
    back = watch.observe(5_100_000, R.LEADER)
    assert back is not None and back.severity == "info" and back.title == RECORDING_TITLE
    assert watch.observe(5_200_000, R.LEADER) is None


async def test_controller_pages_not_recording_when_held_with_the_budget_used_up() -> None:
    w = World(cluster=held())
    pager = FakePager()
    direct = FakePager()
    c = controller(w, settings=ON, store=FakeStore(w, prior=6), pager=pager, idle=True, direct=direct)
    await c.startup()
    await run_for(c, w, 700, step_s=60)
    idle = [p for p in pager.pages if p[1] == IDLE_TITLE]
    assert len(idle) == 1 and idle[0][0] == "critical" and "auto-resume budget used up" in idle[0][2]
    assert direct.titles() == [EXHAUSTED_TITLE, IDLE_TITLE]


async def test_info_pages_and_leader_pages_never_take_the_direct_path() -> None:
    w = World(cluster=held())
    pager = FakePager()
    direct = FakePager()
    c = controller(w, settings=ON, store=FakeStore(w), pager=pager, idle=True, direct=direct)
    await c.startup()
    await run_for(c, w, 320)                          # resumed within the idle limit, then LEADER
    assert c.role is R.LEADER and RESUMED_TITLE in pager.titles()
    assert direct.pages == []


async def test_direct_page_goes_out_on_the_configured_push_channel_and_retries() -> None:
    posts: list[httpx.Request] = []
    answers = [500, 200]

    def handler(req: httpx.Request) -> httpx.Response:
        posts.append(req)
        return httpx.Response(answers.pop(0) if answers else 200)

    settings = NotificationSettings.from_env({"XTNL_PUSH_PROVIDER": "ntfy",
                                              "XTNL_PUSH_URL": "https://ntfy.example.test/xtnl"})
    clients: list[httpx.AsyncClient] = []

    def client() -> httpx.AsyncClient:
        clients.append(httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        return clients[-1]

    pager = DirectChannelPager(lambda: settings, client, clock_ms=lambda: 1_000, retry_s=(0.01, 0.01))
    pager.page("critical", EXHAUSTED_TITLE, "SAFE_HOLD since x; 6 auto-resumes")
    await pager.wait_idle()
    assert len(posts) == 2                            # failed once (500), delivered on the retry
    assert EXHAUSTED_TITLE[:20] in posts[-1].headers["Title"]
    assert all(c.is_closed for c in clients)


async def test_direct_page_without_a_channel_is_logged_only(caplog: pytest.LogCaptureFixture) -> None:
    calls: list[int] = []

    def client() -> httpx.AsyncClient:
        calls.append(1)
        return httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))

    pager = DirectChannelPager(lambda: NotificationSettings.from_env({}), client, clock_ms=lambda: 0)
    pager.page("critical", IDLE_TITLE, "no leader term for 10 min")
    await pager.wait_idle()
    assert "no alert channel configured" in caplog.text
    assert len(calls) == 1


async def test_direct_page_aclose_cancels_a_pending_retry() -> None:
    def client() -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(503)))

    settings = NotificationSettings.from_env({"XTNL_PUSH_PROVIDER": "ntfy",
                                              "XTNL_PUSH_URL": "https://ntfy.example.test/xtnl"})
    pager = DirectChannelPager(lambda: settings, client, clock_ms=lambda: 0, retry_s=(3600.0,))
    pager.page("critical", IDLE_TITLE, "body")
    await asyncio.sleep(0.05)
    await asyncio.wait_for(pager.aclose(), 2)


# ============================================================================================ PostgreSQL store
def test_resume_sql_is_the_operator_clis() -> None:
    from src.ops import sql as S
    assert C.SQL_SAFE_HOLD_RESUME == S.SQL_SAFE_HOLD_RESUME
    assert C.SQL_AUDIT_INSERT == S.SQL_AUDIT_INSERT
    # the auto-resume's form is the operator's plus the judged-hold match, nothing else
    extra = ("safe_hold_since IS NOT DISTINCT FROM $3::bigint AND safe_hold_reason IS NOT DISTINCT FROM $4::text AND ")
    assert C.SQL_SAFE_HOLD_AUTO_RESUME.replace(extra, "") == S.SQL_SAFE_HOLD_RESUME


def test_audit_operator_passes_migration_0007_checks() -> None:
    mig = (Path(__file__).resolve().parents[2] / "src" / "db" / "migrations" / "0007_operator_audit.sql").read_text(
        encoding="utf-8")
    assert "operator ~ '^[A-Za-z0-9._@-]{2,64}$' AND lower(operator) <> 'system'" in mig
    assert re.fullmatch(r"[A-Za-z0-9._@-]{2,64}", AUDIT_OPERATOR) and AUDIT_OPERATOR.lower() != "system"
    assert AUTO_RESUME_ACTOR == "system:single-pc-auto-resume"


async def test_pg_store_resumes_fenced_with_one_audit_row_then_mirrors() -> None:
    conn = FakeConn()
    conn.returns(C.SQL_SAFE_HOLD_AUTO_RESUME, (HOLD_SINCE_MS, "3 abnormal term ends", 1_000_000))
    conn.returns(C.SQL_AUDIT_INSERT, (42, 1_000_000))
    mirrored: list[bool] = []

    async def mirror() -> bool:
        mirrored.append(True)
        return True

    store = PgAutoResumeStore(FakeSource(conn), mirror=mirror)
    rec = await store.resume("single-PC auto-resume 1 of 6 after a 300 s cool-down; held: x", {"auto_resume": 1},
                             HOLD_SINCE_MS, "3 abnormal term ends")
    assert rec == ResumeRecord(7, 42, 1_000_000, HOLD_SINCE_MS, "3 abnormal term ends")
    # fenced on the stored epoch, and only the judged hold
    assert conn.args_of(C.SQL_SAFE_HOLD_AUTO_RESUME) == [(7, AUTO_RESUME_ACTOR, HOLD_SINCE_MS, "3 abnormal term ends")]
    assert C.SQL_SAFE_HOLD_RESUME not in conn.sql
    (audit,) = conn.args_of(C.SQL_AUDIT_INSERT)
    command, target, operator, approver, reason, outcome, detail, epoch, os_user = audit
    assert (command, target, operator, approver, outcome, epoch, os_user) == (
        AUDIT_COMMAND, "cluster", AUDIT_OPERATOR, None, "applied", 7, None)
    assert isinstance(reason, str) and len(reason.strip()) >= 8
    d = json.loads(str(detail))
    assert d["actor"] == AUTO_RESUME_ACTOR and d["safe_hold_reason"] == "3 abnormal term ends"
    assert d["auto_resume"] == 1
    sql = conn.sql
    assert sql.index(C.SQL_SAFE_HOLD_AUTO_RESUME) < sql.index(C.SQL_AUDIT_INSERT)
    assert mirrored == [True]


async def test_pg_store_writes_nothing_when_not_held_or_without_an_epoch() -> None:
    conn = FakeConn()
    store = PgAutoResumeStore(FakeSource(conn))
    assert await store.resume("single-PC auto-resume 1 of 6", {}, 1, "x") is None   # UPDATE matched no row
    assert C.SQL_AUDIT_INSERT not in conn.sql
    conn2 = FakeConn()
    conn2.stored_epoch = None
    assert await PgAutoResumeStore(FakeSource(conn2)).resume("single-PC auto-resume 1 of 6", {}, 1, "x") is None
    assert C.SQL_SAFE_HOLD_AUTO_RESUME not in conn2.sql


async def test_pg_store_counts_this_actors_applied_resumes() -> None:
    conn = FakeConn()
    conn.returns(C.SQL_COUNT_AUTO_RESUMES, (3,))
    assert await PgAutoResumeStore(FakeSource(conn)).count_auto_resumes(123) == 3
    assert conn.args_of(C.SQL_COUNT_AUTO_RESUMES) == [(AUDIT_OPERATOR, 123)]
    q = C.SQL_COUNT_AUTO_RESUMES
    assert "a.command = 'safe_hold.resume'" in q and "a.outcome = 'applied'" in q
    assert "h.operator <> $1::text" in q          # a person's resume starts a fresh budget


# ============================================================================================ wiring
async def test_build_engine_wires_the_auto_resume_only_with_the_flag(tmp_path: Path) -> None:
    from src.runtime.wiring import build_engine
    from tests.runtime.test_wiring import MOCK, cfg
    for env, wired in ((MOCK, False), ({**MOCK, "XTNL_SINGLE_PC": "0"}, False), ({**MOCK, "XTNL_SINGLE_PC": "1"}, True),
                       ({**MOCK, "XTNL_SINGLE_PC": "1", "NOTIFY_SOCKET": "/run/systemd/notify"}, False)):
        engine = build_engine(cfg(env, tmp_path), environ=env)
        try:
            c = engine.controller
            assert (c._auto_resume is not None) is wired, env
            assert (c._idle_watch is not None) is wired, env
            assert (c._direct_pager is not None) is wired, env
        finally:
            await engine.aclose()


def test_hard_checks_used_are_the_campaign_ones() -> None:
    """The auto-resume asks the same HealthRecord the campaign asks (PostgreSQL, etcd, engine Redis, clock)."""
    assert record("n1", 0).failed_hard_checks(0, as_leader=False) == []
    assert record("n1", 0, chrony_offset_ms=51.0).failed_hard_checks(0, as_leader=False) == ["chrony"]
