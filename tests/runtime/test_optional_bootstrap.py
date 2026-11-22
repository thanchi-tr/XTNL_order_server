"""S31 (plan RT-1; design §A2, §B3, §B14, §C2; RT-M11, RT-M13): the `optional-bootstrap` body.

A real `EngineSession` over the in-memory backend, fakeredis (with lupa) as `redis-md`, fake ARM pools. Synthetic
instrument and keys only; no assertion message carries a value."""
from __future__ import annotations

import asyncio
import re
import subprocess
import sys
import textwrap
import time
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import fakeredis
import httpx
import pytest

from src.db.arm_schema import ARM_COLUMN_GRANTS, ARM_GRANTS, ARM_PARTITIONED, ARM_SEQUENCES
from src.db.arm_status_store import SQL_MD_RECORD_SAVE
from src.db.epoch import Epoch
from src.db.lifecycle import LifecycleReport
from src.db.operator_requests import CORE_REQUEST_KINDS
from src.marketdata.config import MdSettings
from src.marketdata.ports import NodeMdRecord
from src.marketdata.store.client import MdClients, MdReadClient, MdWriteClient
from src.marketdata.store.runner import ScriptRunner
from src.riskmgr import golive
from src.riskmgr.lockout import Rule, ScanResult, Violation
from src.runtime.config import RuntimeConfig
from src.runtime.etcd import LEADER_KEY
from src.runtime.health import HealthBoard
from src.runtime.optional import (
    EXEMPT_COMPLETE,
    EXEMPT_PARKED,
    ArmPool,
    OptionalBootstrapRunner,
    OptionalDeps,
    OptionalPlan,
    OptionalServices,
    arm_application_name,
    current_and_next_month,
    make_optional_bootstrap,
)
from src.runtime.optional_settings import ARM_ENABLED_VAR, MD_ENABLED_VAR, OptionalFlags
from src.runtime.roles import NodeRole
from src.runtime.session import OPTIONAL_BOOTSTRAP_TASK, EngineSession, SessionContext
from src.runtime.supervisor import TaskContext, TaskSpec, TaskSupervisor
from src.runtime.wiring import EngineHooks, build_engine
from tests.db.conftest import FakeConn as DbFakeConn
from tests.db.conftest import FakeSource
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import ADAPTER, FAST_TIMINGS, NODE, FakeLease, fixture, mock_adapter, wall_ms
from tests.marketdata.conftest import FakeAof, RunIds
from tests.runtime.fakes import FakeClock, FakeEtcd
from tests.runtime.test_wiring import MOCK, cfg
from tests.runtime.test_wiring_session import ENV as WIRED_ENV
from tests.runtime.test_wiring_session import ControlConn

#: The ARM tests run the engine as `build_engine` would with `XTNL_OVERSEE_STOP_AT_ENTRY=1` (`GOLIVE_ACCEPT_ALL`): the
#: process runs the S07 stop rule, which `oversee_c4` needs in force (R14 review), not only in the setting.
pytestmark = pytest.mark.usefixtures("stop_at_entry_on")

EPOCH = Epoch("c", 31)
KEY = "0123456789abcdef" * 4                       # synthetic test key
MD_ENV = {"XTNL_MD_REDIS_URL": "redis://127.0.0.1:1/0", "XTNL_MD_HMAC_KEY": KEY, "XTNL_MD_HMAC_KEY_ID": "3"}
GOLIVE_ACCEPT_ALL = {golive.ACCEPT_VAR: ",".join(g.gate_id for g in golive.GATES
                                                  if g.state is golive.GateState.OPEN),
                     golive.STOP_AT_ENTRY_VAR: "1"}   # S38f: oversee_c4 counts only with the S07 stop rule on
"""S38e: the ARM tests run the ARM, so they accept every open go-live gate explicitly (the gate itself is tested in
`test_open_go_live_gates_leave_the_arm_idle_with_one_page`)."""
ARM_CHANNELS = {"XTNL_ARM_SMTP_HOST": "smtp.example.test", "XTNL_ARM_SMTP_FROM": "arm@example.test",
                "XTNL_ARM_ALERT_EMAIL_TO": "ops@example.test", "XTNL_ARM_PUSH_PROVIDER": "ntfy",
                "XTNL_ARM_PUSH_URL": "https://ntfy.example.test/arm-test", **GOLIVE_ACCEPT_ALL}


async def run_id(_: Any) -> str:
    return "runs31"


@dataclass
class Pages:
    items: list[tuple[str, str, str]] = field(default_factory=list)

    def __call__(self, severity: str, title: str, body: str) -> None:
        self.items.append((severity, title, body))

    def titles(self) -> list[str]:
        return [t for _, t, _ in self.items]


def md_clients_over(r: Any) -> Callable[[MdSettings], MdClients]:
    def build(_settings: MdSettings) -> MdClients:
        return MdClients(MdReadClient(r), MdWriteClient(r, ScriptRunner(r, run_id_reader=RunIds()), FakeAof()))
    return build


class FakeConn:
    def __init__(self, pool: FakeArmPool) -> None:
        self.pool = pool

    async def execute(self, sql: str, *args: object) -> str:
        """A healthy pool: a write answers with PostgreSQL's command tag and one row affected, so a fenced write
        (`FencedTx.write`, which reads the row count from the tag) succeeds. "OK" reads as 0 rows, which the fence
        takes for a lost epoch: the ARM's daily TEST notice (after `test_at_ms_of_day`, 22:00 UTC) then ended the ARM
        loop mid-test, and tests that ran in that window failed depending on timing."""
        verb = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
        if verb == "WITH":                       # a data-modifying CTE: the tag is the main statement's
            m = re.search(r"\)\s*(INSERT|UPDATE|DELETE)\b", sql, re.I)
            verb = m.group(1).upper() if m else verb
        if verb == "INSERT":
            return "INSERT 0 1"
        if verb in ("UPDATE", "DELETE"):
            return f"{verb} 1"
        return "OK"

    async def fetchrow(self, sql: str, *args: object) -> Any:
        return (1,)

    async def fetch(self, sql: str, *args: object) -> list[Any]:
        self.pool.statements.append(sql)
        if self.pool.grants_down and ("aclexplode" in sql or "relkind" in sql):
            raise ConnectionError("catalog read refused")
        if "aclexplode(att.attacl)" in sql:
            return [(t, c, p) for (t, c), privs in ARM_COLUMN_GRANTS.items() for p in sorted(privs)]
        if "c.relkind = 'S'" in sql:
            return [(s, "USAGE") for s in sorted(ARM_SEQUENCES)]
        if "aclexplode(c.relacl)" in sql:
            return [(t, p) for t, privs in ARM_GRANTS.items() for p in sorted(privs)]
        if "FROM pg_trigger" in sql:                     # S38f: 0020's arm_failures_guard, enabled
            return [("arm_failures_guard", "O")]
        if self.pool.failures_down:
            raise ConnectionError("down")
        return []


@dataclass
class FakeArmPool:
    """An ARM pool whose first `fail_connects` acquires fail (PostgreSQL not reachable yet)."""

    name: str
    fail_connects: int = 0
    failures_down: bool = False
    grants_down: bool = False
    acquires: int = 0
    closed: bool = False
    terminated: bool = False
    statements: list[str] = field(default_factory=list)

    @asynccontextmanager
    async def _acquire(self) -> AsyncIterator[FakeConn]:
        self.acquires += 1
        if self.fail_connects > 0:
            self.fail_connects -= 1
            raise ConnectionError("connect refused")
        yield FakeConn(self)

    def acquire(self) -> AbstractAsyncContextManager[Any]:
        return self._acquire()

    async def close(self) -> None:
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True


@dataclass
class Pools:
    fail_connects: int = 0
    failures_down: bool = False
    built: list[FakeArmPool] = field(default_factory=list)

    def __call__(self, application_name: str) -> ArmPool:
        pool = FakeArmPool(application_name, fail_connects=self.fail_connects, failures_down=self.failures_down)
        self.built.append(pool)
        return pool


def clean_scan() -> ScanResult:
    return ScanResult((), ())


@pytest.fixture
async def redis() -> AsyncIterator[fakeredis.FakeAsyncRedis]:
    r = fakeredis.FakeAsyncRedis()
    try:
        yield r
    finally:
        await r.aclose()


@pytest.fixture
async def md_redis() -> AsyncIterator[fakeredis.FakeAsyncRedis]:
    r = fakeredis.FakeAsyncRedis()
    try:
        yield r
    finally:
        await r.flushall()
        await r.aclose()


@dataclass
class FakeMonths:
    """The engine pool's partition maintenance: records each `ensure_months(parent, months)` call."""

    calls: list[tuple[str, tuple[date, ...]]] = field(default_factory=list)
    errors: bool = False

    async def run_once(self, now_ms: int) -> LifecycleReport:
        return LifecycleReport()

    async def ensure_months(self, parent: str, months: Sequence[date]) -> LifecycleReport:
        self.calls.append((parent, tuple(months)))
        return LifecycleReport(errors=["partition not created"] if self.errors else [])


_DEFAULT: Any = object()


def make_session(tmp_path: Path, redis: Any, *, flags: OptionalFlags, pages: Pages, node_id: str = NODE,
                 lifecycle: Any = _DEFAULT) -> EngineSession:
    backend = MemoryBackend(redis, run_id, EPOCH.token)
    ctx = SessionContext(node_id=node_id, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
                         adapters=lambda: [mock_adapter(fixture(wall_ms()))], lease=FakeLease(),
                         run_id_reader=run_id, exit_fn=lambda _r: None, timings=FAST_TIMINGS, page_sink=pages,
                         optional_flags=flags)
    s = EngineSession(ctx, EPOCH)
    s.init_deps(TaskSupervisor([]))
    s.lifecycle = FakeMonths() if lifecycle is _DEFAULT else lifecycle
    return s


def deps(tmp_path: Path, md_redis: Any, env: dict[str, str], **kw: Any) -> OptionalDeps:
    base: dict[str, Any] = {"environ": env, "repo_root": tmp_path, "spool_dir": tmp_path / "spool",
                            "md_clients": md_clients_over(md_redis), "scan": clean_scan, "retry_base_s": 0.01,
                            "retry_max_s": 0.05, "park_stamp_s": 0.01}
    base.update(kw)
    return OptionalDeps(**base)


async def until(cond: Callable[[], bool], timeout: float = 5.0) -> None:
    end = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > end:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.005)


@asynccontextmanager
async def running(runner: OptionalBootstrapRunner) -> AsyncIterator[TaskContext]:
    ctx = TaskContext(OPTIONAL_BOOTSTRAP_TASK, time.monotonic)
    task = asyncio.create_task(runner.run(ctx))
    try:
        yield ctx
    finally:
        runner.session.stop_event.set()
        await asyncio.wait_for(task, 5.0)
        await runner.session.aclose(1.0)


async def forever(ctx: TaskContext) -> None:
    while True:
        ctx.stamp()
        await asyncio.sleep(0.01)


def arm_plan(tasks: list[str]) -> OptionalPlan:
    def arm_tasks(svc: OptionalServices) -> Sequence[TaskSpec]:
        tasks.append("arm")
        return [TaskSpec("arm", forever, max_stamp_age_s=60.0)]
    return OptionalPlan(arm_tasks=arm_tasks)


# ============================================================================================ the fake ARM pool
@pytest.mark.parametrize("sql", [
    "INSERT INTO notification_outbox (created_at_ms) SELECT 1 WHERE true",   # the ARM daily TEST notice's insert
    "UPDATE arm_status SET x = 1",
    "DELETE FROM arm_notices WHERE false",
    "WITH s AS (SELECT 1) INSERT INTO arm_notices SELECT 1",
])
async def test_a_fenced_write_on_the_fake_arm_pool_succeeds(sql: str) -> None:
    """The fake pool answers writes like PostgreSQL (a command tag with the row count), so a healthy fenced write
    never reads as a lost epoch. Before, "OK" read as 0 rows: from 22:00 UTC the ARM's daily TEST notice raised
    EpochMismatch, ended the ARM loop, and the hanging-clear-probe test failed depending on timing."""
    from src.db.arm_outbox_store import SQL_ARM_OUTBOX_INSERT
    from src.db.fence import FencedTx
    tx = FencedTx(FakeConn(FakeArmPool("xtnl-arm")), 31)
    assert await tx.write(sql) == 1
    assert await tx.write(SQL_ARM_OUTBOX_INSERT, 0, [], "s", "b", 0) == 1


# ============================================================================================ market data
async def test_market_data_graph_binds_the_hooks_and_the_closer(tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True), pages=pages)
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, {**MD_ENV, "XTNL_TICK_SYMBOLS": f"{ADAPTER}:SYN9"}))
    async with running(runner) as ctx:
        await until(lambda: runner.md_started)
        svc = runner.services
        assert svc is not None and runner.parked_reason is None
        assert s._md_hook == svc.md.hook.on_price and s._md_sub_hook == svc.md.hook.on_price_subscription
        # S39b (gate receipt_gap_feed): the heartbeat feed is bound and the watch is on for every adapter
        assert s._md_frame_hook == svc.md.hook.note_frame                                  # noqa: SLF001
        assert all(svc.md.hook._gaps.watched(a) for a in s.adapter_ids)                    # noqa: SLF001
        assert [name for name, _, _ in s._extra_closers] == ["redis-md"]
        assert svc.md.settings.keyring is not None and svc.settings.tick_symbols == {ADAPTER: ("SYN9",)}
        # D6: the cross-checker is md-bars' and the tick-reference book is fed by the writer.
        assert svc.md.bars._crosscheck is svc.md.crosscheck            # noqa: SLF001
        await until(lambda: ctx.exempt_reason == EXEMPT_COMPLETE)
        assert not s.has_task("arm")                                      # the ARM flag is off
    assert all("SYN9" not in body or "tick-only" in title for _, title, body in pages.items)


async def test_redis_md_down_never_blocks_the_term(tmp_path: Path, redis: Any) -> None:
    """`redis-md` refusing every command: the bootstrap still completes (series GAPPED until it answers)."""

    class Down(fakeredis.FakeAsyncRedis):
        async def ping(self, **_kw: Any) -> bool:
            raise ConnectionError("refused")

    down = Down()
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True), pages=pages)
    runner = OptionalBootstrapRunner(s, deps(tmp_path, down, MD_ENV))
    async with running(runner) as ctx:
        await until(lambda: ctx.exempt_reason == EXEMPT_COMPLETE)
        assert runner.md_started and runner.parked_reason is None
    await down.aclose()


# ============================================================================================ settings and scan
async def test_out_of_bounds_fail_setting_pages_and_parks_ingestion_unaffected(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages)
    env = {**MD_ENV, **ARM_CHANNELS, "XTNL_ARM_FAIL_MD_DOWN_S": "999999"}
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, env, arm_pool=Pools()))
    async with running(runner) as ctx:
        await until(lambda: ctx.exempt_reason == EXEMPT_PARKED)
        assert runner.parked_reason == "settings" and runner.services is None
        assert s._md_hook is None and s._extra_closers == []
        assert pages.titles() == ["optional services settings"]
        assert "XTNL_ARM_FAIL_MD_DOWN_S" in pages.items[0][2] and "999999" not in pages.items[0][2]
        # ingestion is untouched: the price path, the calendar and the request kinds are as with the flags off
        assert s.request_kinds() == CORE_REQUEST_KINDS and not s.has_task("md-writer")
        before = time.monotonic()
        await asyncio.sleep(0.05)
        assert ctx.last_stamp >= before                                  # parked but stamping


async def test_a_planted_violation_parks_with_a_page(tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True), pages=pages)

    def planted() -> ScanResult:
        return ScanResult((Violation("src/riskmgr/x.py", 3, Rule.NAME_BAN, "banned name"),), ())

    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, MD_ENV, scan=planted))
    async with running(runner) as ctx:
        await until(lambda: ctx.exempt_reason == EXEMPT_PARKED)
        assert runner.parked_reason == "lockout" and runner.services is None and s._md_hook is None
        assert pages.titles() == ["ARM lockout violation"]


async def test_an_incomplete_scan_raises_retries_and_pages_after_three(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True), pages=pages)
    results = [ScanResult((), ("src/riskmgr/a.py: unreadable",))] * 3 + [ScanResult((), ())]

    def flaky() -> ScanResult:
        return results.pop(0)

    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, MD_ENV, scan=flaky))
    for n in range(1, 4):
        ctx = TaskContext(OPTIONAL_BOOTSTRAP_TASK, time.monotonic)
        with pytest.raises(RuntimeError, match="incomplete"):
            await runner.run(ctx)                    # the supervisor restarts the task with its backoff
        assert runner.scan_incomplete_streak == n
        assert pages.titles() == ([] if n < 3 else ["ARM lockout scan incomplete"])
    async with running(runner) as ctx:
        await until(lambda: runner.md_started)
        assert runner.scanned and runner.scan_incomplete_streak == 0 and pages.titles() == [
            "ARM lockout scan incomplete"]


async def test_the_real_tree_scan_passes(tmp_path: Path, redis: Any, md_redis: Any) -> None:
    """The production scan seam (the repository's real `LOCKOUT_SCOPE`, `md_ports.py` included) is clean."""
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True), pages=pages)
    root = Path(__file__).resolve().parents[2]
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, MD_ENV, scan=None, repo_root=root))
    async with running(runner):
        await until(lambda: runner.md_started, timeout=30.0)
        assert runner.scanned and pages.titles() == []


# ============================================================================================ ARM
async def test_a_pool_build_failing_once_then_succeeding_runs_the_arm_in_the_same_term(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages)
    pools = Pools(fail_connects=1)
    added: list[str] = []
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, {**MD_ENV, **ARM_CHANNELS}, arm_pool=pools,
                                             plan=arm_plan(added)))
    async with running(runner):
        await until(lambda: runner.arm_started)
        assert runner.arm_attempts == 2 and len(pools.built) == 1          # one pool, retried in place
        assert s.has_task("arm") and added == ["arm"]
        arm = runner.services.arm if runner.services is not None else None
        assert arm is not None and arm.may_advise and arm.open_failures == [] and arm.privilege_problems == ()
        assert arm.sinks.names == ("decision", "notice")
        assert [n for n, _, _ in s._extra_closers] == ["redis-md", "arm-pool"]
        assert pages.titles() == []


async def test_unreadable_open_failures_advise_nothing_and_retry(tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages)
    pools = Pools(failures_down=True)
    added: list[str] = []
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, {**MD_ENV, **ARM_CHANNELS}, arm_pool=pools,
                                             plan=arm_plan(added)))
    async with running(runner):
        await until(lambda: runner.arm_attempts >= 2)
        assert not runner.arm_started and added == [] and not s.has_task("arm")
        assert runner.services is not None and runner.services.arm is not None
        assert not runner.services.arm.may_advise
        pools.built[0].failures_down = False
        await until(lambda: runner.arm_started)
        assert added == ["arm"]


async def test_a_missing_arm_channel_leaves_the_arm_idle_with_one_page(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages)
    channels = {k: v for k, v in ARM_CHANNELS.items() if "PUSH" not in k}
    pools = Pools()
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, {**MD_ENV, **channels}, arm_pool=pools))
    async with running(runner) as ctx:
        await until(lambda: ctx.exempt_reason == EXEMPT_COMPLETE)
        await asyncio.sleep(0.05)
        assert runner.md_started and runner.arm_idle_reason == "arm_channels" and pools.built == []
        assert pages.titles() == ["ARM not started"] and "push" in pages.items[0][2]


async def test_open_go_live_gates_leave_the_arm_idle_with_one_page(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    """S38e: `XTNL_ARM_ENABLED=1` with a go-live gate open and not accepted: the ARM stays idle for the term (no pool
    is built), one page names the gate ids, and the market-data layer still runs."""
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages)
    open_ids = [g.gate_id for g in golive.GATES if g.state is golive.GateState.OPEN]
    partial = {**ARM_CHANNELS, golive.ACCEPT_VAR: ",".join(open_ids[1:]) + ",no_such_gate"}
    pools = Pools()
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, {**MD_ENV, **partial}, arm_pool=pools))
    async with running(runner) as ctx:
        await until(lambda: ctx.exempt_reason == EXEMPT_COMPLETE)
        await asyncio.sleep(0.05)
        assert runner.md_started and runner.arm_idle_reason == "golive_gates" and pools.built == []
        assert sorted(pages.titles()) == ["ARM go-live setting", "ARM not started"]
        body = dict((title, text) for _, title, text in pages.items)["ARM not started"]
        assert open_ids[0] in body and all(i not in body for i in open_ids[1:])


def test_arm_application_name_carries_the_epoch_prefix() -> None:
    assert re.match(r"^xtnl:[0-9]+:", arm_application_name(EPOCH, NODE))
    assert arm_application_name(EPOCH, NODE) == f"xtnl:31:{NODE}:arm"
    long = arm_application_name(Epoch("c", 123456789), "n" * 80)
    assert len(long) == 63 and re.match(r"^xtnl:[0-9]+:", long)


async def test_the_arm_pool_is_built_with_the_epoch_application_name(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages)
    pools = Pools()
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, {**MD_ENV, **ARM_CHANNELS}, arm_pool=pools))
    async with running(runner):
        await until(lambda: runner.arm_started)
        assert re.match(r"^xtnl:[0-9]+:", pools.built[0].name) and pools.built[0].name.endswith(":arm")


async def test_ensure_months_covers_both_partitioned_tables_this_and_next_month(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    months = FakeMonths()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages,
                     lifecycle=months)
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, {**MD_ENV, **ARM_CHANNELS}, arm_pool=Pools()))
    async with running(runner):
        await until(lambda: runner.arm_started)
    want = current_and_next_month(s.ctx.clock_ms())
    assert months.calls == [(parent, want) for parent in ARM_PARTITIONED]
    assert set(ARM_PARTITIONED) == {"arm_decisions", "arm_decision_handles"} and pages.titles() == []


async def test_partition_failures_and_a_missing_port_page_once_and_never_block(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    for lifecycle, want in ((FakeMonths(errors=True), ["ARM partitions not ensured"] * 2),
                            (None, ["ARM partitions not ensured"])):
        pages = Pages()
        s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages,
                         lifecycle=lifecycle)
        runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, {**MD_ENV, **ARM_CHANNELS},
                                                 arm_pool=Pools()))
        async with running(runner):
            await until(lambda: runner.arm_started)
        assert pages.titles() == want


async def test_unreadable_grants_page_once_and_never_block_the_arm(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages)
    pools = Pools()
    real = pools.__call__

    def grants_down(name: str) -> ArmPool:
        pool = real(name)
        assert isinstance(pool, FakeArmPool)
        pool.grants_down = True
        return pool

    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, {**MD_ENV, **ARM_CHANNELS}, arm_pool=grants_down))
    async with running(runner):
        await until(lambda: runner.arm_started)
        arm = runner.services.arm if runner.services is not None else None
        assert arm is not None and arm.privilege_problems == ("grants unreadable",) and arm.may_advise
        assert runner.arm_attempts == 1
    assert pages.titles() == ["ARM role grants differ"]


class WrongSinks:
    names = ("notice", "decision")


async def test_a_wrong_sink_set_parks_the_arm_with_one_page_and_no_retry(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages)
    added: list[str] = []
    plan = OptionalPlan(arm_tasks=arm_plan(added).arm_tasks, arm_sinks=lambda _svc: WrongSinks())  # type: ignore[arg-type,return-value]
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, {**MD_ENV, **ARM_CHANNELS}, arm_pool=Pools(),
                                             plan=plan))
    async with running(runner):
        await until(lambda: runner.arm_idle_reason == "arm_sinks")
        await asyncio.sleep(0.1)                                         # several retry periods
        assert runner.arm_attempts == 1 and not runner.arm_started and added == [] and not s.has_task("arm")
    assert pages.titles() == ["ARM sink set refused"] and "('notice', 'decision')" in pages.items[0][2]


async def test_a_node_id_outside_the_token_alphabet_still_saves_its_record(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    """node_id 'a.b:c' (dots and colons, like an EC2 hostname): the md bootstrap's record is saved under the raw
    node_id while it carries the writer-token form, so §A7 finds it after a restart."""
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True), pages=pages, node_id="a.b:c")
    conn = DbFakeConn()
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, MD_ENV, control=FakeSource(conn)))
    async with running(runner):
        await until(lambda: runner.md_started)
        svc = runner.services
        assert svc is not None
        boot = svc.md.bootstrap
        records = boot._records                                         # noqa: SLF001
        assert records is not None and boot._node == "a-b-c"           # noqa: SLF001
        conn.status_for(SQL_MD_RECORD_SAVE, "INSERT 0 1")
        await records.save_record(NodeMdRecord(node="a-b-c", sentinel="r1:c:a-b-c:7.1", last_write_ms=5))
    saves = [args for sql, args in conn.calls if sql == SQL_MD_RECORD_SAVE]
    assert saves and saves[0][0] == "a.b:c"


async def test_unknown_tick_symbols_are_paged_by_name_once_the_instrument_map_loads(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True), pages=pages)
    env = {**MD_ENV, "XTNL_TICK_SYMBOLS": f"{ADAPTER}:SYN9,{ADAPTER}:SYNX"}
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, env))
    instruments = s.orchestrator.runtime(ADAPTER).instruments
    instruments.clear()                                                  # SNAPSHOTTING has not loaded them yet
    async with running(runner):
        await until(lambda: runner.md_started)
        svc = runner.services
        assert svc is not None and runner.tick_check_pending
        assert svc.md.extras["tick_symbols"] == {ADAPTER: ("SYN9", "SYNX")} and pages.titles() == []
        instruments["SYNX"] = object()                                   # type: ignore[assignment]
        await until(lambda: not runner.tick_check_pending)
        assert svc.md.extras["tick_symbols"] == {ADAPTER: ("SYNX",)}
        await asyncio.sleep(0.05)
    assert pages.titles() == ["market data: unknown tick-only symbols"]
    assert f"{ADAPTER}:SYN9" in pages.items[0][2] and "SYNX" not in pages.items[0][2]


async def test_stop_reads_reach_the_demand_book_only_with_the_flag(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    for flag, want in (("0", 0), ("1", 3)):
        pages = Pages()
        s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages)
        env = {**MD_ENV, **ARM_CHANNELS, "XTNL_ARM_STOP_READS": flag}
        runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, env, arm_pool=Pools()))
        runner.stop_read_cost = lambda _aid: 3
        async with running(runner):
            await until(lambda: runner.md_started)
            svc = runner.services
            assert svc is not None
            stop_reads = svc.md.demand._stop_reads                     # noqa: SLF001
            assert stop_reads is not None and stop_reads(ADAPTER) == want


async def test_a_session_closing_during_the_previous_term_read_builds_nothing(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True), pages=pages)
    entered, release = asyncio.Event(), asyncio.Event()

    class Slow:
        @asynccontextmanager
        async def _acq(self) -> AsyncIterator[Any]:
            entered.set()
            await release.wait()
            yield DbFakeConn()

        def acquire(self) -> AbstractAsyncContextManager[Any]:
            return self._acq()

    built: list[MdSettings] = []

    def clients(settings: MdSettings) -> MdClients:
        built.append(settings)
        return md_clients_over(md_redis)(settings)

    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, MD_ENV, control=Slow(), md_clients=clients))
    ctx = TaskContext(OPTIONAL_BOOTSTRAP_TASK, time.monotonic)
    task = asyncio.create_task(runner.run(ctx))
    await asyncio.wait_for(entered.wait(), 5.0)
    s.stop_event.set()
    await s.aclose(1.0)
    release.set()
    await asyncio.wait_for(task, 5.0)
    assert built == [] and runner.services is None and s._extra_closers == [] and s._md_hook is None


def test_flags_off_import_neither_market_data_nor_arm_modules(tmp_path: Path) -> None:
    """In a fresh interpreter: `build_engine` with both flags off leaves no `src.marketdata` / `src.riskmgr` (or
    optional runtime) module in `sys.modules`; with `XTNL_MD_ENABLED=1` the factory's modules are imported."""
    root = Path(__file__).resolve().parents[2]
    script = textwrap.dedent(f"""
        import asyncio, sys
        from pathlib import Path
        from tests.runtime.test_wiring import MOCK, cfg
        from tests.runtime.fakes import FakeClock, FakeEtcd
        from src.runtime.wiring import EngineHooks, build_engine

        PREFIXES = ("src.marketdata", "src.riskmgr", "src.runtime.optional.", "src.runtime.md_ports")

        def loaded():
            return sorted(m for m in sys.modules
                          if m.startswith(PREFIXES) or m in ("src.runtime.optional", "src.runtime.optional_md",
                                                             "src.runtime.optional_plan", "src.runtime.arm_ports"))

        async def build(env, sub):
            d = Path({str(tmp_path)!r}) / sub
            d.mkdir()
            engine = build_engine(cfg(env, d), environ=env,
                                  hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False))
            await engine.aclose()

        asyncio.run(build(MOCK, "off"))
        print("OFF", ",".join(loaded()))
        asyncio.run(build({{**MOCK, "XTNL_MD_ENABLED": "1"}}, "on"))
        print("ON", ",".join(loaded()))
    """)
    out = subprocess.run([sys.executable, "-c", script], cwd=root, capture_output=True, text=True, timeout=120,
                         check=True).stdout
    lines = dict(line.split(" ", 1) if " " in line else (line, "") for line in out.splitlines()
                 if line.startswith(("OFF", "ON")))
    assert lines["OFF"].strip() == ""
    assert "src.runtime.optional" in lines["ON"] and "src.marketdata" in lines["ON"]


def test_current_and_next_month() -> None:
    first, nxt = current_and_next_month(1_735_603_200_000)           # 2024-12-31 UTC
    assert (first.isoformat(), nxt.isoformat()) == ("2024-12-01", "2025-01-01")


# ============================================================================================ flags and wiring
async def test_flags_off_build_no_factory_and_add_no_task(tmp_path: Path) -> None:
    engine = build_engine(cfg(MOCK, tmp_path), environ=MOCK,
                          hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False))
    try:
        assert engine.context.optional_bootstrap is None
    finally:
        await engine.aclose()
    on = {**MOCK, MD_ENABLED_VAR: "1"}
    engine = build_engine(cfg(on, tmp_path), environ=on,
                          hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False))
    try:
        assert engine.context.optional_bootstrap is not None             # the production factory (S31)
    finally:
        await engine.aclose()


async def test_flags_off_session_adds_no_task(tmp_path: Path, redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(), pages=pages)
    called: list[EngineSession] = []

    def factory(sess: EngineSession) -> TaskSpec:
        called.append(sess)
        return TaskSpec(OPTIONAL_BOOTSTRAP_TASK, forever, max_stamp_age_s=60.0)

    object.__setattr__(s.ctx, "optional_bootstrap", factory)
    s.start_optional_services()
    assert called == [] and not s.has_task(OPTIONAL_BOOTSTRAP_TASK)
    await s.aclose(1.0)


def _wired_engine(redis: Any, tmp_path: Path, factory: Callable[[EngineSession], TaskSpec],
                  exits: list[str], extra: dict[str, str]) -> tuple[Any, dict[int, MemoryBackend]]:
    env = {**WIRED_ENV, MD_ENABLED_VAR: "1", **extra}
    backends: dict[int, MemoryBackend] = {}

    def ports(epoch: Epoch) -> Any:
        return backends.setdefault(epoch.token, MemoryBackend(redis, run_id, epoch.token)).ports(epoch)

    async def chrony() -> float:
        return 0.2

    def http(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503 if request.url.port == 8008 else 200)

    fx = fixture(wall_ms())
    hooks = EngineHooks(etcd=FakeEtcd(FakeClock()), redis=redis, control_source=FakeSource(ControlConn()),
                        ports=ports, run_id_reader=run_id, exit_fn=exits.append, chrony=chrony,
                        http_client=lambda: httpx.AsyncClient(transport=httpx.MockTransport(http)),
                        patroni_client=httpx.AsyncClient(transport=httpx.MockTransport(http)),
                        adapters=lambda: [mock_adapter(fx)], timings=FAST_TIMINGS, require_keeper_thread=False,
                        optional_bootstrap=factory)
    config = RuntimeConfig.from_env(env, spill_path=str(tmp_path / "spill.jsonl"))
    return build_engine(config, environ=env, hooks=hooks), backends


def _abnormal(backends: dict[int, MemoryBackend]) -> list[Any]:
    return [r for b in backends.values() for kind, r in b.tables.terms
            if kind == "end_reason" and r in ("lost", "init_failed")]


async def test_node_keeps_publishing_health_while_the_bootstrap_hangs(redis: Any, tmp_path: Path) -> None:
    hung = asyncio.Event()

    def hanging_clients(_settings: MdSettings) -> MdClients:
        hung.set()
        raise AssertionError("never reached")

    class Hang(OptionalBootstrapRunner):
        async def _scan(self, ctx: TaskContext) -> None:
            hung.set()
            await asyncio.Event().wait()                 # a scan that never returns

    def factory(s: EngineSession) -> TaskSpec:
        return Hang(s, deps(tmp_path, redis, {**MD_ENV}, md_clients=hanging_clients)).spec()

    exits: list[str] = []
    engine, backends = _wired_engine(redis, tmp_path, factory, exits, MD_ENV)
    ctl = engine.controller
    try:
        await ctl.startup()
        await ctl.step()
        assert ctl.role is NodeRole.LEADER
        await asyncio.wait_for(hung.wait(), 5.0)
        for _ in range(3):
            await ctl.step()
            await asyncio.sleep(0.01)
        assert ctl.role is NodeRole.LEADER and not exits and not _abnormal(backends)
        assert ctl.last_record is not None and ctl.last_record.role == "LEADER"
        seen = HealthBoard(engine.etcd, "peer").peers()
        assert [p.role for p in seen] == ["LEADER"]
        ctl.request_drain("switchover", exit_after=False)
        await ctl.step()
        assert ctl.role is NodeRole.STANDBY and not exits
    finally:
        await engine.aclose()


async def test_a_raising_constructor_leaves_no_abnormal_term(redis: Any, tmp_path: Path) -> None:
    built: list[int] = []

    def raising_clients(_settings: MdSettings) -> MdClients:
        built.append(1)
        raise RuntimeError("constructor failed")

    def factory(s: EngineSession) -> TaskSpec:
        return make_optional_bootstrap(deps(tmp_path, redis, {**MD_ENV}, md_clients=raising_clients))(s)

    exits: list[str] = []
    engine, backends = _wired_engine(redis, tmp_path, factory, exits, MD_ENV)
    ctl = engine.controller
    try:
        await ctl.startup()
        await ctl.step()
        assert ctl.role is NodeRole.LEADER
        for _ in range(20):
            if built:
                break
            await asyncio.sleep(0.01)
        assert built
        for _ in range(3):
            await ctl.step()
            await asyncio.sleep(0.01)
        assert ctl.role is NodeRole.LEADER and not exits and not _abnormal(backends)
        assert engine.etcd is not None and LEADER_KEY in engine.etcd.kv
        ctl.request_drain("switchover", exit_after=False)
        await ctl.step()
        assert ctl.role is NodeRole.STANDBY and not exits and not _abnormal(backends)
    finally:
        await engine.aclose()


async def test_the_production_factory_with_arm_on_builds_an_arm_pool_factory(tmp_path: Path) -> None:
    env = {**MOCK, MD_ENABLED_VAR: "1", ARM_ENABLED_VAR: "1"}
    engine = build_engine(cfg(env, tmp_path), environ=env,
                          hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False))
    try:
        assert engine.context.optional_bootstrap is not None
        assert engine.context.optional_flags == OptionalFlags(md_enabled=True, arm_enabled=True)
    finally:
        await engine.aclose()


async def test_the_arm_pool_connection_init_sets_the_arm_role() -> None:
    from src.runtime.optional import arm_connection_init

    class Raw:
        def __init__(self) -> None:
            self.executed: list[str] = []
            self.codecs: list[str] = []

        async def set_type_codec(self, typename: str, **_kw: Any) -> None:
            self.codecs.append(typename)

        async def execute(self, sql: str) -> str:
            self.executed.append(sql)
            return "SET"

    raw = Raw()
    await arm_connection_init(raw)
    assert raw.codecs == ["jsonb", "json"] and raw.executed == ["SET ROLE xtnl_arm"]


def test_default_arm_pool_is_lazy_min_zero_with_a_3_s_connect_timeout() -> None:
    from src.runtime.optional import default_arm_pool
    from src.runtime.pg_ports import LazyPgSource

    pool = default_arm_pool("postgresql://u:pw@h/db")(arm_application_name(EPOCH, NODE))
    assert isinstance(pool, LazyPgSource) and not pool.created
    assert pool._kw["min_size"] == 0 and pool._kw["connect_timeout_s"] == 3.0       # noqa: SLF001
    assert pool.application_name == f"xtnl:31:{NODE}:arm" and "pw" not in repr(pool)
