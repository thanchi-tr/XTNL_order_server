"""S38 (plan E2E; design §C3): the market-data layer and the ARM end to end, and the broker never touched.

* `test_arm_pre_post_adjust_stop_moved_complete_end_to_end`: the rig of `tests/e2e/arm_rig.py` (real md store on
  fakeredis + lupa, real ARM stores on the SQL emulator, the real tracker, BarClock, S21 pipeline and notices): arm
  → PRE → POST ADJUST → the user moves the stop → COMPLETE, every notice queued through the real ARM outbox INSERT.
* nothing reaches the broker: every adapter REST client is `ReadOnlyTransport`, no denied path is attempted, the
  mock broker is read only and is asked nothing but reads (R11-m, design §B3).
* PR1 (the read path `XTNL_ARM_STOP_READS=1` turns on): a stop moved between stream frames is read by the PR9
  stop-order list read and seen by Oversee (`effective_stop_detail`) and by the ARM's `S_at_T` function
  (`stop_at_bar_end` over `event_log`) at the same level. The ARM pipeline itself is not run here.
* both flags `0`: the real `EngineSession` on the in-memory backend (the existing replay scenario) runs unchanged;
  no new task, no `redis-md` key or connection, no ARM row, no market-data hook.

Synthetic instruments, ids, levels and times only; no assertion message carries a value.
"""
from __future__ import annotations

import ast
import inspect
import json
import os
import textwrap
from collections.abc import AsyncIterator
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import fakeredis
import httpx
import pytest

from src.adapters import transport as transport_mod
from src.adapters.base import FORBIDDEN_METHOD_NAMES, assert_read_only
from src.adapters.mock import MockBrokerAdapter
from src.adapters.stonex.rest import StoneXRestClient
from src.adapters.transport import ReadOnlyTransport, is_denied_path
from src.cache.codec import dto_json
from src.cache.keys import order_key
from src.config import ExecutionLockedError, StoneXSettings
from src.db.arm_versions import PgArmVersionReader
from src.db.repository import version_labels
from src.dto import OrderDTO
from src.marketdata.ports import HandleRef, HandleStatus, StopHandleRead
from src.oversee.risk import effective_stop_detail
from src.riskmgr.evaluation import stop_versions_of
from src.riskmgr.sm11 import ArmState
from src.riskmgr.stopbasis import StopAtT, stop_at_bar_end
from src.riskmgr.tracker import AnswerStatus, TrackerRequest
from src.runtime.md_ports import StopHandleReadPortImpl
from tests.e2e.arm_rig import (
    TF,
    E2e,
    E2eEmulator,
    bodies,
    e2e_rig,
    engine_db,
    labelled,
    log_position,
    run_main_scenario,
)
from tests.pubsub.conftest import ManualClock
from tests.pubsub.test_orchestrator import make as make_orchestrator
from tests.riskmgr.tracker_fakes import ACCOUNT, ADAPTER, SYMBOL, position, stop_child

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


@pytest.fixture
def no_network(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Any HTTP request (httpx) or `ReadOnlyTransport` request during the test is recorded and refused."""
    sent: list[str] = []

    async def refuse_async(self: Any, request: httpx.Request, **kw: Any) -> httpx.Response:
        sent.append(str(request.url.path))
        raise AssertionError("an HTTP request was attempted")

    def refuse_sync(self: Any, request: httpx.Request, **kw: Any) -> httpx.Response:
        sent.append(str(request.url.path))
        raise AssertionError("an HTTP request was attempted")

    original = ReadOnlyTransport.request

    async def recorded(self: ReadOnlyTransport, method: str, path: str, **kw: Any) -> httpx.Response:
        sent.append(path)
        return await original(self, method, path, **kw)

    monkeypatch.setattr(httpx.AsyncClient, "send", refuse_async)
    monkeypatch.setattr(httpx.Client, "send", refuse_sync)
    monkeypatch.setattr(ReadOnlyTransport, "request", recorded)
    return sent


@pytest.fixture
async def rig() -> AsyncIterator[E2e]:
    r = fakeredis.FakeAsyncRedis()
    e = await e2e_rig(redis=r)
    try:
        yield e
    finally:
        e.barclock.close()
        await r.flushall()
        await r.aclose()


# ---------------------------------------------------------------------------------------------------- the scenario
async def test_arm_pre_post_adjust_stop_moved_complete_end_to_end(rig: E2e, no_network: list[str]) -> None:
    e = rig
    bars = await run_main_scenario(e)
    b1, b2, b3 = bars["b1"], bars["b2"], bars["b3"]

    def at(event: str, bar_end: int) -> list[tuple[str, Any]]:
        return [(d["outcome"], d["trigger_met"]) for d in e.decisions(event) if d["bar_end_ms"] == bar_end]

    assert at("POST", b1) == [("STRATEGY_REASON", False)]
    assert [d["outcome"] for d in e.decisions("PRE") if d["bar_end_ms"] == b2] == ["ADJUST"]
    assert at("POST", b2) == [("ADJUST", True)]
    assert at("PRE", b3) == []                                # at target: no item, never a release at PRE
    assert at("POST", b3) == [("COMPLETE", True)]
    assert e.row()["state"] == ArmState.STRATEGY_COMPLETE.value

    # the POST rows carry the keyed trigger input read through the hash-checked md read path
    post2 = next(d for d in e.decisions("POST") if d["bar_end_ms"] == b2)
    assert post2["close_source"] == "BROKER" and post2["inputs_digest"] is not None
    assert list(post2["series_inputs"])[0].startswith(f"{ADAPTER}:{SYMBOL}:15m:BID")

    # every bar-end decision has a notice (or the suppressed sentinel); the POST ADJUST is in the ARM outbox
    assert all(d["notice_id"] is not None for d in e.decisions() if d["event"] != "PRE")
    rows = e.emu.arm_rows()
    assert rows and all(r["channels"] and set(r["channels"]) <= {"arm_email", "arm_push"} for r in rows)
    items = [(i["outcome"], i["bar_end_ms"]) for b in bodies(rows) for i in b.get("items", [])]
    assert ("ADJUST", b2) in items and ("COMPLETE", b3) in items
    assert len(e.emu.inserts) == len(rows)                   # every INSERT committed (no rollback in this run)

    # arm_disarm through the tracker: applied (a committed CAS on the emulated table), then moot; nothing paged;
    # no HTTP request of any kind was made
    e.tracker.submit(TrackerRequest.of(51, "arm_disarm", f"{ADAPTER}:P1"))
    await e.tracker.step()
    assert e.row()["state"] == ArmState.DISARMED.value
    e.tracker.submit(TrackerRequest.of(52, "arm_disarm", f"{ADAPTER}:P1"))
    await e.tracker.step()
    assert [(i, a.status) for i, a in e.rec.answers] == [(51, AnswerStatus.APPLIED), (52, AnswerStatus.MOOT)]
    assert e.rec.pages == [] and e.row()["account_id"] == ACCOUNT
    assert no_network == []


# ---------------------------------------------------------------------------------------------------- the broker
#: The md / ARM trees and the runtime ports that join them to the broker side: none may open a network client of its
#: own (the composition root `optional_plan.py` hands the ARM notice channels the session's HTTP client, a
#: notification path, never a broker one).
MD_ARM_TREES = ("src/marketdata", "src/riskmgr", "src/armconfig")
MD_ARM_FILES = ("src/runtime/md_ports.py", "src/runtime/arm_ports.py", "src/runtime/optional.py",
                "src/runtime/optional_md.py")
NETWORK_MODULES = {"httpx", "aiohttp", "requests", "urllib3", "socket", "websockets"}
"""HTTP / socket clients (a broker path needs one); `ssl` and `asyncpg` (the Supabase config reader) are not."""


def _py_files(*roots: str) -> list[Path]:
    out: list[Path] = []
    for r in roots:
        p = ROOT / r
        out.extend(sorted(p.rglob("*.py")) if p.is_dir() else [p])
    return out


def _call_name(node: ast.Call) -> str:
    f = node.func
    return f.attr if isinstance(f, ast.Attribute) else f.id if isinstance(f, ast.Name) else ""


def test_every_adapter_rest_client_is_read_only_transport() -> None:
    """The only HTTP client an adapter builds (`reset_http_client`, `src/adapters/sockets.py`) is wrapped in
    `ReadOnlyTransport` at its one call site (`StoneXRestClient.create`), whose constructor takes nothing else; the
    cTrader (Pepperstone) adapter has no HTTP client at all; the md / ARM code imports no network module (R11-m;
    design §B3: `ReadOnlyTransport` is the hard guarantee)."""
    callers: list[Path] = []
    for path in _py_files("src/adapters"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = _call_name(node)
            assert name not in ("AsyncClient", "Client") or path.name == "sockets.py", \
                f"{path.relative_to(ROOT)} builds an HTTP client outside sockets.py"
            if name == "reset_http_client":
                callers.append(path)
    assert [p.relative_to(ROOT).as_posix() for p in callers] == ["src/adapters/stonex/rest.py"]
    create = ast.parse(textwrap.dedent(inspect.getsource(StoneXRestClient.create)))
    wrapped = [n for n in ast.walk(create) if isinstance(n, ast.Call) and _call_name(n) == "ReadOnlyTransport"]
    assert len(wrapped) == 1 and isinstance(wrapped[0].args[0], ast.Name) and wrapped[0].args[0].id == "client"
    assert "ReadOnlyTransport" in str(inspect.signature(StoneXRestClient.__init__).parameters["transport"].annotation)
    for path in _py_files("src/adapters/pepperstone"):
        assert "httpx" not in path.read_text(encoding="utf-8"), path.name
    for path in _py_files(*MD_ARM_TREES, *MD_ARM_FILES):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            assert not {n.split(".")[0] for n in names} & NETWORK_MODULES, \
                f"{path.relative_to(ROOT)} imports a network module"


async def test_a_denied_path_never_reaches_the_wire(no_network: list[str]) -> None:
    """A StoneX REST client built the production way refuses every order-changing path before any byte is sent."""
    settings = StoneXSettings(username="u", password="p", app_key="k", base_url_v2="https://broker.invalid/v2",
                              base_url_v1="https://broker.invalid/TradingAPI", stream_url="https://broker.invalid/ls")
    rest = StoneXRestClient.create(settings)
    transport = rest._t
    try:
        assert isinstance(transport, ReadOnlyTransport)
        for path in ("/order/newtradeorder", "/order/newstoplimitorder", "/order/updatetradeorder",
                     "/order/updatestoplimitorder", "/order/cancel"):
            assert is_denied_path(path)
            with pytest.raises(ExecutionLockedError):
                await transport.request("POST", settings.base_url_v1 + path, json={})
    finally:
        await transport.aclose()
    assert len(no_network) == 5 and all(is_denied_path(p) for p in no_network)   # refused before the wire


def test_no_adapter_or_md_port_has_an_order_changing_method() -> None:
    from src.adapters.pepperstone.adapter import PepperstoneAdapter
    from src.adapters.stonex.adapter import StoneXAdapter
    from src.marketdata.port_impl import MdPort
    from src.runtime.md_ports import StopHandleReadPortImpl
    for cls in (MockBrokerAdapter, StoneXAdapter, PepperstoneAdapter, MdPort, StopHandleReadPortImpl):
        assert_read_only(cls)
    assert {"place_order", "modify_order", "cancel_order"} <= FORBIDDEN_METHOD_NAMES
    assert transport_mod.DENY_PATH_PATTERNS


# ---------------------------------------------------------------------------------------------------- PR1
def _stream(o: Any, broker_ms: int) -> Any:
    return replace(o, broker_updated_at_ms=broker_ms, broker_time_source="BROKER", origin="stream")


async def test_stop_moved_between_frames_seen_by_oversee_and_arm_at_the_same_level(tmp_path: Path) -> None:
    """Design §C3 PR1, the read `XTNL_ARM_STOP_READS=1` turns on: the stop child is moved at the broker with no
    stream frame; the PR9 stop-order list read (`StopHandleReadPortImpl` over the orchestrator's
    `reread_stop_orders` and the mock broker's `fetch_active_stop_orders`) publishes it with origin `stop_read`; once
    committed, Oversee's `effective_stop_detail` and the ARM's `S_at_T` function (`stop_at_bar_end` over `event_log`
    through `PgArmVersionReader`) give the same level, before and after the move. The flag itself and the ARM
    pipeline (`PipelineEvaluator` with `ArmSettings(stop_reads=True)`) are not run here: the test calls the `S_at_T`
    function directly (plan S38 deviation 9). The broker is asked one list read and nothing else."""
    clock = ManualClock(1_700_003_000_000)
    t_stream = clock.ms - 120_000
    pos = labelled(position("P1", stop=None, updated_ms=t_stream), t_stream)
    child = _stream(stop_child("S1", "P1", units="1000", level="1.1900"), t_stream)
    mock = MockBrokerAdapter(orders=[child], positions=[pos], wall_ms=lambda: clock.ms + 3)
    env = make_orchestrator(tmp_path, clock, adapter=mock)  # type: ignore[arg-type]
    await env.orch.run_snapshot("mock")
    env.orders(), env.positions(), env.account_items()
    emu = E2eEmulator()
    reader = PgArmVersionReader(engine_db(emu))
    current: dict[str, Any] = {}
    seq = [0]

    def commit(dtos: list[Any]) -> None:
        """The consumer's commit: the current row and one `event_log` version (the repository's labels)."""
        for d in dtos:
            seq[0] += 1
            if isinstance(d, OrderDTO):
                current[d.broker_order_id] = d
                origin, broker_ms = version_labels(d)
                emu.add_version(entity_type="ORDER", entity_key=order_key(d.adapter_id, d.broker_order_id),
                                seq_id=seq[0], payload=json.loads(dto_json(d)), origin=origin,
                                broker_time_ms=broker_ms)
            else:
                current[d.position_id] = d
                log_position(emu, d, seq[0])

    async def levels(bar_end_ms: int) -> tuple[Decimal | None, Decimal | None]:
        orders = [o for o in current.values() if isinstance(o, OrderDTO)]
        p = current["P1"]
        detail = effective_stop_detail(p, [p], orders)
        rows = await reader.stop_versions("mock", "P1", ["S1"], bar_end_ms)
        at = stop_at_bar_end(stop_versions_of(rows), side="BUY", entry=p.entry_price, units_to_cover=p.units,
                             bar_end_ms=bar_end_ms)
        return (None if detail is None else detail.level), (at.level if isinstance(at, StopAtT) else None)

    commit([pos, child])                                       # the stream frames that opened P1 and its stop
    assert await levels(clock.ms) == (Decimal("1.1900"), Decimal("1.1900"))

    t_move = clock.ms - 10_000                                 # moved at the broker; no stream frame arrives
    mock.set_order(_stream(stop_child("S1", "P1", units="1000", level="1.1950"), t_move))
    mock.calls.clear()
    port = StopHandleReadPortImpl(env.orch.reread_stop_orders)
    read = await port.read_stop_handles("mock", [HandleRef("CHILD_ORDER", "S1")])
    assert isinstance(read, StopHandleRead) and [h.status for h in read.handles] == [HandleStatus.OK]
    published = env.orders()
    assert [(o.broker_order_id, o.origin) for o in published] == [("S1", "stop_read")]
    commit(published)
    oversee, arm = await levels(clock.ms)
    assert oversee is not None and oversee == arm == Decimal("1.1950")
    assert (await levels(t_move - 1))[1] == Decimal("1.1900")  # a bar that ended before the move: the old level
    assert mock.calls == ["fetch_active_stop_orders"]


# ---------------------------------------------------------------------------------------------------- flags off
async def test_both_flags_off_the_engine_runs_as_before_with_nothing_new(tmp_path: Path) -> None:
    """Both flags `0`: the real `EngineSession` (the existing in-memory replay scenario, MockBrokerAdapter) reaches
    LEADER and runs its scenario unchanged; `start_optional_services` at LEADER entry adds no task and never calls
    the bootstrap factory; no market-data hook, no optional view, no operator-request handler, no `md:*` key, no
    kind `arm` outbox row; the broker is asked reads only (no bar read, no stop-order list read)."""
    from src.db.epoch import Epoch
    from src.runtime.optional_settings import optional_flags_from_env
    from src.runtime.session import EngineSession, SessionContext
    from tests.e2e.memory_backend import MemoryBackend
    from tests.e2e.scenario import FAST_TIMINGS, NODE, FakeLease, fixture, mock_adapter, run_scenario, wall_ms

    epoch = Epoch("cluster-e2e", 7)
    redis = fakeredis.FakeAsyncRedis()

    async def run_id(_: Any) -> str:
        return "run-e2e"

    built: list[object] = []

    def factory(session: Any) -> Any:
        built.append(session)
        raise AssertionError("the optional bootstrap was built with both flags off")

    backend = MemoryBackend(redis, run_id, epoch.token)
    fx = fixture(wall_ms())
    adapter = mock_adapter(fx)
    exits: list[str] = []
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
                         adapters=lambda: [adapter], lease=FakeLease(), run_id_reader=run_id, exit_fn=exits.append,
                         timings=FAST_TIMINGS,
                         optional_flags=optional_flags_from_env({"XTNL_MD_ENABLED": "0", "XTNL_ARM_ENABLED": "0"}),
                         optional_bootstrap=factory)
    session = EngineSession(ctx, epoch)
    leader = await run_scenario(session, adapter, fx, epoch, backend)
    try:
        session.start_optional_services()                        # what the NodeController calls at LEADER entry
        assert built == [] and not leader.supervisor.has("optional-bootstrap")
        names = set(leader.supervisor._rt)
        assert not {n for n in names if n.startswith(("md-", "arm", "optional"))}
        assert session._md_hook is None and session.optional_view is None and session._extra_closers == []
        assert await redis.keys("md:*") == []
        assert [k for k, _ in await backend.outbox() if k == "arm"] == []
        assert not exits and ("reached_leader", epoch.label) in backend.tables.terms
        asked = set(adapter.calls)
        assert asked and not asked & FORBIDDEN_METHOD_NAMES
        assert not asked & {"fetch_price_bars", "fetch_active_stop_orders"}
    finally:
        await leader.shutdown()
        await redis.flushall()
        await redis.aclose()
    assert not leader.exits and not exits


# ---------------------------------------------------------------------------------------------------- real redis-md
MD_URL = os.environ.get("ORDER_SERVER_TEST_REDIS_MD")


@pytest.mark.md
@pytest.mark.skipif(not MD_URL, reason="ORDER_SERVER_TEST_REDIS_MD not set")
async def test_the_end_to_end_scenario_on_the_dev_redis_md(no_network: list[str]) -> None:
    """The same scenario with the market data on the DISPOSABLE dev `redis-md` (compose profile `md`) as `md_writer`
    under its ACL: the same decisions, the same notices. Every key the run wrote is deleted afterwards, the
    store-wide ones too (`md:writer`, `md:boot`, `md:events`, the registries), so the `md` tests run in any order."""
    from redis.asyncio import Redis

    from src.marketdata.store import keys as K
    from tests.e2e.arm_rig import SERIES_ASK, SERIES_BID
    from tests.marketdata.conftest import STORE_GLOBAL_KEYS
    from tests.marketdata.test_acl_live import _url

    redis = Redis.from_url(_url("md_writer"))
    info = await redis.info("server")
    e = await e2e_rig(redis=redis, run_id=str(info["run_id"]))
    try:
        bars = await run_main_scenario(e)
        outcomes = [(d["event"], d["bar_end_ms"] - bars["b1"], d["outcome"]) for d in e.decisions()]
        assert outcomes[1:] == [("POST", 0, "STRATEGY_REASON"), ("PRE", TF, "ADJUST"), ("POST", TF, "ADJUST"),
                                ("POST", 2 * TF, "COMPLETE")]
        assert e.row()["state"] == ArmState.STRATEGY_COMPLETE.value and e.rec.pages == [] and no_network == []
    finally:
        e.barclock.close()
        keys = [k for s in (SERIES_BID, SERIES_ASK) for k in (*K.series_keys(s), K.quarantine(s))]
        keys += [K.ticks(ADAPTER, SYMBOL), *STORE_GLOBAL_KEYS]
        await redis.delete(*keys)
        await redis.aclose()
