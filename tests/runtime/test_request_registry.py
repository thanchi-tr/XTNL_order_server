"""S10 PR-G2 runtime side (design §B13, §B6.1 PR13; plan S10: RT-M8, RT-M10, IN-9, ST-13, R11-m non-blocking handlers).

* The operator loop reads the core kinds plus the registered ones: 60 pending `arm_disarm` rows without a handler
  never hide a `reset_task`; they stay pending (never `unsupported_kind`) and are applied once a handler registers
  in the same epoch.
* Registered handlers are non-blocking: one that hands its work to a hanging owner task does not delay a
  `reset_task` beyond one `operator_poll_s`; the owner answers later through `complete_request`.
* Accepted, unanswered requests take no slot of the pending query's LIMIT 50: 50 of them waiting on a hung owner
  never hide a `reset_task`.
* Handler bugs (raise, awaitable, wrong type) are stored as `refused:<code>`; an awaitable is closed or cancelled.
  An accepted request whose owner task was registered and is gone is dispatched again; one whose owner was never
  registered is waited for without calling the handler again.
* Result reasons are codes of letters and `_` only: no number can be stored as a reason.
* Health records carry the node's registry tokens (`registry`, empty until S32) and parse without them.
* The ops etcd client lists a prefix (`get_prefix`, the runtime client's implementation; `HEALTH_PREFIX` from
  `src/runtime/etcd.py`).

Requests carry synthetic ids only; no market data appears in any request, result, page or log checked here.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable
from typing import Any

import fakeredis
import pytest

from src.ops import etcd as ops_etcd
from src.runtime.etcd import HEALTH_PREFIX, HttpEtcdClient
from src.runtime.health import HealthRecord
from src.runtime.ports import OperatorRequest
from src.runtime.probes import NodeHealthProbe
from src.runtime.request_handlers import (
    HANDLER_ERROR,
    HANDLER_NOT_SYNC,
    Accepted,
    Applied,
    HandlerResult,
    Moot,
    Refused,
)
from src.runtime.supervisor import TaskContext, TaskSpec, TaskSupervisor
from tests.db.conftest import FakeConn, FakeSource
from tests.e2e.scenario import FAST_TIMINGS, NODE
from tests.ops.conftest import FakeEtcd as OpsFakeEtcd
from tests.runtime.fakes import record
from tests.runtime.test_followups_4_9 import Built, FakeQueue, built  # noqa: F401  (fixture)

EPOCH_TOKEN = 5          # the `built` session's epoch token
CORE = {"reset_task", "release_account", "refresh_cash_flows"}


async def _idle(ctx: TaskContext) -> None:
    await asyncio.Event().wait()


def _sup(audits: list[tuple[str, str, str]], *extra: TaskSpec) -> TaskSupervisor:
    return TaskSupervisor([TaskSpec("risksweep", _idle, max_stamp_age_s=5.0), *extra],
                          exit_fn=lambda reason: None, audit=lambda op, name, who: audits.append((op, name, who)))


def _results(queue: FakeQueue) -> dict[int, str]:
    return {rid: result for _, rid, result in queue.consumed}


# ============================================================================================ kind filter
async def test_sixty_unhandled_requests_never_hide_a_reset_task(built: Built) -> None:
    s = built.session
    audits: list[tuple[str, str, str]] = []
    sup = _sup(audits)
    s._sup = sup
    queue = FakeQueue([OperatorRequest(i, "arm_disarm", f"P{i}", "alice", {"v": 1}) for i in range(1, 61)]
                      + [OperatorRequest(61, "reset_task", "risksweep", "bob")])
    s.operator_requests = queue
    try:
        await s.operator_pass()
        assert queue.kinds_asked[-1] == CORE                # no handler: arm_disarm is not even read
        assert queue.consumed == [(EPOCH_TOKEN, 61, "applied")] and audits == [("reset_task", "risksweep", "bob")]
        await s.operator_pass()
        assert len(queue.consumed) == 1                      # the 60 stay pending, never `unsupported_kind`

        handled: list[int] = []

        def disarm(req: OperatorRequest) -> HandlerResult:
            handled.append(req.request_id)
            return Applied()

        s.register_request_handler("arm_disarm", disarm)     # registered later in the same epoch
        assert s.request_kinds() == CORE | {"arm_disarm"}
        await s.operator_pass()
        assert queue.kinds_asked[-1] == CORE | {"arm_disarm"}
        assert handled == list(range(1, 51))                 # LIMIT 50, oldest first
        await s.operator_pass()
        assert handled == list(range(1, 61))
        res = _results(queue)
        assert all(res[i] == "applied" for i in range(1, 61)) and len(res) == 61
    finally:
        await sup.stop_all()


async def test_registration_rules_and_unregister(built: Built) -> None:
    s = built.session
    with pytest.raises(ValueError):
        s.register_request_handler("reset_task", lambda r: Applied())        # a core kind
    with pytest.raises(ValueError):
        s.register_request_handler("Arm Disarm", lambda r: Applied())        # not a kind
    s.register_request_handler("md_rebuild", lambda r: Moot("already_built"))
    s.register_request_handler("md_rebuild", lambda r: Refused("not_running"))   # replaces
    assert "md_rebuild" in s.request_kinds()
    s._sup = TaskSupervisor([])
    queue = FakeQueue([OperatorRequest(1, "md_rebuild", "atr:1", "alice")])
    s.operator_requests = queue
    await s.operator_pass()
    assert queue.consumed == [(EPOCH_TOKEN, 1, "refused:not_running")]
    s.unregister_request_handler("md_rebuild")
    assert s.request_kinds() == CORE
    queue.reqs.append(OperatorRequest(2, "md_rebuild", "atr:1", "alice"))
    await s.operator_pass()
    assert len(queue.consumed) == 1                          # read no more: stays pending


def test_result_codes_are_value_free() -> None:
    assert Applied().result() == "applied"
    assert Refused("no_position").result() == "refused:no_position"
    assert Moot("closed").result() == "moot:closed"
    for bad in ("", "has space", "UPPER", "x" * 65, "1.2345 level", "1.2345", "105000", "0.5-1.5", "level_1.2345",
                "_lead", "atr14", "a.b", "a-b"):
        with pytest.raises(ValueError):
            Refused(bad)
        with pytest.raises(ValueError):
            Moot(bad)
    with pytest.raises(ValueError):
        Accepted("")
    assert HANDLER_ERROR.result() == "refused:handler_error"
    assert HANDLER_NOT_SYNC.result() == "refused:handler_not_sync"


# ============================================================================================ non-blocking handlers
async def test_a_hanging_owner_task_does_not_delay_a_reset_task(built: Built) -> None:
    """Design §B13: a `reset_task` filed behind a hanging `arm_clear_failed` probe is applied within one
    `operator_poll_s`; the probe's owner answers later."""
    s = built.session
    work: asyncio.Queue[int] = asyncio.Queue()
    release = asyncio.Event()
    probing: list[int] = []

    async def probe_runner(ctx: TaskContext) -> None:
        while True:
            rid = await work.get()
            probing.append(rid)
            await release.wait()                             # the probe hangs until released
            assert s.complete_request(rid, Applied())
            ctx.stamp()

    audits: list[tuple[str, str, str]] = []
    sup = _sup(audits, TaskSpec("arm-failed-probe", probe_runner, max_stamp_age_s=3600.0))
    s._sup = sup
    sup.start()

    def clear_failed(req: OperatorRequest) -> HandlerResult:
        work.put_nowait(req.request_id)                      # enqueue to the owning task, return at once
        return Accepted("arm-failed-probe")

    s.register_request_handler("arm_clear_failed", clear_failed)
    queue = FakeQueue([OperatorRequest(1, "arm_clear_failed", "series:x", "alice"),
                       OperatorRequest(2, "reset_task", "risksweep", "bob")])
    s.operator_requests = queue
    try:
        await asyncio.wait_for(s.operator_pass(), FAST_TIMINGS.operator_poll_s)
        assert queue.consumed == [(EPOCH_TOKEN, 2, "applied")]
        await asyncio.sleep(0)
        assert probing == [1]
        await asyncio.wait_for(s.operator_pass(), FAST_TIMINGS.operator_poll_s)
        assert len(queue.consumed) == 1 and probing == [1]   # accepted: skipped, never handed over twice
        release.set()
        for _ in range(20):
            await asyncio.sleep(0)
        await s.operator_pass()
        assert _results(queue) == {1: "applied", 2: "applied"}
        assert not s.complete_request(1, Moot("late"))       # answered and consumed: no second answer
        await s.operator_pass()
        assert len(queue.consumed) == 2
    finally:
        await sup.stop_all()


async def test_complete_request_rules(built: Built) -> None:
    s = built.session
    s._sup = sup = TaskSupervisor([TaskSpec("arm-tracker", _idle, max_stamp_age_s=60.0)], exit_fn=lambda r: None)
    s.register_request_handler("arm_disarm", lambda r: Accepted("arm-tracker"))
    queue = FakeQueue([OperatorRequest(7, "arm_disarm", "P1", "alice")])
    s.operator_requests = queue
    assert not s.complete_request(7, Applied())              # not accepted yet
    await s.operator_pass()
    assert queue.consumed == []
    with pytest.raises(TypeError):
        s.complete_request(7, Accepted("arm-tracker"))  # type: ignore[arg-type]
    assert s.complete_request(7, Moot("position_closed"))
    assert not s.complete_request(7, Applied())              # one answer only
    await s.operator_pass()
    assert queue.consumed == [(EPOCH_TOKEN, 7, "moot:position_closed")]
    await sup.stop_all()


async def test_an_accepted_request_whose_owner_is_gone_is_dispatched_again(built: Built) -> None:
    s = built.session
    s._sup = sup = TaskSupervisor([TaskSpec("md-backfill", _idle, max_stamp_age_s=60.0)], exit_fn=lambda r: None)
    calls: list[int] = []

    def to_owner(req: OperatorRequest) -> HandlerResult:
        calls.append(req.request_id)
        return Accepted("md-backfill")

    s.register_request_handler("md_backfill", to_owner)
    queue = FakeQueue([OperatorRequest(3, "md_backfill", "series:x", "alice")])
    s.operator_requests = queue
    await s.operator_pass()
    await s.operator_pass()
    assert calls == [3] and queue.consumed == [] and queue.excluded_asked[-1] == {3}
    s._sup = TaskSupervisor([])                               # the owner task was removed: dispatch again
    await s.operator_pass()
    assert calls == [3, 3] and queue.consumed == []
    await sup.stop_all()


async def test_an_owner_that_was_never_registered_is_waited_for_without_calling_the_handler_again(
        built: Built) -> None:
    """Accepted('x') with no task 'x' registered (the optional service is off): the handler is called once, the
    request waits (no slot of the LIMIT, no repeated enqueue), and the owner answers once it is registered."""
    s = built.session
    s._sup = TaskSupervisor([])
    calls: list[int] = []

    def to_ghost(req: OperatorRequest) -> HandlerResult:
        calls.append(req.request_id)
        return Accepted("ghost-task")

    s.register_request_handler("md_backfill", to_ghost)
    queue = FakeQueue([OperatorRequest(3, "md_backfill", "series:x", "alice")])
    s.operator_requests = queue
    for _ in range(3):
        await s.operator_pass()
    assert calls == [3] and queue.consumed == [] and queue.excluded_asked[-1] == {3}
    s._sup = sup = TaskSupervisor([TaskSpec("ghost-task", _idle, max_stamp_age_s=60.0)], exit_fn=lambda r: None)
    await s.operator_pass()                                   # the owner is registered now: never re-dispatched
    assert calls == [3] and queue.consumed == []
    assert s.complete_request(3, Applied())
    await s.operator_pass()
    assert queue.consumed == [(EPOCH_TOKEN, 3, "applied")] and calls == [3]
    await sup.stop_all()


async def test_fifty_accepted_requests_on_a_hung_owner_never_hide_a_reset_task(built: Built) -> None:
    """Design §B13: accepted, unanswered requests are left out of the pending query, so they take no slot of its
    LIMIT 50 and a later `reset_task` is still read."""
    s = built.session
    audits: list[tuple[str, str, str]] = []
    sup = _sup(audits, TaskSpec("arm-failed-probe", _idle, max_stamp_age_s=3600.0))
    s._sup = sup
    s.register_request_handler("arm_clear_failed", lambda r: Accepted("arm-failed-probe"))
    queue = FakeQueue([OperatorRequest(i, "arm_clear_failed", f"series:{i}", "alice") for i in range(1, 51)])
    s.operator_requests = queue
    try:
        await s.operator_pass()
        assert queue.consumed == [] and len(s._accepted_requests) == 50
        queue.reqs.append(OperatorRequest(51, "reset_task", "risksweep", "bob"))
        await asyncio.wait_for(s.operator_pass(), FAST_TIMINGS.operator_poll_s)
        assert queue.excluded_asked[-1] == frozenset(range(1, 51))
        assert queue.consumed == [(EPOCH_TOKEN, 51, "applied")] and audits == [("reset_task", "risksweep", "bob")]
    finally:
        await sup.stop_all()


@pytest.mark.parametrize(("handler", "expected"), [
    (lambda r: (_ for _ in ()).throw(RuntimeError("boom 1.2345")), "refused:handler_error"),
    (lambda r: "applied", "refused:handler_error"),
])
async def test_a_buggy_handler_is_refused_and_never_blocks_core(built: Built, handler: Any, expected: str,
                                                              caplog: pytest.LogCaptureFixture) -> None:
    s = built.session
    audits: list[tuple[str, str, str]] = []
    sup = _sup(audits)
    s._sup = sup
    s.register_request_handler("md_rehash", handler)
    queue = FakeQueue([OperatorRequest(1, "md_rehash", "k2", "alice"),
                       OperatorRequest(2, "reset_task", "risksweep", "bob")])
    s.operator_requests = queue
    try:
        with caplog.at_level(logging.WARNING):
            await s.operator_pass()
        assert queue.consumed == [(EPOCH_TOKEN, 1, expected), (EPOCH_TOKEN, 2, "applied")]
        assert "1.2345" not in caplog.text                   # error type only, never the message
    finally:
        await sup.stop_all()


async def test_an_async_handler_is_closed_unrun_and_refused(built: Built) -> None:
    s = built.session
    s._sup = TaskSupervisor([])
    ran: list[int] = []

    async def slow(req: OperatorRequest) -> HandlerResult:
        ran.append(req.request_id)
        await asyncio.sleep(3600)
        return Applied()

    def wrong(req: OperatorRequest) -> Awaitable[HandlerResult]:
        return slow(req)

    s.register_request_handler("md_writer_reset", wrong)  # type: ignore[arg-type]
    queue = FakeQueue([OperatorRequest(1, "md_writer_reset", "w", "alice")])
    s.operator_requests = queue
    await asyncio.wait_for(s.operator_pass(), FAST_TIMINGS.operator_poll_s)
    assert queue.consumed == [(EPOCH_TOKEN, 1, "refused:handler_not_sync")] and ran == []


@pytest.mark.parametrize("kind", ["task", "future"])
async def test_a_task_or_future_returned_by_a_handler_is_cancelled_and_refused(built: Built, kind: str) -> None:
    """A Task or Future has no `close()`: it is cancelled, so nothing keeps running unsupervised."""
    s = built.session
    s._sup = TaskSupervisor([])
    ran: list[int] = []
    returned: list[asyncio.Future[Any]] = []

    async def slow(req: OperatorRequest) -> HandlerResult:
        ran.append(req.request_id)
        await asyncio.sleep(3600)
        return Applied()

    def wrong(req: OperatorRequest) -> asyncio.Future[Any]:
        out: asyncio.Future[Any] = (asyncio.ensure_future(slow(req)) if kind == "task"
                                    else asyncio.get_running_loop().create_future())
        returned.append(out)
        return out

    s.register_request_handler("md_writer_reset", wrong)  # type: ignore[arg-type]
    queue = FakeQueue([OperatorRequest(1, "md_writer_reset", "w", "alice")])
    s.operator_requests = queue
    await asyncio.wait_for(s.operator_pass(), FAST_TIMINGS.operator_poll_s)
    for _ in range(3):
        await asyncio.sleep(0)
    assert queue.consumed == [(EPOCH_TOKEN, 1, "refused:handler_not_sync")]
    assert returned[0].cancelled() and ran == []


# ============================================================================================ health registry
def test_health_record_without_registry_parses_and_round_trips() -> None:
    old = json.loads(record(ts_ms=4).to_json())
    del old["registry"]                                     # a node built before S10
    parsed = HealthRecord.from_json(json.dumps(old))
    assert parsed.registry == () and parsed == record(ts_ms=4)
    rec = record(ts_ms=5, registry=("guard.first_bar:1", "strategy.break_even_on_risk_close:1"))
    assert HealthRecord.from_json(rec.to_json()) == rec
    bad = json.loads(rec.to_json())
    bad["registry"] = "x"
    assert HealthRecord.from_json(json.dumps(bad)).registry == ()
    bad["registry"] = ["a:1", 3, None, ""]
    assert HealthRecord.from_json(json.dumps(bad)).registry == ("a:1",)


async def test_probe_fills_the_registry_from_the_injected_tokens() -> None:
    redis = fakeredis.FakeAsyncRedis()

    async def chrony() -> float:
        return 0.1

    probe = NodeHealthProbe(NODE, pg=FakeSource(FakeConn()), redis=redis, chrony=chrony, timeout_s=0.2)
    try:
        assert (await probe.probe(1)).registry == ()          # nothing injected until S32
        probe.registry_tokens = lambda: ["strategy.b:2", "guard.a:1", "strategy.b:2"]
        assert (await probe.probe(2)).registry == ("guard.a:1", "strategy.b:2")

        def broken() -> list[str]:
            raise RuntimeError("registry fault")

        probe.registry_tokens = broken
        rec = await probe.probe(3)
        assert rec.registry == () and rec.etcd_reachable          # a registry fault never fails a hard check
    finally:
        await redis.aclose()


# ============================================================================================ ops etcd
def test_ops_etcd_lists_health_records_by_prefix() -> None:
    assert ops_etcd.HEALTH_PREFIX == HEALTH_PREFIX == "/xtnl/health/"
    assert ops_etcd.OpsEtcdClient.get_prefix is HttpEtcdClient.get_prefix     # the runtime client's implementation
    etcd = OpsFakeEtcd()
    etcd.put(HEALTH_PREFIX + "n2", record("n2", 9, registry=("guard.a:1",)).to_json(), timeout_s=1.0)
    etcd.put(HEALTH_PREFIX + "n1", record("n1", 8).to_json(), timeout_s=1.0)
    etcd.put("/xtnl/leader", "n1", timeout_s=1.0)
    client: ops_etcd.OpsEtcd = etcd
    kvs = client.get_prefix(HEALTH_PREFIX, timeout_s=1.0)
    assert [kv.key for kv in kvs] == [HEALTH_PREFIX + "n1", HEALTH_PREFIX + "n2"]
    assert [HealthRecord.from_json(kv.value).registry for kv in kvs] == [(), ("guard.a:1",)]
