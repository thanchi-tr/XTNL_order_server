"""S31 (plan RT-1; design PR12, PR9, §B14; R11-M10, R11-M11, R11-M20, R11-M05): the runtime port wrappers of
`src/runtime/md_ports.py`. Synthetic instruments only; no assertion message carries a value."""
from __future__ import annotations

import ast
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from src.adapters.base import (
    AuthFailedError,
    BrokerError,
    PriceBarsDecodeError,
    TransientBrokerError,
)
from src.adapters.mock import MockBrokerAdapter
from src.adapters.ratelimit import RestBudget, RestPriority
from src.calendar.machine import SessionState
from src.db.arm_ports import NodeMdRecordRow
from src.db.epoch import Epoch
from src.dto import OrderStatus
from src.dto.bars import BrokerBar, PriceBarsResponse
from src.marketdata.ports import HandleRef, HandleStatus, NodeMdRecord, StopHandleRead
from src.marketdata.types import Side, Transient, Unsupported
from src.pubsub.orchestrator import RereadResult, RereadStatus, StopReadResult
from src.riskmgr.lockout import is_banned_name
from src.riskmgr.ports import FailedSpoolRecord, Pager
from src.riskmgr.types import ScopeKind
from src.runtime.md_ports import (
    AdapterHealthPortImpl,
    BrokerBarsPortImpl,
    CalendarPortImpl,
    FailedSpoolPortImpl,
    LeaderCheckPortImpl,
    NodeRecordPortImpl,
    PagerWrapper,
    PgHealthPortImpl,
    StampPortImpl,
    StopHandleReadPortImpl,
    decode_frames,
    encode_frame,
    record_payload,
    scan_frames,
)
from src.runtime.supervisor import TaskContext
from tests.oversee.helpers import order, position

MD_PORTS = Path(__file__).resolve().parents[2] / "src" / "runtime" / "md_ports.py"
T0 = 1_700_000_100_000 - 1_700_000_100_000 % 900_000


def bar(start: int) -> BrokerBar:
    one = Decimal("1.5")
    return BrokerBar(bucket_start_ms=start, open=one, high=one, low=one, close=one)


# ============================================================================================ broker bars
async def test_bars_are_read_at_the_bars_priority_and_returned_as_is() -> None:
    mock = MockBrokerAdapter()
    mock.set_price_bars("SYN1", "BID", 900, [bar(T0), bar(T0 + 900_000)])
    port = BrokerBarsPortImpl({"mock": mock}.__getitem__)
    got = await port.fetch_price_bars("mock", "SYN1", Side.BID, 900, count=2)
    assert isinstance(got, PriceBarsResponse) and [b.bucket_start_ms for b in got.bars] == [T0, T0 + 900_000]
    assert mock.rest_calls[-1] == ("fetch_price_bars", RestPriority.BARS)


@pytest.mark.parametrize("error, want", [
    (TransientBrokerError("throttled"), Transient("refused")),
    (PriceBarsDecodeError("bad shape"), Transient("decode")),
    (AuthFailedError("auth"), Transient("auth")),
    (BrokerError("other"), Transient("broker_error")),
])
async def test_broker_errors_become_values(error: BrokerError, want: Transient) -> None:
    mock = MockBrokerAdapter()
    mock.fail_price_bars("SYN1", "BID", 900, error)
    port = BrokerBarsPortImpl({"mock": mock}.__getitem__)
    assert await port.fetch_price_bars("mock", "SYN1", Side.BID, 900, count=1) == want


async def test_unsupported_unknown_and_bad_requests_are_values() -> None:
    mock = MockBrokerAdapter()
    mock.set_price_bars_supported(False)
    port = BrokerBarsPortImpl({"mock": mock}.__getitem__)
    assert await port.fetch_price_bars("mock", "SYN1", Side.ASK, 900, count=1) == Unsupported("not_supported")
    assert await port.fetch_price_bars("other", "SYN1", Side.ASK, 900, count=1) == Unsupported("unknown_adapter")
    assert await port.fetch_price_bars("mock", "SYN1", Side.ASK, 900) == Unsupported("bad_request")


async def test_a_hung_bar_read_times_out_as_a_value() -> None:
    import asyncio

    class Hung(MockBrokerAdapter):
        async def fetch_price_bars(self, *a: Any, **kw: Any) -> PriceBarsResponse:
            await asyncio.Event().wait()
            raise AssertionError

    port = BrokerBarsPortImpl({"mock": Hung()}.__getitem__, timeout_s=0.01)
    assert await port.fetch_price_bars("mock", "SYN1", Side.BID, 900, count=1) == Transient("timeout")


# ============================================================================================ stop handles
def reread(result: StopReadResult | BaseException) -> Callable[[str], Awaitable[StopReadResult]]:
    calls: list[str] = []

    async def read(adapter_id: str) -> StopReadResult:
        calls.append(adapter_id)
        if isinstance(result, BaseException):
            raise result
        return result
    read.calls = calls  # type: ignore[attr-defined]
    return read


async def test_one_list_read_answers_every_pinned_handle() -> None:
    child = order("s1", status=OrderStatus.WORKING)
    pos = position("p1", stop="1.0900")
    rows = (RereadResult(RereadStatus.APPLIED, child, "v-child", 111),
            RereadResult(RereadStatus.NOOP, pos, "v-pos", None),
            RereadResult(RereadStatus.TRANSIENT, order("s2")))
    fn = reread(StopReadResult("mock", True, rows, sent_ms=100))
    port = StopHandleReadPortImpl(fn)
    handles = [HandleRef("CHILD_ORDER", "s1"), HandleRef("POSITION_FIELD", "p1"), HandleRef("CHILD_ORDER", "s2"),
               HandleRef("CHILD_ORDER", "gone")]
    got = await port.read_stop_handles("mock", handles)
    assert isinstance(got, StopHandleRead) and got.sent_ms == 100
    assert fn.calls == ["mock"]                                  # type: ignore[attr-defined]  # one read
    by = {h.handle.id: h for h in got.handles}
    assert (by["s1"].status, by["s1"].version, by["s1"].sent_ms) == (HandleStatus.OK, "v-child", 111)
    assert (by["p1"].status, by["p1"].version, by["p1"].sent_ms) == (HandleStatus.OK, "v-pos", 100)
    assert by["s2"].status is HandleStatus.TRANSIENT and by["gone"].status is HandleStatus.NOT_FOUND


@pytest.mark.parametrize("raised, want", [
    (TransientBrokerError("cap"), Transient("refused")),
    (BrokerError("x"), Transient("broker_error")),
    (KeyError("other"), Unsupported("unknown_adapter")),
    (TimeoutError(), Transient("timeout")),
])
async def test_stop_read_errors_are_values(raised: BaseException, want: object) -> None:
    port = StopHandleReadPortImpl(reread(raised))
    assert await port.read_stop_handles("mock", [HandleRef("CHILD_ORDER", "s1")]) == want


async def test_an_adapter_without_the_read_is_unsupported() -> None:
    port = StopHandleReadPortImpl(reread(StopReadResult("mock", False)))
    assert await port.read_stop_handles("mock", [HandleRef("CHILD_ORDER", "s1")]) == Unsupported("not_supported")


async def test_the_orchestrator_read_runs_at_bars_and_lists_once() -> None:
    """Against the mock adapter's real `fetch_active_stop_orders` through `reread_stop_orders`'s contract."""
    mock = MockBrokerAdapter()
    mock.set_stop_orders_supported(True)
    seen: list[RestPriority] = []

    async def read(adapter_id: str) -> StopReadResult:
        from src.adapters.ratelimit import current_rest_priority, rest_priority
        with rest_priority(RestPriority.BARS):
            listed = await mock.fetch_active_stop_orders()
            seen.append(current_rest_priority())
        return StopReadResult(adapter_id, listed.supported)

    got = await StopHandleReadPortImpl(read).read_stop_handles("mock", [HandleRef("CHILD_ORDER", "x")])
    assert isinstance(got, StopHandleRead) and seen == [RestPriority.BARS]
    assert got.handles[0].status is HandleStatus.NOT_FOUND


# ============================================================================================ calendar, health
def test_calendar_errors_count_as_closed() -> None:
    def boom(a: str, s: str) -> SessionState | None:
        raise RuntimeError("x")
    assert CalendarPortImpl(boom).state("mock", "SYN1") is None
    assert CalendarPortImpl(lambda a, s: SessionState.OPEN).state("mock", "SYN1") is SessionState.OPEN


def test_adapter_health_reads_streaming_and_the_rest_record() -> None:
    now = [1_000_000]
    budget = RestBudget("mock", rate_per_s=10, burst=10, wall_ms=lambda: now[0])
    budgets: dict[str, RestBudget | None] = {"mock": budget, "free": None}
    port = AdapterHealthPortImpl(lambda aid: aid == "mock", budgets.__getitem__, clock_ms=lambda: now[0])
    assert port.is_streaming("mock") and not port.is_streaming("free")
    assert port.rest_failing_everywhere("mock") is None              # no outcome yet: unknown
    assert port.rest_failing_everywhere("free") is None              # unmetered: unknown
    assert port.rest_failing_everywhere("nope") is None              # unknown adapter: unknown, never raises
    budget.note_result(RestPriority.POLL, False)
    budget.note_result(RestPriority.RECONCILE, False)
    assert port.rest_failing_everywhere("mock") is True
    budget.note_result(RestPriority.POLL, True)
    assert port.rest_failing_everywhere("mock") is False


def test_pg_health_fails_closed_on_errors() -> None:
    def boom() -> bool:
        raise RuntimeError("x")
    port = PgHealthPortImpl(lambda: 5, boom)
    assert port.last_fenced_commit_ms() == 5 and port.sweep_pg_unreachable() is True
    assert PgHealthPortImpl(lambda: None, lambda: False).sweep_pg_unreachable() is False


def test_stamp_port_wraps_the_task_context() -> None:
    clock = [1.0]
    ctx = TaskContext("t", lambda: clock[0])
    port = StampPortImpl(ctx)
    clock[0] = 5.0
    port.stamp()
    assert ctx.last_stamp == 5.0
    port.set_exempt("waiting")
    assert ctx.exempt and ctx.exempt_reason == "waiting"
    port.set_exempt(None)
    assert not ctx.exempt


# ============================================================================================ pager
def test_pager_wrapper_pages_and_its_mirror_never_pages(caplog: pytest.LogCaptureFixture) -> None:
    pages: list[tuple[str, str, str]] = []
    pager = PagerWrapper(lambda s, t, b: pages.append((s, t, b)))
    assert isinstance(pager, Pager)
    pager.page("high", "redis-md memory high", "75 %")
    pager.page("critical", "x", "y")
    with pytest.raises(ValueError):
        pager.page("loud", "x", "y")
    with caplog.at_level(logging.CRITICAL, logger="src.runtime.md_ports"):
        pager.log_mirror("ARM FAILED p1", "scope series")
    assert pages == [("high", "redis-md memory high", "75 %"), ("critical", "x", "y")]
    assert [r.levelno for r in caplog.records] == [logging.CRITICAL]
    with pytest.raises(AttributeError):
        pager.extra = 1  # type: ignore[attr-defined]               # __slots__: nothing else reachable


# ============================================================================================ leader, record
async def test_leader_check_fails_closed() -> None:
    epoch = Epoch("c", 7)

    async def same() -> Epoch | None:
        return Epoch("c", 7)

    async def newer() -> Epoch | None:
        return Epoch("c", 8)

    async def boom() -> Epoch | None:
        raise ConnectionError("down")

    assert await LeaderCheckPortImpl(lambda: True, epoch, same).confirm_leader()
    assert await LeaderCheckPortImpl(lambda: True, epoch).confirm_leader()
    assert not await LeaderCheckPortImpl(lambda: False, epoch, same).confirm_leader()
    assert not await LeaderCheckPortImpl(lambda: True, epoch, newer).confirm_leader()
    assert not await LeaderCheckPortImpl(lambda: True, epoch, boom).confirm_leader()


@dataclass
class Store:
    rows: dict[str, NodeMdRecordRow] = field(default_factory=dict)

    async def save_md_record(self, row: NodeMdRecordRow) -> bool:
        self.rows[row.node] = row
        return True

    async def load_md_record(self, node: str) -> NodeMdRecordRow | None:
        return self.rows.get(node)


async def test_node_record_round_trip_and_only_its_own_node() -> None:
    port = NodeRecordPortImpl(Store(), "node-a")
    assert await port.load_record() is None
    rec = NodeMdRecord(node="node-a", sentinel="r1:c:node-a:7.1", last_write_ms=5)
    await port.save_record(rec)
    assert await port.load_record() == rec
    with pytest.raises(ValueError):
        await port.save_record(NodeMdRecord(node="node-b", sentinel="s", last_write_ms=1))


async def test_node_record_with_a_hostname_outside_the_token_alphabet() -> None:
    """A node_id with dots and colons (an EC2 hostname): the md bootstrap's record carries the token form, the
    `arm_status` row stays keyed by the raw node_id, and the save succeeds."""
    store = Store()
    port = NodeRecordPortImpl(store, "a.b:c", record_node="a-b-c")
    rec = NodeMdRecord(node="a-b-c", sentinel="r1:c:a-b-c:7.1", last_write_ms=5)
    await port.save_record(rec)
    assert list(store.rows) == ["a.b:c"] and store.rows["a.b:c"].node == "a.b:c"
    assert await port.load_record() == rec
    with pytest.raises(ValueError):
        await port.save_record(NodeMdRecord(node="a.b:c", sentinel="s", last_write_ms=1))


# ============================================================================================ FAILED spool
def rec(fid: str, ping: str = "p1", **kw: Any) -> FailedSpoolRecord:
    base: dict[str, Any] = dict(failure_id=fid, scope_kind=ScopeKind.SERIES, scope_id="mock:SYN1:15m:BID:BROKER",
                                reason="invalid_bar", probe_kind="bar_reread", ping_id=ping, epoch=7,
                                first_fault_at_ms=10, entered_at_ms=20, counters=(("k", 3),),
                                position_ids=("pos1",))
    base.update(kw)
    return FailedSpoolRecord(**base)


async def test_spool_round_trip_latest_record_wins_and_ack_truncates(tmp_path: Path) -> None:
    spool = FailedSpoolPortImpl(tmp_path / "s" / "arm.spool")
    assert await spool.replay() == []
    await spool.append(rec("f1"))
    await spool.append(rec("f2", edge_code="default"))
    await spool.append(rec("f1", ping="p9"))                         # a fold: appended again
    got = await spool.replay()
    assert [(r.failure_id, r.ping_id) for r in got] == [("f1", "p9"), ("f2", "p1")]
    assert got[1].edge_code == "default" and got[0].counters == (("k", 3),)
    await spool.acknowledge(["f1"])
    assert [r.failure_id for r in await spool.replay()] == ["f2"]
    await spool.acknowledge(["f2"])
    assert await spool.replay() == [] and spool.path.read_bytes() == b""


async def test_a_corrupt_frame_is_skipped_and_paged_and_a_torn_tail_ignored(tmp_path: Path) -> None:
    pages: list[tuple[str, str, str]] = []
    path = tmp_path / "arm.spool"
    spool = FailedSpoolPortImpl(path, pager=lambda s, t, b: pages.append((s, t, b)))
    await spool.append(rec("f1"))
    await spool.append(rec("f2"))
    data = bytearray(path.read_bytes())
    data[20] ^= 0xFF                                                # one payload byte of the first frame
    path.write_bytes(bytes(data) + b"XFS1\x00")                      # plus a torn tail
    got = await spool.replay()
    assert [r.failure_id for r in got] == ["f2"]
    assert [t for _, t, _ in pages] == ["ARM FAILED spool damaged"] and "f1" not in pages[0][2]


async def test_a_torn_frame_mid_file_hides_neither_later_records_nor_later_acks(tmp_path: Path) -> None:
    """A torn append (power loss) followed by more frames: the read resyncs past the damage, so the later records
    replay and a later ack lands and is seen."""
    pages: list[tuple[str, str, str]] = []
    path = tmp_path / "arm.spool"
    a, c, d = (encode_frame(record_payload(rec(f))) for f in ("a", "c", "d"))
    path.write_bytes(a + c[: len(c) // 2] + c + d)                   # half a frame, then whole frames
    spool = FailedSpoolPortImpl(path, pager=lambda s, t, b: pages.append((s, t, b)))
    assert [r.failure_id for r in await spool.replay()] == ["a", "c", "d"]
    await spool.acknowledge(["a"])
    assert [r.failure_id for r in await spool.replay()] == ["c", "d"]
    await spool.acknowledge(["c", "d"])
    assert await spool.replay() == [] and path.read_bytes() == b""   # emptied, damage and all
    assert [t for _, t, _ in pages] == ["ARM FAILED spool damaged"]  # one damage, one page


async def test_a_torn_tail_is_cut_back_before_the_next_append(tmp_path: Path) -> None:
    pages: list[tuple[str, str, str]] = []
    path = tmp_path / "arm.spool"
    spool = FailedSpoolPortImpl(path, pager=lambda s, t, b: pages.append((s, t, b)))
    await spool.append(rec("a"))
    good = path.read_bytes()
    half = encode_frame(record_payload(rec("b")))
    path.write_bytes(good + half[: len(half) // 2])                  # the append that lost power
    await spool.append(rec("c"))
    assert path.read_bytes() == good + encode_frame(record_payload(rec("c")))
    assert decode_frames(path.read_bytes())[1] == 0 and [t for _, t, _ in pages] == ["ARM FAILED spool damaged"]
    await spool.acknowledge(["a", "c"])
    assert path.read_bytes() == b""


async def test_a_damaged_spool_pages_once_not_on_every_replay(tmp_path: Path) -> None:
    pages: list[tuple[str, str, str]] = []
    path = tmp_path / "arm.spool"
    path.write_bytes(encode_frame(record_payload(rec("a"))) + b"XFS1\x00")
    spool = FailedSpoolPortImpl(path, pager=lambda s, t, b: pages.append((s, t, b)))
    for _ in range(5):                                               # one replay per ARM retry
        assert [r.failure_id for r in await spool.replay()] == ["a"]
    assert len(pages) == 1


def test_scan_frames_reports_the_last_good_boundary() -> None:
    one, two = encode_frame({"a": 1}), encode_frame({"b": 2})
    assert scan_frames(one + two) == ([{"a": 1}, {"b": 2}], 0, len(one + two))
    assert scan_frames(one + b"junk" + two) == ([{"a": 1}, {"b": 2}], 1, len(one) + 4 + len(two))
    assert scan_frames(one + two[:7]) == ([{"a": 1}], 1, len(one))
    assert scan_frames(b"") == ([], 0, 0)


def test_frames_decode_and_refuse_bad_headers() -> None:
    frames, bad = decode_frames(encode_frame({"a": 1}) + encode_frame({"b": 2}))
    assert frames == [{"a": 1}, {"b": 2}] and bad == 0
    assert decode_frames(b"JUNKJUNKJUNKJUNK") == ([], 1)


async def test_spool_holds_no_value_fields(tmp_path: Path) -> None:
    spool = FailedSpoolPortImpl(tmp_path / "arm.spool")
    await spool.append(rec("f1"))
    text = (tmp_path / "arm.spool").read_bytes()
    for word in (b"price", b"close", b"level", b"bid", b"ask"):
        assert word not in text


# ============================================================================================ lockout name ban
def test_every_definition_in_md_ports_passes_the_name_ban() -> None:
    tree = ast.parse(MD_PORTS.read_text(encoding="utf-8"))
    names = [n.name for n in ast.walk(tree)
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))]
    assert names and not [n for n in names if is_banned_name(n)]


def test_md_ports_builds_no_http_client_and_names_no_mutation() -> None:
    text = MD_PORTS.read_text(encoding="utf-8")
    for banned in ("httpx", "AsyncClient", "place_order", "modify_order", "cancel_order", "aiohttp"):
        assert banned not in text
