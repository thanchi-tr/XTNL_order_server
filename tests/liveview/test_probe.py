"""The engine probe (docs/design/live-view.md s4.6, s6 "probe"): the allow-listed metrics only (the six of the
design plus the three W1 dead-man lines; every other line dropped unread), the LEADER-only adapter states, pass age
and dead-man lines, 3 failures -> down with the last success, and the HTTP limits (1 MiB cap, 2 s timeout, non-200
is a failure) against a real loopback server."""
from __future__ import annotations

import asyncio
import re
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest

from src.liveview import probe as P
from src.liveview.probe import KEPT, MAX_BYTES, EngineProbe, http_get, interpret, parse_metrics

LEADER_BODY = """\
# HELP xtnl_exporter_up exporter
# TYPE xtnl_exporter_up gauge
xtnl_exporter_up 1
xtnl_engine_uptime_seconds 86400
xtnl_account_equity_aud{account="ACC1"} 104523.17
xtnl_account_balance{account="ACC1",currency="AUD"} 100000
xtnl_unrealised_pnl{account="ACC1"} -12.5
xtnl_spread_pips{symbol="EURUSD"} 0.8
xtnl_position_units{position="P1"} 1000
xtnl_node_role{role="STANDBY"} 0
xtnl_node_role{role="LEADER"} 1
xtnl_safe_hold 0
xtnl_engine_epoch 42
xtnl_adapter_state{adapter="stonex",state="STREAMING"} 1
xtnl_adapter_state{adapter="stonex",state="BACKOFF"} 0
xtnl_adapter_state{adapter="ctrader",state="SNAPSHOTTING"} 1
xtnl_risksweep_oldest_pass_age_seconds 3
python_gc_objects_collected_total{generation="0"} 123
process_resident_memory_bytes 1.2e8
"""


def standby_body(role: str = "STANDBY") -> str:
    return (f'xtnl_exporter_up 1\nxtnl_node_role{{role="{role}"}} 1\nxtnl_safe_hold 1\nxtnl_engine_epoch 41\n'
            'xtnl_adapter_state{adapter="stonex",state="STREAMING"} 1\n'
            'xtnl_risksweep_oldest_pass_age_seconds 99\n')


def test_the_kept_names_are_the_six_of_the_design_plus_the_deadman_lines() -> None:
    assert KEPT == {"xtnl_exporter_up", "xtnl_node_role", "xtnl_safe_hold", "xtnl_engine_epoch",
                    "xtnl_adapter_state", "xtnl_risksweep_oldest_pass_age_seconds",
                    # home-server plan W1: a flag, a count and an age (no amount)
                    "xtnl_deadman_configured", "xtnl_deadman_consecutive_failures",
                    "xtnl_deadman_last_ok_age_seconds"}
    assert MAX_BYTES == 1 << 20 and P.TIMEOUT_S == 2.0 and P.FAILS_FOR_DOWN == 3


def test_only_the_allow_listed_metrics_are_kept() -> None:
    samples = parse_metrics(LEADER_BODY)
    assert set(samples) <= KEPT
    assert "xtnl_engine_uptime_seconds" not in samples
    flat = repr(samples)
    for leak in ("104523", "100000", "AUD", "ACC1", "EURUSD", "0.8", "-12.5", "1000", "86400", "1.2e8"):
        assert leak not in flat, leak
    assert samples["xtnl_adapter_state"] == [({"adapter": "stonex", "state": "STREAMING"}, 1.0),
                                             ({"adapter": "stonex", "state": "BACKOFF"}, 0.0),
                                             ({"adapter": "ctrader", "state": "SNAPSHOTTING"}, 1.0)]


def test_other_lines_are_dropped_unread(monkeypatch: pytest.MonkeyPatch) -> None:
    """The labels of a line not on the allow-list are never parsed."""
    seen: list[str] = []

    class Spy:
        def findall(self, text: str) -> list[tuple[str, str]]:
            seen.append(text)
            return real.findall(text)

    real = P.LABEL_RE
    monkeypatch.setattr(P, "LABEL_RE", Spy())
    parse_metrics(LEADER_BODY)
    assert seen and all("account" not in s and "symbol" not in s and "position" not in s for s in seen)
    assert len(seen) == sum(1 for ln in LEADER_BODY.splitlines()
                            if re.match(r"^(xtnl_[a-z_]+)", ln) and ln.split("{")[0].split(" ")[0] in KEPT)


def test_malformed_lines_are_skipped() -> None:
    body = ('xtnl_safe_hold one\nxtnl_engine_epoch{x="1"}  7\nxtnl_node_role{role="LEADER"} 1 123456\n'
            'XTNL_SAFE_HOLD 1\n xtnl_engine_epoch 9\nxtnl_engine_epoch 8\n')
    s = parse_metrics(body)
    assert s == {"xtnl_engine_epoch": [({}, 8.0)]}


def test_a_leader_answer_records_role_adapters_and_sweep_age() -> None:
    ans = interpret(parse_metrics(LEADER_BODY), 1000, 1)
    assert ans.ok and ans.role == "LEADER" and ans.leader_answer is True
    assert ans.safe_hold is False and ans.epoch == 42
    assert ans.adapters == {"stonex": "STREAMING", "ctrader": "SNAPSHOTTING"}
    assert ans.sweep_age_s == 3
    assert ans.last_ok_ms == 1000 and not ans.down


@pytest.mark.parametrize("role", ["STANDBY", "SAFE_HOLD", "LEADER_INIT", "CANDIDATE", "DRAINING"])
def test_adapters_and_sweep_age_only_from_a_leader_answer(role: str) -> None:
    ans = interpret(parse_metrics(standby_body(role)), 2000, 2)
    assert ans.role == role and ans.leader_answer is False
    assert ans.adapters == {} and ans.sweep_age_s is None
    assert ans.safe_hold is True and ans.epoch == 41


def test_an_unknown_role_or_no_role_line_is_not_a_leader() -> None:
    ans = interpret(parse_metrics('xtnl_node_role{role="KING"} 1\nxtnl_adapter_state{adapter="a",state="S"} 1\n'),
                    1, 1)
    assert ans.role is None and not ans.leader_answer and ans.adapters == {}
    ans = interpret(parse_metrics("xtnl_exporter_up 1\n"), 1, 1)
    assert ans.ok and ans.role is None and not ans.leader_answer


def test_nan_and_negative_ages_are_ignored() -> None:
    body = 'xtnl_node_role{role="LEADER"} 1\nxtnl_risksweep_oldest_pass_age_seconds NaN\nxtnl_engine_epoch -1\n'
    ans = interpret(parse_metrics(body), 1, 1)
    assert ans.sweep_age_s is None and ans.epoch is None


# ---- failures and down ---------------------------------------------------------------------------------------------
def scripted(outcomes: list[Any]) -> Callable[[str, int], Awaitable[str]]:
    it = iter(outcomes)

    async def get(_h: str, _p: int) -> str:
        o = next(it)
        if isinstance(o, BaseException):
            raise o
        return str(o)
    return get


async def test_three_failures_make_it_down_with_the_last_success() -> None:
    p = EngineProbe(("127.0.0.1", 9464), getter=scripted([LEADER_BODY, ConnectionRefusedError(),
                                                          TimeoutError(), ConnectionError("x"), LEADER_BODY]))
    a = await p.poll(1000, 1)
    assert a.ok and a.leader_answer and not a.down
    b = await p.poll(2000, 2)
    assert not b.ok and not b.down and b.last_ok_ms == 1000 and not b.leader_answer and b.adapters == {}
    c = await p.poll(3000, 3)
    assert not c.down and p.fails == 2
    d = await p.poll(4000, 4)
    assert d.down and d.last_ok_ms == 1000 and p.fails == 3
    e = await p.poll(5000, 5)
    assert e.ok and not e.down and e.last_ok_ms == 5000 and p.fails == 0


async def test_a_value_error_counts_as_a_failure() -> None:
    p = EngineProbe(("127.0.0.1", 9464), getter=scripted([ValueError("bad"), OSError("x"), OSError("y")]))
    for i in range(3):
        a = await p.poll(i, i)
    assert a.down and a.last_ok_ms is None


async def test_probe_off() -> None:
    p = EngineProbe(None)
    a = await p.poll(1, 1)
    assert p.off and a.off and not a.ok and not a.down


# ---- the HTTP client against a real loopback server ----------------------------------------------------------------
Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


@pytest.fixture
async def serve() -> AsyncIterator[Callable[[Handler], Awaitable[int]]]:
    servers: list[asyncio.Server] = []

    async def start(handler: Handler) -> int:
        srv = await asyncio.start_server(handler, "127.0.0.1", 0)
        servers.append(srv)
        return int(srv.sockets[0].getsockname()[1])

    yield start
    for s in servers:
        s.close()
        try:
            await asyncio.wait_for(s.wait_closed(), 2)
        except (TimeoutError, asyncio.TimeoutError):
            pass


async def _read_request(reader: asyncio.StreamReader) -> bytes:
    return await reader.readuntil(b"\r\n\r\n")


async def test_http_get_reads_a_200_body(serve: Callable[[Handler], Awaitable[int]]) -> None:
    requests: list[bytes] = []

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        requests.append(await _read_request(reader))
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n" + LEADER_BODY.encode())
        await writer.drain()
        writer.close()

    port = await serve(handler)
    text = await http_get("127.0.0.1", port)
    assert text == LEADER_BODY
    assert requests[0].startswith(b"GET /metrics HTTP/1.0\r\n")
    assert f"Host: 127.0.0.1:{port}".encode() in requests[0]
    ans = await EngineProbe(("127.0.0.1", port)).poll(1, 1)
    assert ans.ok and ans.leader_answer


@pytest.mark.parametrize("status", [b"HTTP/1.1 500 Internal Server Error", b"HTTP/1.1 302 Found",
                                    b"HTTP/1.1 404 Not Found", b"garbage", b"HTTP/1.1 2000 OK"])
async def test_a_non_200_answer_is_a_failure(serve: Callable[[Handler], Awaitable[int]], status: bytes) -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await _read_request(reader)
        writer.write(status + b"\r\nLocation: http://127.0.0.1:1/metrics\r\n\r\n" + LEADER_BODY.encode())
        await writer.drain()
        writer.close()

    port = await serve(handler)
    with pytest.raises(ConnectionError):
        await http_get("127.0.0.1", port)
    p = EngineProbe(("127.0.0.1", port))
    a = await p.poll(1, 1)
    assert not a.ok and p.fails == 1 and a.role is None


async def test_the_read_is_cut_at_1_mib(serve: Callable[[Handler], Awaitable[int]]) -> None:
    sent = 0

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal sent
        await _read_request(reader)
        writer.write(b"HTTP/1.1 200 OK\r\n\r\n")
        line = b"xtnl_exporter_up 1\n" * 3000
        try:
            while sent < 64 * MAX_BYTES:            # far more than the cap; stops when the client hangs up
                writer.write(line)
                sent += len(line)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()

    port = await serve(handler)
    started = time.monotonic()
    text = await asyncio.wait_for(http_get("127.0.0.1", port), 10)
    assert time.monotonic() - started < 5
    assert len(text.encode()) <= MAX_BYTES
    assert len(text) > MAX_BYTES // 2
    await asyncio.sleep(0.2)
    assert sent < 16 * MAX_BYTES                    # the client stopped reading and closed


async def test_a_server_that_never_answers_is_a_failure_within_2_5_s(
        serve: Callable[[Handler], Awaitable[int]]) -> None:
    hold = asyncio.Event()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await _read_request(reader)
        try:
            await asyncio.wait_for(hold.wait(), 10)
        except (TimeoutError, asyncio.TimeoutError):
            pass
        writer.close()

    port = await serve(handler)
    p = EngineProbe(("127.0.0.1", port))
    started = time.monotonic()
    a = await p.poll(1, 1)
    elapsed = time.monotonic() - started
    hold.set()
    assert not a.ok and p.fails == 1
    assert 1.5 <= elapsed <= 2.8, elapsed


async def test_a_refused_connection_is_a_failure() -> None:
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    p = EngineProbe(("127.0.0.1", port))
    a = await p.poll(1, 1)
    assert not a.ok and p.fails == 1


def test_the_deadman_lines_are_read_only_from_a_leader_answer() -> None:
    """Home-server plan W1: the receiver acknowledgement flag, failure count and last-ok age (no amount)."""
    lines = ('xtnl_deadman_configured{node="node-a"} 1\nxtnl_deadman_consecutive_failures{node="node-a"} 7\n'
             'xtnl_deadman_last_ok_age_seconds{node="node-a"} 105.5\n')
    ans = interpret(parse_metrics('xtnl_node_role{role="LEADER"} 1\n' + lines), 1, 1)
    assert ans.deadman_configured is True and ans.deadman_failures == 7 and ans.deadman_ok_age_s == 105
    ans = interpret(parse_metrics('xtnl_node_role{role="STANDBY"} 1\n' + lines), 1, 1)
    assert ans.deadman_configured is None and ans.deadman_failures is None and ans.deadman_ok_age_s is None
    # before the term's first acknowledged beat the age line is absent; NaN / negative / non-0/1 flags are ignored
    ans = interpret(parse_metrics('xtnl_node_role{role="LEADER"} 1\nxtnl_deadman_configured 2\n'
                                  'xtnl_deadman_consecutive_failures -1\nxtnl_deadman_last_ok_age_seconds NaN\n'),
                    1, 1)
    assert ans.deadman_configured is None and ans.deadman_failures is None and ans.deadman_ok_age_s is None
    ans = interpret(parse_metrics('xtnl_node_role{role="LEADER"} 1\nxtnl_deadman_configured 0\n'), 1, 1)
    assert ans.deadman_configured is False and ans.deadman_ok_age_s is None
