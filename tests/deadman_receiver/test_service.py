"""Receiver service: HTTP handling, paging transitions, daily test page, independence from the engine stores.

No network: requests are fed through in-memory asyncio streams, and the engine's real `HttpsDeadmanTransport`
posts through an httpx MockTransport whose handler is the receiver's own request path.
"""
from __future__ import annotations

import asyncio
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from src.deadman_receiver.pager import Pager, backoff_ms
from src.deadman_receiver.service import ReceiverService, next_daily_ms
from src.deadman_receiver.settings import ReceiverSettings
from src.notifications.errors import ChannelError
from src.oversee.notify import AlertMessage
from src.runtime.deadman import AdapterBeat, DeadmanPayload, HttpsDeadmanTransport

ROOT = Path(__file__).resolve().parents[2]
TOKEN = "s3cret-token-0123456789abcdef"
T0 = int(datetime(2026, 11, 13, 10, 0, tzinfo=UTC).timestamp() * 1000)
ENV = {"XTNL_RECEIVER_TOKEN": TOKEN, "XTNL_RECEIVER_PUSH_PROVIDER": "ntfy",
       "XTNL_RECEIVER_PUSH_URL": "https://ntfy.example.invalid/xtnl-dm"}


class Clock:
    def __init__(self) -> None:
        self.ms = T0

    def __call__(self) -> int:
        return self.ms


class FakeTransport:
    def __init__(self, backend: str = "fake", fail: int = 0) -> None:
        self.backend = backend
        self.fail = fail
        self.sent: list[AlertMessage] = []

    async def deliver(self, msg: AlertMessage) -> None:
        if self.fail:
            self.fail -= 1
            raise ChannelError("provider rejected (503)")
        self.sent.append(msg)


class FakeWriter:
    def __init__(self) -> None:
        self.data = b""
        self.closed = False

    def write(self, b: bytes) -> None:
        self.data += b

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


def payload(sent_at_ms: int, **kw: Any) -> DeadmanPayload:
    base: dict[str, Any] = dict(
        node="node-a", epoch="7", sent_at_ms=sent_at_ms, gate_state="OPEN", halt_level="NONE",
        adapters={"stonex": AdapterBeat("STREAMING", True, None)}, last_committed_broker_event_age_ms=10,
        oldest_unacked_feed_age_ms=None, oldest_risksweep_pass_age_ms=500, calendar_version=1,
        oldest_undelivered_notification_age_ms=None, sm8_state="READY", sm5_state="NONE", spill_bytes=0,
        degraded=[])
    base.update(kw)
    return DeadmanPayload(**base)


def make(clock: Clock, transport: FakeTransport | None = None, **env: str) -> tuple[ReceiverService, FakeTransport]:
    t = transport or FakeTransport()
    svc = ReceiverService(ReceiverSettings.from_env({**ENV, **env}), Pager([t], clock), clock_ms=clock)
    return svc, t


async def request(svc: ReceiverService, raw: bytes) -> tuple[int, bytes]:
    reader = asyncio.StreamReader()
    reader.feed_data(raw)
    reader.feed_eof()
    w = FakeWriter()
    await svc.handle(reader, w)  # type: ignore[arg-type]
    head, _, body = w.data.partition(b"\r\n\r\n")
    assert w.closed and b"Connection: close" in head
    return int(head.split(b" ", 2)[1]), body


def post(body: bytes, token: str | None = TOKEN, path: str = "/xtnl/heartbeat") -> bytes:
    auth = f"Authorization: Bearer {token}\r\n" if token else ""
    return (f"POST {path} HTTP/1.1\r\nHost: x\r\n{auth}Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n\r\n").encode() + body


# ---------------------------------------------------------------------------------------------- HTTP
async def test_authenticated_beat_is_accepted() -> None:
    clock = Clock()
    svc, _ = make(clock)
    status, _ = await request(svc, post(payload(T0).to_json().encode()))
    assert status == 204 and svc.evaluator.beats == 1


@pytest.mark.parametrize(("raw", "status"), [
    (post(b"{}", token=None), 401),
    (post(b"{}", token="wrong-token-0123456789"), 401),
    (post(b"{}", path="/other"), 404),
    (b"GET /xtnl/heartbeat HTTP/1.1\r\n\r\n", 405),
    (b"GET /healthz HTTP/1.1\r\n\r\n", 200),
    (b"garbage\r\n\r\n", 400),
    (b"POST /xtnl/heartbeat HTTP/1.1\r\nAuthorization: Bearer " + TOKEN.encode() + b"\r\n\r\n", 411),
    (b"POST /xtnl/heartbeat HTTP/1.1\r\nAuthorization: Bearer " + TOKEN.encode()
     + b"\r\nContent-Length: 999999\r\n\r\n", 413),
    (post(b"{\"node\": 1}"), 400),
])
async def test_http_status_codes(raw: bytes, status: int) -> None:
    svc, _ = make(Clock())
    got, body = await request(svc, raw)
    assert got == status
    assert TOKEN.encode() not in body


async def test_unauthorized_post_never_reaches_the_evaluator() -> None:
    svc, _ = make(Clock())
    await request(svc, post(b"not json", token="wrong-token-0123456789"))
    assert svc.evaluator.invalid is None and svc.evaluator.rejected == 0


async def test_engine_sender_posts_what_the_receiver_accepts() -> None:
    """The real engine transport (HTTPS, bearer token, DeadmanPayload JSON) against the receiver's handler."""
    clock = Clock()
    svc, _ = make(clock)

    async def handler(req: httpx.Request) -> httpx.Response:
        raw = (f"{req.method} {req.url.raw_path.decode()} HTTP/1.1\r\n"
               + "".join(f"{k}: {v}\r\n" for k, v in req.headers.items()) + "\r\n").encode() + req.content
        status, body = await request(svc, raw)
        return httpx.Response(status, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = HttpsDeadmanTransport("https://deadman.example.invalid/xtnl/heartbeat", token=TOKEN, client=client)
        await sender.post(payload(T0))
        bad = HttpsDeadmanTransport("https://deadman.example.invalid/xtnl/heartbeat", token="nope-0123456789abcdef",
                                    client=client)
        with pytest.raises(Exception, match="HTTP 401"):
            await bad.post(payload(T0 + 1))
    assert svc.evaluator.beats == 1


# ---------------------------------------------------------------------------------------------- paging
async def test_startup_notice_then_silence_page_repage_and_resolve() -> None:
    clock = Clock()
    svc, t = make(clock, XTNL_RECEIVER_REPAGE_S="300")
    await svc.tick()
    assert [m.subject for m in t.sent] == ["[XTNL DEADMAN] receiver started"]
    clock.ms = T0 + 61_000
    await svc.tick()
    assert t.sent[-1].severity == "critical" and "no heartbeat" in t.sent[-1].subject
    clock.ms += 60_000
    await svc.tick()
    assert len(t.sent) == 2                                    # no re-page before repage_s
    clock.ms += 240_000
    await svc.tick()
    assert t.sent[-1].kind == "repage" and t.sent[-1].subject.startswith("[XTNL DEADMAN] still active")
    assert svc.on_beat(payload(clock.ms).to_json().encode()) == (204, "")
    await svc.tick()
    assert t.sent[-1].severity == "info" and "resolved" in t.sent[-1].subject
    assert "silence" not in svc.active


async def test_each_condition_pages_once_per_activation() -> None:
    clock = Clock()
    svc, t = make(clock)
    for _ in range(3):
        svc.on_beat(payload(clock.ms, degraded=["outbox_relay"]).to_json().encode())
        await svc.tick()
        clock.ms += 15_000
    critical = [m for m in t.sent if m.severity == "critical"]
    assert len(critical) == 1 and "outbox_relay" in critical[0].subject
    assert "never sends orders" in critical[0].body and TOKEN not in critical[0].body


async def test_daily_test_page_at_the_configured_utc_time() -> None:
    clock = Clock()                                            # 10:00 UTC
    svc, t = make(clock, XTNL_RECEIVER_TEST_PAGE_UTC="10:05")
    svc.on_beat(payload(clock.ms).to_json().encode())
    await svc.tick()
    clock.ms = T0 + 5 * 60_000
    svc.on_beat(payload(clock.ms).to_json().encode())
    await svc.tick()
    tests = [m for m in t.sent if "daily test page" in m.subject]
    assert len(tests) == 1 and tests[0].severity == "high"
    clock.ms += 60_000
    svc.on_beat(payload(clock.ms).to_json().encode())
    await svc.tick()
    assert len([m for m in t.sent if "daily test page" in m.subject]) == 1   # next one tomorrow


def test_next_daily_is_strictly_after_now() -> None:
    at = next_daily_ms(T0, (10, 0))
    assert at == T0 + 86_400_000
    assert next_daily_ms(T0, (9, 59)) == T0 + 86_400_000 - 60_000
    assert next_daily_ms(T0, (10, 1)) == T0 + 60_000


async def test_failed_delivery_is_retried_with_backoff() -> None:
    clock = Clock()
    t = FakeTransport(fail=2)
    pager = Pager([t], clock)
    assert await pager.page(AlertMessage("s", "b", "critical", "episode")) is False
    assert pager.pending == 1 and await pager.retry_due() == 0          # back-off not elapsed
    clock.ms += backoff_ms(1)
    assert await pager.retry_due() == 0 and pager.pending == 1          # second failure, re-queued
    clock.ms += backoff_ms(2)
    assert await pager.retry_due() == 1 and pager.pending == 0 and len(t.sent) == 1
    assert [backoff_ms(n) for n in (1, 2, 3, 7, 8, 30)] == [1000, 2000, 4000, 60000, 60000, 60000]


async def test_one_failing_channel_does_not_block_the_other() -> None:
    clock = Clock()
    good, bad = FakeTransport("ses"), FakeTransport("ntfy", fail=99)
    pager = Pager([bad, good], clock, max_attempts=2)
    assert await pager.page(AlertMessage("s", "b", "critical", "episode")) is True
    assert len(good.sent) == 1 and pager.pending == 1
    clock.ms += 60_000
    await pager.retry_due()
    assert pager.pending == 0 and pager.dropped == 1                    # gave up after max_attempts


class HangingTransport(FakeTransport):
    async def deliver(self, msg: AlertMessage) -> None:
        await asyncio.Event().wait()                                       # a provider that never answers


async def test_a_hanging_channel_never_stalls_the_tick() -> None:
    """A channel that never answers is a failed (re-queued) attempt after the delivery timeout: the tick returns,
    the other channel still gets the page, and the next silence check runs on time."""
    clock = Clock()
    good, hung = FakeTransport("ses"), HangingTransport("ntfy")
    svc = ReceiverService(ReceiverSettings.from_env(ENV), Pager([hung, good], clock, delivery_timeout_s=0.05),
                          clock_ms=clock)
    clock.ms = T0 + 61_000
    await asyncio.wait_for(svc.tick(), 2.0)
    assert any("no heartbeat" in m.subject for m in good.sent) and svc._pager.pending == 2   # notice + silence


async def test_conditions_of_one_tick_are_paged_concurrently() -> None:
    """Several conditions changing in one tick cost one delivery round, not one per condition."""
    clock = Clock()

    class Slow(FakeTransport):
        async def deliver(self, msg: AlertMessage) -> None:
            await asyncio.sleep(0.2)
            self.sent.append(msg)

    t = Slow()
    svc = ReceiverService(ReceiverSettings.from_env(ENV), Pager([t], clock), clock_ms=clock)
    svc.on_beat(payload(clock.ms, degraded=["a", "b", "c", "d", "e"]).to_json().encode())
    loop = asyncio.get_running_loop()
    began = loop.time()
    await svc.tick()                                                   # start-up notice + 5 circuit pages
    assert len(t.sent) == 6 and loop.time() - began < 0.9


def test_receiver_never_imports_the_engine_stores() -> None:
    code = ("import sys, src.deadman_receiver.__main__, src.deadman_receiver.service; "
            "bad = sorted(m for m in sys.modules if m.split('.')[0] in ('redis', 'asyncpg') "
            "or m.startswith(('src.db', 'src.cache', 'src.runtime', 'src.pubsub', 'src.adapters'))); "
            "print(','.join(bad))")
    out = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == ""


def test_check_config_exits_78_without_a_channel(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.deadman_receiver.__main__ import main

    for k in list(__import__("os").environ):
        if k.startswith("XTNL_RECEIVER_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("XTNL_RECEIVER_TOKEN", TOKEN)
    assert main(["--check-config"]) == 78
    for k, v in ENV.items():
        monkeypatch.setenv(k, v)
    assert main(["--check-config"]) == 0
