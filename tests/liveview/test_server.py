"""HTTP (docs/design/live-view.md s4.8, s6 "server" incl. "edge page (v9)"; docs/design/live-view-contract.md).

The app from `build_app` runs on a real loopback socket (aiohttp TestServer, with `handler_cancellation=True` as in
`__main__.run`). It is built for port 8090 and every request carries `Host: 127.0.0.1:8090` unless a test says
otherwise, so the DNS-rebinding check sees exactly what a browser would send. No database is involved: the Hub is
fed directly, and the routes that must not read anything are checked to call nothing.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from aiohttp import ClientResponse, ClientTimeout
from aiohttp.test_utils import TestClient, TestServer

from src.liveview.events import Event
from src.liveview.server import (
    CLIENT_QUEUE,
    CSP,
    SECURITY_HEADERS,
    STATIC_FILES,
    Hub,
    _sse,
    allowed_hosts,
    build_app,
    load_static,
)
from src.liveview.stories import DayModel, Series, StoryIndex, local_day_start

ROOT = Path(__file__).resolve().parents[2]
PORT = 8090
GOOD = f"127.0.0.1:{PORT}"
NOW = int(time.time() * 1000)
STATIC = load_static()


class Env:
    def __init__(self, hub: Hub, client: TestClient, flags: dict[str, bool]) -> None:
        self.hub = hub
        self.client = client
        self.flags = flags

    async def get(self, path: str, *, host: str | None = GOOD, method: str = "GET", **headers: str) -> ClientResponse:
        h = dict(headers)
        if host is not None:
            h["Host"] = host
        return await self.client.request(method, path, headers=h, allow_redirects=False)

    async def stream(self, query: str = "", **headers: str) -> ClientResponse:
        return await self.get("/api/stream" + query, **headers)

    def resume_query(self, after: int | None = None) -> str:
        return f"?instance={self.hub.instance}&after={self.hub.n if after is None else after}"


def make_hub(history: int = 200) -> Hub:
    return Hub(history=history, index=StoryIndex(), day=DayModel(local_day_start(NOW)), series=Series())


async def _env(max_clients: int = 4, history: int = 200, port: int = PORT) -> tuple[Env, TestServer]:
    hub = make_hub(history)
    flags = {"db": True}
    app = build_app(hub, STATIC, host="127.0.0.1", port=port, max_clients=max_clients,
                    db_ok=lambda: flags["db"], now_ms=lambda: NOW)
    server = TestServer(app, host="127.0.0.1", handler_cancellation=True)
    client = TestClient(server, timeout=ClientTimeout(total=15))
    await client.start_server()
    return Env(hub, client, flags), server


@pytest.fixture
async def env() -> AsyncIterator[Env]:
    e, _ = await _env()
    try:
        yield e
    finally:
        e.hub.close_all()
        await e.client.close()


@pytest.fixture
async def env1() -> AsyncIterator[Env]:
    """max_clients = 1"""
    e, _ = await _env(max_clients=1)
    try:
        yield e
    finally:
        e.hub.close_all()
        await e.client.close()


def assert_security_headers(resp: ClientResponse) -> None:
    for k, v in SECURITY_HEADERS.items():
        assert resp.headers.get(k) == v, (resp.status, k)
    assert resp.headers["Content-Security-Policy"] == (
        "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; "
        "base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
    assert not [h for h in resp.headers if h.lower().startswith("access-control-")]


# ---- events and SSE frames -------------------------------------------------------------------------------------------
def leader_event(story: str | None = None) -> Event:
    return Event(kind="engine.leader", slots={"node": "pc1", "epoch": 7}, at_ms=NOW, clock="server",
                 detected_ms=NOW, story=story)


def position_event(key: str = "pos:stonex:1") -> Event:
    return Event(kind="position.opened", slots={"symbol": "EURUSD", "side": "BUY", "id": "1", "entry": "1.10203"},
                 at_ms=NOW, clock="broker", detected_ms=NOW, ids={"position_id": "1"}, story=key)


async def read_frame(resp: ClientResponse) -> dict[str, Any] | None:
    """One SSE frame ({"event", "id", "data"}), skipping comments; None at the end of the response."""
    lines: list[str] = []
    while True:
        raw = await resp.content.readline()
        if not raw:
            return None
        line = raw.decode("utf-8").rstrip("\n")
        if line == "":
            if not lines:
                continue
            frame: dict[str, Any] = {"event": None, "id": None, "data": None}
            for ln in lines:
                k, _, v = ln.partition(": ")
                frame[k] = json.loads(v) if k == "data" else v
            return frame
        if line.startswith(":"):
            continue
        lines.append(line)


async def read_until(resp: ClientResponse, event: str, timeout: float = 3.0) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    while True:
        f = await asyncio.wait_for(read_frame(resp), timeout)
        if f is None:
            raise AssertionError(f"stream ended before {event!r}: {[x['event'] for x in out]}")
        out.append(f)
        if f["event"] == event:
            return out


async def read_all(resp: ClientResponse, timeout: float = 3.0) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    while True:
        f = await asyncio.wait_for(read_frame(resp), timeout)
        if f is None:
            return out
        out.append(f)


async def read_quiet(resp: ClientResponse, quiet: float = 0.3) -> list[dict[str, Any]]:
    """Frames until none arrives for `quiet` seconds."""
    out: list[dict[str, Any]] = []
    while True:
        try:
            f = await asyncio.wait_for(read_frame(resp), quiet)
        except (TimeoutError, asyncio.TimeoutError):
            return out
        if f is None:
            return out
        out.append(f)


async def wait_for(cond: Any, timeout: float = 2.0) -> float:
    started = time.monotonic()
    while not cond():
        if time.monotonic() - started > timeout:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)
    return time.monotonic() - started


def state_payload(level: str = "NONE") -> dict[str, Any]:
    return {"halt": {"level": level, "effective_level": level, "hard_latch": False, "at_ms": NOW},
            "engine": {"status": "running", "role": "LEADER"}}


# ---- request checks: Host ----------------------------------------------------------------------------------------------
def test_allowed_hosts() -> None:
    assert allowed_hosts("127.0.0.1", 8090) == {"127.0.0.1:8090", "localhost:8090"}
    assert allowed_hosts("::1", 8090) == {"[::1]:8090", "localhost:8090"}
    assert allowed_hosts("127.0.0.1", 80) == {"127.0.0.1:80", "localhost:80", "127.0.0.1", "localhost"}


@pytest.mark.parametrize("host", ["127.0.0.1:8090", "localhost:8090", "LOCALHOST:8090"])
async def test_good_hosts_are_served(env: Env, host: str) -> None:
    resp = await env.get("/", host=host)
    assert resp.status == 200


@pytest.mark.parametrize("host", ["evil.example", "evil.example:8090", "127.0.0.1", "localhost", "127.0.0.1:8091",
                                  "127.0.0.2:8090", "0.0.0.0:8090", "[::1]:8090", "127.0.0.1:8090.evil.example",
                                  "", "127.0.0.1:80"])
@pytest.mark.parametrize("path", ["/", "/edge", "/api/snapshot", "/api/stream", "/nope"])
async def test_a_bad_host_gets_421_with_the_headers(env: Env, host: str, path: str) -> None:
    resp = await env.get(path, host=host)
    assert resp.status == 421
    assert_security_headers(resp)
    assert env.hub.stream_count == 0


async def _raw(port: int, request: bytes) -> bytes:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(request)
    await writer.drain()
    data = await asyncio.wait_for(reader.read(65536), 3)
    writer.close()
    return data


async def test_a_missing_host_gets_421(env: Env) -> None:
    port = env.client.port
    data = await _raw(port, b"GET / HTTP/1.0\r\n\r\n")
    head = data.split(b"\r\n\r\n", 1)[0].decode()
    assert head.startswith("HTTP/1.0 421") or head.startswith("HTTP/1.1 421")
    assert f"Content-Security-Policy: {CSP}" in head
    data = await _raw(port, b"GET /edge HTTP/1.0\r\n\r\n")
    assert b" 421 " in data.split(b"\r\n", 1)[0]


async def test_port_80_accepts_the_portless_forms() -> None:
    e, _ = await _env(port=80)
    try:
        for host in ("127.0.0.1", "localhost", "127.0.0.1:80", "localhost:80"):
            assert (await e.get("/", host=host)).status == 200, host
        for host in ("127.0.0.1:8090", "evil.example", "127.0.0.1:443"):
            resp = await e.get("/", host=host)
            assert resp.status == 421, host
            assert_security_headers(resp)
    finally:
        await e.client.close()


# ---- request checks: cross-site and method -----------------------------------------------------------------------------
@pytest.mark.parametrize("headers", [{"Origin": "http://evil.example"}, {"Origin": "https://127.0.0.1:8090"},
                                     {"Origin": "http://127.0.0.1:8091"}, {"Origin": "null"},
                                     {"Sec-Fetch-Site": "cross-site"}, {"Sec-Fetch-Site": "same-site"},
                                     {"Sec-Fetch-Site": "same-origin", "Origin": "http://evil.example"}])
async def test_cross_site_gets_403_and_uses_no_stream_slot(env1: Env, headers: dict[str, str]) -> None:
    for path in ("/api/stream" + env1.resume_query(), "/edge", "/", "/api/snapshot"):
        resp = await env1.get(path, **headers)
        assert resp.status == 403, path
        assert_security_headers(resp)
        assert env1.hub.stream_count == 0
    good = await env1.stream(env1.resume_query())      # the one slot is still free
    assert good.status == 200
    assert (await read_frame(good) or {}).get("event") == "hello"
    good.close()


@pytest.mark.parametrize("headers", [{"Origin": "http://127.0.0.1:8090"}, {"Origin": "http://localhost:8090"},
                                     {"Sec-Fetch-Site": "same-origin"}, {"Sec-Fetch-Site": "none"}])
async def test_same_origin_requests_pass(env: Env, headers: dict[str, str]) -> None:
    assert (await env.get("/", **headers)).status == 200


@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
@pytest.mark.parametrize("path", ["/", "/edge", "/api/stream", "/api/snapshot", "/api/story?key=pos:a",
                                  "/healthz", "/static/app.js"])
async def test_other_methods_get_405_with_the_headers(env: Env, method: str, path: str) -> None:
    resp = await env.get(path, method=method)
    assert resp.status == 405
    assert resp.headers.get("Allow") == "GET, HEAD"
    assert_security_headers(resp)
    assert env.hub.stream_count == 0


async def test_head_on_the_stream_gets_405_and_head_elsewhere_works(env: Env) -> None:
    resp = await env.get("/api/stream" + env.resume_query(), method="HEAD")
    assert resp.status == 405
    assert_security_headers(resp)
    assert env.hub.stream_count == 0
    for path in ("/", "/edge", "/static/edge.js"):
        resp = await env.get(path, method="HEAD")
        assert resp.status == 200, path
        assert_security_headers(resp)
        assert await resp.read() == b""


# ---- static files and the edge page ------------------------------------------------------------------------------------
@pytest.mark.parametrize(("path", "name", "ctype"), [
    ("/", "index.html", "text/html; charset=utf-8"),
    ("/edge", "edge.html", "text/html; charset=utf-8"),
    ("/static/app.js", "app.js", "text/javascript; charset=utf-8"),
    ("/static/edge.js", "edge.js", "text/javascript; charset=utf-8"),
    ("/static/app.css", "app.css", "text/css; charset=utf-8"),
])
async def test_static_files_with_exact_content_types(env: Env, path: str, name: str, ctype: str) -> None:
    resp = await env.get(path)
    assert resp.status == 200
    assert resp.headers["Content-Type"] == ctype
    assert STATIC_FILES[name] == ctype
    assert await resp.read() == STATIC[name]
    assert_security_headers(resp)


@pytest.mark.parametrize("path", ["/edge/", "/static/", "/static/../server.py", "/static/index.html",
                                  "/index.html", "/static/app.js/", "/static/settings.py", "/EDGE", "/api/edge",
                                  "/api/sql"])
async def test_unrouted_paths_get_404_with_the_headers(env: Env, path: str) -> None:
    resp = await env.get(path)
    assert resp.status == 404
    assert_security_headers(resp)


async def test_the_edge_page_ignores_a_query_and_reads_nothing(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def no_snapshot(*a: Any, **k: Any) -> Any:
        calls.append("snapshot")
        raise AssertionError("the edge page reads nothing")

    monkeypatch.setattr(env.hub, "snapshot", no_snapshot)
    monkeypatch.setattr(env.hub, "replay_after", no_snapshot)
    for path in ("/edge", "/edge?x=1&instance=abc", "/static/edge.js"):
        resp = await env.get(path)
        assert resp.status == 200
        assert await resp.read() == STATIC["edge.html" if path.startswith("/edge") else "edge.js"]
    assert calls == []


# ---- snapshot, health ---------------------------------------------------------------------------------------------------
async def test_snapshot_shape(env: Env) -> None:
    env.hub.index.open_story("term:5", NOW, NOW)
    env.hub.emit(leader_event("term:5"))
    resp = await env.get("/api/snapshot")
    assert resp.status == 200 and resp.headers["Content-Type"].startswith("application/json")
    assert_security_headers(resp)
    snap = await resp.json()
    assert set(snap) == {"instance", "last_sid", "last_n", "state", "events", "merges", "stories", "day", "series",
                         "poll"}
    assert snap["instance"] == env.hub.instance and snap["last_n"] == 1
    assert snap["last_sid"] == f"{env.hub.instance}.1"
    assert [e["title"] for e in snap["events"]] == ["pc1 is LEADER (epoch 7)"]
    assert [s["key"] for s in snap["stories"]] == ["term:5"]
    assert snap["poll"] == {"at_ms": None, "db": "down", "engine": "off", "backlog": 0}


async def test_healthz_follows_db_ok(env: Env) -> None:
    resp = await env.get("/healthz")
    assert (resp.status, await resp.text()) == (200, "ok")
    env.flags["db"] = False
    resp = await env.get("/healthz")
    assert (resp.status, await resp.text()) == (503, "db down")
    assert_security_headers(resp)


# ---- story drawer -------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("key", ["", "nope", "run:1", "run:abc", "pos:", "pos:a b", "pos:" + "x" * 121, "pos:a;b",
                                 "POS:a", "pos:a'b", "acct", "term:5\n", "pos:%00"])
async def test_story_key_validation_400(env: Env, key: str) -> None:
    from urllib.parse import quote
    resp = await env.get("/api/story?key=" + quote(key, safe=""))
    assert resp.status == 400
    assert await resp.json() == {"error": "bad key"}
    assert_security_headers(resp)


async def test_story_without_a_key_is_400_and_an_unknown_key_404(env: Env) -> None:
    assert (await env.get("/api/story")).status == 400
    resp = await env.get("/api/story?key=pos:stonex:999")
    assert resp.status == 404 and await resp.json() == {"error": "unknown story"}
    assert_security_headers(resp)


@pytest.mark.parametrize("key", ["pos:stonex:1039708845", "out:stonex:prices:1795000000000",
                                 "out:ctrader:orders:r1795000000000", "dlq:17", "term:12", "ord:stonex:77",
                                 "acct:stonex:ACC1", "halt:6"])
async def test_story_returns_the_header_and_its_events(env: Env, key: str) -> None:
    env.hub.index.open_story(key, NOW, NOW)
    ev = env.hub.emit(position_event(key)) if key.startswith(("pos:", "ord:")) else env.hub.emit(leader_event(key))
    env.hub.emit(leader_event(None))                 # not in the story
    resp = await env.get(f"/api/story?key={key}")
    assert resp.status == 200
    body = await resp.json()
    assert body["story"]["key"] == key
    assert [e["sid"] for e in body["events"]] == [ev.sid]


async def test_a_merged_story_key_resolves_to_its_target(env: Env) -> None:
    env.hub.index.open_story("out:stonex:prices:1", NOW, NOW)
    env.hub.index.open_story("term:5", NOW, NOW)
    env.hub.merge("out:stonex:prices:1", "term:5", held=True)
    body = await (await env.get("/api/story?key=out:stonex:prices:1")).json()
    assert body["story"]["key"] == "term:5"


# ---- the stream ---------------------------------------------------------------------------------------------------------
async def test_the_stream_starts_with_hello_and_carries_the_headers(env: Env) -> None:
    resp = await env.stream(env.resume_query())
    assert resp.status == 200
    assert resp.headers["Content-Type"] == "text/event-stream; charset=utf-8"
    assert_security_headers(resp)
    frames = await read_until(resp, "poll")
    assert frames[0] == {"event": "hello", "id": None, "data": {"instance": env.hub.instance}}
    assert [f["event"] for f in frames] == ["hello", "day", "series", "poll"]
    resp.close()


async def test_an_event_between_snapshot_and_stream_is_delivered_exactly_once(env: Env) -> None:
    env.hub.emit(leader_event())
    snap = await (await env.get("/api/snapshot")).json()
    between = env.hub.emit(leader_event())
    resp = await env.stream(f"?instance={snap['instance']}&after={snap['last_n']}")
    first = await read_until(resp, "poll")
    live = env.hub.emit(leader_event())
    rest = await read_until(resp, "activity")
    rest += await read_quiet(resp)
    acts = [f for f in first + rest if f["event"] == "activity"]
    assert [f["id"] for f in acts] == [between.sid, live.sid]
    assert [f["data"]["n"] for f in acts] == [2, 3]
    assert acts[0]["data"]["sid"] == between.sid
    resp.close()


async def test_a_broadcast_during_replay_is_not_repeated(env: Env) -> None:
    """Frames queued for the client while it writes the replay carry ids <= the replayed one and are skipped."""
    for _ in range(5):
        env.hub.emit(leader_event())
    resp = await env.stream(env.resume_query(after=0))
    frames = await read_until(resp, "poll")
    # the 5 replayed ids, then a live frame re-broadcast with an already-replayed id is dropped
    env.hub.broadcast(_sse("activity", {"n": 3}, f"{env.hub.instance}.3"))
    live = env.hub.emit(leader_event())
    frames += await read_until(resp, "activity")
    frames += await read_quiet(resp)
    ids = [f["id"] for f in frames if f["event"] == "activity"]
    assert ids == [f"{env.hub.instance}.{n}" for n in range(1, 6)] + [live.sid]
    resp.close()


async def test_a_merge_between_snapshot_and_stream_is_replayed_and_shown_in_a_later_snapshot(env: Env) -> None:
    env.hub.index.open_story("out:stonex:prices:1795000000000", NOW, NOW)
    env.hub.index.open_story("term:5", NOW, NOW)
    snap = await (await env.get("/api/snapshot")).json()
    env.hub.merge("out:stonex:prices:1795000000000", "term:5", held=True)
    resp = await env.stream(f"?instance={snap['instance']}&after={snap['last_n']}")
    frames = await read_until(resp, "poll")
    frames += await read_quiet(resp)
    merges = [f for f in frames if f["event"] == "story_merge"]
    assert merges == [{"event": "story_merge", "id": f"{env.hub.instance}.1",
                       "data": {"n": 1, "from": "out:stonex:prices:1795000000000", "into": "term:5", "held": True}}]
    stories = {f["data"]["key"]: f["data"] for f in frames if f["event"] == "story"}
    assert set(stories) == {"out:stonex:prices:1795000000000", "term:5"}
    assert len([f for f in frames if f["event"] == "story"]) == 2           # each header once
    assert stories["out:stonex:prices:1795000000000"]["merged_into"] == "term:5"
    resp.close()
    later = await (await env.get("/api/snapshot")).json()
    by_key = {s["key"]: s for s in later["stories"]}
    assert by_key["out:stonex:prices:1795000000000"]["merged_into"] == "term:5"
    assert later["merges"] == [merges[0]["data"]] and later["last_n"] == 1


async def test_a_reconnect_replays_a_merge_by_last_event_id(env: Env) -> None:
    env.hub.index.open_story("out:stonex:orders:1", NOW, NOW)
    env.hub.index.open_story("term:9", NOW, NOW)
    first = env.hub.emit(leader_event("term:9"))
    env.hub.merge("out:stonex:orders:1", "term:9", held=False)
    resp = await env.stream("", **{"Last-Event-ID": first.sid})
    frames = await read_until(resp, "poll")
    assert [f["event"] for f in frames][:2] == ["hello", "story_merge"]
    assert frames[1]["id"] == f"{env.hub.instance}.2"
    resp.close()


async def test_after_hello_and_replay_the_current_state_day_series_and_touched_stories(env: Env) -> None:
    env.hub.set_state(state_payload("NONE"))
    env.hub.index.open_story("pos:stonex:1", NOW, NOW)
    env.hub.index.open_story("term:3", NOW, NOW)
    env.hub.emit(leader_event("term:3"))
    snap = await (await env.get("/api/snapshot")).json()
    ev = env.hub.emit(position_event("pos:stonex:1"))
    env.hub.set_state(state_payload("SOFT"))            # a halt change during the gap
    resp = await env.stream(f"?instance={snap['instance']}&after={snap['last_n']}")
    frames = await read_until(resp, "poll")
    frames += await read_quiet(resp)
    kinds = [f["event"] for f in frames]
    assert kinds[:6] == ["hello", "activity", "state", "day", "series", "poll"]
    assert frames[1]["id"] == ev.sid
    assert frames[2]["data"]["halt"]["level"] == "SOFT"
    # every header of today is resent once after the replay (a header can change with no event), each once
    keys = [f["data"]["key"] for f in frames[6:] if f["event"] == "story"]
    assert sorted(keys) == ["pos:stonex:1", "term:3"]
    assert frames[3]["data"]["start_ms"] == env.hub.day.start_ms
    resp.close()


@pytest.mark.parametrize("last_id", [
    f"{uuid.uuid4()}.1",                                  # another instance
    "abc", "", "1", "not-a-uuid.5", ".5",
    "INSTANCE." + "9" * 4300,                             # a 4300-digit id: just malformed
    "INSTANCE.99999999999999999",                         # newer than the ring
    "INSTANCE.-1", "INSTANCE.1.2", "INSTANCE. 1",
])
async def test_last_event_id_from_another_instance_or_malformed_gets_resync(env: Env, last_id: str) -> None:
    env.hub.emit(leader_event())
    resp = await env.stream(env.resume_query(), **{"Last-Event-ID": last_id.replace("INSTANCE", env.hub.instance)})
    assert resp.status == 200
    frames = await read_all(resp)
    assert [f["event"] for f in frames] == ["hello", "resync"]
    await wait_for(lambda: env.hub.stream_count == 0)


@pytest.mark.parametrize("query", ["", "?instance=&after=", "?instance=INSTANCE", "?after=0",
                                   "?instance=INSTANCE&after=" + "9" * 4300, "?instance=INSTANCE&after=-1",
                                   "?instance=INSTANCE&after=99", f"?instance={uuid.uuid4()}&after=0",
                                   "?instance=INSTANCE&after=1e3"])
async def test_a_bad_or_missing_resume_query_gets_resync(env: Env, query: str) -> None:
    resp = await env.stream(query.replace("INSTANCE", env.hub.instance))
    assert resp.status == 200
    assert [f["event"] for f in await read_all(resp)] == ["hello", "resync"]


async def test_an_id_older_than_the_ring_gets_resync() -> None:
    e, _ = await _env(history=3)
    try:
        for _ in range(10):
            e.hub.emit(leader_event())
        resp = await e.stream(e.resume_query(after=2))
        assert [f["event"] for f in await read_all(resp)] == ["hello", "resync"]
        resp = await e.stream(e.resume_query(after=7))      # exactly the ring's start: replay 8..10
        frames = await read_until(resp, "poll")
        assert [f["data"]["n"] for f in frames if f["event"] == "activity"] == [8, 9, 10]
        resp.close()
    finally:
        e.hub.close_all()
        await e.client.close()


async def test_max_clients_gives_503_busy_with_the_headers(env1: Env) -> None:
    a = await env1.stream(env1.resume_query())
    await read_until(a, "poll")
    assert env1.hub.stream_count == 1
    b = await env1.stream(env1.resume_query())
    assert b.status == 503
    assert await b.json() == {"error": "busy"}
    assert_security_headers(b)
    assert env1.hub.stream_count == 1
    a.close()
    await wait_for(lambda: env1.hub.stream_count == 0)
    c = await env1.stream(env1.resume_query())
    assert c.status == 200
    c.close()


async def test_a_disconnected_client_frees_its_slot_within_2_s(env: Env) -> None:
    resps = []
    for _ in range(3):
        r = await env.stream(env.resume_query())
        await read_until(r, "poll")
        resps.append(r)
    assert env.hub.stream_count == 3 and len(env.hub.clients) == 3
    resps[0].close()
    elapsed = await wait_for(lambda: env.hub.stream_count == 2, timeout=2.0)
    assert elapsed < 2.0 and len(env.hub.clients) == 2
    for r in resps[1:]:
        r.close()
    await wait_for(lambda: env.hub.stream_count == 0, timeout=2.0)
    assert env.hub.clients == set()


async def test_a_slow_client_whose_queue_overflows_gets_resync_and_is_closed(env: Env) -> None:
    resp = await env.stream(env.resume_query())
    await read_until(resp, "poll")
    for i in range(CLIENT_QUEUE + 5):                   # no await: the handler cannot drain in between
        env.hub.broadcast(_sse("poll", {"at_ms": i, "db": "ok", "engine": "ok", "backlog": 0}))
    frames = await read_all(resp, timeout=5)
    assert [f["event"] for f in frames] == ["resync"]
    await wait_for(lambda: env.hub.stream_count == 0)


async def test_close_all_ends_every_stream(env: Env) -> None:
    resps = [await env.stream(env.resume_query()) for _ in range(3)]
    for r in resps:
        await read_until(r, "poll")
    env.hub.close_all()
    for r in resps:
        assert await read_all(r) == []
    await wait_for(lambda: env.hub.stream_count == 0)


# ---- errors and logging -------------------------------------------------------------------------------------------------
SECRET = "postgresql://xtnl_liveview:Sup3rS3cret@127.0.0.1:55433/xtnl"


async def test_a_500_carries_the_headers_and_its_message_never_reaches_the_log(
        env: Env, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    def boom(*_a: Any, **_k: Any) -> Any:
        raise RuntimeError(f"cannot read {SECRET}")

    monkeypatch.setattr(env.hub, "snapshot", boom)
    with caplog.at_level(logging.DEBUG):
        resp = await env.get("/api/snapshot")
        assert resp.status == 500
        assert await resp.text() == "internal error"
        assert_security_headers(resp)
    assert any(getattr(r, "exc_type", None) == "RuntimeError" for r in caplog.records)
    for r in caplog.records:
        assert "Sup3rS3cret" not in r.getMessage()
        assert "Sup3rS3cret" not in repr(vars(r))
        assert r.exc_info is None or r.exc_info[0] is None
    assert "Sup3rS3cret" not in caplog.text


async def test_a_500_in_the_story_route_also_hides_the_message(
        env: Env, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    env.hub.index.open_story("dlq:1", NOW, NOW)

    def boom(*_a: Any, **_k: Any) -> Any:
        raise ValueError(SECRET)

    monkeypatch.setattr(env.hub.index, "resolve", boom)
    with caplog.at_level(logging.DEBUG):
        resp = await env.get("/api/story?key=dlq:1")
    assert resp.status == 500
    assert_security_headers(resp)
    assert "Sup3rS3cret" not in caplog.text


def test_the_runner_has_no_access_log_and_handler_cancellation() -> None:
    src = (ROOT / "src" / "liveview" / "__main__.py").read_text(encoding="utf-8")
    assert "access_log=None" in src
    assert "handler_cancellation=True" in src
    assert "add_static" not in (ROOT / "src" / "liveview" / "server.py").read_text(encoding="utf-8")
    assert "FileResponse" not in (ROOT / "src" / "liveview" / "server.py").read_text(encoding="utf-8")
