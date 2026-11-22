"""HTTP: the page, the Edge shell, the snapshot, the SSE stream and the story drawer (docs/design/live-view.md s4.8).

GET only (HEAD on non-stream routes); every other method gets 405. Request checks, in order: Host (DNS-rebinding
defence: the bound literal or localhost, with the port), cross-site (`Sec-Fetch-Site` / `Origin`, before a stream
slot is counted), method. Every response, errors included, carries the CSP and the other security headers. Static
files are bytes loaded at start from a fixed allow-list; no path from a URL reaches the filesystem, and no value
from a URL reaches SQL (the story drawer is a dictionary lookup).
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Final

from aiohttp import web

from src.liveview.events import STATS, Event, PrivacyError, assert_safe, guard_payload
from src.liveview.stories import DayModel, Series, StoryIndex

log = logging.getLogger("xtnl.liveview.http")

STATIC_DIR: Final = Path(__file__).parent / "static"
STATIC_FILES: Final = {
    "index.html": "text/html; charset=utf-8",
    "edge.html": "text/html; charset=utf-8",
    "app.js": "text/javascript; charset=utf-8",
    "edge.js": "text/javascript; charset=utf-8",
    "app.css": "text/css; charset=utf-8",
}
CSP: Final = ("default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; "
              "base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
SECURITY_HEADERS: Final = {
    "Content-Security-Policy": CSP,
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-store",
    "Cross-Origin-Resource-Policy": "same-origin",
    "Cross-Origin-Opener-Policy": "same-origin",
}
STORY_KEY_PARAM: Final = re.compile(r"^(pos|ord|acct|halt|term|out|dlq):[A-Za-z0-9:._@/-]{1,120}$")
LAST_ID_RE: Final = re.compile(r"^([0-9a-f-]{36})\.(\d{1,18})$")
UUID_RE: Final = re.compile(r"^[0-9a-f-]{36}$")
AFTER_RE: Final = re.compile(r"^\d{1,18}$")
CLIENT_QUEUE: Final = 2000
PING_S: Final = 15.0
WRITE_TIMEOUT_S: Final = 5.0
_CLOSE: Final = object()


def load_static(directory: Path = STATIC_DIR) -> dict[str, bytes]:
    return {name: (directory / name).read_bytes() for name in STATIC_FILES}


def _sse(event: str, data: Any, sid: str | None = None) -> bytes:
    lines = [f"event: {event}"]
    if sid:
        lines.append(f"id: {sid}")
    lines.append("data: " + json.dumps(data, separators=(",", ":"), ensure_ascii=False))
    return ("\n".join(lines) + "\n\n").encode("utf-8")


class Hub:
    """The ring of numbered entries (activity lines and story merges), the current state / day / series / poll
    payloads, and the connected stream clients."""

    def __init__(self, *, history: int, index: StoryIndex, day: DayModel, series: Series) -> None:
        self.instance = str(uuid.uuid4())
        self.n = 0
        self.ring: deque[tuple[int, str, dict[str, Any], str]] = deque(maxlen=history)   # (n, type, data, sid)
        self.index = index
        self.day = day
        self.series = series
        self.state: dict[str, Any] | None = None
        self.poll: dict[str, Any] = {"at_ms": None, "db": "down", "engine": "off", "backlog": 0}
        self.clients: set[asyncio.Queue[Any]] = set()
        self.stream_count = 0
        self.history = history
        self.dropped = 0

    # ---- ring ---------------------------------------------------------------------------------------------------
    def emit(self, ev: Event) -> Event:
        assert_safe(ev)
        self.n += 1
        ev.n, ev.sid = self.n, f"{self.instance}.{self.n}"
        data = ev.to_json()
        self.ring.append((ev.n, "activity", data, ev.sid))
        self.index.add_event(ev, count_as="info" if self._held(ev.story) else None)
        self.day.count(ev)
        self.broadcast(_sse("activity", data, ev.sid))
        return ev

    def _held(self, key: str | None) -> bool:
        st = self.index.get(key) if key else None
        return bool(st and st.info_only)

    def merge(self, src: str, into: str, held: bool) -> None:
        if not self.index.merge(src, into, held=held):
            return
        self.n += 1
        sid = f"{self.instance}.{self.n}"
        data = {"n": self.n, "from": src, "into": into, "held": held}
        self.ring.append((self.n, "story_merge", data, sid))
        self.broadcast(_sse("story_merge", data, sid))

    def broadcast(self, frame: bytes | object) -> None:
        for q in list(self.clients):
            try:
                q.put_nowait(frame)
            except asyncio.QueueFull:
                while not q.empty():
                    q.get_nowait()
                q.put_nowait(_sse("resync", {}))
                q.put_nowait(_CLOSE)

    def close_all(self) -> None:
        for q in list(self.clients):
            try:
                q.put_nowait(_CLOSE)
            except asyncio.QueueFull:
                pass

    # ---- payloads -----------------------------------------------------------------------------------------------
    def set_state(self, state: dict[str, Any]) -> None:
        try:
            guard_payload(state)
        except PrivacyError as exc:
            STATS.dropped[f"state:{str(exc)[:40]}"] += 1
            return
        if state != self.state:
            self.state = state
            self.broadcast(_sse("state", state))

    def set_poll(self, poll: dict[str, Any]) -> None:
        if poll != self.poll:
            self.poll = poll
            self.broadcast(_sse("poll", poll))

    def push_stories(self) -> None:
        for st in self.index.take_touched():
            data = self.safe_story(st)
            if data is not None:
                self.broadcast(_sse("story", data))

    def safe_story(self, st: Any) -> dict[str, Any] | None:
        try:
            data: dict[str, Any] = guard_payload(st.to_json())
            return data
        except PrivacyError as exc:
            STATS.dropped[f"story:{str(exc)[:40]}"] += 1
            return None

    def push_day(self, now_ms: int) -> None:
        if self.day.dirty:
            self.day.dirty = False
            self.broadcast(_sse("day", guard_payload(self.day.to_json(now_ms))))

    def push_series(self) -> None:
        self.broadcast(_sse("series", guard_payload(self.series.to_json())))

    def snapshot(self, now_ms: int) -> dict[str, Any]:
        stories = [d for d in (self.safe_story(s) for s in self.index.headers_today(self.day.start_ms)) if d]
        events = [data for (_n, kind, data, _sid) in self.ring if kind == "activity"]
        merges = [data for (_n, kind, data, _sid) in self.ring if kind == "story_merge"]
        last = self.ring[-1][3] if self.ring else None
        return {"instance": self.instance, "last_sid": last, "last_n": self.ring[-1][0] if self.ring else 0,
                "state": self.state, "events": events, "merges": merges, "stories": stories,
                "day": guard_payload(self.day.to_json(now_ms)), "series": guard_payload(self.series.to_json()),
                "poll": dict(self.poll)}

    def replay_after(self, after: int) -> list[tuple[int, str, dict[str, Any], str]] | None:
        """Ring entries newer than `after`, or None when `after` is older than the ring (resync)."""
        if after > self.n:
            return None
        if self.ring and after < self.ring[0][0] - 1:
            return None
        if not self.ring and after != self.n:
            return None
        return [e for e in self.ring if e[0] > after]


# ---- request checks ----------------------------------------------------------------------------------------------
def allowed_hosts(host: str, port: int) -> frozenset[str]:
    bound = f"[{host}]" if ":" in host else host
    out = {f"{bound}:{port}", f"localhost:{port}"}
    if port == 80:
        out |= {bound, "localhost"}
    return frozenset(h.lower() for h in out)


def build_app(hub: Hub, static: dict[str, bytes], *, host: str, port: int, max_clients: int,
              db_ok: Callable[[], bool], now_ms: Callable[[], int]) -> web.Application:
    hosts = allowed_hosts(host, port)
    origins = frozenset(f"http://{h}" for h in hosts)

    @web.middleware
    async def guard(request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
                    ) -> web.StreamResponse:
        try:
            h = (request.headers.get("Host") or "").lower()
            if h not in hosts:
                return web.Response(status=421, text="misdirected request")
            site = request.headers.get("Sec-Fetch-Site")
            if site is not None and site not in ("same-origin", "none"):
                return web.Response(status=403, text="forbidden")
            origin = request.headers.get("Origin")
            if origin is not None and origin.lower() not in origins:
                return web.Response(status=403, text="forbidden")
            if request.method not in ("GET", "HEAD"):
                return web.Response(status=405, text="method not allowed", headers={"Allow": "GET, HEAD"})
            return await handler(request)
        except web.HTTPException as exc:
            return web.Response(status=exc.status, text=exc.reason or "error")
        except (asyncio.CancelledError, ConnectionResetError):
            raise
        except Exception as exc:
            log.error("request failed", extra={"event": "http_error", "exc_type": type(exc).__name__})
            return web.Response(status=500, text="internal error")

    async def on_prepare(_request: web.Request, response: web.StreamResponse) -> None:
        for k, v in SECURITY_HEADERS.items():
            response.headers[k] = v

    def static_route(name: str) -> Callable[[web.Request], Awaitable[web.Response]]:
        body = static[name]
        ctype = STATIC_FILES[name]

        async def handler(_request: web.Request) -> web.Response:
            return web.Response(body=body, headers={"Content-Type": ctype})
        return handler

    async def snapshot(_request: web.Request) -> web.Response:
        return web.json_response(hub.snapshot(now_ms()), dumps=lambda o: json.dumps(o, ensure_ascii=False,
                                                                                    separators=(",", ":")))

    async def story(request: web.Request) -> web.Response:
        key = request.query.get("key", "")
        if not STORY_KEY_PARAM.fullmatch(key):
            return web.json_response({"error": "bad key"}, status=400)
        real = hub.index.resolve(key)
        st = hub.index.get(real)
        if st is None:
            return web.json_response({"error": "unknown story"}, status=404)
        wanted = set(st.events)
        events = [d for (n, kind, d, _s) in hub.ring if kind == "activity" and n in wanted]
        known = {d["n"] for d in events}
        events += [hub.index.by_sid[f"{hub.instance}.{n}"].to_json() for n in wanted
                   if n not in known and f"{hub.instance}.{n}" in hub.index.by_sid]
        events.sort(key=lambda d: d["n"])
        data = hub.safe_story(st)
        if data is None:
            return web.json_response({"error": "unknown story"}, status=404)
        return web.json_response({"story": data, "events": events},
                                 dumps=lambda o: json.dumps(o, ensure_ascii=False, separators=(",", ":")))

    async def healthz(_request: web.Request) -> web.Response:
        return web.Response(text="ok") if db_ok() else web.Response(status=503, text="db down")

    async def stream(request: web.Request) -> web.StreamResponse:
        if hub.stream_count >= max_clients:
            return web.json_response({"error": "busy"}, status=503)
        hub.stream_count += 1
        log.info("stream opened", extra={"event": "clients", "count": hub.stream_count})
        q: asyncio.Queue[Any] = asyncio.Queue(maxsize=CLIENT_QUEUE)
        resp = web.StreamResponse(status=200, headers={"Content-Type": "text/event-stream; charset=utf-8"})
        try:
            await resp.prepare(request)

            async def write(frame: bytes) -> None:
                await asyncio.wait_for(resp.write(frame), WRITE_TIMEOUT_S)

            await write(_sse("hello", {"instance": hub.instance}))
            last_id = request.headers.get("Last-Event-ID")
            after: int | None = None
            if last_id is not None:
                m = LAST_ID_RE.fullmatch(last_id.strip())
                if m and m.group(1) == hub.instance:
                    after = int(m.group(2))
            else:
                inst, a = request.query.get("instance", ""), request.query.get("after", "")
                if UUID_RE.fullmatch(inst) and inst == hub.instance and AFTER_RE.fullmatch(a):
                    after = int(a)
            replay = hub.replay_after(after) if after is not None else None
            if replay is None:
                await write(_sse("resync", {}))
                return resp
            hub.clients.add(q)             # registered before the replay is written: nothing falls in between
            touched: set[str] = set()
            for n, kind, data, sid in replay:
                await write(_sse(kind, data, sid))
                if kind == "activity" and data.get("story"):
                    touched.add(data["story"])
                elif kind == "story_merge":
                    touched |= {data["from"], data["into"]}
            if hub.state is not None:
                await write(_sse("state", hub.state))
            await write(_sse("day", guard_payload(hub.day.to_json(now_ms()))))
            await write(_sse("series", guard_payload(hub.series.to_json())))
            await write(_sse("poll", hub.poll))
            sent: set[str] = set()
            # A header can change with no event (a badge, an episode's timing, a state change at a tick), so every
            # header of today is sent again after each (re)open, not only those the replay touched.
            for key in [s.key for s in hub.index.headers_today(hub.day.start_ms)] + sorted(touched):
                # the touched story itself (a merge source carries merged_into) and the story it resolves to, once
                for k in (key, hub.index.resolve(key)):
                    if k is None or k in sent:
                        continue
                    sent.add(k)
                    st = hub.index.get(k)
                    header = hub.safe_story(st) if st is not None else None
                    if header is not None:
                        await write(_sse("story", header))
            replayed = replay[-1][0] if replay else (after if after is not None else 0)
            while True:
                try:
                    frame = await asyncio.wait_for(q.get(), PING_S)
                except TimeoutError:
                    await write(b": ping\n\n")
                    continue
                if frame is _CLOSE:
                    break
                if isinstance(frame, bytes):
                    m2 = re.search(rb"\nid: [0-9a-f-]{36}\.(\d+)\n", frame)
                    if m2 and int(m2.group(1)) <= replayed:
                        continue
                    await write(frame)
        except (TimeoutError, ConnectionResetError, asyncio.CancelledError) as exc:
            if request.transport is not None:
                request.transport.abort()
            if isinstance(exc, asyncio.CancelledError):
                raise
        finally:
            hub.clients.discard(q)
            hub.stream_count -= 1
            log.info("stream closed", extra={"event": "clients", "count": hub.stream_count})
        return resp

    app = web.Application(middlewares=[guard])
    app.on_response_prepare.append(on_prepare)
    app.router.add_get("/", static_route("index.html"))
    app.router.add_get("/edge", static_route("edge.html"))
    app.router.add_get("/static/app.js", static_route("app.js"))
    app.router.add_get("/static/edge.js", static_route("edge.js"))
    app.router.add_get("/static/app.css", static_route("app.css"))
    app.router.add_get("/api/snapshot", snapshot)
    app.router.add_get("/api/stream", stream, allow_head=False)
    app.router.add_get("/api/story", story)
    app.router.add_get("/healthz", healthz)
    return app


__all__ = ["CSP", "Hub", "SECURITY_HEADERS", "STATIC_FILES", "allowed_hosts", "build_app", "load_static"]
