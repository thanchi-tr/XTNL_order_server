"""StoryIndex, DayModel and Series (docs/design/live-view.md s4.4.3, s4.7). In memory only.

Grouping is presentation only: a story decides where a line is displayed, never whether it is shown. Every
emitted line is in the ring exactly once; a story keeps the numbers of its last 200 lines.
"""
from __future__ import annotations

import re
import time
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final

from src.liveview.events import SEVERITY_RANK, Event, worse

STORY_KEY_RE: Final = re.compile(r"^(pos|ord|acct|halt|term|out|dlq):[A-Za-z0-9:._@/-]{1,120}$")
KIND_OF_PREFIX: Final = {"pos": "trade", "ord": "trade", "acct": "account", "halt": "halt", "out": "outage",
                         "term": "engine", "dlq": "dlq"}
OPEN_STATES: Final = frozenset({"open", "active", "lost", "starting", "queued", "quarantined"})
HISTORY_POINTS: Final = 100
STORY_EVENTS: Final = 200
DROP_AFTER_MS: Final = 86_400_000
BUCKETS: Final = 96
BUCKET_MS: Final = 900_000


@dataclass
class Episode:
    episode: str
    rule_id: str
    level: str | None = None
    cap: str | None = None
    since_ms: int | None = None
    escalate_min: int | None = None
    acked: bool = False
    repages: int = 0
    last_repage_ms: int | None = None
    resolved: bool = False
    raise_sid: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {"episode": self.episode, "rule_id": self.rule_id, "level": self.level, "cap": self.cap,
                "since_ms": self.since_ms, "escalate_min": self.escalate_min, "acked": self.acked,
                "repages": self.repages, "last_repage_ms": self.last_repage_ms, "resolved": self.resolved}


@dataclass
class Story:
    key: str
    kind: str
    state: str
    first_ms: int
    last_ms: int
    symbol: str | None = None
    side: str | None = None
    ids: dict[str, str] = field(default_factory=dict)
    worst: str = "info"
    counts: dict[str, int] = field(default_factory=dict)
    header: str = ""
    levels: dict[str, str | None] = field(default_factory=lambda: {"entry": None, "stop": None, "take_profit": None})
    history: list[dict[str, Any]] = field(default_factory=list)
    episodes: dict[str, Episode] = field(default_factory=dict)
    badges: dict[str, list[str]] = field(default_factory=dict)
    related: list[str] = field(default_factory=list)
    merged_into: str | None = None
    held: bool = False
    info_only: bool = False          # an outage shown at info (held for catch-up / retry catch-up)
    gap_ms: int | None = None
    dlq_status: str | None = None
    events: deque[int] = field(default_factory=lambda: deque(maxlen=STORY_EVENTS))
    members: set[str] = field(default_factory=set)
    opened_detect_ms: int = 0
    data: dict[str, Any] = field(default_factory=dict)      # kind-specific facts for the header sentence

    @property
    def open(self) -> bool:
        return self.state in OPEN_STATES

    def to_json(self) -> dict[str, Any]:
        return {"key": self.key, "kind": self.kind, "state": self.state, "symbol": self.symbol, "side": self.side,
                "ids": dict(self.ids), "first_ms": self.first_ms, "last_ms": self.last_ms,
                "worst": "info" if self.info_only else (self.worst if self.worst != "debug" else "info"),
                "counts": dict(self.counts), "header": self.header, "levels": dict(self.levels),
                "history": list(self.history), "episodes": [e.to_json() for e in self.episodes.values()],
                "badges": {k: list(v) for k, v in self.badges.items()}, "related": list(self.related),
                "merged_into": self.merged_into, "held": self.held, "gap_ms": self.gap_ms,
                "dlq_status": self.dlq_status, "events": list(self.events)}


class StoryIndex:
    def __init__(self) -> None:
        self.stories: dict[str, Story] = {}
        self.touched: set[str] = set()
        self.by_sid: dict[str, Event] = {}

    def get(self, key: str | None) -> Story | None:
        return self.stories.get(key) if key else None

    def resolve(self, key: str | None) -> str | None:
        """Follow merged_into chains (a chained merge is followed to its end)."""
        seen: set[str] = set()
        while key is not None and key in self.stories and self.stories[key].merged_into and key not in seen:
            seen.add(key)
            key = self.stories[key].merged_into
        return key

    def open_story(self, key: str, at_ms: int, detected_ms: int, *, state: str | None = None) -> Story:
        st = self.stories.get(key)
        if st is None:
            kind = KIND_OF_PREFIX[key.split(":", 1)[0]]
            default = {"trade": "open", "account": "active", "halt": "active", "outage": "lost",
                       "engine": "starting", "dlq": "queued"}[kind]
            st = Story(key=key, kind=kind, state=state or default, first_ms=at_ms, last_ms=at_ms,
                       opened_detect_ms=detected_ms)
            if not STORY_KEY_RE.fullmatch(key):
                return st            # never registered or sent: its lines stand alone (Narrator.emit)
            self.stories[key] = st
            self.touched.add(key)
        return st

    def add_event(self, ev: Event, *, count_as: str | None = None) -> None:
        self.by_sid[ev.sid] = ev
        if len(self.by_sid) > 50_000:
            for k in list(self.by_sid)[:10_000]:
                del self.by_sid[k]
        key = self.resolve(ev.story)
        st = self.stories.get(key) if key else None
        if st is None:
            return
        st.events.append(ev.n)
        t = ev.at_ms if ev.at_ms is not None else ev.detected_ms
        st.first_ms = min(st.first_ms, t)
        st.last_ms = max(st.last_ms, t)
        if not ev.housekeeping:
            st.counts[ev.category or "engine"] = st.counts.get(ev.category or "engine", 0) + 1
            st.worst = worse(st.worst, count_as or ev.severity or "info")
        self.touched.add(st.key)

    def merge(self, src: str, into: str, *, held: bool) -> bool:
        a, b = self.stories.get(src), self.stories.get(into)
        if a is None or b is None or a.merged_into or src == into:
            return False
        a.merged_into = into
        a.held = held
        b.events.extend(a.events)
        b.members |= a.members
        if not held:
            b.worst = worse(b.worst, a.worst)
            for c, n in a.counts.items():
                b.counts[c] = b.counts.get(c, 0) + n
        b.first_ms = min(b.first_ms, a.first_ms)
        b.last_ms = max(b.last_ms, a.last_ms)
        if b.symbol is None:
            b.symbol, b.side = a.symbol, a.side
        for sid, states in a.badges.items():
            b.badges.setdefault(sid, states)
        for ep, episode in a.episodes.items():
            b.episodes.setdefault(ep, episode)
        self.touched |= {src, into}
        return True

    def relate(self, a: str, b: str) -> None:
        sa, sb = self.stories.get(a), self.stories.get(b)
        if sa is not None and b not in sa.related:
            sa.related.append(b)
            self.touched.add(a)
        if sb is not None and a not in sb.related:
            sb.related.append(a)
            self.touched.add(b)

    def set_state(self, key: str, state: str) -> None:
        st = self.stories.get(key)
        if st is not None and st.state != state:
            st.state = state
            self.touched.add(key)

    def add_history(self, key: str, at_ms: int, levels: dict[str, str | None]) -> None:
        st = self.stories.get(key)
        if st is None:
            return
        if st.levels != levels:
            st.levels = dict(levels)
            st.history.append({"at_ms": at_ms, **levels})
            del st.history[:-HISTORY_POINTS]
            self.touched.add(key)

    def badge(self, key: str | None, sid: str, state: str) -> None:
        real = self.resolve(key) if key else None
        st = self.stories.get(real) if real else None
        if st is None:
            return
        cur = st.badges.setdefault(sid, [])
        if state not in cur:
            cur.append(state)
            if state == "delivered" and "queued" in cur:
                cur.remove("queued")
            if state in ("delivered", "undeliverable", "suppressed") and "retrying" in cur:
                cur.remove("retrying")
            self.touched.add(st.key)

    def take_touched(self) -> list[Story]:
        out = [self.stories[k] for k in self.touched if k in self.stories]
        self.touched.clear()
        return out

    def drop_old(self, now_ms: int) -> None:
        for k in [k for k, s in self.stories.items() if not s.open and now_ms - s.last_ms > DROP_AFTER_MS]:
            del self.stories[k]

    def headers_today(self, day_start_ms: int, limit: int = 500) -> list[Story]:
        live = [s for s in self.stories.values() if s.last_ms >= day_start_ms or s.open]
        live.sort(key=lambda s: s.last_ms, reverse=True)
        return live[:limit]


def local_day_start(ms: int) -> int:
    d = datetime.fromtimestamp(ms / 1000)
    return int(datetime(d.year, d.month, d.day).timestamp() * 1000)


@dataclass
class Segment:
    from_ms: int
    to_ms: int | None
    state: str

    def to_json(self, now_ms: int) -> dict[str, Any]:
        return {"from_ms": self.from_ms, "to_ms": self.to_ms, "state": self.state}


class DayModel:
    """The day panel: 96 buckets of counts, the halt band, the engine row and the stream uptime rows (s4.7)."""

    def __init__(self, day_start_ms: int) -> None:
        self.start_ms = day_start_ms
        self.buckets: list[dict[str, Any]] = [self._bucket() for _ in range(BUCKETS)]
        self.halt: list[dict[str, Any]] = []
        self.engine: list[dict[str, Any]] = []
        self.streams: dict[str, dict[str, list[dict[str, Any]]]] = {}
        self.inactive: set[str] = set()
        self.dirty = True

    @staticmethod
    def _bucket() -> dict[str, Any]:
        return {"activity": 0, "notice": 0, "warn": 0, "alert": 0, "housekeeping": 0, "partial": False}

    def count(self, ev: Event) -> None:
        t = ev.detected_ms if ev.late else (ev.at_ms if ev.at_ms is not None else ev.detected_ms)
        i = (t - self.start_ms) // BUCKET_MS
        if not 0 <= i < BUCKETS:
            return
        b = self.buckets[i]
        if ev.housekeeping:
            b["housekeeping"] += 1
        elif ev.severity in ("notice", "warn", "alert"):
            b[ev.severity] += 1
        else:
            b["activity"] += 1
        self.dirty = True

    def mark_partial(self, before_ms: int) -> None:
        for i in range(BUCKETS):
            if self.start_ms + (i + 1) * BUCKET_MS <= before_ms:
                self.buckets[i]["partial"] = True
        self.dirty = True

    def set_halt(self, before: dict[str, Any] | None, rows: Iterable[dict[str, Any]], now_ms: int) -> None:
        segs: list[dict[str, Any]] = []
        level = (before or {}).get("effective_level") or "NONE"
        latch = bool((before or {}).get("hard_latch"))
        start = self.start_ms
        for r in sorted(rows, key=lambda x: x["id"]):
            t = max(int(r["at_ms"]), self.start_ms)
            if t > start:
                segs.append({"from_ms": start, "to_ms": t, "level": level, "hard_latch": latch})
            start, level, latch = t, r.get("effective_level") or "NONE", bool(r.get("hard_latch"))
        segs.append({"from_ms": start, "to_ms": None, "level": level, "hard_latch": latch})
        if segs != self.halt:
            self.halt = segs
            self.dirty = True

    def set_engine(self, runs: Iterable[dict[str, Any]], now_ms: int, probe_ok: bool | None) -> None:
        segs: list[dict[str, Any]] = []
        ordered = sorted((r for r in runs if (r.get("stopped_at_ms") or now_ms) >= self.start_ms),
                         key=lambda r: r["started_at_ms"])
        cursor = self.start_ms
        for i, r in enumerate(ordered):
            s = max(int(r["started_at_ms"]), self.start_ms)
            if s > cursor:
                segs.append({"from_ms": cursor, "to_ms": s, "state": "engine_off"})
            nxt = ordered[i + 1]["started_at_ms"] if i + 1 < len(ordered) else None
            if r.get("stopped_at_ms") is not None:
                segs.append({"from_ms": s, "to_ms": int(r["stopped_at_ms"]), "state": "running"})
                cursor = int(r["stopped_at_ms"])
            elif nxt is not None:      # ended without a clean stop
                last = max(int(r["last_seen_ms"]), s)
                segs.append({"from_ms": s, "to_ms": last, "state": "running"})
                segs.append({"from_ms": last, "to_ms": int(nxt), "state": "unknown"})
                cursor = int(nxt)
            else:                       # the current run
                segs.append({"from_ms": s, "to_ms": None, "state": "running" if probe_ok is not False else "unknown"})
                cursor = now_ms
        if not ordered:
            segs.append({"from_ms": self.start_ms, "to_ms": None, "state": "unknown"})
        if segs != self.engine:
            self.engine = segs
            self.dirty = True

    def stream_event(self, adapter: str, stream: str, t: int, state: str) -> None:
        rows = self.streams.setdefault(adapter, {}).setdefault(stream, [])
        t = max(t, self.start_ms)
        if rows and rows[-1]["to_ms"] is None:
            if rows[-1]["state"] == state:
                return
            rows[-1]["to_ms"] = t
        if not rows and t > self.start_ms:
            rows.append({"from_ms": self.start_ms, "to_ms": t, "state": "not_observed"})
        rows.append({"from_ms": t, "to_ms": None, "state": state})
        self.dirty = True

    def seed_streams(self, rows: Iterable[dict[str, Any]], catchups: Iterable[dict[str, Any]],
                     terms: dict[int, int]) -> None:
        """Start: rule 2 (current row), rule 3 (catch-up gaps) and rule 5 (not_observed) of s4.7."""
        for r in rows:
            adapter, stream = r["adapter_id"], r["stream"]
            segs: list[dict[str, Any]] = []
            gaps = sorted((int(c["gap_start_ms"]), int(c["gap_end_ms"])) for c in catchups
                          if c.get("adapter_id") == adapter and int(c["gap_end_ms"]) >= self.start_ms)
            cursor = self.start_ms
            for a, b in gaps:
                a = max(a, self.start_ms)
                if a > cursor:
                    segs.append({"from_ms": cursor, "to_ms": a, "state": "not_observed"})
                segs.append({"from_ms": a, "to_ms": b, "state": "down"})
                cursor = max(cursor, b)
            if r.get("conn_lost_at") is None:
                up_from = max(int(r.get("reconciled_at") or self.start_ms), cursor)
                if up_from > cursor:
                    segs.append({"from_ms": cursor, "to_ms": up_from, "state": "not_observed"})
                segs.append({"from_ms": up_from, "to_ms": None, "state": "up"})
            else:
                lost = max(int(r["conn_lost_at"]), cursor)
                if lost > cursor:
                    segs.append({"from_ms": cursor, "to_ms": lost, "state": "not_observed"})
                fenced = int(r["conn_lost_at"]) in terms.values()
                segs.append({"from_ms": lost, "to_ms": None, "state": "fenced" if fenced else "down"})
            self.streams.setdefault(adapter, {})[stream] = segs
        self.dirty = True

    def set_inactive(self, adapters: set[str], t: int) -> None:
        for a in adapters - self.inactive:
            for stream in list(self.streams.get(a, {})):
                self.stream_event(a, stream, t, "inactive")
        self.inactive = set(adapters)
        self.dirty = True

    def to_json(self, now_ms: int) -> dict[str, Any]:
        return {"start_ms": self.start_ms, "now_ms": now_ms, "buckets": [dict(b) for b in self.buckets],
                "halt": [dict(h) for h in self.halt], "engine": [dict(e) for e in self.engine],
                "streams": {a: {s: [dict(x) for x in v] for s, v in d.items()} for a, d in self.streams.items()},
                "inactive": sorted(self.inactive)}


SERIES_NAMES: Final = ("sweep_pass_age_s", "alerts_waiting", "dlq_queued", "open_breaches")


class Series:
    """1-minute points (maximum in each minute), last 60, per series (s4.7)."""

    def __init__(self) -> None:
        self.points: dict[str, deque[list[int | None]]] = {n: deque(maxlen=60) for n in SERIES_NAMES}
        self.last_push_minute = 0

    def sample(self, now_ms: int, values: dict[str, int | None]) -> bool:
        minute = now_ms - now_ms % 60_000
        for name in SERIES_NAMES:
            v = values.get(name)
            pts = self.points[name]
            if pts and pts[-1][0] == minute:
                old = pts[-1][1]
                pts[-1][1] = v if old is None else (old if v is None else max(old, v))
            else:
                pts.append([minute, v])
        if minute != self.last_push_minute:
            self.last_push_minute = minute
            return True
        return False

    def to_json(self) -> dict[str, Any]:
        return {n: [list(p) for p in pts] for n, pts in self.points.items()}


def rank(severity: str) -> int:
    return SEVERITY_RANK.get(severity, 0)


def now_ms() -> int:
    return int(time.time() * 1000)


__all__ = ["DayModel", "Episode", "STORY_KEY_RE", "Series", "Story", "StoryIndex", "local_day_start"]
