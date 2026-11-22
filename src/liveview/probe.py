"""Engine probe: the engine's loopback metrics, allow-listed lines only (docs/design/live-view.md s4.6).

`GET http://<XTNL_LIVEVIEW_ENGINE_METRICS>/metrics` each poll: 2 s timeout, at most 1 MiB read, no redirects. Each
line is matched against a strict pattern and only the KEPT metric names are kept (states, counts and ages, never
money); every other line is dropped unread. After 3 consecutive failures the engine is "not responding since <last
success>".

The six names of the design, plus the three dead-man receiver lines of home-server plan W1
(`xtnl_deadman_configured`, `xtnl_deadman_consecutive_failures`, `xtnl_deadman_last_ok_age_seconds`): a flag, a
count and an age, so they carry no amount either. Like the adapter states they are read only from a LEADER answer
(the leader's term is the only one that beats).
"""
from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Final

LINE_RE: Final = re.compile(r"^(xtnl_[a-z_]+)(\{[^}]*\})? (-?[0-9.eE+]+|NaN)$")
LABEL_RE: Final = re.compile(r'([a-z_]+)="([A-Za-z0-9_:.-]{0,64})"')
KEPT: Final = frozenset({"xtnl_exporter_up", "xtnl_node_role", "xtnl_safe_hold", "xtnl_engine_epoch",
                         "xtnl_adapter_state", "xtnl_risksweep_oldest_pass_age_seconds",
                         "xtnl_deadman_configured", "xtnl_deadman_consecutive_failures",
                         "xtnl_deadman_last_ok_age_seconds"})
MAX_BYTES: Final = 1 << 20
TIMEOUT_S: Final = 2.0
FAILS_FOR_DOWN: Final = 3
ROLES: Final = frozenset({"LEADER", "STANDBY", "SAFE_HOLD", "LEADER_INIT", "CANDIDATE", "DRAINING"})


@dataclass
class ProbeAnswer:
    """One probe outcome. `adapters` (SM4 state per adapter), `sweep_age_s` and the `deadman_*` fields come only from
    a LEADER answer (None: not in the answer; `deadman_ok_age_s` stays None until the term's first acknowledged
    beat)."""

    ok: bool
    at_ms: int
    poll_no: int
    role: str | None = None
    leader_answer: bool = False
    safe_hold: bool | None = None
    epoch: int | None = None
    adapters: dict[str, str] = field(default_factory=dict)
    sweep_age_s: int | None = None
    deadman_configured: bool | None = None
    deadman_failures: int | None = None
    deadman_ok_age_s: int | None = None
    down: bool = False
    last_ok_ms: int | None = None
    off: bool = False


def parse_metrics(text: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    """Kept metric samples only; every other line is skipped without parsing its labels."""
    out: dict[str, list[tuple[dict[str, str], float]]] = {}
    for line in text.splitlines():
        if not line.startswith("xtnl_"):
            continue
        m = LINE_RE.match(line.strip())
        if m is None or m.group(1) not in KEPT:
            continue
        try:
            value = float(m.group(3))
        except ValueError:
            continue
        labels = dict(LABEL_RE.findall(m.group(2) or ""))
        out.setdefault(m.group(1), []).append((labels, value))
    return out


def interpret(samples: dict[str, list[tuple[dict[str, str], float]]], at_ms: int, poll_no: int) -> ProbeAnswer:
    ans = ProbeAnswer(ok=True, at_ms=at_ms, poll_no=poll_no, last_ok_ms=at_ms)
    for labels, v in samples.get("xtnl_node_role", []):
        role = labels.get("role")
        if v == 1 and role in ROLES:
            ans.role = role
    ans.leader_answer = ans.role == "LEADER"
    for _labels, v in samples.get("xtnl_safe_hold", []):
        ans.safe_hold = v == 1
    for _labels, v in samples.get("xtnl_engine_epoch", []):
        if v == v and v >= 0:
            ans.epoch = int(v)
    if ans.leader_answer:
        for labels, v in samples.get("xtnl_adapter_state", []):
            if v == 1 and labels.get("adapter") and labels.get("state"):
                ans.adapters[labels["adapter"]] = labels["state"]
        for _labels, v in samples.get("xtnl_risksweep_oldest_pass_age_seconds", []):
            if v == v and v >= 0:
                ans.sweep_age_s = int(v)
        for _labels, v in samples.get("xtnl_deadman_configured", []):
            if v in (0.0, 1.0):
                ans.deadman_configured = v == 1
        for _labels, v in samples.get("xtnl_deadman_consecutive_failures", []):
            if v == v and v >= 0:
                ans.deadman_failures = int(v)
        for _labels, v in samples.get("xtnl_deadman_last_ok_age_seconds", []):
            if v == v and v >= 0:
                ans.deadman_ok_age_s = int(v)
    return ans


Fetch = Callable[[str, int], Awaitable[str]]


async def http_get(host: str, port: int, path: str = "/metrics") -> str:
    """A minimal HTTP/1.0 GET: no redirects followed, at most MAX_BYTES read."""
    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), TIMEOUT_S)
    try:
        hostport = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
        writer.write(f"GET {path} HTTP/1.0\r\nHost: {hostport}\r\nConnection: close\r\n\r\n".encode("ascii"))
        await writer.drain()
        data = b""
        while len(data) <= MAX_BYTES:
            chunk = await asyncio.wait_for(reader.read(65536), TIMEOUT_S)
            if not chunk:
                break
            data += chunk
        data = data[: MAX_BYTES + 4096]
    finally:
        writer.close()
    head, _, body = data.partition(b"\r\n\r\n")
    status = head.split(b"\r\n", 1)[0]
    if not re.match(rb"^HTTP/1\.[01] 200 ", status):
        raise ConnectionError("metrics answer not 200")
    return body[:MAX_BYTES].decode("utf-8", errors="replace")


class EngineProbe:
    def __init__(self, target: tuple[str, int] | None, *, getter: Callable[[str, int], Awaitable[str]] | None = None
                 ) -> None:
        self.target = target
        self._get = getter or (lambda h, p: http_get(h, p))
        self.fails = 0
        self.last_ok_ms: int | None = None
        self.last: ProbeAnswer | None = None

    @property
    def off(self) -> bool:
        return self.target is None

    async def poll(self, at_ms: int, poll_no: int) -> ProbeAnswer:
        if self.target is None:
            self.last = ProbeAnswer(ok=False, at_ms=at_ms, poll_no=poll_no, off=True)
            return self.last
        try:
            text = await asyncio.wait_for(self._get(*self.target), TIMEOUT_S + 0.5)
            ans = interpret(parse_metrics(text), at_ms, poll_no)
            self.fails = 0
            self.last_ok_ms = at_ms
        except (OSError, TimeoutError, ConnectionError, asyncio.TimeoutError, ValueError):
            self.fails += 1
            ans = ProbeAnswer(ok=False, at_ms=at_ms, poll_no=poll_no, last_ok_ms=self.last_ok_ms,
                              down=self.fails >= FAILS_FOR_DOWN)
        self.last = ans
        return ans


__all__ = ["EngineProbe", "KEPT", "ProbeAnswer", "http_get", "interpret", "parse_metrics"]
