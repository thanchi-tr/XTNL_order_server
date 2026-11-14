"""Heartbeat parsing and the paging rules of the Melbourne dead-man receiver (operations.md §9).

Pure and synchronous: no I/O, no clock of its own (every call takes `now_ms`, the receiver's wall clock).

`parse_beat` validates one POSTed payload against the engine's `src.runtime.deadman.DeadmanPayload` (every key
present, values of the right type; a JSON null is a value, an absent key is a malformed beat).

`Evaluator` keeps the receiver's view and returns the set of **active conditions** at any instant:

| key                              | active when (thresholds from operations.md §9)                                   |
|----------------------------------|----------------------------------------------------------------------------------|
| `silence`                        | no accepted beat for > 60 s (counted from receiver start before the first beat)  |
| `payload_invalid`                | the latest POST was malformed, or its `sent_at_ms` is > 5 min off the receiver   |
| `adapter_not_streaming:<a>`      | SM4 not STREAMING for >= 2 min while `market_open` is true and not in maintenance |
| `adapter_fields_missing:<a>`     | `sm4_state` or `market_open` null in the latest beat (pages at once)              |
| `fields_missing:<field>`         | a health field null (or `adapters` empty) in the latest beat (pages at once)      |
| `feed_lag`                       | oldest un-acked feed entry > 30 s                                                 |
| `risksweep_pass_age`             | any account's RiskSweep pass > 10 s old                                           |
| `notification_undelivered`       | oldest undelivered alert > 60 s                                                   |
| `sm8_not_ready`                  | SM8 not READY for > 60 s                                                          |
| `sm5_unknown`                    | SM5 UNKNOWN for > 60 s                                                            |
| `spill_nonempty`                 | spill file non-empty for > 60 s                                                   |
| `circuit_open:<task>`            | the task is in the `degraded` list (its circuit is open)                          |

"When those fields are missing" (operations.md §9) pages on the first beat that lacks them (IMPLEMENTATION_PLAN
8.2; a key absent from the body is already a malformed beat, `payload_invalid`, at once). The engine starts
beating only after LEADER_INIT step 7, when every health field is known, so a null is a real fault.

Durations ("for > 60 s") run from the first beat that showed the condition, in receiver time, and stop at the
first beat that does not. The receiver pages on each condition's activation, re-pages while it stays active and
sends a resolved notice when it clears (`src/deadman_receiver/service.py`).
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final

STREAMING: Final = "STREAMING"
READY: Final = "READY"
UNKNOWN: Final = "UNKNOWN"

#: Every key of src.runtime.deadman.DeadmanPayload (a test keeps the two equal).
PAYLOAD_FIELDS: Final = (
    "node", "epoch", "sent_at_ms", "gate_state", "halt_level", "adapters", "last_committed_broker_event_age_ms",
    "oldest_unacked_feed_age_ms", "oldest_risksweep_pass_age_ms", "calendar_version",
    "oldest_undelivered_notification_age_ms", "sm8_state", "sm5_state", "spill_bytes", "degraded",
)
ADAPTER_FIELDS: Final = ("sm4_state", "market_open", "maintenance_until_ms")
_STR_OR_NULL: Final = ("epoch", "gate_state", "halt_level", "sm8_state", "sm5_state")
_INT_OR_NULL: Final = ("last_committed_broker_event_age_ms", "oldest_unacked_feed_age_ms",
                       "oldest_risksweep_pass_age_ms", "calendar_version", "oldest_undelivered_notification_age_ms",
                       "spill_bytes")
#: Health fields that must not be null: a null here means the engine cannot report it (paged at once, 8.2).
#: The age fields may legitimately be null (nothing un-acked / undelivered / no account yet) and are not listed.
NON_NULL_FIELDS: Final = ("epoch", "gate_state", "halt_level", "calendar_version", "sm8_state", "sm5_state",
                          "spill_bytes")
MAX_BODY_BYTES: Final = 65_536
MAX_CLOCK_SKEW_MS: Final = 300_000


@dataclass(frozen=True)
class Thresholds:
    """operations.md §9, in milliseconds."""

    silence_ms: int = 60_000
    not_streaming_ms: int = 120_000
    fields_missing_ms: int = 0                 # "when those fields are missing": no grace (8.2)
    feed_age_ms: int = 30_000
    pass_age_ms: int = 10_000
    notification_age_ms: int = 60_000
    sm8_sm5_ms: int = 60_000
    spill_ms: int = 60_000


@dataclass(frozen=True)
class AdapterState:
    sm4_state: str | None
    market_open: bool | None
    maintenance_until_ms: int | None


@dataclass(frozen=True)
class Beat:
    node: str
    epoch: str | None
    sent_at_ms: int
    gate_state: str | None
    halt_level: str | None
    adapters: Mapping[str, AdapterState]
    last_committed_broker_event_age_ms: int | None
    oldest_unacked_feed_age_ms: int | None
    oldest_risksweep_pass_age_ms: int | None
    calendar_version: int | None
    oldest_undelivered_notification_age_ms: int | None
    sm8_state: str | None
    sm5_state: str | None
    spill_bytes: int | None
    degraded: tuple[str, ...]


class BeatError(ValueError):
    """A malformed heartbeat. The message names fields only (never values): safe to log and to return."""


def _is_int(v: object) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _text(v: object, name: str, *, limit: int = 200) -> str:
    if not isinstance(v, str) or not v or len(v) > limit:
        raise BeatError(f"{name} must be a non-empty string")
    return v


def _opt_text(d: Mapping[str, Any], name: str) -> str | None:
    v = d[name]
    return None if v is None else _text(v, name)


def _opt_int(d: Mapping[str, Any], name: str) -> int | None:
    v = d[name]
    if v is None:
        return None
    if not _is_int(v):
        raise BeatError(f"{name} must be an integer or null")
    return int(v)


def _adapter(name: str, v: object) -> AdapterState:
    if not isinstance(v, dict):
        raise BeatError(f"adapters.{name} must be an object")
    missing = [f for f in ADAPTER_FIELDS if f not in v]
    if missing:
        raise BeatError(f"adapters.{name} lacks {', '.join(missing)}")
    sm4 = v["sm4_state"]
    if sm4 is not None and not isinstance(sm4, str):
        raise BeatError(f"adapters.{name}.sm4_state must be a string or null")
    mo = v["market_open"]
    if mo is not None and not isinstance(mo, bool):
        raise BeatError(f"adapters.{name}.market_open must be a boolean or null")
    mu = v["maintenance_until_ms"]
    if mu is not None and not _is_int(mu):
        raise BeatError(f"adapters.{name}.maintenance_until_ms must be an integer or null")
    return AdapterState(sm4, mo, mu)


def parse_beat(body: bytes) -> Beat:
    """Decode and validate one heartbeat body (UTF-8 JSON object). Raises BeatError."""
    if len(body) > MAX_BODY_BYTES:
        raise BeatError("body too large")
    try:
        d = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise BeatError("body is not UTF-8 JSON") from None
    if not isinstance(d, dict):
        raise BeatError("body is not a JSON object")
    missing = [f for f in PAYLOAD_FIELDS if f not in d]
    if missing:
        raise BeatError(f"missing fields: {', '.join(missing)}")
    if not _is_int(d["sent_at_ms"]):
        raise BeatError("sent_at_ms must be an integer")
    adapters = d["adapters"]
    if not isinstance(adapters, dict):
        raise BeatError("adapters must be an object")
    degraded = d["degraded"]
    if not isinstance(degraded, list) or not all(isinstance(t, str) and t for t in degraded):
        raise BeatError("degraded must be a list of task names")
    texts = {n: _opt_text(d, n) for n in _STR_OR_NULL}
    ints = {n: _opt_int(d, n) for n in _INT_OR_NULL}
    return Beat(
        node=_text(d["node"], "node"), epoch=texts["epoch"], sent_at_ms=int(d["sent_at_ms"]),
        gate_state=texts["gate_state"], halt_level=texts["halt_level"],
        adapters={_text(k, "adapter id"): _adapter(k, v) for k, v in adapters.items()},
        last_committed_broker_event_age_ms=ints["last_committed_broker_event_age_ms"],
        oldest_unacked_feed_age_ms=ints["oldest_unacked_feed_age_ms"],
        oldest_risksweep_pass_age_ms=ints["oldest_risksweep_pass_age_ms"],
        calendar_version=ints["calendar_version"],
        oldest_undelivered_notification_age_ms=ints["oldest_undelivered_notification_age_ms"],
        sm8_state=texts["sm8_state"], sm5_state=texts["sm5_state"], spill_bytes=ints["spill_bytes"],
        degraded=tuple(str(t)[:100] for t in degraded),
    )


@dataclass(frozen=True)
class Condition:
    """One active paging condition. `summary` is the page title, `detail` its body line (no secrets)."""

    key: str
    summary: str
    detail: str


@dataclass
class Evaluator:
    """The receiver's state between beats. Not thread-safe: the receiver calls it from its event loop only."""

    started_ms: int
    thresholds: Thresholds = field(default_factory=Thresholds)
    last_rx_ms: int | None = None
    last_beat: Beat | None = None
    invalid: str | None = None
    beats: int = 0
    rejected: int = 0
    _since: dict[str, int] = field(default_factory=dict)

    # ------------------------------------------------------------------ input
    def accept(self, beat: Beat, now_ms: int) -> None:
        """Record a valid beat received at `now_ms` (raises BeatError for a stale / skewed `sent_at_ms`)."""
        if abs(beat.sent_at_ms - now_ms) > MAX_CLOCK_SKEW_MS:
            self.reject(f"sent_at_ms is {abs(beat.sent_at_ms - now_ms) // 1000} s off the receiver clock")
            raise BeatError("sent_at_ms is too far from the receiver clock")
        if self.last_beat is not None and beat.sent_at_ms <= self.last_beat.sent_at_ms:
            self.rejected += 1
            raise BeatError("sent_at_ms is not newer than the last accepted beat")
        self.last_beat = beat
        self.last_rx_ms = now_ms
        self.invalid = None
        self.beats += 1
        held = {key for key, _ in self._durations(beat)}
        for key in list(self._since):
            if key not in held:
                del self._since[key]
        for key in held:
            self._since.setdefault(key, now_ms)

    def reject(self, reason: str) -> None:
        """A malformed POST (the reason names fields only)."""
        self.invalid = reason
        self.rejected += 1

    # ------------------------------------------------------------------ rules
    def _durations(self, b: Beat) -> list[tuple[str, int]]:
        """(key, required duration) of every duration-gated condition the beat shows."""
        t = self.thresholds
        out: list[tuple[str, int]] = []
        for a, s in sorted(b.adapters.items()):
            in_maintenance = s.maintenance_until_ms is not None and s.maintenance_until_ms > b.sent_at_ms
            if s.market_open is True and not in_maintenance and s.sm4_state != STREAMING:
                out.append((f"adapter_not_streaming:{a}", t.not_streaming_ms))
            if s.sm4_state is None or s.market_open is None:
                out.append((f"adapter_fields_missing:{a}", t.fields_missing_ms))
        for name in NON_NULL_FIELDS:
            if getattr(b, name) is None:
                out.append((f"fields_missing:{name}", t.fields_missing_ms))
        if not b.adapters:
            out.append(("fields_missing:adapters", t.fields_missing_ms))
        if b.sm8_state != READY:
            out.append(("sm8_not_ready", t.sm8_sm5_ms))
        if b.sm5_state is None or b.sm5_state == UNKNOWN:
            out.append(("sm5_unknown", t.sm8_sm5_ms))
        if b.spill_bytes is not None and b.spill_bytes > 0:
            out.append(("spill_nonempty", t.spill_ms))
        return out

    def active(self, now_ms: int) -> dict[str, Condition]:
        """The conditions active at `now_ms`."""
        t = self.thresholds
        out: dict[str, Condition] = {}
        ref = self.last_rx_ms if self.last_rx_ms is not None else self.started_ms
        silent_ms = now_ms - ref
        if silent_ms > t.silence_ms:
            what = "no heartbeat since the receiver started" if self.last_rx_ms is None else "no heartbeat"
            out["silence"] = Condition("silence", f"dead-man: {what} for {silent_ms // 1000} s",
                                       "the engine leader POSTs every 15 s; check for a leader (deadman-alert.md)")
        if self.invalid is not None:
            out["payload_invalid"] = Condition("payload_invalid", "dead-man: malformed heartbeat", self.invalid)
        b = self.last_beat
        if b is None:
            return out
        for key, need in self._durations(b):
            since = self._since.get(key)
            if since is not None and now_ms - since >= need:
                out[key] = self._describe(key, b, now_ms - since)
        if b.oldest_unacked_feed_age_ms is not None and b.oldest_unacked_feed_age_ms > t.feed_age_ms:
            out["feed_lag"] = Condition("feed_lag", "dead-man: oversee feed entry un-acked > 30 s",
                                        f"oldest un-acked feed entry {b.oldest_unacked_feed_age_ms // 1000} s")
        if b.oldest_risksweep_pass_age_ms is not None and b.oldest_risksweep_pass_age_ms > t.pass_age_ms:
            out["risksweep_pass_age"] = Condition(
                "risksweep_pass_age", "dead-man: RiskSweep pass older than 10 s",
                f"oldest per-account pass {b.oldest_risksweep_pass_age_ms / 1000:.1f} s old")
        undelivered = b.oldest_undelivered_notification_age_ms
        if undelivered is not None and undelivered > t.notification_age_ms:
            out["notification_undelivered"] = Condition(
                "notification_undelivered", "dead-man: alert undelivered > 60 s",
                f"oldest undelivered notification {undelivered // 1000} s")
        for task in sorted(set(b.degraded)):
            key = f"circuit_open:{task}"
            out[key] = Condition(key, f"dead-man: task circuit open ({task})", f"degraded: {task}")
        return out

    def _describe(self, key: str, b: Beat, held_ms: int) -> Condition:
        held = f"for {held_ms // 1000} s"
        kind, _, subject = key.partition(":")
        if kind == "adapter_not_streaming":
            s = b.adapters[subject]
            return Condition(key, f"dead-man: adapter {subject} not STREAMING while its market is open",
                             f"SM4 {s.sm4_state} {held} (market_open, no maintenance window)")
        if kind == "adapter_fields_missing":
            return Condition(key, f"dead-man: adapter {subject} state missing", f"sm4_state / market_open null {held}")
        if kind == "fields_missing":
            return Condition(key, f"dead-man: heartbeat field {subject} missing", f"{subject} null or empty {held}")
        if kind == "sm8_not_ready":
            return Condition(key, "dead-man: SM8 not READY > 60 s", f"SM8 {b.sm8_state} {held}")
        if kind == "sm5_unknown":
            return Condition(key, "dead-man: SM5 UNKNOWN > 60 s", f"SM5 {b.sm5_state} {held}")
        return Condition(key, "dead-man: spill file non-empty > 60 s", f"{b.spill_bytes} bytes {held}")

    def status_line(self, now_ms: int) -> str:
        """One line for the daily test page and the log (no secrets)."""
        b = self.last_beat
        if b is None or self.last_rx_ms is None:
            return f"no heartbeat yet ({self.rejected} rejected)"
        return (f"last beat {(now_ms - self.last_rx_ms) // 1000} s ago from {b.node} epoch {b.epoch}, "
                f"gate {b.gate_state}, halt {b.halt_level}, {self.beats} accepted, {self.rejected} rejected")
