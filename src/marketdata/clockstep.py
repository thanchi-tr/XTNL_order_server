"""Clock-step and receipt-gap detectors (design §A7 "Suspend and clock steps"; plan S14).

Pure: the caller passes every clock reading in. Two detectors, each returning the local-time range whose TICK buckets
become `blind` (the caller applies it: `MdHook.clock_step` / the hook's own receipt-gap handling); consumers then get
`DATA_UNAVAILABLE(clock_step)` for those buckets (S16).

* `WallMonoDetector`: `md-writer` and the BarClock each run their own. Every reading compares the wall-clock delta
  with the monotonic delta; a difference over `STEP_THRESHOLD_MS` (2 s) in either direction is a step. `from_ms` /
  `to_ms` span both the old and the new wall times: `[min(last wall, now), max(last wall + monotonic delta, now)]`.
  The step also carries the last wall reading and the monotonic delta: the stretch that is really at risk is
  `[last wall, last wall + monotonic delta]` in the old wall frame (what `MdHook.clock_step` blinds, with the forming
  buckets); after the step the re-baselined ClockSkew maps the new wall frame.
  **Suspend** (round 14, plan S38g): on Windows `time.monotonic()` keeps counting through a PC sleep, so a sleep
  moves both clocks alike and is no step. With `suspend_after_ms` set (the caller passes a multiple of its own pass
  interval, `SUSPEND_AFTER_MS` = 10 s for the 250 ms BarClock and writer passes), both deltas over it are a suspend:
  the process did not run that long. It is reported as a `ClockStep` of kind `suspend` over `[last wall, now]`, so
  the TICK buckets the sleep covered are blinded and the forming bars reset exactly as for a step.
* `ReceiptGapDetector`: per subscription `(adapter, market)`, no inbound frame for longer than the adapter's
  keepalive interval while SUBSCRIBED. A gap is reported once when found (`[last frame, now]`), extended at every
  later check while the market stays silent (`[last reported end, now]`), and closed by the next frame
  (`[last reported end, frame time]`). An adapter without a stated keepalive interval is not watched (without
  heartbeats a quiet market would read as a gap). **Plan S39b** turns it on: the orchestrator's per-adapter frame
  hook (`on_stream_frame`, every prices-stream heartbeat) feeds `on_frame(adapter, None, ...)` and the runtime sets
  `RECEIPT_GAP_MS` per adapter (`set_keepalive`) together with that feed. As built the gap granularity is **per
  adapter, not per subscription**: that HEARTBEAT is made locally by the channel whenever the client reports
  CONNECTED (not an inbound broker frame), and a market-less frame refreshes every market of the adapter, so the
  detector fires only when the whole adapter stops looking connected; a single market whose subscription went silent
  while the session stays up is not blinded by it (plan S39b deviation 3). After a
  wall-clock step `rebase(delta, now)` moves every watch into the new wall frame, so a backward step never silences
  the watch and a forward one never reads as a gap.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Final

STEP_THRESHOLD_MS: Final = 2_000
RECEIPT_GAP_MS: Final = 20_000
"""No inbound frame (tick or heartbeat) on a SUBSCRIBED market for longer than this is a receipt gap (plan S39b: four
StoneX heartbeats of 5 s, two cTrader heartbeats of 10 s; below the orchestrator's 30 s no-traffic drop)."""
SUSPEND_AFTER_MS: Final = 10_000
"""Both clock deltas above this between two passes of a 250 ms loop: the process was suspended (round 14)."""
STEP: Final = "step"
SUSPEND: Final = "suspend"


@dataclass(frozen=True)
class ClockStep:
    """A wall-versus-monotonic step: the local wall-time range spanning both wall frames, the signed size
    (wall − monotonic), and the last wall reading before it plus the monotonic time since (the stretch at risk is
    `[last_wall_ms, last_wall_ms + mono_delta_ms]` in the old wall frame)."""

    from_ms: int
    to_ms: int
    delta_ms: int
    last_wall_ms: int
    mono_delta_ms: int
    kind: str = STEP
    """`step` (wall versus monotonic) or `suspend` (both clocks moved far beyond the caller's pass interval)."""


class WallMonoDetector:
    def __init__(self, *, threshold_ms: int = STEP_THRESHOLD_MS, suspend_after_ms: int | None = None) -> None:
        self.threshold_ms = threshold_ms
        self.suspend_after_ms = suspend_after_ms
        self._last: tuple[int, int] | None = None
        self.steps = 0
        self.suspends = 0

    def observe(self, wall_ms: int, mono_ms: int) -> ClockStep | None:
        """One reading of both clocks. The first reading only sets the reference."""
        last = self._last
        self._last = (wall_ms, mono_ms)
        if last is None:
            return None
        last_wall, last_mono = last
        d_mono = mono_ms - last_mono
        delta = (wall_ms - last_wall) - d_mono
        if abs(delta) <= self.threshold_ms:
            limit = self.suspend_after_ms
            if limit is not None and min(wall_ms - last_wall, d_mono) > limit:
                self.suspends += 1
                return ClockStep(min(last_wall, wall_ms), max(last_wall + max(d_mono, 0), wall_ms), delta,
                                 last_wall, max(d_mono, 0), SUSPEND)
            return None
        self.steps += 1
        return ClockStep(min(last_wall, wall_ms), max(last_wall + max(d_mono, 0), wall_ms), delta, last_wall,
                         max(d_mono, 0))


@dataclass(frozen=True)
class ReceiptGap:
    """No inbound frame on `(adapter_id, market)` over the local range `[from_ms, to_ms]` while SUBSCRIBED."""

    adapter_id: str
    market: str
    from_ms: int
    to_ms: int
    ended: bool = False


@dataclass
class _Watch:
    last_frame_ms: int
    reported_to_ms: int | None = None


@dataclass
class ReceiptGapDetector:
    keepalive_ms: Mapping[str, int] = field(default_factory=dict)
    _watch: dict[tuple[str, str], _Watch] = field(default_factory=dict)
    gaps: int = 0

    def set_keepalive(self, adapter_id: str, keepalive_ms: int) -> None:
        """Watch the adapter's markets (plan S39b: the runtime turns the detector on once it feeds heartbeats).
        Markets already SUBSCRIBED start being watched at their next `on_subscription`."""
        if keepalive_ms <= 0:
            raise ValueError("the keepalive interval is positive")
        self.keepalive_ms = {**self.keepalive_ms, adapter_id: keepalive_ms}

    def watched(self, adapter_id: str) -> bool:
        return self.keepalive_ms.get(adapter_id, 0) > 0

    def on_subscription(self, adapter_id: str, market: str, subscribed: bool, ts_ms: int) -> ReceiptGap | None:
        """Open (or restart) / close a market's watch. Closing an open gap reports its last part."""
        key = (adapter_id, market)
        old = self._watch.pop(key, None)
        if subscribed and self.watched(adapter_id):
            self._watch[key] = _Watch(ts_ms)
        if old is not None and old.reported_to_ms is not None:
            return ReceiptGap(adapter_id, market, old.reported_to_ms, ts_ms, ended=True)
        return None

    def on_frame(self, adapter_id: str, market: str | None, ts_ms: int) -> tuple[ReceiptGap, ...]:
        """An inbound frame: a market's tick, or (market None) an adapter-wide heartbeat. Ends any open gap."""
        out: list[ReceiptGap] = []
        keys = [(adapter_id, market)] if market is not None else [k for k in self._watch if k[0] == adapter_id]
        for key in keys:
            w = self._watch.get(key)
            if w is None:
                continue
            if w.reported_to_ms is not None:
                out.append(ReceiptGap(key[0], key[1], w.reported_to_ms, ts_ms, ended=True))
                w.reported_to_ms = None
            w.last_frame_ms = max(w.last_frame_ms, ts_ms)
        return tuple(out)

    def check(self, now_ms: int) -> tuple[ReceiptGap, ...]:
        out: list[ReceiptGap] = []
        for (adapter_id, market), w in self._watch.items():
            if w.reported_to_ms is not None:
                if now_ms > w.reported_to_ms:
                    out.append(ReceiptGap(adapter_id, market, w.reported_to_ms, now_ms))
                    w.reported_to_ms = now_ms
                continue
            if now_ms - w.last_frame_ms > self.keepalive_ms.get(adapter_id, 0):
                self.gaps += 1
                out.append(ReceiptGap(adapter_id, market, w.last_frame_ms, now_ms))
                w.reported_to_ms = now_ms
        return tuple(out)

    def rebase(self, delta_ms: int, now_ms: int) -> None:
        """A wall-clock step of `delta_ms` (wall − monotonic): move every watch's times into the new wall frame, never
        past `now_ms`. Without it a backward step leaves `last_frame_ms` in the future and silences the watch for the
        size of the step, and a forward step reads as a gap over the step."""
        for w in self._watch.values():
            w.last_frame_ms = min(w.last_frame_ms + delta_ms, now_ms)
            if w.reported_to_ms is not None:
                w.reported_to_ms = min(w.reported_to_ms + delta_ms, now_ms)

    def in_gap(self, adapter_id: str, market: str) -> bool:
        w = self._watch.get((adapter_id, market))
        return w is not None and w.reported_to_ms is not None
