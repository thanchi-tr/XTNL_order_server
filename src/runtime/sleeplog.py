"""PC sleep restarts: the node-local record of fence exits caused by a suspend (design §A7 "PC sleep"; plan S38g).

On the single PC a sleep longer than the 12 s fence always ends the term: `time.monotonic()` keeps counting through a
Windows sleep, so on wake the lease keeper finds its last acknowledged keepalive more than 12 s old and hard-exits
(`src/runtime/lease.py`, exit code 75); `deploy/local/run-local.ps1` starts the engine again, and the next
LEADER_INIT closes the old term `lost`. The keeper knows the difference between that and a crash: its own thread
did not run for the length of the gap (it never sleeps more than 0.25 s), and its suspend clock (a clock that counts
the machine's sleep against one that leaves it out: Windows `GetTickCount64` / `QueryUnbiasedInterruptTime`, Linux
`CLOCK_BOOTTIME` / `CLOCK_MONOTONIC`; round 14 review) confirms the machine slept rather than the process stalling, so
just before the fence it calls `on_suspended_fence(slept_s)`, which `build_engine` binds to `SleepFenceLog.record`. A
stall (GIL hog, VM pause, swap thrashing, a wall-clock step) is an ordinary fence and is never recorded here.

`SleepFenceLog` is a small JSON-lines file next to the spill file (`sleep_fences.jsonl`; node-local, never
PostgreSQL): one line per sleep fence `{"at_ms": <wall ms>, "gap_ms": <ms>, "reported": false}`, kept for 24 h and
at most 100 lines. Writes are synchronous and best effort (the keeper calls it right before `os._exit`; a failure is
logged and never blocks the fence). It holds times only.

Its two readers (the ARM, plan S38g):
* the unclean-restart counter (`SleepAwareRestarts`, S38f's `RestartHistoryPort`): the first `lost` / `init_failed`
  term end within `SLEEP_RESTART_MATCH_MS` after a sleep fence is that sleep's restart, not a crash, and does not
  count; every other end still counts (a crash loop with sleeps in between still reaches FAILED);
* the report: `take_unreported()` returns the fences not yet reported and marks them, so the ARM pages each sleep
  restart once ("ARM: engine restarted after a PC sleep").
"""
from __future__ import annotations

import inspect
import json
import logging
import os
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

log = logging.getLogger(__name__)

FILE_NAME: Final = "sleep_fences.jsonl"
KEEP_MS: Final = 24 * 3_600_000
MAX_LINES: Final = 100
SLEEP_RESTART_MATCH_MS: Final = 15 * 60_000
"""A term end this long after a sleep fence at most is that sleep's restart (the restart script and LEADER_INIT)."""


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(frozen=True)
class SleepFence:
    at_ms: int
    gap_ms: int
    reported: bool = False


class SleepFenceLog:
    """The node-local list of sleep fences (see the module docstring)."""

    def __init__(self, path: Path, *, clock_ms: Callable[[], int] = _wall_ms) -> None:
        self.path = path
        self._clock = clock_ms

    def _read(self) -> list[SleepFence]:
        try:
            text = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return []
        except OSError as exc:
            log.warning("sleep fence log not read (%s)", type(exc).__name__)
            return []
        out: list[SleepFence] = []
        for line in text.splitlines():
            try:
                raw = json.loads(line)
                at, gap = raw["at_ms"], raw["gap_ms"]
                if isinstance(at, bool) or isinstance(gap, bool) or not isinstance(at, int) or not isinstance(gap, int):
                    continue
                out.append(SleepFence(at, gap, bool(raw.get("reported", False))))
            except (ValueError, KeyError, TypeError):
                continue                                     # a torn or foreign line is skipped
        return out

    def _write(self, fences: Sequence[SleepFence]) -> None:
        now = self._clock()
        keep = [f for f in fences if now - f.at_ms <= KEEP_MS][-MAX_LINES:]
        body = "".join(json.dumps({"at_ms": f.at_ms, "gap_ms": f.gap_ms, "reported": f.reported},
                                  sort_keys=True) + "\n" for f in keep)
        tmp = self.path.with_name(self.path.name + ".tmp")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(body)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)

    def record(self, gap_s: float) -> None:
        """A sleep fence now (the lease keeper, right before the hard exit). Never raises."""
        try:
            self._write([*self._read(), SleepFence(self._clock(), int(max(gap_s, 0.0) * 1000))])
        except Exception as exc:
            log.warning("sleep fence not recorded (%s)", type(exc).__name__)

    def fences(self) -> list[SleepFence]:
        """Every kept fence, oldest first."""
        return sorted(self._read(), key=lambda f: f.at_ms)

    def take_unreported(self) -> list[SleepFence]:
        """The fences not reported yet, marked reported (a failed write returns none, so nothing is reported twice
        and nothing is lost silently: the next term retries)."""
        fences = self.fences()
        new = [f for f in fences if not f.reported]
        if not new:
            return []
        try:
            self._write([SleepFence(f.at_ms, f.gap_ms, True) for f in fences])
        except Exception as exc:
            log.warning("sleep fences not marked reported (%s)", type(exc).__name__)
            return []
        return new


def without_sleep_restarts(ends_ms: Sequence[int], fences_ms: Sequence[int],
                           match_ms: int = SLEEP_RESTART_MATCH_MS) -> list[int]:
    """The unclean term ends that are not a sleep's restart: for each sleep fence, the first end at or after it and
    within `match_ms` is that sleep's restart and is dropped (once); every other end stays."""
    remaining = sorted(ends_ms)
    for fence in sorted(fences_ms):
        match = next((t for t in remaining if fence <= t <= fence + match_ms), None)
        if match is not None:
            remaining.remove(match)
    return remaining


class SleepAwareRestarts:
    """S38f's `RestartHistoryPort` with the sleep restarts left out (`without_sleep_restarts`). `fences` reads the
    sleep-fence times; an awaitable answer is awaited (the runtime reads the node-local file in a thread, never on the
    event loop)."""

    def __init__(self, inner: object,
                 fences: Callable[[], Sequence[int] | Awaitable[Sequence[int]]]) -> None:
        self._inner = inner
        self._fences = fences

    async def unclean_ends_since(self, since_ms: int) -> list[int]:
        ends: list[int] = await self._inner.unclean_ends_since(since_ms)  # type: ignore[attr-defined]
        try:
            got = self._fences()
            if inspect.isawaitable(got):
                got = await got
            fences = list(got)
        except Exception:
            fences = []                                      # unreadable: every end counts (fail toward FAILED)
        return without_sleep_restarts(ends, fences)


__all__ = ["FILE_NAME", "KEEP_MS", "MAX_LINES", "SLEEP_RESTART_MATCH_MS", "SleepAwareRestarts", "SleepFence",
           "SleepFenceLog", "without_sleep_restarts"]
