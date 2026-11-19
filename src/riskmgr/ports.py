"""Non-database ports of the ARM (design §B3, §B14, PR12; plan S19).

The ARM never imports `src.runtime`, `src.notifications` or `src.adapters`: it receives narrow ports, implemented in
`src/runtime/md_ports.py` (S31). The market-data ports are re-exported from `src.marketdata.ports`; the PostgreSQL
store Protocols live in `src/db/arm_ports.py` (S24).

* `Pager`: a `__slots__` wrapper object around two callables: `page` (an operational page) and `log_mirror` (the
  FAILED ping's `log.critical` mirror, never `EngineSession.page`, which would queue a second critical row, §B8). The
  callables sit in private slots, so nothing behind the wrapper is reachable from a scoped module (the lockout
  refuses `_` attributes on anything but `self`).
* `Clock`: wall and monotonic milliseconds (`SystemClock` in production, a fake in tests).
* `PgHealthPort` and `engine_pg_down()`: "engine PostgreSQL down" as §B14 defines it (the term pool's last fenced
  commit older than 30 s, or the RiskSweep's "PostgreSQL unreachable" alert active); the two decision-cap counters
  pause while it holds.
* `ExecutionTimePort` (plan S39b, go-live gate `entry_time_source`): the broker's execution time of a position's
  entry order from its trade history (StoneX `ExecutedDateTimeUtc`, through the orchestrator's read-only
  `fetch_history` at the reconcile REST priority), so an `ESTIMATED` entry time can be upgraded to `BROKER`.
* `FailedSpoolPort`: the CRC-framed local spool of FAILED episodes, appended when the engine may stop before
  PostgreSQL returns and replayed at the next start before any advice (§B14 Record).

Titles and bodies carry codes, ids and counts only, never a price.
"""
from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Final, Protocol

from src.marketdata.ports import (
    AdapterHealthPort,
    BrokerBarsPort,
    CalendarPort,
    HandleRead,
    HandleRef,
    HandleStatus,
    MarketDataPort,
    StampPort,
    StopHandleRead,
    StopHandleReadPort,
    calendar_open,
)
from src.riskmgr.types import ScopeKind

PAGE_SEVERITIES: Final = frozenset({"info", "warning", "critical"})
ENGINE_PG_DOWN_AFTER_MS: Final = 30_000


class Pager:
    """`page(severity, title, body)` and `log_mirror(title, body)`, wrapping the runtime's callables."""

    __slots__ = ("_page", "_mirror")

    def __init__(self, page: Callable[[str, str, str], None], mirror: Callable[[str, str], None]) -> None:
        self._page = page
        self._mirror = mirror

    def page(self, severity: str, title: str, body: str) -> None:
        if severity not in PAGE_SEVERITIES:
            raise ValueError("severity must be info, warning or critical")
        self._page(severity, title, body)

    def log_mirror(self, title: str, body: str) -> None:
        self._mirror(title, body)


class Clock(Protocol):
    def now_ms(self) -> int:
        """Wall-clock milliseconds since the epoch (local clock; bar ends are mapped onto it through ClockSkew)."""
        ...

    def monotonic_ms(self) -> int: ...


class SystemClock:
    __slots__ = ()

    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000

    def monotonic_ms(self) -> int:
        return time.monotonic_ns() // 1_000_000


class PgHealthPort(Protocol):
    def last_fenced_commit_ms(self) -> int | None:
        """The term pool's last successful fenced commit (wall ms), or None when none succeeded this term."""
        ...

    def sweep_pg_unreachable(self) -> bool:
        """The RiskSweep's "PostgreSQL unreachable" alert is active."""
        ...


def engine_pg_down(port: PgHealthPort, now_ms: int, *, after_ms: int = ENGINE_PG_DOWN_AFTER_MS) -> bool:
    """§B14: engine PostgreSQL is down when the last fenced commit is older than 30 s (or none happened) or the
    RiskSweep's unreachable alert is active."""
    if port.sweep_pg_unreachable():
        return True
    last = port.last_fenced_commit_ms()
    return last is None or now_ms - last > after_ms


@dataclass(frozen=True, kw_only=True)
class FailedSpoolRecord:
    """One FAILED episode as spooled before PostgreSQL has it: ids, codes, counts and times only, never a value.

    `ping_id` is the ping the episode is recorded under: its own (`p…` for `f…`) or, once folded, the collapsing
    episode's. A fold after the first append appends the episode again; replay keeps the latest record of a failure
    id, so a restart replays the fold (and the collapsing ping names the folded scopes again)."""

    failure_id: str
    scope_kind: ScopeKind
    scope_id: str
    reason: str
    probe_kind: str
    ping_id: str
    epoch: int
    first_fault_at_ms: int
    entered_at_ms: int
    counters: tuple[tuple[str, int], ...] = ()
    position_ids: tuple[str, ...] = ()
    edge_code: str | None = None
    """The Phase 1 per-edge label of a STRATEGY_VERSION episode (§B11)."""


class FailedSpoolPort(Protocol):
    async def append(self, record: FailedSpoolRecord) -> None:
        """Append one CRC-framed record (durable before it returns)."""
        ...

    async def replay(self) -> Sequence[FailedSpoolRecord]:
        """Every intact record not yet acknowledged, oldest first (a torn or corrupt frame is skipped and paged)."""
        ...

    async def acknowledge(self, failure_ids: Sequence[str]) -> None:
        """Drop records whose `arm_failures` row has committed."""
        ...


@dataclass(frozen=True)
class HistoryFill:
    """What the trade history says of one entry order: its earliest opening fill time (None: not in the window),
    how many distinct fill times it lists and whether the history answer was complete. Times only."""

    fill_ms: int | None
    fills: int = 0
    complete: bool = True


#: The adapters whose trade-history rows carry the entry order's id as the position's `opened_by_client_order_id`
#: (or its position id): StoneX only. On cTrader neither id is the broker order id, so a lookup could never match
#: and would spend whole-window history reads on the shared REST budget (plan S39b review).
HISTORY_EXEC_TIME_ADAPTERS: Final[frozenset[str]] = frozenset({"stonex"})


class ExecutionTimePort(Protocol):
    def supports(self, adapter_id: str) -> bool:
        """Whether a lookup on this adapter can match an entry order at all (no read when False)."""
        ...

    async def execution_time(self, adapter_id: str, order_id: str, since_ms: int, until_ms: int) -> HistoryFill:
        """Read-only (a broker history read at the reconcile priority); raises on a failed read."""
        ...


__all__ = [
    "ExecutionTimePort", "HISTORY_EXEC_TIME_ADAPTERS", "HistoryFill",
    "AdapterHealthPort", "BrokerBarsPort", "CalendarPort", "Clock", "ENGINE_PG_DOWN_AFTER_MS", "FailedSpoolPort",
    "FailedSpoolRecord", "HandleRead", "HandleRef", "HandleStatus", "MarketDataPort", "Pager", "PgHealthPort",
    "StampPort", "StopHandleRead", "StopHandleReadPort", "SystemClock", "calendar_open", "engine_pg_down",
]
