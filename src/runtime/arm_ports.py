"""The engine-side ports of the ARM runtime join (design §B4, §B5.2, §B6.3, §B7, §B14, PR9; plan S36b, RT-3).

* `PgTrackerSnapshot` (`TrackerSnapshotPort`, also the evaluation's account image): one READ ONLY REPEATABLE READ
  snapshot on the engine pool (`EngineDb.snapshot`, named exception 1: no fence, writes nothing) of every account
  holding an OPEN position plus the accounts asked for (those with live ARM rows). Per account: its OPEN positions
  plus each position with a live `arm_positions` row (a CLOSED one included, so the tracker sees the close), its
  non-terminal orders plus the entry orders of its OPEN positions, the adapter's instruments, and **freshness**
  (§B6.3 pre-guard (1)): no `orders` / `account` fence (`stream_health.conn_lost_at`), no integrity incident and the
  adapter's downtime catch-up verified (reason codes `orders_fence`, `account_fence`, `integrity_incident`,
  `catchup_unverified`).
* `CalendarGrid` (`BarGridPort` and the BarClock's `SessionCloseView`) over the SM10 schedule
  (`MarketCalendar.schedule_of`): a bucket is a bar when any part of it is scheduled OPEN (a bucket wholly inside a
  daily break, the weekly close or a holiday is not a bar); a bar whose end starts a closed period is a pre-close bar
  and its next open is the schedule's next open. A symbol without a schedule has no bar (fail closed: nothing is
  decided until its schedule is loaded). The skew is the tick hook's per-adapter estimate; the bar sides are both
  quote sides for an adapter whose `price_bars_supported` is true, else none (`UNSUPPORTED(no_broker_bars)`).
* `LateNoticeSink`: the second `ARM_SINKS` element, built with the ARM pool (before the notices exist) and bound to
  `ArmNotices.sink()` once the ARM graph is built; until then it passes the decisions through (their `notice_id`
  stays NULL and the notices' timer pass reads them, so nothing is lost).
* `TrackerBook`: the arm-tracker's memory as the BarClock's `StopHandlesPort` (PR9) and the FAILED manager's
  `PositionSeriesPort`.
* `OrchestratorExecutionTimes` (`ExecutionTimePort`, plan S39b, go-live gate `entry_time_source`): the entry order's
  execution time from the broker's trade history (`IngestionOrchestrator.history_execution_time`: a read-only
  `fetch_history` at the reconcile REST priority; StoneX `ExecutedDateTimeUtc`), so the arm-tracker can upgrade an
  `ESTIMATED` entry time to `BROKER`. Times only. StoneX only (`HISTORY_EXEC_TIME_ADAPTERS`): another adapter is
  answered "unsupported" at once, with no history read.
* `stop_read_cost`: PR9's stop-read cost per adapter for the demand book (R11-M20): one list read per anchor (PRE,
  POST, the POST re-read) per adapter with armed handles, the reads of one 60 s read window.

Reads ids, codes, order-state levels and times only; nothing here logs a level or a price.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Final, Protocol

from src.calendar.schedule import SymbolSchedule
from src.calendar.sessions import Phase, next_open_ms, opens_between, phase_at
from src.db.arm_ports import ArmPositionRow, ArmStateTxn
from src.db.epoch import EngineDb
from src.db.pg import PgConnection
from src.db.rows import (
    INSTRUMENT_COLS,
    ORDER_COLS,
    POSITION_COLS,
    instrument_from_row,
    order_from_row,
    position_from_row,
)
from src.dto import InstrumentSpec
from src.marketdata.ports import HandleRef
from src.marketdata.timing import COLD_SKEW, SkewEstimate
from src.marketdata.types import BarSpec
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.notices import NoticeSink
from src.riskmgr.ports import HISTORY_EXEC_TIME_ADAPTERS, HistoryFill
from src.riskmgr.sinks import NOTICE_SINK, Written
from src.riskmgr.tracker import AccountImage, ArmTracker
from src.riskmgr.types import ArmEvent

log = logging.getLogger(__name__)

OPEN_ORDER_SQL: Final = "('PENDING', 'WORKING', 'PARTIALLY_FILLED')"
SQL_ARM_OPEN_ACCOUNTS: Final = (
    "SELECT DISTINCT adapter_id, account_id FROM positions_state WHERE status = 'OPEN' ORDER BY 1, 2"
)
SQL_ARM_ACCOUNT_POSITIONS: Final = (
    f"SELECT {POSITION_COLS} FROM positions_state p WHERE p.adapter_id = $1 AND p.account_id = $2 "
    "AND (p.status = 'OPEN' OR EXISTS (SELECT 1 FROM arm_positions a WHERE a.adapter_id = p.adapter_id "
    "AND a.position_id = p.position_id AND a.state NOT IN ('CLOSED', 'DISARMED'))) ORDER BY p.position_id"
)
SQL_ARM_ACCOUNT_ORDERS: Final = (
    f"SELECT {ORDER_COLS} FROM orders_state o WHERE o.adapter_id = $1 AND o.account_id = $2 "
    f"AND (o.status IN {OPEN_ORDER_SQL} OR o.broker_order_id IN (SELECT q.opened_by_client_order_id "
    "FROM positions_state q WHERE q.adapter_id = $1 AND q.account_id = $2 AND q.status = 'OPEN' "
    "AND q.opened_by_client_order_id IS NOT NULL)) ORDER BY o.broker_order_id"
)
SQL_ARM_INSTRUMENTS: Final = f"SELECT {INSTRUMENT_COLS} FROM instruments WHERE adapter_id = $1 ORDER BY symbol"
SQL_ARM_STREAM_FENCES: Final = (
    "SELECT stream FROM stream_health WHERE adapter_id = $1 AND stream IN ('orders', 'account') "
    "AND conn_lost_at IS NOT NULL ORDER BY stream"
)
FENCED_STREAMS: Final = ("orders", "account")
INTEGRITY_INCIDENT: Final = "integrity_incident"
CATCHUP_UNVERIFIED: Final = "catchup_unverified"
STOP_READS_PER_WINDOW: Final = 3
"""PR9: the list reads of one adapter in one 60 s read window (the PRE and POST anchors and the POST re-read)."""
MAX_GRID_BUCKETS: Final = 50_000
"""Bounds one `open_bar_ends` walk (about 520 days of 15m buckets)."""
BOTH_SIDES: Final = frozenset({QuoteSide.BID, QuoteSide.ASK})


def fence_reason(stream: str) -> str:
    return f"{stream}_fence"


class ExecutionTimeReader(Protocol):
    """The orchestrator's read-only trade-history lookup (`IngestionOrchestrator.history_execution_time`)."""

    async def history_execution_time(self, adapter_id: str, order_id: str, since_ms: int,
                                     until_ms: int) -> ExecutionTimeView: ...


class ExecutionTimeView(Protocol):
    @property
    def fill_times_ms(self) -> tuple[int, ...]: ...

    @property
    def complete(self) -> bool: ...


class OrchestratorExecutionTimes:
    """`ExecutionTimePort` over the orchestrator (module docstring). A failed read raises to the tracker's lookup
    task, which counts it and retries later."""

    def __init__(self, orchestrator: ExecutionTimeReader,
                 adapters: frozenset[str] = HISTORY_EXEC_TIME_ADAPTERS) -> None:
        self._orch = orchestrator
        self._adapters = adapters
        self.reads = 0

    def supports(self, adapter_id: str) -> bool:
        """Only the adapters whose history ids can match an entry order (`HISTORY_EXEC_TIME_ADAPTERS`)."""
        return adapter_id in self._adapters

    async def execution_time(self, adapter_id: str, order_id: str, since_ms: int, until_ms: int) -> HistoryFill:
        if not self.supports(adapter_id):
            return HistoryFill(fill_ms=None, complete=False)        # unsupported: no broker read
        self.reads += 1
        got = await self._orch.history_execution_time(adapter_id, order_id, since_ms, until_ms)
        times = tuple(got.fill_times_ms)
        return HistoryFill(fill_ms=times[0] if times else None, fills=len(times), complete=bool(got.complete))


# ---------------------------------------------------------------------------------------------------- snapshot
class PgTrackerSnapshot:
    """`TrackerSnapshotPort` over the engine pool (see the module docstring)."""

    def __init__(self, db: EngineDb, *, integrity_incident: Callable[[], bool] | None = None,
                 catchup_verified: Callable[[str], bool] | None = None) -> None:
        self._db = db
        self._incident = integrity_incident or (lambda: False)
        self._verified = catchup_verified or (lambda _adapter_id: True)
        self.reads = 0

    async def read(self, accounts: Sequence[tuple[str, str]]) -> Sequence[AccountImage]:
        self.reads += 1
        async with self._db.snapshot() as conn:
            wanted = {(str(r[0]), str(r[1])) for r in await conn.fetch(SQL_ARM_OPEN_ACCOUNTS)}
            wanted.update(accounts)
            instruments: dict[str, tuple[InstrumentSpec, ...]] = {}
            fences: dict[str, tuple[str, ...]] = {}
            out: list[AccountImage] = []
            for adapter_id, account_id in sorted(wanted):
                if adapter_id not in instruments:
                    instruments[adapter_id] = tuple(instrument_from_row(r) for r in
                                                    await conn.fetch(SQL_ARM_INSTRUMENTS, adapter_id))
                    fences[adapter_id] = tuple(str(r[0]) for r in await conn.fetch(SQL_ARM_STREAM_FENCES, adapter_id))
                out.append(await self._account(conn, adapter_id, account_id, instruments[adapter_id],
                                               fences[adapter_id]))
        return out

    async def _account(self, conn: PgConnection, adapter_id: str, account_id: str,
                       instruments: tuple[InstrumentSpec, ...],
                       fenced: tuple[str, ...]) -> AccountImage:
        positions = tuple(position_from_row(r) for r in
                          await conn.fetch(SQL_ARM_ACCOUNT_POSITIONS, adapter_id, account_id))
        orders = tuple(order_from_row(r) for r in await conn.fetch(SQL_ARM_ACCOUNT_ORDERS, adapter_id, account_id))
        reasons = [fence_reason(s) for s in FENCED_STREAMS if s in fenced]
        if self._safe(self._incident):
            reasons.append(INTEGRITY_INCIDENT)
        if not self._safe(lambda: self._verified(adapter_id), failed=False):
            reasons.append(CATCHUP_UNVERIFIED)
        return AccountImage(adapter_id=adapter_id, account_id=account_id, fresh=not reasons, reasons=tuple(reasons),
                            positions=positions, orders=orders,
                            instruments=instruments)

    @staticmethod
    def _safe(check: Callable[[], bool], *, failed: bool = True) -> bool:
        """A freshness input that raises is taken as the unsafe answer (not fresh)."""
        try:
            return bool(check())
        except Exception:
            return failed


# ---------------------------------------------------------------------------------------------------- the grid
class CalendarGrid:
    """`BarGridPort` and `SessionCloseView` over the SM10 schedule (see the module docstring)."""

    def __init__(self, *, schedule_of: Callable[[str, str], SymbolSchedule | None],
                 skew_of: Callable[[str], SkewEstimate] | None = None,
                 bars_capable: Callable[[str], bool] | None = None) -> None:
        self._schedule_of = schedule_of
        self._skew_of = skew_of
        self._bars_capable = bars_capable or (lambda _adapter_id: False)

    def _schedule(self, adapter_id: str, symbol: str) -> SymbolSchedule | None:
        try:
            return self._schedule_of(adapter_id, symbol)
        except Exception:
            return None

    def has_schedule(self, adapter_id: str, symbol: str) -> bool:
        """The symbol's SM10 schedule is loaded (the liveness watch counts a row without one as overdue, S38g)."""
        return self._schedule(adapter_id, symbol) is not None

    def skew(self, adapter_id: str) -> SkewEstimate:
        if self._skew_of is None:
            return COLD_SKEW
        try:
            return self._skew_of(adapter_id)
        except Exception:
            return COLD_SKEW

    def open_bar_ends(self, adapter_id: str, symbol: str, spec: BarSpec, from_ms: int, to_ms: int) -> list[int]:
        schedule = self._schedule(adapter_id, symbol)
        if schedule is None or to_ms <= from_ms:
            return []
        tf = spec.timeframe_ms
        end = spec.bucket_start_ms(from_ms) + tf
        if end <= from_ms:
            end += tf
        out: list[int] = []
        steps = 0
        while end <= to_ms and steps < MAX_GRID_BUCKETS:
            if bucket_open(schedule, end - tf, end):
                out.append(end)
            end += tf
            steps += 1
        return out

    def bar_sides(self, adapter_id: str) -> frozenset[QuoteSide]:
        try:
            return BOTH_SIDES if self._bars_capable(adapter_id) else frozenset()
        except Exception:
            return frozenset()

    def next_open_ms(self, adapter_id: str, symbol: str, bar_end_ms: int) -> int | None:
        """For a pre-close bar (an open bucket whose end starts a closed period): the next scheduled open."""
        schedule = self._schedule(adapter_id, symbol)
        if schedule is None or phase_at(schedule, bar_end_ms) is Phase.OPEN:
            return None
        return next_open_ms(schedule, bar_end_ms)


def bucket_open(schedule: SymbolSchedule, start_ms: int, end_ms: int) -> bool:
    """Any part of `[start_ms, end_ms)` is scheduled OPEN."""
    return phase_at(schedule, start_ms) is Phase.OPEN or opens_between(schedule, start_ms, end_ms - 1)


# ---------------------------------------------------------------------------------------------------- sinks
class LateNoticeSink:
    """The `ARM_SINKS` notice slot bound to `ArmNotices.sink()` once the ARM graph exists (see the docstring)."""

    def __init__(self) -> None:
        self._sink: NoticeSink | None = None

    @property
    def name(self) -> str:
        return NOTICE_SINK

    @property
    def bound(self) -> bool:
        return self._sink is not None

    def bind(self, sink: NoticeSink) -> None:
        self._sink = sink

    async def on_decisions(self, tx: ArmStateTxn, event: ArmEvent, bar_end_ms: int,
                           decisions: Sequence[Written]) -> tuple[Written, ...]:
        sink = self._sink
        if sink is None:
            return tuple(decisions)
        return await sink.on_decisions(tx, event, bar_end_ms, decisions)

    def settle(self, tx: ArmStateTxn, *, committed: bool) -> None:
        sink = self._sink
        if sink is not None:
            sink.settle(tx, committed=committed)


# ---------------------------------------------------------------------------------------------------- tracker book
class TrackerBook:
    """The arm-tracker's memory for the BarClock (`StopHandlesPort`) and the FAILED manager (`PositionSeriesPort`)."""

    __slots__ = ("_tracker",)

    def __init__(self, tracker: ArmTracker) -> None:
        self._tracker = tracker

    def handles(self, adapter_id: str) -> dict[str, tuple[HandleRef, ...]]:
        return self._tracker.stop_handles(adapter_id)

    def series_of(self, row: ArmPositionRow) -> Sequence[str] | None:
        return self._tracker.series_of(row)


def stop_read_cost(tracker: ArmTracker | None, adapter_id: str) -> int:
    """PR9's stop-read cost of one adapter for the demand book (see the module docstring)."""
    if tracker is None:
        return 0
    return STOP_READS_PER_WINDOW if tracker.handle_count(adapter_id) > 0 else 0


__all__ = [
    "BOTH_SIDES", "CATCHUP_UNVERIFIED", "CalendarGrid", "FENCED_STREAMS", "INTEGRITY_INCIDENT", "LateNoticeSink",
    "PgTrackerSnapshot", "SQL_ARM_ACCOUNT_ORDERS", "SQL_ARM_ACCOUNT_POSITIONS", "SQL_ARM_INSTRUMENTS",
    "SQL_ARM_OPEN_ACCOUNTS", "SQL_ARM_STREAM_FENCES", "STOP_READS_PER_WINDOW", "TrackerBook", "bucket_open",
    "fence_reason", "stop_read_cost",
]
