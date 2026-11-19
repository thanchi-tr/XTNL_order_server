"""`arm replay --rerun`: the offline wiring of the pipeline re-run (go-live gate `replay_rerun`, plan S39b; the re-run
itself is `src/riskmgr/rerun.py`).

The CLI process has one PostgreSQL connection (`xtnl-ops`) and the `md_reader` client of `redis-md`. Over them it
builds the **real** S21 evaluation (`PipelineEvaluator`) with its real readers, pinned to each decision's time:

* the account image (`PgTrackerSnapshot`, its freshness not judged from today's stream fences: `AsOfImages`), the
  `S_at_T` versions (`PgArmVersionReader`) and the decision memory
  before the decision (`PgArmStateStore.decision_memory_before`), all over the one connection in READ ONLY
  REPEATABLE READ snapshots (named exception 1: no fence, nothing written); the `EngineDb` here has no epoch, so a
  fenced write could not even be attempted;
* `redis-md` through `MdPort` over the `md_reader` client (point-in-time reads, `AsOfMarketData`), trusting the
  stored bars by their own §A4 / keyed-hash checks (`_OpenBook`: SM12 lives in the engine's memory) and the store's
  own writer epoch (`md:writer`) as the leader epoch the reads expect;
* the bar grid and the SM10 state at the decision's time from the latest `market_sessions` version;
* the real `JobRunner` (its own lanes, closed at the end).

Nothing is written to PostgreSQL or `redis-md`, nothing is leased or re-fetched, and no notice is queued or sent.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any

from src.calendar.schedule import SymbolSchedule
from src.db.arm_ports import DecisionRef, PositionKey
from src.db.arm_store import PgArmStateStore
from src.db.arm_versions import PgArmVersionReader
from src.db.epoch import EngineDb, Epoch
from src.db.pg import PgConnection
from src.marketdata.hashing import Keyring
from src.marketdata.port_impl import MdPort, token_epoch
from src.marketdata.timing import COLD_SKEW, timing_for
from src.marketdata.types import BarSpec, DataUnavailable, SeriesId, Sm12State
from src.riskmgr.config import ArmSettings
from src.riskmgr.evaluation import PipelineEvaluator
from src.riskmgr.jobs import JobRunner
from src.riskmgr.rerun import (
    AsOfCalendar,
    AsOfDecisions,
    AsOfImages,
    AsOfMarketData,
    RerunInputs,
    RerunResult,
    RerunTarget,
    RerunVerdict,
    rerun_one,
)
from src.runtime.arm_ports import CalendarGrid, PgTrackerSnapshot
from src.runtime.pg_ports import PgScheduleLoader

from src.ops.md_view import OpsMdReader


class _OneConnection:
    """`ConnectionSource` over the CLI's one connection."""

    def __init__(self, conn: PgConnection) -> None:
        self._conn = conn

    @asynccontextmanager
    async def _acquire(self) -> AsyncIterator[PgConnection]:
        yield self._conn

    def acquire(self) -> Any:
        return self._acquire()


def _no_epoch() -> Epoch:
    raise RuntimeError("arm replay --rerun is read-only: no fenced write")


class _OpenBook:
    """SM12 for the offline reads: every stored series is readable (each bar is still checked by §A4 and its keyed
    hash on read; a series the engine held FAILED is not known here)."""

    def state(self, series: SeriesId) -> Sm12State:
        return Sm12State.LIVE

    def readable(self, series: SeriesId, from_ms: int | None, to_ms: int | None) -> bool:
        return True

    def gaps(self, series: SeriesId) -> int:
        return 0


def target_of(r: Sequence[Any]) -> RerunTarget:
    """One `SQL_DECISIONS_VIEW` row (`arm_view.decision_row` column order) as a re-run target."""
    return RerunTarget(
        decision_id=int(r[0]), key=PositionKey(adapter_id=str(r[2]), position_id=str(r[3])), bar_spec=str(r[4]),
        event=str(r[5]), bar_end_ms=int(r[1]), revision=int(r[6]), decision_seq=int(r[7]), decided_at_ms=int(r[8]),
        outcome=str(r[10]), reason=None if r[12] is None else str(r[12]),
        inputs_digest=None if r[23] is None else str(r[23]).strip())


@dataclass
class Rerunner:
    """The re-run machinery of one `arm replay --rerun` invocation."""

    conn: PgConnection
    reader: OpsMdReader
    keyring: Keyring
    settings: ArmSettings
    schedules: dict[tuple[str, str], SymbolSchedule]
    writer_epoch: int | None
    jobs: JobRunner

    @classmethod
    async def build(cls, conn: PgConnection, reader: OpsMdReader, keyring: Keyring,
                    settings: ArmSettings) -> Rerunner:
        calendar = await PgScheduleLoader(_OneConnection(conn)).load()
        head = await reader.call(reader.store.head())
        epoch = token_epoch(head.writer)
        return cls(conn, reader, keyring, settings, dict(calendar.symbols), epoch, JobRunner())

    def _db(self) -> EngineDb:
        return EngineDb(_OneConnection(self.conn), _no_epoch)

    def _schedule_of(self, adapter_id: str, symbol: str) -> SymbolSchedule | None:
        return self.schedules.get((adapter_id, symbol))

    async def run(self, target: RerunTarget) -> RerunVerdict:
        db = self._db()
        store = PgArmStateStore(db)
        target = replace(target, handles=await store.decision_handles(
            DecisionRef(decision_id=target.decision_id, bar_end_ms=target.bar_end_ms)))
        row = await store.position(target.key)
        if row is None:
            return RerunVerdict(RerunResult.UNVERIFIABLE, "position_unknown")
        try:
            spec = BarSpec.parse(target.bar_spec)
        except ValueError:
            return RerunVerdict(RerunResult.UNVERIFIABLE, "bar_spec")
        if timing_for(spec) is None:
            return RerunVerdict(RerunResult.UNVERIFIABLE, "bar_spec")
        port = MdPort(self.reader.store, keyring=self.keyring, book=_OpenBook(), leader_epoch=lambda: self.writer_epoch)
        md = AsOfMarketData(port, target.decided_at_ms)
        generation = await md.md_generation()
        if isinstance(generation, DataUnavailable):
            return RerunVerdict(RerunResult.UNVERIFIABLE, f"md:{generation.reason}")
        grid = CalendarGrid(schedule_of=self._schedule_of)
        evaluator = PipelineEvaluator(
            md=md, snapshot=AsOfImages(PgTrackerSnapshot(db)), versions=PgArmVersionReader(db),
            decisions=AsOfDecisions(store, target.decision_seq), grid=grid,
            calendar=AsOfCalendar(self._schedule_of, target.decided_at_ms), settings=self.settings,
            keyring=lambda: self.keyring)
        return await rerun_one(evaluator, self.jobs, target, RerunInputs(row, spec, COLD_SKEW, generation))

    def close(self) -> None:
        self.jobs.close()


__all__ = ["Rerunner", "target_of"]
