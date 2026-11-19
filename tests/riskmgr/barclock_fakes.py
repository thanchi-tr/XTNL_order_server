"""In-memory fakes for the BarClock, stop-read and sink tests (S27). Synthetic ids, times and levels only.

The evaluation port is a fake (`FakeEval`): S27 joins the S22 replay planner and job runner but imports no S21
module (the pipeline waits for the user's §9.2 / C1 confirmation), so the tests stand in for `prepare` and the job.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from decimal import Decimal

from src.db.arm_ports import (
    ArmPositionRow,
    ControlsRow,
    DecisionHandleRow,
    DecisionRow,
    DecisionWrite,
    PositionCas,
    PositionKey,
)
from src.marketdata.ports import HandleRead, HandleRef, HandleStatus, StopHandleRead
from src.marketdata.timing import COLD_SKEW, SkewEstimate
from src.marketdata.types import M15, BarSpec, MdGeneration, Transient
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.barclock import (
    BarClock,
    BarClockSettings,
    EvalRequest,
    Evaluated,
    NotReady,
    Prepared,
    Verdict,
)
from src.riskmgr.config import ArmSettings
from src.riskmgr.jobs import JobRunner, JobSpec
from src.riskmgr.ports import Pager
from src.riskmgr.sm11 import Sm11Event
from src.riskmgr.stopreads import StopReads
from src.riskmgr.types import ArmEvent
from tests.riskmgr.tracker_fakes import ADAPTER, ACCOUNT, SYMBOL, T0, FakeStore, FakeTxn, Recorder

D = Decimal
TF = M15.timeframe_ms
T1 = T0 + TF
T2 = T1 + TF
T3 = T2 + TF
SETTLE = COLD_SKEW.settle_ms


# ---------------------------------------------------------------------------------------------------- rows
def arm_row(pid: str = "P1", *, cursor: int | None = T0, state: str = "TRACKING", entry_ms: int = T0 - 3 * TF,
            adapter: str = ADAPTER, reopen: int | None = None, prior: str | None = None) -> ArmPositionRow:
    return ArmPositionRow(
        key=PositionKey(adapter_id=adapter, position_id=pid), account_id=ACCOUNT, symbol=SYMBOL, side="BUY",
        state=state, prior_state=prior, edge_code="default", attribution_source="SINGLE_EDGE_DEFAULT",
        strategy_code="be", strategy_version=1, strategy_kind="break_even_on_risk_close", strategy_impl_version=1,
        strategy_params_json="{}", guards_hash="g" * 64, bar_spec=M15.name, config_doc_hash="ab" * 32,
        entry_level=D("1.2000"), entry_time_ms=entry_ms, entry_time_source="STREAM", s0_level=D("1.1900"),
        s0_source="AT_FILL", pinned_units=D("1000"), last_decided_bar=cursor, reopen_bar_end_ms=reopen,
        armed_at_ms=entry_ms, updated_at_ms=entry_ms)


def template(outcome: str = "ADJUST", category: str = "ACTIONABLE", reason: str | None = None) -> DecisionRow:
    """The outcome fields an evaluation returns (the BarClock fills in the key, bar, event, seq and times)."""
    return DecisionRow(key=PositionKey(adapter_id="x", position_id="x"), bar_end_ms=0, bar_spec="", event="POST",
                       decision_seq=0, decided_at_ms=0, edge_code="", outcome=outcome, category=category,
                       reason=reason, advice=False, envelope_version=1, envelope_params_hash="e" * 64,
                       strategy_kind="", strategy_impl_version=0, guards_hash="")


def decided(*, final: bool = True, events: tuple[Sm11Event, ...] = (), reopen: bool = False,
            outcome: str = "ADJUST", category: str = "ACTIONABLE", reason: str | None = None) -> Evaluated:
    return Evaluated(verdict=Verdict.DECISION, decision=template(outcome, category, reason), final=final,
                     trigger_hash="k1|" + "0" * 64, sm11_events=events, reopen_candidate=reopen)


# ---------------------------------------------------------------------------------------------------- the store
class BcTxn(FakeTxn):
    store: BcStore

    async def record_decision(self, decision: DecisionRow, handles: Sequence[DecisionHandleRow] = (), *,
                              position: PositionCas | None = None) -> DecisionWrite:
        pid = decision.key.position_id
        if pid in self.store.raise_with:
            raise self.store.raise_with.pop(pid)
        if pid in self.store.raise_once:
            self.store.raise_once.discard(pid)
            raise ConnectionError("transient")       # rolled back inside its own savepoint by the real store
        return await super().record_decision(decision, handles, position=position)


class BcStore(FakeStore):
    def __init__(self, *rows: ArmPositionRow) -> None:
        super().__init__()
        for r in rows:
            self.rows[r.key] = r
        self.raise_once: set[str] = set()
        self.raise_with: dict[str, BaseException] = {}
        """`record_decision` of this position raises this error once (e.g. a lost fence)."""
        self.transactions: list[int] = []

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[BcTxn]:
        self.transactions.append(len(self.decisions))
        yield BcTxn(self)

    def rows_of(self, pid: str = "P1") -> list[DecisionRow]:
        return sorted((d for d in self.decisions.values() if d.key.position_id == pid),
                      key=lambda d: d.decision_seq)


# ---------------------------------------------------------------------------------------------------- ports
@dataclass
class FakeControls:
    paused: bool = False
    paused_at_ms: int | None = None
    fail: bool = False
    reads: int = 0

    async def controls(self) -> ControlsRow | None:
        self.reads += 1
        if self.fail:
            raise ConnectionError("controls unreadable")
        return ControlsRow(paused=self.paused, pause_reason="operator" if self.paused else None,
                           paused_at_ms=self.paused_at_ms, drain_notices=False, notify_test_at_ms=None,
                           updated_at_ms=0)


@dataclass
class FakeMdGen:
    generation: MdGeneration = MdGeneration(1, 1)
    bump_every_call: bool = False

    async def md_generation(self) -> MdGeneration:
        g = self.generation
        if self.bump_every_call:
            self.generation = MdGeneration(g.epoch_rev, g.n + 1)
        return g


class FakeGrid:
    """Every 15m bucket is open, except bar ends listed in `closed`."""

    def __init__(self, skew: SkewEstimate = COLD_SKEW, closed: frozenset[int] = frozenset()) -> None:
        self._skew = skew
        self.closed = closed

    def skew(self, adapter_id: str) -> SkewEstimate:
        return self._skew

    def open_bar_ends(self, adapter_id: str, symbol: str, spec: BarSpec, from_ms: int, to_ms: int) -> list[int]:
        first = spec.bucket_start_ms(from_ms) + spec.timeframe_ms
        return [e for e in range(first, to_ms + 1, spec.timeframe_ms) if e not in self.closed]

    def bar_sides(self, adapter_id: str) -> frozenset[QuoteSide]:
        return frozenset({QuoteSide.BID, QuoteSide.ASK})


@dataclass
class FakeClock:
    now: int
    mono_offset: int = 0

    def now_ms(self) -> int:
        return self.now

    def monotonic_ms(self) -> int:
        return self.now + self.mono_offset


Rule = Callable[[EvalRequest], "Evaluated | NotReady | None"]


class FakeEval:
    """`rule(request)` returns an `Evaluated` (the job's value), a `NotReady`, or None for the default (a final
    ADJUST at bar-end events, a PRE row at PRE). `raise_job` makes the job raise."""

    def __init__(self, rule: Rule | None = None) -> None:
        self.rule = rule
        self.requests: list[EvalRequest] = []
        self.raise_job: set[str] = set()
        self.inflight = 0
        self.max_inflight = 0

    async def prepare(self, request: EvalRequest) -> Prepared | NotReady:
        self.requests.append(request)
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        await asyncio.sleep(0)                       # a market-data read: lets other prepares start
        self.inflight -= 1
        got = None if self.rule is None else self.rule(request)
        if isinstance(got, NotReady):
            return got
        value = got if got is not None else (
            decided(final=False) if request.event is ArmEvent.PRE
            else decided(events=(Sm11Event.FINAL_ADJUST,)))
        pid = request.row.key.position_id

        def job() -> Evaluated:
            if pid in self.raise_job:
                raise RuntimeError("boom")
            return value

        return Prepared(JobSpec("strategy:break_even_on_risk_close:1"), job)

    def events(self, pid: str = "P1") -> list[tuple[ArmEvent, int]]:
        return [(r.event, r.bar_end_ms) for r in self.requests if r.row.key.position_id == pid]


class FakeHandles:
    def __init__(self, book: Mapping[str, Mapping[str, tuple[HandleRef, ...]]] | None = None) -> None:
        self.book: dict[str, dict[str, tuple[HandleRef, ...]]] = {a: dict(m) for a, m in (book or {}).items()}

    def handles(self, adapter_id: str) -> Mapping[str, tuple[HandleRef, ...]]:
        return self.book.get(adapter_id, {})


class FakeStopPort:
    """`read_stop_handles` answers at once, sent at the clock's now; `gated`: every read waits until the test calls
    `release()` (a saturated BARS bucket: the request leaves only then), and is sent at the clock's now at release."""

    def __init__(self, clock: FakeClock, *, fail: bool = False, gated: bool = False) -> None:
        self.clock = clock
        self.fail = fail
        self.gate: asyncio.Event | None = asyncio.Event() if gated else None
        self.calls: list[tuple[str, tuple[HandleRef, ...], int]] = []

    def release(self) -> None:
        assert self.gate is not None
        self.gate.set()

    @property
    def held(self) -> bool:
        return self.gate is not None and not self.gate.is_set()

    async def read_stop_handles(self, adapter_id: str, handles: Sequence[HandleRef]) -> StopHandleRead | Transient:
        self.calls.append((adapter_id, tuple(handles), self.clock.now))
        if self.gate is not None:
            await self.gate.wait()
        if self.fail:
            return Transient("down")
        sent = self.clock.now
        return StopHandleRead(adapter_id, tuple(HandleRead(h, HandleStatus.OK, sent, "v1") for h in handles), sent)


def handles_for(*pids: str, per: int = 2, adapter: str = ADAPTER) -> FakeHandles:
    book = {f"{adapter}:{pid}": tuple(HandleRef("CHILD_ORDER", f"{pid}-S{i}") for i in range(per)) for pid in pids}
    return FakeHandles({adapter: book})


@dataclass
class BcRig:
    clock_: BarClock
    store: BcStore
    eval: FakeEval
    controls: FakeControls
    md: FakeMdGen
    clock: FakeClock
    rec: Recorder
    grid: FakeGrid
    reads: StopReads
    port: FakeStopPort | None = None
    lease: list[bool] = field(default_factory=lambda: [True])

    async def step(self, at: int | None = None) -> None:
        if at is not None:
            self.clock.now = at
        await self.clock_.step()
        if self.port is not None and self.port.held:
            await asyncio.sleep(0)                   # the read tasks start and wait at the gate
        else:
            await self.reads.drain()

    async def release_reads(self, at: int) -> None:
        """The gated reads leave (and return) at `at`."""
        assert self.port is not None
        self.clock.now = at
        self.port.release()
        await self.reads.drain()


def bc_rig(*rows: ArmPositionRow, now: int = T1 + SETTLE + 1_000, rule: Rule | None = None,
           settings: ArmSettings | None = None, stop_reads: bool = False, handles: FakeHandles | None = None,
           gated: bool = False, grid: FakeGrid | None = None, closes: object | None = None,
           store: object | None = None, rows_ttl_ms: int = 0, read_port: object | None = None,
           fatal: Callable[[BaseException], bool] | None = None,
           config_pause: Callable[[], str | None] | None = None) -> BcRig:
    settings = settings or ArmSettings(stop_reads=stop_reads)
    store = store or BcStore(*(rows or (arm_row(),)))  # a real `PgArmStateStore` may stand in
    clock = FakeClock(now)
    rec = Recorder()
    port = FakeStopPort(clock, gated=gated) if stop_reads else None
    reads = StopReads(read_port or port, handles or handles_for("P1"),  # type: ignore[arg-type]
                      enabled=settings.stop_reads,
                      pre_offset_s=settings.pre_offset_s)
    ev, controls, md = FakeEval(rule), FakeControls(), FakeMdGen()
    grid = grid or FakeGrid()
    lease = [True]
    bc = BarClock(store=store, controls=controls, md=md, grid=grid, evaluator=ev,  # type: ignore[arg-type]
                  jobs=JobRunner(), pager=Pager(rec.page, lambda t, b: None), clock=clock, settings=settings,
                  lease_valid=lambda: lease[0], stop_reads=reads, closes=closes,  # type: ignore[arg-type]
                  barclock=BarClockSettings(rows_ttl_ms=rows_ttl_ms), fatal=fatal, config_pause=config_pause)
    return BcRig(bc, store, ev, controls, md, clock, rec, grid, reads, port, lease)  # type: ignore[arg-type]


def same_row(a: ArmPositionRow, b: ArmPositionRow) -> bool:
    return replace(a, row_version=0, updated_at_ms=0) == replace(b, row_version=0, updated_at_ms=0)


__all__ = ["ADAPTER", "SYMBOL", "T0", "T1", "T2", "T3", "TF", "SETTLE"]
