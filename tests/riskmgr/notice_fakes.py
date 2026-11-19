"""In-memory fakes for the notice tests (S28). Synthetic ids, times and levels only.

`NStore` keeps positions, decisions (with their `notice_id`), `arm_notices` keys and the outbox rows the notice
port queued, so a test can follow every decision to its notice (or the suppressed sentinel 0). Its transaction rolls
back like PostgreSQL's: on any error, or when a test sets `fail_commits`, every row the transaction wrote (decisions,
notice ids, `arm_notices` keys, outbox rows) is undone.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from decimal import Decimal

from src.armconfig.validate import RoutePolicy
from src.db.arm_ports import (
    TERMINAL_ARM_STATES,
    ArmPositionRow,
    ArmStateTxn,
    CorrectionAuditRow,
    DecisionHandleRow,
    DecisionMemory,
    DecisionRef,
    DecisionRow,
    DecisionWrite,
    DecisionWriteStatus,
    NoticeRow,
    PositionCas,
    PositionKey,
    StoredDecision,
    UnnoticedPage,
)
from src.marketdata.timing import COLD_SKEW, SkewEstimate
from src.marketdata.types import M15, BarSpec
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.config import ArmSettings
from src.riskmgr.notices import ArmNotices, NoticeSettings, OutboxNotice, RouteBook
from src.riskmgr.ports import Pager
from src.riskmgr.render import NoticeBody, decode_body
from src.riskmgr.sinks import Decided, DecisionSink, SinkChain, Written
from src.riskmgr.types import ArmEvent
from tests.riskmgr.tracker_fakes import ADAPTER, ACCOUNT, T0, FakeClock, Recorder

D = Decimal
TF = M15.timeframe_ms
T1 = T0 + TF
T2 = T1 + TF
T3 = T2 + TF
CAP_AFTER = TF + COLD_SKEW.settle_ms
"""`decision_cap` − T with the cold skew."""
ROUTE = "main"
ENTRY = D("1.23456")
S0 = D("1.21456")


def row(pid: str, *, symbol: str = "SYN.A", side: str = "BUY", state: str = "TRACKING", cursor: int | None = T0,
        armed_ms: int = T0 - 5 * TF, adapter: str = ADAPTER, account: str = ACCOUNT) -> ArmPositionRow:
    return ArmPositionRow(
        key=PositionKey(adapter_id=adapter, position_id=pid), account_id=account, symbol=symbol,
        side=side,  # type: ignore[arg-type]
        state=state, edge_code="default", attribution_source="SINGLE_EDGE_DEFAULT", strategy_code="be",
        strategy_version=1, strategy_kind="break_even_on_risk_close", strategy_impl_version=1,
        strategy_params_json="{}", guards_hash="a" * 64, bar_spec=M15.name, config_doc_hash="ab" * 32,
        entry_level=ENTRY, entry_time_ms=armed_ms, entry_time_source="STREAM", s0_level=S0, s0_source="AT_FILL",
        pinned_units=D("1000"), last_decided_bar=cursor, armed_at_ms=armed_ms, updated_at_ms=armed_ms)


def decision(r: ArmPositionRow, *, event: str = "POST", bar_end: int = T1, outcome: str = "ADJUST",
             category: str = "ACTIONABLE", reason: str | None = None, reason_kind: str | None = None,
             seq: int = 1, at: int = T1, caveats: tuple[str, ...] = (), series: tuple[str, ...] = (),
             trigger_met: bool | None = True, revision: int = 0, risk_r: str | None = "0.85",
             supersedes: int | None = None) -> DecisionRow:
    return DecisionRow(
        key=r.key, bar_end_ms=bar_end, bar_spec=M15.name, event=event, revision=revision, decision_seq=seq,
        decided_at_ms=at, edge_code="default", outcome=outcome, category=category, reason=reason,
        reason_kind=reason_kind, trigger_met=trigger_met, advice=event != "PRE" and outcome == "ADJUST",
        proposed_stop=D("1.23500") if outcome == "ADJUST" else None,
        current_risk_r=None if risk_r is None else D(risk_r), caveats=caveats,
        close_source="QUOTE" if event == "PRE" else "BROKER", series_inputs=series, envelope_version=1,
        envelope_params_hash="e" * 64, strategy_kind=r.strategy_kind, strategy_impl_version=1,
        guards_hash=r.guards_hash, supersedes_decision_id=supersedes,
        supersedes_bar_end_ms=None if supersedes is None else bar_end)


@dataclass
class Stored:
    ref: DecisionRef
    row: DecisionRow
    notice_id: int | None = None
    handles: tuple[DecisionHandleRow, ...] = ()


class NTxn:
    def __init__(self, store: NStore) -> None:
        self.store = store
        self.events: list[str] = []

    async def insert_position(self, r: ArmPositionRow) -> bool:
        if r.key in self.store.rows:
            return False
        self.store.rows[r.key] = r
        return True

    async def update_position(self, cas: PositionCas) -> bool:
        cur = self.store.rows.get(cas.row.key)
        if cur is None or cur.state in TERMINAL_ARM_STATES or cur.row_version != cas.expected_row_version:
            return False
        self.store.rows[cas.row.key] = replace(cas.row, row_version=cur.row_version + 1)
        return True

    async def record_decision(self, d: DecisionRow, handles: Sequence[DecisionHandleRow] = (), *,
                              position: PositionCas | None = None) -> DecisionWrite:
        k = (d.key, d.bar_spec, d.event, d.bar_end_ms, d.revision)
        if any((s.row.key, s.row.bar_spec, s.row.event, s.row.bar_end_ms, s.row.revision) == k
               for s in self.store.decisions):
            return DecisionWrite(DecisionWriteStatus.ALREADY_DECIDED)
        if position is not None and not await self.update_position(position):
            return DecisionWrite(DecisionWriteStatus.CAS_MISS)
        ref = DecisionRef(decision_id=len(self.store.decisions) + 1, bar_end_ms=d.bar_end_ms)
        self.store.decisions.append(Stored(ref, d, handles=tuple(handles)))
        return DecisionWrite(DecisionWriteStatus.INSERTED, ref)

    async def insert_notice(self, n: NoticeRow) -> int | None:
        assert 0 <= n.part <= 5 and (n.part == 0 or n.event in ("PRE", "POST"))
        key = (n.route, n.bar_spec, n.event, n.bar_end_ms, n.part, n.item_key)
        if key in self.store.notices:
            return None
        nid = len(self.store.notices) + 1
        self.events.append(n.event)
        self.store.notices[key] = nid
        self.store.notice_rows[nid] = n
        return nid

    def _set(self, value: int, refs: Sequence[DecisionRef]) -> int:
        wanted = set(refs)
        n = 0
        for s in self.store.decisions:
            if s.ref in wanted and s.notice_id is None:
                s.notice_id = value
                n += 1
        return n

    async def attach_notice(self, notice_id: int, refs: Sequence[DecisionRef]) -> int:
        assert notice_id > 0
        return self._set(notice_id, refs)

    async def suppress(self, refs: Sequence[DecisionRef]) -> int:
        return self._set(0, refs)

    async def insert_correction_audit(self, r: CorrectionAuditRow) -> bool:
        self.store.audits.append(r)
        return True

    async def unnoticed_decisions(self, route: Sequence[tuple[str, str]], bar_spec: str, *, since_bar_end_ms: int,
                                  limit: int = 500) -> UnnoticedPage:
        pairs = set(route)
        out = []
        for s in sorted(self.store.decisions, key=lambda s: (s.row.bar_end_ms, s.ref.decision_id)):
            r = self.store.rows.get(s.row.key)
            if (s.notice_id is None and r is not None and (s.row.key.adapter_id, r.account_id) in pairs
                    and s.row.bar_spec == bar_spec and s.row.bar_end_ms >= since_bar_end_ms):
                out.append(StoredDecision(ref=s.ref, row=s.row))
        self.store.reads += 1
        return UnnoticedPage(decisions=tuple(out[:limit]), truncated=len(out) > limit)


class NStore:
    def __init__(self) -> None:
        self.rows: dict[PositionKey, ArmPositionRow] = {}
        self.decisions: list[Stored] = []
        self.notices: dict[tuple[object, ...], int] = {}
        self.notice_rows: dict[int, NoticeRow] = {}
        self.reads = 0
        self.outbox: FakeOutbox | None = None
        self.fail_commits = 0
        """The next this many commits fail (the transaction rolls back and `ConnectionError` is raised)."""
        self.fail_events: set[str] = set()
        """A commit fails while the transaction inserted a notice of one of these events."""
        self.rollbacks = 0
        self.memory_reads = 0
        self.recent_reads = 0
        self.fail_recent = 0
        self.audits: list[CorrectionAuditRow] = []
        """`arm_correction_audit` rows written (audit only; not rolled back by this fake)."""

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[NTxn]:
        saved = (dict(self.rows), [replace(s) for s in self.decisions], dict(self.notices), dict(self.notice_rows),
                 list(self.outbox.rows) if self.outbox is not None else [])
        try:
            txn = NTxn(self)
            yield txn
            if self.fail_events & set(txn.events):
                raise ConnectionError("commit failed")
            if self.fail_commits > 0:
                self.fail_commits -= 1
                raise ConnectionError("commit failed")
        except BaseException:
            self.rows, self.decisions, self.notices, self.notice_rows = saved[0], saved[1], saved[2], saved[3]
            if self.outbox is not None:
                self.outbox.rows = saved[4]
            self.rollbacks += 1
            raise

    async def live_positions(self) -> list[ArmPositionRow]:
        return [r for _, r in sorted(self.rows.items(), key=lambda kv: kv[0].position_id)
                if r.state not in TERMINAL_ARM_STATES]

    async def position(self, key: PositionKey) -> ArmPositionRow | None:
        return self.rows.get(key)

    async def recent_final_decisions(self, since_bar_end_ms: int, limit: int) -> list[StoredDecision]:
        """`RecentDecisionReader` (plan S39b): the `SQL_RECENT_FINAL_DECISIONS` semantics."""
        self.recent_reads += 1
        if self.fail_recent > 0:
            self.fail_recent -= 1
            raise ConnectionError("read failed")
        got = sorted((s for s in self.decisions if s.row.bar_end_ms >= since_bar_end_ms
                      and s.row.event in ("POST", "LATE", "CORRECTED") and s.row.outcome != "UNRESOLVED_RANGE"),
                     key=lambda s: (s.row.bar_end_ms, s.row.decision_seq), reverse=True)
        return [StoredDecision(ref=s.ref, row=s.row) for s in got[:limit]]

    async def decision_memory(self, key: PositionKey, bar_spec: str, bar_end_ms: int) -> DecisionMemory:
        """`ArmDecisionReader` over the committed rows (the `SQL_PRE_ADJUST_LEVELS` / `SQL_LAST_BAR_END_DECISION`
        semantics)."""
        self.memory_reads += 1
        return self._memory(key, bar_spec, bar_end_ms, None)

    async def decision_memory_before(self, key: PositionKey, bar_spec: str, bar_end_ms: int,
                                     decision_seq: int) -> DecisionMemory:
        """`PastDecisionReader` (plan S39b): the same reads bounded by `decision_seq`."""
        return self._memory(key, bar_spec, bar_end_ms, decision_seq)

    def _memory(self, key: PositionKey, bar_spec: str, bar_end_ms: int, before: int | None) -> DecisionMemory:
        mine = [s for s in self.decisions if s.row.key == key and s.row.bar_spec == bar_spec
                and (before is None or s.row.decision_seq < before)]
        pre = [s for s in mine if s.row.event == "PRE" and s.row.bar_end_ms == bar_end_ms and s.row.outcome == "ADJUST"]
        levels = sorted((h.handle_role, h.level_at_pre) for s in pre for h in s.handles if h.level_at_pre is not None)
        ends = sorted((s for s in mine if s.row.event != "PRE" and s.row.outcome != "NO_BROKER_BAR"),
                      key=lambda s: s.row.decision_seq)
        last = ends[-1].row if ends else None
        advice = sorted((s for s in mine if s.row.event in ("POST", "LATE", "REOPEN", "CORRECTED")
                         and s.row.outcome == "ADJUST" and s.row.bar_end_ms < bar_end_ms
                         and s.row.proposed_stop is not None), key=lambda s: s.row.decision_seq)
        prior = sorted((s for s in ends if s.row.bar_end_ms < bar_end_ms),
                       key=lambda s: (s.row.bar_end_ms, s.row.decision_seq))
        prior_row = prior[-1].row if prior else None
        return DecisionMemory(pre_levels=tuple((r, lv) for r, lv in levels if lv is not None),
                              last_outcome=None if last is None else last.outcome,
                              last_reason=None if last is None else last.reason,
                              last_reason_kind=None if last is None else last.reason_kind,
                              final_advice_target=advice[-1].row.proposed_stop if advice else None,
                              prior_final_adjust=prior_row.proposed_stop
                              if prior_row is not None and prior_row.outcome == "ADJUST" else None)

    def notice_of(self, pid: str, event: str = "POST", bar_end: int = T1) -> int | None:
        for s in self.decisions:
            if s.row.key.position_id == pid and s.row.event == event and s.row.bar_end_ms == bar_end:
                return s.notice_id
        raise KeyError(pid)


class FakeOutbox:
    def __init__(self) -> None:
        self.rows: list[OutboxNotice] = []
        self.refuse: set[str] = set()
        """Routes whose rows the store refuses (a persistent fault on one route)."""

    async def enqueue(self, tx: ArmStateTxn, notice: OutboxNotice) -> None:
        if notice.route in self.refuse:
            raise RuntimeError("outbox refused the row")
        self.rows.append(notice)

    def bodies(self, event: str | None = None) -> list[NoticeBody]:
        out = [decode_body(r.body) for r in self.rows]
        return [b for b in out if event is None or b.event == event]


class Grid:
    def skew(self, adapter_id: str) -> SkewEstimate:
        return COLD_SKEW

    def open_bar_ends(self, adapter_id: str, symbol: str, spec: BarSpec, from_ms: int, to_ms: int) -> list[int]:
        first = spec.bucket_start_ms(from_ms) + spec.timeframe_ms
        return list(range(first, to_ms + 1, spec.timeframe_ms))

    def bar_sides(self, adapter_id: str) -> frozenset[QuoteSide]:
        return frozenset({QuoteSide.BID, QuoteSide.ASK})


@dataclass
class Policies:
    mode: str = "every_bar"
    pre: tuple[str, ...] = ("arm_email",)
    post: tuple[str, ...] = ("arm_email",)

    def policy(self, route: str, at_ms: int) -> RoutePolicy:
        return RoutePolicy(route=route, mode=self.mode, pre_channels=self.pre, post_channels=self.post)


@dataclass
class Rig:
    store: NStore
    outbox: FakeOutbox
    clock: FakeClock
    rec: Recorder
    notices: ArmNotices
    policies: Policies
    chain: SinkChain = field(init=False)

    def __post_init__(self) -> None:
        self.chain = SinkChain((DecisionSink(), self.notices.sink()))

    def add(self, *rows: ArmPositionRow) -> None:
        for r in rows:
            self.store.rows[r.key] = r

    async def commit(self, event: str, bar_end: int, *decided: tuple[DecisionRow, bool],
                     at: int | None = None, handles: tuple[DecisionHandleRow, ...] = ()) -> tuple[Written, ...]:
        """One BarClock (event, bar end) transaction through the real sink chain."""
        if at is not None:
            self.clock.now = at
        items = []
        for d, shown in decided:
            cur = self.store.rows[d.key]
            new = replace(cur, last_decided_bar=bar_end if event != "PRE" else cur.last_decided_bar,
                          decision_seq=d.decision_seq)
            items.append(Decided(ref=cur.ref, decision=d, position=PositionCas(row=new,
                                                                                expected_row_version=cur.row_version),
                                 handles=handles, shown=shown))
        held: ArmStateTxn | None = None
        try:
            async with self.store.transaction() as tx:
                held = tx
                out = await self.chain.run(tx, ArmEvent(event), bar_end, items)
        except BaseException:
            self.chain.settle(held, committed=False)
            raise
        self.chain.settle(held, committed=True)
        return out


def takeover(g: Rig) -> ArmNotices:
    """A new leader's notices on the same store and outbox: empty memory (parts, closed windows, extras)."""
    return ArmNotices(store=g.store, outbox=g.outbox, routes=RouteBook({(ADAPTER, ACCOUNT): ROUTE}),
                      policies=g.policies, grid=Grid(), pager=Pager(g.rec.page, lambda t, b: None), clock=g.clock,
                      settings=ArmSettings(), notices=NoticeSettings(test_at_ms_of_day=86_399_999))


def rig(*, mode: str = "every_bar", fold: str = "inside", aliases: bool = True, now: int = T1) -> Rig:
    store = NStore()
    outbox = FakeOutbox()
    store.outbox = outbox
    clock = FakeClock(now)
    rec = Recorder()
    policies = Policies(mode=mode)
    routes = RouteBook({(ADAPTER, ACCOUNT): ROUTE} if aliases else {})
    notices = ArmNotices(store=store, outbox=outbox, routes=routes, policies=policies, grid=Grid(),
                         pager=Pager(rec.page, lambda t, b: None), clock=clock, settings=ArmSettings(),
                         notices=NoticeSettings(fold=fold, test_at_ms_of_day=86_399_999))
    return Rig(store, outbox, clock, rec, notices, policies)
