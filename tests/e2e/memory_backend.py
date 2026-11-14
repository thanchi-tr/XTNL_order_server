"""In-memory `LeaderPorts` (src/runtime/ports.py) for the no-network end-to-end smoke test.

The PostgreSQL side is emulated with the repository's own semantics: the same `decide()` version
compare as PgStateStore, current rows kept as `CommittedRow`s in the integrity fake (so the integrity
worker, SM8 heal and the digest see exactly what was "committed"), and the Oversee tables in
`InMemoryOverseeDb`. Redis is real script code on fakeredis + lupa.
"""
from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from types import MappingProxyType
from typing import Any

import redis.asyncio as aioredis

from src.cache.live_reader import read_account_live, read_gate_keys, read_quotes
from src.cache.scripts import entity_write, instrument_write
from src.calendar.schedule import CalendarSchedule
from src.db.epoch import Epoch
from src.db.gate_reader import GateInputs, reconcile_state
from src.db.repository import PgCommit
from src.db.snapshot_reader import SnapshotInputs
from src.dto import AccountDTO, CashFlowDTO, Decision, InstrumentSpec, OrderDTO, PositionDTO, PriceDTO, decide
from src.integrity.ports import HaltRow
from src.oversee.feed import RecordingQuarantine
from src.oversee.gate import Action, GateContext
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.snapshot import AccountSnapshot, SymbolQuotes, TimedMid
from src.oversee.types import HaltLevel, SymbolKey
from src.ports import DlqRow, StreamKind
from src.runtime.leader_init import FenceOutcome
from src.runtime.ports import LeaderPorts, SodStatus
from src.runtime.roles import EndReason
from tests.integrity.fakes import FakeDlqStore, FakeIntegrityDb

Stateful = OrderDTO | PositionDTO | AccountDTO


def wall_ms() -> int:
    return time.time_ns() // 1_000_000


class WallClock:
    def __call__(self) -> int:
        return wall_ms()


class MemIntegrityDb(FakeIntegrityDb):
    """The integrity fake whose halt rows come from the Oversee tables."""

    def __init__(self, oversee: InMemoryOverseeDb) -> None:
        super().__init__()
        self._oversee = oversee

    async def latest_halt_event(self) -> HaltRow | None:
        evs = self._oversee.t.halt_events
        if not evs:
            return None
        ev = evs[-1]
        return HaltRow(ev.id, ev.effective_level.name)  # type: ignore[arg-type]


@dataclass
class MemTables:
    epoch: int
    oversee: InMemoryOverseeDb
    integrity: MemIntegrityDb
    entities: dict[str, Stateful] = field(default_factory=dict)            # key -> current DTO
    instruments: dict[SymbolKey, InstrumentSpec] = field(default_factory=dict)
    cash_flows: dict[tuple[str, str], CashFlowDTO] = field(default_factory=dict)
    clean_marks: dict[SymbolKey, PriceDTO] = field(default_factory=dict)
    stream_health: dict[tuple[str, str], tuple[int | None, int | None, int | None]] = field(default_factory=dict)
    dlq: list[DlqRow] = field(default_factory=list)
    auth_failed: set[tuple[str, str]] = field(default_factory=set)
    terms: list[tuple[str, Any]] = field(default_factory=list)
    log: list[tuple[str, str]] = field(default_factory=list)                # (action, key)


class MemStateStore:
    """PgStateStore semantics: version CAS with `decide()`, NOOP_ADVANCE advances the stored version."""

    def __init__(self, t: MemTables) -> None:
        self.t = t

    async def commit_entity(self, dto: Stateful) -> PgCommit:
        w = entity_write(dto)
        stored = self.t.entities.get(w.key)
        decision = Decision.APPLY if stored is None else decide(stored, dto)
        if decision in (Decision.APPLY, Decision.NOOP_ADVANCE):
            self.t.entities[w.key] = dto
            self.t.integrity.put(dto, wall_ms())
        self.t.log.append((decision.value, w.key))
        return PgCommit(decision, w)

    async def commit_cash_flow(self, cf: CashFlowDTO) -> bool:
        key = (cf.adapter_id, cf.transaction_id)
        if key in self.t.cash_flows:
            return False
        self.t.cash_flows[key] = cf
        return True

    async def commit_instruments(self, specs: Sequence[InstrumentSpec], seq_id: int) -> list[Any]:
        out: list[Any] = []
        for spec in specs:
            w = instrument_write(spec, seq_id)
            old = self.t.instruments.get((spec.adapter_id, spec.symbol))
            if old is not None and instrument_write(old, seq_id).checksum == w.checksum:
                out.append(None)
                continue
            self.t.instruments[(spec.adapter_id, spec.symbol)] = spec
            self.t.integrity.put(spec, wall_ms(), seq_id=seq_id)
            out.append(w)
        return out

    async def upsert_last_clean_mark(self, p: PriceDTO) -> None:
        cur = self.t.clean_marks.get((p.adapter_id, p.symbol))
        if cur is None or cur.timestamp_ms < p.timestamp_ms:
            self.t.clean_marks[(p.adapter_id, p.symbol)] = p


class MemStreamHealth:
    def __init__(self, t: MemTables) -> None:
        self.t = t

    async def mark_lost(self, adapter_id: str, stream: StreamKind) -> None:
        cur = self.t.stream_health.get((adapter_id, stream), (None, None, None))
        self.t.stream_health[(adapter_id, stream)] = (cur[0] or wall_ms(), cur[1], cur[2])

    async def mark_reconciled(self, adapter_id: str, stream: StreamKind) -> None:
        self.t.stream_health[(adapter_id, stream)] = (None, wall_ms(), self.t.epoch)

    async def heartbeat(self, adapter_id: str, stream: StreamKind) -> None:
        return None


class MemDlqSink:
    def __init__(self, t: MemTables) -> None:
        self.t = t

    async def insert_batch(self, rows: Sequence[DlqRow]) -> None:
        self.t.dlq.extend(rows)


class MemLocalState:
    def __init__(self, t: MemTables) -> None:
        self.t = t

    async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
        orders = [e for e in self.t.entities.values() if isinstance(e, OrderDTO) and e.adapter_id == adapter_id
                  and not e.status.terminal]
        positions = [e for e in self.t.entities.values() if isinstance(e, PositionDTO)
                     and e.adapter_id == adapter_id and e.status == "OPEN"]
        return orders, positions


class MemAuthStore:
    def __init__(self, t: MemTables) -> None:
        self.t = t

    async def known_bad(self, adapter_id: str, credential_generation: str) -> bool:
        return (adapter_id, credential_generation) in self.t.auth_failed

    async def record_auth_failed(self, adapter_id: str, credential_generation: str, detail: str) -> None:
        self.t.auth_failed.add((adapter_id, credential_generation))

    async def rearm(self, adapter_id: str, credential_generation: str, operator: str) -> bool:
        key = (adapter_id, credential_generation)
        if key in self.t.auth_failed:
            self.t.auth_failed.discard(key)
            return True
        return False


class MemOversee(InMemoryOverseeDb):
    """InMemoryOverseeDb whose snapshots are built from the committed entity tables plus the real Redis
    live keys (the same assembly as PgSnapshotReader)."""

    def __init__(self, epoch: int) -> None:
        super().__init__(engine_epoch=epoch)
        self.tables: MemTables | None = None
        self.redis: aioredis.Redis | None = None
        self.inputs = SnapshotInputs()

    def _accounts(self) -> dict[str, str]:
        assert self.tables is not None
        out: dict[str, str] = {}
        for e in self.tables.entities.values():
            if isinstance(e, AccountDTO) or (isinstance(e, PositionDTO) and e.status == "OPEN") \
                    or (isinstance(e, OrderDTO) and not e.status.terminal):
                out.setdefault(e.account_id, e.adapter_id)
        return out

    async def list_accounts(self) -> Sequence[str]:
        self.base = {k: v for k, v in self.base.items() if k in self._accounts()}
        for acct in self._accounts():
            self.base.setdefault(acct, AccountSnapshot(acct, "", 0, None))
        return await super().list_accounts()

    async def read_account(self, account_id: str, now_ms: int) -> AccountSnapshot:
        assert self.tables is not None and self.redis is not None
        t = self.tables
        adapter_id = self._accounts().get(account_id, "")
        account = next((e for e in t.entities.values() if isinstance(e, AccountDTO) and e.account_id == account_id),
                       None)
        orders = tuple(e for e in t.entities.values() if isinstance(e, OrderDTO) and e.account_id == account_id
                       and not e.status.terminal)
        positions = tuple(e for e in t.entities.values() if isinstance(e, PositionDTO)
                          and e.account_id == account_id and e.status == "OPEN")
        keys: set[SymbolKey] = set(t.instruments) | {(o.adapter_id, o.symbol) for o in orders}
        keys |= {(p.adapter_id, p.symbol) for p in positions}
        live_quotes = await read_quotes(self.redis, keys)
        quotes = {k: SymbolQuotes(latest=q.latest, clean_mid=TimedMid(*q.clean_mid) if q.clean_mid else None,
                                  session=self.inputs.session(*k)) for k, q in live_quotes.items()}
        live = await read_account_live(self.redis, account_id)
        self.base[account_id] = AccountSnapshot(
            account_id=account_id, adapter_id=adapter_id, read_at_ms=now_ms, account=account, orders=orders,
            positions=positions, instruments=MappingProxyType(dict(t.instruments)),
            quotes=MappingProxyType(quotes), account_live=live,
            integrity_incident=self.inputs.integrity_incident())
        return await super().read_account(account_id, now_ms)


class MemFencer:
    def __init__(self, t: MemTables) -> None:
        self.t = t

    async def bump_and_fence(self, epoch: Epoch, node_id: str, now_ms: int) -> FenceOutcome:
        self.t.terms.append(("started", (epoch.label, node_id, now_ms)))
        for k, v in list(self.t.stream_health.items()):
            self.t.stream_health[k] = (v[0] or now_ms, v[1], v[2])
        return FenceOutcome(False, None, len(self.t.stream_health), 0)


class MemSeq:
    def __init__(self, t: MemTables) -> None:
        self.t = t

    async def max_seq_id(self) -> int:
        return max((e.seq_id for e in self.t.entities.values()), default=0)


class MemTerms:
    def __init__(self, t: MemTables) -> None:
        self.t = t

    async def mark_reached_leader(self, epoch: Epoch, now_ms: int) -> None:
        self.t.terms.append(("reached_leader", epoch.label))

    async def record_end_reason(self, epoch: Epoch, reason: EndReason) -> None:
        self.t.terms.append(("end_reason", reason))


class MemSchedule:
    async def load(self) -> CalendarSchedule:
        return CalendarSchedule(1, {})


class MemSod:
    def __init__(self, t: MemTables) -> None:
        self.t = t

    async def missing_accounts(self, trading_day: date) -> Sequence[str]:
        have = {acct for (acct, day) in self.t.oversee.t.sod_marks if day == trading_day}
        return sorted({e.account_id for e in self.t.entities.values() if isinstance(e, AccountDTO)} - have)

    async def status(self, trading_day: date, accounts: Sequence[str]) -> SodStatus:
        rows = [m for (acct, day), m in self.t.oversee.t.sod_marks.items() if day == trading_day and acct in accounts]
        return SodStatus(len(rows), any(m.partial_session for m in rows))


class MemGateReader:
    """GateContextReader over the memory tables + the real Redis gate keys."""

    def __init__(self, t: MemTables, redis: aioredis.Redis, inputs: GateInputs, run_id: Any) -> None:
        self.t = t
        self.redis = redis
        self.inputs = inputs
        self.run_id = run_id

    async def read(self, action: Action) -> GateContext:
        keys = await read_gate_keys(self.redis, self.run_id)
        node_epoch = self.inputs.node_epoch()
        rows = [(s, *v) for (a, s), v in self.t.stream_health.items() if a == action.adapter_id]
        open_health, reconciled_at, reconciled_epoch = reconcile_state(rows, node_epoch)
        evs = self.t.oversee.t.halt_events
        halt = None
        if keys.halt_state is not None and keys.halt_state[0] in HaltLevel.__members__:
            halt = (HaltLevel[keys.halt_state[0]], keys.halt_state[1])
        view = self.inputs.sweep_view(action.account_id)
        return GateContext(
            now_ms=self.inputs.clock_ms(), redis_boot_id=keys.boot_id, redis_run_id=keys.run_id,
            node_epoch=node_epoch, engine_epoch=self.t.epoch, lease_valid=self.inputs.lease_valid(),
            halt_state=halt, max_halt_event_id=evs[-1].id if evs else None,
            settle_until_ms=self.inputs.settle_until_ms(), reconciled_at_ms=reconciled_at,
            reconciled_epoch=reconciled_epoch, latest_open_ms=self.inputs.latest_open_ms(action.adapter_id),
            open_stream_health=open_health,
            stale_mark_symbols=frozenset(s for _, s in view.stale_symbols) if view else frozenset(),
            has_unknown_result=view.has_unknown if view else True,
            last_pass_ms=view.last_pass_ms if view else None, sweep_interval_ms=self.inputs.sweep_interval_ms,
            catchup_verified=self.inputs.catchup_verified(action.adapter_id))


async def _noop() -> None:
    return None


class MemoryBackend:
    """One memory 'database' shared by the terms of a test; `ports(epoch)` is the PortsFactory."""

    def __init__(self, redis: aioredis.Redis, run_id_reader: Any, epoch_token: int) -> None:
        self.redis = redis
        self.run_id_reader = run_id_reader
        self.oversee = MemOversee(epoch_token)
        self.oversee.redis = redis
        self.tables = MemTables(epoch_token, self.oversee, MemIntegrityDb(self.oversee))
        self.oversee.tables = self.tables
        self.quarantine = RecordingQuarantine()
        self.dlq_store = FakeDlqStore(WallClock())  # type: ignore[arg-type]

    def ports(self, epoch: Epoch) -> LeaderPorts:
        t = self.tables

        def snapshot_reader(inputs: SnapshotInputs) -> MemOversee:
            self.oversee.inputs = inputs
            return self.oversee

        def gate_reader(inputs: GateInputs) -> MemGateReader:
            return MemGateReader(t, self.redis, inputs, self.run_id_reader)

        return LeaderPorts(
            fencer=MemFencer(t), seq=MemSeq(t), terms=MemTerms(t), state_store=MemStateStore(t),  # type: ignore[arg-type]
            stream_health=MemStreamHealth(t), dlq_sink=MemDlqSink(t), local_state=MemLocalState(t),  # type: ignore[arg-type]
            auth_store=MemAuthStore(t), snapshot_reader=snapshot_reader, sweep_writer=self.oversee,
            halt_store=self.oversee, outbox=self.oversee, dedupe=self.oversee, feed_quarantine=self.quarantine,
            integrity_db=t.integrity, dlq_store=self.dlq_store, schedule=MemSchedule(), gate_reader=gate_reader,
            sod=MemSod(t), close=_noop)

    # --- scenario views -----------------------------------------------------------------------------
    async def breaches(self) -> list[tuple[str, str, str]]:
        return [(s.rule_id, s.subject_id, s.level.name) for s in self.oversee.t.breaches.values()
                if s.resolved_at is None]

    async def halt_events(self) -> list[tuple[int, str]]:
        return [(e.id, e.effective_level.name) for e in self.oversee.t.halt_events]

    async def outbox(self) -> list[tuple[str, str]]:
        return [(r.kind, r.title) for r in self.oversee.t.outbox]
