"""In-memory fakes for the src/integrity protocols (IntegrityDb, DlqStore, DlqBroker, IngestionControl,
BrokerReconciler, PatroniControl, BootSignal). No network, no PostgreSQL."""
from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import replace
from decimal import Decimal
from typing import Any, Literal

from src.adapters.base import note_absent
from src.cache.digest import fold
from src.dto import AccountDTO, AccountLive, CashFlowDTO, InstrumentSpec, OrderDTO, OrderStatus, PositionDTO
from src.integrity.ports import (
    BootState,
    CleanMark,
    CommittedRow,
    DbHighWater,
    DlqRecord,
    DlqStatus,
    HaltLevelName,
    HaltRow,
    IntegrityAlert,
    StatefulDTO,
    committed_row,
)
from src.ports import DlqEventType

T0 = 1_700_000_000_000


class ManualClock:
    def __init__(self, ms: int = T0) -> None:
        self.ms = ms

    def __call__(self) -> int:
        return self.ms

    def advance(self, ms: int) -> None:
        self.ms += ms


class Pages:
    def __init__(self) -> None:
        self.alerts: list[IntegrityAlert] = []

    def __call__(self, alert: IntegrityAlert) -> None:
        self.alerts.append(alert)

    def codes(self) -> list[str]:
        return [a.code for a in self.alerts]


def order(oid: str = "B1", *, t: int = T0, observed: int = 100, seq: int = 1000, status: OrderStatus = OrderStatus.WORKING,
          stop: str | None = "1.0950", adapter_id: str = "stonex", account_id: str = "ACC") -> OrderDTO:
    return OrderDTO(client_order_id=f"c-{oid}", broker_order_id=oid, account_id=account_id, adapter_id=adapter_id,
                    symbol="EURUSD", side="BUY", status=status, entry_price=Decimal("1.1000"),
                    stop_loss=None if stop is None else Decimal(stop), ordered_units=Decimal("10000"),
                    remaining_units=Decimal("10000"), broker_updated_at_ms=t, broker_seq=None,
                    observed_seq=observed, seq_id=seq)


def position(pid: str = "P1", *, t: int = T0, observed: int = 100, seq: int = 1000,
             adapter_id: str = "stonex") -> PositionDTO:
    return PositionDTO(position_id=pid, account_id="ACC", adapter_id=adapter_id, symbol="EURUSD", side="BUY",
                       units=Decimal("10000"), entry_price=Decimal("1.1"), status="OPEN",
                       broker_updated_at_ms=t, observed_seq=observed, seq_id=seq)


def account(aid: str = "ACC", *, balance: str = "100000", t: int = T0, observed: int = 100,
            seq: int = 1000) -> AccountDTO:
    return AccountDTO(account_id=aid, adapter_id="stonex", currency="USD", balance=Decimal(balance),
                      broker_updated_at_ms=t, observed_seq=observed, seq_id=seq)


def cash_flow(tid: str = "T1", *, ts: int = T0) -> CashFlowDTO:
    return CashFlowDTO(transaction_id=tid, account_id="ACC", adapter_id="stonex", kind="DEPOSIT",
                       amount=Decimal("100"), currency="USD", broker_ts=ts)


# ---- IntegrityDb ---------------------------------------------------------------------------------

class FakeIntegrityDb:
    def __init__(self) -> None:
        self.rows: dict[str, CommittedRow] = {}
        self.halts: list[HaltRow] = []
        self.marks: list[CleanMark] = []
        self.lives: list[AccountLive] = []
        self.hw = DbHighWater(timeline=1, epoch=7, max_seq_id=0)
        self.fail: dict[str, int] = {}
        self.calls: list[str] = []

    # helpers
    def put(self, item: StatefulDTO | InstrumentSpec, committed_at_ms: int = T0, *, seq_id: int | None = None) -> CommittedRow:
        row = committed_row(item, committed_at_ms, seq_id=seq_id)
        self.rows[row.key] = row
        self.hw = replace(self.hw, max_seq_id=max(self.hw.max_seq_id, row.seq_id))
        return row

    def corrupt(self, key: str, **changes: Any) -> CommittedRow:
        row = replace(self.rows[key], **changes)
        self.rows[key] = row
        return row

    def halt(self, hid: int, level: HaltLevelName) -> None:
        self.halts.append(HaltRow(hid, level))

    def _enter(self, name: str) -> None:
        self.calls.append(name)
        left = self.fail.get(name, 0)
        if left:
            self.fail[name] = left - 1
            raise ConnectionError(f"{name}: postgres down")

    # protocol
    async def committed_rows(self, from_ms: int, to_ms: int, limit: int) -> Sequence[CommittedRow]:
        self._enter("committed_rows")
        rows = sorted((r for r in self.rows.values() if from_ms <= r.committed_at_ms <= to_ms),
                      key=lambda r: (r.committed_at_ms, r.key))
        return rows[:limit]

    async def rows_by_keys(self, keys: Collection[str]) -> Mapping[str, CommittedRow]:
        self._enter("rows_by_keys")
        return {k: self.rows[k] for k in keys if k in self.rows}

    async def rows_after_key(self, after_key: str, limit: int) -> Sequence[CommittedRow]:
        self._enter("rows_after_key")
        return [self.rows[k] for k in sorted(self.rows) if k > after_key][:limit]

    async def digest(self) -> str:
        self._enter("digest")
        return fold(r.digest_hash for r in self.rows.values())

    async def latest_commit_ms(self) -> int | None:
        self._enter("latest_commit_ms")
        return max((r.committed_at_ms for r in self.rows.values()), default=None)

    async def latest_halt_event(self) -> HaltRow | None:
        self._enter("latest_halt_event")
        return max(self.halts, key=lambda h: h.id, default=None)

    async def high_water(self) -> DbHighWater:
        self._enter("high_water")
        return self.hw

    async def last_clean_marks(self) -> Sequence[CleanMark]:
        self._enter("last_clean_marks")
        return list(self.marks)

    async def latest_live_samples(self) -> Sequence[AccountLive]:
        self._enter("latest_live_samples")
        return list(self.lives)


# ---- DlqStore ------------------------------------------------------------------------------------

class FakeDlqStore:
    def __init__(self, clock: ManualClock) -> None:
        self.clock = clock
        self.rows: dict[int, DlqRecord] = {}
        self.quarantined: list[tuple[int, str]] = []
        self.unverified: list[tuple[str, str]] = []
        self.cash_since: int | None = None
        self._next = 1

    def park(self, event_type: DlqEventType, entity_id: str, reason: str, *, adapter_id: str = "stonex",
             payload: str = "{}", parked_at_ms: int | None = None, retry_count: int = 0) -> DlqRecord:
        dlq_id = self._next
        self._next += 1
        now = self.clock()
        rec = DlqRecord(dlq_id=dlq_id, event_type=event_type, entity_id=entity_id, adapter_id=adapter_id,
                        raw_payload=payload, payload_hash=f"h{dlq_id}", error_reason=reason, retry_count=retry_count,
                        status=DlqStatus.QUEUED, next_retry_at_ms=now,
                        parked_at_ms=now if parked_at_ms is None else parked_at_ms, updated_at_ms=now)
        self.rows[dlq_id] = rec
        return rec

    def _upd(self, dlq_id: int, **kw: Any) -> None:
        self.rows[dlq_id] = replace(self.rows[dlq_id], updated_at_ms=self.clock(), **kw)

    def _owned(self, dlq_id: int, owner: str) -> bool:
        r = self.rows.get(dlq_id)
        return r is not None and r.status is DlqStatus.LEASED and r.lease_owner == owner

    async def lease(self, owner: str, now_ms: int, lease_ms: int, adapters: Collection[str],
                    limit: int) -> Sequence[DlqRecord]:
        due = sorted((r for r in self.rows.values()
                      if r.status in (DlqStatus.QUEUED, DlqStatus.BACKOFF) and r.next_retry_at_ms <= now_ms
                      and r.adapter_id in adapters), key=lambda r: (r.next_retry_at_ms, r.dlq_id))[:limit]
        for r in due:
            self._upd(r.dlq_id, status=DlqStatus.LEASED, lease_owner=owner, lease_until_ms=now_ms + lease_ms)
        return [self.rows[r.dlq_id] for r in due]

    async def release_expired(self, now_ms: int) -> int:
        n = 0
        for r in list(self.rows.values()):
            if r.status is DlqStatus.LEASED and r.lease_until_ms is not None and r.lease_until_ms < now_ms:
                self._upd(r.dlq_id, status=DlqStatus.QUEUED, lease_owner=None, lease_until_ms=None)
                n += 1
        return n

    async def requeue(self, dlq_id: int, owner: str) -> bool:
        if not self._owned(dlq_id, owner):
            return False
        self._upd(dlq_id, status=DlqStatus.QUEUED, lease_owner=None, lease_until_ms=None)
        return True

    async def resolve(self, dlq_id: int, owner: str) -> bool:
        if not self._owned(dlq_id, owner):
            return False
        self._upd(dlq_id, status=DlqStatus.RESOLVED, lease_owner=None, lease_until_ms=None)
        return True

    async def backoff(self, dlq_id: int, owner: str, retry_count: int, next_retry_at_ms: int,
                      last_error: str) -> bool:
        if not self._owned(dlq_id, owner):
            return False
        self._upd(dlq_id, status=DlqStatus.BACKOFF, retry_count=retry_count, next_retry_at_ms=next_retry_at_ms,
                  lease_owner=None, lease_until_ms=None)
        return True

    async def quarantine(self, dlq_id: int, owner: str | None, reason: str) -> bool:
        if owner is not None and not self._owned(dlq_id, owner):
            return False
        self._upd(dlq_id, status=DlqStatus.QUARANTINED, lease_owner=None, lease_until_ms=None)
        self.quarantined.append((dlq_id, reason))
        return True

    async def find_lineage_origin(self, event_type: DlqEventType, entity_id: str, error_reason: str,
                                  before_dlq_id: int, since_ms: int, *, adapter_id: str) -> DlqRecord | None:
        cands = [r for r in self.rows.values()
                 if r.adapter_id == adapter_id and r.event_type == event_type and r.entity_id == entity_id and r.error_reason == error_reason
                 and r.status is DlqStatus.RESOLVED and r.origin_dlq_id is None and r.dlq_id < before_dlq_id
                 and r.updated_at_ms >= since_ms]
        return max(cands, key=lambda r: (r.updated_at_ms, r.dlq_id), default=None)

    async def merge_into_origin(self, dlq_id: int, owner: str, origin_dlq_id: int, retry_count: int,
                                next_retry_at_ms: int) -> bool:
        if not self._owned(dlq_id, owner):
            return False
        child, origin = self.rows[dlq_id], self.rows[origin_dlq_id]
        if (child.adapter_id, child.event_type, child.entity_id) != \
                (origin.adapter_id, origin.event_type, origin.entity_id):
            return False
        self._upd(dlq_id, status=DlqStatus.RESOLVED, origin_dlq_id=origin_dlq_id, lease_owner=None,
                  lease_until_ms=None)
        self._upd(origin_dlq_id, status=DlqStatus.BACKOFF, retry_count=retry_count,
                  next_retry_at_ms=next_retry_at_ms)
        return True

    async def cash_flow_since_ms(self, adapter_id: str) -> int | None:
        return self.cash_since

    async def mark_transfers_unverified(self, adapter_id: str, account_id: str, at_ms: int) -> None:
        self.unverified.append((adapter_id, account_id))


class FakeDlqBroker:
    def __init__(self) -> None:
        self.streaming: set[str] = {"stonex"}
        self.entities: dict[str, StatefulDTO] = {}
        self.flows: dict[str, CashFlowDTO] = {}
        self.errors: list[BaseException] = []
        self.calls: list[tuple[str, str, Any]] = []
        self.on_read: Any = None
        self.absent: set[str] = set()      # ids the broker explicitly reports "not found" (note_absent)

    def streaming_adapters(self) -> Collection[str]:
        return frozenset(self.streaming)

    def _row(self, event_type: Literal["ORDER", "POSITION", "ACCOUNT"], entity_id: str) -> StatefulDTO | None:
        found = self.entities.get(entity_id)
        if found is None and entity_id in self.absent:
            note_absent(event_type, entity_id)
        return found

    def _maybe_fail(self) -> None:
        if self.on_read is not None:
            self.on_read()
        if self.errors:
            raise self.errors.pop(0)

    async def reread_entity(self, adapter_id: str, event_type: Literal["ORDER", "POSITION", "ACCOUNT"],
                            entity_id: str) -> StatefulDTO | None:
        self.calls.append(("reread", event_type, entity_id))
        self._maybe_fail()
        return self._row(event_type, entity_id)

    async def read_entity(self, adapter_id: str, event_type: Literal["ORDER", "POSITION", "ACCOUNT"],
                          entity_id: str) -> StatefulDTO | None:
        self.calls.append(("read", event_type, entity_id))
        self._maybe_fail()
        return self._row(event_type, entity_id)

    async def reread_cash_flow(self, adapter_id: str, transaction_id: str, since_ms: int) -> CashFlowDTO | None:
        self.calls.append(("cash_flow", transaction_id, since_ms))
        self._maybe_fail()
        return self.flows.get(transaction_id)


# ---- incident collaborators -----------------------------------------------------------------------

class FakeIngestion:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def halt_ingestion(self, reason: str) -> None:
        self.calls.append("halt")

    async def resume_prices(self) -> None:
        self.calls.append("resume_prices")

    async def resume_all(self) -> None:
        self.calls.append("resume_all")


class FakeReconciler:
    def __init__(self) -> None:
        self.results: list[bool | BaseException] = []
        self.calls: list[int | None] = []

    async def reconcile_all(self, cash_flows_since_ms: int | None) -> bool:
        self.calls.append(cash_flows_since_ms)
        if self.results:
            r = self.results.pop(0)
            if isinstance(r, BaseException):
                raise r
            return r
        return True


class FakePatroni:
    def __init__(self) -> None:
        self.primary = "pg-a"
        self.standby_ok = True
        self.check_error: BaseException | None = None
        self.switchover_error: BaseException | None = None
        self.reinit_error: BaseException | None = None
        self.calls: list[str] = []

    async def primary_member(self) -> str:
        self.calls.append("primary")
        if self.check_error is not None:
            raise self.check_error
        return self.primary

    async def standby_passes_check(self) -> bool:
        self.calls.append("standby_check")
        return self.standby_ok

    async def switchover(self) -> str:
        self.calls.append("switchover")
        if self.switchover_error is not None:
            raise self.switchover_error
        self.primary = "pg-b"
        return "pg-b"

    async def reinit(self, member: str) -> None:
        self.calls.append(f"reinit:{member}")
        if self.reinit_error is not None:
            raise self.reinit_error


class FakeBoot:
    def __init__(self) -> None:
        self.signals: list[str] = []
        self._state = BootState.READY

    def signal(self, reason: str) -> None:
        self.signals.append(reason)

    @property
    def state(self) -> BootState:
        return self._state
