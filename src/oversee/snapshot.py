"""AccountSnapshot: everything one RiskSweep pass needs for one account (oversee-rules.md §2 Inputs).

The PostgreSQL implementation of `SnapshotReader` (one READ ONLY REPEATABLE READ transaction per
account plus Redis reads with timestamps) belongs to src/db; this module defines the contract.
Redis is optional: without it the reader leaves `latest` / `clean_mid` / `account_live` empty and
sets `redis_ok=False`, so marks fall back to `last_clean_marks` (stale, so capped) and live fields
are UNKNOWN.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from types import MappingProxyType
from typing import Literal, Protocol

from src.dto import AccountDTO, AccountLive, CashFlowDTO, InstrumentSpec, OrderDTO, PositionDTO, PriceDTO
from src.oversee.types import BreachState, Cap, Level, SymbolKey

SessionState = Literal["OPEN", "ROLLOVER", "CLOSED"]   # SM10, per (adapter, symbol)
StopWatchKey = tuple[str, str]                         # (entity_type, entity_key)


@dataclass(frozen=True)
class TimedMid:
    mid: Decimal
    ts_ms: int


@dataclass(frozen=True)
class CleanMark:
    """A PostgreSQL `last_clean_marks(adapter_id, symbol, bid, ask, ts)` row."""

    bid: Decimal
    ask: Decimal
    ts_ms: int

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / 2


@dataclass(frozen=True)
class SymbolQuotes:
    """Price inputs for one (adapter_id, symbol) (§5 clean mid)."""

    latest: PriceDTO | None = None          # Redis price:{adapter_id}:{symbol}
    clean_mid: TimedMid | None = None       # Redis clean_mid:{adapter_id}:{symbol}
    last_clean: CleanMark | None = None     # PostgreSQL last_clean_marks (fallback)
    session: SessionState = "OPEN"


@dataclass(frozen=True)
class SodMark:
    """A `sod_marks` row (tier-persistence.md §4)."""

    account_id: str
    trading_day: date
    roll_ms: int
    balance: Decimal
    sod_equity: Decimal
    unexplained_increase_at_roll: Decimal
    partial_session: bool
    written_at_ms: int
    marks: Mapping[SymbolKey, TimedMid] = field(default_factory=lambda: MappingProxyType({}))
    partial_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class CashFlowRow:
    """A `cash_flows` row with its commit time (needed for pre-roll deposits explained after the roll)."""

    flow: CashFlowDTO
    committed_at_ms: int


@dataclass(frozen=True)
class StreamHealthRow:
    adapter_id: str
    stream: str                 # orders | positions | account | prices
    conn_lost_at_ms: int | None


@dataclass(frozen=True)
class BreachAck:
    """A `breach_acks` row (§4 acknowledge): silences re-pages until level, cap or reason change."""

    ack_id: int
    rule_id: str
    subject_type: str
    subject_id: str
    level: Level
    cap: Cap
    reason: str
    operator: str
    why: str
    at_ms: int


@dataclass(frozen=True)
class AccountSnapshot:
    """One account's consistent inputs for one pass."""

    account_id: str
    adapter_id: str
    read_at_ms: int
    account: AccountDTO | None
    orders: tuple[OrderDTO, ...] = ()
    positions: tuple[PositionDTO, ...] = ()
    instruments: Mapping[SymbolKey, InstrumentSpec] = field(default_factory=lambda: MappingProxyType({}))
    quotes: Mapping[SymbolKey, SymbolQuotes] = field(default_factory=lambda: MappingProxyType({}))
    account_live: AccountLive | None = None
    cash_flows: tuple[CashFlowRow, ...] = ()            # broker_ts >= roll, or committed after the roll
    sod: SodMark | None = None                          # the current trading day's row, if any
    prev_sod: SodMark | None = None                     # needed only to write a new sod row
    prev_day_cash_flows: tuple[CashFlowRow, ...] = ()   # flows of the previous trading day (roll only)
    stop_watch: Mapping[StopWatchKey, int] = field(default_factory=lambda: MappingProxyType({}))
    stream_health: tuple[StreamHealthRow, ...] = ()
    breaches: tuple[BreachState, ...] = ()
    acks: tuple[BreachAck, ...] = ()
    redis_ok: bool = True
    integrity_incident: bool = False                    # SM3 HALTED / SWITCHING_OVER / MANUAL / RECONCILING
    last_status_or_units_change_ms: int | None = None   # commit time, any order / position of the account
    balance_committed_at_ms: int | None = None          # commit time of the latest balance version
    pnl_source: bool = True                             # adapter reports TRADE_PNL
    transfer_source: bool = True                        # adapter has a transfer source
    scheduled_close_before_roll_ms: Mapping[SymbolKey, int] = field(
        default_factory=lambda: MappingProxyType({}))
    leaderless_before_roll: bool = False                # no term covered the 5 min before the roll itself
    # Leader terms [start, end or None] near the roll (only read when the day's sod row is missing). When given,
    # the leaderless test runs per held symbol from its own reference point (dayroll.build_sod_mark).
    leader_terms: tuple[tuple[int, int | None], ...] | None = None
    cash_flow_request_pending: bool = False             # set by the RiskSweep, not the reader


class DependencyUnavailable(Exception):
    """PostgreSQL unreachable for the whole pass: not an account fault (§2)."""


class SnapshotReader(Protocol):
    """READ ONLY REPEATABLE READ snapshot per account + Redis reads (§2). Writes nothing."""

    async def list_accounts(self) -> Sequence[str]: ...

    async def read_account(self, account_id: str, now_ms: int) -> AccountSnapshot:
        """Raises DependencyUnavailable when PostgreSQL is down; any other error is an account fault."""
        ...
