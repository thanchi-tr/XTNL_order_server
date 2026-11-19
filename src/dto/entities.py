"""Canonical, immutable DTOs (dto-contracts.md §3). Money and quantities are Decimal.

`checksum` is derived from the broker payload fields at construction and excludes the
version fields and seq_id, so re-labelling (dataclasses.replace) never changes it. The optional
payload fields added for the ARM prerequisites (`order_kind`, the InstrumentSpec metadata) count
only when set, and the version labels `origin` / `broker_time_source` never count
(src/dto/versioning.py OPTIONAL_PAYLOAD_FIELDS, NON_PAYLOAD_FIELDS); every producer leaves them None
until its stage sets them.
Adapters leave `observed_seq` and `seq_id` at 0; the ingestion orchestrator stamps them.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Literal

from src.dto.versioning import BrokerTimeSource, Origin, Source, payload_checksum

Side = Literal["BUY", "SELL"]
PositionStatus = Literal["OPEN", "CLOSED"]
CashFlowKind = Literal["DEPOSIT", "WITHDRAWAL", "TRADE_PNL", "FINANCING", "COMMISSION", "ADJUSTMENT"]
UnitKind = Literal["UNITS", "CENTI_UNITS", "LOTS", "CONTRACTS", "STAKE"]
#: Broker order type of a child / working order (design PR1): a STOP counts as a stop at any price, a LIMIT
#: never does; None = not known (today's rule applies).
OrderKind = Literal["STOP", "LIMIT"]
ORDER_KINDS: frozenset[str] = frozenset({"STOP", "LIMIT"})
#: Where PriceDTO.timestamp_ms came from (design PR2): the broker's own tick time or the local receipt time.
TsSource = Literal["BROKER", "RECEIPT"]
TRANSFER_KINDS: frozenset[str] = frozenset({"DEPOSIT", "WITHDRAWAL"})


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"

    @property
    def terminal(self) -> bool:
        return self in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED)


# SM1 legal transitions (self-loops are amends; PENDING -> PARTIALLY_FILLED is an implied ack).
_LEGAL: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.PENDING: frozenset({OrderStatus.PENDING, OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED,
                                    OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}),
    OrderStatus.WORKING: frozenset({OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED,
                                    OrderStatus.CANCELLED, OrderStatus.REJECTED}),
    OrderStatus.PARTIALLY_FILLED: frozenset({OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED,
                                             OrderStatus.CANCELLED}),
    OrderStatus.FILLED: frozenset({OrderStatus.FILLED}),
    OrderStatus.CANCELLED: frozenset({OrderStatus.CANCELLED}),
    OrderStatus.REJECTED: frozenset({OrderStatus.REJECTED}),
}


def is_legal_transition(old: OrderStatus | None, new: OrderStatus) -> bool:
    """A first observation (old is None) is never a violation."""
    return old is None or new in _LEGAL[old]


def _seal(obj: object) -> None:
    object.__setattr__(obj, "checksum", payload_checksum(obj))


@dataclass(frozen=True, kw_only=True)
class OrderDTO:
    client_order_id: str | None
    broker_order_id: str
    account_id: str
    adapter_id: str
    symbol: str
    side: Side
    status: OrderStatus
    entry_price: Decimal | None
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    ordered_units: Decimal | None = None
    remaining_units: Decimal | None = None
    instrument_unknown: bool = False
    raw_qty: Decimal | None = None
    is_reduce_only: bool = False
    linked_position_id: str | None = None
    order_kind: OrderKind | None = None   # PR1; optional payload field (checksum-neutral while None)
    broker_updated_at_ms: int = 0
    broker_seq: int | None = None
    source: Source = "STREAM"
    origin: Origin | None = None                         # PR8 version label, non-payload
    broker_time_source: BrokerTimeSource | None = None   # PR8 version label, non-payload
    observed_seq: int = 0
    seq_id: int = 0
    checksum: str = field(init=False, default="", compare=False)

    def __post_init__(self) -> None:
        if not self.instrument_unknown and (self.ordered_units is None or self.remaining_units is None):
            raise ValueError("units are required unless instrument_unknown")
        _seal(self)

    @property
    def entity_key(self) -> str:
        return f"{self.adapter_id}:{self.broker_order_id}"


@dataclass(frozen=True, kw_only=True)
class PositionDTO:
    position_id: str
    account_id: str
    adapter_id: str
    symbol: str
    side: Side
    units: Decimal | None
    entry_price: Decimal | None
    status: PositionStatus
    instrument_unknown: bool = False
    raw_qty: Decimal | None = None
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    opened_by_client_order_id: str | None = None
    accrued_swap: Decimal = Decimal(0)
    commission: Decimal = Decimal(0)
    broker_updated_at_ms: int = 0
    broker_seq: int | None = None
    source: Source = "STREAM"
    origin: Origin | None = None                         # PR8 version label, non-payload
    broker_time_source: BrokerTimeSource | None = None   # PR8 version label, non-payload
    observed_seq: int = 0
    seq_id: int = 0
    checksum: str = field(init=False, default="", compare=False)

    def __post_init__(self) -> None:
        if not self.instrument_unknown and self.units is None:
            raise ValueError("units are required unless instrument_unknown")
        _seal(self)

    @property
    def entity_key(self) -> str:
        return f"{self.adapter_id}:{self.position_id}"


@dataclass(frozen=True, kw_only=True)
class AccountDTO:
    """Versioned account state (balance, realised). Live fields go to AccountLive."""

    account_id: str
    adapter_id: str
    currency: str
    balance: Decimal
    realised_pnl_today: Decimal | None = None
    broker_updated_at_ms: int = 0
    broker_seq: int | None = None
    source: Source = "STREAM"
    origin: Origin | None = None                         # PR8 version label, non-payload
    broker_time_source: BrokerTimeSource | None = None   # PR8 version label, non-payload
    observed_seq: int = 0
    seq_id: int = 0
    checksum: str = field(init=False, default="", compare=False)

    def __post_init__(self) -> None:
        _seal(self)

    @property
    def entity_key(self) -> str:
        return f"{self.adapter_id}:{self.account_id}"


@dataclass(frozen=True, kw_only=True)
class AccountLive:
    """Unversioned live mark-to-market fields (Redis account_live:{id}, latest value wins)."""

    account_id: str
    adapter_id: str
    currency: str
    equity: Decimal | None
    margin_used: Decimal | None
    free_margin: Decimal | None
    unrealised_pnl: Decimal | None
    timestamp_ms: int
    sampled: bool = False


@dataclass(frozen=True, kw_only=True)
class CashFlowDTO:
    transaction_id: str
    account_id: str
    adapter_id: str
    kind: CashFlowKind
    amount: Decimal
    currency: str
    broker_ts: int
    seq_id: int = 0
    checksum: str = field(init=False, default="", compare=False)

    def __post_init__(self) -> None:
        _seal(self)

    @property
    def is_transfer(self) -> bool:
        return self.kind in TRANSFER_KINDS


@dataclass(frozen=True, kw_only=True)
class PriceDTO:
    adapter_id: str
    symbol: str
    bid: Decimal
    ask: Decimal
    spread_pips: Decimal | None
    timestamp_ms: int
    seq_id: int = 0
    broker_market_id: str | None = None
    tradeable: bool = True
    ts_source: TsSource | None = None   # PR2: what timestamp_ms is; None = not stated by the producer
    received_ms: int | None = None      # PR2: local receipt time on the stream thread

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / 2


@dataclass(frozen=True, kw_only=True)
class InstrumentSpec:
    adapter_id: str
    symbol: str
    base_ccy: str
    quote_ccy: str
    pnl_ccy: str
    contract_size: Decimal
    unit_kind: UnitKind
    point_size: Decimal
    min_qty: Decimal
    swap_booked_to_balance: bool
    broker_symbol: str = ""   # broker-native id, e.g. the StoneX MarketId
    # PR7 instrument metadata (not market data); optional payload fields, checksum-neutral while None. Never
    # shown in a notice, page or log line.
    price_increment: Decimal | None = None
    min_stop_distance: Decimal | None = None
    asset_class: str | None = None

    def to_notional(self, broker_qty: Decimal) -> Decimal:
        """Convert a broker quantity into canonical units of the base currency."""
        if self.unit_kind == "UNITS" or self.unit_kind == "STAKE":
            return broker_qty
        if self.unit_kind == "CENTI_UNITS":
            return broker_qty / 100
        return broker_qty * self.contract_size  # LOTS, CONTRACTS


Entity = OrderDTO | PositionDTO | AccountDTO
