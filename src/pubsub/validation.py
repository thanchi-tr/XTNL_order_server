"""Schema and checksum validation of the parkable DTOs (dto-contracts.md §4, §5; SKILL.md §3).

A row on the order, position or account topic that fails either check is parked in events_dlq through
`DlqSpooler.offer()` with reason `schema_invalid` / `checksum` and never published: the DLQ worker
re-reads the entity from the broker (check-before-retry, tier-persistence.md §5).

Pure functions, no I/O: they run on the parser path. The DTO constructors already reject some shapes
(units missing while the instrument is known); these checks cover what a constructor cannot: an empty id,
a non-finite or wrong-typed amount, an unknown side / status / order_kind / kind, a row whose adapter_id is not the
adapter that produced it, and a checksum that no longer matches the payload (a row mutated after it was
sealed).
"""
from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal

from src.dto import ORDER_KINDS, AccountDTO, CashFlowDTO, OrderDTO, OrderStatus, PositionDTO, payload_checksum

Checked = OrderDTO | PositionDTO | AccountDTO | CashFlowDTO

_SIDES = frozenset({"BUY", "SELL"})
_POSITION_STATUSES = frozenset({"OPEN", "CLOSED"})
_CASH_FLOW_KINDS = frozenset({"DEPOSIT", "WITHDRAWAL", "TRADE_PNL", "FINANCING", "COMMISSION", "ADJUSTMENT"})


def _blank(value: object) -> bool:
    return not isinstance(value, str) or not value.strip()


def _bad_amount(value: object, *, optional: bool, non_negative: bool = False) -> bool:
    if value is None:
        return not optional
    if isinstance(value, bool) or not isinstance(value, Decimal) or not value.is_finite():
        return True
    return non_negative and value < 0


def _bad_int(value: object, *, optional: bool) -> bool:
    if value is None:
        return not optional
    return isinstance(value, bool) or not isinstance(value, int) or value < 0


def _first(problems: Iterable[tuple[bool, str]]) -> str | None:
    for bad, what in problems:
        if bad:
            return what
    return None


def schema_problem(dto: Checked, adapter_id: str) -> str | None:
    """None when the row is well formed, else a short description of the first problem found."""
    if dto.adapter_id != adapter_id:
        return f"adapter_id {dto.adapter_id!r} is not {adapter_id!r}"
    if isinstance(dto, OrderDTO):
        return _first((
            (_blank(dto.broker_order_id), "broker_order_id empty"),
            (_blank(dto.account_id), "account_id empty"),
            (_blank(dto.symbol), "symbol empty"),
            (dto.side not in _SIDES, "side invalid"),
            (not isinstance(dto.status, OrderStatus), "status invalid"),
            (dto.order_kind is not None and dto.order_kind not in ORDER_KINDS, "order_kind invalid"),
            (_bad_amount(dto.entry_price, optional=True), "entry_price invalid"),
            (_bad_amount(dto.stop_loss, optional=True), "stop_loss invalid"),
            (_bad_amount(dto.take_profit, optional=True), "take_profit invalid"),
            (_bad_amount(dto.ordered_units, optional=True, non_negative=True), "ordered_units invalid"),
            (_bad_amount(dto.remaining_units, optional=True, non_negative=True), "remaining_units invalid"),
            (_bad_amount(dto.raw_qty, optional=True), "raw_qty invalid"),
            (_bad_int(dto.broker_updated_at_ms, optional=False), "broker_updated_at_ms invalid"),
            (_bad_int(dto.broker_seq, optional=True), "broker_seq invalid"),
        ))
    if isinstance(dto, PositionDTO):
        return _first((
            (_blank(dto.position_id), "position_id empty"),
            (_blank(dto.account_id), "account_id empty"),
            (_blank(dto.symbol), "symbol empty"),
            (dto.side not in _SIDES, "side invalid"),
            (dto.status not in _POSITION_STATUSES, "status invalid"),
            (_bad_amount(dto.units, optional=True, non_negative=True), "units invalid"),
            (_bad_amount(dto.entry_price, optional=True), "entry_price invalid"),
            (_bad_amount(dto.stop_loss, optional=True), "stop_loss invalid"),
            (_bad_amount(dto.take_profit, optional=True), "take_profit invalid"),
            (_bad_amount(dto.raw_qty, optional=True), "raw_qty invalid"),
            (_bad_amount(dto.accrued_swap, optional=False), "accrued_swap invalid"),
            (_bad_amount(dto.commission, optional=False), "commission invalid"),
            (_bad_int(dto.broker_updated_at_ms, optional=False), "broker_updated_at_ms invalid"),
            (_bad_int(dto.broker_seq, optional=True), "broker_seq invalid"),
        ))
    if isinstance(dto, AccountDTO):
        return _first((
            (_blank(dto.account_id), "account_id empty"),
            (_blank(dto.currency), "currency empty"),
            (_bad_amount(dto.balance, optional=False), "balance invalid"),
            (_bad_amount(dto.realised_pnl_today, optional=True), "realised_pnl_today invalid"),
            (_bad_int(dto.broker_updated_at_ms, optional=False), "broker_updated_at_ms invalid"),
            (_bad_int(dto.broker_seq, optional=True), "broker_seq invalid"),
        ))
    return _first((
        (_blank(dto.transaction_id), "transaction_id empty"),
        (_blank(dto.account_id), "account_id empty"),
        (_blank(dto.currency), "currency empty"),
        (dto.kind not in _CASH_FLOW_KINDS, "kind invalid"),
        (_bad_amount(dto.amount, optional=False), "amount invalid"),
    ))


def checksum_valid(dto: Checked) -> bool:
    """The sealed checksum still matches the payload fields (a constructor or `replace()` re-seals it)."""
    try:
        return bool(dto.checksum) and payload_checksum(dto) == dto.checksum
    except (TypeError, ValueError):
        return False


__all__ = ["Checked", "checksum_valid", "schema_problem"]
