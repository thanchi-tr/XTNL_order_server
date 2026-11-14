"""Row <-> value mapping shared by the PostgreSQL stores (tier-persistence.md §4, oversee-rules.md §2).

SELECT column lists live next to their decoders so positions cannot drift. Oversee vocabulary is mapped
onto the 0001 schema's CHECK constraints:
* subject_type / stop_watch entity_type: lower-case in Oversee ('account', 'order'), upper-case in SQL.
* rule level: Level.PASS <-> NULL, FLAG <-> 'FLAG', SOFT_HALT <-> 'SOFT', HARD_HALT <-> 'HARD'.
* halt level: HaltLevel name ('NONE', 'SOFT', 'HARD').
* alert severity: 'info' / 'high' / 'critical' <-> upper case.
"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Final, cast

from src.db.pg import Row, json_loads
from src.dto import AccountDTO, CashFlowDTO, InstrumentSpec, OrderDTO, OrderStatus, PositionDTO
from src.oversee.halt import HaltEvent
from src.oversee.notify import EpisodeKey, OutboxRow
from src.oversee.snapshot import BreachAck, CashFlowRow, SodMark, StreamHealthRow, TimedMid
from src.oversee.types import BreachState, Cap, HaltLevel, Level, ResolvedBy, SubjectType, SymbolKey

# ---- vocabulary --------------------------------------------------------------------------------------

_LEVEL_TO_DB: Final[dict[Level, str | None]] = {
    Level.PASS: None, Level.FLAG: "FLAG", Level.SOFT_HALT: "SOFT", Level.HARD_HALT: "HARD"}
_LEVEL_FROM_DB: Final[dict[str | None, Level]] = {v: k for k, v in _LEVEL_TO_DB.items()}


def level_to_db(level: Level | None) -> str | None:
    return None if level is None else _LEVEL_TO_DB[level]


def level_from_db(value: str | None) -> Level:
    return _LEVEL_FROM_DB[value]


def opt_level_from_db(value: str | None) -> Level | None:
    return None if value is None else _LEVEL_FROM_DB[value]


def subject_to_db(subject_type: str) -> str:
    return subject_type.upper()


def subject_from_db(value: str) -> SubjectType:
    v = value.lower()
    if v not in ("account", "account_day", "order", "position"):
        raise ValueError(f"unknown subject_type {value!r}")
    return cast(SubjectType, v)


def _opt_int(v: object) -> int | None:
    return None if v is None else int(cast(int, v))


def _dec(v: object) -> Decimal:
    return v if isinstance(v, Decimal) else Decimal(str(v))


to_decimal = _dec


def _opt_dec(v: object) -> Decimal | None:
    return None if v is None else _dec(v)


def _json(v: object) -> Any:
    """jsonb arrives decoded (pool codec) or as text (no codec, e.g. a test fake)."""
    if isinstance(v, (str, bytes)):
        return json_loads(v)
    return v


# ---- halt_breaches -----------------------------------------------------------------------------------

BREACH_COLS: Final = (
    "rule_id, subject_type, subject_id, account_id, level, cap, reason, pending_level, pending_since, "
    "active_since, activation_id, release_pending_since, unknown_since, unknown_origin, uncapped_ms, "
    "last_pass_at_ms, below_soft_since, resolved_at, resolved_by, last_sweep_id, clear_generation"
)


def breach_from_row(r: Row) -> BreachState:
    return BreachState(
        rule_id=str(r[0]), subject_type=subject_from_db(str(r[1])), subject_id=str(r[2]), account_id=str(r[3]),
        level=level_from_db(r[4]), cap=Cap(str(r[5])), reason=str(r[6] or ""),
        pending_level=opt_level_from_db(r[7]), pending_since=_opt_int(r[8]), active_since=_opt_int(r[9]),
        activation_id=_opt_int(r[10]), release_pending_since=_opt_int(r[11]), unknown_since=_opt_int(r[12]),
        unknown_origin=bool(r[13]), uncapped_ms=int(r[14] or 0), last_pass_ms=_opt_int(r[15]),
        below_soft_since=_opt_int(r[16]), resolved_at=_opt_int(r[17]),
        resolved_by=cast(ResolvedBy | None, r[18]), last_sweep_id=int(r[19] or 0), clear_generation=int(r[20] or 0),
    )


def breach_params(s: BreachState) -> tuple[object, ...]:
    """Values in BREACH_COLS order (the writers' $2..$22)."""
    return (s.rule_id, subject_to_db(s.subject_type), s.subject_id, s.account_id, level_to_db(s.level),
            s.cap.value, s.reason, level_to_db(s.pending_level), s.pending_since, s.active_since, s.activation_id,
            s.release_pending_since, s.unknown_since, s.unknown_origin, s.uncapped_ms, s.last_pass_ms,
            s.below_soft_since, s.resolved_at, s.resolved_by, s.last_sweep_id, s.clear_generation)


ACK_COLS: Final = (
    "k.ack_id, b.rule_id, b.subject_type, b.subject_id, k.level, k.cap, k.reason, k.operator, k.why, k.acked_at_ms"
)


def ack_from_row(r: Row) -> BreachAck:
    return BreachAck(int(r[0]), str(r[1]), subject_from_db(str(r[2])), str(r[3]), level_from_db(r[4]),
                     Cap(str(r[5])), str(r[6]), str(r[7]), str(r[8]), int(r[9]))


# ---- halt_events -------------------------------------------------------------------------------------

HALT_EVENT_COLS: Final = 'id, level, effective_level, hard_latch, rule_id, reason, "by", at_ms'


def halt_event_from_row(r: Row) -> HaltEvent:
    return HaltEvent(int(r[0]), HaltLevel[str(r[1])], HaltLevel[str(r[2])], bool(r[3]),
                     None if r[4] is None else str(r[4]), str(r[5]), str(r[6]), int(r[7]))


# ---- notification_outbox / dedupe --------------------------------------------------------------------

OUTBOX_COLS: Final = "outbox_id, kind, severity, subject, body, created_at_ms, episode, account_id"


def episode_text(key: EpisodeKey | None) -> str | None:
    return None if key is None else json.dumps(list(key), separators=(",", ":"))


def episode_from(value: object) -> EpisodeKey | None:
    if value is None:
        return None
    v = _json(value)
    return (str(v[0]), str(v[1]), str(v[2]), int(v[3]), int(v[4]))


def outbox_from_row(r: Row) -> OutboxRow:
    return OutboxRow(kind=cast(Any, str(r[1])), severity=cast(Any, str(r[2]).lower()), title=str(r[3]),
                     body=str(r[4]), created_ms=int(r[5]), episode=episode_from(r[6]),
                     account_id=None if r[7] is None else str(r[7]), outbox_id=int(r[0]))


def outbox_arrays(rows: Sequence[OutboxRow]) -> tuple[list[object], ...]:
    """Column arrays for the unnest() insert: created, dedupe_key/episode, bypass, severity, title, body,
    kind, account_id."""
    return ([r.created_ms for r in rows], [episode_text(r.episode) for r in rows],
            [r.bypass_dedupe for r in rows], [r.severity.upper() for r in rows], [r.title for r in rows],
            [r.body for r in rows], [r.kind for r in rows], [r.account_id for r in rows])


# ---- sod_marks ---------------------------------------------------------------------------------------

SOD_COLS: Final = (
    "account_id, trading_day, roll_at_ms, balance, sod_equity, unexplained_increase_at_roll, partial_session, "
    "written_at_ms, clean_mids, partial_reasons"
)


def marks_json(marks: Mapping[SymbolKey, TimedMid]) -> str:
    items = [{"adapter_id": a, "symbol": s, "mid": format(m.mid, "f"), "ts": m.ts_ms}
             for (a, s), m in sorted(marks.items())]
    return json.dumps(items, separators=(",", ":"))


def marks_from(value: object) -> Mapping[SymbolKey, TimedMid]:
    data = _json(value) if value is not None else []
    if not isinstance(data, list):
        return MappingProxyType({})
    return MappingProxyType({(str(i["adapter_id"]), str(i["symbol"])): TimedMid(_dec(i["mid"]), int(i["ts"]))
                             for i in data})


def sod_from_row(r: Row) -> SodMark | None:
    """None when the row lacks its equity inputs (e.g. a LEADER_INIT placeholder)."""
    if r[3] is None or r[4] is None:
        return None
    day = r[1] if isinstance(r[1], date) else date.fromisoformat(str(r[1]))
    return SodMark(account_id=str(r[0]), trading_day=day, roll_ms=int(r[2]), balance=_dec(r[3]),
                   sod_equity=_dec(r[4]), unexplained_increase_at_roll=_dec(r[5] or 0),
                   partial_session=bool(r[6]), written_at_ms=int(r[7]), marks=marks_from(r[8]),
                   partial_reasons=tuple(str(x) for x in (r[9] or ())))


# ---- entities ----------------------------------------------------------------------------------------

ORDER_COLS: Final = (
    "client_order_id, broker_order_id, account_id, adapter_id, symbol, side, status, entry_price, stop_loss, "
    "take_profit, ordered_units, remaining_units, instrument_unknown, raw_qty, is_reduce_only, linked_position_id, "
    "broker_updated_at_ms, broker_seq, source, observed_seq, seq_id"
)


def order_from_row(r: Row) -> OrderDTO:
    return OrderDTO(
        client_order_id=None if r[0] is None else str(r[0]), broker_order_id=str(r[1]), account_id=str(r[2]),
        adapter_id=str(r[3]), symbol=str(r[4]), side=cast(Any, str(r[5])), status=OrderStatus(str(r[6])),
        entry_price=_opt_dec(r[7]), stop_loss=_opt_dec(r[8]), take_profit=_opt_dec(r[9]),
        ordered_units=_opt_dec(r[10]), remaining_units=_opt_dec(r[11]), instrument_unknown=bool(r[12]),
        raw_qty=_opt_dec(r[13]), is_reduce_only=bool(r[14]), linked_position_id=None if r[15] is None else str(r[15]),
        broker_updated_at_ms=int(r[16]), broker_seq=_opt_int(r[17]), source=cast(Any, str(r[18])),
        observed_seq=int(r[19]), seq_id=int(r[20]),
    )


POSITION_COLS: Final = (
    "position_id, account_id, adapter_id, symbol, side, units, entry_price, status, instrument_unknown, raw_qty, "
    "stop_loss, take_profit, opened_by_client_order_id, accrued_swap, commission, broker_updated_at_ms, "
    "broker_seq, source, observed_seq, seq_id"
)


def position_from_row(r: Row) -> PositionDTO:
    return PositionDTO(
        position_id=str(r[0]), account_id=str(r[1]), adapter_id=str(r[2]), symbol=str(r[3]),
        side=cast(Any, str(r[4])), units=_opt_dec(r[5]), entry_price=_opt_dec(r[6]), status=cast(Any, str(r[7])),
        instrument_unknown=bool(r[8]), raw_qty=_opt_dec(r[9]), stop_loss=_opt_dec(r[10]),
        take_profit=_opt_dec(r[11]), opened_by_client_order_id=None if r[12] is None else str(r[12]),
        accrued_swap=_dec(r[13] or 0), commission=_dec(r[14] or 0), broker_updated_at_ms=int(r[15]),
        broker_seq=_opt_int(r[16]), source=cast(Any, str(r[17])), observed_seq=int(r[18]), seq_id=int(r[19]),
    )


# Column 10 is the commit time of the latest balance *change* (the MaxDailyLoss balance / position skew
# input, oversee-rules.md §2), not committed_at_ms: a re-sent unchanged balance (NOOP_ADVANCE) or a
# realised_pnl_today-only update must not look like a newer balance version.
ACCOUNT_COLS: Final = (
    "account_id, adapter_id, currency, balance, realised_pnl_today, broker_updated_at_ms, broker_seq, source, "
    "observed_seq, seq_id, balance_changed_at_ms"
)


def account_from_row(r: Row) -> AccountDTO:
    return AccountDTO(account_id=str(r[0]), adapter_id=str(r[1]), currency=str(r[2]), balance=_dec(r[3]),
                      realised_pnl_today=_opt_dec(r[4]), broker_updated_at_ms=int(r[5]), broker_seq=_opt_int(r[6]),
                      source=cast(Any, str(r[7])), observed_seq=int(r[8]), seq_id=int(r[9]))


INSTRUMENT_COLS: Final = (
    "adapter_id, symbol, base_ccy, quote_ccy, pnl_ccy, contract_size, unit_kind, point_size, min_qty, "
    "swap_booked_to_balance, broker_symbol"
)


def instrument_from_row(r: Row) -> InstrumentSpec:
    return InstrumentSpec(adapter_id=str(r[0]), symbol=str(r[1]), base_ccy=str(r[2]), quote_ccy=str(r[3]),
                          pnl_ccy=str(r[4]), contract_size=_dec(r[5]), unit_kind=cast(Any, str(r[6])),
                          point_size=_dec(r[7]), min_qty=_dec(r[8]), swap_booked_to_balance=bool(r[9]),
                          broker_symbol=str(r[10] or ""))


CASH_FLOW_COLS: Final = (
    "transaction_id, account_id, adapter_id, kind, amount, currency, broker_ts, seq_id, committed_at_ms"
)


def cash_flow_from_row(r: Row) -> CashFlowRow:
    flow = CashFlowDTO(transaction_id=str(r[0]), account_id=str(r[1]), adapter_id=str(r[2]), kind=cast(Any, str(r[3])),
                       amount=_dec(r[4]), currency=str(r[5]), broker_ts=int(r[6]), seq_id=int(r[7]))
    return CashFlowRow(flow, int(r[8]))


def stream_health_from_row(r: Row) -> StreamHealthRow:
    return StreamHealthRow(str(r[0]), str(r[1]), _opt_int(r[2]))
