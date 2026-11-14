"""Trading-day roll at 17:00 America/New_York and the sod_marks row (oversee-rules.md §2 Day roll,
tier-persistence.md §4 sod_marks).

Rolls happen Monday-Friday at 17:00 New York time (DST-aware). The trading day that starts at
Friday's roll covers the weekend; the Sunday open is not a roll. A trading day is labelled with
the weekday it mostly covers (the roll date + 1 weekday). Holiday calendars are SM10's concern.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from typing import Final
from types import MappingProxyType
from zoneinfo import ZoneInfo

from src.oversee.config import OverseeConfig
from src.oversee.risk import ZERO, PreRollMarkBook, RiskUnknown, Valued, flow_in_account_ccy, position_pnl
from src.oversee.snapshot import AccountSnapshot, SodMark, TimedMid
from src.oversee.types import SymbolKey

NEW_YORK = ZoneInfo("America/New_York")
ROLL_TIME = time(17, 0)
LEADERLESS_WINDOW_MS: Final = 5 * 60_000


def terms_cover(window: tuple[int, int], terms: Sequence[tuple[int, int | None]]) -> bool:
    """True when the union of leader terms [start, end or open] covers the whole window."""
    lo, hi = window
    cursor = lo
    for start, end in sorted(terms):
        if start > cursor:
            return False
        cursor = max(cursor, hi if end is None else end)
        if cursor >= hi:
            return True
    return cursor >= hi


def leaderless_symbols(keys: Sequence[SymbolKey], roll_ms: int, closes: Mapping[SymbolKey, int],
                       terms: Sequence[tuple[int, int | None]]) -> list[SymbolKey]:
    """tier-persistence.md §4 sod_marks: the held symbols with no leader term covering the 5 min before their
    reference point (the last scheduled close or break before the roll; the roll itself for a symbol open at
    the roll)."""
    out: list[SymbolKey] = []
    for key in keys:
        ref = closes.get(key, roll_ms)
        if not terms_cover((ref - LEADERLESS_WINDOW_MS, ref), terms):
            out.append(key)
    return out


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _roll_on(d: date) -> int:
    return _to_ms(datetime.combine(d, ROLL_TIME, tzinfo=NEW_YORK))


def latest_roll_ms(now_ms: int) -> int:
    """The most recent roll at or before now (Mon-Fri 17:00 New York)."""
    d = datetime.fromtimestamp(now_ms / 1000, tz=NEW_YORK).date()
    while True:
        if d.weekday() < 5 and _roll_on(d) <= now_ms:
            return _roll_on(d)
        d -= timedelta(days=1)


def previous_roll_ms(roll_ms: int) -> int:
    return latest_roll_ms(roll_ms - 1)


def trading_day_for_roll(roll_ms: int) -> date:
    d = datetime.fromtimestamp(roll_ms / 1000, tz=NEW_YORK).date() + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def trading_day(now_ms: int) -> date:
    return trading_day_for_roll(latest_roll_ms(now_ms))


def build_sod_mark(snap: AccountSnapshot, roll_ms: int, now_ms: int, cfg: OverseeConfig) -> SodMark:
    """Compute the account's sod_marks row from pre-roll clean mids (§2 Day roll).

    `partial_session` when written > 30 s after the roll, when an input is missing, when a held
    symbol's newest pre-roll clean mid is > 5 min older than its last scheduled close before the
    roll (else the roll), or after a leaderless period: per held symbol (every mark used, crosses included) when the snapshot
    carries the leader terms (no term covering the 5 min before that symbol's reference point), else the
    account-wide `leaderless_before_roll`. Post-roll cash flows already in the balance are backed out of the
    sod balance. Raises RiskUnknown without an account row."""
    if snap.account is None:
        raise RiskUnknown("risk_unknown", "no account row")
    ccy = snap.account.currency
    reasons: list[str] = []
    if now_ms - roll_ms > cfg.sweep.partial_session_late_ms:
        reasons.append("late")
    if snap.leader_terms is None and snap.leaderless_before_roll:
        reasons.append("leaderless")
    book = PreRollMarkBook(snap.quotes, snap.instruments, roll_ms)
    post_roll = Valued(ZERO)
    for row in snap.cash_flows:
        if row.flow.broker_ts >= roll_ms:
            try:
                post_roll = post_roll + flow_in_account_ccy(row.flow, book, ccy)
            except RiskUnknown:
                reasons.append("cash_flow_fx_missing")
    sod_balance = snap.account.balance - post_roll.value
    pnl = ZERO
    for p in snap.positions:
        if p.status != "OPEN":
            continue
        try:
            pnl += position_pnl(p, book, ccy).value
        except RiskUnknown:
            reasons.append(f"missing_mark:{p.symbol}")
    for key, mid in book.used.items():
        ref = snap.scheduled_close_before_roll_ms.get(key, roll_ms)
        if ref - mid.ts_ms > cfg.marks.sod_mark_max_age_ms:
            reasons.append(f"stale_mark:{key[1]}")
    if snap.leader_terms is not None:
        for key in leaderless_symbols(sorted(book.used), roll_ms, snap.scheduled_close_before_roll_ms,
                                      snap.leader_terms):
            reasons.append(f"leaderless:{key[1]}")
    uiar = ZERO
    if snap.prev_sod is not None:
        prev_flows = Valued(ZERO)
        for row in snap.prev_day_cash_flows:
            if snap.prev_sod.roll_ms <= row.flow.broker_ts < roll_ms:
                try:
                    prev_flows = prev_flows + flow_in_account_ccy(row.flow, book, ccy)
                except RiskUnknown:
                    reasons.append("cash_flow_fx_missing")
        uiar = max(ZERO, sod_balance - snap.prev_sod.balance - prev_flows.value)
    marks: dict[SymbolKey, TimedMid] = dict(book.used)
    return SodMark(
        account_id=snap.account_id, trading_day=trading_day_for_roll(roll_ms), roll_ms=roll_ms,
        balance=sod_balance, sod_equity=sod_balance + pnl - uiar, unexplained_increase_at_roll=uiar,
        partial_session=bool(reasons), written_at_ms=now_ms, marks=MappingProxyType(marks),
        partial_reasons=tuple(dict.fromkeys(reasons)),
    )

