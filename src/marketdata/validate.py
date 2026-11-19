"""Zero-trust bar and quote checks (design §A4 Validation; plan S12, R11-M15).

Every check returns a value and never raises (a NaN or infinite price is a failed check, not an exception). Hard
checks, in the design's numbering:

1. `prices`: every price finite and > 0, `high >= max(open, close)`, `low <= min(open, close)`;
2. `grid_range`: the bucket start is on the spec grid and inside the requested range `[from, to]` (BROKER, range
   form) or is the bucket that was built (TICK);
3. `side`: the side is the one requested (against the bar's own stated side when the response names one; a bar that
   names none is not refused on that ground; a TICK bar carries the side it was built from);
4. `future`: the bucket end is at most 5 s after receipt (the dto-contracts `future_timestamp` tolerance);
5. `content_hash`: on read, the stored keyed hash matches under a retained key id (`check_stored`); a key id the
   ring does not hold is **missing** (reported, never a failure, never "changed").

**Forming bars (R11-M15).** Only bars whose bucket end is at most 5 s after receipt are validated, stored or counted.
A broker response's forming bar is finality evidence only: `split_response` puts it in `forming`, never in `valid` or
`invalid`, so it is never a check failure. Only the bucket already started at receipt (bucket start at most 5 s after
receipt, bucket end later than that) can be forming; a bar starting later is future-dated (a time-convention error,
say) and fails check 4, so it is quarantined and never counts as finality evidence.

A response for another adapter, symbol, timeframe or side fails every closed bar (`instrument`, check 2 or check 3),
so another instrument's bars are never stored under this series.

Results carry check names only; nothing here formats a price.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Final

from src.dto.bars import BrokerBar, PriceBarsResponse
from src.dto.versioning import FUTURE_TIMESTAMP_TOLERANCE_MS
from src.marketdata.hashing import HashStatus, Keyring, verify
from src.marketdata.types import Bar, BarSpec, SeriesId, Side

FUTURE_TOLERANCE_MS: Final = FUTURE_TIMESTAMP_TOLERANCE_MS


class Check(str, Enum):
    PRICES = "prices"
    GRID_RANGE = "grid_range"
    SIDE = "side"
    FUTURE = "future"
    INSTRUMENT = "instrument"
    CONTENT_HASH = "content_hash"
    QUOTE_PRICES = "quote_prices"
    QUOTE_FUTURE = "quote_future"


@dataclass(frozen=True)
class CheckResult:
    failed: tuple[Check, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.failed


PASS: Final = CheckResult()


def _result(checks: list[tuple[Check, bool]]) -> CheckResult:
    return CheckResult(tuple(c for c, passed in checks if not passed))


# ---------------------------------------------------------------------------------------------------- single checks
def _positive(p: Decimal) -> bool:
    return p.is_finite() and p > 0


def prices_ok(o: Decimal, h: Decimal, low: Decimal, c: Decimal) -> bool:
    """Check 1. Never raises (NaN and infinity fail before any comparison)."""
    if not all(_positive(p) for p in (o, h, low, c)):
        return False
    return h >= max(o, c) and low <= min(o, c)


def grid_ok(spec: BarSpec, bucket_start_ms: int, from_ms: int | None = None, to_ms: int | None = None) -> bool:
    """Check 2: on the spec grid, and inside `[from_ms, to_ms]` when a bound is given."""
    if not spec.on_grid(bucket_start_ms):
        return False
    if from_ms is not None and bucket_start_ms < from_ms:
        return False
    return to_ms is None or bucket_start_ms <= to_ms


def side_ok(expected: Side, stated: str | None) -> bool:
    """Check 3: a stated side must equal the requested one; an unstated side is not refused here."""
    return stated is None or stated.strip().upper() == expected.value


def not_future(bucket_end_ms: int, received_ms: int) -> bool:
    """Check 4: the bucket end is at most 5 s after receipt."""
    return bucket_end_ms - received_ms <= FUTURE_TOLERANCE_MS


def is_forming(spec: BarSpec, bucket_start_ms: int, received_ms: int) -> bool:
    """The bucket already started at receipt (start at most 5 s after it) that ends more than 5 s after it: evidence
    only (R11-M15). A bar starting later is not forming: it fails check 4."""
    started = bucket_start_ms - received_ms <= FUTURE_TOLERANCE_MS
    return started and not not_future(spec.bucket_end_ms(bucket_start_ms), received_ms)


# ---------------------------------------------------------------------------------------------------- bars
def check_broker_bar(bar: BrokerBar, series: SeriesId, *, received_ms: int, from_ms: int | None = None,
                     to_ms: int | None = None) -> CheckResult:
    """Checks 1-4 for one closed broker bar (callers drop forming bars first, `split_response`)."""
    spec = series.spec
    return _result([
        (Check.PRICES, prices_ok(bar.open, bar.high, bar.low, bar.close)),
        (Check.GRID_RANGE, grid_ok(spec, bar.bucket_start_ms, from_ms, to_ms)),
        (Check.SIDE, side_ok(series.side, bar.side)),
        (Check.FUTURE, not_future(spec.bucket_end_ms(bar.bucket_start_ms), received_ms)),
    ])


def check_tick_bar(bar: Bar, *, built_bucket_start_ms: int, built_side: Side, received_ms: int) -> CheckResult:
    """Checks 1-4 for a closed TICK bar: the bucket is the one built, the side the one it was built from."""
    spec = bar.series.spec
    return _result([
        (Check.PRICES, prices_ok(bar.open, bar.high, bar.low, bar.close)),
        (Check.GRID_RANGE, grid_ok(spec, bar.bucket_start_ms, built_bucket_start_ms, built_bucket_start_ms)),
        (Check.SIDE, bar.series.side is built_side),
        (Check.FUTURE, not_future(bar.bucket_end_ms, received_ms)),
    ])


@dataclass(frozen=True)
class StoredCheck:
    """`check_stored`'s answer: the hard-check result (check 5 included as a failure only on MISMATCH) and the hash
    status, so a caller can tell a corrupt member (quarantine) from a hash under a key id no longer held (missing:
    keep waiting, never a correction)."""

    result: CheckResult
    hash_status: HashStatus

    @property
    def usable(self) -> bool:
        return self.result.ok and self.hash_status is HashStatus.VALID


def check_stored(bar: Bar, stored_hash: str, keyring: Keyring, *, now_ms: int) -> StoredCheck:
    """Checks 1, 2 (grid), 4 and 5 on read. Never raises."""
    status = verify(keyring, stored_hash, bar.series.canonical, bar.bucket_end_ms, bar.open, bar.high, bar.low,
                    bar.close)
    result = _result([
        (Check.PRICES, prices_ok(bar.open, bar.high, bar.low, bar.close)),
        (Check.GRID_RANGE, grid_ok(bar.series.spec, bar.bucket_start_ms)),
        (Check.FUTURE, not_future(bar.bucket_end_ms, now_ms)),
        (Check.CONTENT_HASH, status is not HashStatus.MISMATCH),
    ])
    return StoredCheck(result, status)


# ---------------------------------------------------------------------------------------------------- responses
@dataclass(frozen=True)
class ResponseSplit:
    """One broker response sorted for `md-bars`: `valid` closed bars (to queue for writing), `invalid` closed bars
    with their failed checks (to quarantine; their buckets are `blind`), and `forming` bars (finality evidence
    only: never validated, stored or counted)."""

    valid: tuple[BrokerBar, ...] = ()
    invalid: tuple[tuple[BrokerBar, CheckResult], ...] = ()
    forming: tuple[BrokerBar, ...] = ()


def split_response(response: PriceBarsResponse, series: SeriesId) -> ResponseSplit:
    """Validate every closed bar of `response` for `series` (range form: check 2 also bounds the bucket by
    `[from_ms, to_ms]`). A response for another adapter or symbol fails every closed bar on `instrument`, one for
    another timeframe or side on check 2 / 3."""
    valid: list[BrokerBar] = []
    invalid: list[tuple[BrokerBar, CheckResult]] = []
    forming: list[BrokerBar] = []
    same_tf = response.timeframe_s == series.spec.timeframe_s
    same_side = response.side == series.side.value
    same_instrument = response.adapter_id == series.adapter_id and response.symbol == series.symbol
    for bar in response.bars:
        if is_forming(series.spec, bar.bucket_start_ms, response.received_ms):
            forming.append(bar)
            continue
        result = check_broker_bar(bar, series, received_ms=response.received_ms, from_ms=response.from_ms,
                                  to_ms=response.to_ms)
        extra = [c for c, wrong in ((Check.GRID_RANGE, not same_tf), (Check.SIDE, not same_side),
                                    (Check.INSTRUMENT, not same_instrument))
                 if wrong and c not in result.failed]
        if extra:
            order = [c for c in Check if c in result.failed or c in extra]
            result = CheckResult(tuple(order))
        if result.ok:
            valid.append(bar)
        else:
            invalid.append((bar, result))
    return ResponseSplit(tuple(valid), tuple(invalid), tuple(forming))


# ---------------------------------------------------------------------------------------------------- quotes
def check_quote(bid: Decimal, ask: Decimal, broker_ms: int, received_ms: int) -> CheckResult:
    """The latest-quote checks: bid > 0, ask >= bid (both finite), and check 4 on the quote time. A failing quote
    is not kept and `quote()` reports `STALE`."""
    prices = _positive(bid) and ask.is_finite() and ask >= bid
    return _result([
        (Check.QUOTE_PRICES, prices),
        (Check.QUOTE_FUTURE, not_future(broker_ms, received_ms)),
    ])
