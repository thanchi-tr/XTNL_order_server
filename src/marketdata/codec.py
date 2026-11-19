"""Fixed-width sorted-set member codec for `md:bars:{series}` (design §A3; plan S12).

A member is ASCII text::

    <bucket_start:13><revision:4>|<record:87>|<digest:8>|<hash:34>

* The prefix (bucket start in ms, zero-padded to 13 digits; revision to 4) makes two identical bars in different
  buckets, or two revisions of one bucket, distinct members: a sorted set can never collapse them.
* The record is scaled integers, fixed width: `scale:2` (decimal places shared by the four prices), `open`, `high`,
  `low`, `close` as 16-digit mantissas (`price = mantissa × 10^-scale`), `tick_count:7`, `quality:1`
  (`C` complete, `P` partial, `D` degraded, `N` no_trade, `B` blind) and `final_at_ms:13` (all zeros: not final).
* `digest` is the unkeyed CRC32 of `prefix|record|hash` (`hashing.record_digest`), for readers without the key. It
  covers the hash too, so a corrupt key-id byte is a `digest` failure (quarantine and re-fetch, §A4) and can never
  pass for a hash under another key id (`missing`, which only a real rotation may produce, §A13).
* `hash` is the keyed content hash (`hashing.content_hash`), which this codec carries but does not check (§A4
  check 5 needs the keyring: `validate.check_stored`).

`encode_member` raises `CodecError` for a bar it cannot represent (a programming error: only validated bars are
written). `decode_member` never raises: a malformed or corrupt member is a `CodecFailure` value whose reason names the
problem, never a price.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Context, Decimal
from typing import Final

from src.marketdata.hashing import record_digest
from src.marketdata.types import Bar, Quality, SeriesId

BUCKET_DIGITS: Final = 13
REVISION_DIGITS: Final = 4
SCALE_DIGITS: Final = 2
PRICE_DIGITS: Final = 16
TICKS_DIGITS: Final = 7
FINAL_DIGITS: Final = 13
MAX_SCALE: Final = 12
RECORD_LEN: Final = SCALE_DIGITS + 4 * PRICE_DIGITS + TICKS_DIGITS + 1 + FINAL_DIGITS

QUALITY_CODES: Final = {
    Quality.COMPLETE: "C", Quality.PARTIAL: "P", Quality.DEGRADED: "D", Quality.NO_TRADE: "N", Quality.BLIND: "B",
}
_QUALITY_BY_CODE: Final = {code: q for q, code in QUALITY_CODES.items()}
_CTX: Final = Context(prec=60)

_MEMBER_RE: Final = re.compile(
    r"([0-9]{13})([0-9]{4})\|"
    r"(([0-9]{2})([0-9]{16})([0-9]{16})([0-9]{16})([0-9]{16})([0-9]{7})([CPDNB])([0-9]{13}))\|"
    r"([0-9a-f]{8})\|([0-9a-f]{34})"
)


class CodecError(ValueError):
    """A bar the fixed-width record cannot hold (negative, too many digits, out-of-range counters)."""


@dataclass(frozen=True)
class CodecFailure:
    """A member that does not decode: `reason` is `format` or `digest` (no member text, no price)."""

    reason: str


@dataclass(frozen=True)
class DecodedMember:
    bar: Bar
    content_hash: str
    record_digest: str


def _fixed(value: int, digits: int, what: str) -> str:
    if value < 0 or value >= 10 ** digits:
        raise CodecError(f"{what} does not fit {digits} digits")
    return f"{value:0{digits}d}"


def _scale_for(prices: tuple[Decimal, ...]) -> int:
    for scale in range(MAX_SCALE + 1):
        scaled = [p.scaleb(scale, _CTX) for p in prices]
        if all(s == s.to_integral_value(context=_CTX) for s in scaled):
            return scale
    raise CodecError(f"a price has more than {MAX_SCALE} decimal places")


def encode_record(bar: Bar) -> str:
    prices = (bar.open, bar.high, bar.low, bar.close)
    if not all(p.is_finite() and p >= 0 for p in prices):
        raise CodecError("prices must be finite and not negative")
    scale = _scale_for(prices)
    mantissas = "".join(_fixed(int(p.scaleb(scale, _CTX)), PRICE_DIGITS, "a price") for p in prices)
    final = 0 if bar.final_at_ms is None else bar.final_at_ms
    if bar.final_at_ms is not None and final <= 0:
        raise CodecError("final_at_ms must be positive")
    return (f"{scale:02d}{mantissas}{_fixed(bar.tick_count, TICKS_DIGITS, 'tick_count')}"
            f"{QUALITY_CODES[bar.quality]}{_fixed(final, FINAL_DIGITS, 'final_at_ms')}")


def member_prefix(bucket_start_ms: int, revision: int) -> str:
    return (_fixed(bucket_start_ms, BUCKET_DIGITS, "bucket_start_ms")
            + _fixed(revision, REVISION_DIGITS, "revision"))


def _digested(body: str, content_hash: str) -> str:
    """The text the record digest covers: `prefix|record|hash`."""
    return f"{body}|{content_hash}"


def encode_member(bar: Bar, content_hash: str) -> str:
    """The sorted-set member for `bar` (score = `bar.bucket_start_ms`)."""
    if not re.fullmatch(r"[0-9a-f]{34}", content_hash):
        raise CodecError("content hash is not 34 lower-case hex digits")
    body = f"{member_prefix(bar.bucket_start_ms, bar.revision)}|{encode_record(bar)}"
    return f"{body}|{record_digest(_digested(body, content_hash))}|{content_hash}"


def decode_member(series: SeriesId, member: str | bytes) -> DecodedMember | CodecFailure:
    """Parse one member of `md:bars:{series}`. Checks the format and the record digest; never raises."""
    try:
        text = member.decode("ascii") if isinstance(member, bytes) else member
    except UnicodeDecodeError:
        return CodecFailure("format")
    m = _MEMBER_RE.fullmatch(text)
    if m is None:
        return CodecFailure("format")
    body = f"{m.group(1)}{m.group(2)}|{m.group(3)}"
    if record_digest(_digested(body, m.group(13))) != m.group(12):
        return CodecFailure("digest")
    scale = int(m.group(4))
    if scale > MAX_SCALE:
        return CodecFailure("format")
    o, h, low, c = (Decimal(int(m.group(i))).scaleb(-scale, _CTX) for i in (5, 6, 7, 8))
    final = int(m.group(11))
    bar = Bar(series=series, bucket_start_ms=int(m.group(1)), open=o, high=h, low=low, close=c,
              tick_count=int(m.group(9)), quality=_QUALITY_BY_CODE[m.group(10)], revision=int(m.group(2)),
              final_at_ms=final or None)
    return DecodedMember(bar=bar, content_hash=m.group(13), record_digest=m.group(12))


def member_bucket_start(member: str | bytes) -> int | None:
    """The bucket start a member claims (its first 13 digits), or None (for scans that need only the score)."""
    head = member[:BUCKET_DIGITS]
    text = head.decode("ascii", "replace") if isinstance(head, bytes) else head
    return int(text) if re.fullmatch(r"[0-9]{13}", text) else None
