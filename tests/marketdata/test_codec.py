"""Fixed-width bar member codec (plan S12; design §A3). Synthetic instruments only."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.marketdata.codec import (
    RECORD_LEN,
    CodecError,
    CodecFailure,
    DecodedMember,
    decode_member,
    encode_member,
    member_bucket_start,
)
from src.marketdata.hashing import Keyring, content_hash, record_digest
from src.marketdata.types import M15, Bar, Quality, SeriesId, Side, Source

D = Decimal
SERIES = SeriesId("mock", "SYN1", M15, Side.BID, Source.BROKER)
T0 = 1_700_000_100_000
RING = Keyring.of(1, bytes(range(32)))


def bar(start: int = T0, *, o: str = "1.10", h: str = "1.2", low: str = "1.05", c: str = "1.15", revision: int = 0,
        quality: Quality = Quality.COMPLETE, final_at_ms: int | None = T0 + 905_000, ticks: int = 0) -> Bar:
    return Bar(series=SERIES, bucket_start_ms=start, open=D(o), high=D(h), low=D(low), close=D(c), tick_count=ticks,
               quality=quality, revision=revision, final_at_ms=final_at_ms)


def hash_of(b: Bar) -> str:
    return content_hash(RING, SERIES.canonical, b.bucket_end_ms, b.open, b.high, b.low, b.close)


def decoded(member: str | bytes) -> DecodedMember:
    out = decode_member(SERIES, member)
    assert isinstance(out, DecodedMember)
    return out


@pytest.mark.parametrize("b", [
    bar(),
    bar(o="250", h="251.5", low="249.25", c="250.75", revision=3, ticks=4321),
    bar(o="0.000012345678", h="0.000012345679", low="0.000012345677", c="0.000012345678"),
    bar(quality=Quality.PARTIAL, final_at_ms=None),
    bar(quality=Quality.DEGRADED, revision=9999, ticks=9_999_999),
    bar(quality=Quality.BLIND),
    bar(quality=Quality.NO_TRADE),
    bar(o="9999999999999999", h="9999999999999999", low="1", c="2"),
])
def test_round_trip(b: Bar) -> None:
    member = encode_member(b, hash_of(b))
    out = decoded(member)
    assert out.bar == b
    assert out.content_hash == hash_of(b)
    assert hash_of(out.bar) == hash_of(b)          # the decoded prices hash the same
    assert decoded(member.encode("ascii")).bar == b
    assert member_bucket_start(member) == b.bucket_start_ms


def test_member_layout() -> None:
    member = encode_member(bar(revision=7), hash_of(bar()))
    prefix, record, digest, h = member.split("|")
    assert prefix == f"{T0:013d}0007"
    assert len(record) == RECORD_LEN == 87
    assert len(digest) == 8 and len(h) == 34


def test_identical_bars_in_different_buckets_stay_distinct_members() -> None:
    a = bar(T0, final_at_ms=None)
    b = bar(T0 + 900_000, final_at_ms=None)
    members = {encode_member(a, "96" + "0" * 32), encode_member(b, "96" + "0" * 32)}
    assert len(members) == 2
    revisions = {encode_member(bar(revision=0), hash_of(bar())), encode_member(bar(revision=1), hash_of(bar()))}
    assert len(revisions) == 2


@pytest.mark.parametrize("b", [
    bar(o="-1"), bar(c="NaN"), bar(h="1E+17"), bar(o="1.0000000000001"), bar(revision=10_000),
    bar(ticks=10_000_000), bar(start=-900_000), bar(final_at_ms=0),
])
def test_encode_refuses_what_the_record_cannot_hold(b: Bar) -> None:
    with pytest.raises(CodecError):
        encode_member(b, hash_of(bar()))


def test_encode_refuses_a_malformed_hash() -> None:
    with pytest.raises(CodecError):
        encode_member(bar(), "not-a-hash")


def test_decode_failures_are_values_without_member_text() -> None:
    member = encode_member(bar(), hash_of(bar()))
    flipped = member[:30] + ("1" if member[30] != "1" else "2") + member[31:]
    assert decode_member(SERIES, flipped) == CodecFailure("digest")
    for bad in ("", "garbage", member[:-1], member + "0", member.replace("|", ":"), b"\xff\xfe",
                member.replace("C", "X", 1)):
        out = decode_member(SERIES, bad)
        assert isinstance(out, CodecFailure) and out.reason in ("format", "digest")
    assert member_bucket_start("abc") is None


@pytest.mark.parametrize("position", [0, 1, 2, 33])
def test_a_corrupt_hash_is_a_digest_failure_never_another_key_id(position: int) -> None:
    # The digest covers the hash: a flipped key-id digit is quarantined, not read as a hash under a rotated-out key.
    member = encode_member(bar(), hash_of(bar()))
    at = len(member) - 34 + position
    flipped = member[:at] + ("3" if member[at] != "3" else "4") + member[at + 1:]
    assert decode_member(SERIES, flipped) == CodecFailure("digest")


def test_digest_covers_prefix_record_and_hash() -> None:
    member = encode_member(bar(), hash_of(bar()))
    prefix, record, digest, h = member.split("|")
    assert digest == record_digest(f"{prefix}|{record}|{h}")
