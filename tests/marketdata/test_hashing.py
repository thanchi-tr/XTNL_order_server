"""Keyed content hashes, keyring and record digest (plan S12; design §A13, R11-M13). Fixed test keys only."""
from __future__ import annotations

import hashlib
import hmac
import zlib
from decimal import Decimal

import pytest

from src.marketdata.hashing import (
    HASH_HEX_LEN,
    HashComparison,
    HashStatus,
    Keyring,
    KeyringError,
    canonical_decimal,
    compare_hashes,
    content_hash,
    key_id_of,
    record_digest,
    verify,
)

D = Decimal
KEY = bytes(range(32))           # test key A
NEXT_KEY = bytes(range(32, 64))  # test key B (the rotation's new key)
SERIES = "mock:SYN1:15m:BID:BROKER"
END = 1_700_000_100_000
OHLC = (D("1.10"), D("1.2"), D("1.05"), D("1.15"))

# Golden vectors: the hash under a fixed test key must never change without a deliberate key-format change.
GOLDEN = [
    (1, KEY, SERIES, END, OHLC, "017f768ff17435d67d3c149eaccb8bee7c"),
    (1, KEY, "mock:SYN1:15m:ASK:BROKER", END, OHLC, "01c72763be983c04fcb994b2e61927965c"),
    (1, KEY, "mock:SYN2:15m:BID:BROKER", END, (D("250"), D("251.5"), D("249.25"), D("250.75")),
     "0112b9f5dfb3fb4126f9034074a372c5cc"),
    (2, NEXT_KEY, SERIES, END, OHLC, "028a87d49f38a4b038005d1029d9895d42"),
]


@pytest.mark.parametrize(("kid", "key", "series", "end", "ohlc", "expected"), GOLDEN)
def test_golden_hashes(kid: int, key: bytes, series: str, end: int, ohlc: tuple[Decimal, ...],
                       expected: str) -> None:
    assert content_hash(Keyring.of(kid, key), series, end, *ohlc) == expected
    assert len(expected) == HASH_HEX_LEN


def test_hash_matches_the_documented_construction() -> None:
    message = f"{SERIES}|{END}|1.1|1.2|1.05|1.15".encode()
    mac = hmac.new(KEY, message, hashlib.sha256).digest()[:16]
    assert content_hash(Keyring.of(7, KEY), SERIES, END, *OHLC) == f"07{mac.hex()}"
    assert key_id_of(content_hash(Keyring.of(7, KEY), SERIES, END, *OHLC)) == 7


@pytest.mark.parametrize(("value", "text"), [
    ("1.10", "1.1"), ("1.1000", "1.1"), ("100", "100"), ("1E+2", "100"), ("0.000010", "0.00001"), ("0", "0"),
    ("0.00", "0"), ("12345.678900", "12345.6789"),
])
def test_canonical_decimal(value: str, text: str) -> None:
    assert canonical_decimal(D(value)) == text


def test_canonical_decimal_refuses_non_finite() -> None:
    with pytest.raises(ValueError):
        canonical_decimal(D("NaN"))


@pytest.mark.parametrize("value", ["1E+1000000", "1E-1000000", "1E+101", "9" * 70])
def test_canonical_decimal_refuses_unrepresentable_values_with_value_error(value: str) -> None:
    with pytest.raises(ValueError):
        canonical_decimal(D(value))


def test_hash_covers_ohlc_series_and_bar_end_only() -> None:
    ring = Keyring.of(1, KEY)
    base = content_hash(ring, SERIES, END, *OHLC)
    assert content_hash(ring, SERIES, END, D("1.1000"), D("1.20"), D("1.050"), D("1.15")) == base
    assert content_hash(ring, SERIES, END + 900_000, *OHLC) != base
    assert content_hash(ring, SERIES, END, D("1.10"), D("1.2"), D("1.05"), D("1.16")) != base


def test_verify_current_retained_and_unknown_key_ids() -> None:
    old_ring = Keyring.of(1, KEY)
    rotated = Keyring.of(2, NEXT_KEY, ((1, KEY),))       # new key current, old key retained
    old_hash = content_hash(old_ring, SERIES, END, *OHLC)
    new_hash = content_hash(rotated, SERIES, END, *OHLC)
    assert key_id_of(new_hash) == 2 and key_id_of(old_hash) == 1
    assert verify(rotated, old_hash, SERIES, END, *OHLC) is HashStatus.VALID      # a retained key id verifies
    assert verify(rotated, new_hash, SERIES, END, *OHLC) is HashStatus.VALID
    retired = Keyring.of(2, NEXT_KEY)
    assert verify(retired, old_hash, SERIES, END, *OHLC) is HashStatus.MISSING    # unknown id: missing, not changed
    assert verify(rotated, old_hash, SERIES, END, D("1.1"), D("1.2"), D("1.05"), D("1.14")) is HashStatus.MISMATCH


@pytest.mark.parametrize("bad", ["", "zz" + "0" * 32, "96" + "0" * 31, "96" + "0" * 33, "96" + "A" * 32])
def test_verify_malformed_hash_is_missing(bad: str) -> None:
    assert verify(Keyring.of(0x96, KEY), bad, SERIES, END, *OHLC) is HashStatus.MISSING
    assert key_id_of(bad) is None


@pytest.mark.parametrize("price", ["NaN", "sNaN", "Infinity", "1E+1000000", "1E-1000000", "9" * 70])
def test_verify_never_raises(price: str) -> None:
    ring = Keyring.of(1, KEY)
    good = content_hash(ring, SERIES, END, *OHLC)
    assert verify(ring, good, SERIES, END, D(price), *OHLC[1:]) is HashStatus.MISMATCH
    assert verify(ring, good, SERIES, END, *OHLC[:3], D(price)) is HashStatus.MISMATCH


def test_compare_hashes_only_under_one_key_id() -> None:
    a = Keyring.of(1, KEY)
    b = Keyring.of(2, NEXT_KEY)
    h1 = content_hash(a, SERIES, END, *OHLC)
    h2 = content_hash(a, SERIES, END, D("1.1"), D("1.2"), D("1.05"), D("1.16"))
    assert compare_hashes(h1, h1) is HashComparison.SAME
    assert compare_hashes(h1, h2) is HashComparison.CHANGED
    assert compare_hashes(h1, content_hash(b, SERIES, END, *OHLC)) is HashComparison.INCOMPARABLE
    assert compare_hashes(h1, "garbage") is HashComparison.INCOMPARABLE


def test_keyring_refuses_bad_keys_and_never_shows_them() -> None:
    with pytest.raises(KeyringError):
        Keyring.of(1, b"short")
    with pytest.raises(KeyringError):
        Keyring.of(2, KEY, ((1, b"x" * 31),))
    with pytest.raises(KeyringError) as same:
        Keyring.of(2, KEY, ((1, KEY),))                # one key under two ids
    assert KEY.hex() not in str(same.value)
    ring = Keyring.of(2, NEXT_KEY, ((1, KEY),))
    assert ring.key_ids == (2, 1)
    assert ring.holds(1) and ring.key(ring.current_id) == NEXT_KEY
    text = repr(ring)
    assert KEY.hex() not in text and NEXT_KEY.hex() not in text and "entries" not in text


@pytest.mark.parametrize(("current", "retained"), [
    (0, ()), (256, ()), (-1, ()),                      # out of 1-255
    (2, ((2, NEXT_KEY),)),                             # the same id twice
    (2, ((3, NEXT_KEY),)),                             # a retained id must be lower: ids only increase
    (2, ((0, NEXT_KEY),)),
])
def test_keyring_key_ids_are_in_range_and_increase(current: int, retained: tuple[tuple[int, bytes], ...]) -> None:
    with pytest.raises(KeyringError):
        Keyring.of(current, KEY, retained)


def test_a_new_key_never_takes_a_retired_key_id() -> None:
    # Rotation 1 -> 2 -> 3: a hash made under the retired key 1 is never compared as CHANGED with one under key 3.
    first = content_hash(Keyring.of(1, KEY), SERIES, END, *OHLC)
    third_key = bytes(range(64, 96))
    ring = Keyring.of(3, third_key, ((2, NEXT_KEY),))
    third = content_hash(ring, SERIES, END, *OHLC)
    assert compare_hashes(first, third) is HashComparison.INCOMPARABLE
    assert verify(ring, first, SERIES, END, *OHLC) is HashStatus.MISSING


def test_record_digest_is_unkeyed_crc32() -> None:
    text = "17000001000000001|record"
    assert record_digest(text) == f"{zlib.crc32(text.encode()):08x}"
    assert record_digest(text) != record_digest(text + "x")
