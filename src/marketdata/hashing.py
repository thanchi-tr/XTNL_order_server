"""Keyed content hashes, the keyring and the unkeyed record digest (design §A13, §B3; plan S12, R11-M13, R11-m).

**Content hash** of a bar: `key_id ‖ HMAC-SHA256(key, series ‖ bar_end_ms ‖ o ‖ h ‖ l ‖ c)[:16]`, as text: two hex
digits of the 1-byte key id, then 32 hex digits. Prices enter in canonical decimal text (`canonical_decimal`: exact
value, no exponent, no trailing zeros), so `1.10` and `1.1` hash alike and a bar re-fetched, re-written or decoded from
the store hashes the same. It covers the OHLC only, never the revision, `final_at_ms`, quality or tick count. Being
keyed, a hash kept in PostgreSQL cannot be brute-forced back into prices.

**Keyring:** the current key plus the retained previous key (`XTNL_MD_HMAC_KEY_PREV` during a rotation). A hash under
any retained key id verifies; a hash under a key id the ring does not hold is **missing**, never changed (R11-M13).
Hashes under different key ids are never compared (`compare_hashes`), so a rotation can never look like a correction.
A key's 1-byte id is set by the operator with the key (`XTNL_MD_HMAC_KEY_ID`, `XTNL_MD_HMAC_KEY_PREV_ID`, 1-255) and
only ever increases: every retained id must be lower than the current id. Hashes made under a retired key stay in
durable records (the ARM's input hashes and decisions), so a new key must never take a retired key's id, or an old hash
and a new hash would compare as `CHANGED`; increasing ids rule that out for every key ever retired, not only the one
still held. (An id derived from the key would collide with some retired key's id with probability 1/256 per rotation.)

Keys are never logged: `Keyring`'s `repr` shows key ids only.

**Record digest:** an unkeyed CRC32 (`zlib.crc32`, allowed in `src/marketdata` only) of a stored member's prefix,
record and content hash (`codec`), so an out-of-process reader without the key can still detect a corrupt member,
a corrupt key id included (integrity, not authenticity; §A4 check 5).
"""
from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Context, Decimal, DivisionByZero, Inexact, InvalidOperation, Overflow, Rounded
from enum import Enum
from typing import Final
from zlib import crc32

KEY_BYTES: Final = 32
HASH_BYTES: Final = 16
HASH_HEX_LEN: Final = 2 + 2 * HASH_BYTES
"""Text length of a content hash: 2 hex digits of key id + 32 hex digits."""
_HASH_RE: Final = re.compile(r"[0-9a-f]{34}")
KEY_ID_MIN: Final = 1
KEY_ID_MAX: Final = 255
_CANON_CTX: Final = Context(prec=60, traps=[InvalidOperation, DivisionByZero, Overflow, Inexact, Rounded])
"""Exact or refused: a value needing more than 60 digits raises (as ValueError), it is never rounded into a hash."""
_MAX_ADJUSTED: Final = 100
"""Largest power of ten (either sign) `canonical_decimal` accepts: far beyond any price, and it keeps the text short."""


class KeyringError(ValueError):
    """A key of the wrong length, a key id out of range or not increasing, or one key held twice (messages never
    contain key material)."""


@dataclass(frozen=True)
class Keyring:
    """Key id → key; `current_id` signs new hashes, every held id verifies. Build with `Keyring.of`."""

    current_id: int
    entries: tuple[tuple[int, bytes], ...] = field(repr=False)

    @classmethod
    def of(cls, current_id: int, current: bytes, retained: Sequence[tuple[int, bytes]] = ()) -> Keyring:
        """The ring for `current` under `current_id` plus the `(id, key)` pairs still retained. Every id is
        1-255 and every retained id is lower than the current one (ids only increase across rotations)."""
        ids: list[tuple[int, bytes]] = []
        for role, kid, key in (("current", current_id, current), *(("retained", i, k) for i, k in retained)):
            if len(key) != KEY_BYTES:
                raise KeyringError(f"the {role} content-hash key must be {KEY_BYTES} bytes")
            if not KEY_ID_MIN <= kid <= KEY_ID_MAX:
                raise KeyringError(f"the {role} content-hash key id must be {KEY_ID_MIN}-{KEY_ID_MAX}")
            if ids and kid >= current_id:
                raise KeyringError("a retained content-hash key id must be lower than the current key id")
            if any(kid == other for other, _ in ids):
                raise KeyringError(f"the {role} content-hash key id is used by another key")
            if any(hmac.compare_digest(key, other) for _, other in ids):
                raise KeyringError(f"the {role} content-hash key is the same key as another; generate a new key")
            ids.append((kid, key))
        return cls(current_id=current_id, entries=tuple(ids))

    @property
    def key_ids(self) -> tuple[int, ...]:
        return tuple(kid for kid, _ in self.entries)

    def key(self, key_id: int) -> bytes | None:
        for kid, key in self.entries:
            if kid == key_id:
                return key
        return None

    def holds(self, key_id: int) -> bool:
        return self.key(key_id) is not None


# ---------------------------------------------------------------------------------------------------- hashing
def canonical_decimal(value: Decimal) -> str:
    """The exact value as plain decimal text with no exponent and no trailing zeros (`1.10` → `1.1`, `1E+2` → `100`).
    Raises ValueError for a non-finite value (a validated bar never has one) and for one the canonical context cannot
    hold exactly (no `decimal` arithmetic error escapes)."""
    if not value.is_finite():
        raise ValueError("a content hash covers finite prices only")
    if value.is_zero():
        return "0"
    if abs(value.adjusted()) > _MAX_ADJUSTED:
        raise ValueError("a price the content hash cannot represent")
    try:
        return format(value.normalize(_CANON_CTX), "f")
    except ArithmeticError as e:
        raise ValueError("a price the content hash cannot represent") from e


def _message(series: str, bar_end_ms: int, o: Decimal, h: Decimal, low: Decimal, c: Decimal) -> bytes:
    parts = (series, str(bar_end_ms), canonical_decimal(o), canonical_decimal(h), canonical_decimal(low),
             canonical_decimal(c))
    return "|".join(parts).encode("utf-8")


def _mac(key: bytes, message: bytes) -> bytes:
    return hmac.new(key, message, hashlib.sha256).digest()[:HASH_BYTES]


def content_hash(keyring: Keyring, series: str, bar_end_ms: int, o: Decimal, h: Decimal, low: Decimal,
                 c: Decimal) -> str:
    """The keyed content hash under the current key: `f"{key_id:02x}"` + 32 hex digits."""
    key = keyring.key(keyring.current_id)
    if key is None:   # impossible for a ring built by Keyring.of
        raise KeyringError("the keyring holds no current key")
    return f"{keyring.current_id:02x}{_mac(key, _message(series, bar_end_ms, o, h, low, c)).hex()}"


def key_id_of(stored_hash: str) -> int | None:
    """The key id of a well-formed hash, else None."""
    if not _HASH_RE.fullmatch(stored_hash):
        return None
    return int(stored_hash[:2], 16)


class HashStatus(str, Enum):
    VALID = "valid"
    MISMATCH = "mismatch"     # the held key gives another hash: the member is corrupt or forged
    MISSING = "missing"       # malformed, or a key id the ring does not hold: never counts as changed


def verify(keyring: Keyring, stored_hash: str, series: str, bar_end_ms: int, o: Decimal, h: Decimal, low: Decimal,
           c: Decimal) -> HashStatus:
    """Check a stored hash under whichever retained key its id names (§A4 check 5). Never raises."""
    kid = key_id_of(stored_hash)
    key = None if kid is None else keyring.key(kid)
    if key is None:
        return HashStatus.MISSING
    try:
        expected = _mac(key, _message(series, bar_end_ms, o, h, low, c)).hex()
    except (ValueError, ArithmeticError):
        return HashStatus.MISMATCH
    return HashStatus.VALID if hmac.compare_digest(expected, stored_hash[2:]) else HashStatus.MISMATCH


class HashComparison(str, Enum):
    SAME = "same"
    CHANGED = "changed"
    INCOMPARABLE = "incomparable"   # different key ids, or a malformed hash: never a correction


def compare_hashes(a: str, b: str) -> HashComparison:
    """Compare two stored hashes; only hashes under the same key id are ever compared (§A13, §B3)."""
    ka, kb = key_id_of(a), key_id_of(b)
    if ka is None or kb is None or ka != kb:
        return HashComparison.INCOMPARABLE
    return HashComparison.SAME if hmac.compare_digest(a, b) else HashComparison.CHANGED


# ---------------------------------------------------------------------------------------------------- record digest
DIGEST_HEX_LEN: Final = 8


def record_digest(text: str) -> str:
    """Unkeyed CRC32 of `text` (a member's `prefix|record|hash`), 8 hex digits (for readers without the key)."""
    return f"{crc32(text.encode('utf-8', 'replace')):08x}"
