"""Commutative state digest (tier-persistence.md §1): XOR of SHA-256(entity_key || version || checksum).

PostgreSQL stores the same per-row hash (digest_hash), so both tiers' digests can be compared.
"""
from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import Final

DIGEST_HEX_LEN: Final = 64
ZERO_DIGEST: Final = "0" * DIGEST_HEX_LEN


def entity_hash(entity_key: str, version: str, checksum: str) -> str:
    """Lower-case hex SHA-256 of key || version || checksum."""
    return hashlib.sha256(f"{entity_key}{version}{checksum}".encode("utf-8")).hexdigest()


def xor_hex(a: str, b: str) -> str:
    """XOR two 256-bit lower-case hex strings (same algorithm as apply_state.lua)."""
    if len(a) != DIGEST_HEX_LEN or len(b) != DIGEST_HEX_LEN:
        raise ValueError("digest operands must be 64 hex characters")
    return f"{int(a, 16) ^ int(b, 16):064x}"


def fold(hashes: Iterable[str]) -> str:
    """XOR of many entity hashes (the full digest of a set of current rows)."""
    acc = 0
    for h in hashes:
        acc ^= int(h, 16)
    return f"{acc:064x}"
