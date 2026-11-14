"""cTrader token pair persistence (operations.md §13): KMS-encrypted PostgreSQL row with `token_generation`.

* `begin_refresh` (fenced, leader only): commit a 'refresh in flight' marker for generation N before
  calling the broker; refused when generation N already has one, so no leader refreshes N twice.
* `save_token_pair`: compare-and-set on `token_generation` that is **not** epoch-gated (like the spill
  drain), so a save started before a leader change still lands. Returns the new generation, or None
  when the stored generation moved (the caller keeps the pair in memory and re-reads).
Tokens are opaque encrypted bytes here; nothing in this module logs them.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from src.db.fence import FENCE, FencedTx
from src.db.pg import PgConnection

SQL_TOKEN_GET: Final = (
    "SELECT token_generation, access_token_enc, refresh_token_enc, kms_key_id, expires_at_ms, "
    "refresh_in_flight_generation, saved_at_ms FROM ctrader_tokens WHERE adapter_id = $1"
)
SQL_TOKEN_BEGIN_REFRESH: Final = (
    "UPDATE ctrader_tokens SET refresh_in_flight_generation = $3, refresh_in_flight_epoch = $1, "
    "refresh_started_at_ms = now_ms() WHERE adapter_id = $2 AND token_generation = $3 "
    f"AND refresh_in_flight_generation IS DISTINCT FROM $3 AND {FENCE}"
)
SQL_TOKEN_FIRST_SAVE: Final = (
    "INSERT INTO ctrader_tokens (adapter_id, token_generation, access_token_enc, refresh_token_enc, kms_key_id, "
    "expires_at_ms, saved_at_ms) VALUES ($1, 1, $2, $3, $4, $5, now_ms()) ON CONFLICT (adapter_id) DO NOTHING "
    "RETURNING token_generation"
)
SQL_TOKEN_CAS_SAVE: Final = (
    "UPDATE ctrader_tokens SET token_generation = token_generation + 1, access_token_enc = $3, "
    "refresh_token_enc = $4, kms_key_id = $5, expires_at_ms = $6, refresh_in_flight_generation = NULL, "
    "refresh_in_flight_epoch = NULL, refresh_started_at_ms = NULL, saved_at_ms = now_ms() "
    "WHERE adapter_id = $1 AND token_generation = $2 RETURNING token_generation"
)


@dataclass(frozen=True)
class TokenRow:
    generation: int
    access_token_enc: bytes | None
    refresh_token_enc: bytes | None
    kms_key_id: str | None
    expires_at_ms: int | None
    refresh_in_flight_generation: int | None
    saved_at_ms: int | None = None   # when this generation was saved (`now_ms()` of the save; token age)

    def __repr__(self) -> str:  # never print token material
        return (f"TokenRow(generation={self.generation}, expires_at_ms={self.expires_at_ms}, "
                f"saved_at_ms={self.saved_at_ms})")


async def read_token(conn: PgConnection, adapter_id: str) -> TokenRow | None:
    r = await conn.fetchrow(SQL_TOKEN_GET, adapter_id)
    if r is None:
        return None
    return TokenRow(int(r[0]), r[1], r[2], None if r[3] is None else str(r[3]), None if r[4] is None else int(r[4]),
                    None if r[5] is None else int(r[5]), None if r[6] is None else int(r[6]))


async def begin_refresh(tx: FencedTx, adapter_id: str, generation: int) -> bool:
    """Leader-only, fenced: True when this call recorded the in-flight marker for `generation`."""
    return await tx.write(SQL_TOKEN_BEGIN_REFRESH, adapter_id, generation) > 0


async def save_token_pair(conn: PgConnection, adapter_id: str, expected_generation: int, access_enc: bytes,
                          refresh_enc: bytes, kms_key_id: str, expires_at_ms: int | None) -> int | None:
    """Unfenced CAS on token_generation (expected 0 = no row yet). Returns the new generation or None.

    `expires_at_ms` is None when the broker reported no expiry; the column is nullable and stores NULL."""
    if expected_generation == 0:
        row = await conn.fetchrow(SQL_TOKEN_FIRST_SAVE, adapter_id, access_enc, refresh_enc, kms_key_id, expires_at_ms)
        if row is not None:
            return int(row[0])
    row = await conn.fetchrow(SQL_TOKEN_CAS_SAVE, adapter_id, expected_generation, access_enc, refresh_enc,
                              kms_key_id, expires_at_ms)
    return None if row is None else int(row[0])
