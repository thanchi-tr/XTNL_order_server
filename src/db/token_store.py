"""KMS-encrypted cTrader `TokenStore` over `src.db.tokens` (operations.md §13; IMPLEMENTATION_PLAN 4.7).

Implements the adapter-side `TokenStore` protocol (src/adapters/pepperstone/tokens.py) for one adapter:

* `load()`: `read_token` (unfenced read) then decrypt both tokens. No row, or the generation-0 marker row,
  is None (the environment's initial pair stays in use). The pair carries the row's `saved_at_ms`, so the
  token-age metric (`xtnl_token_age_seconds`) is known right after a restart, not only after the next refresh.
* `begin_refresh(N)`: a fenced READ COMMITTED transaction with this node's epoch. For N = 0 (the initial
  pair was never saved) it first inserts the generation-0 marker row (fenced, ON CONFLICT DO NOTHING),
  so `SQL_TOKEN_BEGIN_REFRESH` has a row to mark. A lost fence (`EpochMismatch`) is reported through
  `on_fence_lost` (the runtime hard-exits) and re-raised.
* `save(N, pair)`: encrypt, then the **unfenced** `save_token_pair` compare-and-set (named exception 3),
  so a save started before a leader change still lands. Returns the new generation or None.

Each token is sealed with the encryption context {adapter, field, generation it is saved as}, so a
ciphertext cannot be replayed into another field, adapter or generation. Store and KMS failures surface
as `TransientBrokerError` (SM4 backs off; they are not credential errors). Nothing here logs a token.
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Final

from src.adapters.base import TransientBrokerError
from src.adapters.pepperstone.tokens import TokenPair
from src.db.epoch import Epoch, epoch_transaction
from src.db.fence import FENCE, EpochMismatch
from src.db.pg import ConnectionSource
from src.db.tokens import begin_refresh, read_token, save_token_pair
from src.secrets.kms import Cipher, CipherError

log = logging.getLogger(__name__)

SQL_TOKEN_SEED_MARKER: Final = (
    "INSERT INTO ctrader_tokens (adapter_id, token_generation) "
    f"SELECT $2::text, 0 WHERE {FENCE} ON CONFLICT (adapter_id) DO NOTHING"
)


def token_context(adapter_id: str, field_name: str, generation: int) -> dict[str, str]:
    """KMS encryption context for one sealed token."""
    return {"xtnl:table": "ctrader_tokens", "xtnl:adapter": adapter_id, "xtnl:field": field_name,
            "xtnl:generation": str(generation)}


class KmsTokenStore:
    """TokenStore for one cTrader adapter: PostgreSQL row + `Cipher` (KMS)."""

    def __init__(self, source: ConnectionSource, adapter_id: str, cipher: Cipher, *,
                 epoch: Callable[[], Epoch | None],
                 on_fence_lost: Callable[[BaseException], None] | None = None) -> None:
        self._source = source
        self._adapter_id = adapter_id
        self._cipher = cipher
        self._epoch = epoch
        self._on_fence_lost = on_fence_lost

    def __repr__(self) -> str:
        return f"KmsTokenStore(adapter_id={self._adapter_id!r})"

    async def load(self) -> TokenPair | None:
        try:
            async with self._source.acquire() as conn:
                row = await read_token(conn, self._adapter_id)
        except Exception as exc:
            raise TransientBrokerError(f"cTrader token row read failed: {type(exc).__name__}") from None
        if row is None or row.generation <= 0 or row.access_token_enc is None or row.refresh_token_enc is None:
            return None
        try:
            access = await self._cipher.decrypt(bytes(row.access_token_enc),
                                                token_context(self._adapter_id, "access", row.generation),
                                                key_id=row.kms_key_id)
            refresh = await self._cipher.decrypt(bytes(row.refresh_token_enc),
                                                 token_context(self._adapter_id, "refresh", row.generation),
                                                 key_id=row.kms_key_id)
        except CipherError as exc:
            raise TransientBrokerError(f"cTrader token row (generation {row.generation}) could not be "
                                       f"decrypted: {exc}") from None
        return TokenPair(access.decode("utf-8"), refresh.decode("utf-8"), row.expires_at_ms, row.generation,
                         saved_at_ms=row.saved_at_ms)

    async def begin_refresh(self, generation: int) -> bool:
        epoch = self._epoch()
        if epoch is None:
            raise TransientBrokerError("no leader epoch: cTrader token refresh refused")
        try:
            async with self._source.acquire() as conn:
                async with epoch_transaction(conn, epoch) as tx:
                    if generation == 0:
                        await tx.write(SQL_TOKEN_SEED_MARKER, self._adapter_id)
                    return await begin_refresh(tx, self._adapter_id, generation)
        except EpochMismatch as exc:
            log.critical("ctrader token refresh: fence lost (%s)", exc)
            if self._on_fence_lost is not None:
                self._on_fence_lost(exc)
            raise

    async def save(self, expected_generation: int, pair: TokenPair) -> int | None:
        new_gen = expected_generation + 1
        try:
            access = await self._cipher.encrypt(pair.access_token.encode("utf-8"),
                                                token_context(self._adapter_id, "access", new_gen))
            refresh = await self._cipher.encrypt(pair.refresh_token.encode("utf-8"),
                                                 token_context(self._adapter_id, "refresh", new_gen))
        except CipherError as exc:
            raise TransientBrokerError(f"cTrader token encryption failed: {exc}") from None
        async with self._source.acquire() as conn:
            # An unknown expiry (None) is stored as NULL in the nullable expires_at_ms column.
            return await save_token_pair(conn, self._adapter_id, expected_generation, access, refresh,
                                         self._cipher.key_id, pair.expires_at_ms)
