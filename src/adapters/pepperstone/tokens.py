"""cTrader OAuth token pair as state (operations.md §13, adapter-isolation.md §5).

The pair carries a monotonic ``token_generation``. It is refreshed only by the leader, in a
lease-checked step, and **saved before use** through an injected :class:`TokenStore` (the runtime
backs it with ``src.db.tokens``: ``begin_refresh`` + the ``save_token_pair`` compare-and-set; this
package imports nothing from ``src.db``). Token material never appears in a repr or a log line.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from src.adapters.base import AuthFailedError, TransientBrokerError

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TokenPair:
    access_token: str
    refresh_token: str
    expires_at_ms: int | None
    generation: int = 0       # 0 = not yet saved in a store (the environment's initial pair)
    saved_at_ms: int | None = None   # when this generation was saved (token age); None = unknown

    def __repr__(self) -> str:  # never print token material
        return (f"TokenPair(generation={self.generation}, expires_at_ms={self.expires_at_ms}, "
                f"saved_at_ms={self.saved_at_ms})")

    def expires_within(self, now_ms: int, margin_ms: int) -> bool:
        return self.expires_at_ms is not None and self.expires_at_ms - now_ms <= margin_ms


class TokenStore(Protocol):
    """Persistence of the token pair, matching ``src.db.tokens``.

    - ``load()``: the saved pair with its ``token_generation`` (LEADER_INIT reads it fresh), or None.
      ``saved_at_ms`` should carry the row's ``saved_at_ms`` (the token-age metric reads it).
    - ``begin_refresh(generation)``: commit the 'refresh in flight' marker for ``generation``
      (fenced, leader only). False when generation N already has one (no leader refreshes N twice).
    - ``save(expected_generation, pair)``: un-fenced compare-and-set on ``token_generation``
      (expected 0 = no row yet). Returns the new generation, or None when the stored generation moved.
    """

    async def load(self) -> TokenPair | None: ...

    async def begin_refresh(self, generation: int) -> bool: ...

    async def save(self, expected_generation: int, pair: TokenPair) -> int | None: ...


class InMemoryTokenStore:
    """TokenStore for tests and the probe (same CAS semantics, nothing durable)."""

    def __init__(self, pair: TokenPair | None = None, *, wall_ms: Callable[[], int] | None = None) -> None:
        self.pair = pair
        self.in_flight: int | None = None
        self.saves = 0
        self._wall_ms = wall_ms or _now_ms

    async def load(self) -> TokenPair | None:
        return self.pair

    async def begin_refresh(self, generation: int) -> bool:
        current = 0 if self.pair is None else self.pair.generation
        if current != generation or self.in_flight == generation:
            return False
        self.in_flight = generation
        return True

    async def save(self, expected_generation: int, pair: TokenPair) -> int | None:
        current = 0 if self.pair is None else self.pair.generation
        if current != expected_generation:
            return None
        new_gen = current + 1
        self.pair = TokenPair(pair.access_token, pair.refresh_token, pair.expires_at_ms, new_gen, self._wall_ms())
        self.in_flight = None
        self.saves += 1
        return new_gen


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(frozen=True)
class RefreshedTokens:
    """What the broker returned for a refresh (not yet saved)."""

    access_token: str
    refresh_token: str
    expires_at_ms: int | None

    def __repr__(self) -> str:
        return f"RefreshedTokens(expires_at_ms={self.expires_at_ms})"


Refresher = Callable[[str], Awaitable[RefreshedTokens]]


class TokenManager:
    """Holds the current pair and runs the refresh -> save-before-use cycle."""

    def __init__(self, initial: TokenPair, store: TokenStore | None = None, *,
                 lease_check: Callable[[], None] | None = None, save_attempts: int = 5,
                 save_retry_s: float = 0.5, wall_ms: Callable[[], int] | None = None) -> None:
        self._pair = initial
        self._wall_ms = wall_ms or _now_ms
        self._store = store
        self._lease_check = lease_check
        self._save_attempts = max(1, save_attempts)
        self._save_retry_s = save_retry_s
        self._unsaved: TokenPair | None = None    # refreshed at the broker, save still pending
        self._lock = asyncio.Lock()

    @property
    def pair(self) -> TokenPair:
        return self._pair

    @property
    def issued_at_ms(self) -> int | None:
        """When the pair in use was saved (token age); None when unknown (the environment's initial pair,
        or a stored pair whose store reports no save time)."""
        return self._pair.saved_at_ms

    @property
    def generation(self) -> int:
        """The credential generation (adapter_auth_state) of the pair in use."""
        return self._pair.generation

    def _adopt(self, stored: TokenPair) -> None:
        """Use a stored pair. A store that reports no save time keeps the one already known for that
        same generation (our own save), so the token age does not vanish on a reload."""
        if (stored.saved_at_ms is None and stored.generation == self._pair.generation
                and self._pair.saved_at_ms is not None):
            stored = dataclasses.replace(stored, saved_at_ms=self._pair.saved_at_ms)
        self._pair = stored

    async def load(self) -> TokenPair:
        """LEADER_INIT: read the stored pair fresh; a newer stored generation wins."""
        if self._store is not None and self._unsaved is None:
            stored = await self._store.load()
            if stored is not None and stored.generation >= self._pair.generation:
                self._adopt(stored)
        return self._pair

    async def refresh(self, refresher: Refresher, *, used_generation: int | None = None) -> TokenPair:
        """Refresh the pair at the broker, save it, then (and only then) start using it.

        ``used_generation`` is the generation the caller saw fail; when another task or leader has
        already moved past it, the newer pair is adopted instead of refreshing twice.
        """
        async with self._lock:
            if used_generation is not None and self._pair.generation > used_generation:
                return self._pair
            if self._unsaved is not None:            # broker already rotated it: finish the save
                await self._save(self._unsaved)
                return self._pair
            if self._store is not None:
                stored = await self._store.load()
                if stored is not None and stored.generation > self._pair.generation:
                    self._adopt(stored)               # someone else refreshed: adopt, do not refresh N twice
                    return self._pair
            if self._lease_check is not None:
                self._lease_check()                   # operations.md §2: lease-checked step
            if self._store is not None and not await self._store.begin_refresh(self._pair.generation):
                stored = await self._store.load()
                if stored is not None and stored.generation > self._pair.generation:
                    self._adopt(stored)
                    return self._pair
                raise TransientBrokerError(
                    f"cTrader token refresh for generation {self._pair.generation} already in flight")
            fresh = await refresher(self._pair.refresh_token)
            self._unsaved = TokenPair(fresh.access_token, fresh.refresh_token or self._pair.refresh_token,
                                      fresh.expires_at_ms, self._pair.generation)
            await self._save(self._unsaved)
            return self._pair

    async def _save(self, pending: TokenPair) -> None:
        if self._store is None:
            self._pair = TokenPair(pending.access_token, pending.refresh_token, pending.expires_at_ms,
                                   pending.generation + 1, self._wall_ms())
            self._unsaved = None
            return
        last_exc: Exception | None = None
        for attempt in range(self._save_attempts):
            try:
                new_gen = await self._store.save(pending.generation, pending)
            except Exception as exc:  # noqa: BLE001 - store down: keep the pair in memory and retry
                last_exc = exc
                log.warning("ctrader: token save attempt %d failed: %s", attempt + 1, type(exc).__name__)
                await asyncio.sleep(self._save_retry_s)
                continue
            if new_gen is None:
                # The stored generation moved (another leader saved first): adopt its pair, which
                # supersedes ours; never use an unsaved pair.
                self._unsaved = None
                stored = await self._store.load()
                if stored is not None and stored.generation > pending.generation:
                    log.warning("ctrader: token_generation moved to %d during refresh; adopting it",
                                stored.generation)
                    self._adopt(stored)
                    return
                raise AuthFailedError(
                    "cTrader token_generation compare-and-set failed and no newer pair is stored "
                    f"(expected {pending.generation})")
            # The store stamps its own save time; ours is the fallback when it reports none.
            self._pair = TokenPair(pending.access_token, pending.refresh_token, pending.expires_at_ms, new_gen,
                                   self._wall_ms())
            self._unsaved = None
            return
        raise TransientBrokerError(f"cTrader token save failed {self._save_attempts} times "
                                   f"({type(last_exc).__name__}); the refreshed pair is kept in memory")
