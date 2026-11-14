"""KmsTokenStore over src.db.tokens (4.7; operations.md §13): save-before-use through the adapter's
TokenManager, encryption at rest bound to (adapter, field, generation), fenced refresh marker, unfenced CAS."""
from __future__ import annotations

import itertools
from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import pytest

from src.adapters.base import TransientBrokerError
from src.adapters.pepperstone.tokens import RefreshedTokens, TokenManager, TokenPair
from src.db import tokens as T
from src.db.epoch import Epoch
from src.db.fence import SQL_CHECK_EPOCH, EpochMismatch
from src.db.token_store import SQL_TOKEN_SEED_MARKER, KmsTokenStore, token_context
from src.secrets.kms import CipherError

EPOCH = Epoch("cluster-a", 7)
ADAPTER = "pepperstone"
ENV_PAIR = TokenPair("env-access", "env-refresh", None, 0)


class FakeCipher:
    """Opaque blobs; decrypt only with the exact context and key id used to encrypt."""

    key_id = "arn:aws:kms:eu-west-2:000000000000:key/fake"

    def __init__(self) -> None:
        self._ids = itertools.count(1)
        self.sealed: dict[bytes, tuple[dict[str, str], bytes]] = {}
        self.fail = False

    async def encrypt(self, plaintext: bytes, context: Mapping[str, str]) -> bytes:
        if self.fail:
            raise CipherError("kms Encrypt failed: HTTP 400 KMSInvalidStateException")
        blob = b"\x01" + f"blob-{next(self._ids)}".encode()
        self.sealed[blob] = (dict(context), plaintext)
        return blob

    async def decrypt(self, blob: bytes, context: Mapping[str, str], *, key_id: str | None = None) -> bytes:
        entry = self.sealed.get(bytes(blob))
        if self.fail or entry is None or entry[0] != dict(context) or key_id not in (None, self.key_id):
            raise CipherError("kms Decrypt failed: HTTP 400 InvalidCiphertextException")
        return entry[1]


class TokenTable:
    """ctrader_tokens + engine_epoch, executing exactly the SQL the store sends."""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.stored_epoch = EPOCH.token
        self.sql: list[str] = []
        self.now_ms = 1_800_000_000_000          # now_ms() of the database (saved_at_ms of a save)

    async def execute(self, sql: str, *args: Any) -> str:
        self.sql.append(sql)
        if sql == SQL_TOKEN_SEED_MARKER:
            if args[0] != self.stored_epoch or args[1] in self.rows:
                return "INSERT 0 0"
            self.rows[args[1]] = {"gen": 0, "a": None, "r": None, "key": None, "exp": None, "flight": None,
                                  "saved": self.now_ms}
            return "INSERT 0 1"
        if sql == T.SQL_TOKEN_BEGIN_REFRESH:
            epoch, adapter, gen = args
            row = self.rows.get(adapter)
            if epoch != self.stored_epoch or row is None or row["gen"] != gen or row["flight"] == gen:
                return "UPDATE 0"
            row["flight"] = gen
            return "UPDATE 1"
        return "OK"

    async def fetchrow(self, sql: str, *args: Any) -> tuple[Any, ...] | None:
        self.sql.append(sql)
        if sql == SQL_CHECK_EPOCH:
            return (self.stored_epoch,)
        if sql == T.SQL_TOKEN_GET:
            r = self.rows.get(args[0])
            return None if r is None else (r["gen"], r["a"], r["r"], r["key"], r["exp"], r["flight"], r["saved"])
        if sql == T.SQL_TOKEN_FIRST_SAVE:
            adapter, a, rr, key, exp = args
            if adapter in self.rows:
                return None
            self.rows[adapter] = {"gen": 1, "a": a, "r": rr, "key": key, "exp": exp, "flight": None,
                                  "saved": self.now_ms}
            return (1,)
        if sql == T.SQL_TOKEN_CAS_SAVE:
            adapter, expected, a, rr, key, exp = args
            row = self.rows.get(adapter)
            if row is None or row["gen"] != expected:
                return None
            row.update(gen=expected + 1, a=a, r=rr, key=key, exp=exp, flight=None, saved=self.now_ms)
            return (expected + 1,)
        raise AssertionError(f"unexpected SQL {sql[:60]}")

    async def fetch(self, sql: str, *args: Any) -> list[tuple[Any, ...]]:
        raise AssertionError("not used")


class Source:
    def __init__(self, conn: TokenTable) -> None:
        self.conn = conn

    @asynccontextmanager
    async def _acq(self) -> AsyncIterator[Any]:
        yield self.conn

    def acquire(self) -> AbstractAsyncContextManager[Any]:
        return self._acq()


def make_store(table: TokenTable, cipher: FakeCipher, *, epoch: Epoch | None = EPOCH,
               fenced: list[BaseException] | None = None) -> KmsTokenStore:
    return KmsTokenStore(Source(table), ADAPTER, cipher, epoch=lambda: epoch,
                         on_fence_lost=(fenced.append if fenced is not None else None))


def refresher(n: int) -> Any:
    async def refresh(refresh_token: str) -> RefreshedTokens:
        return RefreshedTokens(f"access-{n}", f"refresh-{n}", 1_900_000_000_000 + n)
    return refresh


async def test_refresh_of_the_environment_pair_is_saved_encrypted_before_use() -> None:
    table, cipher = TokenTable(), FakeCipher()
    store = make_store(table, cipher)
    assert await store.load() is None                       # no row: the environment pair stays
    manager = TokenManager(ENV_PAIR, store, lease_check=lambda: None)
    pair = await manager.refresh(refresher(1))
    assert (pair.access_token, pair.generation) == ("access-1", 1)
    assert table.sql.index(SQL_TOKEN_SEED_MARKER) < table.sql.index(T.SQL_TOKEN_BEGIN_REFRESH)
    row = table.rows[ADAPTER]
    assert row["gen"] == 1 and row["key"] == cipher.key_id and row["exp"] == 1_900_000_000_001
    assert b"access-1" not in row["a"] and b"refresh-1" not in row["r"] and row["flight"] is None
    assert cipher.sealed[row["a"]][0] == token_context(ADAPTER, "access", 1)
    # A fresh process (LEADER_INIT) reads the saved pair, not the environment's.
    fresh = TokenManager(ENV_PAIR, make_store(table, cipher))
    loaded = await fresh.load()
    assert (loaded.access_token, loaded.refresh_token, loaded.generation) == ("access-1", "refresh-1", 1)
    # Next refresh: generation 1 -> 2 through the CAS, no new marker row.
    pair2 = await fresh.refresh(refresher(2))
    assert pair2.generation == 2 and table.rows[ADAPTER]["gen"] == 2
    assert "access" not in repr(store) and "access-2" not in repr(pair2)


async def test_ciphertext_cannot_be_replayed_into_another_field_or_generation() -> None:
    table, cipher = TokenTable(), FakeCipher()
    store = make_store(table, cipher)
    await TokenManager(ENV_PAIR, store, lease_check=lambda: None).refresh(refresher(1))
    row = table.rows[ADAPTER]
    row["a"], row["r"] = row["r"], row["a"]                   # swapped fields
    with pytest.raises(TransientBrokerError, match="could not be decrypted"):
        await store.load()
    row["a"], row["r"] = row["r"], row["a"]
    row["gen"] = 5                                            # an old blob under a newer generation
    with pytest.raises(TransientBrokerError, match="generation 5"):
        await store.load()


async def test_generation_marker_row_and_refresh_in_flight() -> None:
    table, cipher = TokenTable(), FakeCipher()
    store = make_store(table, cipher)
    assert await store.begin_refresh(0) is True
    assert await store.load() is None                         # the generation-0 marker holds no pair
    assert await store.begin_refresh(0) is False              # generation 0 already in flight
    assert await store.begin_refresh(3) is False              # not the stored generation


async def test_lost_fence_on_the_refresh_marker_is_reported_and_raised() -> None:
    table, cipher = TokenTable(), FakeCipher()
    fenced: list[BaseException] = []
    store = make_store(table, cipher, fenced=fenced)
    table.stored_epoch = 8                                    # a newer leader bumped the epoch
    with pytest.raises(EpochMismatch):
        await store.begin_refresh(0)
    assert len(fenced) == 1 and isinstance(fenced[0], EpochMismatch)
    with pytest.raises(TransientBrokerError, match="no leader epoch"):
        await make_store(table, cipher, epoch=None).begin_refresh(0)


async def test_save_is_an_unfenced_cas() -> None:
    table, cipher = TokenTable(), FakeCipher()
    store = make_store(table, cipher)
    table.stored_epoch = 99                                   # leader changed mid-refresh: the save still lands
    assert await store.save(0, TokenPair("a1", "r1", None, 0)) == 1
    assert await store.save(0, TokenPair("a2", "r2", None, 0)) is None   # another leader saved first
    loaded = await store.load()
    assert loaded is not None and (loaded.access_token, loaded.expires_at_ms) == ("a1", None)


async def test_kms_failures_are_transient_and_keep_the_pair_in_memory() -> None:
    table, cipher = TokenTable(), FakeCipher()
    store = make_store(table, cipher)
    cipher.fail = True
    with pytest.raises(TransientBrokerError, match="encryption failed"):
        await store.save(0, TokenPair("a", "r", None, 0))
    manager = TokenManager(ENV_PAIR, store, lease_check=lambda: None, save_attempts=2, save_retry_s=0.0)
    with pytest.raises(TransientBrokerError, match="save failed 2 times"):
        await manager.refresh(refresher(1))
    assert manager.pair.access_token == "env-access"          # the unsaved pair is never used
    cipher.fail = False
    pair = await manager.refresh(refresher(9))                # finishes the pending save; no second refresh
    assert (pair.access_token, pair.generation) == ("access-1", 1)
    cipher.fail = True
    with pytest.raises(TransientBrokerError, match="could not be decrypted"):
        await store.load()


async def test_store_read_errors_are_transient() -> None:
    class Down:
        def acquire(self) -> AbstractAsyncContextManager[Any]:
            raise OSError("connection refused")
    store = KmsTokenStore(Down(), ADAPTER, FakeCipher(), epoch=lambda: EPOCH)  # type: ignore[arg-type]
    with pytest.raises(TransientBrokerError, match="OSError"):
        await store.load()


async def test_a_pair_loaded_after_a_restart_carries_the_rows_saved_at_ms() -> None:
    """7.3: `load()` fills `TokenPair.saved_at_ms` from the row, so `xtnl_token_age_seconds` is known right after a
    restart / takeover (it used to stay absent until the next refresh, days later)."""
    table, cipher = TokenTable(), FakeCipher()
    table.now_ms = 1_800_000_000_000
    await TokenManager(ENV_PAIR, make_store(table, cipher), lease_check=lambda: None).refresh(refresher(1))
    table.now_ms = 1_800_000_500_000                 # a later save moves saved_at_ms
    await TokenManager(ENV_PAIR, make_store(table, cipher), lease_check=lambda: None).load()
    restarted = TokenManager(ENV_PAIR, make_store(table, cipher))          # a fresh process, no in-memory time
    loaded = await restarted.load()
    assert loaded.generation == 1 and loaded.saved_at_ms == 1_800_000_000_000
    assert restarted.issued_at_ms == 1_800_000_000_000
    second = await restarted.refresh(refresher(2))
    assert second.generation == 2
    again = await TokenManager(ENV_PAIR, make_store(table, cipher)).load()
    assert again.saved_at_ms == 1_800_000_500_000 and "access" not in repr(again)
