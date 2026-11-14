"""4.9 spec: `save_token_pair(expires_at_ms: int | None)`; an unknown expiry is stored as NULL, with no cast
in the KMS token store."""
from __future__ import annotations

import inspect
from collections.abc import Mapping

from src.adapters.pepperstone.tokens import TokenPair
from src.db import token_store as TS
from src.db import tokens as T
from tests.db.conftest import FakeConn, FakeSource


class PlainCipher:
    key_id = "arn:aws:kms:eu-west-2:000000000000:key/fake"

    async def encrypt(self, plaintext: bytes, context: Mapping[str, str]) -> bytes:
        return b"sealed:" + context["xtnl:field"].encode()

    async def decrypt(self, blob: bytes, context: Mapping[str, str], *, key_id: str | None = None) -> bytes:
        raise AssertionError("not used")


def test_save_token_pair_types_expiry_as_optional() -> None:
    ann = inspect.signature(T.save_token_pair).parameters["expires_at_ms"].annotation
    assert ann in ("int | None", int | None)
    assert "cast(" not in inspect.getsource(TS)


async def test_unknown_expiry_is_saved_as_null(conn: FakeConn) -> None:
    conn.returns(T.SQL_TOKEN_FIRST_SAVE, (1,))
    assert await T.save_token_pair(conn, "pepperstone", 0, b"a", b"r", "k", None) == 1
    (args,) = conn.args_of(T.SQL_TOKEN_FIRST_SAVE)
    assert args[-1] is None
    conn.returns(T.SQL_TOKEN_CAS_SAVE, (2,))
    assert await T.save_token_pair(conn, "pepperstone", 1, b"a", b"r", "k", 1_900_000_000_000) == 2
    assert conn.args_of(T.SQL_TOKEN_CAS_SAVE)[0][-1] == 1_900_000_000_000


async def test_store_passes_a_none_expiry_through(conn: FakeConn) -> None:
    store = TS.KmsTokenStore(FakeSource(conn), "pepperstone", PlainCipher(), epoch=lambda: None)  # type: ignore[arg-type]
    conn.returns(T.SQL_TOKEN_CAS_SAVE, (4,))
    assert await store.save(3, TokenPair("acc", "ref", None, 3)) == 4
    (args,) = conn.args_of(T.SQL_TOKEN_CAS_SAVE)
    assert args[0:2] == ("pepperstone", 3) and args[-1] is None
