"""Encryption of the cTrader token pair at rest (operations.md §13: a KMS-encrypted PostgreSQL row).

`Cipher` is the pluggable protocol the token store seals with. Every call binds the ciphertext to an
**encryption context** (adapter, field, token generation), so a blob copied into another row, another
field or an older generation does not decrypt.

`KmsCipher` calls AWS KMS `Encrypt` / `Decrypt` directly over httpx + SigV4 (src/secrets/aws.py) with
the instance role; the token pair is far below the 4 KiB direct-encryption limit. A local-key AES-GCM
envelope (KMS `GenerateDataKey` + local AES) would need the `cryptography` package, which is not a
project dependency, so it is not offered. Stored blobs carry a one-byte format tag (`0x01` = direct
KMS ciphertext) so an envelope format can be added later next to it.

    XTNL_TOKEN_KMS_KEY_ID  key id, key ARN or alias ARN used to encrypt (decrypt uses the key id stored
                           in the row, so a key change still reads older rows)
    XTNL_KMS_REGION        the key's region

Plaintext never appears in a log line, a repr or an exception message.
"""
from __future__ import annotations

import asyncio
import base64
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Protocol

import httpx

from src.secrets.aws import AwsError, AwsJsonClient, CredentialProvider

KEY_ID_VAR: Final = "XTNL_TOKEN_KMS_KEY_ID"
REGION_VAR: Final = "XTNL_KMS_REGION"
FORMAT_KMS_DIRECT: Final = 0x01
MAX_PLAINTEXT: Final = 4096


class CipherError(RuntimeError):
    """Sealing or opening failed (names the operation and the cause type, never the material)."""


class Cipher(Protocol):
    @property
    def key_id(self) -> str:
        """The key new blobs are sealed with (stored in the row as `kms_key_id`)."""
        ...

    async def encrypt(self, plaintext: bytes, context: Mapping[str, str]) -> bytes: ...

    async def decrypt(self, blob: bytes, context: Mapping[str, str], *, key_id: str | None = None) -> bytes: ...


@dataclass(frozen=True)
class KmsSettings:
    key_id: str
    region: str

    @staticmethod
    def from_env(environ: Mapping[str, str] | None = None) -> KmsSettings | None:
        """None when neither variable is set; ValueError (names only) when only one is, or a bad region."""
        e = os.environ if environ is None else environ
        key_id = (e.get(KEY_ID_VAR) or "").strip()
        region = (e.get(REGION_VAR) or "").strip()
        if not key_id and not region:
            return None
        if not key_id or not region:
            raise ValueError(f"{KEY_ID_VAR} and {REGION_VAR} must be set together")
        if not region.replace("-", "").isalnum():
            raise ValueError(f"{REGION_VAR} is not a region name")
        return KmsSettings(key_id, region)


class KmsCipher:
    """`Cipher` over AWS KMS Encrypt / Decrypt (symmetric key, SYMMETRIC_DEFAULT)."""

    def __init__(self, key_id: str, region: str, *, credentials: CredentialProvider,
                 http: httpx.Client | None = None, endpoint: str | None = None, timeout_s: float = 5.0) -> None:
        if not key_id:
            raise ValueError("KMS key id is required")
        self._key_id = key_id
        self._client = AwsJsonClient("kms", region, "TrentService", credentials, http=http, endpoint=endpoint,
                                     timeout_s=timeout_s)

    def __repr__(self) -> str:
        return f"KmsCipher(region={self._client.region!r})"

    @property
    def key_id(self) -> str:
        return self._key_id

    def close(self) -> None:
        self._client.close()

    def encrypt_sync(self, plaintext: bytes, context: Mapping[str, str]) -> bytes:
        if len(plaintext) > MAX_PLAINTEXT:
            raise CipherError("KMS Encrypt refused: plaintext exceeds 4096 bytes")
        try:
            out = self._client.call("Encrypt", {
                "KeyId": self._key_id, "Plaintext": base64.b64encode(plaintext).decode("ascii"),
                "EncryptionContext": dict(context)})
        except AwsError as exc:
            raise CipherError(str(exc)) from None
        blob = out.get("CiphertextBlob")
        if not isinstance(blob, str):
            raise CipherError("KMS Encrypt returned no CiphertextBlob")
        return bytes([FORMAT_KMS_DIRECT]) + base64.b64decode(blob)

    def decrypt_sync(self, blob: bytes, context: Mapping[str, str], *, key_id: str | None = None) -> bytes:
        if not blob or blob[0] != FORMAT_KMS_DIRECT:
            raise CipherError("unknown token blob format")
        body: dict[str, object] = {"CiphertextBlob": base64.b64encode(blob[1:]).decode("ascii"),
                                   "EncryptionContext": dict(context)}
        if key_id:
            body["KeyId"] = key_id
        try:
            out = self._client.call("Decrypt", body)
        except AwsError as exc:
            raise CipherError(str(exc)) from None
        plain = out.get("Plaintext")
        if not isinstance(plain, str):
            raise CipherError("KMS Decrypt returned no Plaintext")
        return base64.b64decode(plain)

    async def encrypt(self, plaintext: bytes, context: Mapping[str, str]) -> bytes:
        return await asyncio.to_thread(self.encrypt_sync, plaintext, context)

    async def decrypt(self, blob: bytes, context: Mapping[str, str], *, key_id: str | None = None) -> bytes:
        return await asyncio.to_thread(self.decrypt_sync, blob, context, key_id=key_id)
