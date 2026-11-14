"""In-process fakes for AWS (no network): a KMS and a Secrets Manager behind httpx.MockTransport, and a
credentials provider that counts refreshes."""
from __future__ import annotations

import base64
import json
import uuid
from collections.abc import Mapping
from typing import Any

import httpx

from src.secrets.aws import AwsCredentials

ACCESS_KEY = "AKIAFAKEFAKEFAKE"
SECRET_KEY = "aws-SeCrEt-key-123"
SESSION_TOKEN = "aws-SeSsIoN-token-456"


class CountingCredentials:
    def __init__(self) -> None:
        self.calls = 0
        self.refreshes = 0

    def get(self, *, force_refresh: bool = False) -> AwsCredentials:
        self.calls += 1
        if force_refresh:
            self.refreshes += 1
        return AwsCredentials(ACCESS_KEY, SECRET_KEY, f"{SESSION_TOKEN}-{self.refreshes}")


def _error(status: int, kind: str) -> httpx.Response:
    return httpx.Response(status, json={"__type": kind, "message": "fake"})


class FakeAws:
    """One handler for both services (dispatch on X-Amz-Target)."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.blobs: dict[bytes, tuple[str, dict[str, str], bytes]] = {}   # blob -> (key, context, plaintext)
        self.fail_next: list[tuple[int, str]] = []
        self.secret: dict[str, Any] | str = {}
        self.version = "v1"

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handle))

    def bodies(self, action: str) -> list[dict[str, Any]]:
        return [json.loads(r.content) for r in self.requests if r.headers["x-amz-target"].endswith("." + action)]

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        assert request.headers["authorization"].startswith("AWS4-HMAC-SHA256 Credential=")
        if self.fail_next:
            status, kind = self.fail_next.pop(0)
            return _error(status, kind)
        target = request.headers["x-amz-target"]
        body = json.loads(request.content)
        if target == "TrentService.Encrypt":
            blob = uuid.uuid4().bytes + b"-kms-blob"
            self.blobs[blob] = (body["KeyId"], dict(body.get("EncryptionContext") or {}),
                                base64.b64decode(body["Plaintext"]))
            return httpx.Response(200, json={"CiphertextBlob": base64.b64encode(blob).decode(),
                                             "KeyId": body["KeyId"]})
        if target == "TrentService.Decrypt":
            blob = base64.b64decode(body["CiphertextBlob"])
            entry = self.blobs.get(blob)
            if entry is None or entry[1] != dict(body.get("EncryptionContext") or {}):
                return _error(400, "InvalidCiphertextException")
            if body.get("KeyId") not in (None, entry[0]):
                return _error(400, "IncorrectKeyException")
            return httpx.Response(200, json={"Plaintext": base64.b64encode(entry[2]).decode(), "KeyId": entry[0]})
        if target == "secretsmanager.GetSecretValue":
            text = self.secret if isinstance(self.secret, str) else json.dumps(self.secret)
            return httpx.Response(200, json={"ARN": "arn:fake", "Name": body["SecretId"], "VersionId": self.version,
                                             "SecretString": text})
        return _error(400, "UnknownOperationException")


def blob_context(fake: FakeAws, blob: bytes) -> Mapping[str, str]:
    return fake.blobs[blob[1:]][1]
