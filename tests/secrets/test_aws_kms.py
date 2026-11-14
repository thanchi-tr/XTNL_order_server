"""AWS plumbing (IMDSv2 credentials, signed JSON calls) and the KMS Cipher (4.7). No network."""
from __future__ import annotations

from datetime import datetime

import httpx
import pytest

from src.secrets.aws import AwsError, AwsJsonClient, Imdsv2Credentials
from src.secrets.kms import FORMAT_KMS_DIRECT, CipherError, KmsCipher, KmsSettings

from tests.secrets.fakes import ACCESS_KEY, SECRET_KEY, CountingCredentials, FakeAws, blob_context

CTX = {"xtnl:adapter": "pepperstone", "xtnl:field": "access", "xtnl:generation": "1"}


# --- IMDSv2 ------------------------------------------------------------------------------------------------
class FakeImds:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.expiration = "2026-11-13T12:00:00Z"
        self.status = 200

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if request.method == "PUT" and path == "/latest/api/token":
            assert request.headers["x-aws-ec2-metadata-token-ttl-seconds"] == "21600"
            return httpx.Response(200, text="imds-token")
        assert request.headers.get("x-aws-ec2-metadata-token") == "imds-token"   # IMDSv2 only
        if path == "/latest/meta-data/iam/security-credentials/":
            return httpx.Response(self.status, text="xtnl-engine-role\n")
        if path == "/latest/meta-data/iam/security-credentials/xtnl-engine-role":
            return httpx.Response(200, json={"Code": "Success", "AccessKeyId": ACCESS_KEY,
                                             "SecretAccessKey": SECRET_KEY, "Token": "tok",
                                             "Expiration": self.expiration})
        return httpx.Response(404)


def test_imdsv2_credentials_are_cached_until_near_expiry() -> None:
    imds = FakeImds()
    exp = datetime.fromisoformat("2026-11-13T12:00:00+00:00").timestamp()
    now = [exp - 3600]
    creds = Imdsv2Credentials(http=httpx.Client(transport=httpx.MockTransport(imds.handle)), clock=lambda: now[0])
    c1 = creds.get()
    assert (c1.access_key_id, c1.session_token, c1.expires_at) == (ACCESS_KEY, "tok", exp)
    assert creds.get() is c1 and creds.fetches == 1
    now[0] = exp - 200                        # inside the 5 min refresh margin
    creds.get()
    assert creds.fetches == 2
    creds.get(force_refresh=True)
    assert creds.fetches == 3
    assert SECRET_KEY not in repr(c1) and SECRET_KEY not in repr(creds)


def test_imdsv2_without_a_role_is_an_aws_error() -> None:
    imds = FakeImds()
    imds.status = 404
    creds = Imdsv2Credentials(http=httpx.Client(transport=httpx.MockTransport(imds.handle)))
    with pytest.raises(AwsError, match="NoInstanceRole"):
        creds.get()


# --- signed JSON calls ----------------------------------------------------------------------------------------
def test_json_call_is_signed_and_refreshes_credentials_once_on_an_auth_error() -> None:
    fake = FakeAws()
    creds = CountingCredentials()
    client = AwsJsonClient("kms", "eu-west-2", "TrentService", creds, http=fake.client())
    fake.fail_next.append((400, "ExpiredTokenException"))
    out = client.call("Encrypt", {"KeyId": "k", "Plaintext": "eA==", "EncryptionContext": {}})
    assert "CiphertextBlob" in out
    assert creds.refreshes == 1
    first, second = fake.requests
    assert first.url == "https://kms.eu-west-2.amazonaws.com/"
    assert "/eu-west-2/kms/aws4_request" in second.headers["authorization"]
    assert second.headers["x-amz-security-token"].endswith("-1")      # the refreshed session token
    assert second.headers["content-type"] == "application/x-amz-json-1.1"
    assert SECRET_KEY not in second.headers["authorization"]


def test_json_call_does_not_retry_a_policy_error() -> None:
    fake = FakeAws()
    creds = CountingCredentials()
    client = AwsJsonClient("kms", "eu-west-2", "TrentService", creds, http=fake.client())
    fake.fail_next.append((400, "AccessDeniedException"))
    with pytest.raises(AwsError) as info:
        client.call("Encrypt", {"KeyId": "k", "Plaintext": "eA=="})
    assert info.value.error_type == "AccessDeniedException" and info.value.status == 400
    assert creds.refreshes == 0 and len(fake.requests) == 1


def test_transport_error_names_only_the_type() -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)
    client = AwsJsonClient("kms", "eu-west-2", "TrentService", CountingCredentials(),
                           http=httpx.Client(transport=httpx.MockTransport(boom)))
    with pytest.raises(AwsError, match="kms Encrypt failed: ConnectError"):
        client.call("Encrypt", {})


def test_region_must_be_a_region_name() -> None:
    with pytest.raises(ValueError):
        AwsJsonClient("kms", "eu-west-2.evil.example/", "TrentService", CountingCredentials())


# --- KMS cipher -----------------------------------------------------------------------------------------------
def kms(fake: FakeAws, key_id: str = "alias/xtnl") -> KmsCipher:
    return KmsCipher(key_id, "eu-west-2", credentials=CountingCredentials(), http=fake.client())


async def test_kms_round_trip_binds_the_encryption_context() -> None:
    fake = FakeAws()
    cipher = kms(fake)
    blob = await cipher.encrypt(b"access-token-material", CTX)
    assert blob[0] == FORMAT_KMS_DIRECT and b"access-token-material" not in blob
    (enc,) = fake.bodies("Encrypt")
    assert enc["KeyId"] == "alias/xtnl" and enc["EncryptionContext"] == CTX
    assert blob_context(fake, blob) == CTX
    assert await cipher.decrypt(blob, CTX, key_id="alias/xtnl") == b"access-token-material"
    (dec,) = fake.bodies("Decrypt")
    assert dec["KeyId"] == "alias/xtnl" and dec["EncryptionContext"] == CTX
    with pytest.raises(CipherError, match="InvalidCiphertextException") as info:
        await cipher.decrypt(blob, {**CTX, "xtnl:field": "refresh"})
    assert "access-token-material" not in str(info.value)


async def test_kms_refuses_unknown_formats_and_oversized_plaintext() -> None:
    cipher = kms(FakeAws())
    with pytest.raises(CipherError, match="format"):
        await cipher.decrypt(b"\x02abc", CTX)
    with pytest.raises(CipherError, match="format"):
        await cipher.decrypt(b"", CTX)
    with pytest.raises(CipherError, match="4096"):
        await cipher.encrypt(b"x" * 4097, CTX)


def test_kms_error_reply_is_a_cipher_error() -> None:
    fake = FakeAws()
    fake.fail_next.append((400, "KMSInvalidStateException"))
    with pytest.raises(CipherError, match="KMSInvalidStateException"):
        kms(fake).encrypt_sync(b"x", CTX)


def test_kms_settings_from_env() -> None:
    assert KmsSettings.from_env({}) is None
    assert KmsSettings.from_env({"XTNL_TOKEN_KMS_KEY_ID": "k", "XTNL_KMS_REGION": "eu-west-2"}) == \
        KmsSettings("k", "eu-west-2")
    for env in ({"XTNL_TOKEN_KMS_KEY_ID": "k"}, {"XTNL_KMS_REGION": "eu-west-2"}):
        with pytest.raises(ValueError, match="must be set together"):
            KmsSettings.from_env(env)
    with pytest.raises(ValueError, match="region"):
        KmsSettings.from_env({"XTNL_TOKEN_KMS_KEY_ID": "k", "XTNL_KMS_REGION": "eu west 2"})
