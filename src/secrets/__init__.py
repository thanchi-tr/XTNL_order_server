"""Credential sources and encryption at rest (IMPLEMENTATION_PLAN 4.7; operations.md §13, §16).

* `source`: EnvSecrets / AwsSecretsManager, selected by XTNL_SECRETS_SOURCE.
* `kms`: the `Cipher` protocol and the AWS KMS implementation for the cTrader token row.
* `aws`: instance-role credentials (IMDSv2) and SigV4-signed JSON calls over httpx.
"""
from __future__ import annotations

from src.secrets.aws import AwsCredentials, AwsError, CredentialProvider, Imdsv2Credentials, StaticCredentials
from src.secrets.kms import Cipher, CipherError, KmsCipher, KmsSettings
from src.secrets.source import (
    AwsSecretsManager,
    EnvSecrets,
    SecretsError,
    SecretsSource,
    secrets_source_from_env,
)

__all__ = [
    "AwsCredentials", "AwsError", "AwsSecretsManager", "Cipher", "CipherError", "CredentialProvider", "EnvSecrets",
    "Imdsv2Credentials", "KmsCipher", "KmsSettings", "SecretsError", "SecretsSource", "StaticCredentials",
    "secrets_source_from_env",
]
