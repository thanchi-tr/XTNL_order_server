"""SigV4 signing against the AWS Signature Version 4 test-suite vectors."""
from __future__ import annotations

from datetime import UTC, datetime

from src.notifications.sigv4 import sign_headers

NOW = datetime(2015, 8, 30, 12, 36, 0, tzinfo=UTC)


def _sig(method: str, url: str) -> str:
    h = sign_headers(method, url, {}, b"", access_key="AKIDEXAMPLE",
                     secret_key="wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", region="us-east-1",
                     service="service", now=NOW)
    return h["authorization"]


def test_get_vanilla() -> None:
    assert _sig("GET", "https://example.amazonaws.com/") == (
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, "
        "SignedHeaders=host;x-amz-date, "
        "Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31")


def test_query_order_and_post() -> None:
    assert _sig("GET", "https://example.amazonaws.com/?Param2=value2&Param1=value1").endswith(
        "b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500")
    assert _sig("POST", "https://example.amazonaws.com/?Param1=value1").endswith(
        "28038455d6de14eafc1f9222cf5aa6f1a96197d7deb8263271d420d138af7f11")


def test_session_token_is_signed() -> None:
    h = sign_headers("POST", "https://email.eu-west-2.amazonaws.com/v2/email/outbound-emails",
                     {"content-type": "application/json"}, b"{}", access_key="AK", secret_key="SK",
                     region="eu-west-2", service="ses", now=NOW, session_token="TOKEN")
    assert h["x-amz-security-token"] == "TOKEN" and h["x-amz-date"] == "20150830T123600Z"
    assert "SignedHeaders=content-type;host;x-amz-date;x-amz-security-token" in h["authorization"]
    assert "SK" not in h["authorization"]
