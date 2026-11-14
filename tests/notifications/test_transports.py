"""SMTP (fake smtplib connection), SES v2 and push transports (httpx.MockTransport): no network."""
from __future__ import annotations

import asyncio
import json
import smtplib
import ssl
import threading
import urllib.parse
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.message import EmailMessage

import httpx
import pytest

from src.notifications.errors import ChannelError
from src.notifications.mail import SesTransport, SmtpTransport
from src.notifications.push import NtfyTransport, PushoverTransport
from src.notifications.settings import PushSettings, SesSettings, SmtpSettings
from src.oversee.notify import AlertMessage

SECRET = "s3cr3t-value-never-shown"
MSG = AlertMessage("[XTNL HIGH] SOFT_HALT agg account:A1", "risk 3.2 % > 3.0 %\n\nkind: episode", "high",
                   "episode", 42, "A1")
CTX = ssl.create_default_context()


@dataclass
class FakeSmtp:
    fail_login: bool = False
    fail_send: BaseException | None = None
    calls: list[str] = field(default_factory=list)
    sent: list[EmailMessage] = field(default_factory=list)
    threads: list[str] = field(default_factory=list)

    def ehlo(self, name: str = "") -> object:
        self.calls.append("ehlo")
        return (250, b"ok")

    def starttls(self, *, context: ssl.SSLContext | None = None) -> object:
        self.calls.append("starttls")
        return (220, b"ok")

    def login(self, user: str, password: str) -> object:
        self.calls.append(f"login:{user}")
        if self.fail_login:
            raise smtplib.SMTPAuthenticationError(535, b"bad credentials")
        return (235, b"ok")

    def send_message(self, msg: EmailMessage) -> object:
        self.threads.append(threading.current_thread().name)
        if self.fail_send is not None:
            raise self.fail_send
        self.sent.append(msg)
        return {}

    def quit(self) -> object:
        self.calls.append("quit")
        return (221, b"bye")

    def close(self) -> None:
        self.calls.append("close")


def smtp_settings(**kw: object) -> SmtpSettings:
    base: dict[str, object] = {"host": "smtp.example.com", "port": 587, "sender": "xtnl@example.com",
                               "recipients": ("ops@example.com",), "username": "user", "password": SECRET}
    base.update(kw)
    return SmtpSettings(**base)  # type: ignore[arg-type]


def smtp(fake: FakeSmtp, **kw: object) -> SmtpTransport:
    return SmtpTransport(smtp_settings(**kw), factory=lambda s, c: fake, context=CTX)


async def test_smtp_starttls_login_send_in_worker_thread() -> None:
    fake = FakeSmtp()
    await smtp(fake).deliver(MSG)
    assert fake.calls == ["ehlo", "starttls", "ehlo", "login:user", "quit"]
    em = fake.sent[0]
    assert em["Subject"] == MSG.subject and em["To"] == "ops@example.com" and em["X-Priority"] == "2"
    assert em["X-XTNL-Alert-Id"] == "42" and "risk 3.2 %" in em.get_content()
    assert fake.threads[0] != threading.current_thread().name    # never on the event-loop thread


async def test_smtp_auth_error_is_flagged_and_hides_secret() -> None:
    fake = FakeSmtp(fail_login=True)
    with pytest.raises(ChannelError) as ei:
        await smtp(fake).deliver(MSG)
    assert ei.value.auth and SECRET not in str(ei.value) and "quit" in fake.calls


async def test_smtp_errors_are_classified() -> None:
    refused = FakeSmtp(fail_send=smtplib.SMTPRecipientsRefused({"ops@example.com": (550, b"no")}))
    with pytest.raises(ChannelError) as ei:
        await smtp(refused).deliver(MSG)
    assert not ei.value.retryable
    down = FakeSmtp(fail_send=ConnectionResetError("reset"))
    with pytest.raises(ChannelError) as ei2:
        await smtp(down).deliver(MSG)
    assert ei2.value.retryable and "ConnectionResetError" in str(ei2.value)
    busy = FakeSmtp(fail_send=smtplib.SMTPDataError(451, b"try later"))
    with pytest.raises(ChannelError) as ei3:
        await smtp(busy).deliver(MSG)
    assert ei3.value.retryable and "451" in str(ei3.value)

    def boom(s: SmtpSettings, c: ssl.SSLContext) -> FakeSmtp:
        raise OSError("connection refused")

    with pytest.raises(ChannelError, match="connect failed"):
        await SmtpTransport(smtp_settings(), factory=boom, context=CTX).deliver(MSG)


async def test_smtp_loopback_plain_skips_starttls_and_login() -> None:
    fake = FakeSmtp()
    await smtp(fake, host="127.0.0.1", security="none", username=None, password=None).deliver(MSG)
    assert fake.calls == ["quit"] and len(fake.sent) == 1


async def test_smtp_does_not_block_event_loop() -> None:
    gate = threading.Event()

    class Slow(FakeSmtp):
        def send_message(self, msg: EmailMessage) -> object:
            gate.wait(2)
            return super().send_message(msg)

    slow = Slow()
    task = asyncio.create_task(smtp(slow).deliver(MSG))
    await asyncio.sleep(0.02)
    assert not task.done()          # the loop keeps running while SMTP is blocked
    gate.set()
    await task
    assert len(slow.sent) == 1


def ses_settings(**kw: object) -> SesSettings:
    base: dict[str, object] = {"region": "eu-west-2", "sender": "xtnl@example.com",
                               "recipients": ("ops@example.com",), "access_key_id": "AKIDEXAMPLE",
                               "secret_access_key": SECRET, "configuration_set": "alerts"}
    base.update(kw)
    return SesSettings(**base)  # type: ignore[arg-type]


def mock_client(handler: object) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))  # type: ignore[arg-type]


async def test_ses_send_email_signed_request() -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(200, json={"MessageId": "m-1"})

    t = SesTransport(ses_settings(session_token="TOK"), mock_client(handler),
                     clock=lambda: datetime(2026, 11, 13, 8, 0, 0, tzinfo=UTC))
    await t.deliver(MSG)
    req = seen[0]
    assert req.method == "POST"
    assert str(req.url) == "https://email.eu-west-2.amazonaws.com/v2/email/outbound-emails"
    auth = req.headers["authorization"]
    assert auth.startswith("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261113/eu-west-2/ses/aws4_request")
    assert SECRET not in auth and req.headers["x-amz-security-token"] == "TOK"
    assert req.headers["x-amz-date"] == "20261113T080000Z"
    body = json.loads(req.content)
    assert body["FromEmailAddress"] == "xtnl@example.com"
    assert body["Destination"] == {"ToAddresses": ["ops@example.com"]}
    assert body["Content"]["Simple"]["Subject"]["Data"] == MSG.subject
    assert body["Content"]["Simple"]["Body"]["Text"]["Data"] == MSG.body
    assert body["ConfigurationSetName"] == "alerts"


@pytest.mark.parametrize(("status", "etype", "auth", "retryable"), [
    (403, "AccessDeniedException", True, True),
    (429, "TooManyRequestsException", False, True),
    (400, "MessageRejected", False, False),
    (503, "ServiceUnavailable", False, True),
])
async def test_ses_errors(status: int, etype: str, auth: bool, retryable: bool) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(status, headers={"x-amzn-ErrorType": f"{etype}:http://internal"},
                              json={"message": f"secret detail {SECRET}"})

    with pytest.raises(ChannelError) as ei:
        await SesTransport(ses_settings(), mock_client(handler)).deliver(MSG)
    assert ei.value.auth is auth and ei.value.retryable is retryable
    assert etype in str(ei.value) and SECRET not in str(ei.value)


async def test_ses_network_error() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down", request=req)

    with pytest.raises(ChannelError, match="ConnectError"):
        await SesTransport(ses_settings(), mock_client(handler)).deliver(MSG)


async def test_ntfy_post_headers_and_body() -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(200, json={"id": "x"})

    s = PushSettings("ntfy", "https://ntfy.example/xtnl-topic", token=SECRET)
    await NtfyTransport(s, mock_client(handler)).deliver(
        AlertMessage("[XTNL CRITICAL] HALT HARD – daily loss", "body", "critical", "trip"))
    req = seen[0]
    assert str(req.url) == "https://ntfy.example/xtnl-topic" and req.content == b"body"
    assert req.headers["priority"] == "5" and req.headers["authorization"] == f"Bearer {SECRET}"
    assert req.headers["title"].startswith("=?UTF-8?B?")      # non-ASCII title, RFC 2047 encoded


async def test_ntfy_failure_hides_url() -> None:
    s = PushSettings("ntfy", "https://ntfy.example/xtnl-topic")
    with pytest.raises(ChannelError) as ei:
        await NtfyTransport(s, mock_client(lambda r: httpx.Response(502))).deliver(MSG)
    assert ei.value.retryable and "xtnl-topic" not in str(ei.value)
    with pytest.raises(ChannelError) as ei2:
        await NtfyTransport(s, mock_client(lambda r: httpx.Response(401))).deliver(MSG)
    assert ei2.value.auth


async def test_pushover_form_and_emergency_priority() -> None:
    seen: list[dict[str, list[str]]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(urllib.parse.parse_qs(req.content.decode()))
        return httpx.Response(200, json={"status": 1, "request": "r"})

    s = PushSettings("pushover", "https://api.pushover.net/1/messages.json", token="apptoken", user="userkey")
    t = PushoverTransport(s, mock_client(handler))
    await t.deliver(MSG)
    await t.deliver(AlertMessage("HALT HARD", "x" * 5000, "critical", "trip"))
    assert seen[0]["token"] == ["apptoken"] and seen[0]["user"] == ["userkey"] and seen[0]["priority"] == ["1"]
    assert seen[1]["priority"] == ["2"] and seen[1]["retry"] == ["60"] and seen[1]["expire"] == ["3600"]
    assert len(seen[1]["message"][0]) == 1024


async def test_pushover_rejections() -> None:
    s = PushSettings("pushover", "https://api.pushover.net/1/messages.json", token="t", user="u")
    bad = PushoverTransport(s, mock_client(lambda r: httpx.Response(400, json={"status": 0})))
    with pytest.raises(ChannelError) as ei:
        await bad.deliver(MSG)
    assert not ei.value.retryable
    weird = PushoverTransport(s, mock_client(lambda r: httpx.Response(200, json={"status": 0})))
    with pytest.raises(ChannelError):
        await weird.deliver(MSG)
    with pytest.raises(ValueError):
        PushoverTransport(PushSettings("pushover", "https://api.pushover.net/1/messages.json"), httpx.AsyncClient())
