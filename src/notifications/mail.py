"""Email transports: SMTP (stdlib smtplib, run in a worker thread) and Amazon SES v2 SendEmail (httpx).

Both implement `src.oversee.notify.AlertTransport`. Errors are raised as `ChannelError` with a short
message that names the failure class / status only: never credentials, never the message body.
"""
from __future__ import annotations

import asyncio
import json
import smtplib
import ssl
from collections.abc import Callable
from datetime import datetime
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from typing import Final, Protocol

import httpx

from src.notifications.errors import ChannelError
from src.notifications.settings import SesSettings, SmtpSettings
from src.notifications.sigv4 import sign_headers
from src.oversee.notify import AlertMessage

MAX_SUBJECT: Final = 200
_PRIORITY: Final = {"info": "3", "high": "2", "critical": "1"}   # X-Priority: 1 = highest


def _clean_header(s: str) -> str:
    return " ".join(s.split())[:MAX_SUBJECT]


def build_email(msg: AlertMessage, sender: str, recipients: tuple[str, ...]) -> EmailMessage:
    em = EmailMessage()
    em["From"] = sender
    em["To"] = ", ".join(recipients)
    em["Subject"] = _clean_header(msg.subject)
    em["Date"] = formatdate(localtime=False, usegmt=True)
    em["Message-ID"] = make_msgid(domain=sender.rsplit("@", 1)[-1])
    em["X-Priority"] = _PRIORITY[msg.severity]
    if msg.outbox_id is not None:
        em["X-XTNL-Alert-Id"] = str(msg.outbox_id)
    em.set_content(msg.body)
    return em


class SmtpConnection(Protocol):
    """The subset of smtplib.SMTP the transport uses (a fake in tests)."""

    def ehlo(self, name: str = "") -> object: ...

    def starttls(self, *, context: ssl.SSLContext | None = None) -> object: ...

    def login(self, user: str, password: str) -> object: ...

    def send_message(self, msg: EmailMessage) -> object: ...

    def quit(self) -> object: ...

    def close(self) -> None: ...


SmtpFactory = Callable[[SmtpSettings, ssl.SSLContext], SmtpConnection]


def default_smtp_factory(s: SmtpSettings, context: ssl.SSLContext) -> SmtpConnection:
    if s.security == "ssl":
        return smtplib.SMTP_SSL(s.host, s.port, timeout=s.timeout_s, context=context)
    return smtplib.SMTP(s.host, s.port, timeout=s.timeout_s)


class SmtpTransport:
    """One SMTP session per alert, in `asyncio.to_thread` so the event loop never blocks on SMTP."""

    backend: Final = "smtp"

    def __init__(self, settings: SmtpSettings, *, factory: SmtpFactory = default_smtp_factory,
                 context: ssl.SSLContext | None = None) -> None:
        self._s = settings
        self._factory = factory
        self._context = context

    def _ctx(self) -> ssl.SSLContext:
        if self._context is None:
            self._context = ssl.create_default_context()
        return self._context

    def _send_blocking(self, em: EmailMessage) -> None:
        s = self._s
        try:
            conn = self._factory(s, self._ctx())
        except (OSError, smtplib.SMTPException) as exc:
            raise ChannelError(f"smtp connect failed: {type(exc).__name__}") from None
        try:
            if s.security == "starttls":
                conn.ehlo()
                conn.starttls(context=self._ctx())
                conn.ehlo()
            if s.username is not None and s.password is not None:
                conn.login(s.username, s.password)
            conn.send_message(em)
        except smtplib.SMTPAuthenticationError as exc:
            raise ChannelError(f"smtp auth rejected ({exc.smtp_code})", auth=True) from None
        except smtplib.SMTPRecipientsRefused:
            raise ChannelError("smtp recipients refused", retryable=False) from None
        except smtplib.SMTPSenderRefused as exc:
            raise ChannelError(f"smtp sender refused ({exc.smtp_code})", retryable=False) from None
        except smtplib.SMTPResponseException as exc:
            raise ChannelError(f"smtp error {exc.smtp_code}", retryable=exc.smtp_code < 500) from None
        except (OSError, smtplib.SMTPException) as exc:
            raise ChannelError(f"smtp failed: {type(exc).__name__}") from None
        finally:
            try:
                conn.quit()
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass

    async def deliver(self, msg: AlertMessage) -> None:
        em = build_email(msg, self._s.sender, self._s.recipients)
        try:
            await asyncio.wait_for(asyncio.to_thread(self._send_blocking, em), self._s.timeout_s * 4)
        except TimeoutError:
            raise ChannelError("smtp timed out") from None


def ses_request_body(msg: AlertMessage, s: SesSettings) -> bytes:
    body: dict[str, object] = {
        "FromEmailAddress": s.sender,
        "Destination": {"ToAddresses": list(s.recipients)},
        "Content": {"Simple": {
            "Subject": {"Data": _clean_header(msg.subject), "Charset": "UTF-8"},
            "Body": {"Text": {"Data": msg.body, "Charset": "UTF-8"}},
        }},
    }
    if s.configuration_set:
        body["ConfigurationSetName"] = s.configuration_set
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


class SesTransport:
    """Amazon SES v2 `POST /v2/email/outbound-emails`, signed with SigV4 (service `ses`)."""

    backend: Final = "ses"

    def __init__(self, settings: SesSettings, client: httpx.AsyncClient,
                 *, clock: Callable[[], datetime] | None = None) -> None:
        self._s = settings
        self._client = client
        self._clock = clock

    async def deliver(self, msg: AlertMessage) -> None:
        s = self._s
        url = s.url
        body = ses_request_body(msg, s)
        headers = sign_headers("POST", url, {"content-type": "application/json"}, body,
                               access_key=s.access_key_id, secret_key=s.secret_access_key, region=s.region,
                               service="ses", now=None if self._clock is None else self._clock(),
                               session_token=s.session_token)
        try:
            resp = await self._client.post(url, content=body, headers=headers, timeout=s.timeout_s)
        except httpx.HTTPError as exc:
            raise ChannelError(f"ses request failed: {type(exc).__name__}") from None
        if resp.status_code == 200:
            return
        code = resp.status_code
        kind = _ses_error_type(resp)
        if code in (401, 403):
            raise ChannelError(f"ses auth rejected ({code} {kind})", auth=True)
        retryable = code == 429 or code >= 500 or kind in ("TooManyRequestsException", "LimitExceededException")
        raise ChannelError(f"ses rejected ({code} {kind})", retryable=retryable)


def _ses_error_type(resp: httpx.Response) -> str:
    """The AWS error type name only (never the message text)."""
    t = resp.headers.get("x-amzn-errortype", "")
    if not t:
        try:
            data = resp.json()
        except ValueError:
            data = None
        if isinstance(data, dict):
            t = str(data.get("__type", "") or data.get("code", ""))
    t = t.split(":", 1)[0].rsplit("#", 1)[-1]
    return "".join(c for c in t if c.isalnum())[:60] or "error"
