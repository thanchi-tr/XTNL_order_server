"""Channel settings from environment variables: missing config disables a channel, never crashes."""
from __future__ import annotations

import httpx

from src.notifications.factory import build_channels
from src.notifications.settings import DeadmanSettings, NotificationSettings, SesSettings, SmtpSettings

SECRET = "s3cr3t-value-never-shown"

SMTP_ENV = {"XTNL_SMTP_HOST": "smtp.example.com", "XTNL_SMTP_FROM": "xtnl@example.com",
            "XTNL_SMTP_USERNAME": "user", "XTNL_SMTP_PASSWORD": SECRET,
            "XTNL_ALERT_EMAIL_TO": "ops@example.com, oncall@example.com"}
SES_ENV = {"XTNL_SES_REGION": "eu-west-2", "XTNL_SES_FROM": "xtnl@example.com",
           "XTNL_SES_ACCESS_KEY_ID": "AKIDEXAMPLE", "XTNL_SES_SECRET_ACCESS_KEY": SECRET,
           "XTNL_ALERT_EMAIL_TO": "ops@example.com"}


def test_empty_env_disables_everything_with_reasons() -> None:
    s = NotificationSettings.from_env({})
    assert s.email is None and s.push is None
    assert any("XTNL_SES_REGION" in i for i in s.issues) and any("XTNL_PUSH_PROVIDER" in i for i in s.issues)
    cs = build_channels(s, httpx.AsyncClient())
    assert cs.channels == () and cs.disabled == ("email", "mobile_push")


def test_smtp_settings_and_secret_not_in_repr() -> None:
    s = NotificationSettings.from_env(SMTP_ENV)
    assert isinstance(s.email, SmtpSettings)
    assert s.email.port == 587 and s.email.security == "starttls"
    assert s.email.recipients == ("ops@example.com", "oncall@example.com")
    assert SECRET not in repr(s) and SECRET not in " ".join(s.issues)


def test_smtp_ssl_default_port_and_bad_values() -> None:
    s = NotificationSettings.from_env({**SMTP_ENV, "XTNL_SMTP_SECURITY": "ssl"})
    assert isinstance(s.email, SmtpSettings) and s.email.port == 465
    bad = NotificationSettings.from_env({**SMTP_ENV, "XTNL_SMTP_PORT": "nope"})
    assert bad.email is None and any("XTNL_SMTP_PORT" in i for i in bad.issues)
    half = NotificationSettings.from_env({k: v for k, v in SMTP_ENV.items() if k != "XTNL_SMTP_PASSWORD"})
    assert half.email is None


def test_smtp_plaintext_only_to_loopback_and_never_with_credentials() -> None:
    remote = NotificationSettings.from_env({**SMTP_ENV, "XTNL_SMTP_SECURITY": "none"})
    assert remote.email is None
    env = {"XTNL_SMTP_HOST": "127.0.0.1", "XTNL_SMTP_FROM": "x@example.com", "XTNL_SMTP_SECURITY": "none",
           "XTNL_ALERT_EMAIL_TO": "ops@example.com"}
    local = NotificationSettings.from_env(env)
    assert isinstance(local.email, SmtpSettings) and local.email.security == "none"


def test_recipient_header_injection_refused() -> None:
    s = NotificationSettings.from_env({**SMTP_ENV, "XTNL_ALERT_EMAIL_TO": "ops@example.com\r\nBcc: x@evil"})
    assert s.email is None and any("XTNL_ALERT_EMAIL_TO" in i for i in s.issues)


def test_ses_preferred_when_both_and_url() -> None:
    s = NotificationSettings.from_env({**SMTP_ENV, **SES_ENV})
    assert isinstance(s.email, SesSettings)
    assert s.email.url == "https://email.eu-west-2.amazonaws.com/v2/email/outbound-emails"
    assert any("using SES" in i for i in s.issues) and SECRET not in repr(s)


def test_ses_needs_keys_and_https_endpoint() -> None:
    no_key = NotificationSettings.from_env({k: v for k, v in SES_ENV.items() if k != "XTNL_SES_ACCESS_KEY_ID"})
    assert no_key.email is None
    http = NotificationSettings.from_env({**SES_ENV, "XTNL_SES_ENDPOINT": "http://ses.example.com"})
    assert http.email is None and any("XTNL_SES_ENDPOINT" in i for i in http.issues)


def test_push_providers() -> None:
    ntfy = NotificationSettings.from_env({"XTNL_PUSH_PROVIDER": "ntfy", "XTNL_PUSH_URL": "https://ntfy.sh/xtnl-abc"})
    assert ntfy.push is not None and ntfy.push.provider == "ntfy" and ntfy.push.host == "ntfy.sh"
    assert "xtnl-abc" not in repr(ntfy)
    po = NotificationSettings.from_env({"XTNL_PUSH_PROVIDER": "pushover", "XTNL_PUSH_TOKEN": SECRET,
                                        "XTNL_PUSH_USER": "ukey"})
    assert po.push is not None and po.push.url.startswith("https://api.pushover.net/")
    assert SECRET not in repr(po)
    assert NotificationSettings.from_env({"XTNL_PUSH_PROVIDER": "pushover"}).push is None
    assert NotificationSettings.from_env({"XTNL_PUSH_PROVIDER": "fcm"}).push is None
    insecure = NotificationSettings.from_env({"XTNL_PUSH_PROVIDER": "ntfy", "XTNL_PUSH_URL": "http://ntfy.sh/t"})
    assert insecure.push is None


def test_build_channels_from_settings() -> None:
    s = NotificationSettings.from_env({**SES_ENV, "XTNL_PUSH_PROVIDER": "ntfy", "XTNL_PUSH_URL": "https://ntfy.sh/t"})
    cs = build_channels(s, httpx.AsyncClient())
    assert [c.name for c in cs.channels] == ["email", "mobile_push"] and cs.disabled == ()
    assert [getattr(c, "backend") for c in cs.channels] == ["ses", "ntfy"]


def test_deadman_settings() -> None:
    d = DeadmanSettings.from_env({"XTNL_DEADMAN_URL": "https://hc.example/ping/abc", "XTNL_DEADMAN_TOKEN": SECRET})
    assert d.host == "hc.example" and SECRET not in repr(d) and "abc" not in repr(d)
    assert DeadmanSettings.from_env({}).url is None
