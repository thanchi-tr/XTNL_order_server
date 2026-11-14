"""Receiver settings: XTNL_RECEIVER_* channel mapping and the outside-eu-west-2 rule (operations.md §9)."""
from __future__ import annotations

import pytest

from src.deadman_receiver.settings import CHANNEL_VARS, ReceiverConfigError, ReceiverSettings, channel_settings
from src.notifications.settings import PushSettings, SesSettings, SmtpSettings

TOKEN = "t" * 32
SES = {"XTNL_RECEIVER_SES_REGION": "ap-southeast-4", "XTNL_RECEIVER_SES_FROM": "deadman@example.invalid",
       "XTNL_RECEIVER_SES_ACCESS_KEY_ID": "AKIDEXAMPLE", "XTNL_RECEIVER_SES_SECRET_ACCESS_KEY": "secret",
       "XTNL_RECEIVER_ALERT_EMAIL_TO": "oncall@example.invalid"}
PUSH = {"XTNL_RECEIVER_PUSH_PROVIDER": "ntfy", "XTNL_RECEIVER_PUSH_URL": "https://ntfy.example.invalid/xtnl-dm"}


def env(**kw: str) -> dict[str, str]:
    base = {"XTNL_RECEIVER_TOKEN": TOKEN, "XTNL_RECEIVER_LISTEN": "127.0.0.1:8443", **SES}
    base.update(kw)
    return base


def test_channel_vars_cover_every_engine_channel_variable() -> None:
    import ast
    import inspect

    import src.notifications.settings as engine_settings

    names = {n.value for n in ast.walk(ast.parse(inspect.getsource(engine_settings)))
             if isinstance(n, ast.Constant) and isinstance(n.value, str)
             and n.value.startswith(("XTNL_SES_", "XTNL_SMTP_", "XTNL_PUSH_", "XTNL_ALERT_EMAIL_TO"))}
    assert {"XTNL_RECEIVER_" + n[len("XTNL_"):] for n in names} == set(CHANNEL_VARS)


def test_receiver_variables_map_onto_the_existing_transports() -> None:
    s = ReceiverSettings.from_env(env(**PUSH))
    assert isinstance(s.channels.email, SesSettings) and s.channels.email.region == "ap-southeast-4"
    assert isinstance(s.channels.push, PushSettings)
    assert (s.host, s.port, s.path, s.test_page_utc, s.repage_s) == ("127.0.0.1", 8443, "/xtnl/heartbeat", (22, 0), 900)
    assert TOKEN not in repr(s)


def test_engine_variables_are_not_read() -> None:
    """The receiver must not pick up the London engine's channel by accident."""
    e = {"XTNL_RECEIVER_TOKEN": TOKEN, "XTNL_SES_REGION": "ap-southeast-4", "XTNL_SES_FROM": "a@example.invalid",
         "XTNL_SES_ACCESS_KEY_ID": "k", "XTNL_SES_SECRET_ACCESS_KEY": "s", "XTNL_ALERT_EMAIL_TO": "b@example.invalid"}
    with pytest.raises(ReceiverConfigError, match="no paging channel"):
        ReceiverSettings.from_env(e)


@pytest.mark.parametrize("bad", [
    {"XTNL_RECEIVER_SES_REGION": "eu-west-2"},
    {"XTNL_RECEIVER_SES_REGION": "EU-WEST-2"},
    {"XTNL_RECEIVER_SES_ENDPOINT": "https://email.eu-west-2.amazonaws.com"},
])
def test_ses_in_eu_west_2_is_refused(bad: dict[str, str]) -> None:
    s = channel_settings({**SES, **bad})
    assert s.email is None and any("eu-west-2" in i for i in s.issues)
    with pytest.raises(ReceiverConfigError):
        ReceiverSettings.from_env(env(**bad))


def test_smtp_and_push_in_eu_west_2_are_refused_but_others_remain() -> None:
    smtp = {"XTNL_RECEIVER_SMTP_HOST": "email-smtp.eu-west-2.amazonaws.com", "XTNL_RECEIVER_SMTP_FROM": "a@example.invalid",
            "XTNL_RECEIVER_ALERT_EMAIL_TO": "b@example.invalid"}
    s = channel_settings({**smtp, "XTNL_RECEIVER_PUSH_PROVIDER": "ntfy",
                          "XTNL_RECEIVER_PUSH_URL": "https://ntfy.eu-west-2.example.invalid/t"})
    assert s.email is None and s.push is None
    ok = channel_settings({**smtp, "XTNL_RECEIVER_SMTP_HOST": "smtp.example.invalid", **PUSH})
    assert isinstance(ok.email, SmtpSettings) and isinstance(ok.push, PushSettings)


@pytest.mark.parametrize(("kw", "match"), [
    ({"XTNL_RECEIVER_TOKEN": "short"}, "XTNL_RECEIVER_TOKEN"),
    ({"XTNL_RECEIVER_LISTEN": "10.8.0.5:8443"}, "XTNL_RECEIVER_TLS_CERT"),
    ({"XTNL_RECEIVER_TLS_CERT": "/x.crt"}, "together"),
    ({"XTNL_RECEIVER_LISTEN": "nonsense"}, "host:port"),
    ({"XTNL_RECEIVER_PATH": "no-slash"}, "XTNL_RECEIVER_PATH"),
    ({"XTNL_RECEIVER_TEST_PAGE_UTC": "25:00"}, "HH:MM"),
    ({"XTNL_RECEIVER_REPAGE_S": "5"}, "XTNL_RECEIVER_REPAGE_S"),
])
def test_bad_settings_are_refused_naming_the_variable(kw: dict[str, str], match: str) -> None:
    with pytest.raises(ReceiverConfigError, match=match) as info:
        ReceiverSettings.from_env(env(**kw))
    assert TOKEN not in str(info.value) and "secret" not in str(info.value)


def test_non_loopback_listener_with_tls_is_accepted() -> None:
    s = ReceiverSettings.from_env(env(XTNL_RECEIVER_LISTEN="10.8.0.5:8443", XTNL_RECEIVER_TLS_CERT="/c.pem",
                                      XTNL_RECEIVER_TLS_KEY="/k.pem", XTNL_RECEIVER_TEST_PAGE_UTC="21:30"))
    assert s.tls and s.host == "10.8.0.5" and s.test_page_utc == (21, 30)


def test_missing_channel_issue_names_receiver_variables() -> None:
    s = channel_settings({**SES})
    assert all("XTNL_RECEIVER_" in i or "eu-west-2" in i for i in s.issues if "XTNL_" in i)
    assert any("XTNL_RECEIVER_PUSH_PROVIDER" in i for i in s.issues)
