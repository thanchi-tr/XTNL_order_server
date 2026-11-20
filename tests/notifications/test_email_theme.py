"""The XTNL theme of the email HTML part (`src/notifications/email_theme.py`, `XTNL_EMAIL_THEME`), the ARM levels
block with pips from the entry (owner decision 2026-11-21, `.claude/rules/riskmgr.md` rule 5) and the relay's
send-time point-size read."""
from __future__ import annotations

import asyncio
import importlib.util
import re
import sys
from collections.abc import Mapping, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from src.notifications import email_html, email_html_arm, email_theme
from src.notifications.arm_relay import ArmRelay
from src.notifications.email_html import classify, render_alert_html
from src.notifications.email_html_arm import render_arm_html
from src.notifications.email_theme import PipUnit, pip_distance, pip_unit_from, ticket
from src.notifications.factory import build_channels
from src.notifications.mail import SmtpTransport, build_email
from src.notifications.settings import EMAIL_THEME_VAR, NotificationSettings
from src.oversee.notify import AlertMessage, OutboxRow, render_message
from src.riskmgr.render import RenderSettings

D = Decimal
ROOT = Path(__file__).resolve().parents[2]
NODE = lambda: ("node-t", "77")  # noqa: E731
FX = PipUnit(D("0.0001"), "pips")


def _load_preview(name: str) -> Any:
    path = ROOT / "docs" / "design" / "notifications" / "gen_preview.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


class _Units(dict[tuple[str, str], PipUnit]):
    def get(self, key: tuple[str, str], default: PipUnit | None = None) -> PipUnit | None:  # type: ignore[override]
        return FX


PREVIEW = _load_preview("notif_gen_preview_xtnl")
PREVIEW.render_alert_html = lambda msg, footer=None: render_alert_html(msg, footer, theme="xtnl")
PREVIEW.render_arm_html = lambda body, **kw: render_arm_html(body, theme="xtnl", pip_units=_Units(), **kw)
SAMPLES = PREVIEW.samples()


def _alert(kind: str, sev: str, title: str, body: str, episode: tuple[str, str, str, int, int] | None = None
           ) -> AlertMessage:
    return render_message(OutboxRow(kind, sev, title, body, 1_795_147_261_000, episode=episode,  # type: ignore[arg-type]
                                    account_id="ACC-T", outbox_id=42))


def _word(page: str) -> str:
    m = re.search(r"color:#04080f;border:1px solid #[0-9a-f]+;font:700 11px/1\.5[^>]*>([^<]*)<", page)
    assert m is not None
    return m.group(1)


# ---------------------------------------------------------------------------------------------------- every context
@pytest.mark.parametrize("sample", SAMPLES, ids=[s.context for s in SAMPLES])
def test_every_context_renders_in_the_theme(sample: Any) -> None:
    page = sample.html
    assert "READ-ONLY" in page and "XTNL" in page and "For the operator" in page.replace("FOR THE OPERATOR", "For "
                                                                                                         "the operator")
    assert _word(page) in ("ACT", "FIX", "FYI", "DONE")
    low = page.lower()
    for banned in ("<script", "<style", "<img", "<link", "http://", "https://", "rgba(", "gradient", "javascript:"):
        assert banned not in low, banned
    # the read-only line, never a suggestion that the server acts
    assert "never places, changes or cancels orders" in page


def test_no_favour_figures_and_no_pip_size() -> None:
    for s in SAMPLES:
        assert "favour" not in s.html.lower()
        assert "0.0001" not in s.html          # the pip unit the samples were rendered with


# ---------------------------------------------------------------------------------------------------- status words
@pytest.mark.parametrize(("kind", "sev", "title", "body", "episode", "word", "segment"), [
    ("episode", "high", "SOFT_HALT missing_stop_loss order:ORD-1", "no_stop (cap NONE, sweep 9)",
     ("missing_stop_loss", "order", "ORD-1", 1, 2), "ACT", "Risk check"),
    ("repage", "high", "still SOFT_HALT missing_stop_loss order:ORD-1", "no_stop (cap NONE)",
     ("missing_stop_loss", "order", "ORD-1", 1, 2), "ACT", "Risk check"),
    ("episode", "info", "FLAG max_daily_loss account:ACC-T:2026-11-20", "transfers_unverified (cap NONE, sweep 9)",
     ("max_daily_loss", "account", "ACC-T:2026-11-20", 1, 1), "FYI", "Daily account check"),
    ("info", "info", "resolved missing_stop_loss order:ORD-1", "stop_seen (level SOFT_HALT, cap NONE)", None,
     "DONE", "Risk check"),
    ("trip", "critical", "HALT HARD", "reason: max_daily_loss", None, "ACT", "Halt level"),
    ("info", "info", "HALT cleared", "cool_down", None, "DONE", "Halt level"),
    ("info", "high", "downtime catch-up incomplete: stonex", "catch-up (retention): 4 orders / 2 positions seen, "
     "1 anomalies; uncovered window a .. b", None, "FIX", "Broker connection and data"),
    ("info", "critical", "broker AUTH_FAILED: stonex", "auth", None, "FIX", "Broker connection and data"),
    ("info", "info", "engine recording again on the single PC", "back", None, "DONE", "System health"),
])
def test_status_word_and_segment(kind: str, sev: str, title: str, body: str,
                                 episode: tuple[str, str, str, int, int] | None, word: str, segment: str) -> None:
    msg = _alert(kind, sev, title, body, episode)
    tk = ticket(msg, classify(msg))
    assert (tk.word, tk.segment) == (word, segment)
    page = render_alert_html(msg, NODE, theme="xtnl")
    assert _word(page) == word
    assert msg.subject in page.replace("&#x27;", "'")     # the old subject stays in the operator block


def test_plain_headlines() -> None:
    msg = _alert("episode", "high", "SOFT_HALT missing_stop_loss order:ORD-1", "no_stop (cap NONE)",
                 ("missing_stop_loss", "order", "ORD-1", 1, 2))
    assert ticket(msg, classify(msg)).title == "Add a stop loss to order ORD-1"
    flag = _alert("episode", "info", "FLAG max_daily_loss account:ACC-T:2026-11-20", "transfers_unverified (cap NONE)",
                  ("max_daily_loss", "account", "ACC-T:2026-11-20", 1, 1))
    assert ticket(flag, classify(flag)).title == "Today's deposits and withdrawals are not confirmed"
    auth = _alert("info", "critical", "broker AUTH_FAILED: stonex", "auth")
    assert ticket(auth, classify(auth)).title == "The broker refused the server's login (stonex)"


def test_dynamic_text_is_escaped() -> None:
    msg = _alert("info", "high", "a title no rule <b>knows</b>", "body <script>x</script>")
    page = render_alert_html(msg, NODE, theme="xtnl")
    assert "<b>knows" not in page and "<script>" not in page
    assert "&lt;b&gt;knows" in page


def test_classic_is_unchanged_by_default() -> None:
    msg = _alert("info", "info", "HALT cleared", "cool_down")
    assert render_alert_html(msg, NODE) == render_alert_html(msg, NODE, theme="classic")
    assert "READ-ONLY &middot; PHASE 1" not in render_alert_html(msg, NODE)


# ---------------------------------------------------------------------------------------------------- pips
def test_pip_unit_from() -> None:
    assert pip_unit_from(D("0.0001"), "EUR/USD") == PipUnit(D("0.0001"), "pips")
    assert pip_unit_from(D("0.01"), "USDJPY") == PipUnit(D("0.01"), "pips")
    assert pip_unit_from(D("1"), "AUS200") == PipUnit(D("1"), "points")
    assert pip_unit_from(D("0.0001"), "SYN.EURX", "FX") == PipUnit(D("0.0001"), "pips")
    assert pip_unit_from(D("0.1"), "XAUUSD", "COMMODITY") == PipUnit(D("0.1"), "points")
    assert pip_unit_from(None, "EUR/USD") is None
    assert pip_unit_from(D("0"), "EUR/USD") is None


def test_pip_distance() -> None:
    assert pip_distance(D("1.26800"), D("1.27140"), FX) == (D("34.0"), "below")
    assert pip_distance(D("1.27500"), D("1.27140"), FX) == (D("36.0"), "above")
    assert pip_distance(D("1.27140"), D("1.27140"), FX) == (D("0.0"), "at")
    assert pip_distance(D("1.268005"), D("1.27140"), FX) == (D("34.0"), "below")


def _adjust_body(**item: object) -> Any:
    return PREVIEW._notice("POST", items=(PREVIEW._item(**item),))


def test_arm_levels_show_pips_from_the_entry() -> None:
    body = _adjust_body(entry=D("1.27140"), stop_now=D("1.26800"), proposed_stop=D("1.27020"))
    page = render_arm_html(body, footer=NODE, settings=RenderSettings(show_levels=True), theme="xtnl",
                           pip_units={("demo-broker", "SYN.EURX"): FX})
    assert _word(page) == "ACT"
    assert "1.27140" in page and "1.26800" in page and "1.27020" in page
    assert "34.0 pips" in page and "12.0 pips" in page and "below entry" in page
    assert "Moves the stop 22.0 pips tighter." in page
    assert "0.0001" not in page


def test_arm_levels_without_a_unit_or_entry() -> None:
    body = _adjust_body(entry=D("1.27140"), stop_now=D("1.26800"), proposed_stop=D("1.27020"))
    no_unit = render_arm_html(body, footer=NODE, settings=RenderSettings(show_levels=True), theme="xtnl")
    assert "pips" not in no_unit and "1.27020" in no_unit
    hidden = render_arm_html(body, footer=NODE, settings=RenderSettings(show_levels=False), theme="xtnl",
                             pip_units={("demo-broker", "SYN.EURX"): FX})
    assert "1.27140" not in hidden                      # Q13: the entry stays hidden
    assert "below entry" not in hidden                  # no distance from a hidden entry
    assert "Moves the stop 22.0 pips tighter." in hidden
    assert "show_levels off" in hidden


def test_arm_classic_ignores_pip_units() -> None:
    body = _adjust_body()
    classic = render_arm_html(body, footer=NODE, settings=RenderSettings(show_levels=True),
                              pip_units={("demo-broker", "SYN.EURX"): FX})
    assert classic == render_arm_html(body, footer=NODE, settings=RenderSettings(show_levels=True))


# ---------------------------------------------------------------------------------------------------- settings, plumbing
def test_theme_setting() -> None:
    assert NotificationSettings.from_env({}).email_theme == "xtnl"
    assert NotificationSettings.from_env({EMAIL_THEME_VAR: "classic"}).email_theme == "classic"
    assert NotificationSettings.from_env({EMAIL_THEME_VAR: " XTNL "}).email_theme == "xtnl"
    bad = NotificationSettings.from_env({EMAIL_THEME_VAR: "neon"})
    assert bad.email_theme == "xtnl" and any(EMAIL_THEME_VAR in i for i in bad.issues)


def test_channels_and_email_carry_the_theme() -> None:
    env = {"XTNL_SMTP_HOST": "127.0.0.1", "XTNL_SMTP_FROM": "a@example.test", "XTNL_ALERT_EMAIL_TO": "b@example.test",
           "XTNL_SMTP_SECURITY": "none"}
    for theme in ("xtnl", "classic"):
        cs = build_channels(NotificationSettings.from_env({**env, EMAIL_THEME_VAR: theme}), None)  # type: ignore[arg-type]
        assert cs.email_theme == theme
        transport = cs.channels[0].transport  # type: ignore[attr-defined]
        assert isinstance(transport, SmtpTransport) and transport._theme == theme
    msg = _alert("info", "info", "HALT cleared", "cool_down")
    xtnl = build_email(msg, "a@example.test", ("b@example.test",), theme="xtnl")
    html = xtnl.get_body(("html",))
    assert html is not None and "READ-ONLY" in html.get_content()
    assert xtnl.get_body(("plain",)).get_content().rstrip("\n") == msg.body  # type: ignore[union-attr]


class _Store:
    pass


def _relay(theme: str, source: Any) -> ArmRelay:
    return ArmRelay(_Store(), [], renderer=lambda body: None, email_theme=theme,  # type: ignore[arg-type]
                    point_sizes=source)


def test_relay_reads_point_sizes_for_adjust_items_only() -> None:
    asked: list[Sequence[tuple[str, str]]] = []

    async def source(keys: Sequence[tuple[str, str]]) -> Mapping[tuple[str, str], object]:
        asked.append(keys)
        return {k: D("0.0001") for k in keys}

    body = _adjust_body()
    units = asyncio.run(_relay("xtnl", source)._pip_units(body))
    # SYN.EURX is not a six-letter currency pair, so its distance is in points
    assert units == {("demo-broker", "SYN.EURX"): PipUnit(D("0.0001"), "points")}
    assert asked == [[("demo-broker", "SYN.EURX")]]
    assert asyncio.run(_relay("classic", source)._pip_units(body)) == {}
    hold = PREVIEW._notice("POST", items=(PREVIEW._item(outcome="MARKET_CLOSED", category="HOLD"),))
    assert asyncio.run(_relay("xtnl", source)._pip_units(hold)) == {}


def test_relay_point_size_failure_only_drops_the_pips() -> None:
    async def broken(keys: Sequence[tuple[str, str]]) -> Mapping[tuple[str, str], object]:
        raise ConnectionError("db down")

    async def slow(keys: Sequence[tuple[str, str]]) -> Mapping[tuple[str, str], object]:
        await asyncio.sleep(5)
        return {}

    body = _adjust_body()
    assert asyncio.run(_relay("xtnl", broken)._pip_units(body)) == {}
    relay = _relay("xtnl", slow)
    relay._point_size_timeout_s = 0.05
    assert asyncio.run(relay._pip_units(body)) == {}
    page = relay._html(body, 1, "subject", {})
    assert page is not None and "1.08420" in page and "pips" not in page


def test_modules_export() -> None:
    assert email_theme.DEFAULT_THEME == "xtnl" and "render_oversee" in email_theme.__all__
    assert callable(email_html.render_alert_html) and callable(email_html_arm.render_arm_html)
