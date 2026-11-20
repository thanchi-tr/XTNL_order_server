"""The HTML email part (src/notifications/email_html.py, email_html_arm.py; design docs/design/notifications/).

Every context renders a well-formed, self-contained page; the plain-text part is the old body byte for byte; every
dynamic value is escaped; no forbidden field (market data, favour R, currency amounts) ever appears; the email is
multipart/alternative (SMTP) or carries Text + Html (SES); `XTNL_EMAIL_HTML=0` and a failing renderer give the old
text-only email; the ARM SL advice block shows the order-state entry, current stop and proposed stop."""
from __future__ import annotations

import importlib.util
import json
import logging
import re
import sys
from collections.abc import Callable
from decimal import Decimal
from email.message import EmailMessage
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import httpx
import pytest

from src.notifications import mail
from src.notifications.email_html import LAYOUTS, TOKENS, classify, render_alert_html
from src.notifications.email_html_arm import arm_layout, render_arm_html
from src.notifications.factory import build_channels
from src.notifications.mail import build_email, ses_request_body
from src.notifications.settings import EMAIL_HTML_VAR, NotificationSettings, SesSettings, SmtpSettings
from src.oversee.notify import AlertMessage, OutboxRow, RecordingChannel, render_message
from src.riskmgr.notices import held_stop_level
from src.riskmgr.render import (
    ITEM_KEYS,
    STILL_WORSE,
    ItemBody,
    NoticeBody,
    RenderSettings,
    decode_body,
    encode_body,
    render,
)

D = Decimal
ROOT = Path(__file__).resolve().parents[2]
NODE: Callable[[], tuple[str, str | None]] = lambda: ("node-t", "77")  # noqa: E731


def _load_preview() -> Any:
    path = ROOT / "docs" / "design" / "notifications" / "gen_preview.py"
    spec = importlib.util.spec_from_file_location("notif_gen_preview", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["notif_gen_preview"] = mod
    spec.loader.exec_module(mod)
    return mod


PREVIEW = _load_preview()
SAMPLES = PREVIEW.samples()


def msg_of(kind: str, sev: str, title: str, body: str, **kw: Any) -> AlertMessage:
    return render_message(OutboxRow(kind, sev, title, body, 1_795_147_261_000, **kw))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------- well-formed
VOID = {"meta", "br"}


class _Checker(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: list[str] = []
        self.tags: set[str] = set()
        self.errors: list[str] = []
        self.text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tags.add(tag)
        if tag not in VOID:
            self.stack.append(tag)

    def handle_endtag(self, tag: str) -> None:
        if not self.stack or self.stack[-1] != tag:
            self.errors.append(f"unbalanced </{tag}> (open: {self.stack[-3:]})")
            return
        self.stack.pop()

    def handle_data(self, data: str) -> None:
        self.text.append(data)


def check_page(page: str) -> _Checker:
    c = _Checker()
    c.feed(page)
    c.close()
    assert not c.errors, c.errors[:3]
    assert c.stack == [], c.stack
    assert page.startswith("<!doctype html>") and "<title>" in page
    assert c.tags <= {"html", "head", "meta", "title", "body", "div", "table", "tr", "td", "span", "strong", "br"}
    return c


def test_the_preview_covers_every_layout_and_many_contexts() -> None:
    assert {s.layout for s in SAMPLES} == set(LAYOUTS)
    assert len({s.context for s in SAMPLES}) == len(SAMPLES) >= 60


@pytest.mark.parametrize("sample", SAMPLES, ids=[s.context for s in SAMPLES])
def test_every_context_renders_a_well_formed_self_contained_page(sample: Any) -> None:
    c = check_page(sample.html)
    text = " ".join(c.text)
    assert "XTNL Phase 1, read-only: this server sends no orders." in text
    assert "UTC" in text and "Melbourne" in text
    low = sample.html.lower()
    for bad in ("<img", "<link", "<script", "<style", "@import", "http", "url(", "<a ", "javascript:"):
        assert bad not in low, bad
    assert 'max-width:600px' in sample.html


@pytest.mark.parametrize("sample", SAMPLES, ids=[s.context for s in SAMPLES])
def test_no_forbidden_field_ever_appears(sample: Any) -> None:
    """Content rule (operations.md §9): never favour R, a market price label, an account value or a currency amount."""
    c = check_page(sample.html)
    text = " ".join(c.text)
    assert "favour" not in text.lower()
    assert not re.search(r"(?:AUD|USD|EUR|GBP|JPY|\$|€|£)\s?\d", text)
    assert not re.search(r"\b(?:bid|ask|spread|ATR|OHLC|pips?|balance|equity|P&L|PnL|open|high|low|close)\s*[:=]\s*\d",
                         text, re.I)


def test_the_preview_page_is_generated_from_the_samples(tmp_path: Path) -> None:
    out = tmp_path / "p.html"
    assert PREVIEW.main(["gen", str(out)]) == 0
    page = out.read_text(encoding="utf-8")
    assert page.count("<iframe") == len(SAMPLES) and "ACC-DEMO-1" in page
    committed = (ROOT / "docs" / "design" / "notifications" / "preview.html").read_text(encoding="utf-8")
    assert committed == page, "preview.html is stale: run docs/design/notifications/gen_preview.py"


def test_severity_colours_pass_wcag_aa_with_white_text() -> None:
    def lum(hex_: str) -> float:
        def ch(v: int) -> float:
            c = v / 255
            return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
        r, g, b = (int(hex_[i:i + 2], 16) for i in (1, 3, 5))
        return 0.2126 * ch(r) + 0.7152 * ch(g) + 0.0722 * ch(b)

    for name, (colour, label) in TOKENS.items():
        assert (1.05) / (lum(colour) + 0.05) >= 4.5, name
        assert label


# ---------------------------------------------------------------------------------------------------- multipart
MSG = msg_of("episode", "high", "SOFT_HALT max_position_risk position:P-1", "risk_band (cap NONE, sweep 5)",
             episode=("max_position_risk", "position", "P-1", 2, 2), account_id="A-1", outbox_id=12)


def test_build_email_is_multipart_alternative_and_the_text_part_is_unchanged() -> None:
    old = build_email(MSG, "x@example.test", ("y@example.test",), html_on=False)
    new = build_email(MSG, "x@example.test", ("y@example.test",), footer=NODE)
    assert not old.is_multipart() and old.get_content_type() == "text/plain"
    assert new.get_content_type() == "multipart/alternative"
    plain, page = new.get_body(("plain",)), new.get_body(("html",))
    assert plain is not None and page is not None
    assert plain.get_content() == old.get_content() == MSG.body + "\n"
    assert plain.get_payload(decode=True) == old.get_payload(decode=True)
    assert [p.get_content_type() for p in new.iter_parts()] == ["text/plain", "text/html"]
    html_text = page.get_content()
    check_page(html_text)
    assert "node node-t" in html_text and "epoch 77" in html_text and "alert #12" in html_text
    for h in ("From", "To", "Subject", "X-Priority", "X-XTNL-Alert-Id"):
        assert new[h] == old[h]


def test_a_prebuilt_html_part_is_used_as_is() -> None:
    msg = render_message(OutboxRow("info", "info", "t", "b", 1, html="<!doctype html><title>x</title>pre"))
    em = build_email(msg, "x@example.test", ("y@example.test",))
    page = em.get_body(("html",))
    assert page is not None and page.get_content().startswith("<!doctype html><title>x</title>pre")


def test_ses_body_carries_text_and_html() -> None:
    s = SesSettings("eu-west-2", "x@example.test", ("y@example.test",), "AK", "SK")
    body = json.loads(ses_request_body(MSG, s, footer=NODE))
    parts = body["Content"]["Simple"]["Body"]
    assert parts["Text"] == {"Data": MSG.body, "Charset": "UTF-8"}
    assert parts["Html"]["Charset"] == "UTF-8" and parts["Html"]["Data"].startswith("<!doctype html>")
    off = json.loads(ses_request_body(MSG, s, html_on=False))
    assert set(off["Content"]["Simple"]["Body"]) == {"Text"}


def test_a_failing_renderer_sends_text_only_and_logs_the_class_only(monkeypatch: pytest.MonkeyPatch,
                                                                      caplog: pytest.LogCaptureFixture) -> None:
    def boom(*_a: Any, **_k: Any) -> str:
        raise RuntimeError("secret body text")

    monkeypatch.setattr(mail, "render_alert_html", boom)
    with caplog.at_level(logging.WARNING, logger="src.notifications.mail"):
        em = build_email(MSG, "x@example.test", ("y@example.test",))
        s = SesSettings("eu-west-2", "x@example.test", ("y@example.test",), "AK", "SK")
        ses = json.loads(ses_request_body(MSG, s))
    assert not em.is_multipart() and em.get_content() == MSG.body + "\n"
    assert set(ses["Content"]["Simple"]["Body"]) == {"Text"}
    assert "RuntimeError" in caplog.text and "secret body text" not in caplog.text


# ---------------------------------------------------------------------------------------------------- the switch
class _FakeSmtp:
    def __init__(self) -> None:
        self.sent: list[EmailMessage] = []

    def ehlo(self, name: str = "") -> object:
        return None

    def starttls(self, *, context: Any = None) -> object:
        return None

    def login(self, user: str, password: str) -> object:
        return None

    def send_message(self, msg: EmailMessage) -> object:
        self.sent.append(msg)
        return None

    def quit(self) -> object:
        return None

    def close(self) -> None:
        return None


ENV = {"XTNL_SMTP_HOST": "smtp.example.test", "XTNL_SMTP_FROM": "x@example.test", "XTNL_ALERT_EMAIL_TO": "y@example.test"}


@pytest.mark.parametrize(("value", "multipart"), [(None, True), ("1", True), ("0", False)])
async def test_xtnl_email_html_switch(value: str | None, multipart: bool) -> None:
    env = dict(ENV) if value is None else {**ENV, EMAIL_HTML_VAR: value}
    settings = NotificationSettings.from_env(env)
    assert settings.email_html is multipart
    fake = _FakeSmtp()
    async with httpx.AsyncClient() as client:
        cs = build_channels(settings, client, smtp_factory=lambda s, ctx: fake, footer=NODE)
        (email,) = [c for c in cs.channels if c.name == "email"]
        await email.send(OutboxRow("trip", "critical", "HALT HARD", "-: escalation", 1_795_147_261_000, outbox_id=3))
    (em,) = fake.sent
    assert em.is_multipart() is multipart
    plain = em.get_body(("plain",)) if multipart else em
    assert plain is not None and plain.get_content().startswith("-: escalation\n\nkind: trip")


def test_an_invalid_switch_value_is_reported_and_keeps_html_on() -> None:
    s = NotificationSettings.from_env({**ENV, EMAIL_HTML_VAR: "yes"})
    assert s.email_html is True and any(EMAIL_HTML_VAR in i for i in s.issues)
    arm = NotificationSettings.from_env({EMAIL_HTML_VAR: "0"}, prefix="XTNL_ARM_")
    assert arm.email_html is False      # one switch for both channel sets


def test_a_transport_built_directly_stays_text_only() -> None:
    """The dead-man receiver builds its transports itself: no HTML part unless asked."""
    fake = _FakeSmtp()
    t = mail.SmtpTransport(SmtpSettings("smtp.example.test", 587, "x@example.test", ("y@example.test",)),
                           factory=lambda s, ctx: fake)
    t._send_blocking(build_email(MSG, "x@example.test", ("y@example.test",), html_on=False))
    assert not fake.sent[0].is_multipart()


# ---------------------------------------------------------------------------------------------------- escaping
NASTY = '<script>&"x\'</script>'


def test_every_dynamic_value_is_escaped() -> None:
    msgs = [
        msg_of("episode", "high", f"SOFT_HALT {NASTY}", f"{NASTY} (cap {NASTY}, sweep {NASTY})",
               episode=(NASTY, NASTY, NASTY, 1, 2), account_id=NASTY, outbox_id=1),
        msg_of("info", "high", f"downtime catch-up incomplete: {NASTY}", NASTY, account_id=NASTY),
        msg_of("info", "critical", f"ARM FAILED [{NASTY}] route {NASTY}", f"scope: {NASTY}\npositions (1):\n  {NASTY}"),
        msg_of("info", "high", f"HALT cleared by {NASTY}", f"{NASTY} -> SOFT_HALT"),
        msg_of("info", "info", NASTY + "\x00\x1b", NASTY),
    ]
    for m in msgs:
        page = render_alert_html(m, lambda: (NASTY, NASTY))
        check_page(page)
        assert "<script" not in page and "\x00" not in page and "\x1b" not in page
        assert "&lt;script&gt;" in page
    item = ItemBody(decision_id=1, decision_bar_end_ms=1_795_147_200_000, adapter_id=NASTY, position_id=NASTY,
                    symbol=NASTY, side=NASTY, event="POST", bar_end_ms=1_795_147_200_000, outcome="ADJUST",
                    category=NASTY, handles=(NASTY,), caveats=(NASTY,), quality_flags=(NASTY,), stop_basis=NASTY,
                    strategy_kind=NASTY, proposed_stop=D("1.5"))
    body = NoticeBody(notice_id=1, route=NASTY, event="POST", bar_spec=NASTY, bar_end_ms=1_795_147_200_000,
                      created_ms=1_795_147_260_000, items=(item, ItemBody(**{**item.__dict__, "outcome": NASTY})))
    page = render_arm_html(body, footer=lambda: (NASTY, NASTY), title=NASTY)
    check_page(page)
    assert "<script" not in page and "&lt;script&gt;" in page


def test_a_long_value_is_cut() -> None:
    m = msg_of("info", "high", "flag:x", "y" * 10_000, account_id="z" * 500)
    page = render_alert_html(m)
    assert "y" * 2001 not in page and "z" * 65 not in page


# ---------------------------------------------------------------------------------------------------- classify
CASES = [
    ("downtime catch-up: a1", "info", "ops.catchup", "L3"),
    ("downtime catch-up incomplete: a1", "high", "ops.catchup_incomplete", "L3"),
    ("clock moved back: a1", "high", "ops.clock_moved_back", "L3"),
    ("cash-flow ledger incomplete: a1", "high", "ops.cash_flow_ledger_incomplete", "L3"),
    ("ingestion frame lost: a1:orders", "high", "ops.ingestion", "L3"),
    ("ingestion driver error: a1", "high", "ops.ingestion", "L3"),
    ("consumer commit failed: orders", "high", "ops.consumer", "L3"),
    ("DLQ ring full: orders", "high", "ops.consumer", "L3"),
    ("market sessions need review: a1", "info", "ops.sessions_review", "L3"),
    ("market session unusable: a1:SYN", "high", "ops.session_unusable", "L3"),
    ("market sessions not saved", "high", "ops.sessions_not_saved", "L3"),
    ("maintenance: a1", "info", "ops.maintenance", "L3"),
    ("maintenance windows unreadable", "high", "ops.maintenance_unreadable", "L3"),
    ("calendar:calendar_mismatch", "high", "ops.calendar", "L3"),
    ("flag:pnl_unverified", "high", "ops.flag", "L3"),
    ("adapter re-armed: a1", "info", "ops.adapter_rearmed", "L3"),
    ("oversee:feed lag", "high", "oversee.feed_lag", "L3"),
    ("oversee:slow_pass", "high", "oversee.page", "L3"),
    ("oversee:dependent_alerts_summary", "info", "oversee.page", "L3"),
    ("integrity:dlq_quarantined", "high", "integrity.page", "L3"),
    ("task risk-sweep stale", "high", "ops.task_stale", "L3"),
    ("optional services failed to start", "high", "ops.optional_services", "L3"),
    ("market-data hook failing", "high", "ops.optional_services", "L3"),
    ("lifecycle: DEFAULT partition not empty", "high", "ops.lifecycle", "L3"),
    ("ruleset v7 loaded", "info", "ops.ruleset", "L3"),
    ("ruleset v7: sweep.interval_ms ignored", "info", "ops.ruleset", "L3"),
    ("ruleset v7 rejected", "high", "ops.ruleset_rejected", "L3"),
    ("SAFE_HOLD auto-resumed on the single PC", "info", "ops.safe_hold_resumed", "L3"),
    ("engine recording again on the single PC", "info", "ops.recording_again", "L3"),
    ("ARM: decision loop stalled", "high", "arm.operations", "L3"),
    ("ARM channel arm_email failing", "high", "arm.operations", "L3"),
    ("ARM notice #4 expired undelivered", "high", "arm.operations", "L3"),
    ("ARM notices expired undelivered", "high", "arm.operations", "L3"),
    ("ARM idle: no accepted configuration", "high", "arm.operations", "L3"),
    ("ARM FAILED spool damaged", "high", "arm.operations", "L3"),
    ("redis-md memory shedding step 2", "high", "md.page", "L3"),
    ("indicator faulted", "high", "md.page", "L3"),
    ("ARM FAILED [p-1] route main", "critical", "arm.failed_ping", "L4"),
    ("ARM FAILED re-ping 3 [p-1] series s1", "high", "arm.failed_ping", "L4"),
    ("ARM FAILED not recorded [p-1]", "critical", "arm.failed_not_recorded", "L4"),
    ("broker AUTH_FAILED: a1", "critical", "ops.auth_failed", "L4"),
    ("PRE_OPEN readiness failed: a1", "critical", "ops.pre_open_failed", "L4"),
    ("task risk-sweep circuit open", "critical", "ops.task_circuit", "L4"),
    ("task risk-sweep circuit still open", "critical", "ops.task_circuit", "L4"),
    ("oversee:oversight_lapsed", "critical", "oversee.page", "L4"),
    ("integrity:integrity_halted", "critical", "integrity.page", "L4"),
    ("ruleset v8 rejected at LEADER_INIT", "critical", "ops.ruleset_rejected_init", "L4"),
    ("ARM configuration tampered", "critical", "arm.operations", "L4"),
    ("SAFE_HOLD auto-resume stopped on the single PC", "critical", "ops.safe_hold_stopped", "L4"),
    ("engine not recording on the single PC", "critical", "ops.not_recording", "L4"),
    ("HALT SOFT", "high", "halt.soft", "L2"),
    ("HALT HARD", "critical", "halt.hard", "L2"),
    ("HALT cleared", "info", "halt.cleared", "L2"),
    ("HALT cleared by alice", "high", "halt.cleared_by_operator", "L2"),
    ("resolved max_position_risk position:P1", "info", "oversee.resolved", "L1"),
    ("capped max_daily_loss account:A1:2026-11-20", "info", "oversee.capped", "L1"),
    ("something nobody planned", "info", "generic", "L3"),
    ("something critical nobody planned", "critical", "generic", "L4"),
]


@pytest.mark.parametrize(("title", "sev", "name", "layout"), CASES)
def test_each_title_pattern_maps_to_its_layout(title: str, sev: str, name: str, layout: str) -> None:
    kind = "trip" if title in ("HALT SOFT", "HALT HARD") else "info"
    ctx = classify(msg_of(kind, sev, title, "x"))
    assert (ctx.name, ctx.layout) == (name, layout)
    if name != "generic":
        assert ctx.action, title


def test_episode_rows_are_classified_from_the_episode_key_not_the_text() -> None:
    m = msg_of("episode", "info", "garbled title", "risk_band (cap NONE, sweep 9)",
               episode=("max_position_risk", "position", "P9", 1, 1), account_id="A1")
    ctx = classify(m)
    assert (ctx.name, ctx.layout, ctx.badge, ctx.token) == ("oversee.episode", "L1", "FLAG", "FLAG")
    assert ctx.headline == "Max position risk FLAG: position P9" and "no halt" in (ctx.action or "")
    rp = classify(msg_of("repage", "high", "x", "mark_unknown (cap SOFT)",
                         episode=("max_position_risk", "position", "P9", 1, 2)))
    assert rp.name == "oversee.repage" and ("RE-PAGE", "HIGH") in rp.tags
    assert rp.action is not None and rp.action.startswith("An input is unknown") and "Acknowledge" in rp.action
    sod = classify(msg_of("episode", "high", "x", "late_roll", episode=("sod_partial_session", "account",
                                                                       "A1:2026-11-20", 0, 2)))
    assert sod.name == "oversee.sod_partial_session" and "trading day 2026-11-20" in sod.headline


def test_a_body_that_does_not_parse_becomes_the_detail_block() -> None:
    m = msg_of("episode", "high", "x", "unexpected shape", episode=("margin_usage", "account", "A1", 1, 2))
    ctx = classify(m)
    assert ctx.detail == "unexpected shape"
    assert "unexpected shape" in render_alert_html(m)


# ---------------------------------------------------------------------------------------------------- ARM advice
T = 1_795_147_200_000


def adjust(**kw: Any) -> ItemBody:
    base: dict[str, Any] = dict(
        decision_id=9, decision_bar_end_ms=T, adapter_id="ad", position_id="P1", symbol="SYN.A", side="BUY",
        event="POST", bar_end_ms=T, outcome="ADJUST", category="ACTIONABLE", proposed_stop=D("1.23456"),
        current_risk_r=D("0.85"), handles=("O1",), stop_read_at_ms=T + 5_000, stop_now=D("1.21456"),
        entry=D("1.23456"), s0=D("1.19999"), trigger_input="ad|SYN.A|M15|BID|BROKER|1|k1:abc",
        strategy_kind="break_even_on_risk_close", strategy_impl_version=1)
    base.update(kw)
    return ItemBody(**base)


def notice(*items: ItemBody, event: str = "POST") -> NoticeBody:
    return NoticeBody(notice_id=5, route="main", event=event, bar_spec="M15", bar_end_ms=T, created_ms=T + 60_000,
                      items=items)


def visible(page: str) -> str:
    return " ".join(check_page(page).text)


def test_post_adjust_shows_the_sl_block_with_order_state_levels_only() -> None:
    page = render_arm_html(notice(adjust()), footer=NODE, settings=RenderSettings(show_levels=True))
    text = visible(page)
    assert "FINAL ADVICE" in text and "MOVE STOP" in text and "Move stop to break-even: SYN.A long" in text
    assert "Entry" in text and "1.23456" in text
    assert "Current stop" in text and "1.21456 as read" in text and "handles O1" in text
    assert "Proposed new stop" in text
    assert "Move only if your stop is still worse than 1.23456." in text
    assert STILL_WORSE.removesuffix(" this") in text      # the text's own qualifier, with the level
    assert "At the broker, move the stop on handle(s) O1 to 1.23456" in text and "This server sends nothing" in text
    assert "Final: decided on the closed broker bar." in text
    # every decimal shown is an order-state level or an R value of the body: s0 and the trigger hash never
    shown = set(re.findall(r"\d+\.\d+", text))
    assert shown <= {"1.23456", "1.21456", "0.85"}, shown
    assert "1.19999" not in page and "abc" not in page and "favour" not in page.lower()
    assert arm_layout(notice(adjust())) == "L5"


def test_entry_follows_q13_show_levels() -> None:
    off = visible(render_arm_html(notice(adjust(entry=D("1.11111"), proposed_stop=D("1.22222")))))
    assert "1.11111" not in off and "not shown (show_levels off)" in off


def test_pre_adjust_is_provisional_with_the_bar_zero_label() -> None:
    pre = adjust(event="PRE", trigger_input=None, caveats=("first_bar_conditional",), trigger_met=True,
                 valid_until_ms=T + 120_000)
    text = visible(render_arm_html(notice(pre, event="PRE"), settings=RenderSettings(show_levels=True)))
    assert "PROVISIONAL" in text and "HEADS-UP" in text and "PROVISIONAL, final advice after the bar closes" in text
    assert "Heads-up: SYN.A long stop would move to break-even" in text
    assert "if this bar closes calm, the trigger is met" in text and "Valid until" in text
    assert "No action required yet" in text and "FINAL ADVICE" not in text


def test_late_and_corrected_items_carry_their_tags() -> None:
    late = visible(render_arm_html(notice(adjust(event="LATE", late=True), event="LATE")))
    assert "LATE" in late and "MOVE STOP" in late
    corr = visible(render_arm_html(notice(adjust(event="CORRECTED", revision=2, outcome="BROKER_AHEAD",
                                                 category="INFO", supersedes=8), event="CORRECTED")))
    assert "CORRECTED" in corr and "NOTHING TO MOVE" in corr and "the earlier advice is withdrawn" in corr


def _notice_badge(page: str) -> str:
    """The text of the notice-level badge (the first badge on the page, before any card)."""
    check_page(page)
    m = re.search(r'color:#FFFFFF;font:700 12px/1\.2 [^"]*">([^<]*)</span>', page)
    assert m is not None
    return m.group(1)


def _strip(page: str) -> str:
    m = re.search(r'<td height="\d+" bgcolor="(#[0-9A-F]{6})"', page)
    assert m is not None
    return m.group(1)


def test_a_mixed_notice_keeps_the_stop_move_as_its_badge_and_strip() -> None:
    fault = adjust(position_id="P2", outcome="DATA_UNAVAILABLE", category="FAULT", proposed_stop=None)
    page = render_arm_html(notice(adjust(), fault), footer=NODE)
    assert _notice_badge(page) == "FINAL ADVICE"
    assert _strip(page) == TOKENS["FINAL"][0]
    text = visible(page)
    assert "MOVE STOP" in text and "FAULT" in text and "Check arm decisions" in text   # each card keeps its badge
    assert "ARM advice: 2 items, 1 stop move (main)" in text
    pre = render_arm_html(notice(adjust(event="PRE", trigger_input=None), adjust(event="PRE", position_id="P2",
                                 outcome="NO_STOP", category="FAULT"), event="PRE"))
    assert _notice_badge(pre) == "PROVISIONAL"
    # without a stop move the most significant item still leads, and FAILED scopes keep FAULT
    assert _notice_badge(render_arm_html(notice(fault))) == "FAULT"
    from src.riskmgr.render import FailedLine
    failed = NoticeBody(notice_id=5, route="main", event="POST", bar_spec="M15", bar_end_ms=T, created_ms=T,
                        items=(adjust(),), failed=(FailedLine(scope_kind="position", scope_id="P3", reason="X"),))
    page = render_arm_html(failed)
    assert _notice_badge(page) == "FAULT" and _strip(page) == TOKENS["FAULT"][0]


def test_an_adjust_without_a_proposed_level_leaves_the_rule_line_out() -> None:
    text = visible(render_arm_html(notice(adjust(proposed_stop=None))))
    assert "not recorded." not in text and "to not recorded" not in text
    assert "still worse than" not in text
    assert "The proposed stop was not recorded: check arm decisions" in text and "This server sends nothing" in text
    assert "Proposed new stop not recorded" in text


def test_the_page_declares_the_light_scheme_only() -> None:
    page = render_arm_html(notice(adjust()))
    assert '<meta name="color-scheme" content="light">' in page and "light dark" not in page


def test_favour_r_never_reaches_the_html_even_when_the_text_shows_it() -> None:
    """The relay's text may carry favour R (hash-verified bar); the HTML is built from the body alone."""
    import inspect
    assert "bars" not in inspect.signature(render_arm_html).parameters
    page = render_arm_html(notice(adjust()), settings=RenderSettings(show_levels=True))
    assert "favour" not in page.lower()


def test_an_older_body_without_stop_now_decodes_and_reads_not_recorded() -> None:
    b = notice(adjust(stop_now=None))
    raw = encode_body(b)
    assert '"stop_now"' not in raw          # additive: absent when unknown, so older bodies keep their exact text
    back = decode_body(raw)
    assert back.items[0].stop_now is None
    assert "Current stop not recorded as read" in visible(render_arm_html(back))
    with_level = encode_body(notice(adjust()))
    assert json.loads(with_level)["items"][0]["stop_now"] == "1.21456"
    assert decode_body(with_level).items[0].stop_now == D("1.21456") and "stop_now" in ITEM_KEYS


def test_the_text_part_of_an_arm_notice_is_unchanged_by_the_new_keys() -> None:
    s = RenderSettings(show_levels=True)
    pre = adjust(event="PRE", trigger_input=None)
    assert render(notice(pre, event="PRE"), settings=s).text == render(
        notice(adjust(event="PRE", trigger_input=None, entry=None, stop_now=None), event="PRE"), settings=s).text
    assert render(notice(adjust()), settings=s).text == render(notice(adjust(stop_now=None)), settings=s).text


def test_status_notices_use_l6() -> None:
    from src.riskmgr.render import AliveCounts
    alive = NoticeBody(notice_id=1, route="main", event="ALIVE", bar_end_ms=0, created_ms=T,
                       alive=AliveCounts(armed=2, faulting=0, unmanaged=1))
    assert arm_layout(alive) == "L6"
    assert "ARM alive: 2 managed, 0 in FAULT, 1 not managed" in visible(render_arm_html(alive))


def test_held_stop_level_picks_the_most_distant_stop() -> None:
    assert held_stop_level([], "BUY") is None and held_stop_level([None], "BUY") is None
    assert held_stop_level([D("1.2"), None], "SELL") == D("1.2")
    assert held_stop_level([D("1.2"), D("1.1")], "BUY") == D("1.1")
    assert held_stop_level([D("1.2"), D("1.1")], "SELL") == D("1.2")
    assert held_stop_level([D("1.2"), D("1.1")], "") is None


# ---------------------------------------------------------------------------------------------------- the relays
async def test_the_arm_relay_attaches_the_html_part_and_never_favour() -> None:
    from tests.notifications.test_arm_relay import PRICE_TEXTS, body, enqueue, insert_ping, rig
    r = rig()
    await enqueue(r, body())
    await insert_ping(r)
    await r.relay.tick()
    notice_row, ping_row = r.email.sent
    assert "favour 0.50 R" in notice_row.body                   # the text part still has the verified favour
    assert notice_row.html is not None and ping_row.html is None
    check_page(notice_row.html)
    for p in PRICE_TEXTS:
        assert p not in notice_row.html
    assert "1.23500" in notice_row.html and "Proposed new stop" in notice_row.html
    sent = render_message(notice_row)
    assert sent.html == notice_row.html and sent.body.startswith(notice_row.body)


async def test_a_failing_arm_html_build_still_sends_the_notice(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.notifications import arm_relay
    from tests.notifications.test_arm_relay import body, enqueue, rig

    def boom(*_a: Any, **_k: Any) -> str:
        raise ValueError("x")

    monkeypatch.setattr(arm_relay, "render_arm_html", boom)
    r = rig()
    await enqueue(r, body())
    res = await r.relay.tick()
    assert res.delivered == 1 and r.email.sent[0].html is None


async def test_recording_channel_passes_html_and_episode_to_the_transport() -> None:
    got: list[AlertMessage] = []

    class T:
        backend = "t"

        async def deliver(self, msg: AlertMessage) -> None:
            got.append(msg)

    ch = RecordingChannel(name="email", transport=T())
    row = OutboxRow("episode", "info", "FLAG x", "y", 42, ("r", "s", "i", 1, 1), "A", 3, html="<p>")
    await ch.send(row)
    (m,) = got
    assert (m.created_ms, m.episode, m.html) == (42, ("r", "s", "i", 1, 1), "<p>")
    assert m == AlertMessage(m.subject, m.body, "info", "episode", 3, "A")   # presentation fields not compared


async def test_notices_carry_stop_now_and_the_pre_entry() -> None:
    from src.db.arm_ports import DecisionHandleRow
    from tests.riskmgr.notice_fakes import ENTRY, T1, decision, rig as notice_rig, row

    g = notice_rig()
    r = row("P1")
    g.add(r)
    h = DecisionHandleRow(handle_id="O1", handle_kind="CHILD_ORDER", handle_role="STOP", read_at_ms=T1 + 5_000,
                          level_read=D("1.20000"), target_level=D("1.23500"))
    await g.commit("POST", T1, (decision(r), True), at=T1 + 70_000, handles=(h,))
    (post,) = g.outbox.bodies("POST")
    assert post.items[0].stop_now == D("1.20000")
    g2 = notice_rig()
    r2 = row("P2")
    g2.add(r2)
    await g2.commit("PRE", T1, (decision(r2, event="PRE"), True), at=T1 - 40_000, handles=(h,))
    (pre,) = g2.outbox.bodies("PRE")
    item = pre.items[0]
    assert item.entry == ENTRY and item.trigger_input is None and item.s0 is None and item.stop_now == D("1.20000")
    assert "entry" not in render(pre, settings=RenderSettings(show_levels=True)).text   # text as before


# ==================================================================================================== visual elements
# (src/notifications/email_visuals.py; design docs/design/notifications/, "visual elements" spec V1 to V10)
from src.notifications import email_visuals as V  # noqa: E402
from src.notifications.email_html import MUTED, READ_ONLY_LINE, VALUE_LIMIT, Context, Step, Tile  # noqa: E402

BASELINE: dict[str, str] = json.loads(Path(__file__).with_name("text_baseline.json").read_text(encoding="utf-8"))
BY_CONTEXT = {s.context: s for s in SAMPLES}
XV = re.compile(r'class="xv-([a-z-]+)"')


def xv(page: str) -> set[str]:
    return set(XV.findall(page))


def _texts(fragment: str) -> list[str]:
    return [t.strip() for t in re.sub(r"<[^>]+>", "\n", fragment).split("\n") if t.strip()]


def _part(page: str, cls: str) -> str:
    """The HTML of the first `xv-<cls>` table (helpers never nest a table in these)."""
    i = page.index(f'class="xv-{cls}"')
    return page[i:page.index("</table>", i)]


# ---------------------------------------------------------------------------------------------------- T0 text
def test_t0_the_plain_text_of_every_baseline_sample_is_byte_for_byte_unchanged() -> None:
    import hashlib
    assert len(BASELINE) == 71
    for ctx, digest in BASELINE.items():
        assert ctx in BY_CONTEXT, ctx
        assert hashlib.sha256(BY_CONTEXT[ctx].text.encode("utf-8")).hexdigest() == digest, ctx
    whole = "\x00".join(s.context + "\x01" + s.text for s in SAMPLES if s.context in BASELINE)
    assert hashlib.sha256(whole.encode("utf-8")).hexdigest() == (
        "cc3529ab0087c883832662ebc30be1a2ee6be0f7bc0471d31a594b3445917e10")


@pytest.mark.parametrize("sample", [s for s in SAMPLES if s.layout in ("L1", "L2", "L3", "L4")],
                         ids=lambda s: s.context)
def test_t0_the_text_part_of_every_alert_email_is_the_body(sample: Any) -> None:
    sev = sample.subject[len("[XTNL "):].split("]")[0].lower()
    msg = AlertMessage(sample.subject, sample.text, sev, "info", 1)
    em = build_email(msg, "x@example.test", ("y@example.test",), footer=NODE)
    plain = em.get_body(("plain",))
    assert em.is_multipart() and plain is not None and plain.get_content() == sample.text + "\n"


def test_t0_the_arm_text_is_unchanged_by_the_visuals(monkeypatch: pytest.MonkeyPatch) -> None:
    s = RenderSettings(show_levels=True)
    b = notice(adjust(), adjust(position_id="P2", side="SELL"))
    before = render(b, settings=s).text
    render_arm_html(b, settings=s)
    monkeypatch.setattr(V, "VISUAL_BUDGET", 0)
    render_arm_html(b, settings=s)
    assert render(b, settings=s).text == before


# ---------------------------------------------------------------------------------------------------- T1 structure
RULE_CONTEXTS = {"oversee.episode", "oversee.repage", "oversee.resolved", "oversee.lowered", "oversee.capped",
                 "oversee.uncapped"}
HALT_CONTEXTS = {"halt.soft", "halt.hard", "halt.cleared", "halt.cleared_by_operator"}
ARM_EXPECTED: dict[str, set[str]] = {
    "arm.pre_adjust (bar-0 heads-up)": {"advice-steps", "side", "time-strip", "stop-ladder"},
    "arm.pre_adjust (valid until before bar end)": {"advice-steps", "side", "time-strip", "stop-ladder"},
    "arm.post_adjust (final advice)": {"advice-steps", "stop-ladder", "side"},
    "arm.post_adjust (show_levels off)": {"advice-steps", "stop-ladder", "side"},
    "arm.post_adjust (older body, no stop_now)": {"advice-steps", "side"},
    "arm.post_adjust (short side)": {"advice-steps", "stop-ladder", "side"},
    "arm.post_adjust (entry outside the path)": {"advice-steps", "stop-ladder", "side"},
    "arm.post_adjust (stop already at proposed)": {"advice-steps", "stop-ladder", "side"},
    "arm.late_adjust": {"advice-steps", "stop-ladder", "side"},
    "arm.corrected": {"side"},
    "arm.corrected (ADJUST, revision 3)": {"advice-steps", "stop-ladder", "side"},
    "arm.post_delayed": {"advice-steps", "stop-ladder", "side", "time-strip"},
    "arm.post_mixed (ADJUST + other outcomes)": {"advice-steps", "stop-ladder", "side", "summary"},
    "arm.complete (released)": {"advice-steps", "side"},
    "arm.post_pending (part 0)": {"side"},
    "arm.alive": {"count-tiles"},
    "arm.alive (no FAULT)": {"count-tiles"},
    "arm.test": set(),
    "arm.md_reset": set(),
    "arm.failed_cleared": set(),
    "arm.extras (tracker notices)": {"side", "summary"},
}


def expected_visuals(context: str) -> set[str]:
    if context in ARM_EXPECTED:
        return {"hero"} | ARM_EXPECTED[context]
    name = context.split(": ", 1)[0]
    assert name.startswith(("oversee.", "halt.", "ops.", "integrity.", "arm.", "md.", "generic")), context
    exp = {"hero"}
    if name in RULE_CONTEXTS:
        exp.add("rule-ladder")
    if name == "oversee.repage":
        exp.add("steps")
    if name in HALT_CONTEXTS and context != "halt.cleared_by_operator: HALT cleared by operator-demo-2":
        exp.add("halt-ladder")
    if name in ("ops.catchup", "ops.catchup_incomplete") and not context.endswith("demo-broker-2"):
        exp |= {"count-tiles", "time-strip"}
    if name == "arm.failed_ping":
        exp |= {"steps", "side"}
    return exp


@pytest.mark.parametrize("sample", SAMPLES, ids=[s.context for s in SAMPLES])
def test_t1_each_context_carries_exactly_its_visual_elements(sample: Any) -> None:
    assert xv(sample.html) == expected_visuals(sample.context)


def test_t1_named_contexts() -> None:
    def get(prefix: str) -> set[str]:
        return xv(next(s.html for s in SAMPLES if s.context.startswith(prefix)))
    assert get("halt.hard") == {"hero", "halt-ladder"}
    assert get("oversee.repage") == {"hero", "rule-ladder", "steps"}
    assert get("arm.post_adjust (final advice)") == {"hero", "advice-steps", "stop-ladder", "side"}
    assert "stop-ladder" not in get("arm.post_adjust (older body, no stop_now)")
    assert get("arm.alive") == {"hero", "count-tiles"}
    assert get("ops.catchup:") == {"hero", "count-tiles", "time-strip"}
    assert get("arm.failed_ping") == {"hero", "steps", "side"}
    assert get("oversee.sod_partial_session") == {"hero"}


# ---------------------------------------------------------------------------------------------------- T2 degradation
def _arm_page(*items: ItemBody, event: str = "POST", show: bool = True) -> str:
    return render_arm_html(notice(*items, event=event), footer=NODE, settings=RenderSettings(show_levels=show))


def test_t2_the_stop_ladder_is_left_out_when_a_level_is_missing_or_not_finite() -> None:
    assert "xv-stop-ladder" in _arm_page(adjust())
    assert "xv-stop-ladder" not in _arm_page(adjust(proposed_stop=None))
    assert "xv-stop-ladder" not in _arm_page(adjust(stop_now=None))
    assert "xv-stop-ladder" not in _arm_page(adjust(stop_now=D("NaN")))
    assert "xv-stop-ladder" not in _arm_page(adjust(entry=D("Infinity")))
    assert "xv-stop-ladder" in _arm_page(adjust(entry=D("Infinity")), show=False)   # entry not shown, not read


def test_t2_rule_and_halt_ladders_degrade() -> None:
    unparsed = render_alert_html(msg_of("episode", "high", "x", "unexpected shape",
                                        episode=("margin_usage", "account", "A1", 1, 2)))
    assert "xv-rule-ladder" in unparsed and "capped" not in unparsed and "dashed" not in unparsed
    assert "· cap" not in " ".join(_texts(unparsed))
    trans = render_alert_html(msg_of("info", "info", "resolved max_position_risk position:P1", "free text"))
    assert "xv-rule-ladder" not in trans and "free text" in trans
    unknown = render_alert_html(msg_of("info", "info", "resolved max_position_risk position:P1",
                                       "risk_band (level WEIRD, cap NONE)"))
    assert "xv-rule-ladder" not in unknown
    maybe = render_alert_html(msg_of("info", "high", "HALT cleared by x", "y -> MAYBE"))
    assert "xv-halt-ladder" not in maybe and "→ MAYBE" in visible(maybe)


def test_t2_an_unmatched_catch_up_body_keeps_the_fields_as_before() -> None:
    m = msg_of("info", "info", "downtime catch-up: a1", "a1: engine was offline, counts unknown")
    page = render_alert_html(m)
    assert "xv-count-tiles" not in page and "xv-time-strip" not in page
    assert [f.key for f in classify(m).fields] == ["Alert", "Adapter", "Raised"]
    partial = render_alert_html(msg_of("info", "info", "downtime catch-up: a1",
                                       "a1: engine was offline from T1 to T2 (5 min); catch-up (startup): odd"))
    assert "xv-time-strip" in partial and "xv-count-tiles" not in partial and ">Offline<" not in partial


@pytest.mark.parametrize("outcome", ["MARKET_CLOSED", "NO_STOP", "STRATEGY_REASON", "PRE_NOT_CONFIRMED"])
def test_t2_no_stepper_for_hold_fault_info_or_not_confirmed(outcome: str) -> None:
    assert "xv-advice-steps" not in _arm_page(adjust(outcome=outcome, category="INFO", proposed_stop=None))


def test_t2_a_non_break_even_adjust_has_a_two_step_stepper() -> None:
    page = _arm_page(adjust(strategy_kind="demo_trail_kind"))
    steps = _part(page, "advice-steps")
    assert "Released" not in steps and 'width="48%"' in steps and steps.count("·") == 2
    be = _part(_arm_page(adjust()), "advice-steps")
    assert "Released" in be and 'width="31%"' in be


def test_t2_an_unknown_side_code_shows_the_raw_text_and_no_chip() -> None:
    page = _arm_page(adjust(side="FLAT"))
    assert "xv-side" not in page and "SYN.A · FLAT" in visible(page)


def test_t2_a_ping_without_since_or_clear_keeps_the_single_box() -> None:
    page = render_alert_html(msg_of("info", "critical", "ARM FAILED [p-1] route main", "scope: route main"))
    assert "xv-steps" not in page and "What to do:" in visible(page)


# ---------------------------------------------------------------------------------------------------- T3 fail-safe
def _boom(*_a: Any, **_k: Any) -> str:
    raise RuntimeError("visual failed")


CATCHUP_BODY = ("demo-broker: engine was offline from 2026-11-20T22:00:00Z to 2026-11-21T00:01:00Z (2 h 1 min); "
                "catch-up (startup): 1 orders new, 2 updated, 0 positions closed while offline; 1 positions new, "
                "0 cash flows new; complete")
CATCHUP_MSG = msg_of("info", "info", "downtime catch-up: demo-broker", CATCHUP_BODY)


@pytest.mark.parametrize(("helper", "marker", "back"), [
    ("count_tiles", "xv-count-tiles", "Orders new / updated"),
    ("time_strip", "xv-time-strip", "Offline"),
    ("level_ladder", "xv-halt-ladder", None),
    ("hero_row", "xv-hero", None),
])
def test_t3_a_failing_alert_visual_is_left_out_and_its_fields_come_back(
        monkeypatch: pytest.MonkeyPatch, helper: str, marker: str, back: str | None) -> None:
    assert f">{back}<" not in render_alert_html(CATCHUP_MSG) if back else True
    monkeypatch.setattr(V, helper, _boom)
    m = msg_of("trip", "critical", "HALT HARD", "-: escalation") if helper == "level_ladder" else CATCHUP_MSG
    page = render_alert_html(m, NODE)
    check_page(page)
    assert marker not in page
    if back:
        assert f">{back}<" in page
    if helper == "hero_row":
        assert "font:600 20px/1.35" in page and _notice_badge(page) == "INFO"


def test_t3_a_failing_arm_visual_is_left_out_and_the_fields_come_back(monkeypatch: pytest.MonkeyPatch) -> None:
    pre = adjust(event="PRE", trigger_input=None, valid_until_ms=T + 120_000)
    assert ">Valid until</td>" not in _arm_page(pre, event="PRE")       # replaced by the strip
    monkeypatch.setattr(V, "time_strip", _boom)
    monkeypatch.setattr(V, "stop_ladder", _boom)
    page = _arm_page(pre, event="PRE")
    check_page(page)
    assert ">Valid until</td>" in page and "xv-time-strip" not in page and "xv-stop-ladder" not in page
    monkeypatch.setattr(V, "hero_row", _boom)
    page = _arm_page(adjust())
    check_page(page)
    assert "xv-hero" not in page and _notice_badge(page) == "FINAL ADVICE"


# ---------------------------------------------------------------------------------------------------- T4 ordinal
GEOM = re.compile(r'\b(?:width|height)="([^"]*)"|(?<![-\w])(?:width|height):\s*([^;"]+)')


def geometry(page: str) -> list[str]:
    return [a or b for a, b in GEOM.findall(page)]


def test_t4_the_stop_ladder_is_ordinal_the_gap_never_shapes_it() -> None:
    near = _arm_page(adjust(stop_now=D("1.08010"), proposed_stop=D("1.08420"), strategy_kind="k"), show=False)
    far = _arm_page(adjust(stop_now=D("1.00010"), proposed_stop=D("1.98420"), strategy_kind="k"), show=False)
    assert "xv-stop-ladder" in near and "xv-stop-ladder" in far
    assert near.replace("1.08010", "LVL").replace("1.08420", "LVL") == far.replace("1.00010", "LVL").replace(
        "1.98420", "LVL")
    assert geometry(near) == geometry(far)


def test_t4_rungs_follow_price_order_not_the_side() -> None:
    rungs = _texts(_part(_arm_page(adjust(strategy_kind="k"), show=False), "stop-ladder"))
    assert rungs.index("PROPOSED STOP") < rungs.index("CURRENT STOP") and "▲ above current stop" in rungs
    short = _arm_page(adjust(side="SELL", stop_now=D("1.27410"), proposed_stop=D("1.27000"), strategy_kind="k"),
                      show=False)
    rungs = _texts(_part(short, "stop-ladder"))
    assert rungs.index("CURRENT STOP") < rungs.index("PROPOSED STOP") and "▼ below current stop" in rungs
    assert any("short" in r and "not to scale" in r for r in rungs)


def test_t4_entry_only_with_show_levels_and_every_ladder_level_is_in_the_fields() -> None:
    off = _arm_page(adjust(entry=D("1.11111"), proposed_stop=D("1.22222")), show=False)
    assert "1.11111" not in off and "ENTRY" not in _part(off, "stop-ladder")
    on = _arm_page(adjust(entry=D("1.11111"), proposed_stop=D("1.22222")), show=True)
    rungs = _texts(_part(on, "stop-ladder"))
    assert "ENTRY" in rungs and "1.11111" in rungs
    before = on[:on.index('class="xv-stop-ladder"')]
    for level in re.findall(r"\d+\.\d+", " ".join(rungs)):
        assert level in before, level
    text = visible(on)
    assert "favour" not in on.lower()
    assert set(re.findall(r"\d+\.\d+", text)) <= {"1.22222", "1.21456", "1.11111", "0.85"}


def test_t4_break_even_and_equal_levels_share_a_rung() -> None:
    rungs = _texts(_part(_arm_page(adjust(), show=True), "stop-ladder"))
    assert "PROPOSED STOP = ENTRY (break-even)" in rungs and "▲ above current stop" in rungs
    same = _texts(_part(_arm_page(adjust(stop_now=D("1.23456")), show=False), "stop-ladder"))
    assert "PROPOSED STOP = CURRENT STOP" in same and "same level" in same


# ---------------------------------------------------------------------------------------------------- T5 geometry
ALLOWED_GEOMETRY = {"100%", "36%", "25%", "33%", "34%", "31%", "32%", "48%", "4%", "3%", "4", "6", "10", "12", "18",
                    "20", "28", "30", "44", "56", "600", "600px", "40px", "44px", "56px", "28px", "30px", "6px", "4px",
                    "10px", "12px", "18px"}


@pytest.mark.parametrize("sample", SAMPLES, ids=[s.context for s in SAMPLES])
def test_t5_every_width_and_height_is_a_fixed_constant(sample: Any) -> None:
    assert set(geometry(sample.html)) <= ALLOWED_GEOMETRY, set(geometry(sample.html)) - ALLOWED_GEOMETRY


def test_t5_count_tiles_do_not_depend_on_the_counts() -> None:
    def page(c: tuple[int, int, int, int]) -> str:
        return render_alert_html(msg_of("info", "info", "downtime catch-up: a1", (
            f"a1: engine was offline from X to Y (5 min); catch-up (startup): {c[0]} orders new, {c[1]} updated, "
            f"{c[2]} positions closed while offline; {c[3]} positions new, 0 cash flows new; complete")))
    assert "xv-count-tiles" in page((1, 2, 0, 1))
    assert geometry(page((1, 2, 0, 1))) == geometry(page((7, 3, 5, 9))) == geometry(page((123456, 3, 5, 9)))


# ---------------------------------------------------------------------------------------------------- T6 glyphs
ALLOWED_NON_ASCII = {"·", "→", "▲", "▼", "►", "●", "○", "■", "↓"}


@pytest.mark.parametrize("sample", SAMPLES, ids=[s.context for s in SAMPLES])
def test_t6_only_wgl4_glyphs_and_no_emoji(sample: Any) -> None:
    extra = {ch for ch in sample.html if ord(ch) > 127}
    assert extra <= ALLOWED_NON_ASCII, extra - ALLOWED_NON_ASCII
    assert not any(ord(ch) >= 0x1F000 or 0x2600 <= ord(ch) <= 0x27BF or ch == "️" for ch in sample.html)
    assert V.GLYPHS <= ALLOWED_NON_ASCII


def _helper_fragments() -> list[str]:
    ctx_rule = classify(msg_of("episode", "high", "x", "risk_band (cap SOFT, sweep 1)",
                               episode=("max_position_risk", "position", "P", 1, 2)))
    ctx_halt = classify(msg_of("trip", "critical", "HALT HARD", "-: escalation"))
    return [
        V.hero_row((), "", "Head", "CRITICAL", "!"), V.hero_row((), "", "Head", "HIGH", V.PAUSE),
        V.rule_ladder(ctx_rule), V.halt_ladder(ctx_halt),
        V.stop_ladder(adjust(), RenderSettings(show_levels=True), "FINAL", False),
        V.advice_steps(adjust()), V.advice_steps(adjust(strategy_kind="k", event="PRE")),
        V.count_tiles((Tile("1", "a"), Tile("2", "b", "FAULT"), Tile("3", "c"))),
        V.time_strip((("A", "t1"), ("B", "t2")), "5 min", "INFO"), V.side_chip("BUY"),
        V.step_list((Step("one"), Step("Run", "cmd x")), "CRITICAL", True),
        V.summary_chips([("MOVE STOP", "FINAL", 1), ("FAULT", "FAULT", 2)]),
    ]


def test_t6_helpers_use_only_email_safe_markup() -> None:
    frags = _helper_fragments()
    assert all(frags)
    for f in frags:
        low = f.lower()
        for bad in ("border-radius", "display:flex", "display:grid", "position:", "gradient", "background-image",
                    "<svg", "data:", "<img", "<style", "<script", "http"):
            assert bad not in low, (bad, f[:80])
        for td in re.findall(r"<td\b[^>]*>", f):
            assert "bgcolor=" in td, td
        inner = f if f.startswith("<tr") else f"<tr><td>{f}</td></tr>"
        check_page(f"<!doctype html><title>x</title><table>{inner}</table>")


# ---------------------------------------------------------------------------------------------------- T7 contrast
def _ratio(fg: str, bg: str) -> float:
    def lum(hex_: str) -> float:
        def ch(v: int) -> float:
            c = v / 255
            return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
        r, g, b = (int(hex_[i:i + 2], 16) for i in (1, 3, 5))
        return 0.2126 * ch(r) + 0.7152 * ch(g) + 0.0722 * ch(b)
    hi, lo = sorted((lum(fg), lum(bg)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def test_t7_tints_keep_wcag_aa() -> None:
    for token, (colour, _) in TOKENS.items():
        assert _ratio(colour, V.tint(token)) >= 4.5, token
    for bg in (*V.TINTS.values(), "#F9FAFB", "#F2F4F7"):
        assert _ratio("#101828", bg) >= 4.5 and _ratio(MUTED, bg) >= 4.5, bg
    assert _ratio(V.PILL_INK, V.NEUTRAL_BG) >= 4.5


# ---------------------------------------------------------------------------------------------------- T8 escaping
def test_t8_visual_inputs_are_escaped() -> None:
    ping = "\n".join([f"scope: {NASTY}", "positions (1):", f"  a b c {NASTY}",
                      f"no advice for these positions until FAILED is cleared; issued since {NASTY}",
                      f"clear, after fixing the cause: {NASTY}"])
    msgs = [
        msg_of("info", "info", "downtime catch-up: a1", f"a1: engine was offline from {NASTY} to {NASTY} ({NASTY}); "
               "catch-up (startup): 1 orders new, 2 updated, 0 positions closed while offline; 1 positions new, "
               "0 cash flows new"),
        msg_of("info", "critical", "ARM FAILED [p] route r", ping),
        msg_of("info", "high", f"HALT cleared by {NASTY}", f"{NASTY} -> {NASTY}"),
    ]
    for m in msgs:
        page = render_alert_html(m, NODE)
        check_page(page)
        assert "<script" not in page and "&lt;script&gt;" in page
    assert "xv-time-strip" in render_alert_html(msgs[0]) and "xv-steps" in render_alert_html(msgs[1])
    body = NoticeBody(notice_id=1, route=NASTY, event="POST", bar_spec="M15", bar_end_ms=T, created_ms=T,
                      items=(adjust(symbol=NASTY), adjust(side=NASTY, position_id="P2")))
    page = render_arm_html(body)
    check_page(page)
    assert "<script" not in page and "&lt;script&gt;" in page and "xv-hero" in page
    long_cmd = render_alert_html(msg_of("info", "critical", "ARM FAILED [p] route r",
                                        "clear, after fixing the cause: " + "x" * 500))
    assert "xv-steps" in long_cmd
    assert "x" * (VALUE_LIMIT + 1) not in long_cmd and "x" * (VALUE_LIMIT - 3) + "..." in long_cmd


# ---------------------------------------------------------------------------------------------------- T9 side
def test_t9_long_and_short_chips_share_one_style() -> None:
    buy, sell = V.side_chip("BUY"), V.side_chip("SELL")
    style = re.compile(r'style="([^"]*)"')
    assert style.findall(buy) == style.findall(sell) and "LONG (BUY)" in buy and "SHORT (SELL)" in sell
    assert V.side_chip("FLAT") == "" and V.side_chip(None) == ""
    for bad in ("#067647", "#B42318", "▲", "▼", "→"):
        assert bad not in buy + sell


# ---------------------------------------------------------------------------------------------------- T10 levels
def _cells(page: str, cls: str) -> list[tuple[str, str]]:
    return re.findall(r"(<td\b[^>]*>)(.*?)</td>", _part(page, cls))


def test_t10_halt_and_rule_ladder_semantics() -> None:
    hard = render_alert_html(msg_of("trip", "critical", "HALT HARD", "-: escalation"))
    cells = [c for _, c in _cells(hard, "halt-ladder")]
    assert "HARD" in cells[2] and "now · raised" in cells[2] and "now" not in cells[0] + cells[1]
    op = render_alert_html(msg_of("info", "high", "HALT cleared by op", "why -> SOFT_HALT"))
    cells = [c for _, c in _cells(op, "halt-ladder")]
    assert "SOFT" in cells[1] and "now · cleared" in cells[1]
    for p in (hard, op):
        assert "previous" not in visible(p).lower()
    capped = BY_CONTEXT["oversee.capped: capped max_position_risk position:POS-DEMO-7"].html
    tds = _cells(capped, "rule-ladder")
    assert "dashed" in tds[3][0] and tds[3][1].startswith("HARD") and "capped" in tds[3][1]
    assert "now · capped" in tds[2][1] and "dashed" not in tds[2][0]
    seven = render_alert_html(msg_of("episode", "high", "x", "risk_band (cap NONE, sweep 1)",
                                     episode=("max_position_risk", "position", "P", 1, 7)))
    assert "xv-rule-ladder" not in seven


# ---------------------------------------------------------------------------------------------------- T11 steps
def test_t11_numbered_steps_and_the_unchanged_action() -> None:
    ping_s = next(s for s in SAMPLES if s.context.startswith("arm.failed_ping: ARM FAILED [ping-demo-1]"))
    steps = ping_s.html[ping_s.html.index('class="xv-steps"'):]
    assert re.findall(r">([123])</td>", steps)[:3] == ["1", "2", "3"]
    assert "arm failed clear route:demo-route --reason &lt;text&gt;</span>" in steps
    ping_ctx = classify(msg_of("info", "critical", "ARM FAILED [ping-demo-1] route demo-route",
                               ping_s.text.split("\n\nkind: ")[0]))
    assert ping_ctx.action == ("Do not act on ARM advice for these positions issued since 2026-11-21 00:12:00 UTC. "
                               "Fix the cause, then run arm failed clear route:demo-route --reason <text>.")
    assert len(ping_ctx.steps) == 3
    rp = classify(msg_of("repage", "high", "x", "risk_band (cap NONE)",
                         episode=("max_position_risk", "position", "P", 1, 2)))
    assert len(rp.steps) == 2 and rp.action == (
        "Risk-increasing actions are blocked. Reduce size or tighten the stop at the broker, or acknowledge if "
        "expected. Acknowledge it to silence re-pages if it is expected.")
    assert rp.steps[0].text + " " + rp.steps[1].text == rp.action


# ---------------------------------------------------------------------------------------------------- T12 strip
def test_t12_time_strips_list_times_in_order() -> None:
    early = _texts(_part(_arm_page(adjust(event="PRE", trigger_input=None, valid_until_ms=T - 10_000),
                                   event="PRE"), "time-strip"))
    assert early.index("Valid until") < early.index("Bar end")
    late = _texts(_part(_arm_page(adjust(event="PRE", trigger_input=None, valid_until_ms=T + 10_000), event="PRE"),
                        "time-strip"))
    assert late.index("Bar end") < late.index("Valid until")
    assert "↓ 2 h 1 min" in _texts(_part(render_alert_html(CATCHUP_MSG), "time-strip"))
    delayed = _texts(_part(_arm_page(adjust(caveats=("delayed",), decided_at_ms=T + 7 * 60_000)), "time-strip"))
    assert "↓ 7 min after bar end" in delayed and delayed.index("Bar end") < delayed.index("Decided")


# ---------------------------------------------------------------------------------------------------- T13 size
@pytest.mark.parametrize("sample", SAMPLES, ids=[s.context for s in SAMPLES])
def test_t13_every_sample_stays_well_under_the_gmail_clip(sample: Any) -> None:
    assert len(sample.html.encode("utf-8")) < 60_000


def test_t13_the_mixed_notice_and_large_notices_stay_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.notifications import email_html_arm as arm
    assert len(BY_CONTEXT["arm.post_mixed (ADJUST + other outcomes)"].html.encode("utf-8")) < 52_000
    items = tuple(adjust(position_id=f"P{n}", decision_id=n) for n in range(40))
    s = RenderSettings(show_levels=True)
    page = render_arm_html(notice(*items), settings=s)
    used, with_visuals = 0, 0
    for item in items:
        vis = used < V.VISUAL_BUDGET
        card, _ = arm._adjust_card(item, "M15", "main", s, head=True, visuals=vis)
        used += len(card.encode("utf-8"))
        with_visuals += vis
    assert 0 < page.count('class="xv-stop-ladder"') == with_visuals < 40
    assert page.count('class="xv-advice-steps"') == with_visuals
    # no ladder after the first card whose cumulative bytes reach the budget (card badges only: the summary chip
    # ends in "&nbsp;</span>")
    assert page.count("MOVE STOP</span>", 0, page.rindex('class="xv-stop-ladder"')) == with_visuals
    assert page.count("MOVE STOP&nbsp;</span>") == 1
    monkeypatch.setattr(V, "VISUAL_BUDGET", 0)
    bare = render_arm_html(notice(*items), settings=s)
    assert "xv-stop-ladder" not in bare and "xv-side" not in bare and "xv-hero" in bare
    assert len(page) - len(bare) <= 45_000 * 0.5


# ---------------------------------------------------------------------------------------------------- T14 names
def test_t14_no_helper_name_trips_the_name_ban() -> None:
    import ast

    from src.riskmgr.lockout import NAME_BAN_PATTERN, normalise_name
    for rel in ("email_visuals.py", "email_html.py", "email_html_arm.py"):
        tree = ast.parse((ROOT / "src" / "notifications" / rel).read_text(encoding="utf-8"))
        names = [n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.ClassDef))]
        assert names, rel
        for name in names:
            assert NAME_BAN_PATTERN.search(normalise_name(name)) is None, (rel, name)


# ---------------------------------------------------------------------------------------------------- T15 wording
class _VisualText(HTMLParser):
    """The text inside `xv-*` elements."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.inside: list[bool] = []
        self.text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in VOID:
            return
        parent = bool(self.inside and self.inside[-1])
        self.inside.append(parent or any(k == "class" and (v or "").startswith("xv-") for k, v in attrs))

    def handle_endtag(self, tag: str) -> None:
        if tag not in VOID and self.inside:
            self.inside.pop()

    def handle_data(self, data: str) -> None:
        if self.inside and self.inside[-1]:
            self.text.append(data)


@pytest.mark.parametrize("sample", SAMPLES, ids=[s.context for s in SAMPLES])
def test_t15_pages_stay_read_only_and_visuals_never_say_the_server_acts(sample: Any) -> None:
    text = visible(sample.html)
    assert READ_ONLY_LINE in text
    if "At the broker, move the stop" in text:
        assert "This server sends nothing" in text
    p = _VisualText()
    p.feed(sample.html)
    vis = " ".join(p.text).lower()
    assert vis.strip()
    for bad in ("sent", "placed", "executed", "will move", "favour"):
        assert not re.search(rf"\b{bad}\b", vis), (bad, sample.context)


def test_t15_adjust_pages_still_say_the_server_sends_nothing() -> None:
    for ctx in ("arm.post_adjust (final advice)", "arm.post_adjust (short side)", "arm.late_adjust",
                "arm.post_adjust (stop already at proposed)"):
        assert "This server sends nothing" in visible(BY_CONTEXT[ctx].html)


def test_visual_context_defaults_keep_old_calls_valid() -> None:
    c = Context(name="n", layout="L3", token="INFO", badge="INFO", tag="t", headline="h")
    assert (c.rule_level, c.halt_level, c.tiles, c.window, c.steps, c.side_col, c.replaces, c.cap_soft) == (
        None, None, (), (), (), None, frozenset(), False)


# ==================================================================================================== review fixes
# (client rendering and coverage findings on the visual elements)
from src.notifications.email_html import MSO_CLOSE, MSO_OPEN  # noqa: E402
from src.riskmgr.render import PendingBody  # noqa: E402

MIXED = "arm.post_mixed (ADJUST + other outcomes)"
TIME_RE = re.compile(r"font:700 13px/1\.4 [^>]*>([^<]*)</span>")


@pytest.mark.parametrize("sample", SAMPLES, ids=[s.context for s in SAMPLES])
def test_outlook_gets_a_fixed_600_px_column_and_other_clients_keep_max_width(sample: Any) -> None:
    page = sample.html
    assert page.count(MSO_OPEN) == 1 and page.count(MSO_CLOSE) == 1 and 'width="600"' in MSO_OPEN
    card = page.index('style="max-width:600px;')
    assert page.index(MSO_OPEN) < card < page.index(MSO_CLOSE) < page.index("</body>")
    assert "<!--" not in page.replace(MSO_OPEN, "").replace(MSO_CLOSE, "")
    check_page(page)


def test_the_hero_tile_cannot_shrink_and_a_long_headline_wraps() -> None:
    page = BY_CONTEXT["ops.calendar: calendar:calendar_mismatch"].html
    hero = page[page.index('class="xv-hero"'):]
    assert re.search(r'<table role="presentation" width="44" [^>]*style="width:44px;min-width:44px;"', hero)
    assert 'width="56" valign="top" bgcolor="#FFFFFF" style="width:56px;min-width:56px;' in hero
    assert re.search(r"font:600 20px/1\.35 [^\"]*overflow-wrap:anywhere;", hero)


def test_count_tile_labels_keep_their_own_line_height_and_no_trend_glyph() -> None:
    seen = 0
    for s in SAMPLES:
        if "xv-count-tiles" not in s.html:
            continue
        for td, inner in _cells(s.html, "count-tiles"):
            seen += 1
            assert "font:600 12px/16px" in td and "26px/30px" not in td, s.context
            assert inner.startswith('<span style="font:700 26px/30px') and "</span><br>" in inner, s.context
            assert V.UP not in inner and V.DOWN not in inner, s.context
    assert seen >= 10
    alive = _cells(BY_CONTEXT["arm.alive"].html, "count-tiles")
    assert "■</span> in FAULT" in alive[1][1] and "■" not in alive[0][1] + alive[2][1]


def test_times_and_commands_wrap_at_spaces_not_mid_word() -> None:
    for s in SAMPLES:
        for cls in ("time-strip", "steps"):
            if f'class="xv-{cls}"' in s.html:
                part = s.html[s.html.index(f'class="xv-{cls}"'):]
                part = part[:part.index("</table></td></tr></table>" if cls == "steps" else "</table>")]
                assert "break-all" not in part, (s.context, cls)
    strip = _part(BY_CONTEXT["arm.pre_adjust (bar-0 heads-up)"].html, "time-strip")
    assert strip.count("word-break:break-word;overflow-wrap:anywhere;") == 2
    assert "overflow-wrap:anywhere;" in V.command_chip("arm failed clear") and "break-all" not in V.command_chip("x")


def test_the_side_chip_wraps_between_words_never_inside_one() -> None:
    for code in ("BUY", "SELL"):
        chip = V.side_chip(code)
        assert "nowrap" not in chip and "word-break:normal;overflow-wrap:normal;" in chip


def test_summary_chips_are_spaced_without_margins_for_outlook() -> None:
    page = BY_CONTEXT[MIXED].html
    td = page[page.index('class="xv-summary"'):]
    td = td[:td.index("</td></tr>")]
    assert "mso-line-height-rule:exactly;" in td.split(">", 1)[0]
    assert td.count("&nbsp;</span> <span") == 6 and td.count("&nbsp;<span") == 7


def test_summary_chips_count_the_cards_with_their_badge_labels_in_order() -> None:
    page = BY_CONTEXT[MIXED].html
    td = page[page.index('class="xv-summary"'):]
    chips = re.findall(r"■</span> (\d+) ([^&<]+)&nbsp;", td[:td.index("</td></tr>")])
    assert [f"{n} {label}" for n, label in chips] == [
        "2 FAULT", "1 MOVE STOP", "1 NOT CONFIRMED", "1 STOP MOVED BACK", "3 HOLD", "1 RELEASED", "3 INFO"]
    assert sum(int(n) for n, _ in chips) == 12          # one ADJUST + eleven other outcomes in the sample
    assert "AWAITING BAR" not in td and "FAILED" not in td              # zero counts are left out
    assert "xv-summary" not in _arm_page(adjust())                       # one card: no summary
    assert "xv-summary" not in BY_CONTEXT["arm.alive"].html
    pend = NoticeBody(notice_id=6, route="main", event="POST", bar_spec="M15", bar_end_ms=T, created_ms=T,
                      pending=(PendingBody(adapter_id="ad", position_id="P1", symbol="SYN.A", side="BUY"),
                               PendingBody(adapter_id="ad", position_id="P2", symbol="SYN.B", side="SELL")))
    assert "■</span> 2 AWAITING BAR&nbsp;" in render_arm_html(pend, footer=NODE)


def test_the_uncovered_window_strip_never_shows_the_trailing_semicolon() -> None:
    page = BY_CONTEXT["ops.catchup_incomplete: downtime catch-up incomplete: demo-broker"].html
    assert TIME_RE.findall(_part(page, "time-strip")) == ["2026-11-20T22:00:00Z", "2026-11-20T23:00:00Z"]
    notes = render_alert_html(msg_of("info", "high", "downtime catch-up incomplete: a1",
                                     "a1 (startup): 1 orders / 0 positions seen, 0 anomalies; uncovered window "
                                     "2026-11-20T22:00:00Z .. 2026-11-20T23:00:00Z; retention."))
    times = TIME_RE.findall(_part(notes, "time-strip"))
    assert len(times) == 2 and times[-1].endswith("Z") and not any(";" in t for t in times)


@pytest.mark.parametrize(("event", "revision", "sub"), [
    ("CORRECTED", 3, "after bar end · revision 3"), ("REOPEN", 0, "after bar end"), ("LATE", 0, "after bar end · late"),
])
def test_corrected_reopen_and_late_adjusts_are_on_the_final_advice_step(event: str, revision: int, sub: str) -> None:
    cells = [c for _, c in _cells(_arm_page(adjust(event=event, revision=revision), event=event), "advice-steps")]
    final = cells[2]                                      # step 1, arrow, step 2, arrow, step 3
    assert "Final advice" in final and sub in final and ">now</span>" in final
    assert "now" not in cells[0] + cells[4]
    if event == "REOPEN":
        assert "revision" not in final and "late" not in final


# ---------------------------------------------------------------------------------------------------- T3 more
def _ping_msg() -> AlertMessage:
    s = next(x for x in SAMPLES if x.context.startswith("arm.failed_ping: ARM FAILED [ping-demo-1]"))
    return msg_of("info", "critical", "ARM FAILED [ping-demo-1] route demo-route", s.text.split("\n\nkind: ")[0])


def _repage_msg() -> AlertMessage:
    return msg_of("repage", "high", "x", "risk_band (cap NONE)", episode=("max_position_risk", "position", "P", 1, 2))


@pytest.mark.parametrize("which", ["ping", "repage"])
def test_t3_a_failing_step_list_brings_back_the_single_box_with_the_same_action(
        monkeypatch: pytest.MonkeyPatch, which: str) -> None:
    m = _ping_msg() if which == "ping" else _repage_msg()
    action = classify(m).action
    assert action and "xv-steps" in render_alert_html(m)
    monkeypatch.setattr(V, "step_list", _boom)
    page = render_alert_html(m, NODE)
    text = visible(page)
    assert "xv-steps" not in page and "What to do:" in text and action in text


def test_t3_a_failing_rule_ladder_or_advice_stepper_leaves_a_valid_page(monkeypatch: pytest.MonkeyPatch) -> None:
    ep = msg_of("episode", "high", "x", "risk_band (cap SOFT, sweep 1)",
                episode=("max_position_risk", "position", "P", 1, 2))
    monkeypatch.setattr(V, "rule_ladder", _boom)
    page = render_alert_html(ep, NODE)
    check_page(page)
    assert "xv-rule-ladder" not in page and "xv-hero" in page and "What to do:" in visible(page)
    monkeypatch.setattr(V, "advice_steps", _boom)
    page = _arm_page(adjust())
    check_page(page)
    assert "xv-advice-steps" not in page and "xv-stop-ladder" in page and "This server sends nothing" in visible(page)


def test_t3_a_failing_side_chip_restores_the_old_side_text(monkeypatch: pytest.MonkeyPatch) -> None:
    pend = NoticeBody(notice_id=6, route="main", event="POST", bar_spec="M15", bar_end_ms=T, created_ms=T,
                      pending=(PendingBody(adapter_id="ad", position_id="P1", symbol="SYN.A", side="BUY"),))
    ping = _ping_msg()
    monkeypatch.setattr(V, "side_chip", _boom)
    field = _arm_page(adjust())
    check_page(field)
    assert "xv-side" not in field and "SYN.A · long" in visible(field)
    pp = render_arm_html(pend, footer=NODE)
    check_page(pp)
    assert "xv-side" not in pp and ">long</td>" in pp and ">BUY</td>" not in pp      # the word, as before
    pg = render_alert_html(ping, NODE)
    check_page(pg)
    assert "xv-side" not in pg and ">BUY</td>" in pg and ">SELL</td>" in pg          # raw codes, as before


def test_t3_a_failing_summary_is_left_out(monkeypatch: pytest.MonkeyPatch) -> None:
    body = notice(adjust(), adjust(position_id="P2", outcome="NO_STOP", category="FAULT", proposed_stop=None,
                                   stop_now=None))
    assert "xv-summary" in render_arm_html(body)
    monkeypatch.setattr(V, "summary_chips", _boom)
    page = render_arm_html(body, footer=NODE)
    check_page(page)
    assert "xv-summary" not in page and _notice_badge(page) == "FINAL ADVICE"
