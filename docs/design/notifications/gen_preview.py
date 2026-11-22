"""Generate `docs/design/notifications/preview.html`: every notification context rendered with synthetic data.

    .venv/Scripts/python.exe docs/design/notifications/gen_preview.py [out.html]

Every value here is made up (symbols `SYN.*`, account `ACC-DEMO-*`, adapter `demo-broker`, levels and ids):
no real account, position or price. The page shows each context's HTML email part in a frame, labelled with its
context name and layout, plus the unchanged plain-text part. `samples()` is also used by
`tests/notifications/test_email_html.py`, so every context shown here is checked there.
"""
from __future__ import annotations

import html
import re
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.notifications.email_html import LAYOUTS, classify, render_alert_html  # noqa: E402
from src.notifications.email_html_arm import arm_layout, render_arm_html  # noqa: E402
from src.oversee.notify import OutboxRow, render_message  # noqa: E402
from src.riskmgr.render import (  # noqa: E402
    AliveCounts,
    ExtraBody,
    FailedLine,
    ItemBody,
    NoticeBody,
    PendingBody,
    RenderSettings,
    render,
)

T0 = 1_795_147_200_000            # 2026-11-20 04:00 UTC (synthetic)
BAR = T0 + 15 * 60_000
ACC = "ACC-DEMO-1"
ADP = "demo-broker"
NODE = ("node-demo-a", "1042")
DIRECT = ("node-demo-pc", None)
OUT = Path(__file__).with_name("preview.html")


@dataclass(frozen=True)
class Sample:
    context: str
    layout: str
    subject: str
    html: str
    text: str


def _alert(kind: str, sev: str, title: str, body: str, *, episode: tuple[str, str, str, int, int] | None = None,
           account: str | None = None, outbox_id: int = 9001, direct: bool = False) -> Sample:
    msg = render_message(OutboxRow(kind, sev, title, body, T0 + 61_000,  # type: ignore[arg-type]
                                   episode=episode, account_id=account, outbox_id=None if direct else outbox_id))
    ctx = classify(msg)
    page = render_alert_html(msg, (lambda: DIRECT) if direct else (lambda: NODE))
    return Sample(f"{ctx.name}: {title}", ctx.layout, msg.subject, page, msg.body)


def _item(**kw: object) -> ItemBody:
    base: dict[str, object] = dict(
        decision_id=501, decision_bar_end_ms=BAR, adapter_id=ADP, position_id="POS-DEMO-7", symbol="SYN.EURX",
        side="BUY", event="POST", bar_end_ms=BAR, outcome="ADJUST", category="ACTIONABLE", advice=True,
        proposed_stop=Decimal("1.08420"), current_risk_r=Decimal("0.42"), caveats=(), quality_flags=("ok",),
        stop_basis="broker_stop", trigger_source="BROKER", handles=("SL-DEMO-3",), stop_read_at_ms=BAR + 20_000,
        stop_now=Decimal("1.08010"), entry=Decimal("1.08420"), s0=Decimal("1.08010"), decided_at_ms=BAR + 40_000,
        strategy_kind="break_even_on_risk_close", strategy_impl_version=1)
    base.update(kw)
    return ItemBody(**base)  # type: ignore[arg-type]


def _notice(event: str, **kw: object) -> NoticeBody:
    base: dict[str, object] = dict(notice_id=77, route="demo-route", event=event, bar_spec="M15", bar_end_ms=BAR,
                                   part=0, created_ms=BAR + 45_000)
    base.update(kw)
    return NoticeBody(**base)  # type: ignore[arg-type]


def _arm(context: str, body: NoticeBody, settings: RenderSettings | None = None) -> Sample:
    s = settings or RenderSettings(show_levels=True)
    text = render(body, settings=s)
    subject = f"[XTNL INFO] {text.subject}"
    page = render_arm_html(body, footer=lambda: NODE, settings=s, outbox_id=9100 + body.notice_id, title=subject)
    return Sample(context, arm_layout(body), subject, page, text.text)


PING_BODY = "\n".join([
    "scope: route demo-route", "reason: strategy_error_budget", "edge: E1",
    "counters: strategy_errors=3, timeouts=0", "first counted fault: 2026-11-21 00:12:00 UTC",
    "positions (2):", "  demo-alias demo-broker:POS-DEMO-7 SYN.EURX BUY",
    "  demo-alias demo-broker:POS-DEMO-8 SYN.GBPX SELL",
    "no advice for these positions until FAILED is cleared; do not act on advice for them issued since "
    "2026-11-21 00:12:00 UTC",
    "clear, after fixing the cause: arm failed clear route:demo-route --reason <text>",
])


def samples() -> list[Sample]:
    """Every context of the design (§4, §5) with synthetic data."""
    out: list[Sample] = []
    a = out.append
    # L1 risk alerts
    a(_alert("episode", "info", "FLAG max_position_risk position:POS-DEMO-7", "risk_band (cap NONE, sweep 88001)",
             episode=("max_position_risk", "position", "POS-DEMO-7", 3, 1), account=ACC))
    a(_alert("episode", "high", "SOFT_HALT max_aggregate_open_risk account:ACC-DEMO-1",
             "risk_band (cap NONE, sweep 88002)", episode=("max_aggregate_open_risk", "account", ACC, 4, 2),
             account=ACC))
    a(_alert("episode", "critical", "HARD_HALT max_daily_loss account:ACC-DEMO-1:2026-11-20",
             "daily_loss (cap NONE, sweep 88003)", episode=("max_daily_loss", "account", f"{ACC}:2026-11-20", 5, 3),
             account=ACC))
    a(_alert("episode", "high", "SOFT_HALT missing_stop_loss order:ORD-DEMO-2", "no_stop (cap NONE, sweep 88004)",
             episode=("missing_stop_loss", "order", "ORD-DEMO-2", 6, 2), account=ACC))
    a(_alert("episode", "high", "SOFT_HALT max_position_risk position:POS-DEMO-9",
             "mark_unknown (cap SOFT, sweep 88005)",
             episode=("max_position_risk", "position", "POS-DEMO-9", 7, 2), account=ACC))
    a(_alert("episode", "info", "FLAG stale_account_image account:ACC-DEMO-1", "fenced (cap SOFT, sweep 88006)",
             episode=("stale_account_image", "account", ACC, 8, 1), account=ACC))
    a(_alert("repage", "high", "still SOFT_HALT max_position_risk position:POS-DEMO-7", "risk_band (cap NONE)",
             episode=("max_position_risk", "position", "POS-DEMO-7", 3, 2), account=ACC))
    for t, level in (("resolved", "PASS"), ("lowered", "FLAG"), ("capped", "SOFT_HALT"), ("uncapped", "SOFT_HALT")):
        a(_alert("info", "info", f"{t} max_position_risk position:POS-DEMO-7",
                 f"risk_band (level {level}, cap {'SOFT' if t == 'capped' else 'NONE'})", account=ACC))
    a(_alert("episode", "high", "SOFT_HALT margin_usage account:ACC-DEMO-1", "unexpected shape",
             episode=("margin_usage", "account", ACC, 9, 2), account=ACC))
    a(_alert("episode", "high", f"sod_partial_session account:{ACC} 2026-11-20", "late_roll,stale_mid",
             episode=("sod_partial_session", "account", f"{ACC}:2026-11-20", 0, 2), account=ACC))
    # L2 halt changes
    a(_alert("trip", "high", "HALT SOFT", "max_aggregate_open_risk: rule_state"))
    a(_alert("trip", "critical", "HALT HARD", "-: escalation"))
    a(_alert("info", "info", "HALT cleared", "cool_down"))
    a(_alert("info", "high", "HALT cleared by operator-demo", "reviewed after the drill -> SOFT_HALT"))
    a(_alert("info", "high", "HALT cleared by operator-demo-2", "drill -> MAYBE"))
    # L3 operational
    ops = [
        ("info", "downtime catch-up: demo-broker",
         "demo-broker: engine was offline from 2026-11-20T22:00:00Z to 2026-11-21T00:01:00Z (2 h 1 min); catch-up "
         "(startup): 1 orders new, 2 updated, 0 positions closed while offline; 1 positions new, 0 cash flows new; "
         "complete"),
        ("high", "downtime catch-up incomplete: demo-broker",
         "demo-broker (startup): 4 orders / 2 positions seen, 1 anomalies; uncovered window 2026-11-20T22:00:00Z .. "
         "2026-11-20T23:00:00Z; anomalies: ORD-DEMO-5; retention. The adapter's fence was held until now; check the "
         "broker's order history for that window"),
        ("info", "downtime catch-up: demo-broker-2", "demo-broker-2: catch-up summary in an older shape"),
        ("high", "clock moved back: demo-broker", "coverage ahead of the wall clock by 95 s"),
        ("high", "cash-flow ledger incomplete: demo-broker", "1 closing trade without a realised P&L id"),
        ("high", "ingestion frame lost: demo-broker:orders", "frame 1204 lost; resync requested"),
        ("high", "consumer commit failed: orders-consumer", "OperationalError"),
        ("info", "market sessions need review: demo-broker", "2 notes"),
        ("high", "market session unusable: demo-broker:SYN.EURX", "no weekly session"),
        ("info", "maintenance: demo-broker", "window until 2026-11-21T02:00:00Z"),
        ("high", "calendar:calendar_mismatch", "demo-broker:SYN.EURX"),
        ("high", "flag:pnl_unverified", "account ACC-DEMO-1"),
        ("info", "adapter re-armed: demo-broker", "by operator-demo"),
        ("high", "oversee:feed lag", "pending 5120 entries"),
        ("high", "oversee:sod_missing", f"account={ACC} trading_day=2026-11-20"),
        ("info", "oversee:dependent_alerts_summary", "held 3; reasons fence; accounts ACC-DEMO-1"),
        ("high", "oversee:dependent_alerts_escalation", "still fenced: ACC-DEMO-1"),
        ("high", "integrity:dlq_quarantined", "dlq 311 entity ORD-DEMO-5"),
        ("info", "integrity:integrity_resolved", "redis heal complete"),
        ("high", "task orders-consumer stale", "stamp 75 s old; restarted"),
        ("high", "optional services failed to start", "XTNL_MD_ENABLED set, redis-md unreachable"),
        ("info", "lifecycle: partitions past 90 days await export", "2 partitions"),
        ("info", "ruleset v7 loaded", "activated by operator-demo"),
        ("info", "SAFE_HOLD auto-resumed on the single PC", "after a 300 s cool-down"),
        ("info", "engine recording again on the single PC", "leader term 1042"),
        ("high", "ARM: paused for more than 6 h", "paused since 2026-11-20T17:00:00Z"),
        ("high", "ARM notice #77 expired undelivered", "route demo-route, event POST, part 0: an ADJUST notice expired "
                                                       "before any delivery completed (see arm decisions)"),
        ("high", "ARM channel arm_email failing", "arm_email: smtp auth rejected (535)"),
        ("high", "broker bars unavailable", "demo-broker SYN.EURX M15"),
        ("info", "a title no rule knows", "free text body"),
    ]
    for sev, title, body in ops:
        a(_alert("info", sev, title, body))
    # L4 critical
    a(_alert("info", "critical", "ARM FAILED [ping-demo-1] route demo-route", PING_BODY))
    a(_alert("info", "critical", "ARM FAILED re-ping 2 [ping-demo-1] route demo-route", PING_BODY))
    crit = [
        ("ARM FAILED not recorded [ping-demo-2]", "PostgreSQL write failed"),
        ("broker AUTH_FAILED: demo-broker", "3 attempts; credential generation 12"),
        ("PRE_OPEN readiness failed: demo-broker", "not streaming; token valid < 24 h"),
        ("task risk-sweep circuit open", "6 deaths in 10 min"),
        ("oversee:oversight_lapsed", f"account={ACC} quarantined after 5 min"),
        ("integrity:integrity_halted", "seq_id regression on orders_state"),
        ("ruleset v8 rejected at LEADER_INIT", "unknown rule key"),
        ("ARM configuration tampered", "hash chain mismatch"),
    ]
    for title, body in crit:
        a(_alert("info", "critical", title, body))
    a(_alert("info", "critical", "SAFE_HOLD auto-resume stopped on the single PC", "6 auto-resumes within 24 h",
             direct=True))
    a(_alert("info", "critical", "engine not recording on the single PC", "no leader term for 10 min", direct=True))
    # L5 ARM advice
    a(_arm("arm.pre_adjust (bar-0 heads-up)", _notice("PRE", items=(_item(
        event="PRE", caveats=("first_bar_conditional",), trigger_met=True, valid_until_ms=BAR + 120_000,
        stop_now=Decimal("1.08010"), current_risk_r=Decimal("0.42")),))))
    a(_arm("arm.pre_adjust (valid until before bar end)", _notice("PRE", items=(_item(
        event="PRE", valid_until_ms=BAR - 10_000, decided_at_ms=BAR - 40_000, stop_read_at_ms=BAR - 45_000),))))
    a(_arm("arm.post_adjust (final advice)", _notice("POST", items=(_item(),))))
    a(_arm("arm.post_adjust (show_levels off)", _notice("POST", items=(_item(),)), RenderSettings()))
    a(_arm("arm.post_adjust (older body, no stop_now)", _notice("POST", items=(_item(stop_now=None),))))
    a(_arm("arm.post_adjust (short side)", _notice("POST", items=(_item(
        symbol="SYN.GBPX", side="SELL", entry=Decimal("1.27000"), s0=Decimal("1.27410"), stop_now=Decimal("1.27410"),
        proposed_stop=Decimal("1.27000")),))))
    a(_arm("arm.post_adjust (entry outside the path)", _notice("POST", items=(_item(
        strategy_kind="demo_trail_kind", entry=Decimal("1.08000"), s0=Decimal("1.07600"), stop_now=Decimal("1.08200"),
        proposed_stop=Decimal("1.08420")),))))
    a(_arm("arm.post_adjust (stop already at proposed)", _notice("POST", items=(_item(
        stop_now=Decimal("1.08420")),))))
    a(_arm("arm.late_adjust", _notice("LATE", item_key="late-1", items=(_item(event="LATE", late=True),))))
    a(_arm("arm.corrected", _notice("CORRECTED", item_key="corr-1", items=(_item(
        event="CORRECTED", revision=2, outcome="BROKER_AHEAD", category="INFO", supersedes=500),))))
    a(_arm("arm.corrected (ADJUST, revision 3)", _notice("CORRECTED", item_key="corr-2", items=(_item(
        event="CORRECTED", revision=3, supersedes=501),))))
    a(_arm("arm.post_delayed", _notice("POST", items=(_item(caveats=("delayed",), decided_at_ms=BAR + 7 * 60_000),))))
    others = [
        ("COMPLETE", "INFO", {"loosen_policy": "REARM"}), ("PRE_NOT_CONFIRMED", "INFO", {"reason": "trigger_unmet"}),
        ("AT_TARGET_UNCONFIRMED", "HOLD", {}), ("LOOSENED_AFTER_COMPLETE", "INFO", {}),
        ("STRATEGY_REASON", "INFO", {"reason": "below_threshold", "reason_kind": "break_even_on_risk_close"}),
        ("NO_BROKER_BAR", "INFO", {}), ("MARKET_CLOSED", "HOLD", {}), ("PRICE_THROUGH_LEVEL", "HOLD", {}),
        ("NO_STOP", "FAULT", {}), ("DATA_UNAVAILABLE", "FAULT", {"reason": "series_gapped"}),
        ("UNRESOLVED_RANGE", "HOLD", {"reason": "bars_pending"}),
    ]
    a(_arm("arm.post_mixed (ADJUST + other outcomes)", _notice("POST", part=5, items=(
        _item(), *(_item(decision_id=600 + n, position_id=f"POS-DEMO-{20 + n}", outcome=o, category=c, advice=False,
                         proposed_stop=None, handles=(), stop_now=None, **kw)
                   for n, (o, c, kw) in enumerate(others))))))
    a(_arm("arm.complete (released)", _notice("POST", items=(_item(
        outcome="COMPLETE", category="INFO", advice=False, proposed_stop=None, handles=(), stop_now=None,
        loosen_policy="REARM"),))))
    a(_arm("arm.post_pending (part 0)", _notice("POST", pending=(
        PendingBody(adapter_id=ADP, position_id="POS-DEMO-30", symbol="SYN.GBPX", side="SELL"),))))
    # L6 ARM status
    a(_arm("arm.alive", _notice("ALIVE", bar_end_ms=0, bar_spec="", alive=AliveCounts(armed=3, faulting=1,
                                                                                         unmanaged=2),
                                failed=(FailedLine(scope_kind="route", scope_id="demo-route",
                                                   reason="strategy_error_budget"),))))
    a(_arm("arm.alive (no FAULT)", _notice("ALIVE", bar_end_ms=0, bar_spec="", alive=AliveCounts(
        armed=4, faulting=0, unmanaged=1))))
    a(_arm("arm.test", _notice("TEST", bar_end_ms=0, bar_spec="", channel="arm_email")))
    a(_arm("arm.md_reset", _notice("MD_RESET", bar_end_ms=0, bar_spec="", generation="gen-demo-4")))
    a(_arm("arm.failed_cleared", _notice("FAILED_CLEARED", bar_end_ms=0, bar_spec="", extras=(
        ExtraBody(kind="FAILED_CLEARED", reason="route:demo-route", at_ms=BAR),))))
    a(_arm("arm.extras (tracker notices)", _notice("POST", extras=(
        ExtraBody(kind="SUSPENDED", adapter_id=ADP, position_id="POS-DEMO-7", symbol="SYN.EURX", side="BUY",
                  reason="units_increase", at_ms=BAR),
        ExtraBody(kind="POSITION_NOT_MANAGED", adapter_id=ADP, position_id="POS-DEMO-40", symbol="SYN.GBPX",
                  side="SELL", reason="no_route", at_ms=BAR)))))
    return out


VISUALS: tuple[tuple[str, str, str, str, str], ...] = (
    ("V1", "hero", "Hero row", "A glyph tile (a WGL4 symbol, or two pause bars for a halt) beside the badges and "
     "the headline.", "L1 to L6"),
    ("V2", "rule-ladder", "Rule-level ladder", "PASS · FLAG · SOFT · HARD, the current level marked; HARD dashed "
     "when capped at SOFT.", "L1"),
    ("V3", "halt-ladder", "Halt-level ladder", "NONE · SOFT · HARD with what each level lets through.", "L2"),
    ("V4", "stop-ladder", "Ordinal stop ladder", "Proposed stop, current stop and (with show_levels) entry in price "
     "order, fixed spacing, not to scale.", "L5 ADJUST"),
    ("V5", "advice-steps", "Advice stepper", "Heads-up → Final advice (→ Released), the current step marked.",
     "L5 ADJUST, COMPLETE"),
    ("V6", "count-tiles", "Count tiles", "Big number over its label (catch-up counts, ALIVE counts).", "L3, L6"),
    ("V7", "time-strip", "Time strip", "Two times in order with the body's own duration text, not to scale.",
     "L3 catch-up, L5 PRE / delayed"),
    ("V8", "side", "Side chips", "Neutral LONG (BUY) / SHORT (SELL) pills, the same style for both sides.",
     "L4, L5, L6"),
    ("V9", "steps", "Numbered steps", "\"What to do\" as numbered steps with a command chip.",
     "L1 re-page, L4 ping"),
    ("V10", "summary", "Summary chips", "One chip per card label with its count, for notices with 2 or more cards.",
     "L5, L6"),
)
_CLASS_RE = re.compile(r'class="xv-([a-z-]+)"')


def visuals_of(page: str) -> list[str]:
    """The visual elements a page carries (its `xv-*` markers), in `VISUALS` order."""
    found = set(_CLASS_RE.findall(page))
    return [name for _, key, name, _, _ in VISUALS if key in found]


def _legend(items: list[Sample]) -> str:
    rows = []
    for vid, key, name, desc, lays in VISUALS:
        hits = [n for n, s in enumerate(items) if f'class="xv-{key}"' in s.html]
        links = ("every sample" if len(hits) == len(items) else
                 " ".join(f'<a href="#s{n}" title="{html.escape(items[n].context, quote=True)}">{n + 1}</a>'
                          for n in hits) or "none")
        rows.append(f"<tr><td>{vid}</td><td><strong>{html.escape(name)}</strong><br>"
                    f'<span class="lay">{html.escape(desc)}</span></td><td>{html.escape(lays)}</td>'
                    f'<td class="links">{links}</td></tr>')
    return ('<h2 class="sec">Visual elements</h2><table class="legend"><tr><th>#</th><th>Element</th>'
            "<th>Layouts</th><th>Samples</th></tr>" + "".join(rows) + "</table>")


def build_preview(items: list[Sample] | None = None) -> str:
    items = samples() if items is None else items
    toc = "".join(f'<li><a href="#s{n}">{html.escape(s.context)}</a> <span class="lay">{s.layout}</span></li>'
                  for n, s in enumerate(items))
    cards = []
    for n, s in enumerate(items):
        size = len(s.html.encode("utf-8"))
        vis = ", ".join(visuals_of(s.html)) or "none"
        cards.append(
            f'<section id="s{n}"><h2>{n + 1}. {html.escape(s.context)} <span class="lay">{s.layout} '
            f'{html.escape(LAYOUTS[s.layout])}</span></h2>'
            f'<p class="subj">Subject: <code>{html.escape(s.subject)}</code></p>'
            f'<iframe title="{html.escape(s.context)}" srcdoc="{html.escape(s.html, quote=True)}"></iframe>'
            f'<p class="meta">Visuals: {html.escape(vis)} · Size: {size / 1024:.1f} KB ({size:,} B)</p>'
            f'<details><summary>Plain-text part (unchanged)</summary><pre>{html.escape(s.text)}</pre></details>'
            "</section>")
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        "<title>Notification Email Preview</title><style>"
        ":root{--bg:#F2F4F7;--card:#FFFFFF;--ink:#101828;--muted:#475467;--line:#D0D5DD}"
        "@media (prefers-color-scheme: dark){:root:not([data-theme=light]){--bg:#0C111D;--card:#161B26;"
        "--ink:#F5F5F6;--muted:#94969C;--line:#333741}}"
        ":root[data-theme=dark]{--bg:#0C111D;--card:#161B26;--ink:#F5F5F6;--muted:#94969C;--line:#333741}"
        "body{margin:0;padding:16px;background:var(--bg);color:var(--ink);font:14px/1.5 Arial,Helvetica,sans-serif}"
        "main{max-width:720px;margin:0 auto}h1{font-size:22px;margin:8px 0}h2{font-size:16px;margin:0 0 4px}"
        "h2.sec{margin:18px 0 8px}"
        ".lay{font-weight:400;color:var(--muted);font-size:12px}section{background:var(--card);border:1px solid "
        "var(--line);border-radius:8px;padding:14px;margin:0 0 18px}iframe{width:100%;max-width:100%;"
        "min-height:320px;border:0;display:block;margin:0 auto}pre{white-space:pre-wrap;word-break:break-word;"
        "font:12px/1.5 Consolas,Menlo,monospace}"
        "code{word-break:break-all}.subj{margin:0 0 8px;color:var(--muted)}.meta{margin:6px 0;color:var(--muted);"
        "font-size:12px}a{color:inherit}"
        "ol{columns:2;column-gap:24px;padding-left:20px}@media (max-width:560px){ol{columns:1}}"
        ".legend{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--line);"
        "margin:0 0 12px}.legend th,.legend td{border-top:1px solid var(--line);padding:6px 8px;text-align:left;"
        "vertical-align:top;font-size:13px}.legend .links a{display:inline-block;min-width:22px;margin:0 2px 2px 0;"
        "text-align:center;border:1px solid var(--line);border-radius:4px;text-decoration:none;font-size:12px}"
        ".bar{position:sticky;top:0;z-index:2;display:flex;gap:8px;align-items:center;padding:8px 0;"
        "background:var(--bg)}.bar button{font:600 13px Arial,Helvetica,sans-serif;padding:6px 12px;"
        "border:1px solid var(--line);border-radius:6px;background:var(--card);color:var(--ink);cursor:pointer}"
        ".bar button[aria-pressed=true]{border-color:var(--ink)}"
        "</style></head><body><main>"
        "<h1>Notification email preview</h1>"
        f"<p>{len(items)} contexts, rendered by <code>src/notifications/email_html.py</code>, "
        "<code>email_html_arm.py</code> and <code>email_visuals.py</code> with synthetic data only (made-up "
        "symbols, ids and levels). Each frame is the HTML part; the plain-text part below it is sent unchanged. "
        "Regenerate with <code>.venv/Scripts/python.exe docs/design/notifications/gen_preview.py</code>.</p>"
        '<div class="bar" role="group" aria-label="Frame width"><button type="button" data-w="100%" '
        'aria-pressed="true">Desktop 600</button><button type="button" data-w="375px" aria-pressed="false">'
        "Phone 375</button></div>"
        + _legend(items)
        + f"<ol>{toc}</ol>" + "".join(cards) + "</main>" + FIT_SCRIPT + "</body></html>\n")


FIT_SCRIPT = (
    "<script>function fit(f){try{var d=f.contentDocument;if(!d||!d.body)return;f.style.height='0px';"
    "f.style.height=(d.documentElement.scrollHeight+8)+'px';}catch(e){}}"
    "document.querySelectorAll('iframe').forEach(function(f){f.addEventListener('load',function(){fit(f);});fit(f);});"
    "window.addEventListener('resize',function(){document.querySelectorAll('iframe').forEach(fit);});"
    "document.querySelectorAll('.bar button').forEach(function(b){b.addEventListener('click',function(){"
    "document.querySelectorAll('.bar button').forEach(function(x){x.setAttribute('aria-pressed',String(x===b));});"
    "document.querySelectorAll('iframe').forEach(function(f){f.style.width=b.getAttribute('data-w');fit(f);});});});"
    "</script>")


def main(argv: list[str]) -> int:
    out = Path(argv[1]) if len(argv) > 1 else OUT
    out.write_text(build_preview(), encoding="utf-8", newline="\n")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
