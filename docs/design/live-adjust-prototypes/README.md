# Live stop adjustment — UI prototypes (design 3, Phases 3–5)

**Design only.** These are static HTML prototypes for [design 3](../arm-sl-sample/live-adjust.md). Nothing here is wired to the order-server, and nothing can send anything. Phase 1 stays read-only.

- **Look and theme:** the XTNL site theme, from [xtnl-site-spec.md](../live-view-prototypes/xtnl-site-spec.md).
- **Accessibility deviations from that theme:** secondary text `#7a92ad` and red text `#ff5a73`, so small text meets WCAG AA.
- **Data:** illustrative mock data from scenarios A and D. Levels, R values, ids and times only — no account values, money, pip size or spreads.

## Pages

| Page | Shows | Decisions |
|---|---|---|
| [approve.html](approve.html) | **Phase 3 approval.** The proposed stop move, a countdown to its expiry (at most 60 s after the bar-end decision), Approve / Reject, the SM13 steps (Proposed → Awaiting you → Approved → Checking → Sending → Acked → Confirmed), recent moves, and the later phone-tunnel note. On a phone the countdown comes first and Approve / Reject stay pinned at the bottom. | D4, D5 |
| [ownership.html](ownership.html) | **Stop ownership.** Which stops are yours after a manual move and which the server manages; advice-only history; **Hand back…** drawer with a required, audited reason. | D2 |
| [live-mode.html](live-mode.html) | **Live mode.** Autonomy phase (P1 / P3 / P4 / P5), live-moves switch, "can the server send right now?" tiles, open moves by state, prerequisites, and the halt and recovery choices. | D3, D6 |
| [alerts.html](alerts.html) | **Alerts.** Routine notices (switchable only in Phase 5) and the locked always-on safety list with severities. | D1 |
| [notices.html](notices.html) | **Notices.** Sample push notifications (Phase 3 with routine on; Phase 5 with safety only) and emails (approval request, the new "position ended" notice). | D1, E19 |

Who did what is always marked: a blue **YOU** tag and blue row edge for your actions, a green **SERVER** tag for the order-server's. States always pair a glyph with a word (✓ ▲ ✕ ○), never colour alone.

## Prototype controls

A dashed **Prototype** bar on some pages simulates states. It is not part of the design.

- **Approvals:** new request, 12 s left, refused at check. Approve / Reject run the full flow.
- **Live mode:** Normal, HARD halt, auto-resumed after SAFE_HOLD, dead-man lost, unknown send outcome.
- **Alerts:** show the page as in Phase 3 or Phase 5.

## View them

Open them through the local static server. A `file://` preview in the app drops the stylesheet.

```powershell
python -m http.server 8765 --bind 127.0.0.1 --directory docs/design/live-adjust-prototypes
```

Then open <http://127.0.0.1:8765/>. Screenshots at desktop and phone width are in this folder (`screenshot-*-desktop.png`, `screenshot-*-phone.png`).

## Checked

- **Reviews:** two review rounds — design fidelity, accessibility / responsive, and script behaviour — with every finding fixed.
- **Interaction tests in the browser:**
  - approve, expiry, refusal, a new request during a send;
  - the drawer's focus trap and Escape;
  - every live-mode state, phase change and the switch;
  - the alert switches across Phase 3 and Phase 5.
- **Phone width:** no horizontal overflow on any page at 375 px.
