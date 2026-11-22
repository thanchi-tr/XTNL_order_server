# XTNL website design spec for the live view

Extracted 2026-11-22, read-only, from the XTNL website source (`XTNL/src/XTNL_thesis`); refs are relative to it.

# XTNL ops dashboard restyle spec (merged from 4 reader reports, conflicts checked against source)

All refs are relative to `C:\Users\Thanc\OneDrive\Desktop\XTNL\src\XTNL_thesis\`. Nothing under XTNL was modified.

**Lines I checked myself in source:**
- `app/globals.css`:
  - 1-41: tokens
  - 53-77: body and mono
  - 101-157: type classes and cards
  - 175-217: buttons and chips
  - 261-268: status bar
  - 350-352: scrollbar
  - 387-391: pulse-dot
  - 410-430: chip blur, focus ring, selection
  - 999-1007: layout vars
  - 1040-1053: ticker mask
- `components/analytics/kit.tsx:58-66, 115-134`
- `app/session/SessionClient.tsx:131-134, 214-218, 289-303, 583`
- `components/governance/StepRail.tsx:20-56`
- `components/ui/DialogShell.tsx:109-163` (the "data" report gave wrong line numbers for this file; these are the correct ones)
- `components/session/Sev1Banner.tsx:68-86`
- `components/hero/HeroBackground.tsx:229`

Every other ref comes from the reader reports.

**Conflicts I resolved:**
- **`--ink-4`.** It is referenced but never defined (NavBar.tsx:394 and others), so on the site it inherits its colour. The only fallback the site declares is `#4a5a6a` (HeroBackground.tsx:229). Do **not** define ink-4. For readable text use ink-2. Use ink-3 only for decoration, because `#2a3d52` on `#0b1622` is about 1.6:1. The site itself raises ticker labels from ink-3 to ink-2 on phones (globals.css:1052).
- **Card radius.** The marketing `.card` is 12px (globals.css:144). Logged-in dense cards are 6px: Session `card` with `borderRadius: 6` (SessionClient.tsx:583) and kit CARD (kit.tsx:61). On the ops page, use 6px for dense panels and tiles, and keep 12px for large shell sections and the drawer edge.
- **Tile value font.** kit Tile uses proportional sans at 20px/600 (kit.tsx:120, with a comment saying so). MetricCard uses mono 26px/800. Ops values change live, so use mono with tabular-nums (`.data-value`, globals.css:132-135) to avoid jitter.
- **Chip font.** `.chip` is **sans**, not mono (globals.css:205). Status pills are **mono** (StepRail.tsx:50).

---

## 1. CSS custom properties

```css
:root {
  color-scheme: dark;                          /* layout.tsx:20-21 (themeColor #04080f) */
  /* surfaces  globals.css:7-12 */
  --canvas:#020508; --base:#04080f; --sub:#07101c; --card:#0b1622; --raised:#0f1e2e; --lift:#152436;
  /* ink  globals.css:15-18 */
  --ink-0:#eef2f8; --ink-1:#9ab0c8; --ink-2:#5a7490; --ink-3:#2a3d52;
  /* accents  globals.css:20-29 */
  --green:#00cc7a; --green-hi:#00f090; --green-10:rgba(0,204,122,.10); --green-06:rgba(0,204,122,.06);
  --red:#f03a57;   --red-10:rgba(240,58,87,.10);
  --blue:#4d9cf5;  --blue-10:rgba(77,156,245,.10);
  --amber:#f0a030; --amber-10:rgba(240,160,48,.10);
  /* lines  globals.css:31-33 */
  --line:rgba(255,255,255,.06); --line-hi:rgba(255,255,255,.11); --line-act:rgba(255,255,255,.20);
  /* shadows  globals.css:35-37 */
  --shadow-sm:0 2px 8px rgba(0,0,0,.45); --shadow-md:0 8px 28px rgba(0,0,0,.55); --shadow-lg:0 20px 56px rgba(0,0,0,.65);
  /* chrome heights  globals.css:39-40, 1004 */
  --nav-h:60px; --bar-h:32px;
  /* fonts: no web fonts -> the site's own fallbacks (globals.css:59,76) + Windows faces */
  --font-sans: ui-sans-serif, system-ui, "Segoe UI", sans-serif;
  --font-mono: ui-monospace, "Cascadia Mono", "Cascadia Code", Consolas, monospace;
  /* derived (not site tokens, named here for reuse) */
  --glass: rgba(4,8,15,.90);   /* NavBar.tsx:233-241, PipelineBanner.tsx:748-760 */
  --glass-solid: rgba(4,8,15,.97); /* dropdown/drawer NavBar.tsx:352-358, 626-676 */
  --green-ring: rgba(0,204,122,.75); /* focus globals.css:423 */
  --chart-live:#3987e5; --chart-grid:rgba(255,255,255,.06); --chart-base:rgba(255,255,255,.16); /* kit.tsx:23-49 */
}
@media (max-width:600px){ :root{ --nav-h:52px; --bar-h:26px; } }   /* globals.css:1004 */
```

The site has no light theme: there is no `prefers-color-scheme` rule and no `data-theme` attribute. Any light-mode support would be our own addition and would not match the site.

## 2. Typography mapping

| Role on the ops page | Site style | CSS |
|---|---|---|
| Body | body (globals.css:53-66) | sans 15px / 1.75, ink-0; 16px / 1.7 at ≤600px (68-73) |
| Page title (h1) | Profile h1 (ProfileClient.tsx:107-111) | sans `clamp(22px,3vw,28px)` / 700, `letter-spacing:-.03em`, line-height 1.1, ink-0; then a 1px `--line` rule 16px below |
| Alternative page title in console style | Governance h1 (AnalyticsClient.tsx:180) | mono 18px / 700, `.12em`, uppercase |
| Section eyebrow | `.section-eyebrow` (globals.css:111-119) | mono 10px / 700, `.14em`, uppercase, `--green`, `text-shadow:0 0 14px rgba(0,204,122,.14)`; 11px at ≤600px |
| Panel eyebrow (neutral) | kit EYEBROW (kit.tsx:62) | mono 10.5px / 600, `.14em`, uppercase, ink-1 |
| Card / panel title | CardHeader title (SessionClient.tsx:297) | sans 13px / 600, `-.01em`, ink-0 |
| Small chart title | `.panel-title` (globals.css:122-129) | sans 11px / 600, `.04em`, ink-0, no text-transform |
| Field / tile label | `.label-xs` (101-108) | sans 10.5px / 600, `.04em`, uppercase, ink-2; 12px at ≤600px |
| Mono tile label | kit Tile (kit.tsx:119) | mono 9.5px, `.04em`, ink-2 |
| Meta, timestamps, IDs, table cells | TD (SessionClient.tsx:217); `.jl-meta` | mono 12.5px ink-1 (meta mono 11px / 1.4 ink-2); always `tabular-nums` |
| Big KPI value | MetricCard | mono 26px / 800, line-height 1, coloured by state |
| Tile value | kit Tile, size only | 20px / 600, line-height 1.3, ink-0, **but in mono with tabular-nums** (see conflicts) |
| Clock | `.session-clock` (globals.css:471-477) | mono 26px / 700, `.06em`, line-height 1; 20px at ≤480px |
| Muted helper text | kit MUTED (kit.tsx:63) | 12px / 1.5, ink-2 |

## 3. Component recipes

```css
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}                 /* globals.css:46 */
html{scroll-padding-top:calc(var(--nav-h) + var(--bar-h) + 24px)}               /* :48-51 */
body{background-color:var(--base);
  background-image:radial-gradient(ellipse 130% 50% at 50% -6%,rgba(0,204,122,.034) 0%,transparent 62%),
                   radial-gradient(ellipse 46% 34% at 92% 94%,rgba(0,130,255,.014) 0%,transparent 50%);
  color:var(--ink-0);font:15px/1.75 var(--font-sans);-webkit-font-smoothing:antialiased;min-height:100vh;overflow-x:hidden} /* :53-66 */
code,pre,.mono,.num{font-family:var(--font-mono)} .num{font-variant-numeric:tabular-nums}  /* :75-77,132-135 */
:focus-visible{outline:2px solid var(--green-ring);outline-offset:2px;border-radius:6px}    /* :422-426 */
.clip :focus-visible{outline-offset:-2px}                                                  /* :429 */
::selection{background:rgba(0,204,122,.20);color:var(--ink-0)}                             /* :430 */
::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--line-act);border-radius:3px}                    /* :350-352 */
.scroll{scrollbar-width:thin;scrollbar-color:var(--line-act) transparent}                  /* :296-297 */
.wrap{max-width:1360px;margin:0 auto;padding-inline:clamp(20px,5vw,80px)}                  /* .site-container :82-89 */
a{transition:color .15s,opacity .15s}                                                       /* :420 */
```

### Header / nav with logo
Sources: NavBar.tsx:232-278 and globals.css:229-246. The ops page always uses the scrolled state.

```css
.nav{position:fixed;top:0;left:0;right:0;height:var(--nav-h);z-index:100;background:var(--glass);
  backdrop-filter:blur(18px) saturate(180%);border-bottom:1px solid rgba(0,204,122,.1);box-shadow:0 4px 32px rgba(0,0,0,.38)}
.nav .wrap{height:100%;display:flex;align-items:center;justify-content:space-between}
.brand{display:flex;align-items:center;gap:11px;text-decoration:none}
.brand-mark{width:27px;height:27px;filter:drop-shadow(0 0 6px rgba(0,204,122,.65)) drop-shadow(0 0 16px rgba(0,204,122,.4))}
.brand-word{font:800 17px/1 var(--font-mono);letter-spacing:.13em;background:linear-gradient(180deg,#fff 8%,#8ff2b8 150%);
  -webkit-background-clip:text;background-clip:text;color:transparent;filter:drop-shadow(0 0 12px rgba(0,204,122,.42))}
.brand-sub{font:9px/1 var(--font-mono);letter-spacing:.14em;color:var(--ink-2);padding-left:11px;border-left:1px solid var(--line-hi)}
.nav-links{display:flex;gap:32px}
.nav-link{position:relative;font-size:11px;font-weight:500;letter-spacing:.05em;text-transform:uppercase;color:var(--ink-2);padding:4px 0;text-decoration:none;transition:color .15s}
.nav-link::after{content:"";position:absolute;left:0;right:0;bottom:-1px;height:1px;background:var(--green);transform:scaleX(0);transform-origin:left;transition:transform .2s ease}
.nav-link:hover{color:var(--ink-0)} .nav-link:hover::after,.nav-link[aria-current="page"]::after{transform:scaleX(1)}
.nav-link[aria-current="page"]{color:var(--green)}
.nav-pill{display:inline-flex;align-items:center;gap:6px;background:var(--green-10);border:1px solid rgba(0,204,122,.20);border-radius:5px;padding:5px 10px;font:600 10.5px var(--font-mono);color:var(--green)} /* NavBar.tsx:333-349, e.g. "READ-ONLY" */
```

**Logo SVG.** Inline it (allowed, since it is neither external nor a style attribute). Geometry from XtnlLogo.tsx:16-63:
- viewBox `0 0 80 80`, linearGradient `#3dffb0 → #00cc7a 55% → #009e60`
- Lines (27,8)→(63,52) at stroke 3.4, and (51,13)→(17,52) at stroke 2.6 / opacity .72
- Dashed midline at y=52 with dasharray `4 3`
- Cyan `#2fd0ff` dot at (63,52)
- Nodes at (27,8) and (51,13) filled `#00f090` / `#e6fff4`

Put the node glow in a CSS class: `filter:drop-shadow(0 0 3px rgba(0,240,144,.9))`.

### Status strip
Sources: globals.css:261-268, 1040-1053; StatusBar.tsx:39-167.

```css
.status-bar{position:fixed;top:var(--nav-h);left:0;right:0;z-index:90;height:var(--bar-h);
  background:linear-gradient(to right,#020508,#030710,#020508);border-bottom:1px solid rgba(255,255,255,.05);display:flex;align-items:center;overflow:hidden}
.status-bar .wrap{display:flex;align-items:center;gap:14px;width:100%}
.sb-dot{width:5px;height:5px;border-radius:50%;background:var(--ink-3)} .sb-dot.on{background:var(--green);animation:pulse-dot 2s ease-in-out infinite}
.sb-state{font:700 9px var(--font-mono);letter-spacing:.08em;text-transform:uppercase;color:var(--green);transition:color .5s}
.sb-sep{width:1px;height:12px;background:var(--line)}
.sb-clock{font:10px var(--font-mono);letter-spacing:.04em;color:var(--ink-2)}
.sb-items{flex:1;display:flex;gap:34px;overflow:hidden;
  mask-image:linear-gradient(to right,transparent 0,#000 60px,#000 calc(100% - 60px),transparent 100%)}
.sb-item{display:flex;gap:7px;align-items:baseline;white-space:nowrap}
.sb-label{font:500 9px var(--font-sans);letter-spacing:.04em;text-transform:uppercase;color:var(--ink-2)} /* site uses ink-3; ink-2 for legibility (site does this on phones :1052) */
.sb-value{font:700 10px var(--font-mono);font-variant-numeric:tabular-nums}
.sb-ver{font:9px var(--font-mono);letter-spacing:.06em;color:var(--ink-3)}
@media (max-width:600px){.sb-state{display:none}.sb-clock,.sb-label,.sb-value{font-size:11px}
  .sb-items{mask-image:linear-gradient(to right,transparent 0,#000 14px,#000 calc(100% - 14px),transparent 100%)}}
```

Make it a static strip. The site's 52s scrolling ticker (StatusBar.tsx) is wrong for ops status, which must stay readable; see section 4.

### Page offset and layout

```css
main{padding-top:calc(var(--nav-h) + var(--bar-h) + 20px);padding-bottom:64px}  /* SessionClient.tsx:5006-5009 */
.ops-grid{display:flex;gap:18px} .ops-feed{flex:1;min-width:0;display:flex;flex-direction:column;gap:14px}
.ops-console{width:356px;flex-shrink:0;position:sticky;top:calc(var(--nav-h) + var(--bar-h) + 20px);align-self:flex-start;display:flex;flex-direction:column;gap:14px} /* globals.css:440-468 */
```

### Panels / cards (dense)
Sources: globals.css:140-157; SessionClient.tsx:289-303, 583.

```css
.panel{background-color:var(--card);background-image:linear-gradient(160deg,rgba(255,255,255,.026) 0%,transparent 46%);
  border:1px solid var(--line);border-radius:6px;box-shadow:inset 0 1px 0 rgba(255,255,255,.055);min-width:0;overflow:hidden}
.panel--lg{border-radius:12px}                                   /* shell sections on the Edge page */
.panel-head{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:6px 10px;padding:8px 16px;border-bottom:1px solid var(--line)}
.panel-head h2{font-size:13px;font-weight:600;letter-spacing:-.01em;color:var(--ink-0)}
.panel-body{padding:16px 18px}                                   /* kit CARD kit.tsx:61 */
.panel-strip{background:#07101c;padding:12px 20px;border-bottom:1px solid var(--line)} /* HourlyHeatmap.tsx:13-19 */
@media (hover:hover){.card-hover{transition:border-color .18s ease,transform .18s ease,box-shadow .18s ease}
 .card-hover:hover{border-color:rgba(0,204,122,.22);transform:translateY(-2px);box-shadow:0 12px 34px rgba(0,0,0,.42),0 0 0 1px rgba(0,204,122,.08)}}
```

Apply the hover lift only to clickable story cards, never to static tiles.

### Status tiles (status band and Edge page)
Sources: kit.tsx:117-125; ReviewStep.tsx:106-116, 333.

```css
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px}
.tile{background:var(--raised);border:1px solid var(--line);border-radius:6px;padding:10px 12px;min-width:0}
.tile-label{font:9.5px var(--font-mono);letter-spacing:.04em;color:var(--ink-2);text-transform:uppercase}
.tile-value{font:600 20px/1.3 var(--font-mono);font-variant-numeric:tabular-nums;color:var(--ink-0);margin-top:2px}
.tile-sub{font-size:11px;color:var(--ink-2);margin-top:1px}
.tile[data-tone=ok]{border-left:3px solid var(--green)} .tile[data-tone=warn]{border-left:3px solid var(--amber)}
.tile[data-tone=bad]{border-left:3px solid var(--red)} .tile[data-tone=info]{border-left:3px solid var(--blue)} /* accent-left, ProfileClient.tsx:66 */
.kpi{font:800 26px/1 var(--font-mono);font-variant-numeric:tabular-nums}           /* MetricCard */
```

### Chips (sans) and status pills (mono)
Sources: globals.css:199-217, 413-415; StepRail.tsx:23-55; kit.tsx:128-133.

```css
.chip{display:inline-flex;align-items:center;padding:3px 9px;border-radius:6px;font:700 10px/1.4 var(--font-sans);letter-spacing:.06em;text-transform:uppercase;white-space:nowrap;backdrop-filter:blur(4px)}
.chip-ok{background:var(--green-10);color:var(--green);border:1px solid rgba(0,204,122,.20)}
.chip-warn{background:var(--amber-10);color:var(--amber);border:1px solid rgba(240,160,48,.20)}
.chip-bad{background:var(--red-10);color:var(--red);border:1px solid rgba(240,58,87,.20)}
.chip-info{background:var(--blue-10);color:var(--blue);border:1px solid rgba(77,156,245,.20)}
.chip-muted{background:var(--sub);color:var(--ink-2);border:1px solid var(--line)}
.pill{display:inline-flex;align-items:center;gap:6px;padding:3px 10px;border-radius:999px;font:700 11px/1.3 var(--font-mono);letter-spacing:.03em}
.pill--sm{gap:4px;padding:2px 6px;font-size:10px}
.pill[data-s=done]{color:var(--green);border:1px solid rgba(0,204,122,.40);background:rgba(0,204,122,.10)}
.pill[data-s=attention]{color:var(--amber);border:1px solid rgba(240,160,48,.45);background:rgba(240,160,48,.10)}
.pill[data-s=blocked]{color:var(--red);border:1px solid rgba(240,58,87,.45);background:rgba(240,58,87,.10)}
.pill[data-s=todo],.pill[data-s=info]{color:var(--ink-1);border:1px solid var(--line-hi);background:transparent}
.tag{font:700 9px var(--font-mono);letter-spacing:.08em;padding:2px 6px;border-radius:3px} /* DataClient.tsx:132, colour X at 0x14 bg / 0x35 border */
```

**Severity mapping for ops.** Every state carries a glyph plus a word, never colour alone (kit.tsx:14-15, StepRail.tsx:10).

| Severity | Colour | Glyph and word |
|---|---|---|
| PASS / ok | green | ✓ |
| FLAG / watch | amber | ▲ |
| SOFT/HARD HALT, breach | red | ✕, or the lock path from StepRail.tsx:26 |
| informational | blue | |
| stale / unknown | muted | ○ |

Use these status backgrounds: ok `.06`, warning and critical `.07` (kit.tsx:128-133).

### Buttons / toggles / tabs

```css
.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;border:none;cursor:pointer;border-radius:10px;font:600 12px var(--font-sans);letter-spacing:.04em;white-space:nowrap;transition:all .15s ease} /* globals.css:175-180 */
.btn-ghost{background:var(--sub);color:var(--ink-1);padding:8px 16px;border:1px solid var(--line)}
.btn-secondary{background:transparent;color:var(--ink-1);padding:10px 22px;border:1px solid var(--line-hi)}
.btn-soft{min-height:40px;padding:0 14px;border-radius:8px;border:1px solid rgba(0,204,122,.45);background:rgba(0,204,122,.10);color:var(--green);font-size:13px} /* AnalyticsClient.tsx:41-45 */
@media (hover:hover){.btn-ghost:hover{background:var(--raised);color:var(--ink-0)}.btn-secondary:hover{border-color:var(--line-act);color:var(--ink-0)}
  button:not(:disabled):hover{box-shadow:0 0 4px -1.5px currentColor;transition:box-shadow .18s ease}} /* :169-174 */
.btn:disabled{opacity:.4;cursor:not-allowed} .btn:active:not(:disabled){transform:scale(.97)}
.icon-btn{background:none;border:none;padding:4px;color:var(--ink-2);min-width:40px;min-height:40px} /* SessionClient.tsx:307-315; site uses ink-3 */
.close-btn{width:32px;height:32px;border-radius:5px;border:1px solid var(--line-hi);background:transparent;color:var(--ink-2)} /* SessionClient.tsx:2524-2528 */
.seg{padding:6px 12px;border-radius:6px;font:700 10.5px var(--font-mono);letter-spacing:.06em;text-transform:uppercase;border:1px solid var(--line-hi);background:transparent;color:var(--ink-1)}
.seg[aria-pressed=true]{border-color:var(--green);background:rgba(0,204,122,.10);color:var(--green)}        /* kit.tsx:101-108 */
.tabs{display:flex;border-bottom:1px solid var(--line)}
.tab{padding:9px 20px;font-size:12.5px;color:var(--ink-3);border-bottom:2px solid transparent;margin-bottom:-1px;transition:color .15s,border-color .15s}
.tab[aria-selected=true]{color:var(--ink-0);font-weight:700;border-bottom-color:var(--green)}               /* SessionClient.tsx:5175-5186 */
.switch{width:38px;height:22px;border-radius:11px;border:1px solid var(--line-hi);background:var(--raised);position:relative}
.switch::after{content:"";position:absolute;top:2px;left:2px;width:16px;height:16px;border-radius:50%;background:var(--ink-3);transition:left .2s}
.switch[aria-checked=true]{background:var(--green)} .switch[aria-checked=true]::after{left:18px;background:#04080f} /* DesktopNotifyToggle.tsx:120-133 */
```

The ops page has no actions, so leave out `.btn-primary`. If you ever need it: `linear-gradient(135deg,#00d880,#00b86c)`, colour `#04140c`, padding `11px 22px` (globals.css:181).

### Tables / lists / story cards

```css
.tbl{width:100%;border-collapse:collapse}
.tbl th{text-align:left;padding:8px 12px;font:600 10.5px var(--font-sans);color:var(--ink-2);letter-spacing:.05em;text-transform:uppercase;white-space:nowrap;border-bottom:1px solid var(--line-hi);position:sticky;top:0;background:var(--card)}
.tbl td{padding:9px 12px;font:12.5px var(--font-mono);font-variant-numeric:tabular-nums;color:var(--ink-1);white-space:nowrap;border-bottom:1px solid var(--line)} /* SessionClient.tsx:216-217 */
@media (hover:hover){.tbl tbody tr:hover{background:var(--raised);transition:background .1s}}
.tbl tr[aria-selected=true]{background:rgba(77,156,245,.14)} .tbl tr[aria-selected=true] td:first-child{box-shadow:inset 3px 0 0 var(--blue)} /* globals.css:689-690 */
.kv{display:flex;justify-content:space-between;align-items:baseline;padding:10px 0;border-bottom:1px solid var(--line)}
.kv dt{font-size:12px;color:var(--ink-2)} .kv dd{font:600 12.5px var(--font-mono);color:var(--ink-1)}      /* ProfileClient.tsx:12-19 */
.story{background:var(--sub);border:1px solid var(--line);border-left:3px solid var(--accent,var(--line-hi));border-radius:6px;padding:10px 14px} /* .jl-row globals.css:776-797 */
.story[data-tone=ok]{--accent:#00cc7a}.story[data-tone=warn]{--accent:#f0a030}.story[data-tone=bad]{--accent:rgba(240,58,87,.6)}.story[data-tone=info]{--accent:#4d9cf5}
.story-meta{font:500 11px/1.4 var(--font-mono);color:var(--ink-2)} .story-text{font-size:13px;line-height:1.5;color:var(--ink-1)}
.day-label{position:sticky;top:0;background:var(--card);border-bottom:1px solid var(--line);font:700 11px/1.4 var(--font-mono);letter-spacing:.06em;text-transform:uppercase;color:var(--ink-2);padding:6px 0}
```

### Breach cards, ladders, timeline, sparklines

```css
.breach{background:rgba(240,58,87,.06);border:1px solid rgba(240,58,87,.4);border-left:3px solid var(--red);border-radius:8px;padding:10px 12px;font-size:13px} /* ReviewStep.tsx:253 */
.breach--flag{background:rgba(240,160,48,.06);border-color:rgba(240,160,48,.16);border-left-color:var(--amber)}         /* MonteCarloSimulator.tsx:634 */
.ladder{display:flex;height:6px;border-radius:3px;background:var(--line-hi);overflow:hidden}                            /* AnalyticsClient.tsx:186-189 */
.ladder .pass{background:var(--green)}.ladder .flag{background:var(--amber)}.ladder .halt{background:var(--red)}
.ladder-mark{width:2px;background:var(--ink-0)}
.rung{display:flex;justify-content:space-between;padding:4px 8px;font:11px var(--font-mono)} .rung:nth-child(even){background:rgba(255,255,255,.025)} /* kit.tsx:237-250 */
.timeline{position:relative;height:56px;border-bottom:1px solid var(--chart-base)}
.tl-grid{stroke:var(--chart-grid)} .tl-tick{fill:var(--ink-2);font:10px var(--font-mono)} .tl-now{stroke:var(--line-act)} /* kit TICK :64; reference line kitChart.tsx:358 */
.tl-ev-ok{fill:var(--green)}.tl-ev-warn{fill:var(--amber)}.tl-ev-bad{fill:var(--red)}.tl-ev-info{fill:var(--blue)}
.spark{fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round;stroke:var(--chart-live)}   /* lines 2px, no dots kit.tsx:342 */
.spark--ok{stroke:var(--green)}.spark--warn{stroke:var(--amber)}.spark--bad{stroke:var(--red)}
.spark-area--bad{fill:rgba(240,58,87,.10)} .spark-threshold{stroke:var(--line-act);stroke-dasharray:2 4}
```

Sparklines must plot only engine-side state (for example rule-state history or risk %), never prices, ticks or spreads. CLAUDE.md keeps market data off pages.

### Drawer (slide-in detail)
Sources: NavBar.tsx:626-682 and DialogShell.tsx:109-125.

```css
.drawer-backdrop{position:fixed;inset:0;z-index:98;background:rgba(0,0,0,.68);backdrop-filter:blur(4px);opacity:0;pointer-events:none;transition:opacity .25s}
.drawer{position:fixed;top:calc(var(--nav-h) + var(--bar-h));right:0;bottom:0;z-index:99;width:min(480px,100vw);
  background:var(--glass-solid);backdrop-filter:blur(20px);border-left:1px solid var(--line-hi);box-shadow:0 24px 64px rgba(0,0,0,.6);
  padding:22px 24px;overflow-y:auto;transform:translateX(104%);transition:transform .28s cubic-bezier(.4,0,.2,1)}
.drawer.open{transform:none}.drawer-backdrop.open{opacity:1;pointer-events:auto}
```

The site's drawer slides from the top (`translateY(-108%)`). Our side drawer reuses its glass, timing and easing.

### Banners

```css
.banner{padding:7px 16px;text-align:center;font:700 11px var(--font-mono);letter-spacing:.04em}
.banner--sev1{background:linear-gradient(rgba(240,58,87,.14),rgba(240,58,87,.14)),var(--base);border-bottom:1px solid rgba(240,58,87,.4);color:#f03a57} /* Sev1Banner.tsx:68-86 */
.banner--warn{background:linear-gradient(rgba(240,160,48,.12),rgba(240,160,48,.12)),var(--base);border-bottom:1px solid rgba(240,160,48,.4);color:var(--amber)} /* derived */
.banner--ro{background:var(--green-06);border:1px solid rgba(0,204,122,.25);color:var(--green);border-radius:6px} /* derived from toggle container DesktopNotifyToggle.tsx:96-100 */
.inline-err{display:flex;gap:7px;padding:8px 10px;border-radius:5px;background:rgba(240,58,87,.08);border:1px solid rgba(240,58,87,.28);color:var(--red);font-size:11.5px} /* DialogShell.tsx:162-163 */
```

The SEV1 banner is fixed at `top:calc(var(--nav-h) + var(--bar-h))` with z-index 95.

### Empty states and locked rows
No direct site precedent exists for locked rows. The recipe below combines the pending pipeline step (PipelineBanner.tsx:313-333) with the lock path from StepRail.tsx:26.

```css
.empty{padding:44px 0;text-align:center;font-size:12.5px;color:var(--ink-2)}                                      /* SessionClient.tsx:704-705 */
.empty-box{padding:14px 16px;border-radius:8px;background:var(--card);border:1px solid var(--line);font-size:12px;color:var(--ink-2)} /* ProfileClient.tsx:174 */
.empty-title{font-size:13px;font-weight:600;color:var(--ink-1)} .empty-detail{font:11.5px var(--font-mono);color:var(--ink-2)} /* DataClient.tsx:288-318 */
.locked{display:flex;align-items:center;gap:8px;padding:9px 12px;border:1px solid rgba(255,255,255,.07);border-radius:4px;background:transparent;color:var(--ink-2);font-size:12.5px;cursor:not-allowed}
.locked svg{width:12px;height:12px;stroke:currentColor;fill:none;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
.locked .chip{margin-left:auto}                                   /* e.g. .chip-muted "PHASE 2" */
.skeleton{background:linear-gradient(90deg,var(--sub) 0%,var(--raised) 40%,var(--lift) 50%,var(--raised) 60%,var(--sub) 100%);background-size:800px 100%;border-radius:4px;animation:shimmer 1.8s ease-in-out infinite} /* globals.css:394-410 */
```

The site's pending step uses ink-3 text. Use ink-2 instead for legibility.

### Bottom bar (optional)
Use it only if the page needs a persistent footer status, such as leader, epoch or connection. It mirrors PipelineBanner.tsx:748-779.

```css
.bottombar{position:fixed;left:0;right:0;bottom:0;z-index:97;background:var(--glass);backdrop-filter:blur(18px) saturate(180%);border-top:1px solid rgba(0,204,122,.10);box-shadow:0 -4px 32px rgba(0,0,0,.38);padding-bottom:env(safe-area-inset-bottom)}
.bottombar .wrap{height:44px;display:flex;align-items:center;gap:12px;font:700 9px var(--font-mono);letter-spacing:.12em;text-transform:uppercase;color:var(--ink-2)}
body.has-bottombar{padding-bottom:calc(45px + env(safe-area-inset-bottom))}
```

If you don't use it, use the site footer pattern instead: `border-top:1px solid rgba(0,204,122,.1)`, mono 10px ink-3 version line (Footer.tsx).

### Icons
All icons are inline SVG with `fill="none"`, `stroke="currentColor"`, and round caps and joins:
- 12–14px icons at stroke 1.2–1.8 (NavBar.tsx:372; StepRail.tsx:34-40)
- 9px check at stroke 1.5 using path `M1.5 5l2.5 2.5 4.5-4.5` (PipelineBanner.tsx:255-261)

The site uses no icon font.

## 4. Background and motion

**Keep:**
- The body double radial glow (globals.css:53-66). It is the only background on logged-in pages.
- Card sheen plus inset highlight.

```css
@keyframes pulse-dot{0%,100%{transform:scale(1);opacity:1}50%{transform:scale(1.5);opacity:.6}}            /* :387-391 */
@keyframes pulse-badge{0%,100%{box-shadow:0 0 0 0 rgba(0,204,122,.5)}50%{box-shadow:0 0 0 5px rgba(0,204,122,0)}} /* :213-217 */
@keyframes shimmer{0%{background-position:-400px 0}100%{background-position:400px 0}}                     /* :394 */
@keyframes fadeUp{from{opacity:0;transform:translateY(14px)}to{opacity:1;transform:none}}                    /* :362-366 */
.fade-up{animation:fadeUp .55s cubic-bezier(.4,0,.2,1) both}   /* first paint only, not on every live update */
@media (prefers-reduced-motion:reduce){*,*::before,*::after{animation:none!important;transition:none!important;scroll-behavior:auto!important}}
```

The reduced-motion rule is new: the site lacks one for pulse-dot, fadeUp, shimmer and the ticker.

**Timings:**
- .12s menu items, .15s colour/links/buttons/tabs, .18s card hover and glow, .2s toggles, .25–.28s drawer, .5s state-label colour
- Easing: `ease`; open/close uses `cubic-bezier(.4,0,.2,1)`

**Leave out:**
- The three.js Monte Carlo plume, ConstellationField, the home dot grid (app/page.tsx:60-66), the prospectus scanline (prospectus/page.tsx:109) and the analysis-complete grid overlay. None of these appear on logged-in pages.
- `.xtnl-shimmer-text`, `xtnlDividerTravel`, `.xtnl-cta-glow`, the hero gradient accent word, and count-up number animations.
- The scrolling 52s ticker. Use a static strip.
- `.card-hover` lift on non-interactive tiles.
- Pulsing: limit it to one live dot (status strip) plus pulsing chips for active halts. Do not pulse every badge, and add no flash or tween on value change (the site has none).

## 5. Responsive rules

| Breakpoint | Change | Source |
|---|---|---|
| ≥1200px | Inline nav links | globals.css:222-227 |
| <1200px | Collapse links behind a burger: three bars 20/20/14px at 1.5px, gap 5px; morph `translateY(±6.5px) rotate(±45deg)` over .22s. The burger toggle needs JS (allowed) and class changes (no inline style). For 2 links, simply keep them visible and drop `.brand-sub` instead. | NavBar.tsx:450-511 |
| ≤1099px | The two-column ops grid stacks: console under the feed, `width:auto`, `position:static`. Status strip gets `margin-left:14px`. | globals.css:1043 |
| ≤900px / ≤520px | Tile grids go 3 → 2 → 1 columns (`auto-fit minmax(170px,1fr)` already does this) | `.grid-3-col` :504-510 |
| ≤700px or `(pointer:coarse)` | Tap targets min 40px (hit area 44px via `::after{inset:-9px}`); the session two-column layout stacks | :529-541, :463 |
| ≤600px | `--nav-h:52px`, `--bar-h:26px`; body 16px / 1.7; `.label-xs` 12px, `.panel-title` 12.5px, eyebrow 11px; status-strip label hidden, values 11px, mask 14px | :68-73, :1004, :1047-1053 |
| ≤600px drawer | Full width (`width:100vw`) | |
| ≤480px | Clock 20px | :471-477 |

Also:
- Wide tables scroll horizontally inside the panel, with 28px edge fades `linear-gradient(to right,var(--card),transparent)` (:662-663).
- Gate every hover effect with `@media (hover:hover)`.
- Pad fixed bars with `env(safe-area-inset-bottom)`.

## 6. Top 12 must-match choices

1. Dark-only navy surfaces: body `#04080f` with the faint green/blue radial glows, panels `#0b1622`, sub `#07101c`, tiles `#0f1e2e` (globals.css:7-12, 53-57).
2. Ink ladder `#eef2f8` / `#9ab0c8` / `#5a7490` / `#2a3d52`. Keep ink-3 decorative only (:15-18).
3. One brand green `#00cc7a` for focus, active nav and tabs, eyebrows and the live dot. Red `#f03a57`, amber `#f0a030` and blue `#4d9cf5` are semantic only (:20-29).
4. Hairline white-alpha borders `.06` / `.11` / `.20`, with no solid grey borders (:31-33).
5. Panel recipe: card bg plus `linear-gradient(160deg,rgba(255,255,255,.026) 0%,transparent 46%)`, 1px `--line`, `inset 0 1px 0 rgba(255,255,255,.055)`. Radius 6px for dense panels, 12px for shells (:140-147; SessionClient.tsx:583).
6. Panel header: padding 8px 16px, bottom hairline, optional green mono eyebrow, then a 13px/600/-.01em title and a muted count chip (SessionClient.tsx:289-303).
7. Green mono eyebrow: 10px/700, `.14em`, uppercase, `text-shadow:0 0 14px rgba(0,204,122,.14)` (:111-119).
8. Every number, ID and timestamp in monospace with `tabular-nums` (:132-135).
9. Tinted status treatment: colour text on a `.06–.10` fill with a `.20–.45` border. Chips are sans 10px/700 `.06em` radius 6; pills are mono 11px/700 radius 999. Always glyph plus word (:199-210; StepRail.tsx:23-55).
10. Glass fixed header: `rgba(4,8,15,.90)` + `blur(18px) saturate(180%)` + `rgba(0,204,122,.1)` bottom border; glowing X mark, gradient mono `XTNL` wordmark (17/800/.13em), then the `| SOLUTIONS` tag (NavBar.tsx:233-265).
11. 32px status strip: `linear-gradient(to right,#020508,#030710,#020508)`, a 5px pulsing green dot, mono 9px uppercase state label, mono 10px clock, 1×12px dividers (globals.css:261-268; StatusBar.tsx:116-153).
12. Nav and tab active states: nav links 11px/500/.05em uppercase ink-2 with a 1px green underline that scales from the left; tabs get a 2px green bottom border; focus ring `2px solid rgba(0,204,122,.75)` offset 2px radius 6px; selection `rgba(0,204,122,.20)` (:229-246, :422-430; SessionClient.tsx:5175-5186).

## 7. Constraints: what cannot be reproduced and the closest substitute

- **Inter and JetBrains Mono** (next/font, layout.tsx:16-17) cannot load. Substitute `system-ui`/Segoe UI and `ui-monospace, "Cascadia Mono", "Cascadia Code", Consolas`.
  - Cascadia is narrower than JetBrains Mono. Keep the letter-spacing values as they are, and check that the `.13em` wordmark still reads well.
  - The site's mono stack is `ui-monospace, "Cascadia Code", monospace` (globals.css:76). Adding Consolas is our choice.
- **Inline `style={{}}`** is used everywhere on the site, but CSP forbids style attributes. Every recipe above is class-based in an external stylesheet. Express dynamic values (ladder fill, timeline positions, sparkline paths) as SVG geometry attributes (`x`, `width`, `d`), or set them from external JS through CSSOM properties (`el.style.setProperty`), which CSP permits. `style=""` in markup and `setAttribute('style')` are blocked.
- **Per-element colour hex-alpha** like `${color}0f` / `${color}28` (ProfileClient.tsx:81-88) cannot be passed inline. Use fixed tone classes (`data-tone=ok|warn|bad|info|muted`) with precomputed rgba values.
- **External resources** (favicon `app/icon.svg`, logo component) must be inlined. The logo is inline SVG with no external `<use>` reference.
- **`backdrop-filter`** works in a plain stylesheet. Over the flat body background the effect is barely visible, so the `.90` / `.97` alpha does the real work.
- **Keyframe animations defined in JSX** (`navAlarmPulse`, `_stepGlow` in NavBar.tsx:207-214 and PipelineBanner.tsx:281-298) must move into the stylesheet if you use them. `_stepGlow` could mark a currently escalating rule: `3.2s`, shadow 0 → `0 0 12px` at `.22`, border `.30` → `.55`.