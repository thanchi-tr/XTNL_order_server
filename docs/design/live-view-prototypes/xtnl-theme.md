# XTNL theme for the live view (taken from https://www.xtnl-solutions.com/, 2026-11-21)

The site is dark-only. Its CSS custom properties (`:root`) are reused as-is, so the live view reads as part of the
same product.

## Tokens

| Token | Value | Use on the site | Use in the live view |
|---|---|---|---|
| `--canvas` | `#020508` | page edge | outermost background |
| `--base` | `#04080f` | body background | page background |
| `--sub` | `#07101c` | sub-sections | rails / side panes |
| `--card` | `#0b1622` | cards | feed items, cards, tiles |
| `--raised` | `#0f1e2e` | raised cards | hovered / selected items, drawer |
| `--lift` | `#152436` | top layer | menus, chips on hover |
| `--ink-0` | `#eef2f8` | primary text | titles |
| `--ink-1` | `#9ab0c8` | body text | secondary text |
| `--ink-2` | `#5a7490` | muted text, labels | timestamps, eyebrows, axis labels |
| `--ink-3` | `#2a3d52` | faint text / rules | grid lines, disabled |
| `--green` | `#00cc7a` | brand accent, positive | brand, LIVE, NONE halt, healthy, take-profit |
| `--green-hi` | `#00f090` | highlight | arrival flash, focus ring |
| `--red` | `#f03a57` | negative | alert severity, HARD halt, stream lost, stop |
| `--amber` | `#f0a030` | warning | warn severity, SOFT halt, FLAG |
| `--blue` | `#4d9cf5` | info / operator | notice severity, operator actions, entry level |
| `--line` | `#ffffff0f` | hairline borders | borders |
| `--line-hi` | `#ffffff1c` | stronger borders | card borders on hover, dividers |
| `--line-act` | `#ffffff33` | active borders | selected item, focus |
| tints | each colour at `1a` (10 %) alpha, borders at `33` (20 %) | badges | severity tints, chips |
| brand gradient | `linear-gradient(104deg, #00f090 0%, #00cc7a 40%, #7df0b0 64%, #f0c874 100%)` | headline accent | wordmark accent only (optional; everything else stays flat) |

## Type

- Text: `Inter` (fallback `ui-sans-serif, system-ui, "Segoe UI", sans-serif`), headings 700-800 with tight letter
  spacing (-0.02em).
- Data, ids, times, eyebrows: `JetBrains Mono` (fallback `ui-monospace, "Cascadia Code", Consolas, monospace`).
- Eyebrow labels: mono, 10-11px, weight 700, `letter-spacing: 0.14em`, uppercase, colour `--ink-2` or `--green`
  (the site's `section-eyebrow`, e.g. "XTNL SOLUTIONS · THESIS DOCUMENT").
- The live view must not load web fonts (CSP `font-src` is none and the page works offline): it names the fonts and
  falls back to the system stacks above.

## Motifs

- A thin top **status ticker** strip (the site shows a scrolling mono strip of figures); in the live view it carries
  status only (halt, leader, streams, last poll), never values.
- Square corners (radius 0-4px), hairline borders, no shadows in the live view (flat).
- Small X-mark logo in green; "XTNL" wordmark in mono with wide letter spacing.

## Light variant

The site has none. The live view keeps a light toggle derived from the same hues: background `#f4f7fb`, cards
`#ffffff`, ink `#0b1622` / `#2a3d52` / `#5a7490`, lines `#0b16221a`, and the same accents darkened for contrast
on white (green `#00995c`, red `#d02545`, amber `#b86e00`, blue `#1f6fd1`). Dark is the default.
