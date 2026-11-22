"""Generate `docs/design/notifications/preview-xtnl.html`: every notification context in the XTNL theme.

    .venv/Scripts/python.exe docs/design/notifications/gen_preview_xtnl.py [out.html]

The same synthetic samples as `gen_preview.py` (made-up ids, symbols `SYN.*`, levels), rendered with
`theme="xtnl"` (`src/notifications/email_theme.py`). The ARM samples get a made-up pip unit per instrument, so the
levels block shows its distances in pips; the unit itself is never written to the page.
"""
from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import gen_preview as gp  # noqa: E402
from src.notifications import email_html, email_html_arm  # noqa: E402
from src.notifications.email_theme import PipUnit  # noqa: E402

OUT = HERE / "preview-xtnl.html"


class _AnyInstrument(dict[tuple[str, str], PipUnit]):
    """A made-up pip unit for every (adapter, symbol): a JPY-style size for `*JPY*` symbols, else the usual FX one."""

    def get(self, key: tuple[str, str], default: PipUnit | None = None) -> PipUnit | None:  # type: ignore[override]
        return PipUnit(Decimal("0.01") if "JPY" in key[1].upper() else Decimal("0.0001"), "pips")


def main(argv: list[str]) -> int:
    alert, arm = email_html.render_alert_html, email_html_arm.render_arm_html
    gp.render_alert_html = lambda msg, footer=None: alert(msg, footer, theme="xtnl")  # type: ignore[assignment]
    gp.render_arm_html = lambda body, **kw: arm(body, theme="xtnl", pip_units=_AnyInstrument(), **kw)  # type: ignore[assignment]
    out = Path(argv[1]) if len(argv) > 1 else OUT
    page = gp.build_preview().replace("<title>", "<title>XTNL theme · ", 1)
    out.write_text(page, encoding="utf-8", newline="\n")
    print(f"wrote {out.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
