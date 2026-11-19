"""The local switch of the S07 Oversee stop rule (plan S38f; design §0 C4, §0b "What turns each on").

`XTNL_OVERSEE_STOP_AT_ENTRY` (local only, never Supabase): `1` turns on the S07 / PR1 rule in
`src.oversee.risk.effective_stop_detail` (a STOP child counts at any price, also at or beyond the entry; a LIMIT child
never counts; the `stop_kind_unknown` / `stop_kind_inconsistent` caveats, the latter MissingStopLossRule's UNKNOWN).
Unset, empty or `0` keeps the exact pre-S07 rule (a linked reduce-only child counts only strictly on the loss side of
the entry, whatever its kind; no caveat). Any other value is off plus a logged problem. Read once at engine start
(`build_engine`); the process keeps it until restart.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from src.oversee.risk import set_stop_at_entry

STOP_AT_ENTRY_VAR: Final = "XTNL_OVERSEE_STOP_AT_ENTRY"


def stop_at_entry_from_env(environ: Mapping[str, str]) -> tuple[bool, str | None]:
    """True only for `1`; anything other than unset, empty, `0` or `1` is off plus a problem (names only)."""
    raw = (environ.get(STOP_AT_ENTRY_VAR) or "").strip()
    if raw in ("", "0"):
        return False, None
    if raw == "1":
        return True, None
    return False, f"{STOP_AT_ENTRY_VAR} must be 0 or 1 (taken as 0: the pre-S07 stop rule)"


def configure_stop_rule(environ: Mapping[str, str]) -> str | None:
    """Set the process-wide stop rule from `XTNL_OVERSEE_STOP_AT_ENTRY`; returns the problem, if any."""
    on, problem = stop_at_entry_from_env(environ)
    set_stop_at_entry(on)
    return problem


__all__ = ["STOP_AT_ENTRY_VAR", "configure_stop_rule", "stop_at_entry_from_env"]
