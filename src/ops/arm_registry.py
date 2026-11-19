"""The pinned-registry check (design §B6.1 Version lifecycle, PR13; plan S35).

A strategy `(KIND, IMPL_VERSION)`, guard `(KIND, VERSION)` or indicator `(KIND, VERSION)` is never removed from the
registry while any `arm_positions` row or accepted document references it: live evaluation, replay and corrections
read only the pin. Two checks hold a deploy to that:

* **`PINNED_TOKENS`** is the reviewed list of every `KIND:VERSION` a deployed database may reference (every version
  ever released for pinning). A release that adds a version adds its token here; a token is never taken out. The
  pytest leg (`tests/ops/test_arm_views.py::test_pinned_tokens_all_load_in_this_registry`) and, once the user
  approves it, `verify_execution.py` (proposal `docs/design/proposed-claude-changes/S35-pinned-registry-check.md`)
  fail a build whose registry lacks one.
* **`python -m src.ops arm registry check --against-db`** (`src/ops/arm_view.py`) reads the versions the database
  really references (`arm_positions` pins, guard lists and indicator specs; `arm_decisions` strategy versions; the
  cached accepted document) and refuses (exit 3) when this node's registry lacks one.

Tokens are ids only (`KIND:VERSION`); nothing here reads a level, a price or a parameter value.
"""
from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Final

from src.riskmgr.registry import REGISTRY, ArmRegistry

PINNED_TOKENS: Final[frozenset[str]] = frozenset({
    "break_even_on_risk_close:1",   # kind (a), S20
    "first_bar:1",                  # FirstBarGuard, S20
    "ATR:1",                        # md-indicators ATR, S18 (kind (a) / FirstBarGuard's required indicator)
})
"""Every `KIND:VERSION` released for pinning (append-only; never shortened)."""

KIND_RE: Final = re.compile(r"[A-Za-z0-9_]{1,64}")
"""A kind as a token part: letters, digits and `_` only, so a token read from the database or a document is safe to
print as it is (anything else is unreadable: fail closed)."""


def token(kind: object, version: object) -> str | None:
    """`KIND:VERSION` for a kind of the `KIND_RE` form and a positive integer version, else None."""
    if (not isinstance(kind, str) or not KIND_RE.fullmatch(kind)
            or not isinstance(version, int) or isinstance(version, bool) or version < 1):
        return None
    return f"{kind}:{version}"


def _entries(raw_json: str | None) -> list[Mapping[str, object]] | None:
    if raw_json is None:
        return []
    try:
        raw = json.loads(raw_json)
    except (ValueError, RecursionError):
        return None
    if not isinstance(raw, list) or not all(isinstance(e, Mapping) for e in raw):
        return None
    return list(raw)


@dataclass
class PinTokens:
    """The tokens one or more stored pins reference, and the pins that could not be read (fail closed)."""

    tokens: set[str] = field(default_factory=set)
    unreadable: list[str] = field(default_factory=list)


def pin_tokens(kind: object, impl_version: object, guards_json: str | None, indicators_json: str | None,
               *, where: str, into: PinTokens | None = None) -> PinTokens:
    """The tokens of one stored pin: the strategy, each guard of its guard list and each indicator spec. A part that
    does not parse is reported in `unreadable` (by `where`, never by its text)."""
    out = into if into is not None else PinTokens()
    t = token(kind, impl_version)
    if t is None:
        out.unreadable.append(f"{where}: strategy kind / version")
    else:
        out.tokens.add(t)
    for name, raw in (("guards", guards_json), ("indicator_specs", indicators_json)):
        entries = _entries(raw)
        if entries is None:
            out.unreadable.append(f"{where}: {name}")
            continue
        for e in entries:
            t = token(e.get("kind"), e.get("version"))
            if t is None:
                out.unreadable.append(f"{where}: {name} entry")
            else:
                out.tokens.add(t)
    return out


def missing_tokens(wanted: Iterable[str], registry: ArmRegistry = REGISTRY) -> list[str]:
    """The wanted tokens this node's registry cannot load (sorted)."""
    have = set(registry.registry_tokens())
    return sorted(set(wanted) - have)


def pinned_registry_problems(registry: ArmRegistry = REGISTRY) -> list[str]:
    """The `PINNED_TOKENS` this node's registry lacks (empty: every released version still loads)."""
    return [f"{t} is pinned for release but not in the registry" for t in missing_tokens(PINNED_TOKENS, registry)]


__all__ = ["PINNED_TOKENS", "PinTokens", "missing_tokens", "pin_tokens", "pinned_registry_problems", "token"]
