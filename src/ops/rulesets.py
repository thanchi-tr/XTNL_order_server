"""Ruleset documents for the operator CLI (IMPLEMENTATION_PLAN 4.9; oversee-rules.md s6-s7, operations.md s12).

Every `rulesets` row the CLI writes, and every row it activates, is validated with the engine's own parser
(`parse_ruleset`), so the CLI can never store or activate a row the engine would reject on reload:

* `check_document(text, version)`: parse the operator's JSON over the spec defaults; any problem refuses
  (`invalid_ruleset`, every problem listed). The stored form is `ruleset_json(cfg)`: the complete, explicit
  ruleset (Decimals as strings), so a later change of a code default never changes a stored version, and
  `parse_ruleset(stored) == cfg` is asserted before anything is written.
* `check_row(config, version)`: the same parser over a stored row (for `show` and before `activate`).

No broker call, no order action: a ruleset only sets Oversee thresholds.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, Final

from src.db.pg import json_dumps
from src.oversee.config import OverseeConfig
from src.oversee.ruleset import RulesetError, parse_ruleset, ruleset_json

from src.ops.model import OpsRefused

MAX_DOCUMENT_BYTES: Final = 256 * 1024


@dataclass(frozen=True)
class CheckedRuleset:
    config: OverseeConfig
    canonical: str                 # JSON text written to rulesets.config
    sections: tuple[str, ...]      # top-level sections the operator's document set
    warnings: tuple[str, ...]


def _doc_sections(text: str) -> tuple[tuple[str, ...], bool]:
    try:
        doc = json.loads(text, parse_float=Decimal)
    except ValueError:
        return (), False
    if not isinstance(doc, Mapping):
        return (), False
    sweep = doc.get("sweep")
    sets_interval = isinstance(sweep, Mapping) and "interval_ms" in sweep
    return tuple(str(k) for k in doc), sets_interval


def check_document(text: str, version: int) -> CheckedRuleset:
    """Validate an operator's ruleset document; raises OpsRefused('invalid_ruleset', ...) on any problem."""
    if len(text.encode("utf-8")) > MAX_DOCUMENT_BYTES:
        raise OpsRefused("invalid_ruleset", f"document larger than {MAX_DOCUMENT_BYTES} bytes")
    try:
        cfg = parse_ruleset(text, version)
    except RulesetError as exc:
        raise OpsRefused("invalid_ruleset", "; ".join(exc.problems)) from exc
    canonical_doc = ruleset_json(cfg)
    canonical = json_dumps(canonical_doc)
    try:
        again = parse_ruleset(canonical, version)
    except RulesetError as exc:   # pragma: no cover - ruleset_json is the inverse of parse_ruleset
        raise OpsRefused("roundtrip_mismatch", "; ".join(exc.problems)) from exc
    if again != cfg:
        raise OpsRefused("roundtrip_mismatch", "ruleset_json(cfg) does not parse back to the same ruleset")
    sections, sets_interval = _doc_sections(text)
    warnings: list[str] = []
    if sets_interval:
        warnings.append("sweep.interval_ms is a deployment setting: a running term keeps its own value and "
                        "pages if this one differs")
    if not sections:
        warnings.append("the document sets nothing: this version is the spec ruleset")
    return CheckedRuleset(cfg, canonical, sections, tuple(warnings))


def check_row(config: object, version: int) -> tuple[OverseeConfig | None, tuple[str, ...]]:
    """(config, ()) for a usable stored row, (None, problems) for one the engine would reject."""
    try:
        return parse_ruleset(config, version), ()
    except RulesetError as exc:
        return None, exc.problems


def same_ruleset(a: OverseeConfig, b: OverseeConfig) -> bool:
    """Equal thresholds, whatever the version numbers."""
    return replace(a, ruleset_version=0) == replace(b, ruleset_version=0)


def effective_json(cfg: OverseeConfig) -> str:
    return json_dumps(ruleset_json(cfg))


def summary(cfg: OverseeConfig) -> dict[str, Any]:
    """A few headline thresholds for the CLI output (the full document is `effective`)."""
    def band(name: str) -> str:
        b = getattr(cfg, name)
        return f"flag>{b.flag_above_pct}% soft>{b.halt_above_pct}%" + ("" if b.enabled else " (disabled)")

    return {
        "max_position_risk": band("max_position_risk"),
        "max_aggregate_open_risk": band("max_aggregate_open_risk"),
        "margin_usage": band("margin_usage"),
        "max_daily_loss": f"hard<-{cfg.max_daily_loss.limit_pct_of_sod_equity}%"
                          + ("" if cfg.max_daily_loss.enabled else " (disabled)"),
        "missing_stop_grace_ms": cfg.missing_stop_loss.grace_ms,
        "escalate_after_ms": cfg.sweep.escalate_after_ms,
    }


__all__ = ["CheckedRuleset", "MAX_DOCUMENT_BYTES", "check_document", "check_row", "effective_json", "same_ruleset",
           "summary"]
