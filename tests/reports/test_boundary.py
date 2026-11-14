"""Phase 1 boundary for src/reports: read-only SQL, no adapter import, no order capability."""
from __future__ import annotations

import re
from pathlib import Path

import src.reports

ROOT = Path(src.reports.__file__).parent


def test_reports_package_is_read_only() -> None:
    forbidden = re.compile(r"\bdef\s+(place_order|modify_order|cancel_order|submit_order|close_position|"
                           r"amend_order|update_order)\b|src\.adapters|outbound_intent|\bFencedTx\b|"
                           r"\b(INSERT|UPDATE|DELETE|TRUNCATE)\s+(INTO\s+)?\w")
    files = list(ROOT.glob("*.py"))
    assert len(files) >= 4
    for f in files:
        assert not forbidden.search(f.read_text(encoding="utf-8")), f.name


def test_runbook_exists_and_names_the_command() -> None:
    text = (ROOT.parents[1] / "docs" / "runbooks" / "pinescript-export.md").read_text(encoding="utf-8")
    assert "$OPS pinescript" in text and "Pine Editor" in text and "Add to chart" in text
    assert "no amounts" in text.lower()
