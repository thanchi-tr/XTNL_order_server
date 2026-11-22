"""Home-server review round 6 fix (doc label: review round 8): a pasted backup secret is a leak.

Set-up 4 used to say "rotate that secret when convenient" after a paste on the xtnl desktop. It now sends the
operator to Rotation, "If a backup secret leaked": rotate at once and write a rotation note whose earliest leak
time is the paste. Restore step 2 "A leaked check C URL" and the leak procedure's triggers both list the paste.

Text checks only; nothing here runs rclone, B2, Healthchecks.io or the engine.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESTORE = ROOT / "docs" / "runbooks" / "home-backup-restore.md"


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _section(start: str, end: str) -> str:
    text = RESTORE.read_text(encoding="utf-8")
    i = text.index(start)
    return _flat(text[i:text.index(end, i + len(start))])


def test_setup4_paste_is_rotated_at_once_with_a_note() -> None:
    part = _section("If you pasted one anyway:", "In the PowerShell running as `xtnl-backup`")
    assert "when convenient" not in part
    assert "A pasted secret has leaked" in part
    assert "**at once**" in part
    assert "rotation note" in part
    assert '"If a backup secret leaked"' in part
    assert "earliest leak time is when you pasted it" in part
    assert "not trusted for a restore" in part


def test_restore_step2_leak_list_names_the_paste() -> None:
    part = _section("**A leaked check C URL**", "its runs from the leak until")
    assert "pasted into a window on the `xtnl` desktop" in part
    assert "runas window" in part
    assert "**the PC was stolen**" in part


def test_leak_procedure_triggers_include_the_paste() -> None:
    part = _section("* **If a backup secret leaked**", "2. Rotate every secret among them")
    assert "pasted into a window on the `xtnl` desktop" in part
    assert "do it **at once**" in part
    # Step 1 (delete the names) has nothing to delete after a paste; the operator goes on to step 2.
    assert "After a paste there is nothing to delete" in part
    assert "Go to step 2." in part


def test_operations_rule_treats_a_paste_as_a_leak() -> None:
    text = _flat((ROOT / ".claude" / "rules" / "operations.md").read_text(encoding="utf-8"))
    assert "pasted into a window on the `xtnl` desktop" in text
    assert "rotate it at once with a rotation note whose window starts at the paste" in text


def test_no_when_convenient_rotation_left_in_restore_runbook() -> None:
    text = _flat(RESTORE.read_text(encoding="utf-8"))
    assert "rotate that secret when convenient" not in text
