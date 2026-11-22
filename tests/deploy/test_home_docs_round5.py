"""Home-server review round 5: start-xtnl.ps1 and runbook text.

* A backup secret found in the engine account's environment has leaked: the start-up warning and the runbooks say
  to rotate it at once (the check C ping URL, the B2 upload key), not only to delete it, and that check C runs
  recorded before the rotation are never trusted for a restore. Restore step 2 no longer says that rotating at
  restore time makes check C trustworthy again.
* A bad XTNL_DEADMAN_ALERT_AFTER_S is named on the engine's unprefixed `config: ...` line; no runbook points at an
  `[engine]` line that nothing prints.

Text checks only: nothing here runs PowerShell, Docker or the engine.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.runtime.deadman import alert_after_s_from_env

ROOT = Path(__file__).resolve().parents[2]
START = ROOT / "deploy" / "local" / "start-xtnl.ps1"
AUTOSTART = ROOT / "docs" / "runbooks" / "single-pc-autostart.md"
BACKUP = ROOT / "docs" / "runbooks" / "home-backup-restore.md"
DEADMAN = ROOT / "docs" / "runbooks" / "deadman-alert.md"
MAIN = ROOT / "src" / "main.py"


def _flat(path: Path) -> str:
    return re.sub(r"\s+", " ", path.read_text(encoding="utf-8"))


def _section(text: str, start: str, end: str) -> str:
    i = text.index(start)
    return text[i:text.index(end, i + len(start))]


def _warning_line() -> str:
    lines = [ln for ln in START.read_text(encoding="utf-8").splitlines()
             if "warning: this account's environment holds" in ln]
    assert len(lines) == 1, lines
    return lines[0]


# --- finding 1: a leaked backup secret is rotated at once ---------------------------------------------------------

def test_start_warning_says_rotate_not_only_remove() -> None:
    line = _warning_line()
    assert "Write-Log" in line
    assert "rotate every secret among them now" in line
    assert "check C ping URL" in line and "XTNL_BACKUP_HC_URL" in line
    assert "B2 upload key" in line and "XTNL_BACKUP_B2_KEY" in line
    assert "If a backup secret leaked" in line
    assert "not trusted for a restore" in line
    # still names only, never a value: the format argument is the joined names
    assert line.rstrip().endswith("-f ($inheritedBackup -join ', '))")
    assert "(remove them here)" not in line


def test_autostart_runbook_says_rotate_at_once() -> None:
    text = _flat(AUTOSTART)
    assert "delete them from this account. It logs" not in text
    part = _section(text, "warning: this account's environment holds <names>", "Each machine needs its own")
    assert "rotate every secret among them at once" in part
    assert "XTNL_BACKUP_HC_URL" in part and "XTNL_BACKUP_B2_KEY" in part
    assert 'home-backup-restore.md "If a backup secret leaked"' in part
    assert "rotating later does not make them trusted" in part
    assert "never values" in part


def test_backup_runbook_setup4_points_to_leak_procedure() -> None:
    text = _flat(BACKUP)
    part = _section(text, "**Never** set any of them in the `xtnl` account", "How they reach the job")
    assert "warning: this account's environment holds <names>" in part
    assert 'rotate every secret among them at once (Rotation, "If a backup secret leaked")' in part
    assert "deleting alone is not enough" in part


def test_backup_rotation_has_leak_procedure() -> None:
    text = _flat(BACKUP)
    rotation = _section(text, "## Rotation", "**age key**")
    assert "Regenerating keeps the check and its history" in rotation
    leak = _section(text, "**If a backup secret leaked**", "**age key**")
    assert "**at once**, not at restore time" in leak
    assert "Rotate every secret among them" in leak
    assert "rotation note" in leak and "password manager" in leak
    assert "earliest time it may have leaked" in leak
    assert "never trusted for a restore" in leak
    assert "A leaked check C URL" in leak


def test_restore_step2_distrusts_runs_from_the_leak_window() -> None:
    text = _flat(BACKUP)
    assert "rotate it (Rotation) before you rely on check C" not in text
    step2 = _section(text, "**A leaked check C URL**", "Keep this shell open for step 3")
    assert "regenerating the URL keeps the check's history, fake runs included" in step2
    assert "Rotating now, at restore time, does not make them trusted" in step2
    assert "like a folder with no matching good run" in step2
    assert "runs recorded after the rotation" in step2
    assert "B2 upload times" in step2
    assert "do not restore a folder from inside the window" in step2
    # the step 2 trust argument this exception narrows is still there
    assert "Only `backup-pg.ps1` can ping it" in text


def test_operations_rule_records_rotation_on_warning() -> None:
    text = _flat(ROOT / ".claude" / "rules" / "operations.md")
    assert "the operator rotates them at once" in text
    # review round 7: only the leak window is distrusted, not every run before the rotation
    assert ("check C runs recorded from the earliest time they may have leaked until the rotation are never "
            "trusted for a restore") in text


# --- finding 2: the engine's config line, not an "[engine]" line --------------------------------------------------

@pytest.mark.parametrize("path", [DEADMAN, AUTOSTART])
def test_no_runbook_points_at_an_engine_prefix(path: Path) -> None:
    assert "[engine]" not in path.read_text(encoding="utf-8")


def test_engine_child_has_no_prefix_and_config_lines_are_plain() -> None:
    src = START.read_text(encoding="utf-8")
    assert re.search(r"Start-Pumped \$PsExe @\([^\n]*\$RunLocal[^\n]*\) '' ", src), "engine child prefix changed"
    assert "'[engine] '" not in src
    assert 'print(f"config: {p}", file=sys.stderr)' in MAIN.read_text(encoding="utf-8")
    assert "ALERT: configuration refused (exit 78; see the lines above)" in src


def _problem(raw: str) -> str:
    with pytest.raises(ValueError) as exc:
        alert_after_s_from_env({"XTNL_DEADMAN_ALERT_AFTER_S": raw})
    return str(exc.value)


@pytest.mark.parametrize("path", [DEADMAN, AUTOSTART])
def test_runbooks_quote_the_real_config_line(path: Path) -> None:
    text = _flat(path)
    out_of_range = "config: " + _problem("30")
    assert out_of_range == "config: XTNL_DEADMAN_ALERT_AFTER_S must be between 60 and 3600 s"
    if path == DEADMAN:
        assert out_of_range in text
        assert _problem("300s").removeprefix("XTNL_DEADMAN_ALERT_AFTER_S ") in text
    else:
        assert "config: XTNL_DEADMAN_ALERT_AFTER_S must be ..." in text
    assert "no prefix" in text
    assert "just before `ALERT: configuration refused (exit 78; see the lines above)`" in text
    assert "search the log for `XTNL_DEADMAN_ALERT_AFTER_S`" in text
