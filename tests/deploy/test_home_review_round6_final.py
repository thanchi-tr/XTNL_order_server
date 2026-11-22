"""Home-server final review round 6 (doc label: review round 7), deploy and docs side.

1. After a theft or an upload-key leak, the thief may hide the genuine pre-theft backups or upload new versions
   over their names. The restore runbook now reads them at their locked older version (rclone --b2-versions /
   --b2-version-at, or the B2 console's file versions view) in step 2 and step 6, and the leak and theft
   paragraphs no longer reject a pre-leak folder for looking hidden or carrying a newer upload time.
2. The start-xtnl.ps1 backup-name warning, single-pc-autostart.md and operations.md distrust only the check C
   runs inside the leak window (from the earliest leak time until the rotation), as the restore runbook does.
   Names that are all non-secret get their own warning: delete them, rotate nothing, distrust nothing.

Text checks, plus one optional PowerShell run of the name classification (skipped without powershell.exe);
nothing here runs rclone, B2, Healthchecks.io or the engine.
"""
from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
RESTORE = ROOT / "docs" / "runbooks" / "home-backup-restore.md"
SINGLE_PC = ROOT / "docs" / "runbooks" / "single-pc-autostart.md"
START = ROOT / "deploy" / "local" / "start-xtnl.ps1"
OPERATIONS = ROOT / ".claude" / "rules" / "operations.md"

NON_SECRET = ("XTNL_BACKUP_B2_BUCKET", "XTNL_BACKUP_AGE_RECIPIENT", "XTNL_BACKUP_EXCLUDE_MARKS",
              "XTNL_BACKUP_ARCHIVE_HOST")


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _section(text: str, start: str, end: str) -> str:
    i = text.index(start)
    return _flat(text[i:text.index(end, i + len(start))])


def _restore() -> str:
    return RESTORE.read_text(encoding="utf-8")


# --- finding 1: hidden or replaced backup versions ------------------------------------------------------------

def test_step2_has_hidden_or_replaced_files_procedure() -> None:
    step2 = _section(_restore(), "**Hidden or replaced files**", "**A leaked check C URL**")
    assert "hide a file or upload a new version over its name" in step2
    assert "367-day lock" in step2
    assert "rclone lsf --b2-versions r:<bucket>/daily/<ts>/" in step2
    assert "rclone lsf --b2-version-at <time> r:<bucket>/daily/" in step2
    assert "rclone copy --b2-version-at <time> r:<bucket>/daily/$TS/ /var/tmp/enc/" in step2
    assert "file versions view" in step2
    assert "upload time falls inside a good run" in step2
    assert "sha256sum -c" in step2
    assert "**Unverified:**" in step2


def test_which_backup_judges_the_version_read() -> None:
    which = _section(_restore(), "**Which backup** (review round 2)", "**Hidden or replaced files**")
    assert "Judge the version you read, not only the newest one" in which


def test_leak_and_theft_read_pre_leak_versions_instead_of_rejecting() -> None:
    leak = _section(_restore(), "**A leaked check C URL**", "Keep this shell open for step 3")
    assert "is not rejected for that: read it at its pre-leak version" in leak
    assert "even among the hidden and older versions, stop" in leak
    assert "Restore the newest good folder from before the theft" in leak
    assert "read it at its pre-theft version with `--b2-version-at`" in leak
    assert "Do not reject it for looking hidden or for a newer upload time" in leak


def test_step6_archive_can_read_older_versions() -> None:
    text = _restore()
    part = _section(text, "**Hidden or replaced archive objects**", "ledger rows: $n")
    assert "--b2-version-at <time>" in part
    assert 'rclone lsf --b2-versions -R "$B/archive/"' in part
    assert "Leave `V` empty in a normal restore" in part
    assert 'rclone lsf $V -R --files-only "$B/archive/"' in part
    assert part.count("rclone copyto $V ") == 2
    assert 'rclone copyto "$B/archive/' not in text


def test_rotation_points_to_pre_leak_versions() -> None:
    text = _restore()
    leak = _section(text, "**If a backup secret leaked**", "**age key**")
    assert "Runs recorded before the window stay trusted" in leak
    assert '(Restore step 2, "Hidden or replaced files")' in leak
    theft = _section(text, "* **Theft of the PC**", "## Rollback")
    assert "read them at their pre-theft version" in theft
    assert "the locked versions are still in B2" in theft


# --- finding 2: only the leak window is distrusted; non-secret names distrust nothing --------------------------

def _line_with(fragment: str) -> str:
    lines = [ln for ln in START.read_text(encoding="utf-8").splitlines() if fragment in ln]
    assert len(lines) == 1, lines
    return lines[0]


def test_start_secret_warning_distrusts_only_the_leak_window() -> None:
    line = _line_with("warning: this account's environment holds")
    assert "before that rotation are not trusted" not in line
    assert "write the earliest time they may have leaked in the rotation note" in line
    assert "check C runs recorded from that time until the rotation are not trusted for a restore" in line
    assert "runs from before it stay trusted" in line


def test_start_non_secret_warning_rotates_nothing() -> None:
    line = _line_with("warning: non-secret backup names found in this account's environment")
    assert "Write-Log" in line
    assert "nothing needs rotating and no check C run is distrusted" in line
    assert "step 5" in line
    assert line.rstrip().endswith("-f ($inheritedBackup -join ', '))")


def test_start_non_secret_list_and_branching() -> None:
    src = START.read_text(encoding="utf-8")
    m = re.search(r"\$NonSecretBackupNames = @\((.*?)\)", src, re.S)
    assert m is not None
    names = tuple(re.findall(r"'([A-Z0-9_]+)'", m.group(1)))
    assert names == NON_SECRET
    assert "$secretBackup = @($inheritedBackup | Where-Object { $NonSecretBackupNames -notcontains $_ })" in src
    i_secret = src.index("if ($secretBackup.Count -gt 0) {")
    i_other = src.index("} elseif ($inheritedBackup.Count -gt 0) {")
    assert i_secret < src.index("warning: this account's environment holds") < i_other
    assert i_other < src.index("warning: non-secret backup names found")


@pytest.mark.skipif(shutil.which("powershell.exe") is None, reason="needs Windows PowerShell")
@pytest.mark.parametrize(("names", "expected"), [
    (["XTNL_BACKUP_B2_BUCKET"], ""),
    (["xtnl_backup_age_recipient", "XTNL_BACKUP_ARCHIVE_HOST"], ""),
    (["XTNL_BACKUP_B2_BUCKET", "XTNL_BACKUP_HC_URL"], "XTNL_BACKUP_HC_URL"),
    (["RCLONE_CONFIG_R_KEY"], "RCLONE_CONFIG_R_KEY"),
    (["XTNL_BACKUP_SOMETHING_NEW"], "XTNL_BACKUP_SOMETHING_NEW"),
])
def test_start_classification_runs(names: list[str], expected: str) -> None:
    src = START.read_text(encoding="utf-8")
    decl = re.search(r"\$NonSecretBackupNames = @\(.*?\)", src, re.S)
    assert decl is not None
    split = re.search(r"\$secretBackup = @\(.*?\}\)", src)
    assert split is not None
    listed = ", ".join(f"'{n}'" for n in names)
    script = (f"{decl.group(0)}\n$inheritedBackup = @({listed})\n{split.group(0)}\n"
              "Write-Output ($secretBackup -join ',')")
    out = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
                         capture_output=True, text=True, timeout=60, check=True)
    assert out.stdout.strip() == expected


def test_autostart_runbook_matches_restore_window() -> None:
    part = _section(SINGLE_PC.read_text(encoding="utf-8"), "warning: this account's environment holds <names>",
                    "Each machine needs its own")
    assert "Check C runs recorded before that rotation are not trusted" not in part
    assert "Write the earliest time they may have leaked in the rotation note" in part
    assert "Check C runs recorded from that time until the rotation are not trusted for a restore" in part
    assert "Runs recorded before that time stay trusted" in part
    assert "warning: non-secret backup names found in this account's environment" in part
    assert "nothing needs rotating and no check C run is distrusted" in part
    for name in NON_SECRET:
        assert name in part


def test_restore_rotation_has_non_secret_step() -> None:
    leak = _section(_restore(), "**If a backup secret leaked**", "**age key**")
    assert "warning: non-secret backup names found in this account's environment" in leak
    assert "nothing needs rotating, no rotation note is needed and no check C run is distrusted" in leak
    for name in NON_SECRET:
        assert name in leak


def test_operations_rule_matches_restore_window() -> None:
    text = _flat(OPERATIONS.read_text(encoding="utf-8"))
    assert "check C runs recorded before the rotation are never trusted" not in text
    assert "runs from before that time stay trusted" in text
    assert "non-secret backup names" in text
    assert "--b2-version-at" in text
