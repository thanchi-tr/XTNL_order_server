"""Home-server review round 6 findings, docs side.

1. Theft of the PC is a leak of the backup secrets (check C URL, B2 upload key): the theft item routes to "If a
   backup secret leaked", its window starts at the last time the PC was known to be in your hands, and Restore
   step 2 rejects every check C run and daily/ folder from then until the rotation.
2. Renaming XTNL_NODE_ID on the same box (pc1 -> home1, as start-xtnl.ps1 advises) raises one expected
   node_changed notice; the runbooks no longer send the operator hunting for a second machine.
3. The receiver runbook runs every journalctl with sudo, and says '-- No entries --' means no access.

Text checks only; nothing runs journalctl, systemd, Healthchecks.io or B2.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESTORE = ROOT / "docs" / "runbooks" / "home-backup-restore.md"
RECEIVER = ROOT / "docs" / "runbooks" / "deadman-receiver-home.md"
ALERT = ROOT / "docs" / "runbooks" / "deadman-alert.md"
SINGLE_PC = ROOT / "docs" / "runbooks" / "single-pc-autostart.md"
START = ROOT / "deploy" / "local" / "start-xtnl.ps1"
OPERATIONS = ROOT / ".claude" / "rules" / "operations.md"


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _section(text: str, start: str, end: str) -> str:
    i = text.index(start)
    return _flat(text[i:text.index(end, i + len(start))])


# 1. Theft is a leak of the backup secrets.

def test_theft_item_treats_the_theft_as_a_backup_secret_leak() -> None:
    entry = _section(RESTORE.read_text(encoding="utf-8"), "* **Theft of the PC**", "## Rollback")
    assert "Treat the theft as a leak" in entry
    assert "XTNL_BACKUP_HC_URL" in entry and "B2 upload key" in entry
    assert 'Follow "If a backup secret leaked"' in entry
    assert "last time the PC was known to be in your hands" in entry
    assert "Every check C run and every `daily/` folder from that time until the rotation is rejected" in entry
    assert '"A leaked check C URL"' in entry


def test_restore_step_2_lists_theft_as_a_leak_cause_and_rejects_runs_after_it() -> None:
    text = RESTORE.read_text(encoding="utf-8")
    leak = _section(text, "**A leaked check C URL**", "Keep this shell open for step 3")
    assert "**the PC was stolen**" in leak
    assert "**After a theft**" in leak
    assert "last time the PC was known to be in your hands" in leak
    assert "Reject every check C run and every `daily/` folder after that time and before the rotation" in leak
    assert "Restore the newest good folder from before the theft" in leak


def test_rotation_note_window_after_a_theft() -> None:
    text = RESTORE.read_text(encoding="utf-8")
    leaked = _section(text, "* **If a backup secret leaked**", "* **age key**")
    assert "after a theft, the last time the PC was known to be in your hands" in leaked


def test_operations_rule_counts_theft_as_a_backup_secret_leak() -> None:
    text = _flat(OPERATIONS.read_text(encoding="utf-8"))
    assert "A theft of the PC is such a leak too" in text
    assert "last time the PC was known to be in the operator's hands" in text


# 2. A node_changed after your own rename is expected.

def test_alert_runbook_calls_a_rename_on_the_same_box_expected() -> None:
    text = ALERT.read_text(encoding="utf-8")
    row = next(line for line in text.splitlines() if line.startswith("| `beating node changed` |"))
    assert "changed `XTNL_NODE_ID` on this box" in row and "`pc1 -> home1`" in row
    assert "one notice is expected; it clears after 5 min and needs no action" in row
    assert "Only a change you did not make means a second machine" in row
    table = next(line for line in text.splitlines() if line.startswith("| `beating node changed (<old> -> <new>)` |"))
    assert "once after you change `XTNL_NODE_ID` on the same box" in table
    assert "`beating node changed (pc1 -> home1)` notice: that one is yours" in _flat(text)


def test_receiver_end_to_end_step_6_expects_one_notice_after_a_rename() -> None:
    text = RECEIVER.read_text(encoding="utf-8")
    step = _section(text, "6. **Two engines (W19):**", "## Token rotation")
    assert "change `XTNL_NODE_ID` on the same box" in step and "`pc1 -> home1`" in step
    assert "`node_changed` notice is also expected" in step
    assert "clears after 5 min and needs no action" in step
    assert "Only a change you did not make points to a second machine" in step


def test_single_pc_node_id_paragraph_expects_one_notice_after_a_rename() -> None:
    text = _flat(SINGLE_PC.read_text(encoding="utf-8"))
    assert "one `beating node changed (pc1 -> home1)` notice" in text
    assert "That one is expected: it clears after 5 min and needs no action" in text
    assert "Only a change you did not make points to a second machine" in text


def test_start_script_warning_says_the_notice_is_expected() -> None:
    code = START.read_text(encoding="utf-8")
    line = next(x for x in code.splitlines() if "warning: XTNL_NODE_ID is the example's" in x)
    assert "one 'beating node changed' notice is expected and clears after 5 min" in line


# 3. sudo journalctl on the VPS.

def test_receiver_runbook_runs_every_journalctl_with_sudo() -> None:
    text = RECEIVER.read_text(encoding="utf-8")
    calls = re.findall(r"(\S*)\s*journalctl -u xtnl-deadman-", text)
    assert len(calls) >= 7
    for before in calls:
        assert before.endswith("sudo"), before
    for line in text.splitlines():
        if "journalctl -u" in line:
            assert "sudo journalctl -u" in line, line


def test_receiver_runbook_says_no_entries_means_no_access() -> None:
    text = _flat(RECEIVER.read_text(encoding="utf-8"))
    assert "Every `journalctl` in this runbook runs with `sudo`" in text
    assert "`-- No entries --`" in text and "**no access, not a clean journal**" in text
    assert "false 0" in text


def test_alert_runbook_runs_the_receiver_journal_with_sudo() -> None:
    text = ALERT.read_text(encoding="utf-8")
    for line in text.splitlines():
        if "journalctl -u xtnl-deadman-receiver" in line:
            assert "sudo journalctl -u xtnl-deadman-receiver" in line, line
    assert "which means no access, not a clean journal" in _flat(text)
