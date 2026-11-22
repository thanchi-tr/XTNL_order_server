"""Home-server review round 4 findings, docs side (fixed in round 5).

* Theft of the PC: the rotation list in home-backup-restore.md and plan §5.4 names the engine's alert channel
  credentials (Pushover, SMTP, SES and the ARM ones), which sat in the stolen box's Windows User environment, and
  deadman-receiver-home.md requires the VPS receiver to use its own Pushover token and relay login.
* The receiver writes its log lines to stderr with no syslog priority prefix, so journald stores them at priority
  info and `journalctl -p warning` hides every warning. No runbook check filters the receiver's (or the engine's)
  journal with `-p warning`; the checks grep for the level word instead.

Text checks only; nothing runs journalctl, systemd or Pushover.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESTORE = ROOT / "docs" / "runbooks" / "home-backup-restore.md"
PLAN = ROOT / "docs" / "design" / "home-server-plan.md"
RECEIVER = ROOT / "docs" / "runbooks" / "deadman-receiver-home.md"
ALERT = ROOT / "docs" / "runbooks" / "deadman-alert.md"
UNIT = ROOT / "deploy" / "systemd" / "xtnl-deadman-receiver.home.service"

ALERT_CREDENTIALS = ("XTNL_PUSH_TOKEN", "XTNL_PUSH_USER", "XTNL_SMTP_USERNAME", "XTNL_SMTP_PASSWORD",
                     "XTNL_SES_", "XTNL_ARM_PUSH_", "XTNL_ARM_SMTP_", "XTNL_ARM_SES_")


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _theft_entry() -> str:
    text = RESTORE.read_text(encoding="utf-8")
    start = text.index("* **Theft of the PC**")
    return _flat(text[start:text.index("## Rollback", start)])


def test_theft_rotation_names_the_engine_alert_channel_credentials() -> None:
    entry = _theft_entry()
    for name in ALERT_CREDENTIALS:
        assert name in entry, name
    assert "same Windows User environment" in entry
    assert "revoke the old ones" in entry


def test_theft_rotation_covers_a_receiver_that_shared_the_engine_credentials() -> None:
    entry = _theft_entry()
    assert "XTNL_RECEIVER_PUSH_" in entry and "XTNL_RECEIVER_SMTP_" in entry


def test_plan_rotate_at_once_names_the_alert_channel_credentials() -> None:
    row = next(line for line in PLAN.read_text(encoding="utf-8").splitlines() if line.startswith("| **Theft or fire**"))
    rotate = row[row.index("Rotate at once"):]
    for name in ALERT_CREDENTIALS:
        assert name in rotate, name
    assert "own Pushover token and relay login" in rotate


def test_receiver_prerequisites_require_its_own_paging_credentials() -> None:
    text = RECEIVER.read_text(encoding="utf-8")
    prereq = _flat(text[text.index("## Prerequisites"):text.index("## Install")])
    assert "must be **its own**, never the engine's" in prereq
    assert "XTNL_PUSH_TOKEN" in prereq and "XTNL_SMTP_PASSWORD" in prereq
    assert "home-backup-restore.md" in prereq


def test_receiver_unit_sets_no_syslog_level_so_stderr_lands_at_info() -> None:
    # The doc fix relies on this: without a SyslogLevelPrefix/StandardError override journald uses priority info.
    unit = UNIT.read_text(encoding="utf-8")
    for key in ("SyslogLevel=", "SyslogLevelPrefix=", "StandardError="):
        assert key not in unit, key


def test_no_runbook_filters_the_receiver_or_engine_journal_by_priority() -> None:
    for doc in (RECEIVER, ALERT):
        for line in doc.read_text(encoding="utf-8").splitlines():
            if line.lstrip().removeprefix("sudo ").startswith("journalctl"):
                assert not re.search(r"\s-p\s+\w+", line), f"{doc.name}: {line}"


def test_receiver_checks_grep_for_the_level_word() -> None:
    level_grep = "| grep -E 'WARNING|ERROR|CRITICAL'"
    home = RECEIVER.read_text(encoding="utf-8")
    line = next(x for x in home.splitlines()
                if x.removeprefix("sudo ").startswith('journalctl -u xtnl-deadman-receiver --since "-30 min"'))
    assert level_grep in line and "--no-pager" in line
    assert "'rejected:'" in line and "'wrong bearer token'" in line and "'page via'" in line
    assert "Do not filter the receiver's journal with `-p warning`" in home
    assert "priority info" in _flat(home)

    alert = ALERT.read_text(encoding="utf-8")
    for unit in ("xtnl-deadman-receiver", "xtnl-engine"):
        line = next(x for x in alert.splitlines() if x.removeprefix("sudo ").startswith(f"journalctl -u {unit} "))
        assert level_grep in line and "--no-pager" in line, line
    assert "priority info" in _flat(alert)
