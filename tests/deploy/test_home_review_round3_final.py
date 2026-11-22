"""Home-server review round 3 fixes (deploy/local/backup-pg.ps1, deploy/local/backup/entrypoint.sh, the backup,
restore and receiver runbooks, the home-server plan).

1. A UTF-8 console input code page made Windows PowerShell 5.1 write a BOM into the job's stdin, and the job
   refused the first secret line (exit 78). backup-pg.ps1 now starts the child with a BOM-free input encoding and
   puts the old one back; the job drops a leading BOM on the first line only.
2. Set-up 3 no longer presents the checkout lock-down as what keeps the B2 key and the broker credentials apart:
   docker-users membership reaches the engine's files with xtnl's rights through bind mounts (accepted deviation).
3. The theft rotation list names check A's ping URL (XTNL_HEARTBEAT_COPY_URL).
4. A replacement box is tagged tag:xtnl-home and the old one removed from the tailnet (Restore step 7, plan s4.10).
5. The receiver's token checks count refusals only from a time noted after the 401 probe.
6. An unreadable archive folder ends as exit 66 (LATE FAILURE), not 70; row 66 says what to do.

The job runs under a POSIX sh with stub tools (test_home_backup_round1.py `rig`). The one PowerShell test runs
backup-pg.ps1's own Invoke-Logged in a hidden console of its own, with the repo's python as the child; nothing runs
Docker, B2, icacls or Task Scheduler.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.deploy.test_home_backup import BACKUP_PS1, ENTRYPOINT, RUNBOOK, _bucket_files, _ps_code
from tests.deploy.test_home_backup_round1 import SECRETS, _stdin, rig  # noqa: F401  (fixture)

ROOT = Path(__file__).resolve().parents[2]
RECEIVER_HOME = ROOT / "docs" / "runbooks" / "deadman-receiver-home.md"
PLAN = ROOT / "docs" / "design" / "home-server-plan.md"
POWERSHELL = shutil.which("powershell.exe") or shutil.which("powershell")
BOM = "\ufeff"


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _section(text: str, start: str, end: str) -> str:
    i = text.index(start)
    return text[i:text.index(end, i + len(start))]


def _row(text: str, code: str) -> str:
    m = re.search(rf"^\| {code} \|.*$", text, re.M)
    assert m, f"exit row {code} missing"
    return m.group(0)


# ---------------------------------------------------------------------------------------------- 1. BOM on stdin
def test_job_drops_a_bom_before_the_first_secret_line(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    r = run(BOM + _stdin())
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    for value in SECRETS.values():
        assert value not in out
    assert any(p.name.endswith(".dump.age") for p in _bucket_files(tmp))


def test_job_drops_a_bom_before_crlf_lines_too(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    _, run, _ = rig
    r = run(BOM + _stdin().replace("\n", "\r\n"))
    assert r.returncode == 0, r.stdout + r.stderr


def test_a_bom_on_a_later_line_is_still_refused(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    lines = _stdin().splitlines(keepends=True)
    r = run(lines[0] + BOM + "".join(lines[1:]))
    assert r.returncode == 78, r.stdout + r.stderr
    assert "unexpected line on stdin" in r.stdout
    assert _bucket_files(tmp) == []


def test_backup_ps1_sets_a_bom_free_input_encoding_around_the_start_only() -> None:
    code = "\n".join(_ps_code(BACKUP_PS1))
    body = code[code.index("function Invoke-Logged"):code.index("function Find-Docker")]
    set_at = body.index("[Console]::InputEncoding = $Utf8NoBom")
    start_at = body.index("[System.Diagnostics.Process]::Start($psi)")
    restore_at = body.index("[Console]::InputEncoding = $prevInEnc")
    write_at = body.index("$proc.StandardInput.BaseStream.Write(")
    assert set_at < start_at < restore_at < write_at
    # only when stdin is redirected, and a host without a console does not stop the job
    guard = body[:set_at]
    assert "if ($null -ne $StdinText)" in guard[guard.rindex("$inEncSet = $false"):]
    assert "try {" in guard[guard.rindex("if ($null -ne $StdinText)"):]
    assert re.search(r"\} finally \{\s*if \(\$inEncSet\)", body)


def _invoke_logged_source() -> str:
    text = BACKUP_PS1.read_text(encoding="utf-8")
    return text[text.index("function Format-Arg"):text.index("function Find-Docker")]


@pytest.mark.skipif(POWERSHELL is None or os.name != "nt", reason="Windows PowerShell not available")
def test_invoke_logged_sends_no_bom_under_a_utf8_console(tmp_path: Path) -> None:
    script = tmp_path / "harness.ps1"
    child = "import sys; print('HEX=' + sys.stdin.buffer.read().hex())"
    script.write_text(
        "$ErrorActionPreference = 'Stop'\n"
        "$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)\n"
        f"$JobRoot = '{tmp_path}'\n"
        "$script:WatchDumpSafe = $false\n"
        "$script:Lines = New-Object System.Collections.Generic.List[string]\n"
        "function Write-Log([string]$m) { $script:Lines.Add('LOG ' + $m) }\n"
        "function Write-RawLog([string]$m) { $script:Lines.Add($m) }\n"
        "function Send-HcPing([string]$s) { }\n"
        + _invoke_logged_source() + "\n"
        "try { [Console]::InputEncoding = New-Object System.Text.UTF8Encoding($true) } catch { 'NOCONSOLE'; exit 0 }\n"
        "if ([Console]::InputEncoding.GetPreamble().Length -ne 3) { 'NOCONSOLE'; exit 0 }\n"
        f"$rc = Invoke-Logged '{sys.executable}' @('-c', \"{child}\") '[child] ' 60 \"A=1`nEND`n\"\n"
        "foreach ($l in $script:Lines) { $l }\n"
        "'RC=' + $rc\n"
        "'PREAMBLE=' + [Console]::InputEncoding.GetPreamble().Length\n",
        encoding="utf-8-sig",
    )
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)    # a hidden console of its own, never the caller's
    r = subprocess.run([POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File",
                        str(script)], capture_output=True, text=True, timeout=120, check=False,
                       stdin=subprocess.DEVNULL, creationflags=flags)
    out = r.stdout + r.stderr
    if "NOCONSOLE" in r.stdout:
        pytest.skip("no console to set the input encoding on")
    assert r.returncode == 0, out
    assert "RC=0" in out, out
    assert "[child] HEX=" + b"A=1\nEND\n".hex() in out, out       # no ef bb bf in front
    assert "efbbbf" not in out
    assert "PREAMBLE=3" in out, out                               # the caller's encoding is put back


def test_runbook_tells_the_bom_refusal_apart_from_missing_stdin() -> None:
    flat = _flat(RUNBOOK.read_text(encoding="utf-8"))
    hint = _section(flat, 'If `-Check` ends with exit 78 "the secrets on stdin did not end with END',
                    "The first run builds the image")
    assert '"unexpected line on stdin" is a third cause' in hint
    assert "byte order mark" in hint and "chcp 65001" in hint


# ---------------------------------------------------------------------------------------------- 2. docker-users
def test_setup3_records_that_docker_users_reach_the_checkout_as_xtnl() -> None:
    flat = _flat(RUNBOOK.read_text(encoding="utf-8"))
    setup3 = _section(flat, "### 3. The backup account (admin)", "### 4.")
    deviation = _section(setup3, "**Accepted deviation (the key and Docker).**", "**Folder rights.**")
    assert "(d) the **other direction: the backup account reaches the engine's files**" in deviation
    assert "docker run -v C:/xtnl/order-server:/r" in deviation
    assert "the two accounts are **not** separated" in deviation
    checkout = _section(setup3, "**The engine's checkout**", "**The archive folder**")
    assert "does **not** keep the B2 key and the broker credentials apart" in checkout
    assert "risk (d)" in checkout
    # the old claim that the lock-down is what stops xtnl-backup meeting the broker credentials is gone
    assert "the B2 key and the broker credentials would meet" not in setup3


def test_plan_32_names_the_docker_gap_as_an_accepted_deviation() -> None:
    text = PLAN.read_text(encoding="utf-8")
    line = _section(text, "- **Run it under a separate Windows account**", "\n")
    assert "does **not** fully separate the two accounts" in line
    assert "`xtnl-backup` can change or read the engine's checkout" in line
    assert "drops `xtnl-backup` from `docker-users`" in line


# ---------------------------------------------------------------------------------------------- 3. theft list
def test_theft_rotation_names_check_a_url() -> None:
    flat = _flat(RUNBOOK.read_text(encoding="utf-8"))
    theft = _section(flat, "* **Theft of the PC**", "## Rollback")
    assert "check A's ping URL" in theft and "XTNL_HEARTBEAT_COPY_URL" in theft
    assert "Remove the stolen node from the tailnet" in theft
    row = _section(PLAN.read_text(encoding="utf-8"), "| **Theft or fire** |", "\n")
    assert "XTNL_HEARTBEAT_COPY_URL" in row and "tag:xtnl-home" in row


# ---------------------------------------------------------------------------------------------- 4. tailnet tag
def test_restore_step7_tags_the_replacement_and_rechecks_the_refusals() -> None:
    flat = _flat(RUNBOOK.read_text(encoding="utf-8"))
    step7 = _section(flat, "7. **Start the engine**", "8. **Lost for good:**")
    assert "`tag:xtnl-home`" in step7
    assert "**Remove** the old box from the tailnet" in step7
    assert "deadman-receiver-home.md" in step7 and "step 3" in step7
    assert "ports 22 and 8443 closed" in step7


def test_plan_410_tags_every_engine_box() -> None:
    flat = _flat(PLAN.read_text(encoding="utf-8"))
    s410 = _section(flat, "### 4.10 Tailscale and remote access (admin)", "### 4.11")
    assert "`tag:xtnl-home`" in s410
    assert "a replacement or rebuilt one too" in s410
    assert "Turn off key expiry for this node." not in s410     # tags turn key expiry off


# ---------------------------------------------------------------------------------------------- 5. token checks
def test_token_checks_count_from_a_time_noted_after_the_probe() -> None:
    text = RECEIVER_HOME.read_text(encoding="utf-8")
    flat = _flat(text)
    checks = _section(flat, "From the home PC:", "## End-to-end test")
    assert "401 probe itself leaves one" in checks
    e2e = _section(text, "## End-to-end test", "2. **Silence page:**")
    assert "t=$(date '+%Y-%m-%d %H:%M:%S'); sleep 60" in e2e
    assert "--since \"$t\" | grep -c 'wrong bearer token'" in e2e
    assert "--since \"-5 min\" | grep -c 'wrong bearer token'" not in e2e
    rotation = _section(text, "## Token rotation", "4. VPS: Promote")
    assert "--since \"$t\" | grep -c 'wrong bearer token'" in rotation
    assert "--since \"-5 min\" | grep -c 'wrong bearer token'" not in rotation


# ---------------------------------------------------------------------------------------------- 6. exit 66 / 70
def test_unreadable_archive_folder_ends_as_late_failure_66(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, sh = rig
    real_find = shutil.which("find", path=str(Path(sh).parent))
    if real_find is None:
        pytest.skip("no POSIX find beside sh")
    stub = tmp / "bin" / "find"
    stub.write_text('#!/bin/sh\nif [ "$1" = "$XTNL_BACKUP_ARCHIVE_SRC" ]; then echo "find: Permission denied" >&2; '
                    'exit 1; fi\nexec "$REAL_FIND" "$@"\n', encoding="utf-8", newline="\n")
    stub.chmod(0o755)
    r = run(_stdin(), REAL_FIND=Path(real_find).as_posix())
    out = r.stdout + r.stderr
    assert r.returncode == 66, out
    assert "partition archive: cannot read" in out
    assert "LATE FAILURE (exit 70)" in out and "DUMP SAFE:" in out


def test_exit_rows_and_header_put_the_unreadable_folder_under_66() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    row70 = _row(text, "70")
    assert "the archive folder could not be read" not in row70
    assert "ends as 66, not 70" in row70
    row66 = _row(text, "66")
    assert "an archive folder that could not be read" in row66
    assert "**`partition archive: cannot read /archive`**" in row66
    assert "Running again will not help" in row66 and "file(s) uploaded" in row66
    header = ENTRYPOINT.read_text(encoding="utf-8")
    header = header[header.index("# Exit codes:"):header.index("set -eu")]
    flat = _flat(header.replace("#", " "))
    seventy = flat[flat.index("70 a dump failed"):flat.index("73 ")]
    assert "archive folder" not in seventy
    sixty_six = flat[flat.index("66 the dump"):flat.index("69 ")]
    assert "archive folder that could not be read" in sixty_six
