"""Home-server review round 2: the backup wrapper, installer, comments and runbooks that go with the fixes.

* backup-pg.ps1 pings /fail itself for a job exit 78 (since home-server review round 2 it sends every check C
  ping; the job never pings).
* install-backup-task.ps1 refuses sources other accounts can change, uncommitted job files, and asks before copying.
* The compose file and the Dockerfile never describe the backup secrets as `-e` or environment variables.
* The runbook: a lock as long as the monthly retention, what the upload key can still do, the backup picked from
  independent records, secrets typed (never pasted) on the engine account's desktop, the admin-only clone.
* The dead-man placeholder's comment and the stop-gap rollback describe the W1 page.
"""
from __future__ import annotations

import re
from pathlib import Path

from tests.deploy.test_deploy_assets import _assignments
from tests.deploy.test_home_backup import BACKUP_PS1, RUNBOOK, TASK_PS1, _ps_code

ROOT = Path(__file__).resolve().parents[2]
LOCAL = ROOT / "deploy" / "local"
COMPOSE = LOCAL / "docker-compose.local.yml"
DOCKERFILE = LOCAL / "backup" / "Dockerfile"
LIVE_ENV = ROOT / "deploy" / "env" / "local-live.env.example"
RECEIVER_HOME = ROOT / "docs" / "runbooks" / "deadman-receiver-home.md"


def _flat(text: str) -> str:
    """Whitespace collapsed and comment markers at line starts removed, so a sentence wrapped over comment lines
    reads as one."""
    return re.sub(r"\s+", " ", re.sub(r"(?m)^\s*# ?", "", text))


def _code(path: Path) -> str:
    return "\n".join(_ps_code(path))


# ---------------------------------------------------------------------------------------------- finding 4
def test_wrapper_pings_fail_for_a_job_configuration_refusal() -> None:
    code = _code(BACKUP_PS1)
    codes = re.search(r"\$GoodJobCodes = @\(([^)]*)\)", code)
    assert codes is not None and "78" not in [c.strip() for c in codes.group(1).split(",")]
    branch = code[code.index("if ($code -eq 78)"):]
    assert branch.split("}", 1)[0].rstrip().endswith("-Ping")


# ---------------------------------------------------------------------------------------------- finding 8
def test_installer_checks_its_sources_before_copying() -> None:
    code = _code(TASK_PS1)
    i_offenders = code.index("$offenders = @(Get-SourceOffenders $sourcePaths ($TrustedSids + @($me)))")
    i_copy = code.index("Copy-Item -LiteralPath $src -Destination $dst -Force")
    assert i_offenders < i_copy
    assert "$sourcePaths = @($PSCommandPath, $PSScriptRoot," in code
    assert re.search(r"if \(\$offenders\.Count -gt 0\) \{\s*throw", code)
    assert "status', '--porcelain'" in code and "refused: uncommitted changes to job files" in code
    assert "'verify-commit', 'HEAD'" in code and "commit (HEAD)" in code
    assert code.index("Read-Host 'install these job files") < i_copy
    assert "if ($answer -ne 'yes') { throw" in code and "[switch]$Yes" in code
    assert "-RepoRoot \"{1}\"' -f $BackupScript, $RepoRoot" in code
    # the same write-rights mask as the wrapper's job-file check
    assert "$WriteRightsMask = [int64]0x500D0156" in code and "$WriteRightsMask = [int64]0x500D0156" in _code(BACKUP_PS1)


# ---------------------------------------------------------------------------------------------- finding 10
def test_comments_never_describe_the_backup_secrets_as_environment_variables() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")
    start = compose.index("# Nightly encrypted off-site backup of PostgreSQL")
    block = _flat(compose[start:compose.index("  backup:", start)])
    assert "B2 key, the bucket, the age PUBLIC key and the Healthchecks.io URL are passed by name" not in block
    assert "stdin" in block and "never pass them as -e" in block
    docker = _flat(DOCKERFILE.read_text(encoding="utf-8"))
    assert "the B2 key, the bucket and the age PUBLIC key arrive as environment variables" not in docker
    assert "stdin" in docker and "never pass them as -e or environment variables" in docker
    for text in (block, docker):
        for secret in ("B2 key", "check C URL"):
            for m in re.finditer(re.escape(secret), text):
                window = text[max(0, m.start() - 120):m.end() + 120]
                assert "stdin" in window, window


# ---------------------------------------------------------------------------------------------- finding 7, 9, 11
def test_runbook_lock_covers_the_monthly_retention() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    flat = _flat(text)
    assert "--default-retention-period 367days" in text and "30days" not in text
    assert '"daysFromHidingToDeleting": 337' in text and '"daysFromHidingToDeleting": 2' in text
    days_lock = 367
    assert 31 + 337 > days_lock and 366 + 2 > days_lock      # normal deletes only after the lock ends
    assert "can add objects but never delete" not in flat
    assert "hide existing files or upload over their names" in flat
    assert "a retention date 367 days ahead" in flat


def test_runbook_restore_picks_the_backup_from_independent_records() -> None:
    flat = _flat(RUNBOOK.read_text(encoding="utf-8"))
    assert "pick the newest timestamp" not in flat
    for needle in ("**Which backup**", "Healthchecks.io check C", "backup-pg exit 0", "\"Uploaded\" date",
                   "--format tp", "possible key compromise", "forged backup"):
        assert needle in flat, needle


def test_runbook_secrets_are_typed_not_pasted_on_the_engine_desktop() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    sec = _flat(text[text.index("### 4. The backup account's User variables"):text.index("### 5. Healthchecks.io")])
    for needle in ("Switch user", "TYPE the secrets", "Never paste", "Set-Clipboard -Value ' '", "Clipboard history",
                   "Sync across your devices", "does not protect the clipboard"):
        assert needle in sec, needle
    assert "never reach the screen, the history or a file:" not in sec


def test_runbook_installer_runs_from_an_admin_only_clone() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    sec = _flat(text[text.index("### 7. Job folder and scheduled task"):text.index("### 8. First run by hand")])
    for needle in ("Never run the installer elevated from the engine's checkout", "C:\\xtnl-admin",
                   "/inheritance:r", "git clone", "log -1", "-RepoRoot C:\\xtnl\\order-server", "type `yes`"):
        assert needle in sec, needle
    assert "From an elevated PowerShell in the repository" not in sec


# ---------------------------------------------------------------------------------------------- finding 5
def test_placeholder_comment_and_stop_gap_describe_the_w1_page() -> None:
    text, assigned = _assignments(LIVE_ENV)
    section = _flat(text[text.index("# Dead-man receiver (operations.md s9)"):text.index("XTNL_DEADMAN_URL=")])
    assert "nothing else is affected" not in section
    for needle in ("PAGE[high]", "dead-man receiver not acknowledging", "XTNL_DEADMAN_TOKEN is empty",
                   "empty", "xtnl_deadman_configured 0"):
        assert needle in section, needle
    assert assigned["XTNL_DEADMAN_URL"].startswith("http://127.0.0.1:")
    home = _flat(RECEIVER_HOME.read_text(encoding="utf-8"))
    rollback = home[home.index("**Back to the stop-gap (P1 only):**"):home.index("**Rebuild:**")]
    assert "PAGE[high]" in rollback and "XTNL_DEADMAN_ALERT_AFTER_S" in rollback
    assert "xtnl_deadman_configured" in rollback
