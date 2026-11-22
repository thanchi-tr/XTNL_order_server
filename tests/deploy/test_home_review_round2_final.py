"""Home-server review round 2 (final fixes): backup installer, check C, UPS re-install, receiver and backup runbooks.

1. install-backup-task.ps1 sets the ACL recursively (/T) inside the job folder only. The parent folder gets its
   owner and entries on the folder object alone, and a -JobDir whose parent is a drive root, a system or profile
   folder, the repository's folder, or a folder holding anything else is refused before anything is copied.
2. Only backup-pg.ps1 pings Healthchecks.io check C. The URL never reaches the job's container (not -e, not stdin),
   so someone who reaches the container gets the upload key but cannot record a fake good run. The wrapper sends
   /start before the job, success on the job's `DUMP SAFE:` line, and /fail otherwise. The job has no ping code,
   refuses a check C line on its stdin and drops the variable from its environment unread. Restore step 2 says so.
3. install-ups-watch.ps1 stops a running watcher before it replaces the task and starts the new definition again,
   so a dry-run copy never watches the real test. Runbook step 4 checks the `ups-watch started` mode line.
4. The receiver runbook checks for the receiver's real refusal line ('wrong bearer token'), not '401'.
5. Checks row 78 says which refusals ping /fail and which (before the URL is read) ping nothing.

The job runs under a POSIX sh with stub tools (tests/deploy/test_home_backup_round1.py `rig`); nothing runs Docker,
PowerShell, icacls, Task Scheduler, systemd or B2.
"""
from __future__ import annotations

import re
from pathlib import Path

from tests.deploy.test_home_backup import (
    BACKUP_PS1, DOCKERFILE, ENTRYPOINT, RUNBOOK, TASK_PS1, _bucket_files, _ps_code, _sh_code,
)
from tests.deploy.test_home_backup_round1 import SECRETS, _stdin, rig  # noqa: F401  (fixture)

ROOT = Path(__file__).resolve().parents[2]
UPS_INSTALL = ROOT / "deploy" / "local" / "install-ups-watch.ps1"
UPS_WATCH = ROOT / "deploy" / "local" / "ups-watch.ps1"
AUTOSTART = ROOT / "docs" / "runbooks" / "single-pc-autostart.md"
RECEIVER_HOME = ROOT / "docs" / "runbooks" / "deadman-receiver-home.md"
RECEIVER_SERVICE = ROOT / "src" / "deadman_receiver" / "service.py"
RECEIVER_TOKENS = ROOT / "src" / "deadman_receiver" / "tokens.py"
COMPOSE = ROOT / "deploy" / "local" / "docker-compose.local.yml"
HC = "https://hc.example/ping/check-c-uuid"


def _code(path: Path) -> str:
    return "\n".join(_ps_code(path))


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _flat_comments(text: str) -> str:
    """Whitespace collapsed and comment markers at line starts removed (a sentence wrapped over comment lines)."""
    return re.sub(r"\s+", " ", re.sub(r"(?m)^\s*# ?", "", text))


def _section(text: str, start: str, end: str) -> str:
    i = text.index(start)
    return text[i:text.index(end, i)]


# ---------------------------------------------------------------------------------------------- 1: parent ACL
def test_installer_recurses_only_inside_the_job_folder() -> None:
    code = _code(TASK_PS1)
    icacls = [ln.strip() for ln in code.splitlines() if "icacls.exe" in ln]
    assert icacls, "no icacls call"
    for ln in icacls:
        if "/T" in ln.split():
            assert ln.startswith("& icacls.exe $JobDir "), ln               # recursive only on the job folder
        if "$JobParent" in ln:
            assert "/T" not in ln.split(), ln
    assert any(ln.startswith("& icacls.exe $JobParent /setowner") for ln in icacls)
    assert any(ln.startswith("& icacls.exe $JobParent /inheritance:r /grant:r $parentGrants") for ln in icacls)
    # this-folder-only entries on the parent: no (OI)(CI), so nothing propagates below it
    grants = re.search(r"\$parentGrants = @\(([^)]*\)[^)]*)\)", code)
    assert grants is not None and "(OI)" not in grants.group(1) and "(CI)" not in grants.group(1)
    assert "foreach ($target in @($JobParent, $JobDir))" not in code


def test_installer_refuses_a_shared_parent_before_copying() -> None:
    code = _code(TASK_PS1)
    i_copy = code.index("Copy-Item -LiteralPath $src -Destination $dst -Force")
    i_prompt = code.index("Read-Host 'install these job files")
    checks = {
        "drive root": "sits directly under a drive root",
        "shared folder": "is a shared folder",
        "inside system / profiles": "is inside $f",
        "other children": "holds other items",
    }
    for what, needle in checks.items():
        i = code.index(needle)
        assert i < i_copy and i < i_prompt, what
    for folder in ("$env:ProgramFiles", "${env:ProgramFiles(x86)}", "$env:ProgramData", "$env:SystemRoot",
                   "$env:USERPROFILE", "$profilesDir", "(Split-Path -Parent $RepoRoot)"):
        assert folder in code, folder
    assert "Get-ChildItem -LiteralPath $JobParent -Force | Where-Object { $_.Name -ine $JobLeaf }" in code
    # the parent is computed once, before the checks (not again after the copy)
    assert code.count("$JobParent = Split-Path -Parent $JobDir") == 1
    assert code.index("$JobParent = Split-Path -Parent $JobDir") < code.index("sits directly under a drive root")


def test_runbook_keeps_the_default_job_dir_and_names_the_refusals() -> None:
    sec = _flat(_section(RUNBOOK.read_text(encoding="utf-8"), "### 7. Job folder and scheduled task",
                         "### 8. First run by hand"))
    for needle in ("recursive inside the job folder only", "keep the default `-JobDir`", "a drive root",
                   "`C:\\xtnl`", "anything besides the job folder"):
        assert needle in sec, needle


# ---------------------------------------------------------------------------------------------- 2: check C
def test_wrapper_never_hands_the_check_c_url_to_the_job() -> None:
    code = _code(BACKUP_PS1)
    secret = re.search(r"\$SecretVars = @\(([^)]*)\)", code)
    assert secret is not None
    assert set(re.findall(r"'([A-Z0-9_]+)'", secret.group(1))) == {"XTNL_BACKUP_B2_ACCOUNT", "XTNL_BACKUP_B2_KEY"}
    assert "$WrapperOnlyVars = @('XTNL_BACKUP_HC_URL')" in code
    # read into the script variable only, never into this process's environment or the stdin secrets
    read = code[code.index("foreach ($name in $BackupVars) {"):code.index("XTNL_BACKUP_SECRETS', 'stdin'")]
    assert "if ($WrapperOnlyVars -contains $name) { $script:HcUrl = $v.Trim() }" in read
    assert read.index("$WrapperOnlyVars -contains $name") < read.index("$SecretVars -contains $name")
    assert "$script:Secrets['XTNL_BACKUP_HC_URL']" not in code
    loop = code[code.index("$runArgs = $compose + @('run'"):code.index("$runArgs += @('backup', $mode)")]
    assert "if ($WrapperOnlyVars -contains $name) { continue }" in loop
    stdin = code[code.index("$stdin = ''"):code.index('$stdin += "END`n"')]
    assert "foreach ($name in $SecretVars)" in stdin and "HC_URL" not in stdin


def test_wrapper_sends_start_success_and_fail_itself() -> None:
    code = _code(BACKUP_PS1)
    i_start = code.index("Send-HcPing '/start'")
    i_job = code.index("$code = Invoke-Logged $docker $runArgs '[job] ' ($TimeoutMinutes * 60) $stdin")
    i_build = code.index("'build', '--quiet', 'backup'")
    assert i_build < i_start < i_job                         # after the build, right before the job
    assert code.index("$script:WatchDumpSafe = (-not $Check)") < i_job
    # success once, on the DUMP SAFE line, only while the job runs
    safe = code[code.index("if ($script:WatchDumpSafe -and -not $script:DumpSafe"):]
    safe = safe[:safe.index("}") + 1]
    assert "$script:DumpSafe = $true" in safe and "Send-HcPing ''" in safe
    # a good exit code without DUMP SAFE is a failed night (exit 70, /fail)
    i_guard = code.index("if (($GoodJobCodes -contains $code) -and -not $Check -and -not $script:DumpSafe) {")
    guard = code[i_guard:code.index("\n}", i_guard)]
    assert "Exit-Backup 70" in guard and guard.rstrip().endswith("-Ping")
    assert i_guard < code.index("if ($code -eq 0) {")
    # -Check never pings
    ping = code[code.index("function Send-HcPing"):code.index("function Send-HcFail")]
    assert "if (-not $script:HcUrl -or $Check) { return }" in ping
    assert "$script:HcUrl" in ping and "Write-Log" in ping
    for line in ping.splitlines():
        if "Write-Log" in line:
            assert "HcUrl" not in line, line                  # the URL is never logged


def test_entrypoint_has_no_ping_code_and_no_check_c_url() -> None:
    code = "\n".join(_sh_code(ENTRYPOINT))
    assert "hc_ping" not in code and "curl" not in code
    assert 'SECRET_NAMES="XTNL_BACKUP_B2_ACCOUNT XTNL_BACKUP_B2_KEY"' in code
    assert "XTNL_BACKUP_HC_URL=*)" not in code                    # no stdin line for it: refused as unexpected
    assert "unset XTNL_BACKUP_HC_URL" in code
    docker = DOCKERFILE.read_text(encoding="utf-8")
    install = next(ln for ln in docker.splitlines() if "apt-get install" in ln)
    assert "curl" not in install


def test_job_refuses_a_check_c_line_on_stdin(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    r = run(_stdin({**SECRETS, "XTNL_BACKUP_HC_URL": HC}))
    out = r.stdout + r.stderr
    assert r.returncode == 78, out
    assert "unexpected line on stdin" in out and "check-c-uuid" not in out
    assert _bucket_files(tmp) == [] and not (tmp / "curl.log").exists()


def test_job_drops_a_check_c_url_from_its_environment(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    # the rclone stub exits 8 if XTNL_BACKUP_HC_URL reaches it; the curl stub would record any ping
    r = run(_stdin(), XTNL_BACKUP_HC_URL=HC)
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert "XTNL_BACKUP_HC_URL in the environment ignored" in out and "check-c-uuid" not in out
    assert "DUMP SAFE: dump and globals uploaded to daily/" in out
    assert not (tmp / "curl.log").exists()


def test_the_dump_safe_line_the_wrapper_watches_is_the_one_the_job_logs() -> None:
    code = _code(BACKUP_PS1)
    pattern = re.search(r"\$line -match '([^']+)'", code)
    assert pattern is not None
    job_line = 'log "DUMP SAFE: dump and globals uploaded to daily/$TS/"'
    assert job_line in ENTRYPOINT.read_text(encoding="utf-8")
    sample = "2026-11-22T03:16:00Z xtnl-backup: DUMP SAFE: dump and globals uploaded to daily/20261122T031600Z/"
    assert re.search(pattern.group(1), sample)


def test_compose_and_dockerfile_say_the_url_never_reaches_the_job() -> None:
    compose = _flat_comments(COMPOSE.read_text(encoding="utf-8"))
    docker = _flat_comments(DOCKERFILE.read_text(encoding="utf-8"))
    assert "The Healthchecks.io check C URL never reaches this service, not even on stdin" in compose
    assert "The check C URL never reaches this image, not even on stdin" in docker


def test_runbook_restore_trusts_check_c_because_only_the_wrapper_pings_it() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    which = _flat(_section(text, "**Which backup**", "Keep this shell open for step 3"))
    for needle in ("Only `backup-pg.ps1` can ping it", "never hands the URL to the job's container",
                   "cannot record a run of their own", "one per night", "warning sign",
                   "`backup-pg.ps1` sends it when the job logs `DUMP SAFE:`",
                   # review round 5: rotating at restore time no longer counts as a remedy for a leaked URL
                   "A leaked check C URL", "does not make them trusted"):
        assert needle in which, needle
    setup5 = _flat(_section(text, "### 5. Healthchecks.io check C", "### 6. Partition archive folder"))
    for needle in ("Only `backup-pg.ps1` pings it, never the job", "fake good run", "`<url>/start` right before",
                   "when the job logs `DUMP SAFE:`", "`backup-pg exit 70`"):
        assert needle in setup5, needle
    deviation = _flat(_section(text, "* **Accepted deviation (the key and Docker).**", "* **Folder rights.**"))
    assert "the check C URL never enters the container" in deviation
    assert "curl gets the check C URL" not in _flat(text)
    assert "The job pings `<url>/start`" not in _flat(text)


# ---------------------------------------------------------------------------------------------- 3: UPS re-install
def test_ups_installer_stops_a_running_watcher_before_replacing_it() -> None:
    code = _code(UPS_INSTALL)
    uninstall = code[code.index("if ($Uninstall) {"):code.index("exit 0")]
    rest = code[code.index("exit 0"):]
    i_stop = rest.index("Stop-ScheduledTask -TaskName $TaskName")
    i_register = rest.index("Register-ScheduledTask -TaskName $TaskName -Force")
    assert i_stop < i_register
    assert "$existing.State -eq 'Running'" in rest[:i_stop]
    assert "$wasRunning = $true" in rest[:i_stop]
    # it waits for the stop and refuses to go on with a copy still running
    assert "throw \"the running '$TaskName' did not stop within 15 s" in rest[i_stop:i_register]
    # the new definition starts at once when one was running (or with -StartNow)
    assert "if ($StartNow -or $wasRunning) {" in rest[i_register:]
    start = rest[rest.index("if ($StartNow -or $wasRunning) {"):]
    assert "Start-ScheduledTask -TaskName $TaskName" in start.split("} else {", 1)[0]
    assert "Stop-ScheduledTask" in uninstall                       # unchanged
    assert "-MultipleInstances IgnoreNew" in code                  # why the stop is needed


def test_ups_mode_words_the_installer_names_are_in_the_started_line() -> None:
    watch = UPS_WATCH.read_text(encoding="utf-8")
    assert "then shutdown /s /t {0}" in watch and "(-NoShutdown: PowerPanel shuts down)" in watch
    assert "$mode = 'DRY RUN, no action (would: ' + $mode + ')'" in watch
    assert "'ups-watch started (pid {0})" in watch
    install = UPS_INSTALL.read_text(encoding="utf-8")
    assert "'DRY RUN'" in install and "'-NoShutdown'" in install and "'shutdown /s'" in install


def test_runbook_step4_reinstalls_with_start_now_and_checks_the_mode_line() -> None:
    text = AUTOSTART.read_text(encoding="utf-8")
    step4 = _flat(_section(text, "**4. Test (home-server plan §4.13 step 6).**", "## Checks"))
    for needle in ("Re-install without `-DryRun`, with `-StartNow`", "stops the dry-run copy first",
                   "`ups-watch started`", "`then shutdown /s`", "`-NoShutdown`", "not `DRY RUN`",
                   "after any re-install with new thresholds"):
        assert needle in step4, needle
    assert "keeps the arguments it started with" in _flat(text)


# ---------------------------------------------------------------------------------------------- 4: receiver 401
def test_receiver_runbook_greps_for_the_real_refusal_line() -> None:
    text = RECEIVER_HOME.read_text(encoding="utf-8")
    assert "no 401 lines" not in text and "401 = token mismatch" not in text
    assert text.count("grep -c 'wrong bearer token'") >= 3                  # Checks, end-to-end 1, rotation 3
    service = RECEIVER_SERVICE.read_text(encoding="utf-8")
    assert 'log.warning("dead-man heartbeat with a missing or wrong bearer token refused")' in service
    tokens = RECEIVER_TOKENS.read_text(encoding="utf-8")
    assert '"%s: heartbeat authenticated with the %s token"' in tokens
    e2e = _flat(_section(text, "## End-to-end test", "## Token rotation"))
    for needle in ("heartbeat authenticated with the current token", "never logs an HTTP status code",
                   "do not prove the token"):
        assert needle in e2e, needle
    rotation = _flat(_section(text, "## Token rotation", "Each receiver restart"))
    assert "authenticated with the next token" in rotation and "wrong bearer token" in rotation


# ---------------------------------------------------------------------------------------------- 5: row 78
def test_early_78_refusals_ping_nothing_and_row_78_says_so() -> None:
    code = _code(BACKUP_PS1)
    i_url = code.index("if ($WrapperOnlyVars -contains $name) { $script:HcUrl = $v.Trim() }")
    early = [ln.strip() for ln in code[:i_url].splitlines() if "Exit-Backup 78" in ln]
    assert len(early) >= 4                       # engine account, broker variables, age private key, job files
    assert not any(ln.endswith("-Ping") for ln in early), early
    row78 = next(ln for ln in RUNBOOK.read_text(encoding="utf-8").splitlines() if ln.startswith("| 78 |"))
    assert "so check C turns red at once |" not in row78
    for needle in ("**before** the URL is read", "nothing is pinged", "period plus grace",
                   "a missing or malformed variable, the archive folder missing and the job's own 78"):
        assert needle in row78, needle
    help_text = BACKUP_PS1.read_text(encoding="utf-8").split("#>", 1)[0]
    assert "come before the URL is read, so they ping nothing" in help_text
