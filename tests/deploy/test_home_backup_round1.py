"""Home-server backup, review round 1 (docs/runbooks/home-backup-restore.md).

* Secrets never go into the container's configuration: backup-pg.ps1 hands XTNL_BACKUP_B2_ACCOUNT / _B2_KEY to
  the job on stdin, the job keeps them in shell variables and rclone reads a tmpfs rclone.conf. The check C URL
  never reaches the job (home-server review round 2: backup-pg.ps1 sends every ping). A timed-out container is
  removed (docker rm -f), not only stopped.
* The task runs a protected copy of the job files (install-backup-task.ps1 installs it with an ACL), and
  backup-pg.ps1 refuses to load a secret from files the engine account could change.
* The task's time limit is above backup-pg.ps1's own worst case; a job that never ran its trap is pinged /fail.
* Restore step 6 (partition archive files) works on the layout the real job uploads.

Behavioural parts run the real entrypoint.sh with stub tools under a POSIX sh, as tests/deploy/test_home_backup.py
does; nothing runs Docker, PowerShell, B2 or Task Scheduler.
"""
from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.deploy.test_home_backup import (
    BACKUP_PS1, ENTRYPOINT, RUNBOOK, STUBS, TASK_PS1, _bucket_files, _posix, _ps_code,
)

SECRETS = {"XTNL_BACKUP_B2_ACCOUNT": "stubkeyid0001", "XTNL_BACKUP_B2_KEY": "stub-secret-key-value"}
# A reversible age stub for the restore test only: header lines, then the input as it is; -d drops the header.
REVERSIBLE_AGE = ('out=""; in=""; dec=0\n'
                  'while [ $# -gt 0 ]; do case "$1" in -r|-i) shift ;; -o) out=$2; shift ;; -d) dec=1 ;; '
                  '*) in=$1 ;; esac; shift; done\n'
                  'if [ $dec = 1 ]; then tail -n +3 "$in" > "$out"; '
                  'else { printf "age-encryption.org/v1\\n-> X25519 stub\\n"; cat "$in"; } > "$out"; fi\n')


@pytest.fixture()
def rig(tmp_path: Path):  # type: ignore[no-untyped-def]
    sh = shutil.which("sh")
    if sh is None:
        pytest.skip("no POSIX sh on PATH")
    stubs = tmp_path / "bin"
    stubs.mkdir()
    for name, body in STUBS.items():
        f = stubs / name
        f.write_text("#!/bin/sh\n" + body, encoding="utf-8", newline="\n")
        f.chmod(0o755)
    for d in ("work", "out", "b2", "archive/event_log"):
        (tmp_path / d).mkdir(parents=True)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("XTNL_", "RCLONE_", "PG"))}
    env["PATH"] = os.pathsep.join([str(stubs), str(Path(sh).parent), env.get("PATH", "")])
    env.update({
        "XTNL_BACKUP_WORK_DIR": _posix(tmp_path / "work"), "XTNL_BACKUP_OUT_DIR": _posix(tmp_path / "out"),
        "XTNL_BACKUP_ARCHIVE_SRC": _posix(tmp_path / "archive"), "FAKE_B2": _posix(tmp_path / "b2"),
        "FAKE_CURL_LOG": _posix(tmp_path / "curl.log"),
        "XTNL_BACKUP_AGE_RECIPIENT": "age1stubpublickey", "XTNL_BACKUP_B2_BUCKET": "xtnl-bucket",
        "XTNL_BACKUP_SECRETS": "stdin",
        "PGHOST": "postgres", "PGUSER": "xtnl", "PGDATABASE": "xtnl",
    })

    def run(stdin: str, mode: str = "backup", **extra: str) -> subprocess.CompletedProcess[str]:
        # bytes, so the line ends are exactly what the test wrote (text mode turns LF into CRLF on Windows)
        r = subprocess.run([sh, _posix(ENTRYPOINT), mode], env={**env, **extra}, input=stdin.encode(),
                           capture_output=True, timeout=120, check=False)
        return subprocess.CompletedProcess(r.args, r.returncode, r.stdout.decode(errors="replace"),
                                           r.stderr.decode(errors="replace"))

    return tmp_path, run, sh


def _stdin(secrets: dict[str, str] = SECRETS, end: bool = True) -> str:
    return "".join(f"{k}={v}\n" for k, v in secrets.items()) + ("END\n" if end else "")


# ---------------------------------------------------------------------------------------------- job: stdin
def test_stdin_secrets_run_the_backup_and_never_reach_output_or_children(rig) -> None:  # type: ignore[no-untyped-def]
    tmp, run, _ = rig
    r = run(_stdin())
    assert r.returncode == 0, r.stdout + r.stderr
    out = r.stdout + r.stderr
    for value in SECRETS.values():
        assert value not in out
    # the rclone stub exits 8 if any secret is in its environment; the job never pings (review round 2)
    assert not (tmp / "curl.log").exists()
    assert not any((tmp / "work").iterdir()) and not any((tmp / "out").iterdir())


def test_stdin_mode_ignores_secrets_in_the_environment(rig) -> None:  # type: ignore[no-untyped-def]
    tmp, run, _ = rig
    r = run("END\n", **SECRETS)
    assert r.returncode == 78 and "XTNL_BACKUP_B2_ACCOUNT" in r.stdout and "XTNL_BACKUP_B2_KEY" in r.stdout
    assert _bucket_files(tmp) == []


def test_stdin_without_end_or_with_a_foreign_line_is_refused(rig) -> None:  # type: ignore[no-untyped-def]
    tmp, run, _ = rig
    r = run(_stdin(end=False))
    assert r.returncode == 78 and "END" in r.stdout
    r = run("PATH=/tmp\n" + _stdin())
    assert r.returncode == 78 and "/tmp" not in r.stdout
    assert _bucket_files(tmp) == []


def test_crlf_lines_are_accepted(rig) -> None:  # type: ignore[no-untyped-def]
    _, run, _ = rig
    r = run(_stdin().replace("\n", "\r\n"))
    assert r.returncode == 0, r.stdout + r.stderr


@pytest.mark.parametrize("name,value", [
    ("XTNL_BACKUP_HC_URL", 'https://hc.example/ping/x" -o /tmp/leak'),
    ("XTNL_BACKUP_B2_KEY", "abc\\ndef"),
    ("XTNL_BACKUP_B2_KEY", "AGE-SECRET-KEY-1QQQQ"),
])
def test_values_that_could_inject_config_are_refused(rig, name: str, value: str) -> None:  # type: ignore[no-untyped-def]
    tmp, run, _ = rig
    r = run(_stdin({**SECRETS, name: value}))
    assert r.returncode == 78, r.stdout + r.stderr
    assert value not in r.stdout + r.stderr and _bucket_files(tmp) == []


def test_check_mode_reads_stdin_too(rig) -> None:  # type: ignore[no-untyped-def]
    tmp, run, _ = rig
    (tmp / "b2" / "xtnl-bucket").mkdir()               # the stub lists an existing bucket
    assert run(_stdin(), mode="check").returncode == 0
    assert run("END\n", mode="check").returncode == 78


# ---------------------------------------------------------------------------------------------- wrapper
def _code(path: Path) -> str:
    return "\n".join(_ps_code(path))


def test_wrapper_never_passes_a_secret_with_dash_e_and_drops_them_from_its_environment() -> None:
    code = _code(BACKUP_PS1)
    m = re.search(r"\$SecretVars = @\(([^)]*)\)", code)
    assert m is not None
    assert set(re.findall(r"'([A-Z0-9_]+)'", m.group(1))) == set(SECRETS)
    loop = code[code.index("$runArgs = $compose + @('run'"):code.index("$runArgs += @('backup', $mode)")]
    assert loop.index("if ($SecretVars -contains $name) { continue }") < loop.index("$runArgs += @('-e', $name)")
    assert "XTNL_BACKUP_B2_KEY" not in loop
    # every backup variable leaves this process's environment before anything is started
    read = code[code.index("foreach ($name in $BackupVars) {"):code.index("XTNL_BACKUP_SECRETS', 'stdin'")]
    assert "[Environment]::SetEnvironmentVariable($name, $null, 'Process')" in read
    assert "$script:Secrets[$name] = $v.Trim()" in read
    assert "$code = Invoke-Logged $docker $runArgs '[job] ' ($TimeoutMinutes * 60) $stdin" in code
    assert '$stdin += "END`n"' in code
    assert "$psi.RedirectStandardInput = ($null -ne $StdinText)" in code


def test_wrapper_removes_a_timed_out_container_and_pings_for_jobs_that_never_ran() -> None:
    code = _code(BACKUP_PS1)
    assert "ps -aq --filter 'label=com.docker.compose.project=xtnl-local'" in code
    assert "@('rm', '-f', $id)" in code and "@('stop', $id)" not in code
    # home-server review round 2: the job never pings, so every code that is not a good night pings /fail here
    assert "$JobPingedCodes" not in code and "$GoodJobCodes = @(0, 65, 66)" in code
    assert code.rstrip().endswith("-Ping")


def test_wrapper_refuses_unprotected_job_files_before_reading_any_variable() -> None:
    code = _code(BACKUP_PS1)
    assert "$TrustedSids = @('S-1-5-32-544', 'S-1-5-18'," in code
    assert "$WriteRightsMask = [int64]0x500D0156" in code
    i_check = code.index("$offenders = @(Get-JobFileOffenders ($JobFiles + $JobFolders))")
    assert i_check < code.index("[Environment]::GetEnvironmentVariable($name, 'User')")
    assert re.search(r"if \(\$offenders\.Count -gt 0\) \{\s*Exit-Backup 78", code)
    # the mask covers write data, append, write (extended) attributes, delete child, delete, ACL, owner, generic
    mask = 0x500D0156
    for bit in (0x2, 0x4, 0x10, 0x40, 0x100, 0x10000, 0x40000, 0x80000, 0x40000000, 0x10000000):
        assert mask & bit
    assert not mask & 0x1 and not mask & 0x20 and not mask & 0x20000     # read data, execute, read control


def test_task_runs_the_protected_copy_with_a_time_limit_above_the_worst_case() -> None:
    code = _code(TASK_PS1)
    assert "[string]$JobDir = (Join-Path $env:ProgramFiles 'XTNL\\backup-job')" in code
    assert "is inside the repository" in code
    assert "$BackupScript = Join-Path $JobDir 'backup-pg.ps1'" in code
    assert "-WorkingDirectory $JobDir" in code and "-RepoRoot" in code
    assert "/inheritance:r /grant:r" in code and "/setowner '*S-1-5-32-544'" in code
    assert "Get-FileHash -LiteralPath $src -Algorithm SHA256" in code
    names = re.search(r"\$JobFileNames = @\(([^)]*)\)", code)
    assert names is not None and set(re.findall(r"'([^']+)'", names.group(1))) == {
        "backup-pg.ps1", "docker-compose.local.yml", "backup\\Dockerfile", "backup\\entrypoint.sh"}
    hours = int(re.search(r"\$TaskLimitHours = (\d+)", code).group(1))   # type: ignore[union-attr]
    assert "-ExecutionTimeLimit (New-TimeSpan -Hours $TaskLimitHours)" in code
    wrapper = _code(BACKUP_PS1)
    docker = int(re.search(r"\[int\]\$DockerWaitMinutes = (\d+)", wrapper).group(1))   # type: ignore[union-attr]
    job = int(re.search(r"\[int\]\$TimeoutMinutes = (\d+)", wrapper).group(1))   # type: ignore[union-attr]
    build = int(re.search(r"'build', '--quiet', 'backup'\)\) '\[build\] ' (\d+)", wrapper).group(1)) // 60  # type: ignore[union-attr]
    assert hours * 60 >= docker + build + job + 15, (hours, docker, build, job)


# ---------------------------------------------------------------------------------------------- runbook
def _step6_loop() -> str:
    """Restore step 6 from its per-object download loop to its summary line (review round 4: the bucket listing
    and the ledger come from the two lines before it, which the tests replace)."""
    text = RUNBOOK.read_text(encoding="utf-8")
    start = text.index("mkdir -p /var/tmp/archive && cd /var/tmp/archive\n")
    end = text.index("# failed must be 0", start)
    return text[start:text.index("\n", end)]


#: The restore side's rclone (review round 4): `copyto r:<bucket path> <local>` downloads one object.
RESTORE_RCLONE = '[ "$1" = copyto ] || exit 1\nsrc=${2#r:}\n[ -f "$src" ] || exit 3\ncp "$src" "$3"\n'


def _run_step6(tmp: Path, sh: str, ledger: str) -> tuple[subprocess.CompletedProcess[str], Path]:
    """Run step 6's loop against the fake bucket the job filled: the bucket listing as `rclone lsf -R --files-only`
    writes it, `ledger` as its psql line writes it, and each object fetched on its own with `rclone copyto`."""
    bucket = tmp / "b2" / "xtnl-bucket"
    shm, var, restore = tmp / "shm", tmp / "vartmp", tmp / "restore-archive"
    for d in (shm, var, restore):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir()
    rbin = tmp / "restore-bin"
    rbin.mkdir(exist_ok=True)
    (rbin / "rclone").write_text("#!/bin/sh\n" + RESTORE_RCLONE, encoding="utf-8", newline="\n")
    (rbin / "rclone").chmod(0o755)
    arch = bucket / "archive"
    listing = sorted(q.relative_to(arch).as_posix() for q in arch.rglob("*") if q.is_file()) if arch.exists() else []
    (shm / "remote.lst").write_text("".join(f"{x}\n" for x in listing), encoding="ascii", newline="\n")
    (shm / "identity").write_text("AGE-SECRET-KEY-1STUB\n", encoding="ascii")
    (shm / "ledger.txt").write_text(ledger, encoding="ascii", newline="\n")
    loop = (_step6_loop().replace("/dev/shm", _posix(shm)).replace("/var/tmp", _posix(var))
            .replace("/restore-archive", _posix(restore)))
    env = {**os.environ, "B": "r:" + _posix(bucket),
           "PATH": os.pathsep.join([str(rbin), str(tmp / "bin"), str(Path(sh).parent), os.environ["PATH"]])}
    r = subprocess.run([sh, "-c", loop], env=env, capture_output=True, text=True, timeout=60, check=False)
    return r, restore


def test_restore_step6_restores_every_archive_object_the_job_uploads(rig) -> None:  # type: ignore[no-untyped-def]
    tmp, run, sh = rig
    (tmp / "bin" / "age").write_text("#!/bin/sh\n" + REVERSIBLE_AGE, encoding="utf-8", newline="\n")
    parts = {}
    for table, name in (("event_log", "event_log_p202607.jsonl.gz"), ("account_snapshots", "acs_p202607.jsonl.gz")):
        (tmp / "archive" / table).mkdir(parents=True, exist_ok=True)
        part = tmp / "archive" / table / name
        part.write_bytes(f"rows of {name}\n".encode())
        (part.parent / (name + ".sha256")).write_text(f"{hashlib.sha256(part.read_bytes()).hexdigest()}  {name}\n",
                                                      encoding="ascii", newline="\n")
        parts[f"{table}/{name}"] = part.read_bytes()
    assert run(_stdin()).returncode == 0
    # the restored partition_archive rows (round 3: step 6 is driven by the ledger), as its psql line writes them
    r, restore = _run_step6(tmp, sh, "".join(
        f"{rel.split('/')[0]} {rel.split('/')[1].removesuffix('.jsonl.gz')} sha256:{hashlib.sha256(body).hexdigest()} "
        f"dropped\n" for rel, body in parts.items()))
    assert "ledger rows: 2, restored: 2, failed: 0, missing: 0, unexpected: 0" in r.stdout, r.stdout + r.stderr
    for rel, body in parts.items():
        assert (restore / rel).read_bytes() == body
        assert (restore / (rel + ".sha256")).exists()


def test_restore_step6_reports_a_damaged_object(rig) -> None:  # type: ignore[no-untyped-def]
    tmp, run, sh = rig
    (tmp / "bin" / "age").write_text("#!/bin/sh\n" + REVERSIBLE_AGE, encoding="utf-8", newline="\n")
    part = tmp / "archive" / "event_log" / "event_log_p202607.jsonl.gz"
    part.write_bytes(b"rows\n")
    (part.parent / (part.name + ".sha256")).write_text(f"{hashlib.sha256(b'rows' + bytes([10])).hexdigest()}  "
                                                       f"{part.name}\n", encoding="ascii", newline="\n")
    assert run(_stdin()).returncode == 0
    obj = next((tmp / "b2" / "xtnl-bucket" / "archive").rglob("*.tar.age"))      # damaged in the bucket
    obj.write_bytes(obj.read_bytes() + b"tampered")
    r, _ = _run_step6(tmp, sh, f"event_log event_log_p202607 sha256:{hashlib.sha256(b'rows' + bytes([10])).hexdigest()}"
                               f" dropped\n")
    assert "ledger rows: 1, restored: 0, failed: 1" in r.stdout and "FAILED" in r.stdout, r.stdout + r.stderr


def test_runbook_exit_table_and_secret_handling() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    row70 = next(ln for ln in text.splitlines() if ln.startswith("| 70 |"))
    row73 = next(ln for ln in text.splitlines() if ln.startswith("| 73 |"))
    assert "shm_size" in row70 and "No space left on device" in row70
    assert "shm_size" not in row73
    for needle in ("Accepted deviation", "docker exec", "C:\\Program Files\\XTNL\\backup-job", "stdin",
                   "docker rm -f", "3 h"):
        assert needle in text, needle
