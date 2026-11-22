"""Home-server nightly backup (plan docs/design/home-server-plan.md s3.2 option A, W3) and the partition archive
folder (W10). Runbook: docs/runbooks/home-backup-restore.md.

Static checks of the compose service, the image, the job script and the two PowerShell scripts, plus one
behavioural run of the job script with stub tools (pg_dump, age, rclone, curl ...) when a POSIX `sh` is on PATH:
the dump is checked before anything leaves the PC, only age files are uploaded, nothing plain reaches the bucket,
the monthly copy happens once a month, archive files are encrypted and not re-sent, and every temp file is gone
afterwards. Nothing here runs Docker, PowerShell, B2 or Task Scheduler.
"""
from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.deploy.test_deploy_assets import _assignments, read_yaml

ROOT = Path(__file__).resolve().parents[2]
LOCAL = ROOT / "deploy" / "local"
COMPOSE = LOCAL / "docker-compose.local.yml"
DOCKERFILE = LOCAL / "backup" / "Dockerfile"
ENTRYPOINT = LOCAL / "backup" / "entrypoint.sh"
BACKUP_PS1 = LOCAL / "backup-pg.ps1"
TASK_PS1 = LOCAL / "install-backup-task.ps1"
LIVE_ENV = ROOT / "deploy" / "env" / "local-live.env.example"
RUNBOOK = ROOT / "docs" / "runbooks" / "home-backup-restore.md"
BACKUP_VARS = ("XTNL_BACKUP_AGE_RECIPIENT", "XTNL_BACKUP_B2_ACCOUNT", "XTNL_BACKUP_B2_KEY", "XTNL_BACKUP_B2_BUCKET",
               "XTNL_BACKUP_HC_URL", "XTNL_BACKUP_EXCLUDE_MARKS")


def _list(cfg: dict[str, str], prefix: str) -> list[str]:
    out: list[str] = []
    while f"{prefix}[{len(out)}]" in cfg:
        out.append(cfg[f"{prefix}[{len(out)}]"])
    return out


def _ps_code(path: Path) -> list[str]:
    """PowerShell lines outside comment-based help and line comments."""
    text = re.sub(r"<#.*?#>", "", path.read_text(encoding="utf-8"), flags=re.S)
    return [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]


def _sh_code(path: Path) -> list[str]:
    return [ln for ln in path.read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.lstrip().startswith("#")]


def _service(text: str, name: str) -> str:
    """One service's block of a compose file (until the next service or top-level key)."""
    m = re.search(rf"^  {re.escape(name)}:\n((?:    .*\n|\n)+?)(?=^  \S|^\S|\Z)", text, re.M)
    assert m, f"service {name} missing"
    return m.group(1)


def _function(code: str, name: str) -> str:
    m = re.search(rf"^{name}\(\) \{{\n(.*?)^\}}", code, re.M | re.S)
    assert m, f"function {name} missing"
    return m.group(1)


# ---------------------------------------------------------------------------------------------- compose
def test_backup_service_is_profile_gated_and_runs_once() -> None:
    cfg = read_yaml(COMPOSE)
    text = COMPOSE.read_text(encoding="utf-8")
    block = _service(text, "backup")
    assert re.search(r'^    profiles: \["backup"\]$', block, re.M), "backup must be in profile backup only"
    assert cfg["services.backup.restart"] == "no"
    assert cfg["services.backup.build.context"] == "./backup"
    assert "ports:" not in block and "healthcheck:" not in block and "depends_on" not in block
    for other in ("postgres", "redis", "redis-api", "etcd"):
        assert "profiles:" not in _service(text, other), f"{other} must stay in the plain up"
    # The header says how a plain up treats it.
    assert "--profile backup" in text.split("\nname:", 1)[0] or "profile `backup`" in text.split("\nname:", 1)[0]


def test_backup_service_reads_postgres_with_the_local_login_and_mounts_no_cache_or_market_data() -> None:
    cfg = read_yaml(COMPOSE)
    assert cfg["services.backup.environment.PGHOST"] == "postgres"
    assert cfg["services.backup.environment.PGPORT"] == "5432"
    assert cfg["services.backup.environment.PGUSER"] == cfg["services.postgres.environment.POSTGRES_USER"]
    assert cfg["services.backup.environment.PGPASSWORD"] == cfg["services.postgres.environment.POSTGRES_PASSWORD"]
    assert cfg["services.backup.environment.PGDATABASE"] == cfg["services.postgres.environment.POSTGRES_DB"]
    block = _service(COMPOSE.read_text(encoding="utf-8"), "backup")
    for name in BACKUP_VARS:
        assert name not in block, f"{name} must be passed by name at run time, never in the compose file"
    assert not re.search(r"redis|xtnl_local_pg|xtnl_local_etcd|docker_data", block, re.I)
    assert _list(cfg, "services.backup.volumes") == ["type: bind", "type: tmpfs"]   # nothing else is mounted
    assert cfg["services.backup.cap_drop"] == '["ALL"]'
    assert cfg["services.backup.security_opt"] == '["no-new-privileges:true"]'
    # The archive folder: read-only bind, never created by Docker; the tmpfs keeps no anonymous volume behind.
    assert re.search(r"- type: bind\n\s+source: \$\{XTNL_BACKUP_ARCHIVE_HOST:-C:/xtnl/archive\}\n\s+target: /archive\n"
                     r"\s+read_only: true\n\s+bind:\n\s+create_host_path: false", block)
    assert re.search(r"- type: tmpfs\n\s+target: /var/lib/postgresql/data", block)
    assert re.search(r'^    shm_size: "\d+gb"$', block, re.M)


# ---------------------------------------------------------------------------------------------- image
def test_dockerfile_builds_on_the_stack_postgres_image_with_pinned_rclone_and_age() -> None:
    text = DOCKERFILE.read_text(encoding="utf-8")
    froms = re.findall(r"^FROM\s+(\S+)", text, re.M)
    assert froms[-1] == read_yaml(COMPOSE)["services.postgres.image"] == "postgres:17"
    rclone = next(f for f in froms if f.startswith("rclone/rclone:"))
    assert re.fullmatch(r"rclone/rclone:\d+\.\d+\.\d+", rclone), "rclone pinned to an exact version"
    assert re.search(r"apt-get install -y --no-install-recommends[^\n]*\bage\b", text)
    assert "COPY --from=rclone /usr/local/bin/rclone /usr/local/bin/rclone" in text
    assert re.search(r"^USER postgres$", text, re.M)
    assert 'ENTRYPOINT ["/usr/local/bin/xtnl-backup"]' in text
    assert not re.search(r"(B2_KEY|B2_ACCOUNT|AGE-SECRET|rclone\.conf\s)", text.split("FROM", 1)[1])


# ---------------------------------------------------------------------------------------------- job script
def test_entrypoint_is_strict_posix_sh() -> None:
    text = ENTRYPOINT.read_text(encoding="utf-8")
    assert text.startswith("#!/bin/sh\n")
    assert "\r" not in text
    code = _sh_code(ENTRYPOINT)
    assert code[0] == "set -eu", "set -eu must be the first command"
    joined = "\n".join(code)
    assert "set -o pipefail" not in joined and "[[" not in joined and "function " not in joined
    assert "set -x" not in text and "set -o xtrace" not in text


def test_entrypoint_never_touches_the_market_data_store_or_caches() -> None:
    assert "redis" not in ENTRYPOINT.read_text(encoding="utf-8").lower()


def test_entrypoint_checks_then_encrypts_before_any_upload() -> None:
    code = "\n".join(_sh_code(ENTRYPOINT)) + "\n"
    run = _function(code, "run_backup")
    first = {k: run.index(k) for k in ("pg_restore -l", "pg_dumpall --globals-only --no-role-passwords",
                                       "encrypt \"$dump\"", "encrypt \"$globals\"", "upload ")}
    assert first["pg_restore -l"] < first["encrypt \"$dump\""] < first["upload "]
    assert first["encrypt \"$globals\""] < first["upload "]
    # Every upload call site names an encrypted output under $OUT.
    for m in re.finditer(r"^\s*upload (\S+) ", code, re.M):
        assert m.group(1).startswith('"$f') or m.group(1).startswith('"$OUT/'), m.group(0)
    up = _function(code, "upload")
    assert '"$OUT"/*.age|"$OUT"/*.age.sha256) ;;' in up and "die 70" in up
    enc = _function(code, "encrypt")
    assert 'age -r "$XTNL_BACKUP_AGE_RECIPIENT"' in enc and "age-encryption.org/v1" in enc
    assert enc.index("age-encryption.org/v1") < enc.index('rm -f "$1"')
    assert '"$WORK"/*) ;;' in enc
    # The plain dump lives on tmpfs; the trap removes everything.
    assert "WORK_BASE=${XTNL_BACKUP_WORK_DIR:-/dev/shm}" in code
    assert "trap cleanup EXIT" in code
    clean = _function(code, "cleanup")
    assert 'rm -rf "$WORK"' in clean and 'rm -rf "$OUT"' in clean
    # home-server review round 2: the job never pings check C (backup-pg.ps1 sends every ping, outside Docker)
    assert "hc_ping" not in code and "curl" not in code


def test_entrypoint_needs_no_delete_right_and_keeps_secrets_out_of_output() -> None:
    code = "\n".join(_sh_code(ENTRYPOINT))
    rclone_calls = re.findall(r"^\s*(?:\w+=\S+\s+)?rclone (\w+)", code, re.M)
    assert set(rclone_calls) <= {"copyto", "lsf", "version"}, rclone_calls
    assert "--no-check-dest" in code
    # The key goes into a tmpfs rclone.conf (never an exported RCLONE_CONFIG_* variable, review round 1).
    assert 'RCLONE_CONFIG="$WORK/rclone.conf"' in code and "printf 'key = %s\\n' \"$XTNL_BACKUP_B2_KEY\"" in code
    assert "RCLONE_CONFIG_XTNLB2_KEY" not in code and "export RCLONE_CONFIG_XTNLB2" not in code
    # The only printf lines that may carry a secret: the rclone.conf lines (redirected into the tmpfs file by the
    # enclosing block). The check C URL never reaches the job (review round 2), so no curl line carries it.
    secret_sinks = {"printf 'account = %s\\n' \"$XTNL_BACKUP_B2_ACCOUNT\"", "printf 'key = %s\\n' \"$XTNL_BACKUP_B2_KEY\""}
    assert {ln.strip() for ln in code.splitlines()} >= secret_sinks
    assert "curl" not in code
    for line in code.splitlines():
        if line.strip() in secret_sinks:
            continue
        if re.search(r"\b(log|die|echo|printf)\b", line):
            assert not re.search(r"\$\{?(XTNL_BACKUP_B2_KEY|XTNL_BACKUP_B2_ACCOUNT|XTNL_BACKUP_HC_URL|"
                                 r"RCLONE_CONFIG_XTNLB2_KEY|PGPASSWORD)", line), line.strip()
    assert 'env | grep -q \'AGE-SECRET-KEY-\'' in code
    assert "hc_ping" not in code
    for flag in ("--globals-only", "--no-role-passwords", "-Fc"):
        assert flag in code


# ---------------------------------------------------------------------------------------------- PowerShell
@pytest.mark.parametrize("path", [BACKUP_PS1, TASK_PS1], ids=lambda p: p.name)
def test_scripts_are_windows_powershell_51(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    assert text.isascii() and text.lstrip().startswith("<#") and ".SYNOPSIS" in text and ".EXAMPLE" in text
    assert "$ErrorActionPreference = 'Stop'" in text
    for line in _ps_code(path):
        stripped = re.sub(r"'[^']*'|\"[^\"]*\"", "''", line)
        for bad in ("&&", "||", "??", "?."):
            assert bad not in stripped, (path.name, line.strip())
        assert not re.search(r"\s\?\s[^:]+\s:\s", stripped), (path.name, line.strip())


def test_backup_wrapper_passes_names_only() -> None:
    code = "\n".join(_ps_code(BACKUP_PS1))
    assert "$runArgs += @('-e', $name)" in code
    assert not re.search(r"'-e'\s*,\s*\(?\s*\"?\$name\s*\+\s*'='", code)
    assert not re.search(r"-e\s+\"?\$?\w+=", code)
    assert re.search(r"'run', '--rm', '--no-deps', '-T'", code)
    assert "'--profile', 'backup'" in code
    m = re.search(r"\$BackupVars = @\(([^)]*)\)", code)
    assert m is not None
    assert tuple(re.findall(r"'([A-Z0-9_]+)'", m.group(1))) == BACKUP_VARS
    assert "[Environment]::GetEnvironmentVariable($name, 'User')" in code
    # Values never reach the log: no Write-Log / Write-Host of a variable's value.
    for line in _ps_code(BACKUP_PS1):
        if re.search(r"Write-(Log|Host|RawLog|Output|Warning)", line):
            assert not re.search(r"\$(value|vars\[)", line), line.strip()
    assert "Register-ScheduledTask" not in code


def test_backup_wrapper_refuses_the_engine_account_and_broker_variables() -> None:
    code = "\n".join(_ps_code(BACKUP_PS1))
    assert "[string]$EngineAccount = 'xtnl'" in code
    assert re.search(r"if \(\$shortName -ieq \$EngineAccount\) \{\s*Exit-Backup 78", code)
    assert "$EngineOnlyPatterns = @('STONE_X_*', 'CTRADER_*')" in code
    assert re.search(r"if \(\$visible\.Count -gt 0\) \{\s*Exit-Backup 78", code)
    assert "AGE-SECRET-KEY-" in code
    # The account checks run before any backup variable is read.
    assert code.index("$shortName -ieq $EngineAccount") < code.index("GetEnvironmentVariable($name, 'User')")
    assert code.index("$visible.Count -gt 0") < code.index("GetEnvironmentVariable($name, 'User')")


def test_backup_wrapper_logs_to_a_daily_file_and_documents_exit_codes() -> None:
    text = BACKUP_PS1.read_text(encoding="utf-8")
    assert "var\\logs" in text and "backup-{0}.log" in text
    help_text = text.split("#>", 1)[0]
    assert "docker-users" in help_text
    for code in ("0 done", "10 Docker not reachable", "11 the image build failed", "75 the job timed out",
                 "78 configuration"):
        assert code in help_text, code
    code_lines = "\n".join(_ps_code(BACKUP_PS1))
    for n in ("Exit-Backup 10", "Exit-Backup 11", "Exit-Backup 75", "Exit-Backup 78"):
        assert n in code_lines


def test_install_backup_task_registers_a_daily_task_for_the_backup_account() -> None:
    code = "\n".join(_ps_code(TASK_PS1))
    assert "$TaskName = 'XTNL backup'" in code
    assert "[string]$User = \"$env:COMPUTERNAME\\xtnl-backup\"" in code
    assert "[string]$At = '03:15'" in code
    assert "New-ScheduledTaskTrigger -Daily -At $time" in code
    assert "Register-ScheduledTask -TaskName $TaskName -Force" in code
    assert "if ($Uninstall)" in code and "Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false" in code
    assert re.search(r"if \(\$shortName -ieq \$EngineAccount\) \{\s*throw", code)
    assert "backup-pg.ps1" in code and "-MultipleInstances IgnoreNew" in code
    assert "-LogonType S4U" in code
    # The password (Password logon type) is handed to Task Scheduler only, never printed.
    for line in _ps_code(TASK_PS1):
        if "Write-" in line:
            assert "Password" not in line.replace("-LogonType Password", "") or "logon type" in line, line.strip()
            assert "GetNetworkCredential" not in line


# ---------------------------------------------------------------------------------------------- W10 env example
def test_live_env_example_sets_the_archive_dir_outside_onedrive() -> None:
    text, assigned = _assignments(LIVE_ENV)
    value = assigned["XTNL_ARCHIVE_DIR"]
    assert re.fullmatch(r"[A-Za-z]:\\[^\s]+", value), "an absolute Windows folder"
    assert "onedrive" not in value.lower() and "order-server" not in value.lower()
    section = text.split("XTNL_ARCHIVE_DIR=", 1)[0].rsplit("# ---", 1)[1]
    for needle in ("OUTSIDE OneDrive", "encrypt", "backup-pg.ps1", "await", "90 days"):
        assert needle in section, needle
    assert not [k for k in assigned if k.startswith(("XTNL_BACKUP_", "RCLONE_", "B2_"))]


# ---------------------------------------------------------------------------------------------- runbook
def test_runbook_covers_setup_restore_and_drill() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    for needle in ("never sends", "Object Lock", "Compliance, 367 days", "`daily/` | 31 | 337", "`monthly/` | 366 | 2",
                   "**no** `deleteFiles`", "age-keygen", "password manager", "paper", "docker-users",
                   "install-backup-task.ps1", "check **C", "/fail", "W18", "single-pc-autostart.md",
                   "--approver <second person>", "epoch adopt", "0014_api_role.sql",
                   "new-liveview-credentials.ps1 -Rotate", "new-api-credentials.ps1 -DbRole", "Quarterly drill",
                   "XTNL_ARCHIVE_DIR", "pg_restore -h postgres", "catchup show", "## Rollback"):
        assert needle in text, needle
    assert "listBuckets,listFiles,writeFiles" in text
    assert not re.search(r"listBuckets,listFiles,writeFiles,deleteFiles", text)


# ---------------------------------------------------------------------------------------------- behaviour
STUBS = {
    "pg_isready": "exit \"${FAKE_PG_DOWN:-0}\"\n",
    "pg_dump": ('out=""\nwhile [ $# -gt 0 ]; do [ "$1" = -f ] && out=$2; shift; done\n'
                'printf "PGDMP PLAINTEXT-DUMP account rows\\n" > "$out"\n'),
    "pg_restore": ('[ -z "${FAKE_RESTORE_FAIL:-}" ] || exit 1\n'
                   'printf ";\\n3001; 0 0 TABLE DATA public orders_state xtnl\\n"\n'),
    "pg_dumpall": ('out=""\nwhile [ $# -gt 0 ]; do [ "$1" = -f ] && out=$2; shift; done\n'
                   'printf "CREATE ROLE xtnl; PLAINTEXT-GLOBALS\\n" > "$out"\n'),
    # Writes an age header and a digest of the input, never the input itself.
    "age": ('out=""; in=""\nwhile [ $# -gt 0 ]; do case "$1" in -r) shift ;; -o) out=$2; shift ;; *) in=$1 ;; esac; '
            'shift; done\n{ printf "age-encryption.org/v1\\n-> X25519 stub\\n"; sha256sum < "$in"; } > "$out"\n'),
    # The URL comes as a config line on stdin (-K -); an URL on the command line is recorded as a leak.
    "curl": ('for a in "$@"; do last=$a; case "$a" in *hc.example*) echo ARGV-LEAK >> "$FAKE_CURL_LOG" ;; esac; done\n'
             'if [ "$last" = "-" ]; then url=$(sed -n \'s/^url = "\\(.*\\)"$/\\1/p\'); else url=$last; fi\n'
             'printf "%s\\n" "$url" >> "$FAKE_CURL_LOG"\n'),
    # The remote comes from the job's rclone.conf; no secret may reach rclone through its environment.
    "rclone": ('grep -qx "type = b2" "$RCLONE_CONFIG" || exit 9\n'
               'grep -q "^key = ." "$RCLONE_CONFIG" || exit 9\n'
               '[ -z "${RCLONE_CONFIG_XTNLB2_KEY:-}${XTNL_BACKUP_B2_KEY:-}${XTNL_BACKUP_HC_URL:-}" ] || exit 8\n'
               'cmd=$1; shift; args=""\n'
               'while [ $# -gt 0 ]; do case "$1" in --retries|--low-level-retries|--stats|--max-depth) shift ;; '
               '-*) ;; *) args="$args $1" ;; esac; shift; done\n'
               'set -- $args\n'
               'path() { p=${1#xtnlb2:}; printf "%s/%s" "$FAKE_B2" "$p"; }\n'
               'case $cmd in\n'
               '  copyto) [ -z "${FAKE_UPLOAD_FAIL:-}" ] || exit 1; d=$(path "$2"); mkdir -p "$(dirname "$d")"; '
               'cp "$1" "$d"; printf "%s\\n" "$2" >> "$FAKE_B2/../uploads.log" ;;\n'
               '  lsf) d=$(path "$1"); [ -d "$d" ] || exit 3; (cd "$d" && find . -type f | sed "s|^\\./||") ;;\n'
               '  *) exit 1 ;;\n'
               'esac\n'),
}


def _posix(p: Path) -> str:
    """The path as an MSYS / Git-for-Windows sh sees it (/c/...), or unchanged on POSIX."""
    if os.name != "nt":
        return str(p)
    s = p.resolve().as_posix()
    return f"/{s[0].lower()}{s[2:]}"


@pytest.fixture()
def job(tmp_path: Path):  # type: ignore[no-untyped-def]
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
        "XTNL_BACKUP_AGE_RECIPIENT": "age1stubpublickey", "XTNL_BACKUP_B2_ACCOUNT": "stub-key-id",
        "XTNL_BACKUP_B2_KEY": "stub-secret-key-value", "XTNL_BACKUP_B2_BUCKET": "xtnl-bucket",
        "XTNL_BACKUP_HC_URL": "https://hc.example/ping/check-c-uuid",
        "PGHOST": "postgres", "PGUSER": "xtnl", "PGDATABASE": "xtnl",
    })

    def run(**extra: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run([sh, _posix(ENTRYPOINT)], env={**env, **extra}, capture_output=True, text=True,
                              timeout=120, check=False)

    return tmp_path, run


def _bucket_files(tmp: Path) -> list[Path]:
    return sorted(p for p in (tmp / "b2" / "xtnl-bucket").rglob("*") if p.is_file())


def test_job_run_uploads_only_encrypted_files_and_cleans_up(job) -> None:  # type: ignore[no-untyped-def]
    tmp, run = job
    part = tmp / "archive" / "event_log" / "event_log_p202607.jsonl.gz"
    part.write_bytes(b"PLAINTEXT-ARCHIVE account data\n")
    sidecar = f"{hashlib.sha256(part.read_bytes()).hexdigest()}  {part.name}\n"     # sha256sum -c format
    (part.parent / (part.name + ".sha256")).write_text(sidecar, encoding="ascii", newline="\n")
    (tmp / "archive" / "event_log" / "event_log_p202608.jsonl.gz").write_bytes(b"being exported\n")  # no sidecar

    r = run()
    assert r.returncode == 0, r.stdout + r.stderr
    files = _bucket_files(tmp)
    rel = [p.relative_to(tmp / "b2" / "xtnl-bucket").as_posix() for p in files]
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    daily = [x for x in rel if x.startswith("daily/")]
    monthly = [x for x in rel if x.startswith(f"monthly/{month}/")]
    archive = [x for x in rel if x.startswith("archive/")]
    assert len(daily) == 4 and len(monthly) == 4 and len(archive) == 2, rel
    assert all(x.endswith((".age", ".age.sha256")) for x in rel), rel
    assert any(re.fullmatch(r"archive/event_log/event_log_p202607\.jsonl\.gz\.[0-9a-f]{16}\.tar\.age", x)
               for x in archive)
    for p in files:
        body = p.read_bytes()
        assert b"PLAINTEXT" not in body and b"PGDMP" not in body and b"CREATE ROLE" not in body, p.name
        if p.name.endswith(".age"):
            assert body.startswith(b"age-encryption.org/v1")
    out = r.stdout + r.stderr
    assert "stub-secret-key-value" not in out and "check-c-uuid" not in out and "stub-key-id" not in out
    assert "1 file(s) uploaded" in out and "1 awaiting their sidecar" in out
    # The check C URL in the environment is dropped unread and never pinged (review round 2: backup-pg.ps1 pings).
    assert not (tmp / "curl.log").exists()
    assert "XTNL_BACKUP_HC_URL in the environment ignored" in out
    assert not any((tmp / "work").iterdir()) and not any((tmp / "out").iterdir()), "temp files left behind"

    # Second run the same month: no second monthly copy, the archive file is not sent again.
    uploads_before = len((tmp / "uploads.log").read_text(encoding="utf-8").splitlines())
    r2 = run()
    assert r2.returncode == 0, r2.stdout + r2.stderr
    new = (tmp / "uploads.log").read_text(encoding="utf-8").splitlines()[uploads_before:]
    assert len(new) == 4 and all("/daily/" in x for x in new), new
    assert "already holds this month's copy" in r2.stdout and "1 already in the bucket" in r2.stdout


def test_job_aborts_before_upload_when_the_dump_does_not_list(job) -> None:  # type: ignore[no-untyped-def]
    tmp, run = job
    r = run(FAKE_RESTORE_FAIL="1")
    assert r.returncode == 70, r.stdout + r.stderr
    assert _bucket_files(tmp) == []
    assert not (tmp / "curl.log").exists() and "backup FAILED (exit 70)" in r.stdout
    assert not any((tmp / "work").iterdir()) and not any((tmp / "out").iterdir())


def test_job_refuses_missing_settings_and_private_keys(job) -> None:  # type: ignore[no-untyped-def]
    tmp, run = job
    r = run(XTNL_BACKUP_B2_KEY="")
    assert r.returncode == 78 and "XTNL_BACKUP_B2_KEY" in r.stdout
    r = run(SOME_VAR="AGE-SECRET-KEY-1QQQQ")
    assert r.returncode == 78 and "AGE-SECRET-KEY-1QQQQ" not in r.stdout + r.stderr
    r = run(XTNL_BACKUP_AGE_RECIPIENT="ssh-ed25519 AAAA")
    assert r.returncode == 78
    assert _bucket_files(tmp) == []


def test_job_upload_failure_is_retryable_exit(job) -> None:  # type: ignore[no-untyped-def]
    tmp, run = job
    r = run(FAKE_UPLOAD_FAIL="1")
    assert r.returncode == 75, r.stdout + r.stderr
    assert not any((tmp / "work").iterdir()) and not any((tmp / "out").iterdir())
