"""Home-server backup, review round 4 (deploy/local/backup/entrypoint.sh, backup-pg.ps1, home-backup-restore.md).

* An archive file problem no longer fails the night after the dump is uploaded: objects already in the bucket are
  skipped before the file is hashed again, and a file that cannot be sent is refused, counted and logged; the run
  pings check C's success (the dump is good) and exits 65, which the wrapper turns into an ALERT: line without a
  /fail ping.
* Restore steps 2 and 6 run from the protected job folder's compose file as the backup account, keep the
  encrypted files off the tmpfs, download only the archive objects the ledger names, and take every still-needed
  age key; the rotation text no longer offers a re-upload that the job cannot do.
* Set-up 3 locks the engine's checkout down (no Users / Authenticated Users entries).

The job runs under a POSIX sh with stub tools (tests/deploy/test_home_backup_round1.py `rig`); nothing runs Docker,
PowerShell, B2 or Task Scheduler.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path

from tests.deploy.test_home_backup import BACKUP_PS1, ENTRYPOINT, RUNBOOK, _bucket_files, _ps_code
from tests.deploy.test_home_backup_round1 import _stdin, rig  # noqa: F401  (fixture)


def _export(tmp: Path, name: str, body: bytes, *, sidecar_body: bytes | None = None) -> Path:
    part = tmp / "archive" / "event_log" / name
    part.parent.mkdir(parents=True, exist_ok=True)
    part.write_bytes(body)
    digest = hashlib.sha256(body if sidecar_body is None else sidecar_body).hexdigest()
    (part.parent / (name + ".sha256")).write_text(f"{digest}  {name}\n", encoding="ascii", newline="\n")
    return part


def _pings(tmp: Path) -> list[str]:
    """Pings the job sent: none since home-server review round 2 (backup-pg.ps1 sends every check C ping)."""
    log = tmp / "curl.log"
    return log.read_text(encoding="utf-8").split() if log.exists() else []


def _archive_objects(tmp: Path) -> list[str]:
    root = tmp / "b2" / "xtnl-bucket"
    return [p.relative_to(root).as_posix() for p in _bucket_files(tmp) if p.relative_to(root).as_posix().startswith("archive/")]


def _section(text: str, start: str, end: str) -> str:
    return text[text.index(start):text.index(end)]


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", text)


# ---------------------------------------------------------------------------------------------- job (finding 4)
def test_a_file_already_in_the_bucket_that_rots_later_is_not_hashed_again(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    part = _export(tmp, "event_log_p202607.jsonl.gz", b"rows\n")
    assert run(_stdin()).returncode == 0
    part.write_bytes(b"rows with bit rot\n")              # damaged on disk after its upload
    r = run(_stdin())
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert "1 already in the bucket" in out and "ARCHIVE REFUSED" not in out
    assert "DUMP SAFE:" in out and _pings(tmp) == []


def test_a_damaged_new_file_is_refused_but_the_night_is_good(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    _export(tmp, "event_log_p202607.jsonl.gz", b"changed after export\n", sidecar_body=b"exported\n")
    _export(tmp, "event_log_p202608.jsonl.gz", b"good rows\n")
    r = run(_stdin())
    out = r.stdout + r.stderr
    assert r.returncode == 65, out
    assert "ARCHIVE REFUSED: event_log/event_log_p202607.jsonl.gz fails its .sha256 check" in out
    assert "1 file(s) uploaded" in out and "1 refused" in out
    assert "DUMP SAFE:" in out and "backup FAILED" not in out and _pings(tmp) == []
    root = tmp / "b2" / "xtnl-bucket"
    rel = [p.relative_to(root).as_posix() for p in _bucket_files(tmp)]
    assert len([x for x in rel if x.startswith("daily/")]) == 4                 # the dump is uploaded
    objs = _archive_objects(tmp)
    assert any("event_log_p202608" in x for x in objs) and not any("event_log_p202607" in x for x in objs)
    assert not any((tmp / "work").iterdir()) and not any((tmp / "out").iterdir())


def test_an_unexpected_file_name_is_refused_without_printing_it(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    _export(tmp, "event_log_p202608 (1).jsonl.gz", b"a hand-made copy\n")
    _export(tmp, "event_log_p202607.jsonl.gz", b"good rows\n")
    r = run(_stdin())
    out = r.stdout + r.stderr
    assert r.returncode == 65, out
    assert "ARCHIVE REFUSED: a file with an unexpected name" in out and "(1)" not in out
    assert any("event_log_p202607" in x for x in _archive_objects(tmp))


def test_a_malformed_sidecar_is_refused(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    part = _export(tmp, "event_log_p202607.jsonl.gz", b"rows\n")
    (part.parent / (part.name + ".sha256")).write_text("not a digest\n", encoding="ascii", newline="\n")
    r = run(_stdin())
    assert r.returncode == 65, r.stdout + r.stderr
    assert "malformed .sha256 sidecar" in r.stdout
    assert _archive_objects(tmp) == []


def test_entrypoint_checks_the_bucket_before_hashing() -> None:
    code = ENTRYPOINT.read_text(encoding="utf-8")
    step = code[code.index("archive_step() {"):code.index("# One archive file that cannot be sent")]
    assert step.index('grep -Fxq "$obj.sha256"') < step.index("sha256sum -c --status")
    loop = step.split("while IFS= read -r f; do", 1)[1]
    for gone in ("unexpected file name in the archive folder", "fails its .sha256 check\"", "malformed sidecar for",
                 "tar of $rel failed\""):
        assert gone not in loop, gone                                         # no whole-run failure per file
    run_backup = code[code.index("run_backup() {"):code.index("run_check() {")]
    # home-server review round 1: `DUMP SAFE:` (backup-pg.ps1 pings check C's success on it, review round 2) is
    # logged once the daily upload is done (DUMP_UP=1), still before the run ends and before exit 65
    assert (run_backup.index("DUMP_UP=1") < run_backup.index('log "DUMP SAFE:') < run_backup.index("DONE=1")
            < run_backup.index("exit 65"))
    assert "hc_ping" not in code


def test_wrapper_turns_65_into_an_alert_without_a_fail_ping() -> None:
    code = "\n".join(_ps_code(BACKUP_PS1))
    i65 = code.index("if ($code -eq 65) {")
    branch = code[i65:code.index("}", i65)]
    assert "Exit-Backup 65 'ALERT:" in branch and "-Ping" not in branch
    codes = re.search(r"\$GoodJobCodes = @\(([^)]*)\)", code)
    assert codes is not None and "65" in [c.strip() for c in codes.group(1).split(",")]


def test_runbook_exit_table_has_65_and_70_no_longer_sends_to_forget() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    row65 = next(ln for ln in text.splitlines() if ln.startswith("| 65 |"))
    row70 = next(ln for ln in text.splitlines() if ln.startswith("| 70 |"))
    for needle in ("check C recorded the night as good", ".stale", "Rollback", "archive forget", "Never drop"):
        assert needle in row65, needle
    assert "archive forget" not in row70 and "exit 65" in row70


# ---------------------------------------------------------------------------------------------- restore (6, 7, 8)
def test_restore_never_uses_the_checkouts_compose_file() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    restore = _section(text, "## Restore", "## Quarterly drill")
    assert "-f deploy\\local\\docker-compose.local.yml --profile backup" not in restore
    job = "docker compose -f 'C:\\Program Files\\XTNL\\backup-job\\docker-compose.local.yml' --profile backup run --rm --no-deps"
    step2 = _section(restore, "2. **Download, check, decrypt", "3. **Roles first, then the data.**")
    step6 = _section(restore, "6. **Partition archive files**", "7. **Start the engine**")
    assert f"{job} --entrypoint sh backup" in step2
    assert f"{job} -v C:/xtnl/archive:/restore-archive --entrypoint sh backup" in step6
    flat = _flat(step2)
    for needle in ("Which compose file", "as `xtnl-backup`", "Set-up 7", "-Uninstall", "capture the identity"):
        assert needle in flat, needle


def test_restore_keeps_encrypted_files_off_the_tmpfs_and_downloads_only_ledger_objects() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    step2 = _section(text, "2. **Download, check, decrypt", "3. **Roles first, then the data.**")
    step6 = _section(text, "6. **Partition archive files**", "7. **Start the engine**")
    assert "rclone copy r:<bucket>/daily/$TS/ /var/tmp/enc/" in step2
    assert "rclone copy r:<bucket>/daily/$TS/ /dev/shm/" not in step2
    assert "-o /dev/shm/xtnl.dump xtnl-$TS.dump.age" in step2
    assert "rclone copy r:<bucket>/archive/" not in step6
    assert 'rclone copyto $V "$B/archive/$obj" "$obj"' in step6 and 'rclone lsf $V -R --files-only "$B/archive/"' in step6  # $V: review round 7
    assert "shm_size" in step2


def test_restore_takes_every_still_needed_age_key_and_rotation_has_no_re_upload() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    step2 = _flat(_section(text, "2. **Download, check, decrypt", "3. **Roles first, then the data.**"))
    assert "Which private keys" in step2 and "key list" in step2 and "Enter after each" in step2
    rotation = _flat(_section(text, "## Rotation", "## Rollback"))
    assert "re-upload the archive under the new key" not in rotation
    assert "no re-upload procedure" in rotation and "key list" in rotation and "for ever" in rotation


# ---------------------------------------------------------------------------------------------- checkout ACL (9)
def test_setup3_locks_the_engines_checkout_down() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    s3 = _section(text, "### 3. The backup account (admin)", "### 4. The backup account's User variables")
    assert ('icacls $repo /inheritance:r /grant:r "*S-1-5-32-544:(OI)(CI)F" "*S-1-5-18:(OI)(CI)F" "xtnl:(OI)(CI)M" '
            "/T /C /Q") in s3
    assert '"xtnl-backup:(RX,WD)" "*S-1-3-0:(OI)(IO)M"' in s3
    assert 'icacls "$repo\\deploy\\env\\local.env" /grant "xtnl-backup:(R)"' in s3
    assert "icacls $repo " in s3 and "server.key" in s3
    flat = _flat(s3)
    for needle in ("STONE_X_*", "unverified", "dubious ownership", "Authenticated Users"):
        assert needle in flat, needle
