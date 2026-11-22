"""Home-server backup, review round 5 (deploy/local/backup/entrypoint.sh, home-backup-restore.md).

* A refused archive file (bit rot, an odd name, a damaged sidecar) never fails a night: the same file refused on
  later nights keeps exiting 65 after check C's success ping, with each night's dump uploaded. Restore step 2
  counts such nights as good, and the exit-65 row gives a path for a damaged copy that is already in the bucket
  (a detached partition's file): move it aside as `.stale` and put the bucket copy back.
* After an age key rotation, restore steps 2 and 6 take every still-needed private key, one line each, from a dated
  key list; there is no re-upload under a new key.
* The restore keeps the encrypted files off the tmpfs and says how to give a larger database more tmpfs.
* No restore command that runs the backup image uses the engine checkout's compose file; step 2 rebuilds the image
  from the protected job folder first; before Set-up 7 the admin-only clone is the only source.
* Set-up 3 locks the engine's checkout down (owner Administrators, no inherited entries, two narrow grants for the
  backup account) and lists the step as unverified until it has run on the box.

The job runs under a POSIX sh with stub tools (tests/deploy/test_home_backup_round1.py `rig`); nothing runs Docker,
PowerShell, icacls, B2 or Task Scheduler.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path

from tests.deploy.test_home_backup import RUNBOOK, _bucket_files
from tests.deploy.test_home_backup_round1 import _stdin, rig  # noqa: F401  (fixture)

JOB_COMPOSE = "'C:\\Program Files\\XTNL\\backup-job\\docker-compose.local.yml'"


def _text() -> str:
    return RUNBOOK.read_text(encoding="utf-8")


def _section(text: str, start: str, end: str) -> str:
    return text[text.index(start):text.index(end, text.index(start))]


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _export(tmp: Path, name: str, body: bytes) -> Path:
    part = tmp / "archive" / "event_log" / name
    part.parent.mkdir(parents=True, exist_ok=True)
    part.write_bytes(body)
    digest = hashlib.sha256(body).hexdigest()
    (part.parent / (name + ".sha256")).write_text(f"{digest}  {name}\n", encoding="ascii", newline="\n")
    return part


def _pings(tmp: Path) -> list[str]:
    """Pings the job sent: none since home-server review round 2 (backup-pg.ps1 sends every check C ping)."""
    log = tmp / "curl.log"
    return log.read_text(encoding="utf-8").split() if log.exists() else []


def _daily(tmp: Path) -> list[str]:
    root = tmp / "b2" / "xtnl-bucket"
    return [p.relative_to(root).as_posix() for p in _bucket_files(tmp) if p.relative_to(root).as_posix().startswith("daily/")]


# ---------------------------------------------------------------------------------------------- job (finding 3)
def test_a_refused_file_never_fails_a_night_however_many_nights_it_stays(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    _export(tmp, "event_log_p202608 (1).jsonl.gz", b"a hand-made copy\n")
    for _night in range(2):
        r = run(_stdin())
        out = r.stdout + r.stderr
        assert r.returncode == 65, out
        assert "ARCHIVE REFUSED" in out and "backup FAILED" not in out
        assert "DUMP SAFE:" in out
    assert _pings(tmp) == []
    assert any(x.endswith(".dump.age") for x in _daily(tmp))


def test_a_sidecar_that_rots_after_its_upload_is_refused_not_a_failed_night(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    part = _export(tmp, "event_log_p202607.jsonl.gz", b"rows\n")
    assert run(_stdin()).returncode == 0
    (part.parent / (part.name + ".sha256")).write_bytes(b"\x00\x13garbage\n")   # the sidecar is damaged on disk
    r = run(_stdin())
    out = r.stdout + r.stderr
    assert r.returncode == 65, out
    assert "malformed .sha256 sidecar" in out
    assert "DUMP SAFE:" in out and "backup FAILED" not in out and _pings(tmp) == []


def test_exit_65_row_has_a_bucket_copy_path_for_a_damaged_uploaded_file() -> None:
    row65 = next(ln for ln in _text().splitlines() if ln.startswith("| 65 |"))
    for needle in ("A file already in the bucket", "detached partition", "`archive forget` is not needed",
                   ".stale", "bucket copy back with Restore step 6", "never 70"):
        assert needle in row65, needle


def test_restore_step2_counts_exit_65_nights_as_good() -> None:
    which = _flat(_section(_text(), "**Which backup**", "Keep this shell open for step 3"))
    good = which.index("records as good.")
    assert "`backup-pg exit 65`" in which[good:] and "counts as good" in which[good:]


# ---------------------------------------------------------------------------------------------- age keys (finding 4)
def test_restore_steps_2_and_6_type_every_still_needed_key_one_line_each() -> None:
    text = _text()
    step2 = _flat(_section(text, "2. **Download, check, decrypt", "3. **Roles first, then the data.**"))
    step6 = _flat(_section(text, "6. **Partition archive files**", "7. **Start the engine**"))
    assert "TYPE each needed AGE-SECRET-KEY-1... line" in step2 and "every older key" in step2
    assert "TYPE every still-needed private key into `/dev/shm/identity`, one `AGE-SECRET-KEY-1...` line each" in step6


def test_rotation_keeps_a_dated_key_list_and_offers_no_re_upload() -> None:
    rotation = _flat(_section(_text(), "## Rotation", "## Rollback"))
    assert "re-upload the archive under the new key" not in rotation
    assert "or re-upload" not in rotation
    for needle in ("key list", "the date it became current", "the date it was replaced", "no re-upload procedure"):
        assert needle in rotation, needle


# ---------------------------------------------------------------------------------------------- tmpfs (finding 5)
def test_restore_keeps_encrypted_files_on_disk_and_explains_a_larger_shm_size() -> None:
    text = _text()
    step2 = _section(text, "2. **Download, check, decrypt", "3. **Roles first, then the data.**")
    step6 = _section(text, "6. **Partition archive files**", "7. **Start the engine**")
    assert "rclone copy r:<bucket>/daily/$TS/ /var/tmp/enc/" in step2
    assert "rm -f xtnl-$TS.dump.age globals-$TS.sql.age" in step2
    flat2 = _flat(step2)
    assert "A larger database" in flat2 and "--shm-size" in flat2 and "re-run Set-up 7" in flat2
    assert "mkdir -p /var/tmp/archive" in step6 and "rclone copy r:<bucket>/archive/" not in step6
    loop = step6[step6.index("while read -r parent part sum where; do"):step6.index("done < /dev/shm/ledger.txt")]
    assert 'rclone copyto $V "$B/archive/$obj" "$obj"' in loop                     # per ledger row, inside the loop


# ---------------------------------------------------------------------------------------------- compose file (6, 7)
def test_no_restore_command_runs_the_backup_image_from_the_checkout() -> None:
    restore = _section(_text(), "## Restore", "## Quarterly drill")
    for ln in restore.splitlines():
        if "--profile backup" in ln:
            assert f"-f {JOB_COMPOSE}" in ln, ln
        assert not re.search(r"-f\s+(?:C:\\xtnl\\order-server\\)?deploy\\local\\docker-compose\.local\.yml.*\bbackup\b",
                             ln), ln
    assert "-f deploy\\local\\docker-compose.local.yml --profile backup" not in restore
    assert "-f deploy/local/docker-compose.local.yml --profile backup" not in restore


def test_restore_step2_rebuilds_the_image_from_the_job_folder_first() -> None:
    step2 = _section(_text(), "2. **Download, check, decrypt", "3. **Roles first, then the data.**")
    build = f"docker compose -f {JOB_COMPOSE} --profile backup build --pull backup"
    run = f"docker compose -f {JOB_COMPOSE} --profile backup run --rm --no-deps --entrypoint sh backup"
    assert build in step2 and run in step2 and step2.index(build) < step2.index(run)
    flat = _flat(step2)
    for needle in ("Before Set-up 7 has run", "`C:\\xtnl-admin`", "never build or run it from `C:\\xtnl\\order-server`",
                   "is not reused"):
        assert needle in flat, needle


def test_rollback_runs_the_uninstall_from_the_admin_clone() -> None:
    rollback = _section(_text(), "## Rollback", "## Unverified on the home PC")
    assert "C:\\xtnl-admin\\order-server\\deploy\\local\\install-backup-task.ps1 -Uninstall" in rollback
    assert "-File deploy\\local\\install-backup-task.ps1" not in rollback


# ---------------------------------------------------------------------------------------------- checkout ACL (8)
def test_setup3_sets_owner_administrators_and_removes_inherited_entries() -> None:
    s3 = _section(_text(), "### 3. The backup account (admin)", "### 4. The backup account's User variables")
    owner = 'icacls $repo /setowner "*S-1-5-32-544" /T /C /Q'
    rights = 'icacls $repo /inheritance:r /grant:r "*S-1-5-32-544:(OI)(CI)F" "*S-1-5-18:(OI)(CI)F" "xtnl:(OI)(CI)M"'
    assert owner in s3 and rights in s3 and s3.index(owner) < s3.index(rights)
    rights_line = next(ln for ln in s3.splitlines() if ln.startswith(rights))
    assert "xtnl-backup" not in rights_line and "S-1-5-11" not in rights_line and "S-1-5-32-545" not in rights_line
    # the backup account's only grants: write in var\logs, read on local.env
    grants = [ln for ln in s3.splitlines()
              if ln.startswith("icacls") and "/grant" in ln and "xtnl-backup" in ln and "archive" not in ln]
    assert grants == ['icacls "$repo\\var\\logs" /grant "xtnl-backup:(RX,WD)" "*S-1-3-0:(OI)(IO)M" /C /Q',
                      'icacls "$repo\\deploy\\env\\local.env" /grant "xtnl-backup:(R)" /C /Q']
    assert "(Get-Acl $repo).Owner" in s3 and "BUILTIN\\Administrators" in s3
    flat = _flat(s3)
    for needle in ("owner Administrators", "git config --global --add safe.directory C:/xtnl/order-server",
                   "%LOCALAPPDATA%\\XTNL\\backup-logs", "**unverified** on the new box"):
        assert needle in flat, needle


def test_checkout_lockdown_is_listed_as_unverified_until_run() -> None:
    unverified = _flat(_text()[_text().index("## Unverified on the home PC"):])
    assert "lock-down of the engine's checkout: not run on the box yet" in unverified
