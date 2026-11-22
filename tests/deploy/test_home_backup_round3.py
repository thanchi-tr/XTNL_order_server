"""Home-server backup, review round 3 (docs/runbooks/home-backup-restore.md, single-pc-autostart.md "Carry over").

* Restore step 3 creates every role a migration creates (0011, 0014, 0017, 0021), each through its last
  ALTER ROLE line (0017: its DO block), before `pg_restore --exit-on-error`; the carry-over block does the same.
* Restore step 6 is driven by the restored `partition_archive` rows: only the object named after a row's checksum is
  extracted, its file is checked against that checksum, other objects are reported and never extracted.
* Restore step 2 has the age identity typed (Set-up 4's clipboard rule), the backup log is a hint only, and Set-up 8
  runs `versions` from the protected job folder.

The step 6 loop runs under a POSIX sh with stub tools, as in tests/deploy/test_home_backup_round1.py; nothing runs
Docker, PostgreSQL or B2.
"""
from __future__ import annotations

import hashlib
import io
import os
import re
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

from tests.deploy.test_home_backup import RUNBOOK, _posix
from tests.deploy.test_home_backup_round1 import REVERSIBLE_AGE, _run_step6, _stdin, rig  # noqa: F401  (fixture)

ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = ROOT / "src" / "db" / "migrations"
CARRY_RUNBOOK = ROOT / "docs" / "runbooks" / "single-pc-autostart.md"
PART = "event_log/event_log_p202607.jsonl.gz"


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", text)


# ---------------------------------------------------------------------------------------------- roles (F2, F3)
def _migration_roles() -> dict[str, set[str]]:
    """{migration file: roles it creates}, from the CREATE ROLE statements outside comments."""
    out: dict[str, set[str]] = {}
    for f in sorted(MIGRATIONS.glob("*.sql")):
        code = "\n".join(ln.split("--", 1)[0] for ln in f.read_text(encoding="utf-8").splitlines())
        roles = set(re.findall(r"\bCREATE ROLE (\w+)", code))
        if roles:
            out[f.name] = roles
    return out


STEP = re.compile(r"Get-Content src\\db\\migrations\\(\S+\.sql) (?:-TotalCount (\d+)|\| Select-Object -Skip (\d+) "
                  r"-First (\d+)) \| docker exec -i xtnl-local-postgres-1 psql -U xtnl -d xtnl -v ON_ERROR_STOP=1 -q")


def _role_steps(block: str) -> dict[str, list[str]]:
    """{migration file: the lines its role step pipes into psql}."""
    out: dict[str, list[str]] = {}
    for m in STEP.finditer(block):
        lines = (MIGRATIONS / m.group(1)).read_text(encoding="utf-8").splitlines()
        if m.group(2):
            out[m.group(1)] = lines[:int(m.group(2))]
        else:
            skip, first = int(m.group(3)), int(m.group(4))
            out[m.group(1)] = lines[skip:skip + first]
    return out


def _check_role_steps(block: str) -> None:
    steps = _role_steps(block)
    roles = _migration_roles()
    assert set(steps) == set(roles), (sorted(steps), sorted(roles))     # every CREATE ROLE migration, no other
    for name, piped in steps.items():
        code = [ln for ln in piped if ln.strip() and not ln.lstrip().startswith("--")]
        text = "\n".join(code)
        for role in roles[name]:
            assert f"CREATE ROLE {role}" in text, (name, role)
            # every role-level SET of the migration is piped too (not in a dump, and migrate never re-runs it)
            full = (MIGRATIONS / name).read_text(encoding="utf-8").splitlines()
            sets = [ln for ln in full if ln.startswith(f"ALTER ROLE {role} SET")]
            assert all(s in piped for s in sets), (name, role)
        # a complete statement at the end, and no grant on a table / schema (not restored yet)
        assert code[-1].rstrip().endswith(";"), (name, code[-1])
        assert text.count("$$") % 2 == 0, name                              # no DO block cut in half
        assert not re.search(r"\bON (TABLE|SCHEMA|ALL|SEQUENCE)\b", text), name
        assert not re.search(r"^\s*GRANT \w+ ON ", text, re.M), name


def test_restore_step3_creates_every_migration_role_before_pg_restore() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    step3 = text[text.index("3. **Roles first, then the data.**"):text.index("4. **etcd and the epoch fence.**")]
    _check_role_steps(step3)
    assert max(m.start() for m in STEP.finditer(step3)) < step3.index("pg_restore -h postgres")
    assert "-TotalCount 26" not in step3                                   # 0011 through line 30, not 26
    flat = _flat(step3)
    assert "xtnl_arm" in flat and "GRANT xtnl_arm TO current_user" in flat and "not in the dump" in flat
    assert "Skip 0021" not in flat


def test_carry_over_creates_every_migration_role_before_pg_restore() -> None:
    text = CARRY_RUNBOOK.read_text(encoding="utf-8")
    carry = text[text.index("## Carry over"):text.index("## Rebuild etcd")]
    _check_role_steps(carry)
    assert max(m.start() for m in STEP.finditer(carry)) < carry.index("pg_restore")
    assert "-TotalCount 26" not in carry


def test_0017_role_block_is_where_the_runbooks_cut_it() -> None:
    lines = (MIGRATIONS / "0017_arm.sql").read_text(encoding="utf-8").splitlines()
    block = lines[398:413]
    assert block[0] == "DO $$" and block[-1] == "$$;"
    assert any("CREATE ROLE xtnl_arm NOLOGIN" in ln for ln in block)
    assert any("GRANT xtnl_arm TO %I" in ln for ln in block)
    assert not re.search(r"\b(PASSWORD|LOGIN )\b", "\n".join(block).replace("NOLOGIN", ""))


# ---------------------------------------------------------------------------------------------- step 6 (F6, F8)
def _run_loop(tmp: Path, sh: str, ledger: list[tuple[str, str, str]]) -> tuple[subprocess.CompletedProcess[str], Path]:
    return _run_step6(tmp, sh, "".join(f"{rel.split('/')[0]} {rel.split('/')[1].removesuffix('.jsonl.gz')} "
                                       f"sha256:{hexd} {where}\n" for rel, hexd, where in ledger))


def _export(tmp: Path, body: bytes) -> str:
    part = tmp / "archive" / PART
    part.parent.mkdir(parents=True, exist_ok=True)
    part.write_bytes(body)
    hexd = hashlib.sha256(body).hexdigest()
    (part.parent / (part.name + ".sha256")).write_text(f"{hexd}  {part.name}\n", encoding="ascii", newline="\n")
    return hexd


@pytest.mark.parametrize("ledger_names", ["first", "second"])
def test_two_objects_for_one_partition_restore_only_the_one_the_ledger_names(rig, ledger_names: str) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, sh = rig
    (tmp / "bin" / "age").write_text("#!/bin/sh\n" + REVERSIBLE_AGE, encoding="utf-8", newline="\n")
    first = _export(tmp, b"first export\n")
    assert run(_stdin()).returncode == 0
    second = _export(tmp, b"re-export after archive forget\n")     # a new object with another sha256 prefix
    assert run(_stdin()).returncode == 0
    objs = sorted(p.name for p in (tmp / "b2" / "xtnl-bucket" / "archive" / "event_log").glob("*.tar.age"))
    assert len(objs) == 2
    want, body = (first, b"first export\n") if ledger_names == "first" else (second, b"re-export after archive forget\n")
    r, restore = _run_loop(tmp, sh, [(PART, want, "dropped")])
    assert "ledger rows: 1, restored: 1, failed: 0, missing: 0, unexpected: 1" in r.stdout, r.stdout + r.stderr
    assert "UNEXPECTED (not extracted)" in r.stdout
    assert (restore / PART).read_bytes() == body
    assert (restore / (PART + ".sha256")).read_text(encoding="ascii") == f"{want}  event_log_p202607.jsonl.gz\n"


def test_a_forged_object_under_the_ledger_name_fails_and_is_not_restored(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, sh = rig
    (tmp / "bin" / "age").write_text("#!/bin/sh\n" + REVERSIBLE_AGE, encoding="utf-8", newline="\n")
    genuine = _export(tmp, b"genuine rows\n")
    assert run(_stdin()).returncode == 0
    # someone with the upload key replaces the object: a forged file with a matching sidecar inside the tar and a
    # matching .sha256 beside it, encrypted to the public key (age does not authenticate the sender)
    forged = b"forged rows\n"
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.USTAR_FORMAT) as tf:
        for name, data in ((PART, forged), (PART + ".sha256",
                                            f"{hashlib.sha256(forged).hexdigest()}  event_log_p202607.jsonl.gz\n".encode())):
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    enc = b"age-encryption.org/v1\n-> X25519 stub\n" + buf.getvalue()
    obj = tmp / "b2" / "xtnl-bucket" / "archive" / f"{PART}.{genuine[:16]}.tar.age"
    assert obj.exists()
    obj.write_bytes(enc)
    obj.with_name(obj.name + ".sha256").write_text(f"{hashlib.sha256(enc).hexdigest()}  {obj.name}\n",
                                                   encoding="ascii", newline="\n")
    r, restore = _run_loop(tmp, sh, [(PART, genuine, "dropped")])
    assert "ledger rows: 1, restored: 0, failed: 1" in r.stdout and "FAILED" in r.stdout, r.stdout + r.stderr
    assert not (restore / PART).exists()


def test_a_missing_object_is_a_failure_only_once_its_table_was_dropped(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, sh = rig
    (tmp / "bin" / "age").write_text("#!/bin/sh\n" + REVERSIBLE_AGE, encoding="utf-8", newline="\n")
    have = _export(tmp, b"uploaded rows\n")
    assert run(_stdin()).returncode == 0
    newer = "acs_p202608.jsonl.gz"
    r, _ = _run_loop(tmp, sh, [(PART, have, "dropped"), (f"account_snapshots/{newer}", "ab" * 32, "in-db")])
    assert "ledger rows: 2, restored: 1, failed: 0, missing: 1, unexpected: 0" in r.stdout, r.stdout + r.stderr
    assert "MISSING" in r.stdout
    r, _ = _run_loop(tmp, sh, [(PART, have, "dropped"), (f"account_snapshots/{newer}", "ab" * 32, "dropped")])
    assert "ledger rows: 2, restored: 1, failed: 1, missing: 0, unexpected: 0" in r.stdout, r.stdout + r.stderr


def test_step6_reads_the_ledger_from_the_restored_database() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    step6 = text[text.index("6. **Partition archive files**"):text.index("7. **Start the engine**")]
    assert "FROM partition_archive" in step6 and "checksum" in step6
    assert "for f in $(find . -name '*.tar.age')" not in step6           # never every object
    assert "tar -x -C /restore-archive" not in step6                     # never straight into the archive folder
    flat = _flat(step6)
    for needle in ("Driven by the ledger", "UNEXPECTED", "MISSING", "archive forget", "possible upload-key compromise"):
        assert needle in flat, needle


# ---------------------------------------------------------------------------------------------- docs (F4, F7, F9)
def test_restore_step2_has_the_age_identity_typed_not_pasted() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    step2 = text[text.index("2. **Download, check, decrypt"):text.index("3. **Roles first, then the data.**")]
    flat = _flat(step2)
    assert "paste the AGE-SECRET-KEY" not in flat
    for needle in ("TYPE", "never copy or paste them on this PC", "Switch user", "Set-Clipboard -Value ' '",
                   "Clear all", "Clipboard history", "Sync across your devices", "before step 7 starts the engine"):
        assert needle in flat, needle


def test_the_backup_log_is_a_hint_not_an_independent_record() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    which = text[text.index("**Which backup**"):text.index("Keep this shell open for step 3")]
    bullets = [ln for ln in which.splitlines() if ln.lstrip().startswith("* ")]
    assert bullets and not any("backup log" in b.lower() or "var\\logs" in b for b in bullets), bullets
    flat = _flat(which)
    assert "The backup log is a hint only" in flat and "check C records as good" in flat
    assert "(or the log)" not in flat


def test_setup8_versions_runs_the_protected_job_folder() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    s8 = text[text.index("### 8. First run by hand"):text.index("## Checks")]
    assert ("docker compose -f 'C:\\Program Files\\XTNL\\backup-job\\docker-compose.local.yml' --profile backup run "
            "--rm --no-deps -T backup versions") in s8
    assert "-f deploy\\local\\docker-compose.local.yml" not in s8
