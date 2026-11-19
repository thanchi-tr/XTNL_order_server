"""Plan S38f (round 14, fix 1): migrations above `XTNL_MIGRATIONS_ACCEPTED_UPTO` (default 14) are held back, named in one
line, and the job exits 0, so a start that runs `python -m src.db.migrate` never puts an unreviewed migration live.
The PC start script and the AWS unit pass the setting through; the env examples document it."""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.db import migrate as M

from tests.db.conftest import FakeConn

ROOT = Path(__file__).resolve().parents[2]
CHK = M.ValidateCheck(16, "notification_outbox", "notification_outbox_arm_channels_check")
VALIDATE = "ALTER TABLE notification_outbox VALIDATE CONSTRAINT notification_outbox_arm_channels_check"


def write(d: Path, name: str, sql: str) -> None:
    (d / name).write_text(sql, encoding="utf-8")


@pytest.mark.parametrize(("raw", "upto", "problem"), [
    (None, 14, False), ("", 14, False), ("  ", 14, False), ("14", 14, False), ("20", 20, False), (" 17 ", 17, False),
    ("0", 0, False), ("-1", 14, True), ("15.0", 14, True), ("twenty", 14, True), ("１５", 14, True),
    ("99999", 14, True),
])
def test_accepted_level_parse_fails_closed(raw: str | None, upto: int, problem: bool) -> None:
    env = {} if raw is None else {M.ACCEPTED_UPTO_VAR: raw}
    got, why = M.accepted_upto_from_env(env)
    assert got == upto and (why is not None) is problem
    assert M.DEFAULT_ACCEPTED_UPTO == 14
    if why is not None:
        assert M.ACCEPTED_UPTO_VAR in why and (raw or "x").strip() not in why.replace(M.ACCEPTED_UPTO_VAR, "")


async def test_migrations_above_the_level_are_held_back_and_named(tmp_path: Path, conn: FakeConn) -> None:
    for v in (14, 15, 16):
        write(tmp_path, f"{v:04d}_m{v}.sql", f"SELECT {v};")
    held: list[M.Migration] = []
    assert await M.migrate(conn, tmp_path, accepted_upto=14, held=held, post=()) == [14]
    assert [m.version for m in held] == [15, 16]
    assert "SELECT 15;" not in conn.sql and "SELECT 16;" not in conn.sql
    line = M.held_line(held, 14)
    assert line.startswith("held back above XTNL_MIGRATIONS_ACCEPTED_UPTO=14") and "0015_m15, 0016_m16" in line


async def test_raising_the_level_applies_the_next_ones_in_order(tmp_path: Path, conn: FakeConn) -> None:
    for v in (14, 15, 16):
        write(tmp_path, f"{v:04d}_m{v}.sql", f"SELECT {v};")
    first = M.discover(tmp_path)[0]
    conn.returns_all(M.SQL_APPLIED, [(14, first.checksum)])
    held: list[M.Migration] = []
    assert await M.migrate(conn, tmp_path, accepted_upto=16, held=held, post=()) == [15, 16]
    assert held == []


async def test_no_level_applies_everything(tmp_path: Path, conn: FakeConn) -> None:
    """`migrate()` without `accepted_upto` (the tests' own schema setup) applies the whole set."""
    for v in (14, 15):
        write(tmp_path, f"{v:04d}_m{v}.sql", f"SELECT {v};")
    assert await M.migrate(conn, tmp_path, post=()) == [14, 15]


async def test_the_post_step_never_runs_for_a_held_back_migration(tmp_path: Path, conn: FakeConn) -> None:
    """0016's CHECK does not exist while 0016 is held back: its VALIDATE must not run (the post step would raise
    'constraint is missing' and fail the start)."""
    write(tmp_path, "0014_a.sql", "SELECT 14;")
    write(tmp_path, "0016_x.sql", "SELECT 16;")
    held: list[M.Migration] = []
    assert await M.migrate(conn, tmp_path, accepted_upto=14, held=held, post=(CHK,)) == [14]
    assert M.SQL_CHECK_VALIDATED not in conn.sql and VALIDATE not in conn.sql
    again = FakeConn()               # once applied (an earlier run), the post step runs for it again
    again.returns_all(M.SQL_APPLIED, [(m.version, m.checksum) for m in M.discover(tmp_path)])
    again.returns(M.SQL_CHECK_VALIDATED, (True,))
    assert await M.migrate(again, tmp_path, accepted_upto=14, post=(CHK,)) == []
    assert M.SQL_CHECK_VALIDATED in again.sql


async def test_a_level_below_the_applied_set_changes_nothing(tmp_path: Path, conn: FakeConn) -> None:
    write(tmp_path, "0014_a.sql", "SELECT 14;")
    write(tmp_path, "0015_b.sql", "SELECT 15;")
    conn.returns_all(M.SQL_APPLIED, [(m.version, m.checksum) for m in M.discover(tmp_path)])
    held: list[M.Migration] = []
    assert await M.migrate(conn, tmp_path, accepted_upto=10, held=held, post=()) == [] and held == []


def test_the_shipped_set_holds_back_0015_and_later_by_default() -> None:
    versions = [m.version for m in M.discover()]
    assert max(versions) >= 20 and [v for v in versions if v > M.DEFAULT_ACCEPTED_UPTO][:1] == [15]


def test_entry_point_reads_the_setting_and_names_the_held_migrations() -> None:
    src = (ROOT / "src/db/migrate.py").read_text(encoding="utf-8")
    main = src[src.index("async def _main"):]
    assert "accepted_upto_from_env(os.environ)" in main and "accepted_upto=upto, held=held" in main
    assert "print(held_line(held, upto))" in main and "return 0" in main      # held back is not a failure


def test_start_script_and_units_pass_the_setting_through() -> None:
    ps = (ROOT / "deploy/local/start-xtnl.ps1").read_text(encoding="utf-8")
    assert "XTNL_MIGRATIONS_ACCEPTED_UPTO" in ps and "@('-m', 'src.db.migrate')" in ps
    # the env file and the Windows User environment are loaded into the process before step 6 (the child inherits)
    assert ps.index("SetEnvironmentVariable($k, $fileVars[$k], 'Process')") < ps.index("# 6. migrations")
    unit = (ROOT / "deploy/systemd/xtnl-migrate.service").read_text(encoding="utf-8")
    assert "EnvironmentFile=/etc/xtnl/engine.env" in unit and "XTNL_MIGRATIONS_ACCEPTED_UPTO" in unit
    for name in ("engine.env.example", "local-live.env.example", "local.env.example"):
        text = (ROOT / "deploy/env" / name).read_text(encoding="utf-8")
        assert re.search(r"^XTNL_MIGRATIONS_ACCEPTED_UPTO=14\s*$", text, re.M), name
        assert re.search(r"^XTNL_OVERSEE_STOP_AT_ENTRY=0\s*$", text, re.M), name
