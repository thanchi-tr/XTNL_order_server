"""`python -m src.ops uptime show` / `catchup show` (migration 0012): read-only, counts and ids only, nothing audited
or written. No network: FakeConn answers the exact SQL constants."""
from __future__ import annotations

import io
import re

from src.db.pg import PgConnection
from src.ops import cli
from src.ops import sql as S
from src.ops.service import OpsService
from tests.db.conftest import FakeConn
from tests.ops.test_cli import ENV, SECRET

T = 1_700_000_000_000
R1, R2, R3 = "11111111-0000-4000-8000-000000000001", "22222222-0000-4000-8000-000000000002", \
    "33333333-0000-4000-8000-000000000003"


def seeded() -> FakeConn:
    conn = FakeConn()
    conn.returns_all(S.SQL_UPTIME_RUNS, [
        (R3, "pc-1", T + 7_200_000, T + 8_100_000, None, None, 1),                     # current run, one sleep
        (R2, "pc-1", T + 3_600_000, T + 3_700_000, None, None, 0),                     # killed: no clean stop
        (R1, "pc-1", T, T + 1_800_000, T + 1_900_000, "SIGINT", 0),
    ])
    conn.returns_all(S.SQL_UPTIME_COVERAGE, [("stonex", T + 8_100_000, R3)])
    conn.returns_all(S.SQL_CATCHUP_RUNS, [
        (2, R3, 7, "stonex", "startup", T + 2_800_000, T + 7_200_000, T + 3_000_000, T + 3_700_000,
         T + 7_200_000, T + 7_210_000, 4, 2, 1, 1, 0, 1, 1, 0, False, "broker history retention starts after the gap"),
    ])
    return conn


async def test_uptime_show_lists_runs_offline_gaps_and_coverage() -> None:
    rep = await OpsService(seeded()).uptime_show(limit=20)  # type: ignore[arg-type]
    assert [r["state"] for r in rep.rows] == ["running or no clean stop", "no clean stop", "stopped"]
    assert rep.rows[0]["suspended_gaps"] == 1 and rep.rows[2]["reason"] == "SIGINT"
    assert rep.rows[0]["up"] == "15m 0s" and rep.rows[2]["up"] == "31m 40s"
    gaps = rep.sections["offline gaps"]
    assert [g["offline"] for g in gaps] == ["58m 20s", "28m 20s"]       # from the last proof of life to the next start
    assert rep.sections["adapter coverage"] == [{"adapter": "stonex", "last_streaming": "2023-11-15T00:28:20.000Z",
                                                 "run_id": R3}]


async def test_catchup_show_lists_windows_counts_and_warns_on_incomplete() -> None:
    conn = seeded()
    rep = await OpsService(conn).catchup_show(limit=5, adapter="stonex")  # type: ignore[arg-type]
    (row,) = rep.rows
    assert row["window"].startswith("2023-11-14T23:00:00.000Z..")
    assert (row["orders_new"], row["orders_updated"], row["positions_closed"], row["complete"]) == (2, 1, 1, False)
    assert rep.warnings and "orders may be missing" in rep.warnings[0]
    assert conn.args_of(S.SQL_CATCHUP_RUNS) == [("stonex", 5)]


def test_both_commands_are_read_only_sql() -> None:
    for sql in (S.SQL_UPTIME_RUNS, S.SQL_UPTIME_COVERAGE, S.SQL_CATCHUP_RUNS):
        assert sql.lstrip().upper().startswith("SELECT") and not re.search(r"\b(INSERT|UPDATE|DELETE)\b", sql)
        assert not re.search(r"amount|balance|equity", sql)


def run(*argv: str, conn: FakeConn) -> tuple[int, str, str]:
    async def connect(dsn: str) -> tuple[PgConnection, cli.Closer]:
        async def close() -> None:
            return None
        return conn, close  # type: ignore[return-value]

    out, err = io.StringIO(), io.StringIO()
    code = cli.main(list(argv), environ=ENV, connect=connect, etcd_factory=lambda e: None, out=out, err=err)
    return code, out.getvalue(), err.getvalue()


def test_cli_end_to_end_and_limits() -> None:
    conn = seeded()
    code, out, err = run("uptime", "show", "--limit", "3", conn=conn)
    assert code == cli.EXIT_OK, err
    assert "command: uptime.show" in out and "[offline gaps] 2 row(s)" in out and "state=no clean stop" in out
    assert conn.args_of(S.SQL_UPTIME_RUNS) == [(None, 3)]
    code, out, _ = run("catchup", "show", conn=seeded())
    assert code == cli.EXIT_OK and "complete=False" in out and "warning:" in out
    assert SECRET not in out
    code, _, err = run("catchup", "show", "--limit", "0", conn=seeded())
    assert code == cli.EXIT_USAGE and "--limit" in err
    # nothing is ever written or audited by these commands
    assert not [s for s in conn.sql if re.search(r"\b(INSERT|UPDATE|DELETE)\b", s)]
