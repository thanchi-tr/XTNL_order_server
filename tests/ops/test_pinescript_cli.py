"""`python -m src.ops pinescript` with an injected read-only fake connection: parsing, the read-only
transaction, files written, and a summary of file names and counts only."""
from __future__ import annotations

import io
import re
from pathlib import Path

from src.db.pg import PgConnection, Row
from src.ops import cli
from src.reports import lifecycle as lc_mod

from tests.reports.test_pinescript import privacy_rows

ENV = {"ORDER_SERVER_DSN": "postgresql://xtnl_operator:s3cret-pw@127.0.0.1:5432/xtnl"}


class ReadOnlyConn:
    """Answers the export's three SELECTs; refuses anything that writes."""

    def __init__(self, log: list[Row]) -> None:
        self.log = log
        self.statements: list[str] = []
        self.args: list[tuple[object, ...]] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.statements.append(sql)
        assert not re.match(r"(?i)\s*(insert|update|delete|truncate|create|alter|drop)", sql), sql
        return "OK"

    async def fetchrow(self, sql: str, *args: object) -> Row | None:
        raise AssertionError("not used")

    async def fetch(self, sql: str, *args: object) -> list[Row]:
        self.statements.append(sql)
        self.args.append(args)
        if sql == lc_mod.SQL_LOG_VERSIONS:
            return list(self.log)
        return []


def run(conn: ReadOnlyConn, *argv: str) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    closed: list[int] = []

    async def connect(dsn: str) -> tuple[PgConnection, cli.Closer]:
        async def close() -> None:
            closed.append(1)
        return conn, close

    def no_etcd(endpoints: object) -> None:
        raise AssertionError("pinescript needs no etcd")

    code = cli.main(["pinescript", *argv], environ=ENV, connect=connect, etcd_factory=no_etcd,  # type: ignore[arg-type]
                    out=out, err=err)
    if code == 0:
        assert closed == [1]
    return code, out.getvalue(), err.getvalue()


def test_export_writes_files_and_prints_names_and_counts(tmp_path: Path) -> None:
    conn = ReadOnlyConn(privacy_rows())
    code, out, err = run(conn, "--days", "7", "--out", str(tmp_path), "--adapter", "stonex")
    assert code == 0 and err == ""
    assert conn.statements[:3] == ["BEGIN ISOLATION LEVEL REPEATABLE READ", "SET TRANSACTION READ ONLY",
                                   "SET LOCAL statement_timeout = '60s'"]
    assert conn.statements[-1] == "COMMIT"
    assert conn.args[0][2] == "stonex"                                   # the adapter filter reaches the SQL
    files = sorted(p.name for p in tmp_path.iterdir())
    assert len(files) == 1 and re.fullmatch(r"EURUSD_\d{4}-\d{2}-\d{2}\.pine", files[0])
    lines = out.strip().split("\n")
    assert len(lines) == 1
    assert re.fullmatch(r"\S+EURUSD_\d{4}-\d{2}-\d{2}\.pine orders=1 positions=1 trades=1 pending=0 drawn=1\+0 "
                        r"lines=\d+ labels=\d+ boxes=\d+ left_out=0", lines[0])
    assert "." not in lines[0].split(".pine", 1)[1]                      # no decimal (no price, no amount)
    text = (tmp_path / files[0]).read_text(encoding="utf-8")
    assert text.startswith("//@version=6\n")
    for n in ("123457", "98765", "3.217", "6.543"):
        assert n not in text and n not in out


def test_symbol_filter_and_empty_window(tmp_path: Path) -> None:
    code, out, _ = run(ReadOnlyConn(privacy_rows()), "--symbol", "GBP/USD", "--out", str(tmp_path))
    assert code == 0 and out.strip() == "no order or position versions in the window"
    assert list(tmp_path.iterdir()) == []
    code, out, _ = run(ReadOnlyConn(privacy_rows()), "--symbol", "eur/usd", "--out", str(tmp_path))
    assert code == 0 and "EURUSD_" in out


def test_usage_and_errors(tmp_path: Path) -> None:
    for days in ("0", "91", "x"):
        code, _, err = run(ReadOnlyConn([]), "--days", days, "--out", str(tmp_path))
        assert code == cli.EXIT_USAGE and err.startswith("usage:")

    class Broken(ReadOnlyConn):
        async def fetch(self, sql: str, *args: object) -> list[Row]:
            raise RuntimeError("password=s3cret-pw")

    conn = Broken([])
    code, out, err = run(conn, "--out", str(tmp_path))
    assert code == cli.EXIT_ERROR and "s3cret" not in err and err == "error: RuntimeError\n"
    assert conn.statements[-1] == "ROLLBACK"
    out_err = io.StringIO()
    assert cli.main(["pinescript"], environ={}, out=io.StringIO(), err=out_err) == cli.EXIT_USAGE
    assert "ORDER_SERVER_DSN" in out_err.getvalue()


def test_parser_defaults() -> None:
    ns = cli.build_parser().parse_args(["pinescript"])
    assert (ns.group, ns.cmd, ns.days, ns.symbol, ns.adapter, ns.out) == ("pinescript", "export", 7, None, None,
                                                                          "var/pinescript")
