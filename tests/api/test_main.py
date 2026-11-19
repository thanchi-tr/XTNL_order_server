"""`python -m src.api`: configuration refusals exit 78 without starting; the JSON log formatter keeps extra
fields and drops exception text; a real start / stop on 127.0.0.1 with TLS."""
from __future__ import annotations

import asyncio
import io
import json
import logging
import sys
from pathlib import Path

import pytest

from src.api import __main__ as entry
from src.api.settings import ApiSettings

from tests.api.conftest import SECRET, SECRET_TEXT

TlsFiles = tuple[Path, Path, Path, Path]
DSN = "postgresql://xtnl_api:pw_not_real@127.0.0.1:1/xtnl"


@pytest.mark.parametrize("env", [{}, {"XTNL_API_SECRET": "weak"},
                                 {"XTNL_API_SECRET": SECRET_TEXT},                         # no XTNL_API_DSN
                                 {"XTNL_API_SECRET": SECRET_TEXT, "XTNL_API_DSN": DSN,
                                  "XTNL_API_REDIS_URL": "redis://127.0.0.1:1/2", "XTNL_API_PORT": "x"},
                                 {"XTNL_API_SECRET": SECRET_TEXT, "XTNL_API_DSN": DSN,     # broker credentials
                                  "XTNL_API_REDIS_URL": "redis://127.0.0.1:1/2", "STONE_X_PASS": "weak"}])
def test_configuration_refusals_exit_78(env: dict[str, str], capsys: pytest.CaptureFixture[str]) -> None:
    assert entry.main([], environ=env) == entry.EX_CONFIG
    out = capsys.readouterr().out
    assert "configuration refused" in out and "weak" not in out and SECRET_TEXT not in out


async def test_missing_certificate_exits_78(tmp_path: Path) -> None:
    s = ApiSettings(secret=SECRET, port=0, bind="127.0.0.1", cert_path=tmp_path / "x.crt",
                    key_path=tmp_path / "x.key", dsn=DSN, redis_url="redis://127.0.0.1:1/2")
    assert await entry.run(s) == entry.EX_CONFIG


def test_json_formatter_keeps_extras_and_drops_exception_text() -> None:
    rec = logging.LogRecord("xtnl.api", logging.INFO, __file__, 1, "request", (), None)
    rec.__dict__.update({"event": "request", "status": 200, "latency_ms": 3})
    doc = json.loads(entry.JsonFormatter().format(rec))
    assert doc["event"] == "request" and doc["status"] == 200 and doc["level"] == "info"
    try:
        raise RuntimeError("postgresql://u:hunter2@h/db")
    except RuntimeError:
        rec2 = logging.LogRecord("xtnl.api", logging.ERROR, __file__, 1, "boom", (), sys.exc_info())
    line = entry.JsonFormatter().format(rec2)
    assert "hunter2" not in line and json.loads(line)["exc_type"] == "RuntimeError"


async def test_run_listens_and_stops_on_stdin_eof(tls_files: TlsFiles, monkeypatch: pytest.MonkeyPatch,
                                                  capsys: pytest.CaptureFixture[str]) -> None:
    crt, key, _, _ = tls_files
    entry.configure_logging(io.StringIO())
    stdin = io.BytesIO(b"")                         # EOF at once: stops right after listening

    class FakeStdin:
        buffer = stdin

    monkeypatch.setattr(sys, "stdin", FakeStdin())
    s = ApiSettings(secret=SECRET, port=0, bind="127.0.0.1", cert_path=crt, key_path=key, dsn=DSN,
                    redis_url="redis://127.0.0.1:1/2")
    assert await asyncio.wait_for(entry.run(s, stop_on_stdin_eof=True), 10) == entry.EX_OK
