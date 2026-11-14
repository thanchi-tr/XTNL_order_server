"""Entrypoint: `python -m src.main` (--help, config validation, refusal of unwired dependencies)."""
from __future__ import annotations

import subprocess
import sys

import pytest

from src import main as entry
from src.runtime.config import RuntimeConfig

GOOD_ENV = {"XTNL_NODE_ID": "n1", "XTNL_ETCD_ENDPOINTS": "http://a:2379,http://b:2379,http://c:2379",
            "XTNL_DEADMAN_URL": "https://deadman.example/beat", "BROKER_ENV": "sandbox",
            "ORDER_SERVER_DSN": "postgresql://u:p@127.0.0.1:5432/db", "XTNL_USE_MOCK_BROKER": "1"}


def test_help_runs_as_module() -> None:
    out = subprocess.run([sys.executable, "-m", "src.main", "--help"], capture_output=True, text=True, timeout=60)
    assert out.returncode == 0 and "--check-config" in out.stdout


def test_config_from_env_and_overrides() -> None:
    cfg = RuntimeConfig.from_env(GOOD_ENV, etcd_endpoints="http://x,http://y,http://z", node_id=None)
    assert cfg.node_id == "n1" and cfg.etcd_endpoints == ("http://x", "http://y", "http://z")
    assert cfg.problems() == []
    bad = RuntimeConfig.from_env({"XTNL_NODE_ID": "n1"})
    assert len(bad.problems()) == 2 and str(bad.spill_path).endswith("dlq_spill.jsonl")


def test_check_config(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in list(GOOD_ENV) + ["XTNL_SPILL_PATH", "XTNL_LOG_LEVEL"]:
        monkeypatch.delenv(k, raising=False)
    assert entry.main(["--check-config"]) == entry.EX_CONFIG
    for k, v in GOOD_ENV.items():
        monkeypatch.setenv(k, v)
    assert entry.main(["--check-config"]) == 0


def test_refuses_unknown_broker_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in GOOD_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("BROKER_ENV", "live")
    assert entry.main(["--check-config"]) == entry.EX_CONFIG


def test_engine_without_storage_config_refuses_to_start(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in GOOD_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("ORDER_SERVER_DSN")
    assert entry.main([]) == entry.EX_CONFIG


def test_event_loop_factory() -> None:
    kind, factory = entry.event_loop_factory()
    if sys.platform == "win32":
        assert kind == "asyncio" and factory is None
    else:
        assert kind in ("uvloop", "asyncio")


def test_logging_quiets_http_clients_and_redacts_urls() -> None:
    """httpx-info-log-leaks-secret-urls (logging part): httpx / httpcore log full request URLs at INFO
    (the ntfy topic, the dead-man check id, a session token in a query). Before: the root logger was at
    INFO with no filter, so those lines reached the shipped log."""
    import io
    import logging

    import httpx

    root = logging.getLogger("xtnl.test.redact")
    root.propagate = False
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    root.addHandler(handler)
    saved = {n: logging.getLogger(n).level for n in entry.QUIET_HTTP_LOGGERS}
    try:
        entry.configure_logging("INFO", root=root)
        for n in entry.QUIET_HTTP_LOGGERS:
            assert logging.getLogger(n).getEffectiveLevel() >= logging.WARNING
        assert not logging.getLogger("httpx").isEnabledFor(logging.INFO)
        transport = httpx.MockTransport(lambda request: httpx.Response(200))
        with httpx.Client(transport=transport) as client:     # no network: MockTransport
            client.post("https://hc.example/ping/CHECK-UUID-SECRET?session=TOKEN-SECRET")
        root.warning("POST %s failed", "https://ntfy.example/TOPIC-SECRET?auth=x")
        root.warning("dsn https://user:PASS-SECRET@host.example:8443/p and http://h2.example")
        try:
            raise RuntimeError("for url 'https://api.example/TradingApi/session/deleteSession?session=SESS'")
        except RuntimeError:
            root.exception("call failed")
        out = stream.getvalue()
    finally:
        root.removeHandler(handler)
        for n, lvl in saved.items():
            logging.getLogger(n).setLevel(lvl)
    for secret in ("CHECK-UUID-SECRET", "TOKEN-SECRET", "TOPIC-SECRET", "PASS-SECRET", "deleteSession", "SESS'"):
        assert secret not in out
    assert "https://ntfy.example" in out and "https://host.example:8443" in out and "http://h2.example" in out
    assert "https://api.example" in out and "call failed" in out


def test_redact_urls() -> None:
    assert entry.redact_urls("GET https://a.example/x/y?z=1#f done") == "GET https://a.example/... done"
    assert entry.redact_urls("no url here") == "no url here"
    assert entry.redact_urls("http://h:80") == "http://h:80"


def test_redact_urls_ipv6_and_brackets_in_the_path() -> None:
    """Skeptic review: an IPv6 literal host did not match at all, and a "(" or "[" in the path ended the
    match, so the query after it (a session token, a check id) reached the log."""
    assert entry.redact_urls("POST http://[fd00::5]:8080/ping/CHECK-ID?session=TOK") == "POST http://[fd00::5]:8080/..."
    assert entry.redact_urls("GET https://h.example/a(b)/x?session=TOK end") == "GET https://h.example/... end"
    assert entry.redact_urls("GET https://h.example/a[1]?session=TOK") == "GET https://h.example/..."
    assert entry.redact_urls("https://user:PW@[::1]:9/x") == "https://[::1]:9/..."
    for s in ("http://[fd00::5]:8080/ping/CHECK-ID?session=TOK", "https://h.example/a(b)/x?session=TOK"):
        out = entry.redact_urls(s)
        assert "TOK" not in out and "CHECK-ID" not in out
