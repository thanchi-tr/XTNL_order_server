"""IMPLEMENTATION_PLAN 8.2: the dashboard's `--check-config` (the unit's ExecStartPre, run as `xtnl`) loads the
heartbeat listener's TLS certificate and key, so a file the unit cannot read is exit 78 at install time instead of
a crash loop at start; and the shipped heartbeat.json example is what the listener accepts (TLS on 8444)."""
from __future__ import annotations

import json
import ssl
from pathlib import Path
from typing import Any

import pytest

import src.dashboard.__main__ as entry
from src.dashboard import heartbeat as H

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "deploy" / "env" / "dashboard-heartbeat.json.example"
TOKEN = "copy-token-0123456789abcdef"


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(entry, "configure_logging", lambda *a, **k: "text")
    monkeypatch.setenv("XTNL_DASHBOARD_DSN", "postgresql://d:pw@h/x")


def rendered(token: str = TOKEN, **over: Any) -> dict[str, Any]:
    doc: dict[str, Any] = json.loads(EXAMPLE.read_text(encoding="utf-8").replace("${DASHBOARD_VPN_IP}", "10.8.0.6"))
    doc["token"] = token
    doc.update(over)
    return doc


def test_the_example_is_a_tls_listener_on_8444_and_its_placeholder_token_is_refused() -> None:
    s = H.HeartbeatListenSettings.parse(rendered())
    assert (s.host, s.port, s.path, s.tls) == ("10.8.0.6", 8444, "/xtnl/heartbeat", True)
    assert (s.tls_cert, s.tls_key) == ("/etc/xtnl/dashboard/heartbeat.crt", "/etc/xtnl/dashboard/heartbeat.key")
    with pytest.raises(H.HeartbeatConfigError, match="token"):
        H.HeartbeatListenSettings.parse(json.loads(EXAMPLE.read_text(encoding="utf-8")))   # CHANGE_ME as shipped


def test_check_config_refuses_tls_files_it_cannot_load(tmp_path: Path, env: None) -> None:
    f = tmp_path / "heartbeat.json"
    f.write_text(json.dumps(rendered(tls_cert=str(tmp_path / "absent.crt"), tls_key=str(tmp_path / "absent.key"))))
    assert entry.main(["--check-config", "--heartbeat-file", str(f)]) == 78
    (tmp_path / "bad.crt").write_text("not a certificate")
    (tmp_path / "bad.key").write_text("not a key")
    f.write_text(json.dumps(rendered(tls_cert=str(tmp_path / "bad.crt"), tls_key=str(tmp_path / "bad.key"))))
    assert entry.main(["--check-config", "--heartbeat-file", str(f)]) == 78
    with pytest.raises(H.HeartbeatConfigError, match="tls_cert / tls_key") as err:
        H.HeartbeatListenSettings.from_file(f).check_files()  # type: ignore[union-attr]
    assert "not a" not in str(err.value) and TOKEN not in str(err.value)


def test_check_config_passes_when_the_tls_files_load(tmp_path: Path, env: None,
                                                     monkeypatch: pytest.MonkeyPatch) -> None:
    loaded: list[tuple[str, str]] = []

    def fake(cert: str, key: str) -> ssl.SSLContext:
        loaded.append((cert, key))
        return ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

    monkeypatch.setattr(H, "server_tls_context", fake)
    f = tmp_path / "heartbeat.json"
    f.write_text(json.dumps(rendered()))
    assert entry.main(["--check-config", "--heartbeat-file", str(f)]) == 0
    assert loaded == [("/etc/xtnl/dashboard/heartbeat.crt", "/etc/xtnl/dashboard/heartbeat.key")]
    # A loopback listener without TLS loads nothing; an absent file is no listener (both still exit 0).
    loaded.clear()
    f.write_text(json.dumps({"listen": "127.0.0.1:8444", "token": TOKEN}))
    assert entry.main(["--check-config", "--heartbeat-file", str(f)]) == 0 and loaded == []
    assert entry.main(["--check-config", "--heartbeat-file", str(tmp_path / "absent.json")]) == 0
