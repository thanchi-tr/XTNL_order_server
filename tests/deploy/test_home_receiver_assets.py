"""W6 (home-server-plan §3.1 / §6): the dead-man receiver on a small VPS over Tailscale.

The home unit is a plain copy of the Melbourne unit without LoadCredential= and the TLS lines; a 1-minute timer
checks /healthz and pings Healthchecks.io check B (or its /fail URL) from a 0600 env file it never logs. Static
checks in the style of test_deploy_assets.py, plus one run of the watchdog's shell with a stub curl.
"""
from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

from src.deadman_receiver.settings import DEFAULT_LISTEN, ReceiverSettings
from tests.deploy.test_deploy_assets import read_unit
from tests.deploy.test_observability import _posix_bash

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy"
RUNBOOKS = ROOT / "docs" / "runbooks"
HOME_UNIT = "xtnl-deadman-receiver.home.service"
HEALTHZ_UNIT = "xtnl-deadman-healthz.service"
TLS_ENV = {"XTNL_RECEIVER_TLS_CERT=%d/tls.crt", "XTNL_RECEIVER_TLS_KEY=%d/tls.key"}


def _code(path: Path) -> str:
    return "\n".join(ln for ln in path.read_text(encoding="utf-8").splitlines() if not ln.lstrip().startswith("#"))


def _assignments(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            out[k.strip()] = v.strip()
    return out


# ---------------------------------------------------------------------------------------------- home unit
def test_home_unit_is_the_melbourne_unit_without_credentials_and_tls() -> None:
    _, melbourne = read_unit("xtnl-deadman-receiver.service")
    _, home = read_unit(HOME_UNIT)
    expected = {k: list(v) for k, v in melbourne["Service"].items() if k != "LoadCredential"}
    expected["Environment"] = [v for v in expected["Environment"] if v not in TLS_ENV]
    assert home["Service"] == expected
    assert home["Install"] == melbourne["Install"]
    assert home["Unit"]["After"] == melbourne["Unit"]["After"] and home["Unit"]["Wants"] == melbourne["Unit"]["Wants"]


def test_home_unit_keeps_the_hardening_and_the_receiver_command() -> None:
    cp, _ = read_unit(HOME_UNIT)
    svc = cp["Service"]
    assert svc["DynamicUser"] == "yes" and svc["MemoryMax"] == "256M"
    assert svc["ExecStart"].endswith("python -m src.deadman_receiver")
    assert svc["ExecStartPre"].endswith("python -m src.deadman_receiver --check-config")
    assert svc["RestartPreventExitStatus"] == "78" and svc["Restart"] == "always"
    assert svc["EnvironmentFile"] == "/etc/xtnl/deadman-receiver.env"
    for key, value in (("NoNewPrivileges", "yes"), ("ProtectSystem", "strict"), ("ProtectHome", "yes"),
                       ("PrivateTmp", "yes"), ("RestrictSUIDSGID", "yes"), ("UMask", "0077")):
        assert svc[key] == value, key
    assert svc["CapabilityBoundingSet"] == ""
    code = _code(DEPLOY / "systemd" / HOME_UNIT)
    assert "LoadCredential" not in code and "TLS_CERT" not in code and "TLS_KEY" not in code
    assert "deadman-receiver-home.md" in cp["Unit"]["Documentation"]
    assert "systemd-analyze verify" in (DEPLOY / "systemd" / HOME_UNIT).read_text(encoding="utf-8")


def test_loopback_listener_needs_no_tls_files_and_blank_tls_counts_as_unset() -> None:
    assert DEFAULT_LISTEN == "127.0.0.1:8443"
    s = ReceiverSettings.from_env({"XTNL_RECEIVER_TOKEN": "home-token-0123456789abcdef",
                                   "XTNL_RECEIVER_LISTEN": "127.0.0.1:8443", "XTNL_RECEIVER_TLS_CERT": "",
                                   "XTNL_RECEIVER_TLS_KEY": " ", "XTNL_RECEIVER_CERTS": "",
                                   "XTNL_RECEIVER_PUSH_PROVIDER": "pushover",
                                   "XTNL_RECEIVER_PUSH_TOKEN": "app-token-placeholder",
                                   "XTNL_RECEIVER_PUSH_USER": "user-key-placeholder"})
    assert (s.host, s.port, s.tls, s.certs) == ("127.0.0.1", 8443, False, ())


def test_receiver_env_example_documents_the_home_block() -> None:
    text = (DEPLOY / "env" / "deadman-receiver.env.example").read_text(encoding="utf-8")
    block = text[text.index("# Home VPS"):text.index("# --- service")]
    for needle in ("XTNL_RECEIVER_LISTEN=127.0.0.1:8443", "tailscale serve", "XTNL_RECEIVER_TLS_CERT=",
                   "XTNL_SECRETS_SOURCE=env", "XTNL_RECEIVER_PUSH_", "XTNL_RECEIVER_SMTP_", "deadman-receiver-home.md",
                   HOME_UNIT):
        assert needle in block, needle
    assert _assignments(DEPLOY / "env" / "deadman-receiver.env.example")["XTNL_RECEIVER_TWO_ENGINES_WINDOW_S"] == "60"


# ---------------------------------------------------------------------------------------------- healthz timer
def test_healthz_service_reads_its_secret_without_logging_it() -> None:
    cp, _ = read_unit(HEALTHZ_UNIT)
    svc = cp["Service"]
    assert svc["Type"] == "oneshot" and svc["DynamicUser"] == "yes"
    assert svc["EnvironmentFile"] == "/etc/xtnl/deadman-healthz.env"
    run = svc["ExecStart"]
    assert run.startswith("/bin/sh -c '") and run.endswith("'")
    # systemd expands a single $ itself (and may log the result): the shell must see the variable unexpanded
    assert re.findall(r"(?<!\$)\$\{?XTNL_", run) == []
    assert "$${XTNL_HEALTHZ_PING_URL}" in run and "$${XTNL_HEALTHZ_PING_URL}/fail" in run
    # review round 1: the ping URL reaches curl only as a config line on its stdin, never as an argument
    for call in re.findall(r"curl [^;|]*", run):
        assert "XTNL_HEALTHZ_PING_URL" not in call, call
    assert run.count("| curl -fs --max-time 10 -o /dev/null -K -") == 2
    assert 'echo "url = $${XTNL_HEALTHZ_PING_URL}" | curl' in run
    assert 'echo "url = $${XTNL_HEALTHZ_PING_URL}/fail" | curl' in run
    assert f"http://{DEFAULT_LISTEN}/healthz" in run
    for call in re.findall(r"curl [^;|]*", run):
        assert "--max-time" in call and " -o /dev/null" in call
        assert not re.search(r"\s-[a-zA-Z]*[Sv]", call), "curl must not print errors or the URL"
    # an echo of the URL only ever feeds curl's stdin (never the journal)
    for m in re.finditer(r"echo [^;|]*\$\$\{XTNL_HEALTHZ_PING_URL[^;|]*(\|)?", run):
        assert m.group(1) == "|", m.group(0)
    assert "%" not in run                                     # no systemd specifier
    for key, value in (("NoNewPrivileges", "yes"), ("ProtectSystem", "strict"), ("ProtectHome", "yes"),
                       ("PrivateTmp", "yes"), ("UMask", "0077")):
        assert svc[key] == value, key
    assert svc["CapabilityBoundingSet"] == ""


def test_healthz_timer_runs_every_minute() -> None:
    cp, _ = read_unit("xtnl-deadman-healthz.timer")
    assert cp["Timer"]["OnCalendar"] == "minutely" and cp["Install"]["WantedBy"] == "timers.target"
    assert "OnUnitInactiveSec" not in cp["Timer"]          # a failed run must not stop the next one


def test_healthz_env_example_is_a_placeholder_0600_file() -> None:
    path = DEPLOY / "env" / "deadman-healthz.env.example"
    text = path.read_text(encoding="utf-8")
    assert _assignments(path) == {"XTNL_HEALTHZ_PING_URL": "CHANGE_ME"}
    assert "0600 root:root" in text and "/etc/xtnl/deadman-healthz.env" in text
    assert "XTNL_HEALTHZ_URL=http://127.0.0.1:8443/healthz" in text           # documented, commented default
    unit = (DEPLOY / "systemd" / HEALTHZ_UNIT).read_text(encoding="utf-8")
    for name in set(re.findall(r"\$\$\{(XTNL_[A-Z_]+)", unit)):
        assert name in text, name


def _run_healthz(tmp_path: Path, healthz_ok: bool, ping_url: str) -> tuple[int, str, str]:
    bash = _posix_bash()
    if bash is None:
        pytest.skip("no POSIX bash to run the watchdog's shell")
    cp, _ = read_unit(HEALTHZ_UNIT)
    script = cp["Service"]["ExecStart"].removeprefix("/bin/sh -c '").removesuffix("'").replace("$$", "$")
    stubs = tmp_path / "stubs"
    stubs.mkdir(parents=True)
    curl = stubs / "curl"
    curl.write_bytes(('#!/usr/bin/env bash\n'
                      'for a in "$@"; do case "$a" in *hc-ping*) echo ARGV-LEAK >>"$STUB_LOG";; esac; done\n'
                      'url="${@: -1}"\nif [ "$url" = "-" ]; then url=$(sed -n "s/^url = //p"); fi\n'
                      'echo "$url" >>"$STUB_LOG"\n'
                      'case "$url" in *healthz) [ "$STUB_HEALTHZ" = 1 ];; *) exit 0;; esac\n').encode("utf-8"))
    curl.chmod(0o755)
    (tmp_path / "script.sh").write_bytes(script.encode("utf-8"))
    log = tmp_path / "calls.log"
    cmd = ('to_posix() { if command -v cygpath >/dev/null 2>&1; then cygpath -u "$1"; else echo "$1"; fi; }; '
           'export PATH="$(to_posix "$STUB_DIR"):$PATH"; export STUB_LOG="$(to_posix "$LOG_WIN")"; '
           'sh "$(to_posix "$SCRIPT_WIN")"')
    env = {"PATH": os.environ.get("PATH", ""), "STUB_DIR": str(stubs), "LOG_WIN": str(log),
           "SCRIPT_WIN": str(tmp_path / "script.sh"), "STUB_HEALTHZ": "1" if healthz_ok else "0",
           "XTNL_HEALTHZ_PING_URL": ping_url}
    for k in ("SYSTEMROOT", "TEMP", "TMP", "HOME", "USERPROFILE"):
        if k in os.environ:
            env[k] = os.environ[k]
    out = subprocess.run([bash, "-c", cmd], env=env, capture_output=True, text=True, timeout=60)
    return out.returncode, out.stdout + out.stderr, log.read_text(encoding="utf-8") if log.exists() else ""


def test_healthz_pings_check_b_or_its_fail_url_and_never_prints_the_url(tmp_path: Path) -> None:
    secret = "https://hc-ping.example.invalid/00000000-secret"
    rc, output, calls = _run_healthz(tmp_path / "ok", True, secret)
    assert rc == 0 and calls.split() == ["http://127.0.0.1:8443/healthz", secret]
    assert "secret" not in output
    rc, output, calls = _run_healthz(tmp_path / "down", False, secret)
    assert rc == 1 and calls.split() == ["http://127.0.0.1:8443/healthz", secret + "/fail"]
    assert "FAILED" in output and "secret" not in output
    rc, output, calls = _run_healthz(tmp_path / "unset", True, "")
    assert rc == 78 and calls == "" and "not set" in output
    for i, bad in enumerate(("http://hc-ping.example.invalid/x", 'https://hc-ping.example.invalid/x" -K other')):
        rc, output, calls = _run_healthz(tmp_path / f"bad{i}", True, bad)
        assert rc == 78 and calls == "" and "hc-ping" not in output


# ---------------------------------------------------------------------------------------------- runbook
def test_home_runbook_shape_and_content() -> None:
    text = (RUNBOOKS / "deadman-receiver-home.md").read_text(encoding="utf-8")
    assert "never sends orders" in text and "```" in text
    headings = {h.strip().lower() for h in re.findall(r"^##\s+(.+)$", text, re.M)}
    assert any(h.startswith("trigger") for h in headings)
    assert any(h.startswith(("check", "prerequisites")) or "checks" in h for h in headings)
    assert any(h.startswith("rollback") for h in headings)
    for needle in ("Ubuntu 24.04", "Debian 13", "Python 3.12", "chrony", "MagicDNS", "HTTPS certificates",
                   "neutral", "Certificate Transparency", "tailscale serve", "127.0.0.1:8443", "Tailscale SSH",
                   "Pushover", "SMTP", "XTNL_RECEIVER_TOKEN_NEXT", "stop-xtnl.ps1", "Start-ScheduledTask 'XTNL engine'",
                   "systemd-analyze verify", HOME_UNIT, HEALTHZ_UNIT, "xtnl-deadman-healthz.timer",
                   "deadman-healthz.env", "check B", "/fail", "XTNL_HEARTBEAT_COPY_URL", "XTNL_DEADMAN_URL",
                   "two_engines", "node_changed", "Unverified"):
        assert needle in text, needle
    # rotation: receiver next token first, engine second, promotion last, each receiver step with a restart
    rot = text[text.index("## Token rotation"):]
    rot = rot[rot.index("\n1. "):rot.index("\n## ", 4)]                       # the numbered steps
    order = [rot.index(s) for s in ("XTNL_RECEIVER_TOKEN_NEXT", "XTNL_DEADMAN_TOKEN", "stop-xtnl.ps1",
                                    "Start-ScheduledTask 'XTNL engine'", "Promote")]
    assert order == sorted(order)
    assert rot.count("systemctl restart xtnl-deadman-receiver") >= 2
    assert "AUD" not in text and not re.search(r"\$\s?\d", text)


def test_deadman_alert_links_the_home_runbook_and_documents_w19() -> None:
    text = (RUNBOOKS / "deadman-alert.md").read_text(encoding="utf-8")
    assert "](deadman-receiver-home.md)" in text
    for needle in ("`two_engines`", "`node_changed`", "XTNL_RECEIVER_TWO_ENGINES_WINDOW_S", "two engines beating",
                   "beating node changed"):
        assert needle in text, needle
