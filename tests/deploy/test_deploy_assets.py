"""Deployment asset invariants (IMPLEMENTATION_PLAN 4.2; operations.md s1-s3, s12-s14; tier-persistence.md s1).

Stdlib only: configparser for the systemd units, a minimal indentation-based reader for the YAML templates
(PyYAML is not a project dependency), plain text checks for redis.conf. The timing values are checked
against the constants in src/ so a change on either side fails here first.
"""
from __future__ import annotations

import ast
import configparser
import inspect
import re
from pathlib import Path

import pytest

from src.db.pg import pool_kwargs
from src.integrity.worker import DEFAULT_MAX_TXN_MS
from src.notifications.settings import ARM_PREFIX, CHANNEL_VARS, OVERSEE_PREFIX
from src.runtime.leader_init import ABORT_CANCEL_S
from src.runtime.lease import ENDPOINT_FAILOVER_S, LeaseTimings
from src.runtime.supervisor import LoopWatchdog

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy"
RUNBOOKS = ROOT / "docs" / "runbooks"
SRC = ROOT / "src"

ENV_NAME = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$")
# Set by the service manager, documented in the example as a comment only.
SYSTEM_PROVIDED = {"NOTIFY_SOCKET"}
# Being added by the cTrader builder; must be documented even before the code lands.
EXPECTED_CTRADER = {"CTRADER_CLIENT_ID", "CTRADER_CLIENT_SECRET", "CTRADER_ACCESS_TOKEN",
                    "CTRADER_REFRESH_TOKEN", "CTRADER_ACCOUNT_ID", "CTRADER_HOST"}
SECRET_HINT = re.compile(r"(PASS|PASSWORD|SECRET|TOKEN|APP_KEY|ACCESS_KEY)")
# Names that match SECRET_HINT but hold no secret: the secrets-source selector, the secret's name and
# region, and the KMS key id (4.7). Their example values are real-shaped placeholders, not CHANGE_ME.
NOT_SECRET = {"XTNL_SECRETS_SOURCE", "XTNL_SECRETS_ID", "XTNL_SECRETS_REGION", "XTNL_TOKEN_KMS_KEY_ID"}
# Read by the code but set only in the Windows User environment on the PC (deploy/local/new-api-credentials.ps1,
# deploy/local/new-md-credentials.ps1; the secrets source on AWS): the example documents them in a comment and must
# never assign them. The market-data / ARM secrets are design §C2's list (plan S36).
USER_ENVIRONMENT_ONLY = {"XTNL_API_SECRET", "XTNL_API_DSN",
                         "XTNL_MD_REDIS_URL", "XTNL_MD_REDIS_READER_URL", "XTNL_MD_HMAC_KEY", "XTNL_MD_HMAC_KEY_PREV",
                         "XTNL_ARM_CONFIG_DSN",
                         "XTNL_ARM_SMTP_USERNAME", "XTNL_ARM_SMTP_PASSWORD", "XTNL_ARM_SES_ACCESS_KEY_ID",
                         "XTNL_ARM_SES_SECRET_ACCESS_KEY", "XTNL_ARM_SES_SESSION_TOKEN", "XTNL_ARM_PUSH_TOKEN",
                         "XTNL_ARM_PUSH_USER",
                         # the live view's login (role xtnl_liveview; deploy/local/new-liveview-credentials.ps1)
                         "XTNL_LIVEVIEW_DSN"}
# Module-level `*_VAR` / `*_ENV` constants naming an environment variable: the pure settings parsers
# (src/marketdata/config.py, src/riskmgr/config.py, src/armconfig/client.py) take the environment from their caller
# and never touch os.environ themselves, so such a module is scanned too.
#: The ARM channel credentials (design PR4, §C2): the secret Oversee channel names under the prefix XTNL_ARM_.
ARM_CHANNEL_SECRETS = {ARM_PREFIX + v.removeprefix(OVERSEE_PREFIX) for v in CHANNEL_VARS
                       if SECRET_HINT.search(v) or v.endswith(("_USERNAME", "_USER"))}
_ENV_CONST = re.compile(r"^[A-Z][A-Z0-9_]*_(?:VAR|ENV)\s*(?::[^=\n]*)?=\s*\"[A-Z]", re.M)


# ---------------------------------------------------------------------------------------------- helpers
def read_unit(name: str) -> tuple[configparser.RawConfigParser, dict[str, dict[str, list[str]]]]:
    """(configparser view with last-wins values, multi-valued view keeping every repeated key)."""
    path = DEPLOY / "systemd" / name
    text = path.read_text(encoding="utf-8")
    cp = configparser.RawConfigParser(strict=False, interpolation=None, comment_prefixes=("#", ";"),
                                      inline_comment_prefixes=None, delimiters=("=",))
    cp.optionxform = str  # type: ignore[assignment,method-assign]   # systemd keys are case-sensitive
    cp.read_string(text, source=str(path))
    multi: dict[str, dict[str, list[str]]] = {}
    section = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            multi.setdefault(section, {})
            continue
        key, _, value = line.partition("=")
        multi[section].setdefault(key.strip(), []).append(value.strip())
    return cp, multi


def words(values: list[str]) -> set[str]:
    return {w for v in values for w in v.split()}


def seconds(value: str) -> float:
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(ms|s|min)?", value.strip())
    assert m is not None, f"unparseable duration {value!r}"
    n = float(m.group(1))
    return {"ms": n / 1000, "min": n * 60}.get(m.group(2) or "s", n)


def read_yaml(path: Path) -> dict[str, str]:
    """Minimal YAML reader: block mappings -> {'a.b.c': 'scalar'}; list items -> 'a.b[i]'."""
    out: dict[str, str] = {}
    stack: list[tuple[int, str]] = []
    counters: dict[str, int] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        stripped = _strip_comment(raw).rstrip()
        if not stripped.strip():
            continue
        indent = len(stripped) - len(stripped.lstrip(" "))
        body = stripped.strip()
        while stack and stack[-1][0] >= indent:
            stack.pop()
        parent = ".".join(k for _, k in stack)
        if body.startswith("- "):
            i = counters.get(parent, 0)
            counters[parent] = i + 1
            out[f"{parent}[{i}]"] = _unquote(body[2:].strip())
            continue
        key, sep, value = body.partition(":")
        if not sep:
            continue
        full = f"{parent}.{key.strip()}" if parent else key.strip()
        if value.strip():
            out[full] = _unquote(value.strip())
        else:
            stack.append((indent, key.strip()))
    return out


def _strip_comment(line: str) -> str:
    quote: str | None = None
    for i, ch in enumerate(line):
        if quote:
            if ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
        elif ch == "#" and (i == 0 or line[i - 1] in " \t"):
            return line[:i]
    return line


def _unquote(v: str) -> str:
    return v[1:-1] if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"" else v


def env_example() -> tuple[str, dict[str, str]]:
    text = (DEPLOY / "env" / "engine.env.example").read_text(encoding="utf-8")
    assigned: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            assigned[k.strip()] = v.strip()
    return text, assigned


#: Separate processes on other hosts, each with its own env example (6.4: the Melbourne dead-man receiver).
OWN_ENV_EXAMPLE = {"src/deadman_receiver/": DEPLOY / "env" / "deadman-receiver.env.example",
                   # 7.3: the Melbourne dashboard host has its own env file (never the engine's credentials)
                   "src/dashboard/": DEPLOY / "env" / "dashboard.env.example"}


def env_vars_read_by_code(root: Path = SRC) -> dict[str, str]:
    """Every environment variable name the modules under `root` read, found by AST: `<x>.get("NAME")`,
    `getenv("NAME")`, `<x>["NAME"]`, `*_VAR = "NAME"` constants and string tuples, in modules that touch
    os.environ / getenv or define such a `*_VAR` constant (the pure parsers whose caller passes the environment in).
    Modules under `OWN_ENV_EXAMPLE` prefixes are left out when scanning all of src/."""
    found: dict[str, str] = {}
    for path in sorted(root.rglob("*.py")):
        if root == SRC and any(path.relative_to(ROOT).as_posix().startswith(p) for p in OWN_ENV_EXAMPLE):
            continue
        text = path.read_text(encoding="utf-8")
        if "environ" not in text and "getenv" not in text and not _ENV_CONST.search(text):
            continue
        rel = path.relative_to(ROOT).as_posix()
        for node in ast.walk(ast.parse(text, filename=rel)):
            candidates: list[ast.expr] = []
            if isinstance(node, ast.Call) and node.args:
                fn = node.func
                name = fn.attr if isinstance(fn, ast.Attribute) else fn.id if isinstance(fn, ast.Name) else ""
                if name in ("get", "getenv", "pop", "setdefault"):
                    candidates.append(node.args[0])
                elif name in ("env_flag", "tolerant_flag") and len(node.args) >= 2:   # src/config.py (environ, "NAME")
                    candidates.append(node.args[1])
            elif isinstance(node, ast.Subscript):
                candidates.append(node.slice)
            elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                if any(isinstance(t, ast.Name) and t.id.endswith(("_VAR", "_ENV")) for t in targets):
                    if node.value is not None:
                        candidates.append(node.value)
            elif isinstance(node, ast.Tuple):   # ("STONE_X_USERNAME", ...) checked with e.get(k)
                candidates.extend(node.elts)
            for c in candidates:
                if isinstance(c, ast.Constant) and isinstance(c.value, str) and ENV_NAME.match(c.value):
                    found.setdefault(c.value, rel)
    return found


# ---------------------------------------------------------------------------------------------- systemd
def test_engine_unit_supervision() -> None:
    cp, multi = read_unit("xtnl-engine.service")
    svc = cp["Service"]
    assert svc["Type"] == "notify"
    assert svc["NotifyAccess"] == "main"
    assert svc["Restart"] == "always"
    assert seconds(svc["RestartSec"]) > 0
    assert svc["WatchdogSignal"] == "SIGKILL"
    assert svc["KillMode"] == "mixed"
    assert svc["RestartPreventExitStatus"] == "78"          # src/main.py EX_CONFIG
    assert "-m src.main" in svc["ExecStart"]
    assert any("--check-config" in v for v in multi["Service"]["ExecStartPre"])
    assert "StartLimitBurst" in cp["Unit"] and "StartLimitIntervalSec" in cp["Unit"]
    assert "MemoryMax" in svc


def test_engine_unit_timing_matches_the_code() -> None:
    cp, _ = read_unit("xtnl-engine.service")
    svc = cp["Service"]
    timings = LeaseTimings()
    heartbeat = inspect.signature(LoopWatchdog.__init__).parameters["interval_s"].default
    watchdog = seconds(svc["WatchdogSec"])
    assert watchdog == timings.watchdog_s == 3
    assert watchdog >= 3 * heartbeat, "WatchdogSec must allow at least three missed 1 s heartbeats"
    assert watchdog < timings.fence_after_s < timings.ttl_s
    stop = seconds(svc["TimeoutStopSec"])
    assert stop > timings.draining_deadline_s, "TimeoutStopSec must cover DRAINING (10 s)"
    # K15: a stop during the LEADER_INIT step-1 COMMIT (statement_timeout 0) waits for it; a sync-standby stall
    # bounds that at ~20 s (operations.md §3), then the step unwind and DRAINING must still fit before SIGKILL.
    sync_standby_stall_s = 20.0
    assert stop > sync_standby_stall_s + ABORT_CANCEL_S + timings.draining_deadline_s,         "TimeoutStopSec must cover a step-1 commit stall + the step unwind + DRAINING"
    assert seconds(svc["TimeoutStartSec"]) > 0


def test_engine_unit_environment_and_layout() -> None:
    cp, multi = read_unit("xtnl-engine.service")
    svc = multi["Service"]
    assert "/etc/xtnl/engine.env" in svc["EnvironmentFile"]
    assert cp["Service"]["User"] == "xtnl" and cp["Service"]["Group"] == "xtnl"
    rw = words(svc["ReadWritePaths"])
    assert "/var/lib/xtnl" in rw
    spill = next(v.split("=", 1)[1] for v in svc["Environment"] if v.startswith("XTNL_SPILL_PATH="))
    assert any(spill.startswith(p + "/") for p in rw), "the spill file must be writable"
    tmpfiles = (DEPLOY / "systemd" / "xtnl.tmpfiles.conf").read_text(encoding="utf-8")
    assert re.search(r"^d\s+/var/lib/xtnl\s+0750\s+xtnl\s+xtnl", tmpfiles, re.M)
    assert re.search(r"^L\+?\s+/opt/xtnl/order-server/var\s.*/var/lib/xtnl$", tmpfiles, re.M)
    sysusers = (DEPLOY / "systemd" / "xtnl.sysusers.conf").read_text(encoding="utf-8")
    assert re.search(r"^u\s+xtnl\s", sysusers, re.M)


def test_engine_unit_hardening() -> None:
    cp, _ = read_unit("xtnl-engine.service")
    svc = cp["Service"]
    for key, value in (("NoNewPrivileges", "yes"), ("ProtectSystem", "strict"), ("ProtectHome", "yes"),
                       ("PrivateTmp", "yes"), ("RestrictSUIDSGID", "yes")):
        assert svc[key] == value, key
    assert svc["CapabilityBoundingSet"] == ""
    assert "AF_UNIX" in svc["RestrictAddressFamilies"]        # $NOTIFY_SOCKET


def test_engine_unit_ordering() -> None:
    _, multi = read_unit("xtnl-engine.service")
    unit = multi["Unit"]
    assert "chrony-wait.service" in words(unit["Requires"])  # starts only after chrony is synced (s14)
    assert "chrony-wait.service" in words(unit["After"])
    # Migrations are an operator job, never pulled in at engine start-up (operations.md s12).
    pulls = words(unit.get("Requires", []) + unit.get("Wants", []) + unit.get("BindsTo", []))
    assert "xtnl-migrate.service" not in pulls
    assert "xtnl-migrate.service" in words(unit["After"])


def test_migrate_unit_is_an_operator_oneshot() -> None:
    cp, multi = read_unit("xtnl-migrate.service")
    svc = cp["Service"]
    assert svc["Type"] == "oneshot"
    assert svc["ExecStart"].endswith("-m src.db.migrate")
    assert "xtnl-engine.service" in words(multi["Unit"]["Before"])
    assert "/etc/xtnl/engine.env" in multi["Service"]["EnvironmentFile"]
    assert not cp.has_section("Install"), "never enabled: run by an operator before a switchover"
    assert svc["NoNewPrivileges"] == "yes" and svc["ProtectSystem"] == "strict"


# ---------------------------------------------------------------------------------------------- etcd
def test_etcd_timing_fits_the_fence_budget() -> None:
    cfg = read_yaml(DEPLOY / "etcd" / "etcd.conf.yml")
    heartbeat_ms, election_ms = int(cfg["heartbeat-interval"]), int(cfg["election-timeout"])
    assert (heartbeat_ms, election_ms) == (250, 2500)
    assert election_ms >= 5 * heartbeat_ms
    t = LeaseTimings()
    worst_election = 2 * election_ms / 1000
    assert t.fence_after_s - t.renew_interval_s > worst_election + ENDPOINT_FAILOVER_S
    assert t.ttl_s == 15 and t.renew_interval_s == 1.0 and t.fence_after_s == 12.0


def test_etcd_cluster_and_storage() -> None:
    cfg = read_yaml(DEPLOY / "etcd" / "etcd.conf.yml")
    members = cfg["initial-cluster"].split(",")
    assert len(members) == 3 and all(m.split("=", 1)[1].startswith("https://") for m in members)
    assert cfg["auto-compaction-mode"] == "periodic" and cfg["auto-compaction-retention"] == "1h"
    assert int(cfg["quota-backend-bytes"]) > 0
    for side in ("client-transport-security", "peer-transport-security"):
        assert cfg[f"{side}.cert-file"].startswith("/etc/etcd/pki/")
        assert cfg[f"{side}.trusted-ca-file"] == "/etc/etcd/pki/ca.crt"
    assert cfg["peer-transport-security.client-cert-auth"] == "true"


# ---------------------------------------------------------------------------------------------- Patroni
def test_patroni_dcs_matches_operations() -> None:
    cfg = read_yaml(DEPLOY / "patroni" / "patroni.yml")
    dcs = {k.removeprefix("bootstrap.dcs."): v for k, v in cfg.items() if k.startswith("bootstrap.dcs.")}
    assert dcs["synchronous_mode"] == "true"
    # operations.md s1, s3, s6: losing the standby stalls commits <= ~20 s, then async + CRITICAL alert.
    assert dcs["synchronous_mode_strict"] == "false"
    assert dcs["failsafe_mode"] == "true"
    assert (dcs["ttl"], dcs["loop_wait"], dcs["retry_timeout"]) == ("15", "5", "5")
    assert dcs["primary_start_timeout"] == "0"
    assert cfg["etcd3.protocol"] == "https"
    assert len([k for k in cfg if k.startswith("etcd3.hosts[")]) == 3


def test_patroni_presents_an_etcd_client_certificate() -> None:
    """4.9: etcd client-cert-auth needs every client to present a certificate, Patroni included."""
    cfg = read_yaml(DEPLOY / "patroni" / "patroni.yml")
    assert cfg["etcd3.cacert"] == "/etc/etcd/pki/ca.crt"
    assert cfg["etcd3.cert"].endswith(".crt") and cfg["etcd3.key"].endswith(".key")
    assert "${PG_MEMBER_NAME}" in cfg["etcd3.cert"] and "${PG_MEMBER_NAME}" in cfg["etcd3.key"]


def test_patroni_postgres_parameters() -> None:
    cfg = read_yaml(DEPLOY / "patroni" / "patroni.yml")
    p = {k.removeprefix("bootstrap.dcs.postgresql.parameters."): v
         for k, v in cfg.items() if k.startswith("bootstrap.dcs.postgresql.parameters.")}
    assert p["archive_mode"] == "always"
    assert "pgbackrest --stanza=" in p["archive_command"]
    assert seconds(p["archive_timeout"]) == 60
    assert seconds(p["wal_sender_timeout"]) == 10
    assert p["synchronous_commit"] == "on"
    statement_timeout_ms = inspect.signature(pool_kwargs).parameters["statement_timeout_ms"].default
    idle_ms = seconds(p["idle_in_transaction_session_timeout"]) * 1000
    assert idle_ms > 0
    assert statement_timeout_ms + idle_ms <= DEFAULT_MAX_TXN_MS, "integrity window MAX_TXN_MS too small"
    assert "pgbackrest" in cfg["postgresql.recovery_conf.restore_command"]
    assert any(v == "data-checksums" for k, v in cfg.items() if k.startswith("bootstrap.initdb["))
    assert cfg["postgresql.bin_dir"].endswith("/17/bin")


def test_patroni_and_pgbackrest_hold_no_secrets() -> None:
    patroni = (DEPLOY / "patroni" / "patroni.yml").read_text(encoding="utf-8")
    assert not re.search(r"^\s*password\s*:", patroni, re.M)
    backrest = (DEPLOY / "patroni" / "pgbackrest.conf").read_text(encoding="utf-8")
    assert "[xtnl]" in backrest
    assert not re.search(r"^\s*repo1-(s3-key|s3-key-secret|cipher-pass)\s*=", backrest, re.M)


# ---------------------------------------------------------------------------------------------- Redis
def test_redis_conf() -> None:
    lines = [ln.strip() for ln in (DEPLOY / "redis" / "redis.conf").read_text(encoding="utf-8").splitlines()]
    directives: dict[str, str] = {}
    for ln in lines:
        if ln and not ln.startswith("#") and not ln.startswith("user "):
            k, _, v = ln.partition(" ")
            directives[k] = v.strip()
    assert directives["maxmemory-policy"] == "noeviction"
    assert directives["protected-mode"] == "yes"
    assert directives["appendonly"] == "no" and directives["save"] == '""'
    assert directives["maxmemory"]
    users = [ln for ln in lines if ln.startswith("user ")]
    assert "user default off" in users
    engine = next(u for u in users if u.startswith("user xtnl_engine "))
    assert " >" not in engine, "ACL passwords only as #<sha256> placeholders"
    for cmd in ("+flushdb", "+info"):            # LEADER_INIT / STANDBY FLUSHDB; INFO run_id for redis:boot_id
        assert cmd in engine.split()
    assert "+flushall" not in engine.split()    # 4.9: the engine flushes only its own database


# ---------------------------------------------------------------------------------------------- env
def test_every_env_var_the_code_reads_is_documented() -> None:
    text, _ = env_example()
    read = env_vars_read_by_code()
    assert {"BROKER_ENV", "ORDER_SERVER_DSN", "REDIS_URL", "XTNL_ETCD_ENDPOINTS"} <= read.keys()
    missing = {name: where for name, where in read.items() if not re.search(rf"\b{name}\b", text)}
    assert not missing, f"engine.env.example lacks: {missing}"
    _, assigned = env_example()
    for name in read.keys() - SYSTEM_PROVIDED - USER_ENVIRONMENT_ONLY:
        if name.startswith("STONE_X_BASE") or name == "STONE_X_STREAM_URL":
            continue   # optional overrides, documented as comments
        assert name in assigned, f"{name} must be an assignment, not only a comment"
    for name in USER_ENVIRONMENT_ONLY:
        # the ARM channel credentials are read under a prefix (NotificationSettings.from_env(env, prefix=ARM_PREFIX))
        assert name in read or name in ARM_CHANNEL_SECRETS, f"{name} is not read by the code (typo?)"
        assert name not in assigned, f"{name}: Windows User environment only, never assigned"
        m = re.search(rf"^#.*\b{name}\b.*\n#.*$", text, re.M)
        assert m is not None and "Windows User" in m.group(0), name
    for name in SYSTEM_PROVIDED & read.keys():
        assert name not in assigned, f"{name} is set by systemd"


def _assignments(path: Path) -> tuple[str, dict[str, str]]:
    text = path.read_text(encoding="utf-8")
    assigned: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            assigned[k.strip()] = v.strip()
    return text, assigned


@pytest.mark.parametrize("prefix", sorted(OWN_ENV_EXAMPLE))
def test_separate_services_document_their_own_env(prefix: str) -> None:
    example = OWN_ENV_EXAMPLE[prefix]
    text, assigned = _assignments(example)
    read = env_vars_read_by_code(ROOT / prefix.rstrip("/"))
    assert read, prefix
    missing = {n for n in read if not re.search(rf"\b{n}\b", text)}
    assert not missing, f"{example.name} lacks: {missing}"
    for name, value in assigned.items():
        if SECRET_HINT.search(name):
            assert value == "CHANGE_ME", f"{name} must be a placeholder"
        for host in re.findall(r"@([a-z0-9.-]+\.[a-z]{2,})", value):
            assert host.endswith(".invalid")
    assert assigned.get("XTNL_RECEIVER_SES_REGION", "") != "eu-west-2"
    _, engine_assigned = env_example()
    assert not [k for k in engine_assigned if k.startswith("XTNL_RECEIVER_")], "receiver settings: Melbourne host only"
    assert not [k for k in engine_assigned if k.startswith("XTNL_DASHBOARD_")], "dashboard settings: Melbourne host only"


def test_env_example_has_placeholders_only() -> None:
    _, assigned = env_example()
    assert assigned["BROKER_ENV"] == "sandbox"
    assert EXPECTED_CTRADER <= assigned.keys()
    for name in ("XTNL_ALERT_EMAIL_TO",):
        assert name in assigned
    assert any(k.startswith("XTNL_SMTP_") for k in assigned)
    assert any(k.startswith("XTNL_SES_") for k in assigned)
    assert any(k.startswith("XTNL_PUSH_") for k in assigned)
    for name, value in assigned.items():
        if SECRET_HINT.search(name) and name not in NOT_SECRET:
            assert value == "CHANGE_ME", f"{name} must be a placeholder"
    for url_var in ("ORDER_SERVER_DSN", "REDIS_URL"):
        assert ":CHANGE_ME@" in assigned[url_var]
    for value in assigned.values():
        for host in re.findall(r"@([a-z0-9.-]+\.[a-z]{2,})", value):
            assert host.endswith(".invalid"), "example addresses must use the .invalid TLD"
    assert len(assigned["XTNL_ETCD_ENDPOINTS"].split(",")) == 3


# ---------------------------------------------------------------------------------------------- compose
def test_dev_compose() -> None:
    text = (DEPLOY / "docker-compose.dev.yml").read_text(encoding="utf-8")
    assert re.search(r"^\s*image:\s*postgres:17\s*$", text, re.M)
    assert re.search(r"^\s*image:\s*redis:7\.4\s*$", text, re.M)
    assert "etcd" in text
    ports = re.findall(r'^\s*-\s*"([^"]+:\d+)"\s*$', text, re.M)
    assert ports and all(p.startswith("127.0.0.1:") for p in ports), "dev ports bind to loopback only"
    assert "noeviction" in text
    readme = (DEPLOY / "README.md").read_text(encoding="utf-8")
    assert "ORDER_SERVER_TEST_DSN" in readme and "ORDER_SERVER_TEST_REDIS" in readme
    for test_var, test_file in (("ORDER_SERVER_TEST_DSN", "tests/db/test_pg_integration.py"),
                                ("ORDER_SERVER_TEST_REDIS", "tests/cache/test_redis_integration.py")):
        assert test_var in (ROOT / test_file).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------------------------- runbooks
RUNBOOK_FILES = ("install-node.md", "failover-and-switchover.md", "safe-hold.md", "redis-loss.md",
                 "dlq-quarantine.md", "auth-failed-rearm.md", "deadman-alert.md",
                 "disaster-restore-melbourne.md", "live-readonly-smoke-test.md", "halt-clear.md",
                 "ruleset-change.md", "partition-archive.md", "off-node-alerts.md", "broker-maintenance.md",
                 "downtime-catchup.md", "pinescript-export.md", "single-pc-autostart.md", "order-server-api.md",
                 "marketdata.md", "arm.md", "live-view.md", "home-backup-restore.md",
                 "deadman-receiver-home.md")


def test_runbook_files_lists_every_runbook_on_disk() -> None:
    on_disk = {p.name for p in RUNBOOKS.glob("*.md")} - {"README.md"}
    assert on_disk == set(RUNBOOK_FILES)


@pytest.mark.parametrize("name", RUNBOOK_FILES)
def test_runbook_shape(name: str) -> None:
    text = (RUNBOOKS / name).read_text(encoding="utf-8")
    assert "never sends orders" in text
    headings = {h.strip().lower() for h in re.findall(r"^##\s+(.+)$", text, re.M)}
    assert any(h.startswith("trigger") for h in headings), name
    assert any(h.startswith(("check", "prerequisites")) or "checks" in h for h in headings), name
    assert any(h.startswith("rollback") for h in headings), name
    assert "```" in text, f"{name} needs concrete commands"


def test_runbook_index_lists_every_runbook() -> None:
    index = (RUNBOOKS / "README.md").read_text(encoding="utf-8")
    for name in RUNBOOK_FILES:
        assert f"]({name})" in index
