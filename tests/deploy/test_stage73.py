"""IMPLEMENTATION_PLAN 7.3 deploy / runbook invariants (operations.md §1, §8, §9, §10, §11).

* every pgBackRest caller (WAL archive-push / archive-get under Patroni, the replica restore, the PITR guard,
  the backup job, stanza-create / check, the operator restore commands) gets the encrypted repo's passphrase
  from the one include file pgBackRest always reads, never from a systemd environment only one unit has;
* the Melbourne dashboard reaches each node's Redis read-only over the VPN: a TLS proxy (mutual TLS) in front of
  the loopback-only Redis, and a least-privilege ACL user that covers exactly what the dashboard reads;
* the dashboard host has its own env example and install section; the witness install deploys the defrag job,
  log shipping and its disk metrics; the broker maintenance window runbook keeps both nodes' files identical.
Stdlib + the project's own modules only; nothing here opens a network connection.
"""
from __future__ import annotations

import asyncio
import configparser
import fnmatch
import re
import shlex
from pathlib import Path
from typing import Any

import pytest

from src.cache import keys as K
from src.dashboard import server as S
from src.dashboard import views as V
from tests.db.conftest import FakeSource
from tests.deploy.test_deploy_assets import read_unit, words

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy"
RUNBOOKS = ROOT / "docs" / "runbooks"
CIPHER_INCLUDE = "/etc/pgbackrest/conf.d/xtnl-cipher.conf"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ============================================================================================ pgBackRest cipher
def test_every_pgbackrest_caller_reads_the_cipher_pass_from_the_include_file() -> None:
    """The passphrase used to reach only xtnl-pg-backup.service (EnvironmentFile): archive_command,
    restore_command, the PITR guard and every runbook command ran without it, so WAL archiving failed on both
    members (RPO <= archive_timeout lost) and restores could not decrypt."""
    conf = _text(DEPLOY / "patroni" / "pgbackrest.conf")
    assert re.search(r"^repo1-cipher-type=aes-256-cbc$", conf, re.M)
    assert not re.search(r"^\s*repo1-cipher-pass\s*=", conf, re.M)
    assert "/etc/pgbackrest/pgbackrest.conf" in conf and CIPHER_INCLUDE in conf
    example = _text(DEPLOY / "patroni" / "pgbackrest-cipher.conf.example")
    body = [ln for ln in example.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
    assert body == ["[global]", "repo1-cipher-pass=CHANGE_ME"]
    assert "0640 root:postgres" in example
    # no caller depends on an environment variable any more (it would also override the file for one caller)
    scanned = [p for p in (*DEPLOY.rglob("*"), *RUNBOOKS.glob("*.md")) if p.is_file() and p.suffix != ".pyc"]
    for p in scanned:
        t = _text(p)
        assert "pgbackrest.env" not in t, p.name
        assert not re.search(r"^\s*(export\s+)?PGBACKREST_REPO1_CIPHER_PASS\s*=", t, re.M), p.name
    for unit in ("xtnl-pg-backup.service", "xtnl-pitr-guard.service"):
        cp, _ = read_unit(unit)
        assert cp["Service"]["User"] == "postgres"
        assert "EnvironmentFile" not in cp["Service"], unit
    # install: the include file exists (postgres-readable) before Patroni first starts archiving
    install = _text(RUNBOOKS / "install-node.md")
    i_inc = install.index(f"install -m 0640 -o root -g postgres /dev/null {CIPHER_INCLUDE}")
    i_cfg = install.index("/etc/pgbackrest/pgbackrest.conf")
    i_start = install.index("systemctl enable --now etcd patroni redis-server")
    assert i_cfg < i_start and i_inc < i_start
    assert "pgbackrest-cipher.conf.example" in install


def test_pgbackrest_files_have_no_inline_comments() -> None:
    """pgBackRest's ini parser takes everything after '=' as the value (no inline comments): the template's
    `repo1-s3-bucket=${PGBACKREST_BUCKET}   # ap-southeast-4 ...` named a bucket that does not exist, so every
    archive-push / archive-get / backup / restore failed on both members and the DR stack."""
    for name in ("pgbackrest.conf", "pgbackrest-cipher.conf.example"):
        for n, raw in enumerate(_text(DEPLOY / "patroni" / name).splitlines(), 1):
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith("["):
                continue
            key, eq, value = line.partition("=")
            assert eq and key.strip() and "#" not in value and value == value.strip(), f"{name}:{n}: {raw!r}"
    install = _text(RUNBOOKS / "install-node.md")
    # archive-async spools under spool-path as postgres: the directory exists before Patroni first archives
    spool = re.search(r"^spool-path=(\S+)$", _text(DEPLOY / "patroni" / "pgbackrest.conf"), re.M)
    assert spool is not None
    i_spool = install.index(f"-o postgres -g postgres {spool.group(1)}")
    assert i_spool < install.index("systemctl enable --now etcd patroni redis-server")


def test_postgres_runs_every_pgbackrest_command_in_the_runbooks() -> None:
    """The include file is 0640 root:postgres: a pgBackRest command run as another user cannot decrypt."""
    for f in RUNBOOKS.glob("*.md"):
        text = _text(f)
        for m in re.finditer(r"pgbackrest --stanza", text):
            before = text[max(0, m.start() - 17):m.start()]
            assert before == "sudo -u postgres ", f"{f.name}: '{text[m.start() - 30:m.end() + 20]}'"
    dr = _text(RUNBOOKS / "disaster-restore-melbourne.md")
    assert CIPHER_INCLUDE in dr and "same** repo passphrase" in dr


def test_archiving_and_restore_still_go_through_pgbackrest_with_rpo_60s() -> None:
    patroni = _text(DEPLOY / "patroni" / "patroni.yml")
    assert "archive_command: 'pgbackrest --stanza=xtnl archive-push %p'" in patroni
    assert "archive_timeout: 60s" in patroni and "archive_mode: always" in patroni
    assert "restore_command: 'pgbackrest --stanza=xtnl archive-get %f \"%p\"'" in patroni
    assert "command: /usr/bin/pgbackrest --stanza=xtnl --delta restore" in patroni
    assert CIPHER_INCLUDE in patroni
    # a dropped WAL (archive-push-queue-max) can only follow archive failures, which the guard backs up after
    assert "archive-push-queue-max=" in _text(DEPLOY / "patroni" / "pgbackrest.conf")


# ============================================================================================ dashboard Redis
def _acl(user: str) -> list[str]:
    lines = [ln.strip() for ln in _text(DEPLOY / "redis" / "redis.conf").splitlines()]
    (line,) = [ln for ln in lines if ln.startswith(f"user {user} ")]
    return line.split()[2:]


ALLOWED_DASHBOARD_COMMANDS = {"get", "hgetall", "ping", "select", "client|setinfo"}


def test_dashboard_acl_is_least_privilege_read_only() -> None:
    rules = _acl("xtnl_dashboard")
    assert rules[0] == "on" and rules[1].startswith("#") and not any(r.startswith(">") for r in rules)
    assert "resetkeys" in rules and "resetchannels" in rules and "-@all" in rules
    keys = [r for r in rules if "~" in r]
    assert keys and all(r.startswith("%R~") for r in keys), "read-only key patterns only"
    assert not any(r.startswith(("~", "%W~", "%RW~", "&", "allkeys", "allchannels")) for r in rules)
    granted = {r[1:] for r in rules if r.startswith("+")}
    assert granted == ALLOWED_DASHBOARD_COMMANDS
    assert not any(r.startswith("+@") for r in rules), "no command categories (+@read includes KEYS / SCAN)"
    redis_conf = _text(DEPLOY / "redis" / "redis.conf")
    assert re.search(r"^bind 127\.0\.0\.1 -::1$", redis_conf, re.M), "Redis itself stays on loopback"


class _Recorder:
    """A Redis client that records what the dashboard sends; any other method fails the test."""

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data
        self.calls: list[tuple[str, str]] = []

    async def get(self, key: str) -> Any:
        self.calls.append(("get", key))
        return self.data.get(key)

    def pipeline(self, transaction: bool = True) -> _RecPipe:
        assert transaction is False, "MULTI / EXEC are not granted"
        return _RecPipe(self)

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"the dashboard uses Redis method {name!r}: grant it in redis.conf first")


class _RecPipe:
    def __init__(self, r: _Recorder) -> None:
        self.r = r
        self.keys: list[str] = []

    def hgetall(self, key: str) -> _RecPipe:
        self.r.calls.append(("hgetall", key))
        self.keys.append(key)
        return self

    async def execute(self) -> list[Any]:
        return [self.r.data.get(k, {}) for k in self.keys]


async def test_the_acl_covers_every_command_and_key_the_dashboard_reads() -> None:
    from tests.dashboard.test_dashboard import NOW, scripted
    data = {K.BOOT_ID: b"run-y:42",
            K.price_key("pepperstone", "EURUSD"): {b"bid": b"1.1", b"ask": b"1.2", b"ts": str(NOW).encode()},
            K.account_live_key("ACC"): {b"adapter_id": b"pepperstone", b"currency": b"USD", b"equity": b"1000",
                                        b"ts": str(NOW).encode()}}
    rec = _Recorder(data)
    conn = scripted()
    conn.returns_all(V.SQL_OPEN_SYMBOLS, [("pepperstone", "EURUSD")])
    views = V.DashboardViews(FakeSource(conn), {"node-a": rec}, fresh_ms=30_000)   # type: ignore[dict-item]
    live = await views.live(await views.leader(), NOW, ["ACC"])
    assert live["source"] == "node-a" and not live["stale"]
    rules = _acl("xtnl_dashboard")
    patterns = [r[3:] for r in rules if r.startswith("%R~")]
    granted = {r[1:] for r in rules if r.startswith("+")}
    assert {c for c, _ in rec.calls} == {"get", "hgetall"}
    for cmd, key in rec.calls:
        assert cmd in granted, cmd
        assert any(fnmatch.fnmatchcase(key, p) for p in patterns), f"{cmd} {key} not readable by xtnl_dashboard"


def _stunnel() -> tuple[configparser.RawConfigParser, str]:
    text = _text(DEPLOY / "redis" / "redis-tls-proxy.conf")
    cp = configparser.RawConfigParser(comment_prefixes=(";", "#"), inline_comment_prefixes=None,
                                      interpolation=None, delimiters=("=",))
    cp.optionxform = str  # type: ignore[assignment,method-assign]
    cp.read_string("[global]\n" + text)
    return cp, text


def test_tls_proxy_forwards_the_vpc_port_to_loopback_redis_with_mutual_tls() -> None:
    cp, _ = _stunnel()
    assert cp["global"]["foreground"] == "yes"
    svc = cp["xtnl-redis-dashboard"]
    assert svc["accept"] == "${NODE_IP}:6380"
    redis_port = re.search(r"^port (\d+)$", _text(DEPLOY / "redis" / "redis.conf"), re.M)
    assert redis_port is not None and svc["connect"] == f"127.0.0.1:{redis_port.group(1)}"
    assert svc["verifyChain"] == "yes" and svc["requireCert"] == "yes" and svc["sslVersionMin"] == "TLSv1.2"
    unit, multi = read_unit("xtnl-redis-tls-proxy.service")
    s = unit["Service"]
    assert s["ExecStart"] == "/usr/bin/stunnel /etc/xtnl/redis-tls-proxy.conf" and s["DynamicUser"] == "yes"
    creds = {c.split(":", 1)[0] for c in multi["Service"]["LoadCredential"]}
    for opt, name in (("cert", "tls.crt"), ("key", "tls.key"), ("CAfile", "client-ca.crt")):
        assert svc[opt] == f"/run/credentials/xtnl-redis-tls-proxy.service/{name}" and name in creds
    assert s["Restart"] == "always" and words(multi["Service"]["RestrictAddressFamilies"]) >= {"AF_INET"}


def _dashboard_env() -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in _text(DEPLOY / "env" / "dashboard.env.example").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            out[k.strip()] = v.strip()
    return out


def test_dashboard_env_example_reaches_each_nodes_redis_through_its_tls_proxy() -> None:
    """The example used to point at rediss://...:6379 on the VPC address, where Redis does not listen (loopback
    only, no TLS): every live value always read as stale. Parsed with the dashboard's own settings + client
    factory (no connection is opened)."""
    from redis.asyncio.connection import SSLConnection
    env = _dashboard_env()
    settings = S.DashboardSettings.from_env(env)
    assert set(settings.redis_urls) == {"node-a", "node-b"}
    clients = S.redis_clients(settings.redis_urls)
    try:
        hosts = set()
        for client in clients.values():
            pool = client.connection_pool
            kw = pool.connection_kwargs
            assert pool.connection_class is SSLConnection
            assert kw["port"] == 6380 and kw["username"] == "xtnl_dashboard"
            assert kw["ssl_check_hostname"] is True
            for k in ("ssl_ca_certs", "ssl_certfile", "ssl_keyfile"):
                assert str(kw[k]).startswith("/etc/xtnl/dashboard/"), k
            hosts.add(kw["host"])
        assert hosts == {"10.0.1.10", "10.0.2.10"}
    finally:
        async def close() -> None:
            for c in clients.values():
                await c.aclose()
        asyncio.run(close())
    assert "sslmode=verify-full" in settings.dsn and settings.dsn.startswith("postgresql://xtnl_dashboard:")


def test_dashboard_host_install_section_and_unit_agree() -> None:
    install = _text(RUNBOOKS / "install-node.md")
    section = install[install.index("## Melbourne dashboard host"):install.index("## Checks")]
    unit, _ = read_unit("xtnl-dashboard.service")
    assert unit["Service"]["EnvironmentFile"] == "/etc/xtnl/dashboard.env"
    assert "/etc/xtnl/dashboard.env" in section and "dashboard.env.example" in section
    for path in ("/etc/xtnl/dashboard/ca.crt", "/etc/xtnl/dashboard/redis-client.crt",
                 "/etc/xtnl/dashboard/redis-client.key"):
        assert path in section and path in _text(DEPLOY / "env" / "dashboard.env.example")
    assert "deploy/systemd/xtnl-dashboard.service" in section
    readme = _text(DEPLOY / "README.md")
    for name in ("systemd/xtnl-dashboard.service", "env/dashboard.env.example", "redis/redis-tls-proxy.conf",
                 "systemd/xtnl-redis-tls-proxy.service", "observability/cloudwatch/agent-witness.json",
                 "patroni/pgbackrest-cipher.conf.example"):
        assert f"`{name}`" in readme, name
    engine_step = install[install.index("# 7. Redis TLS proxy"):install.index("# 8.")]
    assert "deploy/systemd/xtnl-redis-tls-proxy.service" in engine_step and "redis-tls-proxy.conf" in engine_step


# ============================================================================================ witness, maintenance
def test_witness_install_deploys_defrag_log_shipping_and_disk_metrics() -> None:
    """install-node.md said both 'witness: defrag + log lines' and 'witness: only chrony + etcd'; one section now."""
    install = _text(RUNBOOKS / "install-node.md")
    assert "only chrony + etcd" not in install
    section = install[install.index("## Commands (witness)"):install.index("## Dashboard role")]
    code = section[section.index("```"):section.rindex("```")]
    for needle in ("xtnl-etcd-defrag.sh /usr/local/bin/xtnl-etcd-defrag", "xtnl-etcd-defrag.timer",
                   "enable --now xtnl-etcd-defrag.timer", "/etc/xtnl/etcd-defrag.env",
                   "30-xtnl-rsyslog.conf /etc/rsyslog.d/30-xtnl.conf", "journald-xtnl.conf",
                   "usermod -aG adm cwagent", "cloudwatch/agent-witness.json"):
        assert needle in code, needle
    assert "agent-prometheus.json" not in code
    assert "/var/lib/etcd" in section
    # the witness agent publishes into CWAgent, not XTNL: its role must allow that namespace (else no samples)
    assert re.search(r"`cloudwatch:PutMetricData` for namespace\s+\*\*`CWAgent`\*\*", section)


def test_install_renders_both_redis_acl_hashes_before_redis_starts() -> None:
    """The xtnl_dashboard ACL line is live now: an unreplaced placeholder keeps the ENGINE's Redis from starting."""
    install = _text(RUNBOOKS / "install-node.md")
    i_note = install.index("replace BOTH ACL placeholders (xtnl_engine and xtnl_dashboard")
    assert i_note < install.index("systemctl enable --now etcd patroni redis-server")
    conf = _text(DEPLOY / "redis" / "redis.conf")
    assert "#REPLACE_WITH_SHA256_HEX_OF_PASSWORD" in conf and "#REPLACE_WITH_SHA256_HEX_OF_DASHBOARD_PASSWORD" in conf


def test_broker_maintenance_runbook_keeps_both_nodes_identical() -> None:
    from src.runtime.maintenance import DEFAULT_MAINTENANCE_FILE, MAINTENANCE_FILE_VAR
    text = _text(RUNBOOKS / "broker-maintenance.md")
    assert DEFAULT_MAINTENANCE_FILE in text and MAINTENANCE_FILE_VAR in text
    assert "identical on node A and node B" in text and "sha256" in text
    assert "$OPS maintenance check" in text
    assert re.search(r"mv /etc/xtnl/maintenance\.json\.new /etc/xtnl/maintenance\.json", text)   # atomic
    assert "declared maintenance windows" in text            # the engine's log line on a changed list
    # the documented example is accepted by the engine's own parser
    import json

    from src.runtime.maintenance import parse_maintenance
    block = text[text.index("1. Write the new file"):text.index("2. Validate")]
    doc = json.loads(block[block.index("{"):block.rindex("}") + 1])
    (w,) = parse_maintenance(doc)
    assert w.adapter_id == "stonex" and w.end_ms > w.start_ms
    for f in ("deadman-alert.md", "install-node.md"):
        assert "broker-maintenance.md" in _text(RUNBOOKS / f), f


@pytest.mark.parametrize("line", ["maintenance check", "maintenance check --file /tmp/maintenance.json"])
def test_runbook_maintenance_commands_parse(line: str) -> None:
    from src.ops.cli import build_parser
    ns = build_parser().parse_args(shlex.split(line))
    assert (ns.group, ns.cmd) == ("maintenance", "check")
