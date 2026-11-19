"""AWS deploy of the optional market-data store and the ARM, their metrics contract, runbooks and diagram (plan S37;
design §A2, §A5, §B14, §C2).

* `redis-md` (deploy/redis/redis-md.conf, deploy/systemd/xtnl-redis-md.service): loopback 6381, AOF everysec, 1 GB
  noeviction, MemoryMax 2G, the ACL rendered at start from the secrets source into the aclfile path; installed but not
  enabled, and nothing in the engine's own units depends on it;
* the TLS proxy's second section 6382 -> 6381 with the same mutual TLS as the dashboard section; the engine Redis
  config changed in comments only (no md user, no md port);
* the ACL rules are one list across the renderer, the dev compose service and the PC credentials script;
* the metrics contract: every item of the design's §C2 metric list has a row in metrics.md, every series it names is
  a defined metric with defined labels, and every `component` / `event` pair is a real counter field;
* runbooks: every page title the market-data / ARM code can send is in `marketdata.md` or `arm.md`; every FAILED probe
  kind and every request a refused clear may name is in `arm.md`; the AOF-aside and key-rotation procedures;
* the architecture diagram's page 6 (market data, ARM, SM11, SM12) is generated, checked and up to date.
Static: no Docker, no AWS, no network.
"""
from __future__ import annotations

import ast
import dataclasses
import importlib.util
import re
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType

import pytest

from src.ops.md_acl import HEALTH_RULES, READER_RULES, WRITER_RULES
from src.riskmgr.failed import CLEAR_COMMAND
from src.riskmgr.probes import FIRST_COMMANDS, PROBE_KINDS
from src.runtime.optional_plan import ARM_EVENT_COMPONENTS, MD_EVENT_COMPONENTS, PRE_SKIPPED_COMPONENT
from tests.deploy import test_redis_md_compose as dev_compose
from tests.deploy.test_deploy_assets import read_unit

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy"
OBS = DEPLOY / "observability"
RUNBOOKS = ROOT / "docs" / "runbooks"
SRC = ROOT / "src"


def _directives(path: Path) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            key, _, value = line.partition(" ")
            out.setdefault(key, []).append(value.strip())
    return out


# ---------------------------------------------------------------------------------------------- redis-md
def test_redis_md_conf() -> None:
    d = _directives(DEPLOY / "redis" / "redis-md.conf")
    one = {k: v[-1] for k, v in d.items()}
    assert one["bind"] == "127.0.0.1 -::1" and one["port"] == "6381" and one["protected-mode"] == "yes"
    assert one["appendonly"] == "yes" and one["appendfsync"] == "everysec" and one["aof-load-truncated"] == "yes"
    assert one["save"] == '""', "no RDB snapshots (Q17: AOF only)"
    assert one["maxmemory"] == "1gb" and one["maxmemory-policy"] == "noeviction"
    assert one["supervised"] == "systemd" and one["daemonize"] == "no"
    assert "user" not in d, "with aclfile set, no user line may be in the file (and never a password)"
    assert one["dir"] == "/var/lib/xtnl-redis-md"
    assert one["aclfile"] == "/run/xtnl-redis-md/users.acl"


def test_redis_md_unit_is_installed_not_enabled_and_renders_the_acl() -> None:
    cp, multi = read_unit("xtnl-redis-md.service")
    svc = cp["Service"]
    assert svc["MemoryMax"] == "2G", "2 x maxmemory: the AOF rewrite fork's headroom (design §A5)"
    assert svc["User"] == "redis" and svc["Type"] == "notify"
    assert svc["RuntimeDirectory"] == "xtnl-redis-md" and svc["StateDirectory"] == "xtnl-redis-md"
    assert svc["ExecStart"] == "/usr/bin/redis-server /etc/xtnl/redis-md.conf"
    pre = multi["Service"]["ExecStartPre"]
    assert len(pre) == 1 and pre[0].startswith("+/opt/xtnl/order-server/.venv/bin/python -m src.ops.md_acl ")
    args = pre[0].split()
    assert args[args.index("--out") + 1] == _directives(DEPLOY / "redis" / "redis-md.conf")["aclfile"][-1]
    assert args[args.index("--env-file") + 1] == "/etc/xtnl/engine.env" and "--any-port" not in args
    assert svc["RestartPreventExitStatus"] == "78" and svc["Restart"] == "always"
    assert "EnvironmentFile" not in svc, "redis-server never gets the engine's environment"
    assert cp["Unit"]["ConditionPathExists"] == "/etc/xtnl/redis-md.conf"
    assert cp["Install"]["WantedBy"] == "multi-user.target"
    assert svc["NoNewPrivileges"] == "yes" and svc["ProtectSystem"] == "strict"


def test_the_engine_does_not_depend_on_redis_md() -> None:
    for unit in ("xtnl-engine.service", "xtnl-migrate.service", "xtnl-redis-tls-proxy.service"):
        _, multi = read_unit(unit)
        deps = " ".join(v for sec in multi.values() for k, vs in sec.items()
                        if k in ("Requires", "Wants", "After", "BindsTo", "PartOf", "Requisite") for v in vs)
        assert "redis-md" not in deps, unit


def test_install_node_installs_but_never_enables_redis_md() -> None:
    install = (RUNBOOKS / "install-node.md").read_text(encoding="utf-8")
    assert "deploy/systemd/xtnl-redis-md.service /etc/systemd/system/" in install
    assert "deploy/redis/redis-md.conf /etc/xtnl/redis-md.conf" in install
    for line in install.splitlines():
        if "systemctl" in line and ("enable" in line or "start" in line):
            assert "redis-md" not in line, line
    md = (RUNBOOKS / "marketdata.md").read_text(encoding="utf-8")
    assert "sudo systemctl enable --now xtnl-redis-md" in md and "XTNL_MD_ENABLED=1" in md


def _sections(path: Path) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    current: dict[str, str] | None = None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith(";"):
            continue
        if line.startswith("[") and line.endswith("]"):
            current = out.setdefault(line[1:-1], {})
            continue
        if current is not None:
            k, _, v = line.partition("=")
            current[k.strip()] = v.strip()
    return out


def test_tls_proxy_second_section() -> None:
    s = _sections(DEPLOY / "redis" / "redis-tls-proxy.conf")
    dash, md = s["xtnl-redis-dashboard"], s["xtnl-redis-md"]
    assert dash["accept"] == "${NODE_IP}:6380" and dash["connect"] == "127.0.0.1:6379"
    assert md["accept"] == "${NODE_IP}:6382" and md["connect"] == "127.0.0.1:6381"
    for key in ("cert", "key", "CAfile", "verifyChain", "requireCert", "sslVersionMin"):
        assert md[key] == dash[key], key
    assert md["verifyChain"] == md["requireCert"] == "yes"


def test_engine_redis_conf_changed_in_comments_only() -> None:
    d = _directives(DEPLOY / "redis" / "redis.conf")
    assert d["port"] == ["6379"] and "aclfile" not in d
    users = " ".join(d.get("user", []))
    assert "md_" not in users and "md:" not in users
    text = (DEPLOY / "redis" / "redis.conf").read_text(encoding="utf-8")
    assert "redis-md.conf" in text           # the comment pointing at the separate store


def _ps1_rules() -> dict[str, str]:
    text = (DEPLOY / "local" / "new-md-credentials.ps1").read_text(encoding="utf-8")
    out: dict[str, str] = {}
    for name in ("WriterRules", "ReaderRules", "HealthRules"):
        m = re.search(rf"^\${name} = ((?:'[^']*'(?: \+\s*\n\s*)?)+)$", text, re.M)
        assert m, name
        out[name] = "".join(re.findall(r"'([^']*)'", m.group(1)))
    return out


def test_acl_rules_are_one_list() -> None:
    assert (WRITER_RULES, READER_RULES, HEALTH_RULES) == (dev_compose.WRITER_RULES, dev_compose.READER_RULES,
                                                          dev_compose.HEALTH_RULES)
    ps1 = _ps1_rules()
    assert (ps1["WriterRules"], ps1["ReaderRules"], ps1["HealthRules"]) == (WRITER_RULES, READER_RULES, HEALTH_RULES)


# ---------------------------------------------------------------------------------------------- metrics contract
def _gen() -> ModuleType:
    spec = importlib.util.spec_from_file_location("xtnl_gen_alerts_s37", OBS / "gen_alerts.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


G = _gen()
#: Design §C2 "Metrics", in its order (market data, then ARM).
C2_ITEMS = (
    "ticks in", "ticks dropped", "receipt-time ticks", "ring and queue depth", "writer lag", "series by SM12 state",
    "gaps", "bars by source and quality", "revisions", "bar-fetch latency and REST waits / timeouts",
    "backfill remaining buckets", "indicator warm / FAULTED and recompute queue",
    "validation failures and quarantined buckets by check", "BROKER-vs-TICK mismatches and run length",
    "AOF wait timeouts", "md generation and store class", "rollback-set depth", "consumer cursor lag",
    "redis-md memory and shedding step", "skew",
    "positions by state", "decisions by kind / event / outcome", "PRE skipped by reason", "POST decision latency",
    "close source", "last POST age", "notice hand-off lag and expiries", "config age",
    "FAILED scopes and error-budget counters")
_SERIES = re.compile(r"`(xtnl_[a-z0-9_]+)(?:\{([^}]*)\})?`")


def _c2_rows() -> dict[str, tuple[str, str]]:
    text = (OBS / "metrics.md").read_text(encoding="utf-8")
    block = text.split("### Design §C2 metric list", 1)[1].split("\n## ", 1)[0]
    rows: dict[str, tuple[str, str]] = {}
    for line in block.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 3 and cells[0] not in ("§C2 item", "---"):
            rows[cells[0]] = (cells[1], cells[2])
    return rows


def test_every_c2_metric_item_has_a_row() -> None:
    rows = _c2_rows()
    assert set(rows) == set(C2_ITEMS)
    for item, (series, status) in rows.items():
        assert status.startswith(("exported", "partly", "not exported")), item
        if status.startswith(("exported", "partly")):
            assert _SERIES.search(series), f"{item}: an exported item names its series"
        else:
            assert ":" in status, f"{item}: a not-exported item says where to read it instead"


def _int_fields(cls: type) -> set[str]:
    return {f.name for f in dataclasses.fields(cls) if f.type in (int, "int")}


@pytest.mark.parametrize("item", C2_ITEMS)
def test_c2_series_are_defined_metrics_and_real_counters(item: str) -> None:
    series, _ = _c2_rows()[item]
    defined = {m.name: m.labels for m in G.DEFINED_METRICS}
    families = {"xtnl_md_events_total": MD_EVENT_COMPONENTS, "xtnl_arm_events_total": ARM_EVENT_COMPONENTS}
    for name, labels in _SERIES.findall(series):
        assert name in defined, f"{item}: {name} is not in metrics.md / gen_alerts"
        pairs = dict(re.findall(r'([a-z_]+)="([^"]*)"', labels))
        assert set(pairs) <= set(defined[name]) - {"node"}, f"{item}: {name} has no label {set(pairs)}"
        if name in families and "component" in pairs:
            comp = pairs["component"]
            if comp == PRE_SKIPPED_COMPONENT:
                assert "event" not in pairs        # the event is the reason code
                continue
            assert comp in families[name], f"{item}: unknown component {comp}"
            if "event" in pairs:
                assert pairs["event"] in _int_fields(families[name][comp]), f"{item}: {comp}.{pairs['event']}"


def test_event_components_are_exported_and_documented() -> None:
    text = (OBS / "metrics.md").read_text(encoding="utf-8")
    for family, comps in (("xtnl_md_events_total", MD_EVENT_COMPONENTS), ("xtnl_arm_events_total", ARM_EVENT_COMPONENTS)):
        row = next(ln for ln in text.splitlines() if ln.startswith(f"| `{family}`"))
        for comp in comps:
            assert f"`{comp}`" in row, f"{family} row does not list component {comp}"
    arm_row = next(ln for ln in text.splitlines() if ln.startswith("| `xtnl_arm_events_total`"))
    assert f"`{PRE_SKIPPED_COMPONENT}`" in arm_row


def test_redis_md_memory_alert() -> None:
    by = {a.name: a for a in G.ALERTS}
    a = by["XtnlRedisMdMemoryHigh"]
    assert "> 0.7" in a.expr and a.metrics == ("xtnl_md_redis_used_memory_ratio",) and a.severity == "warning"
    assert not a.depends_on_epoch
    for cw in a.cloudwatch():
        assert cw.threshold == 0.7 and cw.missing == "notBreaching", "absent while XTNL_MD_ENABLED is off"


# ---------------------------------------------------------------------------------------------- runbooks
#: Pages whose title is not a literal at the call (a variable or an f-string): (title text, a source that builds it).
DYNAMIC_PAGES = (("tick cross-check skipped", "src/marketdata/crosscheck.py"),
                 ("redis-md memory shedding step", "src/marketdata/writer.py"),
                 ("ARM channel", "src/notifications/arm_relay.py"),
                 ("ARM notice #", "src/notifications/arm_relay.py"),
                 ("ARM FAILED", "src/riskmgr/failed.py"))
#: page(severity, title, body) and the helpers' title positions.
_TITLE_POS = {"page": 1, "_page": 1, "_park": 1}
SCANNED = sorted({*(SRC / "marketdata").rglob("*.py"), *(SRC / "riskmgr").rglob("*.py"),
                  *(SRC / "armconfig").rglob("*.py"), *(SRC / "runtime").glob("optional*.py"),
                  SRC / "runtime" / "md_ports.py", SRC / "runtime" / "arm_ports.py",
                  SRC / "notifications" / "arm_relay.py"})


#: The pager wrappers: (module, Class.method) whose page call forwards its own `title` parameter. Their callers are
#: scanned like any other call (`_page`, `_page_once`, `_park`, `page`); a new wrapper must be added here on purpose.
WRAPPERS = frozenset({
    ("src/armconfig/installer.py", "ConfigInstaller._page_once"),
    ("src/marketdata/brokerbars.py", "MdBars._page"),
    ("src/marketdata/writer.py", "MdWriter._page"),
    ("src/notifications/arm_relay.py", "ArmRelay._page_once"),
    ("src/riskmgr/ports.py", "Pager.page"),
    ("src/runtime/md_ports.py", "PagerWrapper.page"),
    ("src/runtime/optional.py", "OptionalBootstrapRunner._page_once"),
    ("src/runtime/optional.py", "OptionalBootstrapRunner._park"),
})


def _enclosing(parents: dict[ast.AST, ast.AST], node: ast.AST) -> tuple[ast.FunctionDef | ast.AsyncFunctionDef | None,
                                                                         str]:
    fn = parents.get(node)
    while fn is not None and not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
        fn = parents.get(fn)
    if fn is None:
        return None, "<module>"
    cls = parents.get(fn)
    while cls is not None and not isinstance(cls, ast.ClassDef):
        cls = parents.get(cls)
    return fn, (f"{cls.name}." if cls is not None else "") + fn.name


def _local_literals(fn: ast.FunctionDef | ast.AsyncFunctionDef | None, name: str) -> list[str] | None:
    """The string literals `name` is bound to in `fn`, or None when any binding is not a plain string literal."""
    if fn is None or name in {a.arg for a in (*fn.args.posonlyargs, *fn.args.args, *fn.args.kwonlyargs)}:
        return None
    values: list[str] = []
    for node in ast.walk(fn):
        if not (isinstance(node, ast.Name) and node.id == name and isinstance(node.ctx, ast.Store)):
            continue
        parent = next((a for a in ast.walk(fn) if isinstance(a, ast.Assign) and node in a.targets), None)
        if parent is None or not (isinstance(parent.value, ast.Constant) and isinstance(parent.value.value, str)):
            return None                                      # bound by a for / with / walrus / tuple / non-literal
        values.append(parent.value.value)
    return values or None


def page_titles(scanned: Sequence[Path] = SCANNED, root: Path = ROOT) -> dict[str, str]:
    """Every page title (a literal, the literal head of an f-string, or a local bound only to literals) the scanned
    modules pass to a pager. Any other title expression fails the scan unless it forwards a parameter inside one of
    the known `WRAPPERS`: a dynamic title at a new call site can never go unnoticed (S37 review)."""
    found: dict[str, str] = {}
    unknown: list[str] = []
    for path in scanned:
        rel = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        parents = {c: n for n in ast.walk(tree) for c in ast.iter_child_nodes(n)}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else fn.id if isinstance(fn, ast.Name) else ""
            if name == "_page_once":
                pos = 2 if len(node.args) == 4 else 1          # (key, severity, title, body) | (severity, title, body)
            elif name in _TITLE_POS:
                pos = _TITLE_POS[name]
            else:
                continue
            kw = next((k.value for k in node.keywords if k.arg == "title"), None)
            if len(node.args) <= pos and kw is None:
                continue                                       # not a page(severity, title, body) call
            arg = node.args[pos] if len(node.args) > pos else kw
            assert arg is not None
            func, qual = _enclosing(parents, node)
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                found.setdefault(arg.value, rel)
            elif (isinstance(arg, ast.JoinedStr) and arg.values and isinstance(arg.values[0], ast.Constant)
                  and str(arg.values[0].value).strip()):
                found.setdefault(str(arg.values[0].value).strip(), rel)
            elif isinstance(arg, ast.Name) and (lits := _local_literals(func, arg.id)) is not None:
                for lit in lits:
                    found.setdefault(lit, rel)
            elif isinstance(arg, ast.Name) and (rel, qual) in WRAPPERS:
                continue                                       # forwards its caller's title (scanned at the caller)
            else:
                unknown.append(f"{rel}:{node.lineno} {qual}: {name}(..., {ast.unparse(arg)}, ...)")
    assert not unknown, ("page titles the runbook scan cannot read; use a literal title, or add the wrapper to "
                         f"WRAPPERS and its titles to DYNAMIC_PAGES: {unknown}")
    return found


def test_the_page_scan_refuses_a_title_it_cannot_read(tmp_path: Path) -> None:
    probe = tmp_path / "src" / "marketdata" / "probe.py"
    probe.parent.mkdir(parents=True)

    def scan(*lines: str) -> dict[str, str]:
        probe.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return page_titles([probe], tmp_path)

    with pytest.raises(AssertionError, match=r"src/marketdata/probe\.py:3 Probe\.go: page\(\.\.\., title_var"):
        scan("class Probe:",
             "    def go(self, sev: str, title_var: str) -> None:",
             "        self._pager.page(sev, title_var, 'body')")           # a new call site forwarding a variable
    with pytest.raises(AssertionError, match="cannot read"):
        scan("def go(pager: object, n: int) -> None:",
             "    pager.page('info', f'{n} things', 'body')")             # an f-string without a literal head
    with pytest.raises(AssertionError, match="cannot read"):
        scan("def go(pager: object, names: list[str]) -> None:",
             "    for title in names:",
             "        pager.page('info', title, 'body')")                 # a local not bound to a literal
    assert set(scan("def go(pager: object, bad: bool) -> None:",
                    "    title = 'one page'",
                    "    if bad:",
                    "        title = 'other page'",
                    "    pager.page('info', title, 'body')")) == {"one page", "other page"}
    assert scan("def go(pager: object) -> None:",
                "    pager.page('info', title='kw page', body='b')") == {"kw page": "src/marketdata/probe.py"}


def test_the_page_scan_finds_the_known_pages() -> None:
    titles = page_titles()
    assert len(titles) >= 40
    for t in ("redis-md memory high", "broker bars unavailable", "ARM: position SUSPENDED", "ARM lockout violation",
              "ARM configuration tampered", "redis-md memory shedding step"):
        assert t in titles, t


def test_every_page_has_a_runbook_entry() -> None:
    books = (RUNBOOKS / "marketdata.md").read_text(encoding="utf-8") + (RUNBOOKS / "arm.md").read_text(encoding="utf-8")
    titles = dict(page_titles())
    for title, source in DYNAMIC_PAGES:
        assert title in (ROOT / source).read_text(encoding="utf-8"), f"{title!r} no longer built in {source}"
        titles.setdefault(title, source)
    missing = {t: src for t, src in titles.items() if t not in books}
    assert not missing, f"pages without a runbook entry: {missing}"


def test_arm_runbook_failed_section() -> None:
    text = (RUNBOOKS / "arm.md").read_text(encoding="utf-8")
    failed = text.split("\n## FAILED: ", 1)[1].split("\n## ", 1)[0]
    for kind in PROBE_KINDS:
        assert f"| `{kind}` |" in failed, f"probe kind {kind} has no row"
    cli = {"md_backfill": "md backfill", "md_rebuild": "md rebuild", "md_writer_reset": "md writer-reset",
           "md_rehash": "md rehash", "reset_task": "task reset", "arm_repin": "arm repin"}
    assert set(cli) == set(FIRST_COMMANDS), "a new first command needs its CLI line in arm.md"
    for first, command in cli.items():
        assert f"$OPS {command} " in failed, f"{first}: the command to run first is not listed"
    assert CLEAR_COMMAND.removeprefix("python -m src.ops ") in failed
    assert "probe_not_wired" in failed and "cause_present_" in failed


def test_marketdata_runbook_procedures() -> None:
    text = (RUNBOOKS / "marketdata.md").read_text(encoding="utf-8")
    aof = text.split("## AOF aside", 1)[1].split("\n## ", 1)[0]
    stop, move, start = (aof.index("systemctl stop xtnl-redis-md"), aof.index("mv /var/lib/xtnl-redis-md/appendonlydir"),
                         aof.index("systemctl start xtnl-redis-md"))
    assert stop < move < start and "FLUSHALL" not in aof
    rot = text.split("## HMAC key rotation", 1)[1].split("\n## ", 1)[0]
    steps = [rot.index("XTNL_MD_HMAC_KEY_PREV"), rot.index("md rehash"), rot.index("rehash: complete"),
             rot.index("correction window"), rot.index("Retire the previous key")]
    assert steps == sorted(steps), "add the key, re-hash, wait for completion plus one window, then retire"
    for book in ("marketdata.md", "arm.md"):
        body = (RUNBOOKS / book).read_text(encoding="utf-8")
        assert "never sends orders" in body
        assert not re.search(r"redis://md_(writer|reader):[^<@\s]+@", body.replace("<pw>", "").replace("<pw2>", "")), \
            "no password in a runbook URL"


def test_runbooks_require_redis_7_2_for_redis_md() -> None:
    """S37 review: Ubuntu 24.04's own redis-server is 7.0, which rejects the md ACL (+waitaof, +client|setinfo)."""
    assert "+waitaof" in WRITER_RULES and "+client|setinfo" in WRITER_RULES     # the reason for the requirement
    install = (RUNBOOKS / "install-node.md").read_text(encoding="utf-8")
    prereq = install.split("## Prerequisites", 1)[1].split("\n## ", 1)[0]
    assert "Redis 7.4" in prereq and "packages.redis.io" in prereq and "7.2 or newer" in prereq
    assert "redis-server=$REDIS_PKG_VERSION" in prereq and "apt-mark hold redis-server" in prereq
    assert "redis-server --version" in prereq
    assert install.index("packages.redis.io") < install.index("systemctl enable --now etcd patroni redis-server")
    md = (RUNBOOKS / "marketdata.md").read_text(encoding="utf-8")
    enable = md.split("## Enable (AWS", 1)[1].split("\n## ", 1)[0]
    block = enable.split("```", 2)[1]                          # the Enable commands
    commands = [ln.strip() for ln in block.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    assert commands[0].startswith("redis-server --version"), "the version check is the first command of Enable"
    assert "7.2 or newer" in enable
    assert "Redis 7.2 or newer is required" in (DEPLOY / "redis" / "redis-md.conf").read_text(encoding="utf-8")


def test_security_group_opens_6382_for_redis_md() -> None:
    install = (RUNBOOKS / "install-node.md").read_text(encoding="utf-8")
    prereq = install.split("## Prerequisites", 1)[1].split("\n## ", 1)[0]
    sg = prereq.split("* Security groups:", 1)[1].split("\n* ", 1)[0]
    assert "6382" in sg and "VPN_CIDR" in sg and "only once redis-md is enabled" in sg
    enable = (RUNBOOKS / "marketdata.md").read_text(encoding="utf-8").split("## Enable (AWS", 1)[1].split("\n## ", 1)[0]
    assert "open 6382" in enable and "VPN_CIDR" in enable
    assert _sections(DEPLOY / "redis" / "redis-tls-proxy.conf")["xtnl-redis-md"]["accept"].endswith(":6382")


def test_redis_md_password_rotation_order() -> None:
    text = (RUNBOOKS / "marketdata.md").read_text(encoding="utf-8")
    rot = text.split("## redis-md password rotation", 1)[1].split("\n## ", 1)[0]
    secret = rot.index("XTNL_MD_REDIS_URL=redis://md_writer:<pw>@")
    store = rot.index("sudo systemctl restart xtnl-redis-md")
    engine = rot.index("sudo systemctl restart xtnl-engine")
    switch = rot.index("Switch over")
    again = rot.index("Repeat steps 2 and 3 on the former leader")
    assert secret < store < engine < switch < again, "secret, re-render the ACL, restart the engine, then the leader"
    assert "standby first" in rot and "-Rotate" in rot

# ---------------------------------------------------------------------------------------------- diagram
def test_architecture_diagram_page_6_is_generated_and_current(tmp_path: Path) -> None:
    gen = ROOT / "docs" / "architecture" / "generator" / "gen_phase1_arch.py"
    out = tmp_path / "arch.drawio"
    run = subprocess.run([sys.executable, "-I", str(gen), str(out),
                          str(ROOT / "docs" / "architecture" / "generator" / "measured.json")],
                         capture_output=True, text=True, timeout=300, cwd=str(tmp_path))
    assert run.returncode == 0, run.stdout + run.stderr          # no overlap, crossing or edge through a box
    assert "[arm]" in run.stdout and "problem" not in run.stdout
    fresh = out.read_text(encoding="utf-8")
    committed = (ROOT / "docs" / "architecture" / "phase1_ingestion_arch.drawio").read_text(encoding="utf-8")
    assert fresh.replace("\r\n", "\n") == committed.replace("\r\n", "\n"), "regenerate the diagram (CLAUDE.md)"
    page = fresh.split('name="6 · Market data &amp; ARM"', 1)[1]
    for word in ("SM12", "SM11", "redis-md", "BarClock", "arm-tracker", "MarketDataPort", "ReadOnlyTransport",
                 "STRATEGY_COMPLETE", "BACKFILLING"):
        assert word in page, word
