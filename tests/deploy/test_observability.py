"""Off-node alerting assets (operations.md s9, s1, s8, s11, s13; tier-persistence.md s5; IMPLEMENTATION_PLAN 5.5).

* the generated Prometheus rules / CloudWatch alarms / agent declarations are up to date with gen_alerts.py;
* every alert operations.md s9 lists (and the cited sections) has a rule AND a CloudWatch alarm;
* every metric a rule or alarm uses is defined in deploy/observability/metrics.md, with the same type and
  labels as the generator (the contract 5.6 exports), and nothing in metrics.md is undefined;
* inhibition: the epoch-change settling alert suppresses exactly the depends_on_epoch alerts, in both backends;
* every alert has a runbook section. Stdlib only (no YAML library): the generated YAML is checked by its text.
"""
from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
OBS = ROOT / "deploy" / "observability"
RUNBOOK = ROOT / "docs" / "runbooks" / "off-node-alerts.md"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("xtnl_gen_alerts", OBS / "gen_alerts.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


G = _load()


def metrics_md() -> dict[str, tuple[str, tuple[str, ...]]]:
    """`| `name` | type | `l1`, `l2` | role | meaning |` rows of metrics.md -> {name: (type, labels)}."""
    out: dict[str, tuple[str, tuple[str, ...]]] = {}
    for line in (OBS / "metrics.md").read_text(encoding="utf-8").splitlines():
        m = re.match(r"^\|\s*`(xtnl_[a-z0-9_]+)`\s*\|\s*(gauge|counter|histogram)\s*\|\s*([^|]*)\|", line)
        if m:
            labels = tuple(re.findall(r"`([a-z_]+)`", m.group(3)))
            assert m.group(1) not in out, f"{m.group(1)} defined twice in metrics.md"
            out[m.group(1)] = (m.group(2), labels)
    return out


def test_generated_files_are_up_to_date() -> None:
    for path, render in G.OUTPUTS.items():
        assert path.read_text(encoding="utf-8") == render(), f"{path.name}: run deploy/observability/gen_alerts.py"
    assert G.main(["--check"]) == 0


def test_metrics_md_is_exactly_the_generator_contract() -> None:
    md = metrics_md()
    gen = {m.name: (m.type, m.labels) for m in G.DEFINED_METRICS}
    assert md == gen
    # METRICS (the exporter contract) = every defined row minus the ones whose export is still pending
    assert {m.name for m in G.METRICS} == set(md) - G.PENDING_EXPORT
    assert G.PENDING_EXPORT <= set(md)
    for name, (_, labels) in md.items():
        assert labels[0] == "node", f"{name}: every metric carries the exporting node"
        assert not {"account", "account_id", "dsn", "url", "password", "token"} & set(labels), name


def test_every_metric_used_is_defined() -> None:
    md = metrics_md()
    for a in G.ALERTS:
        for m in a.metrics:
            assert m in md or m in G.EXTERNAL_METRICS, f"{a.name} uses undefined metric {m}"
        used_in_expr = set(re.findall(r"\b(xtnl_[a-z0-9_]+)\b", a.expr))
        assert used_in_expr <= set(a.metrics), f"{a.name}: expr uses {used_in_expr - set(a.metrics)}"
        for cw in a.cloudwatch():
            for q in cw.inputs.values():
                name = q["Metric"]["MetricName"]
                assert name in md or name in G.EXTERNAL_METRICS, f"{cw.name} uses undefined {name}"
                if name in md:
                    assert {d["Name"] for d in q["Metric"]["Dimensions"]} == set(md[name][1]), cw.name
    rules = (OBS / "prometheus" / "xtnl-alerts.rules.yml").read_text(encoding="utf-8")
    for name in set(re.findall(r"\b(xtnl_[a-z0-9_]+)\b", rules)):
        assert name in md, f"rules use undefined {name}"


SPEC_ALERTS = (   # operations.md s9 off-node list + the alerts it and s1 / s3 / s8 / s11 / s13 cite
    "no LEADER > 30 s", "two LEADERs", "etcd unhealthy", "Patroni unhealthy", "sync standby missing",
    "epoch-change rate", "non-empty spill file > 60 s", "standby not ready",
    "leader not on the primary's node > 10 min", "etcd quota 50 %", "disk 70 / 85 %", "certificate expiry",
    "PITR horizon broken", "SAFE_HOLD off-node page", "lease-renew p99 > 500 ms", "event-loop lag 100 ms",
    "Redis memory before the limit", "feed lag 50 % of the cap", "inhibition: epoch change, RTO + 30 s")


@pytest.mark.parametrize("requirement", SPEC_ALERTS)
def test_every_spec_alert_has_a_rule_and_a_cloudwatch_alarm(requirement: str) -> None:
    names = G.SPEC_COVERAGE[requirement]
    rules = (OBS / "prometheus" / "xtnl-alerts.rules.yml").read_text(encoding="utf-8")
    alarms = json.loads((OBS / "cloudwatch" / "xtnl-alarms.json").read_text(encoding="utf-8"))["Resources"]
    alarm_names = {r["Properties"]["AlarmName"] for r in alarms.values()}
    by_name = {a.name: a for a in G.ALERTS}
    for n in names:
        assert f"      - alert: {n}\n" in rules
        cw = by_name[n].cloudwatch()
        assert cw and all(c.name in alarm_names for c in cw), n


def test_rule_thresholds_match_the_spec() -> None:
    by = {a.name: a for a in G.ALERTS}
    assert by["XtnlNoLeader"].for_ == "30s" and "< 1" in by["XtnlNoLeader"].expr
    assert "> 1" in by["XtnlTwoLeaders"].expr
    assert by["XtnlSpillFileNonEmpty"].for_ == "60s" and "> 0" in by["XtnlSpillFileNonEmpty"].expr
    assert by["XtnlHealthRecordStale"].for_ == "60s" and "> 5" in by["XtnlHealthRecordStale"].expr
    assert by["XtnlStandbyNotEligible"].for_ == "5m"
    assert by["XtnlLeaderNotOnPrimary"].for_ == "10m"
    assert "> 0.5" in by["XtnlEtcdQuota50"].expr
    assert "> 0.70" in by["XtnlDisk70"].expr and "> 0.85" in by["XtnlDisk85"].expr
    assert "30 * 86400" in by["XtnlCertExpiry"].expr
    assert "> 0.5" in by["XtnlLeaseRenewSlow"].expr and "> 0.1" in by["XtnlEventLoopLag"].expr
    assert "> 500000" in by["XtnlFeedLengthHigh"].expr          # 50 % of MAXLEN ~ 1,000,000
    assert f"< {G.RTO_PLUS_30_S}" in by["XtnlEpochChangeSettling"].expr and G.RTO_PLUS_30_S == 90 + 30
    pitr = by["XtnlPitrHorizonBroken"].expr
    assert "xtnl_wal_archive_failed_total" in pitr and "changes(xtnl_pg_timeline" in pitr
    # the CloudWatch side holds the same thresholds and durations (10 s periods)
    assert by["XtnlNoLeader"].cloudwatch()[0].periods == 3 and by["XtnlNoLeader"].cloudwatch()[0].missing == "breaching"


def test_inhibition_is_wired_in_both_backends() -> None:
    dependent = {a.name for a in G.ALERTS if a.depends_on_epoch}
    assert {"XtnlSyncStandbyMissing", "XtnlReplicationLag", "XtnlFeedPendingOld"} <= dependent
    am = (OBS / "alertmanager" / "alertmanager.yml").read_text(encoding="utf-8")
    assert "source_matchers: ['alertname=\"XtnlEpochChangeSettling\"']" in am
    assert "target_matchers: ['depends_on_epoch=\"true\"']" in am
    assert re.search(r"matchers: \['severity=\"none\"'\]\n\s+receiver: blackhole", am)
    assert "sns_configs" in am and "${XTNL_SNS_CRITICAL_TOPIC_ARN}" in am and "sigv4" in am
    assert "XtnlSafeHold" in am and "repeat_interval: 24h" in am          # one off-node page
    # the epoch-dependent summary keeps severities apart: a dependent warning never pages the critical topic
    dep_route = am.split("- matchers: ['depends_on_epoch=\"true\"']", 1)[1].split("\n    - matchers:", 1)[0]
    assert "group_by: [depends_on_epoch, severity]" in dep_route
    assert re.search(r"matchers: \['severity=\"warning\"'\]\n\s+receiver: sns-warning", dep_route), dep_route
    assert {a.severity for a in G.ALERTS if a.depends_on_epoch} <= {"critical", "warning"}
    res: dict[str, Any] = json.loads((OBS / "cloudwatch" / "xtnl-alarms.json").read_text(encoding="utf-8"))["Resources"]
    all_composites = [r["Properties"] for r in res.values() if r["Type"] == "AWS::CloudWatch::CompositeAlarm"]
    composites = [c for c in all_composites if c["ActionsSuppressor"] == "xtnl-epoch-change-settling"]
    dep_alarms = {c.name for a in G.ALERTS if a.depends_on_epoch for c in a.cloudwatch()}
    assert {c["AlarmRule"] for c in composites} == {f'ALARM("{n}")' for n in dep_alarms}
    # K5: the per-node certificate alarms notify through a composite suppressed by that node's engine-down alarm
    suppressed = {c.name: c.suppressed_by for a in G.ALERTS for c in a.cloudwatch() if c.suppressed_by is not None}
    others = [c for c in all_composites if c["ActionsSuppressor"] != "xtnl-epoch-change-settling"]
    assert {(c["AlarmRule"], c["ActionsSuppressor"]) for c in others} == {
        (f'ALARM("{n}")', s) for n, s in suppressed.items()}
    assert all(s.startswith("xtnl-engine-down-") for s in suppressed.values())
    for r in res.values():   # a dependent / suppressed base alarm and the settling alarm never notify directly
        p = r["Properties"]
        if r["Type"] == "AWS::CloudWatch::Alarm" and (p["AlarmName"] in dep_alarms or p["AlarmName"] in suppressed
                                                       or p["AlarmName"] == "xtnl-epoch-change-settling"):
            assert "AlarmActions" not in p
        elif r["Type"] == "AWS::CloudWatch::Alarm":
            assert p["AlarmActions"], p["AlarmName"]


def test_cloudwatch_template_shape() -> None:
    tpl = json.loads((OBS / "cloudwatch" / "xtnl-alarms.json").read_text(encoding="utf-8"))
    assert tpl["AWSTemplateFormatVersion"] == "2010-09-09"
    assert set(tpl["Parameters"]) == {"SnsCriticalTopicArn", "SnsWarningTopicArn"}
    names = [r["Properties"]["AlarmName"] for r in tpl["Resources"].values()]
    assert len(names) == len(set(names)) and len(tpl["Resources"]) < 500     # CloudFormation resource limit
    for r in tpl["Resources"].values():
        p = r["Properties"]
        if r["Type"] != "AWS::CloudWatch::Alarm":
            continue
        returned = [q for q in p["Metrics"] if q.get("ReturnData")]
        assert len(returned) == 1 and p["TreatMissingData"] in {"breaching", "notBreaching", "missing", "ignore"}
        assert p["EvaluationPeriods"] >= p["DatapointsToAlarm"] >= 1
    agent = json.loads((OBS / "cloudwatch" / "agent-prometheus.json").read_text(encoding="utf-8"))
    decls = agent["logs"]["metrics_collected"]["prometheus"]["emf_processor"]["metric_declaration"]
    assert {d["metric_selectors"][0].strip("^$") for d in decls} == {m.name for m in G.DEFINED_METRICS}


def test_every_alert_has_a_runbook_section() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    headings = set(re.findall(r"^###\s+(\S+)\s*$", text, re.M))
    for a in G.ALERTS:
        assert a.name in headings, f"{a.name} has no section in off-node-alerts.md"
    assert "never sends orders" in text


def test_pitr_guard_takes_an_incremental_backup_on_wal_loss_and_promotion() -> None:
    sh = (OBS / "xtnl-pitr-guard.sh").read_text(encoding="utf-8")
    assert sh.startswith("#!/usr/bin/env bash") and "set -euo pipefail" in sh
    assert "pgbackrest --stanza=\"$STANZA\" --type=incr backup" in sh
    assert "pg_stat_archiver" in sh and "pg_control_checkpoint()" in sh and "on_role_change)" in sh
    assert "flock -n 9" in sh and "pg_is_in_recovery()" in sh          # one at a time, on the primary only
    for forbidden in ("place_order", "cancel_order", "modify_order", "pgbackrest --stanza=\"$STANZA\" restore", "--delta", "DROP ", "rm -"):
        assert forbidden not in sh
    patroni = (ROOT / "deploy" / "patroni" / "patroni.yml").read_text(encoding="utf-8")
    assert re.search(r"^  callbacks:\n    on_role_change: /usr/local/bin/xtnl-pitr-guard$", patroni, re.M)
    unit = (ROOT / "deploy" / "systemd" / "xtnl-pitr-guard.service").read_text(encoding="utf-8")
    assert "ExecStart=/usr/local/bin/xtnl-pitr-guard check" in unit and "User=postgres" in unit
    timer = (ROOT / "deploy" / "systemd" / "xtnl-pitr-guard.timer").read_text(encoding="utf-8")
    assert "OnUnitInactiveSec=60s" in timer and "WantedBy=timers.target" in timer
    tmpfiles = (ROOT / "deploy" / "systemd" / "xtnl.tmpfiles.conf").read_text(encoding="utf-8")
    assert re.search(r"^d\s+/var/lib/xtnl-pitr-guard\s+0750\s+postgres\s+postgres", tmpfiles, re.M)


# ============================================================================================ PITR guard (behaviour)
def _posix_bash() -> str | None:
    """A POSIX bash that runs the guard script directly: Git for Windows' bash, or bash on Linux/macOS.
    Never the WSL launcher in System32, which would run in another filesystem."""
    import shutil

    if sys.platform != "win32":
        return shutil.which("bash")
    git = shutil.which("git")
    if git is None:
        return None
    for root in Path(git).resolve().parents[:3]:          # Git/cmd/git.exe or Git/mingw64/bin/git.exe
        for cand in (root / "bin" / "bash.exe", root / "usr" / "bin" / "bash.exe"):
            if cand.exists():
                return str(cand)
    return None


_STUBS = {
    # psql -X -q -A -t -h ... -c '<sql>': answers from the STUB_* environment
    "psql": """#!/usr/bin/env bash
sql=""; while [ $# -gt 0 ]; do if [ "$1" = "-c" ]; then sql="$2"; shift; fi; shift; done
case "$sql" in
  *pg_is_in_recovery*) echo "${STUB_PRIMARY:-t}" ;;   # SELECT NOT pg_is_in_recovery()
  *failed_count*) echo "$STUB_FAILED" ;;
  *timeline_id*) echo "$STUB_TLI" ;;
esac
""",
    "pgbackrest": """#!/usr/bin/env bash
echo "$*" >>"$STUB_LOG"
exit "${STUB_BACKUP_RC:-0}"
""",
    "flock": "#!/usr/bin/env bash\nexit 0\n",
    "logger": "#!/usr/bin/env bash\nexit 0\n",
}


def test_pitr_guard_retries_a_failed_backup_on_the_next_run(tmp_path: Path) -> None:
    """operations.md s8: a dropped WAL triggers an immediate incremental backup. A backup that fails must be
    retried by the next timer run: the guard records the new failed_count / timeline only after the backup
    succeeded (it used to record them first, so a failed backup was never retried)."""
    import subprocess

    bash = _posix_bash()
    if bash is None:
        pytest.skip("no POSIX bash to run deploy/observability/xtnl-pitr-guard.sh")
    stubs = tmp_path / "stubs"
    stubs.mkdir()
    for name, body in _STUBS.items():
        (stubs / name).write_bytes(body.encode("utf-8"))
        (stubs / name).chmod(0o755)
    state = tmp_path / "state"
    log = tmp_path / "pgbackrest.log"
    script = OBS / "xtnl-pitr-guard.sh"

    def run(failed: int, tli: int, rc: int = 0) -> int:
        cmd = ('to_posix() { if command -v cygpath >/dev/null 2>&1; then cygpath -u "$1"; else echo "$1"; fi; }; '
               'export PATH="$(to_posix "$STUB_DIR"):$PATH"; '
               'export XTNL_PITR_STATE_DIR="$(to_posix "$STATE_WIN")" STUB_LOG="$(to_posix "$LOG_WIN")"; '
               'bash "$(to_posix "$SCRIPT_WIN")" check')
        env = {"PATH": __import__("os").environ.get("PATH", ""), "STUB_DIR": str(stubs), "STATE_WIN": str(state),
               "LOG_WIN": str(log), "SCRIPT_WIN": str(script), "STUB_FAILED": str(failed), "STUB_TLI": str(tli),
               "STUB_BACKUP_RC": str(rc)}
        for k in ("SYSTEMROOT", "TEMP", "TMP", "HOME", "USERPROFILE"):
            v = __import__("os").environ.get(k)
            if v is not None:
                env[k] = v
        return subprocess.run([bash, "-c", cmd], env=env, capture_output=True, timeout=60).returncode

    def backups() -> int:
        return len(log.read_text(encoding="utf-8").splitlines()) if log.exists() else 0

    assert run(0, 1) == 0 and backups() == 0                       # first run: baseline only
    assert (state / "failed_count").read_text().strip() == "0"
    assert run(1, 1, rc=1) != 0 and backups() == 1                 # archive failure -> backup, which fails
    assert (state / "failed_count").read_text().strip() == "0"     # baseline kept: the next run retries
    assert run(1, 1) == 0 and backups() == 2                       # retried, succeeds
    assert (state / "failed_count").read_text().strip() == "1"
    assert run(1, 1) == 0 and backups() == 2                       # nothing new: no backup
    assert run(1, 2) == 0 and backups() == 3                       # timeline switch -> backup
    assert "--type=incr backup" in log.read_text(encoding="utf-8")


# ============================================================================================ 7.3
def test_frames_lost_row_is_defined_for_the_orchestrator_counter() -> None:
    """7.3: `xtnl_frames_lost_total` (IngestionOrchestrator.frames_lost, per stream, summed over adapters) is a
    metrics.md row; 7.4 exports it (8.2 drops it from PENDING_EXPORT). No alert may use a pending metric."""
    md = metrics_md()
    assert md["xtnl_frames_lost_total"] == ("counter", ("node", "stream"))
    row = next(ln for ln in (OBS / "metrics.md").read_text(encoding="utf-8").splitlines()
               if ln.startswith("| `xtnl_frames_lost_total`"))
    assert "frames_lost" in row and "| leader |" in row
    for a in G.ALERTS:
        assert not set(a.metrics) & G.PENDING_EXPORT, f"{a.name} uses a metric the engine does not export yet"
        assert not set(re.findall(r"(xtnl_[a-z0-9_]+)", a.expr)) & G.PENDING_EXPORT, a.name
    decl = json.loads((OBS / "cloudwatch" / "agent-prometheus.json").read_text(encoding="utf-8"))
    selectors = {sel for d in decl["logs"]["metrics_collected"]["prometheus"]["emf_processor"]["metric_declaration"]
                 for sel in d["metric_selectors"]}
    assert "^xtnl_frames_lost_total$" in selectors        # published as soon as 7.4 exports it


def test_frames_lost_is_in_the_exported_contract_not_pending() -> None:
    """8.2: the engine exports `xtnl_frames_lost_total` since 7.4, so the contract lists it as exported: it is in
    `METRICS`, nothing is pending, and its metrics.md row no longer says "export pending"."""
    assert "xtnl_frames_lost_total" in G.METRIC_NAMES
    assert G.PENDING_EXPORT == frozenset()
    assert {m.name for m in G.METRICS} == {m.name for m in G.DEFINED_METRICS}
    row = next(ln for ln in (OBS / "metrics.md").read_text(encoding="utf-8").splitlines()
               if ln.startswith("| `xtnl_frames_lost_total`"))
    assert "pending" not in row.lower() and "Exported by the leader" in row


def test_witness_disk_alarms_read_exactly_what_the_witness_agent_publishes() -> None:
    """7.3 / operations.md §11: the witness disk alarms (70 / 85 %) had no data source. The witness agent config
    publishes CWAgent disk_used_percent with exactly the dimensions {host=witness, path} of every witness mount
    (root + the etcd member's own volume); every witness alarm reads one of those series."""
    agent = json.loads((OBS / "cloudwatch" / "agent-witness.json").read_text(encoding="utf-8"))
    assert agent["agent"]["omit_hostname"] is True and agent["agent"]["run_as_user"] == "cwagent"
    disk = agent["metrics"]["metrics_collected"]["disk"]
    assert agent["metrics"]["namespace"] == "CWAgent"
    assert disk["measurement"] == ["used_percent"] and disk["drop_device"] is True
    assert set(disk["resources"]) == set(G.WITNESS_MOUNTS) == {"/", "/var/lib/etcd"}
    assert disk["append_dimensions"] == {"host": G.WITNESS_HOST}
    assert agent["metrics"]["aggregation_dimensions"] == [["host", "path"]]
    published = {("CWAgent", "disk_used_percent", (("host", G.WITNESS_HOST), ("path", mt))) for mt in disk["resources"]}
    by = {a.name: a for a in G.ALERTS}
    seen: dict[int, set[str]] = {70: set(), 85: set()}
    for name, pct in (("XtnlWitnessDisk70", 70), ("XtnlWitnessDisk85", 85)):
        for cw in by[name].cloudwatch():
            (q,) = cw.inputs.values()
            m = q["Metric"]
            key = (m["Namespace"], m["MetricName"], tuple(sorted((d["Name"], d["Value"]) for d in m["Dimensions"])))
            assert key in published, cw.name
            assert cw.threshold == pct and cw.op == "GreaterThanThreshold"
            seen[pct].add(dict(key[2])["path"])
            # a silent witness agent must not leave both alarms in INSUFFICIENT_DATA forever
            assert cw.missing == ("breaching" if pct == 70 else "missing"), cw.name
    assert seen[70] == seen[85] == set(G.WITNESS_MOUNTS)
    # the witness ships its own log lines (the defrag job) off-node
    files = agent["logs"]["logs_collected"]["files"]["collect_list"]
    assert [f["file_path"] for f in files] == ["/var/log/xtnl/xtnl-etcd-defrag.log"]
    install = (ROOT / "docs" / "runbooks" / "install-node.md").read_text(encoding="utf-8")
    assert "cloudwatch/agent-witness.json" in install
