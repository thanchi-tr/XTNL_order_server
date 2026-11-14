"""Storage / ops jobs of IMPLEMENTATION_PLAN 6.4: scheduled pgBackRest backups and the rolling etcd defrag
(operations.md §8, §1), log shipping and the CloudWatch metrics path (§9), the Melbourne dead-man receiver unit.

The two job scripts run for real under a POSIX bash with stubbed psql / pgbackrest / etcdctl (no servers, no
network); they are skipped where no such bash exists.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

import pytest

from src.observability.exporter import DEFAULT_METRICS_PORT
from tests.deploy.test_deploy_assets import read_unit, words
from tests.deploy.test_observability import G, _posix_bash

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy"
JOBS = DEPLOY / "jobs"
LOGS = DEPLOY / "observability" / "logs"


# ============================================================================================ static checks
def test_backup_unit_and_timer_are_daily_on_both_members() -> None:
    cp, _ = read_unit("xtnl-pg-backup.service")
    svc = cp["Service"]
    assert svc["Type"] == "oneshot" and svc["User"] == "postgres"
    assert svc["ExecStart"] == "/usr/local/bin/xtnl-pg-backup auto"
    assert svc["SyslogIdentifier"] == "xtnl-pg-backup"
    timer, _ = read_unit("xtnl-pg-backup.timer")
    assert re.fullmatch(r"\*-\*-\* \d\d:\d\d:00 UTC", timer["Timer"]["OnCalendar"])     # every day
    assert timer["Timer"]["Persistent"] == "true" and timer["Install"]["WantedBy"] == "timers.target"
    sh = (JOBS / "xtnl-pg-backup.sh").read_text(encoding="utf-8")
    assert sh.startswith("#!/usr/bin/env bash") and "set -euo pipefail" in sh
    code = "\n".join(line.split("#", 1)[0] for line in sh.split("set -euo pipefail", 1)[1].splitlines())
    assert not re.search(r"pgbackrest[^\n]*\brestore\b", code)                       # never restores
    for forbidden in ("--delta", "DROP ", "rm -", "place_order", "cancel_order", "modify_order"):
        assert forbidden not in code, forbidden


def test_defrag_unit_and_timer_are_weekly_and_rolling() -> None:
    cp, _ = read_unit("xtnl-etcd-defrag.service")
    svc = cp["Service"]
    assert svc["Type"] == "oneshot" and svc["ExecStart"] == "/usr/local/bin/xtnl-etcd-defrag run"
    assert svc["EnvironmentFile"] == "/etc/xtnl/etcd-defrag.env"
    timer, _ = read_unit("xtnl-etcd-defrag.timer")
    assert timer["Timer"]["OnCalendar"].startswith("Sat ")                 # weekly, FX closed
    sh = (JOBS / "xtnl-etcd-defrag.sh").read_text(encoding="utf-8")
    assert "lock --ttl=" in sh and "move-leader" in sh and 'MEMBERS=3' in sh
    assert '--endpoints="$SELF" --command-timeout=120s defrag' in sh      # only this member, never --cluster
    assert "--cluster" not in sh
    etcd_conf = (DEPLOY / "etcd" / "etcd.conf.yml").read_text(encoding="utf-8")
    assert "xtnl-etcd-defrag.timer" in etcd_conf
    assert "xtnl-pg-backup.timer" in (DEPLOY / "patroni" / "pgbackrest.conf").read_text(encoding="utf-8")


def test_engine_logs_are_shipped_off_node() -> None:
    """journald -> rsyslog -> /var/log/xtnl/<ident>.log -> CloudWatch agent -> CloudWatch Logs."""
    rsyslog = (LOGS / "30-xtnl-rsyslog.conf").read_text(encoding="utf-8")
    m = re.search(r"\$programname == \[([^\]]+)\]", rsyslog)
    assert m is not None
    idents = {x.strip().strip('"') for x in m.group(1).split(",")}
    assert idents == {ident for ident, _, _ in G.LOG_FILES}
    assert 'string="/var/log/xtnl/%programname%.log"' in rsyslog and "stop" in rsyslog
    # rsyslog's 8 KiB default would cut a JSON line carrying a traceback; must match journald's LineMax
    assert 'global(maxMessageSize="64k")' in rsyslog
    # the agent runs as cwagent: the files and their directory must be readable through group adm, and the
    # install step must put cwagent in that group
    assert 'fileGroup="adm"' in rsyslog and 'dirGroup="adm"' in rsyslog and 'fileCreateMode="0640"' in rsyslog
    install = (ROOT / "docs" / "runbooks" / "install-node.md").read_text(encoding="utf-8")
    assert "usermod -aG adm cwagent" in install and "-o syslog -g adm /var/log/xtnl" in install
    for unit, ident in (("xtnl-engine.service", "xtnl-engine"), ("xtnl-migrate.service", "xtnl-migrate"),
                        ("xtnl-pg-backup.service", "xtnl-pg-backup"),
                        ("xtnl-etcd-defrag.service", "xtnl-etcd-defrag")):
        cp, _ = read_unit(unit)
        assert cp["Service"]["SyslogIdentifier"] == ident and ident in idents
    guard = (DEPLOY / "observability" / "xtnl-pitr-guard.sh").read_text(encoding="utf-8")
    assert "logger -t xtnl-pitr-guard" in guard and "xtnl-pitr-guard" in idents
    agent = json.loads((DEPLOY / "observability" / "cloudwatch" / "agent-prometheus.json").read_text(encoding="utf-8"))
    shipped = {f["file_path"]: f["log_group_name"] for f in agent["logs"]["logs_collected"]["files"]["collect_list"]}
    assert shipped["/var/log/xtnl/xtnl-engine.log"] == "/xtnl/engine"
    assert set(shipped) == {f"/var/log/xtnl/{i}.log" for i in idents}
    journald = (LOGS / "journald-xtnl.conf").read_text(encoding="utf-8")
    assert "ForwardToSyslog=yes" in journald
    rotate = (LOGS / "xtnl.logrotate").read_text(encoding="utf-8")
    assert "/var/log/xtnl/*.log" in rotate and "rsyslog-rotate" in rotate


def test_cloudwatch_metrics_path_is_deployable() -> None:
    agent = json.loads((DEPLOY / "observability" / "cloudwatch" / "agent-prometheus.json").read_text(encoding="utf-8"))
    path = agent["logs"]["metrics_collected"]["prometheus"]["prometheus_config_path"]
    assert path == G.CW_SCRAPE_INSTALLED
    scrape = (DEPLOY / "observability" / "cloudwatch" / "xtnl-prometheus.yaml").read_text(encoding="utf-8")
    assert scrape == G.render_scrape()
    assert "job_name: xtnl-engine" in scrape and f"'${{NODE_IP}}:{DEFAULT_METRICS_PORT}'" in scrape
    assert G.SCRAPE_PORT == DEFAULT_METRICS_PORT
    assert re.search(r"^  scrape_interval: 5s$", scrape, re.M)          # metrics.md: every 5 s
    decl = agent["logs"]["metrics_collected"]["prometheus"]["emf_processor"]["metric_declaration"]
    assert all(d["label_matcher"] == "^xtnl-engine$" for d in decl)


def test_deadman_receiver_unit_is_a_melbourne_service() -> None:
    cp, multi = read_unit("xtnl-deadman-receiver.service")
    svc = cp["Service"]
    assert svc["ExecStart"].endswith("python -m src.deadman_receiver")
    assert svc["ExecStartPre"].endswith("python -m src.deadman_receiver --check-config")
    assert svc["RestartPreventExitStatus"] == "78" and svc["Restart"] == "always"
    assert svc["DynamicUser"] == "yes" and svc["EnvironmentFile"] == "/etc/xtnl/deadman-receiver.env"
    assert {"XTNL_RECEIVER_TLS_CERT=%d/tls.crt", "XTNL_RECEIVER_TLS_KEY=%d/tls.key"} <= set(multi["Service"]["Environment"])
    assert words(multi["Service"]["LoadCredential"]) >= {"tls.crt:/etc/xtnl/deadman-receiver/tls.crt",
                                                         "tls.key:/etc/xtnl/deadman-receiver/tls.key"}
    text = (DEPLOY / "systemd" / "xtnl-deadman-receiver.service").read_text(encoding="utf-8")
    assert "MELBOURNE" in text and "OUTSIDE\n# eu-west-2" in text
    readme = (DEPLOY / "README.md").read_text(encoding="utf-8")
    for name in ("xtnl-deadman-receiver.service", "deadman-receiver.env.example", "xtnl-pg-backup", "xtnl-etcd-defrag",
                 "30-xtnl-rsyslog.conf", "xtnl-prometheus.yaml"):
        assert name in readme, name


# ============================================================================================ behaviour
_COMMON_STUBS = {"flock": "#!/usr/bin/env bash\nexit 0\n", "logger": "#!/usr/bin/env bash\nexit 0\n",
                 "sleep": "#!/usr/bin/env bash\nexit 0\n"}


def _run(tmp_path: Path, script: Path, args: str, stubs: dict[str, str], env: dict[str, str]) -> tuple[int, str]:
    bash = _posix_bash()
    if bash is None:
        pytest.skip("no POSIX bash to run the deploy job scripts")
    d = tmp_path / "stubs"
    d.mkdir(exist_ok=True)
    for name, body in {**_COMMON_STUBS, **stubs}.items():
        (d / name).write_bytes(body.encode("utf-8"))
        (d / name).chmod(0o755)
    log = tmp_path / "calls.log"
    cmd = ('to_posix() { if command -v cygpath >/dev/null 2>&1; then cygpath -u "$1"; else echo "$1"; fi; }; '
           'export PATH="$(to_posix "$STUB_DIR"):$PATH"; export STUB_LOG="$(to_posix "$LOG_WIN")"; '
           'export XTNL_BACKUP_STATE_DIR="$(to_posix "$STATE_WIN")"; '
           f'bash "$(to_posix "$SCRIPT_WIN")" {args}')
    full = {"PATH": os.environ.get("PATH", ""), "STUB_DIR": str(d), "LOG_WIN": str(log), "SCRIPT_WIN": str(script),
            "STATE_WIN": str(tmp_path / "state"), **env}
    for k in ("SYSTEMROOT", "TEMP", "TMP", "HOME", "USERPROFILE"):
        if k in os.environ:
            full[k] = os.environ[k]
    rc = subprocess.run([bash, "-c", cmd], env=full, capture_output=True, timeout=60).returncode
    return rc, log.read_text(encoding="utf-8") if log.exists() else ""


_PG_STUBS = {
    "psql": '#!/usr/bin/env bash\necho "${STUB_PRIMARY:-t}"\n',
    "pgbackrest": ('#!/usr/bin/env bash\necho "pgbackrest $*" >>"$STUB_LOG"\n'
                   'n=$(grep -c pgbackrest "$STUB_LOG"); [ "$n" -gt "${STUB_FAIL_FIRST:-0}" ]\n'),
}


def test_backup_runs_on_the_primary_only(tmp_path: Path) -> None:
    script = JOBS / "xtnl-pg-backup.sh"
    rc, calls = _run(tmp_path, script, "incr", _PG_STUBS, {"STUB_PRIMARY": "f"})
    assert rc == 0 and calls == ""                                         # the replica's timer does nothing
    rc, calls = _run(tmp_path, script, "incr", _PG_STUBS, {"STUB_PRIMARY": "t"})
    assert rc == 0 and calls.strip() == "pgbackrest --stanza=xtnl --type=incr backup"


def test_backup_auto_is_full_on_the_full_day_else_incremental(tmp_path: Path) -> None:
    script = JOBS / "xtnl-pg-backup.sh"
    from datetime import UTC, datetime
    today = str(datetime.now(UTC).isoweekday())
    other = "1" if today != "1" else "2"
    rc, calls = _run(tmp_path, script, "auto", _PG_STUBS, {"XTNL_BACKUP_FULL_DOW": today})
    assert rc == 0 and "--type=full backup" in calls
    (tmp_path / "calls.log").unlink()
    rc, calls = _run(tmp_path, script, "auto", _PG_STUBS, {"XTNL_BACKUP_FULL_DOW": other})
    assert rc == 0 and "--type=incr backup" in calls


def test_failed_backup_is_retried_then_fails_the_unit(tmp_path: Path) -> None:
    script = JOBS / "xtnl-pg-backup.sh"
    rc, calls = _run(tmp_path, script, "incr", _PG_STUBS, {"STUB_FAIL_FIRST": "1", "XTNL_BACKUP_RETRY_S": "0"})
    assert rc == 0 and len(calls.splitlines()) == 2                         # lock held by the guard, then ok
    (tmp_path / "calls.log").unlink()
    rc, calls = _run(tmp_path, script, "incr", _PG_STUBS, {"STUB_FAIL_FIRST": "9", "XTNL_BACKUP_ATTEMPTS": "3",
                                                             "XTNL_BACKUP_RETRY_S": "0"})
    assert rc != 0 and len(calls.splitlines()) == 3
    rc, _ = _run(tmp_path, script, "bogus", _PG_STUBS, {})
    assert rc == 2


_SELF = "https://10.0.1.10:2379"
_ETCDCTL = r'''#!/usr/bin/env bash
eps=""; args=()
for a in "$@"; do case "$a" in --endpoints=*) eps="${a#--endpoints=}";; --command-timeout=*|--ttl=*) ;; *) args+=("$a");; esac; done
set -- "${args[@]}"
case "$1" in
  lock) shift 2; echo "lock" >>"$STUB_LOG"; exec bash "$@" ;;
  endpoint)
    if [ "$2" = "health" ]; then
      if [ "$eps" = "$XTNL_ETCD_SELF_ENDPOINT" ]; then exit 0; fi
      for i in $(seq 1 "${STUB_HEALTHY:-3}"); do echo "https://10.0.$i.10:2379 is healthy: successfully committed proposal: took = 2ms"; done
      [ "${STUB_HEALTHY:-3}" -ge 3 ]
    elif [ "$3" = "-w" ] && [ "$4" = "fields" ]; then
      # fields format (etcd 3.4-3.6): the answering member's id and the leader's id, decimal
      [ "$eps" = "$XTNL_ETCD_SELF_ENDPOINT" ] || exit 9
      printf '"ClusterID" : 1\n"MemberID" : 11\n"Revision" : 5\n"RaftTerm" : 2\n"Version" : "3.6.4"\n'
      if [ "${STUB_SELF_LEADER:-false}" = true ]; then echo '"Leader" : 11'; else echo '"Leader" : 33'; fi
      printf '"IsLearner" : false\n"Endpoint" : "%s"\n' "$eps"
    else
      # simple format of etcd 3.6 (storage version / db size / in use / % / quota precede "is leader"): a
      # positional parse of this table must never be what decides leadership
      echo "https://10.0.1.10:2379, aaaa, 3.6.4, 3.6.0, 25 MB, 20 MB, 20%, 2.1 GB, ${STUB_SELF_LEADER:-false}, false, 2, 9, 9, , , false"
    fi ;;
  member)
    echo "aaaa, started, node-a, https://10.0.1.10:2380, https://10.0.1.10:2379,https://127.0.0.1:2379, false"
    echo "dddd, started, learner, https://10.0.4.10:2380, https://10.0.4.10:2379, true"
    echo "bbbb, started, node-b, https://10.0.2.10:2380, https://10.0.2.10:2379, false"
    echo "cccc, started, witness, https://10.0.3.10:2380, https://10.0.3.10:2379, false" ;;
  move-leader) echo "move-leader $2 via $eps" >>"$STUB_LOG" ;;
  defrag) echo "defrag $eps" >>"$STUB_LOG" ;;
  alarm) ;;
esac
'''


def _defrag(tmp_path: Path, **env: str) -> tuple[int, str]:
    script = JOBS / "xtnl-etcd-defrag.sh"
    base = {"XTNL_ETCD_SELF_ENDPOINT": _SELF, "XTNL_ETCD_DEFRAG_SETTLE_S": "0",
            "XTNL_ETCD_ENDPOINTS": "https://10.0.1.10:2379,https://10.0.2.10:2379,https://10.0.3.10:2379"}
    stubs = {"etcdctl": _ETCDCTL}
    return _run(tmp_path, script, "run", stubs, {**base, **env})


def test_defrag_takes_the_lock_and_defrags_only_this_member(tmp_path: Path) -> None:
    rc, calls = _defrag(tmp_path)
    assert rc == 0 and calls.splitlines() == ["lock", f"defrag {_SELF}"]


def test_defrag_moves_leadership_away_first(tmp_path: Path) -> None:
    rc, calls = _defrag(tmp_path, STUB_SELF_LEADER="true")
    assert rc == 0 and calls.splitlines() == ["lock", f"move-leader bbbb via {_SELF}", f"defrag {_SELF}"]


def test_unreadable_leadership_means_no_defrag(tmp_path: Path) -> None:
    """An endpoint-status answer without MemberID / Leader (an etcdctl failure, an unknown format) never lets a
    possible leader defragment itself."""
    bad = _ETCDCTL.replace("\"MemberID\" : 11", "\"Member\" : 11")
    script = JOBS / "xtnl-etcd-defrag.sh"
    base = {"XTNL_ETCD_SELF_ENDPOINT": _SELF, "XTNL_ETCD_DEFRAG_SETTLE_S": "0",
            "XTNL_ETCD_ENDPOINTS": "https://10.0.1.10:2379,https://10.0.2.10:2379,https://10.0.3.10:2379"}
    rc, calls = _run(tmp_path, script, "run", {"etcdctl": bad}, base)
    assert rc != 0 and calls.splitlines() == ["lock"]


def test_no_defrag_unless_three_of_three_healthy(tmp_path: Path) -> None:
    rc, calls = _defrag(tmp_path, STUB_HEALTHY="2")
    assert rc != 0 and calls.splitlines() == ["lock"]
