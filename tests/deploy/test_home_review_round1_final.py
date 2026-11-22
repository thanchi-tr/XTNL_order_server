"""Home-server review round 1 (final fixes): backup job, receiver runbook and unit, live-view design.

1. A failure after the night's dump is in daily/<ts>/ (the monthly copy, the archive listing or an archive upload)
   never turns check C red: the job pings success as soon as the daily upload is done, then exits 66; the
   wrapper logs an ALERT: line for 66 and pings nothing; a timeout after `DUMP SAFE:` is 66 too. Restore step 2
   counts such nights as good.
2. wait_pg retries pg_isready (one attempt each) every 10 s for up to 5 min, so a run started right after a reboot
   waits for postgres instead of exiting 69 at once.
3. The home receiver runbook tags the home PC and the VPS, gives a concrete policy (home PC: tcp:443 only; SSH
   only from your own untagged devices, never root) and checks from the home PC that SSH is refused.
4. The quarterly drill's clean-up removes the decrypted archive files from C:\\xtnl\\archive on the spare.
5. The receiver units run --check-config with a '-' prefix, so a config error stops at the main process's 78
   instead of restarting every 5 s; the runbook says so.
6. The stop-gap placeholder pages once per engine start more than 6 h after the previous page, and episodes are
   per receiver host.
7. docs/design/live-view.md lists the W2 refused names and prefixes, and its exact drop list is the script's.

The job runs under a POSIX sh with stub tools (tests/deploy/test_home_backup_round1.py `rig`); nothing runs Docker,
PowerShell, Tailscale, systemd, B2 or Task Scheduler.
"""
from __future__ import annotations

import fnmatch
import json
import re
from pathlib import Path

from src.deadman_receiver.__main__ import EX_CONFIG
from src.deadman_receiver.__main__ import main as receiver_main
from tests.deploy.test_deploy_assets import read_unit
from tests.deploy.test_home_backup import BACKUP_PS1, ENTRYPOINT, RUNBOOK, _ps_code
from tests.deploy.test_home_backup_round1 import _stdin, rig  # noqa: F401  (fixture)

ROOT = Path(__file__).resolve().parents[2]
HOME_RUNBOOK = ROOT / "docs" / "runbooks" / "deadman-receiver-home.md"
LIVE_ENV = ROOT / "deploy" / "env" / "local-live.env.example"
LIVEVIEW_DESIGN = ROOT / "docs" / "design" / "live-view.md"
START = ROOT / "deploy" / "local" / "start-xtnl.ps1"
HC = "https://hc.example/ping/check-c-uuid"


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _section(text: str, start: str, end: str) -> str:
    return text[text.index(start):text.index(end, text.index(start))]


def _pings(tmp: Path) -> list[str]:
    log = tmp / "curl.log"
    return log.read_text(encoding="utf-8").split() if log.exists() else []


def _daily(tmp: Path) -> list[str]:
    root = tmp / "b2" / "xtnl-bucket"
    return [p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
            and p.relative_to(root).as_posix().startswith("daily/")]


def _wrap_rclone(tmp: Path, fail_case: str) -> None:
    """Put a wrapper in front of the stub rclone that fails (exit 1) when `$*` matches the sh case pattern."""
    stubs = tmp / "bin"
    (stubs / "rclone").rename(stubs / "rclone-real")
    (stubs / "rclone").write_text(
        "#!/bin/sh\n"
        f'case "$*" in {fail_case}) exit 1 ;; esac\n'
        'exec "$(dirname "$0")/rclone-real" "$@"\n', encoding="utf-8", newline="\n")
    (stubs / "rclone").chmod(0o755)


def _export(tmp: Path) -> None:
    import hashlib
    part = tmp / "archive" / "event_log" / "event_log_p202607.jsonl.gz"
    part.write_bytes(b"rows\n")
    digest = hashlib.sha256(b"rows\n").hexdigest()
    (part.parent / (part.name + ".sha256")).write_text(f"{digest}  {part.name}\n", encoding="ascii", newline="\n")


# ---------------------------------------------------------------------------------------------- 1: late failures
def _assert_late_failure(tmp: Path, r) -> None:  # type: ignore[no-untyped-def]
    out = r.stdout + r.stderr
    assert r.returncode == 66, out
    assert "DUMP SAFE: dump and globals uploaded to daily/" in out and "LATE FAILURE" in out
    assert "backup FAILED" not in out
    assert _pings(tmp) == []                     # the job never pings (review round 2); backup-pg.ps1 keeps success
    assert any(x.endswith(".dump.age") for x in _daily(tmp)) and any(x.endswith(".dump.age.sha256") for x in _daily(tmp))
    assert not any((tmp / "work").iterdir()) and not any((tmp / "out").iterdir())


def test_a_failed_monthly_listing_after_the_daily_upload_is_exit_66_with_check_c_green(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    _wrap_rclone(tmp, "lsf*monthly/*")
    _assert_late_failure(tmp, run(_stdin()))


def test_a_failed_archive_listing_is_exit_66(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    _wrap_rclone(tmp, "lsf*/archive/*")
    _assert_late_failure(tmp, run(_stdin()))


def test_a_failed_archive_upload_is_exit_66(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    _export(tmp)
    _wrap_rclone(tmp, "copyto*/archive/*")
    _assert_late_failure(tmp, run(_stdin()))


def test_a_failed_daily_upload_still_pings_fail_and_exits_75(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    _wrap_rclone(tmp, "copyto*/daily/*")
    r = run(_stdin())
    out = r.stdout + r.stderr
    assert r.returncode == 75, out
    assert "DUMP SAFE" not in out and "backup FAILED" in out
    assert _pings(tmp) == []                     # backup-pg.ps1 pings /fail for 75 (review round 2)


def test_a_good_night_pings_success_once_right_after_the_daily_upload(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    r = run(_stdin())
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert _pings(tmp) == []                     # backup-pg.ps1 pings success on the DUMP SAFE line (review round 2)
    assert out.count("DUMP SAFE:") == 1 and out.index("DUMP SAFE:") < out.index("partition archive:")


def test_wrapper_treats_66_like_65_and_a_timeout_after_dump_safe_pings_nothing() -> None:
    code = "\n".join(_ps_code(BACKUP_PS1))
    assert ("if ($script:WatchDumpSafe -and -not $script:DumpSafe -and $line -match 'xtnl-backup: DUMP SAFE: ') {"
            in code)
    i66 = code.index("if ($code -eq 66) {")
    block = code[i66:code.index("}", i66)]
    assert "Exit-Backup 66 'ALERT:" in block and "-Ping" not in block
    timeout = code[code.index("if ($code -eq -1) {"):code.index("if ($code -eq 0) {")]
    lines = timeout.splitlines()
    i_safe = next(i for i, ln in enumerate(lines) if "if ($script:DumpSafe) {" in ln)
    assert lines[i_safe + 1].strip().startswith("Exit-Backup 66 (") and not lines[i_safe + 1].rstrip().endswith("-Ping")
    assert lines[i_safe + 2].strip() == "}"
    assert lines[i_safe + 3].strip().startswith("Exit-Backup 75 (") and lines[i_safe + 3].rstrip().endswith("-Ping")
    assert "$GoodJobCodes = @(0, 65, 66)" in code                             # 66 is not a /fail code


def test_runbook_documents_exit_66_and_restore_step_2_takes_its_dump() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    row66 = next(ln for ln in text.splitlines() if ln.startswith("| 66 |"))
    for needle in ("**were** uploaded to `daily/<ts>/`", "`DUMP SAFE:`", "LATE FAILURE", "pings nothing",
                   "counts for Restore step 2"):
        assert needle in row66, needle
    which = _flat(_section(text, "**Which backup**", "Keep this shell open for step 3"))
    good = which[which.index("records as good."):]
    for needle in ("success ping after its `/start`", "`backup-pg exit 66`", "counts as good",
                   "does not undo it"):
        assert needle in good, needle


# ---------------------------------------------------------------------------------------------- 2: wait_pg
def _stub_pg(tmp: Path, down_times: int) -> None:
    stubs = tmp / "bin"
    (stubs / "pg_isready").write_text(
        "#!/bin/sh\n"
        f'n=$(cat "{_p(tmp)}/pg.count" 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" > "{_p(tmp)}/pg.count"\n'
        f'[ "$n" -gt {down_times} ] || exit 2\n', encoding="utf-8", newline="\n")
    (stubs / "sleep").write_text(f'#!/bin/sh\necho "$1" >> "{_p(tmp)}/sleep.log"\n', encoding="utf-8", newline="\n")
    for name in ("pg_isready", "sleep"):
        (stubs / name).chmod(0o755)


def _p(path: Path) -> str:
    from tests.deploy.test_home_backup import _posix
    return _posix(path)


def test_wait_pg_retries_until_postgres_accepts(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    _stub_pg(tmp, down_times=3)
    r = run(_stdin())
    assert r.returncode == 0, r.stdout + r.stderr
    assert (tmp / "sleep.log").read_text(encoding="utf-8").split() == ["10", "10", "10"]
    assert "PostgreSQL not ready yet" in r.stdout
    assert _pings(tmp) == []


def test_wait_pg_gives_up_with_69_after_5_minutes(rig) -> None:  # type: ignore[no-untyped-def]  # noqa: F811
    tmp, run, _ = rig
    _stub_pg(tmp, down_times=10_000)
    r = run(_stdin())
    out = r.stdout + r.stderr
    assert r.returncode == 69, out
    assert len((tmp / "sleep.log").read_text(encoding="utf-8").split()) == 30          # 30 x 10 s = 5 min
    assert "not reachable after 300 s" in out
    assert _pings(tmp) == [] and _daily(tmp) == []
    code = ENTRYPOINT.read_text(encoding="utf-8")
    assert "PG_WAIT_S=300" in code and "PG_WAIT_STEP_S=10" in code and "pg_isready -q -t 60" not in code
    row69 = next(ln for ln in RUNBOOK.read_text(encoding="utf-8").splitlines() if ln.startswith("| 69 |"))
    assert "within 5 min" in row69 and "every 10 s" in row69


# ---------------------------------------------------------------------------------------------- 3: Tailscale tags
def _policy() -> dict:  # type: ignore[type-arg]
    text = HOME_RUNBOOK.read_text(encoding="utf-8")
    step2 = _section(text, "### 2. Tailscale", "### 3. Close public SSH")
    block = step2[step2.index("```json") + len("```json"):]
    block = block[:block.index("```")]
    return json.loads(re.sub(r"^\s*//.*$", "", block, flags=re.M))      # HuJSON comments are whole lines here


def test_policy_tags_both_machines_and_limits_the_home_pc_to_443() -> None:
    policy = _policy()
    assert set(policy["tagOwners"]) == {"tag:xtnl-home", "tag:xtnl-receiver"}
    assert "acls" not in policy                                          # no default `*` accept rule
    home = [g for g in policy["grants"] if "tag:xtnl-home" in g["src"]]
    assert home == [{"src": ["tag:xtnl-home"], "dst": ["tag:xtnl-receiver"], "ip": ["tcp:443"]}]
    for g in policy["grants"]:
        assert "*" not in g["src"] and "*" not in g["dst"] and "*" not in g["ip"]
        assert "tag:xtnl-receiver" not in g["src"]                       # nothing from the VPS to anything
        assert "autogroup:self" not in g["dst"]
    for rule in policy["ssh"]:
        assert rule["action"] == "check" and rule["src"] == ["autogroup:member"]
        assert "tag:xtnl-home" not in rule["src"]
        assert not {"root", "autogroup:nonroot"} & set(rule["users"])


def test_runbook_tags_the_vps_and_checks_ssh_is_refused_from_the_home_pc() -> None:
    text = HOME_RUNBOOK.read_text(encoding="utf-8")
    flat = _flat(text)
    assert "sudo tailscale up --ssh --hostname=vps-a --advertise-tags=tag:xtnl-receiver" in text
    assert "sudo tailscale up --ssh --hostname=vps-a\n" not in text
    assert "**replace the whole policy**" in flat and "Edit ACL tags" in flat and "`tag:xtnl-home`" in flat
    assert "add an `ssh` rule for your own user" not in flat
    step3 = _section(text, "### 3. Close public SSH", "### 4. The code and the receiver")
    assert "ssh root@vps-a" in step3 and "**from the home PC**" in step3
    assert "Test-NetConnection vps-a -Port 22" in step3 and "# False" in step3
    assert "tag policy in step 2" in flat                                # listed as unverified


# ---------------------------------------------------------------------------------------------- 4: drill clean-up
def test_drill_cleanup_removes_the_decrypted_archive_files_on_the_spare_only() -> None:
    drill = _flat(_section(RUNBOOK.read_text(encoding="utf-8"), "## Quarterly drill", "## Rotation"))
    assert "Get-ChildItem C:\\xtnl\\archive -Recurse -File | Remove-Item -Force" in drill
    assert "`(Get-ChildItem C:\\xtnl\\archive -Recurse -File).Count` is 0" in drill
    assert "**spare only**, never on the production PC" in drill
    assert drill.index("down -v") < drill.index("Remove-Item -Force") < drill.index("the read key")


# ---------------------------------------------------------------------------------------------- 5: unit restarts
def test_receiver_units_do_not_loop_on_a_config_error() -> None:
    for name in ("xtnl-deadman-receiver.home.service", "xtnl-deadman-receiver.service"):
        cp, _ = read_unit(name)
        svc = cp["Service"]
        assert svc["ExecStartPre"].startswith("-/opt/xtnl/order-server/.venv/bin/python"), name
        assert svc["ExecStartPre"].endswith("-m src.deadman_receiver --check-config")
        assert svc["RestartPreventExitStatus"] == "78" and svc["Restart"] == "always"
    flat = _flat(HOME_RUNBOOK.read_text(encoding="utf-8"))
    assert "(exit 78, not restarted)" not in flat
    assert "`ExecStartPre=-`" in flat and "covers only the main process" in flat


def test_the_main_process_refuses_a_bad_config_with_78(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """What the '-' prefix relies on: the main process (no --check-config) exits 78 on the same refusal."""
    import os
    for k in list(os.environ):
        if k.startswith("XTNL_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("XTNL_SECRETS_SOURCE", "env")
    monkeypatch.setenv("XTNL_RECEIVER_TOKEN", "t" * 24)
    monkeypatch.setenv("XTNL_RECEIVER_LISTEN", "10.8.0.5:8443")       # not loopback, no TLS
    assert receiver_main([]) == EX_CONFIG == 78


# ---------------------------------------------------------------------------------------------- 6: stop-gap pages
def test_stop_gap_text_says_one_page_per_start_after_6_h_and_per_host_episodes() -> None:
    env = _flat(LIVE_ENV.read_text(encoding="utf-8"))
    home = _flat(_section(HOME_RUNBOOK.read_text(encoding="utf-8"), "## Rollback", "- **Rebuild:**"))
    for text in (env, home):
        assert "accepting that one page" not in text
        assert "stays open (live view" not in text and "until a real receiver acknowledges" not in text
        assert "more than 6 h after" in text and "per receiver host" in text
        assert "acknowledging again" in text


# ---------------------------------------------------------------------------------------------- 7: live-view design
def _ps_list(name: str) -> list[str]:
    text = START.read_text(encoding="utf-8")
    m = re.search(rf"^\${name} = @\((.*?)\)", text, re.M | re.S)
    assert m, name
    return re.findall(r"'([^']+)'", m.group(1))


def test_liveview_design_lists_match_the_code() -> None:
    from src.liveview.settings import REFUSED_NAMES, REFUSED_PREFIXES
    text = LIVEVIEW_DESIGN.read_text(encoding="utf-8")
    refused = _flat(_section(text, "**Refused environment.**", "The pattern is limited"))
    for prefix in REFUSED_PREFIXES:
        assert f"`{prefix}`" in refused, prefix
    for name in REFUSED_NAMES:
        assert f"`{name}`" in refused, name
    i = text.index("child would exit 78 on every start. The list is:")
    printed = text[i:text.index("The generic rule lives", i)]
    printed = printed[printed.index("`") + 1:]
    printed = printed[:printed.index("`")]
    design = re.findall(r"'([^']+)'", printed)
    drop = _ps_list("LiveViewDropEnv")
    assert design == drop
    # the design's claim: it covers every $UserVarNames entry except the live view's own DSN, and never matches it
    for name in _ps_list("UserVarNames"):
        covered = any(fnmatch.fnmatchcase(name, pat) for pat in drop)
        assert covered == (name != "XTNL_LIVEVIEW_DSN"), name
