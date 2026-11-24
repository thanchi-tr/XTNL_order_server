"""`python -m src.ops <group> <command>`: the audited operator CLI (backlog 4.6, operations.md s16).

    safe-hold status
    safe-hold resume                       --operator A --approver B --reason "..."
    adapter rearm <adapter> <generation>   --operator A --reason "..." [--approver B]
    task reset <name>                      --operator A --reason "..." [--approver B]
    dlq list [--status S] [--limit N] [--all]
    dlq requeue <dlq_id>                   --operator A --reason "..." [--approver B]
    dlq release-quarantine <quarantine_id> --operator A --reason "..." [--approver B]
    epoch show
    epoch adopt <new_cluster_id> --from-cluster-id <old> --operator A --approver B --reason "..."
    halt status [--account X]
    halt clear <account> --reviewed-event-id N [--force] --operator A --reason "..." [--approver B]
                                           (--approver required with --force)
    halt ack <breach_id>                   --operator A --reason "<why>" [--approver B]
    account status [--account X] [--all] [--limit N]
    account release <account>              --operator A --reason "..." [--approver B]
    cash-flows refresh <account> [--since-ms N] --operator A --reason "..." [--approver B]
    archive list [--limit N]
    archive forget <partition>             --operator A --approver B --reason "..."
    ruleset validate <file.json>           (offline: no database; exit 3 when the engine would reject it)
    maintenance check [--file PATH]        (offline: the declared broker maintenance windows of THIS node, read
                                           exactly as the engine reads them, plus the file's sha256 to compare
                                           with the other node; exit 3 when the engine would reject the file)
    ruleset show [--version N] [--limit N]
    ruleset add <file.json>                --operator A --reason "..." [--approver B]   (stored inactive)
    ruleset activate <version>             --operator A --approver B --reason "..."
    audit tail [--limit N] [--command C]
    market-sessions show [--adapter A]     (read-only: the latest SM10 calendar rows, source and review flags)
    market-sessions accept --adapter A --symbols S1,S2 [--holidays FILE.json]   --operator A --reason "..."
                                           [--approver B] (store the reviewed schedule as reviewed_config rows:
                                           a new calendar version, fenced, audited; src/ops/market_sessions.py)
    arm config show [--pending]            (read-only; --pending reads the Supabase document: row diff, validation,
                                           admission, the nodes' registries; not audited)
    arm config accept <hash>               --operator A --reason "..." [--approver B]
    arm pause [--drain-notices] | arm resume | arm notify-test   --operator A --reason "..." [--approver B]
    arm failed show [--all] [--limit N]
    arm failed clear <SCOPE_KIND:scope_id> --operator A --reason "..." [--approver B] [--wait S]
    arm disarm | rearm <adapter:position>  --operator A --reason "..." [--approver B] [--wait S]
    arm repin <adapter:position> [--strategy CODE --strategy-version N]   (actor args, [--wait S])
    md backfill <series> --from-ms N --to-ms N | md rebuild <indicator_id> | md writer-reset | md rehash
                                           --operator A --reason "..." [--approver B] [--wait S]
                                           (ARM / market-data requests: refused without a leader arm_status
                                           heartbeat younger than 60 s or with 20 pending; reported applied,
                                           pending, moot or refused; src/ops/arm.py)
    arm show [<adapter:position>] [--all] [--limit N]
    arm decisions [<adapter:position>] [--from-ms N] [--to-ms N] [--limit N]
    arm config resolve --position <adapter:position>
    arm replay [<adapter:position>] [--from-ms N] [--to-ms N] [--limit N] [--rerun]
               [--request-backfill --operator A --reason "..." [--wait S]]
                                           (--rerun, S39b: re-run the pipeline for each past bar, read-only, no
                                           notice: MATCH, DIFF or UNVERIFIABLE)
    arm registry check --against-db        (exit 3 when a pinned / referenced version is not in this registry)
    arm gates                              (offline, S38e: exit 3 while XTNL_ARM_ENABLED=1 would leave the ARM
                                           idle on open go-live gates; XTNL_ARM_GOLIVE_ACCEPT)
    md show | md bars <series> [--newest N] | md indicators [--indicator ID]
                                           (read-only views, src/ops/arm_view.py and src/ops/md_view.py: ids,
                                           codes, counts and times, never a price; redis-md is read as md_reader
                                           through XTNL_MD_REDIS_READER_URL, the keyring for hash checks)
    pinescript [--days 7] [--symbol S] [--adapter A] [--out var/pinescript]
                                           (read-only: one TradingView Pine Script v6 file per symbol drawing
                                           the order life cycles from event_log; no amounts; not audited)

Ruleset documents are checked with the engine's own parser (`parse_ruleset`) and stored as `ruleset_json`
(complete and explicit), so the CLI never stores or activates a row the engine would reject.

PostgreSQL comes from ORDER_SERVER_DSN (`StorageSettings`; the login is the authenticated identity
recorded in operator_audit.db_user), etcd from XTNL_ETCD_ENDPOINTS. Nothing here reads .env files, and no
secret is ever printed: the DSN appears only redacted, and errors print their type, not their text.

Exit codes: 0 ok / applied / no-op, 1 error (unreachable, unknown COMMIT outcome), 2 usage,
3 refused (audited; for an ARM / market-data request also when the leader answered it refused), 4 applied but the
etcd SAFE_HOLD mirror was not written.
"""
from __future__ import annotations

import argparse
import asyncio
import getpass
import hashlib
import importlib
import json
import os
import sys
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Final, TextIO

from src.db.epoch import CommitUnknownError
from src.db.pg import AsyncpgConnection, PgConnection, init_connection, sqlstate
from src.db.settings import StorageSettings, redact

from src.ops.arm import ArmContext, gates_report
from src.ops.arm_view import ArmViews
from src.ops.etcd import OpsEtcd, endpoints_from_env, make_etcd
from src.ops.market_sessions import check_holidays, parse_symbols, sessions_accept, sessions_show
from src.ops.md_acl import read_env_file
from src.ops.model import Actor, OpsRefused, OpsUsageError, Report, iso_ms
from src.ops.rulesets import MAX_DOCUMENT_BYTES, check_document, summary
from src.ops.service import OpsService
from src.reports.export import export_pinescript, summary_lines
from src.reports.lifecycle import MAX_DAYS as PINE_MAX_DAYS
from src.runtime.maintenance import MAX_FILE_BYTES as MAINTENANCE_MAX_BYTES
from src.runtime.maintenance import FileMaintenanceSource, MaintenanceSourceError, maintenance_source_from_env

APPLICATION_NAME: Final = "xtnl-ops"   # never matches the post-bump termination pattern ^xtnl:[0-9]+:
EXIT_OK: Final = 0
EXIT_ERROR: Final = 1
EXIT_USAGE: Final = 2
EXIT_REFUSED: Final = 3
EXIT_DEGRADED: Final = 4

PINE_OUT_DIR: Final = "var/pinescript"
ARM_WAIT_S: Final = 5.0

Closer = Callable[[], Awaitable[None]]
Connect = Callable[[str], Awaitable[tuple[PgConnection, Closer]]]
EtcdFactory = Callable[[Sequence[str]], OpsEtcd | None]


async def connect_asyncpg(dsn: str) -> tuple[PgConnection, Closer]:   # pragma: no cover - needs a server
    asyncpg = importlib.import_module("asyncpg")
    raw = await asyncpg.connect(dsn, timeout=5, server_settings={"application_name": APPLICATION_NAME,
                                                                 "statement_timeout": "5000"})
    await init_connection(raw)

    async def close() -> None:
        await raw.close()

    return AsyncpgConnection(raw), close


# ---------------------------------------------------------------------------------------------- parser
class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Any:   # never sys.exit from inside main()
        raise OpsUsageError(message)


def _actor_args(p: argparse.ArgumentParser, *, approver_required: bool = False) -> None:
    p.add_argument("--operator", required=True, help="who performs the action (your operator id)")
    p.add_argument("--reason", required=True, help="written reason / ticket reference (>= 8 characters)")
    p.add_argument("--approver", required=approver_required,
                   help="second person (two-person control)" + (" - required" if approver_required else ""))


def _event_id(text: str) -> int | None:
    if text.lower() == "none":
        return None
    try:
        return int(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected an integer halt_events id or 'none'") from exc


def _wait_arg(p: argparse.ArgumentParser) -> None:
    p.add_argument("--wait", type=float, default=ARM_WAIT_S,
                   help=f"seconds to wait for the leader's answer (0-60, default {ARM_WAIT_S:g}; then `pending`)")


def _arm_parsers(groups: Any) -> None:
    """`arm ...` and `md ...` (src/ops/arm.py, plan S34)."""
    arm = groups.add_parser("arm", help="automated risk manager: configuration, controls, FAILED, requests "
                            "(advice only; never sends orders)").add_subparsers(dest="cmd", required=True)
    cfg = arm.add_parser("config", help="ARM configuration (Supabase document, local accept)").add_subparsers(
        dest="sub", required=True)
    a = cfg.add_parser("show", help="the accepted / installed document; --pending reads Supabase and shows the diff")
    a.add_argument("--pending", action="store_true",
                   help="read the pending document (XTNL_ARM_CONFIG_DSN): row diff, validation, admission, registry")
    a = cfg.add_parser("accept", help="accept the pending document whose hash `config show --pending` printed")
    a.add_argument("doc_hash")
    _actor_args(a)
    a = cfg.add_parser("resolve", help="the assignment in force for a position at its arming and now (read-only)")
    a.add_argument("--position", required=True, help="adapter_id:position_id")
    a = arm.add_parser("show", help="ARM positions: state, pins, cursor (read-only; no level)")
    a.add_argument("target", nargs="?", default=None, help="adapter_id:position_id (default: every live position)")
    a.add_argument("--all", action="store_true", help="include CLOSED / DISARMED positions")
    a.add_argument("--limit", type=int, default=50)
    for name, text in (("decisions", "stored decisions, newest first (read-only; no level or R value)"),
                       ("replay", "re-verify stored decisions against redis-md as of their decision time")):
        a = arm.add_parser(name, help=text)
        a.add_argument("target", nargs="?", default=None, help="adapter_id:position_id (default: every position)")
        a.add_argument("--from-ms", type=int, default=None, help="first bar end (epoch ms)")
        a.add_argument("--to-ms", type=int, default=None, help="last bar end (epoch ms)")
        a.add_argument("--limit", type=int, default=50)
    a.add_argument("--rerun", action="store_true",
                   help="re-run the real pipeline for each past bar (read-only, no notice): MATCH, DIFF, UNVERIFIABLE")
    a.add_argument("--request-backfill", action="store_true",
                   help="file one audited md_backfill per series with bars_missing (needs --operator, --reason)")
    a.add_argument("--operator", default=None)
    a.add_argument("--reason", default=None)
    a.add_argument("--approver", default=None)
    _wait_arg(a)
    a = arm.add_parser("gates", help="go-live gates: refused (exit 3) while XTNL_ARM_ENABLED=1 would leave the ARM "
                                     "idle (offline; XTNL_ARM_GOLIVE_ACCEPT)")
    a.add_argument("--env-file", default=None,
                   help="read XTNL_ARM_GOLIVE_ACCEPT from the engine's env file (e.g. /etc/xtnl/engine.env) instead "
                        "of this shell's environment")
    reg = arm.add_parser("registry", help="the strategy / guard / indicator registry").add_subparsers(
        dest="sub", required=True)
    a = reg.add_parser("check", help="refuse (exit 3) when a version the database references is not in this "
                                     "node's registry")
    a.add_argument("--against-db", action="store_true", required=True,
                   help="check the pins, decisions and installed document stored in PostgreSQL")
    a = arm.add_parser("pause", help="no ARM decision until `arm resume` (FAILED pings still go out)")
    a.add_argument("--drain-notices", action="store_true",
                   help="also finish queued, unsent ARM notices as suppressed (never a FAILED ping)")
    _actor_args(a)
    _actor_args(arm.add_parser("resume", help="end the operator pause (never clears FAILED)"))
    _actor_args(arm.add_parser("notify-test", help="record a test-notice request time (no ARM test-notice "
                                                   "sender is wired yet)"))
    failed = arm.add_parser("failed", help="FAILED scopes (arm_failures)").add_subparsers(dest="sub", required=True)
    a = failed.add_parser("show", help="open FAILED episodes, counters and the last probe")
    a.add_argument("--all", action="store_true", help="include cleared episodes")
    a.add_argument("--limit", type=int, default=20)
    a = failed.add_parser("clear", help="file arm_clear_failed: the leader probes the recorded cause, then clears")
    a.add_argument("scope", help="SCOPE_KIND:scope_id as `arm failed show` prints it")
    _actor_args(a)
    _wait_arg(a)
    for name, text in (("disarm", "stop managing the position (arm_disarm)"),
                       ("rearm", "re-arm a SUSPENDED position, cursor as at arming (arm_rearm)")):
        a = arm.add_parser(name, help=text)
        a.add_argument("target", help="adapter_id:position_id")
        _actor_args(a)
        _wait_arg(a)
    a = arm.add_parser("repin", help="move the position to a new strategy version, migrate_state (arm_repin)")
    a.add_argument("target", help="adapter_id:position_id")
    a.add_argument("--strategy", default=None, help="strategy code (default: the assignment in force)")
    a.add_argument("--strategy-version", type=int, default=None)
    _actor_args(a)
    _wait_arg(a)

    md = groups.add_parser("md", help="market-data repairs (redis-md; applied during FAILED too)").add_subparsers(
        dest="cmd", required=True)
    md.add_parser("show", help="md generation, SM12 / D6 states and counters per series, FAULTED indicators")
    a = md.add_parser("bars", help="counts, quality, revisions and quarantined buckets of one series (no values)")
    a.add_argument("series", help="adapter:symbol:spec:SIDE:SOURCE")
    a.add_argument("--newest", type=int, default=16, help="newest buckets listed (1-200)")
    a = md.add_parser("indicators", help="registered indicators: warm, lineage, FAULTED (no values)")
    a.add_argument("--indicator", default=None, help="only this indicator id")
    a = md.add_parser("backfill", help="re-fetch a bar range of one series (md_backfill)")
    a.add_argument("series", help="adapter:symbol:spec:SIDE:SOURCE")
    a.add_argument("--from-ms", type=int, required=True, help="first bucket start (epoch ms, on the series' grid)")
    a.add_argument("--to-ms", type=int, required=True, help="last bucket start (epoch ms)")
    _actor_args(a)
    _wait_arg(a)
    a = md.add_parser("rebuild", help="rebuild one indicator (md_rebuild)")
    a.add_argument("indicator_id", help="16 hex characters")
    _actor_args(a)
    _wait_arg(a)
    for name, text in (("writer-reset", "reset md-writer's circuit and re-run the bootstrap (md_writer_reset)"),
                       ("rehash", "rewrite stored members under the current HMAC key id (md_rehash)")):
        a = md.add_parser(name, help=text)
        _actor_args(a)
        _wait_arg(a)


def build_parser() -> argparse.ArgumentParser:
    p = _Parser(prog="python -m src.ops", description="XTNL audited operator CLI (Phase 1, read-only broker access)")
    groups = p.add_subparsers(dest="group", required=True, parser_class=_Parser)

    g = groups.add_parser("safe-hold", help="SAFE_HOLD (anti-flap brake)").add_subparsers(dest="cmd", required=True)
    g.add_parser("status")
    _actor_args(g.add_parser("resume"), approver_required=True)

    g = groups.add_parser("adapter", help="adapter AUTH_FAILED").add_subparsers(dest="cmd", required=True)
    a = g.add_parser("rearm")
    a.add_argument("adapter")
    a.add_argument("generation", help="credential generation (Secrets Manager version id or token_generation)")
    _actor_args(a)

    g = groups.add_parser("task", help="task circuits").add_subparsers(dest="cmd", required=True)
    a = g.add_parser("reset")
    a.add_argument("name")
    _actor_args(a)

    g = groups.add_parser("dlq", help="events_dlq / events_quarantine").add_subparsers(dest="cmd", required=True)
    a = g.add_parser("list")
    a.add_argument("--status", default=None)
    a.add_argument("--limit", type=int, default=20)
    a.add_argument("--all", action="store_true", help="include released quarantine rows")
    a = g.add_parser("requeue")
    a.add_argument("dlq_id", type=int)
    _actor_args(a)
    a = g.add_parser("release-quarantine")
    a.add_argument("quarantine_id", type=int)
    _actor_args(a)

    g = groups.add_parser("epoch", help="engine_epoch fencing token").add_subparsers(dest="cmd", required=True)
    g.add_parser("show")
    a = g.add_parser("adopt")
    a.add_argument("cluster_id", help="the new etcd cluster_id (as `epoch show` reports it from etcd)")
    a.add_argument("--from-cluster-id", required=True, help="the cluster_id engine_epoch holds now (reviewed)")
    _actor_args(a, approver_required=True)

    g = groups.add_parser("halt", help="halt level (halt_events)").add_subparsers(dest="cmd", required=True)
    a = g.add_parser("status")
    a.add_argument("--account", default=None)
    a = g.add_parser("clear")
    a.add_argument("account")
    a.add_argument("--reviewed-event-id", required=True, type=_event_id,
                   help="the halt_event_id you reviewed (`halt status`); a newer row refuses the clear")
    a.add_argument("--force", action="store_true",
                   help="resolve this account's capped / UNKNOWN HARD states (needs --approver)")
    _actor_args(a)
    a = g.add_parser("ack", help="acknowledge a rule state: silences its re-pages until level, cap or reason change")
    a.add_argument("breach_id", type=int, help="halt_breaches id (`halt status` lists it)")
    _actor_args(a)

    g = groups.add_parser("account", help="RiskSweep account quarantine (sweep_quarantine)").add_subparsers(
        dest="cmd", required=True)
    a = g.add_parser("status")
    a.add_argument("--account", default=None)
    a.add_argument("--all", action="store_true", help="include released rows")
    a.add_argument("--limit", type=int, default=20)
    a = g.add_parser("release", help="release_account(): end the account's sweep quarantine")
    a.add_argument("account")
    _actor_args(a)

    g = groups.add_parser("cash-flows", help="refresh_cash_flows() (read-only broker fetch)").add_subparsers(
        dest="cmd", required=True)
    a = g.add_parser("refresh")
    a.add_argument("account")
    a.add_argument("--since-ms", type=int, default=None,
                   help="epoch ms to fetch from (default: since the current 17:00 New York roll)")
    _actor_args(a)

    g = groups.add_parser("archive", help="partition_archive (exported partitions)").add_subparsers(
        dest="cmd", required=True)
    a = g.add_parser("list")
    a.add_argument("--limit", type=int, default=20)
    a = g.add_parser("forget", help="drop the ledger row of a never-detached export so it is re-exported")
    a.add_argument("partition")
    _actor_args(a, approver_required=True)

    g = groups.add_parser("ruleset", help="Oversee rule thresholds (rulesets)").add_subparsers(dest="cmd",
                                                                                              required=True)
    a = g.add_parser("validate")
    a.add_argument("file", help="ruleset JSON document (sections over the spec defaults)")
    a = g.add_parser("show")
    a.add_argument("--version", type=int, default=None)
    a.add_argument("--limit", type=int, default=20)
    a = g.add_parser("add")
    a.add_argument("file", help="ruleset JSON document; stored as the next version, inactive")
    _actor_args(a)
    a = g.add_parser("activate")
    a.add_argument("version", type=int)
    _actor_args(a, approver_required=True)

    g = groups.add_parser("maintenance", help="declared broker maintenance windows (XTNL_MAINTENANCE_FILE)"
                          ).add_subparsers(dest="cmd", required=True)
    a = g.add_parser("check", help="offline: parse this node's maintenance file as the engine does; print its sha256")
    a.add_argument("--file", default=None,
                   help="default: XTNL_MAINTENANCE_FILE, else /etc/xtnl/maintenance.json (the engine's default)")

    g = groups.add_parser("uptime", help="engine runs and adapter coverage (read-only)").add_subparsers(
        dest="cmd", required=True)
    a = g.add_parser("show", help="runs: start, last seen, stop, reason, suspended gaps; offline gaps; coverage")
    a.add_argument("--limit", type=int, default=20)
    a.add_argument("--node", default=None)

    g = groups.add_parser("catchup", help="downtime catch-ups (read-only)").add_subparsers(dest="cmd",
                                                                                          required=True)
    a = g.add_parser("show", help="windows, counts and completeness of the downtime catch-ups")
    a.add_argument("--limit", type=int, default=20)
    a.add_argument("--adapter", default=None)

    g = groups.add_parser("market-sessions", help="SM10 calendar rows (market_sessions): review and accept"
                          ).add_subparsers(dest="cmd", required=True)
    a = g.add_parser("show", help="read-only: the latest calendar version's rows, source and review flags")
    a.add_argument("--adapter", default=None)
    a = g.add_parser("accept", help="store the reviewed schedule of these symbols as reviewed_config rows "
                     "(new calendar version; the review page then leaves them out)")
    a.add_argument("--adapter", required=True, help="adapter id, e.g. stonex")
    a.add_argument("--symbols", required=True, help="comma separated, e.g. EURUSD,AUDUSD")
    a.add_argument("--holidays", default=None,
                   help="JSON list of holidays ({\"date\": \"YYYY-MM-DD\"} or start / end); sets holidays_known")
    _actor_args(a)

    g = groups.add_parser("audit", help="operator_audit").add_subparsers(dest="cmd", required=True)
    a = g.add_parser("tail")
    a.add_argument("--limit", type=int, default=20)
    a.add_argument("--command", default=None)

    _arm_parsers(groups)

    a = groups.add_parser("pinescript", help="read-only: TradingView Pine Script v6 of the order life cycles "
                          "(one .pine file per symbol, no amounts)")
    a.set_defaults(cmd="export")
    a.add_argument("--days", type=int, default=7, help=f"window: the last N days (1-{PINE_MAX_DAYS}, default 7)")
    a.add_argument("--symbol", default=None, help="only this symbol (EURUSD, EUR/USD, ... are the same)")
    a.add_argument("--adapter", default=None, help="only this adapter id (e.g. stonex, pepperstone)")
    a.add_argument("--out", default=PINE_OUT_DIR, help=f"output folder (default {PINE_OUT_DIR})")
    return p


# ---------------------------------------------------------------------------------------------- dispatch
def _os_user() -> str | None:
    try:
        return getpass.getuser()[:64]
    except Exception:
        return None


def _actor(ns: argparse.Namespace) -> Actor:
    return Actor(operator=ns.operator, reason=ns.reason, approver=ns.approver, os_user=_os_user())


def _limit(n: int) -> int:
    if not 1 <= n <= 500:
        raise OpsUsageError("--limit must be between 1 and 500")
    return n


async def dispatch_arm(svc: OpsService, ns: argparse.Namespace) -> Report:
    """`arm ...` / `md ...` (src/ops/arm.py; the S35 views src/ops/arm_view.py)."""
    arm = svc.arm
    key = (ns.group, ns.cmd, getattr(ns, "sub", None))
    views = ArmViews(arm)
    if key == ("arm", "show", None):
        return await views.show(ns.target, include_ended=ns.all, limit=ns.limit)
    if key == ("arm", "decisions", None):
        return await views.decisions(ns.target, from_ms=ns.from_ms, to_ms=ns.to_ms, limit=ns.limit)
    if key == ("arm", "config", "resolve"):
        return await views.config_resolve(ns.position)
    if key == ("arm", "replay", None):
        actor = None
        if ns.request_backfill:
            if ns.operator is None or ns.reason is None:
                raise OpsUsageError("--request-backfill needs --operator and --reason (the md_backfill requests "
                                    "are audited)")
            actor = _actor(ns)
        return await views.replay(ns.target, from_ms=ns.from_ms, to_ms=ns.to_ms, limit=ns.limit,
                                  request_backfill=ns.request_backfill, actor=actor, wait_s=ns.wait,
                                  rerun=ns.rerun)
    if key == ("arm", "registry", "check"):
        return await views.registry_check()
    if key in (("md", "show", None), ("md", "bars", None), ("md", "indicators", None)):
        return await views.md(ns.cmd, series=getattr(ns, "series", None), newest=getattr(ns, "newest", 16),
                              indicator_id=getattr(ns, "indicator", None))
    if key == ("arm", "config", "show"):
        return await arm.config_show(pending=ns.pending)
    if key == ("arm", "config", "accept"):
        return await arm.config_accept(ns.doc_hash, _actor(ns))
    if key == ("arm", "pause", None):
        return await arm.pause(_actor(ns), drain_notices=ns.drain_notices)
    if key == ("arm", "resume", None):
        return await arm.resume(_actor(ns))
    if key == ("arm", "notify-test", None):
        return await arm.notify_test(_actor(ns))
    if key == ("arm", "failed", "show"):
        return await arm.failed_show(include_cleared=ns.all, limit=_limit(ns.limit))
    if key == ("arm", "failed", "clear"):
        return await arm.failed_clear(ns.scope, _actor(ns), wait_s=ns.wait)
    if key == ("arm", "disarm", None):
        return await arm.disarm(ns.target, _actor(ns), wait_s=ns.wait)
    if key == ("arm", "rearm", None):
        return await arm.rearm(ns.target, _actor(ns), wait_s=ns.wait)
    if key == ("arm", "repin", None):
        return await arm.repin(ns.target, _actor(ns), strategy_code=ns.strategy,
                               strategy_version=ns.strategy_version, wait_s=ns.wait)
    if key == ("md", "backfill", None):
        return await arm.md_backfill(ns.series, ns.from_ms, ns.to_ms, _actor(ns), wait_s=ns.wait)
    if key == ("md", "rebuild", None):
        return await arm.md_rebuild(ns.indicator_id, _actor(ns), wait_s=ns.wait)
    if key == ("md", "writer-reset", None):
        return await arm.md_writer_reset(_actor(ns), wait_s=ns.wait)
    if key == ("md", "rehash", None):
        return await arm.md_rehash(_actor(ns), wait_s=ns.wait)
    raise OpsUsageError(f"unknown command {ns.group} {ns.cmd}")


async def dispatch(svc: OpsService, ns: argparse.Namespace) -> Report:
    if ns.group in ("arm", "md"):
        return await dispatch_arm(svc, ns)
    key = (ns.group, ns.cmd)
    if key == ("safe-hold", "status"):
        return await svc.safe_hold_status()
    if key == ("safe-hold", "resume"):
        return await svc.safe_hold_resume(_actor(ns))
    if key == ("adapter", "rearm"):
        return await svc.adapter_rearm(ns.adapter, ns.generation, _actor(ns))
    if key == ("task", "reset"):
        return await svc.task_reset(ns.name, _actor(ns))
    if key == ("dlq", "list"):
        return await svc.dlq_list(status=ns.status, limit=_limit(ns.limit), include_released=ns.all)
    if key == ("dlq", "requeue"):
        return await svc.dlq_requeue(ns.dlq_id, _actor(ns))
    if key == ("dlq", "release-quarantine"):
        return await svc.dlq_release_quarantine(ns.quarantine_id, _actor(ns))
    if key == ("epoch", "show"):
        return await svc.epoch_show()
    if key == ("epoch", "adopt"):
        return await svc.epoch_adopt(ns.cluster_id, ns.from_cluster_id, _actor(ns))
    if key == ("halt", "status"):
        return await svc.halt_status(ns.account)
    if key == ("halt", "clear"):
        return await svc.halt_clear(ns.account, ns.reviewed_event_id, force=ns.force, actor=_actor(ns))
    if key == ("halt", "ack"):
        return await svc.halt_ack(ns.breach_id, _actor(ns))
    if key == ("account", "status"):
        return await svc.account_status(ns.account, include_released=ns.all, limit=_limit(ns.limit))
    if key == ("account", "release"):
        return await svc.account_release(ns.account, _actor(ns))
    if key == ("cash-flows", "refresh"):
        return await svc.cash_flows_refresh(ns.account, ns.since_ms, _actor(ns))
    if key == ("archive", "list"):
        return await svc.archive_list(limit=_limit(ns.limit))
    if key == ("archive", "forget"):
        return await svc.archive_forget(ns.partition, _actor(ns))
    if key == ("ruleset", "show"):
        return await svc.ruleset_show(ns.version, limit=_limit(ns.limit))
    if key == ("ruleset", "add"):
        return await svc.ruleset_add(read_document(ns.file), ns.file, _actor(ns))
    if key == ("ruleset", "activate"):
        return await svc.ruleset_activate(ns.version, _actor(ns))
    if key == ("market-sessions", "show"):
        return await sessions_show(svc, ns.adapter)
    if key == ("market-sessions", "accept"):
        holidays = None if ns.holidays is None else read_holidays(ns.holidays)
        return await sessions_accept(svc, ns.adapter, parse_symbols(ns.symbols), holidays, _actor(ns),
                                     holidays_source=ns.holidays)
    if key == ("audit", "tail"):
        return await svc.audit_tail(limit=_limit(ns.limit), command=ns.command)
    if key == ("uptime", "show"):
        return await svc.uptime_show(limit=_limit(ns.limit), node=ns.node)
    if key == ("catchup", "show"):
        return await svc.catchup_show(limit=_limit(ns.limit), adapter=ns.adapter)
    raise OpsUsageError(f"unknown command {ns.group} {ns.cmd}")


def read_document(path: str) -> str:
    """The operator's ruleset file (UTF-8, at most MAX_DOCUMENT_BYTES)."""
    try:
        with open(path, "rb") as f:
            raw = f.read(MAX_DOCUMENT_BYTES + 1)
    except OSError as exc:
        raise OpsUsageError(f"cannot read {path} ({type(exc).__name__})") from exc
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise OpsUsageError(f"{path} is larger than {MAX_DOCUMENT_BYTES} bytes")
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise OpsUsageError(f"{path} is not UTF-8") from exc


def read_holidays(path: str) -> list[dict[str, Any]]:
    """`market-sessions accept --holidays`: a UTF-8 JSON list (at most MAX_DOCUMENT_BYTES)."""
    text = read_document(path)
    try:
        doc = json.loads(text)
    except ValueError as exc:
        raise OpsUsageError(f"{path} is not JSON") from exc
    return check_holidays(doc)


def validate_offline(path: str) -> Report:
    """`ruleset validate`: the engine's parser over a local file; touches no database."""
    rep = Report("ruleset.validate")
    rep.fields["file"] = path
    try:
        checked = check_document(read_document(path), 0)
    except OpsRefused as r:
        rep.outcome = "refused"
        rep.fields["refused"] = r.code
        rep.fields["problems"] = r.detail
        return rep
    rep.fields["valid"] = True
    rep.fields["sections"] = list(checked.sections)
    rep.fields.update(summary(checked.config))
    rep.fields["effective"] = checked.canonical
    rep.warnings.extend(checked.warnings)
    return rep


#: A declared window suppresses the adapter's liveness / FENCE pages for its whole length (operations.md §10):
#: a longer one is reported so a typo (a wrong day) is noticed before it hides a real outage.
MAINTENANCE_LONG_MS: Final = 12 * 3_600_000


def check_maintenance_file(path: str, now_ms: int) -> Report:
    """`maintenance check`: the engine's own reader (`FileMaintenanceSource`) over this node's file, offline.

    The file must be identical on both engine nodes (a window present on one node only is silently missing
    after a failover), so the report carries the sha256 of the bytes to compare across nodes. A missing file
    is "no window declared" (as in the engine); a file the engine would refuse is `refused` (exit 3)."""
    rep = Report("maintenance.check")
    rep.fields["file"] = path
    try:
        with open(path, "rb") as f:
            raw = f.read(MAINTENANCE_MAX_BYTES + 1)
    except FileNotFoundError:
        rep.fields["exists"] = False
        rep.fields["windows"] = 0
        rep.warnings.append("no file: this node declares no maintenance window (the other node must match)")
        return rep
    except OSError as exc:
        rep.outcome = "refused"
        rep.fields["refused"] = "unreadable"
        rep.fields["problems"] = type(exc).__name__
        return rep
    rep.fields["exists"] = True
    rep.fields["sha256"] = hashlib.sha256(raw).hexdigest()
    try:
        windows = asyncio.run(FileMaintenanceSource(path).load())
    except MaintenanceSourceError as exc:
        rep.outcome = "refused"
        rep.fields["refused"] = "invalid"
        rep.fields["problems"] = str(exc)
        return rep
    # The digest must describe the bytes the engine's reader just parsed: a file replaced between the two reads
    # (a copy still in progress) is refused, so the sha256 compared across the nodes is never of another file.
    try:
        with open(path, "rb") as f:
            again = f.read(MAINTENANCE_MAX_BYTES + 1)
    except OSError:
        again = b""
    if hashlib.sha256(again).hexdigest() != rep.fields["sha256"]:
        rep.outcome = "refused"
        rep.fields["refused"] = "changed"
        rep.fields["problems"] = "the file changed while it was being checked; run the check again"
        return rep
    rep.fields["windows"] = len(windows)
    latest_end: dict[str, int] = {}     # per adapter: the latest end of the windows seen so far (by start)
    for w in sorted(windows, key=lambda x: (x.adapter_id, x.start_ms)):
        state = "ended" if w.end_ms <= now_ms else "active" if w.start_ms <= now_ms else "future"
        rep.rows.append({"adapter": w.adapter_id, "start": iso_ms(w.start_ms), "end": iso_ms(w.end_ms),
                         "state": state, "reason": w.reason})
        if state == "ended":
            rep.warnings.append(f"{w.adapter_id}: window ending {iso_ms(w.end_ms)} has already ended (remove it)")
        if w.end_ms - w.start_ms > MAINTENANCE_LONG_MS:
            rep.warnings.append(f"{w.adapter_id}: window {iso_ms(w.start_ms)} is longer than 12 h; liveness and "
                                "FENCE pages for this adapter are held back for all of it")
        end_so_far = latest_end.get(w.adapter_id)
        if end_so_far is not None and w.start_ms < end_so_far:
            rep.warnings.append(f"{w.adapter_id}: windows overlap at {iso_ms(w.start_ms)}")
        latest_end[w.adapter_id] = max(w.end_ms, end_so_far if end_so_far is not None else w.end_ms)
    return rep


def _fmt(v: object) -> str:
    if v is None:
        return "-"
    if isinstance(v, (list, tuple)):
        return ",".join(_fmt(x) for x in v) or "-"
    return str(v).replace("\r", " ").replace("\n", " ")


def render(rep: Report, out: TextIO) -> None:
    out.write(f"command: {rep.command}\noutcome: {rep.outcome}\n")
    if rep.audit_id is not None:
        out.write(f"audit_id: {rep.audit_id}\n")
    for k, v in rep.fields.items():
        out.write(f"{k}: {_fmt(v)}\n")
    for row in rep.rows:
        out.write("  " + " ".join(f"{k}={_fmt(v)}" for k, v in row.items()) + "\n")
    for name, rows in rep.sections.items():
        out.write(f"[{name}] {len(rows)} row(s)\n")
        for row in rows:
            out.write("  " + " ".join(f"{k}={_fmt(v)}" for k, v in row.items()) + "\n")
    for w in rep.warnings:
        out.write(f"warning: {w}\n")


def exit_code(rep: Report) -> int:
    if rep.outcome == "refused":
        return EXIT_REFUSED
    if rep.degraded:
        return EXIT_DEGRADED
    return EXIT_OK


async def _run(ns: argparse.Namespace, dsn: str, connect: Connect, etcd: OpsEtcd | None, out: TextIO,
               err: TextIO, arm: ArmContext | None = None) -> int:
    try:
        conn, close = await connect(dsn)
    except Exception as exc:
        err.write(f"error: PostgreSQL unreachable at {redact(dsn)} ({type(exc).__name__})\n")
        return EXIT_ERROR
    try:
        rep = await dispatch(OpsService(conn, etcd, arm=arm), ns)
    except OpsUsageError as exc:
        err.write(f"usage: {exc}\n")
        return EXIT_USAGE
    except CommitUnknownError:
        err.write("error: COMMIT outcome unknown (connection lost at COMMIT); run `audit tail` and the matching "
                  "status command before re-running\n")
        return EXIT_ERROR
    except Exception as exc:
        code = sqlstate(exc)
        err.write(f"error: {type(exc).__name__}{f' (SQLSTATE {code})' if code else ''}\n")
        return EXIT_ERROR
    finally:
        try:
            await close()
        except Exception:
            pass
    render(rep, out)
    return exit_code(rep)


async def _run_pinescript(ns: argparse.Namespace, dsn: str, connect: Connect, out: TextIO, err: TextIO) -> int:
    """`pinescript`: one read-only transaction, then one .pine file per symbol. Prints file names and counts only."""
    try:
        conn, close = await connect(dsn)
    except Exception as exc:
        err.write(f"error: PostgreSQL unreachable at {redact(dsn)} ({type(exc).__name__})\n")
        return EXIT_ERROR
    try:
        plan, paths = await export_pinescript(conn, days=ns.days, now_ms=int(time.time() * 1000),
                                              out_dir=Path(ns.out), symbol=ns.symbol, adapter=ns.adapter)
    except OSError as exc:
        err.write(f"error: cannot write to {ns.out} ({type(exc).__name__})\n")
        return EXIT_ERROR
    except Exception as exc:
        code = sqlstate(exc)
        err.write(f"error: {type(exc).__name__}{f' (SQLSTATE {code})' if code else ''}\n")
        return EXIT_ERROR
    finally:
        try:
            await close()
        except Exception:
            pass
    for line in summary_lines(plan, paths):
        out.write(line + "\n")
    return EXIT_OK


def main(argv: Sequence[str] | None = None, *, environ: Mapping[str, str] | None = None,
         connect: Connect | None = None, etcd_factory: EtcdFactory | None = None,
         out: TextIO | None = None, err: TextIO | None = None, arm_context: ArmContext | None = None) -> int:
    out = out if out is not None else sys.stdout
    err = err if err is not None else sys.stderr
    try:
        ns = build_parser().parse_args(list(argv) if argv is not None else None)
    except OpsUsageError as exc:
        err.write(f"usage: {exc}\n")
        return EXIT_USAGE
    except SystemExit as exc:   # --help
        return int(exc.code) if isinstance(exc.code, int) else EXIT_OK
    if (ns.group, ns.cmd) == ("ruleset", "validate"):
        try:
            rep = validate_offline(ns.file)
        except OpsUsageError as exc:
            err.write(f"usage: {exc}\n")
            return EXIT_USAGE
        render(rep, out)
        return exit_code(rep)
    env = environ if environ is not None else os.environ
    if (ns.group, ns.cmd) == ("arm", "gates"):    # offline (S38e): the go-live verdict on the engine's settings
        if ns.env_file is not None:
            try:
                file_env = read_env_file(Path(ns.env_file))
            except (OSError, UnicodeDecodeError) as exc:
                err.write(f"usage: the env file could not be read ({type(exc).__name__})\n")
                return EXIT_USAGE
            rep = gates_report(file_env, f"env_file:{ns.env_file}")
        else:
            rep = gates_report(env)
        render(rep, out)
        return exit_code(rep)
    if (ns.group, ns.cmd) == ("maintenance", "check"):
        path = ns.file or str(maintenance_source_from_env(env).path)
        rep = check_maintenance_file(path, int(time.time() * 1000))
        render(rep, out)
        return exit_code(rep)
    if ns.group == "pinescript" and not 1 <= ns.days <= PINE_MAX_DAYS:
        err.write(f"usage: --days must be between 1 and {PINE_MAX_DAYS}\n")
        return EXIT_USAGE
    try:
        settings = StorageSettings.from_env(env)
    except (KeyError, ValueError) as exc:
        err.write(f"config: {exc.args[0] if exc.args else type(exc).__name__}\n")
        return EXIT_USAGE
    if ns.group == "pinescript":   # read-only export: no etcd, no audit row
        return asyncio.run(_run_pinescript(ns, settings.dsn, connect or connect_asyncpg, out, err))
    etcd =(etcd_factory or make_etcd)(endpoints_from_env(env))
    try:
        arm = arm_context if arm_context is not None else ArmContext.from_env(env)
        return asyncio.run(_run(ns, settings.dsn, connect or connect_asyncpg, etcd, out, err, arm))
    finally:
        if etcd is not None:
            try:
                etcd.close()
            except Exception:
                pass
