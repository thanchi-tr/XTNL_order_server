"""The ARM and market-data operator commands (design §B4 arm-config, §B6.1 version lifecycle, §B9 Acceptance (Q12),
§B13, §B14 Clearing, PR13; plan S34).

    arm config show [--pending]                  local accept / cache / controls / nodes; with --pending the
                                                 Supabase read, its row diff against the seen rows, validation,
                                                 admission and the nodes' registries (read-only, not audited)
    arm config accept <hash>                     --operator A --reason "..." [--approver B]
    arm pause [--drain-notices]                  --operator A --reason "..." [--approver B]
    arm resume                                   --operator A --reason "..." [--approver B]
    arm notify-test                              --operator A --reason "..." [--approver B]
    arm failed show [--all] [--limit N]
    arm failed clear <SCOPE_KIND:scope_id>       --operator A --reason "..." [--approver B] [--wait S]
    arm disarm | rearm <adapter:position>        --operator A --reason "..." [--approver B] [--wait S]
    arm repin <adapter:position> [--strategy CODE --strategy-version N] ... [--wait S]
    md backfill <series_id> --from-ms N --to-ms N  ... [--wait S]
    md rebuild <indicator_id> | md writer-reset | md rehash   ... [--wait S]

**Controls and the accept** (`arm_controls`, `arm_accepted`) are written only here, under a table lock with the
`operator_audit` row in the same transaction (§B13; the §B3 static check holds every other module to reads). They
are operator-owned configuration the engine only reads, so, like `rulesets`, they carry no epoch fence. `pause`
stops every decision (the BarClock reads the flag; the arm-tracker keeps arming and ending); `--drain-notices` also
makes the arm-relay finish queued, unsent ARM notices as `suppressed` (never a D7 ping); `resume` ends both and never
clears FAILED. `notify-test` records a test-notice request time only: no ARM test-notice sender is wired in this
build (S32 deviation 1), so no notice is sent yet.

**`arm config accept <hash>`** is the only trust boundary of the configuration (§B9): it reads the pending document
from Supabase now (`XTNL_ARM_CONFIG_DSN`, the same pinned-CA reader as the engine) and refuses, with an audit row,
when the read no longer hashes to `<hash>` (it changed after `arm config show --pending`), a seen row was tampered
with, the schema is ahead, a row is invalid against this engine's ids (instruments, accounts and routes from
PostgreSQL), an assignment names an adapter without broker bars (§B6.4), the admission check refuses (series,
bytes, BARS reads per bar end and at a reopen, with the numbers; `src/armconfig/admission.py`), or a fresh node's
health record lacks a referenced `KIND:VERSION` (PR13: a record without the field lacks everything; no etcd
configured or reachable refuses). Then one `arm_accepted` row (mode `manual`, `accepted_at_ms` on this host's clock)
is written; the `arm-config` task installs the document at the next `T + post_grace`.

**One-shot requests** (`operator_requests`, the kinds of 0017) are filed fenced on the current epoch with the audit
row, exactly like `task reset`. Each is refused when `arm_status` has no leader heartbeat of that epoch younger than
60 s (`no_arm_heartbeat`), when 20 requests of its budget are already pending in the epoch (`too_many_pending`) or
when the same request is pending (`already_pending`). The arm-tracker kinds (`arm disarm | rearm | repin`), which no
handler answers until the arm-tracker is wired, have a budget of their own, so they never block `arm failed clear` or
the `md` repairs. A request that has nothing to act on is reported `moot` and not
filed (a position that is not armed, a scope that is not FAILED). The CLI then waits up to `--wait` seconds for the
leader's answer and reports the request as `applied`, `pending` (not answered yet; the owning task answers later),
`moot:<code>` or `refused:<code>` (exit 3).

Nothing here sends anything to a broker or changes an order: a request only asks the leader's own optional tasks
(arm-tracker, md-bars, md-indicators, md-writer, the FAILED probe runner) to act on their own state. Output shows ids,
hashes, codes, counts and times; Supabase row text is shown only by `arm config show --pending`, escaped (control
characters as `\\xNN`) and bounded; never a price, level, account value or credential.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import logging
import math
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final

from src.armconfig.admission import PREWARM_CAP, AdapterLoad, AdmissionLimits, AdmissionReport, admission, \
    live_assignments
from src.armconfig.chain import ChainState, ChainVerdict, SeenRows, check_rows
from src.armconfig.client import ConfigSource, ConfigSourceError, SelfCheckError, safe_source_error
from src.armconfig.document import Document, DocumentProblem, parse_document
from src.armconfig.validate import KnownIds, ValidationResult, ValidationStatus, ValidDocument, escape_text, \
    validate_document
from src.config import rest_bars_per_min
from src.db.arm_config_store import SQL_ACCEPTED_LATEST, SQL_CONFIG_CACHE_GET, SQL_CONTROLS_GET, cache_from_row
from src.db.arm_ports import TERMINAL_ARM_STATES, ConfigCacheRow
from src.db.epoch import SQL_LOCK_TIMEOUT_2S
from src.db.fence import FencedTx, stored_epoch
from src.db.operator_requests import ARM_REQUEST_KINDS
from src.db.pg import PgConnection
from src.marketdata.types import SeriesId
from src.riskmgr.config import STOP_READS_VAR, ArmSettings, parse_arm_settings
from src.riskmgr.envelope import EnvelopeParams, EnvelopeStamp, stamp
from src.riskmgr.failed import parse_target
from src.riskmgr.golive import ACCEPT_VAR as GOLIVE_ACCEPT_VAR
from src.riskmgr.golive import BROKER_BAR_ADAPTERS, GATES, GateState, bar_capable_adapters, refusal_text, s39_blockers
from src.riskmgr.golive import verdict as golive_verdict
from src.riskmgr.registry import REGISTRY, ArmRegistry
from src.runtime.etcd import HEALTH_PREFIX
from src.runtime.health import HealthRecord
from src.runtime.optional_settings import TICK_SYMBOLS_VAR, parse_tick_symbols

from src.ops import arm_sql as A
from src.ops import sql as S
from src.ops.etcd import OpsEtcd
from src.ops.md_view import OpsMdReader, parse_reader_settings
from src.ops.model import Actor, OpsRefused, OpsUsageError, Report, iso_ms

if TYPE_CHECKING:
    from src.ops.service import OpsService

log = logging.getLogger(__name__)

#: The CLI refuses a request when the freshest leader heartbeat in `arm_status` is older than this (§B13).
HEARTBEAT_MAX_AGE_MS: Final = 60_000
#: The CLI refuses a new ARM / market-data request while this many are pending in the epoch (§B13).
MAX_PENDING_ARM_REQUESTS: Final = 20
#: The operator pause's `pause_reason` (the config-hash pause is the installer's `config_hash`).
PAUSE_REASON: Final = "operator"
#: Adapters that serve broker bars to the ARM (`src.riskmgr.golive.bar_capable_adapters()` at import: empty while
#: S39's gates are open; the runtime arm-tracker reads the same function).
BAR_CAPABLE_ADAPTERS: Final[frozenset[str]] = bar_capable_adapters()
#: Per adapter: its REST settings prefix, for the `BARS` cap the adapter uses (read by `src/config.py
#: rest_bars_per_min`, the adapters' own settings rule; PR11).
REST_PREFIX: Final[Mapping[str, str]] = {"stonex": "STONE_X", "pepperstone": "CTRADER"}
#: The arm-tracker request kinds, answered by the leader's `arm-tracker` (wired in S36b) while the ARM runs. They are
#: budgeted apart from the other ARM / market-data kinds (`arm_clear_failed`, the `md` repairs): with the ARM off (or
#: idle) nothing answers them, and a backlog of them never blocks a FAILED clear or a repair.
TRACKER_REQUEST_KINDS: Final[frozenset[str]] = frozenset({"arm_disarm", "arm_rearm", "arm_repin"})
#: Whole-string matches (`fullmatch`: a trailing newline is refused).
HASH_RE: Final = re.compile(r"[0-9a-f]{64}")
INDICATOR_ID_RE: Final = re.compile(r"[0-9a-f]{16}")
TARGET_MAX: Final = 200
WAIT_MAX_S: Final = 60.0
SHOW_TEXT_LIMIT: Final = 400
SHOW_POSITIONS: Final = 20
AUDIT_REFS: Final = 200
PROBLEMS_SHOWN: Final = 50
MD_WRITER_TARGET: Final = "md-writer"
MD_REHASH_TARGET: Final = "md"
PAYLOAD_VERSION: Final = 1


# ---------------------------------------------------------------------------------------------------- go-live gates
def gates_report(environ: Mapping[str, str], source: str = "process_environment") -> Report:
    """`arm gates` (plan S38e, offline: no database, no audit row): every go-live gate with its state and whether the
    `XTNL_ARM_GOLIVE_ACCEPT` of `environ` accepts it. `refused` (exit 3) while `XTNL_ARM_ENABLED=1` would leave the
    ARM idle on a node with these settings; also names the gates S39 still waits for. `source` names where the
    settings were read (`settings_source`: the engine's env file with `--env-file`, else this process's
    environment, which may differ from what the engine reads at start)."""
    rep = Report("arm.gates")
    rep.fields["settings_source"] = source
    v = golive_verdict(environ)
    accepted = set(v.accepted)
    for g in GATES:
        rep.rows.append({"gate": g.gate_id, "state": g.state.value,
                         "accepted": g.gate_id in accepted if g.state is GateState.OPEN else None, "title": g.title})
    rep.fields["arm_enable_allowed"] = v.allowed
    rep.fields["accept_setting"] = GOLIVE_ACCEPT_VAR
    rep.fields["s39_waits_for"] = [g.gate_id for g in s39_blockers()]
    rep.fields["bar_capable_adapters"] = sorted(BAR_CAPABLE_ADAPTERS)
    rep.warnings.extend(v.problems)
    if BAR_CAPABLE_ADAPTERS and s39_blockers():
        rep.warnings.append("an adapter is bar-capable while S39 gates are open")
    if not v.allowed:
        rep.outcome = "refused"
        rep.fields["refused"] = "golive_gates"
        rep.fields["problems"] = refusal_text(v)
    return rep


# ---------------------------------------------------------------------------------------------------- settings
def _no_source() -> ConfigSource | None | list[str]:
    return None


def _no_md_reader() -> OpsMdReader | None | list[str]:
    return None


def default_md_reader_factory(environ: Mapping[str, str]) -> Callable[[], OpsMdReader | None | list[str]]:
    """The `md_reader` client of the views and `arm replay` (`XTNL_MD_REDIS_READER_URL`, plus the content-hash
    keyring for the hash checks; plan S35): None without the URL, the problems (variable names only) for a bad one.
    Connecting is lazy: redis-py opens the pool at the first read."""

    def build() -> OpsMdReader | None | list[str]:
        settings = parse_reader_settings(environ)
        if settings is None or isinstance(settings, list):
            return settings
        return OpsMdReader.connect(settings)

    return build


def envelope_from_env(environ: Mapping[str, str]) -> tuple[EnvelopeStamp | None, tuple[str, ...]]:
    """This engine's envelope stamp from the local ARM settings (`XTNL_ARM_MIN_GAP_R`; `arm replay` reports
    `ENVELOPE_CHANGED` against it), or None and the problems (names only) when the settings do not parse."""
    settings = parse_arm_settings(environ)
    if isinstance(settings, list):
        return None, tuple(settings)
    return stamp(settings.envelope), ()


def bars_caps(environ: Mapping[str, str]) -> tuple[dict[str, int], tuple[str, ...]]:
    """Per adapter, the `BARS` cap per minute its bucket uses (`src/config.py rest_bars_per_min`, the adapters' own
    settings rule), and the settings problems: a REST rate / burst the adapter's start-up refuses is refused here
    too (that adapter has no cap, and `arm config accept` is refused `settings`)."""
    caps: dict[str, int] = {}
    problems: list[str] = []
    for aid, prefix in REST_PREFIX.items():
        try:
            caps[aid] = rest_bars_per_min(environ, prefix)
        except ValueError as exc:
            problems.append(f"{aid}: {exc}")
    return caps, tuple(problems)


def default_source_factory(environ: Mapping[str, str]) -> Callable[[], ConfigSource | None | list[str]]:
    """The engine's Supabase reader (`XTNL_ARM_CONFIG_DSN`, `XTNL_ARM_CONFIG_CA_FILE`) over asyncpg's `connect`
    (imported lazily, as the engine does): None without a DSN, the settings problems (variable names only) for a bad
    setting."""

    def build() -> ConfigSource | None | list[str]:
        from src.armconfig.client import SupabaseConfigSource, parse_source_settings

        settings = parse_source_settings(environ)
        if settings is None or isinstance(settings, list):
            return settings
        asyncpg = importlib.import_module("asyncpg")
        return SupabaseConfigSource(settings, asyncpg.connect)

    return build


@dataclass(frozen=True, kw_only=True)
class ArmContext:
    """What the ARM commands take from the process: the configuration reader, the local settings the admission check
    needs (tick symbols, PR9 stop reads, the BARS caps, the bar-capable adapters) and the answer poll."""

    config_source: Callable[[], ConfigSource | None | list[str]] = _no_source
    tick_symbols: str | None = None
    stop_reads: bool = False
    bars_per_min: Mapping[str, int] = field(default_factory=dict)
    settings_problems: tuple[str, ...] = ()
    bar_capable: frozenset[str] = BAR_CAPABLE_ADAPTERS
    limits: AdmissionLimits = field(default_factory=AdmissionLimits)
    read_timeout_s: float = 60.0
    poll_s: float = 0.5
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    registry: ArmRegistry = REGISTRY
    #: The `md_reader` client of `md show | bars | indicators` and `arm replay` (plan S35).
    md_reader: Callable[[], OpsMdReader | None | list[str]] = _no_md_reader
    #: This engine's envelope stamp (`arm replay`'s ENVELOPE_CHANGED); None when the local settings do not parse.
    envelope: EnvelopeStamp | None = field(default_factory=lambda: stamp(EnvelopeParams()))
    envelope_problems: tuple[str, ...] = ()
    #: The local ARM settings `arm replay --rerun` evaluates under (plan S39b); None when they do not parse.
    arm_settings: ArmSettings | None = field(default_factory=ArmSettings)

    @classmethod
    def from_env(cls, environ: Mapping[str, str]) -> ArmContext:
        caps, problems = bars_caps(environ)
        envelope, envelope_problems = envelope_from_env(environ)
        parsed = parse_arm_settings(environ)
        return cls(config_source=default_source_factory(environ), tick_symbols=environ.get(TICK_SYMBOLS_VAR),
                   stop_reads=(environ.get(STOP_READS_VAR) or "").strip() == "1",
                   bars_per_min=caps, settings_problems=problems, md_reader=default_md_reader_factory(environ),
                   envelope=envelope, envelope_problems=envelope_problems,
                   arm_settings=None if isinstance(parsed, list) else parsed)


# ---------------------------------------------------------------------------------------------------- helpers
def check_hash(text: str) -> str:
    if not HASH_RE.fullmatch(text or ""):
        raise OpsUsageError("the document hash is 64 lower-case hex characters (`arm config show --pending`)")
    return text


def check_position_target(text: str) -> str:
    """`adapter_id:position_id` (both set, printable, no spaces)."""
    adapter, sep, position = (text or "").partition(":")
    if (not sep or not adapter or not position or len(text) > TARGET_MAX or not text.isprintable()
            or any(c.isspace() for c in text)):
        raise OpsUsageError("the target is adapter_id:position_id (`arm show` / the ARM notice names it)")
    return text


def check_scope_target(text: str) -> str:
    """`SCOPE_KIND:scope_id` exactly as `arm failed show` prints it (the clear compares the target as filed)."""
    if parse_target(text) is None or len(text) > TARGET_MAX or not text.isprintable():
        raise OpsUsageError("the scope is SCOPE_KIND:scope_id with SCOPE_KIND one of SERIES, ADAPTER, "
                            "STRATEGY_VERSION, POSITION (POSITION:adapter_id:position_id), ARM (as `arm failed "
                            "show` prints it)")
    return text


def check_wait(wait_s: float) -> float:
    if not 0 <= wait_s <= WAIT_MAX_S:
        raise OpsUsageError(f"--wait must be between 0 and {WAIT_MAX_S:g} seconds")
    return wait_s


def _dumps(d: Mapping[str, Any]) -> str:
    return json.dumps(dict(d), sort_keys=True, separators=(",", ":"))


def _counters_text(raw: object) -> str:
    """`counters` jsonb as `code=count,...` (integer counts only; anything else is left out)."""
    try:
        obj = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
    except ValueError:
        return "-"
    if not isinstance(obj, Mapping):
        return "-"
    parts = [f"{escape_text(str(k), 64)}={v}" for k, v in sorted(obj.items())
             if isinstance(v, int) and not isinstance(v, bool)]
    return ",".join(parts) or "-"


def referenced_tokens(doc: ValidDocument, registry: ArmRegistry = REGISTRY) -> tuple[str, ...]:
    """The `KIND:VERSION` tokens the document references (PR13): every strategy row's `KIND:IMPL_VERSION`, its
    guards' `KIND:VERSION` and the indicators the strategy and its guards require."""
    tokens: set[str] = set()
    for entry in doc.strategies:
        p = entry.params
        tokens.add(f"{p.kind}:{p.impl_version}")
        tokens.update(f"{g.kind}:{g.version}" for g in p.guards)
        strategy = registry.strategy(p.kind, p.impl_version)
        try:
            needs = list(strategy.required_indicators(p)) if strategy is not None else []
            for g in p.guards:
                guard = registry.guard(g.kind, g.version)
                if guard is not None:
                    needs += guard.required_indicators(g)
        except Exception:   # a class raising on its own params was refused at validation; never crash the check
            continue
        tokens.update(f"{n.kind}:{n.version}" for n in needs)
    return tuple(sorted(tokens))


def no_bar_refusals(doc: ValidDocument, bar_capable: frozenset[str], at_ms: int) -> list[str]:
    """§B6.4: an assignment (in force or future-dated) scoped to an adapter without broker bars is refused."""
    return [f"assignments:{a.row_id}: adapter {escape_text(a.adapter, 64)} serves no broker bars"
            for a in live_assignments(doc, at_ms) if a.adapter is not None and a.adapter not in bar_capable]


def _cached_texts(cache: ConfigCacheRow | None) -> dict[str, str]:
    """`ref -> canonical row text` of the cached (installed) document: the "was" side of a changed row."""
    if cache is None:
        return {}
    try:
        raw = json.loads(cache.document_json, parse_float=Decimal, parse_int=int)
    except (ValueError, RecursionError):
        return {}
    doc = parse_document(raw)
    if isinstance(doc, DocumentProblem):
        return {}
    return {r.ref: r.canonical for r in doc.rows}


@dataclass
class Assessment:
    """Everything `arm config show --pending` shows and `arm config accept` checks, for one read."""

    doc: Document
    verdict: ChainVerdict
    seen: dict[str, str]
    validation: ValidationResult | None = None
    admission: AdmissionReport | None = None
    no_bars: list[str] = field(default_factory=list)
    tokens: tuple[str, ...] = ()
    registry_problems: list[str] = field(default_factory=list)
    registry_notes: list[str] = field(default_factory=list)
    local_problem: OpsRefused | None = None

    def refusal(self) -> OpsRefused | None:
        """The first reason the document cannot be accepted (None: it can)."""
        v = self.verdict
        if v.state is ChainState.TAMPERED:
            refs = [f"{t.ref}:{t.kind.value}" for t in v.tampered]
            return OpsRefused("tampered", f"{len(refs)} seen row(s) changed or vanished: {', '.join(refs[:20])}")
        if self.doc.schema_ahead:
            return OpsRefused("schema_ahead", f"arm.schema_version {self.doc.schema_version} is newer than this "
                                              "engine reads")
        if self.local_problem is not None:
            return self.local_problem
        val = self.validation
        if val is None or val.status is not ValidationStatus.VALID or val.document is None:
            problems = [] if val is None else [p.page_text for p in val.problems]
            return OpsRefused("invalid", "; ".join(problems[:PROBLEMS_SHOWN]) or "not validated")
        if self.no_bars:
            return OpsRefused("no_broker_bars", "; ".join(self.no_bars[:PROBLEMS_SHOWN]))
        adm = self.admission
        if adm is not None and not adm.admitted:
            return OpsRefused("admission", "; ".join(adm.refusals))
        if self.registry_problems:
            return OpsRefused("registry", "; ".join(self.registry_problems[:PROBLEMS_SHOWN]))
        return None


# ---------------------------------------------------------------------------------------------------- the commands
class ArmOps:
    """The ARM / market-data commands of one CLI invocation, over the `OpsService`'s connection, transactions and
    audit rows."""

    def __init__(self, svc: OpsService, ctx: ArmContext, etcd: OpsEtcd | None, *, etcd_timeout_s: float = 2.0) -> None:
        self._svc = svc
        self._ctx = ctx
        self._etcd = etcd
        self._etcd_timeout = etcd_timeout_s

    @property
    def _conn(self) -> PgConnection:
        return self._svc.conn

    @property
    def context(self) -> ArmContext:
        return self._ctx

    @property
    def service(self) -> OpsService:
        return self._svc

    async def local_config(self) -> tuple[ConfigCacheRow | None, Any]:
        """The cached (installed) document row and the latest accept (the S35 views)."""
        return await self._local_config()

    async def local_ids(self) -> tuple[KnownIds, list[AdapterLoad], list[str]]:
        """This engine's known ids and admission loads (the S35 `arm config resolve`)."""
        return await self._local_ids()

    def _now(self) -> int:
        return self._svc.clock_ms()

    # ------------------------------------------------------------------------------------------------ controls
    async def _control_write(self, rep: Report, actor: Actor,
                             change: Callable[[PgConnection, Any, int | None], Awaitable[dict[str, Any] | None]],
                             ) -> Report:
        """One controls change under the table lock, with its audit row in the same transaction. `change` returns
        the audit detail of an applied change, or None for a no-op (audited `noop`, the row untouched)."""
        actor.validate(two_person=False)
        target = "arm_controls"
        try:
            async with self._svc.plain() as conn:
                await conn.execute(SQL_LOCK_TIMEOUT_2S)
                await conn.execute(A.SQL_CONTROLS_LOCK)
                row = await conn.fetchrow(A.SQL_CONTROLS_FOR_UPDATE)
                if row is None:
                    raise OpsRefused("no_controls_row", "arm_controls has no row (migration 0017 inserts it)")
                epoch = await stored_epoch(conn)
                detail = await change(conn, row, epoch)
                if detail is None:
                    rep.outcome = "noop"
                    rep.audit_id = await self._svc.audit(conn, rep.command, target, actor, "noop",
                                                         {"paused": bool(row[0]), "drain_notices": bool(row[3])},
                                                         epoch)
        except OpsRefused as r:
            return await self._svc.refused(rep, r, target, actor)
        if rep.outcome != "noop":
            rep.outcome = "applied"
        return rep

    async def pause(self, actor: Actor, *, drain_notices: bool) -> Report:
        rep = Report("arm.pause")

        async def change(conn: PgConnection, row: Any, epoch: int | None) -> dict[str, Any] | None:
            was_paused, was_drain = bool(row[0]), bool(row[3])
            drain = drain_notices or (was_paused and was_drain)
            if was_paused and drain == was_drain:
                rep.fields.update({"paused": True, "since": iso_ms(row[2]), "drain_notices": was_drain})
                rep.warnings.append("already paused" + (" with --drain-notices" if was_drain else ""))
                return None
            detail = {"was_paused": was_paused, "drain_notices": drain, "was_drain_notices": was_drain}
            audit_id = await self._svc.audit(conn, rep.command, "arm_controls", actor, "applied", detail, epoch)
            got = await conn.fetchrow(A.SQL_CONTROLS_PAUSE, PAUSE_REASON, drain, actor.operator, audit_id)
            if got is None:
                raise OpsRefused("no_controls_row", "arm_controls has no row")
            rep.audit_id = audit_id
            rep.fields.update({"paused": True, "pause_reason": PAUSE_REASON, "since": iso_ms(got[0]),
                               "drain_notices": drain})
            return detail

        out = await self._control_write(rep, actor, change)
        if out.outcome == "applied":
            out.warnings.append("no ARM decision is made while paused (the arm-tracker keeps arming and ending "
                                "positions); D7 FAILED pings still go out; `arm resume` ends the pause and never "
                                "clears FAILED")
            if out.fields.get("drain_notices"):
                out.warnings.append("--drain-notices: the arm-relay finishes queued, unsent ARM notices as "
                                    "`suppressed` (never a FAILED ping); they are not sent after the resume")
        return out

    async def resume(self, actor: Actor) -> Report:
        rep = Report("arm.resume")

        async def change(conn: PgConnection, row: Any, epoch: int | None) -> dict[str, Any] | None:
            if not bool(row[0]):
                rep.fields["paused"] = False
                rep.warnings.append("not paused by an operator: nothing to resume (a `config_hash` pause ends when "
                                    "the accepted document is installed)")
                return None
            detail = {"paused_at_ms": row[2], "pause_reason": row[1], "drain_notices": bool(row[3])}
            audit_id = await self._svc.audit(conn, rep.command, "arm_controls", actor, "applied", detail, epoch)
            got = await conn.fetchrow(A.SQL_CONTROLS_RESUME, actor.operator, audit_id)
            if got is None:
                raise OpsRefused("not_paused", "the pause ended during the command")
            rep.audit_id = audit_id
            rep.fields.update({"paused": False, "was_paused_since": iso_ms(row[2]), "drain_notices": False})
            return detail

        out = await self._control_write(rep, actor, change)
        if out.outcome == "applied":
            out.warnings.append("replay re-decides the paused bars inside its horizon (older ones become one "
                                "UNRESOLVED_RANGE row); a resume never clears FAILED (`arm failed show`)")
        return out

    async def notify_test(self, actor: Actor) -> Report:
        rep = Report("arm.notify_test")

        async def change(conn: PgConnection, row: Any, epoch: int | None) -> dict[str, Any] | None:
            detail = {"previous_notify_test_at_ms": row[4]}
            audit_id = await self._svc.audit(conn, rep.command, "arm_controls", actor, "applied", detail, epoch)
            got = await conn.fetchrow(A.SQL_CONTROLS_NOTIFY_TEST, actor.operator, audit_id)
            if got is None:
                raise OpsRefused("no_controls_row", "arm_controls has no row")
            rep.audit_id = audit_id
            rep.fields["notify_test_at"] = iso_ms(got[0])
            return detail

        out = await self._control_write(rep, actor, change)
        if out.outcome == "applied":
            out.warnings.append("recorded in arm_controls.notify_test_at_ms only: the ARM test-notice sender is not "
                                "wired in this build (S32 deviation 1), so no notice is sent yet")
        return out

    # ------------------------------------------------------------------------------------------------ config
    async def _local_config(self) -> tuple[ConfigCacheRow | None, Any]:
        cache_row = await self._conn.fetchrow(SQL_CONFIG_CACHE_GET)
        accepted = await self._conn.fetchrow(SQL_ACCEPTED_LATEST)
        return (None if cache_row is None else cache_from_row(cache_row)), accepted

    async def _read_source(self) -> Document:
        """The Supabase read; every failure is an `OpsRefused` with a fixed code and a sanitised reason (never the
        DSN, host or a driver message)."""
        try:
            source = self._ctx.config_source()
        except Exception as exc:
            raise OpsRefused("config_source_settings", safe_source_error(exc)) from None
        if source is None:
            raise OpsRefused("no_config_source", "XTNL_ARM_CONFIG_DSN is not set: the pending document cannot be read")
        if isinstance(source, list):
            raise OpsRefused("config_source_settings", "; ".join(source))
        try:
            doc = await asyncio.wait_for(source.read(), self._ctx.read_timeout_s)
        except SelfCheckError as exc:
            raise OpsRefused("self_check_failed", ", ".join(exc.problems[:20])) from None
        except ConfigSourceError as exc:
            raise OpsRefused("source_unreadable", str(exc)) from None
        except Exception as exc:
            reason = "timeout" if isinstance(exc, TimeoutError) else safe_source_error(exc)
            raise OpsRefused("source_unreadable", reason) from None
        if isinstance(doc, DocumentProblem):
            raise OpsRefused("document_problem", doc.page_text)
        return doc

    async def _local_ids(self) -> tuple[KnownIds, list[AdapterLoad], list[str]]:
        """This engine's ids (adapters, instruments, accounts, routes `adapter:account` and `*`, as the engine's
        `known_ids`) and per-adapter loads for the admission check, from PostgreSQL and the local settings."""
        instruments: dict[str, set[str]] = {}
        for r in await self._conn.fetch(A.SQL_INSTRUMENT_SYMBOLS):
            instruments.setdefault(str(r[0]), set()).add(str(r[1]))
        accounts: dict[str, set[str]] = {}
        for r in await self._conn.fetch(A.SQL_ACCOUNT_IDS):
            accounts.setdefault(str(r[0]), set()).add(str(r[1]))
        held: dict[str, list[str]] = {}
        for r in await self._conn.fetch(A.SQL_HELD_SYMBOLS):
            held.setdefault(str(r[0]), []).append(str(r[1]))
        armed = {str(r[0]): (int(r[1]), int(r[2])) for r in await self._conn.fetch(A.SQL_ARMED_LOAD)}
        if not instruments:
            raise OpsRefused("known_ids_missing", "the instruments table is empty: no row can be checked against "
                                                  "this engine's ids yet")
        adapters = sorted(set(instruments) | set(accounts))
        routes = {"*"} | {f"{aid}:{acct}" for aid, accts in accounts.items() for acct in accts}
        known = KnownIds(symbols=frozenset(s for syms in instruments.values() for s in syms),
                         adapters=frozenset(adapters),
                         accounts={acct: acct for accts in accounts.values() for acct in accts},
                         routes=frozenset(routes))
        notes: list[str] = []
        ticks = parse_tick_symbols(self._ctx.tick_symbols, adapters)
        if isinstance(ticks, list):
            notes.extend(ticks)
            ticks = {}
        loads: list[AdapterLoad] = []
        for aid in adapters:
            prewarm = list(dict.fromkeys([*held.get(aid, []), *ticks.get(aid, ())]))
            handles, stop_accounts = armed.get(aid, (0, 0))
            loads.append(AdapterLoad(
                adapter_id=aid, symbols=frozenset(instruments.get(aid, set())), prewarm=tuple(prewarm[:PREWARM_CAP]),
                bar_capable=aid in self._ctx.bar_capable, bars_per_min=self._ctx.bars_per_min.get(aid),
                stop_reads=self._ctx.stop_reads, armed_handles=handles, stop_accounts=stop_accounts))
        return known, loads, notes

    async def _registry_check(self, tokens: Sequence[str], now: int) -> tuple[list[str], list[str]]:
        """PR13: every fresh node's health record must list every referenced token (a record without the field
        lists none; an unreadable record fails closed). Returns (problems, notes)."""
        etcd = self._etcd
        if etcd is None:
            return ["etcd_unconfigured: XTNL_ETCD_ENDPOINTS is not set, the nodes' registries cannot be checked"], []
        try:
            kvs = await asyncio.to_thread(etcd.get_prefix, HEALTH_PREFIX, timeout_s=self._etcd_timeout)
        except Exception as exc:
            return [f"etcd_unreachable: {type(exc).__name__}"], []
        problems: list[str] = []
        fresh = 0
        wanted = set(tokens)
        for kv in kvs:
            try:
                rec = HealthRecord.from_json(kv.value)
            except (ValueError, KeyError, TypeError):
                problems.append(f"{escape_text(kv.key, 100)}: unreadable health record (lacks every version)")
                continue
            if not rec.fresh(now):
                continue
            fresh += 1
            missing = sorted(wanted - set(rec.registry))
            if missing:
                shown = ", ".join(escape_text(t, 64) for t in missing[:10])
                more = f" and {len(missing) - 10} more" if len(missing) > 10 else ""
                hint = "" if rec.registry else " (no registry published: XTNL_MD_ENABLED is off on that node)"
                problems.append(f"{escape_text(rec.node, 64)} lacks {shown}{more}{hint}")
        notes = [] if fresh else ["no fresh node health record: no running node's registry was checked"]
        return problems, notes

    async def _assess(self, doc: Document, cache: ConfigCacheRow | None) -> Assessment:
        now = self._now()
        if cache is None:
            stored: tuple[tuple[str, str], ...] = ()
            head, count = SeenRows().head, 0
        else:
            stored, head, count = cache.row_hashes, cache.chain_head, cache.chain_rows
        verdict = check_rows(stored, head, count, doc.row_hashes(), schema_ahead=doc.schema_ahead)
        a = Assessment(doc=doc, verdict=verdict, seen=dict(stored))
        if verdict.state is ChainState.TAMPERED or doc.schema_ahead:
            return a
        if self._ctx.settings_problems:
            a.local_problem = OpsRefused("settings", "; ".join(self._ctx.settings_problems))
            return a
        try:
            known, loads, notes = await self._local_ids()
        except OpsRefused as r:
            a.local_problem = r
            return a
        a.registry_notes.extend(notes)
        a.validation = validate_document(doc, known, self._ctx.registry, seen=a.seen, at_ms=now)
        valid = a.validation.document
        if a.validation.status is not ValidationStatus.VALID or valid is None:
            return a
        a.no_bars = no_bar_refusals(valid, self._ctx.bar_capable, now)
        a.admission = admission(valid, loads, self._ctx.limits, at_ms=now, registry=self._ctx.registry)
        a.tokens = referenced_tokens(valid, self._ctx.registry)
        problems, reg_notes = await self._registry_check(a.tokens, now)
        a.registry_problems.extend(problems)
        a.registry_notes.extend(reg_notes)
        return a

    async def config_show(self, *, pending: bool) -> Report:
        rep = Report("arm.config.show")
        cache, accepted = await self._local_config()
        rep.fields.update({
            "accepted_hash": None if accepted is None else str(accepted[1]).strip(),
            "accepted_at": None if accepted is None else iso_ms(accepted[4]),
            "accepted_by": None if accepted is None else accepted[5],
            "accepted_mode": None if accepted is None else accepted[3],
            "cached_hash": None if cache is None else cache.doc_hash, "seen_rows": 0 if cache is None else cache.chain_rows,
            "cache_updated": None if cache is None else iso_ms(cache.updated_at_ms)})
        controls = await self._conn.fetchrow(SQL_CONTROLS_GET)
        if controls is not None:
            rep.fields.update({"paused": bool(controls[0]), "pause_reason": controls[1],
                               "paused_since": iso_ms(controls[2]), "drain_notices": bool(controls[3]),
                               "notify_test_at": iso_ms(controls[4])})
        rep.sections["nodes"] = [{
            "node": r[0], "epoch": r[1], "heartbeat": iso_ms(r[2]), "heartbeat_age_s": None if r[3] is None
            else int(r[3]) // 1000, "leader": bool(r[4]), "running": bool(r[5]), "paused": bool(r[6]),
            "pause_reason": r[7], "installed_hash": None if r[8] is None else str(r[8]).strip(),
            "armed": r[9], "overdue": r[10], "failed_scopes": r[11], "failed_global": bool(r[12]),
        } for r in await self._conn.fetch(A.SQL_ARM_STATUS_NODES)]
        rep.sections["accepted"] = [{
            "accepted_id": r[0], "doc_hash": str(r[1]).strip(), "schema_version": r[2], "mode": r[3],
            "at": iso_ms(r[4]), "by": r[5], "audit_id": r[6]} for r in await self._conn.fetch(A.SQL_ACCEPTED_RECENT, 5)]
        if accepted is None:
            rep.warnings.append("no accepted document: the ARM is idle until `arm config accept <hash>`")
        elif cache is None:
            rep.warnings.append("the latest accepted document is not installed yet (no cached document): the ARM is "
                                "paused (config_hash) until the arm-config task installs it at the next "
                                "T + post_grace")
        elif cache.doc_hash != str(accepted[1]).strip():
            rep.warnings.append("the cached (installed) document is not the latest accepted one: the ARM is paused "
                                "(config_hash) until the arm-config task installs it at the next T + post_grace")
        if not pending:
            return rep
        try:
            doc = await self._read_source()
        except OpsRefused as r:
            rep.outcome = "refused"
            rep.fields["refused"] = r.code
            rep.fields["detail"] = r.detail
            return rep
        a = await self._assess(doc, cache)
        self._show_assessment(rep, a, cache, None if accepted is None else str(accepted[1]).strip())
        return rep

    def _show_assessment(self, rep: Report, a: Assessment, cache: ConfigCacheRow | None,
                         accepted_hash: str | None) -> None:
        doc, v = a.doc, a.verdict
        rep.fields.update({"read_hash": doc.doc_hash, "read_schema_version": doc.schema_version,
                           "read_rows": len(doc.rows), "chain": v.state.value, "pending_rows": len(v.pending)})
        was = _cached_texts(cache)
        tampered = {t.ref: t.kind.value for t in v.tampered}
        pending = set(v.pending)
        rows: list[dict[str, Any]] = []
        for r in doc.rows:
            status = ("tampered:" + tampered[r.ref]) if r.ref in tampered else "pending" if r.ref in pending else "seen"
            row: dict[str, Any] = {"ref": r.ref, "hash": r.row_hash[:12], "status": status}
            if status != "seen":
                row["row"] = escape_text(r.canonical, SHOW_TEXT_LIMIT)
                if r.ref in was and was[r.ref] != r.canonical:
                    row["was"] = escape_text(was[r.ref], SHOW_TEXT_LIMIT)
            rows.append(row)
        for ref, kind in tampered.items():
            if ref not in doc.row_hashes():
                row = {"ref": escape_text(ref, 64), "hash": a.seen.get(ref, "")[:12], "status": f"tampered:{kind}"}
                if ref in was:
                    row["was"] = escape_text(was[ref], SHOW_TEXT_LIMIT)
                rows.append(row)
        rep.sections["rows"] = rows
        if a.validation is not None:
            rep.fields["valid"] = a.validation.status is ValidationStatus.VALID
            rep.sections["problems"] = [{"code": p.code, "ref": p.ref, "detail": escape_text(p.detail) or None}
                                        for p in a.validation.problems[:PROBLEMS_SHOWN]]
        if a.admission is not None:
            adm = a.admission
            rep.fields.update({"admission_series": adm.series, "admission_bytes": adm.total_bytes,
                               "admission_byte_limit": adm.byte_limit})
            rep.sections["admission"] = [{
                "adapter": x.adapter_id, "symbols": x.symbols, "broker_series": x.broker_series,
                "tick_series": x.tick_series, "indicators": x.indicators, "bytes": x.bytes,
                "reads_per_bar_end": x.reads_per_bar_end, "stop_reads": x.stop_reads_per_bar_end,
                "read_budget": x.read_budget, "reopen_reads": x.reopen_reads, "reopen_budget": x.reopen_budget}
                for x in adm.adapters]
            rep.warnings.extend(adm.notes)
        if a.tokens:
            rep.fields["referenced_versions"] = list(a.tokens)
        rep.warnings.extend(a.registry_notes)
        refusal = a.refusal()
        if refusal is not None:
            rep.fields["acceptable"] = False
            rep.fields["refused_on_accept"] = refusal.code
            rep.warnings.append(f"`arm config accept` would be refused: {refusal}")
        elif accepted_hash == doc.doc_hash:
            rep.fields["acceptable"] = False
            rep.warnings.append("this document is the latest accepted one already")
        else:
            rep.fields["acceptable"] = True
            rep.fields["accept_command"] = f"arm config accept {doc.doc_hash}"

    async def config_accept(self, doc_hash: str, actor: Actor) -> Report:
        """The local accept (module docstring): refused unless the read still hashes to `doc_hash` and passes every
        check; then one `arm_accepted` row with its audit row under the table lock."""
        check_hash(doc_hash)
        actor.validate(two_person=False)
        rep = Report("arm.config.accept")
        target = doc_hash
        try:
            cache, accepted = await self._local_config()
            doc = await self._read_source()
            if doc.doc_hash != doc_hash:
                raise OpsRefused("hash_changed", f"the pending document now hashes to {doc.doc_hash}: it changed "
                                                 "after it was reviewed; review it again with `arm config show "
                                                 "--pending`")
            previous = None if accepted is None else str(accepted[1]).strip()
            a = await self._assess(doc, cache)
            refusal = a.refusal()
            if refusal is not None:
                raise refusal
            adm = a.admission
            detail: dict[str, Any] = {
                "doc_hash": doc.doc_hash, "schema_version": doc.schema_version, "rows": len(doc.rows),
                "pending": [r.audit_ref for r in doc.rows if r.ref in set(a.verdict.pending)][:AUDIT_REFS],
                "previous": previous, "series": None if adm is None else adm.series,
                "bytes": None if adm is None else adm.total_bytes, "referenced_versions": list(a.tokens)}
            async with self._svc.plain() as conn:
                await conn.execute(SQL_LOCK_TIMEOUT_2S)
                await conn.execute(A.SQL_ACCEPTED_LOCK)
                latest = await conn.fetchrow(SQL_ACCEPTED_LATEST)
                epoch = await stored_epoch(conn)
                if latest is not None and str(latest[1]).strip() == doc_hash:
                    rep.outcome = "noop"
                    rep.audit_id = await self._svc.audit(conn, rep.command, target, actor, "noop",
                                                         {**detail, "already_accepted": True}, epoch)
                    rep.fields.update({"doc_hash": doc_hash, "accepted_id": int(latest[0])})
                    rep.warnings.append("already the latest accepted document")
                    return rep
                # `arm_accepted` is ordered by accepted_at_ms: a host clock stepped back must never leave this
                # accept behind the latest one (reported applied, but never installed)
                now = self._now()
                accepted_at = now if latest is None else max(now, int(latest[4]) + 1)
                if accepted_at != now:
                    detail["clock_behind_ms"] = accepted_at - now
                    rep.warnings.append(f"this host's clock is {accepted_at - now} ms behind the latest accept: "
                                        "accepted_at is recorded just after it (check the clock)")
                audit_id = await self._svc.audit(conn, rep.command, target, actor, "applied", detail, epoch)
                row = await conn.fetchrow(A.SQL_ACCEPTED_INSERT, doc_hash, doc.schema_version, accepted_at,
                                          actor.operator, audit_id)
                if row is None:
                    raise RuntimeError("arm_accepted insert returned no row")
                rep.audit_id = audit_id
                rep.fields.update({"doc_hash": doc_hash, "accepted_id": int(row[0]), "previous": previous,
                                   "pending_rows_accepted": len(a.verdict.pending), "epoch": epoch})
        except OpsRefused as r:
            return await self._svc.refused(rep, r, target, actor)
        rep.outcome = "applied"
        rep.warnings.extend(a.registry_notes)
        if adm is not None:
            rep.warnings.extend(adm.notes)
        rep.warnings.append("the arm-config task installs it at the first T + post_grace after its next poll "
                            "(within 120 s); until then the installed document stays in force and the ARM pauses "
                            "(config_hash) if the cache differs; armed positions keep their pins")
        return rep

    # ------------------------------------------------------------------------------------------------ FAILED
    async def failed_show(self, *, include_cleared: bool, limit: int) -> Report:
        rep = Report("arm.failed.show")
        epoch = await stored_epoch(self._conn)
        clears: dict[str, list[int]] = {}
        if epoch is not None:
            for r in await self._conn.fetch(A.SQL_PENDING_CLEARS, epoch):
                clears.setdefault(str(r[1]), []).append(int(r[0]))
        open_n = 0
        for r in await self._conn.fetch(A.SQL_FAILED_LIST, include_cleared, limit):
            scope = f"{r[1]}:{r[2]}"
            positions = [str(p) for p in (r[8] or [])]
            shown = ",".join(escape_text(p, 100) for p in positions[:SHOW_POSITIONS])
            if len(positions) > SHOW_POSITIONS:
                shown += f" and {len(positions) - SHOW_POSITIONS} more"
            if r[16] is None:
                open_n += 1
            rep.rows.append({
                "failure_id": r[0], "scope": escape_text(scope, 200), "edge": r[3], "reason": r[4], "probe": r[5],
                "ping_id": r[6], "counters": _counters_text(r[7]), "positions": len(positions),
                "position_ids": shown or None, "first_fault": iso_ms(r[9]), "entered": iso_ms(r[10]),
                "epoch": r[11], "last_ping": iso_ms(r[12]), "pings": r[13], "last_probe": iso_ms(r[14]),
                "last_probe_result": r[15], "cleared": iso_ms(r[16]), "clear_request_id": r[17],
                "pending_clears": clears.get(scope) or None})
        rep.fields["open"] = open_n
        if open_n:
            rep.warnings.append("no advice for the affected positions until FAILED is cleared; fix the recorded cause "
                                "(probe), then `arm failed clear <scope> --operator A --reason \"...\"`")
        return rep

    async def failed_clear(self, scope: str, actor: Actor, *, wait_s: float) -> Report:
        check_scope_target(scope)
        kind, _, scope_id = scope.partition(":")

        async def precheck(tx: FencedTx) -> str | None:
            return None if await tx.fetchrow(A.SQL_FAILED_OPEN_SCOPE, kind, scope_id) is not None else "not_failed"

        return await self._file("arm.failed.clear", "arm_clear_failed", scope, "{}", actor, wait_s=wait_s,
                                precheck=precheck)

    # ------------------------------------------------------------------------------------------------ arm-tracker
    def _position_precheck(self, target: str) -> Callable[[FencedTx], Awaitable[str | None]]:
        adapter, _, position = target.partition(":")

        async def precheck(tx: FencedTx) -> str | None:
            row = await tx.fetchrow(A.SQL_ARM_POSITION_STATE, adapter, position)
            return "not_armed" if row is None or str(row[0]) in TERMINAL_ARM_STATES else None

        return precheck

    async def disarm(self, target: str, actor: Actor, *, wait_s: float) -> Report:
        check_position_target(target)
        return await self._file("arm.disarm", "arm_disarm", target, "{}", actor, wait_s=wait_s,
                                precheck=self._position_precheck(target))

    async def rearm(self, target: str, actor: Actor, *, wait_s: float) -> Report:
        check_position_target(target)
        return await self._file("arm.rearm", "arm_rearm", target, "{}", actor, wait_s=wait_s,
                                precheck=self._position_precheck(target))

    async def repin(self, target: str, actor: Actor, *, strategy_code: str | None, strategy_version: int | None,
                    wait_s: float) -> Report:
        check_position_target(target)
        if (strategy_code is None) != (strategy_version is None):
            raise OpsUsageError("--strategy and --strategy-version go together (neither: the assignment in force)")
        payload = "{}"
        if strategy_code is not None and strategy_version is not None:
            if not re.fullmatch(r"[a-z0-9_]{1,32}", strategy_code) or not 1 <= strategy_version <= 2**31 - 1:
                raise OpsUsageError("--strategy is a strategy code ([a-z0-9_]{1,32}) and --strategy-version >= 1")
            payload = _dumps({"v": PAYLOAD_VERSION, "strategy_code": strategy_code,
                              "strategy_version": strategy_version})
        return await self._file("arm.repin", "arm_repin", target, payload, actor, wait_s=wait_s,
                                precheck=self._position_precheck(target))

    # ------------------------------------------------------------------------------------------------ market data
    async def md_backfill(self, series: str, from_ms: int, to_ms: int, actor: Actor, *, wait_s: float) -> Report:
        try:
            sid = SeriesId.parse(series)
        except ValueError as exc:
            raise OpsUsageError("the series id is adapter:symbol:spec:SIDE:SOURCE (`md show` names it)") from exc
        if sid.canonical != series:
            raise OpsUsageError("give the series id in its canonical form")
        if not 0 <= from_ms <= to_ms:
            raise OpsUsageError("--from-ms and --to-ms are bucket starts in epoch ms with from <= to")
        payload = _dumps({"v": PAYLOAD_VERSION, "from_bucket_ms": from_ms, "to_bucket_ms": to_ms})
        return await self._file("md.backfill", "md_backfill", series, payload, actor, wait_s=wait_s)

    async def md_rebuild(self, indicator_id: str, actor: Actor, *, wait_s: float) -> Report:
        if not INDICATOR_ID_RE.fullmatch(indicator_id or ""):
            raise OpsUsageError("the indicator id is 16 lower-case hex characters (`md indicators` names it)")
        return await self._file("md.rebuild", "md_rebuild", indicator_id, "{}", actor, wait_s=wait_s)

    async def md_writer_reset(self, actor: Actor, *, wait_s: float) -> Report:
        return await self._file("md.writer_reset", "md_writer_reset", MD_WRITER_TARGET, "{}", actor, wait_s=wait_s)

    async def md_rehash(self, actor: Actor, *, wait_s: float) -> Report:
        return await self._file("md.rehash", "md_rehash", MD_REHASH_TARGET, "{}", actor, wait_s=wait_s)

    # ------------------------------------------------------------------------------------------------ requests
    async def _file(self, command: str, kind: str, target: str, payload: str, actor: Actor, *, wait_s: float,
                    precheck: Callable[[FencedTx], Awaitable[str | None]] | None = None) -> Report:
        """File one ARM / market-data request (module docstring): the heartbeat and budget checks, the pending and
        moot checks, then the audit row and the request in one fenced transaction; then wait for the answer."""
        check_wait(wait_s)
        actor.validate(two_person=False)
        rep = Report(command)
        request_id: int | None = None
        try:
            async with self._svc.fenced() as tx:
                hb = await tx.fetchrow(A.SQL_ARM_HEARTBEAT, tx.epoch)
                if hb is None or hb[2] is None or int(hb[2]) > HEARTBEAT_MAX_AGE_MS:
                    age = "none" if hb is None or hb[2] is None else f"{int(hb[2]) // 1000} s old"
                    raise OpsRefused("no_arm_heartbeat", f"arm_status has no leader heartbeat of epoch {tx.epoch} "
                                                         f"younger than {HEARTBEAT_MAX_AGE_MS // 1000} s ({age}): "
                                                         "the leader's optional services are not running")
                tracker = kind in TRACKER_REQUEST_KINDS
                budget = TRACKER_REQUEST_KINDS if tracker else ARM_REQUEST_KINDS - TRACKER_REQUEST_KINDS
                n = await tx.fetchrow(A.SQL_ARM_PENDING_COUNT, tx.epoch, sorted(budget))
                pending_n = 0 if n is None else int(n[0])
                if pending_n >= MAX_PENDING_ARM_REQUESTS:
                    which = "arm-tracker (disarm / rearm / repin)" if tracker else "ARM / market-data"
                    raise OpsRefused("too_many_pending", f"{pending_n} {which} requests are pending in "
                                                         f"epoch {tx.epoch}; wait for them to be answered")
                dup = await tx.fetchrow(S.SQL_REQUEST_PENDING, kind, target, tx.epoch)
                if dup is not None:
                    raise OpsRefused("already_pending", f"request {dup[0]} for {target} is not answered yet")
                moot = None if precheck is None else await precheck(tx)
                if moot is not None:
                    rep.outcome = "noop"
                    rep.fields.update({"kind": kind, "target": target, "request": f"moot:{moot}", "epoch": tx.epoch})
                    rep.audit_id = await self._svc.audit(tx, command, target, actor, "noop",
                                                         {"kind": kind, "moot": moot}, tx.epoch)
                else:
                    audit_id = await self._svc.audit(tx, command, target, actor, "applied",
                                                     {"kind": kind, "payload": json.loads(payload)}, tx.epoch)
                    row = await tx.write_returning(S.SQL_REQUEST_INSERT_PAYLOAD, kind, target, audit_id, payload,
                                                   must=True)
                    assert row is not None
                    request_id = int(row[0])
                    rep.audit_id = audit_id
                    rep.fields.update({"kind": kind, "target": target, "request_id": request_id, "epoch": tx.epoch})
        except OpsRefused as r:
            return await self._svc.refused(rep, r, target, actor, {"kind": kind})
        if request_id is None:
            return rep
        rep.outcome = "applied"
        await self._await_answer(rep, request_id, wait_s, kind)
        return rep

    async def _await_answer(self, rep: Report, request_id: int, wait_s: float, kind: str) -> None:
        """Poll the request's answer for up to `wait_s`: applied / moot / refused (exit 3), else pending."""
        polls = int(math.ceil(wait_s / self._ctx.poll_s)) if wait_s > 0 else 0
        result: str | None = None
        for i in range(polls + 1):
            try:
                row = await self._conn.fetchrow(A.SQL_REQUEST_RESULT, request_id)
            except Exception as exc:
                rep.warnings.append(f"answer not read ({type(exc).__name__}); `audit tail` shows the filing")
                break
            if row is not None and row[0] is not None:
                result = "applied" if row[1] is None else str(row[1])
                break
            if i < polls:
                await self._ctx.sleep(self._ctx.poll_s)
        if result is None:
            rep.fields["request"] = "pending"
            if kind in TRACKER_REQUEST_KINDS:
                rep.warnings.append("not answered yet: the leader's arm-tracker answers it on its next pass "
                                    "(applied, refused or moot); while the ARM is off or idle it stays pending in "
                                    "this epoch and lapses at the next epoch change")
            else:
                rep.warnings.append("not answered yet: the leader's owning task answers it (applied, refused or "
                                    "moot); a request of an older epoch is moot")
            return
        rep.fields["request"] = escape_text(result, 100)
        if result.startswith("refused"):
            rep.outcome = "refused"
            rep.fields["refused"] = escape_text(result.partition(":")[2] or "refused", 100)


__all__ = [
    "BAR_CAPABLE_ADAPTERS", "BROKER_BAR_ADAPTERS", "HEARTBEAT_MAX_AGE_MS", "MAX_PENDING_ARM_REQUESTS",
    "PAUSE_REASON", "REST_PREFIX", "TRACKER_REQUEST_KINDS", "ArmContext", "ArmOps", "Assessment",
    "bar_capable_adapters", "bars_caps", "check_hash", "check_position_target",
    "check_scope_target", "default_md_reader_factory", "default_source_factory", "envelope_from_env",
    "no_bar_refusals", "referenced_tokens",
]
