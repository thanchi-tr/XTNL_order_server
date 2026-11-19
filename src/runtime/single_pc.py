"""Single-PC self-healing (operations.md §4 and §17, accepted deviation): SAFE_HOLD auto-resume and the
"not recording" page. Active only with `XTNL_SINGLE_PC=1` (set by `deploy/local/start-xtnl.ps1` /
`run-local.ps1`; never by the AWS systemd units). Off: nothing here is built and SAFE_HOLD stays operator-only.

**SAFE_HOLD auto-resume.** On the single Windows PC a SAFE_HOLD (the anti-flap brake) left the engine unable to
record until two people ran `safe-hold resume`; a PC shutdown is not a flap a human must judge. With the flag on,
a node that finds SAFE_HOLD set (at STARTING, or while in SAFE_HOLD; a hold set by an earlier run counts the same)
and passes its hard checks (PostgreSQL writable, etcd reachable, engine Redis writable, clock within the limit)
waits a cool-down, then performs the operator CLI's resume in one fenced transaction: `cluster_state.safe_hold`
cleared, `last_resume_at` / `resumed_by` set, one `operator_audit` row (`safe_hold.resume`, outcome `applied`, the
held reason in the detail), then the etcd mirror. The node then campaigns normally.

* Cool-down: `XTNL_SINGLE_PC_RESUME_FIRST_S` (300) for the first auto-resume, doubling for each further one within
  `XTNL_SINGLE_PC_RESUME_WINDOW_S` (86400) before the hold was set, capped at `XTNL_SINGLE_PC_RESUME_MAX_S` (3600):
  5, 10, 20, 40, 60, 60 min. The cool-down clock restarts whenever a hard check fails.
* Budget: once `XTNL_SINGLE_PC_RESUME_LIMIT` (6) auto-resumes happened within the window before the hold, it is not
  auto-resumed: a genuine restart loop needs a human. It pages critical once and stays held. The count is read
  from `operator_audit` (durable across restarts) and anchored at the hold's `safe_hold_since`, so the decision for
  one hold never changes while it waits. A person's applied resume starts a fresh budget: only auto-resumes after
  the last resume by anyone else count.
* The resume clears only the hold that was judged (same `safe_hold_since` and reason); a newer hold set meanwhile is
  left set and judged afresh.
* Actor: `cluster_state.resumed_by` = `system:single-pc-auto-resume`. `operator_audit.operator` is
  `system.single-pc-auto-resume` (migration 0007's CHECK allows only `[A-Za-z0-9._@-]`, so no colon); the detail
  names the actor `system:single-pc-auto-resume`. No approver: this is the deviation from two-person control.
* Each auto-resume pages info "SAFE_HOLD auto-resumed on the single PC". Only the leader sends notifications
  (operations.md §2), so the page is logged at once and handed to the notification path when this node next
  reaches LEADER (the outbox; unconfigured channels: logged only).

**Not-recording page.** With the flag on, a node that has held no leader term for `XTNL_SINGLE_PC_IDLE_PAGE_S`
(600) since it started (or since its last term) pages critical once, naming its role and failing hard checks, and
pages info when it reaches LEADER again.

**Critical pages without a leader** (operations.md §2 exception, single PC only): the two critical pages above
(`SAFE_HOLD auto-resume stopped`, `engine not recording`) arise exactly when this node holds no leader term, so the
leader's outbox can never carry them. `DirectChannelPager` sends them straight to the configured Oversee channels
(`XTNL_` email / push settings, the same transports the relay uses), trying a failed channel twice more; with no
channel configured they are logged only (`PAGE[critical]`, listed by `deploy/local/status-xtnl.ps1`). Only this node
produces these pages and there is no peer on the single PC, so nothing can send them twice. The info pages still go
through the leader's outbox.

Phase 1 read-only: nothing here contacts a broker; the resume only clears the brake that keeps the engine from
ingesting (reading) broker state. ActionGate is untouched.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Final, Literal, Protocol

import httpx

from src.db.cluster_state import SQL_AUDIT_INSERT, SQL_COUNT_AUTO_RESUMES, SQL_SAFE_HOLD_AUTO_RESUME
from src.db.epoch import epoch_transaction
from src.db.fence import EpochMismatch, stored_epoch
from src.db.pg import ConnectionSource
from src.notifications.errors import safe_error
from src.notifications.factory import build_channels
from src.notifications.mail import SmtpFactory, default_smtp_factory
from src.notifications.settings import NotificationSettings
from src.oversee.notify import OutboxRow, Severity
from src.runtime.health import HealthRecord
from src.runtime.roles import ClusterState, NodeRole

log = logging.getLogger(__name__)

SINGLE_PC_VAR: Final = "XTNL_SINGLE_PC"
RESUME_FIRST_VAR: Final = "XTNL_SINGLE_PC_RESUME_FIRST_S"
RESUME_MAX_VAR: Final = "XTNL_SINGLE_PC_RESUME_MAX_S"
RESUME_LIMIT_VAR: Final = "XTNL_SINGLE_PC_RESUME_LIMIT"
RESUME_WINDOW_VAR: Final = "XTNL_SINGLE_PC_RESUME_WINDOW_S"
IDLE_PAGE_VAR: Final = "XTNL_SINGLE_PC_IDLE_PAGE_S"
#: Set by systemd for a Type=notify unit (deploy/systemd/xtnl-engine.service): the flag is refused there, so an AWS
#: node keeps operator-only SAFE_HOLD even if its env file wrongly sets XTNL_SINGLE_PC=1.
SYSTEMD_NOTIFY_VAR: Final = "NOTIFY_SOCKET"

AUTO_RESUME_ACTOR: Final = "system:single-pc-auto-resume"
#: operator_audit.operator must match ^[A-Za-z0-9._@-]{2,64}$ (migration 0007), so the colon becomes a dot.
AUDIT_OPERATOR: Final = "system.single-pc-auto-resume"
AUDIT_COMMAND: Final = "safe_hold.resume"
RESUMED_TITLE: Final = "SAFE_HOLD auto-resumed on the single PC"
EXHAUSTED_TITLE: Final = "SAFE_HOLD auto-resume stopped on the single PC"
IDLE_TITLE: Final = "engine not recording on the single PC"
RECORDING_TITLE: Final = "engine recording again on the single PC"
RETRY_MS: Final = 30_000
#: DirectChannelPager: the waits before each further try of a channel that failed (seconds).
DIRECT_RETRY_S: Final = (60.0, 300.0)


# ============================================================================================ settings
@dataclass(frozen=True)
class SinglePcSettings:
    enabled: bool = False
    resume_first_s: int = 300
    resume_max_s: int = 3600
    resume_limit: int = 6
    resume_window_s: int = 86_400
    idle_page_s: int = 600
    problems: tuple[str, ...] = ()

    def cooldown_s(self, prior: int) -> int:
        """The cool-down before auto-resume number `prior + 1`: first, doubling, capped."""
        doublings: int = max(0, min(prior, 30))
        return min(self.resume_first_s << doublings, self.resume_max_s)


def _int(env: Mapping[str, str], name: str, default: int, lo: int, hi: int, problems: list[str]) -> int:
    raw = (env.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        problems.append(f"{name} is not an integer; using {default}")
        return default
    if not lo <= value <= hi:
        problems.append(f"{name} must be {lo}..{hi}; using {default}")
        return default
    return value


def single_pc_settings_from_env(env: Mapping[str, str]) -> SinglePcSettings:
    """Tolerant: a bad value never stops the engine (an exit 78 would end the single PC's start task); it falls
    back to the default and is named in `problems` (logged at start). Only exactly `1` turns the flag on."""
    problems: list[str] = []
    raw = (env.get(SINGLE_PC_VAR) or "").strip()
    if raw not in ("", "0", "1"):
        problems.append(f"{SINGLE_PC_VAR} is neither 0 nor 1; treated as 0 (SAFE_HOLD stays operator-only)")
    enabled = raw == "1"
    if enabled and (env.get(SYSTEMD_NOTIFY_VAR) or "").strip():
        problems.append(f"{SINGLE_PC_VAR}=1 is refused under systemd ({SYSTEMD_NOTIFY_VAR} is set): an AWS node "
                        "keeps operator-only SAFE_HOLD")
        enabled = False
    first = _int(env, RESUME_FIRST_VAR, 300, 1, 86_400, problems)
    cap = _int(env, RESUME_MAX_VAR, 3600, 1, 86_400, problems)
    if cap < first:
        problems.append(f"{RESUME_MAX_VAR} is below {RESUME_FIRST_VAR}; using {RESUME_FIRST_VAR}")
        cap = first
    return SinglePcSettings(
        enabled=enabled, resume_first_s=first, resume_max_s=cap,
        resume_limit=_int(env, RESUME_LIMIT_VAR, 6, 1, 100, problems),
        resume_window_s=_int(env, RESUME_WINDOW_VAR, 86_400, 3600, 7 * 86_400, problems),
        idle_page_s=_int(env, IDLE_PAGE_VAR, 600, 60, 86_400, problems),
        problems=tuple(problems))


def _iso(ms: int | None) -> str:
    if ms is None:
        return "unknown"
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# ============================================================================================ store
@dataclass(frozen=True)
class ResumeRecord:
    epoch: int
    audit_id: int
    last_resume_at: int | None
    held_since: int | None
    held_reason: str | None


class AutoResumeStore(Protocol):
    async def count_auto_resumes(self, since_ms: int) -> int:
        """Applied auto-resumes (operator_audit rows of this actor) at or after `since_ms` and after the last applied
        resume by a person (which starts a fresh budget)."""
        ...

    async def resume(self, reason: str, detail: Mapping[str, object], held_since: int | None,
                     held_reason: str | None) -> ResumeRecord | None:
        """The operator CLI's resume, limited to the hold (`held_since`, `held_reason`) that was judged, plus its
        audit row, in one fenced transaction, then the etcd mirror (best effort). None when that hold is no longer
        set (cleared, or replaced by a newer one), there is no engine_epoch row or the epoch moved meanwhile."""
        ...


class PgAutoResumeStore:
    """`AutoResumeStore` over the control pool: fenced on the stored `engine_epoch` exactly like the operator CLI
    (src/ops/service.py `safe_hold_resume`); `mirror` writes PostgreSQL's SAFE_HOLD to /xtnl/safe_hold."""

    def __init__(self, source: ConnectionSource, *, mirror: Callable[[], Awaitable[object]] | None = None) -> None:
        self._source = source
        self._mirror = mirror

    async def count_auto_resumes(self, since_ms: int) -> int:
        async with self._source.acquire() as conn:
            row = await conn.fetchrow(SQL_COUNT_AUTO_RESUMES, AUDIT_OPERATOR, since_ms)
        return 0 if row is None or row[0] is None else int(row[0])

    async def resume(self, reason: str, detail: Mapping[str, object], held_since: int | None,
                     held_reason: str | None) -> ResumeRecord | None:
        async with self._source.acquire() as conn:
            token = await stored_epoch(conn)
            if token is None:
                log.warning("SAFE_HOLD auto-resume: engine_epoch has no row; nothing to fence the resume on")
                return None
            try:
                async with epoch_transaction(conn, token) as tx:
                    row = await tx.write_returning(SQL_SAFE_HOLD_AUTO_RESUME, AUTO_RESUME_ACTOR, held_since,
                                                   held_reason)
                    if row is None:          # not held, or a newer hold than the one judged: nothing written
                        return None
                    since = None if row[0] is None else int(str(row[0]))
                    held = None if row[1] is None else str(row[1])
                    resumed = None if row[2] is None else int(str(row[2]))
                    full = {"actor": AUTO_RESUME_ACTOR, "safe_hold_since": since, "safe_hold_reason": held,
                            "last_resume_at": resumed, **detail}
                    audit = await tx.fetchrow(SQL_AUDIT_INSERT, AUDIT_COMMAND, "cluster", AUDIT_OPERATOR, None,
                                              reason, "applied",
                                              json.dumps(full, sort_keys=True, separators=(",", ":"), default=str),
                                              token, None)
                    if audit is None:
                        raise RuntimeError("operator_audit insert returned no row")
            except EpochMismatch:
                log.warning("SAFE_HOLD auto-resume: engine_epoch moved during the resume; re-checking next step")
                return None
        if self._mirror is not None:
            try:
                await self._mirror()
            except Exception as exc:     # PostgreSQL is the truth; nodes read it first
                log.warning("SAFE_HOLD auto-resume: etcd mirror not written (%s)", type(exc).__name__)
        return ResumeRecord(token, int(audit[0]), resumed, since, held)


# ============================================================================================ policy
@dataclass(frozen=True)
class SinglePcPage:
    kind: Literal["resumed", "exhausted", "idle", "recording"]
    severity: str
    title: str
    body: str


@dataclass
class SafeHoldAutoResume:
    """Pure decision + the store call; the NodeController calls `step` once per SAFE_HOLD step."""

    settings: SinglePcSettings
    store: AutoResumeStore
    retry_ms: int = RETRY_MS
    _key: tuple[int | None, str | None] | None = field(default=None, init=False)
    _prior: int | None = field(default=None, init=False)
    _cooldown_from: int | None = field(default=None, init=False)
    _next_try_ms: int = field(default=0, init=False)
    _exhausted: bool = field(default=False, init=False)
    _checks_failing: bool = field(default=False, init=False)

    def reset(self) -> None:
        self._key = None
        self._prior = None
        self._cooldown_from = None
        self._next_try_ms = 0
        self._exhausted = False
        self._checks_failing = False

    @property
    def exhausted(self) -> bool:
        return self._exhausted

    def due_at_ms(self) -> int | None:
        """When the running cool-down ends (None: not counting down)."""
        if self._cooldown_from is None or self._prior is None:
            return None
        return self._cooldown_from + self.settings.cooldown_s(self._prior) * 1000

    async def step(self, now_ms: int, own: HealthRecord, cluster: ClusterState) -> SinglePcPage | None:
        s = self.settings
        if not cluster.safe_hold:
            self.reset()
            return None
        key = (cluster.safe_hold_since, cluster.safe_hold_reason)
        if key != self._key:
            self.reset()
            self._key = key
        if self._exhausted or now_ms < self._next_try_ms:
            return None
        if self._prior is None:
            anchor = cluster.safe_hold_since if cluster.safe_hold_since is not None else now_ms
            try:
                self._prior = await self.store.count_auto_resumes(anchor - s.resume_window_s * 1000)
            except Exception as exc:
                log.warning("SAFE_HOLD auto-resume: earlier auto-resumes unreadable (%s); retrying in %d s",
                            type(exc).__name__, self.retry_ms // 1000)
                self._next_try_ms = now_ms + self.retry_ms
                return None
            if self._prior >= s.resume_limit:
                self._exhausted = True
                body = (f"SAFE_HOLD since {_iso(cluster.safe_hold_since)} ({cluster.safe_hold_reason}); "
                        f"{self._prior} auto-resumes within {s.resume_window_s // 3600} h before it (limit "
                        f"{s.resume_limit}): a restart loop needs a human. Staying held: no recording until "
                        "`python -m src.ops safe-hold resume` (two people, docs/runbooks/safe-hold.md)")
                log.critical("%s: %s", EXHAUSTED_TITLE, body)
                return SinglePcPage("exhausted", "critical", EXHAUSTED_TITLE, body)
        failed = own.failed_hard_checks(now_ms, as_leader=False)
        if failed:
            if not self._checks_failing:
                log.warning("SAFE_HOLD auto-resume waits: hard checks failing (%s); the cool-down starts when "
                            "they pass", ", ".join(failed))
            self._checks_failing = True
            self._cooldown_from = None
            return None
        self._checks_failing = False
        cooldown_s = s.cooldown_s(self._prior)
        if self._cooldown_from is None:
            self._cooldown_from = now_ms
            log.warning("SAFE_HOLD set since %s (%s); single PC: auto-resume %d of %d in %d s while the hard "
                        "checks pass", _iso(cluster.safe_hold_since), cluster.safe_hold_reason, self._prior + 1,
                        s.resume_limit, cooldown_s)
            return None
        if now_ms - self._cooldown_from < cooldown_s * 1000:
            return None
        n = self._prior + 1
        reason = (f"single-PC auto-resume {n} of {s.resume_limit} after a {cooldown_s} s cool-down; held: "
                  f"{cluster.safe_hold_reason}")[:900]
        detail = {"auto_resume": n, "limit": s.resume_limit, "window_s": s.resume_window_s,
                  "cooldown_s": cooldown_s, "hard_checks": "passed"}
        try:
            rec = await self.store.resume(reason, detail, cluster.safe_hold_since, cluster.safe_hold_reason)
        except Exception as exc:
            log.error("SAFE_HOLD auto-resume failed (%s); retrying in %d s", type(exc).__name__,
                      self.retry_ms // 1000)
            self._next_try_ms = now_ms + self.retry_ms
            return None
        self.reset()
        if rec is None:
            log.info("SAFE_HOLD auto-resume: nothing resumed (no longer held, a newer hold, or the epoch moved)")
            return None
        body = (f"held since {_iso(rec.held_since)} ({rec.held_reason}); auto-resume {n} of {s.resume_limit} "
                f"within {s.resume_window_s // 3600} h after a {cooldown_s} s cool-down with the hard checks "
                f"passing; operator_audit {rec.audit_id}; the engine campaigns again")
        log.warning("%s: %s", RESUMED_TITLE, body)
        return SinglePcPage("resumed", "info", RESUMED_TITLE, body)


@dataclass
class NotRecordingWatch:
    """Pages once when the node has held no leader term for `after_ms`, and once when it records again."""

    after_ms: int
    _since: int | None = field(default=None, init=False)
    _paged: bool = field(default=False, init=False)

    def observe(self, now_ms: int, role: NodeRole, detail: str = "") -> SinglePcPage | None:
        if role is NodeRole.LEADER:
            paged, since = self._paged, self._since
            self._since, self._paged = None, False
            if paged and since is not None:
                return SinglePcPage("recording", "info", RECORDING_TITLE,
                                    f"LEADER again after {(now_ms - since) // 60_000} min without recording")
            return None
        if self._since is None:
            self._since = now_ms
        if self._paged or now_ms - self._since < self.after_ms:
            return None
        self._paged = True
        body = (f"no leader term for {(now_ms - self._since) // 60_000} min (role {role.value}"
                f"{'; ' + detail if detail else ''}): broker state is not being recorded. "
                "Check deploy/local/status-xtnl.ps1 and the newest var/logs/xtnl-*.log")
        log.critical("%s: %s", IDLE_TITLE, body)
        return SinglePcPage("idle", "critical", IDLE_TITLE, body)


# ============================================================================================ direct channel pager
def _severity(value: str) -> Severity:
    return "info" if value == "info" else "high" if value == "high" else "critical"


class DirectChannelPager:
    """Single PC only (operations.md §2 exception, §17): sends a page straight to the configured Oversee channels,
    without the outbox, the dedupe table or a lease check. The NodeController uses it only for its own critical
    single-PC pages and only while it has no leader session (no outbox exists then). `page` never blocks and never
    raises: the send runs as a task; a failed channel is tried again after 60 s and 300 s; the outcome is logged
    with channel names only. Channel settings are read at each try (`load`), so a fixed credential is picked up."""

    def __init__(self, load: Callable[[], NotificationSettings], client: Callable[[], httpx.AsyncClient], *,
                 clock_ms: Callable[[], int], smtp_factory: SmtpFactory = default_smtp_factory,
                 retry_s: tuple[float, ...] = DIRECT_RETRY_S, send_timeout_s: float = 60.0) -> None:
        self._load = load
        self._client = client
        self._clock_ms = clock_ms
        self._smtp_factory = smtp_factory
        self._retry_s = retry_s
        self._send_timeout_s = send_timeout_s
        self._tasks: set[asyncio.Task[None]] = set()

    def page(self, severity: str, title: str, body: str) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            log.error("direct page %r not sent: no event loop", title)
            return
        task = loop.create_task(self._send(_severity(severity), title, body))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def wait_idle(self) -> None:
        """Wait for every send in flight (tests; `aclose` cancels instead)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def aclose(self) -> None:
        tasks = list(self._tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send(self, severity: Severity, title: str, body: str) -> None:
        row = OutboxRow(kind="info", severity=severity, title=title[:200], body=body[:2000],
                        created_ms=self._clock_ms())
        waits = (0.0, *self._retry_s)
        todo: set[str] | None = None
        failed: list[str] = []
        for attempt, wait_s in enumerate(waits, start=1):
            if wait_s > 0:
                await asyncio.sleep(wait_s)
            try:
                settings = self._load()
            except Exception as exc:
                log.error("direct page %r: channel settings unreadable (%s)", title, safe_error(exc))
                continue
            client = self._client()
            try:
                cs = build_channels(settings, client, smtp_factory=self._smtp_factory)
                route = [c for c in cs.channels if c.name in row.route and (todo is None or c.name in todo)]
                if todo is None:
                    if not route:
                        log.warning("direct page %r not sent: no alert channel configured (%s); logged only",
                                    title, ", ".join(cs.disabled) or "none routed")
                        return
                    todo = {c.name for c in route}
                failed = []
                for ch in route:
                    try:
                        await asyncio.wait_for(ch.send(row), self._send_timeout_s)
                        todo.discard(ch.name)
                    except Exception as exc:
                        failed.append(f"{ch.name} ({safe_error(exc)})")
            finally:
                await client.aclose()
            if not todo:
                log.warning("direct page %r sent (single PC, no leader) on attempt %d", title, attempt)
                return
            log.error("direct page %r: attempt %d of %d failed: %s", title, attempt, len(waits), "; ".join(failed))
        log.error("direct page %r not delivered on %s; it stays in the log only", title,
                  ", ".join(sorted(todo)) if todo else "any channel")
