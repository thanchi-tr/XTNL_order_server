"""ARM notices: routes, parts, item keys, folding and the timer notices (design §B1, §B5.2, §B5.3, §B8, §B14; plan
S28, R11-M14, R11-M19).

**Routes.** Notices group positions by a locally configured account alias (`RouteBook`; an account with no alias is
routed as `adapter_id:account_id`). Each route's `route_policy` (`mode` `every_bar` | `actionable`, `pre_channels`,
`post_channels`) comes from the accepted document (`AcceptedPolicies`, the §B8 defaults while there is none).

**No decision without a notice.** Every notice transaction reads "decisions of this route with `notice_id` IS NULL"
(`ArmStateTxn.unnoticed_decisions`) and either attaches each to the notice it is rendered in or marks it with the
`suppressed` sentinel (`ArmStateTxn.suppress`), or leaves it waiting for its event's end-of-window part 5. It runs in
two places: `NoticeSink` (the second element of `ARM_SINKS`, inside the BarClock's one fenced transaction per (event,
bar end), §B5.3) and `ArmNotices.step` (every 5 s, one transaction per route: the timer notices below and any
leftover; a failing route is counted and skipped, so it never holds back the other routes, the global extras or the
TEST).

**Memory follows the commit.** What a notice transaction changes in memory (part counters, closed windows, the D3
and PRE memory, notice hours, counters, the extras it took off the queue) is staged and applied only once the
transaction committed: the timer pass's own transactions apply it on exit, the BarClock's through
`SinkChain.settle` (`NoticeSink.settle`). A rollback discards the stage and puts the taken extras back, so a failed
commit never skips a part, closes a window early or loses an extra; ALIVE and TEST count as done only after their
commit.

**Keys and parts** (§B8; `arm_notices` is unique on `(route, bar_spec, event, bar_end_ms, part, item_key)`):

* A PRE or POST **bar notice** (`item_key` empty): part 0 goes out with the event's first decisions, or for POST at
  `T + post_grace` if none is decided by then, listing every position still pending ("awaiting the broker's final
  bar", INFO). Each later commit of the event makes the next part, 1 to 4.
* **After part 4** a POST `ADJUST` or `COMPLETE` is sent at once as its own item-keyed notice (event POST, part 0,
  `adapter_id:position_id:resource:owner:revision`), so advice is never held back by the part cap (R11-M19); every
  other decision waits for **part 5**, sent only if it has items, at the end of the event's window: PRE at its
  valid-until, POST at `decision_cap`. A part-5 item is never superseded before it has been sent once (`supersedes`).
  A POST decision committed after its part 5 goes out only as a LATE item (an item notice whose item carries the
  `late` label); a PRE item whose window ended unsent is suppressed and counted (PRE expiry is a metric, not a
  page). A part that already exists in `arm_notices` (after a takeover) is a key conflict and the next part is tried.
* **LATE, CORRECTED and REOPEN items** are always sent at once as their own part-0 notices (key
  `...:revision`, REOPEN `...:<decided_at_ms>`), whatever Q9 says.
* **Extras** (Q9; tracker notices `POSITION_NOT_MANAGED`, `SUSPENDED`, `REPLACED`, `REPINNED`, `ROLL_ADJUST`,
  `ENTRY_CORRECTED`): with `fold = inside` (default) they ride on the route's next bar notice, or go out as their own
  notice (key `adapter_id:position_id:reason`) one bar after they were queued; `separate` sends them at once; `off`
  drops them (counted). They fold into POST bar notices only, never into a PRE one (a PRE notice can expire or be
  superseded by its POST before it is sent). `MD_RESET` (key: the md generation) and `FAILED_CLEARED` (key: the failure id) are always
  their own notices on the global route `*`. A `POSITION_NOT_MANAGED` notice is keyed at bar end 0, so it is one-shot
  for good. Extras are kept in memory only (a takeover may drop one; the tracker's own memory is not durable either).
* **Timer notices:** the hourly `ALIVE` line per route with managed positions (key: the hour; in `every_bar` mode
  only after an hour with no other notice of the route), carrying the counts of managed and FAULT positions and the
  open FAILED scopes; the daily `TEST` notice per channel on route `*` (key: the channel), and the operator's
  `arm notify-test` TEST per channel (`request_test`, keyed by the request time: plan S36b).

**What is shown** (§B1, §B6.3, §B8, D3): a decision the evaluation marked not shown (`Decided.shown`: a released
position, a repeated "at target, not yet confirmed") is suppressed, and so is a repeated `AT_TARGET_UNCONFIRMED` this
task has seen on the position's previous bar (a `NO_BROKER_BAR` between them does not count); `actionable` mode shows a PRE `ADJUST` only, and at bar-end events an
`ADJUST`, a HOLD, any FAULT, `COMPLETE`, `PRE_NOT_CONFIRMED`, a change from this bar's PRE, a correction or a folded
item. A notice is created only when it has an item to show: every position released gives no bar notice.

**The outbox body holds only ids** (R11-M14): the notice id and key, decision ids, outcome codes and order-state
fields (`src.riskmgr.render.NoticeBody`); `NoticeOutboxPort` (S29's ARM outbox store) inserts it in the same
transaction, and the arm-relay renders favour figures at send time from hash-checked bars. Expiry (`expires_at_ms`):
PRE at local `T + p50`; POST parts and LATE, CORRECTED and REOPEN items one bar after they were created (and
superseded once the route's next POST part 0 exists); `ALIVE` after an hour; the others after a day. No escalation.

A decision whose `notice_id` stays NULL for more than 60 s (one waiting for part 5: 60 s after its window end) while
its route's channels are configured pages once per route episode; after a takeover, a decision whose key's parts
0-4 turn out to exist (key conflicts) counts as a part-5 waiter, so a takeover does not page. Logs and pages carry ids, codes and counts only.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Final, Protocol

from src.armconfig.resolver import route_policy_at
from src.armconfig.validate import RoutePolicy
from src.db.arm_ports import (
    ArmPositionRow,
    ArmStateStore,
    ArmStateTxn,
    DecisionRef,
    DecisionRow,
    NoticeRow,
    PositionKey,
    StoredDecision,
    is_lost_fence,
    position_ref,
)
from src.marketdata.ports import StampPort
from src.marketdata.timing import (
    SkewEstimate,
    decision_cap_ms,
    local_bar_end_ms,
    post_grace_end_ms,
    pre_fire_ms,
    pre_valid_until_ms,
    timing_for,
)
from src.marketdata.types import M15, BarSpec
from src.riskmgr.config import ArmSettings
from src.riskmgr.outcomes import Category, Outcome
from src.riskmgr.ports import Clock, Pager
from src.riskmgr.registry import REGISTRY, ArmRegistry, ParamError
from src.riskmgr.render import (
    AliveCounts,
    ExtraBody,
    FailedLine,
    ItemBody,
    NoticeBody,
    PendingBody,
    encode_body,
    subject,
    trigger_input,
)
from src.riskmgr.sinks import NOTICE_SINK, Written
from src.riskmgr.sm11 import MANAGED, ArmState
from src.riskmgr.strategy import SlStrategy, StrategyParams
from src.riskmgr.tracker import AcceptedConfigPort, BarGridPort, NoticeKind, TrackerNotice
from src.riskmgr.types import ArmEvent

log = logging.getLogger(__name__)

TASK: Final = "arm-notices"
PRE: Final = ArmEvent.PRE.value
POST: Final = ArmEvent.POST.value
BAR_EVENTS: Final = frozenset({PRE, POST})
ITEM_EVENTS: Final = frozenset({ArmEvent.LATE.value, ArmEvent.CORRECTED.value, ArmEvent.REOPEN.value})
LAST_PART: Final = 4
END_PART: Final = 5
IMMEDIATE_OUTCOMES: Final = frozenset({Outcome.ADJUST.value, Outcome.COMPLETE.value})
"""POST outcomes sent at once as item notices after part 4 (R11-M19)."""
ALIVE: Final = "ALIVE"
TEST: Final = "TEST"
MD_RESET: Final = "MD_RESET"
FAILED_CLEARED: Final = "FAILED_CLEARED"
GLOBAL_ROUTE: Final = "*"
FOLDS: Final = ("inside", "separate", "off")
"""Q9: extras inside the per-bar notice (default), separate, or off."""
AT_TARGET: Final = Outcome.AT_TARGET_UNCONFIRMED.value
NO_BROKER_BAR: Final = Outcome.NO_BROKER_BAR.value
MAX_ROUNDS: Final = 8


# ---------------------------------------------------------------------------------------------------- pure rules
def item_key_of(d: DecisionRow) -> str:
    """`adapter_id:position_id:resource:owner:revision` (LATE, CORRECTED, POST after part 4); REOPEN items end in
    the decision time instead (`...:<decided_at_ms>`, the re-check's reopen time)."""
    base = f"{position_ref(d.key.adapter_id, d.key.position_id)}:{d.key.resource}:{d.key.owner}"
    return f"{base}:{d.decided_at_ms}" if d.event == ArmEvent.REOPEN.value else f"{base}:{d.revision}"


def shown_by_mode(d: DecisionRow, mode: str, *, pre_outcome: str | None = None) -> bool:
    """The route policy's `mode` rule (§B8): `every_bar` shows every item; `actionable` shows a PRE `ADJUST` and, at
    bar-end events, an `ADJUST`, a HOLD, any FAULT, `COMPLETE`, `PRE_NOT_CONFIRMED`, a change from PRE or a
    correction."""
    if mode != "actionable":
        return True
    if d.event == PRE:
        return d.outcome == Outcome.ADJUST.value
    if d.outcome in (Outcome.ADJUST.value, Outcome.COMPLETE.value, Outcome.PRE_NOT_CONFIRMED.value):
        return True
    if d.category in (Category.HOLD.value, Category.FAULT.value):
        return True
    if d.event == ArmEvent.CORRECTED.value and d.supersedes_decision_id is not None:
        return True
    return pre_outcome is not None and pre_outcome != d.outcome


@dataclass(frozen=True, kw_only=True)
class NoticeMeta:
    """A notice's key fields, as the relay decodes them from the body (for `supersedes`)."""

    route: str
    bar_spec: str
    event: str
    bar_end_ms: int
    part: int = 0
    item_key: str = ""


def supersedes(new: NoticeMeta, old: NoticeMeta, *, old_sent: bool) -> bool:
    """§B8: a PRE notice is superseded once the POST of its bar exists; POST parts and LATE, CORRECTED and REOPEN
    items once the route's next POST part 0 exists; a part-5 notice never before it has been sent once; nothing
    else is ever superseded. Used by the arm-relay (S29) on unsent rows."""
    if new.route != old.route or new.event != POST or new.bar_spec != old.bar_spec:
        return False
    if old.part == END_PART and not old_sent:
        return False
    if old.event == PRE:
        return new.bar_end_ms == old.bar_end_ms or (new.part == 0 and not new.item_key
                                                    and new.bar_end_ms > old.bar_end_ms)
    if old.event == POST or old.event in ITEM_EVENTS:
        return new.part == 0 and not new.item_key and new.bar_end_ms > old.bar_end_ms
    return False


def expires_at_ms(event: str, *, bar_end_ms: int, created_ms: int, spec: BarSpec, skew: SkewEstimate,
                  alive_ms: int = 3_600_000, other_ms: int = 86_400_000) -> int:
    """PRE at local `T + p50`; POST parts and LATE, CORRECTED, REOPEN items one bar after creation; ALIVE after
    `alive_ms`; any other notice after `other_ms` (§B8)."""
    if event == PRE:
        return local_bar_end_ms(bar_end_ms, skew)
    if event == POST or event in ITEM_EVENTS:
        return created_ms + spec.timeframe_ms
    return created_ms + (alive_ms if event == ALIVE else other_ms)


# ---------------------------------------------------------------------------------------------------- ports, routes
@dataclass(frozen=True, kw_only=True)
class OutboxNotice:
    """One kind `arm` outbox row: an ids-only body (render.encode_body) on the route's channels."""

    notice_id: int
    route: str
    channels: tuple[str, ...]
    subject: str
    body: str = field(repr=False)
    created_ms: int
    expires_at_ms: int
    meta: NoticeMeta


class NoticeOutboxPort(Protocol):
    async def enqueue(self, tx: ArmStateTxn, notice: OutboxNotice) -> None:
        """Insert the kind `arm` outbox row in the notice's own transaction (S29's ARM outbox store; `must=True`)."""
        ...


class RoutePolicyPort(Protocol):
    def policy(self, route: str, at_ms: int) -> RoutePolicy: ...


class AcceptedPolicies:
    """Route policies from the accepted document (`route_policy_at`), the §B8 defaults while there is none."""

    __slots__ = ("_config",)

    def __init__(self, config: AcceptedConfigPort) -> None:
        self._config = config

    def policy(self, route: str, at_ms: int) -> RoutePolicy:
        doc = self._config.accepted()
        return RoutePolicy(route=route) if doc is None else route_policy_at(doc, route, at_ms)


class FailedLinesPort(Protocol):
    def open_scopes(self) -> Sequence[FailedLine]:
        """The open FAILED scopes, as status lines (S30)."""
        ...


@dataclass(frozen=True)
class RouteBook:
    """Local account aliases: `(adapter_id, account_id)` → route."""

    aliases: Mapping[tuple[str, str], str] = field(default_factory=dict)

    def route_of(self, adapter_id: str, account_id: str) -> str:
        return self.aliases.get((adapter_id, account_id), f"{adapter_id}:{account_id}")


@dataclass(frozen=True, kw_only=True)
class NoticeSettings:
    fold: str = "inside"
    sweep_every_ms: int = 5_000
    unnoticed_page_after_ms: int = 60_000
    lookback_ms: int = 86_400_000
    page_limit: int = 500
    alive_every_ms: int = 3_600_000
    other_ttl_ms: int = 86_400_000
    test_at_ms_of_day: int = 22 * 3_600_000
    """UTC time of day of the daily TEST notice (08:00 or 09:00 Melbourne)."""
    test_channels: tuple[str, ...] = ("arm_email", "arm_push")
    global_channels: tuple[str, ...] = ("arm_email",)
    keep_ms: int = 2 * 86_400_000

    def __post_init__(self) -> None:
        if self.fold not in FOLDS:
            raise ValueError(f"fold must be one of {FOLDS}")


@dataclass
class NoticeCounters:
    notices: int = 0
    items: int = 0
    suppressed: int = 0
    pre_expired: int = 0
    duplicate_items: int = 0
    extras_dropped: int = 0
    passes: int = 0
    errors: int = 0
    pages: int = 0

    def add(self, other: NoticeCounters) -> None:
        """Add a committed transaction's staged counts."""
        self.notices += other.notices
        self.items += other.items
        self.suppressed += other.suppressed
        self.pre_expired += other.pre_expired
        self.duplicate_items += other.duplicate_items
        self.extras_dropped += other.extras_dropped
        self.passes += other.passes
        self.errors += other.errors
        self.pages += other.pages


@dataclass(frozen=True)
class _Info:
    account_id: str
    symbol: str
    side: str
    entry: Decimal | None = field(default=None, repr=False)
    s0: Decimal | None = field(default=None, repr=False)
    loosen_policy: str | None = None
    """The pinned Q10 policy (`REARM` | `TELL_ONLY`) the position's strategy returns for its pinned params."""


def _pinned(registry: ArmRegistry, row: ArmPositionRow) -> tuple[SlStrategy, StrategyParams] | None:
    """The position's pinned strategy and params, or None when the pin cannot be loaded here."""
    strategy = registry.strategy(row.strategy_kind, row.strategy_impl_version)
    if strategy is None:
        return None
    try:
        raw = json.loads(row.strategy_params_json)
        guards = json.loads(row.guards_json)
        if not isinstance(raw, dict):
            return None
        params = registry.parse_params(row.strategy_kind, row.strategy_impl_version, {**raw, "guards": guards},
                                       new_pin=False)
    except (ValueError, TypeError, ParamError):
        return None
    except Exception:  # a strategy's own parser: never let it break a notice
        log.warning("arm notices: the pin of %s did not load", position_ref(row.key.adapter_id, row.key.position_id))
        return None
    return strategy, params


def pinned_loosen_policy(registry: ArmRegistry, row: ArmPositionRow) -> str | None:
    """The position's **pinned** Q10 policy (`SlStrategy.loosen_policy` of its stored pin, round 13): a strategy
    parameter pinned per position, never a node-global setting. None when the pin cannot be loaded here (the
    release text then says "told only", the safe statement)."""
    pin = _pinned(registry, row)
    if pin is None:
        return None
    try:
        return pin[0].loosen_policy(pin[1]).value
    except Exception:  # a strategy's own code: never let it break a notice
        log.warning("arm notices: loosen_policy raised for %s", position_ref(row.key.adapter_id,
                                                                            row.key.position_id))
        return None


@dataclass(frozen=True)
class _Fresh:
    shown: bool
    handles: tuple[str, ...]
    read_at_ms: int | None = None
    """The latest read time of the handles to move (order-state time)."""
    level: Decimal | None = field(default=None, repr=False)
    """The order-state stop level the handles to move held when read (`held_stop_level`); None when unknown."""


def held_stop_level(levels: Sequence[Decimal | None], side: str) -> Decimal | None:
    """The stop level the handles to move held when read: their one level, or, when they differ, the most distant
    from price on the loss side (lowest for a long, highest for a short, as `effective_stop_detail` counts children);
    None when no level was read or the side is unknown with differing levels."""
    known = sorted({lv for lv in levels if lv is not None})
    if not known:
        return None
    if len(known) == 1:
        return known[0]
    if side == "BUY":
        return known[0]
    if side == "SELL":
        return known[-1]
    return None


@dataclass
class _Extra:
    route: str
    body: ExtraBody
    event: str
    item_key: str
    bar_end_ms: int
    due_ms: int
    fold: bool
    generation: str | None = None
    member: tuple[str, str] | None = None
    """The (adapter_id, account_id) the extra is about (kept in the route's members while it is queued)."""


@dataclass
class _Group:
    decisions: list[StoredDecision] = field(default_factory=list)
    pending: list[PendingBody] = field(default_factory=list)


_Key = tuple[str, str, str, int]
"""(route, bar_spec, event, bar_end_ms)."""


@dataclass
class _Stage:
    """What one notice transaction changes in memory. It is applied only once the transaction committed
    (`ArmNotices._apply`); a rollback discards it and puts the extras it took back on the queue (`_discard`), so a
    failed commit never skips a part, closes a window or loses an extra."""

    parts: dict[_Key, int] = field(default_factory=dict)
    closed: dict[_Key, int] = field(default_factory=dict)
    taken: list[_Extra] = field(default_factory=list)
    hours: set[tuple[str, int]] = field(default_factory=set)
    last: dict[str, tuple[int, str]] = field(default_factory=dict)
    faults: dict[str, bool] = field(default_factory=dict)
    pre_outcome: dict[tuple[str, int], str] = field(default_factory=dict)
    counts: NoticeCounters = field(default_factory=NoticeCounters)


# ---------------------------------------------------------------------------------------------------- the notices
class ArmNotices:
    """Notice building for one term (see the module docstring). `sink()` is the `ARM_SINKS` notice sink; `step` the
    5 s timer pass; `notice` / `md_reset` / `failed_cleared` queue extras; `post_sent` is the BarClock's
    `PostNoticeView`."""

    def __init__(self, *, store: ArmStateStore, outbox: NoticeOutboxPort, routes: RouteBook,
                 policies: RoutePolicyPort, grid: BarGridPort, pager: Pager, clock: Clock, settings: ArmSettings,
                 notices: NoticeSettings | None = None, failed: FailedLinesPort | None = None,
                 unmanaged: Callable[[], int] | None = None,
                 channels_configured: Callable[[tuple[str, ...]], bool] | None = None,
                 fatal: Callable[[BaseException], bool] | None = None, registry: ArmRegistry = REGISTRY) -> None:
        self._store = store
        self._registry = registry
        self._outbox = outbox
        self._routes = routes
        self._policies = policies
        self._grid = grid
        self._pager = pager
        self._clock = clock
        self._arm = settings
        self._cfg = notices or NoticeSettings()
        self._failed = failed
        self._unmanaged = unmanaged or (lambda: 0)
        self._configured = channels_configured or (lambda channels: True)
        self._fatal = fatal or is_lost_fence
        self.counters = NoticeCounters()
        self._info: dict[PositionKey, _Info] = {}
        self._members: dict[str, set[tuple[str, str]]] = {}
        self._parts: dict[_Key, int] = {}
        self._closed: dict[_Key, int] = {}
        self._last: dict[str, tuple[int, str]] = {}
        """Per position ref: (bar end, outcome) of the newest bar-end decision seen (D3)."""
        self._pre_outcome: dict[tuple[str, int], str] = {}
        self._faulting: set[str] = set()
        self._extras: list[_Extra] = []
        self._notice_hours: dict[str, set[int]] = {}
        """Per route, the hours (start ms) in which a notice was created (the every_bar ALIVE rule)."""
        self._alive_done: dict[str, int] = {}
        self._test_done: int | None = None
        self._tests_asked: list[int] = []
        """Operator test requests (`arm notify-test`: `arm_controls.notify_test_at_ms`) not yet committed."""
        self._paged: set[str] = set()
        self._rows: list[ArmPositionRow] = []
        self._staged: dict[int, tuple[ArmStateTxn, _Stage]] = {}
        """The stages of the BarClock transactions the notice sink worked in, until `settle` (keyed by `id(tx)`)."""

    # ------------------------------------------------------------------------------------------------ public
    def sink(self) -> NoticeSink:
        return NoticeSink(self)

    def notice(self, item: TrackerNotice) -> None:
        """`TrackerNoticePort`: queue a tracker notice as a Q9 extra."""
        if self._cfg.fold == "off":
            self.counters.extras_dropped += 1
            return
        route = self._routes.route_of(item.adapter_id, item.account_id)
        member = (item.adapter_id, item.account_id)
        self._members.setdefault(route, set()).add(member)
        ref = position_ref(item.adapter_id, item.position_id)
        fold = self._cfg.fold == "inside"
        one_shot = item.kind is NoticeKind.POSITION_NOT_MANAGED
        self._extras.append(_Extra(
            route=route, body=ExtraBody(kind=item.kind.value, adapter_id=item.adapter_id,
                                        position_id=item.position_id, symbol=item.symbol, side=item.side,
                                        reason=item.reason, at_ms=item.at_ms),
            event=item.kind.value, item_key=f"{ref}:{item.reason}", bar_end_ms=0 if one_shot else item.at_ms,
            due_ms=item.at_ms + (M15.timeframe_ms if fold else 0), fold=fold, member=member))

    def md_reset(self, generation: str, at_ms: int) -> None:
        """Queue the one INFO notice of a `redis-md` reset (§A13): keyed by the md generation."""
        self._extras.append(_Extra(route=GLOBAL_ROUTE, body=ExtraBody(kind=MD_RESET, at_ms=at_ms), event=MD_RESET,
                                   item_key=generation, bar_end_ms=0, due_ms=at_ms, fold=False,
                                   generation=generation))

    def failed_cleared(self, failure_id: str, scope_kind: str, scope_id: str, at_ms: int) -> None:
        """Queue the FAILED_CLEARED status notice (§B14): keyed by the failure id."""
        self._extras.append(_Extra(route=GLOBAL_ROUTE, body=ExtraBody(kind=FAILED_CLEARED, position_id="",
                                                                      reason=f"{scope_kind}:{scope_id}", at_ms=at_ms),
                                   event=FAILED_CLEARED, item_key=failure_id, bar_end_ms=0, due_ms=at_ms,
                                   fold=False))

    def request_test(self, at_ms: int) -> None:
        """`arm notify-test` (§B13, S34): one TEST notice per channel on route `*`, keyed by the request time (so a
        request seen again, here or by another term, is a key conflict and nothing goes out twice)."""
        if at_ms not in self._tests_asked:
            self._tests_asked.append(at_ms)

    def post_sent(self, row: ArmPositionRow, bar_spec: str, bar_end_ms: int) -> bool:
        """`PostNoticeView`: the route's POST notice window for this bar has ended (part 5 done, or `decision_cap`
        passed), so a decision now is `LATE`."""
        route = self._routes.route_of(row.key.adapter_id, row.account_id)
        if (route, bar_spec, POST, bar_end_ms) in self._closed:
            return True
        spec = BarSpec.parse(bar_spec)
        return self._clock.now_ms() >= decision_cap_ms(bar_end_ms, spec, self._grid.skew(row.key.adapter_id))

    def learn(self, row: ArmPositionRow) -> None:
        """Remember a position's route, symbol, side and pinned levels (the decision rows carry none of them)."""
        self._info[row.key] = _Info(row.account_id, row.symbol, row.side, row.entry_level, row.s0_level,
                                    pinned_loosen_policy(self._registry, row))
        route = self._routes.route_of(row.key.adapter_id, row.account_id)
        self._members.setdefault(route, set()).add((row.key.adapter_id, row.account_id))

    def faulting(self) -> frozenset[str]:
        return frozenset(self._faulting)

    async def on_commit(self, tx: ArmStateTxn, event: ArmEvent, bar_end_ms: int,
                        decisions: Sequence[Written]) -> None:
        """The notice work of one (event, bar end) commit, in its transaction: for every route with a newly
        inserted decision, the unnoticed decisions of the route are attached, suppressed or left for part 5. The
        memory changes stay staged until the transaction's owner calls `settle`."""
        now = self._clock.now_ms()
        st = self._sink_stage(tx)
        fresh: dict[tuple[int, int], _Fresh] = {}
        groups: set[tuple[str, str]] = set()
        for w in decisions:
            d = w.item.decision
            if d is None or w.ref is None:
                continue
            if w.item.position is not None:
                self.learn(w.item.position.row)
            info = self._info.get(d.key)
            if info is None:
                continue                  # its route is unknown here: the timer pass notices it
            moved = [h for h in w.item.handles if h.target_level is not None]
            reads = [h.read_at_ms for h in moved if h.read_at_ms is not None]
            fresh[(w.ref.decision_id, w.ref.bar_end_ms)] = _Fresh(
                shown=w.item.shown, handles=tuple(h.handle_id for h in moved), read_at_ms=max(reads, default=None),
                level=held_stop_level([h.level_read for h in moved], info.side))
            groups.add((self._routes.route_of(d.key.adapter_id, info.account_id), d.bar_spec))
        for route, spec in sorted(groups):
            await self._flush(tx, st, route, spec, now, fresh, commit=(event.value, bar_end_ms), grace=None)

    def settle(self, tx: ArmStateTxn, *, committed: bool) -> None:
        """The BarClock's transaction ended (`SinkChain.settle`): apply the notice sink's stage, or discard it."""
        entry = self._staged.pop(id(tx), None)
        if entry is None:
            return
        if committed:
            self._apply(entry[1])
        else:
            self._discard(entry[1])

    async def run(self, stop: asyncio.Event, stamp_port: StampPort) -> None:
        while not stop.is_set():
            stamp_port.stamp()
            try:
                await self.step()
            except Exception as e:
                if self._fatal(e):
                    raise
                self.counters.errors += 1
                log.warning("%s: pass failed (%r)", TASK, type(e))
            stamp_port.stamp()
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._cfg.sweep_every_ms / 1000)
            except TimeoutError:
                pass

    async def step(self) -> None:
        """One timer pass: live rows, then per route the leftovers, POST part 0 at grace, part 5 at the window end,
        due extras and ALIVE; then the global extras and the daily TEST. A failing route is counted and skipped, so
        it never holds back the other routes, the global extras or the TEST."""
        now = self._clock.now_ms()
        self.counters.passes += 1
        self._rows = list(await self._store.live_positions())
        for row in self._rows:
            self.learn(row)
        routes = sorted(set(self._members) | {e.route for e in self._extras if e.route != GLOBAL_ROUTE})
        for route in routes:
            await self._guarded(self._route_pass(route, now), route)
        await self._guarded(self._due_extras(GLOBAL_ROUTE, now), GLOBAL_ROUTE)
        await self._guarded(self._test(now), TEST)
        await self._guarded(self._asked_tests(now), TEST)
        self._prune(now)

    # ------------------------------------------------------------------------------------------------ the core
    async def _guarded(self, work: Awaitable[None], what: str) -> None:
        try:
            await work
        except Exception as e:
            if self._fatal(e):
                raise
            self.counters.errors += 1
            log.warning("%s: %s failed (%r)", TASK, what, type(e))

    async def _route_pass(self, route: str, now: int) -> None:
        specs = sorted({r.bar_spec for r in self._rows if self._route_of(r) == route} | {M15.name})
        for spec in specs:
            async with self._tx() as (tx, st):
                await self._flush(tx, st, route, spec, now, {}, commit=None, grace=self._grace(route, spec, now))
        await self._due_extras(route, now)
        await self._alive(route, now)

    @asynccontextmanager
    async def _tx(self) -> AsyncIterator[tuple[ArmStateTxn, _Stage]]:
        """One notice transaction of this task with its stage: applied after the commit, discarded on any error."""
        st = _Stage()
        try:
            async with self._store.transaction() as tx:
                yield tx, st
        except BaseException:
            self._discard(st)
            raise
        self._apply(st)

    def _sink_stage(self, tx: ArmStateTxn) -> _Stage:
        entry = self._staged.get(id(tx))
        if entry is None or entry[0] is not tx:
            entry = (tx, _Stage())
            self._staged[id(tx)] = entry
        return entry[1]

    def _apply(self, st: _Stage) -> None:
        for key, part in st.parts.items():
            self._parts[key] = max(part, self._parts.get(key, 0))
        for key, at in st.closed.items():
            self._closed.setdefault(key, at)
        for route, hour in st.hours:
            self._notice_hours.setdefault(route, set()).add(hour)
        for ref, last in st.last.items():
            cur = self._last.get(ref)
            if cur is not None and cur[0] > last[0]:
                continue                  # a newer bar committed meanwhile
            self._last[ref] = last
            if st.faults.get(ref, False):
                self._faulting.add(ref)
            else:
                self._faulting.discard(ref)
        self._pre_outcome.update(st.pre_outcome)
        self.counters.add(st.counts)

    def _discard(self, st: _Stage) -> None:
        self._extras[:0] = [e for e in st.taken if e not in self._extras]
        st.taken.clear()

    def _part(self, st: _Stage, key: _Key) -> int:
        return st.parts.get(key, self._parts.get(key, 0))

    def _known(self, st: _Stage, key: _Key) -> bool:
        return key in st.parts or key in self._parts

    def _is_closed(self, st: _Stage, key: _Key) -> bool:
        return key in st.closed or key in self._closed

    def _route_of(self, row: ArmPositionRow) -> str:
        return self._routes.route_of(row.key.adapter_id, row.account_id)

    def _skew(self, adapter_id: str) -> SkewEstimate:
        return self._grid.skew(adapter_id)

    def _window_end(self, d: DecisionRow) -> int:
        spec = BarSpec.parse(d.bar_spec)
        skew = self._skew(d.key.adapter_id)
        if d.event == PRE:
            row = timing_for(spec)
            if row is None:
                return local_bar_end_ms(d.bar_end_ms, skew)
            return pre_valid_until_ms(pre_fire_ms(d.bar_end_ms, skew, self._arm.pre_offset_s), d.bar_end_ms, row, skew)
        return decision_cap_ms(d.bar_end_ms, spec, skew)

    def _shown(self, st: _Stage, d: DecisionRow, mode: str, fresh: _Fresh | None) -> bool:
        ref = position_ref(d.key.adapter_id, d.key.position_id)
        if d.event == PRE:
            st.pre_outcome[(ref, d.bar_end_ms)] = d.outcome
            shown = fresh is None or fresh.shown
            return shown and shown_by_mode(d, mode)
        prev = st.last[ref] if ref in st.last else self._last.get(ref)
        repeated = d.outcome == AT_TARGET and prev is not None and prev[1] == AT_TARGET and prev[0] < d.bar_end_ms
        if d.outcome != NO_BROKER_BAR and (prev is None or prev[0] <= d.bar_end_ms):
            # a bucket the broker did not trade changes no state: it never breaks an at-target run (S38c review)
            st.last[ref] = (d.bar_end_ms, d.outcome)
            st.faults[ref] = d.category == Category.FAULT.value
        if (fresh is not None and not fresh.shown) or repeated:
            return False
        pk = (ref, d.bar_end_ms)
        pre = st.pre_outcome[pk] if pk in st.pre_outcome else self._pre_outcome.get(pk)
        return shown_by_mode(d, mode, pre_outcome=pre)

    async def _read(self, tx: ArmStateTxn, route: str, spec: str, now: int) -> tuple[list[StoredDecision], bool]:
        members = sorted(self._members.get(route, set()))
        if not members:
            return [], False
        page = await tx.unnoticed_decisions(members, spec, since_bar_end_ms=now - self._cfg.lookback_ms,
                                            limit=self._cfg.page_limit)
        return list(page.decisions), page.truncated

    async def _flush(self, tx: ArmStateTxn, st: _Stage, route: str, spec: str, now: int,
                     fresh: Mapping[tuple[int, int], _Fresh], *, commit: tuple[str, int] | None,
                     grace: tuple[int, list[PendingBody]] | None) -> None:
        for round_ in range(MAX_ROUNDS):
            decisions, truncated = await self._read(tx, route, spec, now)
            changed = await self._act(tx, st, route, spec, now, decisions, fresh,
                                      commit=commit if round_ == 0 else None, grace=grace if round_ == 0 else None)
            if not truncated or changed == 0:
                return

    async def _act(self, tx: ArmStateTxn, st: _Stage, route: str, spec: str, now: int,
                   decisions: Sequence[StoredDecision], fresh: Mapping[tuple[int, int], _Fresh], *,
                   commit: tuple[str, int] | None, grace: tuple[int, list[PendingBody]] | None) -> int:
        policy = self._policies.policy(route, now)
        suppress: list[DecisionRef] = []
        bars: dict[_Key, _Group] = {}
        ends: dict[_Key, _Group] = {}
        # (decision, late): LATE / CORRECTED / REOPEN, a POST ADJUST / COMPLETE after part 4, a POST after part 5.
        items: list[tuple[StoredDecision, bool]] = []
        # (decision, window end, waiting for part 5), for the 60 s page.
        watch: list[tuple[StoredDecision, int, bool]] = []
        for sd in sorted(decisions, key=lambda s: (s.row.bar_end_ms, s.row.decision_seq, s.ref.decision_id)):
            d = sd.row
            key: _Key = (route, spec, d.event, d.bar_end_ms)
            fr = fresh.get((sd.ref.decision_id, sd.ref.bar_end_ms))
            end = self._window_end(d) if d.event in BAR_EVENTS else d.decided_at_ms
            # A part-5 waiter: its key's parts 0-4 are used, or (after a takeover, with no part count in memory)
            # its window has ended and part 5 goes out now.
            waiter = d.event in BAR_EVENTS and (self._part(st, key) > LAST_PART
                                                or (now >= end and not self._known(st, key)))
            watch.append((sd, end, waiter))
            if not self._shown(st, d, policy.mode, fr):
                suppress.append(sd.ref)
                continue
            if d.event not in BAR_EVENTS:
                items.append((sd, False))
                continue
            if self._is_closed(st, key):
                if d.event == PRE:
                    st.counts.pre_expired += 1
                    suppress.append(sd.ref)
                else:
                    items.append((sd, True))          # after part 5: only as a LATE item (§B8)
            elif now >= end:
                ends.setdefault(key, _Group()).decisions.append(sd)
            elif self._part(st, key) <= LAST_PART:
                bars.setdefault(key, _Group()).decisions.append(sd)
            elif d.event == POST and d.outcome in IMMEDIATE_OUTCOMES:
                items.append((sd, False))
        if grace is not None:
            gkey: _Key = (route, spec, POST, grace[0])
            if self._part(st, gkey) == 0 and not self._is_closed(st, gkey) and gkey not in ends:
                bars.setdefault(gkey, _Group()).pending.extend(grace[1])
        extras = [e for e in self._extras if e.route == route and e.fold]
        if commit is not None and commit[0] == POST and extras and not any(k[2] == POST for k in bars):
            ckey: _Key = (route, spec, POST, commit[1])
            if not self._is_closed(st, ckey) and self._part(st, ckey) <= LAST_PART:
                bars[ckey] = _Group()
        changed = 0
        if suppress:
            changed += await tx.suppress(suppress)
            st.counts.suppressed += len(suppress)
        waiting: set[tuple[int, int]] = set()
        for key, group in sorted(bars.items(), key=lambda kv: kv[0][3]):
            # Extras ride on POST bar notices only: a PRE notice can expire or be superseded before it is sent.
            folded = extras if key[2] == POST else []
            created, left = await self._bar_notice(tx, st, key, group, policy, now, folded, fresh)
            changed += len(group.decisions) - len(left)
            if created and folded:
                extras = []
            for sd in left:
                if sd.row.event == POST and sd.row.outcome in IMMEDIATE_OUTCOMES:
                    items.append((sd, False))
                else:
                    waiting.add((sd.ref.decision_id, sd.ref.bar_end_ms))
        for key, group in sorted(ends.items(), key=lambda kv: kv[0][3]):
            changed += await self._end_part(tx, st, key, group, policy, now, fresh)
        for sd, late in items:
            changed += await self._item_notice(tx, st, route, spec, sd, policy, now, fresh, late=late)
        if self._overdue(watch, waiting, now):
            self._page_overdue(route, policy)
        elif route in self._paged:
            self._paged.discard(route)
        return changed

    def _overdue(self, watch: Sequence[tuple[StoredDecision, int, bool]], waiting: set[tuple[int, int]],
                 now: int) -> bool:
        """Still NULL more than 60 s after it was committed (a part-5 waiter, including one found waiting in this
        pass: 60 s after its window end): the earlier notice passes failed, so page, whatever this pass manages
        (§B8)."""
        wait = self._cfg.unnoticed_page_after_ms
        for sd, end, waiter in watch:
            d = sd.row
            if waiter or (sd.ref.decision_id, sd.ref.bar_end_ms) in waiting:
                due = max(end, d.decided_at_ms)
            else:
                due = d.decided_at_ms
            if now > due + wait:
                return True
        return False

    async def _bar_notice(self, tx: ArmStateTxn, st: _Stage, key: _Key, group: _Group, policy: RoutePolicy,
                          now: int, extras: list[_Extra], fresh: Mapping[tuple[int, int], _Fresh],
                          ) -> tuple[bool, list[StoredDecision]]:
        """The next part (0-4) of a bar notice: (created, the decisions left without a notice, parts exhausted). A
        part that already exists in `arm_notices` (a takeover, a concurrent transaction) is a key conflict: the next
        part is tried, so the next part is always the store's next free one."""
        route, spec, event, bar_end = key
        if not group.decisions and not group.pending and not extras:
            return False, []
        start = self._part(st, key)
        last = 0 if group.pending and not group.decisions else LAST_PART
        for part in range(start, last + 1):
            nid = await tx.insert_notice(NoticeRow(route=route, bar_spec=spec, event=event, bar_end_ms=bar_end,
                                                   part=part, decision_count=len(group.decisions),
                                                   created_at_ms=now))
            if nid is None:
                continue
            st.parts[key] = part + 1
            taken = [e for e in extras if e in self._extras]     # the timer pass may have sent one meanwhile
            for e in taken:
                self._extras.remove(e)
            st.taken.extend(taken)                               # back on the queue if the transaction rolls back
            await self._finish(tx, st, nid, route, spec, event, bar_end, part, "", group.decisions, policy, now,
                               fresh, pending=tuple(group.pending), extras=tuple(e.body for e in taken))
            return True, []
        st.parts[key] = last + 1 if last == LAST_PART else max(start, 1)
        return False, list(group.decisions)

    async def _end_part(self, tx: ArmStateTxn, st: _Stage, key: _Key, group: _Group, policy: RoutePolicy, now: int,
                        fresh: Mapping[tuple[int, int], _Fresh]) -> int:
        """Part 5 at the end of the event's window, only with items; after it the window is closed."""
        route, spec, event, bar_end = key
        nid = await tx.insert_notice(NoticeRow(route=route, bar_spec=spec, event=event, bar_end_ms=bar_end,
                                               part=END_PART, decision_count=len(group.decisions),
                                               created_at_ms=now))
        st.closed[key] = now
        if nid is not None:
            await self._finish(tx, st, nid, route, spec, event, bar_end, END_PART, "", group.decisions, policy, now,
                               fresh)
            return len(group.decisions)
        if event == PRE:
            st.counts.pre_expired += len(group.decisions)
            return await tx.suppress([sd.ref for sd in group.decisions])
        changed = 0
        for sd in group.decisions:
            changed += await self._item_notice(tx, st, route, spec, sd, policy, now, fresh, late=True)
        return changed

    async def _item_notice(self, tx: ArmStateTxn, st: _Stage, route: str, spec: str, sd: StoredDecision,
                           policy: RoutePolicy, now: int, fresh: Mapping[tuple[int, int], _Fresh], *,
                           late: bool = False) -> int:
        d = sd.row
        key = item_key_of(d)
        nid = await tx.insert_notice(NoticeRow(route=route, bar_spec=spec, event=d.event, bar_end_ms=d.bar_end_ms,
                                               part=0, item_key=key, decision_count=1, created_at_ms=now))
        if nid is None:
            st.counts.duplicate_items += 1
            return await tx.suppress([sd.ref])
        await self._finish(tx, st, nid, route, spec, d.event, d.bar_end_ms, 0, key, [sd], policy, now, fresh,
                           late=late)
        return 1

    async def _finish(self, tx: ArmStateTxn, st: _Stage, nid: int, route: str, spec: str, event: str, bar_end: int,
                      part: int, item_key: str, decisions: Sequence[StoredDecision], policy: RoutePolicy, now: int,
                      fresh: Mapping[tuple[int, int], _Fresh], *, pending: tuple[PendingBody, ...] = (),
                      extras: tuple[ExtraBody, ...] = (), late: bool = False) -> None:
        """Attach the decisions, build the ids-only body and queue the outbox row, all in `tx`."""
        if decisions:
            await tx.attach_notice(nid, [sd.ref for sd in decisions])
        items = tuple(self._item(sd, fresh.get((sd.ref.decision_id, sd.ref.bar_end_ms)), late=late)
                      for sd in sorted(decisions, key=lambda s: (s.row.key.adapter_id, s.row.key.position_id)))
        adapter = decisions[0].row.key.adapter_id if decisions else (pending[0].adapter_id if pending else "")
        skew = self._skew(adapter) if adapter else SkewEstimate()
        body = NoticeBody(
            notice_id=nid, route=route, event=event, bar_spec=spec, bar_end_ms=bar_end, part=part,
            item_key=item_key, created_ms=now,
            expires_at_ms=expires_at_ms(event, bar_end_ms=bar_end, created_ms=now, spec=BarSpec.parse(spec),
                                        skew=skew, alive_ms=self._cfg.alive_every_ms,
                                        other_ms=self._cfg.other_ttl_ms),
            items=items, pending=pending, extras=extras, failed=self._failed_lines())
        await self._enqueue(tx, st, body, policy.pre_channels if event == PRE else policy.post_channels)
        st.counts.items += len(items)

    async def _enqueue(self, tx: ArmStateTxn, st: _Stage, body: NoticeBody, channels: tuple[str, ...]) -> None:
        assert body.expires_at_ms is not None
        await self._outbox.enqueue(tx, OutboxNotice(
            notice_id=body.notice_id, route=body.route, channels=tuple(channels), subject=subject(body),
            body=encode_body(body), created_ms=body.created_ms, expires_at_ms=body.expires_at_ms,
            meta=NoticeMeta(route=body.route, bar_spec=body.bar_spec, event=body.event, bar_end_ms=body.bar_end_ms,
                            part=body.part, item_key=body.item_key)))
        st.counts.notices += 1
        if body.event != ALIVE:
            st.hours.add((body.route, body.created_ms - body.created_ms % self._cfg.alive_every_ms))

    def _item(self, sd: StoredDecision, fresh: _Fresh | None, *, late: bool = False) -> ItemBody:
        d = sd.row
        info = self._info.get(d.key)
        side = info.side if info is not None else ""
        valid_until = self._window_end(d) if d.event == PRE else None
        trig = None if d.event == PRE else trigger_input(d.series_inputs, bar_end_ms=d.bar_end_ms,
                                                         bar_spec=d.bar_spec, side=side)
        return ItemBody(
            decision_id=sd.ref.decision_id, decision_bar_end_ms=sd.ref.bar_end_ms, adapter_id=d.key.adapter_id,
            position_id=d.key.position_id, symbol=info.symbol if info is not None else "", side=side,
            event=d.event, bar_end_ms=d.bar_end_ms, revision=d.revision, outcome=d.outcome, category=d.category,
            reason=d.reason, reason_kind=d.reason_kind, trigger_met=d.trigger_met, borderline=d.borderline,
            advice=d.advice, proposed_stop=d.proposed_stop, current_risk_r=d.current_risk_r,
            first_bar_ratio=d.first_bar_ratio, caveats=d.caveats, quality_flags=d.quality_flags,
            stop_basis=d.stop_basis, trigger_source=d.close_source, handles=fresh.handles if fresh is not None else (),
            stop_read_at_ms=fresh.read_at_ms if fresh is not None else None, late=late,
            stop_now=fresh.level if fresh is not None else None,
            # the pinned entry: for favour R at send time (with the trigger input), and for a PRE ADJUST item's HTML
            # stop block (order state; the text shows it only as before, Q13 and never at PRE)
            trigger_input=trig, entry=info.entry if info is not None and (
                trig is not None or (d.event == PRE and d.outcome == Outcome.ADJUST.value)) else None,
            s0=info.s0 if info is not None and trig is not None else None, supersedes=d.supersedes_decision_id,
            decided_at_ms=d.decided_at_ms, strategy_kind=d.strategy_kind,
            strategy_impl_version=d.strategy_impl_version, valid_until_ms=valid_until,
            loosen_policy=info.loosen_policy if info is not None and d.outcome == Outcome.COMPLETE.value else None)

    def _failed_lines(self) -> tuple[FailedLine, ...]:
        if self._failed is None:
            return ()
        try:
            return tuple(self._failed.open_scopes())
        except Exception:
            return ()

    def _page_overdue(self, route: str, policy: RoutePolicy) -> None:
        if route in self._paged or not self._configured(tuple(policy.post_channels)):
            return
        self._paged.add(route)
        self.counters.pages += 1
        self._pager.page("warning", "ARM: decisions without a notice",
                         f"route {route}: decisions still without a notice 60 s after their window")

    # ------------------------------------------------------------------------------------------------ timers
    def _grace(self, route: str, spec_name: str, now: int) -> tuple[int, list[PendingBody]] | None:
        """POST part 0 at `T + post_grace` when no decision of bar T is in yet: the positions still pending."""
        spec = BarSpec.parse(spec_name)
        timing = timing_for(spec)
        if timing is None:
            return None
        by_bar: dict[int, list[PendingBody]] = {}
        for row in self._rows:
            if self._route_of(row) != route or row.bar_spec != spec_name or ArmState(row.state) not in MANAGED:
                continue
            skew = self._skew(row.key.adapter_id)
            t = spec.bucket_start_ms(now - skew.p50_ms)
            if now < post_grace_end_ms(t, timing, skew):
                t -= spec.timeframe_ms
            if now >= decision_cap_ms(t, spec, skew) or row.armed_at_ms >= local_bar_end_ms(t, skew):
                continue
            if row.last_decided_bar is not None and row.last_decided_bar >= t:
                continue
            if t not in self._grid.open_bar_ends(row.key.adapter_id, row.symbol, spec, t - 1, t):
                continue
            by_bar.setdefault(t, []).append(PendingBody(adapter_id=row.key.adapter_id,
                                                        position_id=row.key.position_id, symbol=row.symbol,
                                                        side=row.side))
        for t in sorted(by_bar):
            key: _Key = (route, spec_name, POST, t)
            if self._parts.get(key, 0) == 0 and key not in self._closed:
                return t, sorted(by_bar[t], key=lambda p: (p.adapter_id, p.position_id))
        return None

    async def _due_extras(self, route: str, now: int) -> None:
        """Extras whose fold window passed (or `separate` / global ones) as their own notices. They are taken off the
        queue before the first await, so a bar notice built meanwhile cannot carry them too; on an error the ones not
        yet written go back."""
        due = [x for x in self._extras if x.route == route and x.due_ms <= now]
        for e in due:
            self._extras.remove(e)
        policy = self._policies.policy(route, now) if route != GLOBAL_ROUTE else None
        channels = tuple(self._cfg.global_channels if policy is None else policy.post_channels)
        for i, e in enumerate(due):
            try:
                async with self._tx() as (tx, st):
                    nid = await tx.insert_notice(NoticeRow(route=route, event=e.event, bar_end_ms=e.bar_end_ms,
                                                           item_key=e.item_key, created_at_ms=now))
                    if nid is not None:
                        await self._enqueue(tx, st, NoticeBody(
                            notice_id=nid, route=route, event=e.event, bar_end_ms=e.bar_end_ms, item_key=e.item_key,
                            created_ms=now, expires_at_ms=now + self._cfg.other_ttl_ms, extras=(e.body,),
                            generation=e.generation), channels)
            except BaseException:
                self._extras.extend(x for x in due[i:] if x not in self._extras)
                raise

    def _counts(self, route: str | None) -> AliveCounts:
        rows = [r for r in self._rows if route is None or self._route_of(r) == route]
        managed = [r for r in rows if ArmState(r.state) in MANAGED]
        faulting = sum(1 for r in managed if r.ref in self._faulting)
        return AliveCounts(armed=len(managed), faulting=faulting, unmanaged=self._unmanaged() if route is None else 0)

    async def _alive(self, route: str, now: int) -> None:
        """The hourly ALIVE line. The hour counts as done only once its transaction committed (or it was skipped
        after a busy hour), so a failed one is tried again on the next pass."""
        hour = now - now % self._cfg.alive_every_ms
        if self._alive_done.get(route) == hour:
            return
        counts = self._counts(route)
        if counts.armed == 0:
            return
        policy = self._policies.policy(route, now)
        busy = hour - self._cfg.alive_every_ms in self._notice_hours.get(route, set())
        if policy.mode != "actionable" and busy:
            self._alive_done[route] = hour
            return
        async with self._tx() as (tx, st):
            nid = await tx.insert_notice(NoticeRow(route=route, event=ALIVE, bar_end_ms=hour, created_at_ms=now))
            if nid is not None:
                await self._enqueue(tx, st, NoticeBody(notice_id=nid, route=route, event=ALIVE, bar_end_ms=hour,
                                                       created_ms=now, expires_at_ms=now + self._cfg.alive_every_ms,
                                                       alive=counts, failed=self._failed_lines()),
                                    tuple(policy.post_channels))
        self._alive_done[route] = hour

    async def _test(self, now: int) -> None:
        """The daily TEST per channel. The day counts as done only once every channel's transaction committed (a
        channel already sent is a key conflict on the retry, so nothing goes out twice)."""
        day = now - now % 86_400_000
        if self._test_done == day or now < day + self._cfg.test_at_ms_of_day:
            return
        counts = self._counts(None)
        for channel in self._cfg.test_channels:
            async with self._tx() as (tx, st):
                nid = await tx.insert_notice(NoticeRow(route=GLOBAL_ROUTE, event=TEST, bar_end_ms=day,
                                                       item_key=channel, created_at_ms=now))
                if nid is not None:
                    await self._enqueue(tx, st, NoticeBody(notice_id=nid, route=GLOBAL_ROUTE, event=TEST,
                                                           bar_end_ms=day, item_key=channel, created_ms=now,
                                                           expires_at_ms=now + self._cfg.other_ttl_ms, alive=counts,
                                                           channel=channel, failed=self._failed_lines()), (channel,))
        self._test_done = day

    async def _asked_tests(self, now: int) -> None:
        """The operator's test notices (`request_test`): each counts as done only once every channel's transaction
        committed (a channel already sent is a key conflict on the retry)."""
        for at_ms in list(self._tests_asked):
            counts = self._counts(None)
            for channel in self._cfg.test_channels:
                async with self._tx() as (tx, st):
                    nid = await tx.insert_notice(NoticeRow(route=GLOBAL_ROUTE, event=TEST, bar_end_ms=at_ms,
                                                           item_key=channel, created_at_ms=now))
                    if nid is not None:
                        await self._enqueue(tx, st, NoticeBody(notice_id=nid, route=GLOBAL_ROUTE, event=TEST,
                                                               bar_end_ms=at_ms, item_key=channel, created_ms=now,
                                                               expires_at_ms=now + self._cfg.other_ttl_ms,
                                                               alive=counts, channel=channel,
                                                               failed=self._failed_lines()), (channel,))
            self._tests_asked.remove(at_ms)

    def _prune(self, now: int) -> None:
        """Drop what a 24/7 server no longer needs: part counts and closed windows older than `keep_ms`; the D3 and
        PRE memory, route info and FAULT marks of positions gone that long; and the members, ALIVE marks, notice
        hours and pages of routes with no live position, no remembered position and no queued extra."""
        cut = now - self._cfg.keep_ms
        for book in (self._parts, self._closed):
            for k in [k for k in book if k[3] < cut]:
                del book[k]
        for pk in [k for k in self._pre_outcome if k[1] < cut]:
            del self._pre_outcome[pk]
        for hours in self._notice_hours.values():
            hours -= {h for h in hours if h < cut}
        live = {r.ref for r in self._rows}
        for ref in [r for r, (t, _) in self._last.items() if t < cut and r not in live]:
            del self._last[ref]
        self._faulting &= live | set(self._last)
        live_keys = {r.key for r in self._rows}
        remembered = set(self._last) | {ref for ref, _ in self._pre_outcome}
        for pos in [p for p in self._info
                    if p not in live_keys and position_ref(p.adapter_id, p.position_id) not in remembered]:
            del self._info[pos]
        members: dict[str, set[tuple[str, str]]] = {}
        for pos, info in self._info.items():
            members.setdefault(self._routes.route_of(pos.adapter_id, info.account_id), set()).add(
                (pos.adapter_id, info.account_id))
        for e in self._extras:
            if e.member is not None:
                members.setdefault(e.route, set()).add(e.member)
        self._members = members
        for route in [r for r in self._alive_done if r not in members]:
            del self._alive_done[route]
        for route in [r for r in self._notice_hours if r not in members]:
            del self._notice_hours[route]
        self._paged &= set(members)


class NoticeSink:
    """The `ARM_SINKS` notice sink: inside the BarClock's (event, bar end) transaction, the notice work of every
    route that has a newly inserted decision (`ArmNotices.on_commit`). Returns the decisions unchanged."""

    def __init__(self, notices: ArmNotices) -> None:
        self._notices = notices

    @property
    def name(self) -> str:
        return NOTICE_SINK

    async def on_decisions(self, tx: ArmStateTxn, event: ArmEvent, bar_end_ms: int,
                           decisions: Sequence[Written]) -> tuple[Written, ...]:
        await self._notices.on_commit(tx, event, bar_end_ms, decisions)
        return tuple(decisions)

    def settle(self, tx: ArmStateTxn, *, committed: bool) -> None:
        """`SettlingSink`: the BarClock's transaction ended; apply or discard the notices' stage for it."""
        self._notices.settle(tx, committed=committed)


__all__ = [
    "ALIVE", "AcceptedPolicies", "ArmNotices", "BAR_EVENTS", "END_PART", "FAILED_CLEARED", "FOLDS",
    "FailedLinesPort", "GLOBAL_ROUTE", "IMMEDIATE_OUTCOMES", "ITEM_EVENTS", "LAST_PART", "MD_RESET",
    "NoticeCounters", "NoticeMeta", "NoticeOutboxPort", "NoticeSettings", "NoticeSink", "OutboxNotice", "RouteBook",
    "RoutePolicyPort", "TASK", "TEST", "expires_at_ms", "held_stop_level", "item_key_of", "pinned_loosen_policy",
    "shown_by_mode", "supersedes",
]
