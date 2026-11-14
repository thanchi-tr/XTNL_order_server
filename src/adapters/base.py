"""BrokerAdapter: the read-only plugin contract (adapter-isolation.md §4).

Only fetch_* / stream_* / ensure_ticks are defined, plus connect/close for the session.
fetch_market_sessions() is optional (default: no broker schedule) and read-only like every fetch_*.
fetch_history() is optional too (default: an empty BrokerHistory with complete=False, 'history not supported').
No order mutation method exists on this interface or on any implementation.
Adapters never import src.db, src.cache, src.pubsub, src.integrity or src.oversee.
"""
from __future__ import annotations

import abc
import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

from src.dto import AccountDTO, AccountLive, CashFlowDTO, InstrumentSpec, OrderDTO, PositionDTO, PriceDTO

if TYPE_CHECKING:
    from src.adapters.ratelimit import RestBudget

_log = logging.getLogger(__name__)

StreamName = Literal["prices", "orders", "account"]
EntityType = Literal["ORDER", "POSITION", "ACCOUNT"]


class StreamEventKind(str, Enum):
    SUBSCRIBED = "SUBSCRIBED"
    UNSUBSCRIBED = "UNSUBSCRIBED"
    ERROR = "ERROR"            # subscription error while the socket may still be up -> gap
    GAP = "GAP"                # frames could not be handed over
    HEARTBEAT = "HEARTBEAT"    # transport liveness (refreshes conn: keys while SUBSCRIBED)
    AUTH_LOST = "AUTH_LOST"    # session expired / invalidated -> AUTHENTICATING
    DISCONNECTED = "DISCONNECTED"


@dataclass(frozen=True)
class StreamEvent:
    """Control frame from an adapter stream (not an entity)."""

    kind: StreamEventKind
    stream: StreamName
    detail: str = ""
    at_ms: int = 0


@dataclass(frozen=True)
class VersionDecl:
    """Declared per (adapter, entity type), dto-contracts.md §2."""

    has_broker_seq: bool
    has_broker_time: bool


@dataclass(frozen=True)
class MarketSessions:
    """One (adapter, symbol) market schedule as the broker reports it (SM10, operations.md §10).

    ``timezone``, ``schedule`` and ``holidays`` are exactly the `market_sessions` row columns that
    src/calendar/schedule.py:symbol_schedule_from_json parses (weekly sessions, daily breaks, naive
    local holiday closures). ``source`` is the row's source ('ctrader' | 'stonex' | 'mock').
    ``is_default`` marks a documented default used because the broker gave no hours for the symbol
    (it is not broker data and needs review); ``holidays_known`` is False when the broker has no
    holiday source, so an empty ``holidays`` list does not mean "no holidays".
    """

    adapter_id: str
    symbol: str
    timezone: str
    schedule: dict[str, Any]
    holidays: list[dict[str, Any]] = field(default_factory=list)
    source: str = "mock"
    is_default: bool = False
    holidays_known: bool = True
    note: str = ""


class CashFlowBatch(list[CashFlowDTO]):
    """A ``fetch_cash_flows()`` result that knows whether it is complete (IMPLEMENTATION_PLAN 7.1).

    It is a plain list of the flows that could be mapped, so every existing caller keeps working, plus
    ``partial_reasons``: why rows of the window may be missing (a capped / truncated history page, a booked
    balance move the adapter could not map). A partial batch must never look complete: the orchestrator
    reads it with ``cash_flows_partial()`` and turns the adapter's ``pnl_source`` off (MaxDailyLossRule then
    FLAGs ``pnl_unverified``) until a complete fetch arrives."""

    partial_reasons: tuple[str, ...]

    def __init__(self, flows: Iterable[CashFlowDTO] = (), partial_reasons: Iterable[str] = ()) -> None:
        super().__init__(flows)
        self.partial_reasons = tuple(dict.fromkeys(str(r) for r in partial_reasons))

    @property
    def partial(self) -> bool:
        return bool(self.partial_reasons)


def cash_flows_partial(flows: object) -> tuple[str, ...]:
    """Why a ``fetch_cash_flows()`` result is partial; ``()`` when it claims to be complete. A plain list
    (an adapter without the facility) is complete. A malformed marker is read as partial (fail closed)."""
    reasons = getattr(flows, "partial_reasons", None)
    if reasons is None:
        return ()
    if isinstance(reasons, (tuple, list)) and all(isinstance(r, str) for r in reasons):
        return tuple(reasons)
    return ("partial marker unreadable",)


HISTORY_NOT_SUPPORTED = "history not supported"


@dataclass(frozen=True)
class BrokerHistory:
    """What the broker says changed in ``[since_ms, until_ms]`` (``fetch_history``): the off-time catch-up read.

    ``orders`` holds every order whose broker state changed in the window, terminal or not (filled, cancelled,
    rejected and still-working ones), each with its broker version (``broker_updated_at_ms``, and
    ``broker_seq`` where declared), so the normal version compare orders it against stream rows: a historic
    row never carries a broker time later than the broker event it describes. ``positions`` holds the
    positions opened or closed in the window, CLOSED ones included.

    ``orders`` may hold several versions of one order, each at the broker time of the state it describes (StoneX:
    a stop / limit order WORKING at its placement, then CANCELLED or FILLED at its end). Contract: the versions of
    one entity appear oldest first (by broker revision, then broker time); a consumer submits them in that order and
    counts entities, not versions.

    ``covered_from_ms`` is the earliest time the answer really covers (later than ``since_ms`` when the
    broker keeps less history, or when only the newest part of a capped answer was readable). ``complete``
    is False whenever something of the window may be missing (paging limits, retention, a row that could not
    be mapped, a failed page, a kind of order the broker cannot list); ``notes`` then says why, with counts
    and ids only, never an amount. A caller must treat an incomplete history as "orders may be lost".

    ``retryable`` (read only when ``complete`` is False) says whether a later re-read may answer better: True for
    a cause on this side or a passing one (a failed or timed-out page or request, a lost session, a refused
    request budget, a row the adapter could not map, evidence that did not add up between two reads) and
    whenever the cause is unknown (the default: fail closed, the caller holds coverage back and retries).
    False only when every cause is a genuine broker limit that a re-read would repeat: retention, the broker's
    own truncation / paging limit, an unconfirmed paging rule, no history facility. ``unmapped`` counts the
    history rows the adapter could not map (also in ``notes``); they never count on the live-stream
    ``unmapped_rows`` channel (``history_read``)."""

    adapter_id: str
    since_ms: int
    until_ms: int
    covered_from_ms: int
    orders: tuple[OrderDTO, ...]
    positions: tuple[PositionDTO, ...]
    complete: bool
    notes: tuple[str, ...]
    retryable: bool = True
    unmapped: int = 0

    @classmethod
    def unsupported(cls, adapter_id: str, since_ms: int, until_ms: int,
                    note: str = HISTORY_NOT_SUPPORTED) -> BrokerHistory:
        """An empty, incomplete answer that covers nothing (``covered_from_ms = until_ms``, fail closed). Not
        retryable: an adapter without the facility answers the same every time."""
        return cls(adapter_id=adapter_id, since_ms=since_ms, until_ms=until_ms, covered_from_ms=until_ms,
                   orders=(), positions=(), complete=False, notes=(note,), retryable=False)


class HistoryNotes(list[str]):
    """The notes of one ``fetch_history`` read, classified for ``BrokerHistory.retryable``.

    ``append`` / ``extend`` add a note whose cause a re-read may fix, or whose cause is unknown (the fail-closed
    default); ``limit`` adds a genuine broker limit that a re-read would repeat (retention, the broker's own
    truncation or paging limit, an unconfirmed paging rule, a listing the broker does not offer)."""

    def __init__(self) -> None:
        super().__init__()
        self._limits = 0

    def limit(self, note: str) -> None:
        self._limits += 1
        self.append(note)

    @property
    def retryable(self) -> bool:
        """True when any note is not a broker limit."""
        return len(self) > self._limits

    def unique(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(self))


def covered_from(since_ms: int, until_ms: int, retention_ms: int | None) -> int:
    """The window start the broker can still answer for, given its history depth (None: unknown = all)."""
    if retention_ms is None or retention_ms <= 0:
        return since_ms
    return max(since_ms, until_ms - retention_ms)


#: The `unmapped_rows` counter keys (src/pubsub/orchestrator.py UNMAPPED_ATTR): "orders" also covers positions.
UNMAPPED_STREAMS: tuple[StreamName, ...] = ("orders", "account", "prices")


def new_unmapped_rows() -> dict[str, int]:
    """A fresh `unmapped_rows` counter set: monotonic, one per stream kind."""
    return {s: 0 for s in UNMAPPED_STREAMS}


class HistoryRead:
    """The broker rows a catch-up read could not map (``history_read``): kept apart from the live-stream
    ``unmapped_rows`` counters, so a row of a history (or a catch-up by-id re-read) is reported by that read,
    as incomplete, and never opens a stream gap or counts as an interruption of the live stream."""

    def __init__(self, owner: object) -> None:
        self.unmapped = 0
        self._owner = owner          # the asyncio task (or thread marker) that opened the read
        self._parent: HistoryRead | None = None


_HISTORY_READ: ContextVar[HistoryRead | None] = ContextVar("xtnl_history_read", default=None)


def _current_owner() -> object:
    try:
        return asyncio.current_task()
    except RuntimeError:             # no running loop: a synchronous caller
        return None


@contextlib.contextmanager
def history_read() -> Iterator[HistoryRead]:
    """Count every row the adapter could not map while the block runs in the calling task on this read's own
    channel instead of the live-stream ``unmapped_rows``. Travels in a ``ContextVar`` (like ``rest_priority``),
    and counts only in the task that opened it: a task the adapter spawns inside the block (a stream pump, a
    reader loop) keeps counting on the live-stream counters (fail closed). Nested reads add their count to the
    enclosing one on exit."""
    read = HistoryRead(_current_owner())
    read._parent = _HISTORY_READ.get()
    token = _HISTORY_READ.set(read)
    try:
        yield read
    finally:
        _HISTORY_READ.reset(token)
        if read._parent is not None:
            read._parent.unmapped += read.unmapped


def note_unmapped(counters: dict[str, int], adapter_id: str, stream: StreamName, why: str) -> None:
    """Count one broker row (stream or REST) the adapter could not map (dto-contracts.md §4: never silently
    dropped). The orchestrator opens that stream's gap when the counter grows, so no reconcile pass can clear
    the fence while a row is missing. Inside a ``history_read()`` block of the calling task the row counts on
    that read instead (it reports itself incomplete; a live-stream gap would be a false interruption).
    Synchronous, no I/O; ``why`` must carry no credential."""
    read = _HISTORY_READ.get()
    if read is not None and read._owner is _current_owner():
        read.unmapped += 1
        _log.error("%s: unmappable broker row in a history read (%d so far): %s", adapter_id, read.unmapped, why)
        return
    counters[stream] = counters.get(stream, 0) + 1
    _log.error("%s: unmappable broker row on %s (%d so far): %s", adapter_id, stream, counters[stream], why)


def unmapped_mark(counters: Mapping[str, int], stream: StreamName) -> int:
    """A monotonic mark of the rows `note_unmapped` counted for `stream` as the calling task sees them: the live
    counter plus, inside a ``history_read()`` block of this task, that read's own count. An adapter that tells
    "unmappable" (raise) from "nothing proved" (None) by the mark growing around a mapping call must use this,
    never the live counter alone: inside a catch-up read the row lands on the read and the live counter stays
    put, so an unmappable answer would come back as None (a broker "not found")."""
    read = _HISTORY_READ.get()
    own = read.unmapped if read is not None and read._owner is _current_owner() else 0
    return counters.get(stream, 0) + own


class AbsenceProbe:
    """The explicit broker "not found" answers of the by-id reads made inside one ``absence_probe()`` block
    (IMPLEMENTATION_PLAN 8.1). A by-id read (``fetch_order_status`` / ``fetch_position_status``) answers None
    both when the broker said "not found" and when it proved nothing; only the first is absence. The DLQ
    check-before-retry drops a parked row only when its read was an explicit "not found" recorded here."""

    def __init__(self) -> None:
        self._absent: set[tuple[EntityType, str]] = set()

    def absent(self, entity: EntityType, entity_id: str) -> bool:
        """True when a by-id read of this entity in the block got an explicit broker "not found"."""
        return (entity, entity_id) in self._absent

    def _note(self, entity: EntityType, entity_id: str) -> None:
        self._absent.add((entity, entity_id))


_ABSENCE: ContextVar[AbsenceProbe | None] = ContextVar("xtnl_absence_probe", default=None)


@contextlib.contextmanager
def absence_probe() -> Iterator[AbsenceProbe]:
    """Collect explicit "not found" answers of the by-id reads awaited inside the block. The probe travels in a
    ``ContextVar`` (like ``rest_priority``), so it reaches the adapter through any number of awaits and is never
    shared with a concurrent caller's read of the same id."""
    probe = AbsenceProbe()
    token = _ABSENCE.set(probe)
    try:
        yield probe
    finally:
        _ABSENCE.reset(token)


def note_absent(entity: EntityType, entity_id: str) -> None:
    """An adapter's by-id read of ``entity_id`` got an explicit broker "not found" (an HTTP 404, a not-found
    error code, or a well-formed answer that names no such row). Call it only then: a None for any other reason
    (an unparsable id, a body that proves nothing) must not be recorded. Synchronous, no I/O."""
    probe = _ABSENCE.get()
    if probe is not None:
        probe._note(entity, entity_id)


class BrokerError(Exception):
    """Base class for broker errors raised by adapters."""


class TransientBrokerError(BrokerError):
    """Network / timeout / 5xx / throttling: back off and retry."""


class AuthFailedError(BrokerError):
    """Credential error (bad password, PasswordChangeRequired, invalid_grant): never loop."""


class SessionExpiredError(BrokerError):
    """The session token is no longer valid (HTTP 401): re-authenticate."""


OrderStreamItem = OrderDTO | PositionDTO | StreamEvent
AccountStreamItem = AccountDTO | AccountLive | CashFlowDTO | StreamEvent
PriceStreamItem = PriceDTO | StreamEvent


class BrokerAdapter(abc.ABC):
    """Read-only broker adapter. Implementations: StoneXAdapter, MockBrokerAdapter (PepperstoneAdapter later)."""

    adapter_id: str
    VERSIONING: dict[EntityType, VersionDecl]

    # --- runtime introspection (read-only; defaults for adapters without the facility) ---------------
    @property
    def rest_budget(self) -> RestBudget | None:
        """The adapter's one shared REST token bucket (operations.md §15), or None when unmetered.
        Callers tag their class with ``src.adapters.ratelimit.rest_priority``; the metrics read
        ``rest_budget.stats()``."""
        return None

    @property
    def token_issued_at_ms(self) -> int | None:
        """Wall-clock ms when the credential in use was issued (StoneX: last LogOn; cTrader: when the
        token pair was saved). None when unknown or not connected (xtnl_token_age_seconds is then absent)."""
        return None

    @property
    def token_expires_at_ms(self) -> int | None:
        """Wall-clock ms when the credential in use expires; None when it does not expire or is unknown."""
        return None

    # --- session lifecycle (no trading capability) -------------------------------------
    @abc.abstractmethod
    async def connect(self) -> None:
        """Authenticate and open the session. Raises AuthFailedError / TransientBrokerError."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Close streams and the session."""

    # --- REST reads ----------------------------------------------------------------------
    @abc.abstractmethod
    async def fetch_instruments(self) -> list[InstrumentSpec]: ...

    @abc.abstractmethod
    async def fetch_open_orders(self) -> list[OrderDTO]: ...

    @abc.abstractmethod
    async def fetch_order_status(self, broker_order_id: str) -> OrderDTO | None:
        """The broker's row for this id. None means the broker has no such row; call ``note_absent("ORDER", id)``
        only when the broker said so explicitly. A row the adapter cannot map counts in ``unmapped_rows`` and
        raises ``BrokerError`` (never None)."""

    @abc.abstractmethod
    async def fetch_positions(self) -> list[PositionDTO]: ...

    @abc.abstractmethod
    async def fetch_position_status(self, position_id: str) -> PositionDTO | None:
        """As ``fetch_order_status``, for positions (``note_absent("POSITION", id)`` on an explicit not-found)."""

    @abc.abstractmethod
    async def fetch_account_snapshot(self) -> tuple[list[AccountDTO], list[AccountLive]]: ...

    @abc.abstractmethod
    async def fetch_cash_flows(self, since_ms: int) -> list[CashFlowDTO]:
        """Every booked balance move since ``since_ms``. When rows may be missing (a capped page, a row
        that could not be mapped) return a ``CashFlowBatch`` with ``partial_reasons`` set: a partial
        result must never look complete."""

    async def fetch_market_sessions(self, symbols: Sequence[str]) -> dict[str, MarketSessions]:
        """Read-only: weekly sessions, daily breaks and holidays per canonical symbol (SM10).

        A symbol the broker has no schedule for is absent from the result (the calendar then has no
        sessions for it and pages, or uses a reviewed config). Default: no broker schedule at all.

        A result is a snapshot, not a forever schedule: StoneX hours are UTC times that follow the
        current DST offset, and cTrader recurring holidays are expanded around the fetch date, so the
        caller re-fetches at every LEADER_INIT and at least daily (e.g. after each roll)."""
        return {}

    @property
    def history_retention_ms(self) -> int | None:
        """Best-known depth of the broker's order / deal history; None when unknown."""
        return None

    async def fetch_history(self, since_ms: int, until_ms: int) -> BrokerHistory:
        """Read-only: every order and position the broker changed in ``[since_ms, until_ms]``, terminal ones
        included (the start-up / wake-up catch-up after the engine was off). Cash flows of the window keep
        using ``fetch_cash_flows(since_ms)``. Default: no history facility, an empty incomplete answer."""
        return BrokerHistory.unsupported(self.adapter_id, since_ms, until_ms)

    # --- streams -----------------------------------------------------------------------------
    @abc.abstractmethod
    def stream_ticks(self, symbols: Sequence[str]) -> AsyncIterator[PriceStreamItem]: ...

    @abc.abstractmethod
    async def ensure_ticks(self, symbols: Sequence[str]) -> None:
        """Add symbols to the live tick subscription (read-only subscribe)."""

    @abc.abstractmethod
    def stream_order_updates(self) -> AsyncIterator[OrderStreamItem]:
        """Orders and positions (StoneX derives positions from ORDERS items)."""

    @abc.abstractmethod
    def stream_account(self) -> AsyncIterator[AccountStreamItem]: ...

    def stream_positions(self) -> AsyncIterator[OrderStreamItem]:
        """Default: positions arrive on the order stream (StoneX). cTrader overrides."""
        return self.stream_order_updates()


FORBIDDEN_METHOD_NAMES = frozenset(
    {"place_order", "modify_order", "cancel_order", "submit_order", "close_position", "amend_order", "update_order"}
)


def assert_read_only(cls: type) -> None:
    """Contract check used by tests and the verify suite: no mutation method may exist."""
    bad = sorted(n for n in dir(cls) if n.lower() in FORBIDDEN_METHOD_NAMES)
    if bad:
        raise TypeError(f"{cls.__name__} defines forbidden order-changing methods: {bad}")
