"""IngestionOrchestrator: the sole publisher to the four topics (dto-contracts.md §2, §4-§6;
adapter-isolation.md §5).

Per adapter it drives SM4, pumps the adapter streams, labels versions per the adapter's
VERSIONING declaration, stamps seq_id, checks future timestamps and order transitions, publishes
with put_nowait, and parks what cannot be handed over in the DlqSpooler. When the topic and the
DLQ ring are both full it sets backpressure_overflow, opens a gap and asks SM4 for RESYNCING.
Persistence goes only through src/ports.py protocols (no src.db / src.cache imports).

Optional ports (None = skipped):
* `auth_store` (AdapterAuthStore): before every connect the adapter's credential generation is checked;
  a generation known bad is never connected (SM4 goes to AUTH_FAILED). When credential errors exhaust
  SM4's attempts the generation is recorded AUTH_FAILED (a cluster-wide fact, operations.md §16), and
  while AUTH_FAILED a changed generation re-arms the adapter.
* `instrument_sink` (InstrumentSink): the SNAPSHOTTING instrument refresh is persisted after
  fetch_instruments() (driver path, never the socket path); a failed save is retried while STREAMING.
* `on_price_tick(tick, snapshot)`: SM10 open-market evidence (MarketCalendar.on_tick), called on the
  parser path for every tick; `snapshot` marks a symbol's first tick after the prices stream subscribed.
* `on_execution(adapter_id, symbol, ts_ms)`: SM10 execution evidence (MarketCalendar.on_execution), called on
  the parser path when an order / position **stream** row shows a fill or execution against the entity's
  previously known row (see `_execution_evidence`); `ts_ms` is the local receipt time. Never awaited.
* `pager(severity, title, body)`: operational pages (src/pubsub/alerts.py), synchronous, never awaited: a
  critical page when credential errors stop SM4 in AUTH_FAILED (operations.md §13); a throttled page per
  (adapter, reason) for every row parked as a violation (`future_timestamp`, `illegal_transition`,
  `schema_invalid`, `checksum`, `missing_client_order_id`; dto-contracts.md §2, §4, §5); a page when a
  `conn:` key has been absent > 60 s while STREAMING and the market is OPEN; a stale-price page per subscribed
  symbol without a tick for `stale_price_ms` unless the calendar mutes it; a page after repeated failures of
  the flat-account REST poll.
* `market_open(adapter_id)`: SM10, the adapter's market is OPEN and not in BROKER_MAINTENANCE (conn-key
  absence alerts only fire then). None = always open.
* `price_staleness_suppressed(adapter_id, symbol)`: SM10 `MarketCalendar.price_staleness_suppressed` (CLOSED,
  ROLLOVER, PRE_OPEN, maintenance). None = never suppressed.
* `transfer_sources`: adapter_id -> whether that adapter reports deposits / withdrawals; overrides the
  adapter's `HAS_TRANSFER_SOURCE` attribute (see `transfer_source()`).
* `refresh_adapter(adapter_id, current)`: credential rotation within a term (operations.md §13). Called before
  every connect attempt and on every AUTH_FAILED poll; it returns a new adapter built from rotated credentials,
  or None when the credentials have not changed. The old adapter is closed and the new one connects.
`reset_backoff(adapter_id)` is the PRE_OPEN readiness hook: it ends a BACKOFF wait so SM4 reconnects now.
`market_open_resync(adapter_id, open_ms)` is the SM10 open hook: a STREAMING adapter runs a REST-only RESYNCING
at open + 30-60 s (jittered); an adapter that is not STREAMING goes through SNAPSHOTTING, whose pass, sent after
that time, satisfies the open.
While STREAMING the driver also polls `fetch_account_snapshot()` every 60 s when an account is flat (no OPEN
position; adapter-isolation.md §5 Liveness), bounded by `account_poll_timeout_s`; a failed poll never stops the driver.
Every order / position / account / cash-flow row is schema- and checksum-checked before it is labelled; a bad
row is parked (`schema_invalid` / `checksum`) and never published.
A lost fence (an `EpochMismatch` from a port) always propagates: the caller hard-exits.

Never silently dropped (dto-contracts.md §4, IMPLEMENTATION_PLAN 6.2): a stream item of an unexpected type, or one
whose handling raises, opens that stream's gap (order / account streams; a price frame is only counted). An adapter
that skips a broker row it cannot map counts it in an optional `unmapped_rows` attribute (see `UNMAPPED_ATTR`);
any increase opens the gap of that stream, also inside a REST pass, so the pass cannot clear it. The downtime
catch-up's reads (`catchup_fetch`, and the by-id re-reads of `catchup_submit`) run inside
`src.adapters.base.history_read()`: a row skipped there counts on that read's own channel and reports the catch-up
incomplete, never on `unmapped_rows`, so it is never mistaken for an interruption of the live stream, and a live
row skipped meanwhile (by a stream task) always stays one.
Clean-mid watch (adapter-isolation.md §5): every symbol or cross queued for `ensure_ticks()` must see a clean quote
(spread <= `clean_spread(adapter_id, symbol)`, default 5 pips, the Redis clean_mid rule) within
`clean_mid_ms`, or it pages once (also when its ensure_ticks keeps failing); the grace restarts while muted.
SM4 driver supervision: a non-fence error from any driver step is logged, paged (throttled) and treated as a
transient error (BACKOFF, then a fresh connect); only a lost fence, cancellation or stop ends `run_adapter`. The
backoff grows with the error streak, which ends only after `DRIVER_ERROR_STREAK_RESET_MS` of STREAMING without one.
Page bodies built from exception text are URL-redacted (src/observability/redact.py): pages leave the node.

IMPLEMENTATION_PLAN 7.1:
* `unmapped_rows` growth is also absorbed right before a REST pass is sent, so a row skipped before the pass is
  covered by it, while one skipped during the pass keeps the gap open.
* broker_seq inheritance (dto-contracts.md §2): a row of an (adapter, entity type) that declares broker_seq and
  lacks one inherits the entity's last seen broker_seq. The memory is seeded before the first pass from the stored
  rows (`LocalStateReader.open_entities`, plus the optional `account_states(adapter_id)` of the same reader), so it
  survives a restart or a leader change. A row with nothing to inherit cannot be versioned: it is never published
  with a null broker_seq (the store would drop a real change as stale); it is lost like a dropped frame, which
  opens its stream's gap (fail closed) until a row that carries one arrives.
* A partial `fetch_cash_flows` result (`src.adapters.base.CashFlowBatch.partial_reasons`) is paged and turns
  `pnl_source(adapter_id)` off (MaxDailyLossRule FLAGs `pnl_unverified`) until a complete fetch since the roll.
  Callers that fetch cash flows outside this class report the result with `note_cash_flows()`.
* A row the store refused (`IllegalTransitionParked`, relayed by the consumers through
  `CommitBarrier.refused`, or by a caller through `on_store_refused`) does not stay in memory with its refused
  status: the stored status is restored on that row (or the row is forgotten when the stored status is unknown).
"""
from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
import logging
import random
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Literal, Protocol, TypeVar

from src.adapters.base import (
    AuthFailedError,
    BrokerAdapter,
    BrokerError,
    EntityType,
    SessionExpiredError,
    StreamEvent,
    StreamEventKind,
    VersionDecl,
    cash_flows_partial,
    history_read,
)
from src.dto import (
    AccountDTO,
    AccountLive,
    CashFlowDTO,
    Decision,
    InstrumentSpec,
    OrderDTO,
    OrderStatus,
    PositionDTO,
    PriceDTO,
    Source,
    decide,
    is_future_timestamp,
    is_legal_transition,
)
from src.config import ExecutionLockedError
from src.observability.redact import redact_urls
from src.ports import AdapterAuthStore, InstrumentSink, StreamHealthSink, StreamKind
from src.pubsub.alerts import CRITICAL, HIGH, Episodes, PageThrottle, Pager, send_page
from src.pubsub.barrier import BarrierPass, CommitBarrier
from src.pubsub.bus import EventBus, PricePut
from src.pubsub.connection import (
    MAX_CREDENTIAL_ATTEMPTS,
    ConnectionStateMachine,
    ConnState,
    IllegalConnTransition,
    backoff_delay,
)
from src.pubsub.dlq_rows import (
    CHECKSUM,
    DETERMINISTIC_REASONS,
    FUTURE_TIMESTAMP,
    ILLEGAL_TRANSITION,
    MISSING_CLIENT_ORDER_ID,
    QUEUE_FULL,
    SCHEMA_INVALID,
    Parkable,
    dlq_row,
)
from src.pubsub.health import STREAMS, HealthMirror, HealthOp, SubscriptionState
from src.pubsub.hlc import HybridLogicalClock, wall_clock_ms
from src.pubsub.spooler import DlqSpooler
from src.pubsub.tradingday import last_roll_ms
from src.pubsub.validation import checksum_valid, schema_problem

log = logging.getLogger(__name__)

StatefulDTO = OrderDTO | PositionDTO | AccountDTO
_S = TypeVar("_S", OrderDTO, PositionDTO, AccountDTO)
_NO_DECL = VersionDecl(has_broker_seq=False, has_broker_time=False)
HEARTBEAT_TIMEOUT_MS = 30_000
CASH_FLOW_MIN_INTERVAL_MS = 60_000
ENSURE_TICKS_MIN_INTERVAL_MS = 1_000
RING_RESYNC_THRESHOLD = 0.5
INSTRUMENT_SAVE_RETRY_MS = 30_000
DEFAULT_CREDENTIAL_GENERATION = "default"
CONN_KEY_TTL_MS = 30_000            # conn:{adapter}:{stream} PEXPIRE (src/cache RedisStreamHealth)
CONN_ABSENT_ALERT_MS = 60_000       # page when a conn: key is absent longer than this while STREAMING + OPEN
STALE_PRICE_MS = 30_000             # a subscribed symbol without a tick for this long pages (unless muted)
ACCOUNT_POLL_MS = 60_000            # flat-account REST fetch_account_snapshot() period
ACCOUNT_POLL_PAGE_AFTER = 3         # consecutive failed polls before the account path is paged
OPEN_RESYNC_MIN_MS = 30_000         # market-open REST-only RESYNCING at open + 30 s ...
OPEN_RESYNC_SPAN_MS = 30_000        # ... + U(0, 30 s)
PARK_PAGE_INTERVAL_MS = 60_000      # at most one violation page per (adapter, reason) per minute
CLEAN_MID_MS = 30_000               # a symbol queued for ensure_ticks() without a clean quote this long pages
DEFAULT_CLEAN_SPREAD_PIPS = Decimal("5")   # oversee-rules marks.max_clean_spread_pips default
DRIVER_ERROR_PAGE_INTERVAL_MS = 300_000    # at most one SM4 driver-error page per adapter per 5 min
#: The driver-error streak (backoff exponent) ends only after STREAMING this long without a driver error, so an
#: error that recurs right after each reconnect (e.g. in every RESYNCING) keeps backing off instead of
#: reconnecting and re-snapshotting every ~1 s.
DRIVER_ERROR_STREAK_RESET_MS = 60_000
FRAME_LOST_LOG_INTERVAL_MS = 60_000        # full "frame lost" log (with traceback) per (adapter, stream)
#: Optional adapter attribute: monotonic counters of broker rows the adapter could not map and skipped, keyed
#: by stream kind ("orders" also counts positions; "positions" is accepted as an alias), or a single int that
#: counts for both the "orders" and "account" streams. Any increase opens that stream's gap.
UNMAPPED_ATTR = "unmapped_rows"
#: Via-currency preference for two-leg FX conversion; must match src/oversee/risk.py PREFERRED_CROSS.
FX_VIA_ORDER: tuple[str, ...] = ("USD", "EUR", "GBP", "JPY")
#: Parked reasons that are violations (paged), as opposed to QueueFull (backpressure, not paged per row).
PAGED_PARK_REASONS = DETERMINISTIC_REASONS

PumpName = Literal["orders", "positions", "account", "prices"]
CredentialGeneration = Callable[[str], "str | Awaitable[str]"]
CleanSpread = Callable[[str, str], "Decimal | None"]
AdapterRefresher = Callable[[str, BrokerAdapter], "BrokerAdapter | None"]


def _is_fence_loss(exc: BaseException) -> bool:
    """An EpochMismatch from a storage port (src.db is not imported here): never swallowed."""
    return any(cls.__name__ == "EpochMismatch" for cls in type(exc).__mro__)


class LocalStateReader(Protocol):
    """Optional: the locally stored non-terminal orders / OPEN positions of an adapter, read once
    before the first snapshot so absent ones are re-read by id (adapter-isolation.md §5)."""

    async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]: ...


class AccountStateReader(Protocol):
    """Optional extra method of the `local_state` reader: the stored `account_state` rows of an adapter, read
    once before its first snapshot so a row without broker_seq inherits the stored one after a restart."""

    async def account_states(self, adapter_id: str) -> Sequence[AccountDTO]: ...


@dataclass
class SnapshotReport:
    adapter_id: str
    first: bool
    send_seq: int
    send_ms: int
    watermark_ms: int | None
    orders: int = 0
    positions: int = 0
    accounts: int = 0
    cash_flows: int = 0
    parked: int = 0
    reread_orders: list[str] = field(default_factory=list)
    reread_positions: list[str] = field(default_factory=list)
    all_subscribed: bool = False
    dropped_after_watermark: bool = False
    #: entity keys the broker listed in this pass (open orders / positions), for the downtime catch-up
    seen_orders: frozenset[str] = frozenset()
    seen_positions: frozenset[str] = frozenset()


def _keep_known_entry(row: PositionDTO, known: PositionDTO | None) -> PositionDTO:
    """A CLOSED history position without an entry price (its opening trade is older than the window and the adapter,
    after a cold start, had no memory of it) takes side and entry from the row this term already knows (seeded from
    the stored OPEN row, or accepted since). Without this the history row, newer or equal in version, would replace
    the known entry with none. Only that gap is filled; a row the broker gave an entry is never changed."""
    if (known is None or row.status != "CLOSED" or row.entry_price is not None or known.entry_price is None
            or known.adapter_id != row.adapter_id):
        return row
    return replace(row, entry_price=known.entry_price, side=known.side)


@dataclass
class CatchupFetch:
    """What one downtime catch-up read from the broker (src/runtime/catchup.py). `history` is the adapter's
    `BrokerHistory` (shared contract, src/adapters/base.py), or None when the adapter has no `fetch_history`
    (then nothing can prove the window: the catch-up is incomplete). Versioned at `send_seq` / `send_ms`, the
    moment the reads were sent (dto-contracts.md §2), with the stream watermark of that moment."""

    adapter_id: str
    since_ms: int
    until_ms: int
    send_seq: int
    send_ms: int
    watermark_ms: int | None
    history: object | None
    cash_flows: Sequence[CashFlowDTO]
    cash_flows_partial: tuple[str, ...] = ()
    #: rows the adapter could not map during these reads (`history_read`: never on the live-stream counters)
    history_unmapped: int = 0


@dataclass
class CatchupPass:
    """The outcome of submitting a catch-up through the normal REST path (`IngestionOrchestrator.catchup_submit`).
    Counts and ids only."""

    adapter_id: str
    orders_published: int = 0
    positions_published: int = 0
    cash_flows_published: int = 0
    parked: int = 0
    parked_queue_full: int = 0                                # of `parked`: QueueFull (transient: a retry may pass)
    skipped: list[str] = field(default_factory=list)          # rows that are not this adapter's DTOs
    unversioned: list[str] = field(default_factory=list)      # no broker_seq to inherit / no broker time
    reread_orders: list[str] = field(default_factory=list)
    reread_positions: list[str] = field(default_factory=list)
    rechecked_orders: list[OrderDTO] = field(default_factory=list)       # by-id answers that were published
    rechecked_positions: list[PositionDTO] = field(default_factory=list)
    not_found: list[str] = field(default_factory=list)        # 'order <id>' / 'position <id>': broker says none
    reread_errors: list[str] = field(default_factory=list)    # by-id reads that failed (type names only)
    unmapped: int = 0                                         # by-id re-read rows the adapter could not map
    failed_streams: frozenset[str] = frozenset()              # rows the consumers could not commit
    committed: bool = False                                   # every published row settled and committed
    timed_out: bool = False


class _LRU(OrderedDict[str, _S]):
    def __init__(self, cap: int) -> None:
        super().__init__()
        self.cap = cap

    def put(self, key: str, value: _S) -> None:
        self[key] = value
        self.move_to_end(key)
        while len(self) > self.cap:
            self.popitem(last=False)


class AdapterRuntime:
    """Mutable per-adapter state owned by the orchestrator."""

    def __init__(self, adapter: BrokerAdapter, conn: ConnectionStateMachine, max_entities: int) -> None:
        self.adapter = adapter
        self.adapter_id = adapter.adapter_id
        self.conn = conn
        self.subs: dict[StreamKind, SubscriptionState] = {s: SubscriptionState() for s in STREAMS}
        self.watermark_ms: int | None = None       # newest broker time seen on a broker-time stream row
        self.orders: _LRU[OrderDTO] = _LRU(max_entities)
        self.positions: _LRU[PositionDTO] = _LRU(max_entities)
        self.accounts: dict[str, AccountDTO] = {}
        self.broker_seq: dict[str, int] = {}
        self.instruments: dict[str, InstrumentSpec] = {}
        self.snapshots_done = 0
        self.local_loaded = False
        self.resync_event = asyncio.Event()        # set when a RESYNCING is needed
        self.wake = asyncio.Event()                # driver wake-up (any control signal)
        self.auth_lost = False
        self.socket_lost = False
        self.rearm = asyncio.Event()
        self.last_inbound_ms = 0
        self.tasks: dict[PumpName, asyncio.Task[None]] = {}
        self.tick_symbols: set[str] = set()
        self.pending_symbols: set[str] = set()
        self.last_ensure_ms = 0
        self.resubscribe_failures = 0
        self.resync_deadline_ms = 0
        self.dropped_prices = 0
        self.dropped_live = 0
        self.failed_generation: str | None = None   # credential generation recorded AUTH_FAILED
        self.unsaved_instruments: list[InstrumentSpec] = []
        self.instrument_save_failed_ms = 0
        self.backoff_reset = False                  # reset_backoff(): end the BACKOFF wait now
        self.priced_symbols: set[str] = set()      # symbols ticked since the prices stream (re)subscribed
        self.last_tick_ms: dict[str, int] = {}     # symbol -> local receipt time of its newest tick
        self.tick_wanted_ms: dict[str, int] = {}   # symbol -> when its tick subscription was (re)requested
        self.live_ccys: set[str] = set()            # account currencies seen on AccountLive rows
        self.cross_cache: dict[tuple[str, str], tuple[str, ...]] = {}   # (pnl_ccy, account ccy) -> legs
        self.last_account_poll_ms = 0               # last REST account read (poll or snapshot pass)
        self.account_poll_failures = 0
        self.open_resync_due_ms: int | None = None  # SM10 open: REST-only RESYNCING due at this time
        #: A later open that the pending pass does not cover (its due time); promoted once that pass is sent.
        self.open_resync_next_ms: int | None = None
        self.clean_wanted_ms: dict[str, int] = {}   # symbol queued for ensure_ticks -> grace start (no clean yet)
        self.last_spread: dict[str, Decimal | None] = {}   # watched symbol -> spread of its newest (unclean) tick
        self.unmapped_seen: dict[StreamKind, int] = {}     # adapter `unmapped_rows` counters already acted on
        self.driver_errors = 0                      # consecutive non-fence SM4 driver errors
        self.last_driver_error_ms = 0               # local time of the newest driver error
        self.lost_logged_ms: dict[str, int] = {}    # stream -> last full "frame lost" log (throttled)
        #: Why the newest fetch_cash_flows result was partial (empty = complete or none yet): pnl_source() is
        #: off while set, cleared by a complete fetch since the current roll.
        self.cash_flows_partial: tuple[str, ...] = ()
        #: Downtime catch-up (src/runtime/catchup.py): while any hold is set, no reconcile clears this adapter's
        #: stream_health fence (`hold_fence` / `release_fence`).
        self.fence_holds: set[str] = set()
        self.passes_committed = 0                   # SNAPSHOTTING / RESYNCING passes whose rows all settled
        self.last_committed_send_seq = 0            # send_seq of the newest such pass
        self.last_seen_orders: frozenset[str] = frozenset()     # open orders listed by the newest pass
        self.last_seen_positions: frozenset[str] = frozenset()  # positions listed by the newest pass
        #: HLC of the newest interruption of the orders / account path (a gap opened, the socket or the auth lost):
        #: orders placed and finished while it lasted are in no reconcile snapshot, so the downtime catch-up
        #: re-reads the broker history after it (src/runtime/catchup.py). A fence hold is not an interruption.
        self.last_interruption_seq = 0

    def decl(self, entity: EntityType) -> VersionDecl:
        return self.adapter.VERSIONING.get(entity, _NO_DECL)

    @property
    def all_subscribed(self) -> bool:
        return all(s.subscribed for s in self.subs.values())


_FILL_STATUSES = frozenset({OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED})


def _order_executed(prev: OrderDTO, new: OrderDTO) -> bool:
    """The newer row shows fill progress: it enters a fill status, or its remaining units moved while
    in one. Amends (stop / take-profit) and cancels are not executions."""
    if new.status not in _FILL_STATUSES:
        return False
    return prev.status != new.status or prev.remaining_units != new.remaining_units


def _position_executed(prev: PositionDTO, new: PositionDTO) -> bool:
    """A position's units changed or it closed (an opening / reducing / closing deal). With the instrument
    unknown the unit fields are null and `raw_qty` carries the broker quantity, so that is compared too."""
    if prev.status == "OPEN" and new.status == "CLOSED":
        return True
    return new.status == "OPEN" and (prev.units != new.units or prev.raw_qty != new.raw_qty)


def _entity_type(dto: StatefulDTO) -> EntityType:
    if isinstance(dto, OrderDTO):
        return "ORDER"
    if isinstance(dto, PositionDTO):
        return "POSITION"
    return "ACCOUNT"


class IngestionOrchestrator:
    """The single publisher (dto-contracts.md §6)."""

    def __init__(self, bus: EventBus, spooler: DlqSpooler, health: StreamHealthSink,
                 hlc: HybridLogicalClock, *, clock_ms: Callable[[], int] = wall_clock_ms,
                 roll_ms: Callable[[int], int] = last_roll_ms, local_state: LocalStateReader | None = None,
                 tick_symbols: Mapping[str, Sequence[str]] | None = None,
                 on_resync_needed: Callable[[str], None] | None = None,
                 require_client_order_id: bool = False, max_entities: int = 50_000,
                 subscribe_timeout_s: float = 10.0, resync_deadline_ms: int = 120_000,
                 max_resubscribe_failures: int = 5, resync_retry_s: float = 1.0,
                 jitter: Callable[[], float] | None = None, auth_store: AdapterAuthStore | None = None,
                 credential_generation: CredentialGeneration | None = None,
                 instrument_sink: InstrumentSink | None = None, auth_poll_s: float = 30.0,
                 on_price_tick: Callable[[PriceDTO, bool], None] | None = None,
                 on_execution: Callable[[str, str, int], None] | None = None,
                 pager: Pager | None = None,
                 market_open: Callable[[str], bool] | None = None,
                 price_staleness_suppressed: Callable[[str, str], bool] | None = None,
                 stale_price_ms: int = STALE_PRICE_MS, conn_absent_alert_ms: int = CONN_ABSENT_ALERT_MS,
                 account_poll_ms: int = ACCOUNT_POLL_MS, account_poll_timeout_s: float = 10.0,
                 open_resync_jitter: Callable[[], float] = random.random,
                 transfer_sources: Mapping[str, bool] | None = None,
                 refresh_adapter: AdapterRefresher | None = None,
                 park_page_interval_ms: int = PARK_PAGE_INTERVAL_MS,
                 clean_spread: CleanSpread | None = None, clean_mid_ms: int = CLEAN_MID_MS) -> None:
        self.bus = bus
        self.spooler = spooler
        self.health = HealthMirror(health)
        self.hlc = hlc
        self._clock_ms = clock_ms
        self._roll_ms = roll_ms
        self._local_state = local_state
        self._tick_cfg = {k: tuple(v) for k, v in (tick_symbols or {}).items()}
        self._on_resync_needed = on_resync_needed
        self._require_client_order_id = require_client_order_id
        self._max_entities = max_entities
        self._subscribe_timeout_s = subscribe_timeout_s
        self._resync_deadline_ms = resync_deadline_ms
        self._max_resubscribe_failures = max_resubscribe_failures
        self._resync_retry_s = resync_retry_s
        self._jitter = jitter
        self._auth_store = auth_store
        self._credential_generation = credential_generation
        self._instrument_sink = instrument_sink
        self._auth_poll_s = auth_poll_s
        self._on_price_tick = on_price_tick
        self._on_execution = on_execution
        self._pager = pager
        self._market_open = market_open
        self._price_muted = price_staleness_suppressed
        self._stale_price_ms = stale_price_ms
        self._conn_absent_alert_ms = conn_absent_alert_ms
        self._account_poll_ms = account_poll_ms
        self._account_poll_timeout_s = account_poll_timeout_s
        self._open_jitter = open_resync_jitter
        self._transfer_sources = dict(transfer_sources or {})
        self._refresh_adapter = refresh_adapter
        self._park_pages = PageThrottle(pager, clock_ms, park_page_interval_ms)
        self._driver_pages = PageThrottle(pager, clock_ms, DRIVER_ERROR_PAGE_INTERVAL_MS)
        self._clean_spread = clean_spread
        self._clean_mid_ms = clean_mid_ms
        self._episodes = Episodes(pager)
        self._runtimes: dict[str, AdapterRuntime] = {}
        self._account_owner: dict[str, str] = {}
        self._cash_flow_requested_ms: dict[str, int] = {}
        self.backpressure_overflow = False
        self.parked: dict[str, int] = {}
        self.frames_lost: dict[str, int] = {}    # per stream: frames / skipped rows that opened a gap (or prices)
        # Reconcile passes clear their gaps only after their rows commit (src/pubsub/barrier.py). A bus
        # without a barrier (a test double) keeps the old behaviour: clear when the pass is published.
        self._commits: CommitBarrier | None = getattr(bus, "commit_barrier", None)
        self._capture: list[tuple[int, StreamKind]] | None = None   # rows published by the current REST row
        if self._commits is not None:
            self._commits.add_refusal_listener(self.on_store_refused)

    # ================================================================== registry
    def add_adapter(self, adapter: BrokerAdapter) -> AdapterRuntime:
        if adapter.adapter_id in self._runtimes:
            raise ValueError(f"adapter {adapter.adapter_id} already registered")
        conn = ConnectionStateMachine(adapter.adapter_id) if self._jitter is None             else ConnectionStateMachine(adapter.adapter_id, jitter=self._jitter)
        rt = AdapterRuntime(adapter, conn, self._max_entities)
        rt.unmapped_seen = self._unmapped_counts(adapter)      # only rows skipped from now on count
        now = self._clock_ms()
        for sub in rt.subs.values():
            sub.key_until_ms = now          # LEADER_INIT flushed Redis: every conn: key starts absent
        self._runtimes[adapter.adapter_id] = rt
        return rt

    def adapter(self, adapter_id: str) -> BrokerAdapter:
        """The adapter object currently driven for this id (replaced on credential rotation)."""
        return self._runtimes[adapter_id].adapter

    def transfer_source(self, adapter_id: str) -> bool:
        """Whether the adapter reports deposits / withdrawals (`SnapshotInputs.transfer_source`,
        tier-persistence.md §4). The constructor's `transfer_sources` wins, then the adapter's
        `HAS_TRANSFER_SOURCE` attribute. An adapter that declares neither has no known transfer source: its
        accounts' days are marked transfers_unverified (a FLAG), never silently trusted as 0."""
        if adapter_id in self._transfer_sources:
            return self._transfer_sources[adapter_id]
        rt = self._runtimes.get(adapter_id)
        return rt is not None and getattr(rt.adapter, "HAS_TRANSFER_SOURCE", None) is True

    def runtime(self, adapter_id: str) -> AdapterRuntime:
        return self._runtimes[adapter_id]

    def resync_event(self, adapter_id: str) -> asyncio.Event:
        return self._runtimes[adapter_id].resync_event

    def rearm(self, adapter_id: str) -> None:
        """Credential generation changed or an operator re-armed the adapter (AUTH_FAILED exit)."""
        rt = self._runtimes[adapter_id]
        rt.rearm.set()
        rt.wake.set()

    async def drain_health(self) -> bool:
        return await self.health.drain()

    # ================================================================== stream items (sync parser path)
    def handle_stream_item(self, adapter_id: str, stream: StreamKind,
                           item: OrderDTO | PositionDTO | AccountDTO | AccountLive | CashFlowDTO | PriceDTO
                           | StreamEvent) -> None:
        """Handle one stream frame. Never awaits: the socket is always drained."""
        rt = self._runtimes[adapter_id]
        now = self._clock_ms()
        rt.last_inbound_ms = now
        self._check_backpressure()
        try:
            if isinstance(item, StreamEvent):
                self._on_control(rt, item)
            elif isinstance(item, PriceDTO):
                self._on_price(rt, item)
            elif isinstance(item, AccountLive):
                self._on_live(rt, item)
            elif isinstance(item, CashFlowDTO):
                self._on_cash_flow(rt, item, "account")
            elif isinstance(item, (OrderDTO, PositionDTO, AccountDTO)):
                observed = self.hlc.next()
                self._accept_entity(rt, item, "STREAM", observed_seq=observed, local_ms=now,
                                    watermark=None, stream=stream)
            else:
                self._frame_lost(rt, stream, f"unexpected stream item {type(item).__name__}")
        except Exception as exc:
            # A frame whose handling fails is lost like a dropped frame (dto-contracts.md §4): never silently.
            self._frame_lost(rt, stream, f"{type(item).__name__} not handled: {exc!r}", exc)

    def _frame_lost(self, rt: AdapterRuntime, stream: StreamKind, why: str,
                    exc: BaseException | None = None) -> None:
        """A frame (or a broker row the adapter skipped) could not be handed over. On the order / account
        streams it opens that stream's gap (fail closed: stream_health.conn_lost_at, RESYNCING), and no REST
        pass sent before now can clear it; a lost price frame is only counted (latest tick wins). Paged,
        throttled per (adapter, stream); the full log line (with traceback) is throttled the same way, so a
        failure on every tick never floods the log or lags the event loop (`frames_lost` keeps the count)."""
        self.frames_lost[stream] = self.frames_lost.get(stream, 0) + 1
        now = self._clock_ms()
        last = rt.lost_logged_ms.get(stream)
        if last is None or now - last >= FRAME_LOST_LOG_INTERVAL_MS:
            rt.lost_logged_ms[stream] = now
            log.error("%s:%s frame lost (%d so far on this stream): %s", rt.adapter_id, stream,
                      self.frames_lost[stream], why, exc_info=exc)
        self._park_pages.page(f"frame_lost:{rt.adapter_id}:{stream}", HIGH,
                              f"ingestion frame lost: {rt.adapter_id}:{stream}", redact_urls(why)[:300])
        if stream == "prices":
            rt.dropped_prices += 1
            return
        rt.subs[stream].dropped_seq = self.hlc.next()
        self._open_gap(rt, stream, why)

    @staticmethod
    def _unmapped_counts(adapter: BrokerAdapter) -> dict[StreamKind, int]:
        """The adapter's `unmapped_rows` counters (see UNMAPPED_ATTR), normalised to stream kinds."""
        raw = getattr(adapter, UNMAPPED_ATTR, None)
        out: dict[StreamKind, int] = {}
        if isinstance(raw, bool) or raw is None:
            return out
        if isinstance(raw, int):
            return {"orders": raw, "account": raw}
        if isinstance(raw, Mapping):
            for key, n in raw.items():
                stream: StreamKind | None = ("orders" if key in ("orders", "positions")
                                             else "account" if key == "account"
                                             else "prices" if key == "prices" else None)
                if stream is not None and isinstance(n, int) and not isinstance(n, bool):
                    out[stream] = out.get(stream, 0) + n
        return out

    def check_unmapped(self, adapter_id: str) -> set[StreamKind]:
        """Open the gap of every stream whose adapter `unmapped_rows` counter grew since the last check (a
        broker row the adapter could not map and skipped, stream or REST). Returns those streams."""
        rt = self._runtimes[adapter_id]
        try:
            now_counts = self._unmapped_counts(rt.adapter)
        except Exception:
            log.exception("%s: unmapped_rows unreadable", adapter_id)
            return set()
        grew: set[StreamKind] = set()
        for stream, n in now_counts.items():
            seen = rt.unmapped_seen.get(stream, 0)
            if n > seen:
                grew.add(stream)
            rt.unmapped_seen[stream] = n
        for stream in sorted(grew):
            self._frame_lost(rt, stream, f"{now_counts[stream]} broker rows the adapter could not map "
                                         "(skipped by the adapter)")
        return grew

    # ------------------------------------------------------------------ control frames
    def _on_control(self, rt: AdapterRuntime, ev: StreamEvent) -> None:
        sub = rt.subs[ev.stream]
        kind = ev.kind
        if kind is StreamEventKind.SUBSCRIBED:
            if ev.stream == "prices":
                rt.priced_symbols.clear()       # the next tick per symbol is the subscription snapshot
            sub.subscribed = True
            sub.subscribed_seq = self.hlc.next()
            rt.wake.set()
        elif kind is StreamEventKind.HEARTBEAT:
            if sub.subscribed and not sub.lost:
                self._health(rt, "heartbeat", ev.stream)
        elif kind in (StreamEventKind.UNSUBSCRIBED, StreamEventKind.ERROR):
            sub.subscribed = False
            self._open_gap(rt, ev.stream, f"{kind.value}: {ev.detail}")
        elif kind is StreamEventKind.GAP:
            sub.dropped_seq = self.hlc.next()
            self._open_gap(rt, ev.stream, f"GAP: {ev.detail}")
        elif kind is StreamEventKind.AUTH_LOST:
            self._lose_all(rt, "auth lost")
            rt.auth_lost = True
            rt.wake.set()
        elif kind is StreamEventKind.DISCONNECTED:
            self._lose_all(rt, "socket lost")
            rt.socket_lost = True
            rt.wake.set()

    def _open_gap(self, rt: AdapterRuntime, stream: StreamKind, why: str) -> None:
        sub = rt.subs[stream]
        if stream != "prices":
            rt.last_interruption_seq = self.hlc.next()
        if not sub.lost:
            sub.lost = True
            self._health(rt, "lost", stream)
            log.warning("%s:%s gap opened (%s)", rt.adapter_id, stream, why)
        self._request_resync(rt)

    def _lose_all(self, rt: AdapterRuntime, why: str) -> None:
        """Socket closed / lost or auth lost: every subscription on it is no longer SUBSCRIBED."""
        rt.priced_symbols.clear()
        rt.last_interruption_seq = self.hlc.next()
        for stream, sub in rt.subs.items():
            sub.subscribed = False
            if not sub.lost:
                sub.lost = True
                self._health(rt, "lost", stream)
        log.warning("%s: all subscriptions lost (%s)", rt.adapter_id, why)

    def _health(self, rt: AdapterRuntime, op: HealthOp, stream: StreamKind) -> None:
        """Enqueue a health op and track whether the stream's conn: key exists (heartbeat / reconcile write
        it with a 30 s PEXPIRE, a loss deletes it)."""
        sub = rt.subs[stream]
        now = self._clock_ms()
        if op == "lost":
            sub.key_until_ms = min(sub.key_until_ms, now)
        else:
            sub.key_until_ms = now + CONN_KEY_TTL_MS
        self.health.enqueue(op, rt.adapter_id, stream)

    def _request_resync(self, rt: AdapterRuntime) -> None:
        if not rt.resync_event.is_set():
            rt.resync_event.set()
            if self._on_resync_needed is not None:
                self._on_resync_needed(rt.adapter_id)
        rt.wake.set()

    # ------------------------------------------------------------------ backpressure
    def _check_backpressure(self) -> None:
        if self.backpressure_overflow and self.spooler.fill_ratio < RING_RESYNC_THRESHOLD:
            self.backpressure_overflow = False
            log.warning("backpressure_overflow cleared (DLQ ring below 50 %)")

    def _overflow(self, rt: AdapterRuntime, stream: StreamKind) -> None:
        if not self.backpressure_overflow:
            log.error("backpressure_overflow: topic and DLQ ring both full")
        self.backpressure_overflow = True
        rt.subs[stream].dropped_seq = self.hlc.next()
        self._open_gap(rt, stream, "backpressure_overflow")

    def _park(self, rt: AdapterRuntime, dto: Parkable, reason: str, stream: StreamKind, detail: str = "") -> None:
        """Park one row in the DLQ ring (no I/O). A violation (anything but QueueFull) is also paged,
        throttled per (adapter, reason). A row that cannot even be serialised, or a full ring, opens a gap."""
        self.parked[reason] = self.parked.get(reason, 0) + 1
        if reason in PAGED_PARK_REASONS:
            ref = getattr(dto, "entity_key", None) or getattr(dto, "transaction_id", "?")
            self._park_pages.page(f"park:{rt.adapter_id}:{reason}", HIGH, f"ingestion {reason}: {rt.adapter_id}",
                                  f"{type(dto).__name__} {ref} parked in events_dlq"
                                  + (f" ({detail})" if detail else ""))
        try:
            row = dlq_row(dto, reason)
        except Exception:
            log.exception("%s: %s row could not be serialised for the DLQ", rt.adapter_id, reason)
            # Lost like a dropped frame: open that stream's gap (fail closed). Not backpressure: the ring
            # has room, so price frames keep flowing.
            rt.subs[stream].dropped_seq = self.hlc.next()
            self._open_gap(rt, stream, f"{reason} row not serialisable")
            return
        if row.adapter_id != rt.adapter_id:          # the DLQ worker re-reads from the adapter that sent it
            row = replace(row, adapter_id=rt.adapter_id)
        if not self.spooler.offer(row):
            self._overflow(rt, stream)

    def _invalid(self, rt: AdapterRuntime, dto: Parkable) -> tuple[str, str] | None:
        """(reason, detail) when the row fails the schema or checksum check (dto-contracts.md §4)."""
        try:
            problem = schema_problem(dto, rt.adapter_id)
        except Exception as exc:      # a malformed object must never break the parser path
            problem = f"unreadable: {exc!r}"
        if problem is not None:
            return SCHEMA_INVALID, problem
        if not checksum_valid(dto):
            return CHECKSUM, "checksum does not match the payload"
        return None

    # ------------------------------------------------------------------ entities
    def _label(self, rt: AdapterRuntime, dto: StatefulDTO, source: Source, observed_seq: int, local_ms: int,
               watermark: int | None) -> StatefulDTO:
        """Version labelling per the adapter's declaration (dto-contracts.md §2), then seq_id."""
        decl = rt.decl(_entity_type(dto))
        t = dto.broker_updated_at_ms
        if not decl.has_broker_time:
            t = local_ms                                   # receipt time (stream) / send time (REST)
        elif t == 0 and source == "REST":
            t = watermark if watermark is not None else 0  # never a local clock in the broker-time slot
        bseq = dto.broker_seq
        if decl.has_broker_seq and bseq is None:
            bseq = rt.broker_seq.get(dto.entity_key)
        return replace(dto, broker_updated_at_ms=t, broker_seq=bseq, source=source,
                       observed_seq=observed_seq, seq_id=self.hlc.next())

    def _accept_entity(self, rt: AdapterRuntime, dto: StatefulDTO, source: Source, *, observed_seq: int,
                       local_ms: int, watermark: int | None, stream: StreamKind, history: bool = False) -> bool:
        """Validate, label and publish one stateful DTO. True if it reached the bus. `history`: a downtime
        catch-up row; a finished order / CLOSED position from it subscribes no price (it needs none)."""
        bad = self._invalid(rt, dto)
        if bad is not None:
            log.error("%s: %s %s parked: %s", rt.adapter_id, bad[0], type(dto).__name__, bad[1])
            self._park(rt, dto, bad[0], stream, bad[1])     # the raw row, as received
            return False
        decl = rt.decl(_entity_type(dto))
        labelled: StatefulDTO = self._label(rt, dto, source, observed_seq, local_ms, watermark)
        if decl.has_broker_seq and labelled.broker_seq is None:
            # Nothing to inherit: a null broker_seq sorts below every stored revision, so the store would drop a
            # real change as stale. Lost like a dropped frame instead: its stream's gap opens (fail closed).
            self._frame_lost(rt, stream, f"{type(dto).__name__} {dto.entity_key} has no broker_seq and none to "
                                         "inherit")
            return False
        received = self._clock_ms()
        if decl.has_broker_time and dto.broker_updated_at_ms and is_future_timestamp(
                dto.broker_updated_at_ms, received):
            ahead = dto.broker_updated_at_ms - received
            log.error("%s: future_timestamp %s (%d ms ahead)", rt.adapter_id, labelled.entity_key, ahead)
            self._park(rt, labelled, FUTURE_TIMESTAMP, stream, f"{ahead} ms ahead of receipt")
            return False
        if source == "STREAM" and decl.has_broker_time and dto.broker_updated_at_ms:
            rt.watermark_ms = max(rt.watermark_ms or 0, dto.broker_updated_at_ms)

        if isinstance(labelled, OrderDTO):
            if self._require_client_order_id and not labelled.client_order_id:
                self._park(rt, labelled, MISSING_CLIENT_ORDER_ID, stream)
                return False
            prev_o = rt.orders.get(labelled.entity_key)
            newer = prev_o is None or decide(prev_o, labelled) in (Decision.APPLY, Decision.NOOP_ADVANCE)
            if newer and prev_o is not None and not is_legal_transition(prev_o.status, labelled.status):
                self._park(rt, labelled, ILLEGAL_TRANSITION, stream,
                           f"{prev_o.status.value} -> {labelled.status.value}")
                return False
            if newer and source == "STREAM" and prev_o is not None and _order_executed(prev_o, labelled):
                self._execution_evidence(rt, labelled.symbol, local_ms)
            if not self._publish(rt, labelled, stream):
                return False
            if newer:
                rt.orders.put(labelled.entity_key, labelled)
                if not (history and labelled.status.terminal):
                    self._want_symbol(rt, labelled.symbol)
        elif isinstance(labelled, PositionDTO):
            prev_p = rt.positions.get(labelled.entity_key)
            newer = prev_p is None or decide(prev_p, labelled) in (Decision.APPLY, Decision.NOOP_ADVANCE)
            if newer and prev_p is not None and prev_p.status == "CLOSED" and labelled.status == "OPEN":
                self._park(rt, labelled, ILLEGAL_TRANSITION, stream, "CLOSED -> OPEN")
                return False
            if newer and source == "STREAM" and prev_p is not None and _position_executed(prev_p, labelled):
                self._execution_evidence(rt, labelled.symbol, local_ms)
            if not self._publish(rt, labelled, stream):
                return False
            if newer:
                rt.positions.put(labelled.entity_key, labelled)
                if not (history and labelled.status != "OPEN"):
                    self._want_symbol(rt, labelled.symbol)
        else:
            prev_a = rt.accounts.get(labelled.account_id)
            newer = prev_a is None or decide(prev_a, labelled) in (Decision.APPLY, Decision.NOOP_ADVANCE)
            if not self._publish(rt, labelled, stream):
                return False
            self._account_owner[labelled.account_id] = rt.adapter_id
            if newer:
                new_ccy = labelled.currency not in self._account_ccys(rt)
                rt.accounts[labelled.account_id] = labelled
                if new_ccy:
                    self._refresh_crosses(rt)    # held symbols may need a pnl_ccy -> this currency cross
        if newer and labelled.broker_seq is not None:
            rt.broker_seq[labelled.entity_key] = labelled.broker_seq
        return True

    def _publish(self, rt: AdapterRuntime, dto: StatefulDTO, stream: StreamKind) -> bool:
        try:
            if isinstance(dto, OrderDTO):
                self.bus.publish_order_nowait(dto)
            elif isinstance(dto, PositionDTO):
                self.bus.publish_position_nowait(dto)
            else:
                self.bus.publish_account_nowait(dto)
        except asyncio.QueueFull:
            self._park(rt, dto, QUEUE_FULL, stream)
            return False
        if self._capture is not None:
            self._capture.append((dto.seq_id, stream))
        return True

    def _on_cash_flow(self, rt: AdapterRuntime, cf: CashFlowDTO, stream: StreamKind) -> bool:
        bad = self._invalid(rt, cf)
        if bad is not None:
            log.error("%s: %s CashFlowDTO parked: %s", rt.adapter_id, bad[0], bad[1])
            self._park(rt, cf, bad[0], stream, bad[1])
            return False
        stamped = replace(cf, seq_id=self.hlc.next())
        self._account_owner.setdefault(cf.account_id, rt.adapter_id)
        try:
            self.bus.publish_account_nowait(stamped)
        except asyncio.QueueFull:
            self._park(rt, stamped, QUEUE_FULL, stream)
            return False
        if self._capture is not None:
            self._capture.append((stamped.seq_id, stream))
        return True

    def _on_live(self, rt: AdapterRuntime, live: AccountLive) -> None:
        """Unversioned, latest wins: a full topic drops it (the next sample supersedes it)."""
        self._account_owner.setdefault(live.account_id, rt.adapter_id)
        if live.currency and live.currency not in self._account_ccys(rt):
            rt.live_ccys.add(live.currency)
            self._refresh_crosses(rt)
        try:
            self.bus.publish_account_nowait(live)
        except asyncio.QueueFull:
            rt.dropped_live += 1

    def _on_price(self, rt: AdapterRuntime, tick: PriceDTO) -> None:
        rt.last_tick_ms[tick.symbol] = rt.last_inbound_ms     # receipt time (stale-price page)
        if rt.clean_wanted_ms and tick.symbol in rt.clean_wanted_ms:
            self._clean_evidence(rt, tick)
        self._price_evidence(rt, tick)
        if self.backpressure_overflow:
            rt.dropped_prices += 1           # drop price frames at the parser
            return
        if self.bus.publish_price_nowait(replace(tick, seq_id=self.hlc.next())) is PricePut.DROPPED:
            rt.dropped_prices += 1

    def _clean_threshold(self, adapter_id: str, symbol: str) -> Decimal | None:
        if self._clean_spread is None:
            return DEFAULT_CLEAN_SPREAD_PIPS
        try:
            return self._clean_spread(adapter_id, symbol)
        except Exception:
            log.exception("%s: clean_spread hook failed for %s", adapter_id, symbol)
            return DEFAULT_CLEAN_SPREAD_PIPS

    def _clean_evidence(self, rt: AdapterRuntime, tick: PriceDTO) -> None:
        """A watched symbol's tick: a clean quote (spread known and <= the clean threshold; the same rule as
        the Redis clean_mid write) ends the watch; an unclean one is remembered for the page."""
        limit = self._clean_threshold(rt.adapter_id, tick.symbol)
        if tick.spread_pips is not None and limit is not None and tick.spread_pips <= limit:
            rt.clean_wanted_ms.pop(tick.symbol, None)
            rt.last_spread.pop(tick.symbol, None)
            self._episodes.clear(f"no_clean_mid:{rt.adapter_id}:{tick.symbol}")
        else:
            rt.last_spread[tick.symbol] = tick.spread_pips

    def _watch_clean(self, rt: AdapterRuntime, symbols: Iterable[str]) -> None:
        """Start the clean-mid grace for symbols newly queued for ensure_ticks() (adapter-isolation.md §5)."""
        now = self._clock_ms()
        for sym in symbols:
            rt.clean_wanted_ms.setdefault(sym, now)

    def _price_evidence(self, rt: AdapterRuntime, tick: PriceDTO) -> None:
        """SM10 open-market evidence (operations.md §10): every tick goes to `on_price_tick`, flagged as a
        subscription snapshot item when it is the symbol's first tick since the prices stream (re)subscribed
        (those never count as evidence). Pure, no I/O; a hook error never touches the socket path."""
        if self._on_price_tick is None:
            return
        snapshot = tick.symbol not in rt.priced_symbols
        rt.priced_symbols.add(tick.symbol)
        try:
            self._on_price_tick(tick, snapshot)
        except Exception:
            log.exception("%s: price evidence hook failed", rt.adapter_id)

    def _execution_evidence(self, rt: AdapterRuntime, symbol: str, ts_ms: int) -> None:
        """SM10 execution evidence (operations.md §10): an order / position stream row that shows a fill
        or execution goes to `on_execution(adapter_id, symbol, ts_ms)`. Synchronous, never awaited; a hook
        error never touches the socket path. Only stream rows that change a known row count: REST rows and
        an entity's first row (which may be a subscription snapshot / replay item) never do."""
        if self._on_execution is None:
            return
        try:
            self._on_execution(rt.adapter_id, symbol, ts_ms)
        except Exception:
            log.exception("%s: execution evidence hook failed", rt.adapter_id)

    # ================================================================== snapshot / reconcile
    async def run_snapshot(self, adapter_id: str, *, refresh_instruments: bool = True) -> SnapshotReport:
        """SNAPSHOTTING / RESYNCING REST pass with watermark reconciliation (adapter-isolation.md §5).

        Never closes an entity because it is absent: absent ones are re-read by id.
        Broker / REST errors propagate to the SM4 driver."""
        rt = self._runtimes[adapter_id]
        ad = rt.adapter
        await self._load_local(rt)
        first = rt.snapshots_done == 0
        # Rows the adapter skipped before this pass is sent: their gap opens before send_seq, so this pass (which
        # reads the broker after them) may clear it, and no further RESYNCING is needed for them. Rows skipped
        # during the pass are caught after it.
        resync_wanted = rt.resync_event.is_set()
        if self.check_unmapped(adapter_id) and not resync_wanted:
            rt.resync_event.clear()
        watermark = rt.watermark_ms                       # recorded when the REST calls are sent
        send_ms = self._clock_ms()
        send_seq = self.hlc.next()
        rep = SnapshotReport(adapter_id=adapter_id, first=first, send_seq=send_seq, send_ms=send_ms,
                             watermark_ms=watermark)
        parked_before = sum(self.parked.values())

        if refresh_instruments:
            await self._refresh_instruments(rt)
        open_orders = await ad.fetch_open_orders()
        positions = await ad.fetch_positions()
        accounts, lives = await ad.fetch_account_snapshot()
        flows_since = self._roll_ms(send_ms)
        flows = await ad.fetch_cash_flows(flows_since)
        self.note_cash_flows(adapter_id, flows, flows_since)

        published: list[tuple[int, StreamKind]] = []     # (seq_id, stream) of every row handed over
        parked_streams: set[StreamKind] = set()            # streams with a row parked (any reason)
        # Opened before the first publish: the consumers commit rows while this pass still awaits its
        # by-id re-reads, so each row is registered right after its put_nowait (no await in between).
        commits = self._commits
        bpass = commits.open(adapter_id) if commits is not None else None

        def tracked(stream: StreamKind, call: Callable[[], bool]) -> bool:
            """Run one synchronous accept/publish, recording what it published and any park."""
            before = sum(self.parked.values())
            n0 = len(published)
            self._capture = published
            try:
                ok = call()
            finally:
                self._capture = None
            if commits is not None and bpass is not None:
                for seq, s in published[n0:]:
                    commits.add(bpass, seq, s)
            if sum(self.parked.values()) > before:
                parked_streams.add(stream)
            return ok

        def rest(dto: StatefulDTO, stream: StreamKind, seq: int = send_seq, local: int = send_ms,
                 wm: int | None = watermark) -> bool:
            return tracked(stream, lambda: self._accept_entity(rt, dto, "REST", observed_seq=seq, local_ms=local,
                                                               watermark=wm, stream=stream))

        rep.orders = sum(rest(o, "orders") for o in open_orders)
        rep.positions = sum(rest(p, "orders") for p in positions)
        rep.accounts = sum(rest(a, "account") for a in accounts)
        for live in lives:
            self._on_live(rt, live)
        for cf in flows:
            rep.cash_flows += tracked("account", functools.partial(self._on_cash_flow, rt, cf, "account"))

        # --- absent entities: re-read by id, never close on absence
        seen_o = {o.entity_key for o in open_orders}
        seen_p = {p.entity_key for p in positions}
        rep.seen_orders, rep.seen_positions = frozenset(seen_o), frozenset(seen_p)
        rt.last_seen_orders, rt.last_seen_positions = rep.seen_orders, rep.seen_positions
        for key, stored_o in list(rt.orders.items()):
            if stored_o.status.terminal or key in seen_o:
                continue
            if not (first or self._older_than_watermark(rt, stored_o, watermark, send_ms)):
                continue
            o_seq, o_ms, o_wm = self.hlc.next(), self._clock_ms(), rt.watermark_ms
            fresh_o = await ad.fetch_order_status(stored_o.broker_order_id)
            rep.reread_orders.append(stored_o.broker_order_id)
            if fresh_o is not None:
                rest(fresh_o, "orders", o_seq, o_ms, o_wm)
        for key, stored_p in list(rt.positions.items()):
            if stored_p.status != "OPEN" or key in seen_p:
                continue
            if not (first or self._older_than_watermark(rt, stored_p, watermark, send_ms)):
                continue
            p_seq, p_ms, p_wm = self.hlc.next(), self._clock_ms(), rt.watermark_ms
            fresh_p = await ad.fetch_position_status(stored_p.position_id)
            rep.reread_positions.append(stored_p.position_id)
            if fresh_p is not None:
                rest(fresh_p, "orders", p_seq, p_ms, p_wm)

        # A broker row the adapter could not map and skipped (REST or stream) is absent from this pass: its
        # stream's gap opens after send_seq, so this pass cannot clear it (fail closed, dto-contracts.md §4).
        self.check_unmapped(adapter_id)
        rt.snapshots_done += 1
        rt.last_account_poll_ms = max(rt.last_account_poll_ms, send_ms)    # the pass proved the account path
        self._open_resync_sent(rt, send_ms)
        rep.parked = sum(self.parked.values()) - parked_before
        # A REST row of the pass parked for ANY reason (QueueFull, illegal_transition,
        # missing_client_order_id, future_timestamp) never reached the store: like a dropped frame, that
        # stream's gap stays open and RESYNCING retries (fail closed; IMPLEMENTATION_PLAN 4.9 decision).
        if parked_streams:
            self._keep_gap(rt, parked_streams, "reconcile rows parked")
        rep.dropped_after_watermark = any(s.dropped_seq > send_seq for s in rt.subs.values())
        rep.all_subscribed = rt.all_subscribed
        self._reconcile_after_commit(rt, send_seq, bpass)
        return rep

    async def _load_local(self, rt: AdapterRuntime) -> None:
        """Seed the in-memory entity maps from the locally stored non-terminal orders / OPEN positions once,
        before the first pump starts, so a stream row that arrives ahead of the first REST pass is still
        checked for an illegal transition against the stored row."""
        if not rt.local_loaded and self._local_state is not None:
            orders, positions = await self._local_state.open_entities(rt.adapter_id)
            for o in orders:
                rt.orders.setdefault(o.entity_key, o)
                self._seed_broker_seq(rt, o)
            for p in positions:
                rt.positions.setdefault(p.entity_key, p)
                self._seed_broker_seq(rt, p)
            account_states = getattr(self._local_state, "account_states", None)
            if callable(account_states):
                for a in await account_states(rt.adapter_id):
                    self._seed_broker_seq(rt, a)
        rt.local_loaded = True

    @staticmethod
    def _seed_broker_seq(rt: AdapterRuntime, stored: StatefulDTO) -> None:
        """The stored row's broker_seq becomes the entity's last seen one (inherited across a restart)."""
        if stored.broker_seq is not None and stored.adapter_id == rt.adapter_id:
            key = stored.entity_key
            rt.broker_seq[key] = max(rt.broker_seq.get(key, stored.broker_seq), stored.broker_seq)

    def _reconcile_after_commit(self, rt: AdapterRuntime, send_seq: int, bpass: BarrierPass | None) -> None:
        """Clear the pass's gaps only once the consumers have committed every row it published
        (oversee-rules.md §2: the fence clears when the reconcile *has committed*). A row the consumer
        parked instead (timeout / error) keeps that stream's gap open and asks for another RESYNCING."""
        if self._commits is None or bpass is None:
            self._pass_committed(rt, send_seq)
            self._mark_reconciled(rt, send_seq)
            return

        def done(failed: frozenset[StreamKind]) -> None:
            if failed:
                self._keep_gap(rt, failed, "reconcile rows not committed")
            self._pass_committed(rt, send_seq)
            self._mark_reconciled(rt, send_seq)

        self._commits.seal(bpass, done)

    @staticmethod
    def _pass_committed(rt: AdapterRuntime, send_seq: int) -> None:
        """A reconcile pass's rows all settled (the downtime catch-up waits for this)."""
        rt.passes_committed += 1
        rt.last_committed_send_seq = max(rt.last_committed_send_seq, send_seq)

    def _keep_gap(self, rt: AdapterRuntime, streams: Iterable[StreamKind], why: str) -> None:
        """Treat the streams like a dropped frame: no pass sent before now may clear them; a stream whose
        gap is open is resynced again."""
        for stream in streams:
            sub = rt.subs[stream]
            sub.dropped_seq = self.hlc.next()
            if sub.lost:
                log.warning("%s:%s gap kept open (%s)", rt.adapter_id, stream, why)
                self._request_resync(rt)

    def _older_than_watermark(self, rt: AdapterRuntime, stored: OrderDTO | PositionDTO,
                              watermark: int | None, send_ms: int) -> bool:
        if rt.decl(_entity_type(stored)).has_broker_time:
            return watermark is None or stored.broker_updated_at_ms <= watermark
        return stored.broker_updated_at_ms <= send_ms

    def _mark_reconciled(self, rt: AdapterRuntime, send_seq: int) -> None:
        """Clear a stream's gap only when it was SUBSCRIBED before the REST pass was sent and no
        frame was dropped after it. Never while a downtime catch-up holds the adapter's fence (`hold_fence`)."""
        if rt.fence_holds:
            return
        for stream, sub in rt.subs.items():
            if sub.lost and sub.subscribed and sub.subscribed_seq < send_seq and sub.dropped_seq < send_seq:
                sub.lost = False
                self._health(rt, "reconciled", stream)

    async def _refresh_instruments(self, rt: AdapterRuntime) -> None:
        """fetch_instruments() fills the in-memory map, then the InstrumentSink persists it (PostgreSQL
        `instruments`, then the Redis instrument: keys). A failed save does not fail SNAPSHOTTING: it is
        kept and retried from the STREAMING step."""
        specs = await rt.adapter.fetch_instruments()
        rt.instruments = {s.symbol: s for s in specs}
        rt.cross_cache.clear()
        self._refresh_crosses(rt)
        if self._instrument_sink is not None:
            rt.unsaved_instruments = list(specs)
            await self._save_instruments(rt)

    async def _save_instruments(self, rt: AdapterRuntime) -> bool:
        """Persist the pending instrument refresh. True when nothing is left to save."""
        if self._instrument_sink is None or not rt.unsaved_instruments:
            return True
        specs = rt.unsaved_instruments
        try:
            changed = await self._instrument_sink.upsert_instruments(specs, self.hlc.next())
        except Exception as exc:
            if _is_fence_loss(exc):
                raise
            rt.instrument_save_failed_ms = self._clock_ms()
            log.warning("%s: instrument refresh not saved (%d specs), will retry: %r", rt.adapter_id,
                        len(specs), exc)
            return False
        rt.unsaved_instruments = []
        rt.instrument_save_failed_ms = 0
        log.info("%s: instrument refresh saved (%d specs, %d changed)", rt.adapter_id, len(specs), changed)
        return True

    # ================================================================== DLQ worker / Oversee relays
    async def reread_entity(self, adapter_id: str, event_type: Literal["ORDER", "POSITION", "ACCOUNT"],
                            entity_id: str) -> StatefulDTO | None:
        """Check-before-retry (tier-persistence.md §5): read the broker's row for that id, version it
        at its own read and publish it. Returns the fresh row, or None if the broker has none."""
        rt = self._runtimes[adapter_id]
        send_seq, send_ms, wm = self.hlc.next(), self._clock_ms(), rt.watermark_ms
        fresh: StatefulDTO | None
        stream: StreamKind = "orders"
        if event_type == "ORDER":
            fresh = await rt.adapter.fetch_order_status(entity_id)
        elif event_type == "POSITION":
            fresh = await rt.adapter.fetch_position_status(entity_id)
        else:
            stream = "account"
            accounts, _ = await rt.adapter.fetch_account_snapshot()
            fresh = next((a for a in accounts if a.account_id == entity_id), None)
        if fresh is not None:
            self._accept_entity(rt, fresh, "REST", observed_seq=send_seq, local_ms=send_ms, watermark=wm,
                                stream=stream)
        return fresh

    def resubmit_cash_flow(self, cf: CashFlowDTO) -> bool:
        """Check-before-retry re-submission of a cash flow the broker confirmed by transaction_id
        (tier-persistence.md §5): stamped with a fresh seq_id and published on account.state.dto like any
        other (QueueFull parks it again). A REST read, not stream traffic, so the heartbeat-timeout clock
        (`last_inbound_ms`) is left alone. True when it reached the bus."""
        rt = self._runtimes[cf.adapter_id]
        self._check_backpressure()
        return self._on_cash_flow(rt, cf, "account")

    # ================================================================== downtime catch-up (src/runtime/catchup.py)
    def hold_fence(self, adapter_id: str, reason: str) -> None:
        """Keep the adapter's stream_health fence open until `release_fence(reason)`: every stream is marked
        lost now (if it is not already: LEADER_INIT already set every row) and no reconcile pass clears it
        meanwhile, so ActionGate denies risk-increasing actions and Oversee caps as for a gap (fail closed while
        the state may still miss what happened while the engine was off). Synchronous, no I/O."""
        rt = self._runtimes[adapter_id]
        rt.fence_holds.add(reason)
        for stream, sub in rt.subs.items():
            if not sub.lost:
                sub.lost = True
                self._health(rt, "lost", stream)
        log.warning("%s: stream_health fence held (%s)", adapter_id, reason)

    def release_fence(self, adapter_id: str, reason: str) -> bool:
        """Drop one hold. When none is left, the gaps the newest committed reconcile pass covers are cleared now
        (the same rule as `_mark_reconciled`: subscribed before that pass was sent and nothing dropped since);
        any stream still lost asks for a RESYNCING, whose pass then clears it. True when no hold is left."""
        rt = self._runtimes[adapter_id]
        rt.fence_holds.discard(reason)
        if rt.fence_holds:
            return False
        if rt.last_committed_send_seq:
            self._mark_reconciled(rt, rt.last_committed_send_seq)
        if any(sub.lost for sub in rt.subs.values()):
            self._request_resync(rt)
        log.warning("%s: stream_health fence hold released (%s)", adapter_id, reason)
        return True

    def fence_held(self, adapter_id: str) -> bool:
        return bool(self._runtimes[adapter_id].fence_holds)

    def force_resync(self, adapter_id: str, why: str) -> None:
        """Every subscription is treated as lost and SM4 enters RESYNCING (re-subscribe every stream, then a REST
        reconcile pass): used after the process was suspended (PC sleep), when the sockets cannot be trusted."""
        rt = self._runtimes[adapter_id]
        self._lose_all(rt, why)
        rt.resync_event.clear()
        self._request_resync(rt)

    async def _catchup_pace(self, limit_s: float = 30.0) -> None:
        """A long history must not fill a topic (a full topic parks rows and would starve live stream rows): wait,
        bounded, while an entity topic is at least half full. Past the bound the row is offered anyway (a
        QueueFull parks it, and the catch-up then reports itself incomplete)."""
        await asyncio.sleep(0)          # a long history never runs as one synchronous burst on the event loop
        ratios = getattr(self.bus, "fill_ratios", None)
        if not callable(ratios):
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + limit_s
        while loop.time() < deadline:
            r = ratios()
            if max((v for k, v in r.items() if k != "market.price.dto"), default=0.0) < 0.5:
                return
            await asyncio.sleep(0.01)

    async def catchup_fetch(self, adapter_id: str, since_ms: int, until_ms: int) -> CatchupFetch:
        """Read what the broker changed in [since_ms, until_ms]: `fetch_history` (shared contract; an adapter
        without it yields `history=None`) and `fetch_cash_flows(since_ms)`. Read-only. Broker errors propagate.
        Rows the adapter cannot map during these reads count on their own channel (`history_unmapped`), never on
        the live-stream `unmapped_rows`: they make the catch-up incomplete, not the stream interrupted."""
        rt = self._runtimes[adapter_id]
        self.check_unmapped(adapter_id)      # live rows skipped before this read are a real interruption
        send_seq, send_ms, wm = self.hlc.next(), self._clock_ms(), rt.watermark_ms
        fetch = getattr(rt.adapter, "fetch_history", None)
        with history_read() as read:
            history = await fetch(since_ms, until_ms) if callable(fetch) else None
            flows = await rt.adapter.fetch_cash_flows(since_ms)
        partial = self.note_cash_flows(adapter_id, flows, since_ms)
        return CatchupFetch(adapter_id, since_ms, until_ms, send_seq, send_ms, wm, history, list(flows), partial,
                            history_unmapped=read.unmapped)

    async def catchup_submit(self, fetch: CatchupFetch, *, recheck_orders: Sequence[str] = (),
                             recheck_positions: Sequence[str] = (), timeout_s: float = 30.0,
                             reread_timeout_s: float = 30.0) -> CatchupPass:
        """Submit a catch-up through the normal REST path: every history row is validated, version-labelled at
        the fetch's send time and published (the consumers' version compare, event_log, outbox and Redis apply
        decide what changes; nothing is written directly). Then each id in `recheck_*` (stored as still working
        / OPEN, but neither in the latest snapshot nor in the history) is re-read by id and published; a broker
        "not found" is reported, never treated as a close. Waits (bounded) until the consumers committed every
        published row. A history row without a broker_seq it can inherit is skipped and reported (never opens a
        stream gap: the streams are fine). So is a history row without a broker time where the adapter declares
        one: labelling it with the stream watermark (the rule for an untimed snapshot row, which describes the
        present) would let a historic row outrank a newer stream row of the same entity. Broker errors on a by-id
        read (or no answer within `reread_timeout_s`) are reported, not raised. A by-id answer the adapter cannot
        map counts on the re-reads' own channel (`history_read`; `unmapped`), never on the live-stream counters;
        nothing here touches `last_interruption_seq` (a live interruption during the catch-up always survives)."""
        aid = fetch.adapter_id
        rt = self._runtimes[aid]
        res = CatchupPass(aid)
        commits = self._commits
        bpass = commits.open(f"catchup:{aid}") if commits is not None else None
        published: list[tuple[int, StreamKind]] = []
        parked_before = sum(self.parked.values())
        queue_full_before = self.parked.get(QUEUE_FULL, 0)

        def capture(stream: StreamKind, call: Callable[[], bool]) -> bool:
            n0 = len(published)
            self._capture = published
            try:
                ok = call()
            finally:
                self._capture = None
            if commits is not None and bpass is not None:
                for seq, s in published[n0:]:
                    commits.add(bpass, seq, s)
            return ok

        def accept(dto: object, seq: int, local: int, wm: int | None, *, historic: bool = True) -> bool:
            if not isinstance(dto, (OrderDTO, PositionDTO)) or dto.adapter_id != aid:
                res.skipped.append(type(dto).__name__)
                return False
            decl = rt.decl(_entity_type(dto))
            if decl.has_broker_seq and dto.broker_seq is None and rt.broker_seq.get(dto.entity_key) is None:
                res.unversioned.append(dto.entity_key)
                return False
            if historic and decl.has_broker_time and not dto.broker_updated_at_ms:
                res.unversioned.append(dto.entity_key)
                return False
            if historic and isinstance(dto, PositionDTO):
                dto = _keep_known_entry(dto, rt.positions.get(dto.entity_key))
            return capture("orders", lambda: self._accept_entity(
                rt, dto, "REST", observed_seq=seq, local_ms=local, watermark=wm, stream="orders", history=True))

        def oldest_first(rows: tuple[object, ...]) -> list[object]:
            """The rows with each entity's versions re-ordered oldest first (broker revision, then broker time) in
            the slots that entity holds; other entities and non-DTO rows keep their places (stable)."""
            out = list(rows)
            slots: dict[str, list[int]] = {}
            for i, row in enumerate(out):
                if isinstance(row, (OrderDTO, PositionDTO)):
                    slots.setdefault(row.entity_key, []).append(i)
            for idx in slots.values():
                if len(idx) > 1:
                    mine = sorted((out[i] for i in idx), key=lambda r: (
                        -1 if getattr(r, "broker_seq", None) is None else int(getattr(r, "broker_seq")),
                        int(getattr(r, "broker_updated_at_ms", 0) or 0)))
                    for i, row in zip(idx, mine):
                        out[i] = row
            return out

        history = fetch.history
        # A history may carry several versions of one entity (a stop / limit order WORKING at its placement, then
        # its end): each goes through the normal path, oldest first per entity (BrokerHistory contract, re-sorted
        # here stably so an adapter's order never makes a newer version precede an older one). All share the
        # fetch's send time as observed_seq, so the broker version alone orders them; an older version arriving
        # after a newer stored one is a normal stale drop, and a re-read of the same window is a no-op advance.
        for o in oldest_first(tuple(getattr(history, "orders", ()) or ())):
            await self._catchup_pace()
            res.orders_published += accept(o, fetch.send_seq, fetch.send_ms, fetch.watermark_ms)
        for p in oldest_first(tuple(getattr(history, "positions", ()) or ())):
            await self._catchup_pace()
            res.positions_published += accept(p, fetch.send_seq, fetch.send_ms, fetch.watermark_ms)
        for cf in fetch.cash_flows:
            if not isinstance(cf, CashFlowDTO):
                res.skipped.append(type(cf).__name__)
                continue
            await self._catchup_pace()
            res.cash_flows_published += capture("account", functools.partial(self._on_cash_flow, rt, cf, "account"))

        with history_read() as reread:
            await self._catchup_rechecks(rt, res, accept, recheck_orders, recheck_positions, reread_timeout_s)
        res.unmapped = reread.unmapped
        # Catch-up rows never count on `unmapped_rows` (above), so any growth seen now is a live row a stream task
        # skipped during the catch-up: a real interruption. It opens the gap and stays (never reverted), so the
        # catch-up does not vouch for coverage and a `reconnect` re-read follows.
        self.check_unmapped(aid)
        res.parked = sum(self.parked.values()) - parked_before
        res.parked_queue_full = self.parked.get(QUEUE_FULL, 0) - queue_full_before
        if commits is None or bpass is None:
            res.committed = True
            return res
        done: asyncio.Future[frozenset[StreamKind]] = asyncio.get_running_loop().create_future()

        def on_done(failed: frozenset[StreamKind]) -> None:
            if not done.done():
                done.set_result(failed)

        commits.seal(bpass, on_done)
        try:
            failed = await asyncio.wait_for(asyncio.shield(done), timeout_s)
        except TimeoutError:
            res.timed_out = True
            return res
        res.failed_streams = frozenset(failed)
        res.committed = not failed
        return res

    async def _catchup_rechecks(self, rt: AdapterRuntime, res: CatchupPass, accept: Callable[..., bool],
                                recheck_orders: Sequence[str], recheck_positions: Sequence[str],
                                reread_timeout_s: float) -> None:
        """The by-id re-reads of `catchup_submit`: stored working / OPEN, in neither the snapshot nor the history."""
        ad = rt.adapter
        for oid in recheck_orders:
            seq, ms, wm = self.hlc.next(), self._clock_ms(), rt.watermark_ms
            try:
                fresh_o = await asyncio.wait_for(ad.fetch_order_status(oid), reread_timeout_s)
            except Exception as exc:
                if _is_fence_loss(exc):
                    raise
                res.reread_errors.append(f"order {oid}: {type(exc).__name__}")
                continue
            res.reread_orders.append(oid)
            if fresh_o is None:
                res.not_found.append(f"order {oid}")
            elif accept(fresh_o, seq, ms, wm, historic=False):
                res.rechecked_orders.append(fresh_o)
        for pid in recheck_positions:
            seq, ms, wm = self.hlc.next(), self._clock_ms(), rt.watermark_ms
            try:
                fresh_p = await asyncio.wait_for(ad.fetch_position_status(pid), reread_timeout_s)
            except Exception as exc:
                if _is_fence_loss(exc):
                    raise
                res.reread_errors.append(f"position {pid}: {type(exc).__name__}")
                continue
            res.reread_positions.append(pid)
            if fresh_p is None:
                res.not_found.append(f"position {pid}")
            elif accept(fresh_p, seq, ms, wm, historic=False):
                res.rechecked_positions.append(fresh_p)

    async def request_cash_flows(self, account_id: str) -> bool:
        """Relay a cash-flow refresh to the owning adapter, at most once a minute per account.
        False when rate-limited, unknown, or the adapter is not STREAMING."""
        adapter_id = self._account_owner.get(account_id)
        if adapter_id is None:
            return False
        rt = self._runtimes[adapter_id]
        now = self._clock_ms()
        last = self._cash_flow_requested_ms.get(account_id)
        if last is not None and now - last < CASH_FLOW_MIN_INTERVAL_MS:
            return False
        if rt.conn.state is not ConnState.STREAMING:
            return False
        self._cash_flow_requested_ms[account_id] = now
        since = self._roll_ms(now)
        flows = await rt.adapter.fetch_cash_flows(since)
        self.note_cash_flows(adapter_id, flows, since)
        for cf in flows:
            self._on_cash_flow(rt, cf, "account")
        return True

    def note_cash_flows(self, adapter_id: str, flows: object, since_ms: int) -> tuple[str, ...]:
        """Record whether a `fetch_cash_flows(since_ms)` result of this adapter is complete (IMPLEMENTATION_PLAN
        7.1). A partial one (see `src.adapters.base.cash_flows_partial`) turns `pnl_source(adapter_id)` off and
        pages (throttled); a complete one clears that only when it covers the current trading day (since_ms at or
        before the roll). Synchronous, no I/O. Returns the partial reasons (empty = complete)."""
        rt = self._runtimes.get(adapter_id)
        if rt is None:
            return ()
        reasons = cash_flows_partial(flows)
        if reasons:
            if rt.cash_flows_partial != reasons:
                log.error("%s: cash-flow fetch since %d is partial: %s", adapter_id, since_ms, "; ".join(reasons))
            rt.cash_flows_partial = reasons
            self._park_pages.page(f"cash_flows_partial:{adapter_id}", HIGH,
                                  f"cash-flow ledger incomplete: {adapter_id}",
                                  redact_urls("; ".join(reasons))[:300]
                                  + " (MaxDailyLoss flags pnl_unverified until a complete fetch)")
        elif rt.cash_flows_partial and since_ms <= self._roll_ms(self._clock_ms()):
            log.warning("%s: cash-flow fetch complete again", adapter_id)
            rt.cash_flows_partial = ()
        return reasons

    def pnl_source(self, adapter_id: str) -> bool:
        """`SnapshotInputs.pnl_source`: whether the adapter's TRADE_PNL ledger can be trusted. False while its
        newest cash-flow fetch was partial (MaxDailyLossRule FLAGs `pnl_unverified`), and for an adapter this
        orchestrator does not drive (fail closed)."""
        rt = self._runtimes.get(adapter_id)
        return rt is not None and not rt.cash_flows_partial

    def on_store_refused(self, dto: object, stored_status: str | None) -> None:
        """The store refused this published row (an illegal transition against its stored row; it parked it).
        If memory still holds exactly that row, its status goes back to the stored one, so later in-memory
        transition checks and absent-entity re-reads use the truth; an unknown stored status forgets the row
        (the store's guard still checks every later row). A later row in memory is left alone: it is judged on
        its own commit. Synchronous, never raises into the caller."""
        try:
            self._restore_refused(dto, stored_status)
        except Exception:
            log.exception("store refusal of %s not applied to memory", type(dto).__name__)

    def _restore_refused(self, dto: object, stored_status: str | None) -> None:
        if not isinstance(dto, (OrderDTO, PositionDTO)):
            return
        rt = self._runtimes.get(dto.adapter_id)
        if rt is None:
            return
        key = dto.entity_key
        stored = None if stored_status is None else str(stored_status).strip()
        if isinstance(dto, OrderDTO):
            mem_o = rt.orders.get(key)
            if mem_o is None or mem_o.seq_id != dto.seq_id:
                return
            try:
                status = OrderStatus(stored or "")
            except ValueError:
                del rt.orders[key]
                log.warning("%s: refused %s forgotten (stored status %r unknown)", rt.adapter_id, key, stored)
                return
            rt.orders[key] = replace(mem_o, status=status)
        else:
            mem_p = rt.positions.get(key)
            if mem_p is None or mem_p.seq_id != dto.seq_id:
                return
            if stored not in ("OPEN", "CLOSED"):
                del rt.positions[key]
                log.warning("%s: refused %s forgotten (stored status %r unknown)", rt.adapter_id, key, stored)
                return
            rt.positions[key] = replace(mem_p, status="OPEN" if stored == "OPEN" else "CLOSED")
        log.warning("%s: refused %s %s -> stored status %s restored in memory", rt.adapter_id, key,
                    getattr(dto, "status", "?"), stored)

    # ================================================================== tick symbols
    def _want_symbol(self, rt: AdapterRuntime, symbol: str) -> None:
        """A committed order / position needs this symbol and its pnl_ccy -> account-currency crosses. The
        crosses are checked even when the symbol itself is already subscribed: the account currency may have
        become known only after the symbol was (adapter-isolation.md §5 SNAPSHOTTING)."""
        want = {symbol} | self._crosses(rt, (symbol,))
        missing = want - rt.tick_symbols
        if missing:
            self._watch_clean(rt, missing - rt.pending_symbols)
            rt.pending_symbols |= missing

    def _account_ccys(self, rt: AdapterRuntime) -> set[str]:
        return {a.currency for a in rt.accounts.values()} | rt.live_ccys

    @staticmethod
    def _pair(specs: Sequence[InstrumentSpec], a: str, b: str) -> str | None:
        """The first instrument (by symbol) quoting a against b, direct or inverse."""
        return next((c.symbol for c in specs if {c.base_ccy, c.quote_ccy} == {a, b}), None)

    def _cross_for(self, rt: AdapterRuntime, pnl_ccy: str, ccy: str) -> tuple[str, ...]:
        """The pairs Oversee converts pnl_ccy -> ccy with (src/oversee/risk.py MarkBook.fx): the direct or
        inverse pair, else the first two-leg path via FX_VIA_ORDER then the adapter's other currencies.
        Mirrored here so every leg Oversee will mark is subscribed (adapter-isolation.md §5)."""
        key = (pnl_ccy, ccy)
        if key not in rt.cross_cache:
            specs = sorted(rt.instruments.values(), key=lambda c: c.symbol)
            direct = self._pair(specs, pnl_ccy, ccy)
            legs: tuple[str, ...] = (direct,) if direct is not None else ()
            if not legs:
                ccys = {c for s in specs for c in (s.base_ccy, s.quote_ccy)}
                order = [c for c in FX_VIA_ORDER if c in ccys] + sorted(ccys - set(FX_VIA_ORDER))
                for via in order:
                    if via in (pnl_ccy, ccy):
                        continue
                    leg1 = self._pair(specs, pnl_ccy, via)
                    leg2 = self._pair(specs, via, ccy) if leg1 is not None else None
                    if leg1 is not None and leg2 is not None:
                        legs = (leg1, leg2)
                        break
            rt.cross_cache[key] = legs
        return rt.cross_cache[key]

    def _crosses(self, rt: AdapterRuntime, symbols: Iterable[str]) -> set[str]:
        """FX crosses needed to convert each symbol's pnl_ccy into its account currency."""
        account_ccys = self._account_ccys(rt)
        out: set[str] = set()
        for sym in symbols:
            spec = rt.instruments.get(sym)
            if spec is None:
                continue
            for ccy in account_ccys - {spec.pnl_ccy}:
                out.update(self._cross_for(rt, spec.pnl_ccy, ccy))
        return out

    def _refresh_crosses(self, rt: AdapterRuntime) -> None:
        """Re-derive every needed symbol (held + crosses) after an account currency or the instrument map
        changed, and queue what is not subscribed yet for ensure_ticks(). Before the prices pump has started
        there is nothing to do: starting it subscribes the whole required set."""
        if "prices" not in rt.tasks:
            return
        missing = set(self.required_tick_symbols(rt.adapter_id)) - rt.tick_symbols
        if missing:
            self._watch_clean(rt, missing - rt.pending_symbols)
            rt.pending_symbols |= missing

    def required_tick_symbols(self, adapter_id: str) -> list[str]:
        rt = self._runtimes[adapter_id]
        held = set(self._tick_cfg.get(adapter_id, ()))
        held |= {o.symbol for o in rt.orders.values() if not o.status.terminal}
        held |= {p.symbol for p in rt.positions.values() if p.status == "OPEN"}
        return sorted(held | self._crosses(rt, held))

    async def flush_pending_symbols(self, adapter_id: str) -> list[str]:
        """While STREAMING: ensure_ticks() for newly needed symbols, rate-limited per adapter."""
        rt = self._runtimes[adapter_id]
        now = self._clock_ms()
        if not rt.pending_symbols or rt.conn.state is not ConnState.STREAMING \
                or now - rt.last_ensure_ms < ENSURE_TICKS_MIN_INTERVAL_MS:
            return []
        rt.pending_symbols -= rt.tick_symbols
        syms = sorted(rt.pending_symbols)
        if not syms:
            return []
        rt.last_ensure_ms = now
        await rt.adapter.ensure_ticks(syms)
        rt.tick_symbols.update(syms)
        rt.pending_symbols.difference_update(syms)
        for s in syms:
            rt.tick_wanted_ms[s] = now
        return syms

    # ================================================================== stream pumps
    def _pump_factories(self, rt: AdapterRuntime) -> dict[PumpName, tuple[StreamKind,
                                                                         Callable[[], AsyncIterator[object]]]]:
        ad = rt.adapter
        out: dict[PumpName, tuple[StreamKind, Callable[[], AsyncIterator[object]]]] = {
            "orders": ("orders", ad.stream_order_updates),
            "account": ("account", ad.stream_account),
            "prices": ("prices", lambda: ad.stream_ticks(self.required_tick_symbols(rt.adapter_id))),
        }
        if type(ad).stream_positions is not BrokerAdapter.stream_positions:
            out["positions"] = ("orders", ad.stream_positions)   # positions share the orders health key
        return out

    def start_missing_streams(self, adapter_id: str) -> list[PumpName]:
        """Subscribe each stream that is not SUBSCRIBED (restarting its pump task)."""
        rt = self._runtimes[adapter_id]
        started: list[PumpName] = []
        for name, (stream, factory) in self._pump_factories(rt).items():
            task = rt.tasks.get(name)
            if rt.subs[stream].subscribed and task is not None and not task.done():
                continue
            if task is not None and not task.done():
                task.cancel()
            if stream == "prices":
                rt.tick_symbols = set(self.required_tick_symbols(adapter_id))
                rt.pending_symbols -= rt.tick_symbols
                now = self._clock_ms()
                for s in rt.tick_symbols:
                    rt.tick_wanted_ms[s] = now      # a (re)subscribed symbol gets the full stale-price grace
                for s in rt.clean_wanted_ms:
                    rt.clean_wanted_ms[s] = now     # ... and the full clean-mid grace
                rt.last_spread.clear()
            rt.tasks[name] = asyncio.create_task(self._pump(rt, stream, factory),
                                                 name=f"pump:{adapter_id}:{name}")
            started.append(name)
        return started

    async def _pump(self, rt: AdapterRuntime, stream: StreamKind,
                    factory: Callable[[], AsyncIterator[object]]) -> None:
        try:
            async for item in factory():
                # Unexpected item types are not dropped here: handle_stream_item opens the stream's gap.
                self.handle_stream_item(rt.adapter_id, stream, item)  # type: ignore[arg-type]
            rt.subs[stream].subscribed = False
            self._open_gap(rt, stream, "stream ended")
        except asyncio.CancelledError:
            raise
        except SessionExpiredError:
            self._lose_all(rt, "session expired")
            rt.auth_lost = True
            rt.wake.set()
        except Exception as exc:   # socket / transport error: the whole socket goes to BACKOFF
            log.warning("%s:%s stream error: %r", rt.adapter_id, stream, exc)
            self._lose_all(rt, f"stream error {exc!r}")
            rt.socket_lost = True
            rt.wake.set()

    async def _close_socket(self, rt: AdapterRuntime) -> None:
        tasks = [t for t in rt.tasks.values() if not t.done()]
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        rt.tasks.clear()
        with contextlib.suppress(Exception):
            await rt.adapter.close()
        self._lose_all(rt, "socket closed")
        rt.socket_lost = False
        rt.auth_lost = False

    async def _await_subscribed(self, rt: AdapterRuntime) -> bool:
        deadline = asyncio.get_running_loop().time() + self._subscribe_timeout_s
        while not rt.all_subscribed:
            if rt.socket_lost or rt.auth_lost:
                return False
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            rt.wake.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(rt.wake.wait(), remaining)
        return True

    # ================================================================== SM4 driver
    async def run_adapter(self, adapter_id: str, stop: asyncio.Event) -> None:
        """Drive SM4 for one adapter until stop is set (or REFUSED).

        Supervised (IMPLEMENTATION_PLAN 6.2): a non-fence error raised by a step (an asyncpg error from the
        LocalStateReader, a driver bug, an odd adapter exception) never ends the driver. It is logged and paged
        (throttled), the socket is closed and SM4 goes to BACKOFF, so the adapter reconnects and re-snapshots
        with the orchestrator's state intact. Only a lost fence (`EpochMismatch`) or cancellation propagates."""
        rt = self._runtimes[adapter_id]
        conn = rt.conn
        try:
            conn.start()
        except ExecutionLockedError:
            log.critical("%s: BROKER_ENV refused; adapter not started", adapter_id)
            return
        try:
            while not stop.is_set():
                try:
                    streaming = conn.state is ConnState.STREAMING
                    if not await self._drive_step(rt, stop):
                        return
                    # Several steps can complete without suspending (an early-return STREAMING step, a REST pass
                    # whose reads were served without I/O): always yield, so the driver never starves the loop.
                    await asyncio.sleep(0)
                    if streaming and rt.driver_errors and \
                            self._clock_ms() - rt.last_driver_error_ms >= DRIVER_ERROR_STREAK_RESET_MS:
                        rt.driver_errors = 0         # STREAMING for a while without an error: the streak is over
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if _is_fence_loss(exc):
                        raise
                    await self._driver_error(rt, stop, exc)
        finally:
            await self._close_socket(rt)
            conn.stop()

    async def _driver_error(self, rt: AdapterRuntime, stop: asyncio.Event, exc: Exception) -> None:
        """A non-fence error from a driver step: page (throttled per adapter), then restart the session through
        BACKOFF. Outside a session state (BACKOFF / AUTH_FAILED / DISCONNECTED) SM4 stays where it is and the
        driver waits a backoff delay first, so a persistent error never spins the loop."""
        rt.driver_errors += 1
        rt.last_driver_error_ms = self._clock_ms()
        conn = rt.conn
        state = conn.state
        log.error("%s: SM4 driver error in %s (%d in a row), restarting through BACKOFF: %r", rt.adapter_id,
                  state.value, rt.driver_errors, exc, exc_info=exc)
        # The page leaves the node (email / push): any URL in the message is reduced to scheme://host.
        self._driver_pages.page(f"driver:{rt.adapter_id}", HIGH, f"ingestion driver error: {rt.adapter_id}",
                                f"{type(exc).__name__} in {state.value} ({rt.driver_errors} in a row): "
                                f"{redact_urls(str(exc))[:300]}; SM4 restarts through BACKOFF")
        rt.resync_deadline_ms = 0
        if state in (ConnState.AUTHENTICATING, ConnState.SNAPSHOTTING, ConnState.STREAMING, ConnState.RESYNCING):
            # Repeated driver errors back off exponentially even though each reconnect resets SM4's count.
            conn.attempt = max(conn.attempt, rt.driver_errors - 1)
            try:
                conn.on_transient_error(f"driver error: {exc!r}")
            except IllegalConnTransition:
                log.exception("%s: SM4 could not enter BACKOFF", rt.adapter_id)
            else:
                return                       # the BACKOFF step closes the socket and waits
        rt.wake.clear()
        await self._wait_any(rt.wake, stop, timeout=backoff_delay(rt.driver_errors - 1))
        await asyncio.sleep(0)

    async def _drive_step(self, rt: AdapterRuntime, stop: asyncio.Event) -> bool:
        """One SM4 driver step. False when the driver must end (an unexpected terminal state)."""
        conn = rt.conn
        adapter_id = rt.adapter_id
        state = conn.state
        if state is ConnState.AUTHENTICATING:
            await self._step_authenticate(rt)
        elif state in (ConnState.SNAPSHOTTING, ConnState.RESYNCING):
            await self._step_snapshot(rt)
        elif state is ConnState.STREAMING:
            await self._step_streaming(rt, stop)
        elif state is ConnState.BACKOFF:
            await self._close_socket(rt)
            await self._wait_backoff(rt, stop)
            if not stop.is_set():
                conn.on_backoff_elapsed()
        elif state is ConnState.AUTH_FAILED:
            log.critical("%s: AUTH_FAILED (credential error); waiting for re-arm", adapter_id)
            self._page_auth_failed(rt)
            await self._close_socket(rt)
            rt.rearm.clear()
            while not stop.is_set() and not rt.rearm.is_set():
                rt.wake.clear()
                tracked = self._generation_tracked or self._refresh_adapter is not None
                poll = self._auth_poll_s if tracked else None
                await self._wait_any(rt.wake, stop, timeout=poll)
                if poll is None or stop.is_set():
                    continue
                if await self._maybe_replace_adapter(rt):
                    rt.rearm.set()                    # rotated credentials: a new generation
                elif self._generation_tracked and await self._generation_changed(rt):
                    rt.rearm.set()
            if rt.rearm.is_set():
                conn.on_rearmed()
        else:
            return False
        return True

    async def _wait_backoff(self, rt: AdapterRuntime, stop: asyncio.Event) -> None:
        """Sleep SM4's backoff, ended early by stop or by `reset_backoff()` (PRE_OPEN readiness)."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + rt.conn.backoff_s
        while not stop.is_set():
            rt.wake.clear()
            if rt.backoff_reset:
                break
            left = deadline - loop.time()
            if left <= 0:
                break
            await self._wait_any(rt.wake, stop, timeout=left)
        rt.backoff_reset = False
        await asyncio.sleep(0)       # a zero (or reset) backoff still yields: the driver never spins the loop

    def reset_backoff(self, adapter_id: str) -> bool:
        """PRE_OPEN readiness hook (operations.md §10 "reset backoff and reconnect"): SM4's transient-failure
        count is cleared and a BACKOFF wait ends now, so the adapter reconnects at once. Returns True when a
        BACKOFF wait was cut short. AUTH_FAILED is untouched (re-armed only by a new credential generation
        or an operator)."""
        rt = self._runtimes[adapter_id]
        if rt.conn.reset_backoff():
            rt.backoff_reset = True
            rt.wake.set()
            return True
        return False

    async def _wait_any(self, a: asyncio.Event, b: asyncio.Event, timeout: float | None = None) -> None:
        waiters = [asyncio.ensure_future(a.wait()), asyncio.ensure_future(b.wait())]
        try:
            await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for w in waiters:
                w.cancel()

    # ------------------------------------------------------------------ credential generation (§16)
    @property
    def _generation_tracked(self) -> bool:
        return self._auth_store is not None and self._credential_generation is not None

    async def _generation(self, rt: AdapterRuntime) -> str:
        """The adapter's current credential generation (Secrets Manager version id / token_generation)."""
        if self._credential_generation is None:
            return str(getattr(rt.adapter, "credential_generation", None) or DEFAULT_CREDENTIAL_GENERATION)
        value = self._credential_generation(rt.adapter_id)
        if inspect.isawaitable(value):
            value = await value
        return str(value)

    async def _generation_changed(self, rt: AdapterRuntime) -> bool:
        """AUTH_FAILED exit: the generation recorded bad is no longer the current one."""
        try:
            current = await self._generation(rt)
        except Exception as exc:
            log.warning("%s: credential generation unreadable: %r", rt.adapter_id, exc)
            return False
        return rt.failed_generation is not None and current != rt.failed_generation

    async def _record_auth_failed(self, rt: AdapterRuntime, generation: str, detail: str) -> None:
        rt.failed_generation = generation
        if self._auth_store is None:
            return
        try:
            await self._auth_store.record_auth_failed(rt.adapter_id, generation, detail)
        except Exception as exc:
            if _is_fence_loss(exc):
                raise
            log.error("%s: could not record AUTH_FAILED for generation %s: %r", rt.adapter_id, generation, exc)

    def _page_auth_failed(self, rt: AdapterRuntime) -> None:
        """Critical page on every entry to AUTH_FAILED (operations.md §13: at most 3 attempts, then stop and
        page). Sent whatever the market state: auth failures are never muted by the calendar."""
        why = rt.conn.history[-1][2] if rt.conn.history else ""
        send_page(self._pager, CRITICAL, f"broker AUTH_FAILED: {rt.adapter_id}",
                  f"credential generation {rt.failed_generation or '?'}: {why[:300]}. SM4 stopped after "
                  f"{MAX_CREDENTIAL_ATTEMPTS} attempts (or a generation already recorded bad); it waits for a new "
                  "credential generation or an operator re-arm")

    async def _maybe_replace_adapter(self, rt: AdapterRuntime) -> bool:
        """Credential rotation within a term (operations.md §13): ask `refresh_adapter` for an adapter built
        from rotated credentials. A replacement closes the current socket and adapter first; the orchestrator's
        own state (entities, instruments, versions, subscriptions bookkeeping) is kept. True when replaced."""
        if self._refresh_adapter is None:
            return False
        try:
            fresh = self._refresh_adapter(rt.adapter_id, rt.adapter)
        except Exception as exc:
            if _is_fence_loss(exc):
                raise
            log.warning("%s: credential refresh failed; keeping the current adapter: %r", rt.adapter_id, exc)
            return False
        if fresh is None or fresh is rt.adapter:
            return False
        if fresh.adapter_id != rt.adapter_id:
            log.error("%s: refreshed adapter has id %s; ignored", rt.adapter_id, fresh.adapter_id)
            return False
        await self._close_socket(rt)
        rt.adapter = fresh
        rt.unmapped_seen = self._unmapped_counts(fresh)
        rt.conn.credential_errors = 0     # a new credential generation gets its own 3 attempts (§13)
        log.warning("%s: credentials rotated; the next connect uses the new adapter", rt.adapter_id)
        return True

    async def _step_authenticate(self, rt: AdapterRuntime) -> None:
        await self._maybe_replace_adapter(rt)
        generation: str | None = None
        if self._auth_store is not None:
            try:
                generation = await self._generation(rt)
                known_bad = await self._auth_store.known_bad(rt.adapter_id, generation)
            except Exception as exc:
                if _is_fence_loss(exc):
                    raise
                # Unknown auth state: never risk a locked account by connecting blind; back off instead.
                rt.conn.on_transient_error(f"adapter_auth_state unreadable: {exc!r}")
                return
            if known_bad:
                log.critical("%s: credential generation %s is known bad; not connecting", rt.adapter_id,
                             generation)
                rt.failed_generation = generation
                rt.conn.on_known_bad(f"generation {generation}")   # AUTHENTICATING -> AUTH_FAILED, no connect
                return
        try:
            await rt.adapter.connect()
        except AuthFailedError as exc:
            if rt.conn.on_credential_error(str(exc)) is ConnState.AUTH_FAILED:
                if generation is None:
                    generation = await self._generation(rt)
                await self._record_auth_failed(rt, generation, str(exc))
            return
        except (BrokerError, OSError, TimeoutError) as exc:
            rt.conn.on_transient_error(repr(exc))
            return
        rt.auth_lost = rt.socket_lost = False
        rt.last_inbound_ms = self._clock_ms()
        rt.conn.on_authenticated()

    async def _step_snapshot(self, rt: AdapterRuntime) -> None:
        conn = rt.conn
        snapshotting = conn.state is ConnState.SNAPSHOTTING
        if not snapshotting and rt.resync_deadline_ms == 0:
            rt.resync_deadline_ms = self._clock_ms() + self._resync_deadline_ms
        try:
            if snapshotting:
                await self._refresh_instruments(rt)          # instrument map before any stream starts
            await self._load_local(rt)                       # stored rows before any stream row arrives
            rt.resync_event.clear()
            self.start_missing_streams(rt.adapter_id)
            if not await self._await_subscribed(rt):
                if rt.auth_lost:
                    raise SessionExpiredError("auth lost while subscribing")
                if rt.socket_lost or snapshotting:
                    conn.on_transient_error("subscribe failed")
                    return
                rt.resubscribe_failures += 1
                conn.on_resubscribe_failed(rt.resubscribe_failures, self._max_resubscribe_failures)
                return
            rep = await self.run_snapshot(rt.adapter_id, refresh_instruments=False)
        except SessionExpiredError:
            conn.on_auth_lost("session expired")
            return
        except (BrokerError, OSError, TimeoutError) as exc:
            if snapshotting or self.spooler.fill_ratio >= RING_RESYNC_THRESHOLD \
                    or self._clock_ms() >= rt.resync_deadline_ms:
                conn.on_transient_error(repr(exc))
                rt.resync_deadline_ms = 0
                return
            await asyncio.sleep(self._resync_retry_s)          # RESYNCING retries with a new watermark
            return
        rt.resubscribe_failures = 0
        if snapshotting:
            conn.on_snapshot_published()
            if not rep.all_subscribed or rep.dropped_after_watermark:
                self._request_resync(rt)
            return
        new = conn.on_reconciled(rep.all_subscribed, rep.dropped_after_watermark,
                                 self.spooler.fill_ratio < RING_RESYNC_THRESHOLD,
                                 self._clock_ms() >= rt.resync_deadline_ms)
        if new is not ConnState.RESYNCING:
            rt.resync_deadline_ms = 0
        else:
            # A retry with a new watermark (a gap still open, e.g. a row parked or skipped again) waits
            # `resync_retry_s` like a REST error does, so a persistent bad row never spins REST passes.
            await asyncio.sleep(self._resync_retry_s)

    async def _step_streaming(self, rt: AdapterRuntime, stop: asyncio.Event) -> None:
        conn = rt.conn
        if rt.socket_lost:
            conn.on_transient_error("socket lost")
            return
        if rt.auth_lost:
            await self._close_socket(rt)
            conn.on_auth_lost("auth lost")
            return
        now = self._clock_ms()
        if rt.last_inbound_ms and now - rt.last_inbound_ms > HEARTBEAT_TIMEOUT_MS:
            conn.on_transient_error("no inbound traffic for 30 s")
            return
        if rt.resync_event.is_set() or conn.resync_pending:
            if conn.on_gap(self.spooler.fill_ratio < RING_RESYNC_THRESHOLD) is ConnState.RESYNCING:
                return
        if rt.open_resync_due_ms is not None and now >= rt.open_resync_due_ms:
            conn.on_market_open()            # REST-only RESYNCING: SUBSCRIBED streams are left alone
            return
        if self.check_unmapped(rt.adapter_id):
            return                           # a gap opened: RESYNCING on the next step
        try:
            await self.flush_pending_symbols(rt.adapter_id)
        except SessionExpiredError:
            self._lose_all(rt, "session expired (ensure_ticks)")
            rt.auth_lost = True
            return
        except (BrokerError, OSError, TimeoutError) as exc:
            # The symbols stay pending (retried, rate-limited); the clean-mid watch pages if this persists.
            log.warning("%s: ensure_ticks failed: %r", rt.adapter_id, exc)
        if rt.unsaved_instruments and now - rt.instrument_save_failed_ms >= INSTRUMENT_SAVE_RETRY_MS:
            await self._save_instruments(rt)
        if self._account_poll_due(rt, now):
            await self.poll_account(rt.adapter_id)
        self.check_liveness(rt.adapter_id)
        rt.wake.clear()
        await self._wait_any(rt.wake, stop, timeout=1.0)

    # ------------------------------------------------------------------ SM10 open (adapter-isolation.md §5)
    def market_open_resync(self, adapter_id: str, open_ms: int | None = None) -> int:
        """SM10 open hook: schedule the REST-only RESYNCING at open + 30-60 s (uniform jitter). A STREAMING
        adapter enters RESYNCING when it is due; an adapter that is not STREAMING then goes through
        SNAPSHOTTING, and any REST pass sent at or after the due time clears the request.

        An open already covered by a pending pass (one due at or after this open + 30 s, e.g. several symbols
        opening together) schedules nothing new. A later open that the pending pass does not cover is kept as
        the next due time and promoted once the pending pass is sent, so it is never lost (also when it arrives
        while that pass is in flight). Returns the next due time."""
        rt = self._runtimes[adapter_id]
        base = self._clock_ms() if open_ms is None else open_ms
        pending = [d for d in (rt.open_resync_due_ms, rt.open_resync_next_ms) if d is not None]
        if pending and max(pending) >= base + OPEN_RESYNC_MIN_MS:
            return min(pending)
        jitter = min(max(self._open_jitter(), 0.0), 1.0)
        due = base + OPEN_RESYNC_MIN_MS + int(OPEN_RESYNC_SPAN_MS * jitter)
        if rt.open_resync_due_ms is None:
            rt.open_resync_due_ms = due
        else:
            rt.open_resync_next_ms = due     # covers any earlier "next": it is due later
        rt.wake.set()
        log.info("%s: market open; REST-only RESYNCING due at %d", adapter_id, due)
        return rt.open_resync_due_ms

    @staticmethod
    def _open_resync_sent(rt: AdapterRuntime, send_ms: int) -> None:
        """A REST pass sent at `send_ms` satisfies every open whose due time it reached; a later open's due
        time becomes the pending one."""
        if rt.open_resync_next_ms is not None and send_ms >= rt.open_resync_next_ms:
            rt.open_resync_due_ms = rt.open_resync_next_ms = None
        elif rt.open_resync_due_ms is not None and send_ms >= rt.open_resync_due_ms:
            rt.open_resync_due_ms, rt.open_resync_next_ms = rt.open_resync_next_ms, None

    # ------------------------------------------------------------------ flat-account poll (Liveness)
    def _account_poll_due(self, rt: AdapterRuntime, now: int) -> bool:
        if self._account_poll_ms <= 0 or now - rt.last_account_poll_ms < self._account_poll_ms:
            return False
        accounts = set(rt.accounts) | {a for a, owner in self._account_owner.items() if owner == rt.adapter_id}
        if not accounts:
            return True                       # nothing proven yet
        busy = {p.account_id for p in rt.positions.values() if p.status == "OPEN"}   # flat: no OPEN position
        return bool(accounts - busy)

    async def poll_account(self, adapter_id: str) -> bool:
        """REST `fetch_account_snapshot()` while STREAMING, so a flat account (whose stream may stay silent)
        still proves the account path every 60 s. The rows are versioned at the request's send time and
        published like any REST row. A failure is logged; `ACCOUNT_POLL_PAGE_AFTER` consecutive ones page
        once, cleared by the next success. A lost session sends SM4 to AUTHENTICATING. True on success."""
        rt = self._runtimes[adapter_id]
        send_seq, send_ms, wm = self.hlc.next(), self._clock_ms(), rt.watermark_ms
        rt.last_account_poll_ms = send_ms
        key = f"account_poll:{adapter_id}"
        try:
            accounts, lives = await asyncio.wait_for(rt.adapter.fetch_account_snapshot(),
                                                     self._account_poll_timeout_s)
        except SessionExpiredError:
            self._lose_all(rt, "session expired (account poll)")
            rt.auth_lost = True
            rt.wake.set()
            return False
        except asyncio.CancelledError:
            raise
        except Exception as exc:          # an optional liveness probe never stops or hangs the SM4 driver
            if _is_fence_loss(exc):
                raise
            rt.account_poll_failures += 1
            log.warning("%s: flat-account REST poll failed (%d in a row): %r", adapter_id,
                        rt.account_poll_failures, exc)
            if rt.account_poll_failures >= ACCOUNT_POLL_PAGE_AFTER:
                self._episodes.active(key, HIGH, f"account path unproven: {adapter_id}",
                                      f"{rt.account_poll_failures} consecutive REST account polls failed")
            return False
        rt.account_poll_failures = 0
        self._episodes.clear(key)
        for a in accounts:
            self._accept_entity(rt, a, "REST", observed_seq=send_seq, local_ms=send_ms, watermark=wm,
                                stream="account")
        for lv in lives:
            self._on_live(rt, lv)
        return True

    # ------------------------------------------------------------------ liveness pages
    def check_liveness(self, adapter_id: str) -> None:
        """Synchronous liveness pages while SM4 is STREAMING (called by the driver about once a second):
        * a `conn:{adapter}:{stream}` key absent for more than `conn_absent_alert_ms` while the adapter's
          market is OPEN (adapter-isolation.md §5 Liveness);
        * a stale-price page per subscribed symbol with no tick for `stale_price_ms`, unless the calendar
          mutes price staleness for it (CLOSED / ROLLOVER / PRE_OPEN / maintenance; operations.md §10).
        Each pages once per episode and re-arms when the condition clears."""
        rt = self._runtimes[adapter_id]
        if rt.conn.state is not ConnState.STREAMING:
            return
        now = self._clock_ms()
        is_open = True
        if self._market_open is not None:
            try:
                is_open = bool(self._market_open(adapter_id))
            except Exception:
                log.exception("%s: market_open hook failed", adapter_id)
        for stream, sub in rt.subs.items():
            key = f"conn:{adapter_id}:{stream}"
            absent_ms = now - sub.key_until_ms
            if absent_ms <= 0 or rt.fence_holds:
                # A downtime catch-up holds the fence on purpose (its own page reports a failure): the key's
                # absence is expected meanwhile, and its absence clock restarts at the release.
                if rt.fence_holds:
                    sub.key_until_ms = max(sub.key_until_ms, now - 1)
                self._episodes.clear(key)
            elif is_open and absent_ms > self._conn_absent_alert_ms:
                self._episodes.active(key, HIGH, f"conn key absent: {adapter_id}:{stream}",
                                      f"absent for {absent_ms // 1000} s while SM4 is STREAMING and the market "
                                      "is OPEN")
        for sym in sorted(rt.tick_symbols):
            key = f"stale_price:{adapter_id}:{sym}"
            if self._price_muted_for(adapter_id, sym):
                rt.tick_wanted_ms[sym] = now     # the grace restarts when the mute ends (e.g. at the open)
                self._episodes.clear(key)
                continue
            ref = max(rt.last_tick_ms.get(sym, 0), rt.tick_wanted_ms.get(sym, 0))
            if now - ref <= self._stale_price_ms:
                self._episodes.clear(key)
                continue
            last = rt.last_tick_ms.get(sym)
            self._episodes.active(key, HIGH, f"stale price: {adapter_id}:{sym}",
                                  "no tick yet" if last is None else f"no tick for {(now - last) // 1000} s")
        self._check_clean_mids(rt, now)

    def _check_clean_mids(self, rt: AdapterRuntime, now: int) -> None:
        """Page once per symbol queued for ensure_ticks() (a held symbol or a pnl_ccy cross) that has had no
        clean quote for `clean_mid_ms` (adapter-isolation.md §5): its subscribe keeps failing, or it ticks only
        at a wide spread. A symbol whose stale-price page is already open is not paged twice; the grace
        restarts while the calendar mutes price staleness for it and when the prices stream re-subscribes. A
        subscribed symbol with no tick at all is left to the stale-price page (one page per cause)."""
        adapter_id = rt.adapter_id
        for sym, since in sorted(rt.clean_wanted_ms.items()):
            key = f"no_clean_mid:{adapter_id}:{sym}"
            if self._price_muted_for(adapter_id, sym):
                rt.clean_wanted_ms[sym] = now
                self._episodes.clear(key)
                continue
            if now - since <= self._clean_mid_ms:
                continue
            if sym not in rt.tick_symbols:
                why = "its ensure_ticks() subscription has not succeeded"
            elif sym in rt.last_spread:
                spread = rt.last_spread[sym]
                why = ("ticks only above the clean spread (last spread "
                       + ("unknown" if spread is None else f"{spread} pips") + ")")
            else:
                continue                     # subscribed, no tick at all: the stale-price page covers it
            self._episodes.active(key, HIGH, f"no clean mid: {adapter_id}:{sym}",
                                  f"no clean quote for {(now - since) // 1000} s: {why}")

    def _price_muted_for(self, adapter_id: str, symbol: str) -> bool:
        if self._price_muted is None:
            return False
        try:
            return bool(self._price_muted(adapter_id, symbol))
        except Exception:
            log.exception("%s: price_staleness_suppressed hook failed", adapter_id)
            return False

    async def run(self, stop: asyncio.Event) -> None:
        """Run every registered adapter's SM4 driver plus the health mirror until stop is set."""
        tasks = [asyncio.create_task(self.health.run(stop), name="health-mirror")]
        tasks += [asyncio.create_task(self.run_adapter(a, stop), name=f"sm4:{a}") for a in self._runtimes]
        try:
            await stop.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
