"""The runtime side of the market-data and ARM ports (design PR12, §B3, §B14; plan S31).

The scoped packages (`src.marketdata`, `src.riskmgr`) never import the runtime, the adapters or the orchestrator:
they receive the narrow Protocols of `src.marketdata.ports` / `src.riskmgr.ports`, implemented here as `__slots__`
wrappers around callables, so nothing behind a port is reachable from a scoped module (the lockout refuses `_`
attributes on anything but `self`). This module is under the lockout's name ban (`NAME_BAN_FILES`): it reads, it
never sends, changes or cancels anything at a broker, and every broker call it makes goes through the adapter's
existing read path (`fetch_price_bars`, the orchestrator's `reread_stop_orders`), whose only REST client is
`ReadOnlyTransport`.

* `BrokerBarsPortImpl`: the adapter's `fetch_price_bars` inside `rest_priority(RestPriority.BARS)` (as
  `reread_cash_flow` does for DLQ), broker errors turned into `Transient` / `Unsupported` values (PR12).
* `StopHandleReadPortImpl`: one `reread_stop_orders` per adapter per anchor (PR9, R11-M20); each pinned handle comes
  back with the read's send time and the committed version label, or `NOT_FOUND` / `TRANSIENT`.
* `CalendarPortImpl` (SM10 `state()`, None = closed, IN-10), `AdapterHealthPortImpl` (SM4 STREAMING and the REST
  outcome record of S06, R11-M10), `PgHealthPortImpl` (R11-M11), `StampPortImpl` (the task's liveness).
* `PagerWrapper`: `page()` for ordinary pages, `log_mirror()` = `log.critical` only, for the FAILED ping (never
  `EngineSession.page`, which would queue a second critical outbox row, §B8 / R11-M05).
* `LeaderCheckPortImpl` (the md bootstrap's `confirm_leader`), `NodeRecordPortImpl` (this node's §A7 record in its
  `arm_status` row), `FailedSpoolPortImpl` (the CRC-framed local spool of FAILED episodes, §B14 Record).

Nothing here logs or pages a price, a level, a credential or a DSN: errors are reduced to their class name.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import struct
import zlib
from collections.abc import Awaitable, Callable, Collection, Sequence
from pathlib import Path
from typing import Final

from src.adapters.base import (
    AuthFailedError,
    BrokerAdapter,
    BrokerError,
    PriceBarsDecodeError,
    SessionExpiredError,
    TransientBrokerError,
)
from src.adapters.ratelimit import RestBudget, RestPriority, rest_priority
from src.calendar.machine import SessionState
from src.db.arm_ports import ArmNodeRecordStore, NodeMdRecordRow
from src.db.epoch import Epoch
from src.dto import OrderDTO, PositionDTO
from src.marketdata.ports import (
    BarsResult,
    HandleRead,
    HandleRef,
    HandleStatus,
    NodeMdRecord,
    StopHandleRead,
    StopHandleReadResult,
)
from src.marketdata.types import Side, Transient, Unsupported
from src.pubsub.orchestrator import RereadStatus, StopReadResult
from src.riskmgr.ports import FailedSpoolRecord, Pager
from src.riskmgr.types import ScopeKind
from src.runtime.supervisor import TaskContext

log = logging.getLogger(__name__)

BARS_READ_TIMEOUT_S: Final = 20.0
STOP_READ_TIMEOUT_S: Final = 20.0
PAGE_SEVERITIES: Final = frozenset({"info", "warning", "high", "critical"})

# Port value reasons (codes only).
UNKNOWN_ADAPTER: Final = "unknown_adapter"
NOT_SUPPORTED: Final = "not_supported"
BAD_REQUEST: Final = "bad_request"
DECODE: Final = "decode"
TIMEOUT: Final = "timeout"
REFUSED: Final = "refused"
AUTH: Final = "auth"
BROKER_ERROR: Final = "broker_error"
ERROR: Final = "error"


# ---------------------------------------------------------------------------------------------------- broker bars
class BrokerBarsPortImpl:
    """`BrokerBarsPort` over the adapters (`adapter_of(adapter_id)`, the orchestrator's runtime map)."""

    __slots__ = ("_adapter_of", "_timeout_s")

    def __init__(self, adapter_of: Callable[[str], BrokerAdapter], *,
                 timeout_s: float = BARS_READ_TIMEOUT_S) -> None:
        self._adapter_of = adapter_of
        self._timeout_s = timeout_s

    async def fetch_price_bars(self, adapter_id: str, symbol: str, side: Side, timeframe_s: int, *,
                               count: int | None = None, from_ms: int | None = None,
                               to_ms: int | None = None) -> BarsResult:
        try:
            adapter = self._adapter_of(adapter_id)
        except KeyError:
            return Unsupported(UNKNOWN_ADAPTER)
        try:
            with rest_priority(RestPriority.BARS):
                answer = await asyncio.wait_for(
                    adapter.fetch_price_bars(symbol, side.value, timeframe_s, count=count, from_ms=from_ms,
                                             to_ms=to_ms),
                    self._timeout_s)
        except ValueError:
            return Unsupported(BAD_REQUEST)
        except PriceBarsDecodeError:
            return Transient(DECODE)
        except TransientBrokerError:
            return Transient(REFUSED)                 # throttles, BARS-cap refusals, network errors
        except (AuthFailedError, SessionExpiredError):
            return Transient(AUTH)
        except BrokerError:
            return Transient(BROKER_ERROR)
        except (TimeoutError, OSError):
            return Transient(TIMEOUT)
        except Exception as exc:                      # never raise into md-bars (PR12)
            log.warning("%s: fetch_price_bars raised %s", adapter_id, type(exc).__name__)
            return Transient(ERROR)
        if not answer.supported:
            return Unsupported(NOT_SUPPORTED)
        return answer


# ---------------------------------------------------------------------------------------------------- stop handles
def _handle_keys(dto: OrderDTO | PositionDTO | object) -> list[HandleRef]:
    """The handles a listed row answers for: a position's own stop field, an order as a child order (and its own
    stop field, the POSITION_FIELD of an entry order)."""
    if isinstance(dto, PositionDTO):
        return [HandleRef("POSITION_FIELD", dto.position_id)]
    if isinstance(dto, OrderDTO):
        return [HandleRef("CHILD_ORDER", dto.broker_order_id), HandleRef("POSITION_FIELD", dto.broker_order_id)]
    return []


class StopHandleReadPortImpl:
    """`StopHandleReadPort` over `IngestionOrchestrator.reread_stop_orders` (one list read per adapter, at REST
    class BARS; every listed row versioned and published row by row like any REST row, origin `stop_read`)."""

    __slots__ = ("_reread", "_timeout_s")

    def __init__(self, reread: Callable[[str], Awaitable[StopReadResult]], *,
                 timeout_s: float = STOP_READ_TIMEOUT_S) -> None:
        self._reread = reread
        self._timeout_s = timeout_s

    async def read_stop_handles(self, adapter_id: str, handles: Sequence[HandleRef]) -> StopHandleReadResult:
        try:
            result = await asyncio.wait_for(self._reread(adapter_id), self._timeout_s)
        except KeyError:
            return Unsupported(UNKNOWN_ADAPTER)
        except TransientBrokerError:
            return Transient(REFUSED)
        except BrokerError:
            return Transient(BROKER_ERROR)
        except (TimeoutError, OSError):
            return Transient(TIMEOUT)
        except Exception as exc:
            log.warning("%s: stop-order list read raised %s", adapter_id, type(exc).__name__)
            return Transient(ERROR)
        if not result.supported:
            return Unsupported(NOT_SUPPORTED)
        found: dict[HandleRef, HandleRead] = {}
        for row in result.rows:
            for ref in _handle_keys(row.dto):
                if ref in found:
                    continue
                if row.status is RereadStatus.TRANSIENT:
                    found[ref] = HandleRead(ref, HandleStatus.TRANSIENT)
                elif row.status is RereadStatus.NOT_FOUND:
                    found[ref] = HandleRead(ref, HandleStatus.NOT_FOUND)
                else:
                    found[ref] = HandleRead(ref, HandleStatus.OK, sent_ms=row.sent_ms or result.sent_ms,
                                            version=row.version)
        # A pinned handle the list does not name is not active at the broker at the read's send time (PR9); the ARM
        # still uses only levels PostgreSQL has committed, and nothing absent is ever closed.
        out = tuple(found.get(h) or HandleRead(h, HandleStatus.NOT_FOUND) for h in handles)
        return StopHandleRead(adapter_id=adapter_id, handles=out, sent_ms=result.sent_ms)


# ---------------------------------------------------------------------------------------------------- calendar, health
class CalendarPortImpl:
    """`CalendarPort` over `MarketCalendar.state` (None, and any error, counts as closed; IN-10)."""

    __slots__ = ("_state",)

    def __init__(self, state: Callable[[str, str], SessionState | None]) -> None:
        self._state = state

    def state(self, adapter_id: str, symbol: str) -> SessionState | None:
        try:
            return self._state(adapter_id, symbol)
        except Exception:
            return None


class AdapterHealthPortImpl:
    """`AdapterHealthPort` (§B14, R11-M10): SM4 STREAMING and the S06 REST outcome record."""

    __slots__ = ("_streaming", "_budget_of", "_clock_ms")

    def __init__(self, streaming: Callable[[str], bool], budget_of: Callable[[str], RestBudget | None], *,
                 clock_ms: Callable[[], int]) -> None:
        self._streaming = streaming
        self._budget_of = budget_of
        self._clock_ms = clock_ms

    def is_streaming(self, adapter_id: str) -> bool:
        try:
            return bool(self._streaming(adapter_id))
        except Exception:
            return False

    def rest_failing_everywhere(self, adapter_id: str) -> bool | None:
        try:
            budget = self._budget_of(adapter_id)
            if budget is None:
                return None                           # unmetered adapter: no outcome record, unknown
            return budget.stats().failing_at_every_priority(self._clock_ms())
        except Exception:
            return None


class PgHealthPortImpl:
    """`PgHealthPort` (§B14, R11-M11): the term pool's last fenced commit and the RiskSweep's unreachable alert."""

    __slots__ = ("_last_commit", "_unreachable")

    def __init__(self, last_fenced_commit_ms: Callable[[], int | None],
                 sweep_pg_unreachable: Callable[[], bool]) -> None:
        self._last_commit = last_fenced_commit_ms
        self._unreachable = sweep_pg_unreachable

    def last_fenced_commit_ms(self) -> int | None:
        try:
            return self._last_commit()
        except Exception:
            return None

    def sweep_pg_unreachable(self) -> bool:
        try:
            return bool(self._unreachable())
        except Exception:
            return True                               # unknown counts as down (fail closed)


class StampPortImpl:
    """`StampPort` over the supervisor's `TaskContext`."""

    __slots__ = ("_stamp", "_exempt")

    def __init__(self, ctx: TaskContext) -> None:
        self._stamp = ctx.stamp
        self._exempt = ctx.set_exempt

    def stamp(self) -> None:
        self._stamp()

    def set_exempt(self, reason: str | None) -> None:
        self._exempt(reason)


# ---------------------------------------------------------------------------------------------------- pages
def _log_critical(title: str, body: str) -> None:
    log.critical("ARM FAILED %s: %s", title, body)


class PagerWrapper(Pager):
    """The `Pager` handed to the scoped packages (also a `PagePort`). `page()` forwards to the runtime's page
    callable (`EngineSession.page`); `log_mirror()` is `log.critical` only, the FAILED ping's log copy (its critical
    copy is the outbox row of the episode's own fenced transaction, R11-M05)."""

    __slots__ = ()

    def __init__(self, page: Callable[[str, str, str], None],
                 mirror: Callable[[str, str], None] = _log_critical) -> None:
        super().__init__(page, mirror)

    def page(self, severity: str, title: str, body: str) -> None:
        if severity not in PAGE_SEVERITIES:
            raise ValueError("severity must be info, warning, high or critical")
        self._page(severity, title, body)


# ---------------------------------------------------------------------------------------------------- leader, record
class LeaderCheckPortImpl:
    """`LeaderCheckPort` of the md bootstrap: the lease is held and, when a reader is given, `engine_epoch` in
    PostgreSQL is this term's epoch. Any error is False (fail closed)."""

    __slots__ = ("_lease_valid", "_epoch", "_read_epoch")

    def __init__(self, lease_valid: Callable[[], bool], epoch: Epoch,
                 read_epoch: Callable[[], Awaitable[Epoch | None]] | None = None) -> None:
        self._lease_valid = lease_valid
        self._epoch = epoch
        self._read_epoch = read_epoch

    async def confirm_leader(self) -> bool:
        try:
            if not self._lease_valid():
                return False
            if self._read_epoch is None:
                return True
            stored = await self._read_epoch()
        except Exception:
            return False
        return stored is not None and stored == self._epoch


class NodeRecordPortImpl:
    """`NodeRecordPort` over this node's `arm_status` row (`ArmNodeRecordStore`, S24).

    Two names for one node: `node` is the `arm_status` key (the raw `node_id`, as the status writer uses it) and
    `record_node` is the name the market-data bootstrap puts in its record (the writer-token form, `[A-Za-z0-9_-]`;
    default `node`). The record is saved under `node` and read back carrying `record_node`, so a hostname with dots
    or colons never makes every save fail."""

    __slots__ = ("_store", "_node", "_record_node")

    def __init__(self, store: ArmNodeRecordStore, node: str, *, record_node: str | None = None) -> None:
        self._store = store
        self._node = node
        self._record_node = node if record_node is None else record_node

    async def load_record(self) -> NodeMdRecord | None:
        row = await self._store.load_md_record(self._node)
        if row is None:
            return None
        return NodeMdRecord(node=self._record_node, sentinel=row.sentinel, last_write_ms=row.last_write_ms)

    async def save_record(self, record: NodeMdRecord) -> None:
        if record.node != self._record_node:
            raise ValueError("a node saves only its own market-data record")
        await self._store.save_md_record(NodeMdRecordRow(node=self._node, sentinel=record.sentinel,
                                                         last_write_ms=record.last_write_ms))


# ---------------------------------------------------------------------------------------------------- FAILED spool
SPOOL_MAGIC: Final = b"XFS1"
_HEADER: Final = struct.Struct(">4sII")              # magic, payload length, CRC-32 of the payload
MAX_FRAME_BYTES: Final = 64 * 1024
KIND_RECORD: Final = "record"
KIND_ACK: Final = "ack"


def encode_frame(payload: dict[str, object]) -> bytes:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(body) > MAX_FRAME_BYTES:
        raise ValueError("spool frame too large")
    return _HEADER.pack(SPOOL_MAGIC, len(body), zlib.crc32(body)) + body


def _frame_at(data: bytes, pos: int) -> tuple[int, bytes, bool] | None:
    """The frame whose header starts at `pos`: (its end, its payload, CRC good); None when no frame can be framed
    there (a bad magic, an oversize or overrunning length, or a header cut short)."""
    if len(data) - pos < _HEADER.size:
        return None
    magic, length, crc = _HEADER.unpack_from(data, pos)
    start = pos + _HEADER.size
    if magic != SPOOL_MAGIC or length > MAX_FRAME_BYTES or start + length > len(data):
        return None
    body = data[start:start + length]
    return start + length, body, zlib.crc32(body) == crc


def _resync(data: bytes, pos: int) -> int:
    """The offset of the next frame after `pos` whose header and CRC both check out; len(data) when none does."""
    at = data.find(SPOOL_MAGIC, pos + 1)
    while at != -1:
        frame = _frame_at(data, at)
        if frame is not None and frame[2]:
            return at
        at = data.find(SPOOL_MAGIC, at + 1)
    return len(data)


def scan_frames(data: bytes) -> tuple[list[dict[str, object]], int, int]:
    """Every intact frame, the count of bad ones, and the end of the last intact frame (the last good boundary).

    A CRC mismatch, a bad magic, an overrunning length or a cut header is a damaged region (one bad): the read
    resyncs at the next `XFS1` whose length and CRC check out, so a torn append in the middle of the file never
    hides the records and acks written after it."""
    out: list[dict[str, object]] = []
    bad = 0
    pos = 0
    good_end = 0
    while pos < len(data):
        frame = _frame_at(data, pos)
        if frame is None:
            bad += 1
            pos = _resync(data, pos)
            continue
        end, body, crc_ok = frame
        if not crc_ok:
            # A torn append's header may declare a length that runs into the frames written after it: never trust
            # the length of a frame whose CRC fails, resync at the next sound frame instead.
            bad += 1
            pos = _resync(data, pos)
            continue
        pos = good_end = end
        try:
            item = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            bad += 1
            continue
        if isinstance(item, dict):
            out.append(item)
        else:
            bad += 1
    return out, bad, good_end


def decode_frames(data: bytes) -> tuple[list[dict[str, object]], int]:
    """Every intact frame plus the count of bad ones (`scan_frames` without the boundary)."""
    frames, bad, _ = scan_frames(data)
    return frames, bad


def record_payload(r: FailedSpoolRecord) -> dict[str, object]:
    return {"kind": KIND_RECORD, "failure_id": r.failure_id, "scope_kind": r.scope_kind.value,
            "scope_id": r.scope_id, "reason": r.reason, "probe_kind": r.probe_kind, "ping_id": r.ping_id,
            "epoch": r.epoch, "first_fault_at_ms": r.first_fault_at_ms, "entered_at_ms": r.entered_at_ms,
            "counters": [[k, v] for k, v in r.counters], "position_ids": list(r.position_ids),
            "edge_code": r.edge_code}


def _int(v: object) -> int:
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError("not an integer")
    return v


def _str(v: object) -> str:
    if not isinstance(v, str):
        raise ValueError("not a string")
    return v


def record_of_payload(p: dict[str, object]) -> FailedSpoolRecord:
    counters = p.get("counters") or []
    positions = p.get("position_ids") or []
    if not isinstance(counters, list) or not isinstance(positions, list):
        raise ValueError("malformed spool record")
    edge = p.get("edge_code")
    return FailedSpoolRecord(
        failure_id=_str(p.get("failure_id")), scope_kind=ScopeKind(_str(p.get("scope_kind"))),
        scope_id=_str(p.get("scope_id")), reason=_str(p.get("reason")), probe_kind=_str(p.get("probe_kind")),
        ping_id=_str(p.get("ping_id")), epoch=_int(p.get("epoch")),
        first_fault_at_ms=_int(p.get("first_fault_at_ms")), entered_at_ms=_int(p.get("entered_at_ms")),
        counters=tuple((_str(c[0]), _int(c[1])) for c in counters if isinstance(c, list) and len(c) == 2),
        position_ids=tuple(_str(x) for x in positions), edge_code=None if edge is None else _str(edge))


class FailedSpoolPortImpl:
    """`FailedSpoolPort`: an append-only local file of CRC-framed JSON frames (ids, codes, counts and times only).

    `append` writes one record frame and fsyncs before it returns; `acknowledge` appends an ack frame naming the
    failure ids whose `arm_failures` row committed, and truncates the file once nothing readable is left
    unacknowledged; `replay` returns the latest record of each failure id not acknowledged, oldest first. Before
    every write a damaged tail (a torn append: bytes after the last intact frame) is cut back to the last good frame
    boundary, so a new frame never lands behind the damage; damage in the middle of the file is skipped by the
    read's resync (`scan_frames`). A damaged spool is paged once per distinct damage count (`pager`), not once per
    replay. File I/O runs in a thread (never on the event loop)."""

    __slots__ = ("_path", "_page", "_lock", "_paged_bad")

    def __init__(self, path: Path, *, pager: Callable[[str, str, str], None] | None = None) -> None:
        self._path = path
        self._page = pager
        self._lock = asyncio.Lock()
        self._paged_bad = 0

    @property
    def path(self) -> Path:
        return self._path

    def _fsync_dir(self) -> None:
        """Make a new file's directory entry durable (best effort: Windows cannot open a directory for fsync)."""
        try:
            fd = os.open(self._path.parent, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)

    def _write(self, frame: bytes) -> int:
        """Append `frame`, first cutting a damaged tail back to the last good boundary; returns the bytes cut."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        created = not self._path.exists()
        cut = 0
        if not created:
            data = self._path.read_bytes()
            _, _, good_end = scan_frames(data)
            if good_end < len(data):
                cut = len(data) - good_end
                with open(self._path, "r+b") as f:
                    f.truncate(good_end)
                    f.flush()
                    os.fsync(f.fileno())
        with open(self._path, "ab") as f:
            f.write(frame)
            f.flush()
            os.fsync(f.fileno())
        if created:
            self._fsync_dir()
        return cut

    def _read(self) -> bytes:
        try:
            return self._path.read_bytes()
        except FileNotFoundError:
            return b""

    def _truncate(self) -> None:
        with open(self._path, "wb") as f:
            f.flush()
            os.fsync(f.fileno())

    def _note_damage(self, bad: int, detail: str) -> None:
        """Page a damaged spool once per distinct damage count (never once per replay or per ARM retry)."""
        if bad <= 0:
            self._paged_bad = 0
            return
        if bad == self._paged_bad:
            return
        self._paged_bad = bad
        if self._page is not None:
            self._page("warning", "ARM FAILED spool damaged", f"{bad} spool frame(s) could not be read; {detail}")

    def _note_cut(self, cut: int) -> None:
        if cut and self._page is not None:
            self._page("warning", "ARM FAILED spool damaged",
                       "a torn spool tail was cut back to the last intact frame before a write")

    async def append(self, record: FailedSpoolRecord) -> None:
        frame = encode_frame(record_payload(record))
        async with self._lock:
            cut = await asyncio.to_thread(self._write, frame)
        self._note_cut(cut)

    def _pending(self, data: bytes) -> tuple[list[FailedSpoolRecord], int]:
        frames, bad = decode_frames(data)
        latest: dict[str, tuple[int, FailedSpoolRecord]] = {}
        acked: set[str] = set()
        for i, f in enumerate(frames):
            kind = f.get("kind")
            if kind == KIND_ACK:
                ids = f.get("failure_ids")
                if isinstance(ids, list):
                    for fid in ids:
                        if isinstance(fid, str):
                            acked.add(fid)
                            latest.pop(fid, None)
                continue
            try:
                rec = record_of_payload(f)
            except (ValueError, TypeError, IndexError):
                bad += 1
                continue
            acked.discard(rec.failure_id)              # spooled again after an ack: pending again
            first = latest.get(rec.failure_id, (i, rec))[0]
            latest[rec.failure_id] = (first, rec)
        return [r for _, r in sorted(latest.values(), key=lambda t: t[0])], bad

    async def replay(self) -> Sequence[FailedSpoolRecord]:
        async with self._lock:
            data = await asyncio.to_thread(self._read)
        records, bad = self._pending(data)
        self._note_damage(bad, "they were skipped")
        return records

    async def acknowledge(self, failure_ids: Collection[str]) -> None:
        ids = sorted({f for f in failure_ids if f})
        if not ids:
            return
        async with self._lock:
            cut = await asyncio.to_thread(self._write, encode_frame({"kind": KIND_ACK, "failure_ids": ids}))
            data = await asyncio.to_thread(self._read)
            records, bad = self._pending(data)
            if not records:
                # Nothing readable is pending: the damaged frames (unreadable anyway) go with the file.
                await asyncio.to_thread(self._truncate)
        self._note_cut(cut)
        if not records:
            self._note_damage(bad, "they were discarded when the spool was emptied")
            self._paged_bad = 0


__all__ = [
    "AdapterHealthPortImpl", "BrokerBarsPortImpl", "CalendarPortImpl", "FailedSpoolPortImpl",
    "LeaderCheckPortImpl", "NodeRecordPortImpl", "PagerWrapper", "PgHealthPortImpl", "StampPortImpl",
    "StopHandleReadPortImpl", "decode_frames", "encode_frame", "scan_frames",
]
