"""Read-only PR3 price-bar probe: ``python -m src.adapters.stonex.probe_bars`` (design PR3, plan S03).

Reads StoneXSettings.from_env() (environment variables only, never .env files), logs on and measures, for each
(symbol, side, timeframe): whether the broker returns bars, the answer's field names, whether bars name their side
(and the distinct raw tag values), the bar time convention (bucket start or end: one vote from the forming bar when it
is dated on the bucket grid, one independent vote from the newest complete bar), the hard bar checks, and the finality
lag (bucket T read at +5 s, +30 s and +5 min after its end: present? the same as the read before?). Without a known
convention the finality is reported under both hypotheses (``finality_lag_s_if_start`` / ``_if_end``), never as a
broker result. The range form is read once per series. A session that expires mid-run is logged on again once per
failed read (``relogons`` in the report). Optionally it samples the price stream for TickDateRaw resolution, the
broker-time share and the receipt - broker time lag. Bars are read through the adapter's ``fetch_price_bars`` (the
StoneX capability since S39). ``--check-stops`` runs only the PR9 check (plan S39): whether the stop children of the
open positions appear in ``activestoplimitorders``, by order id only (``check_stop_children``).

Run it with the engine stopped (single-PC mode), or only after confirming that StoneX allows concurrent sessions for
the read-only user: the probe opens its own LogOn and Lightstreamer session on the same credentials, which may
displace the running engine's session.

The JSON report holds timings, flags, field names and counts only: never a price, a spread or an account value
(``price_fields_in`` refuses to write one). Every request goes through the adapter's read methods, so through its
existing ``ReadOnlyTransport`` and REST budget; the bar reads run under ``rest_get_only()`` (any non-GET is refused
before it is sent). The probe builds no HTTP client of its own and never writes its report under the repository or a
OneDrive folder unless ``--out`` names the path. It never prints the password, app key or session token.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import sys
import tempfile
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from src.adapters.base import BrokerAdapter, BrokerError, SessionExpiredError, StreamEvent, StreamEventKind
from src.adapters.ratelimit import RestPriority, rest_priority
from src.adapters.stonex.streaming import now_ms
from src.config import StoneXSettings
from src.dto import BAR_SIDES, BarSide, BrokerBar, PriceBarsResponse, PriceDTO

log = logging.getLogger(__name__)

PROBE_NAME = "pr3-price-bars"
PROBE_VERSION = 1
DEFAULT_OFFSETS_S: tuple[int, ...] = (5, 30, 300)
DEFAULT_TIMEFRAMES_S: tuple[int, ...] = (60, 900)
DEFAULT_SIDES: tuple[BarSide, ...] = ("BID", "ASK")
#: Report keys that would hold a market value: the report must never carry one (``price_fields_in``).
PRICE_KEYS = frozenset({"open", "high", "low", "close", "bid", "ask", "mid", "spread", "spread_pips", "price",
                        "prices", "ohlc", "balance", "equity"})
#: The PR9 stop-children item, measured only by ``--check-stops`` (then left out of ``not_measured``).
STOPS_NOT_MEASURED = ("whether the stop children of open positions appear in activestoplimitorders "
                      "(run the probe with --check-stops)")
#: Ids listed per group in the ``--check-stops`` report (order ids only, never a price).
MAX_STOP_IDS = 50
#: PR3 items this probe cannot measure by itself (manual or multi-day work, recorded in the design by hand).
NOT_MEASURED: tuple[str, ...] = (
    "15m bid bars covering 16:45-17:15 New York against tradeable-only TICK bars on several days (roll prints)",
    "whether a manual stop move appears on the ORDERS stream",
    "whether a REST order read's LastChangedDateTimeUTC moves with every stop amendment",
    "Open LastChangedTime against ExecutedDateTimeUtc",
    "OpenPrice across the daily roll",
    "TLS verify-full against the Supabase host",
    "tick delivery with XTNL_STONEX_PRICE_UNFILTERED=1 against 0 (run the probe once with each value)",
    STOPS_NOT_MEASURED,
)

BarReader = Callable[..., Awaitable[PriceBarsResponse]]
Content = tuple[Decimal, Decimal, Decimal, Decimal]
#: Side tags the probe can read as a side (``B`` / ``A`` short forms, StoneX ``OFFER`` for the ask). Any other tag
#: (e.g. a numeric PriceTypeId) is "tagged, form unknown": never counted as a mismatch.
SIDE_TAG_FORMS: dict[str, BarSide] = {"BID": "BID", "B": "BID", "ASK": "ASK", "A": "ASK", "OFFER": "ASK"}
#: Bounds on the distinct raw side-tag values the report keeps (short strings, never a number with a point).
MAX_TAG_VALUES = 8
MAX_TAG_LEN = 16


def bar_reader(adapter: BrokerAdapter) -> tuple[str, BarReader]:
    """The adapter's bar read: ``fetch_price_bars`` (StoneX since S39; before it the probe used a separate
    ``probe_price_bars`` read, now removed)."""
    return "fetch_price_bars", adapter.fetch_price_bars


def percentile(values: Sequence[float], q: float) -> float | None:
    """Nearest-rank percentile (``q`` in 0..100); None without values."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(q / 100 * len(ordered)))
    return ordered[min(rank, len(ordered)) - 1]


def price_fields_in(report: object, path: str = "$") -> list[str]:
    """Where the report would carry a market value: a key named like a price, or a Decimal anywhere. Empty for a
    clean report."""
    found: list[str] = []
    if isinstance(report, Decimal):
        found.append(path)
    elif isinstance(report, Mapping):
        for key, value in report.items():
            if str(key).lower() in PRICE_KEYS:
                found.append(f"{path}.{key}")
            found.extend(price_fields_in(value, f"{path}.{key}"))
    elif isinstance(report, (list, tuple)):
        for i, value in enumerate(report):
            found.extend(price_fields_in(value, f"{path}[{i}]"))
    return found


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {str(exc)[:200]}".rstrip(": ")


@dataclass
class _Read:
    """One count-form read (kept in memory only; the report keeps flags and counts)."""

    offset_s: int
    ok: bool
    latency_ms: int
    error: str = ""
    bars: int = 0
    partial_present: bool = False
    truncated: bool = False
    received_ms: int = 0
    content: dict[int, Content] = field(default_factory=dict)     # complete bars by bucket time (never reported)
    partial_start: int | None = None


@dataclass
class _Series:
    symbol: str
    side: BarSide
    timeframe_s: int
    supported: bool = True
    note: str = ""
    field_names: set[str] = field(default_factory=set)
    side_tagged: int = 0
    side_matches: int = 0
    side_mismatches: int = 0
    side_form_unknown: int = 0
    side_values: set[str] = field(default_factory=set)
    checks_failed: dict[str, int] = field(default_factory=lambda: {"price_not_positive": 0, "high_low_bounds": 0,
                                                                     "off_grid": 0})
    # partial_*: the forming bar's date (only when on the bucket grid; off-grid forming bars are only counted);
    # complete_*: the newest complete bar's date in the reads before the next bucket ends (independent of the first)
    convention_votes: dict[str, int] = field(default_factory=lambda: {
        "partial_start": 0, "partial_end": 0, "partial_off_grid": 0, "complete_start": 0, "complete_end": 0})
    rounds: list[tuple[int, list[_Read]]] = field(default_factory=list)
    range_form: dict[str, Any] | None = None


def _tag_value(tag: str) -> str:
    """The raw side tag as the report keeps it: short, and never anything that reads like a price."""
    if "." in tag or "," in tag:
        return "<decimal-like>"
    return tag[:MAX_TAG_LEN]


def _check_bar(series: _Series, bar: BrokerBar) -> None:
    if bar.side is not None:
        series.side_tagged += 1
        if len(series.side_values) < MAX_TAG_VALUES:
            series.side_values.add(_tag_value(bar.side))
        form = SIDE_TAG_FORMS.get(bar.side.strip().upper())
        if form is None:
            series.side_form_unknown += 1
        elif form == series.side:
            series.side_matches += 1
        else:
            series.side_mismatches += 1
    if bar.partial:
        return
    if min(bar.open, bar.high, bar.low, bar.close) <= 0:
        series.checks_failed["price_not_positive"] += 1
    if bar.high < max(bar.open, bar.close) or bar.low > min(bar.open, bar.close):
        series.checks_failed["high_low_bounds"] += 1
    if bar.bucket_start_ms % (series.timeframe_s * 1000) != 0:
        series.checks_failed["off_grid"] += 1


def _absorb(series: _Series, read: _Read, resp: PriceBarsResponse) -> None:
    tf_ms = series.timeframe_s * 1000
    if not resp.supported:
        series.supported, series.note = False, resp.note
    series.field_names.update(resp.field_names)
    read.bars = len(resp.bars)
    read.truncated = resp.truncated
    read.received_ms = resp.received_ms
    for bar in resp.bars:
        _check_bar(series, bar)
        if bar.partial:
            read.partial_present = True
            read.partial_start = bar.bucket_start_ms
            votes = series.convention_votes
            if bar.bucket_start_ms % tf_ms != 0:
                # dated by its last tick (or otherwise off the grid): says nothing about the bucket convention
                votes["partial_off_grid"] += 1
            elif bar.bucket_start_ms <= resp.received_ms < bar.bucket_start_ms + tf_ms:
                votes["partial_start"] += 1
            elif bar.bucket_start_ms - tf_ms <= resp.received_ms < bar.bucket_start_ms:
                votes["partial_end"] += 1
        else:
            read.content[bar.bucket_start_ms] = (bar.open, bar.high, bar.low, bar.close)


def _vote_complete(series: _Series, bucket_end_ms: int, reads: Sequence[_Read]) -> None:
    """The second, independent convention vote, from the complete bars of one round's reads taken before the next
    bucket ends. A complete bar dated ``bucket_end_ms`` can only be bucket T dated by its end (under 'start' that date
    is the bucket still forming): 'end'. Otherwise the newest of those reads holding T dated by its start
    (``bucket_end_ms - tf``) votes 'start'. No vote when neither date is there."""
    tf_ms = series.timeframe_s * 1000
    early = [r for r in reads if r.ok and r.received_ms < bucket_end_ms + tf_ms]
    if any(bucket_end_ms in r.content for r in early):
        series.convention_votes["complete_end"] += 1
    elif early and bucket_end_ms - tf_ms in early[-1].content:
        series.convention_votes["complete_start"] += 1


def _verdict(start: int, end: int) -> str:
    return "start" if start > end else "end" if end > start else "unknown"


def _convention(series: _Series) -> str:
    """'start' / 'end' when the two votes agree, or when one of them is silent; 'unknown' otherwise."""
    votes = series.convention_votes
    partial = _verdict(votes["partial_start"], votes["partial_end"])
    complete = _verdict(votes["complete_start"], votes["complete_end"])
    if partial == "unknown":
        return complete
    if complete in ("unknown", partial):
        return partial
    return "unknown"          # the two votes disagree


def _round_report(series: _Series, bucket_end_ms: int, reads: list[_Read], convention: str) -> dict[str, Any]:
    """Bucket T is the one ending at ``bucket_end_ms``. With a known convention it is dated by its start ('start')
    or its end ('end'); with an unknown one the round is measured under both hypotheses and never called final or
    never final."""
    if convention not in ("start", "end"):
        return {"bucket_end_ms": bucket_end_ms, "status": "convention_unknown", "finality_lag_s": None,
                "if_start": _round_under(series, bucket_end_ms, reads, "start", hypothetical=True),
                "if_end": _round_under(series, bucket_end_ms, reads, "end", hypothetical=True)}
    return {"bucket_end_ms": bucket_end_ms, **_round_under(series, bucket_end_ms, reads, convention)}


def _round_under(series: _Series, bucket_end_ms: int, reads: list[_Read], convention: str, *,
                 hypothetical: bool = False) -> dict[str, Any]:
    """status: 'final' (lag measured), 'never_final' / 'not_final' (the last read succeeded without T settled;
    'not_final' under a hypothesis) or 'unmeasured' (no read, or the last read failed: no finality result)."""
    tf_ms = series.timeframe_s * 1000
    key = bucket_end_ms if convention == "end" else bucket_end_ms - tf_ms
    contents = [r.content.get(key) if r.ok else None for r in reads]
    out_reads: list[dict[str, Any]] = []
    previous: Content | None = None
    for read, content in zip(reads, contents, strict=True):
        same: bool | None = None if content is None or previous is None else content == previous
        out_reads.append({
            "offset_s": read.offset_s, "ok": read.ok, "error": read.error, "latency_ms": read.latency_ms,
            "bars": read.bars, "partial_present": read.partial_present, "truncated": read.truncated,
            "bucket_present": content is not None,
            "next_bucket_present": read.ok and (key + tf_ms in read.content or read.partial_start == key + tf_ms),
            "same_as_previous": same,
        })
        if content is not None:
            previous = content
    final = contents[-1] if contents else None
    lag: int | None = None
    if not reads or not reads[-1].ok:
        status = "unmeasured"
    elif final is None:
        status = "not_final" if hypothetical else "never_final"
    else:
        status = "final"
        for i, read in enumerate(reads):
            if all(c == final for c in contents[i:]):
                lag = read.offset_s
                break
    return {"reads": out_reads, "finality_lag_s": lag, "status": status}


def _lag_summary(rounds: Sequence[Mapping[str, Any]], unsettled: str) -> dict[str, Any]:
    lags = [float(r["finality_lag_s"]) for r in rounds if r["status"] == "final"]
    return {"p50": percentile(lags, 50), "p99": percentile(lags, 99), "samples": len(lags),
            unsettled: sum(1 for r in rounds if r["status"] == unsettled),
            "unmeasured": sum(1 for r in rounds if r["status"] == "unmeasured")}


def _series_report(series: _Series) -> dict[str, Any]:
    convention = _convention(series)
    rounds = [_round_report(series, end, reads, convention) for end, reads in series.rounds]
    out: dict[str, Any] = {
        "symbol": series.symbol, "side": series.side, "timeframe_s": series.timeframe_s,
        "supported": series.supported, "note": series.note,
        "field_names": sorted(series.field_names),
        "side_tag": {"bars_tagged": series.side_tagged, "matches": series.side_matches,
                     "mismatches": series.side_mismatches, "form_unknown": series.side_form_unknown,
                     "values": sorted(series.side_values)},
        "time_convention": convention, "convention_votes": dict(series.convention_votes),
        "checks_failed": dict(series.checks_failed),
        "range_form": series.range_form,
        "rounds": rounds,
    }
    if convention in ("start", "end"):
        out["finality_lag_s"] = _lag_summary(rounds, "never_final")
    else:
        # no finality result without a convention: both hypotheses, never a "never final" broker verdict
        out["finality_lag_s"] = None
        out["finality_lag_s_if_start"] = _lag_summary([r["if_start"] for r in rounds], "not_final")
        out["finality_lag_s_if_end"] = _lag_summary([r["if_end"] for r in rounds], "not_final")
    return out


class _Relogon:
    """Logs the adapter on again after a session expiry (once per failed read) and counts it for the report."""

    def __init__(self, adapter: BrokerAdapter) -> None:
        self.adapter = adapter
        self.count = 0
        self.failures = 0

    async def __call__(self) -> bool:
        try:
            await self.adapter.connect()
        except BrokerError as exc:
            self.failures += 1
            log.info("PR3 probe: re-logon failed: %s", _error_text(exc))
            return False
        self.count += 1
        return True


async def _bars_once(reader: BarReader, series: _Series, relogon: _Relogon | None,
                     **form: int) -> PriceBarsResponse:
    """One bar read at the probe's (lowest) REST priority; a session expiry logs on again and retries once."""
    try:
        with rest_priority(RestPriority.POLL):
            return await reader(series.symbol, series.side, series.timeframe_s, **form)
    except SessionExpiredError:
        if relogon is None or not await relogon():
            raise
    with rest_priority(RestPriority.POLL):
        return await reader(series.symbol, series.side, series.timeframe_s, **form)


async def _read(reader: BarReader, series: _Series, offset_s: int, count: int,
                clock_ms: Callable[[], int], relogon: _Relogon | None = None) -> _Read:
    started = clock_ms()
    try:
        resp = await _bars_once(reader, series, relogon, count=count)
    except (BrokerError, ValueError) as exc:
        return _Read(offset_s, ok=False, latency_ms=max(0, clock_ms() - started), error=_error_text(exc))
    read = _Read(offset_s, ok=resp.supported, latency_ms=max(0, clock_ms() - started))
    _absorb(series, read, resp)
    return read


async def _read_range(reader: BarReader, series: _Series, bucket_end_ms: int,
                      clock_ms: Callable[[], int], relogon: _Relogon | None = None) -> dict[str, Any]:
    """The range form over the three buckets before T's end: do the bars stay inside it and on the grid?"""
    tf_ms = series.timeframe_s * 1000
    from_ms, to_ms = bucket_end_ms - 3 * tf_ms, bucket_end_ms - tf_ms
    started = clock_ms()
    try:
        resp = await _bars_once(reader, series, relogon, from_ms=from_ms, to_ms=to_ms)
    except (BrokerError, ValueError) as exc:
        return {"ok": False, "error": _error_text(exc), "latency_ms": max(0, clock_ms() - started)}
    series.field_names.update(resp.field_names)
    complete = [b for b in resp.bars if not b.partial]
    return {
        "ok": resp.supported, "error": "" if resp.supported else resp.note, "latency_ms": max(0, clock_ms() - started),
        "from_ms": from_ms, "to_ms": to_ms, "bars": len(complete), "partial_present": len(complete) < len(resp.bars),
        "outside_range": sum(1 for b in complete if not from_ms <= b.bucket_start_ms <= to_ms),
        "off_grid": sum(1 for b in complete if b.bucket_start_ms % tf_ms != 0),
        "truncated": resp.truncated,
    }


async def _sample_ticks(adapter: BrokerAdapter, symbols: Sequence[str], seconds: float,
                        clock_ms: Callable[[], int]) -> dict[str, Any]:
    """Count ticks for ``seconds``: broker-timed share, TickDateRaw resolution, receipt - broker time lag."""
    per_symbol: dict[str, int] = {}
    broker_ts: list[int] = []
    lags: list[float] = []
    receipt_timed = untradeable = subscribed = 0
    markets_named: set[str] = set()
    error = ""
    deadline = clock_ms() + int(seconds * 1000)
    stream = adapter.stream_ticks(list(symbols))
    try:
        while (remaining := deadline - clock_ms()) > 0:
            try:
                item = await asyncio.wait_for(anext(stream), timeout=remaining / 1000)
            except (TimeoutError, StopAsyncIteration):
                break
            if isinstance(item, StreamEvent):
                if item.kind is StreamEventKind.SUBSCRIBED:
                    subscribed += 1
                    if item.market is not None:
                        markets_named.add(item.market)
                continue
            if not isinstance(item, PriceDTO):
                continue
            per_symbol[item.symbol] = per_symbol.get(item.symbol, 0) + 1
            if not item.tradeable:
                untradeable += 1
            if item.ts_source == "BROKER":
                broker_ts.append(item.timestamp_ms)
                if item.received_ms is not None:
                    lags.append(float(item.received_ms - item.timestamp_ms))
            else:
                receipt_timed += 1
    except BrokerError as exc:
        error = _error_text(exc)
    finally:
        aclose = getattr(stream, "aclose", None)
        if callable(aclose):
            await aclose()
    resolution: int | None = None
    if broker_ts:
        resolution = next((r for r in (1000, 100, 10) if all(t % r == 0 for t in broker_ts)), 1)
    drops = getattr(adapter, "dropped_price_frames", None)
    return {
        "seconds": seconds, "ticks": sum(per_symbol.values()), "ticks_per_symbol": per_symbol,
        "broker_timed": len(broker_ts), "receipt_timed": receipt_timed, "untradeable": untradeable,
        "tick_time_resolution_ms": resolution,
        "receipt_minus_broker_ms": {"p50": percentile(lags, 50), "p99": percentile(lags, 99), "samples": len(lags)},
        "subscribed_events": subscribed, "subscribed_markets_named": sorted(markets_named),
        "dropped_or_lost_per_symbol": dict(drops) if isinstance(drops, Mapping) else {},
        "error": error,
    }


async def check_stop_children(adapter: BrokerAdapter) -> dict[str, Any]:
    """PR9 (plan S39): whether the stop children of the open positions appear in the one list read the ARM's stop
    reads use (``fetch_active_stop_orders``, StoneX ``activestoplimitorders``). By id only: counts and order ids,
    never a price or a size. Two sources, reported apart:

    * ``open_orders`` (``stop_children``, ``listed``, ``missing``): the working STOP orders linked to an open position
      in ``fetch_open_orders``. On StoneX that read already merges ``activestoplimitorders``, so its children found
      there are listed by construction: it is no evidence on its own.
    * ``attached`` (``attached_*``): the StopOrder children of the ``openpositions`` rows themselves
      (``fetch_attached_stop_ids``, an adapter without it has no such source), the only independent evidence.

    ``conclusive`` only when the list read is supported, at least one attached child exists, and every open position
    has a stop child in one read or the other (``positions_without_stop_child`` 0); otherwise ``not_conclusive``
    says why (S39 review). ``XTNL_ARM_STOP_READS`` is decided from a conclusive report only."""
    out: dict[str, Any] = {"supported": True, "conclusive": False, "not_conclusive": "", "open_positions": 0,
                           "stop_children": 0, "listed": 0, "missing": 0, "listed_ids": [], "missing_ids": [],
                           "attached_source": False, "attached_children": 0, "attached_listed": 0,
                           "attached_missing": 0, "attached_listed_ids": [], "attached_missing_ids": [],
                           "positions_without_stop_child": 0, "error": ""}
    reader = getattr(adapter, "fetch_attached_stop_ids", None)
    attached_pairs: list[tuple[str, str]] | None = None
    try:
        positions = await adapter.fetch_positions()
        orders = await adapter.fetch_open_orders()
        active = await adapter.fetch_active_stop_orders()
        if callable(reader):
            attached_pairs = list(await reader())
    except BrokerError as exc:
        out["error"] = _error_text(exc)
        out["not_conclusive"] = "read_failed"
        return out
    open_ids = {p.position_id for p in positions if p.status == "OPEN"}
    stops = [o for o in orders
             if o.linked_position_id in open_ids and o.order_kind == "STOP" and not o.status.terminal]
    children = sorted({o.broker_order_id for o in stops})
    with_child = {o.linked_position_id for o in stops}
    in_list = {o.broker_order_id for o in active.orders}
    listed = [c for c in children if c in in_list]
    missing = [c for c in children if c not in in_list]
    attached = sorted({cid for cid, pid in attached_pairs or [] if pid in open_ids})
    with_child |= {pid for _cid, pid in attached_pairs or [] if pid in open_ids}
    a_listed = [c for c in attached if c in in_list]
    a_missing = [c for c in attached if c not in in_list]
    without = len(open_ids - with_child)
    reason = ("list_not_supported" if not active.supported
              else "no_attached_source" if attached_pairs is None
              else "no_attached_child" if not attached
              else "positions_without_stop_child" if without
              else "")
    out.update(supported=active.supported, conclusive=not reason, not_conclusive=reason,
               open_positions=len(open_ids), stop_children=len(children), listed=len(listed),
               missing=len(missing), listed_ids=listed[:MAX_STOP_IDS], missing_ids=missing[:MAX_STOP_IDS],
               attached_source=attached_pairs is not None, attached_children=len(attached),
               attached_listed=len(a_listed), attached_missing=len(a_missing),
               attached_listed_ids=a_listed[:MAX_STOP_IDS], attached_missing_ids=a_missing[:MAX_STOP_IDS],
               positions_without_stop_child=without)
    return out


def _under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def default_report_path(started_ms: int, *, temp_dir: str | None = None) -> Path | None:
    """``<temp>/xtnl-pr3-bars-<started_ms>.json``; None when the temp folder is inside the repository or a OneDrive
    folder (the caller must then pass ``--out``)."""
    path = Path(temp_dir if temp_dir is not None else tempfile.gettempdir()) / f"xtnl-pr3-bars-{started_ms}.json"
    repo = Path(__file__).resolve().parents[3]
    if _under(path, repo) or any("onedrive" in part.lower() for part in path.resolve().parts):
        return None
    return path


async def run(settings: StoneXSettings | None, *, adapter: BrokerAdapter | None = None,
              symbols: Sequence[str] = ("EURUSD",), sides: Iterable[BarSide] = DEFAULT_SIDES,
              timeframes_s: Iterable[int] = DEFAULT_TIMEFRAMES_S, offsets_s: Iterable[int] = DEFAULT_OFFSETS_S,
              rounds: int = 1, count: int = 5, tick_seconds: float = 60.0, out: str | Path | None = None,
              temp_dir: str | None = None, clock_ms: Callable[[], int] = now_ms,
              sleep: Callable[[float], Awaitable[None]] = asyncio.sleep, check_stops: bool = False) -> int:
    """Run the probe and write the report. 0 = written, 1 = broker failure before any measurement, 2 = no safe
    report path or a report that would carry a market value (never written). ``check_stops``: only the PR9
    stop-children check (``check_stop_children``); the bar rounds and the tick sample are skipped."""
    if adapter is None:
        if settings is None:
            raise ValueError("settings or adapter required")
        from src.adapters.stonex.adapter import StoneXAdapter   # the real adapter only when no adapter is given
        adapter = StoneXAdapter(settings)
    started = clock_ms()
    path = Path(out) if out is not None else default_report_path(started, temp_dir=temp_dir)
    if path is None:
        print("PR3 probe: the temp folder is inside the repository or OneDrive; pass --out <path>", file=sys.stderr)
        return 2
    reader_name, reader = bar_reader(adapter)
    tfs = sorted({int(t) for t in timeframes_s if int(t) > 0})
    offs = sorted({int(o) for o in offsets_s if int(o) >= 0})
    series = [] if check_stops else [_Series(sym, side, tf) for sym in symbols for side in sides
                                     if side in BAR_SIDES for tf in tfs]
    if check_stops:
        rounds, tick_seconds = 0, 0.0
    ticks: dict[str, Any] | None = None
    stops: dict[str, Any] | None = None
    relogon = _Relogon(adapter)
    try:
        await adapter.connect()
    except BrokerError as exc:
        print(f"PR3 probe failed: {_error_text(exc)}", file=sys.stderr)
        await _close_quietly(adapter)
        return 1
    try:
        if check_stops:
            stops = await check_stop_children(adapter)
        if tick_seconds > 0:
            ticks = await _sample_ticks(adapter, symbols, tick_seconds, clock_ms)
        step_ms = max(tfs) * 1000 if tfs else 60_000
        after = clock_ms()
        for round_no in range(max(0, rounds)):
            end = (after // step_ms + 1) * step_ms
            reads: dict[int, list[_Read]] = {id(s): [] for s in series}
            for offset in offs:
                wait_ms = end + offset * 1000 - clock_ms()
                if wait_ms > 0:
                    await sleep(wait_ms / 1000)
                for s in series:
                    # enough bars that bucket T is still in the answer at the last offset
                    need = max(count, max(offs, default=0) // s.timeframe_s + 3)
                    reads[id(s)].append(await _read(reader, s, offset, need, clock_ms, relogon))
            for s in series:
                s.rounds.append((end, reads[id(s)]))
                _vote_complete(s, end, reads[id(s)])
                if round_no == 0:
                    s.range_form = await _read_range(reader, s, end, clock_ms, relogon)
            after = clock_ms()
    finally:
        await _close_quietly(adapter)
    series_reports = [_series_report(s) for s in series]
    report: dict[str, Any] = {
        "probe": PROBE_NAME, "version": PROBE_VERSION, "adapter_id": adapter.adapter_id,
        "env": settings.env if settings is not None else "unknown",
        "started_ms": started, "finished_ms": clock_ms(),
        "flags": {"price_unfiltered": bool(settings.price_unfiltered) if settings is not None else False,
                  "bar_reader": reader_name, "offsets_s": offs, "rounds": rounds, "count": count,
                  "check_stops": check_stops},
        "relogons": {"count": relogon.count, "failed": relogon.failures},
        "series": series_reports,
        "ticks": ticks,
        "stop_children": stops,
        "not_measured": [n for n in NOT_MEASURED if not (check_stops and n == STOPS_NOT_MEASURED)],
    }
    leaks = price_fields_in(report)
    if leaks:
        print(f"PR3 probe: report refused, it would carry market values at {leaks[:5]}", file=sys.stderr)
        return 2
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    for sr in series_reports:
        lag = sr["finality_lag_s"]
        finality = (f"finality p50={lag['p50']} p99={lag['p99']} never_final={lag['never_final']} "
                    f"unmeasured={lag['unmeasured']}" if lag is not None else "finality: convention unknown")
        print(f"{sr['symbol']} {sr['side']} {sr['timeframe_s']}s: supported={sr['supported']} "
              f"convention={sr['time_convention']} {finality} checks_failed={sum(sr['checks_failed'].values())}")
    if stops is not None:
        print(f"stop children attached to openpositions rows: {stops['attached_listed']} of "
              f"{stops['attached_children']} listed in activestoplimitorders; all linked stop children: "
              f"{stops['listed']} of {stops['stop_children']} ({stops['open_positions']} open positions, "
              f"{stops['positions_without_stop_child']} without a stop child)")
        print("stop children check: conclusive" if stops["conclusive"]
              else f"stop children check: NOT conclusive ({stops['not_conclusive']}); "
                   "do not set XTNL_ARM_STOP_READS=1 from this report")
    print(f"report: {path}")
    return 0


async def _close_quietly(adapter: BrokerAdapter) -> None:
    try:
        await adapter.close()
    except BrokerError as exc:
        log.info("PR3 probe: close failed (ignored): %s", _error_text(exc))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only StoneX PR3 price-bar probe (no prices in the report)")
    parser.add_argument("--symbols", default="EURUSD", help="comma-separated canonical symbols")
    parser.add_argument("--sides", default="BID,ASK")
    parser.add_argument("--timeframes", default="60,900", help="bar timeframes in seconds")
    parser.add_argument("--offsets", default="5,30,300", help="read offsets after the bucket end, in seconds")
    parser.add_argument("--rounds", type=int, default=1, help="buckets measured per series")
    parser.add_argument("--count", type=int, default=5, help="bars per count-form read")
    parser.add_argument("--tick-seconds", type=float, default=60.0, help="price stream sample length (0 = skip)")
    parser.add_argument("--out", default=None, help="report path (default: the system temp folder)")
    parser.add_argument("--check-stops", action="store_true",
                        help="only check whether open positions' stop children appear in activestoplimitorders "
                             "(ids only; no bar rounds, no tick sample)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    settings = StoneXSettings.from_env()
    print(repr(settings))   # repr hides the password and app key
    sides = [cast(BarSide, s.strip().upper()) for s in args.sides.split(",") if s.strip().upper() in BAR_SIDES]
    return asyncio.run(run(
        settings, symbols=[s.strip().upper() for s in args.symbols.split(",") if s.strip()], sides=sides,
        timeframes_s=[int(t) for t in args.timeframes.split(",") if t.strip()],
        offsets_s=[int(o) for o in args.offsets.split(",") if o.strip()], rounds=args.rounds, count=args.count,
        tick_seconds=args.tick_seconds, out=args.out, check_stops=args.check_stops))


if __name__ == "__main__":
    raise SystemExit(main())
