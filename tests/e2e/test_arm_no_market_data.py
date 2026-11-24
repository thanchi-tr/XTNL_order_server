"""S38 (plan E2E; design §B3, §B8, §C3 "No market data in PostgreSQL"; R11-M14): after the full end-to-end run of
`tests/e2e/arm_rig.py`, no market value is anywhere PostgreSQL, a log or a page could keep it.

Scanned: every argument of every statement the ARM stores sent (every ARM column written, the ARM outbox INSERT
payload included, rolled back or not), every emulated ARM table after the run, every log record (DEBUG and up), every
page, every operator-request answer as the tracker hands it to its answer port (the `Recorder` stand-in, not the
`operator_requests` store: plan S38 deviation 8), and every ARM outbox body unsent, finished as `sent` and finished as
`expired` by the real `ArmRelay` (whose send-time renderer re-reads the hash-checked bar; what it renders goes to the
channel, never back to PostgreSQL).

Market values: the BID / ASK bars' open, high, low and close, both sides of every quote, the planted instrument
increment, the planted indicator quantum and the indicator value served to the evaluator. None of them, and none of
their distinctive digit runs (`PLANTED`), appears inside any decimal number written, and none appears scaled to an
integer (an `int` argument or a bare integer in a string equal to a market value × 10**k); the only decimals written are
order-state levels (entry, stops, proposed stop, handle levels), units and `current_risk_r` (σ(E − S) / R from order
state, quantised to 0.01). Of the bar close, a decision row keeps `trigger_met` and `borderline` only.

Synthetic instrument, ids, levels and times; no assertion message carries a value.
"""
from __future__ import annotations

import json
import logging
import re
from collections.abc import AsyncIterator, Iterable, Iterator
from decimal import Decimal, InvalidOperation
from typing import Any

import fakeredis
import pytest

from src.db import arm_outbox_store as O
from src.db.arm_outbox_store import PgArmOutboxStore
from src.notifications.arm_relay import ArmRelay, SendTimeRenderer
from src.oversee.notify import RecordingChannel
from src.riskmgr.tracker import TrackerRequest
from tests.db.arm_outbox_emulator import ArmOutboxEmulator
from tests.e2e.arm_rig import (
    CLOSE_ABOVE,
    CLOSE_AT_TARGET,
    CLOSE_BELOW,
    ENTRY,
    PLANT_ATR,
    PLANT_INCREMENT,
    PLANT_QUANTUM,
    PLANTED,
    QUOTE_PRE2,
    QUOTE_PRE3,
    SERIES_BID,
    SPREAD,
    STOP0,
    FILL,
    TF,
    E2e,
    PlantedIndicators,
    broker_bar,
    e2e_rig,
    engine_db,
    run_main_scenario,
)
from tests.marketdata.conftest import KEYRING
from tests.riskmgr.tracker_fakes import ADAPTER

D = Decimal
NUMBER = re.compile(r"[-+]?\d*\.\d+(?:[eE][-+]?\d+)?|\d+[eE][-+]?\d+")
"""A decimal number in text (hex digests and integer ids / times carry no decimal point)."""
MAX_ADJUSTED = 100
"""A token whose magnitude is beyond 1e100 is a hex-hash fragment (`03e4200890`), never a market value."""
INTEGER = re.compile(r"(?<![\w.+-])\d+(?![\w.])")
"""A bare integer in text (not part of a decimal, an exponent or a hex / word token)."""
MAX_SCALE = 8
"""A scaled price is an integer `value × 10**k`, k = 0..8 (every market value has at most 8 decimals). Integer
ids, sequence numbers, counts and epoch times never equal one: a time has 13 digits, a scaled market value at most
9, so the check needs no list of `*_ms` / `*_id` / `seq` fields to skip."""
DECISION_DECIMALS = {"proposed_stop", "current_risk_r", "first_bar_ratio"}
CLOSE_FIELDS = {"trigger_met", "borderline"}


class RecordingOutbox(ArmOutboxEmulator):
    """The relay's store with every statement's arguments kept."""

    def __init__(self) -> None:
        super().__init__(epoch=7)
        self.args: list[tuple[str, tuple[Any, ...]]] = []

    def _run(self, sql: str, args: tuple[Any, ...]) -> Any:
        self.args.append((sql, args))
        return super()._run(sql, args)


def market_values() -> set[Decimal]:
    """Every market value the run exposes to the md layer and the ARM."""
    out: set[Decimal] = {PLANT_INCREMENT, PLANT_QUANTUM, PLANT_ATR}
    for close in (CLOSE_BELOW, CLOSE_ABOVE, CLOSE_AT_TARGET):
        for c in (D(close), D(close) + SPREAD):
            b = broker_bar(SERIES_BID, 900_000, str(c))
            out |= {b.open, b.high, b.low, b.close}
    for q in (QUOTE_PRE2, QUOTE_PRE3, CLOSE_BELOW, CLOSE_ABOVE, CLOSE_AT_TARGET):
        out |= {D(q), D(q) + SPREAD}
    return {v.normalize() for v in out}


def walk(value: Any) -> Iterator[Any]:
    """Every leaf of a nested argument / row (dicts, lists, tuples, dataclass-free)."""
    if isinstance(value, dict):
        for k, v in value.items():
            yield from walk(k)
            yield from walk(v)
    elif isinstance(value, (list, tuple, set, frozenset)):
        for v in value:
            yield from walk(v)
    else:
        yield value


def numbers(leaves: Iterable[Any]) -> Iterator[tuple[str, Decimal | None]]:
    """(token, value) for every decimal number in the leaves: a `Decimal` / float, or a number inside a string
    (a JSON body, a log line, a page)."""
    for leaf in leaves:
        if isinstance(leaf, bool) or leaf is None or isinstance(leaf, int):
            continue
        if isinstance(leaf, (Decimal, float)):
            yield str(leaf), D(str(leaf))
            continue
        if isinstance(leaf, bytes):
            leaf = leaf.decode("utf-8", "replace")
        for token in NUMBER.findall(str(leaf)):
            try:
                value = D(token)
            except InvalidOperation:
                yield token, None
                continue
            # a hex hash fragment such as `03e4200890` reads as 3E+4200890: no price, and `normalize()` would
            # overflow on it; its digits are still checked for planted runs
            yield token, value if abs(value.adjusted()) <= MAX_ADJUSTED else None


def integers(leaves: Iterable[Any]) -> Iterator[int]:
    """Every integer in the leaves: an `int` leaf, or a bare integer inside a string (a JSON body, a log line)."""
    for leaf in leaves:
        if isinstance(leaf, bool) or leaf is None:
            continue
        if isinstance(leaf, int):
            yield leaf
            continue
        if isinstance(leaf, (Decimal, float)):
            continue
        if isinstance(leaf, bytes):
            leaf = leaf.decode("utf-8", "replace")
        for token in INTEGER.findall(str(leaf)):
            yield int(token)


def scaled(value: int, market: set[Decimal]) -> bool:
    """`value` is a market value scaled to an integer (`value / 10**k` in `market`, k = 0..MAX_SCALE)."""
    return any((D(value).scaleb(-k)).normalize() in market for k in range(MAX_SCALE + 1))


def leaks(leaves: Iterable[Any], market: set[Decimal]) -> list[str]:
    """Where a market value (or a planted digit run, or a market value scaled to an integer) appears; the found text
    is not returned (values never go into an assertion message), only a short label."""
    found: list[str] = []
    leaves = list(leaves)
    for i, (token, value) in enumerate(numbers(leaves)):
        digits = token.replace(".", "").replace("-", "").replace("+", "")
        if value is not None and value.normalize() in market:
            found.append(f"market value #{i}")
        elif any(run in digits for run in PLANTED):
            found.append(f"planted run #{i}")
    for i, n in enumerate(integers(leaves)):
        if scaled(abs(n), market):
            found.append(f"scaled market value #{i}")
    return found


@pytest.fixture
async def run(caplog: pytest.LogCaptureFixture) -> AsyncIterator[tuple[E2e, dict[str, int]]]:
    caplog.set_level(logging.DEBUG)
    r = fakeredis.FakeAsyncRedis()
    e = await e2e_rig(redis=r)
    try:
        # round 13 review: FirstBarGuard's ATR is read (and recorded) only where the guard applies, so this run arms
        # P1 inside its fill bucket (bar 0 still ahead, the bid history before it stored): B1 is bar 0 and its
        # evaluation reads the planted indicator values the scan looks for
        bar0 = FILL - FILL % TF + TF
        for k in range(20, 0, -1):
            await e.final_bars(bar0 - k * TF, CLOSE_BELOW)
        e.clock.now = FILL + 5 * 60_000
        e.md.clock.now = max(e.md.clock.now, e.clock.now)
        bars = await run_main_scenario(e)
        assert bars["b1"] == bar0
        yield e, bars
    finally:
        e.barclock.close()
        await r.flushall()
        await r.aclose()


async def relay_finish(e: E2e, *, expire: bool) -> RecordingOutbox:
    """The run's ARM outbox rows in the relay's store, then one real `ArmRelay` pass: delivered (`sent`), or after
    every row's expiry (`expired`)."""
    ao = RecordingOutbox()
    for a in e.emu.inserts:
        ao.add_outbox_row(kind="arm", channels=list(a[2]), created=a[1], title=a[3], body=a[4], next_attempt=a[1],
                          expires=a[5])
    now = max(a[5] for a in e.emu.inserts) + 1 if expire else max(a[1] for a in e.emu.inserts) + 1
    relay = ArmRelay(PgArmOutboxStore(engine_db(ao)), [RecordingChannel(name="arm_email"),
                                                       RecordingChannel(name="arm_push")],
                     renderer=SendTimeRenderer(e.md.port, keyring=lambda: KEYRING), clock_ms=lambda: now,
                     page=lambda s, t, b: e.rec.page(s, t, b))
    e.md.clock.now = now
    for _ in range(5):
        await relay.tick()
    return ao


# ---------------------------------------------------------------------------------------------------- the scan
async def test_no_market_value_reaches_postgresql_logs_pages_or_requests(
        run: tuple[E2e, dict[str, int]], caplog: pytest.LogCaptureFixture) -> None:
    e, bars = run
    market = market_values()
    indicator = e.md.port.indicator
    assert isinstance(indicator, PlantedIndicators) and indicator.reads > 0      # the planted values were read
    assert e.emu.inserts and e.decisions("POST")

    # 1. every argument of every statement sent to the ARM pool (each column written, the INSERT payload too)
    sent = [a for _, args in e.emu.args for a in walk(args)]
    assert leaks(sent, market) == []
    # 2. every emulated ARM table after the run
    tables = [e.emu.t.positions, e.emu.t.decisions, e.emu.t.handles, e.emu.t.notices, e.emu.t.audits,
              e.emu.t.failures, e.emu.t.outbox, e.emu.t.status]
    assert leaks(walk(tables), market) == []
    # 3. every log record, its message and its arguments
    logged = [r.getMessage() for r in caplog.records] + [a for r in caplog.records for a in walk(r.args or ())]
    assert leaks(logged, market) == []
    # 4. pages and the operator-request answers (the run's own; a disarm adds one)
    e.tracker.submit(TrackerRequest.of(61, "arm_disarm", f"{ADAPTER}:P1"))
    await e.tracker.step()
    answers = [(i, a.status.value, a.reason) for i, a in e.rec.answers]
    assert answers and leaks(walk(answers + e.rec.pages + [repr(n) for n in e.rec.notices]), market) == []


async def test_outbox_bodies_unsent_expired_and_finished_carry_no_market_value(
        run: tuple[E2e, dict[str, int]]) -> None:
    e, bars = run
    market = market_values()
    unsent = [r["body"] for r in e.emu.arm_rows()]
    inserted = list(e.emu.inserts)
    assert unsent and leaks(walk(unsent), market) == [] and leaks(walk(inserted), market) == []
    for expire in (False, True):
        store = await relay_finish(e, expire=expire)
        rows = store.arm_rows()
        statuses = [json.loads(r["body"])["status"] for r in rows]
        assert len(rows) == len(inserted) and all(r["sent_at"] is not None for r in rows)
        assert set(statuses) <= O.FINISH_STATUSES and ("expired" if expire else "sent") in statuses
        assert leaks(walk(rows), market) == []
        assert leaks([a for _, args in store.args for a in walk(args)], market) == []


async def test_decision_rows_keep_only_order_state_and_the_trigger_flags(run: tuple[E2e, dict[str, int]]) -> None:
    """Of the close, a decision keeps `trigger_met` / `borderline`; every decimal written is an order-state level,
    a unit count or an R figure quantised to 0.01."""
    e, _ = run
    order_state = {D(ENTRY), D(STOP0), D("1000")}
    for d in e.decisions():
        decimals = {k for k, v in d.items() if isinstance(v, Decimal)}
        assert decimals <= DECISION_DECIMALS
        assert d["first_bar_ratio"] is None                 # no first-bar test in this run (armed late)
        if d["current_risk_r"] is not None:
            assert d["current_risk_r"] == d["current_risk_r"].quantize(D("0.01"))
        if d["proposed_stop"] is not None:
            order_state.add(d["proposed_stop"])
        assert set(d) & CLOSE_FIELDS == CLOSE_FIELDS
    for h in e.emu.t.handles:
        for k, v in h.items():
            if isinstance(v, Decimal):
                order_state.add(v)                          # handle levels come from order state (checked below)
    risk = {d["current_risk_r"] for d in e.decisions() if d["current_risk_r"] is not None}
    written = {v for _, args in e.emu.args for v in walk(args) if isinstance(v, Decimal)}
    market = market_values()
    assert not {v.normalize() for v in written} & market
    assert written <= order_state | risk | {D("1.2000")}
    levels = {v for h in e.emu.t.handles for v in h.values() if isinstance(v, Decimal)}
    proposed = {d["proposed_stop"] for d in e.decisions() if d["proposed_stop"] is not None}
    assert levels <= {D(ENTRY), D(STOP0), D("1.2000")} | proposed


def test_the_scan_finds_a_planted_leak() -> None:
    """The scanner itself: a market value inside a JSON body, a log line or a Decimal argument is found."""
    market = market_values()
    assert leaks(['{"favour_r": 0.00, "close": 1.21647}'], market) == ["market value #1"]
    assert leaks([D("1.21729")], market) and leaks(["atr 0.00092617"], market)
    assert leaks([f"increment {PLANT_INCREMENT}"], market) and leaks(["2.1647e0"], market)
    assert leaks(['{"stop": "1.2000", "risk_r": "1.00", "hash": "01ab1647ef"}'], market) == []
    # a price scaled to an integer (points, or a string-encoded integer) is found too; ids and times are not
    assert leaks([121647], market) == ["scaled market value #0"]
    assert leaks(['{"close_points": 121647}'], market) == ["scaled market value #0"]
    assert leaks(["atr 92617 x1e-8"], market) and leaks([b"close=121729000"], market)
    assert leaks(['{"bar_end_ms": 1700003121647, "seq": 12, "units": 1000, "id": "a121647b"}', 1700003121647],
                 market) == []
