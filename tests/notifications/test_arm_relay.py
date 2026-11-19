"""S29 (design PR4, §B8, §B13, §B14; RT-M4, R11-M14): the ARM outbox store and `arm-relay`.

Both relays run against one emulated `notification_outbox` (`ArmOutboxEmulator`, which checks the 0016 channel CHECK
on every kind `arm` write): the ARM relay never escalates, never falls back, never sends at `critical`, never writes
`+escalated`; the Oversee relay never touches an ARM row; an ARM channel down never delays a HARD Oversee alert; the
send-time render shows favour figures only from a re-read, hash-verified bar; unsent, expired and finished bodies
hold ids, codes and order state only."""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any

import pytest

from src.db.arm_outbox_store import PgArmOutboxInserter, PgArmOutboxStore
from src.db.arm_failures_store import SQL_ARM_PING_INSERT
from src.db.arm_store import PgArmTxn
from src.db.epoch import EngineDb, Epoch
from src.db.fence import EpochMismatch
from src.db.notify_store import PgDedupeStore, PgOutboxStore
from src.marketdata.hashing import Keyring, content_hash
from src.marketdata.ports import BarsRead
from src.marketdata.types import M15, Bar, DataUnavailable, MdGeneration, SeriesId, Source
from src.marketdata.types import Side as QuoteSide
from src.notifications.arm_relay import (
    NOTICE_SEVERITY,
    PING_SEVERITY,
    ArmRelay,
    SendTimeRenderer,
    expired_adjust,
)
from src.notifications.errors import ChannelError
from src.notifications.factory import ChannelSet
from src.notifications.relay import OutboxRelay
from src.oversee.notify import OutboxRow, RecordingChannel
from src.riskmgr.notices import NoticeMeta, OutboxNotice
from src.riskmgr.render import ItemBody, NoticeBody, Rendered, RenderSettings, encode_body, render, subject

from tests.db.arm_outbox_emulator import ArmOutboxEmulator, CheckViolation, arm_check

D = Decimal
T = 1_700_001_000_000
KEY = Keyring.of(1, b"k" * 32)
BID = SeriesId("mock", "SYN.A", M15, QuoteSide.BID, Source.BROKER)
O, H, L, C = D("1.24111"), D("1.25222"), D("1.23333"), D("1.24456")
GOOD = content_hash(KEY, BID.canonical, T, O, H, L, C)
BAR = Bar(series=BID, bucket_start_ms=T - M15.timeframe_ms, open=O, high=H, low=L, close=C, final_at_ms=T)
PRICE_TEXTS = ("1.24111", "1.25222", "1.23333", "1.24456", "favour")


def item(**kw: Any) -> ItemBody:
    base: dict[str, Any] = dict(
        decision_id=7, decision_bar_end_ms=T, adapter_id="mock", position_id="P1", symbol="SYN.A", side="BUY",
        event="POST", bar_end_ms=T, outcome="ADJUST", category="ACTIONABLE", proposed_stop=D("1.23500"),
        current_risk_r=D("0.50"), handles=("h1",), trigger_input=f"{BID.canonical}|{T}|{GOOD}",
        entry=D("1.23456"), s0=D("1.21456"), strategy_kind="break_even_on_risk_close", strategy_impl_version=1,
        trigger_source="BROKER")
    base.update(kw)
    return ItemBody(**base)


def body(*items: ItemBody, event: str = "POST", notice_id: int = 42, **kw: Any) -> NoticeBody:
    base: dict[str, Any] = dict(notice_id=notice_id, route="main", event=event, bar_spec="15m", bar_end_ms=T,
                                created_ms=T + 70_000, expires_at_ms=T + 970_000, items=items or (item(),))
    base.update(kw)
    return NoticeBody(**base)


def outbox_notice(b: NoticeBody, channels: tuple[str, ...] = ("arm_email", "arm_push")) -> OutboxNotice:
    assert b.expires_at_ms is not None
    return OutboxNotice(notice_id=b.notice_id, route=b.route, channels=channels, subject=subject(b),
                        body=encode_body(b), created_ms=b.created_ms, expires_at_ms=b.expires_at_ms,
                        meta=NoticeMeta(route=b.route, bar_spec=b.bar_spec, event=b.event, bar_end_ms=b.bar_end_ms,
                                        part=b.part, item_key=b.item_key))


@dataclass
class FakeMd:
    held: dict[tuple[str, int], Bar] = field(default_factory=dict)
    fail: bool = False
    reads: list[tuple[str, int]] = field(default_factory=list)

    async def bars(self, series: SeriesId, *, at_bucket_ms: int | None = None) -> BarsRead | DataUnavailable:
        assert at_bucket_ms is not None
        end = at_bucket_ms + series.spec.timeframe_ms
        self.reads.append((series.canonical, end))
        if self.fail:
            raise ConnectionError("redis-md down")
        bar = self.held.get((series.canonical, end))
        if bar is None:
            return DataUnavailable("blind")
        return BarsRead((bar,), MdGeneration(1, 1))


@dataclass
class FailingChannel:
    name: str
    error: Exception = field(default_factory=lambda: ChannelError("smtp 451"))
    calls: int = 0
    backend: str = "failing"

    async def send(self, row: OutboxRow) -> None:
        self.calls += 1
        raise self.error


class Clock:
    def __init__(self, now: int = T + 100_000) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


@dataclass
class Rig:
    emu: ArmOutboxEmulator
    db: EngineDb
    store: PgArmOutboxStore
    clock: Clock
    md: FakeMd
    pages: list[tuple[str, str, str]]
    email: Any
    push: Any
    relay: ArmRelay
    drain: list[bool]


def rig(*, email: Any = None, push: Any = None, md: FakeMd | None = None, keyring: Keyring | None = KEY,
        renderer: Any = None, lease: list[bool] | None = None) -> Rig:
    emu = ArmOutboxEmulator(epoch=7)
    db = EngineDb(emu, lambda: Epoch("c", 7))
    store = PgArmOutboxStore(db)
    clock = Clock()
    md = md if md is not None else FakeMd({(BID.canonical, T): BAR})
    pages: list[tuple[str, str, str]] = []
    email = email if email is not None else RecordingChannel(name="arm_email")
    push = push if push is not None else RecordingChannel(name="arm_push")
    drain = [False]
    live = lease if lease is not None else [True]
    relay = ArmRelay(store, [email, push], renderer=renderer or SendTimeRenderer(md, keyring=lambda: keyring),
                     clock_ms=clock, page=lambda s, t, b: pages.append((s, t, b)), drain_notices=lambda: drain[0],
                     lease_valid=lambda: live[0])
    return Rig(emu, db, store, clock, md, pages, email, push, relay, drain)


async def enqueue(r: Rig, b: NoticeBody, channels: tuple[str, ...] = ("arm_email", "arm_push")) -> None:
    async with r.db.transaction() as tx:
        await PgArmOutboxInserter().enqueue(PgArmTxn(tx), outbox_notice(b, channels))


def stored_text(r: Rig) -> str:
    """Every stored column of every row, as text."""
    return json.dumps(r.emu.t.outbox, default=str)


# ---------------------------------------------------------------------------------------------------- insert
async def test_insert_writes_an_ids_only_arm_row_on_the_route_channels() -> None:
    r = rig()
    b = body()
    await enqueue(r, b, ("arm_email",))
    (row,) = r.emu.arm_rows()
    assert row["kind"] == "arm" and row["severity"] == "INFO" and row["channels"] == ["arm_email"]
    assert row["created"] == b.created_ms == row["next_attempt"] and row["expires"] == b.expires_at_ms
    assert row["body"] == encode_body(b) and row["title"] == subject(b)
    assert arm_check("arm", row["channels"])
    text = stored_text(r)
    assert not any(p in text for p in PRICE_TEXTS[:4]) and "favour" not in text.lower()


@pytest.mark.parametrize("channels", [(), ("email",), ("arm_email", "arm_email"), ("arm_sms",)])
async def test_insert_refuses_channels_outside_arm_email_and_arm_push(channels: tuple[str, ...]) -> None:
    r = rig()
    with pytest.raises(ValueError):
        await enqueue(r, body(), channels)
    assert r.emu.arm_rows() == []


async def test_insert_needs_the_notice_transaction_and_the_fence() -> None:
    r = rig()
    with pytest.raises(TypeError):
        await PgArmOutboxInserter().enqueue(object(), outbox_notice(body()))
    with pytest.raises(ValueError):
        bad = replace(outbox_notice(body()), body='{"notice_id": 1}')
        async with r.db.transaction() as tx:
            await PgArmOutboxInserter().enqueue(PgArmTxn(tx), bad)
    r.emu.epoch = 8      # a successor's epoch: the fenced INSERT writes nothing and raises
    with pytest.raises(EpochMismatch):
        await enqueue(r, body())
    assert r.emu.arm_rows() == []


# ---------------------------------------------------------------------------------------------------- send
async def test_a_notice_is_rendered_at_send_time_with_favour_from_a_verified_bar_then_scrubbed() -> None:
    r = rig()
    await enqueue(r, body())
    res = await r.relay.tick()
    assert res.delivered == 1 and r.md.reads == [(BID.canonical, T)]
    (sent,) = r.email.sent
    assert sent.severity == NOTICE_SEVERITY and "favour 0.50 R" in sent.body and "#42" in sent.title
    assert r.push.sent[0].body == sent.body
    (row,) = r.emu.arm_rows()
    assert row["sent_at"] == r.clock.now and row["delivered"] is True
    finished = json.loads(row["body"])
    assert set(finished) <= {"v", "status", "notice_id", "route", "bar_spec", "event", "bar_end_ms", "part",
                             "item_key", "decision_ids", "outcomes"}
    assert finished["status"] == "sent" and finished["decision_ids"] == [7] and finished["outcomes"] == ["ADJUST"]
    text = stored_text(r)
    for p in ("1.23500", "1.23456", "1.21456", "0.50", *PRICE_TEXTS):   # no order state left, no price, no favour
        assert p not in text


@pytest.mark.parametrize("case", ["unavailable", "md_down", "no_port", "no_key", "tampered", "pre"])
async def test_favour_is_omitted_whenever_the_bar_cannot_be_verified(case: str) -> None:
    md = FakeMd({(BID.canonical, T): BAR})
    keyring: Keyring | None = KEY
    b = body()
    if case == "unavailable":
        md.held.clear()
    elif case == "md_down":
        md.fail = True
    elif case == "no_key":
        keyring = None
    elif case == "tampered":
        md.held[(BID.canonical, T)] = replace(BAR, close=C + D("0.001"))
    elif case == "pre":
        b = body(item(event="PRE", trigger_input=None), event="PRE")
    r = rig(md=md, keyring=keyring)
    if case == "no_port":
        r.relay = ArmRelay(r.store, [r.email, r.push], renderer=SendTimeRenderer(None, keyring=lambda: KEY),
                           clock_ms=r.clock)
    await enqueue(r, b)
    res = await r.relay.tick()
    assert res.delivered == 1
    assert "favour" not in r.email.sent[0].body
    assert "decision 7" in r.email.sent[0].body


async def test_a_failing_renderer_still_sends_the_notice_without_bar_figures() -> None:
    async def broken(b: NoticeBody) -> Rendered:
        raise RuntimeError("boom")
    r = rig(renderer=broken)
    await enqueue(r, body())
    res = await r.relay.tick()
    assert res.delivered == 1 and res.render_fallbacks == 1
    assert r.email.sent[0].body == render(body()).text and "favour" not in r.email.sent[0].body


# ---------------------------------------------------------------------------------------------------- retries
async def test_a_failed_channel_is_rescheduled_in_check_form_never_escalated_never_falls_back() -> None:
    bad = FailingChannel("arm_email")
    r = rig(email=bad)
    await enqueue(r, body(), ("arm_email",))         # routed to arm_email only
    for _ in range(8):
        await r.relay.tick()
        (row,) = r.emu.arm_rows()
        assert arm_check("arm", row["channels"])      # the 0016 CHECK holds after every reschedule
        assert "+escalated" not in row["channels"] and row["sent_at"] is None
        r.clock.now = row["next_attempt"]
    (row,) = r.emu.arm_rows()
    assert row["channels"] == ["arm_email:8"] and row["attempts"] == 8 and row["delivered_at"] is None
    assert r.push.sent == []                         # no fallback to the other channel
    assert [p[1] for p in r.pages] == ["ARM channel arm_email failing"]   # once per down episode
    assert "smtp 451" in (row["last_error"] or "") and "favour" not in (row["last_error"] or "")
    assert r.emu.t.outbox[0]["body"] == encode_body(body())               # unsent body untouched


async def test_a_partial_delivery_retries_only_the_failed_channel_and_records_the_first_acceptance() -> None:
    push = FailingChannel("arm_push", ChannelError("ntfy 503"))
    r = rig(push=push)
    await enqueue(r, body())
    t0 = r.clock.now
    res = await r.relay.tick()
    assert res.partial == 1
    (row,) = r.emu.arm_rows()
    assert row["channels"] == ["arm_push:1"] and row["delivered_at"] == t0 and row["next_attempt"] == t0 + 2_000
    r.clock.now = row["next_attempt"]
    r.relay.replace_channels(ChannelSet((r.email, RecordingChannel(name="arm_push")), (), ()))
    res = await r.relay.tick()
    assert res.delivered == 1 and len(r.email.sent) == 1        # arm_email is not sent twice
    assert r.emu.arm_rows()[0]["delivered"] is True


async def test_an_unconfigured_channel_waits_at_the_cap_and_the_row_is_not_lost() -> None:
    r = rig()
    r.relay = ArmRelay(r.store, [r.email], renderer=SendTimeRenderer(r.md, keyring=lambda: KEY), clock_ms=r.clock,
                       disabled=("arm_push",))
    await enqueue(r, body())
    await r.relay.tick()
    (row,) = r.emu.arm_rows()
    assert row["channels"] == ["arm_push"] and row["next_attempt"] == r.clock.now + 60_000 and row["sent_at"] is None
    assert any("arm_push is disabled" in w for w in r.relay.warnings)


async def test_the_relay_refuses_non_arm_channels_and_sends_nothing_without_the_lease() -> None:
    r = rig()
    with pytest.raises(ValueError):
        ArmRelay(r.store, [RecordingChannel(name="email")], renderer=SendTimeRenderer(None, keyring=lambda: None))
    lease = [False]
    r = rig(lease=lease)
    await enqueue(r, body())
    res = await r.relay.tick()
    assert res.lease_lost and r.email.sent == [] and r.emu.arm_rows()[0]["sent_at"] is None


# ---------------------------------------------------------------------------------------------------- expiry etc.
async def test_an_expired_adjust_notice_is_finished_scrubbed_and_pages() -> None:
    r = rig()
    await enqueue(r, body())
    r.clock.now = T + 970_000
    res = await r.relay.tick()
    assert res.expired == 1 and r.email.sent == []
    (row,) = r.emu.arm_rows()
    assert row["delivered"] is False and row["last_error"] == "expired"
    assert json.loads(row["body"])["status"] == "expired" and "1.23500" not in row["body"]
    assert [p[1] for p in r.pages] == ["ARM notice #42 expired undelivered"]
    assert expired_adjust(body()) and not expired_adjust(body(item(outcome="HOLD_REASON", category="HOLD")))


async def test_a_pre_notice_is_superseded_by_its_post() -> None:
    r = rig()
    pre = body(item(event="PRE", trigger_input=None, decision_id=5), event="PRE", notice_id=40,
               created_ms=T - 30_000, expires_at_ms=T + 200_000)
    await enqueue(r, pre)
    await enqueue(r, body())
    res = await r.relay.tick()
    assert res.superseded == 1 and res.delivered == 1
    pre_row, post_row = r.emu.arm_rows()
    assert pre_row["superseded_by"] == post_row["outbox_id"] and pre_row["delivered"] is False
    assert json.loads(pre_row["body"])["status"] == "superseded"
    assert len(r.email.sent) == 1 and "#42" in r.email.sent[0].title


async def test_a_part_five_notice_is_never_superseded_before_one_channel_took_it() -> None:
    push = FailingChannel("arm_push")
    email = FailingChannel("arm_email")
    r = rig(email=email, push=push)
    p5 = body(part=5, notice_id=41, created_ms=T + 60_000, expires_at_ms=T + 2_000_000)
    await enqueue(r, p5)
    nxt = body(notice_id=43, bar_end_ms=T + M15.timeframe_ms, created_ms=T + 61_000,
               expires_at_ms=T + 3_000_000, items=(item(bar_end_ms=T + M15.timeframe_ms),))
    await enqueue(r, nxt)
    await r.relay.tick()
    assert all(row["superseded_by"] is None for row in r.emu.arm_rows())   # never sent once: kept
    # one channel accepts it once: from then on the next POST part 0 supersedes it
    r.relay.replace_channels(ChannelSet((RecordingChannel(name="arm_email"), push), (), ()))
    r.clock.now += 120_000
    await r.relay.tick()
    p5_row = r.emu.arm_rows()[0]
    assert p5_row["delivered_at"] is not None and p5_row["sent_at"] is None
    r.clock.now += 120_000
    await r.relay.tick()
    p5_row = r.emu.arm_rows()[0]
    assert p5_row["superseded_by"] == r.emu.arm_rows()[1]["outbox_id"]


async def test_drain_notices_suppresses_notices_but_never_a_ping() -> None:
    r = rig()
    await enqueue(r, body())
    async with r.db.transaction() as tx:
        await tx.write(SQL_ARM_PING_INSERT, T + 90_000, "ARM FAILED ping p-1: series", "scope series; reason x",
                       must=True)
    r.drain[0] = True
    res = await r.relay.tick()
    assert res.suppressed == 1 and res.delivered == 1
    notice, ping = r.emu.arm_rows()
    assert notice["last_error"] == "suppressed" and json.loads(notice["body"])["status"] == "suppressed"
    assert ping["delivered"] is True and ping["body"] == "scope series; reason x"   # a ping body is kept
    (sent,) = r.email.sent
    assert sent.severity == PING_SEVERITY != "critical" and "p-1" in sent.title   # no emergency priority


async def test_a_notice_body_that_does_not_decode_is_finished_unsent() -> None:
    r = rig()
    r.emu.add_outbox_row(kind="arm", channels=["arm_email"], created=T, body='{"notice_id": 3, "x": 1}',
                         expires=T + 10_000_000)
    res = await r.relay.tick()
    assert res.bad_body == 1 and r.email.sent == []
    assert json.loads(r.emu.arm_rows()[0]["body"]) == {"v": 1, "status": "bad_body", "notice_id": 3,
                                                       "decision_ids": [], "outcomes": []}


async def insert_ping(r: Rig, created: int = T + 90_000) -> None:
    async with r.db.transaction() as tx:
        await tx.write(SQL_ARM_PING_INSERT, created, "ARM FAILED ping p-1: series", "scope series; reason x",
                       must=True)


async def test_an_unrenderable_notice_never_blocks_the_good_notice_or_the_ping_behind_it() -> None:
    r = rig()
    bad = body(notice_id=9, route="side", bar_end_ms=10**18)       # decodes, but no clock can show its bar end
    with pytest.raises(Exception):
        render(bad)
    r.emu.add_outbox_row(kind="arm", channels=["arm_email", "arm_push"], created=T, body=encode_body(bad),
                         expires=T + 10_000_000)                     # heads the batch (oldest next attempt)
    await enqueue(r, body())
    await insert_ping(r)
    res = await r.relay.tick()
    assert res.bad_body == 1 and res.delivered == 2 and res.row_errors == 0
    assert [m.title for m in r.email.sent] == [subject(body()), "ARM FAILED ping p-1: series"]
    assert len(r.push.sent) == 2
    bad_row, good, ping = r.emu.arm_rows()
    assert bad_row["sent_at"] is not None and bad_row["delivered"] is False
    assert json.loads(bad_row["body"])["status"] == "bad_body"
    assert good["delivered"] is True and ping["delivered"] is True


async def test_an_error_on_one_row_is_counted_and_the_batch_goes_on_but_a_connection_error_stops_it() -> None:
    r = rig()
    await enqueue(r, body(notice_id=40, route="r1"))
    await enqueue(r, body(notice_id=41, route="r2"))
    await insert_ping(r)
    orig = r.store.finish

    async def finish(row: Any, *a: Any, **kw: Any) -> bool:
        if row.outbox_id == 1:
            raise RuntimeError("statement failed")
        return await orig(row, *a, **kw)
    r.store.finish = finish  # type: ignore[method-assign]
    res = await r.relay.tick()
    assert res.row_errors == 1 and res.delivered == 2                  # row 2 and the ping still went out
    assert [row["sent_at"] is None for row in r.emu.arm_rows()] == [True, False, False]

    async def down(row: Any, *a: Any, **kw: Any) -> bool:
        raise ConnectionError("db gone")
    r.store.finish = down  # type: ignore[method-assign]
    with pytest.raises(ConnectionError):                              # run() reports it as PostgreSQL down
        await r.relay.tick()


async def test_drain_notices_also_suppresses_a_notice_in_back_off_but_never_the_ping() -> None:
    r = rig(email=FailingChannel("arm_email"))
    await enqueue(r, body(), ("arm_email",))
    await insert_ping(r)
    await r.relay.tick()                                              # both fail on arm_email: in back-off
    notice, ping = r.emu.arm_rows()
    assert notice["next_attempt"] > r.clock.now and ping["next_attempt"] > r.clock.now
    r.drain[0] = True
    res = await r.relay.tick()                                        # nothing is due
    assert res.due == 0 and res.suppressed == 1
    notice, ping = r.emu.arm_rows()
    assert notice["last_error"] == "suppressed" and json.loads(notice["body"])["status"] == "suppressed"
    assert ping["sent_at"] is None                                    # the ping keeps retrying
    r.drain[0] = False
    r.relay.replace_channels(ChannelSet((RecordingChannel(name="arm_email"), r.push), (), ()))
    r.clock.now += 120_000
    await r.relay.tick()                                              # after the resume only the ping goes out
    assert r.emu.arm_rows()[1]["delivered"] is True and r.emu.arm_rows()[0]["delivered"] is False


async def test_a_row_finished_elsewhere_first_is_not_counted_and_does_not_page() -> None:
    r = rig()
    await enqueue(r, body())
    r.clock.now = T + 970_000
    orig = r.store.due

    async def due(now_ms: int, limit: int) -> list[Any]:
        rows = await orig(now_ms, limit)
        for row in r.emu.arm_rows():                                  # housekeeping finishes it in between
            row["sent_at"] = now_ms
        return rows
    r.store.due = due  # type: ignore[method-assign]
    res = await r.relay.tick()
    assert res.expired == 0 and r.pages == []


async def test_a_lease_lost_mid_batch_stops_the_next_send() -> None:
    lease = [True]

    @dataclass
    class DropsLease:
        name: str
        sent: list[OutboxRow] = field(default_factory=list)
        backend: str = "rec"

        async def send(self, row: OutboxRow) -> None:
            self.sent.append(row)
            lease[0] = False                                          # the lease is lost right after this send

    r = rig(email=DropsLease("arm_email"), lease=lease)
    await enqueue(r, body(notice_id=40, route="r1"))
    await enqueue(r, body(notice_id=41, route="r2"))
    res = await r.relay.tick()
    assert res.lease_lost and len(r.email.sent) == 1 and r.push.sent == []   # nor arm_push, nor the second row
    first, second = r.emu.arm_rows()
    assert first["sent_at"] is None and first["channels"] == ["arm_push"] and first["delivered_at"] is not None
    assert second["attempts"] == 0 and second["sent_at"] is None


async def test_an_expired_late_adjust_pages_and_an_expired_pre_adjust_does_not() -> None:
    r = rig()
    await enqueue(r, body(item(event="LATE"), event="LATE", notice_id=44, route="r1"))
    await enqueue(r, body(item(event="PRE", trigger_input=None), event="PRE", notice_id=45, route="r2"))
    r.clock.now = T + 970_000
    res = await r.relay.tick()
    assert res.expired == 2 and r.email.sent == []
    assert [p[1] for p in r.pages] == ["ARM notice #44 expired undelivered"]   # PRE expiry is a metric only


@dataclass
class HangingMd:
    calls: int = 0

    async def bars(self, series: SeriesId, *, at_bucket_ms: int | None = None) -> BarsRead:
        self.calls += 1
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


async def test_a_hung_md_costs_one_timeout_per_tick_and_the_reads_run_concurrently() -> None:
    ask = SeriesId("mock", "SYN.A", M15, QuoteSide.ASK, Source.BROKER)
    md = HangingMd()
    renderer = SendTimeRenderer(md, keyring=lambda: KEY, read_timeout_s=0.2)
    r = rig(renderer=renderer)
    two = (item(), item(decision_id=8, trigger_input=f"{ask.canonical}|{T}|{GOOD}"))
    for i in range(3):
        await enqueue(r, body(*two, notice_id=60 + i, route=f"r{i}"))
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    res = await r.relay.tick()
    assert loop.time() - t0 < 0.6                                     # one 0.2 s timeout, not 2 x 3
    assert res.delivered == 3 and all("favour" not in m.body for m in r.email.sent)
    assert md.calls == 2 and renderer.skipped == 4                    # the later notices skip the port
    await enqueue(r, body(*two, notice_id=70, route="r9"))
    await r.relay.tick()
    assert md.calls == 4                                              # a new tick tries the port again


async def test_the_fallback_text_uses_the_renderers_settings_and_settings_are_set_once() -> None:
    class Broken(SendTimeRenderer):
        async def __call__(self, b: NoticeBody) -> Rendered:
            raise RuntimeError("boom")
    levels = RenderSettings(show_levels=True)
    r = rig(renderer=Broken(None, keyring=lambda: KEY, settings=levels))
    await enqueue(r, body())
    res = await r.relay.tick()
    assert res.render_fallbacks == 1 and r.email.sent[0].body == render(body(), settings=levels).text
    assert render(body(), settings=levels).text != render(body()).text
    with pytest.raises(ValueError):
        ArmRelay(r.store, [r.email], renderer=SendTimeRenderer(None, keyring=lambda: KEY, settings=levels),
                 render_settings=RenderSettings())


async def test_oldest_undelivered_follows_the_unsent_arm_rows_only() -> None:
    r = rig(email=FailingChannel("arm_email"))
    r.emu.add_outbox_row(kind="info", channels=["email"], created=T - 1_000_000)    # an Oversee row: not counted
    await enqueue(r, body(), ("arm_email",))
    await r.relay.tick()
    assert r.relay.oldest_undelivered_ms() == body().created_ms


# ---------------------------------------------------------------------------------------------------- isolation
async def test_the_oversee_relay_never_touches_an_arm_row_and_the_arm_relay_never_an_oversee_row() -> None:
    r = rig()
    await enqueue(r, body())
    oversee_db = r.db
    ov_email, ov_push = RecordingChannel(name="email"), RecordingChannel(name="mobile_push")
    oversee = OutboxRelay(PgOutboxStore(oversee_db), PgDedupeStore(oversee_db), [ov_email, ov_push],
                          clock_ms=r.clock)
    hard = OutboxRow("trip", "critical", "HARD MaxDailyLoss account:A1", "reason", T + 99_000)
    await PgOutboxStore(oversee_db).add([hard])
    res = await oversee.tick()
    assert res.delivered == 1 and [m.title for m in ov_email.sent] == [hard.title]
    arm_row = r.emu.arm_rows()[0]
    assert arm_row["sent_at"] is None and arm_row["attempts"] == 0     # untouched by the Oversee relay
    await r.relay.tick()
    assert [m.title for m in r.email.sent] == [subject(body())]        # only the ARM row
    assert len(ov_email.sent) == 1


async def test_an_arm_channel_down_never_delays_a_hard_oversee_alert() -> None:
    r = rig(email=FailingChannel("arm_email"), push=FailingChannel("arm_push"))
    for i in range(5):
        await enqueue(r, body(notice_id=50 + i, created_ms=T + 70_000 + i))
    ov_email, ov_push = RecordingChannel(name="email"), RecordingChannel(name="mobile_push")
    oversee = OutboxRelay(PgOutboxStore(r.db), PgDedupeStore(r.db), [ov_email, ov_push], clock_ms=r.clock)
    await r.relay.tick()                                                # ARM channels fail
    hard = OutboxRow("trip", "critical", "HARD MaxPositionRisk position:P1", "reason", T + 99_000)
    await PgOutboxStore(r.db).add([hard])
    res = await asyncio.wait_for(oversee.tick(), 1.0)
    assert res.delivered == 1 and res.escalated == 0
    assert [m.title for m in ov_email.sent] == [hard.title] and [m.title for m in ov_push.sent] == [hard.title]
    assert all(row["sent_at"] is None and "+escalated" not in row["channels"] for row in r.emu.arm_rows())


async def test_the_emulated_check_refuses_an_escalated_arm_row() -> None:
    r = rig()
    with pytest.raises(CheckViolation):
        r.emu.add_outbox_row(kind="arm", channels=["arm_email", "+escalated"])
    with pytest.raises(CheckViolation):
        r.emu.add_outbox_row(kind="arm", channels=[])

