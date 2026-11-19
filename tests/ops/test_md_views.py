"""The S35 market-data views (plan S35): `md show`, `md bars <series>`, `md indicators` read `redis-md` as `md_reader`
and show ids, states, counts, check names and times, never a price, an indicator value or a price increment; the
reader settings name variables only. fakeredis; synthetic instruments and prices."""
from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

import fakeredis
import pytest

from src.marketdata.indicators.base import IndicatorSpec, IndicatorValue, Lineage, encode_value
from src.marketdata.store import keys as K
from src.marketdata.types import Quality
from src.ops.md_view import MdViews, parse_keyring, parse_quarantine, parse_reader_settings, safe_meta, series_arg
from src.ops.model import OpsRefused, OpsUsageError

from tests.ops.arm_view_fakes import (
    BAR_END,
    BUCKET,
    CLOSE,
    RING,
    RING_OLD,
    SENTINEL,
    SERIES,
    TOKEN,
    bar,
    reader,
    store_bar,
    store_superseded,
)
from tests.ops.emulator import NOW

INCREMENT = Decimal("0.0007")
KEY_HEX = "ab" * 32


@pytest.fixture
async def md() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    await r.set(K.WRITER, TOKEN.text)
    await r.set(K.BOOT, SENTINEL.text)
    yield r
    await r.flushall()
    await r.aclose()


def spec() -> IndicatorSpec:
    return IndicatorSpec(kind="ATR", version=1, series=SERIES, params=(("length", "14"),), price_increment=INCREMENT)


async def seed(r: Any) -> None:
    await r.hset(K.STORED, mapping={SERIES.canonical: "1", "mock:SYN.A": "1"})
    await r.hset(K.meta(SERIES), mapping={"sm12": "LIVE", "d6": "CLOSE_ONLY", "write_seq": "12", "quarantined": "1",
                                          "first_bucket": str(BUCKET - 900_000), "last_bucket": str(BUCKET),
                                          "rehash_key": "2", "planted_close": str(CLOSE), "Bad Name": "1"})
    await r.zadd(K.rollback(SERIES), {f"{12:020d}|{BAR_END:013d}|x": 12, f"{11:020d}|{BUCKET:013d}|y": 11})
    await store_bar(r, bar(bucket=BUCKET - 900_000, final_at=BUCKET + 30_000), RING_OLD)
    await store_bar(r, bar(revision=1, close=Decimal("1.23460")))
    await store_superseded(r, bar(), BAR_END + 600_000)
    await r.zadd(K.quarantine(SERIES), {f"{NOW - 5_000:013d}|content_hash|{BUCKET:013d}|0512345": BUCKET})


async def seed_indicator(r: Any, *, faulted: bool = False) -> str:
    s = spec()
    await r.hset(K.REGISTRY_IND, mapping={s.indicator_id: s.canonical})
    v = IndicatorValue(indicator_id=s.indicator_id, output="value", bucket_start_ms=BUCKET,
                       value=None if faulted else Decimal("0.00123"), quality=Quality.COMPLETE, warm=True,
                       final_at_ms=BAR_END + 40_000,
                       lineage=Lineage(s.indicator_id, BUCKET - 900_000 * 140, "0123456789abcdef", 1,
                                       "inputs_failed" if faulted else ""))
    await r.zadd(K.indicator(s.indicator_id, "value"), {encode_value(v): BUCKET})
    return s.indicator_id


def text_of(rep: Any) -> str:
    return repr((rep.fields, rep.rows, rep.sections, rep.warnings))


# ============================================================================================== settings
def test_reader_settings_and_keyring_name_variables_only() -> None:
    assert parse_reader_settings({}) is None
    bad = parse_reader_settings({"XTNL_MD_REDIS_READER_URL": "http://secret-host"})
    assert bad == ["XTNL_MD_REDIS_READER_URL is not a redis:// or rediss:// URL"]
    ok = parse_reader_settings({"XTNL_MD_REDIS_READER_URL": "rediss://u:pw@h:1/0", "XTNL_MD_HMAC_KEY": KEY_HEX,
                                "XTNL_MD_HMAC_KEY_ID": "4"})
    assert ok is not None and not isinstance(ok, list) and ok.keyring is not None and ok.keyring.current_id == 4
    assert "pw" not in repr(ok) and KEY_HEX not in repr(ok)
    partial = parse_reader_settings({"XTNL_MD_REDIS_READER_URL": "redis://h:1", "XTNL_MD_HMAC_KEY": KEY_HEX})
    assert partial is not None and not isinstance(partial, list) and partial.keyring is None
    assert partial.keyring_problems == ("XTNL_MD_HMAC_KEY and XTNL_MD_HMAC_KEY_ID go together",)
    assert parse_keyring({"XTNL_MD_HMAC_KEY": "zz", "XTNL_MD_HMAC_KEY_ID": "300"}) == [
        "XTNL_MD_HMAC_KEY is not 64 hex digits (32 bytes)", "XTNL_MD_HMAC_KEY_ID is not an integer in [1, 255]"]
    rotating = parse_keyring({"XTNL_MD_HMAC_KEY": KEY_HEX, "XTNL_MD_HMAC_KEY_ID": "4",
                              "XTNL_MD_HMAC_KEY_PREV": "cd" * 32, "XTNL_MD_HMAC_KEY_PREV_ID": "3"})
    assert not isinstance(rotating, list) and rotating is not None and rotating.key_ids == (4, 3)
    same_id = parse_keyring({"XTNL_MD_HMAC_KEY": KEY_HEX, "XTNL_MD_HMAC_KEY_ID": "4",
                             "XTNL_MD_HMAC_KEY_PREV": "cd" * 32, "XTNL_MD_HMAC_KEY_PREV_ID": "4"})
    assert isinstance(same_id, list) and KEY_HEX not in same_id[0]


def test_meta_quarantine_and_series_forms() -> None:
    shown = safe_meta({"sm12": "LIVE", "write_seq": "3", "first_bucket": str(BUCKET), "close": "1.2345",
                       "x\x1b": "1", "note": "a b"})
    assert set(shown) == {"sm12", "write_seq", "first_bucket"} and shown["first_bucket"].endswith("Z")
    q = parse_quarantine(f"{NOW:013d}|grid_range|{BUCKET:013d}|0512345")
    assert q is not None and (q.check, q.bucket_start_ms) == ("grid_range", BUCKET)
    assert parse_quarantine("garbage") is None
    assert series_arg(SERIES.canonical) == SERIES
    with pytest.raises(OpsUsageError):
        series_arg("mock:SYN.A")


# ============================================================================================== md show
async def test_md_show_states_counters_and_rehash_progress(md: Any) -> None:
    await seed(md)
    await seed_indicator(md, faulted=True)
    rep = await MdViews(reader(md, RING), lambda: NOW).show()
    assert rep.fields["md_generation"] == "7.2" and rep.fields["writer_node"] == "node-a"
    assert rep.fields["writer_epoch"] == "7" and rep.fields["series"] == 1 and rep.fields["sm12"] == "LIVE=1"
    row = rep.sections["series"][0]
    assert row["series"] == SERIES.canonical and row["d6"] == "CLOSE_ONLY" and row["rollback_depth"] == 2
    assert row["rehashed"] is False and rep.fields["rehash"] == "1 series pending" and rep.fields["hash_key_id"] == 3
    assert "planted_close" not in row and rep.fields["faulted_indicators"] == 1
    text = text_of(rep)
    assert str(CLOSE) not in text and "s3cretnonce" not in text and str(INCREMENT) not in text
    await md.hset(K.meta(SERIES), "rehash_key", "3")
    assert (await MdViews(reader(md, RING), lambda: NOW).show()).fields["rehash"] == "complete"


async def test_md_show_without_boot_or_keyring(md: Any) -> None:
    await md.delete(K.BOOT)
    rep = await MdViews(reader(md, None), lambda: NOW).show()
    assert rep.fields["md_generation"] is None and rep.fields["series"] == 0
    assert any("md:boot is missing" in w for w in rep.warnings) and any("keyring" in w for w in rep.warnings)


# ============================================================================================== md bars
async def test_md_bars_counts_checks_and_hash_status_without_values(md: Any) -> None:
    await seed(md)
    rep = await MdViews(reader(md, RING), lambda: NOW).bars(SERIES, newest=16)
    f = rep.fields
    assert f["stored"] == 2 and f["quality"] == "complete=2" and f["revised"] == 1 and f["superseded_revisions"] == 1
    assert f["quarantined"] == 1 and f["quarantine_checks"] == "content_hash=1"
    assert [r["hash"] for r in rep.sections["newest"]] == ["key_retired", "valid"]
    assert rep.sections["quarantine"][0]["check"] == "content_hash"
    text = text_of(rep)
    for planted in (str(CLOSE), "1.23460", "1.23401", "0512345"):
        assert planted not in text
    unchecked = await MdViews(reader(md, None), lambda: NOW).bars(SERIES, newest=1)
    assert unchecked.sections["newest"][0]["hash"] == "unchecked" and unchecked.warnings
    with pytest.raises(OpsUsageError):
        await MdViews(reader(md, RING), lambda: NOW).bars(SERIES, newest=0)


async def test_md_bars_reads_are_bounded(md: Any) -> None:
    await seed(md)
    full = await MdViews(reader(md, RING), lambda: NOW).bars(SERIES, newest=4)
    assert full.fields["truncated"] is False and not any("at most the newest" in w for w in full.warnings)
    cut = await MdViews(reader(md, RING), lambda: NOW, count_max=1).bars(SERIES, newest=4)
    assert cut.fields["truncated"] is True and cut.fields["stored"] == 1
    assert cut.fields["superseded_revisions"] == 1 and cut.fields["quarantined"] == 1
    assert any("at most the newest 1 " in w for w in cut.warnings)


async def test_md_bars_reports_undecodable_members(md: Any) -> None:
    await md.zadd(K.bars(SERIES), {"not-a-member": BUCKET})
    rep = await MdViews(reader(md, RING), lambda: NOW).bars(SERIES, newest=4)
    assert rep.fields["quality"] == "undecodable:format=1"
    assert rep.sections["newest"][0] == {"bucket": rep.sections["newest"][0]["bucket"], "decoded": False,
                                         "problem": "format"}


# ============================================================================================== md indicators
async def test_md_indicators_show_lineage_never_the_value_or_increment(md: Any) -> None:
    ind = await seed_indicator(md)
    rep = await MdViews(reader(md, RING), lambda: NOW).indicators(indicator_id=None)
    row = rep.rows[0]
    assert row["indicator"] == ind and row["class"] == "ATR:1" and row["series"] == SERIES.canonical
    assert row["params"] == "length=14" and row["warm"] is True and row["value_revision"] == 1
    assert row["faulted"] is False and rep.fields == {"indicators": 1, "faulted": 0}
    text = text_of(rep)
    assert "0.00123" not in text and str(INCREMENT) not in text and "0.0007" not in text
    await md.hset(K.REGISTRY_IND, "ffffffffffffffff", "{bad")
    more = await MdViews(reader(md, RING), lambda: NOW).indicators(indicator_id="ffffffffffffffff")
    assert more.rows == [{"indicator": "ffffffffffffffff", "class": None, "problem": "undecodable_spec",
                          "faulted": True}]


async def test_md_indicators_faulted(md: Any) -> None:
    await seed_indicator(md, faulted=True)
    rep = await MdViews(reader(md, RING), lambda: NOW).indicators(indicator_id=None)
    assert rep.rows[0]["faulted"] is True and rep.rows[0]["fault_reason"] == "inputs_failed"
    assert rep.fields["faulted"] == 1 and any("md rebuild" in w for w in rep.warnings)


async def test_store_errors_are_refusals_without_the_url() -> None:
    class Broken:
        def pipeline(self, transaction: bool = False) -> Any:
            raise ConnectionError("redis://md_reader:Pl4nted@planted-host:6382 refused")

    with pytest.raises(OpsRefused) as got:
        await MdViews(reader(Broken(), RING), lambda: NOW).show()
    assert got.value.code == "md_unreadable" and "Pl4nted" not in str(got.value) and "planted-host" not in str(
        got.value)
