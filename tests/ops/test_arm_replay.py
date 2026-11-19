"""`arm replay` (plan S35): stored decisions re-verified against `redis-md` as of their decision time. A replay on stored
decisions matches; a missing bar reports `bars_missing` (and files an audited `md_backfill` with `--request-backfill`);
a retired key reports `UNVERIFIABLE`; a superseded revision that aged out is `UNVERIFIABLE(superseded_expired)`, never a
mismatch; another envelope reports `ENVELOPE_CHANGED`. fakeredis; synthetic prices that never reach the output."""
from __future__ import annotations

import io
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

import fakeredis
import pytest

from src.db.pg import PgConnection
from src.marketdata.store import keys as K
from src.ops import cli
from src.ops.arm import ArmContext
from src.marketdata.brokerbars import MD_BACKFILL_MAX
from src.marketdata.types import M1, SeriesId, Side, Source
from src.ops.arm_replay import (
    BACKFILL_CHUNK_MAX,
    BackfillRange,
    Candidate,
    DecisionReplay,
    InputVerdict,
    Result,
    backfill_ranges,
    bar_as_of,
    check_digest,
    compare_input,
    parse_indicator_input,
    parse_series_input,
    replay_one,
)
from src.ops.arm_view import ArmViews, stored_inputs
from src.ops.model import Actor, OpsUsageError
from src.ops.service import OpsService
from src.riskmgr.envelope import EnvelopeParams, stamp
from src.riskmgr.types import IndicatorInput, SeriesInput, inputs_digest

from tests.ops.arm_view_fakes import (
    BAR_END,
    BUCKET,
    CLOSE,
    RING,
    RING_OLD,
    RING_ROTATING,
    SERIES,
    TICK_SERIES,
    ArmViewPg,
    Decision,
    bar,
    bar_hash,
    decision_for,
    member,
    reader,
    store_bar,
    store_superseded,
)
from tests.ops.emulator import NOW

ACTOR = Actor("alice", "INC-88 replay backfill")
READER_URL = "rediss://md_reader:Pl4nted-Reader@md.planted-host.example:6382/0"


@pytest.fixture
async def md() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.flushall()
    await r.aclose()


def views(pg: ArmViewPg, r: Any, ring: Any = RING, **kw: Any) -> ArmViews:
    async def no_sleep(_s: float) -> None:
        return None

    context = ArmContext(md_reader=lambda: reader(r, ring), sleep=no_sleep, **kw)
    return ArmViews(OpsService(pg, None, clock_ms=lambda: NOW, arm=context).arm)


async def replay(pg: ArmViewPg, r: Any, ring: Any = RING, **kw: Any) -> Any:
    v = views(pg, r, ring, **{k: kw.pop(k) for k in list(kw) if k in ("envelope", "envelope_problems")})
    return await v.replay(kw.pop("target", None), from_ms=None, to_ms=None, limit=50,
                          request_backfill=kw.pop("request_backfill", False), actor=kw.pop("actor", None),
                          wait_s=0)


# ============================================================================================== pure parts
def test_input_forms_parse_both_spellings() -> None:
    h = bar_hash(bar())
    a = parse_series_input(f"S|{SERIES.canonical}|{BAR_END}|{h}")
    b = parse_series_input(f"{SERIES.canonical}|{BAR_END}|{h}")
    assert a == b == SeriesInput(SERIES.canonical, BAR_END, h)
    assert parse_series_input("S|bad|1|x") is None and parse_series_input("nonsense") is None
    i = IndicatorInput("0123456789abcdef", "value", BAR_END, BUCKET - 900_000 * 10, h)
    assert parse_indicator_input(i.text) == i == parse_indicator_input(i.text[2:])


def test_digest_recomputes_under_the_stored_key_id_only() -> None:
    inp = SeriesInput(SERIES.canonical, BAR_END, bar_hash(bar(), RING_OLD))
    old = inputs_digest(RING_OLD, (inp,), ())
    assert check_digest(RING_ROTATING, [inp.text], [], old).result is Result.MATCH        # retained key verifies
    got = check_digest(RING, [inp.text], [], old)
    assert (got.result, got.reason) == (Result.UNVERIFIABLE, "key_retired")               # retired: never a mismatch
    other = inputs_digest(RING, (SeriesInput(SERIES.canonical, BAR_END + 900_000, inp.content_hash),), ())
    assert check_digest(RING, [inp.text], [], other).result is Result.DIGEST_MISMATCH
    assert check_digest(RING, ["junk"], [], other).reason == "malformed_input"


def test_as_of_choice_takes_the_revision_current_at_the_decision() -> None:
    rev0, rev1 = bar(revision=0), bar(revision=1, close=Decimal("1.23460"), final_at=BAR_END + 600_000)
    decided = BAR_END + 60_000
    both = [Candidate(member(rev1)), Candidate(member(rev0), superseded_at_ms=BAR_END + 600_000)]
    got = bar_as_of(SERIES, BUCKET, both, decided, RING, NOW)
    assert isinstance(got, type(rev0)) and got.revision == 0
    assert bar_as_of(SERIES, BUCKET, both, BAR_END + 700_000, RING, NOW).revision == 1   # type: ignore[union-attr]
    assert bar_as_of(SERIES, BUCKET, [Candidate(member(rev1))], decided, RING, NOW) == "superseded_expired"
    assert bar_as_of(SERIES, BUCKET, [], decided, RING, NOW) is None
    assert bar_as_of(SERIES, BUCKET, [Candidate(member(rev0, RING_OLD))], decided, RING, NOW) == "key_retired"
    corrupt = member(rev0)[:-3] + "000"
    assert bar_as_of(SERIES, BUCKET, [Candidate(corrupt)], decided, RING, NOW) == "bar_invalid"


# ============================================================================================== against the store
async def test_replay_on_stored_decisions_matches(md: Any) -> None:
    pg = ArmViewPg()
    b = bar()
    await store_bar(md, b)
    pg.decisions.append(decision_for(b))
    pg.decisions.append(Decision(decision_id=2, event="PRE", decision_seq=2, outcome="PRE_NOT_CONFIRMED"))
    rep = await replay(pg, md)
    assert rep.outcome == "ok" and rep.fields["replayed"] == 2
    by_id = {row["decision"]: row for row in rep.rows}
    assert by_id[1]["result"] == "MATCH" and by_id[1]["digest"] == "MATCH" and by_id[1]["envelope"] == "same"
    assert by_id[2]["result"] == "NO_INPUTS"
    assert rep.fields["results"] == "MATCH=1,NO_INPUTS=1"


async def test_a_missing_bar_reports_bars_missing(md: Any) -> None:
    pg = ArmViewPg()
    pg.decisions.append(decision_for(bar()))
    rep = await replay(pg, md)
    row = rep.rows[0]
    assert row["result"] == "bars_missing" and "bars_missing" in row["inputs"]
    assert any("--request-backfill" in w for w in rep.warnings) and "backfill" not in rep.sections


async def test_missing_tick_bars_and_bars_beyond_broker_reach_are_unverifiable(md: Any) -> None:
    pg = ArmViewPg()
    pg.decisions.append(decision_for(bar(series=TICK_SERIES)))
    old_end = NOW - 400 * 86_400_000 - (NOW % 900_000)
    pg.decisions.append(decision_for(bar(bucket=old_end - 900_000, final_at=old_end + 1), decision_id=2,
                                     bar_end_ms=old_end, decided_at_ms=old_end + 60_000))
    rep = await replay(pg, md)
    results = {row["decision"]: row["result"] for row in rep.rows}
    assert results == {1: "UNVERIFIABLE(tick_history_gone)", 2: "UNVERIFIABLE(beyond_broker_reach)"}
    assert not any("--request-backfill" in w for w in rep.warnings)


async def test_a_retired_key_reports_unverifiable(md: Any) -> None:
    pg = ArmViewPg()
    b = bar()
    await store_bar(md, b)                                   # the store was re-hashed under the new key id 3
    pg.decisions.append(decision_for(b, RING_OLD))           # the decision hashed under key id 2, since retired
    rep = await replay(pg, md, RING)
    assert rep.rows[0]["result"] == "UNVERIFIABLE(key_retired)"
    assert rep.rows[0]["digest"] == "UNVERIFIABLE(key_retired)"
    during = await replay(pg, md, RING_ROTATING)            # while the previous key is retained, it verifies
    assert during.rows[0]["result"] == "MATCH"


async def test_a_changed_bar_and_a_tampered_digest(md: Any) -> None:
    pg = ArmViewPg()
    await store_bar(md, bar(close=Decimal("1.23470")))
    pg.decisions.append(decision_for(bar()))
    rep = await replay(pg, md)
    assert rep.rows[0]["result"] == "CHANGED" and any("CHANGED" in w for w in rep.warnings)
    pg2 = ArmViewPg()
    md2 = fakeredis.FakeAsyncRedis()
    await store_bar(md2, bar())
    d = decision_for(bar())
    d.inputs_digest = inputs_digest(RING, (SeriesInput(SERIES.canonical, BAR_END, "03" + "0" * 32),), ())
    pg2.decisions.append(d)
    assert (await replay(pg2, md2)).rows[0]["result"] == "DIGEST_MISMATCH"
    await md2.aclose()


async def test_a_superseded_revision_is_read_as_of_the_decision_or_unverifiable(md: Any) -> None:
    pg = ArmViewPg()
    rev0 = bar()
    await store_bar(md, bar(revision=1, close=Decimal("1.23460"), final_at=BAR_END + 600_000))
    pg.decisions.append(decision_for(rev0))
    assert (await replay(pg, md)).rows[0]["result"] == "UNVERIFIABLE(superseded_expired)"   # aged out of bars_rev
    await store_superseded(md, rev0, BAR_END + 600_000)
    assert (await replay(pg, md)).rows[0]["result"] == "MATCH"


async def test_envelope_changed_is_reported(md: Any) -> None:
    pg = ArmViewPg()
    b = bar()
    await store_bar(md, b)
    pg.decisions.append(decision_for(b))
    rep = await replay(pg, md, envelope=stamp(EnvelopeParams(min_gap_r=Decimal("0.2"))))
    assert rep.rows[0]["envelope"] == "ENVELOPE_CHANGED" and rep.fields["envelope_changed"] == 1
    assert rep.rows[0]["result"] == "MATCH"
    unknown = await replay(pg, md, envelope=None, envelope_problems=("XTNL_ARM_MIN_GAP_R is outside [0, 0.5]",))
    assert unknown.rows[0]["envelope"] == "unknown" and any("XTNL_ARM_MIN_GAP_R" in w for w in unknown.warnings)


async def test_request_backfill_files_one_audited_md_backfill_per_series(md: Any) -> None:
    pg = ArmViewPg()
    pg.heartbeat()
    pg.decisions.append(decision_for(bar()))
    later = BAR_END + 900_000
    pg.decisions.append(decision_for(bar(bucket=BAR_END, final_at=later + 1), decision_id=2, decision_seq=2,
                                     bar_end_ms=later, decided_at_ms=later + 60_000))
    rep = await replay(pg, md, request_backfill=True, actor=ACTOR)
    filed = rep.sections["backfill"]
    assert len(filed) == 1 and filed[0]["outcome"] == "applied" and filed[0]["request"] == "pending"
    req = pg.o.requests[-1]
    assert req["kind"] == "md_backfill" and req["target"] == SERIES.canonical
    assert f'"from_bucket_ms":{BUCKET}' in req["payload"] and f'"to_bucket_ms":{BAR_END}' in req["payload"]
    assert [(a["command"], a["outcome"]) for a in pg.o.audit] == [("md.backfill", "applied")]


async def test_request_backfill_needs_an_actor_and_is_refused_without_a_heartbeat(md: Any) -> None:
    pg = ArmViewPg()
    pg.decisions.append(decision_for(bar()))
    with pytest.raises(OpsUsageError):
        await replay(pg, md, request_backfill=True, actor=None)
    rep = await replay(pg, md, request_backfill=True, actor=ACTOR)
    assert rep.sections["backfill"][0]["refused"] == "no_arm_heartbeat"


async def test_replay_without_reader_or_keyring_is_refused(md: Any) -> None:
    pg = ArmViewPg()
    pg.decisions.append(decision_for(bar()))
    no_key = await replay(pg, md, None)
    assert no_key.outcome == "refused" and no_key.fields["refused"] == "no_keyring"
    v = ArmViews(OpsService(pg, None, clock_ms=lambda: NOW, arm=ArmContext()).arm)
    rep = await v.replay(None, from_ms=None, to_ms=None, limit=5, request_backfill=False, actor=None, wait_s=0)
    assert rep.outcome == "refused" and rep.fields["refused"] == "no_md_reader"


def test_stored_inputs_reads_the_view_row() -> None:
    d = decision_for(bar())
    s = stored_inputs(d.row())
    assert s.decision_id == 1 and s.series_inputs == tuple(d.series_inputs) and s.inputs_digest == d.inputs_digest
    assert s.envelope.version == d.envelope_version


async def test_replay_one_keeps_the_worst_input(md: Any) -> None:
    b = bar()
    await store_bar(md, b)
    missing = bar(bucket=BAR_END, final_at=BAR_END + 900_001)
    inputs = [SeriesInput(SERIES.canonical, x.bucket_end_ms, bar_hash(x)) for x in (b, missing)]
    d = Decision(series_inputs=[i.text for i in inputs], inputs_digest=inputs_digest(RING, tuple(inputs), ()),
                 decided_at_ms=BAR_END + 1_000_000)
    got = await replay_one(reader(md), RING, stored_inputs(d.row()), NOW, None)
    assert got.result is Result.BARS_MISSING and [v.result for v in got.inputs] == [Result.MATCH, Result.BARS_MISSING]


# ============================================================================================== CLI
def _connect(pg: ArmViewPg) -> Any:
    async def connect(dsn: str) -> tuple[PgConnection, cli.Closer]:
        async def close() -> None:
            return None

        return pg, close

    return connect


def test_cli_replay_prints_codes_only() -> None:
    import time

    now = int(time.time() * 1000)                  # the CLI runs on the host clock: bars must not be in the future
    end = now - 3_600_000 - now % 900_000
    at = {"bucket": end - 900_000, "final_at": end + 30_000}
    server = fakeredis.FakeServer()
    seed = fakeredis.FakeRedis(server=server)
    seed.zadd(K.bars(SERIES), {member(bar(**at)): end - 900_000})
    pg = ArmViewPg()
    pg.decisions.append(decision_for(bar(**at), bar_end_ms=end, decided_at_ms=end + 60_000))
    pg.decisions.append(decision_for(bar(close=Decimal("1.23999"), **at), decision_id=2, decision_seq=2,
                                     bar_end_ms=end, decided_at_ms=end + 60_000))
    context = ArmContext(md_reader=lambda: reader(fakeredis.FakeAsyncRedis(server=server)))
    env = {"ORDER_SERVER_DSN": "postgresql://u@h/db", "XTNL_MD_REDIS_READER_URL": READER_URL}
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(["arm", "replay", "mock:P1"], environ=env, connect=_connect(pg), etcd_factory=lambda _e: None,
                    out=out, err=err, arm_context=context)
    text = out.getvalue()
    assert code == cli.EXIT_OK, err.getvalue()
    assert "result=MATCH" in text and "result=CHANGED" in text
    for planted in (str(CLOSE), "1.23999", "1.23401", "Pl4nted-Reader", "planted-host", bar_hash(bar(**at))[2:]):
        assert planted not in text
    err2 = io.StringIO()
    assert cli.main(["arm", "replay", "--request-backfill"], environ=env, connect=_connect(pg),
                    etcd_factory=lambda _e: None, out=io.StringIO(), err=err2,
                    arm_context=context) == cli.EXIT_USAGE
    assert "--operator" in err2.getvalue()


# ============================================================================================== review fixes (S35)
async def test_a_later_refetch_with_the_same_content_matches(md: Any) -> None:
    """md_backfill, the gap heal and a re-bootstrap store a bucket as revision 0 with `final_at_ms` at the fetch time,
    after the decision: identical content still verifies (MATCH); other content stays superseded_expired."""
    pg = ArmViewPg()
    await store_bar(md, bar(final_at=NOW))
    pg.decisions.append(decision_for(bar()))                  # decided at BAR_END + 60 s, before the re-fetch
    rep = await replay(pg, md)
    assert rep.rows[0]["result"] == "MATCH" and rep.rows[0]["digest"] == "MATCH"
    assert bar_as_of(SERIES, BUCKET, [Candidate(member(bar(final_at=NOW)))], BAR_END + 60_000, RING,
                     NOW) == "superseded_expired"             # without the recorded hash nothing stands in
    md2 = fakeredis.FakeAsyncRedis()
    await store_bar(md2, bar(final_at=NOW, close=Decimal("1.23470")))
    assert (await replay(pg, md2)).rows[0]["result"] == "UNVERIFIABLE(superseded_expired)"
    await md2.aclose()


async def test_the_as_of_revision_under_a_retired_key_is_key_retired(md: Any) -> None:
    pg = ArmViewPg()
    rev0 = bar()
    rev1 = bar(revision=1, close=Decimal("1.23460"), final_at=BAR_END + 600_000)
    await store_superseded(md, rev0, BAR_END + 600_000, RING_OLD)             # current at the decision, old key id
    await store_bar(md, rev1)
    pg.decisions.append(decision_for(rev0))
    assert (await replay(pg, md)).rows[0]["result"] == "UNVERIFIABLE(key_retired)"
    both = [Candidate(member(rev1)), Candidate(member(rev0, RING_OLD), superseded_at_ms=BAR_END + 600_000)]
    assert bar_as_of(SERIES, BUCKET, both, BAR_END + 60_000, RING, NOW, expect_hash=bar_hash(rev0)) == "key_retired"


def _missing(*ends: int, series: SeriesId = SERIES) -> DecisionReplay:
    d = stored_inputs(decision_for(bar()).row())
    return DecisionReplay(d, Result.BARS_MISSING, None, "same",
                          [InputVerdict(series.canonical, e, Result.BARS_MISSING) for e in ends])


def _width(b: BackfillRange) -> int:
    return (b.last_bucket_ms - b.first_bucket_ms) // 900_000 + 1


def test_backfill_ranges_are_contiguous_runs_within_the_leader_limit() -> None:
    far = BAR_END - 4_000 * 900_000
    got = backfill_ranges([_missing(BAR_END, BAR_END - 900_000), _missing(far, BAR_END)])
    assert got == [BackfillRange(SERIES.canonical, far - 900_000, far - 900_000),
                   BackfillRange(SERIES.canonical, BUCKET - 900_000, BUCKET)]       # never one 4001-bucket range
    run = backfill_ranges([_missing(*(BAR_END - k * 900_000 for k in range(5)))], chunk_max=2)
    assert [_width(b) for b in run] == [2, 2, 1]                          # oldest first
    assert BACKFILL_CHUNK_MAX <= MD_BACKFILL_MAX
    long = backfill_ranges([_missing(*(BAR_END - k * 900_000 for k in range(BACKFILL_CHUNK_MAX + 3)))])
    assert len(long) == 2 and all(_width(b) <= BACKFILL_CHUNK_MAX for b in long)


async def test_request_backfill_files_one_bounded_range_per_run(md: Any) -> None:
    pg = ArmViewPg()
    pg.heartbeat()
    far = BAR_END - 4_000 * 900_000                            # more than MD_BACKFILL_MAX buckets apart
    pg.decisions.append(decision_for(bar()))
    pg.decisions.append(decision_for(bar(bucket=far - 900_000, final_at=far + 1), decision_id=2, decision_seq=2,
                                     bar_end_ms=far, decided_at_ms=far + 60_000))
    rep = await replay(pg, md, request_backfill=True, actor=ACTOR)
    filed, rest = rep.sections["backfill"], rep.sections["backfill_not_filed"]
    assert len(filed) == 1 and filed[0]["request"] == "pending" and len(rest) == 1   # one pending per series
    req = pg.o.requests[-1]
    assert f'"from_bucket_ms":{far - 900_000}' in req["payload"]
    assert f'"to_bucket_ms":{far - 900_000}' in req["payload"]
    assert len(pg.o.requests) == 1 and any("1 more md_backfill" in w for w in rep.warnings)


def test_broker_reach_is_per_spec() -> None:
    """md-bars reads only specs with a timing row (15m in v1): a missing 1m BROKER bar is never re-fetched."""
    m1 = SeriesId("mock", "SYN.A", M1, Side.BID, Source.BROKER)
    h = bar_hash(bar())
    got = compare_input(SeriesInput(m1.canonical, BAR_END, h), None, RING, NOW)
    assert (got.result, got.reason) == (Result.UNVERIFIABLE, "beyond_broker_reach")
    assert compare_input(SeriesInput(SERIES.canonical, BAR_END, h), None, RING, NOW).result is Result.BARS_MISSING


async def test_a_malformed_input_is_never_echoed(md: Any) -> None:
    pg = ArmViewPg()
    h = bar_hash(bar())
    d = decision_for(bar())
    d.series_inputs = [f"S|{SERIES.canonical}|not-a-time|{h}"]
    pg.decisions.append(d)
    rep = await replay(pg, md)
    row = rep.rows[0]
    assert "malformed#0" in row["inputs"] and "UNVERIFIABLE(malformed_input)" in row["inputs"]
    text = repr((rep.fields, rep.rows, rep.sections, rep.warnings))
    assert h not in text and h[2:14] not in text and "not-a-time" not in text
