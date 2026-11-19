"""`arm replay`: re-verify stored ARM decisions against `redis-md` (design §B8, §A10 `as_of_ms`, §A13, §B6.1 Always
current; plan S35, R11-m "Decisions lack envelope version").

For each stored decision (newest first, bounded) the replay re-reads every bar input from `redis-md` **as of the
decision's `decided_at_ms`** (the current member and the superseded revisions of `md:bars_rev` together: per bucket the
revision with `final_at_ms <= as_of` that was not yet superseded at `as_of`, the highest revision first) and checks
it under the content-hash keyring. When no stored revision was current at `as_of` (it aged out of `md:bars_rev`, or the
bucket was re-fetched later by `md_backfill`, the gap heal or a re-bootstrap, which stores it as revision 0 with
`final_at_ms` at the fetch time), a validated later member whose content verifies against the stored input's keyed hash
stands in for it: the content is the one the decision recorded, whatever its `final_at_ms`.

| Result | When |
|---|---|
| `MATCH` | every bar input re-read and its keyed hash equals the stored one; the stored `inputs_digest` recomputes |
| `CHANGED` | a validated bar read as of the decision hashes differently from the stored input |
| `DIGEST_MISMATCH` | the stored `inputs_digest` does not recompute from the stored input ids and hashes |
| `bars_missing` | a BROKER bar is no longer retained in `redis-md` while `md-bars` can still re-fetch it (its spec has a timing row and the bar is inside `history_reach_ms`); with `--request-backfill` one audited `md_backfill` per contiguous run of missing buckets is filed (at most `BACKFILL_CHUNK_MAX` buckets each; the leader's `md-bars` re-fetches them) |
| `UNVERIFIABLE(reason)` | `key_retired` (the stored hash or digest, or the revision current at the decision, is under a key id the keyring no longer holds), `superseded_expired` (the revision current at the decision aged out of `md:bars_rev` and no later member has the recorded content: never a mismatch, never a backfill), `beyond_broker_reach` (older than the history reach, or a spec `md-bars` does not read), `tick_history_gone` (a TICK bar is never re-fetched), `bar_invalid` (every stored candidate fails its checks), `malformed_input` |
| `NO_INPUTS` | a PRE row (no input bar, C5) or an `UNRESOLVED_RANGE` span |

Separately, `envelope` is `ENVELOPE_CHANGED` when the decision's `envelope_version` / `envelope_params_hash` differ from
this engine's (`ENVELOPE_VERSION` and the local `XTNL_ARM_MIN_GAP_R`): a re-run today would use another envelope.

**Scope (S35):** the replay verifies the inputs a decision recorded. **`--rerun` (plan S39b, go-live gate
`replay_rerun`)** also re-runs the real pipeline for each past bar (`src/ops/arm_rerun.py`, `src/riskmgr/rerun.py`):
read-only, no notice, MATCH / DIFF / UNVERIFIABLE by the recomputed `inputs_digest` and the outcome. Indicator inputs are covered by the digest only (their window bars are not
re-read). Output: ids, bar ends, codes and counts; never a price or a hash value beyond its key id.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Final

from src.marketdata.brokerbars import LANE_MAX_PER_SERIES, MD_BACKFILL_MAX, BrokerBarsSettings
from src.marketdata.codec import DecodedMember, decode_member
from src.marketdata.hashing import HashComparison, HashStatus, Keyring, KeyringError, compare_hashes, key_id_of, \
    verify
from src.marketdata.types import Bar, SeriesId, Source
from src.marketdata.timing import timing_for
from src.marketdata.validate import check_stored
from src.riskmgr.envelope import EnvelopeStamp
from src.riskmgr.types import IndicatorInput, SeriesInput, inputs_digest

from src.ops.md_view import OpsMdReader


class Result(str, Enum):
    MATCH = "MATCH"
    CHANGED = "CHANGED"
    DIGEST_MISMATCH = "DIGEST_MISMATCH"
    BARS_MISSING = "bars_missing"
    UNVERIFIABLE = "UNVERIFIABLE"
    NO_INPUTS = "NO_INPUTS"


#: Worst first: the decision's result is the worst of its inputs' and its digest's.
SEVERITY: Final = (Result.DIGEST_MISMATCH, Result.CHANGED, Result.BARS_MISSING, Result.UNVERIFIABLE, Result.MATCH)
KEY_RETIRED: Final = "key_retired"
SUPERSEDED_EXPIRED: Final = "superseded_expired"
BEYOND_REACH: Final = "beyond_broker_reach"
TICK_GONE: Final = "tick_history_gone"
BAR_INVALID: Final = "bar_invalid"
MALFORMED: Final = "malformed_input"
ENVELOPE_CHANGED: Final = "ENVELOPE_CHANGED"
BACKFILL_CHUNK_MAX: Final = min(MD_BACKFILL_MAX, LANE_MAX_PER_SERIES)
"""Buckets one filed `md_backfill` names: the leader refuses more than `MD_BACKFILL_MAX` (`range_too_large`) and more
than its lane holds (`lane_full`)."""


# ---------------------------------------------------------------------------------------------------- input forms
def parse_series_input(text: str) -> SeriesInput | None:
    """`S|series|bar_end|hash` (`SeriesInput.text`) or `series|bar_end|hash` (the 0017 column comment form)."""
    parts = text.split("|")
    if len(parts) == 4 and parts[0] == "S":
        parts = parts[1:]
    if len(parts) != 3 or not parts[1].isdigit():
        return None
    try:
        return SeriesInput(parts[0], int(parts[1]), parts[2])
    except ValueError:
        return None


def parse_indicator_input(text: str) -> IndicatorInput | None:
    """`I|id|output|bar_end|window_start|hash` (`IndicatorInput.text`) or the same without the `I|` tag."""
    parts = text.split("|")
    if len(parts) == 6 and parts[0] == "I":
        parts = parts[1:]
    if len(parts) != 5 or not parts[2].isdigit() or not parts[3].isdigit():
        return None
    try:
        return IndicatorInput(parts[0], parts[1], int(parts[2]), int(parts[3]), parts[4])
    except ValueError:
        return None


@dataclass(frozen=True)
class DigestCheck:
    result: Result
    reason: str | None = None


def check_digest(keyring: Keyring, series_texts: Sequence[str], indicator_texts: Sequence[str],
                 stored: str | None) -> DigestCheck:
    """Recompute `inputs_digest` under the key id the stored digest names (a retained key verifies; a retired one is
    `UNVERIFIABLE(key_retired)`, never a mismatch)."""
    if stored is None:
        return DigestCheck(Result.UNVERIFIABLE, "no_digest")
    series = [parse_series_input(t) for t in series_texts]
    indicators = [parse_indicator_input(t) for t in indicator_texts]
    kid = key_id_of(stored)
    if kid is None or any(s is None for s in series) or any(i is None for i in indicators):
        return DigestCheck(Result.UNVERIFIABLE, MALFORMED)
    key = keyring.key(kid)
    if key is None:
        return DigestCheck(Result.UNVERIFIABLE, KEY_RETIRED)
    try:
        ring = Keyring.of(kid, key)
    except KeyringError:
        return DigestCheck(Result.UNVERIFIABLE, KEY_RETIRED)
    again = inputs_digest(ring, tuple(s for s in series if s is not None),
                          tuple(i for i in indicators if i is not None))
    return DigestCheck(Result.MATCH if compare_hashes(again, stored) is HashComparison.SAME
                       else Result.DIGEST_MISMATCH)


# ---------------------------------------------------------------------------------------------------- as-of choice
@dataclass(frozen=True)
class Candidate:
    """A stored member of one bucket: the current one (`superseded_at_ms` None) or a superseded revision."""

    member: str = field(repr=False)
    superseded_at_ms: int | None = None


def _current_at(bar: Bar, superseded_at_ms: int | None, as_of_ms: int) -> bool:
    """Final at `as_of_ms` and not yet superseded then."""
    return (bar.final_at_ms is not None and bar.final_at_ms <= as_of_ms
            and (superseded_at_ms is None or superseded_at_ms > as_of_ms))


def bar_as_of(series: SeriesId, bucket_start_ms: int, candidates: Sequence[Candidate], as_of_ms: int,
              keyring: Keyring, now_ms: int, *, expect_hash: str | None = None) -> Bar | str | None:
    """The revision of the bucket current at `as_of_ms` (validated), a reason when none can be used, or None when
    the store holds nothing for the bucket.

    In order: a validated revision current at `as_of_ms` (the highest revision, the current member at a tie); else
    `key_retired` when the revision current then is held only under a retired key id; else, with `expect_hash` (the
    decision's recorded keyed hash), a validated later member whose content verifies against it (a re-fetch or a
    re-bootstrap stores the same content with a later `final_at_ms`); else `superseded_expired` when some member is
    usable, `key_retired` when only retired-key members remain, `bar_invalid` otherwise."""
    if not candidates:
        return None
    best: tuple[int, int, Bar] | None = None
    later: list[Bar] = []
    retired = 0
    retired_as_of = False
    for c in candidates:
        d = decode_member(series, c.member)
        if not isinstance(d, DecodedMember) or d.bar.bucket_start_ms != bucket_start_ms:
            continue
        checked = check_stored(d.bar, d.content_hash, keyring, now_ms=now_ms)
        if not checked.usable:
            if checked.result.ok and checked.hash_status is HashStatus.MISSING:
                retired += 1
                retired_as_of = retired_as_of or _current_at(d.bar, c.superseded_at_ms, as_of_ms)
            continue
        bar = d.bar
        if not _current_at(bar, c.superseded_at_ms, as_of_ms):
            later.append(bar)
            continue
        key = (bar.revision, 1 if c.superseded_at_ms is None else 0)
        if best is None or key > best[:2]:
            best = (key[0], key[1], bar)
    if best is not None:
        return best[2]
    if retired_as_of:
        return KEY_RETIRED
    if expect_hash is not None:
        for bar in later:
            status = verify(keyring, expect_hash, series.canonical, bar.bucket_end_ms, bar.open, bar.high, bar.low,
                            bar.close)
            if status is HashStatus.VALID:
                return bar
            if status is HashStatus.MISSING:
                return KEY_RETIRED              # the recorded hash's key id is retired: nothing can verify it
    if later:
        return SUPERSEDED_EXPIRED
    return KEY_RETIRED if retired else BAR_INVALID


def broker_reach_ms(series: SeriesId) -> int:
    """How far back `md-bars` can re-fetch a BROKER series: its `history_reach_ms` for a spec with a timing row (the
    only specs `md-bars` reads, `timing_for`), else 0 (a missing bar of any other spec is never re-fetched)."""
    if timing_for(series.spec) is None:
        return 0
    return BrokerBarsSettings().history_reach_ms


@dataclass(frozen=True)
class InputVerdict:
    series_id: str
    bar_end_ms: int
    result: Result
    reason: str | None = None

    @property
    def text(self) -> str:
        return self.result.value + (f"({self.reason})" if self.reason else "")


def compare_input(inp: SeriesInput, read: Bar | str | None, keyring: Keyring, now_ms: int,
                  reach_ms: int | None = None) -> InputVerdict:
    """One input's verdict; `reach_ms` overrides the series' `broker_reach_ms` (tests)."""
    sid = SeriesId.parse(inp.series_id)
    if read is None:
        if sid.source is Source.TICK:
            return InputVerdict(inp.series_id, inp.bar_end_ms, Result.UNVERIFIABLE, TICK_GONE)
        if now_ms - inp.bar_end_ms > (broker_reach_ms(sid) if reach_ms is None else reach_ms):
            return InputVerdict(inp.series_id, inp.bar_end_ms, Result.UNVERIFIABLE, BEYOND_REACH)
        return InputVerdict(inp.series_id, inp.bar_end_ms, Result.BARS_MISSING)
    if isinstance(read, str):
        return InputVerdict(inp.series_id, inp.bar_end_ms, Result.UNVERIFIABLE, read)
    status = verify(keyring, inp.content_hash, sid.canonical, inp.bar_end_ms, read.open, read.high, read.low,
                    read.close)
    if status is HashStatus.MISSING:
        return InputVerdict(inp.series_id, inp.bar_end_ms, Result.UNVERIFIABLE, KEY_RETIRED)
    return InputVerdict(inp.series_id, inp.bar_end_ms,
                        Result.MATCH if status is HashStatus.VALID else Result.CHANGED)


# ---------------------------------------------------------------------------------------------------- one decision
@dataclass(frozen=True)
class StoredInputs:
    """What the replay reads of one `arm_decisions` row (ids, codes, keyed hashes; no level)."""

    decision_id: int
    bar_end_ms: int
    position: str
    event: str
    revision: int
    outcome: str
    decided_at_ms: int
    series_inputs: tuple[str, ...]
    indicator_inputs: tuple[str, ...]
    inputs_digest: str | None
    envelope: EnvelopeStamp


@dataclass
class DecisionReplay:
    stored: StoredInputs
    result: Result
    reason: str | None
    envelope: str
    inputs: list[InputVerdict] = field(default_factory=list)
    digest: DigestCheck | None = None

    @property
    def missing(self) -> list[InputVerdict]:
        return [v for v in self.inputs if v.result is Result.BARS_MISSING]


def worst(results: Sequence[tuple[Result, str | None]]) -> tuple[Result, str | None]:
    for r in SEVERITY:
        for got, reason in results:
            if got is r:
                return got, reason
    return Result.MATCH, None


def envelope_text(recorded: EnvelopeStamp, current: EnvelopeStamp | None) -> str:
    if current is None:
        return "unknown"
    return "same" if recorded == current else ENVELOPE_CHANGED


async def read_candidates(reader: OpsMdReader, series: SeriesId, bucket_start_ms: int) -> list[Candidate]:
    current = await reader.call(reader.store.bars_range(series, bucket_start_ms, bucket_start_ms))
    older = await reader.call(reader.store.superseded_range(series, bucket_start_ms, bucket_start_ms))
    return ([Candidate(m) for _, m in current]
            + [Candidate(m, superseded_at) for _, m, superseded_at in older])


async def replay_one(reader: OpsMdReader, keyring: Keyring, d: StoredInputs, now_ms: int,
                     current_envelope: EnvelopeStamp | None) -> DecisionReplay:
    env = envelope_text(d.envelope, current_envelope)
    if not d.series_inputs and not d.indicator_inputs:
        return DecisionReplay(d, Result.NO_INPUTS, None, env)
    out = DecisionReplay(d, Result.MATCH, None, env)
    parsed = [parse_series_input(t) for t in d.series_inputs]
    for i, inp in enumerate(parsed):
        if inp is None:                     # never echo the stored text: it carries a keyed hash
            out.inputs.append(InputVerdict(f"malformed#{i}", 0, Result.UNVERIFIABLE, MALFORMED))
            continue
        sid = SeriesId.parse(inp.series_id)
        start = inp.bar_end_ms - sid.spec.timeframe_ms
        read = bar_as_of(sid, start, await read_candidates(reader, sid, start), d.decided_at_ms, keyring, now_ms,
                         expect_hash=inp.content_hash)
        out.inputs.append(compare_input(inp, read, keyring, now_ms))
    out.digest = check_digest(keyring, d.series_inputs, d.indicator_inputs, d.inputs_digest)
    out.result, out.reason = worst([(v.result, v.reason) for v in out.inputs]
                                   + [(out.digest.result, out.digest.reason)])
    return out


@dataclass(frozen=True)
class BackfillRange:
    """One `md_backfill` to file: a contiguous run of missing bucket starts of one BROKER series."""

    series_id: str
    first_bucket_ms: int
    last_bucket_ms: int


def backfill_ranges(replays: Sequence[DecisionReplay],
                    chunk_max: int = BACKFILL_CHUNK_MAX) -> list[BackfillRange]:
    """The `bars_missing` bucket starts grouped per series into contiguous runs (adjacent buckets only, so nothing
    between two missing bars is re-fetched), each split into chunks of at most `chunk_max` buckets; sorted by series,
    then oldest first."""
    starts: dict[str, set[int]] = {}
    for rp in replays:
        for v in rp.missing:
            sid = SeriesId.parse(v.series_id)
            starts.setdefault(v.series_id, set()).add(v.bar_end_ms - sid.spec.timeframe_ms)
    out: list[BackfillRange] = []
    for series_id in sorted(starts):
        tf = SeriesId.parse(series_id).spec.timeframe_ms
        run: list[int] = []
        for start in sorted(starts[series_id]):
            if run and (start != run[-1] + tf or len(run) >= chunk_max):
                out.append(BackfillRange(series_id, run[0], run[-1]))
                run = []
            run.append(start)
        if run:
            out.append(BackfillRange(series_id, run[0], run[-1]))
    return out


__all__ = [
    "BACKFILL_CHUNK_MAX", "BackfillRange", "Candidate", "DecisionReplay", "DigestCheck", "ENVELOPE_CHANGED",
    "InputVerdict", "Result", "StoredInputs", "backfill_ranges", "bar_as_of", "broker_reach_ms", "check_digest",
    "compare_input", "envelope_text", "parse_indicator_input", "parse_series_input", "replay_one", "worst",
]
