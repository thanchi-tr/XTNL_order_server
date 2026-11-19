"""Read-only `redis-md` views of the operator CLI: `md show | bars | indicators` (design §C2 Operator views, §A3, §A7,
§A13; plan S35).

The CLI reads `redis-md` only as `md_reader` (`XTNL_MD_REDIS_READER_URL`, the read-only ACL: `GET`, `HGETALL`,
`ZRANGE`, `ZRANGEBYSCORE`, `EXISTS`; never `KEYS` / `SCAN`, never a write) and, for the keyed hash checks of `md bars`
and `arm replay`, the content-hash keyring (`XTNL_MD_HMAC_KEY` / `_ID`, and `_PREV` / `_PREV_ID` during a rotation).
Those are the only market-data secrets the ops CLI reads (design §C2 Secrets); they are never printed, and a problem
names the variable only.

* `md show`: the md generation (`md:boot`), the writer's node and epoch (`md:writer`, never its nonce), and per stored
  series (`md:registry:stored`) its `md:meta` states and counters (SM12, D6 status, cross-check counts, coverage
  counts, first / last bucket, `write_seq`, quarantined), the rollback-set depth and, with the keyring, whether its
  re-hash under the current key id is done (`rehash_key`); the FAULTED indicators.
* `md bars <series>`: counts by quality, revised and superseded members, quarantined buckets with their check names
  (and when they were quarantined), and the newest buckets with revision, quality, finality, tick count and, with the
  keyring, the keyed-hash status. **No price**: members are decoded only to classify them. Every read is bounded
  (`COUNT_MAX` newest members, revisions and quarantine entries; `truncated` says when the series holds more).
* `md indicators`: per registered indicator its class `KIND:VERSION`, input series, params, and per output the newest
  value's bucket, quality, warm flag, finality, window start, value revision and FAULTED reason, never the value (and
  never the spec's price increment).

Every text read from the store is escaped (control characters as `\\xNN`) and bounded; `md:meta` values are shown only
when they are integers or plain codes.
"""
from __future__ import annotations

import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final

from redis.asyncio import Redis
from redis.exceptions import RedisError

from src.armconfig.validate import escape_text
from src.marketdata.codec import DecodedMember, decode_member
from src.marketdata.config import (
    HMAC_KEY_ID_VAR,
    HMAC_KEY_PREV_ID_VAR,
    HMAC_KEY_PREV_VAR,
    HMAC_KEY_VAR,
    READER_URL_VAR,
)
from src.marketdata.hashing import KEY_ID_MAX, KEY_ID_MIN, HashStatus, Keyring, KeyringError, verify
from src.marketdata.indicators.base import IndicatorSpec, decode_value
from src.marketdata.indicators.registry import REGISTRY as INDICATOR_REGISTRY
from src.marketdata.store import keys as K
from src.marketdata.store.client import MdReadClient, describe
from src.marketdata.store.reads import StoreReader, StoreUnavailable, parse_superseded
from src.marketdata.types import Bar, SeriesId

from src.ops.model import OpsRefused, OpsUsageError, Report, iso_ms

_URL_RE: Final = re.compile(r"rediss?://\S+")
_KEY_RE: Final = re.compile(r"[0-9a-fA-F]{64}")
_INT_RE: Final = re.compile(r"[0-9]{1,3}")
META_NAME_RE: Final = re.compile(r"[a-z][a-z0-9_]{0,31}")
META_VALUE_RE: Final = re.compile(r"-?[0-9]{1,20}|[A-Za-z][A-Za-z_]{0,31}")
"""`md:meta` values shown: integers (bucket starts, counts, `write_seq`) and plain codes (SM12, D6, phase)."""
_QUARANTINE_RE: Final = re.compile(r"([0-9]{13})\|([a-z][a-z0-9_]{0,31})\|([0-9]{13})\|.*")
TIME_FIELDS: Final = frozenset({"first_bucket", "last_bucket"})
NEWEST_DEFAULT: Final = 16
NEWEST_MAX: Final = 200
QUARANTINE_SHOWN: Final = 50
COUNT_MAX: Final = 50_000
"""`md bars` reads at most this many stored members, superseded revisions and quarantine entries of a series (the
newest ones; the counts say so when the series holds more)."""
TEXT_LIMIT: Final = 120


# ---------------------------------------------------------------------------------------------------- settings
@dataclass(frozen=True)
class MdReaderSettings:
    """The reader URL (secret) and, when set, the content-hash keyring (`repr` shows key ids only)."""

    reader_url: str = field(repr=False)
    keyring: Keyring | None = None
    keyring_problems: tuple[str, ...] = ()


def _key(env: Mapping[str, str], name: str, problems: list[str]) -> bytes | None:
    raw = (env.get(name) or "").strip()
    if not raw:
        return None
    if not _KEY_RE.fullmatch(raw):
        problems.append(f"{name} is not 64 hex digits (32 bytes)")
        return None
    return bytes.fromhex(raw)


def _key_id(env: Mapping[str, str], name: str, problems: list[str]) -> int | None:
    raw = (env.get(name) or "").strip()
    if not raw:
        return None
    if not _INT_RE.fullmatch(raw) or not KEY_ID_MIN <= int(raw) <= KEY_ID_MAX:
        problems.append(f"{name} is not an integer in [{KEY_ID_MIN}, {KEY_ID_MAX}]")
        return None
    return int(raw)


def parse_keyring(env: Mapping[str, str]) -> Keyring | None | list[str]:
    """The keyring from the `XTNL_MD_HMAC_KEY*` variables: None when no key is set, the problems (names only) for a
    bad or partial setting. Never raises."""
    problems: list[str] = []
    current, current_id = _key(env, HMAC_KEY_VAR, problems), _key_id(env, HMAC_KEY_ID_VAR, problems)
    prev, prev_id = _key(env, HMAC_KEY_PREV_VAR, problems), _key_id(env, HMAC_KEY_PREV_ID_VAR, problems)
    if problems:
        return problems
    if current is None and current_id is None and prev is None and prev_id is None:
        return None
    if current is None or current_id is None:
        return [f"{HMAC_KEY_VAR} and {HMAC_KEY_ID_VAR} go together"]
    if (prev is None) != (prev_id is None):
        return [f"{HMAC_KEY_PREV_VAR} and {HMAC_KEY_PREV_ID_VAR} go together"]
    try:
        return Keyring.of(current_id, current, () if prev is None or prev_id is None else ((prev_id, prev),))
    except KeyringError as exc:
        return [f"{HMAC_KEY_VAR} / {HMAC_KEY_PREV_VAR}: {exc}"]


def parse_reader_settings(env: Mapping[str, str]) -> MdReaderSettings | None | list[str]:
    """None without `XTNL_MD_REDIS_READER_URL`; the problems (names only) for a malformed URL. A bad keyring does not
    stop the reader: it is kept as `keyring_problems` for the commands that need the key."""
    raw = (env.get(READER_URL_VAR) or "").strip()
    if not raw:
        return None
    if not _URL_RE.fullmatch(raw):
        return [f"{READER_URL_VAR} is not a redis:// or rediss:// URL"]
    ring = parse_keyring(env)
    if isinstance(ring, list):
        return MdReaderSettings(reader_url=raw, keyring=None, keyring_problems=tuple(ring))
    return MdReaderSettings(reader_url=raw, keyring=ring)


# ---------------------------------------------------------------------------------------------------- the reader
@dataclass(frozen=True)
class QuarantineEntry:
    """One `md:quarantine` member, parsed for its times and check name only (the record carries prices)."""

    at_ms: int
    check: str
    bucket_start_ms: int


def parse_quarantine(member: str) -> QuarantineEntry | None:
    m = _QUARANTINE_RE.fullmatch(member)
    if m is None:
        return None
    return QuarantineEntry(int(m.group(1)), m.group(2), int(m.group(3)))


def _text(v: object) -> str:
    return v.decode("ascii", "replace") if isinstance(v, bytes) else str(v)


class OpsMdReader:
    """`md_reader` reads for the CLI over one redis-py client. Every store error is an `OpsRefused` (`md_unreadable`,
    the class name and allow-listed code only: never the URL)."""

    def __init__(self, redis: Redis, keyring: Keyring | None = None, keyring_problems: Sequence[str] = (),
                 close: Callable[[], Any] | None = None) -> None:
        self._redis = redis
        self._store = StoreReader(redis)
        self.keyring = keyring
        self.keyring_problems = tuple(keyring_problems)
        self._close = close

    @classmethod
    def connect(cls, settings: MdReaderSettings) -> OpsMdReader:
        client = MdReadClient.connect(settings.reader_url)
        return cls(client.redis, settings.keyring, settings.keyring_problems, client.aclose)

    async def aclose(self) -> None:
        if self._close is not None:
            try:
                await self._close()
            except Exception:
                pass

    @property
    def store(self) -> StoreReader:
        return self._store

    def require_keyring(self) -> Keyring:
        if self.keyring is None:
            detail = "; ".join(self.keyring_problems) or f"{HMAC_KEY_VAR} / {HMAC_KEY_ID_VAR} are not set"
            raise OpsRefused("no_keyring", detail)
        return self.keyring

    async def call(self, coro: Any) -> Any:
        try:
            return await coro
        except StoreUnavailable as exc:
            raise OpsRefused("md_unreadable", f"redis-md read failed ({describe(exc)})") from None
        except (RedisError, TimeoutError, OSError) as exc:
            raise OpsRefused("md_unreadable", f"redis-md read failed ({describe(exc)})") from None

    async def stored_series(self) -> list[SeriesId]:
        out: list[SeriesId] = []
        for name in sorted(await self.call(self._store.stored_index())):
            if name.count(":") != 4:
                continue          # a tick stream (adapter:symbol)
            try:
                out.append(SeriesId.parse(name))
            except ValueError:
                continue
        return out

    async def quarantine(self, series: SeriesId, newest: int = COUNT_MAX) -> tuple[list[QuarantineEntry], bool]:
        """The newest `newest` quarantine entries (by bucket), parsed, and whether the read was cut there."""
        raw = await self.call(self._redis.zrange(K.quarantine(series), -newest, -1))
        parsed = (parse_quarantine(_text(m)) for m in raw)
        return [q for q in parsed if q is not None], len(raw) >= newest

    async def superseded_count(self, series: SeriesId, from_start_ms: int, cap: int = COUNT_MAX) -> tuple[int, bool]:
        """The superseded revisions of buckets at or after `from_start_ms`, counted up to `cap` (one bounded
        `ZRANGEBYSCORE ... LIMIT`), and whether the count was cut there."""
        raw = await self.call(self._redis.zrangebyscore(K.bars_rev(series), from_start_ms, "+inf", start=0, num=cap))
        return sum(1 for m in raw if parse_superseded(m) is not None), len(raw) >= cap


def series_arg(text: str) -> SeriesId:
    try:
        sid = SeriesId.parse(text)
    except ValueError as exc:
        raise OpsUsageError("the series id is adapter:symbol:spec:SIDE:SOURCE (`md show` names it)") from exc
    if sid.canonical != text:
        raise OpsUsageError("give the series id in its canonical form")
    return sid


def safe_meta(meta: Mapping[str, str]) -> dict[str, Any]:
    """The shown `md:meta` fields: names and values of the allowed forms only; bucket starts as times."""
    out: dict[str, Any] = {}
    for name in sorted(meta):
        value = meta[name]
        if not META_NAME_RE.fullmatch(name) or not META_VALUE_RE.fullmatch(value):
            continue
        out[name] = iso_ms(int(value)) if name in TIME_FIELDS and value.isdigit() and value != "0" else value
    return out


def _token_parts(text: str | None) -> tuple[str | None, str | None]:
    """`(node, epoch)` of `md:writer` (`cluster:epoch:node:nonce`); the nonce is never shown."""
    if text is None:
        return None, None
    parts = text.split(":")
    if len(parts) != 4:
        return None, None
    return escape_text(parts[2], 64), parts[1] if parts[1].isdigit() else None


def hash_status(keyring: Keyring | None, decoded: DecodedMember) -> str:
    if keyring is None:
        return "unchecked"
    b = decoded.bar
    status = verify(keyring, decoded.content_hash, b.series.canonical, b.bucket_end_ms, b.open, b.high, b.low,
                    b.close)
    return {HashStatus.VALID: "valid", HashStatus.MISMATCH: "mismatch", HashStatus.MISSING: "key_retired"}[status]


# ---------------------------------------------------------------------------------------------------- the views
class MdViews:
    def __init__(self, reader: OpsMdReader, clock_ms: Callable[[], int], count_max: int = COUNT_MAX) -> None:
        self._r = reader
        self._now = clock_ms
        self._count_max = count_max

    async def show(self) -> Report:
        rep = Report("md.show")
        r = self._r
        head = await r.call(r.store.head())
        rep.fields["md_generation"] = None if head.boot is None else str(head.boot.generation)
        rep.fields["boot_node"] = None if head.boot is None else escape_text(head.boot.node, 64)
        node, epoch = _token_parts(head.writer)
        rep.fields.update({"writer_node": node, "writer_epoch": epoch})
        if head.boot is None:
            rep.warnings.append("md:boot is missing: the store has no generation (a bootstrap has not finished); "
                                "every decision read is DATA_UNAVAILABLE")
        series = await r.stored_series()
        metas = await r.call(r.store.metas(series)) if series else {}
        current_key = None if r.keyring is None else str(r.keyring.current_id)
        states: Counter[str] = Counter()
        rehash_pending = 0
        rows: list[dict[str, Any]] = []
        for s in series:
            meta = safe_meta(metas.get(s, {}))
            depth = len(await r.call(r.store.rollback_seqs(s)))
            states[str(meta.get("sm12", "-"))] += 1
            row: dict[str, Any] = {"series": s.canonical, "rollback_depth": depth, **meta}
            if current_key is not None:
                done = meta.get("rehash_key") == current_key
                row["rehashed"] = done
                rehash_pending += 0 if done else 1
            rows.append(row)
        rep.sections["series"] = rows
        rep.fields["series"] = len(series)
        rep.fields["sm12"] = ",".join(f"{k}={v}" for k, v in sorted(states.items())) or None
        if current_key is not None:
            rep.fields["hash_key_id"] = int(current_key)
            rep.fields["rehash"] = "complete" if rehash_pending == 0 else f"{rehash_pending} series pending"
        else:
            rep.warnings.append("no content-hash keyring (XTNL_MD_HMAC_KEY / _ID): the re-hash progress is not shown")
        faulted = [row for row in await self._indicator_rows() if row.get("faulted")]
        rep.sections["faulted_indicators"] = faulted
        rep.fields["faulted_indicators"] = len(faulted)
        return rep

    async def bars(self, series: SeriesId, *, newest: int) -> Report:
        if not 1 <= newest <= NEWEST_MAX:
            raise OpsUsageError(f"--newest must be between 1 and {NEWEST_MAX}")
        rep = Report("md.bars")
        r = self._r
        rep.fields["series"] = series.canonical
        cap = self._count_max
        members = await r.call(r.store.bars_newest(series, cap))
        cut = len(members) >= cap
        superseded, sup_cut = await r.superseded_count(series, members[0][0] if cut else 0, cap)
        quarantined, q_cut = await r.quarantine(series, cap)
        rep.fields["truncated"] = cut or sup_cut or q_cut
        if rep.fields["truncated"]:
            rep.warnings.append(f"the counts cover at most the newest {cap} stored buckets, superseded revisions and "
                                "quarantine entries (the series holds more; `first_bucket` is the oldest read)")
        decoded: list[tuple[int, DecodedMember | None, str]] = []
        for start, member in members:
            got = decode_member(series, member)
            decoded.append((start, got, "") if isinstance(got, DecodedMember) else (start, None, got.reason))
        quality: Counter[str] = Counter()
        for _, d, reason in decoded:
            quality[d.bar.quality.value if d is not None else f"undecodable:{reason}"] += 1
        rep.fields.update({
            "stored": len(members), "quality": ",".join(f"{k}={v}" for k, v in sorted(quality.items())) or None,
            "revised": sum(1 for _, d, _ in decoded if d is not None and d.bar.revision > 0),
            "not_final": sum(1 for _, d, _ in decoded if d is not None and d.bar.final_at_ms is None),
            "superseded_revisions": superseded, "quarantined": len(quarantined),
            "first_bucket": iso_ms(members[0][0]) if members else None,
            "last_bucket": iso_ms(members[-1][0]) if members else None})
        if r.keyring is None:
            rep.warnings.append("no content-hash keyring (XTNL_MD_HMAC_KEY / _ID): hashes are not checked")
        rows: list[dict[str, Any]] = []
        for start, d, reason in decoded[-newest:]:
            if d is None:
                rows.append({"bucket": iso_ms(start), "decoded": False, "problem": reason})
                continue
            b: Bar = d.bar
            rows.append({"bucket": iso_ms(start), "revision": b.revision, "quality": b.quality.value,
                         "final_at": iso_ms(b.final_at_ms), "ticks": b.tick_count,
                         "hash": hash_status(r.keyring, d)})
        rep.sections["newest"] = rows
        checks: Counter[str] = Counter(q.check for q in quarantined)
        rep.fields["quarantine_checks"] = ",".join(f"{k}={v}" for k, v in sorted(checks.items())) or None
        rep.sections["quarantine"] = [{"bucket": iso_ms(q.bucket_start_ms), "check": q.check, "at": iso_ms(q.at_ms)}
                                      for q in quarantined[-QUARANTINE_SHOWN:]]
        return rep

    async def _indicator_rows(self, only: str | None = None) -> list[dict[str, Any]]:
        r = self._r
        defs = await r.call(r.store.registry_ind())
        rows: list[dict[str, Any]] = []
        for ind_id in sorted(defs):
            if only is not None and ind_id != only:
                continue
            spec = IndicatorSpec.parse(defs[ind_id])
            if spec is None:
                rows.append({"indicator": escape_text(ind_id, 32), "class": None, "problem": "undecodable_spec",
                             "faulted": True})
                continue
            cls = INDICATOR_REGISTRY.get(spec.kind, spec.version)
            outputs = [o.name for o in cls.OUTPUTS] if cls is not None else []
            base: dict[str, Any] = {"indicator": escape_text(ind_id, 32), "class": f"{spec.kind}:{spec.version}",
                                    "series": spec.series.canonical,
                                    "params": escape_text(",".join(f"{k}={v}" for k, v in spec.params), TEXT_LIMIT)}
            if cls is None:
                rows.append({**base, "problem": "class_not_in_registry", "faulted": True})
                continue
            for out in outputs:
                raw = await r.call(r.store.indicator_newest(ind_id, out, 1))
                v = decode_value(ind_id, out, raw[-1][1]) if raw else None
                row = {**base, "output": out}
                if v is None:
                    row.update({"bucket": None, "faulted": False, "problem": "no_value" if not raw else "undecodable"})
                else:
                    row.update({"bucket": iso_ms(v.bucket_start_ms), "quality": v.quality.value, "warm": v.warm,
                                "final_at": iso_ms(v.final_at_ms), "window_start": iso_ms(v.lineage.window_start_ms),
                                "value_revision": v.lineage.value_revision, "faulted": v.faulted,
                                "fault_reason": escape_text(v.lineage.reason, 64) or None})
                rows.append(row)
        return rows

    async def indicators(self, *, indicator_id: str | None) -> Report:
        rep = Report("md.indicators")
        rows = await self._indicator_rows(indicator_id)
        rep.rows.extend(rows)
        rep.fields["indicators"] = len({row["indicator"] for row in rows})
        rep.fields["faulted"] = sum(1 for row in rows if row.get("faulted"))
        if rep.fields["faulted"]:
            rep.warnings.append("a FAULTED indicator gives no value until `md rebuild <indicator_id>`")
        return rep


__all__ = ["MdReaderSettings", "MdViews", "OpsMdReader", "QuarantineEntry", "parse_keyring", "parse_quarantine",
           "parse_reader_settings", "safe_meta", "series_arg"]
