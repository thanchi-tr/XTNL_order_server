"""`redis-md` key names, the writer token and the boot sentinel (design §A3, §A7, §A13; plan S13).

Every key starts with `md:` (the ACL of `md_writer` and `md_reader` covers `md:*` only). The `{series}` part is the
canonical `SeriesId` string (`adapter:symbol:spec:side:source`), whose parts are key-safe (`types._PART_RE`).

* `WriterToken` = `md:writer` = `cluster_id:epoch_rev:node:term_nonce`. A new leader SETs it unconditionally after
  its lease and `engine_epoch` checks; every write script requires equality, so a fenced ex-leader cannot write. The
  term nonce comes from the runtime through a port (the lockout refuses `uuid`, `secrets` and `random` here).
* `BootSentinel` = `md:boot` = `run_id:cluster_id:node:generation` (generation `epoch_rev.n`), written last by the
  bootstrap. Ordinary writes require the exact value **and** that its `run_id` equals the connection's current
  `run_id` (read from `INFO server` on every (re)connect), so a flush, an older AOF, a takeover or a Redis restart
  with the AOF reloaded all give `NO_BOOT`.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

from src.marketdata.types import MdGeneration, SeriesId

PREFIX: Final = "md:"
WRITER: Final = "md:writer"
BOOT: Final = "md:boot"
BOOT_LOCK: Final = "md:boot_lock"
EVENTS: Final = "md:events"
REGISTRY_SERIES: Final = "md:registry:series"
REGISTRY_IND: Final = "md:registry:ind"
LEASE_INDEX: Final = "md:registry:leases"
"""`consumer:spec_id` → lease value of every lease written through `LEASE` (S16): the bootstrap finds the live leases
in the store from it (each checked with `EXISTS md:demand:...`) without `KEYS` / `SCAN`."""
STORED: Final = "md:registry:stored"
"""Every series (`adapter:symbol:spec:side:source`) and tick stream (`adapter:symbol`) the writer has written (S16
`INDEX`), so a later term's bootstrap knows the stored `md:meta:*` / `md:ticks:*` without `KEYS` / `SCAN`."""

EVENTS_MAXLEN: Final = 100_000
"""`XADD md:events MAXLEN ~ 100000` (design §A3)."""
REV_RETENTION_MS: Final = 7 * 24 * 3600 * 1000
"""`md:bars_rev` and `md:quarantine` keep 7 days (by bucket start)."""
BOOT_LOCK_PX: Final = 10_000
"""`md:boot_lock` is taken with `SET NX PX 10000` and renewed every 2 s (design §A7 step 2)."""

_PART_RE: Final = re.compile(r"[A-Za-z0-9._/#-]+")
"""A key part: no `:` (the separator), space, braces or glob characters."""
_TOKEN_PART_RE: Final = re.compile(r"[A-Za-z0-9._-]+")
_RUN_ID_RE: Final = re.compile(r"[A-Za-z0-9]+")


def _part(value: str, what: str) -> str:
    if not _PART_RE.fullmatch(value):
        raise ValueError(f"{what} must match {_PART_RE.pattern}")
    return value


def ticks(adapter_id: str, symbol: str) -> str:
    return f"md:ticks:{_part(adapter_id, 'adapter_id')}:{_part(symbol, 'symbol')}"


def bars(series: SeriesId) -> str:
    return f"md:bars:{series.canonical}"


def bars_rev(series: SeriesId) -> str:
    return f"md:bars_rev:{series.canonical}"


def forming(series: SeriesId) -> str:
    return f"md:forming:{series.canonical}"


def meta(series: SeriesId) -> str:
    return f"md:meta:{series.canonical}"


def rollback(series: SeriesId) -> str:
    return f"md:rb:{series.canonical}"


def quarantine(series: SeriesId) -> str:
    return f"md:quarantine:{series.canonical}"


def demand(consumer: str, spec_id: str) -> str:
    return f"md:demand:{_part(consumer, 'consumer')}:{_part(spec_id, 'spec_id')}"


def lease_index_field(consumer: str, spec_id: str) -> str:
    """The `md:registry:leases` field of a lease: `consumer:spec_id` (both key parts, so it splits back exactly)."""
    return f"{_part(consumer, 'consumer')}:{_part(spec_id, 'spec_id')}"


def market_field(adapter_id: str, symbol: str) -> str:
    """The `md:registry:stored` field of a tick stream: `adapter:symbol` (a series field has five parts)."""
    return f"{_part(adapter_id, 'adapter_id')}:{_part(symbol, 'symbol')}"


def skew(adapter_id: str) -> str:
    return f"md:skew:{_part(adapter_id, 'adapter_id')}"


def indicator(indicator_id: str, output: str) -> str:
    return f"md:ind:{_part(indicator_id, 'indicator_id')}:{_part(output, 'output')}"


def indicator_checkpoints(indicator_id: str) -> str:
    return f"md:ind_ckpt:{_part(indicator_id, 'indicator_id')}"


def series_keys(series: SeriesId) -> tuple[str, str, str, str, str]:
    """The five per-series keys a flush writes, in the script's order: bars, bars_rev, forming, meta, rb."""
    return (bars(series), bars_rev(series), forming(series), meta(series), rollback(series))


# ---------------------------------------------------------------------------------------------------- token, sentinel
@dataclass(frozen=True)
class WriterToken:
    """`md:writer`: `(cluster_id, epoch revision, node, term nonce)`."""

    cluster_id: str
    epoch_rev: int
    node: str
    term_nonce: str

    def __post_init__(self) -> None:
        for what, value in (("cluster_id", self.cluster_id), ("node", self.node), ("term_nonce", self.term_nonce)):
            if not _TOKEN_PART_RE.fullmatch(value):
                raise ValueError(f"writer token {what} must match {_TOKEN_PART_RE.pattern}")
        if self.epoch_rev < 0:
            raise ValueError("writer token epoch revision must not be negative")

    @property
    def text(self) -> str:
        return f"{self.cluster_id}:{self.epoch_rev}:{self.node}:{self.term_nonce}"


@dataclass(frozen=True)
class BootSentinel:
    """`md:boot`: `run_id:cluster_id:node:generation`."""

    run_id: str
    cluster_id: str
    node: str
    generation: MdGeneration

    def __post_init__(self) -> None:
        if not _RUN_ID_RE.fullmatch(self.run_id):
            raise ValueError(f"run_id must match {_RUN_ID_RE.pattern}")
        for what, value in (("cluster_id", self.cluster_id), ("node", self.node)):
            if not _TOKEN_PART_RE.fullmatch(value):
                raise ValueError(f"boot sentinel {what} must match {_TOKEN_PART_RE.pattern}")
        if self.generation.epoch_rev < 0 or self.generation.n < 0:
            raise ValueError("md generation parts must not be negative")

    @property
    def text(self) -> str:
        return f"{self.run_id}:{self.cluster_id}:{self.node}:{self.generation}"

    @classmethod
    def parse(cls, text: str | bytes | None) -> BootSentinel | None:
        """The sentinel stored in `md:boot`, or None for a missing or malformed value (never raises)."""
        if text is None:
            return None
        try:
            raw = text.decode("ascii") if isinstance(text, bytes) else text
        except UnicodeDecodeError:
            return None
        m = re.fullmatch(r"([A-Za-z0-9]+):([A-Za-z0-9._-]+):([A-Za-z0-9._-]+):([0-9]{1,18})\.([0-9]{1,18})", raw)
        if m is None:
            return None
        return cls(m.group(1), m.group(2), m.group(3), MdGeneration(int(m.group(4)), int(m.group(5))))


def valid_run_id(run_id: str) -> bool:
    return _RUN_ID_RE.fullmatch(run_id) is not None
