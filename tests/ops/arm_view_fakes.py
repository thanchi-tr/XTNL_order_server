"""Fakes for the S35 views (plan S35): `ArmViewPg` (the S34 `ArmOpsPg` plus handlers for the view SQL of
`src/ops/arm_sql.py`) and a `redis-md` seeder over fakeredis. Not a SQL engine: each handler implements its statement's
documented semantics. Synthetic instruments, accounts and prices only."""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from src.marketdata.codec import encode_member
from src.marketdata.hashing import Keyring, content_hash
from src.marketdata.store import keys as K
from src.marketdata.types import M15, Bar, MdGeneration, Quality, SeriesId, Side, Source
from src.ops import arm_sql as A
from src.ops.md_view import OpsMdReader
from src.riskmgr.envelope import EnvelopeParams, stamp
from src.riskmgr.types import SeriesInput, inputs_digest

from tests.ops.arm_emulator import ArmOpsPg
from tests.ops.emulator import NOW

SERIES = SeriesId("mock", "SYN.A", M15, Side.BID, Source.BROKER)
TICK_SERIES = SeriesId("mock", "SYN.A", M15, Side.BID, Source.TICK)
KEY_OLD = bytes(range(1, 33))
KEY_NEW = bytes(range(33, 65))
RING = Keyring.of(3, KEY_NEW)
RING_ROTATING = Keyring.of(3, KEY_NEW, ((2, KEY_OLD),))
RING_OLD = Keyring.of(2, KEY_OLD)
BAR_END = NOW - 3_600_000 - (NOW % 900_000)
BUCKET = BAR_END - 900_000
#: A distinctive synthetic close: the output of every view and of the replay must never contain it.
CLOSE = Decimal("1.23457")
TOKEN = K.WriterToken("1234567890", 7, "node-a", "s3cretnonce")
SENTINEL = K.BootSentinel("a1b2c3d4e5", "1234567890", "node-a", MdGeneration(7, 2))
ENVELOPE = stamp(EnvelopeParams())


def bar(*, close: Decimal = CLOSE, revision: int = 0, final_at: int | None = BAR_END + 30_000,
        series: SeriesId = SERIES, bucket: int = BUCKET, quality: Quality = Quality.COMPLETE) -> Bar:
    return Bar(series=series, bucket_start_ms=bucket, open=Decimal("1.23401"), high=Decimal("1.23499"),
               low=Decimal("1.23388"), close=close, tick_count=42, quality=quality, revision=revision,
               final_at_ms=final_at)


def bar_hash(b: Bar, ring: Keyring = RING) -> str:
    return content_hash(ring, b.series.canonical, b.bucket_end_ms, b.open, b.high, b.low, b.close)


def member(b: Bar, ring: Keyring = RING) -> str:
    return encode_member(b, bar_hash(b, ring))


async def store_bar(r: Any, b: Bar, ring: Keyring = RING) -> str:
    m = member(b, ring)
    await r.zadd(K.bars(b.series), {m: b.bucket_start_ms})
    return m


async def store_superseded(r: Any, b: Bar, superseded_at: int, ring: Keyring = RING) -> None:
    await r.zadd(K.bars_rev(b.series), {f"{member(b, ring)}|{superseded_at:013d}": b.bucket_start_ms})


def reader(r: Any, ring: Keyring | None = RING) -> OpsMdReader:
    return OpsMdReader(r, ring)


@dataclass
class Decision:
    """One `arm_decisions` row as the view SQL returns it (ids, codes, keyed hashes)."""

    decision_id: int = 1
    bar_end_ms: int = BAR_END
    adapter_id: str = "mock"
    position_id: str = "P1"
    event: str = "POST"
    revision: int = 0
    decision_seq: int = 1
    decided_at_ms: int = BAR_END + 60_000
    outcome: str = "COMPLETE"
    category: str = "INFO"
    reason: str | None = None
    series_inputs: list[str] = field(default_factory=list)
    indicator_inputs: list[str] = field(default_factory=list)
    inputs_digest: str | None = None
    envelope_version: int = ENVELOPE.version
    envelope_params_hash: str = ENVELOPE.params_hash
    strategy_kind: str = "break_even_on_risk_close"
    strategy_impl_version: int = 1
    notice_id: int | None = None
    caveats: list[str] = field(default_factory=list)

    def row(self) -> tuple[Any, ...]:
        return (self.decision_id, self.bar_end_ms, self.adapter_id, self.position_id, "15m", self.event,
                self.revision, self.decision_seq, self.decided_at_ms, 7, self.outcome, self.category, self.reason,
                None, True, False, False, [], self.caveats, "BROKER", "STREAM", self.series_inputs,
                self.indicator_inputs, self.inputs_digest, self.envelope_version, self.envelope_params_hash,
                self.strategy_kind, self.strategy_impl_version, None, None, self.notice_id)


def decision_for(b: Bar, ring: Keyring = RING, **kw: Any) -> Decision:
    """A POST decision whose single input is `b`, hashed and digested under `ring`'s current key."""
    inp = SeriesInput(b.series.canonical, b.bucket_end_ms, bar_hash(b, ring))
    return Decision(series_inputs=[inp.text], inputs_digest=inputs_digest(ring, (inp,), ()), **kw)


@dataclass
class PositionView:
    adapter_id: str = "mock"
    position_id: str = "P1"
    account_id: str = "acct-1"
    symbol: str = "SYN.A"
    state: str = "TRACKING"
    state_reason: str | None = None
    strategy_code: str = "be_a"
    strategy_version: int = 1
    strategy_kind: str = "break_even_on_risk_close"
    strategy_impl_version: int = 1
    guards_json: str = '[{"kind":"first_bar","params":{},"version":1}]'
    indicators_json: str = ('[{"bar_spec":"15m","kind":"ATR","params":{"length":"14"},"side":"BID",'
                            '"version":1}]')
    config_doc_hash: str = "a" * 64
    armed_at_ms: int = NOW - 86_400_000

    def row(self) -> tuple[Any, ...]:
        return (self.adapter_id, self.position_id, self.account_id, self.symbol, "BUY", self.state, None,
                self.state_reason, "default", "SINGLE_EDGE_DEFAULT", self.strategy_code, self.strategy_version,
                self.strategy_kind, self.strategy_impl_version, "b" * 16, "15m", self.config_doc_hash,
                self.armed_at_ms - 60_000, "STREAM", "AT_FILL", BAR_END, True, None, None, None, 3, False,
                self.armed_at_ms, NOW - 1_000, None, 7, 4)


class ArmViewPg(ArmOpsPg):
    def __init__(self) -> None:
        super().__init__()
        self.view_positions: list[PositionView] = []
        self.decisions: list[Decision] = []
        self._h.update({
            A.SQL_POSITIONS_VIEW: self._positions, A.SQL_DECISIONS_VIEW: self._decisions,
            A.SQL_PINNED_VERSIONS: self._pinned, A.SQL_DECISION_VERSIONS: self._decision_versions,
        })

    def _positions(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        rows = [p for p in sorted(self.view_positions, key=lambda p: (p.adapter_id, p.position_id))
                if (a[0] is None or p.adapter_id == a[0]) and (a[1] is None or p.position_id == a[1])
                and (a[2] or p.state not in ("CLOSED", "DISARMED"))]
        return [p.row() for p in rows[:a[3]]]

    def _decisions(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        rows = [d for d in self.decisions
                if (a[0] is None or d.adapter_id == a[0]) and (a[1] is None or d.position_id == a[1])
                and a[2] <= d.bar_end_ms <= a[3]]
        rows.sort(key=lambda d: (d.bar_end_ms, d.decision_seq, d.revision), reverse=True)
        return [d.row() for d in rows[:a[4]]]

    def _pinned(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        groups: dict[tuple[str, int, str, str], list[int]] = {}
        for p in self.view_positions:
            g = groups.setdefault((p.strategy_kind, p.strategy_impl_version, p.guards_json, p.indicators_json), [0, 0])
            g[0] += 1
            g[1] += p.state not in ("CLOSED", "DISARMED")
        return [(*k, n, live) for k, (n, live) in sorted(groups.items())]

    def _decision_versions(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        counts: dict[tuple[str, int], int] = {}
        for d in self.decisions:
            key = (d.strategy_kind, d.strategy_impl_version)
            counts[key] = counts.get(key, 0) + 1
        return [(*k, n) for k, n in sorted(counts.items())]
