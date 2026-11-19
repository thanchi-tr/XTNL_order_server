"""Envelope parameters and their stamp on each decision (design §B6.1 Always current, §B6.7; plan S19, R11-m).

The envelope (pre- and post-guards, rounding, `EnvelopeParams`) is versioned with the engine and **always current**:
it is never pinned per position. `EnvelopeParams` come only from local engine settings (`src/riskmgr/config.py`),
never from Supabase: `ENVELOPE_KEYS` are refused in a strategy row's params (`src/riskmgr/registry.py`) and in a
config document (S25).

Each decision records `envelope_version` and `envelope_params_hash` (`EnvelopeStamp`), so `arm replay` reports
`ENVELOPE_CHANGED` when a re-run uses another envelope (`envelope_changed`). Bump `ENVELOPE_VERSION` with every change
of envelope behaviour (S21 builds the pipeline).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Final

from src.marketdata.hashing import canonical_decimal

ENVELOPE_VERSION: Final = 1

MIN_GAP_R_DEFAULT: Final = Decimal("0.0")
MIN_GAP_R_MIN: Final = Decimal("0")
MIN_GAP_R_MAX: Final = Decimal("0.5")

ENVELOPE_KEYS: Final = frozenset({"min_gap_r", "pre_offset", "pre_offset_s", "post_grace", "decision_cap"})
"""Local engine settings and constants a Supabase document may never set (§B6.7)."""


@dataclass(frozen=True)
class EnvelopeParams:
    """`min_gap_r`: step (12)'s gap floor in R (0.0, 0-0.5)."""

    min_gap_r: Decimal = MIN_GAP_R_DEFAULT

    def __post_init__(self) -> None:
        if not self.min_gap_r.is_finite() or not MIN_GAP_R_MIN <= self.min_gap_r <= MIN_GAP_R_MAX:
            raise ValueError(f"min_gap_r must be in [{MIN_GAP_R_MIN}, {MIN_GAP_R_MAX}]")

    @property
    def canonical(self) -> str:
        return json.dumps({"min_gap_r": canonical_decimal(self.min_gap_r)}, sort_keys=True, separators=(",", ":"))

    @property
    def params_hash(self) -> str:
        return hashlib.sha256(self.canonical.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class EnvelopeStamp:
    """What a decision records about the envelope it ran under."""

    version: int
    params_hash: str


def stamp(params: EnvelopeParams, version: int = ENVELOPE_VERSION) -> EnvelopeStamp:
    return EnvelopeStamp(version, params.params_hash)


def envelope_changed(recorded: EnvelopeStamp, current: EnvelopeStamp) -> bool:
    """True when a re-run would use another envelope version or other params (`arm replay`: `ENVELOPE_CHANGED`)."""
    return recorded != current
