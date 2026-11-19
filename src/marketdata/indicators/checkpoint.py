"""Indicator state checkpoints (design §A3 `md:ind_ckpt`, §A5: every 96 buckets, kept 30 days; plan S18).

A checkpoint is the indicator's `State` at a clock bucket whose index on the UTC grid is a multiple of 96 (one day of
15m buckets). It lives in `redis-md` only (`md:ind_ckpt:{indicator_id}`, member `<bucket:13>|<state json>`, score =
bucket start), never in PostgreSQL, a log or a page. The engine writes it with the value of that bucket (the same
`IND` script) and the golden test checks that the incremental values equal the canonical value at every checkpoint.
A checkpoint that does not decode is ignored (the indicator re-seeds from stored bars); it is never an error that
reaches the task.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Final

from src.marketdata.indicators.base import State
from src.marketdata.types import BarSpec

CHECKPOINT_EVERY: Final = 96
CHECKPOINT_RETENTION_MS: Final = 30 * 86_400_000
CHECKPOINT_MAX_LEN: Final = 4_096
_MEMBER_RE: Final = re.compile(r"([0-9]{13})\|(\{.*\})", re.DOTALL)


def due(bucket_start_ms: int, clock: BarSpec) -> bool:
    """True for a clock bucket that carries a checkpoint (every 96th bucket on the UTC grid)."""
    return (bucket_start_ms // clock.timeframe_ms) % CHECKPOINT_EVERY == 0


@dataclass(frozen=True)
class Checkpoint:
    bucket_start_ms: int
    state: State = field(repr=False)


def encode_checkpoint(bucket_start_ms: int, state: State) -> str:
    member = f"{bucket_start_ms:013d}|{state.encode()}"
    if len(member) > CHECKPOINT_MAX_LEN:
        raise ValueError("checkpoint state too large")
    return member


def decode_checkpoint(member: str | bytes) -> Checkpoint | None:
    """The checkpoint of a stored member, or None (never raises)."""
    try:
        text = member.decode("utf-8") if isinstance(member, bytes) else member
    except UnicodeDecodeError:
        return None
    m = _MEMBER_RE.fullmatch(text)
    if m is None:
        return None
    state = State.decode(m.group(2))
    return None if state is None else Checkpoint(int(m.group(1)), state)


__all__ = ["CHECKPOINT_EVERY", "CHECKPOINT_RETENTION_MS", "Checkpoint", "due", "encode_checkpoint",
           "decode_checkpoint"]
