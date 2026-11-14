"""Error translation between the PostgreSQL stores and the Oversee ports (oversee-rules.md §2).

* A lost fence (src.db.fence.EpochMismatch) surfaces as `SweepFenced`, which is BOTH the db and the
  Oversee `EpochMismatch`, so either layer's handler sees it; the supervisor hard-exits (operations.md §3).
* PostgreSQL unreachable (connection-class errors) -> `DependencyUnavailable`: the RiskSweep treats it as
  a dependency-wide failure, never as an account fault.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from src.db.fence import EpochMismatch as DbEpochMismatch
from src.db.pg import is_connection_error
from src.oversee.snapshot import DependencyUnavailable
from src.oversee.sweep import EpochMismatch as SweepEpochMismatch


class SweepFenced(DbEpochMismatch, SweepEpochMismatch):
    """The fence is lost: this node is no longer the leader."""


@asynccontextmanager
async def oversee_errors() -> AsyncIterator[None]:
    try:
        yield
    except (DependencyUnavailable, SweepFenced):
        raise
    except DbEpochMismatch as exc:
        raise SweepFenced(str(exc)) from exc
    except Exception as exc:
        if is_connection_error(exc):
            raise DependencyUnavailable(f"postgres unavailable: {type(exc).__name__}") from exc
        raise
