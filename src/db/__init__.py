"""PostgreSQL: single source of truth (tier-persistence.md §4, operations.md §2).

asyncpg (binary protocol) behind a small driver-neutral protocol (`PgConnection`); single-leader
lock-free fencing on every state write (`src.db.fence`).
"""
from pathlib import Path

from src.db.epoch import (
    HALT_LOCK,
    CommitUnknownError,
    EngineDb,
    Epoch,
    EpochFenced,
    application_name,
    bump_epoch,
    epoch_transaction,
    read_epoch,
    snapshot_transaction,
    spill_drain_insert,
)
from src.db.fence import FENCE, EpochMismatch, FencedTx
from src.db.pg import ConnectionSource, PgConnection, create_asyncpg_source

MIGRATIONS_DIR = Path(__file__).with_name("migrations")

__all__ = [
    "FENCE", "HALT_LOCK", "MIGRATIONS_DIR", "CommitUnknownError", "ConnectionSource", "EngineDb", "Epoch",
    "EpochFenced", "EpochMismatch", "FencedTx", "PgConnection", "application_name", "bump_epoch",
    "create_asyncpg_source", "epoch_transaction", "read_epoch", "snapshot_transaction", "spill_drain_insert",
]
