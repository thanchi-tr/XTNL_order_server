"""The seen-row chain: pending versus tampered (design §B9 Trust boundary; plan S25). Pure.

Supabase is untrusted storage. The engine keeps every Supabase row it has seen as `(ref, row hash)` (`ref` =
`table:id`, the hash from `src/armconfig/document.py`) and a sha256 chain over them, stored in `arm_config_cache`
(`row_hashes`, `chain_head`, `chain_rows`; S24). The chain runs over the seen rows in canonical order (table order,
then id; ids are append-only, so this is the order rows were added), starting from `CHAIN_SEED`:
`head' = sha256(head "\\n" ref "\\n" hash)`.

`check_rows(seen, current)` compares a new read with what was seen:

* **Tampered** (pages at once; the accepted document stays in force and never installs): a seen row whose hash
  changed (`CHANGED`), a seen row missing from the read (`VANISHED`: fewer rows), or a stored chain that does not
  verify (`CHAIN_BROKEN`: the stored head or row count does not match its rows, or a stored ref / hash is out of
  form).
* **Pending** (the accepted document stays in force until `arm config accept`; the installer pages after 1 day): a
  row not seen before, including one that supersedes an older row.
* Otherwise **clean**.

A row becomes seen when its document is accepted: the installer (S33) calls `SeenRows.extend` with every row of the
accepted read. A seen ref never changes its hash in `extend` (a tampered row never becomes seen).

A **schema-ahead** read (`check_rows(..., schema_ahead=True)`) skips only `CHANGED`: its rows may carry new columns,
so their hashes may differ without tampering. `VANISHED` and `CHAIN_BROKEN` do not depend on columns and are still
reported, so bumping `arm.schema_version` can never mask the deletion of a seen row.
"""
from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Final

from src.armconfig.document import TABLES

CHAIN_SEED: Final = hashlib.sha256(b"xtnl:arm:config:seen-rows:v1").hexdigest()
REF_RE: Final = re.compile(r"(" + "|".join(TABLES) + r"):([1-9][0-9]{0,18})")
HASH_RE: Final = re.compile(r"[0-9a-f]{64}")


class ChainError(ValueError):
    """Seen rows out of form, a ref seen twice, or a seen ref offered with another hash."""


def _order(ref: object) -> tuple[int, int]:
    m = REF_RE.fullmatch(ref) if isinstance(ref, str) else None
    if m is None:
        raise ChainError("a seen row ref is out of form")
    return (TABLES.index(m.group(1)), int(m.group(2)))


def chain_head(rows: Iterable[tuple[str, str]]) -> str:
    """The chain head over `rows` (`(ref, hash)`) taken in canonical order."""
    head = CHAIN_SEED
    for ref, row_hash in sorted(rows, key=lambda p: _order(p[0])):
        if not isinstance(row_hash, str) or not HASH_RE.fullmatch(row_hash):
            raise ChainError("a seen row hash is out of form")
        head = hashlib.sha256(f"{head}\n{ref}\n{row_hash}".encode("ascii")).hexdigest()
    return head


@dataclass(frozen=True)
class SeenRows:
    """The seen rows in canonical order and their chain head (the `arm_config_cache` columns)."""

    rows: tuple[tuple[str, str], ...] = ()
    head: str = CHAIN_SEED

    @property
    def count(self) -> int:
        return len(self.rows)

    @classmethod
    def build(cls, rows: Iterable[tuple[str, str]]) -> SeenRows:
        """Seen rows with a freshly computed head; `ChainError` when a ref repeats or a ref / hash is out of form."""
        items = sorted(rows, key=lambda p: _order(p[0]))
        refs = [r for r, _ in items]
        if len(set(refs)) != len(refs):
            raise ChainError("a seen row ref repeats")
        return cls(tuple(items), chain_head(items))

    def hashes(self) -> dict[str, str]:
        return dict(self.rows)

    def extend(self, current: Mapping[str, str]) -> SeenRows:
        """These rows plus every row of `current` not seen yet (the accepted read). `ChainError` when a seen ref
        comes with another hash: a tampered row never becomes seen."""
        seen = self.hashes()
        for ref, row_hash in current.items():
            if ref in seen and seen[ref] != row_hash:
                raise ChainError("a seen row changed")
        return SeenRows.build([*self.rows, *((r, h) for r, h in current.items() if r not in seen)])


class ChainState(str, Enum):
    CLEAN = "clean"
    PENDING = "pending"
    TAMPERED = "tampered"


class TamperKind(str, Enum):
    CHANGED = "changed"
    VANISHED = "vanished"
    CHAIN_BROKEN = "chain_broken"


@dataclass(frozen=True)
class TamperedRow:
    """A tampered seen row by ref (`table:id`), or `ref == "chain"` for a stored chain that does not verify."""

    ref: str
    kind: TamperKind


@dataclass(frozen=True)
class ChainVerdict:
    state: ChainState
    pending: tuple[str, ...] = ()
    tampered: tuple[TamperedRow, ...] = ()


def verify_stored(rows: Iterable[tuple[str, str]], head: str, count: int) -> SeenRows | None:
    """The stored seen rows when their chain verifies (head and count), else None."""
    try:
        seen = SeenRows.build(rows)
    except (ChainError, TypeError, ValueError, IndexError):   # stored rows out of form (not (ref, hash) pairs)
        return None
    if seen.head != head or seen.count != count:
        return None
    return seen


def check_rows(stored_rows: Iterable[tuple[str, str]], stored_head: str, stored_count: int,
               current: Mapping[str, str], *, schema_ahead: bool = False) -> ChainVerdict:
    """Compare a read (`ref -> row hash`) with the stored seen rows (module docstring); `schema_ahead` skips only
    `CHANGED`. Never raises."""
    seen = verify_stored(stored_rows, stored_head, stored_count)
    if seen is None:
        return ChainVerdict(ChainState.TAMPERED, tampered=(TamperedRow("chain", TamperKind.CHAIN_BROKEN),))
    known = seen.hashes()
    tampered: list[TamperedRow] = []
    for ref, row_hash in seen.rows:
        now = current.get(ref)
        if now is None:
            tampered.append(TamperedRow(ref, TamperKind.VANISHED))
        elif now != row_hash and not schema_ahead:
            tampered.append(TamperedRow(ref, TamperKind.CHANGED))
    pending = [ref for ref in current if ref not in known]
    try:
        pending.sort(key=_order)
    except ChainError:
        pending.sort(key=str)
    if tampered:
        return ChainVerdict(ChainState.TAMPERED, pending=tuple(pending), tampered=tuple(tampered))
    if pending:
        return ChainVerdict(ChainState.PENDING, pending=tuple(pending))
    return ChainVerdict(ChainState.CLEAN)


__all__ = [
    "CHAIN_SEED", "ChainError", "ChainState", "ChainVerdict", "SeenRows", "TamperKind", "TamperedRow", "chain_head",
    "check_rows", "verify_stored",
]
