"""The ARM configuration document: shape, canonical form and hashes (design §B9; plan S25). Pure.

A document is what one read of the Supabase schema `arm` returns: `schema_version` plus every row of the three tables
`strategies`, `assignments` and `route_policy` (rows are never updated; a change is a new row, so the document holds
superseded rows and tombstones too and the resolver, `src/armconfig/resolver.py`, picks those in force).
`parse_document(raw)` checks only what the hashes need: the four top-level keys (an envelope or sink key at the top
is refused by name; any other unknown key without echoing it), a list of objects per table, an integer `id` per row
unique in its table, JSON values only (`None`, `bool`, `int`, `Decimal`, text, lists and objects with text keys; a
`float` is refused, since the client parses JSON with `Decimal`), a nesting depth limit, a row byte cap and a row-count
cap. What a row means is checked by `src/armconfig/validate.py`.

* **Canonical form** (`canonical_json`): sorted keys, no whitespace, ASCII-escaped text, numbers in plain decimal form
  with trailing zeros removed (`1.50` and `1.5` are one value; an integral `Decimal` prints as an integer), so the
  same row read twice hashes the same and a harmless re-encoding never looks like tampering. A number is never
  rounded: it is normalised under one fixed context (`_CANON_CONTEXT`, independent of the caller's decimal context)
  that refuses any rounding, and a number outside `10**±MAX_EXPONENT` (checked before any arithmetic, so an extreme
  exponent or a huge integer costs nothing) has no canonical form.
* **Row hash:** sha256 of `"<table>\\n<canonical row>"`, 64 hex. Every column, the append-only guard columns (`id`,
  `set_at_ms`, `session_user`, `current_user`) included, is covered, so any change to a seen row is detected
  (`src/armconfig/chain.py`). A row is named only as `ref` (`table:id`) or `audit_ref` (`table:id:hash`, the
  `arm_config_audit.row_refs` form), never by its text (§B9).
* **Document hash** (`doc_hash`, the hash `arm config accept <hash>` names and `arm_accepted.doc_hash` stores): sha256
  over the canonical list of `(table, id, row hash)` in document order plus `schema_version`, 64 hex.
* **Schema ahead:** `schema_version` above `SUPPORTED_SCHEMA_VERSION` parses (hashes included) but is never
  validated or installed (`schema_ahead`); the accepted document stays in force. `schema_version` is read first, so a
  newer schema may add top-level tables: they are ignored (only the known tables are parsed and hashed), while an
  envelope or sink key at the top is still refused by name.

`parse_document` never raises: any input it cannot read becomes a `DocumentProblem`.

Problems (`DocumentProblem`) carry a fixed code and a location (table, row id), never row text.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Context, Decimal, Inexact, InvalidOperation, Overflow
from typing import Final

from src.riskmgr.registry import ENVELOPE_KEYS

SUPPORTED_SCHEMA_VERSION: Final = 1
"""The `arm.schema_version` this engine reads (`deploy/supabase/arm_schema.sql`, S33)."""
TABLES: Final = ("strategies", "assignments", "route_policy")
"""The document's tables, in document (and hash) order."""
SCHEMA_VERSION_KEY: Final = "schema_version"
DOCUMENT_KEYS: Final = frozenset({SCHEMA_VERSION_KEY, *TABLES})
SINK_KEYS: Final = frozenset({"sink", "sinks", "arm_sinks"})
"""A document can never name a sink (§B3: the Phase 1 sink set is fixed in the runtime wiring)."""
LOCAL_ONLY_KEYS: Final = frozenset(ENVELOPE_KEYS)
"""Local engine settings and constants a document may never set (`pre_offset`, the envelope keys; §B6.7)."""

MAX_ROWS_PER_TABLE: Final = 1_000
MAX_DEPTH: Final = 8
MAX_ROW_BYTES: Final = 8_192
MAX_EXPONENT: Final = 64
ROW_ID_MAX: Final = 2**63 - 1
SCHEMA_VERSION_MAX: Final = 2**31 - 1
INT_LIMIT: Final = 10 ** (MAX_EXPONENT + 1)
"""An integer at or above this (in absolute value) has no canonical form (the `Decimal` bound: adjusted exponent at
most `MAX_EXPONENT`)."""
_CANON_CONTEXT: Final = Context(prec=2 * MAX_EXPONENT + 2, Emax=4 * MAX_EXPONENT, Emin=-4 * MAX_EXPONENT,
                                traps=[Inexact, InvalidOperation, Overflow])
"""The one context numbers are normalised under: wide enough for every in-range value, and any rounding traps."""


class CanonicalError(ValueError):
    """A value with no canonical JSON form (a float, NaN, a non-text key, too deep, an extreme exponent)."""


@dataclass(frozen=True)
class DocumentProblem:
    """Why a read is not a document: a fixed code and where (table, row id), never row text."""

    code: str
    table: str = ""
    row_id: int | None = None
    detail: str = ""

    @property
    def page_text(self) -> str:
        where = self.table if self.row_id is None else f"{self.table}:{self.row_id}"
        return f"{self.code} {where}".strip() if where else self.code


# ---------------------------------------------------------------------------------------------------- canonical JSON
def _decimal_text(value: Decimal) -> str:
    if not value.is_finite():
        raise CanonicalError("a number must be finite")
    if value.is_zero():
        return "0"
    if abs(value.adjusted()) > MAX_EXPONENT:     # before any arithmetic: 1e1000000 never reaches normalize
        raise CanonicalError("a number is out of range")
    try:
        n = value.normalize(_CANON_CONTEXT)
    except ArithmeticError:                      # Inexact: more significant digits than the context holds
        raise CanonicalError("a number has too many significant digits") from None
    exponent = n.as_tuple().exponent
    if not isinstance(exponent, int) or abs(exponent) > MAX_EXPONENT or n.adjusted() > MAX_EXPONENT:
        raise CanonicalError("a number is out of range")
    return format(n, "f")


def _canon(value: object, depth: int) -> str:
    if depth < 0:
        raise CanonicalError(f"nesting deeper than {MAX_DEPTH}")
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        if abs(value) >= INT_LIMIT:              # also keeps str() clear of the int-to-text digit limit
            raise CanonicalError("a number is out of range")
        return str(value)
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=True)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_canon(v, depth - 1) for v in value) + "]"
    if isinstance(value, Mapping):
        if not all(isinstance(k, str) for k in value):
            raise CanonicalError("object keys must be text")
        items = sorted(value.items())
        return "{" + ",".join(f"{json.dumps(k, ensure_ascii=True)}:{_canon(v, depth - 1)}" for k, v in items) + "}"
    raise CanonicalError("only JSON values (no floats)")


def canonical_json(value: object) -> str:
    """The canonical JSON text of `value` (module docstring); `CanonicalError` when it has none."""
    return _canon(value, MAX_DEPTH)


def _parse_canonical(text: str) -> object:
    return json.loads(text, parse_float=Decimal, parse_int=int)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def row_hash(table: str, canonical: str) -> str:
    return _sha(f"{table}\n{canonical}")


# ---------------------------------------------------------------------------------------------------- rows, document
@dataclass(frozen=True)
class ConfigRow:
    """One Supabase row as read: its table, id, hash and canonical text (kept out of `repr`)."""

    table: str
    row_id: int
    row_hash: str
    canonical: str = field(repr=False)

    @property
    def ref(self) -> str:
        return f"{self.table}:{self.row_id}"

    @property
    def audit_ref(self) -> str:
        return f"{self.table}:{self.row_id}:{self.row_hash}"

    def columns(self) -> dict[str, object]:
        """The row's columns, parsed back from the canonical text (numbers as `int` or `Decimal`)."""
        value = _parse_canonical(self.canonical)
        assert isinstance(value, dict)
        return value


@dataclass(frozen=True)
class Document:
    """A parsed read: rows in document order (table order, then id) and the document hash."""

    schema_version: int
    rows: tuple[ConfigRow, ...]
    doc_hash: str

    @property
    def schema_ahead(self) -> bool:
        return self.schema_version > SUPPORTED_SCHEMA_VERSION

    def table(self, name: str) -> tuple[ConfigRow, ...]:
        return tuple(r for r in self.rows if r.table == name)

    def row_hashes(self) -> dict[str, str]:
        """`ref -> row hash` of every row (the input of the seen-row chain check)."""
        return {r.ref: r.row_hash for r in self.rows}

    @property
    def empty(self) -> bool:
        return not self.rows


def document_hash(schema_version: int, rows: Sequence[ConfigRow]) -> str:
    body = canonical_json({SCHEMA_VERSION_KEY: schema_version,
                           "rows": [[r.table, r.row_id, r.row_hash] for r in rows]})
    return _sha(body)


def int_value(value: object, lo: int, hi: int) -> int | None:
    """An integer in `[lo, hi]` (an `int` or integral `Decimal`; never a bool or text), else None. A number at or
    above `INT_LIMIT` is never in range (it is refused before any conversion)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        if not value.is_finite() or value.adjusted() > MAX_EXPONENT or value != value.to_integral_value():
            return None
        value = int(value)
    if not isinstance(value, int) or not lo <= value <= hi:
        return None
    return value


def _top_level_problem(raw: Mapping[object, object], *, ahead: bool) -> DocumentProblem | None:
    """Envelope and sink keys are refused by name; another unknown key is refused unless the schema is ahead (a newer
    schema may add tables; they are ignored)."""
    if not all(isinstance(k, str) for k in raw):
        return DocumentProblem("unknown_key")
    for key in sorted(str(k) for k in raw):
        if key in DOCUMENT_KEYS:
            continue
        if key in LOCAL_ONLY_KEYS:
            return DocumentProblem("envelope_key", detail=f"{key} is a local engine setting")
        if key in SINK_KEYS:
            return DocumentProblem("sink_key", detail="a document never names a sink")
        if not ahead:
            return DocumentProblem("unknown_key")
    missing = sorted(DOCUMENT_KEYS - set(raw))
    if missing:
        return DocumentProblem("missing_key", detail=", ".join(missing))
    return None


def _parse_rows(table: str, raw: object) -> list[ConfigRow] | DocumentProblem:
    if not isinstance(raw, (list, tuple)):
        return DocumentProblem("table_form", table)
    if len(raw) > MAX_ROWS_PER_TABLE:
        return DocumentProblem("too_many_rows", table, detail=f"at most {MAX_ROWS_PER_TABLE}")
    rows: dict[int, ConfigRow] = {}
    for item in raw:
        if not isinstance(item, Mapping):
            return DocumentProblem("row_form", table)
        row_id = int_value(item.get("id"), 1, ROW_ID_MAX)
        if row_id is None:
            return DocumentProblem("row_id_form", table)
        if row_id in rows:
            return DocumentProblem("duplicate_row_id", table, row_id)
        try:
            canonical = canonical_json(item)
        except CanonicalError as exc:
            return DocumentProblem("value_form", table, row_id, detail=str(exc))
        if len(canonical) > MAX_ROW_BYTES:
            return DocumentProblem("row_too_large", table, row_id, detail=f"at most {MAX_ROW_BYTES} bytes")
        rows[row_id] = ConfigRow(table, row_id, row_hash(table, canonical), canonical)
    return [rows[k] for k in sorted(rows)]


def parse_document(raw: object) -> Document | DocumentProblem:
    """A `Document`, or the first `DocumentProblem` (module docstring). Never raises."""
    if not isinstance(raw, Mapping):
        return DocumentProblem("not_an_object")
    version = int_value(raw.get(SCHEMA_VERSION_KEY), 1, SCHEMA_VERSION_MAX)
    top = _top_level_problem(raw, ahead=version is not None and version > SUPPORTED_SCHEMA_VERSION)
    if top is not None:
        return top
    if version is None:
        return DocumentProblem("schema_version_form")
    rows: list[ConfigRow] = []
    for table in TABLES:
        parsed = _parse_rows(table, raw[table])
        if isinstance(parsed, DocumentProblem):
            return parsed
        rows.extend(parsed)
    return Document(schema_version=version, rows=tuple(rows), doc_hash=document_hash(version, rows))


__all__ = [
    "CanonicalError", "ConfigRow", "DOCUMENT_KEYS", "Document", "DocumentProblem", "INT_LIMIT", "LOCAL_ONLY_KEYS",
    "MAX_ROWS_PER_TABLE", "SINK_KEYS", "SUPPORTED_SCHEMA_VERSION", "TABLES", "canonical_json", "document_hash",
    "int_value", "parse_document", "row_hash",
]
