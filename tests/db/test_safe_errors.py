"""S24: `safe_db_error` gives the class name and SQLSTATE only, never message text (R11-m)."""
from __future__ import annotations

from src.db.safe_errors import safe_db_error


class UniqueViolationError(Exception):
    sqlstate = "23505"


class OddState(Exception):
    sqlstate = "not a code; password=hunter2"


class Raising(Exception):
    @property
    def sqlstate(self) -> str:  # type: ignore[override]
        raise RuntimeError("boom")


def test_class_and_sqlstate_only() -> None:
    exc = UniqueViolationError('duplicate key value violates "k" DETAIL: Key (bid)=(1.2345) already exists')
    out = safe_db_error(exc)
    assert out == "UniqueViolationError(23505)"
    assert "1.2345" not in out and "bid" not in out


def test_no_sqlstate_or_a_bad_one_gives_the_class_name() -> None:
    assert safe_db_error(ConnectionResetError("postgresql://user:secret@host/db")) == "ConnectionResetError"
    assert safe_db_error(OddState("x")) == "OddState"
    assert safe_db_error(Raising("x")) == "Raising"
