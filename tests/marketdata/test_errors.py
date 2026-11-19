"""`safe_redis_error` never carries message text (plan S12; design §B3)."""
from __future__ import annotations

import pytest
from redis import exceptions as rex
from redis._parsers.base import BaseParser

from src.marketdata.errors import OTHER_CODE, REDIS_ERROR_CODES, safe_redis_error

SECRET = "md:bars:mock:SYN1 redis://md_writer:test-secret-pw@127.0.0.1 1.23457"


def parsed(reply: str) -> BaseException:
    """The exception redis-py builds for an error reply (its own parser)."""
    err = BaseParser.parse_error(reply)  # type: ignore[no-untyped-call]
    assert isinstance(err, BaseException)
    return err


@pytest.mark.parametrize(("reply", "expected"), [
    (f"NOSCRIPT {SECRET}", "NoScriptError(NOSCRIPT)"),
    (f"NOPERM {SECRET}", "NoPermissionError(NOPERM)"),
    (f"ERR {SECRET}", "ResponseError(ERR)"),
    (f"WRONGTYPE {SECRET}", "ResponseError(WRONGTYPE)"),
    (f"NO_BOOT {SECRET}", "ResponseError(NO_BOOT)"),
    ("NO_BOOT", "ResponseError(NO_BOOT)"),
    (f"SOMETHINGNEW {SECRET}", f"ResponseError({OTHER_CODE})"),
    (f"{SECRET}", f"ResponseError({OTHER_CODE})"),
    (f"OOM {SECRET}", "OutOfMemoryError(OOM)"),
    (f"LOADING {SECRET}", "BusyLoadingError"),      # a ConnectionError in redis-py
])
def test_response_errors_give_class_and_allow_listed_code(reply: str, expected: str) -> None:
    text = safe_redis_error(parsed(reply))
    assert text == expected
    for fragment in ("test-secret-pw", "md:bars", "1.23457", "SYN1", "127.0.0.1"):
        assert fragment not in text


@pytest.mark.parametrize("exc", [
    rex.ConnectionError(SECRET), rex.TimeoutError(SECRET), rex.AuthenticationError(SECRET), TimeoutError(SECRET),
    OSError(SECRET), ValueError(SECRET), rex.ResponseError(),
])
def test_other_errors_give_the_class_name_only(exc: BaseException) -> None:
    text = safe_redis_error(exc)
    assert "test-secret-pw" not in text and "md:bars" not in text and "1.23457" not in text
    assert text.split("(")[0] == type(exc).__name__


def test_auth_error_is_not_mistaken_for_a_response_error() -> None:
    assert safe_redis_error(parsed(f"WRONGPASS {SECRET}")) == "AuthenticationError"


class _UnprintableResponseError(rex.ResponseError):
    def __str__(self) -> str:
        raise RuntimeError("no text")

    @property
    def status_code(self) -> str:
        raise RuntimeError("no status")

    @status_code.setter
    def status_code(self, value: str | None) -> None:
        pass


def test_an_exception_whose_str_raises_never_makes_the_helper_raise() -> None:
    assert safe_redis_error(_UnprintableResponseError()) == f"_UnprintableResponseError({OTHER_CODE})"


def test_codes_are_upper_case_tokens() -> None:
    assert all(code == code.upper() and " " not in code for code in REDIS_ERROR_CODES)
