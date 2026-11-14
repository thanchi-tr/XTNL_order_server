"""Structured JSON logs (operations.md §9 Logs; IMPLEMENTATION_PLAN 5.6). No network."""
from __future__ import annotations

import io
import json
import logging
from collections.abc import Iterator
from decimal import Decimal

import pytest

from src.dto import OrderDTO, PositionDTO
from src.observability import logs as L
from src.observability.redact import UrlRedactingFilter


@pytest.fixture
def capture() -> Iterator[tuple[logging.Logger, io.StringIO]]:
    logger = logging.getLogger("xtnl.test.jsonlogs")
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logger.addHandler(handler)
    saved = L.process_fields()
    try:
        yield logger, stream
    finally:
        logger.removeHandler(handler)
        L.set_process_fields(node=saved["node"] or "", epoch=saved["epoch"], clear_epoch=saved["epoch"] is None)


def lines(stream: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(x) for x in stream.getvalue().splitlines() if x.strip()]


def test_json_lines_carry_ts_level_logger_msg_epoch_node(capture: tuple[logging.Logger, io.StringIO]) -> None:
    """Before 5.6: plain text `%(asctime)s %(levelname)s ...`; nothing carried the epoch or the node."""
    logger, stream = capture
    assert L.configure_logging("INFO", logger, fmt="json", node="node-a") == "json"
    L.set_process_fields(epoch="4711")
    logger.warning("hello %s", "world")
    L.clear_epoch_if("4711")
    logger.info("standby line")
    first, second = lines(stream)
    assert first["msg"] == "hello world" and first["level"] == "WARNING" and first["logger"] == "xtnl.test.jsonlogs"
    assert first["epoch"] == "4711" and first["node"] == "node-a"
    assert str(first["ts"]).endswith("Z") and "T" in str(first["ts"])
    assert second["epoch"] is None                       # a STANDBY line has no epoch, not a stale one


def test_entity_fields_from_the_log_context_and_an_entity_logger(capture: tuple[logging.Logger, io.StringIO]) -> None:
    logger, stream = capture
    L.configure_logging("DEBUG", logger, fmt="json")
    with L.log_context(client_order_id="C-1", account_id="A-1", position_id=None):
        logger.info("in context")
        with L.log_context(seq_id=99):
            logger.info("nested")
        L.EntityLogger(logger, position_id="P-7", account_id="A-2").info("adapter", extra={"seq_id": 5})
    logger.info("outside")
    a, b, c, d = lines(stream)
    assert a["client_order_id"] == "C-1" and a["account_id"] == "A-1" and "position_id" not in a
    assert b["seq_id"] == 99 and b["client_order_id"] == "C-1"
    assert c["position_id"] == "P-7" and c["account_id"] == "A-2" and c["seq_id"] == 5   # record fields win
    assert "client_order_id" not in d and "seq_id" not in d


def test_json_lines_stay_redacted_and_carry_exceptions(capture: tuple[logging.Logger, io.StringIO]) -> None:
    logger, stream = capture
    L.configure_logging("INFO", logger, fmt="json")
    assert any(isinstance(f, UrlRedactingFilter) for f in logger.handlers[0].filters)
    logger.warning("dsn postgresql://u:PASS-SECRET@db.example:5432/x?password=P2")
    try:
        raise RuntimeError("GET https://api.example/session?token=TOK-SECRET")
    except RuntimeError:
        logger.exception("call failed")
    text = stream.getvalue()
    for secret in ("PASS-SECRET", "P2", "TOK-SECRET"):
        assert secret not in text
    rows = lines(stream)
    assert "postgresql://db.example:5432" in str(rows[0]["msg"])
    assert "RuntimeError" in str(rows[1]["exc"])


def test_default_format_is_json_on_linux_and_text_elsewhere() -> None:
    assert L.default_format({}, "linux") == "json"
    assert L.default_format({}, "win32") == "text"
    assert L.default_format({"XTNL_LOG_FORMAT": "text"}, "linux") == "text"
    assert L.default_format({"XTNL_LOG_FORMAT": "JSON"}, "win32") == "json"


def test_entity_fields_of_dtos() -> None:
    order = OrderDTO(client_order_id="C-9", broker_order_id="B-9", account_id="A", adapter_id="mock", symbol="EURUSD",
                     side="BUY", status="WORKING", entry_price=Decimal("1.1"), ordered_units=Decimal(1),
                     remaining_units=Decimal(1), linked_position_id="P-1", broker_updated_at_ms=1, seq_id=123)
    pos = PositionDTO(position_id="P-2", account_id="A", adapter_id="mock", symbol="EURUSD", side="BUY",
                      units=Decimal(1), entry_price=Decimal("1.1"), status="OPEN", broker_updated_at_ms=1)
    assert L.entity_fields(order) == {"client_order_id": "C-9", "account_id": "A", "position_id": "P-1",
                                      "seq_id": 123}
    assert L.entity_fields(pos) == {"position_id": "P-2", "account_id": "A"}    # seq_id 0: not stamped yet


def test_main_reexports_the_redaction_helpers() -> None:
    import src.main as entry
    from src.observability import redact
    assert entry.redact_urls is redact.redact_urls and entry.UrlRedactingFilter is redact.UrlRedactingFilter
