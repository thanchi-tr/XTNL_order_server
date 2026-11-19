"""0016_outbox_arm.sql (S08, design PR4 / §C1 0016): idempotent, two nullable columns, the ARM due index ON ONLY
the partitioned parent, and the kind 'arm' channel CHECK added NOT VALID in the no-subquery, coalesce form.

The CHECK table test evaluates the migration's own expression (its regex is read from the file) with PostgreSQL's
semantics for `cardinality`, `array_to_string` (NULL elements skipped) and `coalesce`; the `pg`-marked test in
tests/db/test_pg_integration.py runs the same table against a real server."""
from __future__ import annotations

import re

import pytest

from src.db.migrate import POST_STEP, PartitionIndex, ValidateCheck, discover

MIG = next(m for m in discover() if m.version == 16)
SQL = MIG.sql
CODE = "\n".join(line.split("--", 1)[0] for line in SQL.splitlines())
REGEX = re.search(r"~\s*'([^']+)'", CODE)
assert REGEX is not None
ARM_CHANNELS = REGEX.group(1)
MARKET_DATA_WORDS = ("bid", "ask", "mid", "quote", "spread", "tick", "ohlc", "price", "high", "low", "close", "atr",
                     "indicator", "favour")


def arm_check(kind: str, channels: list[str | None] | None) -> bool:
    """`(kind = 'arm') = (coalesce(cardinality(channels), 0) > 0 AND coalesce(array_to_string(channels, ','), '')
    ~ <regex>)` as PostgreSQL evaluates it (True = the row is accepted)."""
    card = None if channels is None else len(channels)
    text = None if channels is None else ",".join(c for c in channels if c is not None)
    rhs = (card or 0) > 0 and re.search(ARM_CHANNELS, text or "") is not None
    return (kind == "arm") == rhs


#: (kind, channels, accepted) - the plan's S08 CHECK table plus the encoded retry forms.
CHECK_TABLE: list[tuple[str, list[str | None] | None, bool]] = [
    ("arm", ["arm_email:3"], True),
    ("arm", ["arm_email", "arm_push"], True),
    ("arm", ["arm_push:12", "arm_email:1"], True),
    ("arm", ["arm_email", "+escalated"], False),          # the Oversee SM6 marker never fits an ARM row
    ("arm", ["+escalated"], False),
    ("arm", ["email"], False),
    ("arm", ["arm_email:"], False),
    ("arm", ["arm_email:x"], False),
    ("arm", ["arm_sms"], False),
    ("arm", [], False),                                   # empty array never passes for arm (R11-m)
    ("arm", None, False),                                 # nor NULL
    ("info", ["arm_email"], False),                       # an arm_* channel on any other kind fails
    ("episode", ["arm_push"], False),
    ("info", ["arm_email:1", "arm_push"], False),
    # the design's expression is an "if and only if" over the whole array: a non-arm row whose channels are not
    # all arm_* passes (no Oversee writer produces an arm_* channel; recorded, not tightened, S08 status note)
    ("episode", ["email", "arm_push"], True),
    ("info", [], True),                                   # non-arm with empty channels passes
    ("info", None, True),
    ("info", ["email"], True),
    ("trip", ["email:5", "mobile_push", "+escalated"], True),
    ("repage", ["mobile_push:2"], True),
]


def test_migration_is_0016_outbox_arm() -> None:
    assert MIG.name == "outbox_arm" and SQL.startswith("-- 0016_outbox_arm.sql")
    assert "SET client_min_messages = warning;" in CODE


def test_idempotent_and_catalog_only() -> None:
    assert not re.search(r"ADD COLUMN (?!IF NOT EXISTS)", CODE)
    assert not re.search(r"CREATE (UNIQUE )?INDEX (?!IF NOT EXISTS)", CODE)
    adds = re.findall(r"ALTER TABLE (\w+) ADD CONSTRAINT (\w+)", CODE)
    assert adds == [("notification_outbox", "notification_outbox_arm_channels_check")]
    assert re.search(r"IF NOT EXISTS \(SELECT 1 FROM pg_constraint\s+WHERE conrelid = 'notification_outbox'::regclass"
                     r"\s+AND conname = 'notification_outbox_arm_channels_check'\) THEN", CODE)
    assert CODE.count("DO $$") == 1 and CODE.count("$$;") == 1
    assert "VALIDATE CONSTRAINT" not in CODE and "CONCURRENTLY" not in CODE   # the post step does both
    assert "DROP " not in CODE and "UPDATE " not in CODE and "INSERT " not in CODE


def test_columns_nullable_without_default() -> None:
    added = re.findall(r"ALTER TABLE (\w+) ADD COLUMN IF NOT EXISTS (\w+) (\w+);", CODE)
    assert added == [("notification_outbox", "expires_at_ms", "bigint"),
                     ("notification_outbox", "superseded_by", "bigint")]
    for stmt in re.findall(r"ADD COLUMN IF NOT EXISTS [^;]*;", CODE):
        assert "NOT NULL" not in stmt and "DEFAULT" not in stmt
    for _, col, _ in added:
        assert not any(w in col for w in MARKET_DATA_WORDS), col


def test_index_is_on_only_the_parent_and_registered_for_the_post_step() -> None:
    assert re.search(r"CREATE INDEX IF NOT EXISTS notification_outbox_arm_due_idx\s+ON ONLY notification_outbox "
                     r"\(kind, next_attempt_at_ms, outbox_id\) WHERE sent_at_ms IS NULL;", CODE)
    idx = [it for it in POST_STEP if isinstance(it, PartitionIndex) and it.version == 16]
    assert idx == [PartitionIndex(16, "notification_outbox", "notification_outbox_arm_due_idx", "arm_due_idx",
                                  "kind, next_attempt_at_ms, outbox_id", "sent_at_ms IS NULL")]


def test_checks_are_not_valid_and_registered_for_validation() -> None:
    assert "NOT VALID;" in CODE
    assert "SELECT" not in CODE.split("ADD CONSTRAINT notification_outbox_arm_channels_check", 1)[1].split(";")[0]
    assert "coalesce(cardinality(channels), 0) > 0" in CODE
    assert "coalesce(array_to_string(channels, ','), '')" in CODE
    assert ARM_CHANNELS == "^(arm_email|arm_push)(:[0-9]+)?(,(arm_email|arm_push)(:[0-9]+)?)*$"
    checks = {(it.version, it.table, it.constraint) for it in POST_STEP if isinstance(it, ValidateCheck)}
    assert checks == {(15, "event_log", "event_log_origin_check"),
                      (16, "notification_outbox", "notification_outbox_arm_channels_check")}


@pytest.mark.parametrize("kind,channels,accepted", CHECK_TABLE)
def test_check_table(kind: str, channels: list[str | None] | None, accepted: bool) -> None:
    assert arm_check(kind, channels) is accepted


def test_existing_oversee_channel_forms_all_pass() -> None:
    """Every channels value the Oversee writers produce (SQL_OUTBOX_INSERT, encode_channels) stays valid, so the
    post step's VALIDATE never fails on existing rows."""
    from src.oversee.notify import encode_channels
    for kind in ("episode", "repage", "trip", "info"):
        for chans in (["email"], ["email", "mobile_push"]):
            assert arm_check(kind, list(chans))
            for esc in (False, True):
                assert arm_check(kind, list(encode_channels(chans, {c: 4 for c in chans}, esc)))
                assert arm_check(kind, list(encode_channels(chans[1:], {}, esc)))
