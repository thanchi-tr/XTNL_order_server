"""The operator CLI's SQL against a real PostgreSQL (skipped unless ORDER_SERVER_TEST_DSN points at a
DISPOSABLE database: the tests truncate engine tables). operator_audit itself is never truncated: it is
append-only, which is one of the things checked here."""
from __future__ import annotations

import importlib
import os
from collections.abc import AsyncIterator

import pytest

from src.db.epoch import Epoch, bump_epoch
from src.db.migrate import migrate
from src.db.pg import AsyncpgConnection, PgConnection, init_connection
from src.ops.model import Actor
from src.ops.service import OpsService

DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
pytestmark = [pytest.mark.integration, pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")]

TABLES = ("engine_epoch, halt_breaches, breach_acks, halt_events, oversee_events, notification_outbox, "
          "events_dlq, events_quarantine, adapter_auth_state, operator_requests, account_state")
A2 = Actor("it-alice", "integration test resume", approver="it-bob")


@pytest.fixture
async def conn() -> AsyncIterator[PgConnection]:
    asyncpg = importlib.import_module("asyncpg")
    raw = await asyncpg.connect(DSN, server_settings={"application_name": "xtnl-ops"})
    await init_connection(raw)
    c = AsyncpgConnection(raw)
    try:
        await migrate(c)
        await c.execute(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE")
        await c.execute("UPDATE cluster_state SET safe_hold = false WHERE id = 1")
        await bump_epoch(c, Epoch("it-ops", 11), "it-node", terminate_older=False)
        yield c
    finally:
        await raw.close()


async def last_audit(c: PgConnection) -> tuple[object, ...] | None:
    return await c.fetchrow("SELECT command, outcome, operator, approver, epoch, db_user = session_user "
                            "FROM operator_audit ORDER BY audit_id DESC LIMIT 1")


async def test_resume_and_audit_commit_together(conn: PgConnection) -> None:
    await conn.execute("UPDATE cluster_state SET safe_hold = true, safe_hold_since = 1, safe_hold_reason = 'it' "
                       "WHERE id = 1")
    rep = await OpsService(conn, None).safe_hold_resume(A2)
    assert rep.outcome == "applied"
    row = await conn.fetchrow("SELECT safe_hold, resumed_by FROM cluster_state WHERE id = 1")
    assert row == (False, "it-alice")
    assert await last_audit(conn) == ("safe_hold.resume", "applied", "it-alice", "it-bob", 11, True)


async def test_operator_audit_rejects_update_delete_truncate(conn: PgConnection) -> None:
    await OpsService(conn, None).safe_hold_resume(A2)   # refused -> one audit row
    for stmt in ("UPDATE operator_audit SET reason = 'rewritten history'", "DELETE FROM operator_audit",
                 "TRUNCATE operator_audit CASCADE"):
        with pytest.raises(Exception, match="append-only"):
            await conn.execute(stmt)


async def test_fenced_writes_and_dlq_release(conn: PgConnection) -> None:
    svc = OpsService(conn, None)
    await conn.execute("INSERT INTO adapter_auth_state (adapter_id, credential_generation, state) "
                       "VALUES ('stonex', 'v1', 'AUTH_FAILED')")
    assert (await svc.adapter_rearm("stonex", "v1", A2)).outcome == "applied"
    await conn.execute("INSERT INTO events_dlq (dlq_id, event_type, entity_id, adapter_id, raw_payload, payload_hash, "
                       "error_reason, retry_count, status) VALUES (1, 'ORDER', 'E1', 'stonex', '{}', "
                       "repeat('a', 64), 'schema_invalid', 5, 'QUARANTINED')")
    await conn.execute("INSERT INTO events_quarantine (quarantine_id, source, dlq_id, error_reason) "
                       "VALUES (1, 'dlq', 1, 'schema_invalid')")
    rep = await svc.dlq_release_quarantine(1, A2)
    assert rep.outcome == "applied"
    assert await conn.fetchrow("SELECT status, retry_count FROM events_dlq WHERE dlq_id = 1") == ("QUEUED", 0)
    assert (await svc.task_reset("risksweep", A2)).outcome == "applied"
    assert await conn.fetchrow("SELECT requested_epoch FROM operator_requests") == (11,)
    # a new leader term: each command reads the epoch afresh, so it is written under (and audited with) 12
    await bump_epoch(conn, Epoch("it-ops", 12), "it-node-b", terminate_older=False)
    await conn.execute("UPDATE cluster_state SET safe_hold = true WHERE id = 1")
    assert (await svc.safe_hold_resume(A2)).outcome == "applied"
    assert (await last_audit(conn) or ())[4] == 12


async def test_halt_clear_writes_halt_events_and_audit(conn: PgConnection) -> None:
    await conn.execute("""INSERT INTO halt_events (level, effective_level, hard_latch, rule_id, reason, "by")
                          VALUES ('HARD', 'HARD', true, 'r', 'escalation', 'system')""")
    await conn.execute("INSERT INTO account_state (adapter_id, account_id, currency, balance, broker_updated_at_ms, "
                       "observed_seq, source, version, seq_id, checksum, digest_hash) VALUES ('it', 'ACC', 'USD', 1, "
                       "1, 1, 'REST', repeat('0', 56), 1, repeat('0', 64), repeat('0', 64))")
    eid = (await conn.fetchrow("SELECT max(id) FROM halt_events"))
    assert eid is not None
    rep = await OpsService(conn, None).halt_clear("ACC", int(eid[0]), force=False,
                                                  actor=Actor("it-alice", "integration clear test"))
    assert rep.outcome == "applied"
    assert await conn.fetchrow('SELECT effective_level, hard_latch, "by" FROM halt_events ORDER BY id DESC LIMIT 1') \
        == ("NONE", False, "it-alice")
    assert (await last_audit(conn) or ())[:2] == ("halt.clear", "applied")


async def test_ruleset_add_and_activate_round_trip_through_jsonb(conn: PgConnection) -> None:
    """4.9: what `ruleset add` stores (jsonb) is what the engine's reader parses, and activation leaves one
    active row; both writes are audited in their own transaction."""
    from src.db.ruleset_store import SQL_ACTIVE
    from src.oversee.ruleset import parse_ruleset

    await conn.execute("DELETE FROM rulesets")
    doc = '{"max_position_risk": {"flag_above_pct": "0.8", "halt_above_pct": "1.2"}, ' \
          '"marks": {"max_clean_spread_pips": 3.5}}'
    svc = OpsService(conn, None)
    one = Actor("it-alice", "integration ruleset add")
    assert (await svc.ruleset_add(doc, "it.json", one)).outcome == "applied"
    assert (await svc.ruleset_add(doc, "it.json", one)).fields["version"] == 2
    assert await last_audit(conn) is not None
    assert (await svc.ruleset_activate(1, A2)).outcome == "applied"
    assert (await svc.ruleset_activate(2, A2)).fields["previous_active"] == [1]
    row = await conn.fetchrow(SQL_ACTIVE)
    assert row is not None and int(row[0]) == 2
    assert parse_ruleset(row[1], 2) == parse_ruleset(doc, 2)
    assert await conn.fetchrow("SELECT count(*) FROM rulesets WHERE active") == (1,)
    assert (await last_audit(conn) or ())[:2] == ("ruleset.activate", "applied")
