"""PgEmulator plus the ARM outbox statements (S29): the Oversee relay's store and the ARM relay's store work on one
`notification_outbox` table, so a test can run both relays against the same rows.

Like PgEmulator it is not a SQL engine: each handler implements one statement's documented semantics (fence, the
`kind = 'arm'` filter, `sent_at_ms IS NULL`). Every write of a kind `arm` row is checked against the 0016 channel
CHECK, evaluated from the migration's own regex, so a statement that would be refused by PostgreSQL fails here too."""
from __future__ import annotations

import re
from typing import Any

from src.db import arm_failures_store as F
from src.db import arm_outbox_store as A
from src.db.migrate import discover

from tests.db.pg_emulator import PgEmulator

_MIG = next(m for m in discover() if m.version == 16)
_CODE = "\n".join(line.split("--", 1)[0] for line in _MIG.sql.splitlines())
_REGEX = re.search(r"~\s*'([^']+)'", _CODE)
assert _REGEX is not None
ARM_CHANNELS_REGEX = _REGEX.group(1)


class CheckViolation(Exception):
    """notification_outbox_arm_channels_check (23514) would refuse the row."""


def arm_check(kind: str, channels: list[str] | None) -> bool:
    card = None if channels is None else len(channels)
    text = None if channels is None else ",".join(c for c in channels if c is not None)
    rhs = (card or 0) > 0 and re.search(ARM_CHANNELS_REGEX, text or "") is not None
    return (kind == "arm") == rhs


class ArmOutboxEmulator(PgEmulator):
    def __init__(self, epoch: int = 7) -> None:
        super().__init__(epoch)
        self._h.update({
            A.SQL_ARM_OUTBOX_INSERT: self._arm_insert, A.SQL_ARM_OUTBOX_DUE: self._arm_due,
            A.SQL_ARM_OUTBOX_PENDING: self._arm_pending, A.SQL_ARM_OUTBOX_RECENT: self._arm_recent,
            A.SQL_ARM_OUTBOX_OLDEST_UNSENT: self._arm_oldest, A.SQL_ARM_OUTBOX_RESCHEDULE: self._arm_reschedule,
            A.SQL_ARM_OUTBOX_FINISH: self._arm_finish, A.SQL_ARM_OUTBOX_EXPIRED: self._arm_expired,
            A.SQL_ARM_OUTBOX_EXPIRE: self._arm_expire, F.SQL_ARM_PING_INSERT: self._ping_insert,
            "SET LOCAL lock_timeout = '2s'": lambda a: "SET",
        })

    def add_outbox_row(self, *, kind: str, channels: list[str], created: int = 1, severity: str = "INFO",
                       title: str = "t", body: str = "b", episode: Any = None, bypass: bool = False,
                       account_id: Any = None, next_attempt: int | None = None,
                       expires: int | None = None) -> int:
        if not arm_check(kind, list(channels)):
            raise CheckViolation(f"{kind} {channels}")
        oid = super().add_outbox_row(kind=kind, channels=channels, created=created, severity=severity, title=title,
                                     body=body, episode=episode, bypass=bypass, account_id=account_id,
                                     next_attempt=next_attempt)
        self.t.outbox[-1].update({"expires": expires, "superseded_by": None, "last_error": None,
                                  "delivered_at": None})
        return oid

    # ---- helpers -------------------------------------------------------------------------------------
    def arm_rows(self) -> list[dict[str, Any]]:
        return [r for r in self.t.outbox if r["kind"] == "arm"]

    def _find(self, oid: int, created: int) -> dict[str, Any] | None:
        for r in self.t.outbox:
            if r["outbox_id"] == oid and r["created"] == created and r["kind"] == "arm" and r["sent_at"] is None:
                return r
        return None

    @staticmethod
    def _cols(r: dict[str, Any]) -> tuple[Any, ...]:
        return (r["outbox_id"], r["created"], r["severity"], r["title"], r["body"], list(r["channels"]),
                r["attempts"], r.get("expires"), r.get("delivered_at"))

    def _unsent_arm(self) -> list[dict[str, Any]]:
        return [r for r in self.t.outbox if r["kind"] == "arm" and r["sent_at"] is None]

    # ---- handlers ------------------------------------------------------------------------------------
    def _arm_insert(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        created, channels, subject, body, expires = a[1:6]
        self.add_outbox_row(kind="arm", channels=list(channels), created=created, severity="INFO", title=subject,
                            body=body, next_attempt=created, expires=expires)
        return 1

    def _ping_insert(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        created, title, body = a[1:4]
        self.add_outbox_row(kind="arm", channels=["arm_email", "arm_push"], created=created, severity="CRITICAL",
                            title=title, body=body)
        return 1

    def _arm_due(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        now, limit = a
        rows = [r for r in self._unsent_arm()
                if r["next_attempt"] <= now or (r.get("expires") is not None and r["expires"] <= now)]
        rows.sort(key=lambda r: (r["next_attempt"], r["outbox_id"]))
        return [self._cols(r) for r in rows[:limit]]

    def _arm_pending(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        return [self._cols(r) for r in self._unsent_arm()[:a[0]]]

    def _arm_recent(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        since, limit = a
        rows = sorted((r for r in self.arm_rows() if r["created"] >= since), key=lambda r: -r["outbox_id"])
        return [(r["outbox_id"], r["created"], r["severity"], r["body"]) for r in rows[:limit]]

    def _arm_oldest(self, a: tuple[Any, ...]) -> tuple[Any, ...]:
        return (min((r["created"] for r in self._unsent_arm()), default=None),)

    def _arm_reschedule(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        oid, created, channels, next_ms, error, inc, delivered_at = a[1:8]
        r = self._find(oid, created)
        if r is None:
            return 0
        if not arm_check("arm", list(channels)):
            raise CheckViolation(str(channels))
        r.update(channels=list(channels), next_attempt=next_ms, last_error=error, attempts=r["attempts"] + inc)
        if r["delivered_at"] is None:
            r["delivered_at"] = delivered_at
        return 1

    def _arm_finish(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        oid, created, at, delivered, inc, error, body, by = a[1:9]
        r = self._find(oid, created)
        if r is None:
            return 0
        r.update(sent_at=at, delivered=delivered, attempts=r["attempts"] + inc, last_error=error, body=body,
                 superseded_by=by)
        if delivered and r["delivered_at"] is None:
            r["delivered_at"] = at
        return 1

    def _arm_expired(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        now, limit = a
        rows = [r for r in self._unsent_arm() if r.get("expires") is not None and r["expires"] <= now]
        return [(r["outbox_id"], r["created"], r["body"]) for r in rows[:limit]]

    def _arm_expire(self, a: tuple[Any, ...]) -> int:
        if not self._fenced(a):
            return 0
        now, ids, created, bodies = a[1:5]
        n = 0
        for oid, c, body in zip(ids, created, bodies):
            r = self._find(oid, c)
            if r is None or r.get("expires") is None or r["expires"] > now:
                continue
            r.update(sent_at=now, delivered=False, last_error="expired", body=body)
            n += 1
        return n
