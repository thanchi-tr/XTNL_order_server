"""IMPLEMENTATION_PLAN 8.2: the receiver's bearer token follows the secrets source with an overlap window, so a
rotation needs no restart and refuses no heartbeat.

Before: `ReceiverSettings.from_env` read one fixed XTNL_RECEIVER_TOKEN at start-up; the documented order
("receiver first, then the secret") meant restarting the receiver with the new token, after which every beat from
the engine (still on the old token until its 5 min secret refresh) got 401 and Melbourne paged "no heartbeat".
No network: a scripted secrets source, in-memory streams."""
from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

import pytest

from src.deadman_receiver.pager import Pager
from src.deadman_receiver.service import ReceiverService
from src.deadman_receiver.settings import ReceiverConfigError, ReceiverSettings, receiver_secrets_source
from src.deadman_receiver.tokens import INVALIDATE_EVERY_MS, TokenRing, TokenSet
from src.secrets.source import overlay
from tests.deadman_receiver.test_service import ENV, T0, Clock, FakeTransport, payload, post, request

OLD = "old-token-0123456789abcdef"
NEW = "new-token-0123456789abcdef"
NEXT = "next-token-0123456789abcdef"


class Secret:
    """A SecretsSource whose cached secret the test rotates (resolve_cached never does I/O)."""

    kind = "aws-sm"
    version_id: str | None = "v1"

    def __init__(self, values: dict[str, str]) -> None:
        self.values = values
        self.invalidations = 0
        self.fail = False

    def resolve(self, base: Mapping[str, str]) -> dict[str, str]:
        return self.resolve_cached(base)

    def resolve_cached(self, base: Mapping[str, str]) -> dict[str, str]:
        if self.fail:
            raise RuntimeError("the secret has not been read yet")
        return overlay(base, self.values)[0]

    def invalidate(self) -> None:
        self.invalidations += 1

    def start(self) -> None:
        return None

    def close(self) -> None:
        return None


def service(clock: Clock, secret: Secret, **env: str) -> tuple[ReceiverService, FakeTransport]:
    base = {**ENV, **env}
    settings = ReceiverSettings.from_env(secret.resolve(base))
    ring = TokenRing(TokenSet(settings.token, settings.token_next), load=lambda: secret.resolve_cached(base),
                     invalidate=secret.invalidate, overlap_ms=settings.token_overlap_s * 1000)
    t = FakeTransport()
    return ReceiverService(settings, Pager([t], clock), clock_ms=clock, tokens=ring), t


async def beat(svc: ReceiverService, clock: Clock, token: str) -> int:
    clock.ms += 15_000
    status, _ = await request(svc, post(payload(clock.ms).to_json().encode(), token=token))
    await svc.tick()
    return status


async def test_rotation_in_the_secret_refuses_no_heartbeat_and_needs_no_restart() -> None:
    """Receiver secret rotated first: it accepts the new token at once and the old one for the overlap window;
    the engine switches minutes later (its own 5 min refresh) without a single 401; then the old token ends."""
    clock = Clock()
    secret = Secret({"XTNL_RECEIVER_TOKEN": OLD})
    svc, t = service(clock, secret)
    assert await beat(svc, clock, OLD) == 204
    secret.values = {"XTNL_RECEIVER_TOKEN": NEW}                 # rotation in Secrets Manager
    for _ in range(20):                                         # 5 min: the engine still sends the old token
        assert await beat(svc, clock, OLD) == 204
    assert svc.tokens.rotations == 1
    for _ in range(4):
        assert await beat(svc, clock, NEW) == 204                # the engine picked the new secret up
    assert "silence" not in svc.active and not [m for m in t.sent if "no heartbeat" in m.subject]
    clock.ms += 1_800_000                                       # past the 30 min overlap
    assert await beat(svc, clock, OLD) == 401                   # the replaced token is refused now
    assert await beat(svc, clock, NEW) == 204


async def test_a_next_token_is_accepted_beside_the_current_one_then_promoted() -> None:
    clock = Clock()
    secret = Secret({"XTNL_RECEIVER_TOKEN": OLD, "XTNL_RECEIVER_TOKEN_NEXT": NEXT})
    svc, _ = service(clock, secret)
    assert await beat(svc, clock, OLD) == 204 and await beat(svc, clock, NEXT) == 204
    secret.values = {"XTNL_RECEIVER_TOKEN": NEXT}               # promote: next becomes current, no next
    assert await beat(svc, clock, NEXT) == 204 and await beat(svc, clock, OLD) == 204   # old: overlap only
    clock.ms += 1_800_000
    assert await beat(svc, clock, OLD) == 401 and await beat(svc, clock, NEXT) == 204


async def test_a_bad_secret_keeps_the_last_valid_tokens_and_pages() -> None:
    clock = Clock()
    secret = Secret({"XTNL_RECEIVER_TOKEN": OLD})
    svc, t = service(clock, secret)
    secret.values = {"XTNL_RECEIVER_TOKEN": "short"}
    assert await beat(svc, clock, OLD) == 204                   # never an empty or weaker token set
    assert await beat(svc, clock, "short") == 401
    assert "token_source" in svc.active
    assert any("token source invalid" in m.subject and m.severity == "critical" for m in t.sent)
    assert all("short" not in m.body and OLD not in m.body for m in t.sent)
    secret.fail = True
    assert await beat(svc, clock, OLD) == 204 and "token_source" in svc.active
    secret.fail = False
    secret.values = {"XTNL_RECEIVER_TOKEN": OLD}
    assert await beat(svc, clock, OLD) == 204 and "token_source" not in svc.active


async def test_an_unknown_token_asks_for_a_reread_at_most_once_a_minute() -> None:
    clock = Clock()
    secret = Secret({"XTNL_RECEIVER_TOKEN": OLD})
    svc, _ = service(clock, secret)
    for _ in range(3):
        assert await beat(svc, clock, NEW) == 401                # 45 s of unknown tokens
    assert secret.invalidations == 1
    clock.ms += INVALIDATE_EVERY_MS
    assert await beat(svc, clock, NEW) == 401
    assert secret.invalidations == 2
    secret.values = {"XTNL_RECEIVER_TOKEN": NEW}                 # the re-read brought the rotated secret
    assert await beat(svc, clock, NEW) == 401                    # this request ran before the tick re-read it
    assert await beat(svc, clock, NEW) == 204


async def test_matched_slot_is_logged_without_the_value(caplog: pytest.LogCaptureFixture) -> None:
    clock = Clock()
    secret = Secret({"XTNL_RECEIVER_TOKEN": OLD, "XTNL_RECEIVER_TOKEN_NEXT": NEXT})
    svc, _ = service(clock, secret)
    with caplog.at_level(logging.INFO):
        await beat(svc, clock, OLD)
        await beat(svc, clock, NEXT)
    assert "authenticated with the current token" in caplog.text and "with the next token" in caplog.text
    assert OLD not in caplog.text and NEXT not in caplog.text
    assert OLD not in repr(svc.tokens.current) and NEXT not in repr(svc.tokens.current)


def test_env_settings_take_next_and_overlap_and_refuse_bad_values() -> None:
    s = ReceiverSettings.from_env({**ENV, "XTNL_RECEIVER_TOKEN_NEXT": NEXT, "XTNL_RECEIVER_TOKEN_OVERLAP_S": "900"})
    assert (s.token_next, s.token_overlap_s) == (NEXT, 900) and NEXT not in repr(s)
    assert ReceiverSettings.from_env(ENV).token_overlap_s == 1_800
    for kw, match in (({"XTNL_RECEIVER_TOKEN_NEXT": "short"}, "XTNL_RECEIVER_TOKEN_NEXT"),
                      ({"XTNL_RECEIVER_TOKEN_OVERLAP_S": "60"}, "XTNL_RECEIVER_TOKEN_OVERLAP_S"),
                      ({"XTNL_RECEIVER_TOKEN_OVERLAP_S": "x"}, "XTNL_RECEIVER_TOKEN_OVERLAP_S")):
        with pytest.raises(ReceiverConfigError, match=match):
            ReceiverSettings.from_env({**ENV, **kw})


def test_the_receiver_secret_must_be_outside_eu_west_2() -> None:
    with pytest.raises(ReceiverConfigError, match="XTNL_SECRETS_REGION"):
        receiver_secrets_source({"XTNL_SECRETS_SOURCE": "aws-sm", "XTNL_SECRETS_ID": "xtnl/deadman",
                                 "XTNL_SECRETS_REGION": "eu-west-2"})
    with pytest.raises(ReceiverConfigError, match="XTNL_SECRETS_ID"):
        receiver_secrets_source({"XTNL_SECRETS_SOURCE": "aws-sm", "XTNL_SECRETS_REGION": "ap-southeast-4"})
    src = receiver_secrets_source({"XTNL_SECRETS_SOURCE": "aws-sm", "XTNL_SECRETS_ID": "xtnl/deadman",
                                   "XTNL_SECRETS_REGION": "ap-southeast-4"})
    try:
        assert src.kind == "aws-sm"
    finally:
        src.close()
    assert receiver_secrets_source({}).kind == "env"


def test_main_uses_the_secret_and_exits_75_when_it_cannot_be_read(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.deadman_receiver.__main__ as entry
    from src.secrets.source import SecretsError

    for k in list(__import__("os").environ):
        if k.startswith("XTNL_RECEIVER_") or k.startswith("XTNL_SECRETS_"):
            monkeypatch.delenv(k)
    for k, v in ENV.items():
        if k != "XTNL_RECEIVER_TOKEN":
            monkeypatch.setenv(k, v)
    secret = Secret({"XTNL_RECEIVER_TOKEN": NEW})
    monkeypatch.setattr(entry, "receiver_secrets_source", lambda base: secret)
    assert entry.main(["--check-config"]) == 0                  # the token comes from the secret only

    class Down(Secret):
        def resolve(self, base: Mapping[str, str]) -> dict[str, Any]:
            raise SecretsError("cannot read the secret: secretsmanager GetSecretValue failed: HTTP 503 x")

    monkeypatch.setattr(entry, "receiver_secrets_source", lambda base: Down({}))
    assert entry.main(["--check-config"]) == 75                 # temporary: systemd restarts it (not 78)


def test_token_ring_for_the_env_source_is_fixed() -> None:
    import src.deadman_receiver.__main__ as entry

    settings = ReceiverSettings.from_env(ENV)
    ring = entry.token_ring(settings, receiver_secrets_source({}), ENV)
    ring.refresh(T0)
    assert ring.match(ENV["XTNL_RECEIVER_TOKEN"], T0) == "current" and ring.match(NEW, T0) is None
