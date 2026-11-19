"""K5 / K10 at the Melbourne receiver (operations.md §13): the paging channel credentials follow the secrets source
without a restart, the server certificate is re-loaded in place, and the watched Melbourne certificates (its own and
the dashboard's Redis client certificate, which no London exporter can see) page when unreadable or within 30 days.
No network: fake transports, a scripted secret, certificates made with openssl."""
from __future__ import annotations

from pathlib import Path

from src.deadman_receiver.__main__ import channels_source
from src.deadman_receiver.pager import Pager
from src.deadman_receiver.service import CERT_CHECK_S, CHANNEL_REFRESH_S, ReceiverService
from src.deadman_receiver.settings import ReceiverSettings, channel_settings
from src.notifications.settings import NotificationSettings
from src.observability.certs import CertReading
from src.oversee.notify import AlertTransport
from tests.certutil import make_cert
from tests.deadman_receiver.test_service import ENV, Clock, FakeTransport
from tests.deadman_receiver.test_token_rotation_82 import Secret


def _service(clock: Clock, secret: Secret, built: list[NotificationSettings],
             **kw: object) -> tuple[ReceiverService, FakeTransport]:
    settings = ReceiverSettings.from_env(secret.resolve(ENV))
    first = FakeTransport("ntfy")

    def transports_for(channels: NotificationSettings) -> list[AlertTransport]:
        built.append(channels)
        return [FakeTransport("ntfy")]
    src = channels_source(secret, ENV)  # type: ignore[arg-type]
    svc = ReceiverService(settings, Pager([first], clock), clock_ms=clock, channels_source=src,
                          transports_for=transports_for, **kw)  # type: ignore[arg-type]
    return svc, first


async def test_k10_rotated_channel_credentials_rebuild_the_transports_without_a_restart() -> None:
    clock = Clock()
    secret = Secret({"XTNL_RECEIVER_PUSH_TOKEN": "push-token-one-0123456789"})
    built: list[NotificationSettings] = []
    svc, first = _service(clock, secret, built, read_certs=lambda: [])
    await svc.tick()
    assert built == [] and first.sent                         # unchanged: the start-up transports page
    secret.values = {"XTNL_RECEIVER_PUSH_TOKEN": "push-token-two-0123456789"}   # rotated in Secrets Manager
    clock.ms += CHANNEL_REFRESH_S * 1000
    await svc.tick()
    assert len(built) == 1 and built[0] == channel_settings(secret.resolve(ENV))
    # A secret with no usable channel keeps the rebuilt transports and pages channel_source.
    secret.values = {"XTNL_RECEIVER_PUSH_URL": "https://push.eu-west-2.example.invalid/x"}
    clock.ms += CHANNEL_REFRESH_S * 1000
    await svc.tick()
    assert len(built) == 1 and "channel_source" in svc.active
    secret.values = {"XTNL_RECEIVER_PUSH_TOKEN": "push-token-two-0123456789"}
    clock.ms += CHANNEL_REFRESH_S * 1000
    await svc.tick()
    assert "channel_source" not in svc.active and len(built) == 1   # same settings as already built


async def test_k10_pending_retries_move_to_the_rebuilt_transport() -> None:
    clock = Clock()
    old = FakeTransport("ntfy", fail=1)
    pager = Pager([old], clock)
    from src.oversee.notify import AlertMessage
    assert not await pager.page(AlertMessage("s", "b", "critical", "episode"))
    new = FakeTransport("ntfy")
    pager.replace_transports([new])
    clock.ms += 60_000
    assert await pager.retry_due() == 1 and new.sent and not old.sent


async def test_k5_watched_certificates_page_from_melbourne(tmp_path: Path) -> None:
    clock = Clock()
    good, _ = make_cert(tmp_path, "good", days=400)
    soon, _ = make_cert(tmp_path, "soon", days=5)
    env = {**ENV, "XTNL_RECEIVER_CERTS": f"dashboard-redis-client={soon},ok={good},gone={tmp_path / 'gone.crt'}"}
    settings = ReceiverSettings.from_env(env)
    assert dict(settings.certs) == {"dashboard-redis-client": str(soon), "ok": str(good),
                                    "gone": str(tmp_path / "gone.crt")}
    t = FakeTransport()
    svc = ReceiverService(settings, Pager([t], clock), clock_ms=clock)
    await svc.tick()
    assert {"cert:dashboard-redis-client", "cert:gone"} <= set(svc.active) and "cert:ok" not in svc.active
    assert any("expires in" in m.subject and "dashboard-redis-client" in m.subject for m in t.sent)
    assert any("unreadable" in m.subject for m in t.sent)


async def test_k5_own_certificate_is_always_watched_and_reloaded(tmp_path: Path) -> None:
    clock = Clock()
    crt, key = make_cert(tmp_path, "tls", days=5)
    env = {**ENV, "XTNL_RECEIVER_LISTEN": "127.0.0.1:8443", "XTNL_RECEIVER_TLS_CERT": str(crt),
           "XTNL_RECEIVER_TLS_KEY": str(key)}
    settings = ReceiverSettings.from_env(env)
    assert dict(settings.certs) == {"deadman-receiver": str(crt)}
    svc = ReceiverService(settings, Pager([FakeTransport()], clock), clock_ms=clock)
    assert svc.ssl_context() is not None
    await svc.tick()
    assert "cert:deadman-receiver" in svc.active
    renewed, renewed_key = make_cert(tmp_path, "renewed", days=400)
    crt.write_bytes(renewed.read_bytes())
    key.write_bytes(renewed_key.read_bytes())
    clock.ms += CERT_CHECK_S * 1000
    await svc.tick()
    assert "cert:deadman-receiver" not in svc.active and svc._tls is not None and svc._tls.reloads == 1


async def test_k5_reader_errors_do_not_clear_or_crash() -> None:
    clock = Clock()
    settings = ReceiverSettings.from_env(ENV)
    calls = [[CertReading("x", None)]]

    def read() -> list[CertReading]:
        if calls:
            return calls.pop()
        raise OSError("boom")
    svc = ReceiverService(settings, Pager([FakeTransport()], clock), clock_ms=clock, read_certs=read)
    await svc.tick()
    assert "cert:x" in svc.active
    clock.ms += CERT_CHECK_S * 1000
    await svc.tick()
    assert "cert:x" in svc.active                    # a failed check keeps the last result
