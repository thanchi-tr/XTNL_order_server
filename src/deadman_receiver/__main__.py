"""`python -m src.deadman_receiver [--check-config]`: the Melbourne dead-man receiver process.

Exit 78 (EX_CONFIG) on a bad configuration (the message names variables only); exit 75 (EX_TEMPFAIL, systemd
restarts it) when the Secrets Manager secret cannot be read at start. SIGTERM / SIGINT stop it.

Credentials follow the secrets source (`receiver_secrets_source`: XTNL_SECRETS_SOURCE env | aws-sm): read once at
start for the settings, then the bearer tokens are re-read from the cached secret on every tick (`TokenRing`).
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from collections.abc import Mapping, Sequence

import httpx

from src.deadman_receiver.pager import Pager
from src.deadman_receiver.service import ReceiverService, wall_ms
from src.deadman_receiver.settings import ReceiverConfigError, ReceiverSettings, receiver_secrets_source
from src.deadman_receiver.tokens import TokenRing, TokenSet
from src.notifications.mail import SesTransport, SmtpTransport
from src.notifications.push import NtfyTransport, PushoverTransport
from src.notifications.settings import NotificationSettings, SesSettings
from src.observability.logs import configure_logging
from src.oversee.notify import AlertTransport
from src.secrets.source import SecretsError, SecretsSource

log = logging.getLogger("xtnl.deadman_receiver")
EX_CONFIG = 78
EX_TEMPFAIL = 75


def build_transports(channels: NotificationSettings, client: httpx.AsyncClient) -> list[AlertTransport]:
    """The paging transports (the caller owns and closes `client`)."""
    out: list[AlertTransport] = []
    if isinstance(channels.email, SesSettings):
        out.append(SesTransport(channels.email, client))
    elif channels.email is not None:
        out.append(SmtpTransport(channels.email))
    if channels.push is not None:
        out.append(PushoverTransport(channels.push, client) if channels.push.provider == "pushover"
                   else NtfyTransport(channels.push, client))
    return out


def token_ring(settings: ReceiverSettings, source: SecretsSource, base: Mapping[str, str]) -> TokenRing:
    """The receiver's tokens, following `source` (env: fixed; aws-sm: the cached secret, re-read every tick)."""
    if source.kind == "env":
        return TokenRing(TokenSet(settings.token, settings.token_next), overlap_ms=settings.token_overlap_s * 1000)
    return TokenRing(TokenSet(settings.token, settings.token_next), load=lambda: source.resolve_cached(base),
                     invalidate=source.invalidate, overlap_ms=settings.token_overlap_s * 1000)


async def serve(settings: ReceiverSettings, tokens: TokenRing | None = None) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError, ValueError):
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    async with httpx.AsyncClient(timeout=10.0) as client:
        service = ReceiverService(settings, Pager(build_transports(settings.channels, client), wall_ms),
                                  tokens=tokens)
        await service.run(stop)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m src.deadman_receiver", description=__doc__)
    parser.add_argument("--check-config", action="store_true", help="validate the configuration and exit")
    args = parser.parse_args(argv)
    configure_logging(os.environ.get("XTNL_LOG_LEVEL", "INFO"), node="deadman-receiver")
    base = dict(os.environ)
    try:
        source = receiver_secrets_source(base)
    except ReceiverConfigError as exc:
        log.critical("dead-man receiver configuration refused: %s", exc)
        return EX_CONFIG
    try:
        try:
            resolved = source.resolve(base)
        except SecretsError as exc:
            log.critical("dead-man receiver cannot read its secret (will be restarted): %s", exc)
            return EX_TEMPFAIL
        try:
            settings = ReceiverSettings.from_env(resolved)
        except ReceiverConfigError as exc:
            log.critical("dead-man receiver configuration refused: %s", exc)
            return EX_CONFIG
        return _run(args.check_config, settings, source, base)
    finally:
        source.close()


def _run(check_config: bool, settings: ReceiverSettings, source: SecretsSource, base: Mapping[str, str]) -> int:
    for issue in settings.issues:
        log.warning("paging channel: %s", issue)
    if check_config:
        print(f"dead-man receiver config ok: listen {settings.host}:{settings.port}, "
              f"{'https' if settings.tls else 'http (loopback)'}, email={'on' if settings.channels.email else 'off'}, "
              f"push={'on' if settings.channels.push else 'off'}, tokens from {source.kind}")
        return 0
    source.start()
    asyncio.run(serve(settings, token_ring(settings, source, base)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
