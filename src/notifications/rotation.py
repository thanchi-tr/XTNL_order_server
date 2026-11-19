"""Channel credentials that follow the secrets source (oversee-rules.md §10, operations.md §13).

`RotatingChannels` is the relay's `channel_source`: `poll()` re-reads the notification settings through
`load` (no network I/O: `settings_from_secrets` uses `SecretsSource.resolve_cached`, whose background
thread refreshes the secret every `refresh_s`) and returns a rebuilt `ChannelSet` only when the settings
changed, so a rotated SMTP / SES / push credential is picked up without a restart. `invalidate()` (called
by the relay on a channel auth error) asks the secrets source to re-read the secret at once; the relay
then polls every tick for a while, so the fresh value is used as soon as the refresher has read it.

Wiring (5.6): `rc = RotatingChannels(settings_from_secrets(source, os.environ), http_client,
smtp_factory=..., invalidate=source.invalidate)`; `cs = rc.current`; then
`OutboxRelay(..., cs.channels, disabled=cs.disabled, issues=cs.issues, channel_source=rc)`.

Settings are compared, never logged: only the backends and the issue lines (names only) are.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Mapping

import httpx

from src.notifications.errors import safe_error
from src.notifications.factory import OVERSEE_CHANNEL_NAMES, ChannelSet, build_channels
from src.notifications.mail import SmtpFactory, default_smtp_factory
from src.notifications.settings import OVERSEE_PREFIX, NotificationSettings
from src.secrets.source import SecretsSource

log = logging.getLogger(__name__)


def settings_from_secrets(source: SecretsSource, environ: Mapping[str, str], *,
                          prefix: str = OVERSEE_PREFIX) -> Callable[[], NotificationSettings]:
    """A loader that reads the channel settings from the environment with the cached secret laid over it
    (never blocks: `resolve_cached`). `prefix` is `XTNL_` (Oversee) or `XTNL_ARM_` (the ARM channels, S29)."""
    def load() -> NotificationSettings:
        return NotificationSettings.from_env(source.resolve_cached(environ), prefix=prefix)
    return load


class RotatingChannels:
    def __init__(self, load: Callable[[], NotificationSettings], client: httpx.AsyncClient, *,
                 smtp_factory: SmtpFactory = default_smtp_factory,
                 invalidate: Callable[[], None] = lambda: None,
                 initial: NotificationSettings | None = None,
                 names: tuple[str, str] = OVERSEE_CHANNEL_NAMES) -> None:
        self._load = load
        self._names = names
        self._client = client
        self._smtp_factory = smtp_factory
        self._invalidate = invalidate
        self._settings = load() if initial is None else initial
        self._current = self._build(self._settings)
        self.rebuilds = 0

    def _build(self, settings: NotificationSettings) -> ChannelSet:
        return build_channels(settings, self._client, smtp_factory=self._smtp_factory, names=self._names)

    @property
    def current(self) -> ChannelSet:
        return self._current

    def poll(self) -> ChannelSet | None:
        try:
            settings = self._load()
        except Exception as exc:  # an unreadable secret keeps the channels already built
            log.warning("notifications: channel settings not reloaded (%s)", safe_error(exc))
            return None
        if settings == self._settings:
            return None
        self._settings = settings
        self._current = self._build(settings)
        self.rebuilds += 1
        log.warning("notifications: channel settings changed (rotated credentials); channels rebuilt")
        return self._current

    def invalidate(self) -> None:
        self._invalidate()
