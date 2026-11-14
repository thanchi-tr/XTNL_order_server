"""Build the alert channels from settings. A channel whose settings are missing is left out (disabled)
and named in `ChannelSet.disabled`; the relay reports it at start-up and in its status."""
from __future__ import annotations

from dataclasses import dataclass

import httpx

from src.notifications.mail import SesTransport, SmtpFactory, SmtpTransport, default_smtp_factory
from src.notifications.push import NtfyTransport, PushoverTransport
from src.notifications.settings import NotificationSettings, SesSettings
from src.oversee.notify import EMAIL, MOBILE_PUSH, AlertTransport, Channel, EmailChannel, MobilePushChannel


@dataclass(frozen=True)
class ChannelSet:
    channels: tuple[Channel, ...]
    disabled: tuple[str, ...]
    issues: tuple[str, ...]


def build_channels(settings: NotificationSettings, client: httpx.AsyncClient, *,
                   smtp_factory: SmtpFactory = default_smtp_factory) -> ChannelSet:
    """`client` is shared by the HTTPS transports (SES, ntfy, Pushover); the caller owns and closes it."""
    channels: list[Channel] = []
    disabled: list[str] = []
    email: AlertTransport | None = None
    if isinstance(settings.email, SesSettings):
        email = SesTransport(settings.email, client)
    elif settings.email is not None:
        email = SmtpTransport(settings.email, factory=smtp_factory)
    if email is None:
        disabled.append(EMAIL)
    else:
        channels.append(EmailChannel(transport=email))
    push: AlertTransport | None = None
    if settings.push is not None:
        push = (PushoverTransport(settings.push, client) if settings.push.provider == "pushover"
                else NtfyTransport(settings.push, client))
    if push is None:
        disabled.append(MOBILE_PUSH)
    else:
        channels.append(MobilePushChannel(transport=push))
    return ChannelSet(tuple(channels), tuple(disabled), settings.issues)
