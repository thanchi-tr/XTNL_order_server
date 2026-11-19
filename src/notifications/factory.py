"""Build the alert channels from settings. A channel whose settings are missing is left out (disabled)
and named in `ChannelSet.disabled`; the relay reports it at start-up and in its status.

`build_channels` builds the Oversee channels (`email`, `mobile_push`, from the `XTNL_` settings);
`build_arm_channels` the ARM relay's (`arm_email`, `arm_push`, from the `XTNL_ARM_` settings, design PR4, plan
S29): the same transports under the ARM channel names. Tests use `RecordingChannel(name=...)` for either set."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import httpx

from src.notifications.mail import SesTransport, SmtpFactory, SmtpTransport, default_smtp_factory
from src.notifications.push import NtfyTransport, PushoverTransport
from src.notifications.settings import NotificationSettings, SesSettings
from src.oversee.notify import EMAIL, MOBILE_PUSH, AlertTransport, Channel, EmailChannel, MobilePushChannel

ARM_EMAIL: Final = "arm_email"
ARM_PUSH: Final = "arm_push"
ARM_CHANNEL_NAMES: Final = (ARM_EMAIL, ARM_PUSH)
OVERSEE_CHANNEL_NAMES: Final = (EMAIL, MOBILE_PUSH)


@dataclass(frozen=True)
class ChannelSet:
    channels: tuple[Channel, ...]
    disabled: tuple[str, ...]
    issues: tuple[str, ...]


def build_channels(settings: NotificationSettings, client: httpx.AsyncClient, *,
                   smtp_factory: SmtpFactory = default_smtp_factory,
                   names: tuple[str, str] = OVERSEE_CHANNEL_NAMES) -> ChannelSet:
    """`client` is shared by the HTTPS transports (SES, ntfy, Pushover); the caller owns and closes it.
    `names` = (email channel name, push channel name)."""
    email_name, push_name = names
    channels: list[Channel] = []
    disabled: list[str] = []
    email: AlertTransport | None = None
    if isinstance(settings.email, SesSettings):
        email = SesTransport(settings.email, client)
    elif settings.email is not None:
        email = SmtpTransport(settings.email, factory=smtp_factory)
    if email is None:
        disabled.append(email_name)
    else:
        channels.append(EmailChannel(name=email_name, transport=email))
    push: AlertTransport | None = None
    if settings.push is not None:
        push = (PushoverTransport(settings.push, client) if settings.push.provider == "pushover"
                else NtfyTransport(settings.push, client))
    if push is None:
        disabled.append(push_name)
    else:
        channels.append(MobilePushChannel(name=push_name, transport=push))
    return ChannelSet(tuple(channels), tuple(disabled), settings.issues)


def build_arm_channels(settings: NotificationSettings, client: httpx.AsyncClient, *,
                       smtp_factory: SmtpFactory = default_smtp_factory) -> ChannelSet:
    """The ARM relay's `arm_email` / `arm_push` channels from the `XTNL_ARM_` settings
    (`NotificationSettings.from_env(env, prefix=ARM_PREFIX)`)."""
    return build_channels(settings, client, smtp_factory=smtp_factory, names=ARM_CHANNEL_NAMES)
