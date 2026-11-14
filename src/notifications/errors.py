"""Channel errors: messages are safe to log (no credentials, no alert body, no URL path)."""
from __future__ import annotations


class ChannelError(Exception):
    """A delivery failed. `retryable=False`: the provider rejected the request itself (bad address,
    bad request) and an immediate retry would fail the same way. `auth=True`: credentials rejected."""

    def __init__(self, message: str, *, retryable: bool = True, auth: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.auth = auth


def safe_error(exc: BaseException) -> str:
    """A short, loggable description: ChannelError text, otherwise only the exception class."""
    if isinstance(exc, ChannelError):
        return str(exc)[:200]
    return type(exc).__name__
