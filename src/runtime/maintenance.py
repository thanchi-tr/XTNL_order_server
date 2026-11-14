"""Declared broker maintenance windows (operations.md §10 BROKER_MAINTENANCE; IMPLEMENTATION_PLAN 6.5).

The SM10 overlay (`MarketCalendar.in_maintenance`) only runs when something declares a window. Phase 1 has no
PostgreSQL table or CLI for that yet, so the production source is a reviewed JSON file that the operator deploys
to **both** engine nodes (`XTNL_MAINTENANCE_FILE`, default `/etc/xtnl/maintenance.json`):

    {"stonex": [{"start": "2026-11-14T22:00:00+00:00", "end": "2026-11-15T02:00:00+00:00",
                 "reason": "CIAPI weekend maintenance"}],
     "pepperstone": [{"start_ms": 1763157600000, "end_ms": 1763164800000}]}

Each window is parsed by `src.calendar.schedule.maintenance_window_from_json` (instants with an offset, or epoch
milliseconds). A missing file means "no windows declared" (not an error). A file that cannot be read or parsed
raises `MaintenanceSourceError`: the session keeps the last good list and pages (fail loud, never a silent
un-declare or a silent declare). The read runs in a worker thread (`asyncio.to_thread`), never on the event loop.
"""
from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final, Protocol

from src.calendar.schedule import MaintenanceWindow, ScheduleError, maintenance_window_from_json

MAINTENANCE_FILE_VAR: Final = "XTNL_MAINTENANCE_FILE"
DEFAULT_MAINTENANCE_FILE: Final = "/etc/xtnl/maintenance.json"
MAX_FILE_BYTES: Final = 1 << 20
MAX_WINDOWS: Final = 1_000


class MaintenanceSourceError(RuntimeError):
    """The declared windows cannot be read (the message names the file, never its contents)."""


class MaintenanceSource(Protocol):
    """Where the session reads the declared maintenance windows from (re-read periodically; idempotent)."""

    async def load(self) -> Sequence[MaintenanceWindow]: ...


def parse_maintenance(doc: Any) -> list[MaintenanceWindow]:
    """{adapter_id: [window, ...]} -> windows. Raises MaintenanceSourceError on any malformed entry."""
    if not isinstance(doc, Mapping):
        raise MaintenanceSourceError("maintenance file must be a JSON object {adapter_id: [windows]}")
    out: list[MaintenanceWindow] = []
    for adapter_id, windows in doc.items():
        if not isinstance(adapter_id, str) or not adapter_id.strip():
            raise MaintenanceSourceError("maintenance file: adapter ids must be non-empty strings")
        if not isinstance(windows, list):
            raise MaintenanceSourceError(f"maintenance file: {adapter_id} must map to a list of windows")
        for obj in windows:
            if not isinstance(obj, Mapping):
                raise MaintenanceSourceError(f"maintenance file: {adapter_id} has a window that is not an object")
            try:
                out.append(maintenance_window_from_json(adapter_id.strip(), obj))
            except ScheduleError as exc:
                raise MaintenanceSourceError(f"maintenance file: {adapter_id}: {exc}") from None
            if len(out) > MAX_WINDOWS:
                raise MaintenanceSourceError(f"maintenance file declares more than {MAX_WINDOWS} windows")
    return out


class FileMaintenanceSource:
    """MaintenanceSource over a JSON file (see the module docstring)."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)

    def __repr__(self) -> str:
        return f"FileMaintenanceSource({str(self.path)!r})"

    def _read(self) -> list[MaintenanceWindow]:
        try:
            size = self.path.stat().st_size
        except FileNotFoundError:
            return []
        except OSError as exc:
            raise MaintenanceSourceError(f"{self.path}: unreadable ({type(exc).__name__})") from None
        if size > MAX_FILE_BYTES:
            raise MaintenanceSourceError(f"{self.path}: larger than {MAX_FILE_BYTES} bytes")
        try:
            text = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return []
        except (OSError, UnicodeDecodeError) as exc:
            raise MaintenanceSourceError(f"{self.path}: unreadable ({type(exc).__name__})") from None
        if not text.strip():
            return []
        try:
            doc = json.loads(text)
        except ValueError as exc:
            raise MaintenanceSourceError(f"{self.path}: not valid JSON (line {getattr(exc, 'lineno', '?')})") from None
        return parse_maintenance(doc)

    async def load(self) -> Sequence[MaintenanceWindow]:
        return await asyncio.to_thread(self._read)


def maintenance_source_from_env(environ: Mapping[str, str]) -> FileMaintenanceSource:
    path = (environ.get(MAINTENANCE_FILE_VAR) or "").strip() or DEFAULT_MAINTENANCE_FILE
    return FileMaintenanceSource(path)


__all__ = ["DEFAULT_MAINTENANCE_FILE", "MAINTENANCE_FILE_VAR", "FileMaintenanceSource", "MaintenanceSource",
           "MaintenanceSourceError", "maintenance_source_from_env", "parse_maintenance"]
