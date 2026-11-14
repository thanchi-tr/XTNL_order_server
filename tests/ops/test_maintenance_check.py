"""7.3: `python -m src.ops maintenance check` - the declared broker maintenance windows (operations.md §10
BROKER_MAINTENANCE) of this node, read exactly as the engine reads them (offline, no database), with the
file's sha256 so the operator can prove both engine nodes hold the identical file."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from src.ops import cli
from src.runtime.maintenance import MAINTENANCE_FILE_VAR

from tests.ops.test_cli import Harness

NOW = 1_800_000_000_000          # 2027-01-15T08:00:00Z
WINDOW = {"stonex": [{"start": "2027-01-16T22:00:00+00:00", "end": "2027-01-17T02:00:00+00:00",
                      "reason": "CIAPI weekend maintenance"}]}


def write(tmp_path: Path, body: object, name: str = "maintenance.json") -> Path:
    p = tmp_path / name
    p.write_bytes((body if isinstance(body, str) else json.dumps(body)).encode("utf-8"))
    return p


def test_check_parses_like_the_engine_and_prints_the_digest_offline(tmp_path: Path) -> None:
    p = write(tmp_path, WINDOW)
    code, out, err = Harness().run("maintenance", "check", "--file", str(p), env={})   # no DSN: offline
    assert code == 0 and err == ""
    assert "outcome: ok" in out and "windows: 1" in out
    assert f"sha256: {hashlib.sha256(p.read_bytes()).hexdigest()}" in out
    assert "adapter=stonex start=2027-01-16T22:00:00.000Z end=2027-01-17T02:00:00.000Z" in out


def test_the_default_path_is_the_engines_variable(tmp_path: Path) -> None:
    p = write(tmp_path, WINDOW, "other.json")
    code, out, _ = Harness().run("maintenance", "check", env={MAINTENANCE_FILE_VAR: str(p)})
    assert code == 0 and f"file: {p}" in out


def test_a_file_the_engine_would_refuse_is_refused(tmp_path: Path) -> None:
    naive = {"stonex": [{"start": "2027-01-16T22:00:00", "end": "2027-01-17T02:00:00"}]}   # no offset
    for body in (naive, "{not json", {"stonex": {"start_ms": 1}}, {"stonex": [{"start_ms": 5, "end_ms": 4}]}):
        p = write(tmp_path, body)
        code, out, _ = Harness().run("maintenance", "check", "--file", str(p), env={})
        assert code == cli.EXIT_REFUSED and "refused: invalid" in out, body


def test_a_missing_file_declares_nothing_and_says_so(tmp_path: Path) -> None:
    code, out, _ = Harness().run("maintenance", "check", "--file", str(tmp_path / "absent.json"), env={})
    assert code == 0 and "exists: False" in out and "windows: 0" in out and "declares no maintenance window" in out


def test_ended_long_and_overlapping_windows_are_warned() -> None:
    import tempfile
    body = {"pepperstone": [{"start_ms": NOW - 7_200_000, "end_ms": NOW - 3_600_000},      # ended
                            {"start_ms": NOW - 60_000, "end_ms": NOW + 13 * 3_600_000},     # active, 13 h
                            {"start_ms": NOW + 3_600_000, "end_ms": NOW + 7_200_000}]}      # overlaps it
    with tempfile.TemporaryDirectory() as d:
        p = write(Path(d), body)
        rep = cli.check_maintenance_file(str(p), NOW)
    assert rep.outcome == "ok" and [r["state"] for r in rep.rows] == ["ended", "active", "future"]
    text = " | ".join(rep.warnings)
    assert "has already ended" in text and "longer than 12 h" in text and "overlap" in text


def test_a_window_inside_an_earlier_longer_one_is_an_overlap_even_after_a_short_one(tmp_path: Path) -> None:
    """The overlap check compared each window with the previous one only: C overlapping a long A was missed
    when a short B ended between them."""
    body = {"stonex": [{"start_ms": NOW + 0, "end_ms": NOW + 10 * 3_600_000},                 # A: 10 h
                       {"start_ms": NOW + 3_600_000, "end_ms": NOW + 2 * 3_600_000},          # B inside A
                       {"start_ms": NOW + 5 * 3_600_000, "end_ms": NOW + 6 * 3_600_000}]}     # C inside A, after B
    rep = cli.check_maintenance_file(str(write(tmp_path, body)), NOW - 60_000)
    overlaps = [w for w in rep.warnings if "overlap" in w]
    assert len(overlaps) == 2, rep.warnings
    assert any("2027-01-15T13:00:00" in w for w in overlaps)     # C's start


def test_a_file_replaced_while_it_is_checked_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The sha256 printed for the cross-node comparison must be of the bytes the engine's reader parsed."""
    from src.runtime.maintenance import FileMaintenanceSource
    p = write(tmp_path, WINDOW)
    real = FileMaintenanceSource._read

    def read_then_replace(self: FileMaintenanceSource) -> object:
        out = real(self)
        p.write_bytes(b"{}")                       # another copy lands between the hash and the parse
        return out
    monkeypatch.setattr(FileMaintenanceSource, "_read", read_then_replace)
    rep = cli.check_maintenance_file(str(p), NOW)
    assert rep.outcome == "refused" and rep.fields["refused"] == "changed"
    assert cli.exit_code(rep) == cli.EXIT_REFUSED
