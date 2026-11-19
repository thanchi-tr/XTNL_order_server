"""Self-signed test certificates through the openssl binary (Git for Windows ships one); skip without it."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


def find_openssl() -> str | None:
    for cand in (shutil.which("openssl"), r"C:\Program Files\Git\usr\bin\openssl.exe",
                 r"C:\Program Files\Git\mingw64\bin\openssl.exe"):
        if cand and Path(cand).exists():
            return cand
    return None


def make_cert(out: Path, name: str, *, days: int = 400, cn: str = "xtnl-test") -> tuple[Path, Path]:
    """(cert, key) valid for `days`, with IP SAN 127.0.0.1."""
    openssl = find_openssl()
    if openssl is None:
        pytest.skip("openssl not found (Git for Windows ships one)")
    crt, key = out / f"{name}.crt", out / f"{name}.key"
    subprocess.run([openssl, "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                    "-keyout", str(key), "-out", str(crt), "-days", str(days), "-subj", f"/CN={cn}",
                    "-addext", "subjectAltName=IP:127.0.0.1"], check=True, capture_output=True, timeout=60)
    return crt, key
