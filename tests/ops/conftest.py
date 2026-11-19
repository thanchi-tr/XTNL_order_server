"""Fixtures for the operator CLI tests: the PostgreSQL emulator, a fake etcd and a frozen clock.
No network: etcd and PostgreSQL are in-memory fakes."""
from __future__ import annotations

from collections.abc import Sequence

import pytest

from src.ops.service import OpsService
from src.runtime.etcd import KeyValue

from tests.ops.emulator import NOW, OpsPg


class FakeEtcd:
    def __init__(self, cluster_id: str = "cluster-a", revision: int = 900) -> None:
        self.kv: dict[str, str] = {}
        self.cluster_id = cluster_id
        self.revision = revision
        self.fail_put = False
        self.fail_all = False
        self.puts: list[tuple[str, str]] = []
        self.closed = False

    def _check(self) -> None:
        if self.fail_all:
            raise ConnectionError("etcd down")

    def get(self, key: str, *, timeout_s: float) -> KeyValue | None:
        self._check()
        v = self.kv.get(key)
        return None if v is None else KeyValue(key, v, 1, 1, 0)

    def put(self, key: str, value: str, *, lease_id: int = 0, timeout_s: float) -> int:
        self._check()
        if self.fail_put:
            raise ConnectionError("etcd put failed")
        self.kv[key] = value
        self.puts.append((key, value))
        self.revision += 1
        return self.revision

    def get_prefix(self, prefix: str, *, timeout_s: float) -> list[KeyValue]:
        self._check()
        return [KeyValue(k, v, 1, 1, 0) for k, v in sorted(self.kv.items()) if k.startswith(prefix)]

    def header(self, *, timeout_s: float) -> tuple[str, int]:
        self._check()
        return self.cluster_id, self.revision

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def pg() -> OpsPg:
    return OpsPg()


@pytest.fixture
def etcd() -> FakeEtcd:
    return FakeEtcd()


@pytest.fixture
def svc(pg: OpsPg, etcd: FakeEtcd) -> OpsService:
    return OpsService(pg, etcd, clock_ms=lambda: NOW)


def outcomes(pg: OpsPg) -> Sequence[tuple[str, str]]:
    return [(r["command"], r["outcome"]) for r in pg.o.audit]
