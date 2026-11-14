"""`python -m src.ops` end to end with injected PostgreSQL / etcd fakes: parsing, exit codes, output, and
that no secret is ever printed."""
from __future__ import annotations

import io
import re
from collections.abc import Sequence
from pathlib import Path

import pytest

from src.db.pg import PgConnection
from src.ops import cli
from src.ops.etcd import OpsEtcd, OpsEtcdClient, endpoints_from_env

from tests.ops.conftest import FakeEtcd
from tests.ops.emulator import OpsPg

SECRET = "pa55-Wo3d-xyz"
ENV = {"ORDER_SERVER_DSN": f"postgresql://xtnl_operator:{SECRET}@10.0.1.10:5432,10.0.2.10:5432/xtnl",
       "XTNL_ETCD_ENDPOINTS": "https://10.0.1.10:2379, https://10.0.2.10:2379,https://10.0.3.10:2379"}


class Harness:
    def __init__(self) -> None:
        self.pg = OpsPg()
        self.etcd = FakeEtcd()
        self.closed = 0
        self.endpoints: Sequence[str] = ()

    async def connect(self, dsn: str) -> tuple[PgConnection, cli.Closer]:
        assert dsn == ENV["ORDER_SERVER_DSN"]

        async def close() -> None:
            self.closed += 1

        return self.pg, close

    def etcd_factory(self, endpoints: Sequence[str]) -> OpsEtcd:
        self.endpoints = endpoints
        return self.etcd

    def run(self, *argv: str, env: dict[str, str] | None = None) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        code = cli.main(list(argv), environ=ENV if env is None else env, connect=self.connect,
                        etcd_factory=self.etcd_factory, out=out, err=err)
        return code, out.getvalue(), err.getvalue()


@pytest.fixture
def h() -> Harness:
    return Harness()


def test_resume_end_to_end(h: Harness) -> None:
    h.pg.o.cluster.update(safe_hold=True, since=1, reason="flap")
    code, out, err = h.run("safe-hold", "resume", "--operator", "alice", "--approver", "bob",
                           "--reason", "INC-42 fixed etcd latency")
    assert code == cli.EXIT_OK, err
    assert "outcome: applied" in out and "etcd_mirror: written" in out and re.search(r"audit_id: \d+", out)
    assert h.closed == 1 and h.etcd.closed and len(h.endpoints) == 3
    assert SECRET not in out + err


def test_exit_codes(h: Harness) -> None:
    # usage: missing --approver (two-person) / missing --reason
    code, _, err = h.run("safe-hold", "resume", "--operator", "alice", "--reason", "INC-42 fixed latency")
    assert code == cli.EXIT_USAGE and "--approver" in err
    code, _, _ = h.run("dlq", "requeue", "5", "--operator", "alice")
    assert code == cli.EXIT_USAGE
    code, _, err = h.run("dlq", "requeue", "5", "--operator", "alice", "--reason", "short")
    assert code == cli.EXIT_USAGE and "reason" in err
    code, _, err = h.run("halt", "clear", "ACC", "--reviewed-event-id", "3", "--force", "--operator", "alice",
                         "--reason", "INC-9 stale HARD input")
    assert code == cli.EXIT_USAGE and "two-person" in err
    assert h.pg.o.audit == []                       # usage errors write nothing
    # refused: audited, exit 3
    code, out, _ = h.run("safe-hold", "resume", "--operator", "alice", "--approver", "bob", "--reason",
                         "INC-42 fixed latency")
    assert code == cli.EXIT_REFUSED and "refused: not_in_safe_hold" in out and len(h.pg.o.audit) == 1
    # degraded: applied but the mirror failed, exit 4
    h.pg.o.cluster.update(safe_hold=True)
    h.etcd.fail_put = True
    code, out, _ = h.run("safe-hold", "resume", "--operator", "alice", "--approver", "bob", "--reason",
                         "INC-42 fixed latency")
    assert code == cli.EXIT_DEGRADED and "outcome: applied" in out and "warning: etcd mirror not written" in out


def test_reads_print_tables(h: Harness) -> None:
    h.pg.seed_dlq(5, "BACKOFF", payload="RAW-PAYLOAD-NEVER-PRINTED")
    code, out, _ = h.run("dlq", "list")
    assert code == 0 and "[dlq] 1 row(s)" in out and "dlq_id=5" in out and "RAW-PAYLOAD" not in out
    code, out, _ = h.run("epoch", "show")
    assert code == 0 and "cluster_id: cluster-a" in out and "etcd_cluster_id: cluster-a" in out
    code, out, _ = h.run("halt", "status")
    assert code == 0 and "effective_level: NONE" in out
    code, out, _ = h.run("audit", "tail", "--limit", "5")
    assert code == 0 and "command: audit.tail" in out
    code, _, err = h.run("audit", "tail", "--limit", "0")
    assert code == cli.EXIT_USAGE


def test_config_and_connection_errors_never_print_the_password(h: Harness) -> None:
    code, _, err = h.run("epoch", "show", env={})
    assert code == cli.EXIT_USAGE and "ORDER_SERVER_DSN" in err

    async def refuse(dsn: str) -> tuple[PgConnection, cli.Closer]:
        raise OSError(f"could not connect to {dsn}")   # a driver message that echoes the DSN

    out, err = io.StringIO(), io.StringIO()
    code = cli.main(["epoch", "show"], environ=ENV, connect=refuse, etcd_factory=h.etcd_factory, out=out, err=err)
    assert code == cli.EXIT_ERROR
    assert SECRET not in out.getvalue() + err.getvalue()
    assert "postgresql://xtnl_operator:***@10.0.1.10:5432,10.0.2.10:5432/xtnl" in err.getvalue()


def test_unexpected_errors_print_only_their_type(h: Harness) -> None:
    h.pg.down = True
    code, out, err = h.run("dlq", "list")
    assert code == cli.EXIT_ERROR and err.strip() == "error: ConnectionRefusedError" and out == ""


def test_reviewed_event_id_accepts_none(h: Harness) -> None:
    h.pg.o.accounts.add("ACC")
    code, out, _ = h.run("halt", "clear", "ACC", "--reviewed-event-id", "none", "--operator", "alice",
                         "--reason", "INC-9 nothing recorded yet")
    assert code == 0 and "outcome: applied" in out   # first clear with no halt_events writes the NONE row
    assert h.pg.t.halt_events[-1][6] == "alice"


def test_etcd_endpoints_and_client() -> None:
    assert endpoints_from_env(ENV) == ("https://10.0.1.10:2379", "https://10.0.2.10:2379", "https://10.0.3.10:2379")
    assert endpoints_from_env({}) == ()
    assert cli.make_etcd(()) is None
    c = cli.make_etcd(["http://127.0.0.1:1"])   # constructing connects nothing
    assert isinstance(c, OpsEtcdClient)
    c.close()


def test_main_module_and_package_have_no_order_capability() -> None:
    """Phase 1 boundary: the operator CLI never imports an adapter or defines an order-changing method."""
    root = Path(cli.__file__).parent
    forbidden = re.compile(r"\bdef\s+(place_order|modify_order|cancel_order|submit_order|close_position|"
                           r"amend_order|update_order)\b|src\.adapters|outbound_intent")
    for f in root.glob("*.py"):
        assert not forbidden.search(f.read_text(encoding="utf-8")), f.name
