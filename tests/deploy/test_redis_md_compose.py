"""The dev `redis-md` service (plan S13, design §A2): compose profile `md` (a plain `up` is unchanged), AOF on,
`noeviction`, loopback port, and exactly the design's ACL users. Static: no Docker needed."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "deploy" / "docker-compose.dev.yml"

WRITER_RULES = ("resetkeys ~md:* resetchannels -@all +@string +@hash +@sortedset +@stream +del +unlink +exists "
                "+pexpire +evalsha +script|load +info +waitaof +ping +select +client|setinfo")
READER_RULES = ("resetkeys %R~md:* resetchannels -@all +get +hgetall +zrange +zrangebyscore +xrange +xrevrange "
                "+xread +exists +ping +select +client|setinfo")
HEALTH_RULES = "nopass resetkeys resetchannels -@all +ping"


def _service(text: str, name: str) -> str:
    m = re.search(rf"^  {re.escape(name)}:\n((?:    .*\n|\n)+?)(?=^  \S|\Z)", text, re.M)
    assert m, f"service {name} missing"
    return m.group(1)


def _command(block: str) -> list[str]:
    items = re.findall(r"^      - (.*)$", block.split("    command:\n", 1)[1].split("\n    ports:", 1)[0], re.M)
    return [i.strip().strip('"') for i in items]


def _users(cmd: list[str]) -> dict[str, list[str]]:
    users: dict[str, list[str]] = {}
    current: list[str] | None = None
    for arg in cmd:
        if arg == "--user":
            current = []
            continue
        if arg.startswith("--"):
            current = None
            continue
        if current is not None:
            if not current:
                users[arg] = current
            current.append(arg)
    return {u: rules[1:] for u, rules in users.items()}


def test_redis_md_is_in_profile_md_with_aof_and_the_design_acl() -> None:
    text = COMPOSE.read_text(encoding="utf-8")
    block = _service(text, "redis-md")
    assert re.search(r'^    profiles: \["md"\]$', block, re.M), "redis-md must be in profile md only"
    for other in ("postgres", "redis", "etcd"):
        assert "profiles:" not in _service(text, other), f"{other} must stay in the plain up"
    cmd = _command(block)
    joined = " ".join(cmd)
    for setting in ("--appendonly yes", "--appendfsync everysec", "--aof-load-truncated yes",
                    "--maxmemory-policy noeviction"):
        assert setting in joined
    assert re.search(r'"127\.0\.0\.1:\d+:6379"', block), "loopback port only"
    assert "md_health" in block.split("healthcheck:", 1)[1]
    users = _users(cmd)
    assert users["default"] == ["off"]
    writer = users["md_writer"]
    assert writer[0] == "on" and writer[1].startswith(">") and "dev_only" in writer[1]
    assert " ".join(writer[2:]) == WRITER_RULES
    reader = users["md_reader"]
    assert reader[0] == "on" and "dev_only" in reader[1] and " ".join(reader[2:]) == READER_RULES
    assert users["md_health"][0] == "on" and " ".join(users["md_health"][1:]) == HEALTH_RULES
    for banned in ("+@all", "+@read", "+@write", "+@dangerous", "+eval ", "+keys", "+scan", "+flushdb", "+flushall",
                   "+config", "+acl", "+multi", "+exec"):
        assert banned not in f" {' '.join(writer + reader)} ", banned


def test_the_header_documents_the_md_test_variable() -> None:
    header = COMPOSE.read_text(encoding="utf-8").split("services:", 1)[0]
    assert "--profile md" in header and "ORDER_SERVER_TEST_REDIS_MD" in header
    assert "ORDER_SERVER_TEST_REDIS_MD" in (ROOT / "tests" / "marketdata" / "test_acl_live.py").read_text("utf-8")
