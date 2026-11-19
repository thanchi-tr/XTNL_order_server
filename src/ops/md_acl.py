"""Render the `redis-md` ACL file on an AWS engine node at boot (design §A2 ACL, §C2 Secrets; plan S37).

    python -m src.ops.md_acl --env-file /etc/xtnl/engine.env --out /run/xtnl-redis-md/users.acl --group redis

Run as `ExecStartPre=+` of `deploy/systemd/xtnl-redis-md.service` (root, before `redis-server` reads `aclfile`). The
two `redis-md` passwords come from the same place the engine reads them: the env file plus, when it says
`XTNL_SECRETS_SOURCE=aws-sm`, the Secrets Manager secret laid over it (`src.secrets.source`). Each password is taken
from its URL (`XTNL_MD_REDIS_URL` = `md_writer`, `XTNL_MD_REDIS_READER_URL` = `md_reader`), so the store and the engine
can never disagree, and only its SHA-256 is written (`#<hex>` entries, never a `>password`). `md_health` is `nopass`
with `+ping` only (the health check needs no secret); the default user is off.

The rules are the design's allow-lists, the same as the dev compose service (`deploy/docker-compose.dev.yml`) and the
PC's `deploy/local/new-md-credentials.ps1` (the deploy tests keep the three equal). The file is written to a temporary
file in the target directory and renamed over the old one (0640, group `--group`), so `redis-server` never reads half
a file. Nothing is printed but the path and the user names; a problem names the variable only, never a value. Exit
78 (EX_CONFIG) is a configuration error (a URL missing or wrong, an unknown source); exit 75 (EX_TEMPFAIL) is a secrets
source that could not be read just now (AWS / network at boot), which the unit retries. The ARM
and market-data services stay off unless `XTNL_MD_ENABLED=1`: this only prepares the store the operator enabled.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import os
import shutil
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from urllib.parse import unquote, urlsplit

from src.marketdata.config import READER_URL_VAR, REDIS_URL_VAR
from src.secrets.source import SecretsError, SecretsUnavailable, secrets_source_from_env

WRITER_USER: Final = "md_writer"
READER_USER: Final = "md_reader"
HEALTH_USER: Final = "md_health"
#: Design §A2: the data commands are listed because Redis also checks each command a script runs; INFO is
#: @dangerous, so it is granted by name. Never +@all, +@read (KEYS / SCAN), FLUSH*, CONFIG, ACL or EVAL.
WRITER_RULES: Final = ("resetkeys ~md:* resetchannels -@all +@string +@hash +@sortedset +@stream +del +unlink +exists "
                       "+pexpire +evalsha +script|load +info +waitaof +ping +select +client|setinfo")
#: Read-only by key (%R~) and by command; it writes nothing, not even acknowledgements (§A13).
READER_RULES: Final = ("resetkeys %R~md:* resetchannels -@all +get +hgetall +zrange +zrangebyscore +xrange +xrevrange "
                       "+xread +exists +ping +select +client|setinfo")
HEALTH_RULES: Final = "nopass resetkeys resetchannels -@all +ping"
#: Where `redis-md` listens on an AWS node (design §A2: loopback only; remote readers come through the TLS proxy).
AWS_HOST: Final = "127.0.0.1"
AWS_PORT: Final = 6381
EXIT_CONFIG: Final = 78     # EX_CONFIG: a missing or wrong URL / env file / source; a retry cannot fix it
EXIT_TEMPFAIL: Final = 75   # EX_TEMPFAIL: the secrets source could not be read now (AWS / network); the unit retries


class AclError(ValueError):
    """The ACL cannot be rendered (names only, never a value)."""


class SourceUnavailable(RuntimeError):
    """The secrets source could not be read just now (an AWS / network error); a retry may succeed."""


@dataclass(frozen=True)
class MdPasswords:
    writer: str
    reader: str

    def __repr__(self) -> str:
        return "MdPasswords(<hidden>)"


def sha256_hex(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def password_from_url(name: str, url: str | None, user: str, *, expect_port: int | None = AWS_PORT) -> str:
    """The password of `url` for ACL user `user`. Refuses (naming `name` only) a missing URL, another scheme or user,
    an empty password, or (with `expect_port`) a host / port other than the node's loopback `redis-md`."""
    if not url or not url.strip():
        raise AclError(f"{name} is not set")
    try:
        parts = urlsplit(url.strip())
        port = parts.port
    except ValueError:
        raise AclError(f"{name} is not a redis URL") from None
    if parts.scheme not in ("redis", "rediss"):
        raise AclError(f"{name} must use redis:// or rediss://")
    if unquote(parts.username or "") != user:
        raise AclError(f"{name} must name the ACL user {user}")
    password = unquote(parts.password or "")
    if not password:
        raise AclError(f"{name} has no password")
    if any(ch.isspace() for ch in password):
        raise AclError(f"{name} has a password with whitespace")
    if expect_port is not None and (parts.hostname != AWS_HOST or port != expect_port):
        raise AclError(f"{name} must point at {AWS_HOST}:{expect_port} (redis-md on this node)")
    return password


def passwords(env: Mapping[str, str], *, expect_port: int | None = AWS_PORT) -> MdPasswords:
    problems: list[str] = []
    found: dict[str, str] = {}
    for var, user in ((REDIS_URL_VAR, WRITER_USER), (READER_URL_VAR, READER_USER)):
        try:
            found[user] = password_from_url(var, env.get(var), user, expect_port=expect_port)
        except AclError as exc:
            problems.append(str(exc))
    if problems:
        raise AclError("; ".join(problems))
    if found[WRITER_USER] == found[READER_USER]:
        raise AclError(f"{REDIS_URL_VAR} and {READER_URL_VAR} must carry different passwords")
    return MdPasswords(found[WRITER_USER], found[READER_USER])


def render_acl(pw: MdPasswords) -> str:
    """The ACL file text: password hashes only, LF line ends."""
    return (f"user default off\n"
            f"user {WRITER_USER} on #{sha256_hex(pw.writer)} {WRITER_RULES}\n"
            f"user {READER_USER} on #{sha256_hex(pw.reader)} {READER_RULES}\n"
            f"user {HEALTH_USER} on {HEALTH_RULES}\n")


def read_env_file(path: Path) -> dict[str, str]:
    """`KEY=VALUE` lines of a systemd EnvironmentFile (comments, blank lines and `export ` skipped; one level of
    matching quotes removed). Values are never logged."""
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith(("#", ";")):
            continue
        line = line.removeprefix("export ").strip()
        key, sep, value = line.partition("=")
        if not sep:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        out[key.strip()] = value
    return out


def write_atomically(out: Path, text: str, *, group: str | None) -> None:
    """Write `text` next to `out` and rename it over `out` (0640; group `group` when given)."""
    out.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".users-", suffix=".acl", dir=out.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp, 0o640)
        if group is not None:
            shutil.chown(tmp, group=group)
        os.replace(tmp, out)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def resolved_env(env_file: Path | None, base: Mapping[str, str]) -> dict[str, str]:
    """`base` overlaid with the env file, then with the secrets source the result selects."""
    env = dict(base)
    if env_file is not None:
        try:
            env.update(read_env_file(env_file))
        except (OSError, UnicodeDecodeError) as exc:
            raise AclError(f"the env file could not be read ({type(exc).__name__})") from None
    source = secrets_source_from_env(env)
    try:
        return source.resolve(env)
    except SecretsUnavailable as exc:
        raise SourceUnavailable(str(exc)) from None
    except SecretsError:
        raise
    except Exception as exc:     # an AWS / network error: the type only, never a message that could carry a value
        raise SourceUnavailable(f"the secrets source could not be read ({type(exc).__name__})") from None
    finally:
        source.close()


def main(argv: Sequence[str] | None = None, *, environ: Mapping[str, str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m src.ops.md_acl",
                                 description="render the redis-md ACL file (password hashes only) from the secrets source")
    ap.add_argument("--env-file", type=Path, default=None, help="systemd EnvironmentFile to read (e.g. /etc/xtnl/engine.env)")
    ap.add_argument("--out", type=Path, required=True, help="ACL file to write (the aclfile of redis-md.conf)")
    ap.add_argument("--group", default=None, help="group that may read the file (redis)")
    ap.add_argument("--any-port", action="store_true",
                    help="accept URLs that do not point at 127.0.0.1:6381 (tests and non-default installs)")
    ns = ap.parse_args(argv)
    try:
        env = resolved_env(ns.env_file, os.environ if environ is None else environ)
        pw = passwords(env, expect_port=None if ns.any_port else AWS_PORT)
        write_atomically(ns.out, render_acl(pw), group=ns.group)
    except SourceUnavailable as exc:
        print(f"md_acl: not rendered, retry: {exc}", file=sys.stderr)
        return EXIT_TEMPFAIL
    except (AclError, SecretsError) as exc:
        print(f"md_acl: refused: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    except OSError as exc:
        print(f"md_acl: cannot write {ns.out} ({type(exc).__name__})", file=sys.stderr)
        return 1
    print(f"md_acl: wrote {ns.out} (users default, {WRITER_USER}, {READER_USER}, {HEALTH_USER}; password hashes only)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
