"""`python -m src.ops.md_acl`: the AWS `redis-md` ACL file rendered from the secrets source (design §A2, §C2; plan S37).

Synthetic passwords only; the tests check that the file holds `#<sha256>` entries and the design's rules, that a
refusal names variables only, and that nothing printed carries a password."""
from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path

import pytest

from src.ops import md_acl
from src.ops.md_acl import (
    HEALTH_RULES,
    READER_RULES,
    WRITER_RULES,
    AclError,
    MdPasswords,
    main,
    password_from_url,
    passwords,
    read_env_file,
    render_acl,
)
from src.secrets.source import SecretsError, SecretsUnavailable

W_PW = "synthetic-writer-pw-1"
R_PW = "synthetic-reader-pw-2"
GOOD = {"XTNL_MD_REDIS_URL": f"redis://md_writer:{W_PW}@127.0.0.1:6381/0",
        "XTNL_MD_REDIS_READER_URL": f"redis://md_reader:{R_PW}@127.0.0.1:6381/0"}


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def test_render_holds_hashes_and_the_design_rules_only() -> None:
    text = render_acl(passwords(GOOD))
    lines = text.splitlines()
    assert lines == ["user default off",
                     f"user md_writer on #{sha(W_PW)} {WRITER_RULES}",
                     f"user md_reader on #{sha(R_PW)} {READER_RULES}",
                     f"user md_health on {HEALTH_RULES}"]
    assert W_PW not in text and R_PW not in text and ">" not in text
    assert text.endswith("\n") and "\r" not in text
    for banned in ("+@all", "+@read", "+@write", "+@dangerous", "+eval ", "+keys", "+scan", "+flushdb", "+flushall",
                   "+config", "+acl"):
        assert banned not in f" {WRITER_RULES} {READER_RULES} {HEALTH_RULES} "
    assert "%R~md:*" in READER_RULES and "~md:*" in WRITER_RULES and "+waitaof" in WRITER_RULES


def test_password_is_unquoted_and_hidden_in_repr() -> None:
    assert password_from_url("X", "redis://md_writer:a%40b%3Ac@127.0.0.1:6381/0", "md_writer") == "a@b:c"
    assert "synthetic" not in repr(passwords(GOOD)) and repr(MdPasswords("a", "b")) == "MdPasswords(<hidden>)"


@pytest.mark.parametrize(("env", "needle"), [
    ({}, "XTNL_MD_REDIS_URL is not set"),
    ({**GOOD, "XTNL_MD_REDIS_URL": "http://md_writer:x@127.0.0.1:6381/0"}, "redis:// or rediss://"),
    ({**GOOD, "XTNL_MD_REDIS_URL": "redis://default:x@127.0.0.1:6381/0"}, "must name the ACL user md_writer"),
    ({**GOOD, "XTNL_MD_REDIS_READER_URL": "redis://md_writer:x@127.0.0.1:6381/0"}, "ACL user md_reader"),
    ({**GOOD, "XTNL_MD_REDIS_URL": "redis://md_writer@127.0.0.1:6381/0"}, "has no password"),
    ({**GOOD, "XTNL_MD_REDIS_URL": "redis://md_writer:x@127.0.0.1:6379/0"}, "must point at 127.0.0.1:6381"),
    ({**GOOD, "XTNL_MD_REDIS_URL": "redis://md_writer:x@10.0.0.5:6381/0"}, "must point at 127.0.0.1:6381"),
    ({**GOOD, "XTNL_MD_REDIS_READER_URL": f"redis://md_reader:{W_PW}@127.0.0.1:6381/0"}, "different passwords"),
])
def test_refusals_name_variables_never_values(env: dict[str, str], needle: str) -> None:
    with pytest.raises(AclError) as exc:
        passwords(env)
    assert needle in str(exc.value)
    for value in env.values():
        pw = value.split(":")[2].split("@")[0] if value.count(":") >= 2 else ""
        if pw:
            assert pw not in str(exc.value)


def test_any_port_accepts_a_non_default_install() -> None:
    env = {k: v.replace("127.0.0.1:6381", "127.0.0.1:56382") for k, v in GOOD.items()}
    with pytest.raises(AclError):
        passwords(env)
    assert passwords(env, expect_port=None).writer == W_PW


def test_env_file_reader(tmp_path: Path) -> None:
    f = tmp_path / "engine.env"
    f.write_text("# comment\n\nexport A=1\nB='two words'\nC=\"x=y\"\nnot a line\n", encoding="utf-8")
    assert read_env_file(f) == {"A": "1", "B": "two words", "C": "x=y"}


def test_main_writes_atomically_and_prints_no_secret(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    env_file = tmp_path / "engine.env"
    env_file.write_text("".join(f"{k}={v}\n" for k, v in GOOD.items()), encoding="utf-8")
    out = tmp_path / "run" / "users.acl"
    out.parent.mkdir()
    out.write_text("old\n", encoding="utf-8")
    assert main(["--env-file", str(env_file), "--out", str(out)], environ={}) == 0
    assert out.read_text(encoding="utf-8") == render_acl(MdPasswords(W_PW, R_PW))
    assert [p.name for p in out.parent.iterdir()] == ["users.acl"]          # no temporary file left
    printed = capsys.readouterr()
    assert W_PW not in printed.out + printed.err and "password hashes only" in printed.out


def test_main_refuses_with_exit_78_and_keeps_the_old_file(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = tmp_path / "users.acl"
    out.write_text("old\n", encoding="utf-8")
    env = {"XTNL_MD_REDIS_URL": f"redis://md_writer:{W_PW}@127.0.0.1:6379/0"}
    assert main(["--out", str(out)], environ=env) == md_acl.EXIT_CONFIG == 78
    assert out.read_text(encoding="utf-8") == "old\n"
    err = capsys.readouterr().err
    assert "refused" in err and W_PW not in err and "XTNL_MD_REDIS_READER_URL is not set" in err
    assert main(["--env-file", str(tmp_path / "missing.env"), "--out", str(out)], environ={}) == 78
    # an unknown secrets source is a configuration error too (names only)
    assert main(["--out", str(out)], environ={**GOOD, "XTNL_SECRETS_SOURCE": "vault"}) == 78
    assert "XTNL_SECRETS_SOURCE" in capsys.readouterr().err


class _FailingSource:
    """A secrets source whose read fails with `exc` (an AWS / network error at boot)."""

    def __init__(self, exc: Exception) -> None:
        self.exc, self.closed = exc, False

    def resolve(self, base: Mapping[str, str]) -> dict[str, str]:
        raise self.exc

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize("exc", [SecretsUnavailable("cannot read the secret: secretsmanager GetSecretValue failed: "
                                                    "HTTP 503 ServiceUnavailable"),
                                 TimeoutError(), OSError("network unreachable"), RuntimeError("boom")])
def test_a_secrets_source_read_failure_is_a_temporary_failure_not_78(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], exc: Exception) -> None:
    """S37 review: a passing outage at boot is retried by the unit (75), never reported as a configuration error."""
    src = _FailingSource(exc)
    monkeypatch.setattr(md_acl, "secrets_source_from_env", lambda env: src)
    out = tmp_path / "users.acl"
    out.write_text("old\n", encoding="utf-8")
    code = main(["--out", str(out)], environ=GOOD)
    assert code == md_acl.EXIT_TEMPFAIL == 75 and code != md_acl.EXIT_CONFIG
    assert src.closed and out.read_text(encoding="utf-8") == "old\n"
    err = capsys.readouterr().err
    assert "retry" in err and W_PW not in err and R_PW not in err
    # a configuration error from the same source stays 78
    monkeypatch.setattr(md_acl, "secrets_source_from_env", lambda env: _FailingSource(SecretsError("XTNL_X bad")))
    assert main(["--out", str(out)], environ=GOOD) == md_acl.EXIT_CONFIG


def test_env_values_win_over_the_process_environment(tmp_path: Path) -> None:
    env_file = tmp_path / "engine.env"
    env_file.write_text("".join(f"{k}={v}\n" for k, v in GOOD.items()), encoding="utf-8")
    base = {"XTNL_MD_REDIS_URL": "redis://md_writer:stale@127.0.0.1:6381/0"}
    assert md_acl.resolved_env(env_file, base)["XTNL_MD_REDIS_URL"] == GOOD["XTNL_MD_REDIS_URL"]
