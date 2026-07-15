import os
import subprocess
import sys

import pytest


HIPPIUS_CLI = [sys.executable, "-m", "hippius_hub.cli"]


def test_cli_login_writes_token(tmp_path, monkeypatch):
    from hippius_hub import auth, cli

    token_file = tmp_path / "token"
    monkeypatch.setattr(auth, "TOKEN_PATH", str(token_file))
    monkeypatch.setattr(sys, "argv", ["hippius-hub", "login", "--token", "fake"])

    cli.main()

    assert token_file.read_text() == "Bearer fake"

    # Audit C3 pin: the token file must be 0o600 (owner-read/write only).
    # The chmod happens inside auth.login (auth.py:106-109) after the write.
    # On Windows os.chmod is a no-op and the test would always fail; skip
    # rather than yield a false negative — Windows users are off-policy
    # for the CLI anyway (see auth.py comment).
    if sys.platform.startswith("win"):
        pytest.skip("chmod 0o600 is a POSIX-only invariant; Windows has no equivalent.")
    mode = os.stat(token_file).st_mode & 0o777
    assert mode == 0o600, (
        f"token file permissions must be 0o600, got 0o{mode:o}. "
        f"World-readable token = anyone on the same box (CI runner, shared "
        f"laptop) can pull your bearer JWT out of ~/.cache/hippius/hub/token."
    )


def _delete_args(**over):
    import argparse
    ns = argparse.Namespace(repo_id="acme/model", repo_type=None,
                            yes=False, missing_ok=False)
    for k, v in over.items():
        setattr(ns, k, v)
    return ns


def test_cli_delete_aborts_without_confirmation(monkeypatch, capsys):
    """A bare `delete` without -y must NOT call delete_repo unless the user
    types 'y' — a typo'd repo_id would otherwise wipe the wrong model."""
    from hippius_hub import _repo_ops, cli

    called = []
    monkeypatch.setattr(_repo_ops, "delete_repo", lambda *a, **k: called.append((a, k)))
    monkeypatch.setattr("builtins.input", lambda _prompt: "n")

    cli._cmd_delete(_delete_args())

    assert called == [], "delete_repo must not run when the user declines"
    assert "Aborted." in capsys.readouterr().out


def test_cli_delete_proceeds_on_yes_confirmation(monkeypatch):
    from hippius_hub import _repo_ops, cli

    called = []
    monkeypatch.setattr(_repo_ops, "delete_repo", lambda *a, **k: called.append((a, k)))
    monkeypatch.setattr("builtins.input", lambda _prompt: "y")

    cli._cmd_delete(_delete_args())

    assert called == [(("acme/model",), {"repo_type": None, "missing_ok": False})]


def test_cli_delete_yes_flag_skips_prompt(monkeypatch):
    """-y is the CI/non-interactive path: it must never touch stdin."""
    from hippius_hub import _repo_ops, cli

    called = []
    monkeypatch.setattr(_repo_ops, "delete_repo", lambda *a, **k: called.append((a, k)))

    def _boom(_prompt):
        raise AssertionError("--yes must not prompt")

    monkeypatch.setattr("builtins.input", _boom)

    cli._cmd_delete(_delete_args(yes=True))

    assert len(called) == 1


def test_cli_delete_noninteractive_stdin_aborts(monkeypatch, capsys):
    """Piped stdin with no --yes raises EOFError on input(); that is treated
    as 'no' (safe default), not a crash."""
    from hippius_hub import _repo_ops, cli

    called = []
    monkeypatch.setattr(_repo_ops, "delete_repo", lambda *a, **k: called.append((a, k)))

    def _eof(_prompt):
        raise EOFError

    monkeypatch.setattr("builtins.input", _eof)

    cli._cmd_delete(_delete_args())

    assert called == []
    assert "Aborted." in capsys.readouterr().out


@pytest.mark.e2e
def test_cli_download_smoke(tmp_path, test_repo, creds, revision):
    src = tmp_path / "cli.bin"
    src.write_bytes(b"hippius-cli-smoke-test\n" * 32)

    env = {**os.environ, "HOME": str(tmp_path)}
    (tmp_path / ".cache" / "hippius" / "hub").mkdir(parents=True)

    if creds.get("user") and creds.get("password"):
        login_cmd = HIPPIUS_CLI + ["login", "--username", creds["user"], "--password", creds["password"]]
    else:
        login_cmd = HIPPIUS_CLI + ["login", "--token", creds["token"]]
    subprocess.run(login_cmd, env=env, check=True, capture_output=True)

    subprocess.run(
        HIPPIUS_CLI + ["upload", test_repo, str(src), "--revision", revision],
        env=env, check=True, capture_output=True,
    )

    target_cache = tmp_path / "cli_cache"
    result = subprocess.run(
        HIPPIUS_CLI + [
            "download", test_repo, "cli.bin",
            "--revision", revision,
            "--cache-dir", str(target_cache),
        ],
        env=env, check=True, capture_output=True,
    )

    out = result.stdout.decode()
    assert "downloaded to" in out.lower()
    expected_file = target_cache / f"models--{test_repo.replace('/', '--')}" / "snapshots" / revision / "cli.bin"
    assert expected_file.is_file()
