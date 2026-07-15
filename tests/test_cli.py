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


@pytest.mark.e2e
def test_cli_delete_smoke(tmp_path, test_repo, creds):
    """End-to-end for the `delete` CLI: create a disposable repo (via upload),
    delete it with `-y`, then prove it's gone by a second delete returning the
    not-found exit code (11). Skips when the credential is push-only (delete
    hits Harbor's admin API and 403s → exit 14), matching the admin-perm
    handling in test_repo_lifecycle. Uses a uuid-suffixed repo under the
    existing test namespace so concurrent CI runs never collide.

    Note: on a push-only credential the upload succeeds but the delete (and the
    finally-cleanup) 403 and skip, so the uploaded stub repo is left behind —
    inherent to testing delete (you must create something first) and consistent
    with the push-only handling already accepted in test_phase_b's delete e2e."""
    import uuid

    repo = f"{test_repo.split('/')[0]}/cli-del-{uuid.uuid4().hex[:8]}"
    src = tmp_path / "d.bin"
    src.write_bytes(b"hippius-cli-delete-smoke\n" * 16)

    env = {**os.environ, "HOME": str(tmp_path)}
    (tmp_path / ".cache" / "hippius" / "hub").mkdir(parents=True)

    if creds.get("user") and creds.get("password"):
        login_cmd = HIPPIUS_CLI + ["login", "--username", creds["user"], "--password", creds["password"]]
    else:
        login_cmd = HIPPIUS_CLI + ["login", "--token", creds["token"]]
    subprocess.run(login_cmd, env=env, check=True, capture_output=True)

    # Upload materializes the repo under the (already-provisioned) namespace.
    subprocess.run(
        HIPPIUS_CLI + ["upload", repo, str(src)],
        env=env, check=True, capture_output=True,
    )

    try:
        deleted = subprocess.run(
            HIPPIUS_CLI + ["delete", repo, "-y"],
            env=env, capture_output=True, text=True,
        )
        if deleted.returncode == 14:
            pytest.skip(
                "delete requires Harbor project_admin perms; credential is "
                "push-only (mapped to access-denied, exit 14)."
            )
        assert deleted.returncode == 0, (
            f"delete exited {deleted.returncode}\n{deleted.stdout}\n{deleted.stderr}"
        )
        assert "Deleted repository" in deleted.stdout

        # Proof of removal: a second delete (no --missing-ok) 404s, which the
        # CLI maps to the repository-not-found exit code (11).
        gone = subprocess.run(
            HIPPIUS_CLI + ["delete", repo, "-y"],
            env=env, capture_output=True, text=True,
        )
        assert gone.returncode == 11, (
            f"second delete should report not-found (11), got {gone.returncode}\n{gone.stdout}"
        )
    finally:
        subprocess.run(
            HIPPIUS_CLI + ["delete", repo, "-y", "--missing-ok"],
            env=env, capture_output=True,
        )
