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
