"""Commands that mint a registry secret print it alone on stdout, once.

`registry provision`, `rotate-token`, `keys create` and `keys rotate` used to
print the secret beside a ready `docker login ... -p '<secret>'` line, which
lands in shell history and agent transcripts. Now stdout holds the secret and
nothing else (so `cmd > key.txt` captures exactly it), every other line goes to
stderr, and every suggested login reads the secret from stdin.

Console calls, `login()` and `docker` are stubbed: the handlers' output is what
these tests pin.
"""
from __future__ import annotations

import argparse
import io
import sys

import pytest

from hippius_hub import cli

SECRET = "s3cr3t-Value_42"
LOGIN = "robot$myns+agent-push"
HOST = "registry.hippius.com"


@pytest.fixture
def saved_logins(monkeypatch):
    """Record `login()` calls instead of writing the token cache."""
    calls = []
    monkeypatch.setattr(cli, "login", lambda **kw: calls.append(kw))
    monkeypatch.setattr(cli.shutil, "which", lambda _name: None)
    return calls


def _assert_secret_alone_on_stdout(captured):
    assert captured.out == SECRET + "\n"
    assert SECRET not in captured.err
    for line in captured.err.splitlines():
        if "docker login " in line or "hippius-hub login " in line:
            assert "--password-stdin" in line, line
            assert " -p " not in line and "--password " not in line, line


def test_keys_create_prints_secret_once_and_stdin_hints(monkeypatch, capsys, saved_logins):
    monkeypatch.setattr(cli.console, "create_key", lambda name, role, expires_days: {
        "name": name, "role": role, "login": LOGIN, "secret": SECRET,
        "docker_login_cmd": f"docker login {HOST} -u '{LOGIN}' -p '{SECRET}'",
    })
    args = argparse.Namespace(name="agent-push", role="push-delete", expires_days=1,
                              docker_login=False)

    cli.cmd_registry_keys_create(args)

    captured = capsys.readouterr()
    _assert_secret_alone_on_stdout(captured)
    assert f"docker login {HOST} -u '{LOGIN}' --password-stdin" in captured.err
    assert f"hippius-hub login --username '{LOGIN}' --password-stdin" in captured.err
    assert saved_logins == []


def test_keys_create_docker_login_saves_creds_without_printing_secret_twice(
    monkeypatch, capsys, saved_logins
):
    monkeypatch.setattr(cli.console, "create_key", lambda name, role, expires_days: {
        "name": name, "role": role, "login": LOGIN, "secret": SECRET,
        "docker_login_cmd": f"docker login {HOST} -u '{LOGIN}' -p '{SECRET}'",
    })
    args = argparse.Namespace(name="agent-push", role="push", expires_days=None,
                              docker_login=True)

    cli.cmd_registry_keys_create(args)

    _assert_secret_alone_on_stdout(capsys.readouterr())
    assert saved_logins == [{"username": LOGIN, "password": SECRET}]


def test_keys_rotate_prints_secret_once(monkeypatch, capsys):
    monkeypatch.setattr(cli.console, "rotate_key", lambda key_id: {
        "id": key_id, "name": "agent-push", "role": "push", "login": LOGIN, "secret": SECRET,
    })

    cli.cmd_registry_keys_rotate(argparse.Namespace(key_id=7))

    captured = capsys.readouterr()
    _assert_secret_alone_on_stdout(captured)
    assert f"hippius-hub login --username '{LOGIN}' --password-stdin" in captured.err


def test_rotate_token_prints_secret_once(monkeypatch, capsys, saved_logins):
    monkeypatch.setattr(cli.console, "rotate_robot",
                        lambda: {"robot_login": LOGIN, "robot_secret": SECRET})
    monkeypatch.setattr(cli.console, "me", lambda: {"registry_url": f"https://{HOST}"})

    cli.cmd_registry_rotate(argparse.Namespace(docker_login=False))

    captured = capsys.readouterr()
    _assert_secret_alone_on_stdout(captured)
    assert f"docker login {HOST} -u '{LOGIN}' --password-stdin" in captured.err
    assert saved_logins == [{"username": LOGIN, "password": SECRET}]


def test_provision_prints_secret_once(monkeypatch, capsys, saved_logins):
    monkeypatch.setattr(cli.console, "provision", lambda ns: {
        "project_name": ns, "plan_name": "free", "storage_quota_bytes": 1024,
        "robot_login": LOGIN, "robot_secret": SECRET, "registry_url": f"https://{HOST}",
    })

    cli.cmd_registry_provision(argparse.Namespace(namespace="myns", docker_login=True))

    captured = capsys.readouterr()
    _assert_secret_alone_on_stdout(captured)
    assert "Created 'myns'" in captured.err
    assert f"docker login {HOST} -u '{LOGIN}' --password-stdin" in captured.err


def test_provision_without_secret_leaves_stdout_empty(monkeypatch, capsys):
    monkeypatch.setattr(cli.console, "provision", lambda ns: {
        "idempotent": True, "project_name": ns, "plan_name": "free",
    })

    cli.cmd_registry_provision(argparse.Namespace(namespace="myns", docker_login=False))

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "already exists" in captured.err


# ----- `hippius-hub login --password-stdin` -----

def _login_args(**overrides):
    base = {"hippius_token": None, "username": LOGIN, "password": None, "token": None,
            "password_stdin": True}
    base.update(overrides)
    return argparse.Namespace(**base)


@pytest.mark.parametrize("stdin, expected", [
    (SECRET + "\n", SECRET),
    (SECRET + "\r\n", SECRET),
    (SECRET, SECRET),
    (" pad " + "\n", " pad "),
])
def test_login_password_stdin_reads_one_line(monkeypatch, saved_logins, stdin, expected):
    monkeypatch.setattr(sys, "stdin", io.StringIO(stdin))

    cli._cmd_login(_login_args())

    assert saved_logins == [{"username": LOGIN, "password": expected, "token": None}]


@pytest.mark.parametrize("stdin", ["", "\n", f"Login: {LOGIN}\n{SECRET}\n"])
def test_login_password_stdin_rejects_empty_or_multiline(monkeypatch, saved_logins, stdin):
    monkeypatch.setattr(sys, "stdin", io.StringIO(stdin))

    with pytest.raises(SystemExit) as exc:
        cli._cmd_login(_login_args())

    assert exc.value.code == 1
    assert saved_logins == []


def test_login_password_stdin_needs_username(monkeypatch, saved_logins):
    monkeypatch.setattr(sys, "stdin", io.StringIO(SECRET))

    with pytest.raises(SystemExit):
        cli._cmd_login(_login_args(username=None))

    assert saved_logins == []


def test_login_password_and_password_stdin_are_exclusive(capsys):
    parser = cli._build_parser()

    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["login", "--username", "u", "--password", "p", "--password-stdin"])

    assert exc.value.code == 2
    assert "not allowed with argument" in capsys.readouterr().err
