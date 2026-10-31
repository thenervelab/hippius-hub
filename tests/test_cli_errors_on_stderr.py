"""A failing command prints its error on stderr and leaves stdout empty (issue #132).

`registry keys create NAME --role R > key.txt` must not write an API error into
key.txt, where a later `--password-stdin < key.txt` would send it as the password.
Each case drives `cli.main()` with one failure stubbed at its boundary (console API,
`login()`, Harbor, the upload, stdin) and checks the shell-visible contract: a
nonzero exit, nothing on stdout, the message on stderr.
"""
from __future__ import annotations

import io
import sys

import httpx
import pytest

from hippius_hub import cli
from hippius_hub.console import ConsoleError
from hippius_hub.errors import RepositoryNotFoundError

CLI = "hippius_hub.cli"


def _raise(exc):
    def _f(*_a, **_k):
        raise exc
    return _f


def _console_error(status):
    return _raise(ConsoleError(status, {"detail": f"stubbed {status}"}))


def _http_error(status):
    resp = httpx.Response(status, request=httpx.Request("DELETE", "https://x/y"))
    return _raise(httpx.HTTPStatusError("no", request=resp.request, response=resp))


def _repo_not_found(status):
    resp = httpx.Response(status, request=httpx.Request("GET", "https://x/y"))
    return _raise(RepositoryNotFoundError("gone", response=resp))


KEYS_CREATE = ["registry", "keys", "create", "agent", "--role", "push"]
REPOS_DELETE = ["registry", "repos", "delete", "org/m", "--yes"]

# id: (argv, dotted name to stub or None, stub, text expected on stderr)
CASES = {
    # The issue's case: the API refuses to mint the key.
    "keys create, 401": (KEYS_CREATE, f"{CLI}.console.create_key", _console_error(401),
                         "Not logged in"),
    "keys create, 404": (KEYS_CREATE, f"{CLI}.console.create_key", _console_error(404),
                         "Not found"),
    "keys create, 500": (KEYS_CREATE, f"{CLI}.console.create_key", _console_error(500),
                         "Hippius API error 500"),
    "provision, 402": (["registry", "provision", "myns"], f"{CLI}.console.provision",
                       _console_error(402), "Not enough credits"),
    "check, taken": (["registry", "check", "myns"], f"{CLI}.console.check_namespace",
                     lambda _n: {"available": False, "message": "myns is taken"},
                     "myns is taken"),
    "subscribe, unknown plan": (["registry", "subscribe", "nope"],
                                f"{CLI}.console.list_plans",
                                lambda: [{"id": 1, "name": "Basic"}], "Unknown plan"),
    "artifacts, bad repo": (["registry", "artifacts", "no-slash"], None, None,
                            "<project>/<repo>"),
    "models show, bad repo": (["models", "show", "no-slash"], None, None,
                              "<project>/<repo>"),
    "repos delete, 403": (REPOS_DELETE, f"{CLI}.delete_repo", _http_error(403),
                          "admin permissions"),
    "repos delete, 404": (REPOS_DELETE, f"{CLI}.delete_repo", _http_error(404),
                          "Repository not found"),
    "repos delete, no credentials": (REPOS_DELETE, f"{CLI}.delete_repo",
                                     _repo_not_found(401), "Not logged in"),
    "revisions, not found": (["revisions", "org/m"], f"{CLI}._list_tags",
                             lambda *_a, **_k: None, "Repository not found"),
    "diagnose, typed error": (["diagnose", "org/m", "f.bin"], f"{CLI}._cmd_diagnose",
                              _repo_not_found(404), "Repository not found"),
    "upload, failure": (["upload", "org/m", "missing.bin"],
                        "hippius_hub.file_upload.hippius_hub_upload",
                        _raise(ValueError("boom")), "Operation failed"),
    "login, rejected": (["login", "--username", "u", "--password", "p"], f"{CLI}.login",
                        _raise(ValueError("bad credentials")), "Login failed"),
    "login, password-stdin without username": (["login", "--password-stdin"], None, None,
                                               "needs --username"),
    "download, failure": (["download", "org/m", "f.bin"], f"{CLI}.hippius_hub_download",
                          _raise(ValueError("boom")), "Operation failed"),
    "no command": ([], None, None, "usage:"),
    "registry without subcommand": (["registry"], None, None, "usage:"),
    "models without subcommand": (["models"], None, None, "usage:"),
}


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    monkeypatch.setenv("HIPPIUS_HUB_NO_UPDATE_CHECK", "1")
    monkeypatch.setattr(cli, "resolve_token_value", lambda *_a, **_k: "tok")
    monkeypatch.setattr(cli, "get_oci_bearer_token", lambda *_a, **_k: "oci")


def _run_main_expecting_failure(monkeypatch, argv):
    monkeypatch.setattr(sys, "argv", ["hippius-hub", *argv])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    return exc.value.code


@pytest.mark.parametrize("argv, target, stub, message", CASES.values(), ids=CASES.keys())
def test_failing_command_prints_error_on_stderr_only(monkeypatch, capsys, argv, target, stub,
                                                     message):
    if target is not None:
        monkeypatch.setattr(target, stub)

    code = _run_main_expecting_failure(monkeypatch, argv)

    assert code != 0
    captured = capsys.readouterr()
    assert captured.out == ""
    assert message in captured.err


@pytest.mark.parametrize("stdin", ["", "line one\nline two\n"])
def test_login_password_stdin_bad_input_prints_error_on_stderr_only(monkeypatch, capsys,
                                                                    stdin):
    monkeypatch.setattr(sys, "stdin", io.StringIO(stdin))

    code = _run_main_expecting_failure(monkeypatch, ["login", "--username", "u",
                                                     "--password-stdin"])

    assert code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "--password-stdin read" in captured.err
