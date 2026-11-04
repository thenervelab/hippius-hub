"""Shutdown notices: the import warning and the per-surface Harbor notices (issue #159).

The import warning runs in a subprocess: the package is already imported here, and
Python runs its `__init__` once per process. Harbor calls go to the RFC 6761 reserved
host `harbor.test.invalid` behind respx, so nothing reaches the network.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import warnings

import httpx
import pytest
import respx

from hippius_hub import _harbor, _oci, _sunset, auth, cli
from hippius_hub.errors import LocalTokenNotFoundError

HARBOR = "https://harbor.test.invalid"
REPLACED = "hippius_hub is replaced by huggingface_hub"


def _python(*args: str) -> subprocess.CompletedProcess:
    """Run this interpreter with `args` and no inherited warning filters."""
    env = {k: v for k, v in os.environ.items() if k != "PYTHONWARNINGS"}
    return subprocess.run(
        [sys.executable, *args], capture_output=True, text=True, env=env, check=True
    )


def _notice(surface: str) -> str:
    """The start of the notice for `surface`, as a `pytest.warns(match=...)` pattern."""
    return "^" + re.escape(
        f"{surface} depends on the Hippius registry (Harbor) and stops working on "
        f"{_sunset.HARBOR_SHUTDOWN_DATE}."
    )


def test_import_warns_once_with_migration_steps():
    stderr = _python(
        "-c", "import importlib, hippius_hub; importlib.import_module('hippius_hub')"
    ).stderr

    assert stderr.count(REPLACED) == 1, stderr
    assert "FutureWarning" in stderr
    for step in (
        f"stops working on {_sunset.HARBOR_SHUTDOWN_DATE}",
        f"export HF_ENDPOINT={_sunset.HUB_URL} before Python starts",
        "hip_ API key as the token",
        "replace `hippius_hub` imports with `huggingface_hub`",
        _sunset.MIGRATION_GUIDE_URL,
    ):
        assert step in stderr, step


@pytest.mark.parametrize(
    "args",
    [
        ("-W", "ignore:hippius_hub:FutureWarning", "-c", "import hippius_hub"),
        (
            "-c",
            "import warnings; warnings.filterwarnings('ignore', 'hippius_hub', FutureWarning);"
            " import hippius_hub",
        ),
    ],
    ids=["command-line", "filterwarnings"],
)
def test_documented_filter_silences_import_warning(args):
    assert REPLACED not in _python(*args).stderr


def test_login_and_logout_warn(tmp_path, monkeypatch):
    monkeypatch.setattr(auth, "DEFAULT_CACHE_DIR", str(tmp_path))
    monkeypatch.setattr(auth, "TOKEN_PATH", str(tmp_path / "token"))

    with pytest.warns(FutureWarning, match=_notice("hippius_hub.login()")):
        auth.login(token="secret")
    with pytest.warns(FutureWarning, match=_notice("hippius_hub.logout()")):
        auth.logout()


def test_whoami_warns_before_resolving_credentials():
    with pytest.warns(FutureWarning, match=_notice("hippius_hub.whoami()")) as record:
        with pytest.raises(LocalTokenNotFoundError):
            auth.whoami(token=False)

    # stacklevel names the caller's line, so a user can find the call to migrate.
    assert record[0].filename == __file__


@respx.mock
def test_internal_harbor_calls_do_not_warn_again():
    """whoami calls _harbor.harbor_whoami; only whoami's own notice reaches the user."""
    respx.get(f"{HARBOR}/api/v2.0/users/current").mock(
        return_value=httpx.Response(200, json={"username": "me", "user_id": 1})
    )

    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        auth.whoami(token="Bearer x", endpoint=HARBOR)

    assert [str(w.message).split(" depends on")[0] for w in record] == ["hippius_hub.whoami()"]


@respx.mock
@pytest.mark.parametrize(
    "call",
    [
        lambda: _harbor.harbor_get_project("Bearer x", "ns", endpoint=HARBOR),
        lambda: _harbor.harbor_delete_project("Bearer x", "ns", endpoint=HARBOR, missing_ok=True),
        lambda: _harbor.harbor_get_repository("Bearer x", "ns", "repo", endpoint=HARBOR),
        lambda: _harbor.harbor_delete_repository(
            "Bearer x", "ns", "repo", endpoint=HARBOR, missing_ok=True
        ),
        lambda: _harbor.harbor_get_artifact("Bearer x", "ns", "repo", "main", endpoint=HARBOR),
    ],
    ids=["get_project", "delete_project", "get_repository", "delete_repository", "get_artifact"],
)
def test_harbor_lookups_warn(call):
    respx.route(host="harbor.test.invalid").mock(return_value=httpx.Response(404))

    with pytest.warns(FutureWarning, match=_notice("hippius_hub._harbor")):
        call()


@respx.mock
def test_harbor_whoami_and_create_project_warn():
    respx.get(f"{HARBOR}/api/v2.0/users/current").mock(
        return_value=httpx.Response(200, json={"username": "me", "user_id": 1})
    )
    respx.post(f"{HARBOR}/api/v2.0/projects").mock(return_value=httpx.Response(201))

    with pytest.warns(FutureWarning, match=_notice("hippius_hub._harbor")):
        _harbor.harbor_whoami("Bearer x", endpoint=HARBOR)
    with pytest.warns(FutureWarning, match=_notice("hippius_hub._harbor")):
        _harbor.harbor_create_project("Bearer x", "ns", endpoint=HARBOR)


@respx.mock
def test_oci_manifest_requests_warn():
    respx.route(host="harbor.test.invalid").mock(return_value=httpx.Response(404))

    with pytest.warns(FutureWarning, match=_notice("hippius_hub._oci")):
        assert _oci.fetch_manifest(HARBOR, "ns/repo", "main", "tok", missing_ok=True) is None
    with pytest.warns(FutureWarning, match=_notice("hippius_hub._oci")):
        assert _oci.head_manifest(HARBOR, "ns/repo", "main", "tok").status_code == 404


@pytest.mark.parametrize(("command", "notified"), [("registry", True), ("models", False)])
def test_cli_registry_prints_notice(command, notified, monkeypatch, capsys):
    monkeypatch.setenv("HIPPIUS_HUB_NO_UPDATE_CHECK", "1")
    monkeypatch.setattr(sys, "argv", ["hippius-hub", command])

    # Without a subcommand both print their help and exit 1, with no network call.
    with pytest.raises(SystemExit):
        cli.main()

    captured = capsys.readouterr()
    notice = re.compile(_notice("hippius-hub registry"), re.MULTILINE)
    assert bool(notice.search(captured.err)) is notified
    assert "Hippius registry" not in captured.out
