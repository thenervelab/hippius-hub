"""Unit coverage for the `registry repos delete` CLI handler.

Mirrors `hf repos delete`. These tests drive the handler directly with a
stubbed `delete_repo` so no network is touched — they pin the confirmation
prompt, argument pass-through, the success line, and the httpx/auth error
mapping. The live round-trip (create → delete → gone) lives in
`test_repo_lifecycle.py` under the `e2e` marker.
"""
import argparse

import httpx
import pytest

from hippius_hub import cli
from hippius_hub.errors import RepositoryNotFoundError


def _args(**over):
    """Build the Namespace the parser would produce for `repos delete`."""
    base = dict(repo_id="org/model", repo_type=None, token=None,
                missing_ok=False, yes=False)
    base.update(over)
    return argparse.Namespace(func=cli.cmd_registry_repos_delete, **base)


@pytest.fixture
def spy_delete(monkeypatch):
    """Replace cli.delete_repo with a recorder; nothing hits the network."""
    calls = []

    def _fake(repo_id, **kw):
        calls.append((repo_id, kw))

    monkeypatch.setattr(cli, "delete_repo", _fake)
    return calls


# ---------- confirmation prompt ----------

def test_yes_flag_skips_prompt_and_deletes(spy_delete, monkeypatch, capsys):
    """`--yes` must not read stdin at all and must call delete_repo."""
    def _boom(_prompt):
        raise AssertionError("input() must not be called when --yes is set")
    monkeypatch.setattr("builtins.input", _boom)

    cli.cmd_registry_repos_delete(_args(yes=True))

    assert spy_delete == [("org/model", {"token": None, "repo_type": None,
                                         "missing_ok": False})]
    assert "Repo deleted: org/model" in capsys.readouterr().out


@pytest.mark.parametrize("reply", ["y", "yes", "Y", "YES", " yes "])
def test_prompt_affirmative_proceeds(spy_delete, monkeypatch, reply):
    monkeypatch.setattr("builtins.input", lambda _p: reply)
    cli.cmd_registry_repos_delete(_args())
    assert len(spy_delete) == 1


@pytest.mark.parametrize("reply", ["n", "no", "", "x", "delete"])
def test_prompt_negative_aborts_without_deleting(spy_delete, monkeypatch, capsys, reply):
    monkeypatch.setattr("builtins.input", lambda _p: reply)
    cli.cmd_registry_repos_delete(_args())
    assert spy_delete == []
    assert "Aborted." in capsys.readouterr().out


def test_prompt_text_matches_hf_wording(spy_delete, monkeypatch):
    """The prompt names the repo_type + repo_id like `hf repos delete` does."""
    seen = {}

    def _capture(prompt):
        seen["p"] = prompt
        return "y"

    monkeypatch.setattr("builtins.input", _capture)
    cli.cmd_registry_repos_delete(_args(repo_type="dataset"))
    assert "permanently delete dataset 'org/model'" in seen["p"]


# ---------- argument pass-through ----------

def test_passthrough_repo_type_missing_ok_token(spy_delete):
    cli.cmd_registry_repos_delete(
        _args(yes=True, repo_type="space", missing_ok=True, token="Bearer x")
    )
    assert spy_delete == [("org/model", {"token": "Bearer x",
                                         "repo_type": "space",
                                         "missing_ok": True})]


# ---------- error mapping ----------

def _raise(exc):
    def _f(*a, **k):
        raise exc
    return _f


def test_403_maps_to_admin_message_exit_1(monkeypatch, capsys):
    resp = httpx.Response(403, request=httpx.Request("DELETE", "https://x/y"))
    monkeypatch.setattr(cli, "delete_repo",
                        _raise(httpx.HTTPStatusError("no", request=resp.request, response=resp)))
    with pytest.raises(SystemExit) as ei:
        cli.cmd_registry_repos_delete(_args(yes=True))
    assert ei.value.code == 1
    assert "admin permissions" in capsys.readouterr().out


def test_404_maps_to_not_found_exit_11(monkeypatch, capsys):
    resp = httpx.Response(404, request=httpx.Request("DELETE", "https://x/y"))
    monkeypatch.setattr(cli, "delete_repo",
                        _raise(httpx.HTTPStatusError("no", request=resp.request, response=resp)))
    with pytest.raises(SystemExit) as ei:
        cli.cmd_registry_repos_delete(_args(yes=True))
    assert ei.value.code == 11
    assert "not found" in capsys.readouterr().out.lower()


def test_other_http_status_bubbles(monkeypatch):
    """A non-403/404 (e.g. 500) is not swallowed — it must propagate."""
    resp = httpx.Response(500, request=httpx.Request("DELETE", "https://x/y"))
    monkeypatch.setattr(cli, "delete_repo",
                        _raise(httpx.HTTPStatusError("boom", request=resp.request, response=resp)))
    with pytest.raises(httpx.HTTPStatusError):
        cli.cmd_registry_repos_delete(_args(yes=True))


def test_no_credentials_maps_to_login_hint_exit_1(monkeypatch, capsys):
    fake_resp = httpx.Response(401, request=httpx.Request("GET", "https://x/y"))
    monkeypatch.setattr(cli, "delete_repo",
                        _raise(RepositoryNotFoundError("delete_repo requires authentication",
                                                       response=fake_resp)))
    with pytest.raises(SystemExit) as ei:
        cli.cmd_registry_repos_delete(_args(yes=True))
    assert ei.value.code == 1
    assert "login" in capsys.readouterr().out.lower()
