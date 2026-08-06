"""Edge cases for `hippius-hub revisions` and the `_list_tags` status mapping.

Motivating incident: three users reported "can't fetch older models". Two of
the repos had been deleted outright (registry answers 404), but the third sat
in a namespace that had been deleted wholesale — and for that the registry
answers **401**, not 404, because it will not confirm whether a private
namespace exists. `_list_tags` handled 404 and let everything else fall into
`resp.raise_for_status()`, so the single most useful diagnostic command
(`hippius-hub revisions <repo>`) answered a deleted namespace with a raw
`httpx.HTTPStatusError` traceback instead of a diagnosis.

Three properties are pinned here, because each failed independently:

1. **Status mapping** — 401 -> RepositoryNotFoundError, 403 -> GatedRepoError,
   matching `huggingface_hub.hf_raise_for_status` so drop-in callers catching
   HF's typed errors keep working. 5xx must still raise: a genuine server
   fault is not "your repo is gone".
2. **Exit codes** — every one of these paths must land on the documented code
   from `_format_download_error` (11 not-found / 14 access-denied / 16 HTTP),
   so a CI wrapper can branch. `revisions` previously hardcoded `sys.exit(1)`
   for the 404 path, making "this repo is gone" indistinguishable from any
   other generic failure.
3. **No traceback** — the user-visible property. A traceback for an *expected*
   registry condition reads as "the tool is broken" rather than "your repo is
   gone", which is what sent the incident to engineering instead of resolving
   at the CLI. Guarded without over-reaching: a genuine bug (TypeError) must
   STILL traceback, or this backstop would hide real defects.
"""
from __future__ import annotations

import argparse

import httpx
import pytest
import respx

from hippius_hub import cli
from hippius_hub._repo_ops import _list_tags, list_repo_refs, repo_exists
from hippius_hub.errors import GatedRepoError, RepositoryNotFoundError

from tests.respx_fixtures import MOCK_REGISTRY, token_route

REPO = "someproject/somerepo"


def _tags_route(mock_router, *, status: int, json_body=None):
    """Stub `GET /v2/{REPO}/tags/list` with a given status.

    Matched with `url__startswith` because `_list_tags` appends an explicit
    `?n=1000` page size (without it the registry returns only the first tag —
    see the comment in `_list_tags`), and we don't want the route coupled to
    that constant.
    """
    response = httpx.Response(status, json=json_body) if json_body is not None else httpx.Response(status)
    return mock_router.get(
        url__startswith=f"{MOCK_REGISTRY}/v2/{REPO}/tags/list"
    ).mock(return_value=response)


def _revisions_args(repo_id: str = REPO, *, as_json: bool = False) -> argparse.Namespace:
    """Build the Namespace `cmd_revisions` reads (repo_id / repo_type / json)."""
    return argparse.Namespace(repo_id=repo_id, repo_type=None, json=as_json)


@pytest.fixture(autouse=True)
def _no_saved_credentials(monkeypatch):
    """Force the anonymous-token path.

    `cmd_revisions` calls `resolve_token_value(None)`, which would otherwise
    read the developer's real `~/.cache/hippius/hub/token`. Pinning it to None
    keeps these tests hermetic and identical on CI and a logged-in laptop.
    """
    monkeypatch.setattr(cli, "resolve_token_value", lambda _: None)


# ---------- 1. `_list_tags` status mapping ----------

@respx.mock
def test_list_tags_returns_none_on_404(monkeypatched_registry):
    """404 = repo genuinely absent. Sentinel stays None (not an exception):
    callers like `create_repo` branch on it to decide whether to create."""
    token_route(respx.mock)
    _tags_route(respx.mock, status=404)
    assert _list_tags(MOCK_REGISTRY, REPO, "tok") is None


@respx.mock
def test_list_tags_raises_repository_not_found_on_401(monkeypatched_registry):
    """The regression under test: a deleted *namespace* answers 401.

    Must be a typed RepositoryNotFoundError, not httpx.HTTPStatusError.
    """
    token_route(respx.mock)
    _tags_route(respx.mock, status=401)
    with pytest.raises(RepositoryNotFoundError) as exc:
        _list_tags(MOCK_REGISTRY, REPO, "tok")
    # The real response must ride along for callers that inspect `.response`.
    assert exc.value.response is not None
    assert exc.value.response.status_code == 401


@respx.mock
def test_list_tags_401_message_mentions_login(monkeypatched_registry):
    """401 is ambiguous (deleted namespace OR private repo we can't see), so
    the message must name both and point at the one action the user can take.
    A bare 'not found' would strand a user who is merely logged out."""
    token_route(respx.mock)
    _tags_route(respx.mock, status=401)
    with pytest.raises(RepositoryNotFoundError) as exc:
        _list_tags(MOCK_REGISTRY, REPO, "tok")
    message = str(exc.value)
    assert "deleted" in message.lower()
    assert "login" in message.lower()


@respx.mock
def test_list_tags_raises_gated_on_403(monkeypatched_registry):
    """403 = the repo exists and we were refused. Distinct from 401 so the
    CLI can say 'access denied' (14) rather than 'not found' (11)."""
    token_route(respx.mock)
    _tags_route(respx.mock, status=403)
    with pytest.raises(GatedRepoError):
        _list_tags(MOCK_REGISTRY, REPO, "tok")


@respx.mock
def test_list_tags_still_raises_on_500(monkeypatched_registry):
    """A 5xx must NOT be laundered into 'repository not found'.

    Reporting a registry outage as a missing repo would send users chasing a
    data-loss scare during what is actually a transient backend failure.
    """
    token_route(respx.mock)
    _tags_route(respx.mock, status=500)
    with pytest.raises(httpx.HTTPStatusError):
        _list_tags(MOCK_REGISTRY, REPO, "tok")


@respx.mock
def test_list_tags_returns_empty_list_for_repo_with_no_tags(monkeypatched_registry):
    """Empty list and None are NOT interchangeable: [] means 'exists, never
    tagged', None means 'does not exist'. `create_repo` branches on that."""
    token_route(respx.mock)
    _tags_route(respx.mock, status=200, json_body={"tags": []})
    assert _list_tags(MOCK_REGISTRY, REPO, "tok") == []


@respx.mock
def test_list_tags_returns_tags_on_success(monkeypatched_registry):
    token_route(respx.mock)
    _tags_route(respx.mock, status=200, json_body={"tags": ["main", "v2"]})
    assert _list_tags(MOCK_REGISTRY, REPO, "tok") == ["main", "v2"]


# ---------- 2. `revisions` exit codes ----------

@respx.mock
def test_revisions_404_exits_11(monkeypatched_registry, capsys):
    """Was `sys.exit(1)` — indistinguishable from any generic failure."""
    token_route(respx.mock)
    _tags_route(respx.mock, status=404)
    with pytest.raises(SystemExit) as exc:
        cli.cmd_revisions(_revisions_args())
    assert exc.value.code == 11
    assert "not found" in capsys.readouterr().out.lower()


@respx.mock
def test_revisions_401_exits_11_via_typed_error(monkeypatched_registry):
    """Deleted namespace: reaches the caller as a typed error (which `main`
    renders), NOT as an httpx traceback."""
    token_route(respx.mock)
    _tags_route(respx.mock, status=401)
    with pytest.raises(RepositoryNotFoundError):
        cli.cmd_revisions(_revisions_args())


@respx.mock
def test_revisions_empty_repo_does_not_exit(monkeypatched_registry, capsys):
    """A pushed-but-untagged repo is a success (exit 0), not an error. Pinning
    this stops a future 'treat empty as missing' simplification."""
    token_route(respx.mock)
    _tags_route(respx.mock, status=200, json_body={"tags": []})
    cli.cmd_revisions(_revisions_args())  # must NOT raise SystemExit
    assert "No revisions yet." in capsys.readouterr().out


# ---------- 3. `main()` renders typed errors without a traceback ----------

def _run_main(monkeypatch, argv, handler):
    """Drive `cli.main()` with a stubbed `revisions` handler."""
    monkeypatch.setattr("sys.argv", argv)
    monkeypatch.setattr(cli, "cmd_revisions", handler)
    return cli.main()


@pytest.mark.parametrize(
    "exc, expected_code",
    [
        (RepositoryNotFoundError(
            "gone", response=httpx.Response(401, request=httpx.Request("GET", "about:blank"))), 11),
        (GatedRepoError(
            "denied", response=httpx.Response(403, request=httpx.Request("GET", "about:blank"))), 14),
    ],
)
def test_main_renders_typed_errors_as_clean_exit(monkeypatch, capsys, exc, expected_code):
    """The user-visible contract: a typed registry error exits with its
    documented code and prints a message — no stack trace."""
    def _raise(_args):
        raise exc

    with pytest.raises(SystemExit) as got:
        _run_main(monkeypatch, ["hippius-hub", "revisions", REPO], _raise)
    assert got.value.code == expected_code
    captured = capsys.readouterr()
    assert "Traceback" not in captured.out and "Traceback" not in captured.err
    assert "❌" in captured.out


def test_main_lets_genuine_bugs_traceback(monkeypatch):
    """Counterpart to the above, and the reason the backstop is narrow.

    A TypeError is a defect in our code, not a registry condition. If the
    dispatch-level handler ever widened to `except Exception`, real bugs would
    be flattened into a tidy "❌ Operation failed" with no traceback and become
    substantially harder to debug. This test fails if that happens.
    """
    def _raise(_args):
        raise TypeError("this is a real bug")

    with pytest.raises(TypeError):
        _run_main(monkeypatch, ["hippius-hub", "revisions", REPO], _raise)


# ---------- 4. HF drop-in parity ----------

@respx.mock
def test_repo_exists_false_on_401(monkeypatched_registry):
    """`huggingface_hub.repo_exists` answers with a bool, never an exception.
    Since `_list_tags` now raises on 401, `repo_exists` must absorb it."""
    token_route(respx.mock)
    _tags_route(respx.mock, status=401)
    assert repo_exists(REPO, token=False) is False


@respx.mock
def test_repo_exists_true_on_403_gated(monkeypatched_registry):
    """Ordering guard. GatedRepoError IS-A RepositoryNotFoundError, so if the
    two except-arms in `repo_exists` were swapped, a gated repo would report
    False — claiming a repo that demonstrably exists does not."""
    token_route(respx.mock)
    _tags_route(respx.mock, status=403)
    assert repo_exists(REPO, token=False) is True


@respx.mock
def test_repo_exists_false_on_404(monkeypatched_registry):
    token_route(respx.mock)
    _tags_route(respx.mock, status=404)
    assert repo_exists(REPO, token=False) is False


@respx.mock
def test_repo_exists_true_when_tagged(monkeypatched_registry):
    token_route(respx.mock)
    _tags_route(respx.mock, status=200, json_body={"tags": ["main"]})
    assert repo_exists(REPO, token=False) is True


@respx.mock
def test_repo_exists_false_for_untagged_repo(monkeypatched_registry):
    """Exists in the registry but never pushed a tag -> False, per the
    docstring contract ('has ever been pushed to')."""
    token_route(respx.mock)
    _tags_route(respx.mock, status=200, json_body={"tags": []})
    assert repo_exists(REPO, token=False) is False


@respx.mock
def test_list_repo_refs_raises_typed_error_on_401(monkeypatched_registry):
    """`list_repo_refs` already mapped 404 -> RepositoryNotFoundError; 401 fell
    through to httpx. Both now agree, so `except RepositoryNotFoundError`
    around it catches a deleted namespace as well as a deleted repo."""
    token_route(respx.mock)
    _tags_route(respx.mock, status=401)
    with pytest.raises(RepositoryNotFoundError):
        list_repo_refs(REPO, token=False)
