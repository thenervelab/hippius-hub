"""respx regression-pins for hippius_hub._harbor.

_harbor.py wraps Harbor's /api/v2.0/ admin API. Each helper builds a URL
template, picks a verb, and decodes specific keys from the response. A
typo (e.g. `projects/{name}` becomes `project/{name}`, or DELETE swapped
to POST) ships silently today — these tests pin one happy path + one
error path per public helper.

Harbor URLs are passed explicitly via the `endpoint=` kwarg, so no
constants need to be monkeypatched — this is a deliberately simpler
plumbing than `test_console_api.py`, which has to dual-patch
`DEFAULT_API_URL`.

The mock host is `https://harbor.test.invalid` (RFC 6761 reserved,
never resolves) so accidental real-network hits fail with DNS rather
than leaking to production.
"""
from __future__ import annotations

import base64

import httpx
import pytest
import respx

from hippius_hub import _harbor
from hippius_hub._harbor import (
    FORBIDDEN,
    harbor_create_project,
    harbor_delete_project,
    harbor_delete_repository,
    harbor_get_artifact,
    harbor_get_project,
    harbor_get_repository,
    harbor_whoami,
    split_repo_id,
)
from hippius_hub.errors import LocalTokenNotFoundError


HARBOR = "https://harbor.test.invalid"


def _basic(username: str, password: str = "x") -> str:
    """Build a `Basic ...` header for a Harbor service account."""
    raw = f"{username}:{password}".encode()
    return "Basic " + base64.b64encode(raw).decode()


# -------- whoami --------

@respx.mock
def test_whoami_user():
    """Non-robot user: GET /api/v2.0/users/current and reshape to HF schema.

    Pins the response-field mapping: `username` -> `name`, `realname` ->
    `fullname` (with username fallback), `user_id` -> stringified `id`.
    """
    route = respx.get(f"{HARBOR}/api/v2.0/users/current").mock(
        return_value=httpx.Response(200, json={
            "user_id": 42, "username": "alice",
            "realname": "Alice A", "email": "a@b.c",
        }),
    )
    result = harbor_whoami(_basic("alice"), endpoint=HARBOR)
    assert route.called
    assert result == {
        "type": "user", "id": "42", "name": "alice",
        "fullname": "Alice A", "email": "a@b.c",
        "emailVerified": True, "isPro": False, "orgs": [],
    }


def test_whoami_robot_short_circuits():
    """Basic auth username starting with `robot$` must NOT hit the network.

    Harbor returns 401 to /users/current for robot accounts; the synthesized
    dict is the only viable answer. No respx.mock decorator here — if this
    test ever issues a real httpx.get it MUST fail loudly.
    """
    result = harbor_whoami(_basic("robot$proj+ci"), endpoint=HARBOR)
    assert result["type"] == "robot"
    assert result["id"] == "robot$proj+ci"
    assert result["email"] is None


@respx.mock
def test_whoami_401_raises_local_token_not_found():
    """401 to /users/current must surface as LocalTokenNotFoundError.

    CLI catches this to print "run login again" — a generic httpx error
    would skip that guidance.
    """
    respx.get(f"{HARBOR}/api/v2.0/users/current").mock(
        return_value=httpx.Response(401, json={"errors": []}),
    )
    with pytest.raises(LocalTokenNotFoundError):
        harbor_whoami(_basic("alice"), endpoint=HARBOR)


# -------- create_project --------

@respx.mock
def test_create_project_returns_id_from_location_header():
    """POST /api/v2.0/projects; parse the project id out of Location header."""
    route = respx.post(f"{HARBOR}/api/v2.0/projects").mock(
        return_value=httpx.Response(201, headers={"Location": "/api/v2.0/projects/77"}),
    )
    import json as _json
    pid = harbor_create_project(_basic("alice"), "myproj", public=True, endpoint=HARBOR)
    assert pid == 77
    body = _json.loads(route.calls.last.request.content)
    assert body == {"project_name": "myproj", "public": True}
    assert route.calls.last.request.headers["Content-Type"] == "application/json"


@respx.mock
def test_create_project_no_location_returns_sentinel():
    """Missing Location header must yield -1 (not crash on header lookup)."""
    respx.post(f"{HARBOR}/api/v2.0/projects").mock(
        return_value=httpx.Response(201),
    )
    assert harbor_create_project(_basic("a"), "p", endpoint=HARBOR) == -1


@respx.mock
def test_create_project_409_raises():
    """409 (already exists) must surface as httpx.HTTPStatusError."""
    respx.post(f"{HARBOR}/api/v2.0/projects").mock(
        return_value=httpx.Response(409, json={"errors": [{"code": "CONFLICT"}]}),
    )
    with pytest.raises(httpx.HTTPStatusError):
        harbor_create_project(_basic("a"), "p", endpoint=HARBOR)


# -------- get_project --------

@respx.mock
def test_get_project_200_returns_body():
    """GET /api/v2.0/projects/<name> returns the decoded JSON."""
    route = respx.get(f"{HARBOR}/api/v2.0/projects/myproj").mock(
        return_value=httpx.Response(200, json={"project_id": 1, "name": "myproj"}),
    )
    result = harbor_get_project(_basic("a"), "myproj", endpoint=HARBOR)
    assert result == {"project_id": 1, "name": "myproj"}
    assert route.called


@respx.mock
def test_get_project_404_returns_none():
    """404 must return None (consumer pattern: `if result is None`)."""
    respx.get(f"{HARBOR}/api/v2.0/projects/missing").mock(
        return_value=httpx.Response(404),
    )
    assert harbor_get_project(_basic("a"), "missing", endpoint=HARBOR) is None


@respx.mock
def test_get_project_403_returns_forbidden_sentinel():
    """401/403 must return the FORBIDDEN sentinel, NOT None.

    Distinguishing "can't see it" from "doesn't exist" matters for the
    private/public ModelInfo flag — collapsing them would mis-report
    private projects as missing. See _harbor.harbor_get_project docstring.
    """
    respx.get(f"{HARBOR}/api/v2.0/projects/p").mock(
        return_value=httpx.Response(403),
    )
    assert harbor_get_project(_basic("a"), "p", endpoint=HARBOR) is FORBIDDEN


# -------- delete_project --------

@respx.mock
def test_delete_project():
    """DELETE /api/v2.0/projects/<name>."""
    route = respx.delete(f"{HARBOR}/api/v2.0/projects/p").mock(
        return_value=httpx.Response(200),
    )
    harbor_delete_project(_basic("a"), "p", endpoint=HARBOR)
    assert route.called
    assert route.calls.last.request.method == "DELETE"


@respx.mock
def test_delete_project_missing_ok_swallows_404():
    """missing_ok=True must not raise on 404."""
    respx.delete(f"{HARBOR}/api/v2.0/projects/nope").mock(
        return_value=httpx.Response(404),
    )
    harbor_delete_project(_basic("a"), "nope", endpoint=HARBOR, missing_ok=True)


@respx.mock
def test_delete_project_404_without_missing_ok_raises():
    """Default behavior: 404 propagates."""
    respx.delete(f"{HARBOR}/api/v2.0/projects/nope").mock(
        return_value=httpx.Response(404),
    )
    with pytest.raises(httpx.HTTPStatusError):
        harbor_delete_project(_basic("a"), "nope", endpoint=HARBOR)


# -------- get_repository --------

@respx.mock
def test_get_repository_encodes_slash():
    """Repo names with `/` must be URL-encoded as `%2F`.

    Harbor rejects literal slashes in the repo segment. A refactor that
    passes the raw name would 404 silently — pin the encoding.
    """
    route = respx.get(
        f"{HARBOR}/api/v2.0/projects/proj/repositories/foo%2Fbar"
    ).mock(return_value=httpx.Response(200, json={"name": "proj/foo/bar"}))
    result = harbor_get_repository(_basic("a"), "proj", "foo/bar", endpoint=HARBOR)
    assert route.called
    assert result == {"name": "proj/foo/bar"}


@respx.mock
def test_get_repository_404_returns_none():
    """404 must return None."""
    respx.get(f"{HARBOR}/api/v2.0/projects/proj/repositories/r").mock(
        return_value=httpx.Response(404),
    )
    assert harbor_get_repository(_basic("a"), "proj", "r", endpoint=HARBOR) is None


# -------- delete_repository --------

@respx.mock
def test_delete_repository_encodes_slash():
    """DELETE /api/v2.0/projects/<proj>/repositories/<encoded>."""
    route = respx.delete(
        f"{HARBOR}/api/v2.0/projects/proj/repositories/foo%2Fbar"
    ).mock(return_value=httpx.Response(200))
    harbor_delete_repository(_basic("a"), "proj", "foo/bar", endpoint=HARBOR)
    assert route.called
    assert route.calls.last.request.method == "DELETE"


@respx.mock
def test_delete_repository_missing_ok():
    """missing_ok=True swallows 404."""
    respx.delete(f"{HARBOR}/api/v2.0/projects/proj/repositories/r").mock(
        return_value=httpx.Response(404),
    )
    harbor_delete_repository(_basic("a"), "proj", "r", endpoint=HARBOR, missing_ok=True)


# -------- get_artifact --------

@respx.mock
def test_get_artifact():
    """GET /api/v2.0/projects/<proj>/repositories/<encoded>/artifacts/<ref>.

    Pins: slash-encoding on repo, default with_tag="true" + with_label="false".
    """
    route = respx.get(
        f"{HARBOR}/api/v2.0/projects/proj/repositories/foo%2Fbar/artifacts/v1"
    ).mock(return_value=httpx.Response(200, json={"digest": "sha256:abc"}))
    result = harbor_get_artifact(_basic("a"), "proj", "foo/bar", "v1", endpoint=HARBOR)
    assert result == {"digest": "sha256:abc"}
    p = route.calls.last.request.url.params
    assert p["with_tag"] == "true"
    assert p["with_label"] == "false"


@respx.mock
def test_get_artifact_404_returns_none():
    """404 -> None (no exception); 401/403 -> FORBIDDEN."""
    respx.get(
        f"{HARBOR}/api/v2.0/projects/proj/repositories/r/artifacts/missing"
    ).mock(return_value=httpx.Response(404))
    assert harbor_get_artifact(_basic("a"), "proj", "r", "missing", endpoint=HARBOR) is None


# -------- split_repo_id --------

def test_split_repo_id_simple():
    """`proj/repo` -> (`proj`, `repo`)."""
    assert split_repo_id("proj/repo") == ("proj", "repo")


def test_split_repo_id_multi_slash_keeps_remainder():
    """Multi-slash repo_id keeps everything after the first `/` as the repo."""
    assert split_repo_id("proj/foo/bar") == ("proj", "foo/bar")


def test_split_repo_id_rejects_no_slash():
    """Single-segment must raise ValueError (Harbor requires a project)."""
    with pytest.raises(ValueError, match="project/repository"):
        split_repo_id("solo")
